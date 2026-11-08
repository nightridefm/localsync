// Applies frames to a local SQLite database: one frame is one transaction.
// Rows are refcounted by unit membership (_lsync_member), so overlapping
// units on one table need no client-side dedupe, and a row is deleted when
// its last membership goes (docs/design.md section 10.2).
import {
	type ColumnDesc,
	decodeFrame,
	type Op,
	OP_PUT,
	OP_PATCH,
	OP_REMOVE,
	readPatch,
	Reader,
	readRow,
	readValue,
	memberHash,
	rowHash,
	type SqlValue,
	type UnitDesc,
} from './codec.ts'

/** The SQLite surface the applier needs (sqlite-wasm oo1 fits it). */
export interface Db {
	exec(sql: string, bind?: SqlValue[]): void
	all(sql: string, bind?: SqlValue[]): SqlValue[][]
}

const q = (n: string) => `"${n.replaceAll('"', '""')}"`

export const STATE_DDL = `
CREATE TABLE IF NOT EXISTS _lsync_member (
	unit INTEGER NOT NULL, tbl TEXT NOT NULL, pk TEXT NOT NULL, hash TEXT NOT NULL,
	PRIMARY KEY (unit, pk)
) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS _lsync_member_row ON _lsync_member (tbl, pk);
CREATE TABLE IF NOT EXISTS _lsync_unit (id INTEGER PRIMARY KEY, def TEXT NOT NULL UNIQUE, checksum TEXT);
CREATE TABLE IF NOT EXISTS _lsync_table (tbl TEXT PRIMARY KEY, pk_cols TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS _lsync_state (k TEXT PRIMARY KEY, v TEXT NOT NULL);
`

// Databases synced before _lsync_table: unit rows carried held / drift flags
// and their table's key columns. Ids are kept (WANTs already sent use them).
const MIGRATE_UNIT_FLAGS = `
INSERT OR IGNORE INTO _lsync_table SELECT tbl, pk_cols FROM _lsync_unit WHERE tbl IS NOT NULL AND pk_cols IS NOT NULL;
CREATE TABLE _lsync_unit_new (id INTEGER PRIMARY KEY, def TEXT NOT NULL UNIQUE, checksum TEXT);
INSERT INTO _lsync_unit_new SELECT id, def, CASE WHEN held = 1 AND drift = 0 THEN checksum END FROM _lsync_unit;
DROP TABLE _lsync_unit;
ALTER TABLE _lsync_unit_new RENAME TO _lsync_unit
`

export type ApplyResult = { tables: Set<string>; rows: number; drift: number[]; why: string[] }

// Hashes and checksums are unsigned 64-bit, stored as decimal text:
// sqlite-wasm reads INTEGER columns into JS numbers, which would round them.

export class Applier {
	private schemaReady = false
	/** Units by engine ref, for this connection (refs are per engine epoch). */
	private byRef = new Map<number, UnitDesc>()
	/** Unit ids in the latest units message: anything else is dropped. */
	private current = new Set<number>()
	/** The next frame drops units held but no longer current. */
	private sweep = false
	/** Why the last frame drifted, for diagnosis. */
	private why: string[] = []
	/** The fragment being applied, described only if it drifts. */
	private fragNote = ''

	ensureSchema(db: Db): void {
		if (this.schemaReady) return
		for (const stmt of STATE_DDL.split(';')) if (stmt.trim()) db.exec(stmt)
		if (db.all(`SELECT 1 FROM pragma_table_info('_lsync_unit') WHERE name = 'held'`).length) {
			db.exec('BEGIN')
			for (const stmt of MIGRATE_UNIT_FLAGS.split(';')) if (stmt.trim()) db.exec(stmt)
			db.exec('COMMIT')
		}
		this.schemaReady = true
	}

	setUnits(db: Db, units: UnitDesc[]): void {
		this.ensureSchema(db)
		this.current = new Set(units.map((u) => u.id))
		this.sweep = true
		for (const u of units) {
			this.byRef.set(u.ref, u)
			// How rows of the table are keyed, so they can be deleted later
			// even when no unit on it is wanted any more.
			db.exec('INSERT OR REPLACE INTO _lsync_table VALUES (?, ?)', [
				u.table,
				JSON.stringify(u.pk.map((i) => u.columns[i]!.name)),
			])
		}
	}

	resetRefs(): void {
		this.byRef.clear()
	}

	/** Records a unit's checksum after a fragment, or forgets a removed unit. */
	/** check is false for a piece of a unit that continues in a later part. */
	private finishUnit(
		db: Db,
		u: UnitDesc,
		sum: bigint,
		want: bigint,
		drift: number[],
		check = true
	): void {
		// A unit no longer wanted was already dropped by the sweep.
		if (!this.current.has(u.id)) return
		if (!check) {
			db.exec('UPDATE _lsync_unit SET checksum = ? WHERE id = ?', [sum.toString(), u.id])
			return
		}
		// A drifted unit claims no checksum, so the next WANT gets it whole.
		if (sum !== want) {
			drift.push(u.id)
			this.why.push(`unit ${u.id} (${u.rule}): checksum ${sum} != engine ${want} ${this.fragNote}`)
		}
		db.exec('UPDATE _lsync_unit SET checksum = ? WHERE id = ?', [
			sum === want ? sum.toString() : null,
			u.id,
		])
	}

	/**
	 * A run of PUTs in set-based statements per chunk: snapshots, and big
	 * move-ins into a unit that already holds rows. Returns the checksum
	 * delta (the unit's members before are read per chunk, in one query).
	 */
	private bulkPut(
		db: Db,
		u: UnitDesc,
		ops: Op[],
		orphans: Map<string, SqlValue[]> | undefined
	): bigint {
		const sql = bulkUpsertSql(u)
		// Per key, the last row wins (the upserts apply in order) and the
		// first sighting decides what it replaced: each row counts once.
		const hashes = new Map<string, bigint>()
		let delta = 0n
		const values: SqlValue[] = []
		for (let start = 0; start < ops.length; start += BULK_CHUNK) {
			const end = Math.min(start + BULK_CHUNK, ops.length)
			const rows: Array<Array<string | number | null>> = []
			const members: Array<[string, string]> = []
			const fresh: string[] = []
			for (let i = start; i < end; i++) {
				const op = ops[i]!
				readRow(new Reader(op.bytes), u.columns, values)
				rows.push(values.map(jsonable))
				const key = JSON.stringify(u.pk.map((j) => jsonable(values[j]!)))
				const h = memberHash(rowHash(u.columns, op.bytes))
				if (!hashes.has(key)) fresh.push(key)
				hashes.set(key, h)
				members.push([key, h.toString()])
				orphans?.delete(key)
			}
			if (fresh.length)
				for (const [, hash] of db.all(
					'SELECT pk, hash FROM _lsync_member WHERE unit = ? AND pk IN (SELECT value FROM json_each(?))',
					[u.id, JSON.stringify(fresh)]
				))
					delta -= BigInt(hash as string)
			db.exec(sql, [JSON.stringify(rows)])
			db.exec(
				`INSERT OR REPLACE INTO _lsync_member
				 SELECT ?, ?, json_extract(value, '$[0]'), json_extract(value, '$[1]') FROM json_each(?)`,
				[u.id, u.table, JSON.stringify(members)]
			)
		}
		for (const h of hashes.values()) delta += h
		return delta
	}

	/** Applies one frame in one transaction and records the cursor. */
	/** Applies one frame (all of its parts) in one transaction and records the cursor. */
	apply(db: Db, parts: Uint8Array | Uint8Array[], epoch: string): ApplyResult {
		this.ensureSchema(db)
		const decoded = (Array.isArray(parts) ? parts : [parts]).map(decodeFrame)
		const frame = { lsn: decoded.at(-1)!.lsn, fragments: decoded.flatMap((d) => d.fragments) }
		const tables = new Set<string>()
		const drift: number[] = []
		this.why = []
		let rows = 0
		db.exec('BEGIN')
		try {
			// Rows that lost a membership in this frame; deleted at the end if
			// no unit holds them any more.
			const orphans = new Map<string, Map<string, SqlValue[]>>()
			const orphan = (u: { table: string }, key: string, values: SqlValue[]) => {
				let m = orphans.get(u.table)
				if (!m) orphans.set(u.table, (m = new Map()))
				m.set(key, values)
			}
			// The first frame after a units message drops every unit held but
			// no longer wanted (or served), atomically with that frame. The
			// unit row stays: it is the definition's id, and a later WANT may
			// want the unit back under it.
			if (this.sweep) {
				const current = JSON.stringify([...this.current])
				for (const [id, tbl] of db.all(
					'SELECT DISTINCT unit, tbl FROM _lsync_member WHERE unit NOT IN (SELECT value FROM json_each(?))',
					[current]
				)) {
					for (const [pk] of db.all('SELECT pk FROM _lsync_member WHERE unit = ?', [id!]))
						orphan({ table: tbl as string }, pk as string, JSON.parse(pk as string) as SqlValue[])
					db.exec('DELETE FROM _lsync_member WHERE unit = ?', [id!])
					tables.add(tbl as string)
				}
				db.exec(
					'UPDATE _lsync_unit SET checksum = NULL WHERE id NOT IN (SELECT value FROM json_each(?))',
					[current]
				)
			}
			for (const f of frame.fragments) {
				const u = this.byRef.get(f.ref)
				if (!u) throw new Error(`frame references unknown unit ${f.ref}`)
				tables.add(u.table)
				this.fragNote = `[frame lsn ${frame.lsn} reset ${f.reset} ops ${f.ops.length}]`
				// NULL (a new or drifted unit) is followed by a RESET, or by
				// nothing that matters: the unit is resent whole after a drift.
				let sum = BigInt(
					String(db.all('SELECT checksum FROM _lsync_unit WHERE id = ?', [u.id])[0]?.[0] ?? '0')
				)
				if (f.reset) {
					for (const [pk] of db.all('SELECT pk FROM _lsync_member WHERE unit = ?', [u.id]))
						orphan(u, pk as string, JSON.parse(pk as string) as SqlValue[])
					db.exec('DELETE FROM _lsync_member WHERE unit = ?', [u.id])
					sum = 0n
				}
				if (f.ops.length >= BULK_MIN && f.ops.every((op) => op.kind === OP_PUT)) {
					// Snapshots and big move-ins: rows and memberships in
					// set-based statements per chunk.
					rows += f.ops.length
					const bulk = this.bulkPut(db, u, f.ops, orphans.get(u.table))
					this.finishUnit(db, u, BigInt.asUintN(64, sum + bulk), f.checksum, drift, !f.continues)
					continue
				}
				const pkCols = u.pk.map((i) => u.columns[i]!)
				const values: SqlValue[] = []
				const upsert = upsertSql(u)
				for (const op of f.ops) {
					rows++
					if (op.kind === OP_PUT) {
						readRow(new Reader(op.bytes), u.columns, values)
						const pkv = u.pk.map((i) => jsonable(values[i]!))
						const key = JSON.stringify(pkv)
						const h = memberHash(rowHash(u.columns, op.bytes))
						const prev = db.all('SELECT hash FROM _lsync_member WHERE unit = ? AND pk = ?', [
							u.id,
							key,
						])[0]
						if (prev) sum -= BigInt(prev[0] as string)
						sum += h
						db.exec(upsert, values)
						db.exec('INSERT OR REPLACE INTO _lsync_member VALUES (?, ?, ?, ?)', [
							u.id,
							u.table,
							key,
							h.toString(),
						])
						orphans.get(u.table)?.delete(key)
					} else if (op.kind === OP_PATCH) {
						// Changed columns only; the engine sends the new row hash
						// because the client does not keep row bytes to recompute it.
						const p = readPatch(u.columns, op.bytes)
						const pr = new Reader(p.pk)
						const pkv = pkCols.map((c) => readValue(pr, c))
						const key = JSON.stringify(pkv.map(jsonable))
						const prev = db.all('SELECT hash FROM _lsync_member WHERE unit = ? AND pk = ?', [
							u.id,
							key,
						])[0]
						if (!prev || !p.changed.length) {
							// A patch for a row this unit does not hold: state drifted.
							drift.push(u.id)
							this.why.push(
								`unit ${u.id} (${u.rule}): PATCH for ${key} which it does not hold (changed ${p.changed.length} cols)`
							)
							continue
						}
						const sets = p.changed.map((i) => `${q(u.columns[i]!.name)} = ?`).join(', ')
						const where = u.pk.map((i) => `${q(u.columns[i]!.name)} = ?`).join(' AND ')
						db.exec(`UPDATE ${q(u.table)} SET ${sets} WHERE ${where}`, [...p.values, ...pkv])
						const h = memberHash(p.hash)
						sum += h - BigInt(prev[0] as string)
						db.exec('UPDATE _lsync_member SET hash = ? WHERE unit = ? AND pk = ?', [
							h.toString(),
							u.id,
							key,
						])
					} else if (op.kind === OP_REMOVE) {
						const r = new Reader(op.bytes)
						const pkv = pkCols.map((c) => readValue(r, c))
						const key = JSON.stringify(pkv.map(jsonable))
						const prev = db.all('SELECT hash FROM _lsync_member WHERE unit = ? AND pk = ?', [
							u.id,
							key,
						])[0]
						if (prev) {
							sum -= BigInt(prev[0] as string)
							db.exec('DELETE FROM _lsync_member WHERE unit = ? AND pk = ?', [u.id, key])
						}
						orphan(u, key, pkv)
					}
				}
				this.finishUnit(db, u, BigInt.asUintN(64, sum), f.checksum, drift, !f.continues)
			}
			for (const [table, keys] of orphans) {
				const known = db.all('SELECT pk_cols FROM _lsync_table WHERE tbl = ?', [table])[0]
				if (!known) throw new Error(`no primary key known for table ${table}`)
				const pkNames = JSON.parse(String(known[0])) as string[]
				const del = `DELETE FROM ${q(table)} WHERE ${pkNames.map((n) => `${q(n)} = ?`).join(' AND ')}`
				for (const [key, pkv] of keys) {
					if (
						db.all('SELECT 1 FROM _lsync_member WHERE tbl = ? AND pk = ? LIMIT 1', [table, key])
							.length
					)
						continue
					db.exec(del, pkv)
				}
			}
			db.exec(`INSERT OR REPLACE INTO _lsync_state VALUES ('cursor', ?), ('epoch', ?)`, [
				String(frame.lsn),
				epoch,
			])
			db.exec('COMMIT')
			this.sweep = false
		} catch (err) {
			db.exec('ROLLBACK')
			throw err
		}
		return { tables, rows, drift, why: this.why }
	}
}

const BULK_MIN = 64
const BULK_CHUNK = 2000

const bulkUpserts = new WeakMap<UnitDesc, string>()

/** Upsert of a JSON array of rows; bigints travel as text, INTEGER affinity restores them. */
function bulkUpsertSql(u: UnitDesc): string {
	let s = bulkUpserts.get(u)
	if (s) return s
	const cols = u.columns.map((c: ColumnDesc) => q(c.name))
	const pk = u.pk.map((i) => q(u.columns[i]!.name))
	const set = u.columns
		.filter((_, i) => !u.pk.includes(i))
		.map((c) => `${q(c.name)} = excluded.${q(c.name)}`)
	const sel = u.columns.map((_, i) => `json_extract(value, '$[${i}]')`)
	// `WHERE true` tells SQLite the ON CONFLICT is the upsert's, not a join's.
	s = `INSERT INTO ${q(u.table)} (${cols.join(', ')}) SELECT ${sel.join(', ')} FROM json_each(?) WHERE true
		ON CONFLICT (${pk.join(', ')}) ${set.length ? `DO UPDATE SET ${set.join(', ')}` : 'DO NOTHING'}`
	bulkUpserts.set(u, s)
	return s
}

/** JSON-safe PK value (bigints as strings), so keys are stable text. */
function jsonable(v: SqlValue): string | number | null {
	return typeof v === 'bigint' ? v.toString() : v
}

const upserts = new WeakMap<UnitDesc, string>()

function upsertSql(u: UnitDesc): string {
	let s = upserts.get(u)
	if (s) return s
	const cols = u.columns.map((c: ColumnDesc) => q(c.name))
	const pk = u.pk.map((i) => q(u.columns[i]!.name))
	const set = u.columns
		.filter((_, i) => !u.pk.includes(i))
		.map((c) => `${q(c.name)} = excluded.${q(c.name)}`)
	s = `INSERT INTO ${q(u.table)} (${cols.join(', ')}) VALUES (${cols.map(() => '?').join(', ')})
		ON CONFLICT (${pk.join(', ')}) ${set.length ? `DO UPDATE SET ${set.join(', ')}` : 'DO NOTHING'}`
	upserts.set(u, s)
	return s
}
