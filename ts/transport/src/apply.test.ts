// Applier against hand-built frames on node:sqlite, for orderings the
// engine produces only under load.
import assert from 'node:assert/strict'
import { DatabaseSync } from 'node:sqlite'
import { test } from 'node:test'

import { Applier, type Db } from './apply.ts'
import {
	type ColumnDesc,
	memberHash,
	OP_PUT,
	rowHash,
	type SqlValue,
	type UnitDesc,
} from './codec.ts'

function sqlite(): Db {
	const db = new DatabaseSync(':memory:')
	return {
		exec(sql, bind) {
			if (bind) db.prepare(sql).run(...(bind as Array<string | number | bigint | null>))
			else db.exec(sql)
		},
		all(sql, bind) {
			return db
				.prepare(sql)
				.all(...((bind ?? []) as Array<string | number | bigint | null>))
				.map((r) => Object.values(r) as SqlValue[])
		},
	}
}

const COLS: ColumnDesc[] = [{ name: 'id', kind: 'text' }]
const unit = (id: number, ref: number): UnitDesc => ({
	id,
	ref,
	rule: `r${id}`,
	table: 'task',
	columns: COLS,
	pk: [0],
})

const uvarint = (out: number[], v: number) => {
	for (; v >= 0x80; v = Math.floor(v / 128)) out.push((v % 128) | 0x80)
	out.push(v)
}
const row = (id: string) => {
	const b = [0]
	const s = new TextEncoder().encode(id)
	uvarint(b, s.length)
	return Uint8Array.from([...b, ...s])
}
const sum = (ids: string[]) =>
	ids.reduce((a, id) => (a + memberHash(rowHash(COLS, row(id)))) & ((1n << 64n) - 1n), 0n)

type Frag = { ref: number; reset?: boolean; puts?: string[]; checksum: bigint }
let lsn = 100
function frame(frags: Frag[]): Uint8Array {
	const b: number[] = [1]
	uvarint(b, lsn++)
	uvarint(b, frags.length)
	for (const f of frags) {
		uvarint(b, f.ref)
		b.push(f.reset ? 1 : 0)
		const c = new DataView(new ArrayBuffer(8))
		c.setBigUint64(0, f.checksum, true)
		b.push(...new Uint8Array(c.buffer))
		uvarint(b, f.puts?.length ?? 0)
		for (const id of f.puts ?? []) {
			const r = row(id)
			b.push(OP_PUT)
			uvarint(b, r.length)
			b.push(...r)
		}
	}
	return Uint8Array.from(b)
}

void test('a unit removed while a WANT for it again is in flight keeps its checksum', () => {
	const db = sqlite()
	db.exec('CREATE TABLE task (id TEXT PRIMARY KEY)')
	const ap = new Applier()
	ap.ensureSchema(db)
	// Ids as SyncClient assigns them, one per definition.
	db.exec(`INSERT INTO _lsync_unit (id, def) VALUES (1, 'project A'), (2, 'project B')`)
	const A = unit(1, 10)
	const B = unit(2, 20)
	ap.setUnits(db, [A])
	assert.deepEqual(
		ap.apply(
			db,
			frame([{ ref: 10, reset: true, puts: ['a1', 'a2'], checksum: sum(['a1', 'a2']) }]),
			'e'
		).drift,
		[]
	)
	// Swap to B, and straight back to A: the second WANT (A, under the
	// same id) is sent before the first swap's frame arrives. That frame
	// removes A; the next one sends A whole again.
	ap.setUnits(db, [B])
	ap.apply(
		db,
		frame([
			{ ref: 10, reset: true, checksum: 0n },
			{ ref: 20, reset: true, puts: ['b1'], checksum: sum(['b1']) },
		]),
		'e'
	)
	ap.setUnits(db, [A])
	const back = ap.apply(
		db,
		frame([
			{ ref: 20, reset: true, checksum: 0n },
			{ ref: 10, reset: true, puts: ['a1', 'a2'], checksum: sum(['a1', 'a2']) },
		]),
		'e'
	)
	assert.deepEqual(back.drift, [])
	// A later delta must build on A's stored checksum.
	const next = ap.apply(
		db,
		frame([{ ref: 10, puts: ['a3'], checksum: sum(['a1', 'a2', 'a3']) }]),
		'e'
	)
	assert.deepEqual(next.drift, [], next.why.join('; '))
	assert.deepEqual(db.all('SELECT id FROM task ORDER BY id'), [['a1'], ['a2'], ['a3']])
	assert.deepEqual(db.all('SELECT id, held FROM _lsync_unit ORDER BY id'), [
		[1, 1],
		[2, 0],
	])
})
