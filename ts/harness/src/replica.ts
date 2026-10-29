// The reference client's local database: in-memory sqlite-wasm, the same
// engine localdb runs in the browser, with app-owned DDL generated from the
// Postgres catalog the way localdb's codegen would.
import sqlite3InitModule from '@sqlite.org/sqlite-wasm'

import type { Catalog, Kind, Table } from './catalog.ts'
import { type Canon, fromReplica } from './canonical.ts'

type Init = (opts?: {
	print?: (s: string) => void
	printErr?: (s: string) => void
}) => ReturnType<typeof sqlite3InitModule>

type Sqlite = Awaited<ReturnType<typeof sqlite3InitModule>>
export type SqliteDb = InstanceType<Sqlite['oo1']['DB']>

let sqlite: Promise<Sqlite> | null = null
function loadSqlite(): Promise<Sqlite> {
	sqlite ??= (sqlite3InitModule as unknown as Init)({ print: () => {}, printErr: () => {} })
	return sqlite
}

const q = (n: string) => `"${n.replaceAll('"', '""')}"`

function affinity(kind: Kind): string {
	if (kind === 'int' || kind === 'bool') return 'INTEGER'
	if (kind === 'float') return 'REAL'
	return 'TEXT'
}

/** Rows of one table keyed by canonical PK, values by column name. */
export type TableRows = Map<string, Record<string, Canon>>
export type Rows = Map<string, TableRows>

export function pkKey(values: Canon[]): string {
	return JSON.stringify(values)
}

export class Replica {
	readonly db: SqliteDb
	readonly catalog: Catalog

	private constructor(db: SqliteDb, catalog: Catalog) {
		this.db = db
		this.catalog = catalog
	}

	static async open(catalog: Catalog): Promise<Replica> {
		const s = await loadSqlite()
		const db = new s.oo1.DB(':memory:')
		for (const t of catalog.values()) {
			const cols = t.columns.map((c) => `${q(c.name)} ${affinity(c.kind)}`)
			db.exec(
				`CREATE TABLE ${q(t.name)} (${cols.join(', ')}, PRIMARY KEY (${t.pk.map(q).join(', ')}))`
			)
		}
		return new Replica(db, catalog)
	}

	table(name: string): Table {
		const t = this.catalog.get(name)
		if (!t) throw new Error(`replica has no table ${name}`)
		return t
	}

	/** Reads a table canonically. Ints come back as text (int8 exceeds 2^53). */
	read(name: string, only?: string[]): TableRows {
		const t = this.table(name)
		const cols = only ? t.columns.filter((c) => only.includes(c.name)) : t.columns
		const sel = cols.map((c) => (c.kind === 'int' ? `CAST(${q(c.name)} AS TEXT)` : q(c.name)))
		const out: TableRows = new Map()
		const rows = this.db.selectArrays(`SELECT ${sel.join(', ')} FROM ${q(name)}`) as unknown[][]
		const pkIdx = t.pk.map((p) => cols.findIndex((c) => c.name === p))
		for (const r of rows) {
			const rec: Record<string, Canon> = {}
			for (let i = 0; i < cols.length; i++) rec[cols[i]!.name] = fromReplica(cols[i]!, r[i])
			out.set(pkKey(pkIdx.map((i) => (i < 0 ? null : rec[cols[i]!.name]!))), rec)
		}
		return out
	}

	close(): void {
		this.db.close()
	}
}
