// Thin Postgres access for the harness: every value comes back as Postgres
// text (no driver-side parsing), so the oracle compares exactly what the
// database says, canonicalised in one place (canonical.ts).
import pg from 'pg'

export const PG_URL =
	process.env.LOCALSYNC_PG_URL ??
	`postgresql://postgres:postgres@localhost:${process.env.LOCALSYNC_PG_PORT ?? '5532'}/localsync`

export type Row = Record<string, string | null>

const rawTypes = { getTypeParser: () => (v: string) => v }

export type Query = (sql: string, params?: unknown[]) => Promise<Row[]>

export class Db {
	readonly pool: pg.Pool

	constructor(url = PG_URL) {
		this.pool = new pg.Pool({ connectionString: url, max: 8, options: '-c TimeZone=UTC' })
	}

	async query(sql: string, params: unknown[] = []): Promise<Row[]> {
		const r = await this.pool.query({ text: sql, values: params, types: rawTypes })
		return r.rows as Row[]
	}

	/** Runs fn in one transaction. */
	async tx<T>(fn: (q: Query) => Promise<T>): Promise<T> {
		const c = await this.pool.connect()
		try {
			await c.query('BEGIN')
			const q: Query = async (sql, params = []) =>
				(await c.query({ text: sql, values: params, types: rawTypes })).rows as Row[]
			const out = await fn(q)
			await c.query('COMMIT')
			return out
		} catch (err) {
			await c.query('ROLLBACK').catch(() => {})
			throw err
		} finally {
			c.release()
		}
	}

	async lsn(): Promise<bigint> {
		const [r] = await this.query('SELECT pg_current_wal_lsn()::text AS l')
		return parseLsn(r!.l!)
	}

	close(): Promise<void> {
		return this.pool.end()
	}
}

/** 'X/Y' -> 64-bit LSN. */
export function parseLsn(s: string): bigint {
	const i = s.indexOf('/')
	return (BigInt(`0x${s.slice(0, i)}`) << 32n) | BigInt(`0x${s.slice(i + 1)}`)
}
