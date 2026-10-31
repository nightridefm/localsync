// Scenario context: the vocabulary capability tests are written in.
// Everything asserted is observable client state compared with the oracle.
import { randomUUID } from 'node:crypto'

import type { Adapter, Client, Marker } from './adapter.ts'
import { type Catalog, introspect } from './catalog.ts'
import { RULES } from './fixture.ts'
import { diff, formatDiff, oracle } from './oracle.ts'
import type { Db, Query } from './pg.ts'
import { comparedColumns } from './oracle.ts'
import type { Rows } from './replica.ts'
import { type Bound, bind, type Principal, type Rule, type Want } from './rules.ts'

export class Fail extends Error {}

export type Result = { status: 'pass' | 'fail' | 'unsupported' | 'error'; note: string; ms: number }

export type Scenario = { id: string; title: string; run(ctx: Ctx): Promise<string | void> }

export class Conn {
	bounds: Bound[]
	/** Row signatures that were visible at some transaction boundary. */
	private allowed: Map<string, Set<string>> | null = null
	private seen: Map<string, Set<string>> | null = null

	readonly ctx: Ctx
	readonly principal: Principal
	readonly client: Client
	wants: Want[]
	readonly catalog: Catalog
	/** The client also wants things left out of the comparison (Ctx.connect). */
	unchecked = false

	constructor(ctx: Ctx, principal: Principal, client: Client, wants: Want[], catalog: Catalog) {
		this.ctx = ctx
		this.principal = principal
		this.client = client
		this.wants = wants
		this.catalog = catalog
		this.bounds = ctx.bindAll(principal, wants)
	}

	async want(wants: Want[]): Promise<void> {
		this.wants = wants
		this.bounds = this.ctx.bindAll(this.principal, wants)
		await this.client.want(this.bounds)
		await this.recordAllowed()
	}

	/** The replica, restricted to the wanted tables and compared columns. */
	replicaRows(): Rows {
		const out: Rows = new Map()
		for (const b of this.bounds)
			out.set(b.table, this.client.replica.read(b.table, comparedColumns(this.catalog, b)))
		return out
	}

	oracle(): Promise<Rows> {
		return oracle(this.ctx.db, this.catalog, this.bounds)
	}

	/** Start the "no row outside visibility, ever" check (see verifyNoLeaks). */
	async trackVisibility(): Promise<void> {
		this.allowed = new Map()
		this.seen = new Map()
		await this.recordAllowed()
		this.client.onCommit(() => {
			for (const [t, rows] of this.replicaRows()) {
				let s = this.seen!.get(t)
				if (!s) this.seen!.set(t, (s = new Set()))
				for (const [pk, r] of rows) s.add(`${pk}:${JSON.stringify(r)}`)
			}
		})
	}

	async recordAllowed(): Promise<void> {
		if (!this.allowed) return
		for (const [t, rows] of await this.oracle()) {
			let s = this.allowed.get(t)
			if (!s) this.allowed.set(t, (s = new Set()))
			for (const [pk, r] of rows) s.add(`${pk}:${JSON.stringify(r)}`)
		}
	}

	/** Every row the client ever held was visible at some transaction boundary. */
	verifyNoLeaks(): void {
		if (!this.allowed || !this.seen) throw new Error('trackVisibility() not called')
		const leaks: string[] = []
		for (const [t, s] of this.seen) {
			const ok = this.allowed.get(t) ?? new Set()
			for (const sig of s) if (!ok.has(sig)) leaks.push(`${t} ${sig.slice(0, 200)}`)
		}
		if (leaks.length)
			throw new Fail(
				`${leaks.length} row states never visible to the principal:\n  ${leaks.slice(0, 5).join('\n  ')}`
			)
	}
}

export class Ctx {
	private conns: Conn[] = []
	private rules = new Map<string, Rule>(Object.entries(RULES))

	readonly db: Db
	readonly adapter: Adapter
	catalog: Catalog

	constructor(db: Db, adapter: Adapter, catalog: Catalog) {
		this.db = db
		this.adapter = adapter
		this.catalog = catalog
	}

	principal(name = 'p'): Principal {
		return { name, sub: randomUUID() }
	}

	addRule(rule: Rule): void {
		this.rules.set(rule.name, rule)
		this.adapter.registerRule?.(rule)
	}

	rule(name: string): Rule {
		const r = this.rules.get(name)
		if (!r) throw new Error(`no rule ${name}`)
		return r
	}

	bindAll(p: Principal, wants: Want[]): Bound[] {
		return wants.map((w) => bind(this.rule(w.rule), p, w))
	}

	async refreshCatalog(): Promise<void> {
		this.catalog = await introspect(this.db)
	}

	/**
	 * unchecked: also wanted by the client, but left out of the oracle
	 * comparison (wants expected to fail).
	 */
	async connect(p: Principal, wants: Array<Want | string>, unchecked: Want[] = []): Promise<Conn> {
		const ws = wants.map((w) => (typeof w === 'string' ? { rule: w } : w))
		const bounds = this.bindAll(p, [...ws, ...unchecked])
		const client = await this.adapter.connect(p, bounds, this.catalog)
		const conn = new Conn(this, p, client, ws, this.catalog)
		conn.unchecked = unchecked.length > 0
		this.conns.push(conn)
		return conn
	}

	/** One write transaction; tracked connections record the new visible state. */
	async write(
		fn: string | ((q: Query) => Promise<unknown>),
		params: unknown[] = []
	): Promise<Marker> {
		const m = await this.db.tx(async (q) => {
			if (typeof fn === 'string') await q(fn, params)
			else await fn(q)
			const [r] = await q('SELECT pg_current_xact_id()::text AS x')
			return r!.x!
		})
		const lsn = await this.db.lsn()
		for (const c of this.conns) await c.recordAllowed()
		return { xid: m, lsn }
	}

	/** Waits until the replica equals the oracle. Returns the time it took. */
	async converge(conn: Conn, timeoutMs = 30_000): Promise<number> {
		const start = Date.now()
		let last = ''
		let interval = 20
		for (;;) {
			// Counts first: diffing 100k rows on every poll would starve the
			// client being measured (it shares this event loop).
			const counts = (await this.countsMatch(conn)) || Date.now() - start > timeoutMs
			// The engine refused a want (unknown rule, bad parameter): waiting
			// cannot help, so fail now with the reason. A refusal for want of
			// a claim is an access outcome the oracle checks (empty).
			const refused = conn.client
				.errors()
				.find((e) => /^(unit \d+|want \S+): /.test(e) && !e.includes(': not allowed:'))
			// Unless the scenario wants something it expects to fail.
			if (refused && !conn.unchecked) throw new Fail(`engine refused a want: ${refused}`)
			const d = counts
				? diff(await conn.oracle(), conn.replicaRows())
				: [{ table: '', pk: '', kind: 'missing' as const }]
			if (d.length === 0) return Date.now() - start
			if (Date.now() - start > timeoutMs) {
				last = formatDiff(d)
				const errs = conn.client.errors()
				throw new Fail(
					`replica != oracle after ${timeoutMs}ms (${d.length} rows):\n${last}${errs.length ? `\n  client errors: ${errs.slice(-3).join(' | ')}` : ''}`
				)
			}
			await new Promise((r) => setTimeout(r, interval))
			interval = Math.min(interval * 2, 100)
		}
	}

	private async countsMatch(conn: Conn): Promise<boolean> {
		const tables = new Map<string, number>()
		for (const b of conn.bounds) {
			const [r] = await this.db.query(`SELECT count(*) AS n FROM (${b.sql}) q`, b.values)
			tables.set(b.table, (tables.get(b.table) ?? 0) + Number(r!.n))
		}
		for (const [t, n] of tables) {
			const local = Number(conn.client.replica.db.selectValue(`SELECT count(*) FROM "${t}"`))
			// Overlapping rules can make the oracle sum exceed the union; only
			// a short replica is a definite mismatch.
			if (local < n && conn.bounds.filter((b) => b.table === t).length === 1) return false
		}
		return true
	}

	async closeAll(): Promise<void> {
		for (const c of this.conns) await c.client.close().catch(() => {})
		this.conns = []
	}
}
