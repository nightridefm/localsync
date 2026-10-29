// Electric adapter. The client is localdb's own sync code
// (`sync/electric.ts` shapeStream + `sync/apply.ts` makeApplier) on
// in-memory sqlite-wasm, run the way localdb's writer worker runs it: one
// stream per shape, one local table per shape, each batch applied in its
// own transaction with the cursor kept alongside. So the baseline measures
// the client the apps actually ship. Not reproduced: the writer's
// coalescing window and the opt-in `alignShapes`.
import { execFileSync } from 'node:child_process'
import path from 'node:path'

import { makeApplier, type TableApplier } from '../../../../localdb/src/sync/apply.ts'
import { type ShapeCursor, shapeStream } from '../../../../localdb/src/sync/electric.ts'
import { type Adapter, type Client, Unsupported } from './adapter.ts'
import type { Catalog, Table } from './catalog.ts'
import { Replica } from './replica.ts'
import type { Bound, Principal } from './rules.ts'

const ELECTRIC_URL =
	process.env.LOCALSYNC_ELECTRIC_URL ??
	`http://localhost:${process.env.LOCALSYNC_ELECTRIC_PORT ?? '3130'}`
const SECRET = process.env.ELECTRIC_SECRET ?? 'localsync-dev-secret'
const REPO = path.resolve(import.meta.dirname, '../../..')
const DEBUG = !!process.env.HARNESS_DEBUG

function meta(t: Table) {
	const of = (k: string) => t.columns.filter((c) => c.kind === k).map((c) => c.name)
	return {
		dateColumns: of('ts'),
		boolColumns: of('bool'),
		jsonColumns: of('json'),
		listColumns: of('array'),
	}
}

type Run = {
	bound: Bound
	applier: TableApplier
	cursor: ShapeCursor | null
	lsn: bigint
	abort: AbortController | null
	done: Promise<void> | null
}

class ElectricClient implements Client {
	private runs = new Map<string, Run>()
	private listeners = new Set<() => void>()
	private received = 0
	private errs: string[] = []

	readonly replica: Replica
	private catalog: Catalog

	constructor(replica: Replica, catalog: Catalog) {
		this.replica = replica
		this.catalog = catalog
	}

	private fetchImpl: typeof fetch = async (input, init) => {
		const res = await fetch(input, init)
		const buf = new Uint8Array(await res.arrayBuffer())
		this.received += buf.byteLength
		return new Response(res.status === 204 || res.status === 304 ? null : buf, {
			status: res.status,
			statusText: res.statusText,
			headers: res.headers,
		})
	}

	onCommit(cb: () => void): () => void {
		this.listeners.add(cb)
		return () => this.listeners.delete(cb)
	}

	private commit(): void {
		for (const l of this.listeners) l()
	}

	async want(bounds: Bound[]): Promise<void> {
		const tables = new Set<string>()
		for (const b of bounds) {
			if (tables.has(b.table))
				throw new Unsupported(
					`two rules on table ${b.table}: localdb holds one Electric shape per table`
				)
			tables.add(b.table)
		}
		const keep = new Set(bounds.map((b) => b.key))
		for (const [key, run] of this.runs) {
			if (keep.has(key)) continue
			await this.stop(run)
			this.runs.delete(key)
			const db = this.replica.db
			db.exec('BEGIN')
			run.applier.truncate()
			db.exec('COMMIT')
			this.commit()
		}
		for (const b of bounds) {
			if (this.runs.has(b.key)) continue
			const t = this.catalog.get(b.table)
			if (!t) throw new Error(`no table ${b.table}`)
			const run: Run = {
				bound: b,
				applier: makeApplier(
					{ db: this.replica.db } as unknown as Parameters<typeof makeApplier>[0],
					b.table,
					t.pk,
					meta(t)
				),
				cursor: null,
				lsn: 0n,
				abort: null,
				done: null,
			}
			this.runs.set(b.key, run)
			this.start(run)
		}
	}

	private start(run: Run): void {
		const abort = new AbortController()
		run.abort = abort
		const params: Record<string, string> = { table: run.bound.table, secret: SECRET }
		if (run.bound.where) params.where = run.bound.where
		if (run.bound.columns) params.columns = run.bound.columns.map((c) => `"${c}"`).join(',')
		run.done = (async () => {
			const db = this.replica.db
			for await (const batch of shapeStream({
				url: `${ELECTRIC_URL}/v1/shape`,
				params,
				cursor: run.cursor,
				signal: abort.signal,
				fetchImpl: this.fetchImpl,
				onRetry: (err, _delay, status) => {
					this.errs.push(`${run.bound.key}: retry ${status ?? ''} ${String(err)}`)
				},
			})) {
				if (abort.signal.aborted) return
				const t0 = DEBUG ? performance.now() : 0
				db.exec('BEGIN')
				try {
					if (batch.mustRefetch) {
						run.applier.truncate()
						run.cursor = null
					} else {
						run.applier.apply(batch.messages)
						run.cursor = batch.cursor
					}
					db.exec('COMMIT')
				} catch (err) {
					db.exec('ROLLBACK')
					this.errs.push(`${run.bound.key}: apply failed: ${String(err)}`)
					return
				}
				if (batch.upToDateLsn) {
					const l = BigInt(batch.upToDateLsn)
					if (l > run.lsn) run.lsn = l
				}
				this.commit()
				if (DEBUG && batch.messages.length > 100)
					console.error(
						`[electric] ${run.bound.key} applied ${batch.messages.length} msgs in ${Math.round(performance.now() - t0)}ms`
					)
			}
		})().catch((err: unknown) => {
			this.errs.push(`${run.bound.key}: stream died: ${String(err)}`)
		})
	}

	private async stop(run: Run): Promise<void> {
		run.abort?.abort()
		await run.done
		run.abort = null
		run.done = null
	}

	async disconnect(): Promise<void> {
		for (const run of this.runs.values()) await this.stop(run)
	}

	async reconnect(): Promise<void> {
		for (const run of this.runs.values()) if (!run.abort) this.start(run)
	}

	bytes(): number {
		return this.received
	}

	errors(): string[] {
		return this.errs
	}

	async close(): Promise<void> {
		await this.disconnect()
		this.replica.close()
	}
}

async function waitHealthy(timeoutMs = 60_000): Promise<void> {
	const end = Date.now() + timeoutMs
	while (Date.now() < end) {
		try {
			const r = await fetch(`${ELECTRIC_URL}/v1/health`)
			if (r.ok) {
				const body = (await r.json()) as { status?: string }
				if (body.status === 'active') return
			}
		} catch {
			// not up yet
		}
		await new Promise((r) => setTimeout(r, 250))
	}
	throw new Error('electric did not become healthy')
}

function compose(...args: string[]): void {
	execFileSync('docker', ['compose', ...args], { cwd: REPO, stdio: 'ignore' })
}

export class ElectricAdapter implements Adapter {
	readonly replicationSlots = 'electric\\_slot\\_%'
	readonly name = 'electric 1.7.9'
	private clients = new Set<ElectricClient>()

	async connect(_principal: Principal, bounds: Bound[], catalog: Catalog): Promise<Client> {
		const c = new ElectricClient(await Replica.open(catalog), catalog)
		await c.want(bounds)
		this.clients.add(c)
		return c
	}

	async restartServer(): Promise<void> {
		compose('restart', 'electric')
		await waitHealthy()
	}

	async expireRetention(): Promise<void> {
		// Electric keeps shape logs in the container: a fresh container has
		// none, so every client handle is unknown to it.
		compose('up', '-d', '--force-recreate', 'electric')
		await waitHealthy()
	}

	async schemaReset(): Promise<void> {
		// Recreated tables would be served stale shape handles (seen in
		// Phase 0), so start Electric from empty storage too.
		await this.expireRetention()
	}

	async close(): Promise<void> {
		for (const c of this.clients) await c.close()
		this.clients.clear()
	}
}
