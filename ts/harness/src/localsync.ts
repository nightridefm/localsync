// localsync adapter: runs the Go engine (bin/engine) against the compose
// Postgres with the harness rules, and the real @nightride/localsync client
// on the same in-memory sqlite-wasm replica the Electric adapter uses.
import { type ChildProcess, execFileSync, spawn } from 'node:child_process'
import { createHmac, randomUUID } from 'node:crypto'
import { mkdtempSync, writeFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import path from 'node:path'

import { type ConnMode, type Db as SyncDb, type SqlValue, SyncClient } from '@nightride/localsync'

import type { Adapter, Client } from './adapter.ts'
import type { Catalog } from './catalog.ts'
import { PG_URL } from './pg.ts'
import { Replica, type SqliteDb } from './replica.ts'
import { RULES } from './fixture.ts'
import type { Bound, Principal, Rule } from './rules.ts'

const REPO = path.resolve(import.meta.dirname, '../../..')
const SECRET = 'harness-secret'
export const PORT = Number(process.env.LOCALSYNC_ENGINE_PORT ?? 3141)
const DEBUG = !!process.env.HARNESS_DEBUG
// LOCALSYNC_DRIFT_DUMP=1: dump client and engine members on drift, without
// the debug logging (which changes timing enough to hide some races).
const DRIFT_DUMP = DEBUG || !!process.env.LOCALSYNC_DRIFT_DUMP
// Clients may reach the engine through a gateway (docker/gateway):
// LOCALSYNC_URL is the ws(s):// /sync URL (default: the engine directly),
// LOCALSYNC_HTTP_URL the /sync/http one, LOCALSYNC_TRANSPORT websocket
// (default here, so runs are deterministic), http or auto.
const URL_ = process.env.LOCALSYNC_URL ?? `ws://127.0.0.1:${PORT}/sync`
const HTTP_URL = process.env.LOCALSYNC_HTTP_URL
const TRANSPORT = (process.env.LOCALSYNC_TRANSPORT ?? 'websocket') as 'websocket' | 'http' | 'auto'

// Engines started by this process: killed when it exits, so an interrupted
// run never leaves one holding the port (which would then answer the next
// run's clients with stale rules).
const live = new Set<ChildProcess>()
const killAll = () => {
	for (const p of live) p.kill('SIGKILL')
}
process.once('exit', killAll)
for (const sig of ['SIGINT', 'SIGTERM'] as const)
	process.once(sig, () => {
		killAll()
		process.exit(130)
	})

const b64url = (b: Buffer | string) => Buffer.from(b).toString('base64url')

export function mintToken(
	sub: string,
	opts: { secret?: string; expSeconds?: number; aud?: string; grants?: string[] } = {}
): string {
	const header = b64url(JSON.stringify({ alg: 'HS256', typ: 'JWT' }))
	const exp = Math.floor(Date.now() / 1000) + (opts.expSeconds ?? 600)
	const payload = b64url(
		JSON.stringify({ sub, exp, aud: opts.aud ?? 'localsync', grants: opts.grants })
	)
	const sig = createHmac('sha256', opts.secret ?? SECRET)
		.update(`${header}.${payload}`)
		.digest('base64url')
	return `${header}.${payload}.${sig}`
}

/** sqlite-wasm oo1 behind the transport's minimal Db interface. */
function wrap(db: SqliteDb): SyncDb {
	return {
		exec(sql, bind) {
			db.exec({ sql, bind: bind as never })
		},
		all(sql, bind) {
			return db.selectArrays(sql, bind as never) as SqlValue[][]
		},
	}
}

/** rules.yaml for the engine (also used by the browser tests). */
export function yamlRules(rules: Iterable<Rule>): string {
	const lines = ['rules:']
	for (const r of rules) {
		lines.push(`  ${JSON.stringify(r.name)}:`)
		lines.push(`    sql: ${JSON.stringify(r.sql)}`)
		if (r.params?.length) lines.push(`    params: ${JSON.stringify(r.params)}`)
		if (r.public) lines.push('    public: true')
		if (r.claims?.length) lines.push(`    claims: ${JSON.stringify(r.claims)}`)
	}
	return `${lines.join('\n')}\n`
}

/** How clients reach the engine (defaults: the environment, see above). */
export type ConnOptions = {
	/** WebSocket class (benchmarks wrap it to see raw messages). */
	WebSocket?: typeof WebSocket
	url?: string
	httpUrl?: string
	transport?: 'websocket' | 'http' | 'auto'
}

export class LocalsyncClient implements Client {
	readonly replica: Replica
	private sync: SyncClient
	private listeners = new Set<() => void>()
	private received = 0
	private bounds: Bound[]
	private live = false

	constructor(replica: Replica, principal: Principal, bounds: Bound[], conn: ConnOptions) {
		const sub = principal.sub
		this.replica = replica
		this.bounds = bounds
		this.sync = new SyncClient({
			WebSocket: conn.WebSocket,
			url: conn.url ?? URL_,
			httpUrl: conn.httpUrl ?? HTTP_URL,
			transport: conn.transport ?? TRANSPORT,
			// Read at every mint: a scenario may change the grants, which the
			// engine checks at the next WANT (a token refresh).
			// principal.sub too: a scenario may sign someone else in on this
			// replica (catalogue 7b).
			token: () =>
				mintToken(principal.sub, { grants: principal.claims?.grants as string[] | undefined }),
			db: wrap(replica.db),
			onCommit: () => {
				for (const l of this.listeners) l()
			},
			onBytes: (n) => {
				this.received += n
			},
			onState: (s) => {
				this.live = s === 'live'
			},
			onError: (m) => {
				if (DEBUG) console.error(`[localsync] ${m}`)
				if (DRIFT_DUMP && m.includes('drift')) void this.dumpDrift(sub)
			},
		})
		this.sync.start(this.wantsOf(bounds))
	}

	/** Debug: the client's and the engine's member hashes, for drift diagnosis. */
	private async dumpDrift(sub: string): Promise<void> {
		const db = this.replica.db
		const units = db.selectArrays('SELECT id, def, checksum FROM _lsync_unit') as unknown[][]
		const members = db.selectArrays('SELECT unit, pk, hash FROM _lsync_member') as unknown[][]
		const rows: Record<string, unknown[]> = {}
		for (const t of ['project', 'task', 'comment']) {
			try {
				rows[t] = db.selectObjects(`SELECT * FROM "${t}"`) as unknown[]
			} catch {
				// table not synced here
			}
		}
		const engine = await (await fetch(`http://127.0.0.1:${PORT}/debug/members?key=`)).json()
		const file = path.join(REPO, 'sim/runs', `drift-${sub}-${Date.now()}.json`)
		writeFileSync(file, JSON.stringify({ sub, units, members, engine, rows }, null, 1))
		console.error(`[localsync] drift dump: ${file}`)
	}

	/** The transport the client is using now. */
	get connMode(): ConnMode {
		return this.sync.connMode
	}

	private wantsOf(bounds: Bound[]) {
		return bounds.map((b) => ({ rule: b.rule.name, params: b.params }))
	}

	onCommit(cb: () => void): () => void {
		this.listeners.add(cb)
		return () => this.listeners.delete(cb)
	}

	async want(bounds: Bound[]): Promise<void> {
		this.bounds = bounds
		await this.sync.want(this.wantsOf(bounds))
	}

	async disconnect(): Promise<void> {
		this.sync.stop()
	}

	async reconnect(): Promise<void> {
		this.sync.start(this.wantsOf(this.bounds))
	}

	bytes(): number {
		return this.received
	}

	errors(): string[] {
		return this.sync.errors
	}

	caughtUp(): boolean {
		return this.live
	}

	failedWants(): Array<{ rule: string; error: string; permanent: boolean }> {
		return this.sync.wantErrors
	}

	async close(): Promise<void> {
		this.sync.stop()
		this.replica.close()
	}
}

export class LocalsyncAdapter implements Adapter {
	readonly name = 'localsync'
	readonly replicationSlots = 'localsync\\_%'
	private proc: ChildProcess | null = null
	/** Rules the running engine was started with. */
	private rules = new Map<string, Rule>()
	/** Rules registered for later use (Ctx.addRule), loaded at the next start. */
	private registered = new Map<string, Rule>()
	private clients = new Set<LocalsyncClient>()
	private dir = mkdtempSync(path.join(tmpdir(), 'localsync-harness-'))
	private conn: ConnOptions

	constructor(conn: ConnOptions = {}) {
		this.conn = conn
	}

	private async start(): Promise<void> {
		// A health check alone is not proof: an engine left over from a killed
		// run answers it too, and the new one then fails to bind and exits.
		// Refuse a busy port, and wait for THIS child's own "listening" line.
		if (
			await fetch(`http://127.0.0.1:${PORT}/health`).then(
				(r) => r.ok,
				() => false
			)
		)
			throw new Error(
				`port ${PORT} already has an engine (stale process?); kill it or set LOCALSYNC_ENGINE_PORT`
			)
		const rulesPath = path.join(this.dir, 'rules.yaml')
		writeFileSync(rulesPath, yamlRules(this.rules.values()))
		const proc = spawn(
			path.join(REPO, 'bin/engine'),
			['serve', '--pg', PG_URL, '--rules', rulesPath, '--listen', `127.0.0.1:${PORT}`],
			{ env: { ...process.env, LOCALSYNC_JWT_SECRET: SECRET }, stdio: ['ignore', 'ignore', 'pipe'] }
		)
		live.add(proc)
		proc.once('exit', () => live.delete(proc))
		this.proc = proc
		let stderr = ''
		await new Promise<void>((resolve, reject) => {
			const timer = setTimeout(
				() => reject(new Error(`engine did not start in 30s: ${stderr}`)),
				30_000
			)
			proc.stderr!.on('data', (d: Buffer) => {
				const s = d.toString()
				if (DEBUG) process.stderr.write(s)
				stderr = (stderr + s).slice(-4000)
				if (stderr.includes('msg=listening') || stderr.includes(' INFO listening ')) {
					clearTimeout(timer)
					resolve()
				}
			})
			proc.once('exit', (code) => {
				clearTimeout(timer)
				reject(new Error(`engine exited (${code}) before listening: ${stderr}`))
			})
		})
	}

	private async stop(): Promise<void> {
		const p = this.proc
		if (!p || p.exitCode !== null) return
		const exited = new Promise((r) => p.once('exit', r))
		p.kill('SIGKILL')
		await exited
		this.proc = null
	}

	registerRule(rule: Rule): void {
		this.registered.set(rule.name, rule)
	}

	async connect(principal: Principal, bounds: Bound[], catalog: Catalog): Promise<Client> {
		const missing =
			bounds.some((b) => this.rules.get(b.rule.name)?.sql !== b.rule.sql) ||
			[...this.registered.values()].some((r) => this.rules.get(r.name)?.sql !== r.sql)
		// An engine that exited (failed start, crash) is restarted too.
		if (!this.proc || this.proc.exitCode !== null || missing) {
			// The fixture rules plus whatever this connection adds; ad-hoc
			// rules from earlier scenarios may reference dropped tables.
			this.rules = new Map(Object.entries(RULES))
			for (const r of this.registered.values()) this.rules.set(r.name, r)
			for (const b of bounds) this.rules.set(b.rule.name, b.rule)
			// Registered rules belong to the scenario that added them; a later
			// scenario's restart must not load them (their tables may be gone).
			this.registered.clear()
			await this.stop()
			await this.start()
		}
		const c = new LocalsyncClient(await Replica.open(catalog), principal, bounds, this.conn)
		this.clients.add(c)
		return c
	}

	/** Engine process id, for resource measurements. */
	enginePid(): number | undefined {
		return this.proc?.pid
	}

	async restartServer(): Promise<void> {
		await this.stop()
		await this.start()
	}

	async expireRetention(): Promise<void> {
		// The resume window is in memory: a restart is a new epoch, so every
		// cursor is too old and clients reconcile by checksum.
		await this.restartServer()
	}

	async tokenCheck(): Promise<string> {
		const cases: Array<[string, string]> = [
			['forged', mintToken(randomUUID(), { secret: 'not-the-secret' })],
			['expired', mintToken(randomUUID(), { expSeconds: -60 })],
			['wrong audience', mintToken(randomUUID(), { aud: 'someone-else' })],
		]
		for (const [what, token] of cases) {
			const got = await new Promise<string>((resolve) => {
				const ws = new WebSocket(`ws://127.0.0.1:${PORT}/sync`)
				const timer = setTimeout(() => resolve('no answer'), 5000)
				ws.onopen = () =>
					ws.send(
						JSON.stringify({
							type: 'want',
							token,
							wants: [{ id: 1, rule: 'tags', params: {} }],
							have: {},
						})
					)
				ws.onmessage = (ev) => {
					if (typeof ev.data === 'string' && ev.data.includes('"error"')) {
						clearTimeout(timer)
						resolve('rejected')
					} else {
						clearTimeout(timer)
						resolve('accepted')
					}
					ws.close()
				}
				ws.onerror = () => {}
			})
			if (got !== 'rejected') throw new Error(`${what} token: ${got}`)
		}
		// A token that expires mid-connection: the engine must stop serving
		// the socket (the client is expected to send a fresh token first).
		const stopped = await new Promise<string>((resolve) => {
			const ws = new WebSocket(`ws://127.0.0.1:${PORT}/sync`)
			const timer = setTimeout(() => resolve('still served after expiry'), 8000)
			ws.onopen = () =>
				ws.send(
					JSON.stringify({
						type: 'want',
						token: mintToken(randomUUID(), { expSeconds: 2 }),
						wants: [{ id: 1, rule: 'tags', params: {} }],
						have: {},
					})
				)
			ws.onmessage = (ev) => {
				if (typeof ev.data === 'string' && ev.data.includes('token expired')) {
					clearTimeout(timer)
					resolve('stopped')
				}
			}
			ws.onerror = () => {}
		})
		if (stopped !== 'stopped') throw new Error(`expiring token: ${stopped}`)
		return 'forged, expired and wrong-audience tokens rejected; served connections stop at token expiry'
	}

	async schemaReset(): Promise<void> {
		await this.stop()
	}

	async close(): Promise<void> {
		for (const c of this.clients) await c.close()
		this.clients.clear()
		await this.stop()
	}
}

/** Builds bin/engine (Go) so the adapter always runs the current source. */
export function buildEngine(): void {
	execFileSync('go', ['build', '-o', 'bin/engine', './cmd/engine'], { cwd: REPO, stdio: 'inherit' })
}
