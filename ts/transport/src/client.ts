// One connection to the engine. The client only ever says what it wants and
// what it has (WANT); the engine answers with the difference as atomic
// frames (docs/design.md section 7). Reconnect, subscribe, unsubscribe and
// resync are all the same message. The connection is a WebSocket, or HTTP
// requests where WebSockets fail (http.ts).
import { Applier, type Db } from './apply.ts'
import { MSG_FRAME_PART, type UnitDesc } from './codec.ts'
import { type Conn, HttpConn, httpUrlFor, WsConn } from './http.ts'

export type Want = { rule: string; params?: Record<string, string> }

/** Exclusive access to the local database (localdb runs it inside its write fence). */
export type Run = <T>(fn: (db: Db) => T) => Promise<T>

export type SyncClientOptions = {
	url: string
	token: () => string | Promise<string>
	/** A database used directly, or `run` for access that must be serialised. */
	db?: Db
	run?: Run
	/** After every applied frame, with the local tables it touched. */
	onCommit?: (tables: Set<string>, rows: number) => void
	/** Socket state, for status reporting. */
	onState?: (state: 'connecting' | 'open' | 'closed') => void
	onError?: (message: string) => void
	/** Payload bytes received (frames and control), for measurement. */
	onBytes?: (n: number) => void
	WebSocket?: typeof WebSocket
	/**
	 * 'auto' (default): WebSocket, falling back to HTTP when WebSockets
	 * fail on this network (and trying them again every 10 minutes).
	 */
	transport?: 'auto' | 'websocket' | 'http'
	/** The engine's /sync/http URL; default: `url` with http(s) and /http appended. */
	httpUrl?: string
}

/** How the client is connecting: a WebSocket, a streamed HTTP response or long polls. */
export type ConnMode = 'websocket' | 'stream' | 'poll'

type UnitsMsg = {
	type: 'units'
	epoch: string
	units: UnitDesc[]
	errors: Array<{ id: number; error: string }>
}
type ErrorMsg = { type: 'error'; message: string }
/** The engine restarted in process (lost its replication stream): send WANT again. */
type ResyncMsg = { type: 'resync'; epoch: string }
/** Liveness on an idle connection (engine sends one every 15s). */
type BeatMsg = { type: 'beat' }

// The first message (the answer to WANT) must arrive within firstMs, then
// something (a frame or a beat) every idleMs; otherwise the connection is
// dead or a proxy is holding it back.
// A long poll behind a buffering proxy answers only when it ends (the
// engine ends it within 20s).
const firstMs = 10_000
const firstStreamMs = 5_000
const firstPollMs = 30_000
const idleMs = 45_000
// After WebSockets failed this often in a row, use HTTP.
const wsFailLimit = 2
const wsRetryMs = 10 * 60_000

// A unit is a rule, its parameters and the principal: the same rule for
// someone else (another user signing in on this database, a masquerade) is
// a different unit, so their state is never taken for this user's. Their
// units are then held but not wanted, and the next frame drops them.
const defKey = (w: Want, sub: string) =>
	JSON.stringify([
		w.rule,
		Object.entries(w.params ?? {}).sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0)),
		sub,
	])

/** The token's subject (the token is the engine's to verify; this only names state). */
function subOf(token: string): string {
	try {
		const payload = JSON.parse(
			atob(token.split('.')[1]!.replace(/-/g, '+').replace(/_/g, '/'))
		) as {
			sub?: unknown
		}
		return typeof payload.sub === 'string' ? payload.sub : ''
	} catch {
		return ''
	}
}

export class SyncClient {
	private opts: SyncClientOptions
	private applier: Applier
	private conn: Conn | null = null
	/** Got a message on the current connection; got an error message. */
	private heard = false
	private failed = false
	private state: 'connecting' | 'open' | 'closed' | null = null
	private watch: ReturnType<typeof setTimeout> | null = null
	private mode: ConnMode
	private wsFailures = 0
	private httpSince = 0
	private auto: boolean
	/** The mode recorded in _lsync_state (auto: where the next start begins). */
	private storedMode = ''
	private wants: Want[] = []
	private epoch = ''
	private stopped = true
	private backoff = 250
	private timer: ReturnType<typeof setTimeout> | null = null
	private refreshTimer: ReturnType<typeof setTimeout> | null = null
	private run: Run
	/** Parts of the frame being received. */
	private parts: Uint8Array[] = []
	/** Messages are applied strictly in arrival order, one at a time. */
	private queue: Promise<unknown> = Promise.resolve()
	readonly errors: string[] = []

	constructor(opts: SyncClientOptions) {
		this.opts = opts
		const db = opts.db
		if (opts.run) this.run = opts.run
		else if (db) this.run = async (fn) => fn(db)
		else throw new Error('SyncClient needs db or run')
		this.applier = new Applier()
		this.mode = opts.transport === 'http' ? 'stream' : 'websocket'
		this.auto = (opts.transport ?? 'auto') === 'auto'
	}

	/** The current transport mode (status and tests). */
	get connMode(): ConnMode {
		return this.mode
	}

	private enqueue<T>(fn: () => Promise<T>): Promise<T> {
		const next = this.queue.then(fn)
		this.queue = next.catch(() => {})
		return next
	}

	start(wants: Want[]): void {
		this.wants = wants
		this.stopped = false
		if (!this.auto) {
			this.connect()
			return
		}
		// Start where this database last connected: on a network that needs
		// HTTP (or long polls), every app load would otherwise wait for the
		// WebSocket and buffering checks again.
		void this.enqueue(() =>
			this.run((db) => {
				this.applier.ensureSchema(db)
				return db.all(`SELECT v FROM _lsync_state WHERE k = 'transport'`)[0]?.[0]
			})
		)
			.then(
				(v) => {
					const [mode, since] = String(v ?? '').split(' ')
					this.storedMode = mode || 'websocket'
					if (mode === 'stream' || mode === 'poll') {
						this.mode = mode
						this.httpSince = Number(since)
					}
				},
				() => {}
			)
			.then(() => this.connect())
	}

	/** Record the mode that works, for the next start. */
	private storeMode(): void {
		if (!this.auto || this.mode === this.storedMode) return
		this.storedMode = this.mode
		const v = `${this.mode} ${this.httpSince}`
		void this.enqueue(() =>
			this.run((db) => db.exec(`INSERT OR REPLACE INTO _lsync_state VALUES ('transport', ?)`, [v]))
		)
	}

	/** Replace the wanted set; applied atomically by the engine. */
	async want(wants: Want[]): Promise<void> {
		this.wants = wants
		await this.rewant()
	}

	/** Send WANT again; over HTTP that is a new request. */
	private async rewant(): Promise<void> {
		const conn = this.conn
		if (!conn) return
		if (conn.oneShot) {
			this.conn = null
			conn.close()
			this.connect()
		} else if (conn.open) await this.sendWant()
	}

	stop(): void {
		this.stopped = true
		if (this.timer) clearTimeout(this.timer)
		if (this.refreshTimer) clearTimeout(this.refreshTimer)
		if (this.watch) clearTimeout(this.watch)
		this.conn?.close()
		this.conn = null
	}

	/** Unit id for a want, stable across connections (one per definition). */
	private unitId(db: Db, w: Want, sub: string): number {
		const key = defKey(w, sub)
		const row = db.all('SELECT id FROM _lsync_unit WHERE def = ?', [key])[0]
		if (row) return Number(row[0])
		db.exec('INSERT INTO _lsync_unit (def) VALUES (?)', [key])
		return Number(db.all('SELECT id FROM _lsync_unit WHERE def = ?', [key])[0]![0])
	}

	private async sendWant(): Promise<void> {
		const wantList = this.wants
		// The token first: its subject names the units (defKey).
		const token = await this.opts.token()
		const sub = subOf(token)
		const { state, wants, have } = await this.enqueue(() =>
			this.run((db) => {
				this.applier.ensureSchema(db)
				return {
					state: new Map(
						db.all('SELECT k, v FROM _lsync_state').map(([k, v]) => [k as string, v as string])
					),
					wants: wantList.map((w) => ({
						id: this.unitId(db, w, sub),
						rule: w.rule,
						params: w.params ?? {},
					})),
					have: db
						.all('SELECT id, checksum FROM _lsync_unit WHERE held = 1 AND drift = 0')
						.map(([id, checksum]) => ({ id: Number(id), checksum: String(checksum) })),
				}
			})
		)
		const conn = this.conn
		if (!conn?.open) return
		conn.send(
			JSON.stringify({
				type: 'want',
				token,
				wants,
				have: { epoch: state.get('epoch') ?? '', cursor: state.get('cursor') ?? '0', units: have },
			})
		)
		this.watchdog(
			conn,
			this.mode === 'poll' ? firstPollMs : this.mode === 'stream' ? firstStreamMs : firstMs
		)
		this.scheduleRefresh(token)
	}

	/** Close conn unless a message arrives within ms (reset per message). */
	private watchdog(conn: Conn, ms: number): void {
		if (this.watch) clearTimeout(this.watch)
		this.watch = setTimeout(() => {
			if (this.conn !== conn) return
			if (!this.heard && this.mode === 'stream') {
				// The response is held back: a proxy buffers it. Long-poll
				// (not a failure: the next request goes out at once).
				this.mode = 'poll'
				this.error('HTTP stream held back by a proxy; long polling')
				this.conn = null
				conn.close()
				this.connect()
				return
			}
			this.error(this.heard ? 'connection idle; reconnecting' : 'no answer to WANT; reconnecting')
			conn.close()
		}, ms)
	}

	/**
	 * The engine stops serving a connection when its token expires, so
	 * re-send WANT (same wants, fresh token) a minute before that.
	 */
	private scheduleRefresh(token: string): void {
		if (this.refreshTimer) clearTimeout(this.refreshTimer)
		let exp = 0
		try {
			const payload = JSON.parse(
				atob(token.split('.')[1]!.replace(/-/g, '+').replace(/_/g, '/'))
			) as { exp?: number }
			exp = (payload.exp ?? 0) * 1000
		} catch {
			return
		}
		const delay = Math.max(5_000, exp - Date.now() - 60_000)
		this.refreshTimer = setTimeout(() => {
			this.rewant().catch((err: unknown) => this.error(`token refresh: ${String(err)}`))
		}, delay)
	}

	private connect(): void {
		if (this.stopped) return
		if (this.mode !== 'websocket' && this.auto && Date.now() - this.httpSince > wsRetryMs) {
			// Networks change: try WebSockets again; one more failure returns here.
			this.mode = 'websocket'
			this.wsFailures = wsFailLimit - 1
		}
		const conn: Conn =
			this.mode === 'websocket'
				? new WsConn(this.opts.url, this.opts.WebSocket ?? WebSocket)
				: new HttpConn(this.opts.httpUrl ?? httpUrlFor(this.opts.url), this.mode === 'poll')
		this.conn = conn
		this.heard = false
		this.failed = false
		// Through the queue: frames of the previous connection may still be applying.
		this.parts = []
		void this.enqueue(async () => this.applier.resetRefs())
		// The next HTTP request of a live client is not a reconnect.
		if (this.state !== 'open') this.setState('connecting')
		conn.onopen = () => {
			if (this.conn !== conn) return
			this.sendWant().catch((err: unknown) => this.error(`token: ${String(err)}`))
		}
		conn.onmessage = (data) => {
			if (this.conn !== conn) return
			if (!this.heard) {
				this.heard = true
				this.backoff = 250
				if (this.mode === 'websocket') this.wsFailures = 0
				this.setState('open')
				this.storeMode()
			}
			this.watchdog(conn, idleMs)
			this.message(data, conn)
		}
		conn.onclose = (clean) => {
			if (this.conn !== conn) return
			this.conn = null
			if (this.watch) clearTimeout(this.watch)
			if (this.stopped) return
			if (!this.heard && this.auto) {
				if (this.mode === 'websocket' && ++this.wsFailures >= wsFailLimit) {
					this.mode = 'stream'
					this.httpSince = Date.now()
					this.error('WebSocket failed; trying HTTP')
					this.setState('closed')
					this.connect()
					return
				}
				if (this.mode !== 'websocket') {
					// HTTP failed too: likely the engine is down, not the
					// network. Alternate until one of them answers.
					this.mode = 'websocket'
					this.wsFailures = wsFailLimit - 1
				}
			}
			if (clean && this.heard && !this.failed) {
				// The engine ended an HTTP response: the next one, at once.
				this.connect()
				return
			}
			this.setState('closed')
			this.timer = setTimeout(() => this.connect(), this.backoff)
			this.backoff = Math.min(this.backoff * 2, 10_000)
		}
	}

	private setState(s: 'connecting' | 'open' | 'closed'): void {
		if (this.state === s) return
		this.state = s
		this.opts.onState?.(s)
	}

	private message(data: string | ArrayBuffer, conn: Conn): void {
		if (typeof data === 'string') {
			this.opts.onBytes?.(data.length)
			const m = JSON.parse(data) as UnitsMsg | ErrorMsg | ResyncMsg | BeatMsg
			if (m.type === 'units') {
				this.epoch = m.epoch
				void this.enqueue(() => this.run((db) => this.applier.setUnits(db, m.units)))
				for (const e of m.errors ?? []) this.error(`unit ${e.id}: ${e.error}`)
			} else if (m.type === 'error') {
				this.failed = true
				this.error(m.message)
			} else if (m.type === 'resync') {
				// A frame cut short by the restart never completes; its parts
				// were never applied. Refs are per epoch, like a reconnect.
				this.parts = []
				void this.enqueue(async () => this.applier.resetRefs())
				this.rewant().catch((err: unknown) => this.error(`resync: ${String(err)}`))
			}
			return
		}
		const buf = new Uint8Array(data)
		this.opts.onBytes?.(buf.byteLength)
		// A frame may arrive in parts; it is applied once, whole.
		if (buf[0] === MSG_FRAME_PART) {
			this.parts.push(buf)
			return
		}
		const parts = this.parts.length ? [...this.parts, buf] : buf
		this.parts = []
		const epoch = this.epoch
		this.enqueue(() => this.run((db) => this.applier.apply(db, parts, epoch))).then(
			(r) => {
				this.opts.onCommit?.(r.tables, r.rows)
				if (r.drift.length) {
					// Checksum drift: the unit is excluded from `have` on the next
					// WANT, so the engine re-sends it whole.
					this.error(
						`checksum drift in units ${r.drift.join(', ')}; resyncing (${r.why.join('; ')})`
					)
					conn.close()
				}
			},
			(err: unknown) => {
				this.error(`apply failed: ${String(err)}`)
				conn.close()
			}
		)
	}

	private error(msg: string): void {
		this.errors.push(msg)
		this.opts.onError?.(msg)
	}
}
