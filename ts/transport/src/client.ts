// One WebSocket to the engine. The client only ever says what it wants and
// what it has (WANT); the engine answers with the difference as atomic
// frames (docs/design.md section 7). Reconnect, subscribe, unsubscribe and
// resync are all the same message.
import { Applier, type Db } from './apply.ts'
import { MSG_FRAME_PART, type UnitDesc } from './codec.ts'

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
}

type UnitsMsg = {
	type: 'units'
	epoch: string
	units: UnitDesc[]
	errors: Array<{ id: number; error: string }>
}
type ErrorMsg = { type: 'error'; message: string }
/** The engine restarted in process (lost its replication stream): send WANT again. */
type ResyncMsg = { type: 'resync'; epoch: string }

const defKey = (w: Want) =>
	JSON.stringify([
		w.rule,
		Object.entries(w.params ?? {}).sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0)),
	])

export class SyncClient {
	private opts: SyncClientOptions
	private applier: Applier
	private ws: WebSocket | null = null
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
	}

	private enqueue<T>(fn: () => Promise<T>): Promise<T> {
		const next = this.queue.then(fn)
		this.queue = next.catch(() => {})
		return next
	}

	start(wants: Want[]): void {
		this.wants = wants
		this.stopped = false
		this.connect()
	}

	/** Replace the wanted set; applied atomically by the engine. */
	async want(wants: Want[]): Promise<void> {
		this.wants = wants
		if (this.ws?.readyState === 1) await this.sendWant()
	}

	stop(): void {
		this.stopped = true
		if (this.timer) clearTimeout(this.timer)
		if (this.refreshTimer) clearTimeout(this.refreshTimer)
		this.ws?.close()
		this.ws = null
	}

	/** Unit id for a want, stable across connections (one per definition). */
	private unitId(db: Db, w: Want): number {
		const key = defKey(w)
		const row = db.all('SELECT id FROM _lsync_unit WHERE def = ?', [key])[0]
		if (row) return Number(row[0])
		db.exec('INSERT INTO _lsync_unit (def) VALUES (?)', [key])
		return Number(db.all('SELECT id FROM _lsync_unit WHERE def = ?', [key])[0]![0])
	}

	private async sendWant(): Promise<void> {
		const wantList = this.wants
		const { state, wants, have } = await this.enqueue(() =>
			this.run((db) => {
				this.applier.ensureSchema(db)
				return {
					state: new Map(
						db.all('SELECT k, v FROM _lsync_state').map(([k, v]) => [k as string, v as string])
					),
					wants: wantList.map((w) => ({
						id: this.unitId(db, w),
						rule: w.rule,
						params: w.params ?? {},
					})),
					have: db
						.all('SELECT id, checksum FROM _lsync_unit WHERE held = 1 AND drift = 0')
						.map(([id, checksum]) => ({ id: Number(id), checksum: String(checksum) })),
				}
			})
		)
		const token = await this.opts.token()
		this.ws?.send(
			JSON.stringify({
				type: 'want',
				token,
				wants,
				have: { epoch: state.get('epoch') ?? '', cursor: state.get('cursor') ?? '0', units: have },
			})
		)
		this.scheduleRefresh(token)
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
			if (this.ws?.readyState === 1)
				this.sendWant().catch((err: unknown) => this.error(`token refresh: ${String(err)}`))
		}, delay)
	}

	private connect(): void {
		if (this.stopped) return
		const WS = this.opts.WebSocket ?? WebSocket
		const ws = new WS(this.opts.url)
		ws.binaryType = 'arraybuffer'
		this.ws = ws
		// Through the queue: frames of the previous socket may still be applying.
		this.parts = []
		void this.enqueue(async () => this.applier.resetRefs())
		this.opts.onState?.('connecting')
		ws.onopen = () => {
			this.opts.onState?.('open')
			this.backoff = 250
			this.sendWant().catch((err: unknown) => this.error(`token: ${String(err)}`))
		}
		ws.onmessage = (ev: MessageEvent) => {
			if (typeof ev.data === 'string') {
				this.opts.onBytes?.(ev.data.length)
				const m = JSON.parse(ev.data) as UnitsMsg | ErrorMsg | ResyncMsg
				if (m.type === 'units') {
					this.epoch = m.epoch
					void this.enqueue(() => this.run((db) => this.applier.setUnits(db, m.units)))
					for (const e of m.errors ?? []) this.error(`unit ${e.id}: ${e.error}`)
				} else if (m.type === 'error') {
					this.error(m.message)
				} else if (m.type === 'resync') {
					// A frame cut short by the restart never completes; its parts
					// were never applied. Refs are per epoch, like a reconnect.
					this.parts = []
					void this.enqueue(async () => this.applier.resetRefs())
					this.sendWant().catch((err: unknown) => this.error(`resync: ${String(err)}`))
				}
				return
			}
			const buf = new Uint8Array(ev.data as ArrayBuffer)
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
						ws.close()
					}
				},
				(err: unknown) => {
					this.error(`apply failed: ${String(err)}`)
					ws.close()
				}
			)
		}
		ws.onclose = () => {
			if (this.ws !== ws) return
			this.ws = null
			this.opts.onState?.('closed')
			if (this.stopped) return
			this.timer = setTimeout(() => this.connect(), this.backoff)
			this.backoff = Math.min(this.backoff * 2, 10_000)
		}
		ws.onerror = () => {}
	}

	private error(msg: string): void {
		this.errors.push(msg)
		this.opts.onError?.(msg)
	}
}
