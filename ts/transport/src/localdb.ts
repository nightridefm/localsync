// localsync as a localdb SyncTransport. localdb defines the interface and a
// registry; the app's writer worker entry registers this factory:
//
//   import { registerSyncTransport } from '@nightride/localdb/writer.worker'
//   import { localsyncTransport } from '@nightride/localsync'
//   registerSyncTransport('localsync', localsyncTransport)
//
// and LocalDbConfig.sync = { transport: 'localsync', options: { url, tokenUrl, wants } }.
// The types below are structural, so this package does not depend on
// localdb (and localdb not on it).
import type { Db } from './apply.ts'
import { type Run, SyncClient, type Want, type WantError } from './client.ts'

/** localdb's SyncStatus, the states a transport reports. */
export type TransportStatus =
	| { state: 'connecting' }
	/** Caught up on every want the engine serves. */
	| { state: 'live' }
	| { state: 'paused' }
	/** The connection failed; the transport retries after retryInMs. */
	| { state: 'error'; error: string; retryInMs: number; httpStatus?: number }
	/**
	 * Some wants are not served; everything else syncs as usual (after
	 * 'live', it is caught up). permanent: refused, so retrying cannot help.
	 * Otherwise the engine keeps retrying, and 'live' follows once every
	 * want is served again.
	 */
	| {
			state: 'failed'
			error: string
			failures: Array<{ name: string; error: string; permanent: boolean }>
	  }

/** What localdb's writer worker hands a transport. */
export type TransportContext = {
	/** Exclusive write access: inside localdb's write fence. */
	run: Run
	/** A transaction committed: localdb broadcasts it to readers. */
	committed(tables: string[], rows: number): void
	status(s: TransportStatus): void
	log?(message: string): void
}

export type TransportHandle = {
	start(): void
	stop(): void
	/** Commands from the page (LocalDb.syncCommand), e.g. a new want list. */
	command?(payload: unknown): void
}

export type LocalsyncOptions = {
	/** ws(s):// URL of the engine's /sync endpoint (or a relay). */
	url: string
	/** Endpoint returning a short-lived JWT as text (same-origin cookies sent). */
	tokenUrl?: string
	/** A fixed token (tests). */
	token?: string
	wants: Want[]
	/** 'auto' (default) falls back to HTTP where WebSockets fail. */
	transport?: 'auto' | 'websocket' | 'http'
	/** The engine's /sync/http URL; default derived from `url`. */
	httpUrl?: string
}

async function fetchToken(opts: LocalsyncOptions): Promise<string> {
	if (opts.token) return opts.token
	if (!opts.tokenUrl) throw new Error('localsync: options.tokenUrl or options.token is required')
	const res = await fetch(opts.tokenUrl, { credentials: 'include' })
	if (!res.ok) {
		// The status reaches TransportStatus.httpStatus: 401/403 means the
		// session no longer authorises syncing.
		throw Object.assign(new Error(`token endpoint ${res.status}`), { status: res.status })
	}
	return (await res.text()).trim()
}

function wantName(w: WantError): string {
	const params = Object.entries(w.params ?? {})
	return params.length ? `${w.rule}(${params.map(([k, v]) => `${k}=${v}`).join(', ')})` : w.rule
}

export function localsyncTransport(ctx: TransportContext, options: unknown): TransportHandle {
	const opts = options as LocalsyncOptions
	let live = false
	let wantErrors: WantError[] = []
	// After 'live' only: until then the app is not reading anything as
	// caught up, and the list is final by the time 'live' comes.
	const reportFailures = () => {
		if (!live || !wantErrors.length) return
		const failures = wantErrors.map((w) => ({
			name: wantName(w),
			error: w.error,
			permanent: w.permanent,
		}))
		ctx.status({
			state: 'failed',
			error: failures.map((f) => `${f.name}: ${f.error}`).join('; '),
			failures,
		})
	}
	const client = new SyncClient({
		url: opts.url,
		httpUrl: opts.httpUrl,
		transport: opts.transport,
		token: () => fetchToken(opts),
		run: ctx.run,
		onCommit: (tables, rows) => ctx.committed([...tables], rows),
		onError: (m) => ctx.log?.(`localsync: ${m}`),
		onState: (s, detail) => {
			if (s === 'open') return // connected, still catching up: stays 'connecting'
			live = s === 'live'
			if (s === 'connecting') ctx.status({ state: 'connecting' })
			else if (s === 'live') {
				ctx.status({ state: 'live' })
				reportFailures()
			} else
				ctx.status({
					state: 'error',
					error: detail?.error ?? 'connection closed',
					retryInMs: detail?.retryInMs ?? 250,
					...(detail?.httpStatus !== undefined && { httpStatus: detail.httpStatus }),
				})
		},
		onWantErrors: (errors) => {
			wantErrors = errors
			if (errors.length) reportFailures()
			else if (live) ctx.status({ state: 'live' })
		},
	})
	let wants = opts.wants
	return {
		start: () => client.start(wants),
		stop: () => client.stop(),
		command(payload) {
			const p = payload as { want?: Want[] }
			if (p?.want) {
				wants = p.want
				void client.want(wants)
			}
		},
	}
}

export type { Db }
