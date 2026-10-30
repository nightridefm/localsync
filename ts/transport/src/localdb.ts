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
import { type Run, SyncClient, type Want } from './client.ts'

export type TransportStatus =
	| { state: 'connecting' }
	| { state: 'live' }
	| { state: 'paused' }
	| { state: 'error'; error: string; retryInMs: number }

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
	if (!res.ok) throw new Error(`token endpoint ${res.status}`)
	return (await res.text()).trim()
}

export function localsyncTransport(ctx: TransportContext, options: unknown): TransportHandle {
	const opts = options as LocalsyncOptions
	const client = new SyncClient({
		url: opts.url,
		httpUrl: opts.httpUrl,
		transport: opts.transport,
		token: () => fetchToken(opts),
		run: ctx.run,
		onCommit: (tables, rows) => ctx.committed([...tables], rows),
		onError: (m) => ctx.log?.(`localsync: ${m}`),
		onState: (s) => {
			if (s === 'connecting') ctx.status({ state: 'connecting' })
			else if (s === 'open') ctx.status({ state: 'live' })
			else ctx.status({ state: 'error', error: 'connection closed', retryInMs: 250 })
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
