// The protocol over plain HTTP (the engine's /sync/http), for networks
// where WebSockets fail. One request is one connection: its body is the
// WANT, its response the engine's messages as records (a kind byte, the
// length as 4 bytes little-endian, the payload; kind 0x80 marks raw
// deflate) until the engine ends it and the client re-POSTs. A new WANT is
// a new request that resumes from the client's cursor.
//
// `poll` asks the engine to end each response after the first frame with
// data: behind a proxy that buffers whole responses, that still delivers.

export const REC_TEXT = 0
export const REC_BINARY = 1
export const REC_DEFLATED = 0x80

/** A connection to the engine, as SyncClient uses it (WebSocket-shaped). */
export type Conn = {
	/** HTTP: one WANT per connection; a new WANT needs a new one. */
	readonly oneShot: boolean
	readonly open: boolean
	send(data: string): void
	close(): void
	onopen: (() => void) | null
	onmessage: ((data: string | ArrayBuffer) => void) | null
	/** clean: the engine ended it (HTTP: reconnect at once, no backoff). */
	onclose: ((clean: boolean) => void) | null
}

export class WsConn implements Conn {
	readonly oneShot = false
	onopen: (() => void) | null = null
	onmessage: ((data: string | ArrayBuffer) => void) | null = null
	onclose: ((clean: boolean) => void) | null = null
	private ws: WebSocket

	constructor(url: string, WS: typeof WebSocket) {
		const ws = new WS(url)
		ws.binaryType = 'arraybuffer'
		ws.onopen = () => this.onopen?.()
		ws.onmessage = (ev: MessageEvent) => this.onmessage?.(ev.data as string | ArrayBuffer)
		ws.onclose = () => this.onclose?.(false)
		ws.onerror = () => {}
		this.ws = ws
	}

	get open(): boolean {
		return this.ws.readyState === 1
	}

	send(data: string): void {
		this.ws.send(data)
	}

	close(): void {
		this.ws.close()
	}
}

const inflate = async (b: Uint8Array): Promise<ArrayBuffer> =>
	new Response(
		new Blob([b as Uint8Array<ArrayBuffer>])
			.stream()
			.pipeThrough(new DecompressionStream('deflate-raw'))
	).arrayBuffer()

const utf8 = new TextDecoder()

export class HttpConn implements Conn {
	readonly oneShot = true
	onopen: (() => void) | null = null
	onmessage: ((data: string | ArrayBuffer) => void) | null = null
	onclose: ((clean: boolean) => void) | null = null
	private abort = new AbortController()
	private sent = false
	private closed = false
	private url: string

	constructor(url: string, poll: boolean) {
		this.url = poll ? `${url}${url.includes('?') ? '&' : '?'}mode=poll` : url
		// Nothing to open: the request goes out with the WANT.
		queueMicrotask(() => {
			if (!this.closed) this.onopen?.()
		})
	}

	get open(): boolean {
		return !this.closed && !this.sent
	}

	send(want: string): void {
		if (this.sent) throw new Error('one WANT per HTTP request')
		this.sent = true
		this.run(want).then(
			() => this.end(true),
			() => this.end(false)
		)
	}

	close(): void {
		this.abort.abort()
		this.end(false)
	}

	private end(clean: boolean): void {
		if (this.closed) return
		this.closed = true
		queueMicrotask(() => this.onclose?.(clean))
	}

	private async run(want: string): Promise<void> {
		// A string body is text/plain: a simple request, no CORS preflight.
		const res = await fetch(this.url, {
			method: 'POST',
			body: want,
			cache: 'no-store',
			signal: this.abort.signal,
		})
		if (!res.ok || !res.body) throw new Error(`HTTP ${res.status}`)
		const reader = res.body.getReader()
		let buf = new Uint8Array(64 * 1024)
		let start = 0
		let end = 0
		for (;;) {
			const { done, value } = await reader.read()
			if (done) break
			if (end + value.length > buf.length) {
				// Move the partial record to the front; grow if it still won't fit.
				const live = buf.subarray(start, end)
				const next =
					live.length + value.length > buf.length
						? new Uint8Array(Math.max(buf.length * 2, live.length + value.length))
						: buf
				next.set(live)
				buf = next
				end = live.length
				start = 0
			}
			buf.set(value, end)
			end += value.length
			while (end - start >= 5) {
				const n =
					(buf[start + 1]! | (buf[start + 2]! << 8) | (buf[start + 3]! << 16)) +
					buf[start + 4]! * 0x1000000
				if (end - start < 5 + n) break
				const kind = buf[start]!
				// A copy with its own buffer: buf is reused for later records.
				const raw = buf.slice(start + 5, start + 5 + n)
				start += 5 + n
				const data = kind & REC_DEFLATED ? await inflate(raw) : raw.buffer
				if (this.closed) return
				this.onmessage?.((kind & ~REC_DEFLATED) === REC_TEXT ? utf8.decode(data) : data)
			}
		}
		if (end !== start) throw new Error('response ended inside a record')
	}
}

/** The /sync/http URL next to a ws(s):// /sync URL. */
export function httpUrlFor(wsUrl: string): string {
	const u = new URL(wsUrl)
	u.protocol = u.protocol === 'wss:' ? 'https:' : 'http:'
	u.pathname = `${u.pathname.replace(/\/$/, '')}/http`
	return u.toString()
}
