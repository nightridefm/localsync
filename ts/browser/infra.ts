// Shared by the browser smoke (run.ts) and the collab soak (collab.ts):
// static server with the COOP/COEP headers OPFS needs, a CDP page driver,
// and the engine + headless Chrome stack.
import { type ChildProcess, execFileSync, spawn } from 'node:child_process'
import { mkdtempSync, readFileSync, realpathSync, rmSync, writeFileSync } from 'node:fs'
import http from 'node:http'
import os from 'node:os'
import path from 'node:path'

import { RULES } from '@nightride/localsync-harness/fixture.ts'
import { startMiddlebox } from '@nightride/localsync-harness/middlebox.ts'
import { PG_URL } from '@nightride/localsync-harness/pg.ts'

import { buildSmoke } from './build.ts'

export const ROOT = import.meta.dirname
export const REPO = path.resolve(ROOT, '../..')
export const HTTP_PORT = 8078
export const ENGINE_PORT = 3142
const CDP_PORT = 9224

// Where the tabs load the page and reach the engine (LOCALSYNC_BROWSER_VIA):
// direct (default), gateway (the TLS gateway, docker/gateway: page and
// sync on one https:// origin) or hostile (the gateway's /hostile/ path,
// through a middlebox that refuses WebSockets and buffers responses).
// LOCALSYNC_TRANSPORT (websocket, http, auto) is passed to the transport.
const VIA = process.env.LOCALSYNC_BROWSER_VIA ?? 'direct'
const GATEWAY = `${process.env.LOCALSYNC_TLS_DOMAIN}:8444`
export const PAGE_ORIGIN = VIA === 'direct' ? `http://127.0.0.1:${HTTP_PORT}` : `https://${GATEWAY}`
export const SYNC_URL =
	VIA === 'direct'
		? `ws://127.0.0.1:${ENGINE_PORT}/sync`
		: `wss://${GATEWAY}${VIA === 'hostile' ? '/hostile' : ''}/sync`
export const TRANSPORT = process.env.LOCALSYNC_TRANSPORT ?? ''
const SQLITE = realpathSync(path.join(ROOT, 'node_modules/@sqlite.org/sqlite-wasm'))

export const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms))
export const children: ChildProcess[] = []
export const cleanup: Array<() => void> = []

export async function waitFor<T>(
	fn: () => Promise<T | undefined | false>,
	what: string,
	timeoutMs = 30_000
): Promise<T> {
	const end = Date.now() + timeoutMs
	for (;;) {
		const v = await fn()
		if (v) return v
		if (Date.now() > end) throw new Error(`timeout waiting for ${what}`)
		await sleep(100)
	}
}

function serve(): http.Server {
	const mime: Record<string, string> = {
		'.js': 'text/javascript',
		'.mjs': 'text/javascript',
		'.html': 'text/html',
		'.wasm': 'application/wasm',
		'.map': 'application/json',
	}
	return http
		.createServer((req, res) => {
			const url = new URL(req.url ?? '/', 'http://x').pathname
			const file = url.startsWith('/node_modules/@sqlite.org/sqlite-wasm/')
				? path.join(SQLITE, url.slice('/node_modules/@sqlite.org/sqlite-wasm/'.length))
				: path.join(ROOT, url === '/' ? 'index.html' : url)
			try {
				const body = readFileSync(file)
				res.writeHead(200, {
					'Content-Type': mime[path.extname(file)] ?? 'application/octet-stream',
					'Cross-Origin-Opener-Policy': 'same-origin',
					'Cross-Origin-Embedder-Policy': 'require-corp',
					'Cache-Control': 'no-cache',
				})
				res.end(body)
			} catch {
				res.writeHead(404)
				res.end()
			}
		})
		.listen(HTTP_PORT, '127.0.0.1')
}

export async function cdpPage(url: string) {
	const target = await waitFor(async () => {
		try {
			const r = await fetch(`http://127.0.0.1:${CDP_PORT}/json/new`, { method: 'PUT' })
			return r.ok ? ((await r.json()) as { webSocketDebuggerUrl: string }) : undefined
		} catch {
			return undefined
		}
	}, 'Chrome DevTools')
	const ws = new WebSocket(target.webSocketDebuggerUrl)
	await new Promise((r, j) => {
		ws.onopen = r
		ws.onerror = () => j(new Error('CDP socket failed'))
	})
	let id = 0
	const pending = new Map<number, (m: { result?: { result?: { value?: unknown } } }) => void>()
	const consoleErrors: string[] = []
	ws.onmessage = (e) => {
		const m = JSON.parse(String(e.data)) as {
			id?: number
			method?: string
			params?: { type?: string; args?: Array<{ value?: unknown; description?: string }> }
			result?: { result?: { value?: unknown } }
		}
		if (m.method === 'Runtime.consoleAPICalled' && m.params?.type === 'error')
			consoleErrors.push(
				(m.params.args ?? []).map((a) => String(a.value ?? a.description ?? '')).join(' ')
			)
		if (m.id && pending.has(m.id)) {
			pending.get(m.id)!(m)
			pending.delete(m.id)
		}
	}
	const send = (method: string, params: object) =>
		new Promise<{ result?: { result?: { value?: unknown } } }>((resolve) => {
			const mid = ++id
			pending.set(mid, resolve)
			ws.send(JSON.stringify({ id: mid, method, params }))
		})
	await send('Runtime.enable', {})
	await send('Page.navigate', { url })
	return {
		consoleErrors,
		evaluate: async (expression: string) =>
			(await send('Runtime.evaluate', { expression, returnByValue: true })).result?.result?.value,
		close: () => ws.close(),
	}
}

/** Builds the pages and the engine, starts the engine with `rules`, the static server and Chrome. */
export async function startStack(rules: string[]): Promise<void> {
	await buildSmoke(ROOT)
	execFileSync('go', ['build', '-o', 'bin/engine', './cmd/engine'], { cwd: REPO, stdio: 'inherit' })
	const dir = mkdtempSync(path.join(os.tmpdir(), 'localsync-browser-'))
	cleanup.push(() => rmSync(dir, { recursive: true, force: true }))
	const yaml = rules
		.map((n) => {
			const r = RULES[n]!
			return `  ${n}:\n    sql: ${JSON.stringify(r.sql)}${r.params?.length ? `\n    params: ${JSON.stringify(r.params)}` : ''}`
		})
		.join('\n')
	writeFileSync(path.join(dir, 'rules.yaml'), `rules:\n${yaml}\n`)
	const engine = spawn(
		path.join(REPO, 'bin/engine'),
		[
			'serve',
			'--pg',
			PG_URL,
			'--rules',
			path.join(dir, 'rules.yaml'),
			'--listen',
			`127.0.0.1:${ENGINE_PORT}`,
		],
		{
			env: { ...process.env, LOCALSYNC_JWT_SECRET: 'harness-secret' },
			stdio: ['ignore', 'ignore', 'inherit'],
		}
	)
	children.push(engine)
	await waitFor(
		async () => (await fetch(`http://127.0.0.1:${ENGINE_PORT}/health`).catch(() => null))?.ok,
		'engine'
	)
	const server = serve()
	cleanup.push(() => server.close())
	if (VIA === 'hostile') {
		const box = await startMiddlebox()
		cleanup.push(() => box.close())
	}
	const chrome = spawn(
		process.env.CHROME_BIN ?? 'google-chrome',
		[
			'--headless=new',
			`--remote-debugging-port=${CDP_PORT}`,
			`--user-data-dir=${path.join(dir, 'chrome')}`,
			'--no-first-run',
			'--no-default-browser-check',
			'about:blank',
		],
		{ stdio: 'ignore' }
	)
	children.push(chrome)
}

export function shutdown(): void {
	for (const c of children) c.kill('SIGKILL')
	for (const f of cleanup) f()
}
