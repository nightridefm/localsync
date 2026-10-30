// Clients through a TLS gateway (docker/gateway: nginx configured like the
// apps' gateways, real certificate): WebSocket over wss://, the HTTP
// transport over https://, and the fallback through a hostile middlebox
// (no WebSocket upgrade, whole responses buffered). Each check converges,
// stays idle past nginx's 60s proxy timeout, then times live updates and
// asserts the connection was never cut and the transport is the expected
// one.
//
//   docker compose --profile gateway up -d gateway
//   pnpm --filter @nightride/localsync-harness gateway [--idle 75] [--only wss,https,hostile]
import { randomUUID } from 'node:crypto'
import { parseArgs } from 'node:util'

import { introspect } from './catalog.ts'
import { ensureFixture, resetData, seedWorld } from './fixture.ts'
import {
	buildEngine,
	type ConnOptions,
	LocalsyncAdapter,
	type LocalsyncClient,
} from './localsync.ts'
import { startMiddlebox } from './middlebox.ts'
import { Db } from './pg.ts'
import { Ctx } from './scenario.ts'

const { values } = parseArgs({
	options: {
		idle: { type: 'string', default: '75' },
		only: { type: 'string', default: 'wss,https,hostile,auto' },
	},
})
const domain = process.env.LOCALSYNC_TLS_DOMAIN
if (!domain) throw new Error('set LOCALSYNC_TLS_DOMAIN (the gateway certificate name; see .env)')
const base = `${domain}:8443`

type Check = {
	name: string
	what: string
	conn: ConnOptions
	mode: string
	/** Update latency p50 must be below this (ms). */
	p50Max: number
}
const checks: Check[] = [
	{
		name: 'wss',
		what: 'WebSocket over wss://',
		conn: { url: `wss://${base}/sync`, transport: 'websocket' },
		mode: 'websocket',
		p50Max: 100,
	},
	{
		name: 'https',
		what: 'HTTP stream over https://',
		conn: { url: `wss://${base}/sync`, transport: 'http' },
		mode: 'stream',
		p50Max: 100,
	},
	{
		name: 'auto',
		what: 'auto through the gateway: stays on WebSocket',
		conn: { url: `wss://${base}/sync`, transport: 'auto' },
		mode: 'websocket',
		p50Max: 100,
	},
	{
		name: 'hostile',
		what: 'auto through a middlebox that drops upgrades and buffers responses',
		conn: { url: `wss://${base}/hostile/sync`, transport: 'auto' },
		mode: 'poll',
		// A long poll: the update waits for the response to end and the
		// next request, plus nginx's buffering.
		p50Max: 500,
	},
]

const pct = (xs: number[], p: number) => {
	const s = [...xs].sort((a, b) => a - b)
	return s.length ? s[Math.min(s.length - 1, Math.floor((p / 100) * s.length))]! : 0
}

async function check(db: Db, k: Check): Promise<string> {
	await resetData(db)
	const adapter = new LocalsyncAdapter(k.conn)
	try {
		const ctx = new Ctx(db, adapter, await introspect(db))
		const p = ctx.principal()
		const tag = randomUUID()
		await ctx.write(async (q) => {
			await seedWorld(q, { orgs: 1, projects: 1, tasks: 2000, comments: 1, memberOf: [[p.sub]] })
			await q(`INSERT INTO tag VALUES ($1, '0')`, [tag])
		})
		const c = await ctx.connect(p, ['tasks', 'comments', 'tags'])
		const t0 = performance.now()
		await ctx.converge(c, 120_000)
		const loadMs = Math.round(performance.now() - t0)
		const client = c.client as LocalsyncClient
		const replica = client.replica.db
		let seq = 0
		const timeTag = async (): Promise<number> => {
			const n = ++seq
			let resolve!: () => void
			const seen = new Promise<void>((r) => (resolve = r))
			const look = () => {
				const v = replica.selectValue('SELECT label FROM tag WHERE id = ?', [tag])
				if (typeof v === 'string' && Number(v) >= n) resolve()
			}
			const off = client.onCommit(look)
			const start = performance.now()
			await db.query('UPDATE tag SET label = $2 WHERE id = $1', [tag, String(n)])
			look()
			await Promise.race([
				seen,
				new Promise((_, rej) =>
					setTimeout(() => rej(new Error(`update ${n} not seen in 30s`)), 30_000)
				),
			])
			off()
			return performance.now() - start
		}
		const before: number[] = []
		for (let i = 0; i < 20; i++) before.push(await timeTag())
		// Idle past the proxy's read timeout: nothing written, nothing sent
		// but the engine's beats.
		const errorsBefore = client.errors().length
		await new Promise((r) => setTimeout(r, Number(values.idle) * 1000))
		const after: number[] = []
		for (let i = 0; i < 20; i++) after.push(await timeTag())
		const idleErrors = client.errors().slice(errorsBefore)
		await ctx.converge(c)
		const mode = client.connMode
		// An app reload on the same database: a write while it was away,
		// then a fresh start must pick up where the last one connected.
		await client.disconnect()
		await db.query('UPDATE tag SET label = $2 WHERE id = $1', [tag, String(++seq)])
		const r0 = performance.now()
		await client.reconnect()
		await ctx.converge(c, 60_000)
		const restartMs = Math.round(performance.now() - r0)
		await ctx.closeAll()
		const lat = [...before, ...after]
		const line = `${k.what}: mode ${mode}, load ${loadMs}ms (2000 tasks + comments), restart ${restartMs}ms, update p50 ${pct(lat, 50).toFixed(0)}ms max ${Math.max(...lat).toFixed(0)}ms`
		if (restartMs > 3000) throw new Error(`${line}: restart above 3s`)
		if (mode !== k.mode) throw new Error(`${line}: expected mode ${k.mode}`)
		if (pct(lat, 50) > k.p50Max) throw new Error(`${line}: p50 above ${k.p50Max}ms`)
		// The hostile path ends every long poll on purpose; elsewhere an
		// idle connection must survive the proxy's timeout.
		if (k.mode !== 'poll' && idleErrors.length)
			throw new Error(`${line}: connection cut while idle (${idleErrors.join('; ')})`)
		return k.mode === 'poll' ? line : `${line}; idle ${values.idle}s without a reconnect`
	} finally {
		await adapter.close()
	}
}

buildEngine()
const middlebox = await startMiddlebox()
const db = new Db()
await ensureFixture(db)
let failed = 0
for (const k of checks) {
	if (!values.only!.split(',').includes(k.name)) continue
	try {
		console.log(`ok   ${await check(db, k)}`)
	} catch (err) {
		failed++
		console.log(`FAIL ${err instanceof Error ? err.message : String(err)}`)
	}
}
await db.close()
middlebox.close()
process.exit(failed ? 1 : 0)
