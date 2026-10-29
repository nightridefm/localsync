// What permessage-deflate would save (design Q5), measured on the exact
// messages the engine sends. The client's WebSocket is wrapped to keep
// every received message; they are then compressed offline the two ways
// permessage-deflate can run: each message on its own (no context
// takeover, cheap for the server) or one deflate stream per connection
// flushed per message (context takeover, ~300 kB of compressor state per
// connection on the server, but small deltas compress against earlier
// messages). Sizes include the 4 trailing bytes the extension strips.
//
//   pnpm --filter @nightride/localsync-harness bench:deflate [--rows=10000] [--write]
import { randomUUID } from 'node:crypto'
import { mkdirSync, writeFileSync } from 'node:fs'
import path from 'node:path'
import { parseArgs } from 'node:util'
import zlib from 'node:zlib'

import { introspect } from './catalog.ts'
import { ensureFixture, resetData, seedWorld } from './fixture.ts'
import { buildEngine, LocalsyncAdapter } from './localsync.ts'
import { Db } from './pg.ts'
import { Ctx } from './scenario.ts'

const { values } = parseArgs({
	options: {
		rows: { type: 'string', default: '10000' },
		write: { type: 'boolean', default: false },
	},
})
const rows = Number(values.rows)

type Msg = { socket: number; data: Uint8Array }
const phases = new Map<string, Msg[]>()
let phase = 'load'
let sockets = 0

class CapturingWebSocket extends WebSocket {
	constructor(url: string | URL, protocols?: string | string[]) {
		super(url, protocols)
		const id = ++sockets
		this.addEventListener('message', (ev: MessageEvent) => {
			const data =
				typeof ev.data === 'string'
					? new TextEncoder().encode(ev.data)
					: new Uint8Array(ev.data as ArrayBuffer).slice()
			let list = phases.get(phase)
			if (!list) phases.set(phase, (list = []))
			list.push({ socket: id, data })
		})
	}
}

// One message alone: raw deflate, sync flush, 4-byte tail stripped.
function perMessage(m: Uint8Array, level: number): number {
	return zlib.deflateRawSync(m, { level, finishFlush: zlib.constants.Z_SYNC_FLUSH }).length - 4
}

// One stream per connection (context takeover): sizes per message.
async function takeover(msgs: Msg[], level: number): Promise<number[]> {
	const streams = new Map<number, zlib.DeflateRaw>()
	const out: number[] = []
	for (const m of msgs) {
		let z = streams.get(m.socket)
		if (!z) streams.set(m.socket, (z = zlib.createDeflateRaw({ level })))
		let n = 0
		const onData = (b: Buffer) => (n += b.length)
		z.on('data', onData)
		z.write(m.data)
		await new Promise<void>((r) => z.flush(zlib.constants.Z_SYNC_FLUSH, () => r()))
		// 'data' events for the flushed output are emitted before the callback.
		z.off('data', onData)
		out.push(n - 4)
	}
	for (const z of streams.values()) z.close()
	return out
}

const db = new Db()
if (await ensureFixture(db)) console.log('fixture recreated')
await resetData(db)
buildEngine()
const adapter = new LocalsyncAdapter({ WebSocket: CapturingWebSocket })
const ctx = new Ctx(db, adapter, await introspect(db))
const p = ctx.principal()
let w!: Awaited<ReturnType<typeof seedWorld>>
await ctx.write(async (q) => {
	w = await seedWorld(q, { orgs: 1, projects: 1, tasks: rows, comments: 1, memberOf: [[p.sub]] })
})
const c = await ctx.connect(p, ['tasks', 'comments'])
await ctx.converge(c, 300_000)

phase = 'update'
for (let i = 0; i < 200; i++) {
	await db.query('UPDATE task SET title = $2 WHERE id = $1', [w.tasks[i % 50], `bench ${i}`])
	if (i % 20 === 19) await ctx.converge(c)
}
await ctx.converge(c)

phase = 'two-table txn'
for (let i = 0; i < 50; i++) {
	await db.tx(async (q) => {
		await q('UPDATE task SET txn_tag = $2 WHERE id = $1', [w.tasks[i], `multi ${i}`])
		await q('UPDATE comment SET txn_tag = $2 WHERE task_id = $1', [w.tasks[i], `multi ${i}`])
	})
}
await ctx.converge(c)

phase = 'insert'
for (let i = 0; i < 50; i++)
	await db.query(`INSERT INTO task (id, project_id, title) VALUES ($1, $2, 'new')`, [
		randomUUID(),
		w.projects[0],
	])
await ctx.converge(c)

phase = 'grant (20 rows)'
for (let i = 0; i < 10; i++) {
	phase = 'seed'
	let other!: Awaited<ReturnType<typeof seedWorld>>
	await ctx.write(async (q) => {
		other = await seedWorld(q, { orgs: 1, projects: 1, tasks: 20, comments: 0, memberOf: [[]] })
	})
	await ctx.converge(c)
	phase = 'grant (20 rows)'
	await db.query(`INSERT INTO member VALUES (gen_random_uuid(), $1, $2, 'member')`, [
		other.orgs[0],
		p.sub,
	])
	await ctx.converge(c)
}
await ctx.closeAll()
await adapter.close()
await db.close()

type Row = {
	phase: string
	messages: number
	raw: number
	msg1: number
	msg6: number
	ctx1: number
	ctx6: number
	ctx1Ms: number
}
const results: Row[] = []
for (const [name, msgs] of phases) {
	if (name === 'seed') continue
	let raw = 0
	let msg1 = 0
	let msg6 = 0
	for (const m of msgs) {
		raw += m.data.length
		msg1 += perMessage(m.data, 1)
		msg6 += perMessage(m.data, 6)
	}
	const t = performance.now()
	const ctx1 = (await takeover(msgs, 1)).reduce((a, b) => a + b, 0)
	const ctx1Ms = performance.now() - t
	const ctx6 = (await takeover(msgs, 6)).reduce((a, b) => a + b, 0)
	const r = { phase: name, messages: msgs.length, raw, msg1, msg6, ctx1, ctx6, ctx1Ms }
	results.push(r)
	console.log(JSON.stringify(r))
}

if (values.write) {
	const pct = (a: number, raw: number) =>
		`${a.toLocaleString('en')} (${Math.round((100 * a) / raw)}%)`
	const lines = [
		`# permessage-deflate estimate (${new Date().toISOString().slice(0, 10)})`,
		'',
		'Generated by `ts/harness/src/bench-deflate.ts` (design Q5). The',
		'messages the engine sent one client during the `bench.ts` workload',
		`(${rows} tasks with a comment each, then single-row updates, two-table`,
		'transactions, inserts and grants of 20 rows), compressed offline the',
		'two ways permessage-deflate runs. Bytes per phase, all messages',
		'(frames and JSON control) included; percent of raw.',
		'',
		'| Phase | Messages | Raw | Per message, level 1 | Per message, level 6 | Context takeover, level 1 | Context takeover, level 6 |',
		'| --- | --- | --- | --- | --- | --- | --- |',
		...results.map(
			(r) =>
				`| ${r.phase} | ${r.messages} | ${r.raw.toLocaleString('en')} | ${pct(r.msg1, r.raw)} | ${pct(r.msg6, r.raw)} | ${pct(r.ctx1, r.raw)} | ${pct(r.ctx6, r.raw)} |`
		),
		'',
	]
	const dir = path.resolve(import.meta.dirname, '../../../docs/bench')
	mkdirSync(dir, { recursive: true })
	writeFileSync(path.join(dir, 'deflate.md'), lines.join('\n'))
}
