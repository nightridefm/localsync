// Head-of-line blocking during a move-in (design Q3). One client wants
// `tasks` and `tags`. A grant brings a large org's tasks into `tasks` (a
// move-in that needs a query); meanwhile a tag is updated every few ms.
// localsync delivers one frontier per client, so tag updates committed
// while the move-in query runs wait for it; Electric's shapes are
// independent streams. Reports tag-update latency (commit to local commit)
// before and during the move-in, and how long the move-in took to land.
//
//   pnpm --filter @nightride/localsync-harness bench:movein [--rows=20000] [--write]
import { mkdirSync, writeFileSync } from 'node:fs'
import path from 'node:path'
import { parseArgs } from 'node:util'
import { randomUUID } from 'node:crypto'

import type { Adapter } from './adapter.ts'
import { introspect } from './catalog.ts'
import { ElectricAdapter } from './electric.ts'
import { ensureFixture, resetData, seedWorld } from './fixture.ts'
import { buildEngine, LocalsyncAdapter } from './localsync.ts'
import { Db } from './pg.ts'
import { Ctx } from './scenario.ts'

const { values } = parseArgs({
	options: {
		adapter: { type: 'string', default: 'localsync,electric' },
		rows: { type: 'string', default: '20000' },
		write: { type: 'boolean', default: false },
	},
})
const rows = Number(values.rows)
const progress = (m: string) => {
	if (process.env.HARNESS_DEBUG) console.error(`[movein] ${m}`)
}

const pct = (xs: number[], p: number) => {
	const s = [...xs].sort((a, b) => a - b)
	return s.length ? s[Math.min(s.length - 1, Math.floor((p / 100) * s.length))]! : 0
}

// Arrival times of big binary messages (localsync only): when the move-in
// frame starts arriving, which splits engine time from client apply time.
const bigAt: number[] = []
class TimingWebSocket extends WebSocket {
	constructor(url: string | URL, protocols?: string | string[]) {
		super(url, protocols)
		this.addEventListener('message', (ev: MessageEvent) => {
			if (typeof ev.data !== 'string' && (ev.data as ArrayBuffer).byteLength > 64 * 1024)
				bigAt.push(performance.now())
		})
	}
}

type Result = {
	name: string
	moveInMs: number
	frameAtMs?: number
	beforeP50: number
	duringP50: number
	duringMax: number
	during: number
}

async function bench(adapter: Adapter, db: Db): Promise<Result> {
	await resetData(db)
	const ctx = new Ctx(db, adapter, await introspect(db))
	const p = ctx.principal()
	const tag = randomUUID()
	let big!: Awaited<ReturnType<typeof seedWorld>>
	await ctx.write(async (q) => {
		await seedWorld(q, { orgs: 1, projects: 1, tasks: 100, comments: 0, memberOf: [[p.sub]] })
		big = await seedWorld(q, { orgs: 1, projects: 1, tasks: rows, comments: 0, memberOf: [[]] })
		await q(`INSERT INTO tag VALUES ($1, 'start')`, [tag])
	})
	progress('seeded')
	const c = await ctx.connect(p, ['tasks', 'tags'])
	await ctx.converge(c, 300_000)
	progress('converged')
	const replica = c.client.replica.db

	// A tag update every 5 ms; latency = commit to the local commit showing
	// it or a later one (updates held together arrive folded into one).
	let seq = 0
	const timeTag = async (): Promise<number> => {
		const n = ++seq
		let resolve!: () => void
		const seen = new Promise<void>((r) => (resolve = r))
		const check = () => {
			const v = replica.selectValue('SELECT label FROM tag WHERE id = ?', [tag])
			if (typeof v === 'string' && Number(v) >= n) resolve()
		}
		const off = c.client.onCommit(check)
		const start = performance.now()
		await db.query('UPDATE tag SET label = $2 WHERE id = $1', [tag, String(n)])
		check()
		await seen
		off()
		return performance.now() - start
	}
	const before: number[] = []
	for (let i = 0; i < 40; i++) before.push(await timeTag())

	// The move-in: grant the big org, then keep updating the tag until the
	// replica holds the org's last task.
	progress('before done')
	const probe = big.tasks.at(-1)!
	let landed = false
	let resolveLanded!: () => void
	const landedP = new Promise<void>((r) => (resolveLanded = r))
	const off = c.client.onCommit(() => {
		if (!landed && replica.selectValue('SELECT 1 FROM task WHERE id = ?', [probe])) {
			landed = true
			resolveLanded()
		}
	})
	bigAt.length = 0
	const t0 = performance.now()
	await db.query(`INSERT INTO member VALUES (gen_random_uuid(), $1, $2, 'member')`, [
		big.orgs[0],
		p.sub,
	])
	const during: Promise<number>[] = []
	let i = 0
	while (!landed) {
		during.push(timeTag())
		i++
		await Promise.race([landedP, new Promise((r) => setTimeout(r, 5))])
	}
	const moveInMs = performance.now() - t0
	progress(`landed after ${i} updates`)
	off()
	const lat = await Promise.all(during)
	await ctx.closeAll()
	return {
		name: adapter.name,
		moveInMs: Math.round(moveInMs),
		frameAtMs: bigAt.length ? Math.round(bigAt[0]! - t0) : undefined,
		beforeP50: pct(before, 50),
		duringP50: pct(lat, 50),
		duringMax: Math.max(...lat),
		during: lat.length,
	}
}

const db = new Db()
const results: Result[] = []
for (const name of values.adapter!.split(',')) {
	let adapter: Adapter
	if (name === 'electric') adapter = new ElectricAdapter()
	else {
		buildEngine()
		adapter = new LocalsyncAdapter({ WebSocket: TimingWebSocket })
	}
	if ((await ensureFixture(db)) && adapter.schemaReset) await adapter.schemaReset(db)
	const r = await bench(adapter, db)
	await adapter.close()
	results.push(r)
	console.log(JSON.stringify(r))
}
await db.close()

if (values.write) {
	const f = (n: number) => (n < 10 ? n.toFixed(1) : String(Math.round(n)))
	const lines = [
		`# Move-in head-of-line benchmark (${new Date().toISOString().slice(0, 10)})`,
		'',
		'Generated by `ts/harness/src/bench-movein.ts` (design Q3). One client',
		"wants `tasks` and `tags`; a grant brings another org's",
		`${rows} tasks into \`tasks\` while one tag is updated every 5 ms. Tag`,
		'latency is from the update committing to the local commit that shows',
		'it; "move-in" is from the grant committing until the last granted task',
		'is local.',
		'',
		'| Engine | Move-in | First move-in bytes | Tag update p50 before | Tag update p50 during | Tag update max during | Updates during |',
		'| --- | --- | --- | --- | --- | --- | --- |',
		...results.map(
			(r) =>
				`| ${r.name} | ${r.moveInMs} ms | ${r.frameAtMs === undefined ? '-' : `${r.frameAtMs} ms`} | ${f(r.beforeP50)} ms | ${f(r.duringP50)} ms | ${f(r.duringMax)} ms | ${r.during} |`
		),
		'',
	]
	const dir = path.resolve(import.meta.dirname, '../../../docs/bench')
	mkdirSync(dir, { recursive: true })
	writeFileSync(path.join(dir, 'movein.md'), lines.join('\n'))
}
