// Benchmarks, separate from pass/fail (docs/design.md 14.3): initial load,
// change latency (commit to local commit) and bytes per change, for each
// adapter on the same fixture and reference client. Writes docs/bench/.
//
//   pnpm --filter @nightride/localsync-harness bench [--adapter=localsync,electric] [--rows=10000]
import { randomUUID } from 'node:crypto'
import { mkdirSync, writeFileSync } from 'node:fs'
import path from 'node:path'
import { parseArgs } from 'node:util'

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
		rows: { type: 'string', default: '10000' },
		changes: { type: 'string', default: '200' },
		write: { type: 'boolean', default: false },
	},
})
const rows = Number(values.rows)
const changes = Number(values.changes)

const pct = (xs: number[], p: number) => {
	const s = [...xs].sort((a, b) => a - b)
	return s[Math.min(s.length - 1, Math.floor((p / 100) * s.length))]!
}

type Result = {
	name: string
	loadMs: number
	loadBytes: number
	p50: number
	p99: number
	bytesPerChange: number
	multiP50: number
	insertP50: number
	grantP50: number
}

async function bench(adapter: Adapter, db: Db): Promise<Result> {
	await resetData(db)
	const ctx = new Ctx(db, adapter, await introspect(db))
	const p = ctx.principal()
	let w!: Awaited<ReturnType<typeof seedWorld>>
	await ctx.write(async (q) => {
		w = await seedWorld(q, { orgs: 1, projects: 1, tasks: rows, comments: 1, memberOf: [[p.sub]] })
	})
	const t0 = Date.now()
	const c = await ctx.connect(p, ['tasks', 'comments'])
	await ctx.converge(c, 300_000)
	const loadMs = Date.now() - t0
	const loadBytes = c.client.bytes()

	// Single-row change latency: commit -> the local commit that shows it.
	const replica = c.client.replica.db
	const task = w.tasks[0]!
	const lat: number[] = []
	const b0 = c.client.bytes()
	for (let i = 0; i < changes; i++) {
		const title = `bench ${i}`
		let resolve!: () => void
		const seen = new Promise<void>((r) => (resolve = r))
		const off = c.client.onCommit(() => {
			if (replica.selectValue('SELECT title FROM task WHERE id = ?', [task]) === title) resolve()
		})
		const start = performance.now()
		await db.query('UPDATE task SET title = $2 WHERE id = $1', [task, title])
		await seen
		lat.push(performance.now() - start)
		off()
	}
	const bytesPerChange = Math.round((c.client.bytes() - b0) / changes)

	// A transaction touching both tables: commit -> both visible.
	const multi: number[] = []
	for (let i = 0; i < 50; i++) {
		const tag = `multi ${i}`
		let resolve!: () => void
		const seen = new Promise<void>((r) => (resolve = r))
		const off = c.client.onCommit(() => {
			const t = replica.selectValue('SELECT txn_tag FROM task WHERE id = ?', [task])
			const m = replica.selectValue('SELECT txn_tag FROM comment WHERE task_id = ? LIMIT 1', [task])
			if (t === tag && m === tag) resolve()
		})
		const start = performance.now()
		await db.tx(async (q) => {
			await q('UPDATE task SET txn_tag = $2 WHERE id = $1', [task, tag])
			await q('UPDATE comment SET txn_tag = $2 WHERE task_id = $1', [task, tag])
		})
		await seen
		multi.push(performance.now() - start)
		off()
	}
	// Insert: a new task in a visible project (membership needs the chain).
	const inserts: number[] = []
	for (let i = 0; i < 50; i++) {
		const id = randomUUID()
		let resolve!: () => void
		const seen = new Promise<void>((r) => (resolve = r))
		const off = c.client.onCommit(() => {
			if (replica.selectValue('SELECT 1 FROM task WHERE id = ?', [id])) resolve()
		})
		const start = performance.now()
		await db.query(`INSERT INTO task (id, project_id, title) VALUES ($1, $2, 'new')`, [
			id,
			w.projects[0],
		])
		await seen
		inserts.push(performance.now() - start)
		off()
	}

	// Link change: a grant makes another org's 20 tasks visible.
	const grants: number[] = []
	for (let i = 0; i < 10; i++) {
		let other!: Awaited<ReturnType<typeof seedWorld>>
		await ctx.write(async (q) => {
			other = await seedWorld(q, { orgs: 1, projects: 1, tasks: 20, comments: 0, memberOf: [[]] })
		})
		// Measure the grant on a settled system, not queued behind the seed.
		await ctx.converge(c)
		const probe = other.tasks[19]!
		let resolve!: () => void
		const seen = new Promise<void>((r) => (resolve = r))
		const off = c.client.onCommit(() => {
			if (replica.selectValue('SELECT 1 FROM task WHERE id = ?', [probe])) resolve()
		})
		const start = performance.now()
		await db.query(`INSERT INTO member VALUES (gen_random_uuid(), $1, $2, 'member')`, [
			other.orgs[0],
			p.sub,
		])
		await seen
		grants.push(performance.now() - start)
		off()
	}
	await ctx.closeAll()
	return {
		name: adapter.name,
		loadMs,
		loadBytes,
		p50: pct(lat, 50),
		p99: pct(lat, 99),
		bytesPerChange,
		multiP50: pct(multi, 50),
		insertP50: pct(inserts, 50),
		grantP50: pct(grants, 50),
	}
}

const db = new Db()
const results: Result[] = []
for (const name of values.adapter!.split(',')) {
	let adapter: Adapter
	if (name === 'electric') adapter = new ElectricAdapter()
	else {
		buildEngine()
		adapter = new LocalsyncAdapter()
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
		`# Benchmarks (${new Date().toISOString().slice(0, 10)})`,
		'',
		'Generated by `ts/harness/src/bench.ts`: one principal syncing two subquery',
		`rules (tasks via project via member, comments one level deeper) over ${rows}`,
		'tasks with one comment each, on the reference client (in-memory',
		'sqlite-wasm in Node) against the compose Postgres 18. Latency is from the',
		'write committing to the local commit that shows it. Bytes are payload',
		'bytes after decompression (Electric) / as received (localsync, which is',
		'not compressed yet). Single machine, not a load test.',
		'',
		'| Engine | Initial load | Load bytes | Update p50 | Update p99 | Bytes / update | 2-table txn p50 | Insert p50 | Grant (20 rows) p50 |',
		'| --- | --- | --- | --- | --- | --- | --- | --- | --- |',
		...results.map(
			(r) =>
				`| ${r.name} | ${f(r.loadMs)} ms | ${r.loadBytes.toLocaleString('en')} | ${f(r.p50)} ms | ${f(r.p99)} ms | ${r.bytesPerChange} | ${f(r.multiP50)} ms | ${f(r.insertP50)} ms | ${f(r.grantP50)} ms |`
		),
		'',
	]
	const dir = path.resolve(import.meta.dirname, '../../../docs/bench')
	mkdirSync(dir, { recursive: true })
	writeFileSync(path.join(dir, 'README.md'), lines.join('\n'))
}
