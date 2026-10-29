// Dense-data benchmarks: large jsonb / TOAST rows and a wide table, for
// each adapter. Measures initial load, bytes and latency of updates that
// touch a small column vs the jsonb document, WAL growth under
// REPLICA IDENTITY FULL vs DEFAULT, and engine memory. Writes
// docs/bench/dense.md with --write.
//
//   pnpm --filter @nightride/localsync-harness bench:dense [--docs=5000] [--wide=50000] [--write]
import { randomUUID } from 'node:crypto'
import { readFileSync, writeFileSync } from 'node:fs'
import path from 'node:path'
import { parseArgs } from 'node:util'

import type { Adapter } from './adapter.ts'
import { introspect } from './catalog.ts'
import { ElectricAdapter } from './electric.ts'
import { ensureFixture, resetData } from './fixture.ts'
import { buildEngine, LocalsyncAdapter } from './localsync.ts'
import { Db } from './pg.ts'
import { Ctx } from './scenario.ts'

const { values } = parseArgs({
	options: {
		adapter: { type: 'string', default: 'localsync,electric' },
		docs: { type: 'string', default: '5000' },
		wide: { type: 'string', default: '50000' },
		write: { type: 'boolean', default: false },
	},
})
const nDocs = Number(values.docs)
const nWide = Number(values.wide)
const run = randomUUID().slice(0, 6)
const DOC = `dense_doc_${run}`
const WIDE = `dense_wide_${run}`

const pct = (xs: number[], p: number) =>
	[...xs].sort((a, b) => a - b)[Math.min(xs.length - 1, Math.floor((p / 100) * xs.length))]!

function rssMB(pid: number | undefined): number | null {
	if (!pid) return null
	try {
		const m = /VmRSS:\s+(\d+)/.exec(readFileSync(`/proc/${pid}/status`, 'utf8'))
		return m ? Math.round(Number(m[1]) / 1024) : null
	} catch {
		return null
	}
}

async function createTables(db: Db, owner: string): Promise<void> {
	const wideCols: string[] = []
	for (let i = 0; i < 20; i++) wideCols.push(`i${i} int`, `t${i} text`, `n${i} numeric(12,2)`)
	await db.query(`
		CREATE TABLE ${DOC} (id uuid PRIMARY KEY, owner text NOT NULL, counter int NOT NULL DEFAULT 0,
			updated_at timestamptz NOT NULL DEFAULT now(), doc jsonb NOT NULL, body text NOT NULL, tags int[] NOT NULL);
		CREATE TABLE ${WIDE} (id uuid PRIMARY KEY, owner text NOT NULL, ${wideCols.join(', ')});
	`)
	// ~2 kB jsonb document (nested objects, arrays, strings, numbers) and
	// ~10 kB of text that Postgres TOASTs.
	await db.query(
		`INSERT INTO ${DOC} (id, owner, doc, body, tags)
		 SELECT gen_random_uuid(), $1,
			jsonb_build_object('title', 'doc ' || g, 'n', g, 'flags', jsonb_build_object('a', g % 2 = 0, 'b', g % 3 = 0),
				'items', (SELECT jsonb_agg(jsonb_build_object('k', k, 'v', md5((g * k)::text), 'w', k * 1.5)) FROM generate_series(1, 25) k)),
			repeat(md5(g::text), 320), ARRAY[g, g + 1, g + 2]
		 FROM generate_series(1, $2) g`,
		[owner, nDocs]
	)
	const sel: string[] = []
	for (let i = 0; i < 20; i++) sel.push(`g + ${i}`, `md5((g + ${i})::text)`, `(g + ${i}) * 1.25`)
	await db.query(
		`INSERT INTO ${WIDE} SELECT gen_random_uuid(), $1, ${sel.join(', ')} FROM generate_series(1, $2) g`,
		[owner, nWide]
	)
}

/** WAL bytes written by `count` single-row updates under each replica identity. */
async function walPerUpdate(db: Db): Promise<{ full: number; def: number }> {
	const ids = (await db.query(`SELECT id FROM ${DOC} LIMIT 200`)).map((r) => r.id!)
	const measure = async () => {
		const before = await db.lsn()
		for (const id of ids)
			await db.query(`UPDATE ${DOC} SET counter = counter + 1 WHERE id = $1`, [id])
		return Number((await db.lsn()) - before) / ids.length
	}
	await db.query(`ALTER TABLE ${DOC} REPLICA IDENTITY DEFAULT`)
	const def = await measure()
	await db.query(`ALTER TABLE ${DOC} REPLICA IDENTITY FULL`)
	const full = await measure()
	return { full: Math.round(full), def: Math.round(def) }
}

type Result = {
	name: string
	docLoadMs: number
	docLoadMB: number
	wideLoadMs: number
	wideLoadMB: number
	smallUpdateP50: number
	smallUpdateBytes: number
	jsonUpdateP50: number
	jsonUpdateBytes: number
	engineRssMB: number | null
}

async function bench(adapter: Adapter, db: Db): Promise<Result> {
	await resetData(db)
	const ctx = new Ctx(db, adapter, await introspect(db))
	const p = ctx.principal()
	await createTables(db, p.sub)
	await ctx.refreshCatalog()
	ctx.addRule({ name: 'docs', sql: `SELECT * FROM ${DOC} WHERE owner = $sub` })
	ctx.addRule({ name: 'wide', sql: `SELECT * FROM ${WIDE} WHERE owner = $sub` })

	let t0 = Date.now()
	const c = await ctx.connect(p, ['docs'])
	await ctx.converge(c, 600_000)
	const docLoadMs = Date.now() - t0
	const docLoadMB = c.client.bytes() / 1e6

	const replica = c.client.replica.db
	const ids = (await db.query(`SELECT id FROM ${DOC} LIMIT 100`)).map((r) => r.id!)
	const measure = async (sql: string, check: (id: string, i: number) => boolean) => {
		const lat: number[] = []
		const b0 = c.client.bytes()
		for (const [i, id] of ids.entries()) {
			let resolve!: () => void
			const seen = new Promise<void>((r) => (resolve = r))
			const off = c.client.onCommit(() => {
				if (check(id, i)) resolve()
			})
			const start = performance.now()
			await db.query(sql, [id, i])
			await seen
			lat.push(performance.now() - start)
			off()
		}
		return { p50: pct(lat, 50), bytes: Math.round((c.client.bytes() - b0) / ids.length) }
	}
	const small = await measure(
		`UPDATE ${DOC} SET counter = 1000 + $2 WHERE id = $1`,
		(id, i) =>
			Number(replica.selectValue(`SELECT counter FROM ${DOC} WHERE id = ?`, [id])) === 1000 + i
	)
	const json = await measure(
		`UPDATE ${DOC} SET doc = jsonb_set(doc, '{title}', to_jsonb('edited ' || $2::text)) WHERE id = $1`,
		(id, i) =>
			String(replica.selectValue(`SELECT doc FROM ${DOC} WHERE id = ?`, [id])).includes(
				`"edited ${i}"`
			)
	)

	t0 = Date.now()
	const bw = c.client.bytes()
	await c.want([{ rule: 'docs' }, { rule: 'wide' }])
	await ctx.converge(c, 600_000)
	const wideLoadMs = Date.now() - t0
	const wideLoadMB = (c.client.bytes() - bw) / 1e6
	const rss = adapter instanceof LocalsyncAdapter ? rssMB(adapter.enginePid()) : null
	// BENCH_HEAP=<file>: save the engine's heap profile at the peak, for pprof.
	if (adapter instanceof LocalsyncAdapter && process.env.BENCH_HEAP) {
		const port = Number(process.env.LOCALSYNC_ENGINE_PORT ?? 3141)
		const res = await fetch(`http://127.0.0.1:${port}/debug/pprof/heap`)
		writeFileSync(process.env.BENCH_HEAP, Buffer.from(await res.arrayBuffer()))
	}
	await ctx.closeAll()
	return {
		name: adapter.name,
		docLoadMs,
		docLoadMB,
		wideLoadMs,
		wideLoadMB,
		smallUpdateP50: small.p50,
		smallUpdateBytes: small.bytes,
		jsonUpdateP50: json.p50,
		jsonUpdateBytes: json.bytes,
		engineRssMB: rss,
	}
}

const db = new Db()
const results: Result[] = []
let wal: { full: number; def: number } | null = null
try {
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
		wal ??= await walPerUpdate(db)
		await db.query(`DROP TABLE IF EXISTS ${DOC}, ${WIDE}`)
	}
} finally {
	await db.query(`DROP TABLE IF EXISTS ${DOC}, ${WIDE}`).catch(() => {})
	await db.close()
}
console.log(JSON.stringify({ walBytesPerSmallUpdate: wal }))

if (values.write) {
	const f = (n: number) => (n < 10 ? n.toFixed(1) : String(Math.round(n)))
	const lines = [
		`# Dense-data benchmarks (${new Date().toISOString().slice(0, 10)})`,
		'',
		'Generated by `ts/harness/src/bench-dense.ts`. Same reference client and',
		`compose stack as \`README.md\`. Documents: ${nDocs} rows, each a ~2 kB jsonb`,
		'document, ~10 kB of TOASTed text, an int array, a counter and a',
		`timestamp. Wide: ${nWide} rows of 60 columns (20 each of int, text, numeric).`,
		'Updates: 100 single-row updates of the `counter` int, then 100',
		'`jsonb_set` edits of the document.',
		'',
		'| Engine | Docs load | Docs MB | Wide load | Wide MB | Int update p50 | Bytes / int update | jsonb update p50 | Bytes / jsonb update | Engine RSS |',
		'| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |',
		...results.map(
			(r) =>
				`| ${r.name} | ${f(r.docLoadMs)} ms | ${r.docLoadMB.toFixed(1)} | ${f(r.wideLoadMs)} ms | ${r.wideLoadMB.toFixed(1)} | ${f(r.smallUpdateP50)} ms | ${r.smallUpdateBytes.toLocaleString('en')} | ${f(r.jsonUpdateP50)} ms | ${r.jsonUpdateBytes.toLocaleString('en')} | ${r.engineRssMB ?? '-'} MB |`
		),
		'',
		`WAL bytes per single-row \`counter\` update on the document table: ${wal?.def} with REPLICA IDENTITY DEFAULT, ${wal?.full} with FULL.`,
		'',
	]
	writeFileSync(path.resolve(import.meta.dirname, '../../../docs/bench/dense.md'), lines.join('\n'))
}
