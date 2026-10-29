// Browser smoke: real localdb (OPFS, reader/writer workers, write fence) in
// headless Chrome, syncing through the localsync transport from the Go
// engine. Asserts initial load, live transactional updates as seen by
// localdb's reactive queries (never a torn pair), and a want change sent
// with LocalDb.syncCommand.
//
//   pnpm --filter @nightride/localsync-browser smoke
import { randomUUID } from 'node:crypto'

import { ensureFixture, resetData, seedWorld } from '@nightride/localsync-harness/fixture.ts'
import { mintToken } from '@nightride/localsync-harness/localsync.ts'
import { Db } from '@nightride/localsync-harness/pg.ts'

import { cdpPage, ENGINE_PORT, HTTP_PORT, shutdown, sleep, startStack, waitFor } from './infra.ts'

type Ev = [string, ...unknown[]]

async function main(): Promise<void> {
	const db = new Db()
	await ensureFixture(db)
	await resetData(db)
	const sub = randomUUID()
	let world!: Awaited<ReturnType<typeof seedWorld>>
	await db.tx(async (q) => {
		world = await seedWorld(q, { orgs: 1, projects: 1, tasks: 200, comments: 1, memberOf: [[sub]] })
	})

	await startStack(['tasks', 'comments'])

	// Two tabs on the same writes, each its own database: one with
	// LocalDbConfig.alignCommits, one without.
	const open = async (align: boolean) => {
		const qs = new URLSearchParams({
			run: randomUUID().slice(0, 8),
			ws: `ws://127.0.0.1:${ENGINE_PORT}/sync`,
			token: mintToken(sub),
			align: align ? '1' : '0',
		})
		const page = await cdpPage(`http://127.0.0.1:${HTTP_PORT}/?${qs}`)
		const events = async (): Promise<Ev[]> => {
			const raw = (await page.evaluate(
				'JSON.stringify({e: window.__events || [], err: window.__error})'
			)) as string | undefined
			const v = JSON.parse(raw ?? '{"e":[]}') as { e: Ev[]; err: string | null }
			if (v.err) throw new Error(`page error: ${v.err}`)
			return v.e
		}
		return { page, events }
	}
	const last = (ev: Ev[], name: string) => ev.filter((e) => e[0] === name).at(-1)?.[1]
	const aligned = await open(true)
	const plain = await open(false)
	const { page, events } = aligned

	const t0 = Date.now()
	for (const tab of [aligned, plain])
		await waitFor(
			async () =>
				last(await tab.events(), 'tasks') === 200 && last(await tab.events(), 'comments') === 200,
			'initial load (200 tasks, 200 comments)',
			90_000
		).catch(async (err: unknown) => {
			const tail = (await tab.events().catch(() => [])).slice(-12)
			throw new Error(
				`${String(err)}; last page events: ${JSON.stringify(tail)}; console errors: ${tab.page.consoleErrors.slice(-5).join(' | ')}`
			)
		})
	console.log(
		`ok   initial load in a real browser (two tabs): 200 tasks + 200 comments in ${Date.now() - t0}ms (incl. wasm boot)`
	)

	const pairs: Array<{ task: string; comment: string }> = []
	for (let i = 0; i < 30; i++) {
		const task = randomUUID()
		const comment = randomUUID()
		const tag = `pair:${randomUUID()}`
		await db.tx(async (q) => {
			await q(`INSERT INTO task (id, project_id, title, txn_tag) VALUES ($1, $2, 'pair', $3)`, [
				task,
				world.projects[0],
				tag,
			])
			await q(`INSERT INTO comment VALUES ($1, $2, 'pair', $3)`, [comment, task, tag])
		})
		pairs.push({ task, comment })
	}
	for (let i = 0; i < 90; i++) {
		const p = pairs[i % pairs.length]!
		const tag = `pair:${randomUUID()}`
		await db.tx(async (q) => {
			await q('UPDATE task SET txn_tag = $2 WHERE id = $1', [p.task, tag])
			await q('UPDATE comment SET txn_tag = $2 WHERE id = $1', [p.comment, tag])
		})
		// Paced like live edits: separate commits, not one frame.
		await sleep(15)
	}
	for (const tab of [aligned, plain])
		await waitFor(
			async () =>
				last(await tab.events(), 'tasks') === 230 && last(await tab.events(), 'comments') === 230,
			'live inserts (230 / 230)'
		)
	const xtorn: number[] = []
	for (const tab of [aligned, plain]) {
		const ev = await tab.events()
		const torn = ev.filter((e) => e[0] === 'torn' && e[1] !== 0)
		const renders = ev.filter((e) => e[0] === 'torn').length
		if (torn.length) throw new Error(`a reactive query rendered ${torn.length} torn states`)
		if (tab === aligned)
			console.log(
				`ok   120 two-table transactions: ${renders} reactive deliveries of the torn-pair query, all 0`
			)
		xtorn.push(ev.filter((e) => e[0] === 'xtorn' && e[1] !== 0).length)
	}
	// Two separate live queries (task tags, comment tags) on a reader pool:
	// with alignCommits they must never render from different commits.
	if (xtorn[0])
		throw new Error(`alignCommits: ${xtorn[0]} renders mixed two commits across queries`)
	console.log(
		`ok   alignCommits: two live queries never rendered from different commits (without it: ${xtorn[1]} torn renders)`
	)

	await page.evaluate(`window.__db.syncCommand({ want: [{ rule: 'tasks' }] })`)
	await waitFor(
		async () => last(await events(), 'comments') === 0 && last(await events(), 'tasks') === 230,
		'want change (comments dropped)'
	)
	console.log('ok   LocalDb.syncCommand want change: comments unit dropped, tasks kept')
	page.close()
	plain.page.close()
	await db.close()
}

let code = 0
try {
	await main()
	console.log('browser smoke OK')
} catch (err) {
	console.error(`FAIL ${err instanceof Error ? err.message : String(err)}`)
	code = 1
} finally {
	shutdown()
}
process.exit(code)
