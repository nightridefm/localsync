// Collab soak: two people working at once in a data- and update-heavy app,
// each in a real browser tab running localdb (OPFS, reader pool) synced by
// the engine. Alice edits one project, Bob another; both see both. Writes
// keep a task and its two comments on one tag, so every render the page
// checks (collab.html) is a test of "one change on screen" across queries
// on different readers. Asserts: tabs with LocalDbConfig.alignCommits never
// render a torn turn, and every tab converges. Reports render latency.
//
//   pnpm --filter @nightride/localsync-browser collab [--seconds 60] [--rate 50]
import { randomUUID } from 'node:crypto'
import { parseArgs } from 'node:util'

import { ensureFixture, resetData, seedWorld } from '@nightride/localsync-harness/fixture.ts'
import { mintToken } from '@nightride/localsync-harness/localsync.ts'
import { Db, type Query } from '@nightride/localsync-harness/pg.ts'

import { cdpPage, ENGINE_PORT, HTTP_PORT, shutdown, sleep, startStack, waitFor } from './infra.ts'

const { values } = parseArgs({
	options: {
		seconds: { type: 'string', default: '60' },
		// Transactions per second per person.
		rate: { type: 'string', default: '50' },
	},
})
const seconds = Number(values.seconds)
const pause = 1000 / Number(values.rate)

type Stats = {
	turns: number
	torn: number
	tornExamples: string[]
	latencies: number[]
	counts: { tasks: number; comments: number }
	error: string | null
}

const pct = (xs: number[], p: number) => {
	const s = [...xs].sort((a, b) => a - b)
	return s.length ? s[Math.min(s.length - 1, Math.floor((p / 100) * s.length))]! : 0
}

async function writer(db: Db, user: string, project: string, until: number): Promise<number> {
	const mine: string[] = []
	let n = 0
	const tag = () => `w:${user}:${++n}:${Date.now()}`
	const insert = async (q: Query, t: string) => {
		const id = randomUUID()
		await q(`INSERT INTO task (id, project_id, title, txn_tag) VALUES ($1, $2, $3, $4)`, [
			id,
			project,
			`${user} ${n}`,
			t,
		])
		await q(
			`INSERT INTO comment VALUES (gen_random_uuid(), $1, 'a', $2), (gen_random_uuid(), $1, 'b', $2)`,
			[id, t]
		)
		mine.push(id)
	}
	const retag = async (q: Query, ids: string[], t: string) => {
		await q('UPDATE task SET txn_tag = $2 WHERE id = ANY($1)', [ids, t])
		await q('UPDATE comment SET txn_tag = $2 WHERE task_id = ANY($1)', [ids, t])
	}
	while (Date.now() < until) {
		const r = Math.random()
		await db.tx(async (q) => {
			if (mine.length < 20 || r < 0.3) await insert(q, tag())
			else if (r < 0.4) {
				// A bulk edit: twenty of my tasks in one transaction.
				const ids = [...mine].sort(() => Math.random() - 0.5).slice(0, 20)
				await retag(q, ids, tag())
			} else if (r < 0.55) {
				const id = mine.splice(Math.floor(Math.random() * mine.length), 1)[0]!
				await q('DELETE FROM comment WHERE task_id = $1', [id])
				await q('DELETE FROM task WHERE id = $1', [id])
			} else {
				await retag(q, [mine[Math.floor(Math.random() * mine.length)]!], tag())
			}
		})
		await sleep(pause)
	}
	return n
}

async function main(): Promise<void> {
	const db = new Db()
	await ensureFixture(db)
	await resetData(db)
	const alice = randomUUID()
	const bob = randomUUID()
	let world!: Awaited<ReturnType<typeof seedWorld>>
	await db.tx(async (q) => {
		// A sizeable workspace: 2 projects x 1000 tasks, 2 comments each.
		world = await seedWorld(q, {
			orgs: 1,
			projects: 2,
			tasks: 1000,
			comments: 2,
			memberOf: [[alice, bob]],
		})
	})
	await startStack(['tasks', 'comments'])

	const tabs = [
		{ name: 'alice (aligned)', sub: alice, align: true },
		{ name: 'bob (aligned)', sub: bob, align: true },
		{ name: 'alice (not aligned)', sub: alice, align: false },
	]
	const pages = []
	for (const t of tabs) {
		const qs = new URLSearchParams({
			run: randomUUID().slice(0, 8),
			ws: `ws://127.0.0.1:${ENGINE_PORT}/sync`,
			token: mintToken(t.sub, { expSeconds: 3600 }),
			align: t.align ? '1' : '0',
			projects: world.projects.join(','),
		})
		pages.push(await cdpPage(`http://127.0.0.1:${HTTP_PORT}/collab.html?${qs}`))
	}
	const stats = async (i: number): Promise<Stats> => {
		const raw = (await pages[i]!.evaluate('JSON.stringify(window.__collab || null)')) as string
		const s = JSON.parse(raw ?? 'null') as Stats | null
		if (!s)
			return {
				turns: 0,
				torn: 0,
				tornExamples: [],
				latencies: [],
				counts: { tasks: -1, comments: -1 },
				error: null,
			}
		if (s.error) throw new Error(`${tabs[i]!.name}: ${s.error}`)
		return s
	}
	const truth = async () => {
		const [r] = await db.query(
			'SELECT (SELECT count(*) FROM task)::int AS tasks, (SELECT count(*) FROM comment)::int AS comments'
		)
		return { tasks: Number(r!.tasks), comments: Number(r!.comments) }
	}
	const converged = async () => {
		const want = await truth()
		for (let i = 0; i < tabs.length; i++) {
			const c = (await stats(i)).counts
			if (c.tasks !== want.tasks || c.comments !== want.comments) return false
		}
		return true
	}
	const t0 = Date.now()
	await waitFor(converged, 'initial load in every tab', 120_000)
	console.log(
		`ok   initial load in 3 tabs: ${(await truth()).tasks} tasks, ${(await truth()).comments} comments in ${Date.now() - t0}ms`
	)

	const until = Date.now() + seconds * 1000
	const [na, nb] = await Promise.all([
		writer(db, 'alice', world.projects[0]!, until),
		writer(db, 'bob', world.projects[1]!, until),
	])
	await waitFor(converged, 'every tab converged after the writes', 60_000)
	console.log(`ok   ${na + nb} transactions in ${seconds}s by two people; every tab converged`)

	let failed = ''
	for (let i = 0; i < tabs.length; i++) {
		const s = await stats(i)
		console.log(
			`     ${tabs[i]!.name.padEnd(20)} ${s.turns} render turns, ${s.torn} torn; latency p50 ${pct(s.latencies, 50)}ms p99 ${pct(s.latencies, 99)}ms max ${Math.max(0, ...s.latencies)}ms`
		)
		if (tabs[i]!.align && s.torn)
			failed += `${tabs[i]!.name}: ${s.torn} torn turns (${s.tornExamples.join('; ')}). `
	}
	if (failed) throw new Error(failed)
	console.log('ok   aligned tabs never rendered a task and its comments from different commits')
	for (const p of pages) p.close()
	await db.close()
}

let code = 0
try {
	await main()
	console.log('collab soak OK')
} catch (err) {
	console.error(`FAIL ${err instanceof Error ? err.message : String(err)}`)
	code = 1
} finally {
	shutdown()
}
process.exit(code)
