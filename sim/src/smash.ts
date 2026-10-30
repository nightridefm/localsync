// Smash simulation (docs/design.md 14.2): a seeded random workload over the
// harness fixture with several principals, membership churn, want swaps,
// disconnects and engine kills, checking invariants continuously and the
// oracle at every checkpoint. Replay a failure with the printed --seed.
//
//   pnpm --filter @nightride/localsync-sim smash [--seed N] [--steps N] [--clients N] [--faults]
import { randomUUID } from 'node:crypto'
import { mkdirSync, writeFileSync } from 'node:fs'
import path from 'node:path'
import { parseArgs } from 'node:util'

import type { Adapter } from '@nightride/localsync-harness/adapter.ts'
import { introspect } from '@nightride/localsync-harness/catalog.ts'
import { ElectricAdapter } from '@nightride/localsync-harness/electric.ts'
import { ensureFixture, resetData, seedWorld } from '@nightride/localsync-harness/fixture.ts'
import { buildEngine, LocalsyncAdapter } from '@nightride/localsync-harness/localsync.ts'
import { Db } from '@nightride/localsync-harness/pg.ts'
import { type Conn, Ctx, Fail } from '@nightride/localsync-harness/scenario.ts'

const { values } = parseArgs({
	options: {
		seed: { type: 'string' },
		steps: { type: 'string', default: '400' },
		clients: { type: 'string', default: '6' },
		adapter: { type: 'string', default: 'localsync' },
		faults: { type: 'boolean', default: false },
		checkpoint: { type: 'string', default: '50' },
	},
})

const seed = values.seed ? Number(values.seed) : Math.floor(Math.random() * 2 ** 31)
const steps = Number(values.steps)
const nClients = Number(values.clients)
const checkpointEvery = Number(values.checkpoint)

// mulberry32: small, fast, seedable.
let state = seed >>> 0
function rand(): number {
	state = (state + 0x6d2b79f5) >>> 0
	let t = state
	t = Math.imul(t ^ (t >>> 15), t | 1)
	t ^= t + Math.imul(t ^ (t >>> 7), t | 61)
	return ((t ^ (t >>> 14)) >>> 0) / 4294967296
}
const pick = <T>(xs: readonly T[]): T => xs[Math.floor(rand() * xs.length)]!
const chance = (p: number) => rand() < p

const log: string[] = []
const note = (s: string) => log.push(s)
const short = (id: string) => id.slice(0, 8)

const db = new Db()
let adapter: Adapter
if (values.adapter === 'electric') adapter = new ElectricAdapter()
else {
	if (!process.env.LOCALSYNC_SKIP_BUILD) buildEngine()
	adapter = new LocalsyncAdapter()
}
if ((await ensureFixture(db)) && adapter.schemaReset) await adapter.schemaReset(db)
await resetData(db)
const ctx = new Ctx(db, adapter, await introspect(db))

type Sim = { conn: Conn; torn: number; commits: number; online: boolean; project: string | null }
const principals = Array.from({ length: nClients }, (_, i) => ctx.principal(`p${i}`))
let world!: Awaited<ReturnType<typeof seedWorld>>
await ctx.write(async (q) => {
	// Orgs 0..3; each principal starts in one or two of them.
	world = await seedWorld(q, {
		orgs: 4,
		projects: 3,
		tasks: 40,
		comments: 1,
		memberOf: [0, 1, 2, 3].map((o) =>
			principals.filter((_, i) => i % 4 === o || (i + o) % 5 === 0).map((p) => p.sub)
		),
	})
})
const projectOrg = new Map<string, string>()
for (const [i, p] of world.projects.entries()) projectOrg.set(p, world.orgs[Math.floor(i / 3)]!)
const pairs: Array<{ task: string; comment: string }> = []
for (const [p, org] of projectOrg) note(`project ${short(p)} in org ${short(org)}`)

function wantsFor(i: number, project: string | null) {
	const base: Array<string | { rule: string; params: Record<string, string> }> = [
		'projects',
		'tasks',
		'comments',
		'members',
	]
	if (project) {
		base.push({ rule: 'project_comments', params: { project } })
		// Shared bodies with per-principal gates (several sims may pick the
		// same project; grants and revokes flip their gates).
		base.push({ rule: 'shared_tasks', params: { project } })
		base.push({ rule: 'shared_comments', params: { project } })
	}
	return i % 2 === 0 ? base : base.filter((w) => w !== 'members')
}

const sims: Sim[] = []
for (const [i, p] of principals.entries()) {
	const project = i % 3 === 0 ? pick(world.projects) : null
	const conn = await ctx.connect(p, wantsFor(i, project))
	await conn.trackVisibility()
	const sim: Sim = { conn, torn: 0, commits: 0, online: true, project }
	note(`p${i} project ${project ? short(project) : '-'}`)
	conn.client.onCommit(() => {
		sim.commits++
		const r = conn.client.replica.db
		const bad = Number(
			r.selectValue(`SELECT
				(SELECT count(*) FROM task t JOIN comment c ON c.task_id = t.id
				 WHERE t.txn_tag LIKE 'pair:%' AND c.txn_tag LIKE 'pair:%' AND c.txn_tag IS NOT t.txn_tag)`)
		)
		if (bad) sim.torn++
	})
	sims.push(sim)
}

async function checkpoint(label: string) {
	for (const [i, s] of sims.entries()) {
		if (!s.online) continue
		try {
			await ctx.converge(s.conn, 60_000)
		} catch (err) {
			throw new Fail(
				`${label}: client p${i} (sub ${s.conn.principal.sub}): ${(err as Error).message}`
			)
		}
		const errors = s.conn.client.errors()
		const d = errors.findIndex((e) => e.includes('drift'))
		if (d >= 0)
			throw new Fail(
				`${label}: client p${i}: ${errors[d]}; errors before it: ${JSON.stringify(errors.slice(Math.max(0, d - 5), d))}`
			)
		if (s.torn) throw new Fail(`${label}: client p${i} saw ${s.torn} torn transactions`)
		s.conn.verifyNoLeaks()
	}
}

const ops: Array<[number, string, () => Promise<void>]> = [
	[
		20,
		'insert pair',
		async () => {
			const project = pick(world.projects)
			const task = randomUUID()
			const comment = randomUUID()
			const tag = `pair:${randomUUID()}`
			await ctx.write(async (q) => {
				await q(`INSERT INTO task (id, project_id, title, txn_tag) VALUES ($1, $2, 'pair', $3)`, [
					task,
					project,
					tag,
				])
				await q(`INSERT INTO comment VALUES ($1, $2, 'pair', $3)`, [comment, task, tag])
			})
			pairs.push({ task, comment })
			note(`  project ${short(project)} task ${short(task)}`)
		},
	],
	[
		25,
		'retag pair',
		async () => {
			if (!pairs.length) return
			const p = pick(pairs)
			const tag = `pair:${randomUUID()}`
			await ctx.write(async (q) => {
				await q('UPDATE task SET txn_tag = $2 WHERE id = $1', [p.task, tag])
				await q('UPDATE comment SET txn_tag = $2 WHERE id = $1', [p.comment, tag])
			})
		},
	],
	[
		15,
		'update task',
		async () => {
			await ctx.write(`UPDATE task SET title = 'u ' || $2, done = NOT done WHERE id = $1`, [
				pick(world.tasks),
				String(rand()),
			])
		},
	],
	[
		5,
		'delete task',
		async () => {
			const t = pick(world.tasks)
			await ctx.write(async (q) => {
				await q('DELETE FROM comment WHERE task_id = $1', [t])
				await q('DELETE FROM task WHERE id = $1', [t])
			})
		},
	],
	[
		8,
		'grant membership',
		async () => {
			await ctx.write(`INSERT INTO member VALUES (gen_random_uuid(), $1, $2, 'member')`, [
				pick(world.orgs),
				pick(principals).sub,
			])
		},
	],
	[
		8,
		'revoke membership',
		async () => {
			const p = pick(principals)
			let org = '-'
			await ctx.write(async (q) => {
				const [r] = await q(
					`DELETE FROM member WHERE id = (SELECT id FROM member WHERE user_id = $1 ORDER BY random() LIMIT 1) RETURNING org_id`,
					[p.sub]
				)
				if (r) org = short(String(r.org_id))
			})
			note(`  p${principals.indexOf(p)} org ${org}`)
		},
	],
	[
		4,
		'move project',
		async () => {
			const p = pick(world.projects)
			const org = pick(world.orgs)
			projectOrg.set(p, org)
			await ctx.write('UPDATE project SET org_id = $2 WHERE id = $1', [p, org])
		},
	],
	[
		1,
		'bulk insert',
		async () => {
			await ctx.write(
				`INSERT INTO task (id, project_id, title) SELECT gen_random_uuid(), $1, 'bulk ' || g FROM generate_series(1, 2000) g`,
				[pick(world.projects)]
			)
		},
	],
	[
		4,
		'swap want',
		async () => {
			const i = Math.floor(rand() * sims.length)
			const s = sims[i]!
			if (!s.online) return
			s.project = chance(0.2) ? null : pick(world.projects)
			note(`  p${i} project ${s.project ? short(s.project) : '-'}`)
			await s.conn.want(
				wantsFor(i, s.project).map((w) => (typeof w === 'string' ? { rule: w } : w))
			)
		},
	],
	[
		3,
		'disconnect / reconnect',
		async () => {
			const s = pick(sims)
			note(`  p${sims.indexOf(s)} ${s.online ? 'offline' : 'online'}`)
			if (s.online) await s.conn.client.disconnect()
			else await s.conn.client.reconnect()
			s.online = !s.online
		},
	],
]
if (values.faults && adapter.restartServer) {
	ops.push([1, 'kill engine', async () => adapter.restartServer!()])
}
if (values.faults && adapter.replicationSlots) {
	const slots = adapter.replicationSlots
	ops.push([
		1,
		'kill walsender',
		async () => {
			await db.query(
				`SELECT pg_terminate_backend(active_pid) FROM pg_replication_slots
				 WHERE slot_name LIKE $1 AND database = current_database() AND active_pid IS NOT NULL`,
				[slots]
			)
		},
	])
}
const total = ops.reduce((n, o) => n + o[0], 0)

console.log(
	`smash seed=${seed} steps=${steps} clients=${nClients} adapter=${adapter.name}${values.faults ? ' faults' : ''}`
)
let failure: string | null = null
const start = Date.now()
try {
	for (let step = 1; step <= steps; step++) {
		let r = rand() * total
		const op = ops.find((o) => (r -= o[0]) < 0)!
		note(`${step} ${op[1]}`)
		await op[2]()
		if (step % checkpointEvery === 0 || step === steps) {
			for (const s of sims) {
				if (!s.online) {
					note(`  checkpoint: p${sims.indexOf(s)} online`)
					await s.conn.client.reconnect()
					s.online = true
				}
			}
			await checkpoint(`step ${step}`)
			console.log(`  step ${step}: ok (${sims.map((s) => s.commits).join('/')} local commits)`)
		}
	}
} catch (err) {
	failure = err instanceof Error ? err.message : String(err)
}
await ctx.closeAll()
await adapter.close()
await db.close()
const dir = path.resolve(import.meta.dirname, '../runs')
mkdirSync(dir, { recursive: true })
const file = path.join(dir, `smash-${seed}.log`)
writeFileSync(file, `${log.join('\n')}\n`)
if (failure) {
	console.log(`FAIL seed=${seed} after ${log.length} steps (ops: ${file})\n${failure}`)
	process.exit(1)
}
console.log(`PASS seed=${seed} ${steps} steps in ${Math.round((Date.now() - start) / 1000)}s`)
