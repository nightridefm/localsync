// The capability catalogue (docs/design.md section 14.1), one
// scenario per capability. Each returns a short note for the matrix
// (timings, bytes) or throws Fail / Unsupported.
import { randomUUID } from 'node:crypto'

import { Unsupported } from '../adapter.ts'
import { seedSink, seedWorld } from '../fixture.ts'
import { Ctx, Fail, type Scenario } from '../scenario.ts'
import type { Query } from '../pg.ts'

const ALL = ['members', 'projects', 'tasks', 'comments', 'project_tags', 'tags', 'sink']

async function tags(q: Query, projects: string[]): Promise<void> {
	const ids = [randomUUID(), randomUUID(), randomUUID()]
	for (const [i, id] of ids.entries()) await q('INSERT INTO tag VALUES ($1, $2)', [id, `tag ${i}`])
	for (const p of projects)
		await q('INSERT INTO project_tag VALUES ($1, $2, $3)', [randomUUID(), p, ids[0]])
}

/** Inserts a task+comment pair in one transaction, tagged identically. */
async function insertPair(ctx: Ctx, project: string): Promise<{ task: string; comment: string }> {
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
	return { task, comment }
}

async function retagPair(ctx: Ctx, pair: { task: string; comment: string }): Promise<void> {
	const tag = `pair:${randomUUID()}`
	await ctx.write(async (q) => {
		await q('UPDATE task SET txn_tag = $2 WHERE id = $1', [pair.task, tag])
		await q('UPDATE comment SET txn_tag = $2 WHERE id = $1', [pair.comment, tag])
	})
}

/** Counts replica states where a task/comment pair disagrees (torn transaction). */
function pairViolations(conn: {
	client: { replica: { db: { selectValue(sql: string): unknown } } }
}): number {
	const db = conn.client.replica.db
	return Number(
		db.selectValue(`SELECT
			(SELECT count(*) FROM task t LEFT JOIN comment c ON c.task_id = t.id AND c.txn_tag LIKE 'pair:%'
			 WHERE t.txn_tag LIKE 'pair:%' AND c.txn_tag IS NOT t.txn_tag) +
			(SELECT count(*) FROM comment c LEFT JOIN task t ON t.id = c.task_id
			 WHERE c.txn_tag LIKE 'pair:%' AND t.txn_tag IS NOT c.txn_tag)`)
	)
}

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms))

export const catalogue: Scenario[] = [
	{
		id: '1',
		title: 'initial snapshot == oracle, all types',
		async run(ctx) {
			const p = ctx.principal()
			await ctx.write(async (q) => {
				const w = await seedWorld(q, {
					orgs: 2,
					projects: 2,
					tasks: 20,
					comments: 2,
					memberOf: [[p.sub], []],
				})
				await tags(q, w.projects)
				await seedSink(q, p.sub)
				await seedSink(q, 'someone-else')
			})
			const c = await ctx.connect(p, ALL)
			return `${await ctx.converge(c)}ms`
		},
	},
	{
		id: '2',
		title: 'live insert / update / delete',
		async run(ctx) {
			const p = ctx.principal()
			let w!: Awaited<ReturnType<typeof seedWorld>>
			await ctx.write(async (q) => {
				w = await seedWorld(q, { orgs: 1, projects: 1, tasks: 5, comments: 1, memberOf: [[p.sub]] })
			})
			const c = await ctx.connect(p, ['tasks', 'comments'])
			await ctx.converge(c)
			const id = randomUUID()
			const times: number[] = []
			await ctx.write(`INSERT INTO task (id, project_id, title) VALUES ($1, $2, 'new')`, [
				id,
				w.projects[0],
			])
			times.push(await ctx.converge(c))
			await ctx.write(`UPDATE task SET title = 'renamed', done = true WHERE id = $1`, [id])
			times.push(await ctx.converge(c))
			await ctx.write(`UPDATE comment SET body = 'edited' WHERE task_id = $1`, [w.tasks[0]])
			times.push(await ctx.converge(c))
			await ctx.write('DELETE FROM task WHERE id = $1', [id])
			times.push(await ctx.converge(c))
			return `converge ms: ${times.join(', ')}`
		},
	},
	{
		id: '3',
		title: 'snapshot under concurrent writes: no dupes / gaps',
		async run(ctx) {
			const p = ctx.principal()
			let w!: Awaited<ReturnType<typeof seedWorld>>
			await ctx.write(async (q) => {
				w = await seedWorld(q, {
					orgs: 1,
					projects: 2,
					tasks: 1000,
					comments: 0,
					memberOf: [[p.sub]],
				})
			})
			let stop = false
			let n = 0
			const writer = (async () => {
				while (!stop) {
					const project = w.projects[n % 2]
					await ctx.db.tx(async (q) => {
						await q(
							`INSERT INTO task (id, project_id, title) VALUES (gen_random_uuid(), $1, 'concurrent')`,
							[project]
						)
						await q(`UPDATE task SET title = 'touched ' || $2 WHERE id = $1`, [
							w.tasks[n % w.tasks.length],
							n,
						])
					})
					n++
				}
			})()
			await sleep(100)
			const c = await ctx.connect(p, ['tasks'])
			await sleep(1500)
			stop = true
			await writer
			return `${n} concurrent txns, converged in ${await ctx.converge(c)}ms`
		},
	},
	{
		id: '4',
		title: 'row enters / leaves scope via its own column',
		async run(ctx) {
			const p = ctx.principal()
			let ids: string[] = []
			await ctx.write(async (q) => {
				ids = await seedSink(q, 'someone-else', false)
			})
			const c = await ctx.connect(p, ['sink'])
			await ctx.converge(c)
			await ctx.write('UPDATE kitchen_sink SET owner = $2 WHERE id = $1', [ids[0], p.sub])
			const inMs = await ctx.converge(c)
			await ctx.write(`UPDATE kitchen_sink SET owner = 'someone-else' WHERE id = $1`, [ids[0]])
			const outMs = await ctx.converge(c)
			return `in ${inMs}ms, out ${outMs}ms`
		},
	},
	{
		id: '5',
		title: 'visibility gained / revoked via related-table write (multi-hop)',
		async run(ctx) {
			const p = ctx.principal()
			let w!: Awaited<ReturnType<typeof seedWorld>>
			await ctx.write(async (q) => {
				w = await seedWorld(q, {
					orgs: 2,
					projects: 2,
					tasks: 30,
					comments: 2,
					memberOf: [[p.sub], []],
				})
			})
			const c = await ctx.connect(p, ['projects', 'tasks', 'comments'])
			await ctx.converge(c)
			const member = randomUUID()
			await ctx.write(`INSERT INTO member VALUES ($1, $2, $3, 'member')`, [
				member,
				w.orgs[1],
				p.sub,
			])
			const gain = await ctx.converge(c)
			await ctx.write('DELETE FROM member WHERE id = $1', [member])
			const lose = await ctx.converge(c)
			return `gain ${gain}ms, revoke ${lose}ms`
		},
	},
	{
		id: '6',
		title: 'never observe a partial transaction',
		async run(ctx) {
			const p = ctx.principal()
			let w!: Awaited<ReturnType<typeof seedWorld>>
			await ctx.write(async (q) => {
				w = await seedWorld(q, { orgs: 1, projects: 1, tasks: 0, comments: 0, memberOf: [[p.sub]] })
			})
			const c = await ctx.connect(p, ['tasks', 'comments'])
			await ctx.converge(c)
			let bad = 0
			let commits = 0
			c.client.onCommit(() => {
				commits++
				if (pairViolations(c) > 0) bad++
			})
			const pairs = []
			for (let i = 0; i < 40; i++) pairs.push(await insertPair(ctx, w.projects[0]!))
			for (let i = 0; i < 120; i++) await retagPair(ctx, pairs[i % pairs.length]!)
			await ctx.converge(c)
			if (bad) throw new Fail(`${bad} of ${commits} local commits showed a torn task/comment pair`)
			return `${commits} local commits, none torn`
		},
	},
	{
		id: '7',
		title: 'resume after disconnect without full re-snapshot',
		async run(ctx) {
			const p = ctx.principal()
			let w!: Awaited<ReturnType<typeof seedWorld>>
			await ctx.write(async (q) => {
				w = await seedWorld(q, {
					orgs: 1,
					projects: 1,
					tasks: 2000,
					comments: 0,
					memberOf: [[p.sub]],
				})
			})
			const c = await ctx.connect(p, ['tasks'])
			await ctx.converge(c)
			const initial = c.client.bytes()
			await c.client.disconnect()
			await ctx.write(`UPDATE task SET title = 'while away' WHERE id = $1`, [w.tasks[0]])
			const before = c.client.bytes()
			await c.client.reconnect()
			await ctx.converge(c)
			const resumed = c.client.bytes() - before
			const note = `initial ${initial} B, resume ${resumed} B`
			if (resumed > initial * 0.1) throw new Fail(`resume re-sent too much: ${note}`)
			return note
		},
	},
	{
		id: '8',
		title: 'server restart: converge, no loss',
		async run(ctx) {
			if (!ctx.adapter.restartServer) throw new Unsupported('adapter cannot restart its server')
			const p = ctx.principal()
			let w!: Awaited<ReturnType<typeof seedWorld>>
			await ctx.write(async (q) => {
				w = await seedWorld(q, {
					orgs: 1,
					projects: 1,
					tasks: 200,
					comments: 1,
					memberOf: [[p.sub]],
				})
			})
			const c = await ctx.connect(p, ['tasks', 'comments'])
			await ctx.converge(c)
			const before = c.client.bytes()
			await ctx.write(`UPDATE task SET title = 'before restart' WHERE id = $1`, [w.tasks[0]])
			const restart = ctx.adapter.restartServer()
			for (let i = 1; i < 20; i++)
				await ctx.write(`UPDATE task SET title = 'during' WHERE id = $1`, [w.tasks[i]])
			await restart
			await ctx.write(`UPDATE task SET title = 'after restart' WHERE id = $1`, [w.tasks[20]])
			const ms = await ctx.converge(c, 90_000)
			return `converged ${ms}ms after restart, ${c.client.bytes() - before} B re-sent`
		},
	},
	{
		id: '8b',
		title: 'replication connection lost: converge, no loss',
		async run(ctx) {
			const slots = ctx.adapter.replicationSlots
			if (!slots) throw new Unsupported('adapter does not name its replication slots')
			const p = ctx.principal()
			let w!: Awaited<ReturnType<typeof seedWorld>>
			await ctx.write(async (q) => {
				w = await seedWorld(q, {
					orgs: 1,
					projects: 1,
					tasks: 200,
					comments: 1,
					memberOf: [[p.sub]],
				})
			})
			const c = await ctx.connect(p, ['tasks', 'comments'])
			await c.trackVisibility()
			await ctx.converge(c)
			const before = c.client.bytes()
			// What a Postgres failover, a network blip or an operator does to
			// the walsender: the engine process itself keeps running.
			const killed = await ctx.db.query(
				`SELECT pg_terminate_backend(active_pid) AS ok FROM pg_replication_slots
				 WHERE slot_name LIKE $1 AND database = current_database() AND active_pid IS NOT NULL`,
				[slots]
			)
			if (!killed.length) throw new Fail(`no active slot matching ${slots}`)
			for (let i = 0; i < 20; i++)
				await ctx.write(`UPDATE task SET title = 'during' WHERE id = $1`, [w.tasks[i]])
			await ctx.write(`DELETE FROM comment WHERE task_id = $1`, [w.tasks[30]])
			await ctx.write(`UPDATE task SET title = 'after' WHERE id = $1`, [w.tasks[40]])
			const ms = await ctx.converge(c, 90_000)
			c.verifyNoLeaks()
			return `converged ${ms}ms after the walsender was killed, ${c.client.bytes() - before} B re-sent`
		},
	},
	{
		id: '9',
		title: 'client beyond retention still converges',
		async run(ctx) {
			if (!ctx.adapter.expireRetention) throw new Unsupported('adapter cannot expire retention')
			const p = ctx.principal()
			let w!: Awaited<ReturnType<typeof seedWorld>>
			await ctx.write(async (q) => {
				w = await seedWorld(q, {
					orgs: 1,
					projects: 1,
					tasks: 200,
					comments: 0,
					memberOf: [[p.sub]],
				})
			})
			const c = await ctx.connect(p, ['tasks'])
			await ctx.converge(c)
			await c.client.disconnect()
			await ctx.write('DELETE FROM task WHERE id = $1', [w.tasks[0]])
			await ctx.write(`UPDATE task SET title = 'gone long' WHERE id = $1`, [w.tasks[1]])
			await ctx.adapter.expireRetention()
			await c.client.reconnect()
			return `${await ctx.converge(c, 90_000)}ms`
		},
	},
	{
		id: '10a',
		title: 'authz: forged / expired token rejected',
		async run(ctx) {
			if (!ctx.adapter.tokenCheck)
				throw new Unsupported('no client auth in the engine; the app proxy authorises requests')
			return ctx.adapter.tokenCheck()
		},
	},
	{
		id: '10b',
		title: 'authz: no row outside visibility, even transiently; revocation purges',
		async run(ctx) {
			const p = ctx.principal()
			let w!: Awaited<ReturnType<typeof seedWorld>>
			await ctx.write(async (q) => {
				w = await seedWorld(q, {
					orgs: 2,
					projects: 2,
					tasks: 20,
					comments: 2,
					memberOf: [[p.sub], []],
				})
			})
			const c = await ctx.connect(p, ['projects', 'tasks', 'comments'])
			await c.trackVisibility()
			await ctx.converge(c)
			const member = randomUUID()
			for (let round = 0; round < 3; round++) {
				await ctx.write(`INSERT INTO member VALUES ($1, $2, $3, 'member')`, [
					member,
					w.orgs[1],
					p.sub,
				])
				await ctx.write(`UPDATE task SET title = 'churn ' || $2 WHERE project_id = $1`, [
					w.projects[2],
					round,
				])
				await ctx.write('DELETE FROM member WHERE id = $1', [member])
				await ctx.write(`UPDATE task SET title = 'hidden ' || $2 WHERE project_id = $1`, [
					w.projects[2],
					round,
				])
			}
			await ctx.converge(c)
			c.verifyNoLeaks()
			return 'no leaked row states, revocations purged'
		},
	},
	{
		id: '11',
		title: 'updates leave unchanged TOAST columns intact',
		async run(ctx) {
			const p = ctx.principal()
			let ids: string[] = []
			await ctx.write(async (q) => {
				ids = await seedSink(q, p.sub, false)
			})
			const c = await ctx.connect(p, ['sink'])
			await ctx.converge(c)
			await ctx.write('UPDATE kitchen_sink SET i4 = 42 WHERE id = $1', [ids[0]])
			return `${await ctx.converge(c)}ms`
		},
	},
	{
		id: '11b',
		title: 'out-of-line TOAST (incompressible) kept on updates of other columns',
		async run(ctx) {
			const p = ctx.principal()
			let ids: string[] = []
			await ctx.write(async (q) => {
				ids = await seedSink(q, p.sub, false)
				// ~40 kB of random hex: pglz cannot compress it inline, so
				// Postgres stores it out of line (external TOAST).
				await q(
					`UPDATE kitchen_sink SET big = (SELECT string_agg(md5(random()::text), '') FROM generate_series(1, 1250)) WHERE id = $1`,
					[ids[0]]
				)
			})
			const [ext] = await ctx.db.query(
				`SELECT pg_column_size(big) < octet_length(big) AS compressed, pg_column_size(big) AS stored FROM kitchen_sink WHERE id = $1`,
				[ids[0]]
			)
			const c = await ctx.connect(p, ['sink'])
			await ctx.converge(c)
			await ctx.write('UPDATE kitchen_sink SET i4 = 7 WHERE id = $1', [ids[0]])
			await ctx.write('UPDATE kitchen_sink SET note = $2 WHERE id = $1', [ids[0], 'touched'])
			return `${await ctx.converge(c)}ms (stored ${ext?.stored} B, compressed ${ext?.compressed})`
		},
	},
	{
		id: '11c',
		title: 'REPLICA IDENTITY DEFAULT: unsent TOAST, entering rows, key-only deletes',
		async run(ctx) {
			const table = `ident_default_${randomUUID().slice(0, 8)}`
			const big = `(SELECT string_agg(md5(random()::text || k), '') FROM generate_series(1, 1250) k)`
			await ctx.db.query(
				`CREATE TABLE ${table} (id uuid PRIMARY KEY, owner text NOT NULL, n int NOT NULL, big text NOT NULL)`
			)
			try {
				const p = ctx.principal()
				const ids = [randomUUID(), randomUUID(), randomUUID()]
				for (const id of ids)
					await ctx.db.query(`INSERT INTO ${table} VALUES ($1, 'someone-else', 0, ${big})`, [id])
				await ctx.refreshCatalog()
				// A fast rule, and a subquery rule on the same table (its own
				// table stays DEFAULT; only the member table is FULL).
				ctx.addRule({ name: 'own', sql: `SELECT * FROM ${table} WHERE owner = $sub` })
				ctx.addRule({
					name: 'own_via_member',
					sql: `SELECT * FROM ${table} WHERE owner IN (SELECT user_id FROM member WHERE user_id = $sub)`,
				})
				await ctx.write(`INSERT INTO org VALUES (gen_random_uuid(), 'o')`)
				await ctx.write(
					`INSERT INTO member SELECT gen_random_uuid(), id, $1, 'm' FROM org LIMIT 1`,
					[p.sub]
				)
				const c = await ctx.connect(p, ['own'])
				const c2 = await ctx.connect(ctx.principal(), [])
				await c2.client.close()
				const c3 = await ctx.connect(p, ['own_via_member'])
				await ctx.converge(c)
				await ctx.converge(c3)
				// Enter: owner changes, big is unchanged TOAST and not sent.
				await ctx.write(`UPDATE ${table} SET owner = $2 WHERE id = $1`, [ids[0], p.sub])
				await ctx.write(`UPDATE ${table} SET owner = $2 WHERE id = $1`, [ids[1], p.sub])
				await ctx.converge(c)
				await ctx.converge(c3)
				// Member update with unsent TOAST: the stored value must stay.
				await ctx.write(`UPDATE ${table} SET n = 42 WHERE id = $1`, [ids[0]])
				// TOAST itself changes.
				await ctx.write(`UPDATE ${table} SET big = ${big} WHERE id = $1`, [ids[1]])
				await ctx.converge(c)
				await ctx.converge(c3)
				// Delete: only the key is sent.
				await ctx.write(`DELETE FROM ${table} WHERE id = $1`, [ids[0]])
				const ms = await ctx.converge(c)
				await ctx.converge(c3)
				const [r] = await ctx.db.query(
					`SELECT relreplident::text AS i FROM pg_class WHERE oid = $1::regclass`,
					[table]
				)
				return `${ms}ms, identity left at ${r?.i === 'd' ? 'DEFAULT' : r?.i}`
			} finally {
				await ctx.db.query(`DROP TABLE ${table}`)
			}
		},
	},
	{
		id: '12a',
		title: 'TRUNCATE',
		async run(ctx) {
			const p = ctx.principal()
			await ctx.write(async (q) => tags(q, []))
			const c = await ctx.connect(p, ['tags'])
			await ctx.converge(c)
			await ctx.write('TRUNCATE tag')
			return `${await ctx.converge(c)}ms`
		},
	},
	{
		id: '12b',
		title: 'column added upstream: sync continues on known columns',
		async run(ctx) {
			const table = `ddl_probe_${randomUUID().slice(0, 8)}`
			await ctx.db.query(`CREATE TABLE ${table} (id uuid PRIMARY KEY, label text)`)
			try {
				await ctx.db.query(
					`INSERT INTO ${table} VALUES (gen_random_uuid(), 'a'), (gen_random_uuid(), 'b')`
				)
				await ctx.refreshCatalog()
				ctx.addRule({ name: 'probe', sql: `SELECT * FROM ${table}` })
				const c = await ctx.connect(ctx.principal(), ['probe'])
				await ctx.converge(c)
				await ctx.write(`ALTER TABLE ${table} ADD COLUMN extra text DEFAULT 'x'`)
				await ctx.write(`UPDATE ${table} SET label = 'changed', extra = 'y' WHERE label = 'a'`)
				await ctx.write(`INSERT INTO ${table} VALUES (gen_random_uuid(), 'c', 'z')`)
				return `${await ctx.converge(c)}ms`
			} finally {
				await ctx.db.query(`DROP TABLE ${table}`)
			}
		},
	},
	{
		id: '13',
		title: 'read-your-writes: client can tell when a write is reflected',
		async run(ctx) {
			// The convention from docs/design.md Q10: the write transaction
			// also records the client's last mutation id, and the client syncs
			// its own row. The confirmation must arrive with the data it
			// confirms, including for writes that change nothing else it syncs.
			const p = ctx.principal()
			let w!: Awaited<ReturnType<typeof seedWorld>>
			await ctx.write(async (q) => {
				w = await seedWorld(q, { orgs: 1, projects: 1, tasks: 5, comments: 1, memberOf: [[p.sub]] })
			})
			const c = await ctx.connect(p, ['tasks', 'comments', 'mutations'])
			await ctx.converge(c)
			const db = c.client.replica.db
			const confirmed = (n: number) =>
				Number(
					db.selectValue('SELECT last_id FROM client_mutation WHERE client_id = ?', [p.sub]) ?? 0
				) >= n
			let early = 0
			let target = { n: 0, title: '' }
			c.client.onCommit(() => {
				if (
					target.title &&
					confirmed(target.n) &&
					db.selectValue('SELECT title FROM task WHERE id = ?', [w.tasks[0]!]) !== target.title
				)
					early++
			})
			const mutate = async (n: number, sql: string, params: unknown[]) =>
				ctx.write(async (q) => {
					await q(sql, params)
					await q(
						`INSERT INTO client_mutation VALUES ($1, $2) ON CONFLICT (client_id) DO UPDATE SET last_id = excluded.last_id`,
						[p.sub, n]
					)
				})
			const times: number[] = []
			const wait = async (n: number) => {
				const t0 = Date.now()
				while (!confirmed(n)) {
					if (Date.now() - t0 > 20_000) throw new Fail(`mutation ${n} never confirmed`)
					await new Promise((r) => setTimeout(r, 5))
				}
				times.push(Date.now() - t0)
			}
			for (let n = 1; n <= 20; n++) {
				target = { n, title: `ryw ${n}` }
				await mutate(n, 'UPDATE task SET title = $2 WHERE id = $1', [w.tasks[0], target.title])
				await wait(n)
			}
			target = { n: 0, title: '' }
			await mutate(21, `INSERT INTO org VALUES (gen_random_uuid(), 'unrelated')`, [])
			await wait(21)
			if (early)
				throw new Fail(`${early} local commits showed a mutation confirmed before its data arrived`)
			times.sort((a, b) => a - b)
			return `confirmed with its data; p50 ${times[Math.floor(times.length / 2)]}ms; unrelated write confirmed`
		},
	},
	{
		id: '14',
		title: '100k-row transaction',
		async run(ctx) {
			const p = ctx.principal()
			let w!: Awaited<ReturnType<typeof seedWorld>>
			await ctx.write(async (q) => {
				w = await seedWorld(q, { orgs: 1, projects: 1, tasks: 0, comments: 0, memberOf: [[p.sub]] })
			})
			const c = await ctx.connect(p, ['tasks'])
			await ctx.converge(c)
			const partial = new Set<number>()
			c.client.onCommit(() => {
				const n = Number(c.client.replica.db.selectValue('SELECT count(*) FROM task'))
				if (n !== 0 && n !== 100_000) partial.add(n)
			})
			await ctx.write(
				`INSERT INTO task (id, project_id, title) SELECT gen_random_uuid(), $1, 'bulk ' || g FROM generate_series(1, 100000) g`,
				[w.projects[0]]
			)
			const ms = await ctx.converge(c, 400_000)
			if (partial.size)
				throw new Fail(
					`converged in ${ms}ms but showed ${partial.size} partial states (e.g. ${[...partial].slice(0, 3).join(', ')} rows)`
				)
			return `${ms}ms, atomic`
		},
	},
	{
		id: '15',
		title: 'subscribe / unsubscribe mid-session, atomically',
		async run(ctx) {
			const p = ctx.principal()
			let w!: Awaited<ReturnType<typeof seedWorld>>
			await ctx.write(async (q) => {
				w = await seedWorld(q, {
					orgs: 1,
					projects: 2,
					tasks: 300,
					comments: 1,
					memberOf: [[p.sub]],
				})
			})
			const [a, b] = w.projects as [string, string]
			const wants = (project: string) => [
				{ rule: 'project_tasks', params: { project } },
				{ rule: 'project_comments', params: { project } },
			]
			const c = await ctx.connect(p, wants(a))
			await ctx.converge(c)
			const full = (project: string) =>
				`(SELECT count(*) FROM task WHERE project_id = '${project}') = 300 AND
				 (SELECT count(*) FROM comment c JOIN task t ON t.id = c.task_id WHERE t.project_id = '${project}') = 300`
			let bad = 0
			let commits = 0
			c.client.onCommit(() => {
				commits++
				const db = c.client.replica.db
				const onlyA = db.selectValue(
					`SELECT (${full(a)}) AND NOT EXISTS (SELECT 1 FROM task WHERE project_id = '${b}')`
				)
				const onlyB = db.selectValue(
					`SELECT (${full(b)}) AND NOT EXISTS (SELECT 1 FROM task WHERE project_id = '${a}')`
				)
				if (!onlyA && !onlyB) bad++
			})
			await c.want(wants(b))
			const ms = await ctx.converge(c)
			if (bad)
				throw new Fail(
					`${bad} of ${commits} local commits during the swap showed neither project fully`
				)
			return `swapped in ${ms}ms in ${commits} commit(s)`
		},
	},
	{
		id: '17',
		title: 'rule dropped while offline: its rows go on reconnect',
		async run(ctx) {
			const p = ctx.principal()
			await ctx.write(async (q) => {
				await seedWorld(q, { orgs: 1, projects: 1, tasks: 10, comments: 2, memberOf: [[p.sub]] })
			})
			const c = await ctx.connect(p, ['tasks', 'comments'])
			await ctx.converge(c)
			await c.client.disconnect()
			await c.want([{ rule: 'tasks' }])
			await ctx.write(`UPDATE task SET title = 'while away'`)
			await c.client.reconnect()
			const ms = await ctx.converge(c)
			const left = Number(c.client.replica.db.selectValue('SELECT count(*) FROM comment'))
			// Converge only compares wanted tables; the dropped rule's rows
			// must not linger unsynced.
			const deadline = Date.now() + 10_000
			let n = left
			while (n > 0 && Date.now() < deadline) {
				await sleep(50)
				n = Number(c.client.replica.db.selectValue('SELECT count(*) FROM comment'))
			}
			if (n > 0) throw new Fail(`${n} rows of the dropped rule still held after reconnect`)
			return `${ms}ms`
		},
	},
	{
		id: '16',
		title: 'two rules on one table (multi-role union)',
		async run(ctx) {
			const p = ctx.principal()
			let w!: Awaited<ReturnType<typeof seedWorld>>
			await ctx.write(async (q) => {
				w = await seedWorld(q, {
					orgs: 2,
					projects: 1,
					tasks: 10,
					comments: 0,
					memberOf: [[p.sub], []],
				})
			})
			const c = await ctx.connect(p, [
				'tasks',
				{ rule: 'project_tasks', params: { project: w.projects[0]! } },
			])
			return `${await ctx.converge(c)}ms`
		},
	},
	{
		id: '18',
		title: 'project-scoped rule with a permission gate: grant and revoke per viewer',
		async run(ctx) {
			const alice = ctx.principal('alice')
			const bob = ctx.principal('bob')
			const carol = ctx.principal('carol')
			let w!: Awaited<ReturnType<typeof seedWorld>>
			await ctx.write(async (q) => {
				w = await seedWorld(q, {
					orgs: 1,
					projects: 2,
					tasks: 100,
					comments: 1,
					memberOf: [[alice.sub, carol.sub]],
				})
			})
			const project = w.projects[0]!
			const wants = [
				{ rule: 'shared_tasks', params: { project } },
				{ rule: 'shared_comments', params: { project } },
			]
			const conns = [
				await ctx.connect(alice, wants),
				await ctx.connect(bob, wants),
				await ctx.connect(carol, wants),
			]
			for (const c of conns) await c.trackVisibility()
			for (const c of conns) await ctx.converge(c)
			await ctx.write(`UPDATE task SET title = 'live' WHERE project_id = $1 AND title LIKE 'task 1%'`, [
				project,
			])
			await ctx.write(`UPDATE task SET project_id = $2 WHERE id = $1`, [w.tasks[0], w.projects[1]])
			for (const c of conns) await ctx.converge(c)
			// Bob gains the project, then Alice loses it, each in one write.
			await ctx.write(`INSERT INTO member VALUES (gen_random_uuid(), $1, $2, 'member')`, [
				w.orgs[0],
				bob.sub,
			])
			await ctx.write(`DELETE FROM member WHERE user_id = $1`, [alice.sub])
			await ctx.write(`UPDATE task SET title = 'after' WHERE project_id = $1 AND title LIKE 'task 2%'`, [
				project,
			])
			for (const c of conns) await ctx.converge(c)
			for (const c of conns) c.verifyNoLeaks()
			const n = (c: (typeof conns)[number]) => c.client.replica.db.selectValue('SELECT count(*) FROM task')
			return `alice ${n(conns[0]!)} tasks, bob ${n(conns[1]!)}, carol ${n(conns[2]!)}; no row outside visibility`
		},
	},
]
