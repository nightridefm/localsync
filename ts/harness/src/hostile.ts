// Hostile suite: tries to break a running engine on purpose. Three kinds of
// attack, each a pass/fail check:
//
// - leaks: injection-shaped client parameters, principals (token sub) and
//   stored values, checked against the oracle (the rule SQL run in Postgres
//   with bound parameters): the replica must never hold a row the oracle
//   does not, whatever the engine does with the input;
// - protocol and auth abuse on raw sockets: forged or malformed tokens,
//   parameters the rule does not declare, malformed and oversized messages;
// - resource exhaustion: connections that never authenticate, and one
//   authenticated client churning through units. These check the engine
//   bounds what an attacker can pin, not just that it survives.
//
// After every attack the engine must still answer /health.
//
//   pnpm --filter @nightride/localsync-harness hostile [--only=leak-params,churn]
import { createHmac, randomUUID } from 'node:crypto'
import { readFileSync } from 'node:fs'
import { parseArgs } from 'node:util'

import { introspect } from './catalog.ts'
import { ensureFixture, resetData, seedWorld } from './fixture.ts'
import { buildEngine, LocalsyncAdapter, mintToken, PORT } from './localsync.ts'
import { Db } from './pg.ts'
import { Ctx, Fail } from './scenario.ts'

const { values } = parseArgs({ options: { only: { type: 'string' } } })
const only = values.only ? new Set(values.only.split(',')) : null

const WS_URL = `ws://127.0.0.1:${PORT}/sync`
const HTTP_URL = `http://127.0.0.1:${PORT}`
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms))

// Strings that break naive quoting, lexing or key building somewhere.
const NASTY = [
	"' OR '1'='1",
	"'; SELECT pg_sleep(3); --",
	"x') OR true --",
	'\\',
	"\\'",
	"\\'; SELECT 1; --",
	'$1',
	'$$ OR true $$',
	"E'\\x27",
	'*/ OR true /*',
	'-- comment',
	'"quoted"',
	'%',
	'_',
	'\u0000',
	'a\u0000b',
	'\ud800',
	'‮\u0000￿',
	'x'.repeat(100_000),
	'00000000-0000-0000-0000-000000000000',
	"00000000-0000-0000-0000-000000000000' OR '1'='1",
	'{}',
	'NULL',
]

type Attack = {
	id: string
	title: string
	run(ctx: Ctx, adapter: LocalsyncAdapter): Promise<string>
}

const b64url = (s: string) => Buffer.from(s).toString('base64url')
/** A token with any header and payload, signed with the harness secret. */
function rawToken(header: object, payload: object, secret = 'harness-secret'): string {
	const h = b64url(JSON.stringify(header))
	const p = b64url(JSON.stringify(payload))
	const sig = createHmac('sha256', secret).update(`${h}.${p}`).digest('base64url')
	return `${h}.${p}.${sig}`
}
const exp = () => Math.floor(Date.now() / 1000) + 600

/** Sends messages on a fresh socket and collects what comes back. */
function probe(
	msgs: Array<string | Uint8Array>,
	waitMs = 1500
): Promise<{ texts: string[]; closed: boolean }> {
	return new Promise((resolve) => {
		const ws = new WebSocket(WS_URL)
		const texts: string[] = []
		let closed = false
		const done = () => {
			clearTimeout(timer)
			try {
				ws.close()
			} catch {
				// already closed
			}
			resolve({ texts, closed })
		}
		const timer = setTimeout(done, waitMs)
		ws.onopen = () => {
			for (const m of msgs) ws.send(m as string)
		}
		ws.onmessage = (ev) => {
			if (typeof ev.data === 'string') texts.push(ev.data)
		}
		ws.onclose = () => {
			closed = true
			done()
		}
		ws.onerror = () => {}
	})
}

const want = (token: string, wants: unknown[]) =>
	JSON.stringify({ type: 'want', token, wants, have: {} })

async function healthy(): Promise<void> {
	const ok = await fetch(`${HTTP_URL}/health`).then(
		(r) => r.ok,
		() => false
	)
	if (!ok) throw new Fail('engine no longer answers /health')
}

async function engineUnits(): Promise<number> {
	const r = (await (await fetch(`${HTTP_URL}/debug/units`)).json()) as { units: unknown[] | null }
	return r.units?.length ?? 0
}

function rssMiB(pid: number | undefined): number {
	if (!pid) return NaN
	const m = /VmRSS:\s+(\d+) kB/.exec(readFileSync(`/proc/${pid}/status`, 'utf8'))
	return m ? Number(m[1]) / 1024 : NaN
}

/** Waits until the client reports caught up (every want served or failed). */
async function caughtUp(c: { client: { caughtUp?(): boolean } }, ms = 30_000): Promise<void> {
	const start = Date.now()
	while (!c.client.caughtUp?.()) {
		if (Date.now() - start > ms) throw new Fail('client never caught up')
		await sleep(20)
	}
}

const attacks: Attack[] = [
	{
		id: 'leak-params',
		title: 'injection-shaped client parameters never leak another tenant',
		async run(ctx) {
			const attacker = ctx.principal('attacker')
			const victim = ctx.principal('victim')
			let mine!: Awaited<ReturnType<typeof seedWorld>>
			let theirs!: Awaited<ReturnType<typeof seedWorld>>
			await ctx.write(async (q) => {
				mine = await seedWorld(q, {
					orgs: 1,
					projects: 1,
					tasks: 5,
					comments: 1,
					memberOf: [[attacker.sub]],
				})
				theirs = await seedWorld(q, {
					orgs: 1,
					projects: 2,
					tasks: 5,
					comments: 1,
					memberOf: [[victim.sub]],
				})
			})
			const values = [...NASTY, theirs.projects[0]!, `${theirs.projects[0]}' OR '1'='1`]
			const rules = [
				'project_tasks',
				'shared_tasks',
				'shared_comments',
				'own_membership',
				'project_comments',
			]
			const unchecked = rules.flatMap((rule) =>
				values.map((v) => ({ rule, params: { project: v } }))
			)
			// Checked: the attacker's own data, so every synced table is
			// compared with the oracle; anything of the victim's is an extra row.
			const c = await ctx.connect(
				attacker,
				[
					'members',
					'tasks',
					'comments',
					{ rule: 'project_tasks', params: { project: mine.projects[0]! } },
				],
				unchecked
			)
			const t0 = Date.now()
			await caughtUp(c)
			const ms = Date.now() - t0
			await ctx.converge(c)
			// Changes after the fact go through restricted queries.
			await ctx.write(`UPDATE task SET title = title || ' (edited)'`)
			await ctx.write(`UPDATE comment SET body = body || ' (edited)'`)
			await ctx.converge(c)
			await healthy()
			if (ms > 20_000) throw new Fail(`caught up only after ${ms}ms (a parameter ran slow SQL?)`)
			return `${unchecked.length} hostile wants over ${rules.length} rules: no leak, caught up in ${ms}ms`
		},
	},
	{
		id: 'leak-sub',
		title: 'an injection-shaped principal (token sub) sees exactly its rows',
		async run(ctx) {
			const notes: string[] = []
			for (const sub of NASTY.filter(
				(s) => !s.includes('\u0000') && s.length < 1000 && s !== '\ud800'
			)) {
				const p = { name: 'nasty', sub }
				const other = ctx.principal('other')
				await ctx.write(async (q) => {
					await seedWorld(q, { orgs: 1, projects: 1, tasks: 3, comments: 1, memberOf: [[p.sub]] })
					await seedWorld(q, {
						orgs: 1,
						projects: 1,
						tasks: 3,
						comments: 1,
						memberOf: [[other.sub]],
					})
					await q(
						`INSERT INTO kitchen_sink (id, owner, note) VALUES (gen_random_uuid(), $1, 'mine')`,
						[p.sub]
					)
					await q(
						`INSERT INTO kitchen_sink (id, owner, note) VALUES (gen_random_uuid(), $1, 'theirs')`,
						[other.sub]
					)
					await q(`INSERT INTO client_mutation VALUES ($1, 1) ON CONFLICT DO NOTHING`, [p.sub])
					await q(`INSERT INTO client_mutation VALUES ($1, 1) ON CONFLICT DO NOTHING`, [
						`${p.sub}x`,
					])
				})
				const c = await ctx.connect(p, ['members', 'projects', 'tasks', 'sink', 'mutations'])
				await ctx.converge(c)
				// Text primary keys and owners in restricted queries.
				await ctx.write(`UPDATE client_mutation SET last_id = last_id + 1`)
				await ctx.write(`UPDATE kitchen_sink SET note = note || '!'`)
				await ctx.write(`DELETE FROM client_mutation WHERE client_id = $1`, [`${p.sub}x`])
				await ctx.converge(c)
				await ctx.closeAll()
				await resetData(ctx.db)
				notes.push(JSON.stringify(sub).slice(0, 12))
			}
			await healthy()
			return `${notes.length} principals, each exactly its own rows through inserts, updates and deletes`
		},
	},
	{
		id: 'leak-stored',
		title: 'stored values that reach restriction SQL are quoted, never run',
		async run(ctx) {
			// A text join: member.role (written by anyone who can edit a
			// membership) decides which kitchen_sink rows are visible, so a
			// role change is a move-in whose restriction carries the role.
			ctx.addRule({
				name: 'sink_by_role',
				sql: 'SELECT * FROM kitchen_sink WHERE note IN (SELECT role FROM member WHERE user_id = $sub)',
			})
			const p = ctx.principal()
			const org = randomUUID()
			const member = randomUUID()
			const secret = randomUUID()
			await ctx.write(async (q) => {
				await q('INSERT INTO member VALUES ($1, $2, $3, $4)', [member, org, p.sub, 'none'])
				for (const v of NASTY.filter((s) => !s.includes('\u0000') && s !== '\ud800'))
					await q(
						`INSERT INTO kitchen_sink (id, owner, note) VALUES (gen_random_uuid(), 'x', $1)`,
						[v]
					)
				await q(`INSERT INTO kitchen_sink (id, owner, note) VALUES ($1, 'x', 'secret')`, [secret])
			})
			const c = await ctx.connect(p, ['sink_by_role'])
			await ctx.converge(c)
			let n = 0
			for (const v of NASTY.filter((s) => !s.includes('\u0000') && s !== '\ud800')) {
				await ctx.write('UPDATE member SET role = $2 WHERE id = $1', [member, v])
				await ctx.converge(c)
				// The same value arriving with the row (a move-in by row change).
				await ctx.write('UPDATE kitchen_sink SET note = $2 WHERE id = $1', [secret, v])
				await ctx.converge(c)
				await ctx.write(`UPDATE kitchen_sink SET note = 'secret' WHERE id = $1`, [secret])
				await ctx.converge(c)
				n++
			}
			await healthy()
			return `${n} hostile stored values through move-ins and row changes: client always equals the oracle`
		},
	},
	{
		id: 'auth',
		title: 'malformed and forged tokens are refused',
		async run() {
			const sub = randomUUID()
			const cases: Array<[string, string]> = [
				[
					'alg none',
					`${b64url(JSON.stringify({ alg: 'none', typ: 'JWT' }))}.${b64url(JSON.stringify({ sub, exp: exp(), aud: 'localsync' }))}.`,
				],
				[
					'HS512',
					(() => {
						const h = b64url(JSON.stringify({ alg: 'HS512', typ: 'JWT' }))
						const pl = b64url(JSON.stringify({ sub, exp: exp(), aud: 'localsync' }))
						return `${h}.${pl}.${createHmac('sha512', 'harness-secret').update(`${h}.${pl}`).digest('base64url')}`
					})(),
				],
				['empty secret', rawToken({ alg: 'HS256' }, { sub, exp: exp(), aud: 'localsync' }, '')],
				['no exp', rawToken({ alg: 'HS256' }, { sub, aud: 'localsync' })],
				['no aud', rawToken({ alg: 'HS256' }, { sub, exp: exp() })],
				['no sub', rawToken({ alg: 'HS256' }, { exp: exp(), aud: 'localsync' })],
				['numeric sub', rawToken({ alg: 'HS256' }, { sub: 42, exp: exp(), aud: 'localsync' })],
				[
					'grants not strings',
					rawToken({ alg: 'HS256' }, { sub, exp: exp(), aud: 'localsync', grants: [1] }),
				],
				[
					'grants a string',
					rawToken({ alg: 'HS256' }, { sub, exp: exp(), aud: 'localsync', grants: 'admin' }),
				],
				[
					'not yet valid',
					rawToken({ alg: 'HS256' }, { sub, exp: exp(), nbf: exp(), aud: 'localsync' }),
				],
				['empty', ''],
				['garbage', 'a.b.c'],
				['huge', 'x'.repeat(900_000)],
			]
			for (const [what, token] of cases) {
				const r = await probe([want(token, [{ id: 1, rule: 'tags' }])])
				if (
					!r.texts.some((t) => t.includes('"error"')) ||
					r.texts.some((t) => t.includes('"units"'))
				)
					throw new Fail(`${what}: not refused (${r.texts.join(' ').slice(0, 200)})`)
			}
			await healthy()
			return `${cases.length} bad tokens refused`
		},
	},
	{
		id: 'params',
		title: 'parameters the rule does not declare, and bad wants, are refused',
		async run() {
			const victim = randomUUID()
			const token = mintToken(randomUUID())
			const r = await probe([
				want(token, [
					{ id: 1, rule: 'members', params: { sub: victim } },
					{ id: 2, rule: 'project_tasks', params: { project: randomUUID(), sub: victim } },
					{ id: 3, rule: 'project_tasks' },
					{ id: 4, rule: 'no_such_rule' },
					{ id: 5, rule: 'tags', params: { __proto__: 'x' } },
				]),
			])
			const units = r.texts
				.map((t) => JSON.parse(t) as { type: string; errors?: Array<{ id: number }> })
				.find((m) => m.type === 'units')
			const refused = new Set(units?.errors?.map((e) => e.id))
			for (const id of [1, 2, 3, 4])
				if (!refused.has(id))
					throw new Fail(`want ${id} not refused: ${r.texts.join(' ').slice(0, 300)}`)
			// 2001 wants: the last is refused, the engine survives.
			const many = Array.from({ length: 2001 }, (_, i) => ({ id: i + 1, rule: 'tags' }))
			const m = await probe([want(token, many)], 5000)
			if (!m.texts.some((t) => t.includes('too many wants')))
				throw new Fail('2001 wants: no "too many wants"')
			await healthy()
			return 'declared-only parameters ($sub never from the client), missing and unknown rules refused; want limit holds'
		},
	},
	{
		id: 'malformed',
		title: 'malformed, binary and oversized messages',
		async run() {
			const token = mintToken(randomUUID())
			const cases: Array<[string, Array<string | Uint8Array>, 'error' | 'close' | 'ignored']> = [
				['not JSON', ['{'], 'error'],
				['wrong type', [JSON.stringify({ type: 'nope' })], 'error'],
				[
					'negative id',
					[JSON.stringify({ type: 'want', token, wants: [{ id: -1, rule: 'tags' }] })],
					'error',
				],
				[
					'id overflow',
					[JSON.stringify({ type: 'want', token, wants: [{ id: 2 ** 40, rule: 'tags' }] })],
					'error',
				],
				[
					'params not strings',
					[
						JSON.stringify({
							type: 'want',
							token,
							wants: [{ id: 1, rule: 'tags', params: { a: 1 } }],
						}),
					],
					'error',
				],
				['binary', [new Uint8Array([1, 2, 3])], 'ignored'],
				['2 MiB', ['x'.repeat(2 << 20)], 'close'],
				['deep JSON', ['['.repeat(100_000)], 'error'],
				[
					'bogus have',
					[
						JSON.stringify({
							type: 'want',
							token,
							wants: [{ id: 1, rule: 'tags' }],
							have: {
								epoch: 'x',
								cursor: '99999999999999999999999',
								units: [{ id: 1, checksum: '-5' }],
							},
						}),
					],
					'ignored',
				],
			]
			for (const [what, msgs, expect] of cases) {
				const r = await probe(msgs)
				const err = r.texts.some((t) => t.includes('"error"'))
				if (expect === 'error' && !err)
					throw new Fail(`${what}: no error (${r.texts.join(' ').slice(0, 200)})`)
				if (expect === 'close' && !r.closed) throw new Fail(`${what}: connection not closed`)
				await healthy()
			}
			// /sync/http
			const get = await fetch(`${HTTP_URL}/sync/http`)
			if (get.status !== 405) throw new Fail(`GET /sync/http: ${get.status}`)
			const bad = await fetch(`${HTTP_URL}/sync/http`, { method: 'POST', body: '{' })
			if (bad.status !== 400) throw new Fail(`bad JSON over HTTP: ${bad.status}`)
			const big = await fetch(`${HTTP_URL}/sync/http`, {
				method: 'POST',
				body: 'x'.repeat(2 << 20),
			}).catch(() => null)
			if (big && big.status !== 400) throw new Fail(`2 MiB over HTTP: ${big.status}`)
			await healthy()
			return `${cases.length} malformed WebSocket messages and 3 bad HTTP requests handled`
		},
	},
	{
		id: 'unauthenticated',
		title: 'connections that never authenticate are closed, and cost little meanwhile',
		async run(_ctx, adapter) {
			const N = 500
			const before = rssMiB(adapter.enginePid())
			const sockets: WebSocket[] = []
			let closed = 0
			for (let i = 0; i < N; i++) {
				const ws = new WebSocket(WS_URL)
				ws.onclose = () => closed++
				ws.onerror = () => {}
				sockets.push(ws)
			}
			await sleep(2000)
			const after = rssMiB(adapter.enginePid())
			const perConn = ((after - before) * 1024) / N
			// Then wait out an authentication deadline.
			const start = Date.now()
			while (closed < N && Date.now() - start < 20_000) await sleep(200)
			for (const ws of sockets) ws.close()
			await healthy()
			if (closed < N)
				throw new Fail(
					`${N - closed}/${N} silent connections still open after 20s (${perConn.toFixed(0)} KiB each)`
				)
			return `${N} silent connections closed in ${((Date.now() - start) / 1000).toFixed(1)}s; ${perConn.toFixed(0)} KiB each meanwhile`
		},
	},
	{
		id: 'churn',
		title: 'one client churning through units cannot grow the engine without bound',
		async run(ctx, adapter) {
			const p = ctx.principal()
			await ctx.write((q) =>
				seedWorld(q, { orgs: 1, projects: 1, tasks: 1, comments: 0, memberOf: [[p.sub]] })
			)
			const before = await engineUnits()
			const rss0 = rssMiB(adapter.enginePid())
			const token = mintToken(p.sub)
			const rounds = 20
			const r = await new Promise<string>((resolve) => {
				const ws = new WebSocket(WS_URL)
				let i = 0
				const next = () => {
					if (i++ >= rounds) return resolve('done')
					const wants = Array.from({ length: 2000 }, (_, j) => ({
						id: j + 1,
						rule: 'project_tasks',
						params: { project: randomUUID() },
					}))
					ws.send(want(token, wants))
				}
				ws.onopen = next
				ws.onmessage = (ev) => {
					if (typeof ev.data === 'string' && ev.data.includes('"type":"units"')) next()
					if (typeof ev.data === 'string' && ev.data.includes('"type":"error"'))
						resolve(`closed: ${ev.data}`)
				}
				ws.onclose = () => resolve('closed')
				ws.onerror = () => {}
				setTimeout(() => resolve('timeout'), 60_000)
			})
			await sleep(3000)
			const units = (await engineUnits()) - before
			const rss = rssMiB(adapter.enginePid()) - rss0
			await healthy()
			const note = `${rounds} WANTs x 2000 new units (${r}): ${units} units in memory after, +${rss.toFixed(0)} MiB`
			if (units > 10_000) throw new Fail(note)
			return note
		},
	},
]

buildEngine()
const db = new Db()
await ensureFixture(db)
const adapter = new LocalsyncAdapter()
let failed = 0
// The engine starts with the first connection.
{
	await resetData(db)
	const ctx = new Ctx(db, adapter, await introspect(db))
	await ctx.connect(ctx.principal(), ['tags'])
	await ctx.closeAll()
}
for (const a of attacks) {
	if (only && !only.has(a.id)) continue
	await resetData(db)
	const ctx = new Ctx(db, adapter, await introspect(db))
	const start = Date.now()
	try {
		const note = await a.run(ctx, adapter)
		console.log(`pass  ${a.id.padEnd(16)} ${a.title}\n      ${note} (${Date.now() - start}ms)`)
	} catch (err) {
		failed++
		console.log(
			`FAIL  ${a.id.padEnd(16)} ${a.title}\n      ${err instanceof Error ? err.message : String(err)}`
		)
	} finally {
		await ctx.closeAll()
	}
}
await adapter.close()
await db.close()
process.exit(failed ? 1 : 0)
