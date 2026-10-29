// Runs smash seeds in parallel, one Postgres database and one engine port
// per worker, so a batch uses the machine instead of waiting on round trips.
//
//   pnpm --filter @nightride/localsync-sim parallel [--jobs 8] [--seeds 100-131] [smash flags...]
import { spawn } from 'node:child_process'

import { Db } from '@nightride/localsync-harness/pg.ts'

// --jobs and --seeds are ours; everything else is forwarded to smash.
let jobs = 8
let seedSpec = '100-115'
const extra: string[] = []
const argv = process.argv.slice(2)
for (let i = 0; i < argv.length; i++) {
	const a = argv[i]!
	if (a === '--jobs') jobs = Number(argv[++i])
	else if (a === '--seeds') seedSpec = argv[++i]!
	else extra.push(a)
}
const [lo, hi] = seedSpec.split('-').map(Number) as [number, number]
const seeds = Array.from({ length: hi - lo + 1 }, (_, i) => lo + i)

const admin = new Db()
const base = new URL(
	process.env.LOCALSYNC_PG_URL ??
		`postgresql://postgres:postgres@localhost:${process.env.LOCALSYNC_PG_PORT ?? '5532'}/localsync`
)
for (let j = 0; j < jobs; j++) {
	const name = `localsync_sim_${j}`
	const [r] = await admin.query('SELECT 1 AS x FROM pg_database WHERE datname = $1', [name])
	if (!r) await admin.query(`CREATE DATABASE ${name}`)
}
await admin.close()

const results: string[] = []
let next = 0
async function worker(j: number): Promise<void> {
	const url = new URL(base)
	url.pathname = `/localsync_sim_${j}`
	while (next < seeds.length) {
		const seed = seeds[next++]!
		const out = await new Promise<string>((resolve) => {
			let buf = ''
			const p = spawn(
				process.execPath,
				['--disable-warning=ExperimentalWarning', 'src/smash.ts', '--seed', String(seed), ...extra],
				{
					cwd: import.meta.dirname + '/..',
					env: {
						...process.env,
						LOCALSYNC_PG_URL: url.toString(),
						LOCALSYNC_ENGINE_PORT: String(3200 + j),
						LOCALSYNC_SKIP_BUILD: j === 0 && seed === seeds[0] ? '' : '1',
					},
				}
			)
			p.stdout.on('data', (d: Buffer) => (buf += d.toString()))
			p.stderr.on('data', (d: Buffer) => (buf += d.toString()))
			p.on('exit', () => resolve(buf))
		})
		const verdict =
			out.split('\n').find((l) => l.startsWith('PASS') || l.startsWith('FAIL')) ??
			`CRASH seed=${seed}\n${out.slice(-800)}`
		const detail = verdict.startsWith('FAIL')
			? `\n${out
					.split('\n')
					.slice(out.split('\n').indexOf(verdict) + 1, out.split('\n').indexOf(verdict) + 3)
					.join('\n')}`
			: ''
		results.push(verdict + detail)
		console.log(verdict + detail)
	}
}
const t0 = Date.now()
// Build the engine once up front, then let workers skip it.
await new Promise((r) =>
	spawn('go', ['build', '-o', 'bin/engine', './cmd/engine'], {
		cwd: import.meta.dirname + '/../..',
		stdio: 'inherit',
	}).on('exit', r)
)
await Promise.all(Array.from({ length: jobs }, (_, j) => worker(j)))
const failed = results.filter((r) => !r.startsWith('PASS'))
console.log(
	`${results.length - failed.length}/${results.length} passed in ${Math.round((Date.now() - t0) / 1000)}s with ${jobs} jobs`
)
process.exit(failed.length ? 1 : 0)
