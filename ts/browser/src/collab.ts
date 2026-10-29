// Collab soak page: one person's tab in a data- and update-heavy app. Live
// queries like a project screen's (lists, counts, a task's comments) run
// on localdb's reader pool while two people write. After every delivery
// turn the page checks what a user would see: a task and its comments are
// always written together with one tag, so the task query and the comment
// query (different tables, so different readers) must agree.
import { LocalDb } from '../../../../localdb/src/index.ts'

const params = new URLSearchParams(location.search)
// Each person has their own foreground window; the soak's tabs share one
// headless browser where all but one would count as hidden (localdb ticks
// hidden tabs at 1 Hz), so present every tab as visible.
Object.defineProperty(document, 'visibilityState', { get: () => 'visible' })
Object.defineProperty(document, 'hidden', { get: () => false })
const SCHEMA = `
	CREATE TABLE IF NOT EXISTS task (id TEXT PRIMARY KEY, project_id TEXT, title TEXT, done INTEGER, txn_tag TEXT);
	CREATE TABLE IF NOT EXISTS comment (id TEXT PRIMARY KEY, task_id TEXT, body TEXT, txn_tag TEXT);
`
const stats = {
	turns: 0,
	torn: 0,
	tornExamples: [] as string[],
	latencies: [] as number[],
	counts: { tasks: -1, comments: -1 },
	error: null as string | null,
}
const w = globalThis as unknown as { __collab: typeof stats; __db: LocalDb }
w.__collab = stats

const db = new LocalDb(
	{
		dbName: `localsync-collab-${params.get('run')}.db`,
		schemaSql: SCHEMA,
		schemaVersion: 1,
		shapes: [],
		readers: 3,
		alignCommits: params.get('align') === '1',
		sync: {
			transport: 'localsync',
			options: {
				url: params.get('ws'),
				token: params.get('token'),
				wants: [{ rule: 'tasks' }, { rule: 'comments' }],
			},
		},
	},
	{
		writer: new Worker('/dist/writer.worker.js', { type: 'module' }),
		spawnReader: () => new Worker('/dist/reader.worker.js', { type: 'module' }),
	}
)
w.__db = db

const projects = (params.get('projects') ?? '').split(',')
let written: Array<{ id: string; t: string }> | null = null
let comments: Array<{ task_id: string; t: string }> | null = null
const seenTags = new Set<string>()
let queued = false

function check(): void {
	queued = false
	if (!written || !comments) return
	stats.turns++
	const tag = new Map<string, string>()
	for (const r of written) tag.set(r.id, r.t)
	let bad = 0
	const perTask = new Map<string, number>()
	for (const c of comments) {
		perTask.set(c.task_id, (perTask.get(c.task_id) ?? 0) + 1)
		if (tag.get(c.task_id) !== c.t) {
			bad++
			if (stats.tornExamples.length < 3)
				stats.tornExamples.push(
					`comment of ${c.task_id.slice(0, 8)}: ${c.t} vs task ${tag.get(c.task_id) ?? 'missing'}`
				)
		}
	}
	for (const [id] of tag) if ((perTask.get(id) ?? 0) !== 2) bad++
	if (bad) stats.torn++
	// Commit-to-render latency: tags carry the writer's commit time.
	const now = Date.now()
	for (const r of written) {
		if (seenTags.has(r.t)) continue
		seenTags.add(r.t)
		const ms = Number(r.t.split(':')[3])
		if (ms) stats.latencies.push(now - ms)
	}
}
const queueCheck = () => {
	if (queued) return
	queued = true
	queueMicrotask(check)
}
const onError = (e: string | undefined) => {
	if (e) stats.error = e
}

// What a project screen shows: two project task lists, per-project and
// per-task counts, totals, the written tasks and their comments.
for (const p of projects)
	db.subscribe(
		{
			sql: 'SELECT id, title, txn_tag FROM task WHERE project_id = ?1 ORDER BY title LIMIT 200',
			params: [p],
		},
		(r) => onError(r.error)
	)
db.subscribe({ sql: 'SELECT project_id, count(*) AS n FROM task GROUP BY project_id' }, (r) =>
	onError(r.error)
)
db.subscribe({ sql: 'SELECT task_id, count(*) AS n FROM comment GROUP BY task_id' }, (r) =>
	onError(r.error)
)
db.subscribe<{ tasks: number; comments: number }>(
	{
		sql: 'SELECT (SELECT count(*) FROM task) AS tasks, (SELECT count(*) FROM comment) AS comments',
	},
	(r) => {
		onError(r.error)
		if (r.rows[0]) stats.counts = r.rows[0]
	}
)
db.subscribe<{ id: string; t: string }>(
	{ sql: `SELECT id, txn_tag AS t FROM task WHERE txn_tag LIKE 'w:%'` },
	(r) => {
		onError(r.error)
		written = r.rows
		queueCheck()
	}
)
db.subscribe<{ task_id: string; t: string }>(
	{ sql: `SELECT task_id, txn_tag AS t FROM comment WHERE txn_tag LIKE 'w:%'` },
	(r) => {
		onError(r.error)
		comments = r.rows
		queueCheck()
	}
)
