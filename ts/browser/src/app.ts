// Browser smoke page: localdb in a real browser (OPFS, workers, write
// fence) syncing through the localsync transport. Reactive queries record
// what the UI would render, so the driver can assert "one change on screen".
import { LocalDb } from '../../../../localdb/src/index.ts'

type Ev = [string, ...unknown[]]
const events: Ev[] = []
const w = globalThis as unknown as { __events: Ev[]; __db: LocalDb; __error: string | null }
w.__events = events
w.__error = null

const params = new URLSearchParams(location.search)
const SCHEMA = `
	CREATE TABLE IF NOT EXISTS task (id TEXT PRIMARY KEY, project_id TEXT, title TEXT, done INTEGER, txn_tag TEXT);
	CREATE TABLE IF NOT EXISTS comment (id TEXT PRIMARY KEY, task_id TEXT, body TEXT, txn_tag TEXT);
`

const db = new LocalDb(
	{
		dbName: `localsync-smoke-${params.get('run')}.db`,
		schemaSql: SCHEMA,
		schemaVersion: 1,
		shapes: [],
		// 3: the task and comment queries pin to different readers (table
		// affinity), which is where cross-query tearing can happen.
		readers: 3,
		alignCommits: params.get('align') === '1',
		sync: {
			transport: 'localsync',
			options: {
				url: params.get('ws'),
				token: params.get('token'),
				transport: params.get('transport') || undefined,
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

db.onSyncStatus((s) => {
	events.push(['status', s.state])
	// 'live' means caught up: what the database holds at that moment.
	if (s.state === 'live')
		void db
			.query<{ t: number; c: number }>(
				'SELECT (SELECT count(*) FROM task) AS t, (SELECT count(*) FROM comment) AS c'
			)
			.then((r) => events.push(['live-counts', `${r[0]?.t}/${r[0]?.c}`]))
})
db.subscribe<{ n: number }>({ sql: 'SELECT count(*) AS n FROM task' }, (r) => {
	if (r.error) w.__error = r.error
	events.push(['tasks', r.rows[0]?.n])
})
db.subscribe<{ n: number }>({ sql: 'SELECT count(*) AS n FROM comment' }, (r) =>
	events.push(['comments', r.rows[0]?.n])
)
// Torn pairs as the UI would see them: a task and its comment written in one
// transaction must never render with different tags.
db.subscribe<{ n: number }>(
	{
		sql: `SELECT count(*) AS n FROM task t JOIN comment c ON c.task_id = t.id
		      WHERE t.txn_tag LIKE 'pair:%' AND c.txn_tag IS NOT t.txn_tag`,
	},
	(r) => events.push(['torn', r.rows[0]?.n])
)
// The same pairs through two SEPARATE live queries (often on different
// readers): after each delivery turn their tags must agree, or the screen
// shows a task and its comment from different commits.
let taskTags: string[] | null = null
let commentTags: string[] | null = null
let checkQueued = false
const checkPairs = () => {
	checkQueued = false
	if (!taskTags || !commentTags) return
	const a = new Set(taskTags)
	let n = 0
	for (const t of commentTags) if (!a.has(t)) n++
	n += Math.max(0, taskTags.length - commentTags.length)
	events.push(['xtorn', n])
}
const queueCheck = () => {
	if (checkQueued) return
	checkQueued = true
	// After every listener of this delivery turn has run.
	queueMicrotask(checkPairs)
}
db.subscribe<{ t: string }>(
	{ sql: `SELECT txn_tag AS t FROM task WHERE txn_tag LIKE 'pair:%'` },
	(r) => {
		taskTags = r.rows.map((x) => x.t)
		queueCheck()
	}
)
db.subscribe<{ t: string }>(
	{ sql: `SELECT txn_tag AS t FROM comment WHERE txn_tag LIKE 'pair:%'` },
	(r) => {
		commentTags = r.rows.map((x) => x.t)
		queueCheck()
	}
)
