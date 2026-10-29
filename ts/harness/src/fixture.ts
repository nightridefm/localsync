// Synthetic fixture: every scoping shape the corpora use (direct column,
// one hop, many-to-many, multi-hop, client-chosen project param) and every
// column kind. Created once in `public`; each scenario truncates it and
// uses fresh ids, so shapes from earlier scenarios never collide.
import { randomUUID } from 'node:crypto'

import type { Db, Query } from './pg.ts'
import type { Rule } from './rules.ts'

export const FIXTURE_VERSION = '2'

export const FIXTURE_TABLES = [
	'org',
	'member',
	'project',
	'tag',
	'project_tag',
	'task',
	'comment',
	'kitchen_sink',
	'client_mutation',
]

const DDL = `
CREATE TABLE org (id uuid PRIMARY KEY, name text NOT NULL);
CREATE TABLE member (id uuid PRIMARY KEY, org_id uuid NOT NULL, user_id text NOT NULL, role text);
CREATE TABLE project (id uuid PRIMARY KEY, org_id uuid NOT NULL, name text NOT NULL, archived bool NOT NULL DEFAULT false);
CREATE TABLE tag (id uuid PRIMARY KEY, label text NOT NULL);
CREATE TABLE project_tag (id uuid PRIMARY KEY, project_id uuid NOT NULL, tag_id uuid NOT NULL);
CREATE TABLE task (id uuid PRIMARY KEY, project_id uuid NOT NULL, title text NOT NULL, done bool NOT NULL DEFAULT false, txn_tag text);
CREATE TABLE comment (id uuid PRIMARY KEY, task_id uuid NOT NULL, body text NOT NULL, txn_tag text);
CREATE TABLE kitchen_sink (
	id uuid PRIMARY KEY, owner text NOT NULL,
	i2 smallint, i4 int, i8 bigint, f8 float8, num numeric(16,4), b bool,
	ts timestamptz, tsn timestamp, d date, j jsonb, arr text[], iarr int[],
	big text, note text
);
-- Read-your-writes convention (docs/design.md Q10): the app records the
-- last mutation it applied for a client inside the same transaction.
CREATE TABLE client_mutation (client_id text PRIMARY KEY, last_id bigint NOT NULL);
CREATE INDEX ON member (user_id);
CREATE INDEX ON task (project_id);
CREATE INDEX ON comment (task_id);
`

const visibleProjects =
	'SELECT id FROM project WHERE org_id IN (SELECT org_id FROM member WHERE user_id = $sub)'
const memberGate = `EXISTS (SELECT 1 FROM member m WHERE m.user_id = $sub AND m.org_id IN (SELECT org_id FROM project WHERE id = $project))`
const visibleTasks = `SELECT id FROM task WHERE project_id IN (${visibleProjects})`

export const RULES: Record<string, Rule> = Object.fromEntries(
	(
		[
			['members', 'SELECT * FROM member WHERE user_id = $sub'],
			['projects', `SELECT * FROM project WHERE id IN (${visibleProjects})`],
			['tasks', `SELECT * FROM task WHERE project_id IN (${visibleProjects})`],
			['comments', `SELECT * FROM comment WHERE task_id IN (${visibleTasks})`],
			['project_tags', `SELECT * FROM project_tag WHERE project_id IN (${visibleProjects})`],
			['tags', 'SELECT * FROM tag'],
			['sink', 'SELECT * FROM kitchen_sink WHERE owner = $sub'],
			['mutations', 'SELECT * FROM client_mutation WHERE client_id = $sub'],
			[
				'project_tasks',
				`SELECT * FROM task WHERE project_id = $project AND project_id IN (${visibleProjects})`,
				['project'],
			],
			// A common shape: scoped by project, plus a permission gate that
			// is the only part depending on the principal (shared body).
			[
				'shared_tasks',
				`SELECT * FROM task WHERE project_id = $project AND ${memberGate}`,
				['project'],
			],
			[
				'shared_comments',
				`SELECT * FROM comment WHERE task_id IN (SELECT id FROM task WHERE project_id = $project) AND ${memberGate}`,
				['project'],
			],
			[
				'project_comments',
				`SELECT * FROM comment WHERE task_id IN (SELECT id FROM task WHERE project_id = $project AND project_id IN (${visibleProjects}))`,
				['project'],
			],
		] as Array<[string, string, string[]?]>
	).map(([name, sql, params]) => [name, { name, sql, params }])
)

/** Creates the fixture if missing or outdated. Returns true if (re)created. */
export async function ensureFixture(db: Db): Promise<boolean> {
	await db.query('CREATE TABLE IF NOT EXISTS harness_meta (k text PRIMARY KEY, v text NOT NULL)')
	const [r] = await db.query(`SELECT v FROM harness_meta WHERE k = 'fixture'`)
	if (r?.v === FIXTURE_VERSION) return false
	await db.tx(async (q) => {
		await q(`DROP TABLE IF EXISTS ${FIXTURE_TABLES.join(', ')} CASCADE`)
		await q(DDL)
		await q(
			`INSERT INTO harness_meta VALUES ('fixture', $1) ON CONFLICT (k) DO UPDATE SET v = excluded.v`,
			[FIXTURE_VERSION]
		)
	})
	return true
}

export async function resetData(db: Db): Promise<void> {
	await db.query(`TRUNCATE ${FIXTURE_TABLES.join(', ')}`)
}

export type World = { orgs: string[]; projects: string[]; tasks: string[] }

/**
 * Seeds orgs with projects / tasks / comments. `memberOf[i]` lists the subs
 * that are members of org i.
 */
export async function seedWorld(
	q: Query,
	opts: { orgs: number; projects: number; tasks: number; comments: number; memberOf: string[][] }
): Promise<World> {
	const w: World = { orgs: [], projects: [], tasks: [] }
	for (let o = 0; o < opts.orgs; o++) {
		const org = randomUUID()
		w.orgs.push(org)
		await q('INSERT INTO org VALUES ($1, $2)', [org, `org ${o}`])
		for (const sub of opts.memberOf[o] ?? []) {
			await q('INSERT INTO member VALUES ($1, $2, $3, $4)', [randomUUID(), org, sub, 'member'])
		}
		for (let p = 0; p < opts.projects; p++) {
			const project = randomUUID()
			w.projects.push(project)
			await q('INSERT INTO project (id, org_id, name) VALUES ($1, $2, $3)', [
				project,
				org,
				`project ${o}.${p}`,
			])
			const tasks = await q(
				`INSERT INTO task (id, project_id, title) SELECT gen_random_uuid(), $1, 'task ' || g FROM generate_series(1, $2) g RETURNING id`,
				[project, opts.tasks]
			)
			for (const t of tasks) w.tasks.push(t.id!)
			await q(
				`INSERT INTO comment (id, task_id, body) SELECT gen_random_uuid(), t.id, 'comment ' || g
				 FROM task t, generate_series(1, $2) g WHERE t.project_id = $1`,
				[project, opts.comments]
			)
		}
	}
	return w
}

/** Kitchen-sink rows: edge values for every column kind, plus an all-NULL row. */
export async function seedSink(q: Query, owner: string, edgeArray = true): Promise<string[]> {
	const ids = [randomUUID(), randomUUID(), randomUUID()]
	await q(
		`INSERT INTO kitchen_sink VALUES
		($1, $4, -32768, 2147483647, 9007199254740993, 0.1, 12345678.1234, true,
		 '2026-03-01 12:34:56.789123+00', '2026-01-01 10:00:00.5', '2024-02-29',
		 '{"b": 1, "a": [1, "x", null], "n": {"k": true}}',
		 CASE WHEN $5 THEN ARRAY['a', 'b c', '1', 'NULL', NULL, 'q"uote', 'comma,inside'] ELSE ARRAY['a', 'b c', 'NULL', NULL] END,
		 ARRAY[1, 2, 3],
		 repeat('toast ', 2000), '2024-01-01 10:00:00+02'),
		($2, $4, 0, -1, -9007199254740993, 1e300, -0.0001, false,
		 '1999-12-31 23:59:59.999+02', '1970-01-01 00:00:00', '1900-01-01',
		 '[]', ARRAY[]::text[], ARRAY[]::int[], '', 'true'),
		($3, $4, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)`,
		[...ids, owner, edgeArray]
	)
	return ids
}
