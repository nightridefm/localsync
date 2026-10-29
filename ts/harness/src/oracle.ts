// The oracle: each wanted rule run directly against Postgres, unioned per
// table by primary key, projected to the columns the client knows.
import type { Catalog } from './catalog.ts'
import { fromPg } from './canonical.ts'
import type { Db } from './pg.ts'
import { pkKey, type Rows, type TableRows } from './replica.ts'
import type { Bound } from './rules.ts'

/** Columns compared for a bound rule: its projection within the client's catalog. */
export function comparedColumns(catalog: Catalog, b: Bound): string[] {
	const t = catalog.get(b.table)
	if (!t) throw new Error(`no table ${b.table}`)
	const known = t.columns.map((c) => c.name)
	return b.columns ? b.columns.filter((c) => known.includes(c)) : known
}

export async function oracle(db: Db, catalog: Catalog, bounds: Bound[]): Promise<Rows> {
	const out: Rows = new Map()
	for (const b of bounds) {
		const t = catalog.get(b.table)!
		const cols = comparedColumns(catalog, b)
		const sel = cols.map((c) => `q."${c}"::text AS "${c}"`).join(', ')
		const rows = await db.query(`SELECT ${sel} FROM (${b.sql}) q`, b.values)
		let tr = out.get(b.table)
		if (!tr) {
			tr = new Map()
			out.set(b.table, tr)
		}
		for (const r of rows) {
			const rec: Record<string, string | null> = {}
			for (const c of cols)
				rec[c] = fromPg(
					t.columns.find((x) => x.name === c)!,
					r[c] ?? null
				)
			tr.set(pkKey(t.pk.map((p) => rec[p] ?? null)), rec)
		}
	}
	return out
}

export type Diff = {
	table: string
	pk: string
	kind: 'missing' | 'extra' | 'different'
	expected?: unknown
	actual?: unknown
}

export function diff(expected: Rows, actual: Rows): Diff[] {
	const out: Diff[] = []
	for (const [table, exp] of expected) {
		const act: TableRows = actual.get(table) ?? new Map()
		for (const [pk, e] of exp) {
			const a = act.get(pk)
			if (!a) out.push({ table, pk, kind: 'missing', expected: e })
			else {
				const bad: Record<string, unknown> = {}
				for (const c of Object.keys(e)) if (e[c] !== a[c]) bad[c] = { expected: e[c], actual: a[c] }
				if (Object.keys(bad).length) out.push({ table, pk, kind: 'different', expected: bad })
			}
		}
		for (const [pk, a] of act) if (!exp.has(pk)) out.push({ table, pk, kind: 'extra', actual: a })
	}
	return out
}

export function formatDiff(d: Diff[], max = 8): string {
	const lines = d.slice(0, max).map((x) => {
		const detail = JSON.stringify(x.expected ?? x.actual)
		return `  ${x.kind} ${x.table} ${x.pk} ${detail.length > 300 ? `${detail.slice(0, 300)}...` : detail}`
	})
	if (d.length > max) lines.push(`  ... ${d.length - max} more`)
	return lines.join('\n')
}
