// Table metadata from the Postgres catalog: column kinds drive both the
// replica DDL and canonical comparison.
import type { Db } from './pg.ts'

export type Kind = 'int' | 'float' | 'numeric' | 'bool' | 'ts' | 'date' | 'json' | 'array' | 'text'

export type Column = { name: string; kind: Kind; elem: Kind | null }

export type Table = { name: string; columns: Column[]; pk: string[] }

export type Catalog = Map<string, Table>

function kindOf(typname: string, category: string): Kind {
	switch (typname) {
		case 'int2':
		case 'int4':
		case 'int8':
			return 'int'
		case 'float4':
		case 'float8':
			return 'float'
		case 'numeric':
			return 'numeric'
		case 'bool':
			return 'bool'
		case 'timestamp':
		case 'timestamptz':
			return 'ts'
		case 'date':
			return 'date'
		case 'json':
		case 'jsonb':
			return 'json'
	}
	return category === 'A' ? 'array' : 'text'
}

export async function introspect(db: Db): Promise<Catalog> {
	const cols = await db.query(`
		SELECT c.relname AS tbl, a.attname AS col, t.typname, t.typcategory AS cat,
		       e.typname AS elem, e.typcategory AS elem_cat,
		       COALESCE(a.attnum = ANY(i.indkey::int2[]), false) AS is_pk,
		       array_position(i.indkey::int2[], a.attnum) AS pk_pos
		FROM pg_attribute a
		JOIN pg_class c ON c.oid = a.attrelid
		JOIN pg_namespace n ON n.oid = c.relnamespace
		JOIN pg_type t ON t.oid = a.atttypid
		LEFT JOIN pg_type e ON e.oid = t.typelem AND t.typcategory = 'A'
		LEFT JOIN pg_index i ON i.indrelid = c.oid AND i.indisprimary
		WHERE n.nspname = 'public' AND c.relkind = 'r' AND a.attnum > 0 AND NOT a.attisdropped
		ORDER BY c.relname, a.attnum`)
	const out: Catalog = new Map()
	const pkPos = new Map<string, Array<[number, string]>>()
	for (const r of cols) {
		const name = r.tbl!
		let t = out.get(name)
		if (!t) {
			t = { name, columns: [], pk: [] }
			out.set(name, t)
			pkPos.set(name, [])
		}
		t.columns.push({
			name: r.col!,
			kind: kindOf(r.typname!, r.cat!),
			elem: r.elem ? kindOf(r.elem, r.elem_cat!) : null,
		})
		if (r.is_pk === 't') pkPos.get(name)!.push([Number(r.pk_pos), r.col!])
	}
	for (const [name, t] of out) {
		t.pk = pkPos
			.get(name)!
			.sort((a, b) => a[0] - b[0])
			.map((p) => p[1])
	}
	return out
}
