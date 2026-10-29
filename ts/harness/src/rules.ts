// Rules as the harness sees them: plain SQL over one table, with named
// params ($sub from the principal, others supplied by the want). Bound
// once per (rule, principal, params) into the forms each consumer needs:
// positional SQL for the oracle, inlined literals for Electric shapes.

export type Rule = { name: string; sql: string; params?: string[] }

export type Principal = { name: string; sub: string; claims?: Record<string, unknown> }

export type Want = { rule: string; params?: Record<string, string> }

export type Bound = {
	/** Stable identity: rule name + param values. */
	key: string
	rule: Rule
	params: Record<string, string>
	table: string
	/** Output columns, or null for `*`. */
	columns: string[] | null
	/** SQL with $1..$n and the values to bind (oracle). */
	sql: string
	values: string[]
	/** WHERE clause with params inlined as quoted literals (Electric), or null. */
	where: string | null
}

const SHAPE = /^\s*SELECT\s+(.+?)\s+FROM\s+("?[A-Za-z_][\w]*"?)\s*(?:WHERE\s+([\s\S]+?))?\s*$/i

/** Replaces $name outside single-quoted literals. */
function substitute(sql: string, fn: (name: string) => string): string {
	let out = ''
	let i = 0
	while (i < sql.length) {
		const ch = sql[i]!
		if (ch === "'") {
			let j = i + 1
			while (j < sql.length) {
				if (sql[j] === "'" && sql[j + 1] === "'") j += 2
				else if (sql[j] === "'") break
				else j++
			}
			out += sql.slice(i, j + 1)
			i = j + 1
		} else if (ch === '$' && /[A-Za-z_]/.test(sql[i + 1] ?? '')) {
			let j = i + 1
			while (j < sql.length && /\w/.test(sql[j]!)) j++
			out += fn(sql.slice(i + 1, j))
			i = j
		} else {
			out += ch
			i++
		}
	}
	return out
}

const literal = (v: string) => `'${v.replaceAll("'", "''")}'`

export function bind(rule: Rule, principal: Principal, want: Want): Bound {
	const m = SHAPE.exec(rule.sql)
	if (!m) throw new Error(`rule ${rule.name}: not of the form SELECT ... FROM t [WHERE ...]`)
	const value = (name: string): string => {
		if (name === 'sub') return principal.sub
		const v = want.params?.[name]
		if (v === undefined) throw new Error(`rule ${rule.name}: missing param $${name}`)
		return v
	}
	const order: string[] = []
	const sql = substitute(rule.sql, (name) => {
		let i = order.indexOf(name)
		if (i < 0) i = order.push(name) - 1
		return `$${i + 1}`
	})
	const cols = m[1]!.trim()
	const where = m[3] ? substitute(m[3], (name) => literal(value(name))) : null
	const params = Object.entries(want.params ?? {})
		.sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0))
		.map(([k, v]) => `${k}=${v}`)
	return {
		key: [rule.name, ...params].join('|'),
		rule,
		params: want.params ?? {},
		table: m[2]!.replaceAll('"', ''),
		columns: cols === '*' ? null : cols.split(',').map((c) => c.trim().replaceAll('"', '')),
		sql,
		values: order.map(value),
		where,
	}
}
