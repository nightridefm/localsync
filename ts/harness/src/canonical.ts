// Canonical values: the oracle (Postgres text) and every replica are mapped
// to the same string form per column kind, so comparisons check data, not
// each engine's formatting. Deliberately strict on the replica side: a
// value stored with the wrong JS type (a number in a text[] element, the
// string 'true' in a bool column) does NOT canonicalise to the oracle's
// form, so the harness reports it.
import type { Column, Kind } from './catalog.ts'

export type Canon = string | null

// 'YYYY-MM-DD[ T]HH:MM:SS[.frac][Z|+HH[:MM]]' -> ISO at millisecond
// precision (both apps use timestamptz(3); localdb keeps ms).
const TS =
	/^(\d{4})-(\d{2})-(\d{2})[ T](\d{2}):(\d{2}):(\d{2})(?:\.(\d+))?(Z|[+-]\d{2}(?::?\d{2})?)?$/

function tsCanon(s: string): Canon {
	const m = TS.exec(s)
	if (!m) return `#ts:${s}`
	const ms = Number(((m[7] ?? '') + '000').slice(0, 3))
	let t = Date.UTC(+m[1]!, +m[2]! - 1, +m[3]!, +m[4]!, +m[5]!, +m[6]!, ms)
	const z = m[8]
	if (z && z !== 'Z') {
		const sign = z[0] === '-' ? -1 : 1
		const digits = z.slice(1).replace(':', '')
		const off = +digits.slice(0, 2) * 60 + (digits.length > 2 ? +digits.slice(2, 4) : 0)
		t -= sign * off * 60_000
	}
	return new Date(t).toISOString()
}

function stable(v: unknown): string {
	if (Array.isArray(v)) return `[${v.map(stable).join(',')}]`
	if (v !== null && typeof v === 'object') {
		const o = v as Record<string, unknown>
		return `{${Object.keys(o)
			.sort()
			.map((k) => `${JSON.stringify(k)}:${stable(o[k])}`)
			.join(',')}}`
	}
	return JSON.stringify(v)
}

/** Postgres 1-D array literal -> elements as Postgres text (null for NULL). */
export function parsePgArray(s: string): Array<string | null> | null {
	if (!s.startsWith('{') || !s.endsWith('}')) return null
	const out: Array<string | null> = []
	let i = 1
	const end = s.length - 1
	if (i === end) return out
	for (;;) {
		if (s[i] === '"') {
			let v = ''
			i++
			while (i < end && s[i] !== '"') {
				if (s[i] === '\\') i++
				v += s[i]
				i++
			}
			i++
			out.push(v)
		} else {
			let j = i
			while (j < end && s[j] !== ',') j++
			const raw = s.slice(i, j)
			if (raw.startsWith('{')) return null
			out.push(raw === 'NULL' ? null : raw)
			i = j
		}
		if (i >= end) return out
		if (s[i] !== ',') return null
		i++
	}
}

/** Canonical form of a Postgres text value. */
export function fromPg(col: Column, v: string | null): Canon {
	if (v === null) return null
	return pgScalar(col.kind, col.elem, v)
}

function pgScalar(kind: Kind, elem: Kind | null, v: string): Canon {
	switch (kind) {
		case 'int':
			return BigInt(v).toString()
		case 'float':
			return String(Number(v))
		case 'bool':
			return v === 't' || v === 'true' ? '1' : v === 'f' || v === 'false' ? '0' : `#bool:${v}`
		case 'ts':
			return tsCanon(v)
		case 'json':
			return stable(JSON.parse(v))
		case 'array': {
			const items = parsePgArray(v)
			if (!items) return `#array:${v}`
			return `[${items.map((x) => (x === null ? 'null' : JSON.stringify(pgScalar(elem ?? 'text', null, x)))).join(',')}]`
		}
		default:
			return v
	}
}

/** Canonical form of a value read back from a SQLite replica. */
export function fromReplica(col: Column, v: unknown): Canon {
	if (v === null || v === undefined) return null
	switch (col.kind) {
		case 'int':
			return typeof v === 'string' && /^-?\d+$/.test(v) ? BigInt(v).toString() : `#int:${String(v)}`
		case 'float':
			return typeof v === 'number' ? String(v) : `#float:${String(v)}`
		case 'bool':
			return v === 1 || v === 0 ? String(v) : `#bool:${String(v)}`
		case 'ts':
			return typeof v === 'string' ? tsCanon(v) : `#ts:${String(v)}`
		case 'json':
			try {
				return stable(typeof v === 'string' ? JSON.parse(v) : v)
			} catch {
				return `#json:${String(v)}`
			}
		case 'array': {
			let items: unknown
			try {
				items = typeof v === 'string' ? JSON.parse(v) : v
			} catch {
				return `#array:${String(v)}`
			}
			if (!Array.isArray(items)) return `#array:${String(v)}`
			const elem = col.elem ?? 'text'
			return `[${items.map((x) => (x === null ? 'null' : JSON.stringify(replicaElem(elem, x)))).join(',')}]`
		}
		default:
			return typeof v === 'string' ? v : `#${typeof v}:${String(v)}`
	}
}

function replicaElem(kind: Kind, x: unknown): string {
	switch (kind) {
		case 'int':
			return typeof x === 'number' ? String(x) : `#int:${String(x)}`
		case 'float':
			return typeof x === 'number' ? String(x) : `#float:${String(x)}`
		case 'bool':
			return x === true ? '1' : x === false ? '0' : `#bool:${String(x)}`
		default:
			return typeof x === 'string' ? x : `#${typeof x}:${String(x)}`
	}
}
