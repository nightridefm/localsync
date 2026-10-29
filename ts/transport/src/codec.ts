// Wire codec, mirroring internal/wire (Go) byte for byte. Values are
// positional, typed by the unit's column descriptor. See codec.go for the
// layout.

export type Kind =
	| 'text'
	| 'int'
	| 'float'
	| 'numeric'
	| 'bool'
	| 'uuid'
	| 'timestamp'
	| 'date'
	| 'json'
	| 'array'
	| 'other'

export type ColumnDesc = { name: string; kind: Kind; elem?: Kind }

export type UnitDesc = {
	id: number
	ref: number
	rule: string
	table: string
	columns: ColumnDesc[]
	pk: number[]
}

/** A value as stored in SQLite (localdb conventions: bool 0/1, ISO ms timestamps, JSON text arrays). */
export type SqlValue = string | number | bigint | null

const MSG_FRAME = 1
/** A frame part: more parts follow; the frame ends with a MSG_FRAME. */
export const MSG_FRAME_PART = 2
export const OP_PUT = 1
export const OP_REMOVE = 2
export const OP_PATCH = 3
const FLAG_RESET = 1
const FLAG_CONTINUES = 2

const utf8 = new TextDecoder()
const HEX: string[] = []
for (let i = 0; i < 256; i++) HEX.push(i.toString(16).padStart(2, '0'))

export class Reader {
	pos = 0
	readonly buf: Uint8Array
	private view: DataView

	constructor(buf: Uint8Array) {
		this.buf = buf
		this.view = new DataView(buf.buffer, buf.byteOffset, buf.byteLength)
	}

	byte(): number {
		return this.buf[this.pos++]!
	}

	/** Unsigned LEB128 varint; a bigint when it exceeds 2^53. */
	uvarint(): number | bigint {
		let n = 0
		let shift = 0
		for (;;) {
			const b = this.buf[this.pos++]!
			if (shift >= 49) return this.uvarintBig(n, shift, b)
			n += (b & 0x7f) * 2 ** shift
			if (b < 0x80) return n
			shift += 7
		}
	}

	private uvarintBig(low: number, shift: number, first: number): bigint {
		let n = BigInt(low)
		let b = first
		let s = BigInt(shift)
		for (;;) {
			n |= BigInt(b & 0x7f) << s
			if (b < 0x80) return n
			s += 7n
			b = this.buf[this.pos++]!
		}
	}

	uint(): number {
		const v = this.uvarint()
		return typeof v === 'bigint' ? Number(v) : v
	}

	zigzag(): number | bigint {
		const v = this.uvarint()
		if (typeof v === 'bigint') return (v >> 1n) ^ -(v & 1n)
		return v % 2 === 0 ? v / 2 : -(v + 1) / 2
	}

	u64(): bigint {
		const v = this.view.getBigUint64(this.pos, true)
		this.pos += 8
		return v
	}

	f64(): number {
		const v = this.view.getFloat64(this.pos, true)
		this.pos += 8
		return v
	}

	bytes(n: number): Uint8Array {
		const out = this.buf.subarray(this.pos, this.pos + n)
		this.pos += n
		return out
	}
}

function uuid(b: Uint8Array): string {
	let s = ''
	for (let i = 0; i < 16; i++) {
		if (i === 4 || i === 6 || i === 8 || i === 10) s += '-'
		s += HEX[b[i]!]
	}
	return s
}

const MAX_I64 = (1n << 63n) - 1n
const MIN_I64 = -(1n << 63n)

function timestamp(us: number | bigint): string {
	if (typeof us === 'bigint') {
		if (us === MAX_I64) return 'infinity'
		if (us === MIN_I64) return '-infinity'
		us = Number(us)
	}
	return new Date(Math.floor(us / 1000)).toISOString()
}

/** Reads one non-NULL value. */
export function readValue(r: Reader, c: ColumnDesc): SqlValue {
	switch (c.kind) {
		case 'int': {
			const v = r.zigzag()
			return typeof v === 'bigint' && v >= -(2n ** 53n) && v <= 2n ** 53n ? Number(v) : v
		}
		case 'float': {
			// sqlite-wasm binds any JS number that passes Number.isInteger as an
			// int64, so 1e300 would be stored as 0 (overflow). Large integral
			// doubles go as text, which a REAL column converts exactly.
			const f = r.f64()
			return Number.isInteger(f) && !Number.isSafeInteger(f) ? String(f) : f
		}
		case 'bool':
			return r.byte()
		case 'uuid':
			return uuid(r.bytes(16))
		case 'timestamp':
			return timestamp(r.zigzag())
		default:
			return utf8.decode(r.bytes(r.uint()))
	}
}

/** Reads a row: null bitmap, then the non-NULL values. */
export function readRow(r: Reader, cols: ColumnDesc[], out: SqlValue[]): void {
	const nb = (cols.length + 7) >> 3
	const start = r.pos
	r.pos += nb
	for (let i = 0; i < cols.length; i++) {
		out[i] = (r.buf[start + (i >> 3)]! >> (i & 7)) & 1 ? null : readValue(r, cols[i]!)
	}
}

/** Encoded length of the value at r.pos, without decoding it (as wire.valueLen). */
function valueLen(r: Reader, c: ColumnDesc): number {
	const start = r.pos
	switch (c.kind) {
		case 'int':
		case 'timestamp':
			while (r.buf[r.pos++]! >= 0x80);
			break
		case 'float':
			r.pos += 8
			break
		case 'bool':
			r.pos += 1
			break
		case 'uuid':
			r.pos += 16
			break
		default: {
			const n = r.uint()
			r.pos += n
		}
	}
	const n = r.pos - start
	r.pos = start
	return n
}

/**
 * Row hash, as wire.RowHash: the sum (mod 2^64) of per-column hashes, each
 * two FNV-1a 32-bit lanes over (column index, NULL flag, encoded value).
 * The lanes are summed as 32-bit halves with carry, so no bigint work per
 * column.
 */
const M64 = (1n << 64n) - 1n

/**
 * A row's contribution to its unit's checksum: the row hash through the
 * splitmix64 finalizer, so values moving between rows change the checksum
 * (wire.MemberHash in Go).
 */
export function memberHash(rowHash: bigint): bigint {
	let z = rowHash
	z = ((z ^ (z >> 30n)) * 0xbf58476d1ce4e5b9n) & M64
	z = ((z ^ (z >> 27n)) * 0x94d049bb133111ebn) & M64
	return z ^ (z >> 31n)
}

export function rowHash(cols: ColumnDesc[], b: Uint8Array): bigint {
	const r = new Reader(b)
	const nb = (cols.length + 7) >> 3
	r.pos = nb
	let lo = 0
	let hi = 0
	for (let i = 0; i < cols.length; i++) {
		let h1 = 0x811c9dc5
		let h2 = 0x9e3779b9
		h1 = Math.imul(h1 ^ (i & 0xff), 16777619) >>> 0
		h2 = Math.imul(h2 ^ (i & 0xff), 16777619) >>> 0
		h1 = Math.imul(h1 ^ ((i >> 8) & 0xff), 16777619) >>> 0
		h2 = Math.imul(h2 ^ ((i >> 8) & 0xff), 16777619) >>> 0
		if ((b[i >> 3]! >> (i & 7)) & 1) {
			h1 = Math.imul(h1, 16777619) >>> 0
			h2 = Math.imul(h2, 16777619) >>> 0
		} else {
			h1 = Math.imul(h1 ^ 1, 16777619) >>> 0
			h2 = Math.imul(h2 ^ 1, 16777619) >>> 0
			const n = valueLen(r, cols[i]!)
			for (let k = r.pos; k < r.pos + n; k++) {
				h1 = Math.imul(h1 ^ b[k]!, 16777619) >>> 0
				h2 = Math.imul(h2 ^ b[k]!, 16777619) >>> 0
			}
			r.pos += n
		}
		lo += h1
		hi += h2
		if (lo >= 2 ** 32) {
			lo -= 2 ** 32
			hi += 1
		}
		hi %= 2 ** 32
	}
	return (BigInt(hi) << 32n) | BigInt(lo)
}

export type Patch = { hash: bigint; pk: Uint8Array; changed: number[]; values: SqlValue[] }

/** Decodes a PATCH op (as wire.DecodePatch); values[i] for each changed column. */
export function readPatch(cols: ColumnDesc[], b: Uint8Array): Patch {
	const r = new Reader(b)
	const hash = r.u64()
	const pk = r.bytes(r.uint())
	const nb = (cols.length + 7) >> 3
	const chg = r.bytes(nb)
	const nul = r.bytes(nb)
	const changed: number[] = []
	const values: SqlValue[] = []
	for (let i = 0; i < cols.length; i++) {
		if (!((chg[i >> 3]! >> (i & 7)) & 1)) continue
		changed.push(i)
		values.push((nul[i >> 3]! >> (i & 7)) & 1 ? null : readValue(r, cols[i]!))
	}
	return { hash, pk, changed, values }
}

export type Op = { kind: number; bytes: Uint8Array }
/** continues: more ops for the unit follow in a later part of the frame. */
export type Fragment = {
	ref: number
	reset: boolean
	continues: boolean
	checksum: bigint
	ops: Op[]
}
export type Frame = { lsn: number | bigint; fragments: Fragment[]; more: boolean }

export function decodeFrame(buf: Uint8Array): Frame {
	const r = new Reader(buf)
	const kind = r.byte()
	if (kind !== MSG_FRAME && kind !== MSG_FRAME_PART) throw new Error('not a frame')
	const lsn = r.uvarint()
	const n = r.uint()
	const fragments: Fragment[] = []
	for (let i = 0; i < n; i++) {
		const ref = r.uint()
		const flags = r.byte()
		const checksum = r.u64()
		const nops = r.uint()
		// oxlint-disable-next-line unicorn/no-new-array -- preallocation, every slot written
		const ops: Op[] = new Array(nops)
		for (let j = 0; j < nops; j++) {
			const kind = r.byte()
			ops[j] = { kind, bytes: r.bytes(r.uint()) }
		}
		fragments.push({
			ref,
			reset: (flags & FLAG_RESET) !== 0,
			continues: (flags & FLAG_CONTINUES) !== 0,
			checksum,
			ops,
		})
	}
	return { lsn, fragments, more: kind === MSG_FRAME_PART }
}
