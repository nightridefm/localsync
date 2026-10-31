// Row hashes must match the Go engine byte for byte (wire.RowHash), or every
// unit checksum drifts. Golden data: internal/wire/testdata/rowhash.json.
import assert from 'node:assert/strict'
import { readFileSync } from 'node:fs'
import path from 'node:path'
import { test } from 'node:test'

import { type ColumnDesc, type Kind, memberHash, rowHash } from './codec.ts'

const golden = JSON.parse(
	readFileSync(
		path.resolve(import.meta.dirname, '../../../internal/wire/testdata/rowhash.json'),
		'utf8'
	)
) as { kinds: Kind[]; rows: Array<{ hex: string; hash: string; member: string }> }

void test('row hash matches the Go engine', () => {
	const cols: ColumnDesc[] = golden.kinds.map((kind, i) => ({ name: `c${i}`, kind }))
	for (const r of golden.rows)
		assert.equal(rowHash(cols, Buffer.from(r.hex, 'hex')).toString(), r.hash)
})

void test('member hash matches the Go engine', () => {
	for (const r of golden.rows) assert.equal(memberHash(BigInt(r.hash)).toString(), r.member)
})
