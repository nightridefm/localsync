import assert from 'node:assert/strict'
import http from 'node:http'
import type { AddressInfo } from 'node:net'
import { test } from 'node:test'
import zlib from 'node:zlib'

import { HttpConn, httpUrlFor, REC_BINARY, REC_DEFLATED, REC_TEXT } from './http.ts'

const record = (kind: number, payload: Buffer) => {
	const head = Buffer.alloc(5)
	head[0] = kind
	head.writeUInt32LE(payload.length, 1)
	return Buffer.concat([head, payload])
}

void test('records split at any byte, deflated or not, in order', async () => {
	const big = Buffer.from('row body, compressible '.repeat(5000))
	const body = Buffer.concat([
		record(REC_TEXT, Buffer.from('{"type":"units"}')),
		record(REC_BINARY | REC_DEFLATED, zlib.deflateRawSync(big)),
		record(REC_BINARY, Buffer.from([1, 2, 3])),
		record(REC_TEXT | REC_DEFLATED, zlib.deflateRawSync(Buffer.from('{"type":"beat"}'))),
	])
	let want = ''
	const server = http.createServer(async (req, res) => {
		for await (const c of req) want += String(c)
		res.writeHead(200, { 'content-type': 'application/octet-stream' })
		// 1 to 7 bytes at a time: records cross every boundary.
		for (let i = 0, n = 1; i < body.length; i += n, n = (n % 7) + 1) {
			res.write(body.subarray(i, i + n))
			if (i % 50 === 0) await new Promise((r) => setImmediate(r))
		}
		res.end()
	})
	await new Promise<void>((r) => server.listen(0, '127.0.0.1', r))
	const url = `http://127.0.0.1:${(server.address() as AddressInfo).port}/sync/http`
	const got: Array<string | Uint8Array> = []
	const clean = await new Promise<boolean>((resolve) => {
		const conn = new HttpConn(url, false)
		conn.onopen = () => conn.send('{"type":"want"}')
		conn.onmessage = (d) => got.push(typeof d === 'string' ? d : new Uint8Array(d))
		conn.onclose = resolve
	})
	server.close()
	assert.equal(want, '{"type":"want"}')
	assert.equal(clean, true)
	assert.deepEqual(got[0], '{"type":"units"}')
	assert.deepEqual(Buffer.from(got[1] as Uint8Array), big)
	assert.deepEqual(got[2], new Uint8Array([1, 2, 3]))
	assert.deepEqual(got[3], '{"type":"beat"}')
	assert.equal(got.length, 4)
})

void test('a response cut inside a record is not clean', async () => {
	const server = http.createServer((_req, res) => {
		res.writeHead(200)
		res.end(record(REC_BINARY, Buffer.alloc(100)).subarray(0, 50))
	})
	await new Promise<void>((r) => server.listen(0, '127.0.0.1', r))
	const url = `http://127.0.0.1:${(server.address() as AddressInfo).port}/`
	const clean = await new Promise<boolean>((resolve) => {
		const conn = new HttpConn(url, true)
		conn.onopen = () => conn.send('{}')
		conn.onclose = resolve
	})
	server.close()
	assert.equal(clean, false)
})

void test('httpUrlFor', () => {
	assert.equal(httpUrlFor('wss://example.test/sync'), 'https://example.test/sync/http')
	assert.equal(httpUrlFor('ws://127.0.0.1:3141/sync?x=1'), 'http://127.0.0.1:3141/sync/http?x=1')
})
