// A hostile middlebox, as on some corporate networks (antivirus or DLP
// proxies): it answers WebSocket upgrades with 403 and holds every
// response until the upstream has sent all of it. The gateway's
// /hostile/ locations go through it (docker/gateway/snippets/sync.conf),
// and it forwards to the engine named by the X-Engine header.
import http from 'node:http'

export const MIDDLEBOX_PORT = 8079

export function startMiddlebox(port = MIDDLEBOX_PORT): Promise<http.Server> {
	const server = http.createServer(async (req, res) => {
		const chunks: Buffer[] = []
		for await (const c of req) chunks.push(c as Buffer)
		try {
			const up = await fetch(`http://${req.headers['x-engine']}${req.url}`, {
				method: req.method,
				headers: { 'content-type': req.headers['content-type'] ?? 'text/plain' },
				body: req.method === 'POST' ? Buffer.concat(chunks) : undefined,
			})
			// The whole response, then forward it.
			const body = Buffer.from(await up.arrayBuffer())
			res.writeHead(up.status, {
				'content-type': up.headers.get('content-type') ?? 'application/octet-stream',
				'content-length': body.length,
			})
			res.end(body)
		} catch {
			res.writeHead(502).end()
		}
	})
	server.on('upgrade', (_req, socket) => {
		socket.end('HTTP/1.1 403 Forbidden\r\nContent-Length: 0\r\n\r\n')
	})
	return new Promise((resolve) => server.listen(port, '127.0.0.1', () => resolve(server)))
}
