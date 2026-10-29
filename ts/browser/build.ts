// Bundles the smoke page and the two worker entries. sqlite-wasm stays
// external and is served from node_modules so its .wasm resolves next to it
// (the same rule localdb's harness/build.js uses).
import { build } from 'esbuild'

const sqliteWasmExternal = {
	name: 'sqlite-wasm-external',
	setup(b: {
		onResolve: (o: { filter: RegExp }, f: () => { path: string; external: boolean }) => void
	}) {
		b.onResolve({ filter: /^@sqlite\.org\/sqlite-wasm$/ }, () => ({
			path: '/node_modules/@sqlite.org/sqlite-wasm/dist/index.mjs',
			external: true,
		}))
	},
}

export async function buildSmoke(root: string): Promise<void> {
	for (const name of ['app', 'collab', 'writer.worker', 'reader.worker']) {
		await build({
			absWorkingDir: root,
			entryPoints: [`src/${name}.ts`],
			outfile: `dist/${name}.js`,
			bundle: true,
			format: 'esm',
			platform: 'browser',
			target: 'es2022',
			sourcemap: true,
			plugins: [sqliteWasmExternal],
			logLevel: 'warning',
		})
	}
}
