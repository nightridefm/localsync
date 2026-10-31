// Build the packed dist/: JS + .d.ts + source maps. The release tarball
// (.github/workflows/release.yml) ships dist/ only; the workspace imports
// src/ directly. rewriteRelativeImportExtensions covers the emitted JS, but
// tsc leaves the .ts specifiers in declarations, so they are rewritten here
// (as in localdb's scripts/build-dist.mjs).
import { execSync } from 'node:child_process'
import { readdirSync, readFileSync, rmSync, writeFileSync } from 'node:fs'
import path from 'node:path'

rmSync('dist', { recursive: true, force: true })
execSync('tsc -p tsconfig.build.json', { stdio: 'inherit' })

function* walk(dir) {
	for (const e of readdirSync(dir, { withFileTypes: true })) {
		const p = path.join(dir, e.name)
		if (e.isDirectory()) yield* walk(p)
		else yield p
	}
}

const RELATIVE_TS = /(['"])(\.{1,2}\/[^'"]*)\.ts\1/g
for (const f of walk('dist')) {
	if (!f.endsWith('.d.ts')) continue
	const src = readFileSync(f, 'utf8')
	const out = src.replace(RELATIVE_TS, (_, quote, spec) => `${quote}${spec}.js${quote}`)
	if (out !== src) writeFileSync(f, out)
}

// No emitted file may still import a relative .ts specifier.
for (const f of walk('dist')) {
	if (!f.endsWith('.js') && !f.endsWith('.d.ts')) continue
	if (RELATIVE_TS.test(readFileSync(f, 'utf8'))) {
		console.error(`build-dist: ${f} still imports a .ts specifier`)
		process.exit(1)
	}
}
