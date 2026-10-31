#!/usr/bin/env bash
# Security checks: known vulnerabilities in Go and npm dependencies, static
# analysis, and a short pass of every fuzzer. The Go tools are pinned in
# tools/go.mod and built into bin/tools, so every run uses the same versions.
#
#   scripts/security.sh                 # everything (fuzzers 20s each)
#   FUZZTIME=5m scripts/security.sh     # longer fuzzing
#   SKIP_FUZZ=1 scripts/security.sh     # CI: vulnerabilities and static analysis
#
# Fuzzers that need Postgres (internal/engine) skip without the compose one.
# The hostile suite (pnpm --filter @nightride/localsync-harness hostile)
# needs the full stack and runs separately.
set -euo pipefail
cd "$(dirname "$0")/.."

go -C tools build -o ../bin/tools/ tool

echo '== govulncheck (reachable vulnerabilities in Go dependencies and the toolchain)'
bin/tools/govulncheck ./...

echo '== staticcheck'
bin/tools/staticcheck ./...

# Excluded, after review:
#   G115 integer conversions: the wire codec, LSNs and xids convert between
#        widths on purpose; decoders bound lengths themselves (fuzzed below).
#   G304 file paths from variables: the rules file comes from a CLI flag.
echo '== gosec'
bin/tools/gosec -quiet -exclude=G115,G304 ./...

echo '== pnpm audit (npm dependencies)'
pnpm audit --prod

if [[ -z "${SKIP_FUZZ:-}" ]]; then
	t="${FUZZTIME:-20s}"
	for target in \
		internal/wire:FuzzArrayJSON \
		internal/wire:FuzzValues \
		internal/wire:FuzzDecodePatch \
		internal/rules:FuzzRestrictionLiterals \
		internal/engine:FuzzSimpleProtocolParam; do
		pkg="./${target%%:*}"
		name="${target##*:}"
		echo "== fuzz $name ($t)"
		go test "$pkg" -run '^$' -fuzz "^$name\$" -fuzztime "$t"
	done
fi
echo 'security checks OK'
