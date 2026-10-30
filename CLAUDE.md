# localsync

Postgres WAL -> realtime sync engine for `@nightride/localdb` clients
(github.com/nightridefm/localdb). Replaces ElectricSQL. Generic: used by
two apps (App A and App B in the docs); nothing app-specific in the engine.

**Start here: `docs/design.md`** (guarantees, design, decisions,
verification, phases). The original handoff plan, the apps' real rule
corpora and the corpus check results are private, in `../localsync-corpus`
(see its README); never commit them or name the apps here. `corpus/` is
gitignored for a local copy.

## Layout

```
cmd/engine/      # hub service: slot, rules, splice, encode, WebSocket
cmd/relay/       # optional spoke fan-out relay
internal/        # shared Go packages
ts/transport/    # @nightride/localsync - localdb SyncTransport client
ts/harness/      # capability harness (Electric vs localsync) + adapters
sim/             # "smash" simulation: workloads, faults, soak, shrinking
docs/            # design.md, capability matrix, bench results
```

## Stack

- Go (pinned in go.mod: `go 1.26.0`, `toolchain go1.26.10`), `jackc/pglogrepl` + `pgx`, `pganalyze/pg_query_go`.
- TypeScript on Node 26.13.0, pinned by pnpm (`useNodeVersion` in `pnpm-workspace.yaml`,
  `engines` in `package.json`); run node through pnpm (`pnpm node`, `pnpm exec`).
  **pnpm** only (never npm/yarn), pinned via `packageManager`.
- Postgres 18 (`wal_level=logical`), Electric `1.7.9` for the baseline.
- Docker compose for local Postgres + Electric.

## Commands

```sh
docker compose up -d && scripts/check-compose.sh   # Postgres 18 + Electric 1.7.9
go test -race ./...                                 # Go unit tests (some need compose)
go build -o bin/engine ./cmd/engine                 # engine serve | engine plan
pnpm check && pnpm lint                             # TS typecheck + lint
pnpm --filter @nightride/localsync-harness matrix --adapter=localsync,electric [--only=6] [--write]
pnpm --filter @nightride/localsync-harness bench [--write]
pnpm --filter @nightride/localsync-harness bench:deflate [--write]  # permessage-deflate on the engine's real messages
pnpm --filter @nightride/localsync-harness bench:fanout [--write]  # many clients, one warm unit (snapshot cache)
pnpm --filter @nightride/localsync-sim smash [--seed N] [--faults]
pnpm --filter @nightride/localsync-sim parallel --jobs 5 --seeds 100-119 --faults  # one DB + engine per job
pnpm --filter @nightride/localsync-browser smoke   # real localdb in headless Chrome via the transport
pnpm --filter @nightride/localsync-browser collab [--seconds 60]  # two people editing; torn renders and latency per tab
docker compose --profile gateway up -d gateway      # TLS nginx like the apps' (docker/gateway/README.md; needs .env)
pnpm --filter @nightride/localsync-harness gateway  # wss / https / auto / hostile-middlebox checks through it
# Any harness, sim or browser run through the gateway or over HTTP:
#   LOCALSYNC_URL=wss://<domain>:8443/sync  LOCALSYNC_TRANSPORT=websocket|http|auto
#   LOCALSYNC_BROWSER_VIA=gateway|hostile (browser)
```

The harness, `smash` and the browser smoke share the compose `localsync`
database (they TRUNCATE the fixture): run one at a time. `parallel` uses its
own `localsync_sim_N` databases, so it can run beside them. The Electric adapter imports localdb's real
client code from `../localdb` (sibling checkout).

## Code style

- TS: strict, ES modules, oxfmt + oxlint, no semicolons, single quotes, tabs.
  Format only the files you touched, never the whole repo.
- Go: gofmt / go vet; run tests with `-race`.
- **NEVER use em-dashes (U+2014)** anywhere: code, comments, docs, output.
  Use `-` or rephrase.
- Prefer `for (const x of items)` over `.forEach`.
- Locality of behaviour over DRY: duplicate a few lines rather than invent
  a shared abstraction. Pure utilities are the exception.
- Hot paths (WAL decode, per-txn routing, per-client frame assembly, wire
  codec, client apply): small dedicated functions, preallocated / reused
  buffers, no per-row allocations or maps, type-stable variables. Profile
  with pprof (Go) / `--trace-opt --trace-deopt` (V8) before and after.

## Working rules

- Never create branches or commits on your own; leave changes uncommitted
  and ask how the user wants them.
- Deliverables (design docs, capability matrices, bench reports) are
  markdown in this repo, not external artifacts.
- Capability tests assert observable client state against the oracle
  (the rule SQL run directly in Postgres), never protocol details.
- Any correctness shortcut must have a safe fallback (when analysis is
  unsure, re-query); a wrong rewrite is a visibility leak.
