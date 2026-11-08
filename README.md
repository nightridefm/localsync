# localsync

Realtime sync from Postgres into a local SQLite database in each client.

localsync reads Postgres' logical replication stream (the WAL). It decides
from SQL rules which rows each client may see, and streams the difference
to the client. The client keeps the rows in SQLite, so the app runs queries
locally and gets new data within milliseconds of the Postgres commit.

It is built for [`@nightride/localdb`](https://github.com/nightridefm/localdb)
(SQLite in the browser over OPFS, with workers) and replaces ElectricSQL
there. The engine is generic: nothing in it is specific to an app.

```
            writes (your API)
  app  ──────────────────────────►  Postgres 18
   ▲                                   │ logical replication (pgoutput)
   │ local SQL queries                 ▼
 SQLite  ◄── frames over WebSocket ── engine  ── rules.yaml
 (localdb)    or HTTP streaming        (Go)       (SQL per rule)
```

## What it guarantees

The capability harness and the simulation assert each of these against
an oracle: every rule's SQL run directly in Postgres.

- **Visibility.** A client is never sent a row, key or column outside the
  rules it is authorised for, not even for a moment. A revoked grant
  removes the rows in the same frame as the revoking transaction.
- **Atomicity.** The local database always equals the oracle at a single
  Postgres LSN. No transaction is ever seen half-applied, across any
  number of tables or rules, including while the client changes what it
  subscribes to.
- **Convergence.** After writes stop, every connected client reaches the
  oracle. This holds through engine restarts, dropped connections and a
  lost replication connection.
- **Read-your-writes.** A client can wait until a write it made is
  reflected locally.

## Compared with Electric

Measured on 2026-10-30 with the same localdb client code against both
engines, on the same fixture and oracle
([`docs/electric-comparison.md`](docs/electric-comparison.md),
[`docs/bench/`](docs/bench/README.md)). Electric 1.7.9 fails on these:

| | localsync | Electric 1.7.9 |
| --- | --- | --- |
| Transaction spanning two tables | always one local commit | 165 of 283 commits torn |
| Subscribe and unsubscribe mid-session | one commit | 4 of 6 commits showed neither project fully |
| 100k-row transaction | 2.4 s, atomic | timed out |
| Two rules on one table (role union) | yes | one shape per table in localdb |
| Token claims in rules, auth in the engine | yes | done in the app's proxy |
| Read-your-writes | data arrives before the confirmation | confirmation can arrive before the data |
| Initial load, 10k rows over two joins | 481 ms, 0.9 MB | 549 ms, 7.1 MB |
| Single-row update latency p50 / p99 | 1.6 / 5.3 ms | 2.2 / 22 ms |

[`docs/landscape.md`](docs/landscape.md) compares localsync with
PowerSync, Supabase Realtime, Zero and other WAL-based sync systems.

## How it works

The design rests on four ideas
([`docs/design.md`](docs/design.md) section 2):

1. **Everything is a unit delta.** A *unit* is a rule bound to parameter
   values, such as the tasks of project 42. Every unit is shared by all
   the clients that want it. Initial loads, live changes, move-ins,
   revocations and resyncs are all deltas made of `PUT`, `REMOVE` and
   `RESET`.
2. **One way to compute a delta.** Run the rule's SQL restricted to the
   rows whose membership might have changed, at a snapshot spliced into
   the stream. Analysis only narrows that restriction. When analysis is
   unsure, it falls back to `TRUE`, which is a slower query but never a
   wrong one.
3. **Atomicity is a frontier.** Each unit is known through some LSN. A
   client advances to the minimum LSN over the units it holds. A slow unit
   delays only its own clients, and nobody ever sees a mix of two states.
4. **One message: `WANT`.** The client sends its token, its rules and what
   it already has (a cursor and unit checksums). The engine sends the
   difference. Connect, reconnect, subscribe, token refresh and resync
   after a restart all use that same message.

Postgres is the only source of truth. Everything the engine holds is a
cache, so the replication slot is temporary. A dead or redeploying engine
never pins WAL, and there are no checkpoints or disk logs to manage.

## Quick start

### 1. Postgres

You need Postgres 18 with `wal_level=logical`. The engine connects as a
role with `REPLICATION` that owns the synced tables. At startup it:

- creates the `localsync` publication for exactly the tables your rules
  read;
- sets `REPLICA IDENTITY FULL` where it needs the old row values;
- creates a one-row `sync.fence` table.

For a local stack:

```sh
docker compose up -d --wait
```

### 2. Rules

```yaml
# rules.yaml
rules:
  directory/users:
    sql: SELECT id, name FROM "user"
    public: true                     # any valid token
  admin/documents:
    sql: SELECT * FROM document
    claims: admin                    # token grants must include "admin"
  member/documents: >                # $sub is the token's subject
    SELECT * FROM document WHERE id IN (
      SELECT document_id FROM document_team WHERE team_id IN (
        SELECT team_id FROM team_member WHERE user_id = $sub))

groups:
  project:
    params: [project]                # supplied by the client
    gate: >                          # ANDed into every rule of the group
      EXISTS (SELECT 1 FROM project_grant
              WHERE user_id = $sub AND project_id = $project)
    rules:
      project/tasks: SELECT * FROM task WHERE project_id = $project
      project/comments: >
        SELECT * FROM comment WHERE task_id IN
          (SELECT id FROM task WHERE project_id = $project)
```

A rule is one SQL `SELECT` returning rows of one table with its primary
key. Filters can use subqueries, `EXISTS`, `NOT IN`, gates and views over
any tables. Aggregates, computed columns and functions that are not
`IMMUTABLE` are refused at load. Every rule must say who may want it
(`$sub`, `claims` or `public`), or it fails to load.

`engine plan` prints each rule's access, evaluation strategy and
dependency tables. It then runs every rule against the database, so a
broken rule fails the deploy rather than a user's session:

```sh
go build -o bin/engine ./cmd/engine
bin/engine plan --pg "$LOCALSYNC_PG_URL" --rules rules.yaml
```

### 3. The engine

```sh
docker build -t localsync-engine .
docker run -p 3140:3140 \
  -e LOCALSYNC_PG_URL=postgresql://... \
  -e LOCALSYNC_JWT_SECRET=... \
  -v ./rules.yaml:/rules.yaml \
  localsync-engine
```

| Setting | Default | |
| --- | --- | --- |
| `LOCALSYNC_PG_URL` / `--pg` | | Postgres URL (needs `REPLICATION`) |
| `LOCALSYNC_JWT_SECRET` | required | HS256 secret shared with your token endpoint |
| `LOCALSYNC_JWT_AUDIENCE` / `--audience` | `localsync` | required `aud`; use a different one per deployment |
| `--rules` | `rules.yaml` | |
| `--listen` | `:3140` | |
| `--window` | `5m` | in-memory resume window |
| `--workers` | `4` | concurrent snapshot queries |

Every optimisation has a switch that turns it back into the slow path
(`LOCALSYNC_SNAP_CACHE=0` and others, listed in
[`docs/design.md`](docs/design.md#8-engine) section 8).

Endpoints: `/sync` (WebSocket), `/sync/http` (HTTP streaming and long
polling), `/health`. `/metrics`, `/debug/*` and pprof share the listener,
so expose only `/sync` and `/sync/http` through your gateway.
`engine health` is the container healthcheck, because the image has no
shell.

### 4. Tokens

Your app server mints a short-lived HS256 JWT with `sub`, `exp`, `aud`
and optional `grants: [...]`, using the same checks its own routes use.
The client fetches it from `tokenUrl` and refreshes it before it expires.
Changed claims take effect at the next refresh. Use gate tables for
anything that must be revoked at once.

### 5. The client

```ts
// writer worker entry
import { registerSyncTransport } from '@nightride/localdb/writer.worker'
import { localsyncTransport } from '@nightride/localsync'
registerSyncTransport('localsync', localsyncTransport)
```

```ts
// page
const db = new LocalDb({
	// ...schema, readers
	alignCommits: true, // every live query re-renders from one commit
	sync: {
		transport: 'localsync',
		options: {
			url: 'wss://example.com/sync',
			tokenUrl: '/api/sync-token',
			wants: [{ rule: 'member/documents' }, { rule: 'project/tasks', params: { project: id } }],
		},
	},
})

// switching projects is one atomic swap
db.syncCommand({ want: [{ rule: 'project/tasks', params: { project: next } }] })
```

The client is published as a tarball on each
[release](https://github.com/nightridefm/localsync/releases). Its status
is one of:

- `connecting`
- `live`: caught up
- `error`: the connection dropped and the client retries; the status
  carries the real reason and the retry delay
- `failed`: some wants are not served, each with its error and whether
  the refusal is permanent

On the default `transport: 'auto'`, a client that cannot keep a WebSocket
open falls back to HTTP streaming, then to long polls. This covers proxies
that strip `Upgrade`, cut long-lived sockets or buffer whole responses. The
client remembers which mode worked.

## Verification

Every check compares what a client can observe with an oracle: the rule's
SQL run directly in Postgres.

| | What it does |
| --- | --- |
| Capability harness | 30 scenarios, each comparing client state with the oracle ([`docs/capability-matrix.md`](docs/capability-matrix.md)) |
| Smash simulation | randomised workloads, many clients and subscription churn, with engine and walsender kills |
| Browser smoke | real localdb in headless Chrome over OPFS and workers |
| Hostile suite | leak attempts with hostile parameters, bad tokens, malformed frames, connection and unit exhaustion |
| Security checks | govulncheck, staticcheck, gosec, pnpm audit, and fuzzers for the wire codec, SQL literals and parameters |
| Gateway checks | WSS, HTTPS and a hostile middlebox through a TLS nginx |

Commands are under [Development](#development).

## Scope

localsync only reads from Postgres. Clients write through your own API,
the same way they already do. It does not cover:

- client writes through the engine;
- offline mutation queues;
- presence or broadcast channels;
- more than one source database per engine (run one engine per database).

Clients connect to the engine directly. Fan-out relays, priority classes
and persistent slots are designed but not built; each is listed in
[`docs/design.md`](docs/design.md#14-not-built-yet) section 14 with the
measurement that would justify it.

## Development

Go (pinned in `go.mod`, cgo for the embedded Postgres parser), Node 26
through pnpm (pinned in `pnpm-workspace.yaml`), Docker for Postgres 18.

```
cmd/engine/      engine: serve | plan | health
internal/        replication decode, rules analysis, engine, wire codec
ts/transport/    @nightride/localsync: the client and localdb transport
ts/harness/      capability harness, benchmarks, hostile suite, gateway checks
ts/browser/      headless Chrome smoke and collab soak
sim/             smash simulation
docs/            design, capability matrix, benchmarks
```

```sh
docker compose up -d --wait  # Postgres 18 with wal_level=logical
pnpm install
pnpm test                    # Go tests (-race, some need compose) + client tests
pnpm check && pnpm lint      # go vet + TS typecheck, oxlint
go build -o bin/engine ./cmd/engine

pnpm harness matrix [--only=6] [--write]
LOCALSYNC_TRANSPORT=http pnpm harness matrix   # the same over HTTP; run both
pnpm harness bench | bench:dense | bench:fanout | bench:deflate | bench:movein [--write]
pnpm harness hostile
pnpm sim smash [--seed N] [--faults]
pnpm sim parallel --jobs 5 --seeds 100-119 --faults
pnpm browser smoke
pnpm browser collab [--seconds 60]
scripts/security.sh          # FUZZTIME=20s, SKIP_FUZZ=1
```

The harness, `smash` and the browser runs share the compose `localsync`
database and truncate its fixture, so run them one at a time. `parallel`
gives each job its own database and engine, so it can run beside them.
The harness and the browser runs use localdb's real code from a sibling
checkout (`../localdb`). `--write` regenerates the matching
report in `docs/`. Every tool builds `bin/engine` from the source first;
`LOCALSYNC_SKIP_BUILD=1` runs whatever is there instead (another version,
for comparison).

Through a TLS gateway like a production one
([`docker/gateway/README.md`](docker/gateway/README.md), needs `.env`):

```sh
docker compose --profile gateway up -d gateway
pnpm harness gateway
# any harness, sim or browser run, through the gateway or over HTTP:
LOCALSYNC_URL=wss://<domain>:8443/sync LOCALSYNC_TRANSPORT=websocket|http|auto
LOCALSYNC_BROWSER_VIA=gateway|hostile
```

The engine image is built from the repository (`docker build -t
localsync-engine .`); a version tag releases the engine and the client
tarball together (`.github/workflows/release.yml`).

## License

MIT
