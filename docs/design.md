# localsync design

How the engine and client work, and why. The [README](../README.md) covers
what localsync is and how to run it. This document is for changing it.

The design is four ideas (section 2). Snapshots, move-ins, revocation,
resync, subscribe and unsubscribe all fall out of them as special cases
instead of being built separately. Every optimisation is a cache or a
shortcut over that core, never a new semantics, and each has a switch that
turns it back into the slow path (section 8).

## Contents

1. [Guarantees](#1-guarantees)
2. [The four ideas](#2-the-four-ideas)
3. [Rules](#3-rules)
4. [Computing deltas](#4-computing-deltas)
5. [Snapshots and the splice](#5-snapshots-and-the-splice)
6. [Frontiers](#6-frontiers)
7. [Reconcile](#7-reconcile)
8. [Engine](#8-engine)
9. [Wire format](#9-wire-format)
10. [Client](#10-client)
11. [Auth](#11-auth)
12. [Postgres footprint](#12-postgres-footprint)
13. [Verification](#13-verification)
14. [Not built yet](#14-not-built-yet)

## 1. Guarantees

The harness and simulation assert exactly these.

- **G1 Visibility.** A client never holds, and is never sent, a row, PK
  or column outside the result of a rule it is authorised for at an LSN it
  has passed. Not even transiently.
- **G2 Atomicity.** Every client-visible state equals the oracle (each
  wanted rule run directly against Postgres, unioned per table) at one
  LSN. No Postgres transaction is ever seen half-applied, across any
  number of tables or rules, including while subscriptions change.
- **G3 Convergence.** After writes stop, every connected client reaches
  the oracle at head, whatever restarts, disconnects or slot losses
  happened.
- **G4 Read-your-writes.** Given a write marker, a client can wait until
  its state reflects that write (section 12).

Out of scope: client writes through the engine, offline mutation queues,
and more than one source database per engine (run one engine per
database).

## 2. The four ideas

### Idea 1: everything is a unit delta

A **unit** is a rule bound to parameter values: a set of rows of one table,
defined by SQL. All clients wanting the same rule with the same parameters
share one unit.

Everything the engine sends is a **delta** that moves units to an LSN.
A live transaction, an initial load, a move-in, a re-run, a resync,
an unsubscribe and a revocation are all deltas. A delta has three ops:
`PUT row` (the row is in the unit with these values), `REMOVE pk` (it is
not), and `RESET` (the unit's membership is exactly the PUTs that follow in
this frame). The client has exactly one operation: apply a frame of deltas
in one SQLite transaction and advance its cursor.

### Idea 2: one way to compute, restricted re-evaluation

Any change to a unit can be computed by running the rule SQL **restricted
to the rows whose membership might have changed**, at a snapshot spliced to
the stream (section 5). Initial load and resync are the restriction
`TRUE`. A change to a link table is "rows reachable from the changed link
values". An update to the rule's own table is `pk = ANY(changed)`.

Analysis (section 4) never decides membership. It only produces the
restriction, and any superset of the affected rows is correct, so
the safe fallback is always `TRUE`. Evaluating a restriction in memory
instead of in Postgres (on the WAL row) is a speed-up of the same
computation, verifiable against it.

### Idea 3: atomicity is a frontier

Each unit is **known through** some LSN. A client's cursor is the
**minimum** over the units it holds, and the engine only sends a client
frames up to that minimum. A unit waiting on a query just lags; its
clients wait, nobody else does. This single rule gives atomic transactions
across units, atomic subscribe and unsubscribe, atomic move-ins, and safe
coalescing (a frontier that jumps skips intermediate states, never mixes
them).

### Idea 4: one connection message, reconcile

The client sends `WANT`: its token, the rules (with parameters) it wants,
and what it has (cursor and per-unit checksums). The engine sends the
difference. Connect, reconnect, subscribe, unsubscribe, token refresh,
resync after an engine restart and a project switch are all `WANT`
with different contents. The bytes sent are the measured difference
between what the client has and what it should have.

### Consequence: the engine holds only caches

Postgres is the only truth. Everything in the engine (unit state, the
recent-transaction window, snapshots) is a cache. Losing any of it (crash,
deploy, a dropped replication connection) costs Postgres reads, never
correctness: the engine rebuilds lazily and clients reconcile by checksum.
That is why the replication slot can be temporary (section 12) and why
there is no disk log, no checkpoint and no retention policy.

## 3. Rules

### 3.1 Format

One `rules.yaml` per deployment, owned by the app:

```yaml
rules:
  admin/documents:
    sql: SELECT * FROM document
    claims: admin
  member/documents: >
    SELECT * FROM document WHERE id IN (
      SELECT document_id FROM document_team WHERE team_id IN (
        SELECT team_id FROM team_member WHERE user_id = $sub))
  directory/users:
    sql: SELECT id, name, has_avatar FROM "user"
    public: true

groups:
  project:
    params: [project]
    gate: >
      EXISTS (SELECT 1 FROM project_grant WHERE user_id = $sub
              AND project_id = $project AND role IN ('owner','editor','viewer'))
    rules:
      project/tasks: SELECT * FROM task WHERE project_id = $project
      project/comments: >
        SELECT * FROM comment WHERE task_id IN
          (SELECT id FROM task WHERE project_id = $project)
```

A group's gate is ANDed into each of its rules before analysis, so every
rule is still one plain SQL query: the same text is the config, the spec
and the oracle.

### 3.2 Access and parameters

- **Every rule declares who may want it**, or fails to load. With no
  routes in front of the engine, any valid token can want any rule, so an
  undeclared rule would be readable by everyone:
  - `$sub` in its SQL: rows or a gate scoped to the principal.
  - `claims: [a, b]`: the token's `grants` must include one of them.
  - Both: `claims` and `$sub`.
  - `public: true`: any valid token.

  `engine plan` prints this as the Access column, which is the review
  surface. It calls a condition "rows" only when the condition provably
  reads the row's own columns.
- `$sub` and `grants` come from the token. The app's token endpoint mints
  it with the same checks its own routes run, so coarse rights (admin,
  curator, tenant member) stay in the app's code.
- **Claims are checked at every `WANT`**, all wants in one pass, in
  memory. A refused want gets an error (`not allowed`), so apps can tell
  "not allowed" from "empty". A token refresh is a new `WANT`, so a grant
  missing from the new token drops its units in the next frame. Claims
  are therefore as fresh as the token: keep it short-lived (minutes), and
  gate on tables for anything that must be revoked at once.
- **Client parameters** (`$project`) come from `WANT`. A rule that uses
  one must be constrained by `$sub` or claims, or be marked `public:
  true`.
- Named parameters are rewritten to `$1..$n` by a lexer-aware pass
  (skipping literals, quoted identifiers, dollar quoting and comments)
  before parsing with `pg_query_go`; types are inferred from use sites.

### 3.3 Gates

A top-level `EXISTS` / `NOT EXISTS` conjunct that never references the
rule's table (a per-project permission check) is a **gate**: membership is
`body AND gate`, where the body is the rule without its gates. Gates do
not read the row, so the engine serves a gated rule as separate units:

- one **body** unit, keyed by the parameters the body reads. When `$sub`
  appears only in gates, that is one unit per project, shared by every
  viewer.
- one small **gate unit** per gate, keyed by the parameters the gate
  reads: the gate's subquery as a rule of its own (`EXISTS (SELECT 1 FROM
  t WHERE ...)` becomes `SELECT * FROM t WHERE ...`), which holds when it
  is non-empty (`NOT EXISTS`: when empty).

Body and gate units are ordinary units, so routing, restrictions, gaps,
rounds and frontiers need nothing new; history entries record the member
count, so a gate's value at any frame LSN is a lookup. Per viewer, a
frame sends the body's ops while its gates hold, an empty `RESET` when one
closes, and the body as a snapshot (usually from the snapshot cache) when
they open again, with the frontier held at the client's cursor until it
can be delivered. A permission change re-evaluates only a tiny gate unit,
never the body, and the body keeps its own strategy: on both apps' real
rules, every gated rule's body is evaluated on the row or by the same
restricted queries as before the split.

Gates are ordinary SQL, maintained like any other unit, so deleting a
`project_grant` row revokes the units in the same frame as the deleting
transaction.

The split is decided at rule load and is fail-safe: it is purely
syntactic (top-level conjuncts), and the rule is served unsplit if a
gate's subquery is not one table, or a derived rule fails analysis or
Postgres' `PREPARE` (an unqualified name in a gate that resolved to the
outer table would change meaning once the gate stands alone). Unsplit,
a gate is just a subquery: correct, re-evaluated in full on every change
to its tables. Derived rules are never wantable.

### 3.4 Accepted rules

Any filter over tables in `public`; the output must be rows of one table,
including its PK, with plain (not computed) columns, so the client can
store them by PK. Joins live in subqueries and gates. Top-level semijoins
are normalised to `EXISTS`; top-level aggregates, window functions, set
operations and computed columns are rejected. `ORDER BY ... LIMIT` is
accepted and always re-evaluated with restriction `TRUE`.

**Views** are expanded: before analysis, every view a rule reads is
replaced by its definition as a subquery, recursively, so analysis, the
publication and the queries only ever see tables (the engine must decode
every table a rule reads). A view is a named subquery, so this changes no
result. A rule's own `FROM` must still be a table, and a
`security_barrier` view is refused, since inlining it would drop its
protection. Any other relation (another schema, an unknown name, a CTE
inside a subquery) is refused at load, so `engine plan` catches what
`engine serve` could not publish.

Every function must be `IMMUTABLE`, and SQL value functions
(`CURRENT_TIMESTAMP`) are rejected: anything else (`now()`, `random()`,
`current_setting()`, sequences, a user function reading another table)
can change visibility without WAL. Virtual generated columns (Postgres
18's default kind) are never replicated, so rules must not filter on
them.

### 3.5 `engine plan`

`engine plan --rules rules.yaml` prints, per rule, its access (section
3.2), tier and evaluation strategy (section 4.2), and dependency tables.
`--max-tier` makes it exit non-zero when a rule needs Postgres re-queries
for its common changes, for CI.

It also runs every rule the way the engine will serve it (the Run
column): a split rule as its body and each gate, any other as itself,
each as a whole-unit query and as a restricted one, through the engine's
own query code, read only and at most one row, with a sample value of
each parameter's inferred type. A split rule's tier is its body's.
Analysis accepts SQL that fails only when run (a cast of a parameter),
and those failures belong at deploy, not in a user's browser.
`--run=false` skips it.

## 4. Computing deltas

### 4.1 Restrictions

For each transaction, for each table it changes, analysis maps each
changed row to the units it can affect and a restriction per unit:

| Change | Restriction (rows of the rule's table to re-check) |
| --- | --- |
| row of the rule's own table | `pk = ANY(changed pks)` |
| row of a table inside a monotone chain (`IN`, `EXISTS`, equi-join) | the **delta query**: the chain with the changed row's values substituted, e.g. `id IN (SELECT document_id FROM document_team WHERE team_id = $changed)` |
| row of a table under a non-monotone node (`NOT IN`, `NOT EXISTS`, aggregates, `LIMIT`) | `TRUE` (re-evaluate the unit) |
| anything analysis does not understand | `TRUE` |

Which units are affected comes from the same substitution: a change to
`team_member(user_id = U)` affects only units of rules over `team_member`
bound to `$sub = U`. When analysis cannot narrow the units, every unit of
the rule is affected.

### 4.2 Evaluation strategies

A restriction is evaluated one of two ways, chosen per rule by analysis.
They are speed-ups of one computation, not separate mechanisms:

1. **On the row** (tier 1). The predicate is row-local over a whitelist of
   operators whose Postgres semantics the engine reproduces exactly (ints,
   numerics, uuids, bools, timestamps, `C`-collated text, `IS [NOT] NULL`,
   `IS DISTINCT FROM`, `IN (list)`, `= ANY`, three-valued `AND/OR/NOT`).
   Old and new row give "was in" and "is in" directly.
2. **In Postgres** (tiers 2 and 3). The restricted query at a spliced
   snapshot, diffed against the unit's stored per-column hashes for the
   restricted rows. Tier 2 rules are monotone chains that maintained sets
   could serve from memory (section 14); tier 3 needs Postgres for good.
   Anything outside the tier 1 whitelist (other collations, `citext`,
   `LIKE`, jsonb operators) is a restricted query too, `pk = ANY(changed)`.

**Column pruning.** Membership is a function of the columns a rule reads,
so an update that changes none of them cannot change any unit's
membership: on the rule's table it becomes a direct `PUT` for members, on
a table inside its subqueries it is skipped. Analysis records these
columns per table (`RefCols`, an over-approximation by name, which is
always safe).

The order of rows inside a transaction never matters: restricted queries
run after the whole transaction has been routed, at a snapshot that
contains all of it.

## 5. Snapshots and the splice

The one correctness primitive: evaluate a (restricted) rule at a snapshot
that lines up exactly with the stream.

```sql
BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY;
SELECT pg_current_snapshot(), pg_current_wal_insert_lsn();  -- S, L_S
SELECT ...;                                                 -- restricted rule
COMMIT;
```

The snapshot is fixed at the first statement's start and `L_S` is read
after it, so every transaction visible in `S` committed below `L_S`. For a
streamed transaction `X` with commit LSN `c(X)`:

- `c(X) >= L_S`: not visible, apply it on top of the result;
- `c(X) < L_S`: visible iff `xid(X) < xmax` and not in `xip`.

Filtering is per xid, not by an LSN cut, because a commit record can be
written before the transaction leaves the ProcArray. Stream xids (32-bit)
are widened to `xid8` with the epoch of the snapshot's `xmax`.

**Regular snapshots.** Let `B` be the largest commit LSN among visible
transactions (known once the stream passes `L_S`). The snapshot is
**regular** if no invisible transaction committed below `B`; then the
result is exactly "the database at `B`" and the unit can be declared known
through `B`. Irregular snapshots (a tiny race window) are retried. This is
what lets query results join the frontier (section 6) without tearing.

**Fences.** A result must wait until the stream has passed `L_S`. If the
last routed commit record ends at or after `L_S`, every earlier commit has
been streamed and nothing waits. Otherwise, when nothing else is
committing, the engine updates `sync.fence` and waits to see it in the
stream. Fences are written only on demand.

The splice is used for everything slow: unit creation (restriction
`TRUE`), move-ins, restricted queries and reconcile. Clients waiting on
the same unit share one query, and a bounded pool of query workers caps
the load on Postgres.

## 6. Frontiers

- Each unit has `known(u)`, an LSN. A unit whose deltas are computed on
  the row is known through every transaction as soon as it is routed.
- **Pending units.** A transaction `T` that gives `u` a query-dependent
  delta makes `u` **pending**: `known(u)` stays below `c(T)`. The pipeline
  keeps going, and later changes to `u` are buffered. When the restricted
  query returns at a regular boundary `B`, `u` becomes known through `B`:
  rows inside the restriction come from the query result, rows outside it
  from the buffered changes up to `B`.
- **Full re-evaluation is the universal fallback.** A restriction that
  committed before `B` but was routed after the query was issued is not
  covered by its result, so the unit is re-evaluated whole. A whole-unit
  evaluation covers everything up to its own boundary, so it always
  resolves.
- **Resolution gaps.** A query resolution is a *jump*: the unit's state is
  known at `p` (just before its first pending transaction) and at `B`, but
  undefined strictly between them. History entries record that gap, and a
  frame may only land on an LSN inside no held unit's gap, so the frontier
  is "the largest LSN `<= min(known)` valid for every unit", not just the
  minimum. With plain `min(known)`, one unit resolved at 100 and another
  still at its state before 100 could share a frame at 104: a torn
  transaction.
- **Snapshot rounds.** Gaps alone are correct but could stall a client
  whose units keep resolving at different `B`s. All queries scheduled
  together run in one round: a leader transaction exports its snapshot
  (`pg_export_snapshot()`) and workers import it, so units resolved in the
  same round share `B` and their gaps end together.
- **Frames.** A client's frontier is the valid `min(known(u))` over its
  units. The engine sends a frame when the frontier advances, covering
  everything up to it. Ops for transactions the client skips over are
  merged (one net op per row), which is the coalescing.
- **Head-of-line blocking** is per client and only behind its own pending
  units; the global pipeline never waits. Per-unit frontiers would remove
  it but cost cross-unit atomicity. Measured, the engine sent a 20,000-row
  move-in 36 ms after the grant; the rest was the client applying it,
  which the bulk apply path (section 10.2) brought down to 215 ms.
- **Failing units hold no one back.** A query that fails (a rule that
  errors for some parameters, a gate Postgres refuses) is retried as a
  whole-unit evaluation with backoff (250 ms doubling to 30 s), the unit
  staying in flight meanwhile so nothing else queries it. The first
  failure is retried silently; from the second, every hold on the unit
  (or on one of its gate units) is **broken**: left out of its client's
  frontier, held empty on the client (a `RESET` in the next frame) and
  reported (section 7). The client's other units carry on. When a query
  for the unit succeeds again, its holds get a fresh snapshot and rejoin
  the frontier like a new want.

Transactional subscription changes fall out: a new unit starts pending,
the client's frontier waits for it, then one frame carries the new unit's
`RESET` and rows, the removal of dropped units, and any live changes, all
at one LSN. A project switch of about 118 units out and 118 in is one
`WANT` and one frame.

## 7. Reconcile

`WANT` carries the token, the wants `[(id, rule, params)]`, and the haves:
the engine epoch and cursor the client last saw, and a checksum per held
unit.

The engine resolves wants to units (refusing unauthorised ones) and
answers per unit:

| Client has | Engine sends |
| --- | --- |
| same epoch, cursor inside the unit's history | the unit's history from the cursor |
| anything else (older cursor, new epoch, after an engine restart) | a snapshot at a regular boundary; equal checksum: nothing, just known through `B`; different: `RESET` + rows |
| unit not held | `RESET` + rows |
| unit held but not wanted, or no longer authorised | the client drops it inside the next frame's transaction (the engine forces one) |

The client owns dropping units it no longer wants. A client that swapped
projects and disconnected before the removal frame arrived would
otherwise keep the old unit's rows forever.

**Refused and failing wants.** The `UNITS` answer lists refused wants
(`errors`: unknown rule, not allowed, a missing or unknown parameter),
which this `WANT` will never get, apart from failing ones (`failures`:
broken holds, section 6), which the engine keeps retrying. Later changes
to the failing list come as a `FAILURES` message. The client reports
both to the app (section 10.1).

**Caught up.** After the frame in which every hold of the latest `WANT`
is delivered (or broken), the engine sends `SYNCED`, once per `WANT`. The
client reports `live` only after that frame is applied: `live` means the
local database holds every unit it can have, at one LSN.

**Haves belong to a principal.** The client names each unit by (rule,
params, the token's `sub`), so when another user signs in on the same
local database none of the first user's units match. They are held but
not wanted, so the first frame drops them, and the new user's units
arrive whole. Naming units by rule and params alone would resume the first
user's state as the second's.

**Every `WANT` is a new connection.** A project switch, a token refresh
or an engine restart closes the connection and opens another with the new
`WANT`; the engine refuses a second `WANT` on one connection. The client
writes its haves from what it has applied, and frames still in flight on
the old connection are dropped with it, so the haves are exactly the
client's state and the engine can trust them. The price is one handshake
per want change; locally it does not show in a project swap.

Anything the client has that the engine does not hold after a `WANT`
(no longer wanted, refused, or wanted under a token that lost a grant)
makes the engine force the next frame, inside which the client drops it.

**Checksums.** A column hash is a 64-bit FNV pair over (column index,
canonical wire bytes of the value), so client and engine hash identical
bytes; a row hash is the sum of its column hashes (so `PATCH` can carry
the new row hash after changing a few columns). A unit's checksum is
`sum(mix(row hash)) mod 2^64` over its members, `mix` being the
splitmix64 finalizer. The mix matters: without it the checksum is a sum
over (column, value) pairs and cannot see values moving between rows (two
projects swapping orgs), and an equal-checksum reconnect would keep stale
rows. The checksum stays additive per row: `PUT` adds the new row's term
and subtracts the old (the engine keeps per-column hashes per member),
`REMOVE` subtracts. Every fragment carries its unit's checksum, so
checksums are also an always-on production verifier.

The recent history is in memory (default 5 minutes, at most 32 MiB per
unit) and serves brief disconnects only. Everything older reconciles by
checksum, which after an unchanged deploy sends nothing but frontier
updates.

## 8. Engine

```
 Postgres 18 ── temporary slot, pgoutput ──> [decode] -> [route: row eval, restrictions]
      ^                                                       |
      |  spliced restricted queries (RR, bounded pool)        v
      +──────────────────────────────── [query workers] <- [units + frontiers]
                                                              |
                                          [ops encoded once per unit] -> [unit history]
                                                              |
                                           [per-client writers] -> WebSocket / HTTP -> clients
```

- **One loop goroutine** owns all engine state: it routes transactions,
  applies query results and builds frames, so there are no locks.
- **Query workers**: a bounded pool (default 4) running spliced queries.
- **Per-client writers**: one goroutine per socket. A slow client never
  slows the loop; one that falls behind its unit's history gets a fresh
  snapshot.
- **Boot** has no special path: create a temporary slot, start streaming,
  then create units lazily as `WANT`s arrive, each by splice. An engine
  with no clients streams and discards. The epoch is a random id per slot.
- **Encode once**: a unit's ops for a transaction are encoded once and
  shared by every client holding the unit.
- **Units linger** for the resume window after their last client leaves
  (at most 10,000 idle units), so a reconnect follows the unit's history
  instead of re-snapshotting.
- **Memory is bounded per client and per unit**, never by trusting the
  peer: a client's send queue is capped at 64 MiB (a client that cannot
  keep up is disconnected and reconciles), a unit keeps at most 32 MiB of
  history (holds behind what was dropped get a fresh snapshot), and one
  `WANT` names at most 2000 units. Big frames are not built whole: the
  writer pulls ~1 MiB parts from the shared op bytes while its queue is
  below 4 MiB.
- **Query results are not copied**: rows are encoded straight into a slab
  per query with their column hashes, and members, snapshot ops and
  frames all reference those bytes.
- **Snapshot cache**: a unit's latest full snapshot is kept for a minute
  (256 MiB budget, oldest dropped first), so a client joining a warm unit
  (a second tab or device, a reconnect storm) needs no query: it gets the
  cached snapshot plus the unit's history since. It is used only when it
  is joinable exactly like a fresh one (the unit is resolved past it and
  its history reaches back to it). Concurrent waiters already share one
  query, so the cache matters for staggered arrivals: 20 staggered clients
  on two units need 0 snapshot queries instead of 40
  (`docs/bench/fanout.md`). It holds the result slabs, so it accounts for
  most of the live heap after a big load; set `GOMEMLIMIT` to bound RSS.
- **A lost replication stream restarts the engine in process**
  (walsender killed, Postgres restarted or failed over, network). Engine
  state is a cache, so it is dropped whole (the `epochState` struct): a
  new slot under a new epoch, setup retried with backoff (250 ms to 10 s)
  until Postgres is back, queries still in flight discarded. Then every
  client connection is closed cleanly (WebSocket close code 1000, or the
  end of the HTTP response), and clients reconnect at once. Their `WANT`s
  carry the old epoch and so reconcile by checksum like any reconnect.

**Switches.** Each optimisation can be turned off, which leaves the
engine computing the same result the slow way. They exist for A/B
measurement and as safety valves:

| Variable | Off means |
| --- | --- |
| `LOCALSYNC_SPLIT=0` | no split: gates are evaluated as subqueries of one unit per principal |
| `LOCALSYNC_SNAP_CACHE=0` | every join runs a snapshot query |
| `LOCALSYNC_EQUAL_SKIP=0` | units whose checksum matches on reconnect are resent anyway |
| `LOCALSYNC_WS_COMPRESSION=0` | no permessage-deflate |
| `LOCALSYNC_VERIFY=1` | (on) after every resolution, re-evaluate the unit in full and log any difference from its incremental state |
| `LOCALSYNC_DEBUG=1` | (on) debug logging |

**Observability.** `/metrics` (Prometheus text, no client library):

- Gauges: clients by transport; units, held units and units with pending
  changes; commit lag (seconds from commit in Postgres to routing, for
  the last transaction); the furthest-behind client's WAL bytes; the
  engine's head LSN.
- Counters: transactions, row changes, WANTs and refused wants, snapshot
  queries and cache hits, in-process restarts, messages and bytes sent.
  Counters stay monotonic across in-process restarts.
- Also `/debug/pprof`, `/debug/units` and the rule plan, all on the
  engine's own port: put only `/sync` and `/sync/http` behind the public
  gateway, and scrape the rest privately. `engine health` checks
  `/health` for container healthchecks (the image is distroless: no
  shell, no curl).

## 9. Wire format

One connection per client: a WebSocket, or HTTP where WebSockets fail
(section 9.1); the messages are the same.

| Direction | Message | Body |
| --- | --- | --- |
| client | `WANT` (JSON) | token, wants `[(id, rule, params)]`, haves `(epoch, cursor, [(id, checksum)])`; always the full set |
| engine | `UNITS` (JSON) | per want: client id, engine ref, rule, table, columns, PK positions; refused wants (`errors`) and failing ones (`failures`), by id |
| engine | `FAILURES` (JSON) | the failing wants changed: the full list, by id |
| engine | `SYNCED` (JSON) | caught up on the latest `WANT`: follows the frame that completed it (a response never ends between them) |
| engine | `FRAME` (binary) | lsn and fragments; an empty frame just advances the cursor |
| engine | `ERROR` (JSON) | message |
| engine | `BEAT` (JSON) | nothing sent for 15 s; liveness only |

Control messages are rare and small, so they are JSON text; only frames
are binary. Unit ids are chosen by the client, one per distinct (rule,
params, principal) and never reused, so a want that changes parameters is
a new id, and removals and additions in one frame cannot collide.

A big frame (an initial load) is sent as several `FRAME_PART` messages
ending with a `FRAME`. The client buffers parts and applies the whole
frame in one transaction, so a big load spans many messages but is still
one change on screen. A unit's ops may be split across parts; the
`continues` flag says more follow, and its checksum is checked only on
the last piece.

```
message  = type lsn count fragment*        type: 1 FRAME, 2 FRAME_PART
fragment = ref flags checksum count op*    flags: RESET, CONTINUES; checksum: u64 LE
op       = kind len body                   PUT row | REMOVE pk | PATCH patch
patch    = u64 row hash | pk | changed bitmap | null bitmap | changed values
row      = null bitmap, value*             positional, the unit's columns
```

Integers are LEB128 varints. Values are typed by the unit's column
descriptor, never self-describing: zigzag varints, `bool` in the bitmap,
`uuid` 16 bytes, `timestamptz` int64 µs, `date` as its text form
(`2026-07-03`), `float8` 8 bytes, `numeric` as text, `text`/`bytea`
length-prefixed, `json`/`jsonb` as text, arrays as count and elements,
anything else (PostGIS `geometry`, pgvector `halfvec`) as its text form
unless projected away.

`PATCH` carries only the columns whose hash changed, plus the new row
hash, so the client needs no row bytes to verify it. A one-int update of
a 12 kB row is 49 bytes. Resolutions always send `PUT`: a `PATCH` is
relative to the unit's own history, and resolutions are what bring
clients that joined mid-gap onto that history.

Messages of 512 bytes or more are compressed with permessage-deflate,
without context takeover. Initial loads and move-ins shrink about 40%;
small deltas would grow about 10%, so they go out as they are. Context
takeover would shrink them another 35% but keeps a compressor per
connection (hundreds of kB in Go), too much at thousands of sockets
(`docs/bench/deflate.md`).

### 9.1 Transports

- **WebSocket** (`/sync`) is the default. Each connection carries one
  `WANT` (section 7).
- **HTTP** (`/sync/http`) is the same protocol for networks where
  WebSockets fail: proxies that strip `Upgrade`, cut long-lived sockets,
  or hold responses until they are complete (antivirus and DLP proxies).
  - Each request is a connection. Its body is one `WANT`. Its response
    streams the engine's messages as records: a kind byte (0 text, 1
    binary, `0x80` set when the payload is raw deflate), the length as 4
    bytes little-endian, the payload.
  - Records of 512 bytes or more are deflated alone. The response is
    `application/octet-stream`, so gateways' `gzip_types` leave it alone,
    and compression works even where a gateway strips `Accept-Encoding`.
  - A stream ends after 5 minutes. A new `WANT` (new wants, a fresh token)
    is a new request. Either way the client re-POSTs at once and resumes
    from its cursor, which costs about a millisecond.
  - `?mode=poll` ends the response after the first frame with unit data,
    or after 20 s. Behind a proxy that buffers whole responses, every
    long poll still delivers, with no extra latency: the response ends
    the moment there is something to send.
  - The engine sends `X-Accel-Buffering: no` (nginx then streams even
    where `proxy_buffering` is on), `Cache-Control: no-store,
    no-transform`, and `Access-Control-Allow-Origin: *`. The token is in
    the body, never a cookie, and a plain-text body needs no CORS
    preflight.
- **Choosing** (`transport`, default `auto`):
  - The client starts on WebSocket. Two WebSocket attempts in a row that
    never deliver a message switch it to HTTP streaming.
  - A stream with no message within 5 s is being held back by a proxy:
    the client switches to long polls.
  - When HTTP fails too (the engine is down, not the network), the client
    alternates between transports until one answers.
  - The mode that works is stored in `_lsync_state`, so the next app load
    on that database starts there. WebSockets are tried again after 10
    minutes.
- **Liveness**: the engine sends `BEAT` on any connection idle for 15 s,
  because nginx's default `proxy_read_timeout` (60 s) closes idle proxied
  WebSockets. The client treats 45 s without any message, or 10 s without
  an answer to `WANT`, as a dead connection and reconnects.
- **No WebTransport.** Its benefits are separate streams, connection
  migration and 0-RTT; localsync delivers one ordered stream per client
  by design, and a resume already costs about a millisecond. UDP is often
  blocked on enterprise networks, so it would need the HTTP fallback
  anyway.

## 10. Client

### 10.1 localdb `SyncTransport`

localdb does not import localsync. It exports `registerSyncTransport(name,
factory)` from its writer worker module; the app's writer worker entry
registers localsync, and `LocalDbConfig.sync = { transport: 'localsync',
options: { url, tokenUrl, wants } }` (structured-cloneable) selects it.
The transport's database access goes through `ctx.run(fn)`, which localdb
executes inside its write fence, so frames commit exactly like any other
sync batch and broadcast the same commit notices.
`LocalDb.syncCommand({ want })` changes the wanted set (a project switch).

Status, as localdb's `SyncStatus`:

| Status | When |
| --- | --- |
| `connecting` | connecting, and connected but not caught up yet |
| `live` | `SYNCED` applied (section 7); again whenever every want is served after a failure |
| `error` | the connection closed: the actual reason (`token endpoint 401`, `no answer to WANT`, ...), the real retry delay, and the token endpoint's `httpStatus` when that is what failed |
| `failed` | after `live`, some wants are not served: name, error and `permanent` (refused) per want; everything else is live |

`error` is a blip the transport retries. `failed` with a permanent
failure is a mismatch between client and rules (a deploy skew, a bug)
that the app should show.

### 10.2 Apply

```sql
CREATE TABLE _lsync_member (
	unit INTEGER NOT NULL, tbl TEXT NOT NULL, pk TEXT NOT NULL, hash TEXT NOT NULL,
	PRIMARY KEY (unit, pk)
) WITHOUT ROWID;
CREATE INDEX _lsync_member_row ON _lsync_member (tbl, pk);
-- One row per unit definition (rule, params, principal); checksum NULL:
-- the next WANT does not claim the unit (new, dropped or drifted).
CREATE TABLE _lsync_unit (id INTEGER PRIMARY KEY, def TEXT NOT NULL UNIQUE, checksum TEXT);
-- How rows of each synced table are keyed, to delete them by key.
CREATE TABLE _lsync_table (tbl TEXT PRIMARY KEY, pk_cols TEXT NOT NULL);
```

- A frame (all parts) is one SQLite transaction, held open across parts
  (sync is the only writer; WAL-mode readers keep their snapshot), with
  the cursor and unit checksums written inside it.
- `PUT`: upsert row and membership. `REMOVE`: drop membership. `RESET`:
  drop all memberships of the unit. At commit, rows with no membership
  left are deleted, so overlapping rules on one table (an admin rule and
  a member rule) need no dedupe.
- The first frame after a `UNITS` message drops the memberships of every
  unit not in it, inside that frame's transaction.
- Long runs of `PUT`s (snapshots, big move-ins) take a set-based path
  that reads the previous member hashes per chunk: 20,000 rows in 215 ms.
- Row hashes and unit checksums are unsigned 64-bit and stored as
  decimal text, because sqlite-wasm reads `INTEGER` columns into JS
  numbers. Every fragment carries the unit checksum after it; a mismatch
  clears the unit's stored checksum and reconnects, so the engine resends
  it whole.
- sqlite-wasm binds any JS number passing `Number.isInteger` as int64, so
  a double like `1e300` would be stored as `0`; such values are bound as
  text, which a `REAL` column converts exactly.
- Decoding is positional: one prepared statement per (table, columns),
  per-column decoders resolved once.
- Local DDL stays app-owned: descriptor columns are mapped by name,
  unknown ones skipped.

### 10.3 One change on screen

G2 ends at the SQLite commit. For the screen, localdb must also deliver
all live query results of one commit together. By default it does not:
live queries are spread over the reader pool by table, and each reader
re-runs its own on its own tick, so one render can show a task at a new
commit and its comments at an older one. The collab soak
(`docs/bench/collab.md`: two people editing at about 78 transactions/s)
measured 93% of render turns torn that way. localdb's opt-in
`alignCommits` puts every live query on one reader: a tick reads them all
in one snapshot and posts them in one message, delivered in one render.
Measured: 0 torn renders in 2 minutes, commit-to-render 65 ms p50 (59 ms
without), one-shot queries still on the whole pool. Alignment is per
database file, so data that must render together belongs in one file.

## 11. Auth

The app server mints a short-lived JWT, refreshed by sending `WANT` with
a new token before expiry. Expired without refresh: frames stop, then
the connection closes. Forged or expired tokens are rejected at the first
`WANT`. Each `WANT` re-resolves units, so changed claims revoke or add
units through the normal frame path. The engine is the only place
visibility is decided.

Tokens are HS256 only, with `exp`, `sub` and the deployment's `aud`
required (`alg: none`, other algorithms and string `grants` are refused).
The audience is per deployment: with one engine per tenant database and
a shared secret, a token for one tenant must not open another's engine.
`$sub` always comes from the token: a client parameter named like an
undeclared one is refused.

What an attacker can pin is bounded:

| Limit | Value | Past it |
| --- | --- | --- |
| Time to a valid `WANT` (WebSocket), to the `WANT` body (HTTP) | 10 s | connection closed |
| Message size | 1 MiB | connection closed |
| Wants per connection | 2,000 | want refused |
| New units per principal, across connections (joining a unit that exists is free) | 4,000 at once, then 2,000 a minute | want refused, ask again later |
| Parameter value | 4 KiB, no NUL | want refused |
| Units no client holds (the resume cache) | 10,000 | oldest dropped early |
| Bytes queued for one slow client | 64 MiB | connection closed |

Client parameters reach Postgres through pgx's simple protocol and row
values through `rules.Literal`; both depend on
`standard_conforming_strings`, which the engine sets on every connection.

## 12. Postgres footprint

Postgres 18 only.

- **Temporary logical slot** (`CREATE_REPLICATION_SLOT ... TEMPORARY
  LOGICAL pgoutput`). Postgres drops it when the engine's replication
  connection ends, so a dead or deploying engine never pins WAL, and
  there is no slot loss to recover from. The price: a dropped
  replication connection means a cache-cold restart, inside the process
  (section 8), and clients reconcile.
- **Publication** `localsync`, `FOR TABLE` exactly the tables rules
  reference, no row filters or column lists. It coexists with other
  publications.
- **`REPLICA IDENTITY FULL`** only where old values are read: tables
  inside some rule's subqueries (restriction values, column pruning). A
  rule's own table keeps its identity, because units keep per-column
  hashes, and FULL logs the whole detoasted old row on every update (up
  to 200 times the WAL per update on rows with large out-of-line values,
  `docs/bench/dense.md`). An unchanged TOAST column of a member keeps its
  stored value; a row entering a unit with an unsent value is fetched by
  key. Identity is never downgraded, so another consumer's FULL survives.
- **`sync` schema**: one table, `sync.fence (id int PRIMARY KEY, n bigint
  NOT NULL)`, for fences (section 5).
- **Read-your-writes is an app convention, not protocol**: the app writes
  `(client_id, last_mutation_id)` in its own transaction and the client
  wants a rule over its own row. The confirmation then arrives in the same
  atomic frame as the data. There is no `WAIT` message.
- Recommended backstops for other slots on the same database:
  `max_slot_wal_keep_size` and `idle_replication_slot_timeout`.

## 13. Verification

Capability tests assert what the client can observe, compared with the
oracle (the rule SQL run directly in Postgres), never protocol details.

**Capability harness** (`ts/harness`). Run it over both transports
(`LOCALSYNC_TRANSPORT=http` too): their connections differ, and so do the
paths a reconnect takes through the engine. A scenario is a TS module: schema
SQL, seed, principals, rules as SQL, and steps (write, connect, want,
disconnect, restart, quiesce, expect oracle), plus optional samplers run
after every local commit: *no partial transaction* (each workload
transaction stamps one fresh tag on rows in several tables; a tag present
anywhere must be complete) and *no row outside visibility* (every replica
row is in the oracle at some LSN in the client's window). The oracle
compares in canonical form (timestamps as µs, numerics as decimal text,
jsonb with sorted keys). The reference client is localdb's real code on
in-memory sqlite-wasm in Node, so it tests what apps ship. Results:
`docs/capability-matrix.md`; the same catalogue against Electric 1.7.9,
measured before localdb dropped Electric: `docs/electric-comparison.md`.

**Smash simulation** (`sim/`). Seeded workloads with many clients:
hot keys, link churn, rows flapping in and out of visibility, clients
joining, switching projects and leaving, and with `--faults` engine kills
and walsender kills. Every client's state is checked against the oracle
at checkpoints and after quiesce. `parallel` runs many seeds at once, each
with its own database and engine.

**Differential checking.** `LOCALSYNC_VERIFY=1` re-evaluates every unit
in full after each resolution and logs any difference from its
incremental state: the fast paths checked against the definition.

**Browser.** `ts/browser` runs real localdb in headless Chrome with OPFS
and workers: an initial load, two-table transactions that a reactive
query must never render torn, and a want change through `syncCommand`.
The collab soak measures torn renders and latency per tab with two people
editing.

**Through a TLS gateway.** `docker/gateway` is nginx configured like a
production gateway: HTTP/2 and HTTP/3, gzip on, nginx's default proxy
timeouts and buffering, and a real certificate for a name that resolves
to 127.0.0.1. `LOCALSYNC_URL` and `LOCALSYNC_TRANSPORT` route the harness
and simulation through it; `LOCALSYNC_BROWSER_VIA=gateway|hostile` does
the same for the browser runs. The gateway check loads 2,000 tasks and
comments, idles 75 s, then times updates:

| Case | Mode | Load | Update p50 |
| --- | --- | --- | --- |
| WebSocket over WSS | WebSocket | 153 ms | 2 ms |
| HTTP streaming over HTTPS | stream | 97 ms | 2 ms |
| `auto` through the gateway | WebSocket | 152 ms | 2 ms |
| `auto` through a hostile middlebox | poll | 5,350 ms | 3 ms |

The middlebox refuses WebSocket upgrades and holds every response until
it is complete. A reload on the same database resumes in poll mode in
39 ms.

**Hostile suite.** Attacks a running engine on purpose. Leak attacks are
checked against the oracle: injection-shaped client parameters on every
parameterised rule, injection-shaped principals with text primary keys,
and stored values that reach restriction SQL through a text join.
Raw-socket attacks: forged and malformed tokens, undeclared parameters,
malformed, binary and oversized messages. Exhaustion: 500 connections
that never authenticate, and one client churning through 40,000 new
units. After each, the engine must still answer `/health`.

**Security checks.** `scripts/security.sh` runs govulncheck, staticcheck,
gosec and `pnpm audit`, then every Go fuzzer briefly (`FUZZTIME`): the
wire codec and patch decoder, restriction SQL (row values must parse back
as exactly those string constants) and client parameters through the
simple protocol against Postgres. The tools are pinned in `tools/go.mod`.

**Benchmarks** (`docs/bench/`): initial load time and bytes, change
latency, bytes per change, move-ins, fan-out and deflate. Reports from
before localdb dropped Electric include its numbers.

## 14. Not built yet

Each is a cache or a shortcut over the core, adopted only when a
measurement asks for it.

| Idea | Buys | Build when |
| --- | --- | --- |
| maintained sets (tier 2 in memory) | inserts and link changes without restricted queries; move-ins still need a query for row content | inserts or grants dominate latency (today at parity with or ahead of Electric) |
| batched restricted queries (`LATERAL` over the units' parameters) | one query instead of one per unit when analysis cannot narrow the units | many units of one rule are affected per write |
| bucketed checksums | resync sends only differing buckets | whole-unit `RESET`s are common after restarts |
| persistent slot and checkpoints, or a disk log | restart and long-offline catch-up without re-reading units | snapshot load after deploys is too high |
| splices on a physical replica | no snapshot load on the primary | Postgres load is too high |
| per-fragment zstd or a trained dictionary | fewer bytes on small deltas | bytes per change matter |
| in-memory row cache per unit | move-ins and joins without queries | move-in latency |
| priority classes (one frontier per class) | core data never waits behind a heavy table | head-of-line blocking shows up |
| fan-out relays | many clients per engine | one engine's connections or CPU run out |
| deferred indexes on big loads (drop a table's secondary indexes before a large load into it, recreate them after, inside the frame's transaction) | in-memory with 8 indexes: 15 to 21% of load time (100k rows: 1.2 s to 0.95 s) | a load on OPFS with an app's real schema is too slow |

A relay would be a client of the union of its clients' units: one `WANT`
upstream, each client's token forwarded to the engine for authorisation,
per-client frontiers by the same `min` rule, and frames assembled from
upstream fragments by unit ref without decoding rows.
