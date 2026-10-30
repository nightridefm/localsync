# localsync design

Status: Phases 0 to 3 implemented (2026-10-29): harness + Electric
baseline, corpus check, and the correct core, passing the capability
catalogue and the smash simulation with faults. Section 17 lists what
implementation changed. Expands the original handoff plan ("the plan",
kept privately). Where this document amends the plan, the user agreed to it
and it is listed under [Decisions](#decisions); everything still open is
under [Open questions](#open-questions).

The design is built on four ideas (section 2). Most of what an engine like
this usually needs (snapshot paths, move-in/move-out messages, holds,
resync, priorities, disk logs, checkpoints, slot recovery) falls out of
them as special cases instead of being built separately. The design stays
small, and every later optimisation is a cache or a shortcut over the same
core, never a new semantics.

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
13. [Relays](#13-relays)
14. [Verification: harness and simulation](#14-verification-harness-and-simulation)
15. [Optimisation slate](#15-optimisation-slate)
16. [Phases](#16-phases)
17. [What implementation changed](#17-what-implementation-changed)
18. [Decisions](#decisions)
19. [Open questions](#open-questions)

---

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
  its state reflects that write.

Non-goals for v1: client writes through the engine, offline mutation
queues, multiple source databases per engine (App A runs one engine per
tenant database).

## 2. The four ideas

### Idea 1: everything is a unit delta

A **unit** is a rule bound to parameter values: a set of rows of one table,
defined by SQL. All units with the same normalised SQL and parameters are
one unit, shared by every client that wants it.

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
instead of in Postgres (on the WAL row, or from maintained sets) is a
speed-up of the same computation, verifiable against it.

### Idea 3: atomicity is a frontier

Each unit is **known through** some LSN. A client's cursor is the
**minimum** over the units it holds, and the engine only sends a client
frames up to that minimum. A unit waiting on a query just lags; its
clients wait, nobody else does. This single rule gives atomic transactions
across units, atomic subscribe and unsubscribe, atomic move-ins, safe
coalescing (a frontier that jumps skips intermediate states, never mixes
them) and priorities (one frontier per priority class).

### Idea 4: one connection message, reconcile

The client sends `WANT`: its token, the rules (with parameters) it wants,
and what it has (cursor and per-unit checksums). The engine sends the
difference. Connect, reconnect, subscribe, unsubscribe, token refresh,
resync after an engine restart and a project switch are all `WANT`
with different contents. The bytes sent are the measured difference
between what the client has and what it should have.

### Consequence: the engine holds only caches

Postgres is the only truth. Everything in the engine (maintained sets, the
recent-transaction window, unit results, snapshots) is a cache. Losing any
of it (crash, deploy, network drop of the replication connection) costs
Postgres reads, never correctness: the engine rebuilds lazily and clients
reconcile by checksum. That is why the replication slot can be temporary
(section 12) and why v1 has no disk log, no checkpoints and no retention
policy.

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
    priority: 0
    rules:
      project/tasks: SELECT * FROM task WHERE project_id = $project
      project/comments: >
        SELECT * FROM comment WHERE task_id IN
          (SELECT id FROM task WHERE project_id = $project)
```

A group's gate is ANDed into each rule before analysis, so every rule is
still one plain SQL query: the same text is the config, the spec, the
oracle and (via a converter in the harness) the Electric shape. Corpus
shapes convert mechanically: `SELECT <columns or *> FROM <table> [WHERE
<where>]`.

### 3.2 Parameters and gates

- **Every rule declares who may want it** (A22), or fails to load:
  - `$sub` in its SQL: rows or a gate scoped to the principal.
  - `claims: [a, b]`: the token's `grants` must include one of them.
  - Both: `claims` and `$sub`.
  - `public: true`: any valid token.
  `engine plan` prints this as the Access column.
- `$sub` and `grants` come from the token. The app's token endpoint mints
  it, running the same checks its routes run today, so coarse rights
  (admin, curator, tenant member) stay in the app's code.
- **Claims are checked at every `WANT`**, all wants in one pass, in
  memory. A refused want gets an error (`not allowed`), so apps can tell
  "not allowed" from "empty". A token refresh is a `WANT`, so a grant
  missing from the new token drops its units in the next frame. Claims
  are therefore as fresh as the token: keep it short-lived (minutes), and
  gate on tables for anything that must revoke at once (A11).
- **Client parameters** (`$project`) come from `WANT`. A rule that uses
  one must be constrained by `$sub` or claims, or be marked `public:
  true`. Gates are ordinary SQL, maintained like any other part of the
  rule, so revoking a `project_grant` row revokes the units in the same
  frame.
- Named parameters are rewritten to `$1..$n` with a lexer-aware pass
  (skipping literals, quoted identifiers, dollar-quoting, comments) before
  parsing with `pg_query_go`; types are inferred from use sites.

### 3.2a Unit gates

A top-level `EXISTS` / `NOT EXISTS` conjunct that never references the
rule's table (a per-project permission gate) is a **unit gate**: membership
is `row predicate AND gate`, the gate is a per-unit boolean computed with
every full evaluation of the unit, and a change to a gate's tables
re-evaluates the unit. The row predicate stays on the fast path, which is
why 134 of App A's 140 tenant rules are tier 1 (measured on the private
corpus).

**Shared bodies.** When `$sub` appears only in gates, the rows do not
depend on the principal, only whether they are visible at all. Such a
rule is served from one **body** unit per non-principal parameters (the
rule without its gates: one unit per project, shared by every viewer)
and one small **gate unit** per gate and principal: the gate's subquery
as a rule of its own (`EXISTS (SELECT 1 FROM t WHERE ...)` becomes `SELECT
* FROM t WHERE ...`), which holds when it is non-empty (`NOT EXISTS`: when
empty). This is exactly `body AND gates`, because gates do not read the
row. Gate units are ordinary units, so routing, restrictions, gaps,
rounds and frontiers need nothing new; history entries record the member
count, so a gate's value at any frame LSN is a lookup. Per viewer, a
frame sends the body's ops while its gates hold, an empty `RESET` when one
closes, and when they open again the body as a snapshot (usually from the
snapshot cache), with the frontier held at the client's cursor until it
can be delivered. Permission changes no longer re-evaluate the body at
all; each viewer costs one tiny unit instead of a copy of the project.

Sharing is decided at rule load and is fail-safe: the split is purely
syntactic (top-level conjuncts), and the rule stays per principal if the
body still uses `$sub`, a gate's subquery is not one table, or a derived
rule fails analysis or Postgres' `PREPARE` (an unqualified name in a gate
that resolved to the outer table would change meaning once the gate
stands alone). Derived rules are never wantable. `LOCALSYNC_SHARE=0`
turns sharing off. Catalogue 18 and the simulation (sims wanting
`shared_tasks` / `shared_comments` while memberships are granted and
revoked) cover it.

### 3.3 Accepted rules

Any filter; the output must be rows of one table, including its PK, with
plain (not computed) columns, so the client can store them by PK.
Top-level semijoins are normalised to `EXISTS`; top-level aggregates,
window functions, set operations and computed columns are rejected.
`ORDER BY ... LIMIT` is accepted (always re-evaluated with restriction
`TRUE`). Views are expanded. Volatile functions (`now()`, `random()`,
`current_setting()`, sequences) are rejected: visibility would change
without WAL. `LANGUAGE sql` functions are inlined; other user functions are
rejected unless `IMMUTABLE` with declared table deps, because a table
outside the publication is never decoded, so "re-run on any commit"
cannot be a fallback the way it is in localdb. Virtual generated columns
(PG 18's default kind) are never replicated, so rules must not filter on
them.

One client may not hold two units on the same table with different column
lists (rejected at `WANT`); this avoids per-row column masks entirely.

### 3.4 Plan output

Startup prints, per rule, who may want it (Access, section 3.2), the
evaluation strategy of each part (section 4.2), its dependency tables,
and which maintained sets it shares. Reviewing access means reading the
Access column of `engine plan`.
`engine plan rules.yaml --max-strategy=memory` lets CI fail when a rule
needs Postgres re-queries for its common changes.

## 4. Computing deltas

### 4.1 Restrictions

For each transaction, for each table it changes, analysis maps each
changed row to the units it can affect and a restriction per unit:

| Change | Restriction (rows of the rule's table to re-check) |
| --- | --- |
| row of the rule's own table T | `pk = ANY(changed pks)` |
| row of a table inside a monotone chain (`IN`, `EXISTS`, equi-join) | the **delta query**: the chain with the changed row's values substituted, e.g. `id IN (SELECT document_id FROM document_team WHERE team_id = $changed)` |
| row of a table under a non-monotone node (`NOT IN`, `NOT EXISTS`, aggregates, `LIMIT`) | `TRUE` (re-evaluate the unit) |
| anything analysis does not understand | `TRUE` |

Which units are affected comes from the same substitution: a change to
`team_member(user_id = U)` affects only units of rules over `team_member`
bound to `$sub = U`. When analysis cannot narrow the units, all units of
the rule are affected; their restricted queries run as one batched query
over the parameter list (`LATERAL` over `VALUES`), not one per unit.

### 4.2 Evaluation strategies

The restriction can be evaluated three ways, chosen per rule part by
analysis. They are the plan's tiers, now defined as speed-ups of one
computation:

1. **On the row** (tier 1). Restriction is the changed rows themselves and
   the predicate is row-local over a whitelist of operators whose
   Postgres semantics the engine reproduces exactly (ints, numerics,
   uuids, bools, timestamps, `C`-collated text, `IS [NOT] NULL`,
   `IS DISTINCT FROM`, `IN (list)`, `= ANY`, three-valued `AND/OR/NOT`).
   Old and new row (FULL replica identity) give `was_in` / `is_in`
   directly. Anything outside the whitelist (other collations, `citext`,
   `LIKE`, jsonb operators) is evaluated by Postgres as a restricted
   query, `pk = ANY(changed)`, not by a separate mechanism.
2. **From maintained sets** (tier 2, Phase 4). The inner parts of chains
   (`S = {team_id | team_member.user_id = $sub}`) are maintained in
   memory as counted multisets over small projection indexes. Identical
   normalised subtrees are **one** set shared by every rule using them
   (all ten of App B's per-team rules share one `team_member` set per user):
   this common-subexpression sharing replaces the plan's separate key
   decomposition. A set delta gives the delta query's keys without asking
   Postgres; only rows whose content the engine does not hold (move-ins)
   still need a query, restricted to those keys.
3. **In Postgres** (tier 3). The restricted query at a spliced snapshot,
   diffed against the unit's stored `pk -> row hash` for the restricted
   rows.

**Column pruning** (built in Phase 4, before any maintained set):
membership is a function of the columns a rule reads, so an update that
changes none of them (on the rule's table: a content update; on a table
inside its subqueries: an irrelevant one) cannot change any unit's
membership. The former becomes a direct `PUT` for members, the latter is
skipped. The analysis records these columns per table (`RefCols`, an
over-approximation by name, which is always safe). This took tier-2 update
latency from 5.1 to 1.5 ms p50 in `docs/bench/`.

Every transaction is processed in two phases (apply all its changes to
sets, then compute memberships before vs after the whole transaction),
so the order of rows inside a transaction, including rules that filter a
table by a subquery on the same table (three in App B), never matters.

### 4.3 Corpus expectations

Phase 2 checks these mechanically. Both apps should need only strategies
1 and 2 for common changes.

**App B** (86 shapes, 39 tables, PG 18, Electric 1.7.9):

| Pattern | Count | Strategy | Sharing |
| --- | --- | --- | --- |
| no `WHERE` | 52 | 1 | one unit per table and column list, all roles |
| row-local `user_id = $sub` | 1 | 1 | per user |
| `IN` chains, 1 to 4 levels deep | 31 | 2 | per user, shared sets |
| `OR` of two chains | 1 | 2 | per user |
| `NOT IN` without `$sub` | 1 | 2; the inner column is a PK, so no NULL tracking | one unit for the role |
| filtered by a subquery on the same table | 3 of the above | 2, two-phase | per user |
| column projection (a `user` directory, within the 52) | 1 | 1 | shared |

**App A** (153 shapes, a control-plane database and per-tenant databases):

| Pattern | Count | Strategy | Sharing |
| --- | --- | --- | --- |
| no `WHERE` (admin tables, shared libraries) | 29 | 1 | per database |
| `project_id = $project` (+ gate) | 111 | 1, gate 2 | one unit per project, shared by its members |
| `$project` with an extra row-local conjunct | 2 | 1, gate 2 | per project (one also per user) |
| one hop via a project-owned parent | 5 | 2, gate 2 | per project |
| `$sub` row-local (incl. `user_id IS NULL OR user_id = $sub`) | 3 | 1 | per user |
| global rows (`project_id IS NULL`, one hop via it) | 2 | 1 / 2 | shared |
| `project` by id (+ gate) | 1 | 1, gate 2 | per project |

App A's `project` shape is unscoped today only because of Electric
subquery bugs; the rule it wants, `project WHERE id IN (SELECT project_id
FROM project_grant WHERE user_id = $sub)`, is strategy 2.

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
are widened to `xid8` with the epoch of the snapshot's `xmin`.

**Regular snapshots.** Let `B` be the largest commit LSN among visible
transactions (known once the stream passes `L_S`). The snapshot is
**regular** if no invisible transaction committed below `B`; then the
result is exactly "the database at `B`" and the unit can be declared known
through `B`. Irregular snapshots (a tiny race window) are retried. This is
what lets query results join the frontier (section 6) without tearing.

The splice is used for everything slow: unit creation (restriction
`TRUE`), move-ins, strategy-3 deltas, building maintained sets, adding a
table to the publication (wait for the `ALTER PUBLICATION` commit to pass
in the stream, then splice), and reconcile. Results for the same
`(query, params, B)` are shared between units and clients; a bounded pool
of query workers caps load on Postgres.

## 6. Frontiers

- Each unit has `known(u)`, an LSN. A unit whose deltas are computed on
  the pipeline (strategies 1 and 2 without move-ins) is known through
  every transaction as soon as it is routed.
- **Resolution gaps.** A query resolution is a *jump*: the unit's state is
  known at `p` (just before its first pending transaction) and at `B`, but
  undefined strictly between them. History entries record that gap, and a
  frame may only land on an LSN that is inside no held unit's gap, so the
  frontier is "the largest LSN `<= min(known)` valid for every unit", not
  just the minimum. (Found by the harness: with plain `min(known)`, a frame
  at 104 showed one unit resolved at 100 and another still at its
  pre-100 state, a torn transaction.)
- **Snapshot rounds.** Gaps alone are correct but could stall a client
  whose units keep resolving at different `B`s. All queries scheduled
  together run in one round: a leader transaction exports its snapshot
  (`pg_export_snapshot()`) and workers import it, so units resolved in the
  same round share `B` and their gaps end together.
- A transaction `T` that gives `u` a query-dependent delta makes `u`
  **pending**: `known(u)` stays below `c(T)`. The pipeline keeps going;
  later fast deltas for `u` are kept (by reference into the
  recent-transaction window). When the restricted query returns at a
  regular boundary `B`, `u` becomes known through `B`: rows inside the
  restriction come from the query result, rows outside it from the kept
  deltas up to `B`. Restrictions that arrived for transactions up to `B`
  after the query was issued go into the next round; each round covers
  through its own `B`, so it converges.
- A client's frontier is `min(known(u))` over its units, per priority
  class. The engine sends a frame when the frontier advances, covering
  everything up to it in one frame. Fragments for transactions the client
  skips over are merged (net op per row), which is the coalescing.
- Head-of-line blocking is per client and only behind its own pending
  units; the global pipeline never waits. Priority classes are separate
  frontiers, so core data is never stuck behind a heavy table.

Transactional subscription changes fall out: a new unit starts pending;
the client's frontier (for that class) waits for it, then one frame
carries the new unit's `RESET` + rows, the removal of dropped units, and
any live changes, all at one LSN. A project switch in App A (about 118
units out, 118 in) is one `WANT` and one frame.

## 7. Reconcile

`WANT` carries: token, wants `[(rule, params)]`, and haves: the engine
epoch and cursor the client last saw, and a checksum per held unit.

The engine resolves wants to units (refusing unauthorised ones) and
answers per unit:

| Client has | Engine sends |
| --- | --- |
| unit not wanted any more, or not authorised | `RESET` with no rows (removal) |
| same epoch, cursor inside the recent window | the kept deltas from the cursor |
| anything else (older cursor, new epoch, after an engine restart) | evaluate the unit at a regular snapshot; equal checksum: nothing, just known through `B`; different: `RESET` + rows |
| unit not held | `RESET` + rows |
| unit held but not in the wants | nothing from the engine; the client drops it inside the next frame's transaction (the engine forces one) |

Haves describe the principal they were synced for: the client names each
unit by (rule, params, the token's `sub`), so when another user signs in
on the same local database (sign out and in, an admin's masquerade) none
of the first user's units match. They are held but not wanted, so the
first frame drops them, and the new user's units arrive whole (catalogue
7b). Naming units by rule and params alone resumed the first user's
state as the second's: a leak, found by App B's shadow comparison.

Haves count only on a connection's first `WANT`. Later `WANT`s on the
same connection (a project switch, a token refresh) keep the holds they
share with the last one untouched, and a hold that is new gets `RESET` +
rows whatever the client says it has. The client writes its haves from
what it has applied, and the engine may already have sent frames it has
not: switching away from a project and straight back sends a removal of
the project's units and then a `WANT` that still lists them as held
(catalogue 15b). Over
HTTP every `WANT` is a new connection (section 9.1), so haves always
count there.

**Checksums.** A column hash is a 64-bit FNV pair over (column index,
canonical wire bytes of the value), so client and engine hash identical
bytes; a row hash is the sum of its column hashes (so `PATCH` can carry
the new row hash after changing a few columns). A unit's checksum is
`sum(mix(row hash)) mod 2^64` over its members, `mix` being the
splitmix64 finalizer. The mix is essential: without it the checksum is a
sum over (column, value) pairs and cannot see values moving between rows
(two projects swapping orgs), which is what made the reconnect skip unsafe
(A16). Still additive per row, so PUT adds the new row's term and
subtracts the old (the engine keeps per-column hashes per member) and
REMOVE subtracts. Checksums ride along in
frames periodically, so they are also an always-on production verifier.

The recent-transaction window is in memory (default 5 minutes or 256 MiB),
serving brief disconnects only. Everything older reconciles by checksum,
which after an unchanged deploy sends nothing but frontier updates.
Bucketed checksums (send only differing PK-hash buckets) are the first
optimisation if whole-unit `RESET`s turn out to be common (section 15).

## 8. Engine

```
 Postgres 18 ── temporary slot, pgoutput ──> [decode] -> [route: sets, row eval, restrictions]
      ^                                                      |
      |  spliced restricted queries (RR, bounded pool)       v
      +─────────────────────────────── [query workers] <- [units + frontiers]
                                                             |
                                    [encode fragment once per unit] -> [recent window]
                                                             |
                                          [per-client writers] -> WebSocket -> clients / relays
```

- **One ordered pipeline goroutine** decodes, routes and encodes, so the
  hot path has no locks. pgoutput with `binary 'true'` so ints, uuids and
  timestamps arrive unparsed; rows decode into reusable typed batches.
  Hot-path rules from `CLAUDE.md` apply: no per-row allocations or maps,
  pprof before and after.
- **Query workers**: a bounded pool (default 4) running spliced queries.
- **Per-client writers**: one goroutine per socket, reading from the
  shared window. A slow client never slows the pipeline; one that falls
  out of the window reconciles.
- **Boot** has no special path: create a temporary slot, start streaming,
  then create units and sets lazily as `WANT`s arrive, each by splice. An
  engine with no clients streams and discards. The epoch is a random id
  per process start.
- **Encode once**: a unit's delta for a transaction is one fragment, shared
  by every client and relay holding the unit.
- **Memory is bounded per client and per unit**, never by trusting the
  peer: a client's send queue is capped at 64 MiB (a client that cannot
  keep up is disconnected and reconciles), a unit keeps at most 32 MiB of
  history (holds lagging behind what was dropped get a fresh snapshot),
  and one `WANT` names at most 2000 units. Big frames are not built whole:
  the writer pulls ~1 MiB parts from the shared op bytes while its queue
  is below 4 MiB.
- **Query results are not copied**: rows are encoded straight into a slab
  per query with their column hashes, and members, snapshot ops and
  frames all reference those bytes.
- **Snapshot cache**: a unit's latest full snapshot is kept for a minute
  (256 MiB budget, oldest dropped first), so a client joining a warm unit
  (a second tab or device, a reconnect storm) needs no query: it gets the
  cached snapshot plus the unit's history since. It is used only when it
  is joinable exactly like a fresh one (the unit is resolved past it and
  its history reaches back to it); otherwise the client gets a query.
  `LOCALSYNC_SNAP_CACHE=0` turns it off. Concurrent waiters already share
  one in-flight query, so the cache matters for staggered arrivals: 20
  staggered clients on two units need 0 snapshot queries instead of 40
  (`docs/bench/fanout.md`). It holds the result slabs, so it accounts for
  most of the live heap after a big load; set `GOMEMLIMIT` to bound RSS.
- **A lost replication stream restarts the engine in process**
  (walsender killed, Postgres restarted or failed over, network). Engine
  state is a cache, so it is dropped: a new slot under a new epoch, setup
  retried with backoff (250 ms to 10 s) until Postgres is back, queries
  still in flight discarded. Client sockets stay open; each client gets
  `RESYNC`, drops any partly received frame, and sends its `WANT` again,
  which carries the old epoch and so reconciles by checksum exactly like
  a reconnect. No new correctness path: catalogue 8b and the simulation's
  `kill walsender` fault cover it.
- Observability: `/metrics` (Prometheus text, no client library):
  - Gauges: clients by transport; units, held units and units with
    pending changes; commit lag (seconds from commit in Postgres to
    routing, for the last transaction); the furthest-behind client's WAL
    bytes; the engine's head LSN.
  - Counters: transactions, row changes, WANTs and refused wants,
    snapshot queries and cache hits, in-process restarts, messages and
    bytes sent. Counters stay monotonic across in-process restarts.
  - Also `/debug/pprof`, `/debug/units` and the rule plan.
  - All on the engine's own port: put only `/sync` and `/sync/http` behind
    the public gateway, and scrape the rest privately.
  - `engine health` checks `/health` for container healthchecks (the image
    is distroless: no shell, no curl).

## 9. Wire format

One connection per client: a WebSocket, or HTTP where WebSockets fail
(section 9.1); the messages are the same. Binary messages start with a type byte:
`1` FRAME, `2` FRAME_PART (a part of a frame; more follow, and the frame
ends with a FRAME). The client buffers parts and applies the whole frame
in one transaction, so a big initial load spans many messages but is
still one change on screen. Messages of 512 bytes or more are compressed with
permessage-deflate (no context takeover, A15). A unit's ops may be split across parts; its
fragment flag `continues` says more follow and its checksum is checked
only on the last piece. Integers are LEB128 varints.

| Direction | Message | Body |
| --- | --- | --- |
| client | `WANT` (JSON) | token, wants `[(id, rule, params)]`, haves `(epoch, cursor, [(id, checksum)])`; always the full set |
| engine | `UNITS` (JSON) | per want: client id, engine ref, rule, table, columns, PK positions, or an error |
| engine | `SCHEMA` | table descriptors (or changes) at an LSN, before the frame that needs them |
| engine | `FRAME` | lsn, `[fragment]`; an empty frame just advances the cursor |
| engine | `ERROR` (JSON) | message |
| engine | `RESYNC` (JSON) | new epoch: the engine restarted in process; send `WANT` again |
| engine | `BEAT` (JSON) | nothing sent for 15 s; liveness only |

Control messages are JSON text frames (rare, small); only `FRAME` is
binary. Unit ids are chosen by the client, one per distinct (rule,
params, principal) and never reused, so a want that changes parameters is
a new id, and so is the same want for someone else on the same local
database (section 7).

```
fragment = unit_ref len ops
ops      = (table_id count op*)*
op       = PUT row | REMOVE pk | PATCH patch | RESET
patch    = u64 row hash | len pk | changed bitmap | null bitmap | changed values
row      = null_bitmap value*          positional, the unit's columns
```

Values are typed by the descriptor, never self-describing: zigzag
varints, `bool` in the bitmap, `uuid` 16 bytes, `timestamptz` int64 µs,
`date` as its text form (`2026-07-03`, as the Electric path stores it), `float8` 8 bytes, `numeric` as text, `text`/`bytea`
length-prefixed, `json`/`jsonb` as text, arrays as count + elements,
anything else (PostGIS `geometry`, pgvector `halfvec`) as its text form
unless projected away. PUT always carries the full projected row.

### 9.1 Transports

- **WebSocket** (`/sync`) is the default. A WANT on the open socket
  changes the wanted set or refreshes the token.
- **HTTP** (`/sync/http`) is the same protocol for networks where
  WebSockets fail: proxies that strip `Upgrade`, cut long-lived sockets,
  or hold responses until they are complete (antivirus and DLP proxies).
  - Each request is a connection. Its body is one WANT. Its response
    streams the engine's messages as records: a kind byte (0 text, 1
    binary, `0x80` set when the payload is raw deflate), the length as 4
    bytes little-endian, the payload.
  - Records of 512 bytes or more are deflated alone, as A15 does on the
    WebSocket. The response is `application/octet-stream`, so gateways'
    `gzip_types` leave it alone. Compressing inside the protocol works
    even where a gateway strips `Accept-Encoding` before forwarding.
  - A stream ends after 5 minutes. A new WANT (new wants, a fresh token)
    is a new request. Either way the client re-POSTs at once and resumes
    from its cursor, which is the WebSocket reconnect path (A16) and
    costs about a millisecond.
  - `?mode=poll` ends the response after the first frame with unit data,
    or after 20 s. Behind a proxy that buffers whole responses, every
    long poll still delivers, and with no extra latency: the response
    ends the moment there is something to send.
  - The engine sends `X-Accel-Buffering: no` (nginx then streams even
    where `proxy_buffering` is on), `Cache-Control: no-store,
    no-transform`, and `Access-Control-Allow-Origin: *`. The token is in
    the body, never a cookie, and a plain-text body needs no CORS
    preflight.
- **Choosing** (`SyncClientOptions.transport`, default `auto`):
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
  WebSockets (measured: closed at 60.1 s without beats). The client
  treats 45 s without any message, or 10 s without an answer to WANT, as
  a dead connection and reconnects.

## 10. Client

### 10.1 localdb `SyncTransport`

Implemented as below with one change: localdb does not import localsync.
localdb exports `registerSyncTransport(name, factory)` from its writer
worker module; the app's writer worker entry registers localsync, and
`LocalDbConfig.sync = { transport: 'localsync', options: { url, tokenUrl,
wants } }` (structured-cloneable) selects it. The transport's database
access goes through `ctx.run(fn)`, which localdb executes inside its write
fence, so frames commit exactly like Electric batches and broadcast the
same commit notices. `LocalDb.syncCommand({ want })` changes the wanted set
(e.g. a project switch).

Original sketch:

localdb hard-wires Electric today: one `shapeStream` per table, cursors in
`_ldb_shape`, tags in `_ldb_row_tag`, and `alignShapes` as a partial
workaround for cross-shape tearing. Phase 6 adds an interface owned by
the writer worker, with Electric (unchanged code) and localsync as
implementations:

```ts
export interface SyncTransport {
	start(ctx: SyncContext): void
	stop(): Promise<void>
	want?(wants: Want[]): Promise<void>
	waitFor?(marker: string): Promise<'visible' | 'not-visible'>
}

export interface SyncContext {
	// One fenced write transaction and one commit notice; the transport
	// stores its cursor through tx inside it.
	transaction<T>(fn: (tx: SyncTx) => T): T
	status(s: SyncStatus): void
	signal: AbortSignal
}
```

### 10.2 Apply

```sql
CREATE TABLE _lsync_member (
	unit INTEGER NOT NULL, tbl INTEGER NOT NULL, pk BLOB NOT NULL, hash INTEGER NOT NULL,
	PRIMARY KEY (unit, tbl, pk)
) WITHOUT ROWID;
CREATE INDEX _lsync_member_row ON _lsync_member (tbl, pk);
```

- A frame (all parts) is one SQLite transaction, held open across parts
  (sync is the only writer; WAL-mode readers keep their snapshot), with
  the cursor and unit checksums written inside it.
- `PUT`: upsert row and membership. `REMOVE`: drop membership. `RESET`:
  drop all memberships of the unit. At commit, rows with no membership
  left are deleted.
- Row hashes and unit checksums are unsigned 64-bit and stored as
  decimal text: sqlite-wasm reads `INTEGER` columns into JS numbers. Every
  fragment carries the unit checksum after it; a mismatch drops the unit
  from `have` and reconnects, so the engine re-sends it whole.
- sqlite-wasm binds any JS number passing `Number.isInteger` as int64, so
  a double like `1e300` would be stored as `0`; such values are bound as
  text, which a `REAL` column converts exactly (found by catalogue 1). Overlapping rules on one table (an admin rule and a
  member rule) need no client-side dedupe.
- Decoding is positional: one prepared statement per (table, columns),
  per-column decoders resolved once.
- Local DDL stays app-owned: descriptor columns are mapped by name,
  unknown ones skipped.

### 10.3 One change on screen

G2 ends at the SQLite commit. For the screen, localdb must also deliver
all live query results of one commit together. By default it does not:
live queries are spread over the reader pool by table, each reader re-runs
its own on its own tick, so one render can show a task at a new commit and
its comments at an older one. The collab soak (`docs/bench/collab.md`: two
people editing at ~78 transactions/s) measured **93% of render turns torn**
that way. localdb's opt-in `alignCommits` (A20) puts every live query on
one reader: a tick reads them all in one snapshot (fence reads) and posts
them in one message, delivered in one render. Measured: 0 torn renders in
2 minutes, commit-to-render 65 ms p50 (59 ms without), one-shot queries
still on the whole pool. App A keeps tenant-level and per-project data in
separate files with no cross-file queries (A19), so per-file alignment is
all it needs.

## 11. Auth

The app server mints a short-lived JWT (`sub`, `exp`, app claims; HMAC or
JWKS), refreshed by sending `WANT` with a new token before expiry. Expired
without refresh: frames stop, then the socket closes. Forged or expired
tokens are rejected at the first `WANT`. Each `WANT` re-resolves units,
so changed claims revoke or add units through the normal frame path. The
engine is the only place visibility is decided; relays never interpret
rules.

## 12. Postgres footprint

PG 18 only (both apps; App A upgrades from 17.4).

- **Temporary logical slot** (`CREATE_REPLICATION_SLOT ... TEMPORARY
  LOGICAL pgoutput`). Postgres drops it when the engine's replication
  connection ends, so a dead or deploying engine never pins WAL, and
  there is no slot loss to recover from. Price: a dropped replication
  connection means a cache-cold restart (clients reconcile). The restart
  happens inside the process (section 8): sockets stay open. A permanent
  slot is an optimisation (section 15).
- **Publication** `localsync`, `FOR TABLE` exactly the tables rules
  reference, all four operations, `publish_via_partition_root = true`, no
  row filters or column lists (plan decision 12). Coexists with
  Electric's publication during migration.
- **`REPLICA IDENTITY FULL`** on every published table: old rows for exact
  row-local moves without leaking PKs, incremental checksums, and filling
  unchanged TOAST columns. Cost is WAL volume on updates and deletes, to be
  measured on both apps' write mixes (A8).
- **`sync` schema** (SQL only):

  ```sql
  CREATE SCHEMA IF NOT EXISTS sync;
  CREATE TABLE IF NOT EXISTS sync.fence (id int PRIMARY KEY, n bigint NOT NULL);
  ```

  When a query result waits for the stream to pass its snapshot LSN and
  nothing else is committing, the engine updates `sync.fence` and waits to
  see it in the stream: a **fence**, written only on demand. Read-your-
  writes is the A13 convention (a `client_mutation` row written in the
  app's transaction, synced by a rule), so there is no `write_marker()`
  and no `WAIT` message.
- Docs recommend `max_slot_wal_keep_size` (for Electric's slot during
  migration) and `idle_replication_slot_timeout` (PG 18) as backstops.

## 13. Relays

A relay is a client of the union of its clients' units plus fan-out: it
sends one `WANT` upstream for the union, forwards each client's token to
the engine for authorisation, keeps per-client frontiers (the same `min`
rule), and assembles each client's frames from upstream fragments by
`unit_ref` without decoding rows. Optional: single-node deployments
connect clients to the engine directly with the same protocol. Relays
speak WebSocket only; clients that need HTTP (section 9.1) connect to
the engine.

## 14. Verification: harness and simulation

### 14.1 Capability harness (`ts/harness`)

Tests capabilities, never protocol details. A scenario is a TS module:
schema SQL, seed, principals, rules as SQL, and steps (write, connect,
want, disconnect, restart, quiesce, expect oracle), plus optional
continuous samplers.

- **Oracle**: each wanted rule run against Postgres with the principal's
  params, unioned per table, compared in canonical form (timestamps as µs,
  numerics as decimal text, jsonb with sorted keys).
- **Adapters**: Electric (rules converted to `table`/`where`/`columns`;
  anything that does not fit is `unsupported`) and localsync. Interface as
  in the plan: `configure`, `connect`, `replica`, `quiesce`,
  `disconnect`, `reconnect`, `restartServer`, `bytesTransferred`,
  optional `awaitWrite`, `want`.
- **Reference client**: localdb's actual client code (`sync/electric.ts`,
  `sync/apply.ts`, later the localsync transport) on in-memory sqlite-wasm
  in Node, as localdb's own tests already run it, so the baseline
  measures what the apps ship (A12).
- **Samplers**, run after every local commit: *no partial transaction*
  (each workload transaction stamps one fresh tag on rows in several
  tables; a tag present anywhere must be complete), *no row outside
  visibility* (every replica row is in the oracle at some LSN in the
  client's window).
- **Fixture schema**: direct scope column, 1 hop, many-to-many, multi-hop,
  a self-referencing link, and a table with every type (uuid,
  timestamptz, jsonb, arrays, bool, numeric, NULLs, TOAST-sized text).
  Corpus mode runs both apps' real schemas and rules on generated data.
- **Catalogue**: the plan's 15 capabilities, plus: Electric's behaviour
  when a table with a live shape is dropped and recreated (seen in Phase
  0), a project switch as one atomic swap, and a permission-scoped
  `project` rule. Baseline matrix in `docs/capability-matrix.md`.

### 14.2 Smash simulation (`sim/`)

- Seeded workloads (hot keys, 100k-row transactions, link churn, rows
  flapping in and out of visibility, clients joining, switching and
  leaving), random rules from the FK graph (SQLancer-style, NULL-heavy,
  with ternary-logic-partitioning metamorphic checks), faults (`kill -9`,
  Postgres restart, replication connection drops, toxiproxy, slow
  readers).
- Invariants: replica == oracle after quiesce, both samplers above,
  client checksums == engine checksums.
- **Differential checking is free**: every fast-path delta (strategies 1
  and 2) can be recomputed by its restricted query (strategy 3) and
  compared. The simulation does it for every delta; production can do it
  for a sample, using the same code path.
- Leaks (goroutines, heap, FDs, Postgres connections back to baseline
  after soak), `-race`, `go test -fuzz` on the pgoutput decoder and wire
  codec, fuzzed TS decoder.
- Shrinking: delta-debug failing op sequences; interleavings are not
  deterministic, so each candidate runs several times.

### 14.3 Through a TLS gateway

`docker/gateway` is nginx configured like the apps' gateways: HTTP/2 and
HTTP/3, gzip on, nginx's default proxy timeouts and buffering. It serves
a real certificate for a name that resolves to 127.0.0.1, so tests run
over `wss://` and `https://` exactly as browsers reach the apps.

- **Harness and simulation**: `LOCALSYNC_URL` and `LOCALSYNC_TRANSPORT`
  route every client through it. The catalogue passes 23/23 over WSS
  through nginx, and 23/23 over HTTP streaming and over long polls
  (before catalogue 15b was added).
  20/20 fault-simulation seeds pass over HTTP.
- **`pnpm ... gateway`** checks four cases, each with a load of 2,000
  tasks and comments, 75 s idle, then timed updates:

  | Case | Mode | Load | Update p50 |
  | --- | --- | --- | --- |
  | WebSocket over WSS | WebSocket | 153 ms | 2 ms |
  | HTTP streaming over HTTPS | stream | 97 ms | 2 ms |
  | `auto` through the gateway | WebSocket | 152 ms | 2 ms |
  | `auto` through a hostile middlebox | poll | 5,350 ms | 3 ms |

  - The first three survive 75 s idle without a reconnect.
  - The middlebox refuses WebSocket upgrades and holds every response
    until it is complete. The hostile load is slow only the first time:
    a reload on the same database resumes in poll mode in 39 ms.
- **Browser**: `LOCALSYNC_BROWSER_VIA=gateway` (or `hostile`) serves the
  smoke and the collab soak from one `https://` origin through the
  gateway. All four cases pass, with 0 torn renders on aligned tabs.

### 14.4 Benchmarks

Both engines: initial load time and bytes, change latency p50/p99,
bytes per change, reconnect bytes after a deploy, engine CPU/RSS at N
clients, Postgres load from splices. Results in `docs/bench/`.

## 15. Optimisation slate

Each is a cache or shortcut over the core; none changes semantics, and
each is adopted only when a benchmark asks for it.

| Optimisation | Buys | Trigger |
| --- | --- | --- |
| maintained sets (strategy 2) | inserts and link-table changes without restricted queries; move-ins still need a query for row content | inserts or grants dominate latency; after column pruning they are at parity with or ahead of Electric (`docs/bench/`), so deferred |
| snapshot-apply bulk path (client) | initial load | done in Phase 4: 1.5 s -> 0.47 s for 20k rows |
| bucketed checksums | resync sends only differing buckets | whole-unit `RESET`s common after restarts |
| persistent slot + checkpoints | restart without re-reading units | splice load after deploys too high |
| disk transaction log | catch-up without re-reading for long-offline clients | same |
| splices on a physical replica | no snapshot load on the primary | Postgres load measured too high |
| `permessage-deflate` | fewer bytes on loads and move-ins | **done** (A15) |
| per-fragment zstd / trained dictionary | fewer bytes on small deltas | bytes per change matter after A15 |
| column-delta PUTs | fewer bytes on wide rows | **done** (A9) |
| chunked snapshot delivery | bounded engine memory on big initial loads | **done**: frames go out in ~1 MiB parts; RSS after a ~110 MB load 485 -> 407 MB, the rest is the snapshot cache and GC headroom (`docs/bench/dense.md`) |
| zero-copy query results | no per-row copies between query, members and frames | **done** |
| snapshot cache | joins to warm units without queries | **done** (section 8) |
| in-memory row cache per unit | move-ins without queries | move-in latency |

## 16. Phases

Status as of 2026-10-30: 0 to 4 done; 6 started (localdb transport,
`alignCommits`, HTTP fallback and gateway checks done); 5 not started.

0. Skeleton, compose, this document. **Done.**
1. Harness + Electric baseline (section 14.1), first benchmarks.
   **Done**: `docs/capability-matrix.md`, `docs/bench/`.
2. Corpus check: print section 4.3's tables mechanically for both
   corpora; any surprise is raised with the user. **Done** (results kept
   with the private corpus), no surprises.
3. **Done** (except per-fragment compression, priority classes and an
   in-engine differential verifier). Correct core: temporary slot and pgoutput decode, splice with regular
   snapshots, units and frontiers, restrictions with strategies 1 and 3
   (every rule correct, link changes via restricted queries), reconcile,
   recent window, checksums, `sync` schema, JWT, wire codec. Pass the
   catalogue; simulation with differential checking from the first commit.
4. Strategy 2 (maintained, shared sets), as far as benchmarks justify.
   **Done as far as justified**: column pruning, fence-free resolution
   when no WAL follows the last routed commit, and bulk snapshot apply
   closed every benchmark gap; maintained sets are on the slate with a
   trigger (section 15).
5. Relays, priorities, the optimisation slate as measured.
6. localdb `SyncTransport` and commit-aligned delivery, then App B behind
   a per-user flag next to Electric with replica diffing, then App A.
   **localdb transport done, uncommitted in `../localdb`**:
   `src/sync/transport.ts` (interface + `registerSyncTransport`),
   `LocalDbConfig.sync`, `LocalDb.syncCommand`, and the writer worker
   running a registered transport inside its write fence; localdb's 9 test
   files pass. The localsync side is `localsyncTransport`
   (`ts/transport/src/localdb.ts`). `ts/browser` is an end-to-end smoke in
   headless Chrome: real localdb with OPFS and workers, initial load, 120
   two-table transactions never rendered torn by a reactive query, and a
   want change through `syncCommand`. Not done: commit-aligned delivery
   in localdb (Q8), App A / App B integration (needs the user).

## 17. What implementation changed

Things the plan or earlier drafts of this document did not anticipate,
each found by building or by the harness:

- **Resolution gaps and snapshot rounds** (section 6): `min(known)` is
  not a safe frontier once units resolve by query; frames must avoid each
  unit's gap, and rounds sharing one exported snapshot keep frontiers
  moving.
- **Unit gates** (section 3.2a): uncorrelated `EXISTS` gates keep gated
  rules on the fast path; without them all of App A's project rules
  would need a query per write.
- **Full re-evaluation is the universal fallback**: a restricted query
  that misses a restriction (a transaction committed before its boundary
  but routed after it was issued) is retried as a full evaluation, which
  always resolves because it covers everything up to its own boundary.
- **Units linger** for the resume window after their last client leaves,
  otherwise every reconnect re-snapshots (catalogue 7 failed until then).
- **Client-chosen unit ids** (section 9), never reused, so removals and
  additions in one frame cannot collide.
- **Read-your-writes needs no protocol** (A13): the harness's catalogue 13
  passes on localsync with a `client_mutation` row and fails on Electric
  because the confirmation arrives on a different stream than the data.
- **Client fixes the baseline exposed**, all in localdb's Electric path or
  sqlite-wasm, not in Electric itself: `pgArrayToJson` turns `'1'` in a
  `text[]` into the number 1 (catalogue 1); a column added upstream wedges
  the shape (`no column named ...`, catalogue 12b); sqlite-wasm stores a
  bound `1e300` as 0. localsync's codec avoids all three.
- **Fences are only needed when WAL follows the last routed commit**: if
  a query's snapshot LSN is at or before the end of the last routed commit
  record, every commit before it has been streamed.
- **A held unit that is no longer wanted is the client's to drop**: the
  smash simulation found a client that swapped projects, disconnected
  before the removal frame, and kept the old unit's rows forever (also
  deletes upstream never reached them). Catalogue 17 covers it.
- **Fuzzing found two codec bugs in seconds**: Go escapes for invalid
  UTF-8 and non-JSON numbers (`000000`, `NaN`) in arrays.
- **The browser smoke catches what Node cannot**: a drift diagnostic used
  `Buffer`, which exists in Node (so the harness and simulation passed)
  but not in a browser worker, where every frame failed to apply. Lint
  now forbids Node globals in `ts/transport/src`.
- **Waiting clients already share one snapshot query**, so a cache only
  helps clients that arrive after the query finished (section 8).
- **Switching away from a project and straight back** hid two bugs:
  - **Engine** (section 7): a `WANT` resumed units on the strength of
    haves the client wrote before applying a frame that removed them.
    The client ended up with an empty unit the engine believed full,
    found while chasing the next bug and made deterministic as
    catalogue 15b. Haves now count only on a connection's first `WANT`.
  - **Client**: a removal deleted the unit's `_lsync_unit` row, which is
    also the definition's id. The `WANT` that wanted the unit back was
    already sent under that id, so the unit's checksum was never stored
    again, and its next delta read as drift. The smash simulation found
    it (seed 213, only under four parallel jobs). Removals now mark the
    unit not held (`apply.test.ts` replays the ordering).
- **Local state belongs to a principal**: App B's shadow comparison
  signed users in one after another in one browser, sharing one local
  database file. Units named by (rule, params) took one curator's synced
  state for the next curator's, and the engine resumed it (its units were
  warm). Electric avoided it only because App B's proxy puts the user id
  into each shape's `where`. Units are now named per principal (section
  7, catalogue 7b).
- **nginx closes idle WebSockets at 60 s** (`proxy_read_timeout`), which
  is the default on most routes an app would mount the engine on. Hence
  the `BEAT` messages (section 9.1).
- **nginx's `proxy_buffering` does not hold a stream**: it still forwards
  data as it arrives. The hostile case needs a real holding proxy
  (`ts/harness/src/middlebox.ts`).
- **The gateway exposed a race in localdb**: in `multi` mode a reader
  opened its connection outside the read fence. When the first sync
  frame landed while readers booted, a torn header read poisoned the
  reader for good (`SQLITE_CORRUPT` on every query, since fence mode
  never reopens). It failed 9 of 18 browser runs through the gateway
  and 0 direct, where the timing differs. The open now takes the fence:
  0 of 9 failed (`reader.worker.ts`, uncommitted in `../localdb`).

## Decisions

Agreed with the user on 2026-10-29, amending the plan where noted.

- **A1. The four-idea core** (section 2) replaces separate mechanisms for
  snapshots, move-ins, holds, resync, priorities and subscribe.
- **A2. Engine state is a cache; no disk log or checkpoints in v1.**
  Amends plan decision 6 (bounded durable log): resume is an in-memory
  window plus checksum reconcile; a disk log is an optimisation (section
  15). Consequently: temporary replication slot, lazy boot with no
  exported snapshot (amends decision 7's boot clause), on-demand fence
  instead of a periodic heartbeat table (amends decision 13).
- **A3. Postgres 18 only.** App A upgrades from 17.4; recommended with
  it: Electric 1.7.6 -> 1.7.9 so the
  baseline is one Electric version.
- **A4. Tiers are evaluation strategies of restricted re-evaluation**;
  the plan's key decomposition (decision 4) becomes sharing of identical
  normalised subtrees.
- **A5. Second app is App A** (private corpus), equal in priority to
  App B. It adds client parameters with mandatory `$sub` gates and rule
  groups (section 3).
- **A6. Review gate before Phase 1** (the plan said before Phase 2).
- **A8. REPLICA IDENTITY FULL only where old values are read** (Q1
  resolved): tables inside some rule's subqueries. Other tables keep their
  identity (never downgraded, so Electric's own FULL settings survive
  migration). Unchanged TOAST columns of a member row keep their stored
  value; a row entering a unit with an unsent value is fetched by key.
  Measured: avoids up to 200x WAL per update on rows with large
  out-of-line values (`docs/bench/dense.md`); catalogue 11c covers it.
- **A9. Column-delta updates (`PATCH`).** Units store one hash per
  projected column per member; an update sends only the columns whose hash
  changed, plus the new row hash (the client does not keep row bytes). Row
  hash = sum of column hashes; each row enters the unit checksum through
  a mix (section 7), so checksums stay additive per row. Measured: a
  one-int update of a 12 kB row went from 11,973 to 49 bytes (Electric:
  365); a jsonb edit from 11,976 to 1,686 (Electric: 2,207).
- **A7. Node is the latest release, pinned by pnpm**: 26.13.0 via
  `useNodeVersion` (amends the plan's `node >=22.13`; the reference client
  is sqlite-wasm, so `node:sqlite` is not needed).
- **A10. Rules have one-table output** (was Q2): a rule is `SELECT
  <columns or *> FROM <table> WHERE ...`; joins live in subqueries and
  gates. Every corpus shape from both apps fits.
- **A11. Gates: SQL for anything revocable, claims for the rest** (was
  Q4). Both are supported; SQL gates on tables are live (unit gates,
  section 3.2a), claim conjuncts are fresh only at token refresh.
- **A12. The reference client is localdb's real code on sqlite-wasm**
  (was Q6), in Node for the harness and simulation and in headless Chrome
  for the browser smoke.
- **A13. Read-your-writes is an app convention, not protocol** (was Q10):
  the app writes `(client_id, last_mutation_id)` in its transaction and
  the client wants a rule over its own row; the confirmation arrives in
  the same atomic frame as the data. No `WAIT` message (catalogue 13).
- **A14. Public, MIT, like localdb** (was Q7): `LICENSE` matches localdb's;
  `@nightride/localsync` is published from `ts/transport`.
- **A15. permessage-deflate, per message, 512-byte threshold** (was
  Q5). Measured on the engine's real messages (`docs/bench/deflate.md`):
  initial loads and move-ins shrink ~40%; small deltas compressed alone
  grow ~10%, so they are sent as they are; context takeover would shrink
  them ~35% more but keeps a compressor per connection (hundreds of kB in
  Go), too much at thousands of sockets. Browsers and Node negotiate it
  natively; `LOCALSYNC_WS_COMPRESSION=0` turns it off. Per-fragment zstd
  or a trained dictionary stay in the slate (section 15).
- **A16. Checksum-equal reconnect skip is on** (was Q11), after its
  root cause was found. Unit checksums summed row hashes that are
  themselves sums of column hashes, so two states that differ only by
  values swapped between rows (the simulation's "move project": two
  projects trading orgs) had equal checksums, and the skip kept the
  client's stale rows (seed 903 reproduced it every run; seed 321 earlier
  intermittently). Fixed by mixing each row hash before summing (section
  7), which also lets drift detection see such swaps. A second, smaller
  bug: a hold kept the client's `WANT`-time checksum after it started
  delivering, so a later re-snapshot (history pruned past its cursor)
  could compare against a state the client had left; that checksum is now
  dropped on first delivery. After both: seed 903 passes 3 of 3 and a
  64-seed fault soak 64 of 64 with the skip on. `LOCALSYNC_EQUAL_SKIP=0`
  turns it off.

- **A17. Shared bodies with per-principal gate units** (section 3.2a):
  rules whose principal dependence is only in gates are served from one
  body per project and a gate unit per viewer, falling back to per
  principal units whenever the split does not apply.
- **A18. No relaxed delivery; big move-ins apply in bulk** (was Q3).
  Measured (`docs/bench/movein.md`): a grant bringing in 20,000 rows held
  another unit's updates for 1.2 s, but the engine sent the move-in 36 ms
  after the grant; the rest was the client applying it row by row. Large
  runs of PUTs now take the set-based path snapshots used (previous
  member hashes read per chunk), and the move-in lands in 215 ms (Electric
  1,306 ms), so per-unit frontiers would buy at most the 36 ms and cost
  cross-unit atomicity. Updates behind a big move-in still wait for its
  apply; chunked application is the next step only if that shows up.
- **A19. App A's tenant-level and per-project files stay separate** (was
  Q9). By design: one file for tenant-level data for the session and one
  per project (many, LRU-evicted), two `LocalDb` instances with no
  `ATTACH`; the project files deliberately mirror no tenant tables, so no
  query joins the two and each file gets its own atomic frames. Nothing to build.
  Re-opening an evicted or idle project reconciles by checksum (A16),
  and shared bodies (A17) make a project's units shared by its team.

- **A20. `alignCommits` in localdb: one reader for live queries** (was
  Q8). First built as exact commit labels plus per-reader marks with held
  delivery; the collab soak showed readers on independent ticks almost
  never sit on the same commit under steady writes (22 of 31 releases fell
  back to a timeout, still torn, 600 ms p50). One reader per database for
  live queries is exact by construction and costs ~6 ms p50 at that load;
  the marks were removed. Off by default (live-query parallelism matters
  for heavy aggregates); screens in either app that combine queries
  should turn it on.

- **A21. HTTP fallback, no WebTransport.**
  - App A runs on Electric today, which works over plain HTTP. Moving
    App A to WebSocket-only sync would break it on every customer network
    where Electric works only because it is plain HTTP. So `/sync/http`
    (section 9.1) ships with the engine, chosen automatically by the
    client.
  - It is one mode that degrades to long polls, not streaming alone,
    because the proxies that break WebSockets often also buffer
    responses.
  - WebTransport (QUIC) is not planned:
    - Its benefits are separate streams, connection migration and 0-RTT.
      We deliver one ordered stream per client by design, and a resume
      already costs about a millisecond.
    - UDP is often blocked on enterprise networks, so it would need this
      fallback anyway.
    - QUIC in Go runs in user space, so it costs more CPU per connection.
  - Revisit if measured mobile reconnects matter.

- **A22. Default-deny rules; grants in the token; per-deployment
  audience.**
  - Electric authorises in each app's proxy, per route. localsync has no
    routes: any valid token can want any rule. So a rule with no
    declared access would be readable by everyone, and the first corpus
    conversion did exactly that (an admin route's shape became a rule
    with no condition).
  - Every rule now declares access (section 3.2), or the engine refuses
    to load it.
  - Coarse rights travel as `grants` in the token, decided by the app's
    own code, and are checked at every `WANT`.
  - Data-derived permissions stay SQL gates, which revoke in the same
    transaction.
  - The JWT audience is per deployment (`--audience`): App A runs one
    engine per tenant database, and with a shared secret a token for one
    tenant must not open another's engine.
  - Rejected:
    - **An app proxy in the middle.** Admitting wants there is what
      claims do, without proxying WebSockets and the HTTP fallback in
      every app.
    - **Filtering rows there.** That would mean decoding frames and
      recomputing checksums, a second sync engine. Shared bodies (A17)
      already give common buckets with a small per-viewer part.
  - Catalogue 10c tests the grant, the refusal, and revocation at the
    next token.
  - App B's audit (private, with the corpus) turned every one of its 86
    routes' middleware into rules:
    - Its roles are table rows, so each became a unit gate, not a claim.
      All 86 rules load, with the same tiers as before.
    - Revocation goes from the next long poll (about 20 s) to the same
      transaction.
    - `engine plan`'s Access column (`$sub gate`, `$sub rows`, both,
      claims, public) is the review surface. It calls a condition "rows"
      only when the condition provably reads the row's own columns.

## Open questions

None open. New ones go here, with proposals in brackets.
