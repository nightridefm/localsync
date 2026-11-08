# Landscape: realtime from Postgres

Researched 2026-11-01 from public docs and announcements. Links are the
sources. Claims marked *(unverified)* were not confirmed in the docs. The
localsync column comes from this repo's harness. The other columns come from
documentation and have not been measured here, except Electric 1.7.9 (see
[`capability-matrix.md`](capability-matrix.md)).

## What changed in 2026

- **Electric has joined Databricks.** It moved into Neon on 2026-08-11, and
  Electric Cloud is shutting down. The open source sync service stays
  Apache-2.0 and self-hosted
  ([announcement](https://electric.ax/blog/2026/08/11/electric-joining-databricks),
  [Neon changelog](https://neon.com/docs/changelog/2026-08-14)). The latest
  release is `@core/sync-service` 1.8.1.
- **Neon Realtime / Live SQL** has been in public beta since 2026-10-15.
  The Electric team built it
  ([blog](https://neon.com/blog/neon-now-has-realtime),
  [docs](https://neon.com/docs/realtime/overview)).
- **Electric Circuits** (research preview, 2026-07-17) is incremental view
  maintenance (DBSP) over the WAL. It maintains joins, subqueries and
  aggregates ([post](https://electric.ax/blog/2026/07/17/electric-circuits)).
  It looks like Live SQL's engine *(unverified)*.
- **PowerSync Sync Streams** went GA on 2026-05-14
  ([announcement](https://releases.powersync.com/announcements/ann_7UgfQuNwRmpQk)).
- **Zero 1.0** shipped in June 2026
  ([InfoQ](https://infoq.com/news/2026/06/zero-version-1)).

## The closest alternatives

| | localsync | Electric 1.x | PowerSync | Neon Live SQL (beta) | Supabase Realtime |
| --- | --- | --- | --- | --- | --- |
| Mechanism | temporary logical slot; engine state is only a cache | logical slot; per-shape logs on disk | logical slot into a bucket store (MongoDB or Postgres) | logical replication; incremental work on a replica | slot read through wal2json (Postgres Changes); triggers into `realtime.messages` (Broadcast) |
| Client store | SQLite (localdb) | up to the app (TanStack DB, PGlite, memory) | SQLite | live result sets (`useLiveQuery`) | none: an event stream |
| What a client subscribes to | SQL rules with parameters and gates, several unioned per table | one table per shape: WHERE plus columns; subqueries in WHERE since 1.6 ([shapes](https://electric.ax/docs/guides/shapes)) | named streams in SQL with `auth.*` and subscription parameters ([queries](https://docs.powersync.com/sync/streams/queries)) | any deterministic SELECT, including joins and aggregates | per table, AND-only filters |
| Authorisation | JWT checked in the engine; claims and `$sub` in rules; revocation purges in the same frame | your proxy sets the WHERE and checks the token ([auth](https://electric.ax/docs/guides/auth)) | service checks the JWT; rules read its claims | queries sealed on the server; no RLS yet | RLS per subscriber per event; DELETE events are not filtered ([limits](https://supabase.com/docs/guides/realtime/postgres-changes)) |
| Joins in the subscription | multi-hop, through any tables | filters only; related rows need their own shapes | INNER JOIN on equality, nested `IN (SELECT)` | full | none |
| Atomicity on the client | one Postgres LSN across every table and rule, including subscription changes | per shape; measured torn across shapes (catalogue 6, 15) | checkpoints consistent across buckets ([consistency](https://docs.powersync.com/architecture/consistency)) | not documented | none; delivery not guaranteed |
| Client writes | your API; read-your-writes marker | your API; TanStack DB optimistic writes with `awaitTxId` ([writes](https://electric.ax/docs/guides/writes)) | persistent upload queue; checkpoints wait for it to drain | your API; `awaitTxId` | none |
| Resume | cursor plus unit checksums, sends the difference | offset plus handle; `409 must-refetch` resyncs the shape | from the last checkpoint | not documented | Broadcast Replay (alpha, 3 days) ([blog](https://supabase.com/blog/realtime-broadcast-replay)) |
| Fan-out | snapshot cache shared per unit; relays designed, not built | plain HTTP, CDN cacheable | shared buckets, horizontally scaled | Neon's ingress | one thread for Postgres Changes; Broadcast recommended above ~3k subscribers |
| Hosting, licence | self-host, MIT | self-host only (Cloud closing), Apache-2.0 | cloud or Open Edition, FSL (SDKs Apache-2.0) ([blog](https://www.powersync.com/blog/powersync-open-edition-release)) | hosted only | hosted or self-host, Apache-2.0 |
| Maturity | new; two apps | mature, future inside Databricks | GA, widely used | beta | mature |

## Others in the space

**Sync engines with a client store**

- **[Zero](https://zero.rocicorp.dev/docs/self-host)** (Rocicorp, 1.0,
  Apache-2.0). This is the closest peer in architecture.
  - One replication manager keeps a SQLite replica of Postgres, backed up to
    S3 with Litestream.
  - View syncers run IVM over that replica and keep a record of each
    client's view.
  - Clients hold data in IndexedDB or SQLite and query in ZQL, defined on
    the server.
  - Permissions are just filters in those queries
    ([auth](https://zero.rocicorp.dev/docs/auth)).
  - Writes go through custom mutators, run optimistically on the client and
    again in a server transaction.
  - Subscriptions are chosen at runtime, not from a rules file.
- **[InstantDB](https://www.instantdb.com/about)** (OSS) stores everything
  as triples in its own Postgres. It tails the WAL to invalidate queries,
  and has CEL permission rules, optimistic writes and presence. It owns the
  schema, so it cannot sit on an existing database.
- **[Convex](https://docs.convex.dev/self-hosting)** is its own
  transactional store with reactive server functions (dependency tracking,
  not WAL). It is a developer-experience competitor, not a Postgres sync
  engine.
- **[Turso Sync](https://docs.turso.tech/sync)** syncs SQLite to SQLite
  with push, pull and offline writes. It is beta, and has no Postgres
  source.
- **[LiveStore](https://docs.livestore.dev/evaluation/how-livestore-works)**
  is event sourcing materialised into local SQLite. It is not WAL-based.
- **Dormant or gone.** Triplit's founder joined Supabase in 2025. Prisma
  Pulse's docs pages are gone *(no sunset notice found)*.

**Streaming and CDC (server side, no per-user visibility)**

- **[Materialize](https://materialize.com/docs/markdown-docs/ingest-data/postgres/logical-replica/index.md)
  and RisingWave** ingest a slot, maintain views incrementally and push
  changes to backends (`SUBSCRIBE`).
- **[Sequin](https://github.com/sequinstream/sequin)** (MIT) and
  **Debezium** (3.6) send CDC to Kafka, SQS, NATS, search indexes and
  webhooks, with backfills.
- **[Hasura](https://hasura.io/docs/latest/subscriptions/postgres/livequery/index/)**
  live queries poll in batches (1 s by default), and its streaming
  subscriptions follow a cursor column. Permissions come from JWT session
  variables. It is not WAL-based.
- **Not researched:** WalEx (an Elixir WAL listener), PostGraphile `@live`,
  pg_ivm, Epsio, Jazz, Ditto, Firebase Data Connect and Liveblocks.

## Where localsync sits

**Strong points**

- **Atomicity.** The client sees one LSN across every table, rule and
  subscription change. Only PowerSync's checkpoints document something
  comparable.
- **Visibility as a tested guarantee.** No transient leaks, and a
  revocation purges in the frame of the revoking transaction. Supabase
  sends unfiltered DELETEs. Electric and Zero leave visibility to proxy
  or query code.
- **Rules are plain SQL.** They support multi-hop joins, several rules
  per table and permission gates, and the same text serves as config, spec
  and oracle.
- **Auth inside the engine.** It is short-lived, and HS256 tokens are
  refreshed in-band.
- **Efficiency.** The client receives the difference: column deltas,
  shared snapshot caches and deflate. On localdb's fixture: 8x fewer load
  bytes than Electric, and a p99 update of 5 ms against 22 ms.
- **Operations.** A temporary slot never pins WAL. There is no state to
  back up, and the client gets through proxies with HTTP fallback.

**Gaps**

- **Writes.** No offline queue or optimistic writes. This is deliberate:
  an app's API holds business logic (validation, derived fields, side
  effects in other tables, external calls). Predicting its result on the
  client means duplicating that logic there. Zero's mutators and
  PowerSync's upload queue both assume the client can do that, or that
  writes are close to plain CRUD. The read-your-writes marker covers what
  generalises: after the API returns, wait until its effects are local.
- **Fixed rules.** Rules are deployed, not chosen per query at runtime.
  That suits apps with a known data model, but query-based engines are
  more flexible.
- **No aggregates or computed columns.** Rows of one table only.
- **Cold start after restart.** Every unit is re-read from Postgres.
  Electric, PowerSync and Zero keep durable server state.
- **Single node.** Relays are designed but not built, and there is no CDN
  story.
- **One source database per engine,** and Postgres 18 only.
- **No presence or broadcast channels.**
- **Maturity.** Two apps, and no external users yet.

## Ideas for improvement

Ordered by value for the two apps over cost. Each one fits the four ideas
in [`design.md`](design.md) section 2 as a cache or a new kind of unit,
not as new semantics.

1. **A write helper around the read-your-writes marker.** The API stays
   the only write path and the source of truth. localdb could offer one
   call that sends the request, then resolves when the response's
   `client_mutation` marker is in a local commit. The UI then never shows
   the stale state between "the API said OK" and "the data arrived".
   Errors come straight from the API. This needs no business logic on the
   client. Optimistic display stays app code, written per field where the
   result is trivially predictable (a checkbox, a rename) and discarded
   when the marker lands. The marker already exists (catalogue 13).
2. **Keep swapped-away units on the client for a TTL.** Today, switching
   project and back re-sends the whole project. If the client keeps
   recently dropped units' memberships and checksums for a few minutes,
   the switch back costs only a checksum match. The rows have to stay out
   of the app's queries while they are not wanted, which takes a "parked"
   flag on the membership and a localdb view over it. Zero and PowerSync
   both offer subscription TTLs.
3. **An Electric migration path.** Electric Cloud users must move.
   `engine plan --corpus` already converts Electric shapes into rules
   mechanically. That could become a documented `engine convert`, plus a
   guide and a comparison run.
4. **Persistent slot and an on-disk unit log** (`design.md` section 14). This
   is the cold-start gap above. Measure splice load after a deploy on App
   A's real tenant sizes first. The temporary slot stays the fallback.
5. **Live counts.** Unread badges and per-project totals are the usual
   reason apps want aggregates. A `count` rule could be a unit with one
   row per group key, maintained from the membership deltas the engine
   already computes. It would be a cache over the same restricted query
   (`SELECT key, count(*) ... GROUP BY key WHERE key = ANY(changed)`).
6. **Splices on a physical replica** (slate). This moves snapshot load off
   the primary, which matters most with item 4 deferred.
7. **Relays,** once one engine's sockets or CPU are the limit. Public
   units are identical for every viewer, so their snapshots at an LSN
   could also be CDN-cacheable blobs on the HTTP transport.
8. **Sync progress in the status.** Report per-want progress (`3 of 5
   units loaded, 40 MB`) during initial sync, as PowerSync does. The
   frontier already knows it.
9. **Still open from the security review.**
   - A separate admin listener, or pprof off by default.
   - A retryable status for rate-limited wants.
   - `/health` reporting not-ready during a stream restart.
10. **Rules from RLS.** `engine plan --from-rls` could draft rules from
    existing RLS policies, for teams coming from Supabase. It is risky:
    policies use `current_setting()` and functions the rules reject. So
    it should be an offline draft for review, never live.

Not recommended: CDC sinks (Sequin and Debezium do it well), presence
(ephemeral, not WAL), and general IVM over arbitrary joins (Zero and
Circuits exist, and restricted re-queries with the safe `TRUE` fallback
are simpler to keep correct).
