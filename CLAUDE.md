# localsync

Postgres WAL -> realtime sync engine for `@nightride/localdb` clients
(github.com/nightridefm/localdb). Replaces ElectricSQL. Generic: used by
two apps (App A and App B in the docs); nothing app-specific in the engine.

**Start here:** `README.md` (what it is; commands under Development) and
`docs/design.md` (how it works and why). The apps' real rule corpora and
the original plan are private, in `../localsync-corpus` (see its README);
never commit them or name the apps here. `corpus/` is gitignored for a
local copy.

## Philosophy

Every system fights entropy: structure is a low-entropy state that decays
unless work is continuously spent to hold it. Order is never a state you
own, only a flow you fund. Store the source of truth and derive the rest;
redundancy is stored disorder waiting to diverge. A frozen copy is
legitimate only at a boundary with a recompute-and-compare path (a cache
with a safe fallback), never as silent derived state that drifts.

Make the hard thing easy: the solution is smaller than the problem.
Intelligence is compression: one idea that explains many cases, one path
that serves many callers, one truth from which many readings follow. Every
concept, stored value, path, setting and question we add is entropy we must
keep paying to hold, so each one earns its place by the hardness it
removes. A design that needs a special case per situation, a second path
for the same job, a stored copy of what can be derived, or a question the
system could answer itself has not yet understood the problem. The test
runs both ways: the user's hard problem became easier, and our own
description of it became shorter.

In this repo: before adding a mechanism, check whether it is a special
case of the four ideas (`docs/design.md` section 2). Optimisations are
caches over the core, each with a switch back to the slow path
(section 8), and are built only when a measurement asks (section 14).

## Code style

- **pnpm** only (never npm/yarn); run node through pnpm (`pnpm node`,
  `pnpm exec`), which pins Node 26.
- TS: strict, ES modules, oxfmt + oxlint, no semicolons, single quotes, tabs.
  Format only the files you touched, never the whole repo.
- Go: gofmt / go vet; run tests with `-race`.
- **NEVER use em-dashes (U+2014)** anywhere: code, comments, docs, output.
  Use `-` or rephrase.
- Prefer `for (const x of items)` over `.forEach`.
- Locality of behaviour over DRY: duplicate a few lines rather than invent
  a shared abstraction. Pure utilities are the exception.
- Comments say why, not what, and not history: no "found by seed N",
  "v0.1.0 did X" or decision ids. Tests and git log keep the history.
- Hot paths (WAL decode, per-txn routing, per-client frame assembly, wire
  codec, client apply): small dedicated functions, preallocated / reused
  buffers, no per-row allocations or maps, type-stable variables. Profile
  with pprof (Go) / `--trace-opt --trace-deopt` (V8) before and after.

## Working rules

- Never create branches or commits on your own; leave changes uncommitted
  and ask how the user wants them.
- Deliverables (design docs, capability matrices, bench reports) are
  markdown in this repo, not external artifacts. `docs/` holds what is
  true now; plans and status logs do not go there.
- Capability tests assert observable client state against the oracle
  (the rule SQL run directly in Postgres), never protocol details.
- Any correctness shortcut must have a safe fallback (when analysis is
  unsure, re-query); a wrong rewrite is a visibility leak.
- The harness, `smash` and the browser runs share one database: run them
  one at a time.
