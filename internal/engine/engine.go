// Package engine is the localsync hub: it routes committed transactions to
// units, resolves restricted queries against spliced snapshots, and sends
// each client atomic frames up to its frontier (docs/design.md sections
// 2, 5, 6, 7). All state lives in one goroutine (Engine.loop).
package engine

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/nightridefm/localsync/internal/catalog"
	"github.com/nightridefm/localsync/internal/repl"
	"github.com/nightridefm/localsync/internal/rules"
	"github.com/nightridefm/localsync/internal/wire"
)

type Config struct {
	PG          string
	Rules       []rules.Rule
	JWTSecret   []byte
	Window      time.Duration
	Workers     int
	Publication string
}

type stats struct {
	snapshotQueries int // queries run to give a client a unit's snapshot
	cacheHits       int // snapshots served from the cache instead
}

type sharedRule struct {
	body  *rules.Analysis
	gates []sharedGate
}

type sharedGate struct {
	a   *rules.Analysis
	neg bool
}

type depRef struct {
	a   *rules.Analysis
	occ *rules.Occurrence
}

type txnRec struct {
	xid uint32
	lsn uint64
	at  time.Time
}

type Engine struct {
	cfg   Config
	pool  *pgxpool.Pool
	cat   *catalog.Catalog
	epoch string

	analyses map[string]*rules.Analysis
	// shared: wantable rules served from a shared body plus per-principal
	// gate units (rules.Share). The derived analyses route like any other
	// but are never wantable themselves.
	shared  map[*rules.Analysis]*sharedRule
	byTable map[string][]*rules.Analysis
	byDep   map[string][]depRef
	units   map[string]*Unit
	byRule  map[*rules.Analysis]map[*Unit]struct{}
	nextRef uint32
	clients map[*Client]struct{}

	head uint64
	// ringXid indexes ring commits by (32-bit) xid, for boundary().
	ringXid map[uint32]uint64
	// headEnd is the end of the last routed commit record. A query whose
	// snapshot LSN is at or before it cannot have missed a commit: every
	// WAL record before that point has been streamed (no fence needed).
	headEnd uint64
	start   uint64
	ring    []txnRec
	// relMap caches relation column order -> catalog column index.
	relMap map[uint32][]int

	stream  *repl.Stream
	results chan *query
	work    chan job
	// next collects queries for the next snapshot round; one round runs at
	// a time and every query in it shares one exported snapshot, so units
	// resolved together share their boundary B.
	next      []*query
	roundBusy bool
	requests  chan func()
	waiting   []*query // results waiting for the stream to pass lsnS
	stats     *stats
	restarts  int // in-process restarts after a lost replication stream
	fencing   bool
	fenceAt   time.Time
}

func New(ctx context.Context, cfg Config) (*Engine, error) {
	if cfg.Window == 0 {
		cfg.Window = 5 * time.Minute
	}
	if cfg.Workers == 0 {
		cfg.Workers = 4
	}
	if cfg.Publication == "" {
		cfg.Publication = "localsync"
	}
	pcfg, err := pgxpool.ParseConfig(cfg.PG)
	if err != nil {
		return nil, err
	}
	pcfg.ConnConfig.RuntimeParams["TimeZone"] = "UTC"
	pcfg.ConnConfig.RuntimeParams["DateStyle"] = "ISO, YMD"
	pcfg.MaxConns = int32(cfg.Workers + 3)
	pool, err := pgxpool.NewWithConfig(ctx, pcfg)
	if err != nil {
		return nil, err
	}
	var b [8]byte
	_, _ = rand.Read(b[:])
	e := &Engine{
		cfg: cfg, pool: pool, epoch: hex.EncodeToString(b[:]),
		units: map[string]*Unit{}, byRule: map[*rules.Analysis]map[*Unit]struct{}{},
		clients: map[*Client]struct{}{}, relMap: map[uint32][]int{}, ringXid: map[uint32]uint64{},
		results: make(chan *query, 1024), work: make(chan job, 1024), requests: make(chan func(), 1024),
	}
	if err := e.setup(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	return e, nil
}

// setup loads the catalog and rules, installs the sync schema, manages the
// publication and replica identity, and opens the replication stream.
func (e *Engine) setup(ctx context.Context) error {
	if _, err := e.pool.Exec(ctx, `
		CREATE SCHEMA IF NOT EXISTS sync;
		CREATE TABLE IF NOT EXISTS sync.fence (id int PRIMARY KEY, n bigint NOT NULL);
		INSERT INTO sync.fence VALUES (1, 0) ON CONFLICT DO NOTHING`); err != nil {
		return fmt.Errorf("sync schema: %w", err)
	}
	cat, err := catalog.Load(ctx, e.pool)
	if err != nil {
		return err
	}
	e.cat = cat
	e.analyses = map[string]*rules.Analysis{}
	e.byTable = map[string][]*rules.Analysis{}
	e.byDep = map[string][]depRef{}
	tables := map[string]bool{}
	e.shared = map[*rules.Analysis]*sharedRule{}
	register := func(a *rules.Analysis) {
		e.byTable[a.Table.Name] = append(e.byTable[a.Table.Name], a)
		for i := range a.Occurrences {
			o := &a.Occurrences[i]
			e.byDep[o.Table] = append(e.byDep[o.Table], depRef{a: a, occ: o})
		}
		for _, d := range a.Deps {
			tables[d] = true
		}
	}
	for _, r := range e.cfg.Rules {
		a, err := rules.Analyze(r, cat)
		if err != nil {
			return fmt.Errorf("rule %s: %w", r.Name, err)
		}
		e.analyses[r.Name] = a
		register(a)
		sr := e.shareRule(ctx, a, cat)
		if sr != nil {
			e.shared[a] = sr
			register(sr.body)
			for _, g := range sr.gates {
				register(g.a)
			}
		}
		slog.Info("rule", "name", r.Name, "table", a.Table.Name, "tier", a.Tier, "fast", a.Fast, "deps", a.Deps, "shared", sr != nil)
	}
	names := []string{"sync.fence"}
	for t := range tables {
		names = append(names, rules.Ident(t))
	}
	// REPLICA IDENTITY FULL only where old values are read: tables inside
	// some rule's subqueries (restriction values, column pruning). A rule's
	// own table does not need it (units keep per-column hashes), and FULL
	// logs the whole detoasted old row on every update (design Q1). Never
	// downgrade: another consumer (Electric during migration) may need FULL.
	for t := range e.byDep {
		var ident string
		if err := e.pool.QueryRow(ctx, "SELECT relreplident::text FROM pg_class WHERE oid = $1::regclass", rules.Ident(t)).Scan(&ident); err != nil {
			return fmt.Errorf("replica identity %s: %w", t, err)
		}
		if ident == "f" {
			continue
		}
		if _, err := e.pool.Exec(ctx, "ALTER TABLE "+rules.Ident(t)+" REPLICA IDENTITY FULL"); err != nil {
			return fmt.Errorf("replica identity %s: %w", t, err)
		}
	}
	sort.Strings(names)
	var exists bool
	if err := e.pool.QueryRow(ctx, "SELECT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = $1)", e.cfg.Publication).Scan(&exists); err != nil {
		return err
	}
	verb := "CREATE PUBLICATION " + rules.Ident(e.cfg.Publication) + " FOR TABLE "
	if exists {
		verb = "ALTER PUBLICATION " + rules.Ident(e.cfg.Publication) + " SET TABLE "
	}
	if _, err := e.pool.Exec(ctx, verb+strings.Join(names, ", ")); err != nil {
		return fmt.Errorf("publication: %w", err)
	}
	slot := "localsync_" + e.epoch
	e.stream, err = repl.Open(ctx, e.cfg.PG, slot, e.cfg.Publication)
	if err != nil {
		return err
	}
	e.start = e.stream.Start
	e.head = e.stream.Start
	e.headEnd = e.stream.Start
	return nil
}

// shareRule turns a rule's sharing proposal into analyses, or returns nil
// (the rule stays per principal) when any derived rule fails analysis or
// Postgres refuses it (an unqualified name in a gate that resolves to the
// outer table would change meaning once the gate stands alone).
func (e *Engine) shareRule(ctx context.Context, a *rules.Analysis, cat *catalog.Catalog) *sharedRule {
	if a.Share == nil || os.Getenv("LOCALSYNC_SHARE") == "0" {
		return nil
	}
	check := func(r rules.Rule) *rules.Analysis {
		d, err := rules.Analyze(r, cat)
		if err != nil {
			slog.Info("rule not shared", "rule", a.Rule.Name, "derived", r.Name, "err", err)
			return nil
		}
		conn, err := e.pool.Acquire(ctx)
		if err != nil {
			return nil
		}
		defer conn.Release()
		if _, err := conn.Conn().Prepare(ctx, "", d.SQL); err != nil {
			slog.Info("rule not shared", "rule", a.Rule.Name, "derived", r.Name, "err", err)
			return nil
		}
		return d
	}
	sr := &sharedRule{body: check(a.Share.Body)}
	if sr.body == nil {
		return nil
	}
	for _, g := range a.Share.Gates {
		d := check(g.Rule)
		if d == nil {
			return nil
		}
		sr.gates = append(sr.gates, sharedGate{a: d, neg: g.Negated})
	}
	return sr
}

// Run drives the engine until ctx ends. A lost replication stream does not
// end it: the engine reconnects and starts a new epoch (restart).
func (e *Engine) Run(ctx context.Context) error {
	e.stats = &stats{}
	for range e.cfg.Workers {
		go func() {
			for j := range e.work {
				j.q.run(ctx, e.pool, j.snapID)
				j.done()
			}
		}()
	}
	tick := time.NewTicker(100 * time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case t, ok := <-e.stream.Txns:
			if !ok {
				err := e.stream.Err()
				if err == nil {
					err = errors.New("replication stream ended")
				}
				if err := e.restart(ctx, err); err != nil {
					return err
				}
				continue
			}
			e.route(t)
			e.afterChange()
		case q := <-e.results:
			e.waiting = append(e.waiting, q)
			e.afterChange()
		case fn := <-e.requests:
			fn()
			e.afterChange()
		case <-tick.C:
			e.prune()
			e.pruneSnapCache()
			e.afterChange()
			e.heartbeat()
		}
	}
}

// restart recovers from a lost replication stream inside the process, the
// same way a process restart would but without dropping client sockets:
// engine state is a cache (design A2), so it is thrown away, a new slot is
// opened under a new epoch, and every client is asked to send its WANT
// again. That WANT carries the old epoch, so it reconciles by checksum
// exactly like a reconnect. Retries with backoff until ctx ends.
func (e *Engine) restart(ctx context.Context, cause error) error {
	slog.Warn("replication stream lost; restarting in process", "err", cause)
	e.restarts++
	backoff := 250 * time.Millisecond
	for {
		var b [8]byte
		_, _ = rand.Read(b[:])
		e.epoch = hex.EncodeToString(b[:])
		err := e.setup(ctx)
		if err == nil {
			break
		}
		slog.Warn("restart: setup failed; retrying", "err", err, "in", backoff)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}
		backoff = min(backoff*2, 10*time.Second)
	}
	// Queries still running belong to the old units; their results are
	// dropped (resolveWaiting and apply check that the unit is current).
	// roundBusy and fencing clear themselves when those finish.
	e.units = map[string]*Unit{}
	e.byRule = map[*rules.Analysis]map[*Unit]struct{}{}
	e.ring = nil
	e.ringXid = map[uint32]uint64{}
	e.relMap = map[uint32][]int{}
	e.next = nil
	e.waiting = nil
	for c := range e.clients {
		c.holds = nil
		c.cursor = 0
		c.removals = nil
		c.stream = nil
		c.forceFrame = false
		// A frame cut short by this is never completed; the client drops
		// its buffered parts on this message.
		c.sendJSON(map[string]any{"type": "resync", "epoch": e.epoch})
	}
	slog.Info("restarted", "epoch", e.epoch, "clients", len(e.clients))
	return nil
}

// Do runs fn on the engine goroutine.
func (e *Engine) Do(fn func()) { e.requests <- fn }

func (e *Engine) afterChange() {
	e.resolveWaiting()
	e.schedule()
	e.deliver()
	e.fenceIfNeeded()
}

// ---- routing ----

func (e *Engine) rowOf(rel *repl.Relation, t *catalog.Table, vals [][]byte) rules.Row {
	if vals == nil {
		return nil
	}
	m, ok := e.relMap[rel.ID]
	if !ok || len(m) != len(rel.Columns) {
		m = make([]int, len(rel.Columns))
		for i, c := range rel.Columns {
			m[i] = t.ColumnIndex(c)
		}
		e.relMap[rel.ID] = m
	}
	row := make(rules.Row, len(t.Columns))
	for i, ci := range m {
		if ci >= 0 && i < len(vals) {
			row[ci] = vals[i]
		}
	}
	return row
}

func (e *Engine) route(t *repl.Txn) {
	touched := map[*Unit]struct{}{}
	for _, c := range t.Changes {
		if c.Rel == nil || c.Rel.Namespace != "public" {
			continue
		}
		tbl, ok := e.cat.Table(c.Rel.Name)
		if !ok {
			continue
		}
		ch := change{lsn: t.Commit, old: e.rowOf(c.Rel, tbl, c.Old), new: e.rowOf(c.Rel, tbl, c.New), oldKeyOnly: c.OldKeyOnly}
		if ch.new != nil && c.Unchanged != nil {
			// Unchanged TOAST values are not sent. A FULL old row has them;
			// otherwise (DEFAULT identity) they are unknown, and units keep a
			// member's stored column or fetch the row by key.
			m := e.relMap[c.Rel.ID]
			for i, unch := range c.Unchanged {
				if !unch || m[i] < 0 {
					continue
				}
				if ch.old != nil && !ch.oldKeyOnly {
					ch.new[m[i]] = ch.old[m[i]]
					continue
				}
				if ch.unknown == nil {
					ch.unknown = make([]bool, len(tbl.Columns))
				}
				ch.unknown[m[i]] = true
			}
		}
		// An update that leaves every column a rule reads unchanged cannot
		// change membership (Analysis.RefCols): content-only on the rule's
		// table, irrelevant on the tables inside its subqueries. It needs a
		// complete old row to tell.
		update := c.Kind == 'U' && ch.old != nil && !ch.oldKeyOnly && ch.new != nil
		for _, a := range e.byTable[tbl.Name] {
			content := update && sameCols(ch.old, ch.new, a.RefCols[tbl.Name]) && !unknownIn(ch.unknown, a.RefCols[tbl.Name])
			for u := range e.byRule[a] {
				if content {
					u.contentChange(ch)
				} else {
					u.ownChange(ch)
				}
				touched[u] = struct{}{}
			}
		}
		for _, d := range e.byDep[tbl.Name] {
			if update && sameCols(ch.old, ch.new, d.a.RefCols[tbl.Name]) && !unknownIn(ch.unknown, d.a.RefCols[tbl.Name]) {
				continue
			}
			for u := range e.byRule[d.a] {
				if ch.oldKeyOnly || unknownIn(ch.unknown, d.a.RefCols[tbl.Name]) {
					// Tables inside subqueries are kept REPLICA IDENTITY FULL;
					// if one is not, the old values are missing, so re-evaluate.
					u.addRestriction(restriction{lsn: t.Commit, full: true})
					continue
				}
				u.depChange(t.Commit, d.occ, tbl, ch.old, ch.new)
			}
		}
	}
	for _, rel := range t.Truncates {
		if rel == nil || rel.Namespace != "public" {
			continue
		}
		for _, a := range e.byTable[rel.Name] {
			for u := range e.byRule[a] {
				u.truncate(t.Commit)
				touched[u] = struct{}{}
			}
		}
		for _, d := range e.byDep[rel.Name] {
			for u := range e.byRule[d.a] {
				u.addRestriction(restriction{lsn: t.Commit, full: true})
			}
		}
	}
	for u := range touched {
		u.commit(t.Commit)
	}
	e.head = t.Commit
	e.headEnd = t.End
	e.ring = append(e.ring, txnRec{xid: t.Xid, lsn: t.Commit, at: time.Now()})
	e.ringXid[t.Xid] = t.Commit
}

func unknownIn(unknown []bool, cols []int) bool {
	if unknown == nil {
		return false
	}
	for _, i := range cols {
		if unknown[i] {
			return true
		}
	}
	return false
}

func sameCols(a, b rules.Row, cols []int) bool {
	for _, i := range cols {
		if string(a[i]) != string(b[i]) || (a[i] == nil) != (b[i] == nil) {
			return false
		}
	}
	return true
}

// ---- queries ----

func (e *Engine) schedule() {
	if e.roundBusy {
		return
	}
	for _, u := range e.units {
		if u.inflight || u.state == creating || len(u.pending) == 0 {
			continue
		}
		q := &query{u: u}
		for _, r := range u.pending {
			if r.lsn > e.head {
				break
			}
			q.restr = append(q.restr, r)
			q.maxLSN = r.lsn
			if r.full {
				q.full = true
			}
		}
		if q.full || !u.a.Fast && len(q.restr) > 64 {
			q.full = true
		}
		if u.a.Fast {
			// Fast units go pending on gate or TRUNCATE changes, and on
			// fetches (a row entering with an unsent TOAST value). They buffer
			// their own changes meanwhile, which only a whole-unit evaluation
			// covers, so their queries are always full.
			q.full = true
		}
		u.inflight = true
		e.next = append(e.next, q)
	}
	if len(e.next) == 0 {
		return
	}
	qs := e.next
	e.next = nil
	e.roundBusy = true
	go e.runRound(context.Background(), qs)
}

type job struct {
	q      *query
	snapID string
	done   func()
}

// runRound runs queries in one snapshot: a leader transaction exports it
// and every worker imports it, so all results share S, lsnS and B.
func (e *Engine) runRound(ctx context.Context, qs []*query) {
	defer e.Do(func() { e.roundBusy = false })
	finish := func() {
		for _, q := range qs {
			e.results <- q
		}
	}
	if len(qs) == 1 {
		done := make(chan struct{})
		e.work <- job{q: qs[0], done: func() { close(done) }}
		<-done
		finish()
		return
	}
	err := pgx.BeginTxFunc(ctx, e.pool, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly}, func(tx pgx.Tx) error {
		var id, snap, lsn string
		if err := tx.QueryRow(ctx, "SELECT pg_export_snapshot(), pg_current_snapshot()::text, pg_current_wal_insert_lsn()::text").Scan(&id, &snap, &lsn); err != nil {
			return err
		}
		sn, err := parseSnapshot(snap)
		if err != nil {
			return err
		}
		l, err := parseLSN(lsn)
		if err != nil {
			return err
		}
		var wg sync.WaitGroup
		for _, q := range qs {
			q.snap, q.lsnS = sn, l
			wg.Add(1)
			e.work <- job{q: q, snapID: id, done: wg.Done}
		}
		wg.Wait()
		return nil
	})
	if err != nil {
		for _, q := range qs {
			if q.err == nil {
				q.err = err
			}
		}
	}
	finish()
}

func (e *Engine) createUnit(a *rules.Analysis, params []string, key string) *Unit {
	e.nextRef++
	u := &Unit{key: key, ref: e.nextRef, a: a, params: params, members: map[string][]uint64{}, regHead: e.head}
	u.proj = newProjection(a.Table, a.Columns)
	u.qproj = queryProjection(u.proj)
	e.units[key] = u
	if e.byRule[a] == nil {
		e.byRule[a] = map[*Unit]struct{}{}
	}
	e.byRule[a][u] = struct{}{}
	u.inflight = true
	e.next = append(e.next, &query{u: u, full: true})
	return u
}

func (e *Engine) dropUnit(u *Unit) {
	delete(e.units, u.key)
	delete(e.byRule[u.a], u)
}

// boundary computes B for a query whose snapshot was taken at lsnS, once
// the stream has passed it. ok is false for an irregular snapshot.
func (e *Engine) boundary(q *query) (uint64, bool) {
	// The ring is in commit order. B is the newest visible commit below
	// lsnS: scan back from lsnS (only in-progress or later transactions sit
	// on top of it, so this stops quickly).
	b := e.start
	i := sort.Search(len(e.ring), func(i int) bool { return e.ring[i].lsn >= q.lsnS })
	for i--; i >= 0; i-- {
		if q.snap.visible(e.ring[i].xid) {
			b = e.ring[i].lsn
			break
		}
	}
	// Irregular: an invisible commit below B. Only transactions in the
	// snapshot's in-progress list can be: one that started after the
	// snapshot also committed after every visible one.
	for xid := range q.snap.xip {
		if lsn, ok := e.ringXid[uint32(xid)]; ok && lsn < b && q.snap.widen(uint32(xid)) == xid {
			return 0, false
		}
	}
	return b, true
}

func (e *Engine) resolveWaiting() {
	keep := e.waiting[:0]
	for _, q := range e.waiting {
		if q.err == nil && !e.passed(q.lsnS) {
			keep = append(keep, q)
			continue
		}
		e.resolve(q)
	}
	e.waiting = keep
}

func (e *Engine) retry(q *query, full bool) {
	n := &query{u: q.u, full: full || q.full, forHolds: q.forHolds, restr: q.restr, maxLSN: q.maxLSN}
	e.next = append(e.next, n)
}

func (e *Engine) resolve(q *query) {
	u := q.u
	if e.units[u.key] != u {
		return // dropped meanwhile
	}
	if q.err != nil {
		slog.Error("query failed", "unit", u.key, "err", q.err)
		if q.forHolds {
			u.snapIn = false
			for _, h := range u.waiters {
				h.c.fail("query failed: " + q.err.Error())
			}
			u.waiters = nil
		} else {
			u.inflight = false
			if u.state == creating {
				for c := range e.clients {
					for _, h := range c.holds {
						if h.u == u {
							c.fail("query failed: " + q.err.Error())
						}
					}
				}
			}
		}
		return
	}
	if q.verify {
		e.verifyResult(q)
		return
	}
	b, regular := e.boundary(q)
	if os.Getenv("LOCALSYNC_DEBUG") != "" {
		slog.Info("resolve", "unit", u.key[:min(len(u.key), 40)], "full", q.full, "forHolds", q.forHolds, "b", b, "regular", regular,
			"lsnS", q.lsnS, "head", e.head, "headEnd", e.headEnd, "maxLSN", q.maxLSN, "rows", len(q.res), "xmin", q.snap.xmin, "xmax", q.snap.xmax, "xip", len(q.snap.xip))
	}
	if !regular || b < q.maxLSN || (u.state == creating && !q.forHolds && b < u.regHead) {
		e.retry(q, false)
		return
	}
	if q.forHolds {
		e.resolveSnapshot(q, b)
		return
	}
	if !q.full {
		for _, r := range u.pending {
			if r.lsn > b {
				break
			}
			if r.lsn > q.maxLSN {
				// Committed before B but not included in the restriction:
				// re-evaluate the whole unit, which covers everything up to
				// its own boundary and therefore always resolves.
				e.retry(q, true)
				return
			}
		}
	}
	e.apply(q, b)
	if verifyMode && !q.verify {
		e.next = append(e.next, &query{u: u, full: true, verify: true})
	}
}

// verifyMode (LOCALSYNC_VERIFY): after every resolution, re-evaluate the
// unit and compare with its own state, logging any difference. A
// differential check of the incremental paths against the definition.
var verifyMode = os.Getenv("LOCALSYNC_VERIFY") != ""

func (e *Engine) verifyResult(q *query) {
	u := q.u
	if e.units[u.key] != u || q.err != nil || u.state != ready {
		return
	}
	b, regular := e.boundary(q)
	// Comparable only if the unit's state is its state at b: nothing
	// pending, and no history after b.
	if !regular || len(u.pending) > 0 || u.inflight || (len(u.hist) > 0 && u.hist[len(u.hist)-1].lsn > b) || len(u.buffered) > 0 {
		return
	}
	want := map[string]uint64{}
	if q.gateOK {
		for _, r := range q.res {
			want[r.key] = rowSum(r.hashes)
		}
	}
	var bad []string
	for key, hs := range u.members {
		var h uint64
		for _, x := range hs {
			h += x
		}
		if w, ok := want[key]; !ok {
			bad = append(bad, "extra "+hex.EncodeToString([]byte(key)))
		} else if w != h {
			bad = append(bad, "stale "+hex.EncodeToString([]byte(key)))
		}
	}
	for key := range want {
		if _, ok := u.members[key]; !ok {
			bad = append(bad, "missing "+hex.EncodeToString([]byte(key)))
		}
	}
	if len(bad) > 0 {
		slog.Error("VERIFY MISMATCH", "unit", u.key, "b", b, "head", e.head, "rows", bad)
	}
}

// apply installs a query result as the unit's state at b.
func (e *Engine) apply(q *query, b uint64) {
	u := q.u
	creatingNow := u.state == creating
	// The unit was last known just before its first pending restriction.
	gapFrom := u.known(e.head)
	if len(u.pending) > 0 {
		gapFrom = u.pending[0].lsn - 1
	}
	u.gateOK = q.gateOK
	if q.full {
		seen := make(map[string]bool, len(q.res))
		if q.gateOK {
			for _, r := range q.res {
				seen[r.key] = true
				u.putRow(r.key, r.row, r.hashes)
			}
		}
		for key := range u.members {
			if !seen[key] {
				u.remove(key)
			}
		}
	} else {
		covered := map[string]bool{}
		for _, r := range q.res {
			covered[r.key] = true
			if r.member {
				u.putRow(r.key, r.row, r.hashes)
			} else {
				u.remove(r.key)
			}
		}
		for _, r := range q.restr {
			for _, key := range r.pks {
				if !covered[key] {
					u.remove(key)
				}
			}
		}
	}
	if creatingNow {
		// The creation result is the unit's starting point, not a change.
		u.cur = nil
		u.state = ready
		u.created = b
		u.histFrom = b
		u.histCount = len(u.members)
		e.snapshotWaiters(u, b, q)
	} else {
		u.commitResolution(gapFrom, b)
	}
	i := 0
	for i < len(u.pending) && u.pending[i].lsn <= b {
		i++
	}
	u.pending = u.pending[i:]
	u.inflight = false
	if len(u.pending) == 0 {
		buf := u.buffered
		u.buffered = nil
		for _, c := range buf {
			if c.lsn > b {
				// ownChange, not fastChange: a replayed change may need a
				// fetch, after which the rest must buffer again.
				u.ownChange(c)
				u.commit(c.lsn)
			}
		}
	}
}

// ---- holds and snapshots ----

// snapshotWaiters hands a fresh full result at b to holds waiting on u.
func (e *Engine) snapshotWaiters(u *Unit, b uint64, q *query) {
	if len(u.waiters) == 0 {
		// Creation results are cached even when no client waits.
		ops, sum := snapshotOps(u, q)
		e.cacheSnapshot(u, b, ops, sum)
		return
	}
	ops, sum := snapshotOps(u, q)
	e.cacheSnapshot(u, b, ops, sum)
	for _, h := range u.waiters {
		if b < h.c.cursor {
			// The client moved past this snapshot; take a newer one.
			e.requestSnapshot(h)
			continue
		}
		h.setSnapshot(b, ops, sum)
	}
	u.waiters = slices.DeleteFunc(u.waiters, func(h *Hold) bool { return !h.snapPending })
}

// snapshotOps turns a full result into PUT ops sharing its encoded rows.
func snapshotOps(u *Unit, q *query) ([]opRec, uint64) {
	if !q.gateOK {
		return nil, 0
	}
	ops := make([]opRec, len(q.res))
	var sum uint64
	for i, r := range q.res {
		sum += wire.MemberHash(rowSum(r.hashes))
		ops[i] = opRec{key: r.key, op: wire.Op{Kind: wire.OpPut, Bytes: r.row}}
	}
	return ops, sum
}

func (e *Engine) resolveSnapshot(q *query, b uint64) {
	u := q.u
	u.snapIn = false
	if b < u.created {
		e.retry(q, true)
		u.snapIn = true
		return
	}
	e.snapshotWaiters(u, b, q)
	if len(u.waiters) > 0 && !u.snapIn {
		u.snapIn = true
		e.next = append(e.next, &query{u: u, full: true, forHolds: true})
	}
}

func (e *Engine) requestSnapshot(h *Hold) {
	u := h.u
	if s := u.snap; s != nil && u.state == ready && s.b >= h.c.cursor && u.histFrom <= s.b && u.valid(s.b) == s.b {
		// Warm unit: hand over the cached snapshot; the unit's history after
		// s.b brings the client to the present. No query.
		e.stats.cacheHits++
		h.setSnapshot(s.b, s.ops, s.sum)
		u.waiters = slices.DeleteFunc(u.waiters, func(x *Hold) bool { return x == h })
		return
	}
	h.snapPending = true
	if !slices.Contains(u.waiters, h) {
		u.waiters = append(u.waiters, h)
	}
	if u.state == creating || u.snapIn {
		return
	}
	u.snapIn = true
	e.stats.snapshotQueries++
	e.next = append(e.next, &query{u: u, full: true, forHolds: true})
}

// cachedSnap is a unit's most recent full snapshot, kept briefly so clients
// joining a warm unit (reconnects after a deploy, a team opening the same
// project) need no query. Rows share the query result's encoded bytes.
type cachedSnap struct {
	b     uint64
	ops   []opRec
	sum   uint64
	at    time.Time
	bytes int
}

const (
	snapCacheTTL    = time.Minute
	snapCacheBudget = 256 << 20
)

// LOCALSYNC_SNAP_CACHE=0 turns the cache off (A/B measurements, safety valve).
var snapCacheOff = os.Getenv("LOCALSYNC_SNAP_CACHE") == "0"

func (e *Engine) cacheSnapshot(u *Unit, b uint64, ops []opRec, sum uint64) {
	if snapCacheOff || u.snap != nil && u.snap.b >= b {
		return
	}
	u.snap = &cachedSnap{b: b, ops: ops, sum: sum, at: time.Now(), bytes: opBytes(ops)}
}

// pruneSnapCache drops expired or uncovered snapshots, then the oldest
// until the cache fits its budget.
func (e *Engine) pruneSnapCache() {
	var live []*Unit
	total := 0
	for _, u := range e.units {
		s := u.snap
		if s == nil {
			continue
		}
		if time.Since(s.at) > snapCacheTTL || u.histFrom > s.b {
			u.snap = nil
			continue
		}
		live = append(live, u)
		total += s.bytes
	}
	if total <= snapCacheBudget {
		return
	}
	sort.Slice(live, func(i, j int) bool { return live[i].snap.at.Before(live[j].snap.at) })
	for _, u := range live {
		if total <= snapCacheBudget {
			break
		}
		total -= u.snap.bytes
		u.snap = nil
	}
}

// ---- fences and pruning ----

// passed reports whether every commit before lsn has been routed.
func (e *Engine) passed(lsn uint64) bool {
	return e.head >= lsn || lsn <= e.headEnd
}

// fenceIfNeeded writes to sync.fence when a result waits for the stream to
// pass its snapshot LSN and nothing else is moving the stream.
func (e *Engine) fenceIfNeeded() {
	need := false
	for _, q := range e.waiting {
		if !e.passed(q.lsnS) {
			need = true
		}
	}
	if !need || (e.fencing && time.Since(e.fenceAt) < time.Second) {
		return
	}
	e.fencing, e.fenceAt = true, time.Now()
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if _, err := e.pool.Exec(ctx, "UPDATE sync.fence SET n = n + 1 WHERE id = 1"); err != nil {
			slog.Warn("fence", "err", err)
		}
		e.Do(func() { e.fencing = false })
	}()
}

// maxHistBytes bounds the history one unit keeps for catch-up.
const maxHistBytes = 32 << 20

// prune drops history no client needs and old ring entries.
func (e *Engine) prune() {
	cutoff := time.Now().Add(-e.cfg.Window)
	i := 0
	for i < len(e.ring) && e.ring[i].at.Before(cutoff) {
		i++
	}
	if i > 0 {
		e.start = max(e.start, e.ring[i-1].lsn)
		for _, t := range e.ring[:i] {
			if e.ringXid[t.xid] == t.lsn {
				delete(e.ringXid, t.xid)
			}
		}
		e.ring = slices.Clone(e.ring[i:])
	}
	minCursor := map[*Unit]uint64{}
	for c := range e.clients {
		for _, h := range c.holds {
			for _, u := range h.units() {
				cur, ok := minCursor[u]
				if !ok || c.cursor < cur {
					minCursor[u] = c.cursor
				}
			}
		}
	}
	for _, u := range e.units {
		if u.holds <= 0 && !u.idleSince.IsZero() && u.idleSince.Before(cutoff) && !u.inflight {
			e.dropUnit(u)
			continue
		}
		keepFrom := e.start
		if cur, ok := minCursor[u]; ok && cur < keepFrom {
			keepFrom = cur
		}
		j := 0
		for j < len(u.hist) && u.hist[j].lsn <= keepFrom {
			j++
		}
		// Bounded in bytes too: past the cap the oldest entries go even if a
		// lagging client still needs them; that client gets a fresh snapshot
		// of the unit instead (below).
		bytes := u.histBytes
		for k := 0; k < j; k++ {
			bytes -= opBytes(u.hist[k].ops)
		}
		for j < len(u.hist) && bytes > maxHistBytes {
			bytes -= opBytes(u.hist[j].ops)
			j++
		}
		if j > 0 {
			keepFrom = max(keepFrom, u.hist[j-1].lsn)
			u.histCount = u.hist[j-1].count
		}
		u.histBytes = bytes
		if j > 0 {
			u.hist = slices.Clone(u.hist[j:])
		}
		u.histFrom = max(u.histFrom, keepFrom)
		for c := range e.clients {
			for _, h := range c.holds {
				if h.u == u && h.delivered && c.cursor < u.histFrom {
					h.delivered = false
					e.requestSnapshot(h)
				}
			}
		}
	}
}
