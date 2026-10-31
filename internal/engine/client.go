package engine

import (
	"encoding/json"
	"fmt"
	"os"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/nightridefm/localsync/internal/catalog"
	"github.com/nightridefm/localsync/internal/rules"
	"github.com/nightridefm/localsync/internal/wire"
)

type outMsg struct {
	text bool
	data []byte
	// part: more parts of this frame follow. units: the message carries
	// unit data (an empty frame only advances the cursor). A long-poll
	// response ends after a whole frame with units (serveHTTP).
	part  bool
	units bool
	// tail: on a frame's last part, a text message that belongs with the
	// frame follows ("synced"); on that message, it is the one. A response
	// ends after the tail, not between them.
	tail bool
}

// Client is one connection. Owned by the engine loop; the WebSocket
// goroutines only exchange messages with it through out and Engine.Do.
type Client struct {
	e    *Engine
	sub  string
	out  chan outMsg
	done chan struct{}
	// queued counts bytes sent to out but not yet written to the socket
	// (decremented by the writer goroutine).
	queued atomic.Int64
	// stream is a frame still being sent in parts; pumping asked marks a
	// pending request from the writer to queue more.
	stream  *partStream
	pumping atomic.Bool
	holds   map[uint32]*Hold // by the client's own unit id
	cursor  uint64
	// removals are refs dropped by the last WANT, sent as RESET in the
	// next frame together with whatever that WANT added.
	removals []uint32
	// forceFrame: send the next frame even if empty, so a client holding
	// units it no longer wants drops them (inside that frame's transaction).
	forceFrame bool
	closed     bool
	// exp is the current token's expiry. Past it the client gets nothing
	// more: it must send a WANT with a fresh token before then.
	exp time.Time
	// grants are the current token's (A22); a WANT for a rule with claims
	// needs one of them, re-checked on every WANT (token refreshes too).
	grants []string
	// http: connected over /sync/http (metrics).
	http bool
	// lastSend: when the engine last queued a message; an idle connection
	// gets a beat so proxies' read timeouts (nginx: 60s) never cut it.
	lastSend time.Time
	// synced: "synced" was sent for the current WANT (every hold delivered
	// or failed). Reset by each WANT.
	synced bool
	// budget: new wants this client may still make (newWantBurst at most,
	// refilled at newWantRate from budgetAt), so churning through wants
	// cannot flood the database with snapshot queries or the engine with
	// lingering units.
	budget   float64
	budgetAt time.Time
}

// Hold is a client's view of one unit: waiting for a snapshot, holding an
// undelivered snapshot at snapB, or following the unit's history.
type Hold struct {
	c  *Client
	u  *Unit
	id uint32
	// key identifies the want (rule and every parameter value, $sub
	// included); u.key is the body's key when the rule is shared.
	key  string
	rule string
	// gates: for a shared rule, the client holds u's rows only while
	// every gate holds; off means it holds the unit empty (gate closed).
	gates []gateHold
	off   bool
	// reopen: the gates opened while the client held the unit empty; until
	// the body snapshot is delivered no frame may pass the cursor.
	reopen      bool
	snapPending bool
	snapB       uint64
	snapOps     []opRec
	snapSum     uint64
	snapReset   bool
	delivered   bool
	// haveSum is the client's checksum for the unit as of its WANT; valid
	// (haveOK) only until the hold first delivers.
	haveSum uint64
	haveOK  bool
	// broken: a unit the hold depends on keeps failing (Hold.failure). The
	// hold is left out of the client's frontier and held empty until the
	// unit recovers, then snapshotted afresh; brokenReset: the client may
	// still have rows of it, dropped by the next frame.
	broken      bool
	brokenReset bool
}

// failure is the error of a unit the hold depends on that keeps failing.
func (h *Hold) failure() string {
	if h.u.failures >= failAfter {
		return h.u.err
	}
	for _, g := range h.gates {
		if g.u.failures >= failAfter {
			return g.u.err
		}
	}
	return ""
}

// checkFailures breaks holds whose units keep failing and mends those
// whose units recovered. changed: the client's list of failing wants
// (failureList) is different now.
func (e *Engine) checkFailures(c *Client) (changed bool) {
	for _, h := range c.holds {
		msg := h.failure()
		switch {
		case msg != "" && !h.broken:
			// A hold behind closed gates is already empty on the client.
			h.brokenReset = !(h.delivered && h.off)
			h.broken, h.delivered, h.off, h.reopen, h.haveOK, h.snapOps = true, false, false, false, false, nil
			h.snapPending = true
			changed = true
		case msg == "" && h.broken:
			h.broken, h.brokenReset = false, false
			if h.snapPending {
				// Unless the recovering query already handed it a snapshot.
				e.requestSnapshot(h)
			}
			changed = true
		}
	}
	return changed
}

// failureList is the client's failing wants, by id.
func (c *Client) failureList() []wantErr {
	errs := []wantErr{}
	for _, h := range c.holds {
		if h.broken {
			errs = append(errs, wantErr{ID: h.id, Error: "query failed: " + h.failure()})
		}
	}
	sort.Slice(errs, func(i, j int) bool { return errs[i].ID < errs[j].ID })
	return errs
}

type gateHold struct {
	u   *Unit
	neg bool
}

// open reports whether every gate holds at lsn (known through it).
func (h *Hold) open(lsn uint64) bool {
	for _, g := range h.gates {
		if (g.u.countAt(lsn) > 0) == g.neg {
			return false
		}
	}
	return true
}

// gatesCover: every gate unit's value is known at lsn.
func gatesCover(h *Hold, lsn uint64) bool {
	for _, g := range h.gates {
		if g.u.state != ready || lsn < g.u.histFrom || g.u.known(h.c.e.head) < lsn {
			return false
		}
	}
	return true
}

// units are the units the hold's state depends on.
func (h *Hold) units() []*Unit {
	us := []*Unit{h.u}
	for _, g := range h.gates {
		us = append(us, g.u)
	}
	return us
}

// LOCALSYNC_EQUAL_SKIP=0 always resends units on reconnect.
var equalSkipOff = os.Getenv("LOCALSYNC_EQUAL_SKIP") == "0"

func (h *Hold) setSnapshot(b uint64, ops []opRec, sum uint64) {
	h.snapPending = false
	h.snapB = b
	h.snapSum = sum
	h.delivered = false
	// Checksums equal: the client keeps its state (design A16). Should it
	// still differ, the client's checksum check on later frames flags the
	// unit as drifted and its next WANT gets it whole.
	if h.haveOK && h.haveSum == sum && !equalSkipOff {
		// The client already holds exactly this state: send nothing.
		h.snapOps, h.snapReset = nil, false
		return
	}
	h.snapOps, h.snapReset = ops, true
}

// joinable: the snapshot can be handed over, because the unit's history is
// defined at snapB: known through it and snapB not inside a resolution
// gap. Only then is "snapshot, then the unit's later entries" the unit's
// own state (later PATCH ops and checksums are relative to that history).
// A snapshot that lands inside a gap is retaken.
func (h *Hold) joinable(head uint64) bool {
	u := h.u
	if u.known(head) < h.snapB {
		return false
	}
	if u.valid(h.snapB) != h.snapB {
		h.c.e.requestSnapshot(h)
		return false
	}
	return true
}

func (h *Hold) known(head uint64) uint64 {
	if h.snapPending {
		return 0
	}
	k := h.u.known(head)
	if !h.delivered {
		k = max(k, h.snapB)
	}
	for _, g := range h.gates {
		k = min(k, g.u.known(head))
	}
	return max(k, h.c.cursor)
}

type wantMsg struct {
	Type  string `json:"type"`
	Token string `json:"token"`
	Wants []struct {
		ID     uint32            `json:"id"`
		Rule   string            `json:"rule"`
		Params map[string]string `json:"params"`
	} `json:"wants"`
	Have struct {
		Epoch  string `json:"epoch"`
		Cursor string `json:"cursor"`
		Units  []struct {
			ID       uint32 `json:"id"`
			Checksum string `json:"checksum"`
		} `json:"units"`
	} `json:"have"`
}

type unitDesc struct {
	ID      uint32    `json:"id"`
	Ref     uint32    `json:"ref"`
	Rule    string    `json:"rule"`
	Table   string    `json:"table"`
	Columns []colDesc `json:"columns"`
	PK      []int     `json:"pk"`
}

type colDesc struct {
	Name string `json:"name"`
	Kind string `json:"kind"`
	Elem string `json:"elem,omitempty"`
}

type wantErr struct {
	ID    uint32 `json:"id"`
	Error string `json:"error"`
}

func (c *Client) sendJSON(v any) {
	b, _ := json.Marshal(v)
	c.send(outMsg{text: true, data: b})
}

// maxWants bounds the units one connection may hold.
const maxWants = 2000

// A connection may make newWantBurst new wants at once, then newWantRate a
// second (a want the client already holds costs nothing). Well above what
// navigating an app needs; a want over it is refused, and the client's next
// WANT may ask again.
const (
	newWantBurst = 2 * maxWants
	newWantRate  = maxWants / 60.0
)

// maxParamLen bounds a client parameter value (they are ids and keys).
const maxParamLen = 4096

// maxQueuedBytes bounds what one slow client can pin in engine memory.
const maxQueuedBytes = 64 << 20

func (c *Client) send(m outMsg) {
	if c.closed {
		return
	}
	if c.queued.Load()+int64(len(m.data)) > maxQueuedBytes {
		c.fail("client too slow")
		return
	}
	select {
	case c.out <- m:
		c.queued.Add(int64(len(m.data)))
		c.lastSend = time.Now()
	default:
		// A client that cannot keep up reconnects and reconciles.
		c.fail("client too slow")
	}
}

func (c *Client) fail(msg string) {
	if c.closed {
		return
	}
	b, _ := json.Marshal(map[string]string{"type": "error", "message": msg})
	select {
	case c.out <- outMsg{text: true, data: b}:
	default:
	}
	c.e.removeClient(c)
}

func (e *Engine) addClient(c *Client) {
	c.lastSend = time.Now()
	e.clients[c] = struct{}{}
}

func (e *Engine) removeClient(c *Client) {
	if c.closed {
		return
	}
	c.closed = true
	close(c.done)
	delete(e.clients, c)
	for _, h := range c.holds {
		e.release(h)
	}
	c.holds = nil
}

func (e *Engine) release(h *Hold) {
	u := h.u
	u.waiters = slices.DeleteFunc(u.waiters, func(x *Hold) bool { return x == h })
	u.holds--
	if u.holds <= 0 {
		// Linger for the resume window so a client that reconnects can
		// follow the unit's history instead of re-snapshotting it.
		u.idleSince = time.Now()
	}
	for _, g := range h.gates {
		g.u.holds--
		if g.u.holds <= 0 {
			g.u.idleSince = time.Now()
		}
	}
}

// want applies a WANT: the full desired set of units plus what the client
// already has (docs/design.md section 7).
func (e *Engine) want(c *Client, sub string, m *wantMsg) {
	e.m.wants++
	// What the client says it has describes its state only before this
	// connection's first frame. On a later WANT frames may be in flight
	// that the client had not applied when it wrote `have` (a removal of
	// the very unit it now wants back, say), so a new hold then always
	// gets the unit whole; holds that are kept need nothing.
	first := c.holds == nil
	c.synced = false
	if first {
		c.holds = map[uint32]*Hold{}
		if m.Have.Epoch == e.epoch {
			c.cursor, _ = strconv.ParseUint(m.Have.Cursor, 10, 64)
		}
	}
	if c.sub != "" && c.sub != sub {
		// A different principal on the same socket: start over.
		for _, h := range c.holds {
			c.removals = append(c.removals, h.u.ref)
			e.release(h)
		}
		c.holds = map[uint32]*Hold{}
	}
	c.sub = sub
	have := map[uint32]uint64{}
	for _, h := range m.Have.Units {
		if v, err := strconv.ParseUint(h.Checksum, 10, 64); err == nil {
			have[h.ID] = v
		}
	}
	sameEpoch := m.Have.Epoch == e.epoch
	wanted := map[uint32]bool{}
	for _, w := range m.Wants {
		wanted[w.ID] = true
	}
	for id := range have {
		if !wanted[id] {
			c.forceFrame = true
		}
	}
	var errs []wantErr
	keep := map[uint32]bool{}
	for i, w := range m.Wants {
		if i >= maxWants {
			errs = append(errs, wantErr{ID: w.ID, Error: fmt.Sprintf("too many wants (limit %d)", maxWants)})
			continue
		}
		a, ok := e.analyses[w.Rule]
		if !ok {
			errs = append(errs, wantErr{ID: w.ID, Error: "unknown rule " + w.Rule})
			continue
		}
		if len(a.Rule.Claims) > 0 && !slices.ContainsFunc(a.Rule.Claims, func(g string) bool { return slices.Contains(c.grants, g) }) {
			// Refused, and a hold from an earlier token is dropped below
			// (not kept), inside the next frame.
			errs = append(errs, wantErr{ID: w.ID, Error: "not allowed: the token grants none of " + strings.Join(a.Rule.Claims, ", ")})
			continue
		}
		vals := make([]string, len(a.Params))
		bad := ""
		for i, name := range a.Params {
			if name == "sub" {
				vals[i] = sub
				continue
			}
			v, ok := w.Params[name]
			if !ok {
				bad = "missing parameter " + name
				break
			}
			vals[i] = v
		}
		for name, v := range w.Params {
			if !slices.Contains(a.Rule.ClientParams, name) {
				bad = "unknown parameter " + name
			} else if len(v) > maxParamLen || strings.IndexByte(v, 0) >= 0 {
				// Postgres text cannot hold NUL; the query would only fail.
				bad = "bad parameter " + name
			}
		}
		if bad != "" {
			errs = append(errs, wantErr{ID: w.ID, Error: bad})
			continue
		}
		key := w.Rule + "\x00" + strings.Join(vals, "\x00")
		if h, ok := c.holds[w.ID]; ok {
			if h.key == key {
				keep[w.ID] = true
				continue
			}
			c.removals = append(c.removals, h.u.ref)
			e.release(h)
			delete(c.holds, w.ID)
		}
		now := time.Now()
		if c.budgetAt.IsZero() {
			c.budget = newWantBurst
		} else {
			c.budget = min(newWantBurst, c.budget+now.Sub(c.budgetAt).Seconds()*newWantRate)
		}
		c.budgetAt = now
		if c.budget < 1 {
			errs = append(errs, wantErr{ID: w.ID, Error: "too many new wants; ask again later"})
			continue
		}
		c.budget--
		h := &Hold{c: c, id: w.ID, key: key, rule: w.Rule}
		if sr := e.shared[a]; sr != nil {
			// One body per non-principal parameters, gates per principal.
			h.u = e.holdUnit(sr.body, sub, w.Params)
			for _, g := range sr.gates {
				h.gates = append(h.gates, gateHold{u: e.holdUnit(g.a, sub, w.Params), neg: g.neg})
			}
		} else {
			h.u = e.holdUnit(a, sub, w.Params)
		}
		u := h.u
		if first {
			h.haveSum, h.haveOK = have[w.ID]
		}
		c.holds[w.ID] = h
		keep[w.ID] = true
		if sameEpoch && h.haveOK && u.state == ready && c.cursor >= u.histFrom && gatesCover(h, c.cursor) {
			// Resume: the client's state at its cursor is valid; follow
			// the unit's history from there. Behind closed gates that
			// state is the empty unit.
			h.delivered, h.haveOK = true, false
			h.off = !h.open(c.cursor)
			continue
		}
		e.requestSnapshot(h)
	}
	for id, h := range c.holds {
		if !keep[id] {
			c.removals = append(c.removals, h.u.ref)
			e.release(h)
			delete(c.holds, id)
		}
	}
	descs := make([]unitDesc, 0, len(c.holds))
	for _, h := range c.holds {
		u := h.u
		d := unitDesc{ID: h.id, Ref: u.ref, Rule: h.rule, Table: u.a.Table.Name, PK: u.proj.PK}
		for _, col := range u.proj.Cols {
			cd := colDesc{Name: col.Name, Kind: col.Kind.String()}
			if col.Kind == catalog.Array {
				cd.Elem = col.Elem.String()
			}
			d.Columns = append(d.Columns, cd)
		}
		descs = append(descs, d)
	}
	sort.Slice(descs, func(i, j int) bool { return descs[i].ID < descs[j].ID })
	e.m.refused += int64(len(errs))
	// errors: refused, for good (this WANT will not be served). failures:
	// wants the engine cannot evaluate right now and keeps retrying; later
	// changes to that list come as a "failures" message.
	e.checkFailures(c)
	c.sendJSON(map[string]any{"type": "units", "epoch": e.epoch, "units": descs, "errors": errs, "failures": c.failureList()})
}

// holdUnit finds or creates the unit of a for these parameter values and
// counts a hold on it.
func (e *Engine) holdUnit(a *rules.Analysis, sub string, params map[string]string) *Unit {
	vals := make([]string, len(a.Params))
	for i, name := range a.Params {
		if name == "sub" {
			vals[i] = sub
		} else {
			vals[i] = params[name]
		}
	}
	key := a.Rule.Name + "\x00" + strings.Join(vals, "\x00")
	u := e.units[key]
	if u == nil {
		u = e.createUnit(a, vals, key)
	}
	u.holds++
	u.idleSince = time.Time{}
	return u
}

// deliver sends every client a frame up to its frontier, if it has one.
// Empty frames (cursor advance only) go out on the tick.
func (e *Engine) deliver() {
	for c := range e.clients {
		e.frame(c, false)
	}
}

func (e *Engine) heartbeat() {
	now := time.Now()
	for c := range e.clients {
		if !c.exp.IsZero() && now.After(c.exp) {
			c.fail("token expired")
			continue
		}
		e.frame(c, true)
		if now.Sub(c.lastSend) >= beatEvery {
			c.sendJSON(beatMsg)
		}
	}
}

// beatEvery: an idle connection gets a beat this often. Proxies cut idle
// connections (nginx's proxy_read_timeout defaults to 60s) and the client
// treats three missed beats as a dead connection.
const beatEvery = 15 * time.Second

var beatMsg = map[string]string{"type": "beat"}

var syncedMsg = map[string]string{"type": "synced"}

func (e *Engine) frame(c *Client, empty bool) {
	if c.holds == nil || c.stream != nil || (!c.exp.IsZero() && time.Now().After(c.exp)) {
		return
	}
	if e.checkFailures(c) {
		c.sendJSON(map[string]any{"type": "failures", "failures": c.failureList()})
	}
	f := e.head
	waiting := false
	for _, h := range c.holds {
		if h.broken {
			// Failing units hold no one back.
			continue
		}
		if h.snapPending || (!h.delivered && !h.joinable(e.head)) {
			waiting = true
		}
		f = min(f, h.known(e.head))
	}
	// Land on an LSN that is inside no unit's resolution gap: a unit that
	// jumped from p to B by query has no defined state in between.
	for {
		g := f
		for _, h := range c.holds {
			if h.broken {
				continue
			}
			if h.delivered || (!h.snapPending && h.snapB <= g) {
				g = h.u.valid(g)
			}
			for _, gh := range h.gates {
				g = gh.u.valid(g)
			}
		}
		if g == f {
			break
		}
		f = g
	}
	if waiting && len(c.removals) > 0 {
		// Removals ride with the additions of the same WANT.
		return
	}
	if f < c.cursor {
		return
	}
	// Gates that opened behind a closed hold: the client needs the body,
	// which takes a snapshot; until it can be delivered the frontier stays
	// at the cursor (the client must not be at an LSN where it may see
	// rows it does not have yet).
	for _, h := range c.holds {
		if h.delivered && h.off && f > c.cursor && h.open(f) {
			h.delivered, h.off, h.reopen, h.haveOK = false, false, true, false
			e.requestSnapshot(h)
		}
	}
	for _, h := range c.holds {
		if h.reopen && (h.snapPending || h.snapB > f || !gatesCover(h, f) || !h.joinable(e.head)) {
			return
		}
	}
	ids := make([]uint32, 0, len(c.holds))
	for id := range c.holds {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	// Removals first: an id the client re-wanted with new parameters must
	// not be wiped by the removal of its old unit.
	var frags []frag
	for _, ref := range c.removals {
		frags = append(frags, frag{ref: ref, reset: true})
	}
	c.removals = nil
	for _, id := range ids {
		h := c.holds[id]
		if h.broken {
			if h.brokenReset {
				frags = append(frags, frag{ref: h.u.ref, reset: true})
				h.brokenReset = false
			}
			continue
		}
		if h.snapPending {
			continue
		}
		u := h.u
		if !h.delivered {
			if h.snapB > f || !gatesCover(h, f) || !h.joinable(e.head) {
				continue
			}
			if !h.open(f) {
				// Behind closed gates the client holds the unit empty.
				frags = append(frags, frag{ref: u.ref, reset: true})
				h.delivered, h.off, h.reopen, h.snapOps, h.haveOK = true, true, false, nil, false
				continue
			}
			// Snapshot rows and later history fold into one net op per row:
			// the client counts each op it applies toward the checksum.
			recs, sum, n := history(u, h.snapB, f)
			all := netOps(u, append(append(make([]opRec, 0, len(h.snapOps)+len(recs)), h.snapOps...), recs...))
			if n == 0 {
				sum = h.snapSum
			}
			if h.snapReset || len(all) > 0 {
				frags = append(frags, frag{ref: u.ref, reset: h.snapReset, sum: sum, ops: all})
			}
			// From here the client's state follows the unit, so the checksum
			// it sent with WANT no longer describes it; a later re-snapshot
			// (history pruned past its cursor) must resend, never skip.
			h.delivered, h.snapOps, h.haveOK, h.off, h.reopen = true, nil, false, false, false
			continue
		}
		if h.off {
			continue
		}
		if !h.open(f) {
			// A gate closed: the client's copy goes, in this frame.
			frags = append(frags, frag{ref: u.ref, reset: true})
			h.off = true
			continue
		}
		ops, sum, n := merged(u, c.cursor, f)
		if n > 0 {
			frags = append(frags, frag{ref: u.ref, sum: sum, ops: ops})
		}
	}
	// Caught up: every hold is delivered (or failing, reported apart). The
	// client hears it after this frame, so "synced" means its database
	// holds every unit it can have, at one LSN.
	synced := !c.synced
	for _, h := range c.holds {
		if !h.broken && (!h.delivered || h.reopen) {
			synced = false
		}
	}
	if len(frags) == 0 && !c.forceFrame && (!empty || f == c.cursor) {
		if synced {
			c.synced = true
			c.sendJSON(syncedMsg)
		}
		return
	}
	c.forceFrame = false
	c.cursor = f
	c.synced = c.synced || synced
	c.stream = &partStream{lsn: f, frags: frags, synced: synced}
	e.pump(c)
}

type frag struct {
	ref   uint32
	reset bool
	sum   uint64
	ops   []wire.Op
}

// partStream is a frame being sent in parts: big frames (initial loads)
// go out in ~partBytes pieces built on demand from the shared op bytes, and
// only as fast as the socket drains, so neither a whole-frame copy per
// client nor an unbounded send queue is ever held.
type partStream struct {
	lsn    uint64
	frags  []frag
	fi, oi int
	// synced: send "synced" after the last part.
	synced bool
}

const (
	partBytes = 1 << 20
	// The writer asks for more parts when the queue drains below lowWater.
	lowWater = 4 << 20
)

// pump queues parts of the client's pending frame while its send queue is
// below lowWater. No other frame is built for the client meanwhile.
func (e *Engine) pump(c *Client) {
	for c.stream != nil && !c.closed && c.queued.Load() < lowWater {
		s := c.stream
		var body []byte
		units := 0
		size := 0
		for s.fi < len(s.frags) && (size < partBytes || units == 0) {
			fr := s.frags[s.fi]
			first := s.oi == 0
			end := s.oi
			for end < len(fr.ops) && (size < partBytes || end == s.oi) {
				size += len(fr.ops[end].Bytes) + 4
				end++
			}
			continues := end < len(fr.ops)
			body = wire.AppendUnitPart(body, fr.ref, fr.reset && first, continues, fr.sum, fr.ops[s.oi:end])
			units++
			if continues {
				s.oi = end
				break
			}
			s.fi++
			s.oi = 0
		}
		msg := byte(wire.MsgFramePart)
		if s.fi >= len(s.frags) {
			msg = wire.MsgFrame
			c.stream = nil
		}
		buf := wire.AppendPartHeader(make([]byte, 0, len(body)+16), msg, s.lsn, units)
		last := msg == wire.MsgFrame
		c.send(outMsg{data: append(buf, body...), part: !last, units: units > 0, tail: last && s.synced})
		if last && s.synced {
			b, _ := json.Marshal(syncedMsg)
			c.send(outMsg{text: true, data: b, tail: true})
		}
	}
}

// merged returns the net ops of u's history in (from, to], the checksum
// after the last entry, and how many entries contributed.
func merged(u *Unit, from, to uint64) ([]wire.Op, uint64, int) {
	recs, sum, n := history(u, from, to)
	return netOps(u, recs), sum, n
}

// history returns the ops of u's entries in (from, to] in order.
func history(u *Unit, from, to uint64) ([]opRec, uint64, int) {
	lo := sort.Search(len(u.hist), func(i int) bool { return u.hist[i].lsn > from })
	hi := sort.Search(len(u.hist), func(i int) bool { return u.hist[i].lsn > to })
	if lo >= hi {
		return nil, 0, 0
	}
	es := u.hist[lo:hi]
	var recs []opRec
	for _, e := range es {
		recs = append(recs, e.ops...)
	}
	return recs, es[len(es)-1].checksum, len(es)
}

// netOps folds ops on the same row into one, in order of first
// appearance: a later PUT or REMOVE replaces, a PATCH merges into an
// earlier PATCH or is applied to an earlier PUT.
func netOps(u *Unit, recs []opRec) []wire.Op {
	at := make(map[string]int, len(recs))
	order := make([]wire.Op, 0, len(recs))
	for _, o := range recs {
		i, seen := at[o.key]
		if !seen {
			at[o.key] = len(order)
			order = append(order, o.op)
			continue
		}
		order[i] = fold(u, order[i], o.op)
	}
	return order
}

// fold combines an earlier and a later op on the same row.
func fold(u *Unit, prev, next wire.Op) wire.Op {
	if next.Kind != wire.OpPatch {
		return next
	}
	cols := u.proj.Cols
	np, err := wire.DecodePatch(cols, next.Bytes)
	if err != nil {
		return next
	}
	switch prev.Kind {
	case wire.OpPut:
		if row, err := wire.ApplyPatch(cols, prev.Bytes, np); err == nil {
			return wire.Op{Kind: wire.OpPut, Bytes: row}
		}
	case wire.OpPatch:
		if pp, err := wire.DecodePatch(cols, prev.Bytes); err == nil {
			return wire.Op{Kind: wire.OpPatch, Bytes: wire.AppendPatch(nil, len(cols), wire.MergePatches(pp, np))}
		}
	}
	return next
}
