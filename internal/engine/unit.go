package engine

import (
	"slices"
	"sort"
	"time"

	"github.com/nightridefm/localsync/internal/catalog"
	"github.com/nightridefm/localsync/internal/rules"
	"github.com/nightridefm/localsync/internal/wire"
)

// opRec is one op on one row, keyed by encoded PK for merging.
type opRec struct {
	key string
	op  wire.Op
}

// entry is a unit's net change at one LSN (a transaction, or a query
// resolution covering everything up to that LSN).
type entry struct {
	lsn      uint64
	ops      []opRec
	checksum uint64 // unit checksum after this entry
	// gapFrom: for a query resolution, the last LSN at which the unit's
	// state was known before it. States strictly between gapFrom and lsn
	// are undefined (only the jump is known), so no frame may land there.
	// Equal to lsn for a transaction entry.
	gapFrom uint64
	count   int // members after this entry (gate units read it)
}

// restriction is one change waiting for a query: either the whole unit
// (full) or rows selected by SQL over alias r, or rows by PK.
type restriction struct {
	lsn  uint64
	full bool
	sql  string
	pks  []string // encoded PK keys of rows the SQL selects by PK
}

// change is one row change as routed: new may have columns whose value was
// not sent (unchanged TOAST without a FULL old row), marked in unknown.
// oldKeyOnly: old holds only the replica identity key (DEFAULT identity).
type change struct {
	lsn        uint64
	old, new   rules.Row
	unknown    []bool
	oldKeyOnly bool
}

type unitState uint8

const (
	creating unitState = iota
	ready
)

// Unit is a rule bound to parameter values (docs/design.md section 2).
// Owned by the engine loop; never touched from other goroutines.
type Unit struct {
	key    string
	ref    uint32
	a      *rules.Analysis
	params []string // positional values for a.Params
	proj   wire.Projection
	// qproj encodes query result rows, which arrive in projection order.
	qproj wire.Projection

	state unitState
	// members: per member row, one hash per projected column. The row hash
	// is their sum; comparing column hashes yields column deltas without
	// holding rows or old values.
	members map[string][]uint64
	sum     uint64
	gateOK  bool
	// created is the boundary of the creating query; regHead the stream
	// head when the unit was registered (the result must be at or past it).
	created uint64
	regHead uint64
	// histFrom: hist holds every entry after this LSN; histCount is the
	// member count at histFrom.
	histFrom  uint64
	histCount int

	pending  []restriction
	inflight bool
	buffered []change

	// failures counts the unit's consecutive failed queries (err is the
	// last one's); the query is retried at retryAt, backing off. From
	// failAfter on, holds stop waiting for the unit (Hold.failure).
	failures int
	err      string
	retryAt  time.Time

	hist      []entry
	histBytes int     // encoded bytes held in hist
	cur       []opRec // ops of the transaction being routed

	holds     int
	idleSince time.Time // when holds dropped to 0
	waiters   []*Hold   // holds waiting for a snapshot of this unit
	snapIn    bool      // a snapshot query for waiters is in flight
	snap      *cachedSnap
}

func newProjection(t *catalog.Table, cols []int) wire.Projection {
	p := wire.Projection{}
	for _, i := range cols {
		c := t.Columns[i]
		p.Cols = append(p.Cols, wire.Column{Name: c.Name, Kind: c.Kind, Elem: c.Elem})
		p.Src = append(p.Src, i)
	}
	for _, pk := range t.PK {
		for j, i := range cols {
			if t.Columns[i].Name == pk {
				p.PK = append(p.PK, j)
			}
		}
	}
	return p
}

// known is the LSN through which this unit's history is complete.
func (u *Unit) known(head uint64) uint64 {
	if u.state == creating {
		return 0
	}
	if len(u.pending) > 0 {
		return u.pending[0].lsn - 1
	}
	return head
}

func (u *Unit) pk(row rules.Row) (string, bool) {
	b, err := u.proj.AppendPK(nil, row)
	return string(b), err == nil
}

// predicateNeedsUnknown: the rule's predicate reads a column whose value
// this change did not send.
func (u *Unit) predicateNeedsUnknown(unknown []bool) bool {
	if unknown == nil {
		return false
	}
	for _, i := range u.a.RefCols[u.a.Table.Name] {
		if unknown[i] {
			return true
		}
	}
	return false
}

// put records row as a member with its current values. Columns marked
// unknown keep their stored values (a member's unchanged TOAST). Returns
// false when it cannot: the row is not a member yet and some projected
// value is unknown (the caller fetches the row instead).
func (u *Unit) put(row rules.Row, unknown []bool) bool {
	key, ok := u.pk(row)
	if !ok {
		return true
	}
	vals, known, err := u.proj.EncodeColumns(row, unknown)
	if err != nil {
		return true
	}
	return u.putCols(key, vals, known, false)
}

// putCols is put with already-encoded column values. absolute forces a full
// PUT instead of a PATCH (see putEncoded).
func (u *Unit) putCols(key string, vals [][]byte, known []bool, absolute bool) bool {
	n := len(u.proj.Cols)
	stored, member := u.members[key]
	if !member {
		for _, k := range known {
			if !k {
				return false
			}
		}
	}
	hashes := make([]uint64, n)
	changed := make([]bool, n)
	nChanged := 0
	var rowHash, oldHash uint64
	for i := range n {
		if member {
			oldHash += stored[i]
		}
		if !known[i] {
			hashes[i] = stored[i]
		} else {
			hashes[i] = wire.ColumnHash(i, vals[i])
			if !member || hashes[i] != stored[i] {
				changed[i] = true
				nChanged++
			}
		}
		rowHash += hashes[i]
	}
	if member && nChanged == 0 {
		return true
	}
	u.members[key] = hashes
	if member {
		u.sum -= wire.MemberHash(oldHash)
	}
	u.sum += wire.MemberHash(rowHash)
	if !member || nChanged == n || absolute {
		u.cur = append(u.cur, opRec{key: key, op: wire.Op{Kind: wire.OpPut, Bytes: wire.BuildRow(n, vals)}})
		return true
	}
	p := &wire.Patch{Hash: rowHash, PK: []byte(key), Changed: changed, Vals: vals}
	u.cur = append(u.cur, opRec{key: key, op: wire.Op{Kind: wire.OpPatch, Bytes: wire.AppendPatch(nil, n, p)}})
	return true
}

// putRow records a query result row (already encoded, with its column
// hashes) as an absolute PUT, never a PATCH: a PATCH is relative to the
// unit's own state, and resolutions are what bring clients that joined
// mid-gap onto that state. PATCH is only emitted from transaction entries,
// where history is continuous. (Found by the smash simulation, seed 321.)
func (u *Unit) putRow(key string, row []byte, hashes []uint64) {
	stored, member := u.members[key]
	same := member
	var rowHash, oldHash uint64
	for i, h := range hashes {
		rowHash += h
		if member {
			oldHash += stored[i]
			if stored[i] != h {
				same = false
			}
		}
	}
	if same {
		return
	}
	u.members[key] = slices.Clone(hashes)
	if member {
		u.sum -= wire.MemberHash(oldHash)
	}
	u.sum += wire.MemberHash(rowHash)
	u.cur = append(u.cur, opRec{key: key, op: wire.Op{Kind: wire.OpPut, Bytes: row}})
}

// queryProjection encodes rows already in projection order.
func queryProjection(p wire.Projection) wire.Projection {
	q := wire.Projection{Cols: p.Cols, PK: p.PK, Src: make([]int, len(p.Cols))}
	for i := range q.Src {
		q.Src[i] = i
	}
	return q
}

func (u *Unit) remove(key string) {
	old, ok := u.members[key]
	if !ok {
		return
	}
	delete(u.members, key)
	u.sum -= wire.MemberHash(rowSum(old))
	u.cur = append(u.cur, opRec{key: key, op: wire.Op{Kind: wire.OpRemove, Bytes: []byte(key)}})
}

// commit closes the current entry at lsn.
func (u *Unit) commit(lsn uint64) {
	if len(u.cur) == 0 {
		return
	}
	u.hist = append(u.hist, entry{lsn: lsn, ops: u.cur, checksum: u.sum, gapFrom: lsn, count: len(u.members)})
	u.histBytes += opBytes(u.cur)
	u.cur = nil
}

func opBytes(ops []opRec) int {
	n := 0
	for _, o := range ops {
		n += len(o.key) + len(o.op.Bytes)
	}
	return n
}

// commitResolution closes the current entry as a jump from gapFrom to lsn.
// It is recorded even when empty, because the gap still constrains frames.
func (u *Unit) commitResolution(gapFrom, lsn uint64) {
	u.hist = append(u.hist, entry{lsn: lsn, ops: u.cur, checksum: u.sum, gapFrom: gapFrom, count: len(u.members)})
	u.histBytes += opBytes(u.cur)
	u.cur = nil
}

// countAt is the number of members at lsn (at or after histFrom, at or
// before what is known).
func (u *Unit) countAt(lsn uint64) int {
	i := sort.Search(len(u.hist), func(i int) bool { return u.hist[i].lsn > lsn })
	if i == 0 {
		return u.histCount
	}
	return u.hist[i-1].count
}

// valid moves x down until it is not inside one of u's resolution gaps.
func (u *Unit) valid(x uint64) uint64 {
	for i := len(u.hist) - 1; i >= 0; i-- {
		e := u.hist[i]
		if e.lsn <= x {
			break
		}
		if e.gapFrom < x {
			x = e.gapFrom
		}
	}
	return x
}

// fetch asks for the row(s) of c by primary key: the change could not be
// applied from what the WAL sent.
func (u *Unit) fetch(c change) {
	u.addRestriction(u.pkRestriction(c))
}

func (u *Unit) pkRestriction(c change) restriction {
	r := restriction{lsn: c.lsn}
	var vals [][]string
	for _, row := range []rules.Row{c.old, c.new} {
		if row == nil {
			continue
		}
		key, ok := u.pk(row)
		if !ok {
			continue
		}
		r.pks = append(r.pks, key)
		pkv := make([]string, len(u.a.Table.PK))
		for i, col := range u.a.Table.PK {
			pkv[i] = string(row[u.a.Table.ColumnIndex(col)])
		}
		vals = append(vals, pkv)
	}
	if len(vals) == 0 {
		return restriction{lsn: c.lsn, full: true}
	}
	r.sql = rules.PKRestriction(u.a.Table, vals)
	return r
}

// fastChange applies an own-table change to a fast (row-local) unit.
func (u *Unit) fastChange(c change) {
	if len(u.a.Gates) > 0 && !u.gateOK {
		return
	}
	if u.predicateNeedsUnknown(c.unknown) {
		u.fetch(c)
		return
	}
	var newKey string
	if c.new != nil {
		k, ok := u.pk(c.new)
		if !ok {
			return
		}
		newKey = k
	}
	if c.old != nil {
		if oldKey, ok := u.pk(c.old); ok && (c.new == nil || oldKey != newKey) {
			u.remove(oldKey)
		}
	}
	if c.new == nil {
		return
	}
	if u.a.Pred == nil || u.a.Pred.Eval(c.new, u.params) == rules.True {
		if !u.put(c.new, c.unknown) {
			u.fetch(c)
		}
	} else {
		u.remove(newKey)
	}
}

func notTrue(e rules.Expr, row rules.Row, params []string) bool {
	return row == nil || e.Eval(row, params) != rules.True
}

// ownChange routes a change of the rule's own table.
func (u *Unit) ownChange(c change) {
	if u.a.Fast {
		if u.state != ready || len(u.pending) > 0 {
			u.buffered = append(u.buffered, c)
			return
		}
		u.fastChange(c)
		return
	}
	if !c.oldKeyOnly && !u.predicateNeedsUnknown(c.unknown) {
		for _, e := range u.a.OwnNarrow {
			if notTrue(e, c.old, u.params) && notTrue(e, c.new, u.params) {
				return
			}
		}
	}
	u.addRestriction(u.pkRestriction(c))
}

// contentChange routes an update that changed none of the columns the
// rule reads (old is a complete row): the row's membership is what it
// was, only its content moved.
func (u *Unit) contentChange(c change) {
	if u.a.Fast || u.state != ready || len(u.pending) > 0 {
		// Fast units evaluate on the row anyway; a pending unit's members
		// are not current, so let the restriction path decide.
		u.ownChange(c)
		return
	}
	oldKey, ok1 := u.pk(c.old)
	newKey, ok2 := u.pk(c.new)
	if !ok1 || !ok2 || oldKey != newKey {
		u.ownChange(c)
		return
	}
	if _, member := u.members[newKey]; member {
		u.put(c.new, c.unknown)
	}
}

// depChange routes a change of a table inside the rule's subqueries.
func (u *Unit) depChange(lsn uint64, o *rules.Occurrence, t *catalog.Table, old, new rules.Row) {
	if o.Narrow != nil && notTrue(o.Narrow, old, u.params) && notTrue(o.Narrow, new, u.params) {
		return
	}
	if o.Full {
		u.addRestriction(restriction{lsn: lsn, full: true})
		return
	}
	col := t.ColumnIndex(o.ValueCol)
	var vals []string
	for _, row := range []rules.Row{old, new} {
		if row == nil {
			continue
		}
		v := row[col]
		if v == nil {
			if o.NullSensitive {
				u.addRestriction(restriction{lsn: lsn, full: true})
				return
			}
			continue
		}
		if len(vals) == 0 || vals[0] != string(v) {
			vals = append(vals, string(v))
		}
	}
	if len(vals) == 0 {
		return
	}
	u.addRestriction(restriction{lsn: lsn, sql: o.Restriction(vals)})
}

func (u *Unit) addRestriction(r restriction) {
	if n := len(u.pending); n > 0 {
		last := &u.pending[n-1]
		if last.lsn == r.lsn && (last.full || (r.sql == last.sql && !r.full)) {
			return
		}
	}
	u.pending = append(u.pending, r)
}

// truncate handles TRUNCATE of the rule's own table.
func (u *Unit) truncate(lsn uint64) {
	if u.state != ready || len(u.pending) > 0 || !u.a.Fast {
		u.addRestriction(restriction{lsn: lsn, full: true})
		return
	}
	for key := range u.members {
		u.remove(key)
	}
}
