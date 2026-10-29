package engine

import (
	"testing"

	"github.com/nightridefm/localsync/internal/wire"
)

func TestSnapshotVisibility(t *testing.T) {
	s, err := parseSnapshot("100:110:103,107")
	if err != nil {
		t.Fatal(err)
	}
	for xid, want := range map[uint32]bool{99: true, 100: true, 103: false, 105: true, 107: false, 110: false, 111: false} {
		if got := s.visible(xid); got != want {
			t.Errorf("xid %d visible=%v want %v", xid, got, want)
		}
	}
	// Epoch widening: a 32-bit xid just below the wrap belongs to the
	// previous epoch when the snapshot is in the next one.
	w, _ := parseSnapshot("4294967390:4294967400:")
	if got := w.widen(0xfffffff0); got != 0xfffffff0 {
		t.Errorf("widen across epoch: %d", got)
	}
	if got := w.widen(5); got != 1<<32|5 {
		t.Errorf("widen same epoch: %d", got)
	}
}

func TestBoundary(t *testing.T) {
	e := &Engine{start: 10, ringXid: map[uint32]uint64{}}
	e.ring = []txnRec{{xid: 100, lsn: 20}, {xid: 101, lsn: 30}, {xid: 102, lsn: 40}, {xid: 103, lsn: 50}}
	for _, t := range e.ring {
		e.ringXid[t.xid] = t.lsn
	}
	snap := func(s string) snapshot { x, _ := parseSnapshot(s); return x }

	// 100 and 101 visible, 102 and 103 committed after the snapshot.
	b, ok := e.boundary(&query{snap: snap("102:104:102,103"), lsnS: 60})
	if !ok || b != 30 {
		t.Errorf("regular: b=%d ok=%v", b, ok)
	}
	// 101's commit record precedes 102's, but 101 was still in the
	// ProcArray when the snapshot was taken while 102 was already gone:
	// no LSN boundary matches the snapshot, so it must be retried.
	if _, ok := e.boundary(&query{snap: snap("101:104:101,103"), lsnS: 60}); ok {
		t.Error("irregular snapshot accepted")
	}
	// Nothing visible in the ring: boundary is the stream start.
	b, ok = e.boundary(&query{snap: snap("100:100:"), lsnS: 60})
	if !ok || b != 10 {
		t.Errorf("empty: b=%d ok=%v", b, ok)
	}
	// Transactions at or past lsnS never count, whatever the snapshot says.
	b, _ = e.boundary(&query{snap: snap("200:200:"), lsnS: 35})
	if b != 30 {
		t.Errorf("lsnS cut: b=%d", b)
	}
}

func TestMergedNetOps(t *testing.T) {
	u := &Unit{}
	put := func(key, v string) opRec { return opRec{key: key, op: wire.Op{Kind: wire.OpPut, Bytes: []byte(v)}} }
	rm := func(key string) opRec { return opRec{key: key, op: wire.Op{Kind: wire.OpRemove, Bytes: []byte(key)}} }
	u.hist = []entry{
		{lsn: 10, ops: []opRec{put("a", "a1"), put("b", "b1")}, checksum: 1},
		{lsn: 20, ops: []opRec{put("a", "a2"), rm("b")}, checksum: 2},
		{lsn: 30, ops: []opRec{put("c", "c1")}, checksum: 3},
	}
	ops, sum, n := merged(u, 5, 20)
	if n != 2 || sum != 2 || len(ops) != 2 || string(ops[0].Bytes) != "a2" || ops[1].Kind != wire.OpRemove {
		t.Errorf("merged (5,20]: n=%d sum=%d ops=%v", n, sum, ops)
	}
	ops, sum, n = merged(u, 20, 100)
	if n != 1 || sum != 3 || len(ops) != 1 || string(ops[0].Bytes) != "c1" {
		t.Errorf("merged (20,100]: n=%d sum=%d", n, sum)
	}
	if _, _, n := merged(u, 30, 40); n != 0 {
		t.Errorf("empty range merged %d entries", n)
	}
}

func TestParseLSN(t *testing.T) {
	v, err := parseLSN("16/B374D848")
	if err != nil || v != 0x16<<32|0xB374D848 {
		t.Errorf("lsn %x %v", v, err)
	}
}

// A unit resolved by query from 99 to 105 has no state in (99, 105): a
// frame there would mix pre-100 state of this unit with post-100 state of
// another (the torn pair seen in the capability harness, scenario 6).
func TestValidSkipsResolutionGaps(t *testing.T) {
	u := &Unit{hist: []entry{
		{lsn: 50, gapFrom: 50},
		{lsn: 105, gapFrom: 99},
		{lsn: 120, gapFrom: 110},
	}}
	for x, want := range map[uint64]uint64{200: 200, 120: 120, 115: 110, 110: 110, 106: 106, 105: 105, 104: 99, 100: 99, 99: 99, 60: 60} {
		if got := u.valid(x); got != want {
			t.Errorf("valid(%d) = %d, want %d", x, got, want)
		}
	}
}
