package wire

import (
	"bytes"
	"testing"

	"github.com/nightridefm/localsync/internal/catalog"
)

func TestPatchRoundTrip(t *testing.T) {
	p := Projection{
		Cols: []Column{{Name: "id", Kind: catalog.UUID}, {Name: "n", Kind: catalog.Int}, {Name: "body", Kind: catalog.Text}, {Name: "f", Kind: catalog.Float}},
		Src:  []int{0, 1, 2, 3},
		PK:   []int{0},
	}
	old := [][]byte{[]byte("0d1c2b3a-0000-4000-8000-00000000abcd"), []byte("1"), []byte("big text"), []byte("1.5")}
	oldRow, _ := p.AppendRow(nil, old)
	// Update n; body arrives as unchanged TOAST (unknown).
	newVals, known, err := p.EncodeColumns([][]byte{old[0], []byte("2"), nil, old[3]}, []bool{false, false, true, false})
	if err != nil || known[2] {
		t.Fatalf("encode: %v %v", err, known)
	}
	oldVals, _ := SplitRow(p.Cols, oldRow)
	var hash uint64
	patch := &Patch{Changed: make([]bool, 4), Vals: make([][]byte, 4)}
	for i := range p.Cols {
		v := oldVals[i]
		if known[i] && !bytes.Equal(newVals[i], oldVals[i]) {
			v = newVals[i]
			patch.Changed[i], patch.Vals[i] = true, newVals[i]
		}
		hash += ColumnHash(i, v)
	}
	patch.Hash = hash
	patch.PK, _ = p.AppendPK(nil, old)
	enc := AppendPatch(nil, 4, patch)
	got, err := DecodePatch(p.Cols, enc)
	if err != nil || !got.Changed[1] || got.Changed[2] || got.Changed[0] {
		t.Fatalf("decode: %v %+v", err, got)
	}
	merged, err := ApplyPatch(p.Cols, oldRow, got)
	if err != nil {
		t.Fatal(err)
	}
	want, _ := p.AppendRow(nil, [][]byte{old[0], []byte("2"), old[2], old[3]})
	if !bytes.Equal(merged, want) {
		t.Errorf("applied row %x, want %x", merged, want)
	}
	if h, _ := RowHash(p.Cols, merged); h != got.Hash {
		t.Errorf("hash of patched row %d != patch hash %d", h, got.Hash)
	}
	// A NULL set by a patch, then merged with a later patch.
	second := &Patch{Hash: 7, PK: patch.PK, Changed: []bool{false, false, false, true}, Vals: make([][]byte, 4)}
	m := MergePatches(got, second)
	if !m.Changed[1] || !m.Changed[3] || m.Vals[3] != nil || m.Hash != 7 {
		t.Errorf("merge: %+v", m)
	}
}

func TestAppendRowHashed(t *testing.T) {
	p := Projection{Cols: []Column{{Kind: catalog.UUID}, {Kind: catalog.Int}, {Kind: catalog.Text}}, Src: []int{0, 1, 2}}
	row := [][]byte{[]byte("0d1c2b3a-0000-4000-8000-00000000abcd"), nil, []byte("x")}
	hashes := make([]uint64, 3)
	b, err := p.AppendRowHashed(nil, row, hashes)
	if err != nil {
		t.Fatal(err)
	}
	want, _ := p.AppendRow(nil, row)
	if !bytes.Equal(b, want) {
		t.Fatalf("row bytes differ")
	}
	if h, _ := RowHash(p.Cols, b); h != hashes[0]+hashes[1]+hashes[2] {
		t.Errorf("hashes do not sum to RowHash")
	}
}
