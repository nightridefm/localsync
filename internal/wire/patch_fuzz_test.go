package wire

import (
	"bytes"
	"slices"
	"testing"

	"github.com/nightridefm/localsync/internal/catalog"
)

var fuzzCols = []Column{{Kind: catalog.UUID}, {Kind: catalog.Text}, {Kind: catalog.Int}, {Kind: catalog.Float}, {Kind: catalog.Bool}, {Kind: catalog.Array, Elem: catalog.Text}}

// Decoding arbitrary bytes as a patch or a row never panics, and a patch
// that decodes survives encoding and decoding again unchanged.
func FuzzDecodePatch(f *testing.F) {
	f.Add(AppendPatch(nil, len(fuzzCols), &Patch{Hash: 1, PK: []byte("k"), Changed: []bool{true, true, false, false, true, false}, Vals: [][]byte{bytes.Repeat([]byte{7}, 16), {3, 'a', 'b', 'c'}, nil, nil, {1}, nil}}))
	f.Add([]byte{})
	f.Add(bytes.Repeat([]byte{0xff}, 40))
	f.Fuzz(func(t *testing.T, b []byte) {
		_, _ = SplitRow(fuzzCols, b)
		p, err := DecodePatch(fuzzCols, b)
		if err != nil {
			return
		}
		again, err := DecodePatch(fuzzCols, AppendPatch(nil, len(fuzzCols), p))
		if err != nil {
			t.Fatalf("%x: re-encoded patch does not decode: %v", b, err)
		}
		if again.Hash != p.Hash || !bytes.Equal(again.PK, p.PK) || !slices.Equal(again.Changed, p.Changed) ||
			!slices.EqualFunc(again.Vals, p.Vals, func(x, y []byte) bool { return bytes.Equal(x, y) && (x == nil) == (y == nil) }) {
			t.Fatalf("%x: %+v re-decodes as %+v", b, p, again)
		}
	})
}
