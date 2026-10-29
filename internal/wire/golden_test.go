package wire

import (
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"testing"

	"github.com/nightridefm/localsync/internal/catalog"
)

var update = flag.Bool("update", false, "rewrite testdata/rowhash.json")

// The TypeScript client must compute the same row hashes (checksums would
// drift otherwise): ts/transport/src/codec.test.ts reads this golden file.
func TestRowHashGolden(t *testing.T) {
	p := Projection{
		Cols: []Column{
			{Name: "id", Kind: catalog.UUID}, {Name: "n", Kind: catalog.Int}, {Name: "f", Kind: catalog.Float},
			{Name: "b", Kind: catalog.Bool}, {Name: "ts", Kind: catalog.Timestamp}, {Name: "s", Kind: catalog.Text},
			{Name: "j", Kind: catalog.JSON}, {Name: "a", Kind: catalog.Array, Elem: catalog.Int}, {Name: "num", Kind: catalog.Numeric},
		},
		Src: []int{0, 1, 2, 3, 4, 5, 6, 7, 8},
	}
	rows := [][][]byte{
		{[]byte("0d1c2b3a-0000-4000-8000-00000000abcd"), []byte("-9007199254740993"), []byte("1e+300"), []byte("t"),
			[]byte("2026-03-01 12:34:56.789123+00"), []byte("héllo"), []byte(`{"a": [1, null]}`), []byte("{1,2,3}"), []byte("12.3400")},
		{[]byte("ffffffff-ffff-ffff-ffff-ffffffffffff"), nil, nil, []byte("f"), nil, []byte(""), nil, []byte("{}"), nil},
	}
	type golden struct {
		Kinds []string `json:"kinds"`
		Rows  []struct {
			Hex    string `json:"hex"`
			Hash   string `json:"hash"`
			Member string `json:"member"`
		} `json:"rows"`
	}
	var g golden
	for _, c := range p.Cols {
		g.Kinds = append(g.Kinds, c.Kind.String())
	}
	for _, r := range rows {
		b, err := p.AppendRow(nil, r)
		if err != nil {
			t.Fatal(err)
		}
		h, err := RowHash(p.Cols, b)
		if err != nil {
			t.Fatal(err)
		}
		g.Rows = append(g.Rows, struct {
			Hex    string `json:"hex"`
			Hash   string `json:"hash"`
			Member string `json:"member"`
		}{hex.EncodeToString(b), fmt.Sprint(h), fmt.Sprint(MemberHash(h))})
	}
	out, _ := json.MarshalIndent(g, "", "\t")
	if *update {
		if err := os.WriteFile("testdata/rowhash.json", append(out, '\n'), 0o644); err != nil {
			t.Fatal(err)
		}
		return
	}
	want, err := os.ReadFile("testdata/rowhash.json")
	if err != nil || string(want) != string(out)+"\n" {
		t.Fatalf("golden mismatch (go test ./internal/wire -run Golden -update):\n%s", out)
	}
}
