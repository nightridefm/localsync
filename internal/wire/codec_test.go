package wire

import (
	"math"
	"testing"

	"github.com/nightridefm/localsync/internal/catalog"
)

func TestParseTimestamp(t *testing.T) {
	cases := map[string]int64{
		"1970-01-01 00:00:00":           0,
		"1970-01-01 00:00:00.5+00":      500_000,
		"2026-03-01 12:34:56.789123+00": 1772368496789123,
		"1999-12-31 23:59:59.999+02":    946677599999000,
		"1970-01-01 01:00:00+01:00":     0,
		"infinity":                      math.MaxInt64,
	}
	for in, want := range cases {
		got, err := ParseTimestamp([]byte(in))
		if err != nil || got != want {
			t.Errorf("%s: %d %v, want %d", in, got, err, want)
		}
	}
}

func TestArrayJSON(t *testing.T) {
	cases := []struct {
		in   string
		elem catalog.Kind
		want string
	}{
		{`{a,"b c",1,"NULL",NULL,"q\"uote","comma,inside"}`, catalog.Text, `["a","b c","1","NULL",null,"q\"uote","comma,inside"]`},
		{`{1,2,3}`, catalog.Int, `[1,2,3]`},
		{`{}`, catalog.Int, `[]`},
		{`{t,f}`, catalog.Bool, `[true,false]`},
	}
	for _, c := range cases {
		got, ok := ArrayJSON(c.in, c.elem)
		if !ok || got != c.want {
			t.Errorf("%s: %s %v want %s", c.in, got, ok, c.want)
		}
	}
	if _, ok := ArrayJSON(`{{1,2},{3,4}}`, catalog.Int); ok {
		t.Error("multidimensional arrays are not converted")
	}
}

func TestRowRoundTripShape(t *testing.T) {
	p := Projection{
		Cols: []Column{{Name: "id", Kind: catalog.UUID}, {Name: "n", Kind: catalog.Int}, {Name: "s", Kind: catalog.Text}},
		Src:  []int{0, 1, 2},
		PK:   []int{0},
	}
	row := [][]byte{[]byte("0d1c2b3a-0000-4000-8000-00000000abcd"), nil, []byte("hi")}
	b, err := p.AppendRow(nil, row)
	if err != nil {
		t.Fatal(err)
	}
	// bitmap (n NULL) + 16 uuid bytes + len + "hi"
	if b[0] != 0b010 || len(b) != 1+16+1+2 {
		t.Errorf("row bytes %x", b)
	}
	if Hash(b) == Hash(append([]byte{}, b[:len(b)-1]...)) {
		t.Error("hash ignores bytes")
	}
}
