package wire

import (
	"encoding/json"
	"testing"

	"github.com/nightridefm/localsync/internal/catalog"
)

// Arbitrary Postgres text must never panic the encoder, and converted
// arrays must always be valid JSON.
func FuzzArrayJSON(f *testing.F) {
	for _, s := range []string{`{}`, `{1,2}`, `{"a\"b",NULL,"NULL"}`, `{{1},{2}}`, `{"`, `{a,}`} {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, s string) {
		for _, k := range []catalog.Kind{catalog.Text, catalog.Int, catalog.Bool} {
			if j, ok := ArrayJSON(s, k); ok && !json.Valid([]byte(j)) {
				t.Fatalf("%q -> invalid JSON %q", s, j)
			}
		}
	})
}

func FuzzValues(f *testing.F) {
	for _, s := range []string{"2026-01-01 00:00:00+00", "1e+300", "-1", "0d1c2b3a-0000-4000-8000-00000000abcd", "{1,2}"} {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, s string) {
		for k := catalog.Text; k <= catalog.Other; k++ {
			_, _ = AppendValue(nil, Column{Kind: k, Elem: catalog.Text}, []byte(s))
		}
	})
}
