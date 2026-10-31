package rules

import (
	"encoding/json"
	"slices"
	"testing"
	"unicode/utf8"

	pg "github.com/pganalyze/pg_query_go/v6"

	"github.com/nightridefm/localsync/internal/catalog"
)

// Restriction SQL carries row values from the WAL, which anyone who can
// write a row controls. Whatever the values, the SQL must parse to a
// predicate whose only string constants are exactly those values, in order:
// anything else means a value escaped its literal.
func FuzzRestrictionLiterals(f *testing.F) {
	for _, s := range []string{"x", "'", "''", `\`, `\'`, "' OR true --", "$$", "$a$", "*/", "/*", "\n--", "e'\\x27'", " "} {
		f.Add(s, "ok")
	}
	f.Fuzz(func(t *testing.T, a, b string) {
		if !utf8.ValidString(a) || !utf8.ValidString(b) || slices.ContainsFunc([]string{a, b}, hasNUL) {
			return // Postgres text holds neither
		}
		o := Occurrence{Hops: []Hop{{Table: "member", Col: "user_id", Up: "org_id"}, {Col: "org_id"}}}
		tbl := &catalog.Table{Name: "t", PK: []string{"id", "k"}}
		for _, where := range []string{
			o.Restriction([]string{a, b}),
			PKRestriction(tbl, [][]string{{a, b}, {b, a}}),
			PKRestriction(&catalog.Table{Name: "t", PK: []string{"id"}}, [][]string{{a}, {b}}),
		} {
			got := stringConsts(t, "SELECT 1 FROM t r WHERE "+where)
			want := []string{a, b}
			if len(got) == 4 {
				want = []string{a, b, b, a}
			}
			if !slices.Equal(got, want) {
				t.Fatalf("values %q %q: SQL %s has string constants %q", a, b, where, got)
			}
		}
	})
}

func hasNUL(s string) bool {
	for i := range len(s) {
		if s[i] == 0 {
			return true
		}
	}
	return false
}

// stringConsts lists the string constants of sql's parse tree, in order.
func stringConsts(t *testing.T, sql string) []string {
	j, err := pg.ParseToJSON(sql)
	if err != nil {
		t.Fatalf("does not parse: %v\n%s", err, sql)
	}
	var tree any
	if err := json.Unmarshal([]byte(j), &tree); err != nil {
		t.Fatal(err)
	}
	var out []string
	var walk func(v any)
	walk = func(v any) {
		switch x := v.(type) {
		case map[string]any:
			if c, ok := x["A_Const"].(map[string]any); ok {
				if s, ok := c["sval"].(map[string]any); ok {
					sv, _ := s["sval"].(string)
					out = append(out, sv)
					return
				}
			}
			// Deterministic order: the tree's own keys in a fixed order.
			keys := make([]string, 0, len(x))
			for k := range x {
				keys = append(keys, k)
			}
			slices.Sort(keys)
			for _, k := range keys {
				walk(x[k])
			}
		case []any:
			for _, e := range x {
				walk(e)
			}
		}
	}
	walk(tree)
	return out
}
