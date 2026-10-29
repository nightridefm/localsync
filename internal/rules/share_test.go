package rules

import (
	"strings"
	"testing"
)

func TestShare(t *testing.T) {
	c := fixtureCatalog(t)
	gate := `EXISTS (SELECT 1 FROM project p JOIN member m ON m.org_id = p.org_id WHERE p.id = $project AND m.user_id = $sub)`
	simpleGate := `EXISTS (SELECT 1 FROM member m WHERE m.user_id = $sub AND m.role = 'admin')`
	for _, tc := range []struct {
		sql    string
		shared bool
		gates  int
	}{
		// The common shape: project scope plus a permission gate.
		{`SELECT * FROM task WHERE project_id = $project AND ` + simpleGate, true, 1},
		{`SELECT * FROM task WHERE project_id = $project AND ` + simpleGate + ` AND NOT EXISTS (SELECT 1 FROM project q WHERE q.id = $project AND q.archived)`, true, 2},
		// A gate whose subquery joins: proposed, the engine's analysis of
		// the derived rule then refuses it (FROM must name one table).
		{`SELECT * FROM task WHERE project_id = $project AND ` + gate, false, 0},
		// Rows that depend on the principal: never shared.
		{`SELECT * FROM task WHERE project_id IN (` + visibleProjects + `)`, false, 0},
		{`SELECT * FROM member WHERE user_id = $sub`, false, 0},
		// No principal at all: nothing to share across.
		{`SELECT * FROM tag`, false, 0},
	} {
		var params []string
		if strings.Contains(tc.sql, "$project") {
			params = []string{"project"}
		}
		a := analyze(t, c, tc.sql, params...)
		if (a.Share != nil) != tc.shared {
			t.Errorf("%s: shared=%v", tc.sql, a.Share != nil)
			continue
		}
		if a.Share == nil {
			continue
		}
		if len(a.Share.Gates) != tc.gates {
			t.Errorf("%s: %d gates", tc.sql, len(a.Share.Gates))
		}
		if strings.Contains(a.Share.Body.SQL, "$sub") {
			t.Errorf("body uses $sub: %s", a.Share.Body.SQL)
		}
		// The derived rules analyse like any rule.
		if _, err := Analyze(a.Share.Body, c); err != nil {
			t.Errorf("body %s: %v", a.Share.Body.SQL, err)
		}
		for _, g := range a.Share.Gates {
			if _, err := Analyze(g.Rule, c); err != nil {
				t.Errorf("gate %s: %v", g.Rule.SQL, err)
			}
		}
	}
}

func TestNamedInvertsPositional(t *testing.T) {
	for _, sql := range []string{
		`SELECT * FROM t WHERE a = $project AND b = $sub AND c = '$1' AND d = $project`,
		`SELECT 1 /* $2 */ WHERE x = $sub -- $3`,
	} {
		pos, names := Positional(sql)
		back, ok := Named(pos, names)
		if !ok || back != sql {
			t.Errorf("%q -> %q -> %q", sql, pos, back)
		}
	}
	if _, ok := Named(`SELECT $3`, []string{"a"}); ok {
		t.Error("out of range $3 accepted")
	}
}
