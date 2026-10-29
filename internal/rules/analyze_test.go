package rules

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/nightridefm/localsync/internal/catalog"
	"github.com/nightridefm/localsync/internal/testdb"
)

// The harness fixture (ts/harness/src/fixture.ts) is the schema under test.
func fixtureCatalog(t *testing.T) *catalog.Catalog {
	t.Helper()
	p := testdb.Pool(t)
	c, err := catalog.Load(context.Background(), p)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := c.Table("task"); !ok {
		t.Skip("harness fixture not created yet (run the harness once)")
	}
	return c
}

const visibleProjects = `SELECT id FROM project WHERE org_id IN (SELECT org_id FROM member WHERE user_id = $sub)`

func analyze(t *testing.T, c *catalog.Catalog, sql string, clientParams ...string) *Analysis {
	t.Helper()
	a, err := Analyze(Rule{Name: "r", SQL: sql, ClientParams: clientParams}, c)
	if err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
	return a
}

func occ(a *Analysis, table string) *Occurrence {
	for i := range a.Occurrences {
		if a.Occurrences[i].Table == table {
			return &a.Occurrences[i]
		}
	}
	return nil
}

func TestFastRules(t *testing.T) {
	c := fixtureCatalog(t)
	for _, sql := range []string{
		`SELECT * FROM tag`,
		`SELECT * FROM member WHERE user_id = $sub`,
		`SELECT * FROM kitchen_sink WHERE owner = $sub AND (b IS NULL OR b) AND i4 IN (1, 2)`,
		`SELECT id, title FROM task WHERE done = true AND txn_tag IS NOT NULL`,
	} {
		a := analyze(t, c, sql)
		if !a.Fast || a.Tier != 1 {
			t.Errorf("%s: fast=%v tier=%d notes=%v", sql, a.Fast, a.Tier, a.Notes)
		}
	}
}

func TestEvalThreeValued(t *testing.T) {
	c := fixtureCatalog(t)
	a := analyze(t, c, `SELECT * FROM kitchen_sink WHERE owner = $sub AND (b IS NULL OR b)`)
	tbl := a.Table
	row := make(Row, len(tbl.Columns))
	set := func(col, v string) { row[tbl.ColumnIndex(col)] = []byte(v) }
	set("owner", "alice")
	if got := a.Pred.Eval(row, []string{"alice"}); got != True {
		t.Errorf("b NULL: %v", got)
	}
	set("b", "f")
	if got := a.Pred.Eval(row, []string{"alice"}); got != False {
		t.Errorf("b false: %v", got)
	}
	set("b", "t")
	if got := a.Pred.Eval(row, []string{"bob"}); got != False {
		t.Errorf("other owner: %v", got)
	}
}

func TestChainRestrictions(t *testing.T) {
	c := fixtureCatalog(t)
	a := analyze(t, c, `SELECT * FROM task WHERE project_id IN (`+visibleProjects+`)`)
	if a.Fast || a.Tier != 2 {
		t.Fatalf("fast=%v tier=%d", a.Fast, a.Tier)
	}
	m := occ(a, "member")
	if m == nil || m.Full || m.ValueCol != "org_id" || m.Narrow == nil {
		t.Fatalf("member occurrence: %+v", m)
	}
	if got, want := m.Restriction([]string{"o1"}), `r."project_id" IN (SELECT "id" FROM "project" WHERE "org_id" IN ('o1'))`; got != want {
		t.Errorf("member restriction:\n got %s\nwant %s", got, want)
	}
	p := occ(a, "project")
	if got, want := p.Restriction([]string{"p1", "p'2"}), `r."project_id" IN ('p1', 'p''2')`; got != want {
		t.Errorf("project restriction: %s", got)
	}
	// Narrowing: the member level predicate is user_id = $sub.
	mt, _ := c.Table("member")
	row := make(Row, len(mt.Columns))
	row[mt.ColumnIndex("user_id")] = []byte("alice")
	if m.Narrow.Eval(row, []string{"alice"}) != True || m.Narrow.Eval(row, []string{"bob"}) != False {
		t.Error("member narrowing")
	}
}

func TestThreeLevelsAndExists(t *testing.T) {
	c := fixtureCatalog(t)
	a := analyze(t, c, `SELECT * FROM comment WHERE task_id IN (SELECT id FROM task WHERE project_id IN (`+visibleProjects+`))`)
	m := occ(a, "member")
	want := `r."task_id" IN (SELECT "id" FROM "task" WHERE "project_id" IN (SELECT "id" FROM "project" WHERE "org_id" IN ('o1')))`
	if got := m.Restriction([]string{"o1"}); got != want {
		t.Errorf("got  %s\nwant %s", got, want)
	}
	e := analyze(t, c, `SELECT * FROM task t WHERE EXISTS (SELECT 1 FROM project p WHERE p.id = t.project_id AND p.archived = false)`)
	po := occ(e, "project")
	if po == nil || po.Full || po.ValueCol != "id" || po.Narrow == nil {
		t.Fatalf("exists occurrence: %+v", po)
	}
	if got := po.Restriction([]string{"x"}); got != `r."project_id" IN ('x')` {
		t.Errorf("exists restriction %s", got)
	}
}

func TestGateAndNotIn(t *testing.T) {
	c := fixtureCatalog(t)
	a := analyze(t, c, `SELECT * FROM task WHERE project_id = $project AND EXISTS (SELECT 1 FROM member WHERE user_id = $sub AND role = 'admin')`, "project")
	m := occ(a, "member")
	if m == nil || !m.Full || m.Narrow == nil {
		t.Fatalf("gate occurrence %+v", m)
	}
	if !a.Fast || len(a.Gates) != 1 || !strings.HasPrefix(a.Gates[0], "SELECT 1 WHERE EXISTS") {
		t.Errorf("gated fast rule: fast=%v gates=%q", a.Fast, a.Gates)
	}
	n := analyze(t, c, `SELECT * FROM task WHERE id NOT IN (SELECT task_id FROM comment WHERE body = 'hide')`)
	if o := occ(n, "comment"); o == nil || !o.NullSensitive || o.Full {
		t.Errorf("not in occurrence %+v", o)
	}
}

func TestRejected(t *testing.T) {
	c := fixtureCatalog(t)
	for sql, why := range map[string]string{
		`SELECT * FROM task WHERE title = now()::text`:                    "not IMMUTABLE",
		`SELECT * FROM task WHERE done AND current_user = 'x'`:            "changes without writes",
		`SELECT id, upper(title) FROM task`:                               "plain columns",
		`SELECT title FROM task`:                                          "primary key",
		`SELECT * FROM task JOIN project ON project.id = task.project_id`: "exactly one table",
		`SELECT * FROM task WHERE project_id = $project`:                  "not constrained",
	} {
		var cp []string
		if strings.Contains(sql, "$project") {
			cp = []string{"project"}
		}
		_, err := Analyze(Rule{Name: "r", SQL: sql, ClientParams: cp}, c)
		if !errors.Is(err, ErrRejected) || !strings.Contains(err.Error(), why) {
			t.Errorf("%s: %v (want %q)", sql, err, why)
		}
	}
}

func TestRefCols(t *testing.T) {
	c := fixtureCatalog(t)
	a := analyze(t, c, `SELECT * FROM task WHERE project_id IN (`+visibleProjects+`)`)
	names := func(table string) []string {
		tbl, _ := c.Table(table)
		var out []string
		for _, i := range a.RefCols[table] {
			out = append(out, tbl.Columns[i].Name)
		}
		return out
	}
	// title, done, txn_tag on task, name/archived on project and role on
	// member can never change membership, so updates to them are pruned.
	// Matching is by name, so member.id is included because project.id is
	// referenced: an over-approximation, which is always safe.
	for table, want := range map[string]string{"task": "id,project_id", "project": "id,org_id", "member": "id,org_id,user_id"} {
		if got := strings.Join(names(table), ","); got != want {
			t.Errorf("%s: %s, want %s", table, got, want)
		}
	}
}
