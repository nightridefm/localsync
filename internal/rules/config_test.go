package rules

import (
	"strings"
	"testing"
)

func TestParse(t *testing.T) {
	rs, err := Parse([]byte(`
rules:
  users:
    sql: SELECT id FROM "user"
    public: true
  docs: SELECT * FROM document WHERE owner = $sub
groups:
  project:
    params: [project]
    gate: EXISTS (SELECT 1 FROM grant_ WHERE user_id = $sub AND project_id = $project)
    rules:
      project/tasks: SELECT * FROM task WHERE project_id = $project
`))
	if err != nil {
		t.Fatal(err)
	}
	if len(rs) != 3 || rs[1].Name != "project/tasks" || !strings.Contains(rs[1].SQL, "grant_") || rs[1].ClientParams[0] != "project" {
		t.Errorf("parsed %+v", rs)
	}
	// Unknown keys fail the load, at every level: a typo must never
	// silently change what a rule shows.
	for _, bad := range []string{
		"rules:\n  r:\n    sql: SELECT 1\n    claim: admin\n",
		"rules:\n  r: SELECT 1\nrule:\n  s: SELECT 1\n",
		"groups:\n  g:\n    gates: TRUE\n    rules:\n      r: SELECT 1\n",
	} {
		if _, err := Parse([]byte(bad)); err == nil {
			t.Errorf("accepted:\n%s", bad)
		}
	}
	if rs, err := Parse(nil); err != nil || len(rs) != 0 {
		t.Errorf("empty file: %v %v", rs, err)
	}
}
