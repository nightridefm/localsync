package rules

import (
	"slices"
	"testing"
)

func TestPositional(t *testing.T) {
	cases := []struct {
		in, out string
		names   []string
	}{
		{`SELECT * FROM t WHERE a = $sub`, `SELECT * FROM t WHERE a = $1`, []string{"sub"}},
		{`x = $project AND y = $sub AND z = $project`, `x = $1 AND y = $2 AND z = $1`, []string{"project", "sub"}},
		{`a = '$sub' AND "$col" = $sub -- $x`, `a = '$sub' AND "$col" = $1 -- $x`, []string{"sub"}},
		{`a = 'it''s $x' AND b = $tag$ $y $tag$ AND c = $z`, `a = 'it''s $x' AND b = $tag$ $y $tag$ AND c = $1`, []string{"z"}},
		{`/* $a */ $b`, `/* $a */ $1`, []string{"b"}},
	}
	for _, c := range cases {
		out, names := Positional(c.in)
		if out != c.out || !slices.Equal(names, c.names) {
			t.Errorf("Positional(%q) = %q %v, want %q %v", c.in, out, names, c.out, c.names)
		}
	}
}

func TestGroupGate(t *testing.T) {
	rs, err := Parse([]byte(`
groups:
  project:
    params: [project]
    gate: EXISTS (SELECT 1 FROM perm WHERE user_id = $sub AND project_id = $project)
    rules:
      p/tasks: SELECT * FROM task WHERE project_id = $project
      p/all: SELECT * FROM thing
rules:
  plain: SELECT * FROM tag
`))
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]string{
		"p/tasks": `SELECT * FROM task WHERE (EXISTS (SELECT 1 FROM perm WHERE user_id = $sub AND project_id = $project)) AND (project_id = $project)`,
		"p/all":   `SELECT * FROM thing WHERE (EXISTS (SELECT 1 FROM perm WHERE user_id = $sub AND project_id = $project))`,
		"plain":   `SELECT * FROM tag`,
	}
	for _, r := range rs {
		if want[r.Name] != r.SQL {
			t.Errorf("%s: %q", r.Name, r.SQL)
		}
	}
}
