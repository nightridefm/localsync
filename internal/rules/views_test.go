package rules

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"

	"github.com/nightridefm/localsync/internal/catalog"
	"github.com/nightridefm/localsync/internal/testdb"
)

func TestViews(t *testing.T) {
	ctx := context.Background()
	pool := testdb.Pool(t)
	if _, err := pool.Exec(ctx, `
		CREATE OR REPLACE VIEW view_admin AS SELECT * FROM member WHERE role = 'admin';
		CREATE OR REPLACE VIEW view_admin_orgs AS SELECT org_id, user_id FROM view_admin;
		CREATE OR REPLACE VIEW view_barrier WITH (security_barrier) AS SELECT * FROM member`); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), "DROP VIEW IF EXISTS view_admin_orgs, view_admin, view_barrier")
	})
	c, err := catalog.Load(ctx, pool)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := c.Table("task"); !ok {
		t.Skip("harness fixture not created yet (run the harness once)")
	}

	// A view of a view inside a filter: analysis sees only tables, and the
	// SQL the engine runs is the expanded one.
	a := analyze(t, c, `SELECT * FROM project WHERE org_id IN (SELECT org_id FROM view_admin_orgs WHERE user_id = $sub)`)
	if !slices.Equal(a.Deps, []string{"member", "project"}) {
		t.Errorf("deps %v", a.Deps)
	}
	if strings.Contains(a.SQL, "FROM view_") || !strings.Contains(a.SQL, "'admin'") {
		t.Errorf("not expanded: %s", a.SQL)
	}
	if _, err := pool.Exec(ctx, "PREPARE view_check AS "+a.SQL+"; DEALLOCATE view_check"); err != nil {
		t.Errorf("expanded SQL does not run: %v\n%s", err, a.SQL)
	}

	for _, tc := range []struct{ sql, why string }{
		{`SELECT * FROM view_admin WHERE user_id = $sub`, "is a view"},
		{`SELECT * FROM project WHERE org_id IN (SELECT org_id FROM view_barrier WHERE user_id = $sub)`, "security_barrier"},
		{`SELECT * FROM project WHERE EXISTS (SELECT 1 FROM pg_catalog.pg_class) AND org_id IN (SELECT org_id FROM member WHERE user_id = $sub)`, "only tables in public"},
		{`SELECT * FROM project WHERE org_id IN (SELECT org_id FROM no_such_relation WHERE user_id = $sub)`, "unknown table"},
	} {
		_, err := Analyze(Rule{Name: "r", SQL: tc.sql}, c)
		if !errors.Is(err, ErrRejected) || !strings.Contains(err.Error(), tc.why) {
			t.Errorf("%s: want %q, got %v", tc.sql, tc.why, err)
		}
	}
}
