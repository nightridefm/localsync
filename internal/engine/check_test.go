package engine

import (
	"context"
	"strings"
	"testing"

	"github.com/nightridefm/localsync/internal/catalog"
	"github.com/nightridefm/localsync/internal/rules"
	"github.com/nightridefm/localsync/internal/testdb"
)

func TestCheck(t *testing.T) {
	ctx := context.Background()
	pool := testdb.Pool(t)
	if _, err := pool.Exec(ctx, `
		DROP TABLE IF EXISTS check_item, check_user;
		CREATE TABLE check_user (id uuid PRIMARY KEY, active bool NOT NULL);
		CREATE TABLE check_item (id uuid PRIMARY KEY, owner uuid NOT NULL, project uuid NOT NULL, n int)`); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = pool.Exec(context.Background(), "DROP TABLE IF EXISTS check_item, check_user") })
	cat, err := catalog.Load(ctx, pool)
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range []struct {
		name, sql string
		shared    bool
		fails     string
	}{
		// $sub in the rows and a client parameter, so not shared, with a
		// gate reading $sub alone: v0.1.0 bound every parameter to the gate
		// and pgx refused ("unused argument").
		{name: "sub rows, gate on a subset", sql: `SELECT * FROM check_item WHERE owner = $sub AND project = $project AND EXISTS (SELECT 1 FROM check_user WHERE id = $sub AND active)`},
		{name: "gate on the client parameter alone", sql: `SELECT * FROM check_item WHERE owner = $sub AND EXISTS (SELECT 1 FROM check_item WHERE project = $project)`},
		{name: "shared body", sql: `SELECT * FROM check_item WHERE project = $project AND EXISTS (SELECT 1 FROM check_user WHERE id = $sub AND active)`, shared: true},
		// Accepted by analysis, fails only when run.
		{name: "run-time failure", sql: `SELECT * FROM check_item WHERE owner = $sub AND project = $project AND EXISTS (SELECT 1 FROM check_user WHERE 1 / (SELECT count(*)::int * 0 FROM check_user) = 1)`, fails: "division by zero"},
	} {
		t.Run(c.name, func(t *testing.T) {
			a, err := rules.Analyze(rules.Rule{Name: "r", SQL: c.sql, ClientParams: []string{"project"}}, cat)
			if err != nil {
				t.Fatal(err)
			}
			shared, err := Check(ctx, pool, cat, a)
			if c.fails != "" {
				if err == nil || !strings.Contains(err.Error(), c.fails) {
					t.Fatalf("want an error with %q, got %v", c.fails, err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if shared != c.shared {
				t.Errorf("shared = %v, want %v", shared, c.shared)
			}
		})
	}
}
