// Package testdb gives tests a connection to the compose Postgres, skipping
// when it is not reachable.
package testdb

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

func URL() string {
	if u := os.Getenv("LOCALSYNC_PG_URL"); u != "" {
		return u
	}
	port := os.Getenv("LOCALSYNC_PG_PORT")
	if port == "" {
		port = "5532"
	}
	return "postgres://postgres:postgres@localhost:" + port + "/localsync?sslmode=disable"
}

func Pool(t testing.TB) *pgxpool.Pool {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	p, err := pgxpool.New(ctx, URL())
	if err == nil {
		err = p.Ping(ctx)
	}
	if err != nil {
		t.Skipf("postgres not reachable (docker compose up -d): %v", err)
	}
	t.Cleanup(p.Close)
	return p
}
