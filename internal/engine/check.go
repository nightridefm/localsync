package engine

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/nightridefm/localsync/internal/catalog"
	"github.com/nightridefm/localsync/internal/rules"
)

// Check runs a rule the way the engine will: every gate and the body, as a
// whole-unit query and as a restricted one, through the same code (query.run)
// with sample parameter values, plus the shared body and gates when the rule
// is shared. Analysis alone accepts SQL that fails only when run (a gate
// bound with parameters it does not read, a cast of a parameter). Read only;
// bodies are limited to one row. shared reports whether the rule is shared.
func Check(ctx context.Context, pool *pgxpool.Pool, cat *catalog.Catalog, a *rules.Analysis) (shared bool, err error) {
	if err := probe(ctx, pool, a); err != nil {
		return false, err
	}
	e := &Engine{pool: pool}
	sr := e.shareRule(ctx, a, cat)
	if sr == nil {
		return false, nil
	}
	if err := probe(ctx, pool, sr.body); err != nil {
		return true, fmt.Errorf("shared body: %w", err)
	}
	for i, g := range sr.gates {
		if err := probe(ctx, pool, g.a); err != nil {
			return true, fmt.Errorf("shared gate %d: %w", i, err)
		}
	}
	return true, nil
}

func probe(ctx context.Context, pool *pgxpool.Pool, a *rules.Analysis) error {
	params, err := sampleParams(ctx, pool, a)
	if err != nil {
		return err
	}
	u := &Unit{a: a, params: params, members: map[string][]uint64{}}
	u.proj = newProjection(a.Table, a.Columns)
	u.qproj = queryProjection(u.proj)
	for _, q := range []*query{
		{u: u, full: true, probe: true},
		{u: u, probe: true, restr: []restriction{{sql: "false"}}},
	} {
		q.run(ctx, pool, "")
		if q.err != nil {
			kind := "whole-unit query"
			if !q.full {
				kind = "restricted query"
			}
			return fmt.Errorf("%s: %w", kind, q.err)
		}
	}
	return nil
}

// sampleParams picks a value of the right type for each parameter, from
// the types Postgres infers for them.
func sampleParams(ctx context.Context, pool *pgxpool.Pool, a *rules.Analysis) ([]string, error) {
	if len(a.Params) == 0 {
		return nil, nil
	}
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Release()
	sd, err := conn.Conn().Prepare(ctx, "", a.SQL)
	if err != nil {
		return nil, fmt.Errorf("prepare: %w", err)
	}
	vals := make([]string, len(a.Params))
	for i, oid := range sd.ParamOIDs {
		var typ string
		if err := conn.QueryRow(ctx, "SELECT format_type($1, NULL)", oid).Scan(&typ); err != nil {
			return nil, err
		}
		v, ok := sample(typ)
		if !ok {
			return nil, fmt.Errorf("no sample value for $%s of type %s", a.Params[i], typ)
		}
		vals[i] = v
	}
	return vals, nil
}

func sample(typ string) (string, bool) {
	switch {
	case typ == "uuid":
		return "00000000-0000-0000-0000-000000000000", true
	case typ == "smallint" || typ == "integer" || typ == "bigint" || typ == "oid" ||
		typ == "real" || typ == "double precision" || strings.HasPrefix(typ, "numeric"):
		return "0", true
	case typ == "boolean":
		return "false", true
	case typ == "text" || typ == "unknown" || typ == "name" || strings.HasPrefix(typ, "character"):
		return "localsync-plan", true
	case typ == "json" || typ == "jsonb":
		return "null", true
	case strings.HasPrefix(typ, "timestamp") || typ == "date":
		return "2000-01-01", true
	}
	return "", false
}
