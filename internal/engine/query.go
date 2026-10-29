package engine

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/nightridefm/localsync/internal/rules"
)

// query is one spliced evaluation of a unit: the whole unit (full) or the
// rows selected by a set of restrictions.
type query struct {
	u    *Unit
	full bool
	// forHolds: a snapshot for waiting holds only; the unit itself is not
	// changed by the result.
	forHolds bool
	// verify (debug): a whole-unit evaluation compared against the unit's
	// own state, never applied (LOCALSYNC_VERIFY).
	verify bool
	restr  []restriction
	maxLSN uint64 // largest LSN among restrictions included

	// results
	snap   snapshot
	lsnS   uint64
	res    []resultRow
	gateOK bool
	err    error
}

// resultRow is one query result row, already encoded.
type resultRow struct {
	key    string // encoded PK
	row    []byte // encoded row (projection order)
	hashes []uint64
	member bool // restricted queries: whether the row is in the unit
}

func rowSum(hashes []uint64) uint64 {
	var h uint64
	for _, x := range hashes {
		h += x
	}
	return h
}

type snapshot struct {
	xmin, xmax uint64
	xip        map[uint64]bool
}

func parseSnapshot(s string) (snapshot, error) {
	parts := strings.Split(s, ":")
	if len(parts) != 3 {
		return snapshot{}, fmt.Errorf("bad snapshot %q", s)
	}
	xmin, err1 := strconv.ParseUint(parts[0], 10, 64)
	xmax, err2 := strconv.ParseUint(parts[1], 10, 64)
	if err1 != nil || err2 != nil {
		return snapshot{}, fmt.Errorf("bad snapshot %q", s)
	}
	sn := snapshot{xmin: xmin, xmax: xmax, xip: map[uint64]bool{}}
	if parts[2] != "" {
		for _, x := range strings.Split(parts[2], ",") {
			v, err := strconv.ParseUint(x, 10, 64)
			if err != nil {
				return snapshot{}, err
			}
			sn.xip[v] = true
		}
	}
	return sn, nil
}

// widen turns a 32-bit stream xid into an xid8 relative to the snapshot.
func (s snapshot) widen(xid uint32) uint64 {
	full := s.xmax&^0xffffffff | uint64(xid)
	if full > s.xmax {
		full -= 1 << 32
	}
	return full
}

func (s snapshot) visible(xid uint32) bool {
	x := s.widen(xid)
	return x < s.xmax && !s.xip[x]
}

func parseLSN(s string) (uint64, error) {
	i := strings.IndexByte(s, '/')
	if i < 0 {
		return 0, fmt.Errorf("bad lsn %q", s)
	}
	hi, err1 := strconv.ParseUint(s[:i], 16, 32)
	lo, err2 := strconv.ParseUint(s[i+1:], 16, 32)
	if err1 != nil || err2 != nil {
		return 0, fmt.Errorf("bad lsn %q", s)
	}
	return hi<<32 | lo, nil
}

// columnList selects the unit's projected columns from alias.
func columnList(u *Unit, alias string) string {
	cols := make([]string, len(u.proj.Cols))
	for i, c := range u.proj.Cols {
		cols[i] = alias + "." + rules.Ident(c.Name)
	}
	return strings.Join(cols, ", ")
}

func (q *query) sql() string {
	u := q.u
	if q.full {
		return "SELECT " + columnList(u, "x") + " FROM (" + u.a.SQL + ") x"
	}
	var join []string
	for _, pk := range u.a.Table.PK {
		join = append(join, "x."+rules.Ident(pk)+" = r."+rules.Ident(pk))
	}
	var where []string
	for _, r := range q.restr {
		where = append(where, "("+r.sql+")")
	}
	return "SELECT " + columnList(u, "r") + ", EXISTS (SELECT 1 FROM (" + u.a.SQL + ") x WHERE " +
		strings.Join(join, " AND ") + ") FROM " + rules.Ident(u.a.Table.Name) + " r WHERE " + strings.Join(where, " OR ")
}

func paramArgs(params []string) []any {
	args := make([]any, len(params))
	for i, p := range params {
		args[i] = p
	}
	return args
}

// run executes the query in its own REPEATABLE READ snapshot. Simple
// protocol: parameters are sent as literals Postgres types from context,
// and results come back as text, the same form pgoutput uses.
func (q *query) run(ctx context.Context, pool *pgxpool.Pool, snapID string) {
	q.err = pgx.BeginTxFunc(ctx, pool, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly}, func(tx pgx.Tx) error {
		var err error
		if snapID != "" {
			// Part of a round: the leader already captured S and lsnS.
			if _, err := tx.Exec(ctx, "SET TRANSACTION SNAPSHOT "+rules.Literal(snapID), pgx.QueryExecModeSimpleProtocol); err != nil {
				return err
			}
		} else {
			var snap, lsn string
			if err := tx.QueryRow(ctx, "SELECT pg_current_snapshot()::text, pg_current_wal_insert_lsn()::text", pgx.QueryExecModeSimpleProtocol).Scan(&snap, &lsn); err != nil {
				return err
			}
			if q.snap, err = parseSnapshot(snap); err != nil {
				return err
			}
			if q.lsnS, err = parseLSN(lsn); err != nil {
				return err
			}
		}
		args := append([]any{pgx.QueryExecModeSimpleProtocol}, paramArgs(q.u.params)...)
		q.gateOK = true
		for _, g := range q.u.a.Gates {
			rows, err := tx.Query(ctx, g, args...)
			if err != nil {
				return err
			}
			has := rows.Next()
			rows.Close()
			if err := rows.Err(); err != nil {
				return err
			}
			if !has {
				q.gateOK = false
			}
		}
		if !q.gateOK && q.full {
			return nil
		}
		rows, err := tx.Query(ctx, q.sql(), args...)
		if err != nil {
			return err
		}
		defer rows.Close()
		// Rows are encoded straight from the driver's buffer in one pass
		// (encoded row, per-column hashes, PK); unit state, snapshots and
		// frames then share these bytes. Column hashes go into one slab.
		n := len(q.u.proj.Cols)
		enc := &q.u.qproj
		var slab []uint64
		for rows.Next() {
			raw := rows.RawValues()
			if len(slab) < n {
				slab = make([]uint64, n*256)
			}
			hashes := slab[:n:n]
			slab = slab[n:]
			row, err := enc.AppendRowHashed(nil, raw[:n], hashes)
			if err != nil {
				continue
			}
			key, err := enc.AppendPK(nil, raw[:n])
			if err != nil {
				continue
			}
			r := resultRow{key: string(key), row: row, hashes: hashes, member: true}
			if !q.full {
				r.member = len(raw[n]) == 1 && raw[n][0] == 't'
			}
			q.res = append(q.res, r)
		}
		return rows.Err()
	})
}
