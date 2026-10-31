// Package repl streams committed transactions from a temporary logical
// replication slot (pgoutput, protocol 1, text format). The slot is
// TEMPORARY on purpose: engine state is a cache (docs/design.md section 2),
// so when the engine's connection ends Postgres drops the slot and no WAL
// is pinned for a dead engine.
package repl

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
)

type Relation struct {
	ID        uint32
	Namespace string
	Name      string
	Columns   []string
}

type Change struct {
	Rel  *Relation
	Kind byte // 'I', 'U', 'D'
	// Old and New in the relation's column order; a nil value is NULL.
	// Old is only present with REPLICA IDENTITY FULL (or a key change).
	Old, New [][]byte
	// Unchanged marks New values that are unchanged TOAST (not sent).
	Unchanged []bool
	// OldKeyOnly: Old holds only the replica identity key columns (the
	// table is not REPLICA IDENTITY FULL); its other values are absent.
	OldKeyOnly bool
}

type Txn struct {
	Xid       uint32
	Commit    uint64    // commit LSN
	End       uint64    // end of the commit record
	Time      time.Time // commit time (Postgres's clock)
	Changes   []Change
	Truncates []*Relation
}

type Stream struct {
	conn *pgconn.PgConn
	// Txns delivers committed transactions in commit order. It is closed
	// when the stream fails; Err then reports why.
	Txns  chan *Txn
	err   error
	Start uint64
}

func (s *Stream) Err() error { return s.err }

// Open connects with replication=database, creates the temporary slot and
// starts streaming the publication.
func Open(ctx context.Context, connString, slot, publication string) (*Stream, error) {
	cfg, err := pgconn.ParseConfig(connString)
	if err != nil {
		return nil, err
	}
	cfg.RuntimeParams["replication"] = "database"
	cfg.RuntimeParams["TimeZone"] = "UTC"
	cfg.RuntimeParams["DateStyle"] = "ISO, YMD"
	conn, err := pgconn.ConnectConfig(ctx, cfg)
	if err != nil {
		return nil, err
	}
	res, err := pglogrepl.CreateReplicationSlot(ctx, conn, slot, "pgoutput",
		pglogrepl.CreateReplicationSlotOptions{Temporary: true, Mode: pglogrepl.LogicalReplication, SnapshotAction: "NOEXPORT_SNAPSHOT"})
	if err != nil {
		_ = conn.Close(ctx)
		return nil, fmt.Errorf("create slot: %w", err)
	}
	start, err := pglogrepl.ParseLSN(res.ConsistentPoint)
	if err != nil {
		_ = conn.Close(ctx)
		return nil, err
	}
	err = pglogrepl.StartReplication(ctx, conn, slot, start, pglogrepl.StartReplicationOptions{
		PluginArgs: []string{"proto_version '1'", fmt.Sprintf("publication_names '%s'", publication)},
	})
	if err != nil {
		_ = conn.Close(ctx)
		return nil, fmt.Errorf("start replication: %w", err)
	}
	s := &Stream{conn: conn, Txns: make(chan *Txn, 64), Start: uint64(start)}
	// #nosec G118 -- run closes the connection with context.Background on purpose: ctx may be done by then.
	go s.run(ctx)
	return s, nil
}

func (s *Stream) run(ctx context.Context) {
	defer close(s.Txns)
	defer s.conn.Close(context.Background())
	rels := map[uint32]*Relation{}
	var cur *Txn
	var received pglogrepl.LSN
	nextStatus := time.Now()
	for {
		if time.Now().After(nextStatus) {
			// Engine state is a cache: nothing to replay after a restart,
			// so confirm everything received.
			if err := pglogrepl.SendStandbyStatusUpdate(ctx, s.conn, pglogrepl.StandbyStatusUpdate{WALWritePosition: received}); err != nil {
				s.err = err
				return
			}
			nextStatus = time.Now().Add(5 * time.Second)
		}
		rctx, cancel := context.WithDeadline(ctx, nextStatus)
		raw, err := s.conn.ReceiveMessage(rctx)
		cancel()
		if err != nil {
			if pgconn.Timeout(err) && ctx.Err() == nil {
				continue
			}
			s.err = err
			return
		}
		cd, ok := raw.(*pgproto3.CopyData)
		if !ok {
			if e, ok := raw.(*pgproto3.ErrorResponse); ok {
				s.err = pgconn.ErrorResponseToPgError(e)
				return
			}
			continue
		}
		switch cd.Data[0] {
		case pglogrepl.PrimaryKeepaliveMessageByteID:
			ka, err := pglogrepl.ParsePrimaryKeepaliveMessage(cd.Data[1:])
			if err != nil {
				s.err = err
				return
			}
			if ka.ServerWALEnd > received && cur == nil {
				received = ka.ServerWALEnd
			}
			if ka.ReplyRequested {
				nextStatus = time.Time{}
			}
		case pglogrepl.XLogDataByteID:
			x, err := pglogrepl.ParseXLogData(cd.Data[1:])
			if err != nil {
				s.err = err
				return
			}
			msg, err := pglogrepl.Parse(x.WALData)
			if err != nil {
				s.err = err
				return
			}
			switch m := msg.(type) {
			case *pglogrepl.RelationMessage:
				r := &Relation{ID: m.RelationID, Namespace: m.Namespace, Name: m.RelationName}
				for _, c := range m.Columns {
					r.Columns = append(r.Columns, c.Name)
				}
				rels[m.RelationID] = r
			case *pglogrepl.BeginMessage:
				cur = &Txn{Xid: m.Xid, Commit: uint64(m.FinalLSN)}
			case *pglogrepl.InsertMessage:
				new, _ := tuple(m.Tuple)
				cur.Changes = append(cur.Changes, Change{Rel: rels[m.RelationID], Kind: 'I', New: new})
			case *pglogrepl.UpdateMessage:
				new, unchanged := tuple(m.NewTuple)
				var old [][]byte
				if m.OldTuple != nil {
					old, _ = tuple(m.OldTuple)
				}
				cur.Changes = append(cur.Changes, Change{Rel: rels[m.RelationID], Kind: 'U', Old: old, New: new, Unchanged: unchanged, OldKeyOnly: m.OldTupleType == 'K'})
			case *pglogrepl.DeleteMessage:
				old, _ := tuple(m.OldTuple)
				cur.Changes = append(cur.Changes, Change{Rel: rels[m.RelationID], Kind: 'D', Old: old, OldKeyOnly: m.OldTupleType == 'K'})
			case *pglogrepl.TruncateMessage:
				for _, id := range m.RelationIDs {
					cur.Truncates = append(cur.Truncates, rels[id])
				}
			case *pglogrepl.CommitMessage:
				if cur == nil {
					continue
				}
				received = m.TransactionEndLSN
				cur.End = uint64(m.TransactionEndLSN)
				cur.Time = m.CommitTime
				select {
				case s.Txns <- cur:
				case <-ctx.Done():
					return
				}
				cur = nil
			case *pglogrepl.TypeMessage, *pglogrepl.OriginMessage:
			default:
				slog.Debug("repl: ignoring message", "type", fmt.Sprintf("%T", m))
			}
		}
	}
}

// tuple copies values out of the receive buffer (which pgconn reuses).
func tuple(t *pglogrepl.TupleData) ([][]byte, []bool) {
	if t == nil {
		return nil, nil
	}
	vals := make([][]byte, len(t.Columns))
	var unchanged []bool
	for i, c := range t.Columns {
		switch c.DataType {
		case 'n':
		case 'u':
			if unchanged == nil {
				unchanged = make([]bool, len(t.Columns))
			}
			unchanged[i] = true
		default:
			v := make([]byte, len(c.Data))
			copy(v, c.Data)
			vals[i] = v
		}
	}
	return vals, unchanged
}
