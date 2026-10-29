// Package catalog introspects the Postgres catalog: tables, columns, primary
// keys and function volatility, which rule analysis and the wire codec need.
package catalog

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
)

// Kind is how the engine treats a column's values: in the row-local
// evaluator and in the wire encoding.
type Kind uint8

const (
	Text Kind = iota // text-like: sent as text, compared bytewise
	Int
	Float
	Numeric
	Bool
	UUID
	Timestamp // timestamptz and timestamp (naive is read as UTC)
	Date
	JSON
	Array
	// Other is text on the wire but never compared in the engine (citext,
	// non-deterministic collations, extension types).
	Other
)

func (k Kind) String() string {
	return [...]string{"text", "int", "float", "numeric", "bool", "uuid", "timestamp", "date", "json", "array", "other"}[k]
}

type Column struct {
	Name    string
	Type    string // pg_type.typname
	Kind    Kind
	Elem    Kind // element kind for arrays
	NotNull bool
	// Comparable: the engine may evaluate equality on this column itself
	// (deterministic collation, a kind with canonical text output).
	Comparable bool
}

type Table struct {
	Schema  string
	Name    string
	Columns []Column
	PK      []string
	index   map[string]int
}

func (t *Table) Column(name string) (*Column, bool) {
	i, ok := t.index[name]
	if !ok {
		return nil, false
	}
	return &t.Columns[i], true
}

// ColumnIndex returns the position of a column, or -1.
func (t *Table) ColumnIndex(name string) int {
	i, ok := t.index[name]
	if !ok {
		return -1
	}
	return i
}

type Catalog struct {
	Tables map[string]*Table // by name, public schema
	// Volatility of functions by name: 'i', 's' or 'v' (the most volatile
	// overload wins).
	Volatility map[string]byte
}

func (c *Catalog) Table(name string) (*Table, bool) {
	t, ok := c.Tables[name]
	return t, ok
}

func kindOf(typname, category string) Kind {
	switch typname {
	case "int2", "int4", "int8":
		return Int
	case "float4", "float8":
		return Float
	case "numeric":
		return Numeric
	case "bool":
		return Bool
	case "uuid":
		return UUID
	case "timestamptz", "timestamp":
		return Timestamp
	case "date":
		return Date
	case "json", "jsonb":
		return JSON
	case "text", "varchar", "bpchar", "name":
		return Text
	}
	if category == "A" {
		return Array
	}
	return Other
}

type Querier interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}

// Load reads the public schema.
func Load(ctx context.Context, q Querier) (*Catalog, error) {
	c := &Catalog{Tables: map[string]*Table{}, Volatility: map[string]byte{}}
	rows, err := q.Query(ctx, `
		SELECT c.relname, a.attname, t.typname, t.typcategory,
		       COALESCE(e.typname, ''), COALESCE(e.typcategory, ''),
		       a.attnotnull,
		       -- indkey is an int2vector, whose array form is 0-based.
		       COALESCE((SELECT k.ord FROM unnest(i.indkey) WITH ORDINALITY k(attnum, ord) WHERE k.attnum = a.attnum), 0),
		       COALESCE(co.collisdeterministic, true)
		FROM pg_attribute a
		JOIN pg_class c ON c.oid = a.attrelid
		JOIN pg_namespace n ON n.oid = c.relnamespace
		JOIN pg_type t ON t.oid = a.atttypid
		LEFT JOIN pg_type e ON e.oid = t.typelem AND t.typcategory = 'A'
		LEFT JOIN pg_index i ON i.indrelid = c.oid AND i.indisprimary
		LEFT JOIN pg_collation co ON co.oid = a.attcollation
		WHERE n.nspname = 'public' AND c.relkind IN ('r', 'p') AND a.attnum > 0 AND NOT a.attisdropped
		ORDER BY c.relname, a.attnum`)
	if err != nil {
		return nil, err
	}
	pkPos := map[string][]string{}
	for rows.Next() {
		var tbl, name, typ, cat, elem, elemCat string
		var notNull, deterministic bool
		var pos int
		if err := rows.Scan(&tbl, &name, &typ, &cat, &elem, &elemCat, &notNull, &pos, &deterministic); err != nil {
			return nil, err
		}
		t := c.Tables[tbl]
		if t == nil {
			t = &Table{Schema: "public", Name: tbl, index: map[string]int{}}
			c.Tables[tbl] = t
		}
		k := kindOf(typ, cat)
		col := Column{Name: name, Type: typ, Kind: k, NotNull: notNull}
		if k == Array {
			col.Elem = kindOf(elem, elemCat)
		}
		col.Comparable = deterministic && (k == Text || k == Int || k == Bool || k == UUID || k == Date)
		t.index[col.Name] = len(t.Columns)
		t.Columns = append(t.Columns, col)
		if pos > 0 {
			for len(pkPos[tbl]) < pos {
				pkPos[tbl] = append(pkPos[tbl], "")
			}
			pkPos[tbl][pos-1] = col.Name
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	for name, pk := range pkPos {
		c.Tables[name].PK = pk
	}
	frows, err := q.Query(ctx, `SELECT proname, max(provolatile) FROM pg_proc GROUP BY proname`)
	if err != nil {
		return nil, err
	}
	for frows.Next() {
		var name string
		var vol string
		if err := frows.Scan(&name, &vol); err != nil {
			return nil, err
		}
		// 'v' > 's' > 'i' as bytes, so max() is the most volatile overload.
		c.Volatility[name] = vol[0]
	}
	if err := frows.Err(); err != nil {
		return nil, err
	}
	for _, t := range c.Tables {
		if len(t.PK) == 0 {
			continue
		}
		for _, p := range t.PK {
			if p == "" {
				return nil, fmt.Errorf("table %s: incomplete primary key", t.Name)
			}
		}
	}
	return c, nil
}
