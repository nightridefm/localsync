package rules

import (
	"strconv"
	"strings"

	"github.com/nightridefm/localsync/internal/catalog"
)

// Tri is SQL three-valued logic.
type Tri int8

const (
	False Tri = iota
	True
	Null
)

func and(a, b Tri) Tri {
	if a == False || b == False {
		return False
	}
	if a == Null || b == Null {
		return Null
	}
	return True
}

func or(a, b Tri) Tri {
	if a == True || b == True {
		return True
	}
	if a == Null || b == Null {
		return Null
	}
	return False
}

func not(a Tri) Tri {
	switch a {
	case True:
		return False
	case False:
		return True
	}
	return Null
}

// Row is one tuple in Postgres text output form; nil is NULL.
type Row [][]byte

// Expr is a compiled row-local predicate. params are the unit's parameter
// values by position ($1 = params[0]).
type Expr interface {
	Eval(row Row, params []string) Tri
}

// operand is the non-column side of a comparison: a constant (already
// canonical) or a parameter, canonicalised at evaluation.
type operand struct {
	param int // >= 0: params[param]
	value string
}

func (o operand) resolve(kind catalog.Kind, params []string) (string, bool) {
	if o.param < 0 {
		return o.value, true
	}
	if o.param >= len(params) {
		return "", false
	}
	return Canonical(kind, params[o.param])
}

type andExpr []Expr

func (e andExpr) Eval(r Row, p []string) Tri {
	out := True
	for _, x := range e {
		out = and(out, x.Eval(r, p))
		if out == False {
			return False
		}
	}
	return out
}

type orExpr []Expr

func (e orExpr) Eval(r Row, p []string) Tri {
	out := False
	for _, x := range e {
		out = or(out, x.Eval(r, p))
		if out == True {
			return True
		}
	}
	return out
}

type notExpr struct{ x Expr }

func (e notExpr) Eval(r Row, p []string) Tri { return not(e.x.Eval(r, p)) }

type constExpr Tri

func (e constExpr) Eval(Row, []string) Tri { return Tri(e) }

// cmpExpr is `col = operand` or `col <> operand` on a comparable column.
type cmpExpr struct {
	col  int
	kind catalog.Kind
	eq   bool
	rhs  operand
}

func (e cmpExpr) Eval(r Row, p []string) Tri {
	v := r[e.col]
	if v == nil {
		return Null
	}
	rhs, ok := e.rhs.resolve(e.kind, p)
	if !ok {
		return Null
	}
	if (string(v) == rhs) == e.eq {
		return True
	}
	return False
}

// colCmpExpr is `a = b` between two comparable columns of the same kind.
type colCmpExpr struct {
	a, b int
	eq   bool
}

func (e colCmpExpr) Eval(r Row, _ []string) Tri {
	if r[e.a] == nil || r[e.b] == nil {
		return Null
	}
	if (string(r[e.a]) == string(r[e.b])) == e.eq {
		return True
	}
	return False
}

// inExpr is `col [NOT] IN (operands)` with SQL NULL semantics.
type inExpr struct {
	col   int
	kind  catalog.Kind
	items []operand
	neg   bool
}

func (e inExpr) Eval(r Row, p []string) Tri {
	v := r[e.col]
	if v == nil {
		return Null
	}
	out := False
	for _, it := range e.items {
		rhs, ok := it.resolve(e.kind, p)
		if !ok {
			out = Null
			continue
		}
		if string(v) == rhs {
			out = True
			break
		}
	}
	if e.neg {
		return not(out)
	}
	return out
}

type nullTest struct {
	col    int
	isNull bool
}

func (e nullTest) Eval(r Row, _ []string) Tri {
	if (r[e.col] == nil) == e.isNull {
		return True
	}
	return False
}

// boolCol is a bare boolean column used as a predicate.
type boolCol struct{ col int }

func (e boolCol) Eval(r Row, _ []string) Tri {
	v := r[e.col]
	if v == nil {
		return Null
	}
	if len(v) == 1 && v[0] == 't' {
		return True
	}
	return False
}

// boolTest is `x IS [NOT] TRUE / FALSE / UNKNOWN`: never NULL.
type boolTest struct {
	x    Expr
	want Tri
	neg  bool
}

func (e boolTest) Eval(r Row, p []string) Tri {
	got := e.x.Eval(r, p) == e.want
	if got != e.neg {
		return True
	}
	return False
}

// Canonical maps a parameter or constant to the text Postgres would output
// for that value in a column of the given kind, so it can be compared with
// WAL values bytewise. ok is false if the value is not valid for the kind.
func Canonical(kind catalog.Kind, s string) (string, bool) {
	switch kind {
	case catalog.Int:
		n, err := strconv.ParseInt(strings.TrimSpace(s), 10, 64)
		if err != nil {
			return "", false
		}
		return strconv.FormatInt(n, 10), true
	case catalog.UUID:
		return canonicalUUID(s)
	case catalog.Bool:
		switch strings.ToLower(strings.TrimSpace(s)) {
		case "t", "true", "y", "yes", "on", "1":
			return "t", true
		case "f", "false", "n", "no", "off", "0":
			return "f", true
		}
		return "", false
	case catalog.Date:
		if len(s) == 10 && s[4] == '-' && s[7] == '-' {
			return s, true
		}
		return "", false
	case catalog.Text:
		return s, true
	}
	return "", false
}

func canonicalUUID(s string) (string, bool) {
	s = strings.TrimSuffix(strings.TrimPrefix(strings.TrimSpace(s), "{"), "}")
	var hex [32]byte
	n := 0
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c == '-':
			continue
		case c >= '0' && c <= '9', c >= 'a' && c <= 'f':
		case c >= 'A' && c <= 'F':
			c += 'a' - 'A'
		default:
			return "", false
		}
		if n == 32 {
			return "", false
		}
		hex[n] = c
		n++
	}
	if n != 32 {
		return "", false
	}
	h := string(hex[:])
	return h[0:8] + "-" + h[8:12] + "-" + h[12:16] + "-" + h[16:20] + "-" + h[20:32], true
}
