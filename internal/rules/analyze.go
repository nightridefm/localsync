package rules

import (
	"errors"
	"fmt"
	"slices"
	"sort"
	"strings"

	pg "github.com/pganalyze/pg_query_go/v6"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/nightridefm/localsync/internal/catalog"
)

// Analysis is everything the engine derives from one rule. It never decides
// membership itself: it says how to evaluate it cheaply (Fast) and, for
// every table the rule reads, which rows a change can affect (restrictions,
// docs/design.md section 4). Anything it does not understand becomes the
// restriction TRUE, which is always correct.
type Analysis struct {
	Rule Rule
	// SQL with positional parameters; Params[i] names $i+1.
	SQL    string
	Params []string
	Table  *catalog.Table
	// Columns are indexes into Table.Columns: the projection, PK included.
	Columns []int
	// Fast: membership is a row-local predicate the engine evaluates on
	// the WAL row itself (Pred nil means TRUE).
	Fast bool
	Pred Expr
	// Gates are row-independent conjuncts (uncorrelated EXISTS / NOT
	// EXISTS), each as a standalone query `SELECT 1 WHERE <gate>` over the
	// unit's parameters. A fast rule's unit is empty unless all gates hold;
	// changes to a gate's tables re-evaluate the unit (Full occurrences).
	Gates []string
	// OwnNarrow are top-level AND conjuncts of the rule's own table that
	// the engine can evaluate: a change for which one is not TRUE on either
	// the old or the new row cannot affect the unit.
	OwnNarrow []Expr
	// AllFull: every change to any dependency re-evaluates the whole unit
	// (LIMIT, top-level constructs the analysis does not decompose).
	AllFull     bool
	Occurrences []Occurrence
	Deps        []string
	Tier        int
	Notes       []string
	// RefCols, per table, are the columns the rule's WHERE may read (an
	// over-approximation: every referenced name that is a column of that
	// table). An update that changes none of them cannot change any
	// unit's membership.
	RefCols map[string][]int
	// subRows: $sub is in a condition on the rule's own columns (only the
	// principal's rows); subGate: in any other condition, which shows
	// every row once it holds. Conservative for review: a condition is
	// "rows" only when it provably reads the row (Access).
	subRows, subGate bool
	// Share, when set, proposes serving the rule from a body shared by
	// every principal plus per-principal gates (share.go).
	Share *Share

	pgVersion int32
}

// Hop is one table above a changed level: `Table.Col IN (SELECT <child's
// column> ...)`. Up is Table's own select column toward its parent, or ""
// when Table is the rule's table (the last hop).
type Hop struct {
	Table string
	Col   string
	Up    string
}

// Occurrence is one place a table appears inside the rule's subqueries.
type Occurrence struct {
	Table string
	// ValueCol: the changed table's column whose values flow up the chain.
	ValueCol string
	// Hops from the changed level up to the rule's table.
	Hops []Hop
	// Full: changes here re-evaluate the whole unit (uncorrelated gate,
	// or a construct the analysis does not decompose).
	Full bool
	// Narrow is this level's own row-local predicate, if evaluable.
	Narrow Expr
	// NullSensitive: under NOT IN, a NULL in ValueCol affects every row.
	NullSensitive bool
}

// ErrRejected marks rules that cannot be synced correctly at all.
var ErrRejected = errors.New("rule rejected")

func reject(format string, args ...any) error {
	return fmt.Errorf("%w: "+format, append([]any{ErrRejected}, args...)...)
}

// Analyze parses and analyses one rule against the catalog.
func Analyze(rule Rule, cat *catalog.Catalog) (*Analysis, error) {
	sql, names := Positional(rule.SQL)
	a := &Analysis{Rule: rule, SQL: sql, Params: names}
	for _, n := range names {
		if n != "sub" && !slices.Contains(rule.ClientParams, n) {
			return nil, reject("parameter $%s is neither $sub nor a declared client parameter", n)
		}
	}
	tree, err := pg.Parse(sql)
	if err != nil {
		return nil, reject("parse: %v", err)
	}
	if len(tree.Stmts) != 1 {
		return nil, reject("must be exactly one statement")
	}
	a.pgVersion = tree.Version
	sel := tree.Stmts[0].Stmt.GetSelectStmt()
	if sel == nil {
		return nil, reject("must be a SELECT")
	}
	if err := checkFunctions(sel, cat); err != nil {
		return nil, err
	}
	if sel.Op != pg.SetOperation_SETOP_NONE || sel.WithClause != nil || len(sel.ValuesLists) > 0 {
		return nil, reject("set operations, WITH and VALUES are not supported at the top level")
	}
	if len(sel.FromClause) != 1 || sel.FromClause[0].GetRangeVar() == nil {
		return nil, reject("FROM must name exactly one table (use IN / EXISTS for related tables)")
	}
	if len(sel.GroupClause) > 0 || sel.HavingClause != nil || len(sel.WindowClause) > 0 || len(sel.DistinctClause) > 0 {
		return nil, reject("GROUP BY, HAVING, DISTINCT and window functions are not supported at the top level")
	}
	rv := sel.FromClause[0].GetRangeVar()
	t, ok := cat.Table(rv.Relname)
	if !ok || (rv.Schemaname != "" && rv.Schemaname != "public") {
		return nil, reject("unknown table %s", rv.Relname)
	}
	if len(t.PK) == 0 {
		return nil, reject("table %s has no primary key", t.Name)
	}
	a.Table = t
	if err := a.projection(sel.TargetList); err != nil {
		return nil, err
	}
	a.Deps = tablesIn(sel)
	a.refCols(sel.WhereClause, cat)
	if sel.LimitCount != nil || sel.LimitOffset != nil {
		a.AllFull = true
		a.Notes = append(a.Notes, "LIMIT/OFFSET: every change re-evaluates the whole unit")
	}
	scope := level{table: t, alias: aliasOf(rv)}
	if sel.WhereClause == nil {
		a.Fast = true
	} else if !a.AllFull {
		if e, ok := compile(sel.WhereClause, scope, names); ok {
			a.Fast, a.Pred = true, e
		} else if pred, gates, ok := a.gated(sel.WhereClause, scope, cat); ok {
			a.Fast, a.Pred, a.Gates = true, pred, gates
		} else {
			for _, c := range conjuncts(sel.WhereClause) {
				if hasSubLink(c) {
					continue
				}
				if e, ok := compile(c, scope, names); ok {
					a.OwnNarrow = append(a.OwnNarrow, e)
				}
			}
			a.walk(sel.WhereClause, scope, nil, "", false, cat)
		}
	}
	if a.AllFull {
		for _, d := range a.Deps {
			a.Occurrences = append(a.Occurrences, Occurrence{Table: d, Full: true})
		}
	}
	a.tier()
	a.share(sel, scope)
	// Access last, so a malformed rule reports what is wrong with it.
	sub := slices.Contains(names, "sub")
	a.subRows, a.subGate = subPlacement(sel.WhereClause, int32(slices.Index(names, "sub")+1))
	if rule.Public && len(rule.Claims) > 0 {
		return nil, reject("public and claims together: public means any valid token")
	}
	if len(rule.ClientParams) > 0 && !rule.Public && len(rule.Claims) == 0 && !sub {
		return nil, reject("client parameters %v are not constrained by $sub or claims (mark the rule public if intended)", rule.ClientParams)
	}
	if !rule.Public && len(rule.Claims) == 0 && !sub {
		// Default deny (A22): a rule nobody declared access for would
		// otherwise be readable by every valid token.
		return nil, reject("no access declared: reference $sub, require claims, or mark the rule public")
	}
	return a, nil
}

func (a *Analysis) refCols(where *pg.Node, cat *catalog.Catalog) {
	names := map[string]bool{}
	visit(where, func(m protoreflect.Message) {
		if cr, ok := m.Interface().(*pg.ColumnRef); ok && !isStar(cr) {
			names[lastField(cr)] = true
		}
	})
	a.RefCols = map[string][]int{}
	for _, d := range a.Deps {
		t, ok := cat.Table(d)
		if !ok {
			continue
		}
		cols := []int{}
		for i, c := range t.Columns {
			if names[c.Name] {
				cols = append(cols, i)
			}
		}
		a.RefCols[d] = cols
	}
	if _, ok := a.RefCols[a.Table.Name]; !ok {
		a.RefCols[a.Table.Name] = []int{}
	}
}

// gated splits a WHERE into a compilable row predicate and uncorrelated
// EXISTS gates. ok is false unless every conjunct is one or the other.
func (a *Analysis) gated(where *pg.Node, scope level, cat *catalog.Catalog) (Expr, []string, bool) {
	var local []Expr
	var gates []*pg.Node
	for _, c := range conjuncts(where) {
		if isGate(c, scope) {
			gates = append(gates, c)
			continue
		}
		if hasSubLink(c) {
			return nil, nil, false
		}
		e, ok := compile(c, scope, a.Params)
		if !ok {
			return nil, nil, false
		}
		local = append(local, e)
	}
	if len(gates) == 0 {
		return nil, nil, false
	}
	var sqls []string
	for _, g := range gates {
		q, err := pg.Deparse(&pg.ParseResult{Version: a.pgVersion, Stmts: []*pg.RawStmt{{Stmt: &pg.Node{Node: &pg.Node_SelectStmt{SelectStmt: &pg.SelectStmt{
			TargetList:  []*pg.Node{{Node: &pg.Node_ResTarget{ResTarget: &pg.ResTarget{Val: &pg.Node{Node: &pg.Node_AConst{AConst: &pg.A_Const{Val: &pg.A_Const_Ival{Ival: &pg.Integer{Ival: 1}}}}}}}}},
			WhereClause: g,
		}}}}}})
		if err != nil {
			return nil, nil, false
		}
		sqls = append(sqls, q)
		a.walk(g, scope, nil, "", false, cat)
	}
	var pred Expr
	if len(local) > 0 {
		pred = andExpr(local)
	}
	return pred, sqls, true
}

// isGate: EXISTS / NOT EXISTS over a subquery that never references scope.
func isGate(n *pg.Node, scope level) bool {
	if b := n.GetBoolExpr(); b != nil && b.Boolop == pg.BoolExprType_NOT_EXPR && len(b.Args) == 1 {
		n = b.Args[0]
	}
	sl := n.GetSubLink()
	if sl == nil || sl.SubLinkType != pg.SubLinkType_EXISTS_SUBLINK {
		return false
	}
	outer := false
	visit(sl.Subselect, func(m protoreflect.Message) {
		if cr, ok := m.Interface().(*pg.ColumnRef); ok && len(cr.Fields) == 2 {
			q := cr.Fields[0].GetString_().GetSval()
			if q == scope.alias || q == scope.table.Name {
				outer = true
			}
		}
	})
	return !outer
}

func (a *Analysis) projection(targets []*pg.Node) error {
	t := a.Table
	if len(targets) == 1 {
		if cr := targets[0].GetResTarget().GetVal().GetColumnRef(); cr != nil && isStar(cr) {
			for i := range t.Columns {
				a.Columns = append(a.Columns, i)
			}
			return nil
		}
	}
	for _, n := range targets {
		rt := n.GetResTarget()
		cr := rt.GetVal().GetColumnRef()
		if cr == nil || rt.Name != "" || isStar(cr) {
			return reject("output must be plain columns of %s (no expressions or aliases)", t.Name)
		}
		name := lastField(cr)
		i := t.ColumnIndex(name)
		if i < 0 {
			return reject("unknown column %s.%s", t.Name, name)
		}
		a.Columns = append(a.Columns, i)
	}
	for _, pk := range t.PK {
		if !slices.Contains(a.Columns, t.ColumnIndex(pk)) {
			return reject("output must include primary key column %s", pk)
		}
	}
	return nil
}

func (a *Analysis) tier() {
	switch {
	case a.Fast:
		a.Tier = 1
	case a.AllFull:
		a.Tier = 3
	default:
		a.Tier = 2
		for _, o := range a.Occurrences {
			if o.Full && o.Narrow == nil {
				a.Tier = 3
			}
		}
	}
}

type level struct {
	table *catalog.Table
	alias string
}

func (l level) owns(cr *pg.ColumnRef) bool {
	fs := cr.Fields
	switch len(fs) {
	case 1:
		return l.table.ColumnIndex(lastField(cr)) >= 0
	case 2:
		q := fs[0].GetString_().GetSval()
		return q == l.alias || q == l.table.Name
	}
	return false
}

// walk records an Occurrence for every table inside subqueries of n, an
// expression evaluated at scope. hops lead from scope's parent up to the
// rule's table; scopeUp is scope's own select column toward its parent (""
// for the rule's table). neg tracks NOT for NOT IN's NULL sensitivity.
func (a *Analysis) walk(n *pg.Node, scope level, hops []Hop, scopeUp string, neg bool, cat *catalog.Catalog) {
	if n == nil {
		return
	}
	if b := n.GetBoolExpr(); b != nil {
		for _, x := range b.Args {
			a.walk(x, scope, hops, scopeUp, neg != (b.Boolop == pg.BoolExprType_NOT_EXPR), cat)
		}
		return
	}
	sl := n.GetSubLink()
	if sl == nil {
		if hasSubLink(n) {
			a.opaque(n, "subquery inside an expression")
		}
		return
	}
	sub := sl.Subselect.GetSelectStmt()
	inner, innerAlias, ok := simpleSubselect(sub, cat)
	if !ok {
		a.opaque(n, "subquery shape not decomposed")
		return
	}
	innerScope := level{table: inner, alias: innerAlias}
	switch sl.SubLinkType {
	case pg.SubLinkType_ANY_SUBLINK:
		cr := sl.Testexpr.GetColumnRef()
		if cr == nil || !scope.owns(cr) || !(len(sl.OperName) == 0 || operName(sl.OperName) == "=") || len(sub.TargetList) != 1 {
			a.opaque(n, "IN with a non-column left side or non-equality operator")
			return
		}
		sc := sub.TargetList[0].GetResTarget().GetVal().GetColumnRef()
		if sc == nil || !innerScope.owns(sc) {
			a.opaque(n, "IN subquery must select one column")
			return
		}
		a.level(sub.WhereClause, innerScope, lastField(sc), Hop{Table: scope.table.Name, Col: lastField(cr), Up: scopeUp}, hops, neg, nil, cat)
	case pg.SubLinkType_EXISTS_SUBLINK:
		var rest []*pg.Node
		var innerCol, outerCol string
		for _, c := range conjuncts(sub.WhereClause) {
			if innerCol == "" {
				if ic, oc, ok := correlation(c, innerScope, scope); ok {
					innerCol, outerCol = ic, oc
					continue
				}
			}
			rest = append(rest, c)
		}
		if innerCol == "" {
			// Uncorrelated: a gate. Any relevant change re-evaluates the
			// unit, narrowed by the gate's own predicate.
			a.gate(sub.WhereClause, innerScope, cat)
			return
		}
		a.level(nil, innerScope, innerCol, Hop{Table: scope.table.Name, Col: outerCol, Up: scopeUp}, hops, neg, rest, cat)
	default:
		a.opaque(n, "scalar or ALL subquery")
	}
}

// level records the occurrence for a subquery level and recurses into its
// nested subqueries. where is the level's WHERE; extra are already-split
// conjuncts (EXISTS after removing the correlation).
func (a *Analysis) level(where *pg.Node, scope level, valueCol string, up Hop, hops []Hop, neg bool, extra []*pg.Node, cat *catalog.Catalog) {
	// This level's hops: its parent first, then the parent's own hops.
	chain := make([]Hop, 0, len(hops)+1)
	chain = append(chain, up)
	chain = append(chain, hops...)
	cs := conjuncts(where)
	cs = append(cs, extra...)
	var local []Expr
	for _, c := range cs {
		if hasSubLink(c) {
			continue
		}
		if e, ok := compile(c, scope, a.Params); ok {
			local = append(local, e)
		} else if referencesOutside(c, scope) {
			a.opaque(c, "correlated predicate inside a subquery")
			return
		}
	}
	o := Occurrence{Table: scope.table.Name, ValueCol: valueCol, Hops: chain, NullSensitive: neg}
	if len(local) > 0 {
		o.Narrow = andExpr(local)
	}
	a.Occurrences = append(a.Occurrences, o)
	for _, c := range cs {
		if hasSubLink(c) {
			a.walk(c, scope, chain, valueCol, neg, cat)
		}
	}
}

// gate records Full occurrences for an uncorrelated subquery and
// everything inside it, keeping each level's narrowing predicate.
func (a *Analysis) gate(where *pg.Node, scope level, cat *catalog.Catalog) {
	var local []Expr
	for _, c := range conjuncts(where) {
		if hasSubLink(c) {
			a.opaque(c, "subquery inside a gate")
			continue
		}
		if e, ok := compile(c, scope, a.Params); ok {
			local = append(local, e)
		}
	}
	o := Occurrence{Table: scope.table.Name, Full: true}
	if len(local) > 0 {
		o.Narrow = andExpr(local)
	}
	a.Occurrences = append(a.Occurrences, o)
}

func (a *Analysis) opaque(n *pg.Node, why string) {
	for _, t := range tablesIn(n) {
		a.Occurrences = append(a.Occurrences, Occurrence{Table: t, Full: true})
	}
	a.Notes = append(a.Notes, why)
}

// Restriction returns SQL (over alias r of the rule's table) selecting the
// rows a change with these ValueCol values can affect. Values are Postgres
// text; NULLs must already be removed.
func (o *Occurrence) Restriction(values []string) string {
	cur := "IN (" + literals(values) + ")"
	for _, h := range o.Hops {
		if h.Up == "" {
			return "r." + Ident(h.Col) + " " + cur
		}
		cur = "IN (SELECT " + Ident(h.Up) + " FROM " + Ident(h.Table) + " WHERE " + Ident(h.Col) + " " + cur + ")"
	}
	return "TRUE"
}

// PKRestriction selects rows by primary key values (one []string per row).
func PKRestriction(t *catalog.Table, pks [][]string) string {
	if len(t.PK) == 1 {
		vals := make([]string, len(pks))
		for i, p := range pks {
			vals[i] = p[0]
		}
		return "r." + Ident(t.PK[0]) + " IN (" + literals(vals) + ")"
	}
	cols := make([]string, len(t.PK))
	for i, c := range t.PK {
		cols[i] = "r." + Ident(c)
	}
	rows := make([]string, len(pks))
	for i, p := range pks {
		rows[i] = "(" + literals(p) + ")"
	}
	return "(" + strings.Join(cols, ", ") + ") IN (" + strings.Join(rows, ", ") + ")"
}

func literals(vals []string) string {
	var b strings.Builder
	for i, v := range vals {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString(Literal(v))
	}
	return b.String()
}

// Literal quotes a value as a SQL string literal (standard_conforming_strings).
func Literal(v string) string {
	return "'" + strings.ReplaceAll(v, "'", "''") + "'"
}

func Ident(s string) string {
	return `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
}

// ---- AST helpers ----

func conjuncts(n *pg.Node) []*pg.Node {
	if n == nil {
		return nil
	}
	if b := n.GetBoolExpr(); b != nil && b.Boolop == pg.BoolExprType_AND_EXPR {
		var out []*pg.Node
		for _, x := range b.Args {
			out = append(out, conjuncts(x)...)
		}
		return out
	}
	return []*pg.Node{n}
}

func simpleSubselect(s *pg.SelectStmt, cat *catalog.Catalog) (*catalog.Table, string, bool) {
	if s == nil || s.Op != pg.SetOperation_SETOP_NONE || s.WithClause != nil || len(s.FromClause) != 1 ||
		len(s.GroupClause) > 0 || s.HavingClause != nil || s.LimitCount != nil || s.LimitOffset != nil ||
		len(s.DistinctClause) > 0 || len(s.WindowClause) > 0 {
		return nil, "", false
	}
	rv := s.FromClause[0].GetRangeVar()
	if rv == nil {
		return nil, "", false
	}
	t, ok := cat.Table(rv.Relname)
	if !ok {
		return nil, "", false
	}
	for _, tn := range s.TargetList {
		if hasAggregate(tn) {
			return nil, "", false
		}
	}
	return t, aliasOf(rv), true
}

// correlation recognises `inner.col = outer.col` (either order).
func correlation(n *pg.Node, inner, outer level) (string, string, bool) {
	e := n.GetAExpr()
	if e == nil || e.Kind != pg.A_Expr_Kind_AEXPR_OP || operName(e.Name) != "=" {
		return "", "", false
	}
	l, r := e.Lexpr.GetColumnRef(), e.Rexpr.GetColumnRef()
	if l == nil || r == nil {
		return "", "", false
	}
	qualifiedTo := func(cr *pg.ColumnRef, lv level) bool {
		return len(cr.Fields) == 2 && lv.owns(cr)
	}
	switch {
	case qualifiedTo(r, outer) && inner.owns(l) && !qualifiedTo(l, outer):
		return lastField(l), lastField(r), true
	case qualifiedTo(l, outer) && inner.owns(r) && !qualifiedTo(r, outer):
		return lastField(r), lastField(l), true
	}
	return "", "", false
}

// referencesOutside reports a column reference not owned by scope.
func referencesOutside(n *pg.Node, scope level) bool {
	found := false
	visit(n, func(m protoreflect.Message) {
		if cr, ok := m.Interface().(*pg.ColumnRef); ok && !scope.owns(cr) {
			found = true
		}
	})
	return found
}

// subPlacement classifies the top-level conjuncts that use parameter k
// ($sub): rows when the conjunct reads the rule's own columns outside any
// subquery (`user_id = $sub`, `id IN (... $sub ...)`), otherwise gate.
func subPlacement(where *pg.Node, k int32) (rows, gate bool) {
	if k == 0 {
		return false, false
	}
	for _, c := range conjuncts(where) {
		uses := false
		visit(c, func(m protoreflect.Message) {
			if p, ok := m.Interface().(*pg.ParamRef); ok && p.Number == k {
				uses = true
			}
		})
		if !uses {
			continue
		}
		if rowColumn(c) {
			rows = true
		} else {
			gate = true
		}
	}
	return rows, gate
}

// rowColumn reports a column reference outside subqueries: with one table
// in FROM, a column of the rule's own row. For `x IN (subquery)` only x.
func rowColumn(n proto.Message) bool {
	if n == nil {
		return false
	}
	switch x := n.(type) {
	case *pg.ColumnRef:
		return true
	case *pg.SubLink:
		return rowColumn(x.Testexpr)
	}
	m := n.ProtoReflect()
	if !m.IsValid() {
		return false
	}
	found := false
	m.Range(func(fd protoreflect.FieldDescriptor, v protoreflect.Value) bool {
		switch {
		case fd.IsList() && fd.Message() != nil:
			l := v.List()
			for i := 0; i < l.Len() && !found; i++ {
				found = rowColumn(l.Get(i).Message().Interface())
			}
		case fd.Message() != nil && !fd.IsMap():
			found = rowColumn(v.Message().Interface())
		}
		return !found
	})
	return found
}

func hasSubLink(n *pg.Node) bool {
	found := false
	visit(n, func(m protoreflect.Message) {
		if _, ok := m.Interface().(*pg.SubLink); ok {
			found = true
		}
	})
	return found
}

func hasAggregate(n *pg.Node) bool {
	found := false
	visit(n, func(m protoreflect.Message) {
		if fc, ok := m.Interface().(*pg.FuncCall); ok {
			switch funcName(fc) {
			case "count", "sum", "min", "max", "avg", "array_agg", "string_agg", "bool_and", "bool_or", "every", "json_agg", "jsonb_agg":
				found = true
			}
		}
	})
	return found
}

func tablesIn(n proto.Message) []string {
	seen := map[string]bool{}
	visit(n, func(m protoreflect.Message) {
		if rv, ok := m.Interface().(*pg.RangeVar); ok {
			seen[rv.Relname] = true
		}
	})
	out := make([]string, 0, len(seen))
	for t := range seen {
		out = append(out, t)
	}
	sort.Strings(out)
	return out
}

// checkFunctions rejects functions whose result can change without a WAL
// event (anything not IMMUTABLE) and SQL value functions like
// CURRENT_TIMESTAMP.
func checkFunctions(n proto.Message, cat *catalog.Catalog) error {
	var err error
	visit(n, func(m protoreflect.Message) {
		if err != nil {
			return
		}
		switch x := m.Interface().(type) {
		case *pg.SQLValueFunction:
			err = reject("%s changes without writes; not allowed in a rule", x.Op.String())
		case *pg.FuncCall:
			name := funcName(x)
			vol, ok := cat.Volatility[name]
			switch {
			case !ok:
				err = reject("unknown function %s", name)
			case vol != 'i' && !hasAggregate(&pg.Node{Node: &pg.Node_FuncCall{FuncCall: x}}):
				err = reject("function %s is not IMMUTABLE: its result can change without writes", name)
			}
		}
	})
	return err
}

func visit(n proto.Message, fn func(protoreflect.Message)) {
	if n == nil {
		return
	}
	m := n.ProtoReflect()
	if !m.IsValid() {
		return
	}
	fn(m)
	m.Range(func(fd protoreflect.FieldDescriptor, v protoreflect.Value) bool {
		switch {
		case fd.IsList() && fd.Message() != nil:
			l := v.List()
			for i := 0; i < l.Len(); i++ {
				visit(l.Get(i).Message().Interface(), fn)
			}
		case fd.Message() != nil && !fd.IsMap():
			visit(v.Message().Interface(), fn)
		}
		return true
	})
}

func funcName(fc *pg.FuncCall) string {
	if len(fc.Funcname) == 0 {
		return ""
	}
	return fc.Funcname[len(fc.Funcname)-1].GetString_().GetSval()
}

func operName(ns []*pg.Node) string {
	if len(ns) == 0 {
		return ""
	}
	return ns[len(ns)-1].GetString_().GetSval()
}

func lastField(cr *pg.ColumnRef) string {
	if len(cr.Fields) == 0 {
		return ""
	}
	return cr.Fields[len(cr.Fields)-1].GetString_().GetSval()
}

func isStar(cr *pg.ColumnRef) bool {
	return len(cr.Fields) > 0 && cr.Fields[len(cr.Fields)-1].GetAStar() != nil
}

func aliasOf(rv *pg.RangeVar) string {
	if rv.Alias != nil {
		return rv.Alias.Aliasname
	}
	return rv.Relname
}

// ---- row-local compilation ----

// compile turns a predicate over one table into an Expr, or reports false
// if any part is outside what the engine evaluates exactly.
func compile(n *pg.Node, scope level, params []string) (Expr, bool) {
	switch {
	case n.GetBoolExpr() != nil:
		b := n.GetBoolExpr()
		var xs []Expr
		for _, x := range b.Args {
			e, ok := compile(x, scope, params)
			if !ok {
				return nil, false
			}
			xs = append(xs, e)
		}
		switch b.Boolop {
		case pg.BoolExprType_AND_EXPR:
			return andExpr(xs), true
		case pg.BoolExprType_OR_EXPR:
			return orExpr(xs), true
		case pg.BoolExprType_NOT_EXPR:
			return notExpr{xs[0]}, true
		}
	case n.GetNullTest() != nil:
		nt := n.GetNullTest()
		col, ok := scopeColumn(nt.Arg, scope)
		if !ok {
			return nil, false
		}
		return nullTest{col: col, isNull: nt.Nulltesttype == pg.NullTestType_IS_NULL}, true
	case n.GetBooleanTest() != nil:
		bt := n.GetBooleanTest()
		x, ok := compile(bt.Arg, scope, params)
		if !ok {
			return nil, false
		}
		switch bt.Booltesttype {
		case pg.BoolTestType_IS_TRUE:
			return boolTest{x: x, want: True}, true
		case pg.BoolTestType_IS_NOT_TRUE:
			return boolTest{x: x, want: True, neg: true}, true
		case pg.BoolTestType_IS_FALSE:
			return boolTest{x: x, want: False}, true
		case pg.BoolTestType_IS_NOT_FALSE:
			return boolTest{x: x, want: False, neg: true}, true
		case pg.BoolTestType_IS_UNKNOWN:
			return boolTest{x: x, want: Null}, true
		case pg.BoolTestType_IS_NOT_UNKNOWN:
			return boolTest{x: x, want: Null, neg: true}, true
		}
	case n.GetColumnRef() != nil:
		col, ok := scopeColumn(n, scope)
		if !ok || scope.table.Columns[col].Kind != catalog.Bool {
			return nil, false
		}
		return boolCol{col: col}, true
	case n.GetAConst() != nil:
		c := n.GetAConst()
		if bv := c.GetBoolval(); bv != nil {
			if bv.Boolval {
				return constExpr(True), true
			}
			return constExpr(False), true
		}
	case n.GetAExpr() != nil:
		return compileAExpr(n.GetAExpr(), scope, params)
	}
	return nil, false
}

func compileAExpr(e *pg.A_Expr, scope level, params []string) (Expr, bool) {
	op := operName(e.Name)
	switch e.Kind {
	case pg.A_Expr_Kind_AEXPR_OP:
		if op != "=" && op != "<>" && op != "!=" {
			return nil, false
		}
		eq := op == "="
		lc, lok := scopeColumn(e.Lexpr, scope)
		rc, rok := scopeColumn(e.Rexpr, scope)
		cols := scope.table.Columns
		switch {
		case lok && rok:
			if !cols[lc].Comparable || !cols[rc].Comparable || cols[lc].Kind != cols[rc].Kind {
				return nil, false
			}
			return colCmpExpr{a: lc, b: rc, eq: eq}, true
		case lok || rok:
			col, other := lc, e.Rexpr
			if rok {
				col, other = rc, e.Lexpr
			}
			c := cols[col]
			if !c.Comparable && c.Kind != catalog.Bool {
				return nil, false
			}
			o, ok := operandOf(other, c.Kind, params)
			if !ok {
				return nil, false
			}
			return cmpExpr{col: col, kind: c.Kind, eq: eq, rhs: o}, true
		}
	case pg.A_Expr_Kind_AEXPR_IN:
		col, ok := scopeColumn(e.Lexpr, scope)
		if !ok {
			return nil, false
		}
		c := scope.table.Columns[col]
		if !c.Comparable {
			return nil, false
		}
		list := e.Rexpr.GetList()
		if list == nil {
			return nil, false
		}
		var items []operand
		for _, it := range list.Items {
			o, ok := operandOf(it, c.Kind, params)
			if !ok {
				return nil, false
			}
			items = append(items, o)
		}
		return inExpr{col: col, kind: c.Kind, items: items, neg: op == "<>"}, true
	}
	return nil, false
}

func scopeColumn(n *pg.Node, scope level) (int, bool) {
	if n == nil {
		return 0, false
	}
	if tc := n.GetTypeCast(); tc != nil {
		return 0, false
	}
	cr := n.GetColumnRef()
	if cr == nil || isStar(cr) || !scope.owns(cr) {
		return 0, false
	}
	i := scope.table.ColumnIndex(lastField(cr))
	return i, i >= 0
}

func operandOf(n *pg.Node, kind catalog.Kind, params []string) (operand, bool) {
	if tc := n.GetTypeCast(); tc != nil {
		n = tc.Arg
	}
	if p := n.GetParamRef(); p != nil {
		if int(p.Number) < 1 || int(p.Number) > len(params) {
			return operand{}, false
		}
		return operand{param: int(p.Number) - 1}, true
	}
	c := n.GetAConst()
	if c == nil || c.Isnull {
		return operand{}, false
	}
	var raw string
	switch {
	case c.GetSval() != nil:
		raw = c.GetSval().Sval
	case c.GetIval() != nil:
		raw = fmt.Sprint(c.GetIval().Ival)
	case c.GetBoolval() != nil:
		raw = fmt.Sprint(c.GetBoolval().Boolval)
	default:
		return operand{}, false
	}
	v, ok := Canonical(kind, raw)
	if !ok {
		return operand{}, false
	}
	return operand{param: -1, value: v}, true
}

// Access says who may want the rule (A22): "public", the claims a token
// must grant, "$sub" (row-scoped or gated per principal), or both.
func (a *Analysis) Access() string {
	if a.Rule.Public {
		return "public"
	}
	var parts []string
	if len(a.Rule.Claims) > 0 {
		parts = append(parts, "claims: "+strings.Join(a.Rule.Claims, " or "))
	}
	switch {
	case a.subRows && a.subGate:
		parts = append(parts, "$sub rows + gate")
	case a.subRows:
		parts = append(parts, "$sub rows")
	case a.subGate:
		parts = append(parts, "$sub gate")
	}
	return strings.Join(parts, " + ")
}
