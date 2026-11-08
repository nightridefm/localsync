package rules

import (
	pg "github.com/pganalyze/pg_query_go/v6"
	"google.golang.org/protobuf/proto"
)

// Split splits a rule with gates (top-level uncorrelated EXISTS / NOT
// EXISTS) into a body and one gate rule per gate. The rule's rows are the
// body's rows while every gate holds, none otherwise, which is exactly
// `body AND gates` because gates do not depend on the row. Each derived
// rule binds only its own parameters, so when the principal appears only
// in gates, every principal with the same other parameters shares one
// body.
//
// Split is a proposal: the engine analyses the derived rules and checks
// them against Postgres, and serves the rule unsplit when any step fails.
// Unsplit, a gate is an ordinary subquery: correct, re-evaluated in full
// on every change to its tables.
type Split struct {
	Body  Rule
	Gates []GateRule
}

// GateRule: the gate holds when Rule's result is non-empty (EXISTS), or
// empty when Negated (NOT EXISTS).
type GateRule struct {
	Rule    Rule
	Negated bool
}

func (a *Analysis) split(sel *pg.SelectStmt, scope level) {
	if sel.WhereClause == nil {
		return
	}
	var body []*pg.Node
	var gates []GateRule
	for _, c := range conjuncts(sel.WhereClause) {
		if !isGate(c, scope) {
			body = append(body, c)
			continue
		}
		neg := false
		n := c
		if b := n.GetBoolExpr(); b != nil && b.Boolop == pg.BoolExprType_NOT_EXPR {
			neg, n = true, b.Args[0]
		}
		sub := proto.Clone(n.GetSubLink().Subselect.GetSelectStmt()).(*pg.SelectStmt)
		if sub == nil || len(sub.FromClause) != 1 || sub.FromClause[0].GetRangeVar() == nil {
			return
		}
		star := &pg.Node{Node: &pg.Node_ColumnRef{ColumnRef: &pg.ColumnRef{Fields: []*pg.Node{{Node: &pg.Node_AStar{AStar: &pg.A_Star{}}}}}}}
		sub.TargetList = []*pg.Node{{Node: &pg.Node_ResTarget{ResTarget: &pg.ResTarget{Val: star}}}}
		sql, ok := a.named(sub)
		if !ok {
			return
		}
		gates = append(gates, GateRule{Rule: Rule{Name: a.Rule.Name + "#gate" + itoa(len(gates)), SQL: sql, Public: true, ClientParams: a.Rule.ClientParams}, Negated: neg})
	}
	if len(gates) == 0 {
		return
	}
	b := proto.Clone(sel).(*pg.SelectStmt)
	switch len(body) {
	case 0:
		b.WhereClause = nil
	case 1:
		b.WhereClause = body[0]
	default:
		b.WhereClause = &pg.Node{Node: &pg.Node_BoolExpr{BoolExpr: &pg.BoolExpr{Boolop: pg.BoolExprType_AND_EXPR, Args: body}}}
	}
	sql, ok := a.named(b)
	if !ok {
		return
	}
	a.Split = &Split{Body: Rule{Name: a.Rule.Name + "#body", SQL: sql, Public: true, ClientParams: a.Rule.ClientParams}, Gates: gates}
}

// named deparses s and turns its positional parameters back into the
// rule's parameter names.
func (a *Analysis) named(s *pg.SelectStmt) (string, bool) {
	sql, err := pg.Deparse(&pg.ParseResult{Version: a.pgVersion, Stmts: []*pg.RawStmt{{Stmt: &pg.Node{Node: &pg.Node_SelectStmt{SelectStmt: s}}}}})
	if err != nil {
		return "", false
	}
	return Named(sql, a.Params)
}
