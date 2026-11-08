package rules

import (
	pg "github.com/pganalyze/pg_query_go/v6"
	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/nightridefm/localsync/internal/catalog"
)

// expandViews replaces every view the statement reads with its definition,
// as a subquery under the view's alias (or name), so analysis only ever sees
// tables: the engine decodes tables, never views. A view is a named
// subquery, so this changes no result. Views of views expand too (visit
// walks into each replacement). It reports whether anything changed.
func expandViews(sel *pg.SelectStmt, cat *catalog.Catalog) (bool, error) {
	changed := false
	var err error
	visit(sel, func(m protoreflect.Message) {
		node, ok := m.Interface().(*pg.Node)
		if !ok || err != nil {
			return
		}
		rv := node.GetRangeVar()
		if rv == nil || (rv.Schemaname != "" && rv.Schemaname != "public") {
			return
		}
		v, ok := cat.Views[rv.Relname]
		if !ok {
			return
		}
		if v.Barrier {
			err = reject("view %s is a security_barrier view; expanding it would drop that protection", rv.Relname)
			return
		}
		tree, perr := pg.Parse(v.SQL)
		if perr != nil || len(tree.Stmts) != 1 || tree.Stmts[0].Stmt.GetSelectStmt() == nil {
			err = reject("view %s: cannot read its definition", rv.Relname)
			return
		}
		alias := rv.Alias
		if alias == nil {
			alias = &pg.Alias{Aliasname: rv.Relname}
		}
		node.Node = &pg.Node_RangeSubselect{RangeSubselect: &pg.RangeSubselect{Subquery: tree.Stmts[0].Stmt, Alias: alias}}
		changed = true
	})
	return changed, err
}

// checkRelations rejects any relation that is not a table in public: the
// engine must decode every table a rule reads, so it publishes them all.
func checkRelations(sel *pg.SelectStmt, cat *catalog.Catalog) error {
	var err error
	visit(sel, func(m protoreflect.Message) {
		rv, ok := m.Interface().(*pg.RangeVar)
		if !ok || err != nil {
			return
		}
		if rv.Schemaname != "" && rv.Schemaname != "public" {
			err = reject("%s.%s: only tables in public can be synced", rv.Schemaname, rv.Relname)
		} else if _, ok := cat.Table(rv.Relname); !ok {
			err = reject("unknown table %s (views expand; other relations, CTEs inside subqueries and other schemas are not supported)", rv.Relname)
		}
	})
	return err
}
