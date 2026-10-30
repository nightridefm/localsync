package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"regexp"
	"strings"

	"github.com/jackc/pgx/v5"

	"github.com/nightridefm/localsync/internal/catalog"
	"github.com/nightridefm/localsync/internal/rules"
)

// plan prints how each rule will be evaluated (docs/design.md 3.4).
func plan(args []string) error {
	fs := flag.NewFlagSet("plan", flag.ExitOnError)
	pgURL := fs.String("pg", os.Getenv("LOCALSYNC_PG_URL"), "Postgres URL whose catalog the rules are checked against")
	rulesPath := fs.String("rules", "", "rules.yaml")
	corpus := fs.String("corpus", "", "corpus shapes.json (Electric shapes, converted to rules)")
	prefix := fs.String("prefix", "", "with --corpus: only shapes whose file starts with this")
	gate := fs.String("gate", "", "with --corpus: SQL gate ANDed into rules that use a client parameter")
	maxTier := fs.Int("max-tier", 3, "exit non-zero if any rule lands above this tier")
	_ = fs.Parse(args)

	var rs []rules.Rule
	var err error
	switch {
	case *rulesPath != "":
		rs, err = rules.Load(*rulesPath)
	case *corpus != "":
		rs, err = corpusRules(*corpus, *prefix, *gate)
	default:
		return errors.New("plan needs --rules or --corpus")
	}
	if err != nil {
		return err
	}
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, *pgURL)
	if err != nil {
		return err
	}
	defer conn.Close(ctx)
	cat, err := catalog.Load(ctx, conn)
	if err != nil {
		return err
	}

	counts := map[string]int{}
	worst := 0
	fmt.Println("| Rule | Table | Access | Tier | Strategy | Depends on | Notes |")
	fmt.Println("| --- | --- | --- | --- | --- | --- | --- |")
	for _, r := range rs {
		a, err := rules.Analyze(r, cat)
		if err != nil {
			counts["rejected"]++
			worst = 4
			fmt.Printf("| %s | | | rejected | | | %s |\n", r.Name, md(err.Error()))
			continue
		}
		counts[fmt.Sprintf("tier %d", a.Tier)]++
		worst = max(worst, a.Tier)
		strategy := "on the row"
		if !a.Fast {
			strategy = "restricted queries"
			if a.Tier == 2 {
				strategy += " (Phase 4: maintained sets)"
			}
		}
		fmt.Printf("| %s | %s | %s | %d | %s | %s | %s |\n", r.Name, a.Table.Name, md(a.Access()), a.Tier, strategy, strings.Join(a.Deps, ", "), md(strings.Join(a.Notes, "; ")))
	}
	fmt.Printf("\n%d rules: tier 1 %d, tier 2 %d, tier 3 %d, rejected %d\n", len(rs), counts["tier 1"], counts["tier 2"], counts["tier 3"], counts["rejected"])
	if worst > *maxTier {
		return fmt.Errorf("a rule is above --max-tier %d", *maxTier)
	}
	return nil
}

func md(s string) string { return strings.ReplaceAll(s, "|", `\|`) }

type shape struct {
	File    string   `json:"file"`
	Line    int      `json:"line"`
	Route   string   `json:"route"`
	Table   string   `json:"table"`
	Where   *string  `json:"where"`
	Columns []string `json:"columns"`
}

var roleRe = regexp.MustCompile(`/api/([^/]+)/`)

// corpusRules converts Electric shapes into rules: SELECT <columns or *>
// FROM <table> [WHERE <where>], named role + route.
func corpusRules(path, prefix, gate string) ([]rules.Rule, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var shapes []shape
	if err := json.Unmarshal(raw, &shapes); err != nil {
		return nil, err
	}
	var out []rules.Rule
	seen := map[string]int{}
	for _, s := range shapes {
		if !strings.HasPrefix(s.File, prefix) {
			continue
		}
		cols := "*"
		if len(s.Columns) > 0 {
			q := make([]string, len(s.Columns))
			for i, c := range s.Columns {
				q[i] = rules.Ident(c)
			}
			cols = strings.Join(q, ", ")
		}
		sql := fmt.Sprintf("SELECT %s FROM %s", cols, rules.Ident(s.Table))
		where := ""
		if s.Where != nil && *s.Where != "" {
			where = *s.Where
		}
		r := rules.Rule{}
		if strings.Contains(where, "$project") {
			r.ClientParams = []string{"project"}
			if gate != "" {
				where = "(" + gate + ") AND (" + where + ")"
			}
		}
		if where != "" {
			sql += " WHERE " + where
		}
		role := "?"
		if m := roleRe.FindStringSubmatch(s.File); m != nil {
			role = m[1]
		}
		name := role + s.Route
		if n := seen[name]; n > 0 {
			name = fmt.Sprintf("%s#%d", name, n+1)
		}
		seen[role+s.Route]++
		r.Name, r.SQL = name, sql
		out = append(out, r)
	}
	return out, nil
}
