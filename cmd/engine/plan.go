package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/nightridefm/localsync/internal/catalog"
	"github.com/nightridefm/localsync/internal/engine"
	"github.com/nightridefm/localsync/internal/rules"
)

// plan prints how each rule will be evaluated (docs/design.md section 3.5).
func plan(args []string) error {
	fs := flag.NewFlagSet("plan", flag.ExitOnError)
	pgURL := fs.String("pg", os.Getenv("LOCALSYNC_PG_URL"), "Postgres URL whose catalog the rules are checked against")
	rulesPath := fs.String("rules", "", "rules.yaml")
	maxTier := fs.Int("max-tier", 3, "exit non-zero if any rule lands above this tier")
	run := fs.Bool("run", true, "run every rule and gate (read only, one row) with sample parameters, as the engine will")
	_ = fs.Parse(args)

	if *rulesPath == "" {
		return errors.New("plan needs --rules")
	}
	rs, err := rules.Load(*rulesPath)
	if err != nil {
		return err
	}
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, *pgURL)
	if err != nil {
		return err
	}
	defer pool.Close()
	cat, err := catalog.Load(ctx, pool)
	if err != nil {
		return err
	}

	counts := map[string]int{}
	worst := 0
	failed := 0
	fmt.Println("| Rule | Table | Access | Tier | Strategy | Depends on | Run | Notes |")
	fmt.Println("| --- | --- | --- | --- | --- | --- | --- | --- |")
	for _, r := range rs {
		a, err := rules.Analyze(r, cat)
		if err != nil {
			counts["rejected"]++
			worst = 4
			fmt.Printf("| %s | | | rejected | | | | %s |\n", r.Name, md(err.Error()))
			continue
		}
		// A split rule is served by its body and gate units, so the body's
		// strategy is the rule's.
		served := a
		ran := "not run"
		if *run {
			body, err := engine.Check(ctx, pool, cat, a)
			switch {
			case err != nil:
				failed++
				ran = "FAILED: " + err.Error()
			case body != nil:
				ran = "ok (split)"
			default:
				ran = "ok"
			}
			if body != nil {
				served = body
			}
		} else if a.Split != nil {
			if b, err := rules.Analyze(a.Split.Body, cat); err == nil {
				served = b
			}
		}
		counts[fmt.Sprintf("tier %d", served.Tier)]++
		worst = max(worst, served.Tier)
		strategy := "on the row"
		if !served.Fast {
			strategy = "restricted queries"
		}
		fmt.Printf("| %s | %s | %s | %d | %s | %s | %s | %s |\n", r.Name, a.Table.Name, md(a.Access()), served.Tier, strategy, strings.Join(a.Deps, ", "), md(ran), md(strings.Join(a.Notes, "; ")))
	}
	fmt.Printf("\n%d rules: tier 1 %d, tier 2 %d, tier 3 %d, rejected %d", len(rs), counts["tier 1"], counts["tier 2"], counts["tier 3"], counts["rejected"])
	if *run {
		fmt.Printf(", failed to run %d", failed)
	}
	fmt.Println()
	if worst > *maxTier {
		return fmt.Errorf("a rule is above --max-tier %d", *maxTier)
	}
	if failed > 0 {
		return fmt.Errorf("%d rules failed to run", failed)
	}
	return nil
}

func md(s string) string { return strings.ReplaceAll(s, "|", `\|`) }
