// Package rules loads, normalises and analyses sync rules: plain SQL
// queries over one table, parameterised by the principal ($sub, claims)
// and by client-chosen values ($project). See docs/design.md section 3.
package rules

import (
	"fmt"
	"os"
	"slices"
	"sort"
	"strings"

	"gopkg.in/yaml.v3"
)

// Rule is one named rule after group expansion. SQL still uses named
// parameters; Positional turns it into Postgres syntax.
//
// Every rule declares who may want it (A22), or fails to load: Public
// (any valid token), Claims (the token's grants must include one), a
// $sub reference (row-scoped or gated per principal), or claims and $sub
// together.
type Rule struct {
	Name     string
	SQL      string
	Priority int
	Public   bool
	// Claims: the token's `grants` must include at least one of these.
	Claims []string
	// ClientParams are the parameters the client supplies in WANT.
	ClientParams []string
}

type groupFile struct {
	Params   []string          `yaml:"params"`
	Gate     string            `yaml:"gate"`
	Priority int               `yaml:"priority"`
	Public   bool              `yaml:"public"`
	Claims   claimList         `yaml:"claims"`
	Rules    map[string]string `yaml:"rules"`
}

type ruleFile struct {
	SQL      string    `yaml:"sql"`
	Params   []string  `yaml:"params"`
	Priority int       `yaml:"priority"`
	Public   bool      `yaml:"public"`
	Claims   claimList `yaml:"claims"`
}

// claimList is `claims: admin` or `claims: [admin, curator]`.
type claimList []string

func (c *claimList) UnmarshalYAML(n *yaml.Node) error {
	if n.Kind == yaml.ScalarNode {
		*c = claimList{n.Value}
		return nil
	}
	var l []string
	if err := n.Decode(&l); err != nil {
		return err
	}
	*c = l
	return nil
}

type file struct {
	Rules  map[string]yaml.Node `yaml:"rules"`
	Groups map[string]groupFile `yaml:"groups"`
}

// Load reads a rules.yaml. Rule values are either a SQL string or a mapping
// with sql / params / priority / public / claims.
func Load(path string) ([]Rule, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return Parse(raw)
}

func Parse(raw []byte) ([]Rule, error) {
	var f file
	if err := yaml.Unmarshal(raw, &f); err != nil {
		return nil, err
	}
	var out []Rule
	for name, node := range f.Rules {
		var r ruleFile
		if node.Kind == yaml.ScalarNode {
			r.SQL = node.Value
		} else if err := node.Decode(&r); err != nil {
			return nil, fmt.Errorf("rule %s: %w", name, err)
		}
		out = append(out, Rule{Name: name, SQL: strings.TrimSpace(r.SQL), Priority: r.Priority, Public: r.Public, Claims: r.Claims, ClientParams: r.Params})
	}
	for gname, g := range f.Groups {
		for name, sql := range g.Rules {
			full := strings.TrimSpace(sql)
			if g.Gate != "" {
				full = andWhere(full, g.Gate)
			}
			out = append(out, Rule{Name: name, SQL: full, Priority: g.Priority, Public: g.Public, Claims: g.Claims, ClientParams: g.Params})
			_ = gname
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	for i := 1; i < len(out); i++ {
		if out[i].Name == out[i-1].Name {
			return nil, fmt.Errorf("rule %s defined twice", out[i].Name)
		}
	}
	for i := range out {
		slices.Sort(out[i].ClientParams)
	}
	return out, nil
}

// andWhere appends `AND (gate)` to the rule's WHERE, or adds one. The rule
// is re-parsed during analysis, so a textual join is enough here.
func andWhere(sql, gate string) string {
	if i := topLevelWhere(sql); i >= 0 {
		return sql[:i] + " WHERE (" + gate + ") AND (" + strings.TrimSpace(sql[i+len(" WHERE"):]) + ")"
	}
	return sql + " WHERE (" + gate + ")"
}

// topLevelWhere finds " WHERE" outside parentheses and quotes.
func topLevelWhere(sql string) int {
	depth := 0
	var quote byte
	upper := strings.ToUpper(sql)
	for i := 0; i < len(sql); i++ {
		c := sql[i]
		if quote != 0 {
			if c == quote {
				quote = 0
			}
			continue
		}
		switch c {
		case '\'', '"':
			quote = c
		case '(':
			depth++
		case ')':
			depth--
		default:
			if depth == 0 && strings.HasPrefix(upper[i:], "WHERE") && i > 0 && isSpace(sql[i-1]) {
				return i - 1
			}
		}
	}
	return -1
}

func isSpace(c byte) bool { return c == ' ' || c == '\n' || c == '\t' || c == '\r' }
