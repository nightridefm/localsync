package rules

import (
	"strings"
)

// Positional rewrites named parameters ($sub, $project, $claims_x) into
// $1..$n, skipping string literals, quoted identifiers, dollar-quoted bodies
// and comments, so Postgres' parser accepts the SQL. names[i] is the name of
// parameter $i+1, in order of first use.
func Positional(sql string) (out string, names []string) {
	var b strings.Builder
	b.Grow(len(sql))
	index := map[string]int{}
	for i := 0; i < len(sql); {
		c := sql[i]
		switch {
		case c == '\'' || c == '"':
			j := i + 1
			for j < len(sql) {
				if sql[j] == c {
					if j+1 < len(sql) && sql[j+1] == c {
						j += 2
						continue
					}
					break
				}
				j++
			}
			end := min(j+1, len(sql))
			b.WriteString(sql[i:end])
			i = end
		case c == '-' && i+1 < len(sql) && sql[i+1] == '-':
			j := strings.IndexByte(sql[i:], '\n')
			if j < 0 {
				j = len(sql) - i
			}
			b.WriteString(sql[i : i+j])
			i += j
		case c == '/' && i+1 < len(sql) && sql[i+1] == '*':
			j := strings.Index(sql[i+2:], "*/")
			end := len(sql)
			if j >= 0 {
				end = i + 2 + j + 2
			}
			b.WriteString(sql[i:end])
			i = end
		case c == '$' && i+1 < len(sql) && isIdentStart(sql[i+1]):
			j := i + 1
			for j < len(sql) && isIdent(sql[j]) {
				j++
			}
			if j < len(sql) && sql[j] == '$' {
				// Dollar-quoted body: $tag$ ... $tag$.
				tag := sql[i : j+1]
				k := strings.Index(sql[j+1:], tag)
				end := len(sql)
				if k >= 0 {
					end = j + 1 + k + len(tag)
				}
				b.WriteString(sql[i:end])
				i = end
				continue
			}
			name := sql[i+1 : j]
			n, ok := index[name]
			if !ok {
				names = append(names, name)
				n = len(names)
				index[name] = n
			}
			b.WriteByte('$')
			b.WriteString(itoa(n))
			i = j
		default:
			b.WriteByte(c)
			i++
		}
	}
	return b.String(), names
}

func isIdentStart(c byte) bool {
	return c == '_' || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')
}

func isIdent(c byte) bool {
	return isIdentStart(c) || (c >= '0' && c <= '9')
}

func itoa(n int) string {
	if n < 10 {
		return string(rune('0' + n))
	}
	return itoa(n/10) + string(rune('0'+n%10))
}

// Named is the inverse of Positional: $n becomes $names[n-1], with the
// same lexing (literals, quoted identifiers and comments are left alone).
// ok is false for a $n out of range.
func Named(sql string, names []string) (string, bool) {
	var b strings.Builder
	b.Grow(len(sql) + 16)
	for i := 0; i < len(sql); {
		c := sql[i]
		switch {
		case c == '\'' || c == '"':
			j := i + 1
			for j < len(sql) {
				if sql[j] == c {
					if j+1 < len(sql) && sql[j+1] == c {
						j += 2
						continue
					}
					break
				}
				j++
			}
			end := min(j+1, len(sql))
			b.WriteString(sql[i:end])
			i = end
		case c == '-' && i+1 < len(sql) && sql[i+1] == '-':
			j := strings.IndexByte(sql[i:], '\n')
			if j < 0 {
				j = len(sql) - i
			}
			b.WriteString(sql[i : i+j])
			i += j
		case c == '/' && i+1 < len(sql) && sql[i+1] == '*':
			j := strings.Index(sql[i+2:], "*/")
			end := len(sql)
			if j >= 0 {
				end = i + 2 + j + 2
			}
			b.WriteString(sql[i:end])
			i = end
		case c == '$' && i+1 < len(sql) && sql[i+1] >= '0' && sql[i+1] <= '9':
			j := i + 1
			n := 0
			for j < len(sql) && sql[j] >= '0' && sql[j] <= '9' {
				n = n*10 + int(sql[j]-'0')
				j++
			}
			if n < 1 || n > len(names) {
				return "", false
			}
			b.WriteByte('$')
			b.WriteString(names[n-1])
			i = j
		default:
			b.WriteByte(c)
			i++
		}
	}
	return b.String(), true
}
