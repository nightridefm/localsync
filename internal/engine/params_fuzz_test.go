package engine

import (
	"context"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/jackc/pgx/v5"

	"github.com/nightridefm/localsync/internal/testdb"
)

// Client parameters reach Postgres through pgx's simple protocol, which
// quotes them into the SQL text on the client side. Whatever a client
// sends, Postgres must see exactly that string, next to SQL shaped like
// rules (comments, a minus, a dollar-quoted body, a second parameter).
func FuzzSimpleProtocolParam(f *testing.F) {
	pool := testdb.Pool(f)
	for _, s := range []string{"x", "'", `\'`, "' OR '1'='1", "$$", "*/", "--", "-1", "$1", " ", ""} {
		f.Add(s, "ok")
	}
	ctx := context.Background()
	f.Fuzz(func(t *testing.T, a, b string) {
		if !utf8.ValidString(a) || !utf8.ValidString(b) || strings.ContainsRune(a+b, 0) {
			return // refused before a query (want: "bad parameter")
		}
		var gotA, gotB string
		var n int
		err := pool.QueryRow(ctx, `SELECT $1::text /* $2 */, -- $1
			$2::text, 1 - -length($1) + length($$ $1 $$) * 0`,
			pgx.QueryExecModeSimpleProtocol, a, b).Scan(&gotA, &gotB, &n)
		if err != nil {
			t.Fatalf("%q %q: %v", a, b, err)
		}
		if gotA != a || gotB != b || n != 1+utf8.RuneCountInString(a) {
			t.Fatalf("sent %q %q, Postgres saw %q %q (%d)", a, b, gotA, gotB, n)
		}
	})
}
