package query

import (
	"bytes"
	"testing"
	"time"

	"github.com/ncode/chronicle/internal/ingest"
	"github.com/ncode/chronicle/internal/store"
)

// compile is a pure transformation (Query -> SQL), so it is tested here with a
// literal path map and zero database. The DB-backed behaviour tests live in
// query_integration_test.go and still gate that the SQL is semantically correct.

// h returns the value_hash bytes the durable subquery binds for a value, matching
// what parseLiteral produces (bare words -> Go string).
func h(v any) []byte {
	x := store.ValueHash(v)
	return x[:]
}

func compileTestClassifier(t *testing.T) *ingest.Classifier {
	t.Helper()
	cl, err := ingest.NewClassifier([]string{"uptime", "memory.system.*"})
	if err != nil {
		t.Fatal(err)
	}
	return cl
}

// testPaths: durable paths and their fixed ids. Volatile paths (uptime, memory.*)
// are deliberately absent — they were never interned, so resolvePaths would miss.
var testPaths = map[string]int64{"os.name": 10, "role": 20, "kernel": 30}

func TestCompile(t *testing.T) {
	cl := compileTestClassifier(t)
	at, err := time.Parse(time.RFC3339, "2026-01-01T10:00:00Z")
	if err != nil {
		t.Fatal(err)
	}

	cases := []struct {
		name      string
		dsl       string
		inactive  bool
		wantEmpty bool
		wantSQL   string
		wantArgs  []any
	}{
		{
			name:     "filter single durable now",
			dsl:      `os.name=Debian`,
			wantSQL:  `SELECT n.certname FROM nodes n WHERE n.node_id IN ((SELECT node_id FROM current_facts WHERE path_id = $1 AND value_hash = $2)) AND n.deactivated IS NULL AND n.expired IS NULL ORDER BY n.certname`,
			wantArgs: []any{int64(10), h("Debian")},
		},
		{
			name:     "filter AND two durable terms",
			dsl:      `role=web os.name=Debian`,
			wantSQL:  `SELECT n.certname FROM nodes n WHERE n.node_id IN ((SELECT node_id FROM current_facts WHERE path_id = $1 AND value_hash = $2) INTERSECT (SELECT node_id FROM current_facts WHERE path_id = $3 AND value_hash = $4)) AND n.deactivated IS NULL AND n.expired IS NULL ORDER BY n.certname`,
			wantArgs: []any{int64(20), h("web"), int64(10), h("Debian")},
		},
		{
			name:     "filter include_inactive drops active clause",
			dsl:      `os.name=Debian`,
			inactive: true,
			wantSQL:  `SELECT n.certname FROM nodes n WHERE n.node_id IN ((SELECT node_id FROM current_facts WHERE path_id = $1 AND value_hash = $2)) ORDER BY n.certname`,
			wantArgs: []any{int64(10), h("Debian")},
		},
		{
			name:     "filter at-T routes through facts_at",
			dsl:      `os.name=Debian at 2026-01-01T10:00:00Z`,
			wantSQL:  `SELECT n.certname FROM nodes n WHERE n.node_id IN ((SELECT node_id FROM facts_at($1) WHERE path_id = $2 AND value_hash = $3)) AND n.deactivated IS NULL AND n.expired IS NULL ORDER BY n.certname`,
			wantArgs: []any{at, int64(10), h("Debian")},
		},
		{
			name:     "filter volatile term routes to node_volatile",
			dsl:      `uptime=12345`,
			wantSQL:  `SELECT n.certname FROM nodes n WHERE n.node_id IN ((SELECT node_id FROM node_volatile WHERE volatile -> $1 = $2::jsonb)) AND n.deactivated IS NULL AND n.expired IS NULL ORDER BY n.certname`,
			wantArgs: []any{"uptime", "12345"},
		},
		{
			name:      "filter unknown durable path is empty",
			dsl:       `nope=x`,
			wantEmpty: true,
		},
		{
			name:     "groupby durable now",
			dsl:      `os.name where role=web group by os.name`,
			wantSQL:  `SELECT gf.value, count(*) FROM current_facts gf JOIN nodes n ON n.node_id = gf.node_id WHERE gf.node_id IN ((SELECT node_id FROM current_facts WHERE path_id = $1 AND value_hash = $2)) AND gf.path_id = $3 AND n.deactivated IS NULL AND n.expired IS NULL GROUP BY gf.value_hash, gf.value ORDER BY count(*) DESC, gf.value`,
			wantArgs: []any{int64(20), h("web"), int64(10)},
		},
		{
			// The hardest arg ordering: facts_at($) timestamp bound in BOTH the
			// where-term subquery ($1) and the group src ($5), around the term/group
			// path ids. Locks the order [ts, pid_term, hash, pid_group, ts].
			name:     "groupby durable field at-T",
			dsl:      `os.name where role=web group by os.name at 2026-01-01T10:00:00Z`,
			wantSQL:  `SELECT gf.value, count(*) FROM facts_at($5) gf JOIN nodes n ON n.node_id = gf.node_id WHERE gf.node_id IN ((SELECT node_id FROM facts_at($1) WHERE path_id = $2 AND value_hash = $3)) AND gf.path_id = $4 AND n.deactivated IS NULL AND n.expired IS NULL GROUP BY gf.value_hash, gf.value ORDER BY count(*) DESC, gf.value`,
			wantArgs: []any{at, int64(20), h("web"), int64(10), at},
		},
		{
			name:     "groupby durable include_inactive drops active clause",
			dsl:      `os.name where role=web group by os.name`,
			inactive: true,
			wantSQL:  `SELECT gf.value, count(*) FROM current_facts gf JOIN nodes n ON n.node_id = gf.node_id WHERE gf.node_id IN ((SELECT node_id FROM current_facts WHERE path_id = $1 AND value_hash = $2)) AND gf.path_id = $3 GROUP BY gf.value_hash, gf.value ORDER BY count(*) DESC, gf.value`,
			wantArgs: []any{int64(20), h("web"), int64(10)},
		},
		{
			name:     "groupby volatile field now",
			dsl:      `uptime where role=web group by uptime`,
			wantSQL:  `SELECT gv.volatile -> $3, count(*) FROM node_volatile gv JOIN nodes n ON n.node_id = gv.node_id WHERE gv.node_id IN ((SELECT node_id FROM current_facts WHERE path_id = $1 AND value_hash = $2)) AND gv.volatile ? $4 AND n.deactivated IS NULL AND n.expired IS NULL GROUP BY gv.volatile -> $3 ORDER BY count(*) DESC, gv.volatile -> $3`,
			wantArgs: []any{int64(20), h("web"), "uptime", "uptime"},
		},
		{
			name:      "groupby unknown group field is empty",
			dsl:       `nope where role=web group by nope`,
			wantEmpty: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			q, err := Parse(tc.dsl)
			if err != nil {
				t.Fatalf("parse %q: %v", tc.dsl, err)
			}
			sql, args, empty, err := compile(q, cl, testPaths, tc.inactive)
			if err != nil {
				t.Fatalf("compile: %v", err)
			}
			if empty != tc.wantEmpty {
				t.Fatalf("empty = %v, want %v", empty, tc.wantEmpty)
			}
			if tc.wantEmpty {
				return
			}
			if sql != tc.wantSQL {
				t.Fatalf("SQL mismatch:\n got: %s\nwant: %s", sql, tc.wantSQL)
			}
			if !argsEqual(args, tc.wantArgs) {
				t.Fatalf("args mismatch:\n got: %#v\nwant: %#v", args, tc.wantArgs)
			}
		})
	}
}

func argsEqual(got, want []any) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		switch w := want[i].(type) {
		case []byte:
			g, ok := got[i].([]byte)
			if !ok || !bytes.Equal(g, w) {
				return false
			}
		case time.Time:
			g, ok := got[i].(time.Time)
			if !ok || !g.Equal(w) {
				return false
			}
		default:
			if got[i] != want[i] {
				return false
			}
		}
	}
	return true
}
