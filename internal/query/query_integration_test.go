package query

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/ncode/chronicle/internal/classify"
	"github.com/ncode/chronicle/internal/store"
)

func testEngine(t *testing.T) (*Engine, *store.Store, context.Context) {
	t.Helper()
	dsn := os.Getenv("CHRONICLE_TEST_DB")
	if dsn == "" {
		t.Skip("set CHRONICLE_TEST_DB to run query integration tests")
	}
	ctx := context.Background()
	st, err := store.Open(ctx, dsn, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(st.Close)
	// Cross-node queries see ALL nodes, so these tests need a clean slate. Run
	// integration tests with `-p 1` (Makefile does) so packages don't share the
	// DB concurrently.
	if _, err := st.Pool().Exec(ctx, `TRUNCATE nodes, fact_paths RESTART IDENTITY CASCADE`); err != nil {
		t.Fatal(err)
	}
	cl, err := classify.New([]string{"uptime", "memory.system.*"})
	if err != nil {
		t.Fatal(err)
	}
	return &Engine{store: st, classifier: cl}, st, ctx
}

var (
	qt1 = time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC)
	qt2 = time.Date(2026, 1, 1, 11, 0, 0, 0, time.UTC)
)

// seed applies a full durable fact set for a node at time `at` (clean discovery,
// so omitted facts would tombstone — always pass the complete desired set).
func seed(t *testing.T, st *store.Store, ctx context.Context, certname string, at time.Time, facts map[string]any) {
	t.Helper()
	seedSnapshot(t, st, ctx, certname, at, facts, json.RawMessage(`{}`))
}

func seedSnapshot(t *testing.T, st *store.Store, ctx context.Context, certname string, at time.Time, facts map[string]any, vol json.RawMessage) {
	t.Helper()
	leaves := make([]store.PendingLeaf, 0, len(facts))
	for path, v := range facts {
		name, _, _ := strings.Cut(path, ".")
		raw, err := json.Marshal(v)
		if err != nil {
			t.Fatal(err)
		}
		leaves = append(leaves, store.PendingLeaf{Path: path, FactName: name, Value: raw, Hash: store.ValueHash(v)})
	}
	if _, err := st.ApplySnapshot(ctx, certname, at, at, 0, leaves, vol, true); err != nil {
		t.Fatal(err)
	}
}

func wipe(t *testing.T, st *store.Store, ctx context.Context, certnames ...string) {
	t.Helper()
	for _, cn := range certnames {
		if _, err := st.Pool().Exec(ctx, `DELETE FROM nodes WHERE certname=$1`, cn); err != nil {
			t.Fatal(err)
		}
	}
}

func run(t *testing.T, e *Engine, ctx context.Context, dsl string, inactive bool) *Result {
	t.Helper()
	q, err := Parse(dsl)
	if err != nil {
		t.Fatalf("parse %q: %v", dsl, err)
	}
	res, err := e.Run(ctx, q, inactive)
	if err != nil {
		t.Fatalf("run %q: %v", dsl, err)
	}
	return res
}

func TestFilterAndGroupBy(t *testing.T) {
	e, st, ctx := testEngine(t)
	wipe(t, st, ctx, "qweb01", "qweb02", "qdb01")
	seed(t, st, ctx, "qweb01", qt1, map[string]any{"role": "web", "os.name": "Debian"})
	seed(t, st, ctx, "qweb02", qt1, map[string]any{"role": "web", "os.name": "Ubuntu"})
	seed(t, st, ctx, "qdb01", qt1, map[string]any{"role": "db", "os.name": "Debian"})

	// Compound AND -> INTERSECT.
	res := run(t, e, ctx, `role=web os.name=Debian`, false)
	if !sameSet(res.Nodes, []string{"qweb01"}) {
		t.Fatalf("AND filter = %v, want [qweb01]", res.Nodes)
	}
	// Single term.
	res = run(t, e, ctx, `os.name=Debian`, false)
	if !sameSet(res.Nodes, []string{"qweb01", "qdb01"}) {
		t.Fatalf("single filter = %v", res.Nodes)
	}
	// Group-by counts per group over the filtered set.
	res = run(t, e, ctx, `os.name where role=web group by os.name`, false)
	counts := map[string]int{}
	for _, g := range res.Groups {
		counts[string(g.Value)] = g.Count
	}
	if counts[`"Debian"`] != 1 || counts[`"Ubuntu"`] != 1 {
		t.Fatalf("group-by counts = %+v", res.Groups)
	}
}

func TestAtTimePointInTime(t *testing.T) {
	e, st, ctx := testEngine(t)
	wipe(t, st, ctx, "qhist01")
	seed(t, st, ctx, "qhist01", qt1, map[string]any{"role": "web", "os.name": "Debian"})
	seed(t, st, ctx, "qhist01", qt2, map[string]any{"role": "web", "os.name": "Ubuntu"})

	// Between t1 and t2 the node was Debian.
	mid := qt1.Add(30 * time.Minute).Format(time.RFC3339)
	res := run(t, e, ctx, `os.name=Debian at `+mid, false)
	if !sameSet(res.Nodes, []string{"qhist01"}) {
		t.Fatalf("at-T Debian = %v", res.Nodes)
	}
	// Now it is Ubuntu, so Debian-now finds nothing.
	res = run(t, e, ctx, `os.name=Debian`, false)
	if len(res.Nodes) != 0 {
		t.Fatalf("Debian-now = %v, want empty", res.Nodes)
	}
}

func TestVolatileRoutingAndNoHistory(t *testing.T) {
	e, st, ctx := testEngine(t)
	wipe(t, st, ctx, "qvol01")
	seedSnapshot(t, st, ctx, "qvol01", qt1, map[string]any{"role": "web"}, json.RawMessage(`{"uptime":12345}`))

	// at <past> on a volatile path => typed no-history error.
	_, err := e.Run(ctx, mustParse(t, `uptime=12345 at 2026-01-01T09:00:00Z`), false)
	if !errors.Is(err, ErrNoHistory) {
		t.Fatalf("volatile at-past => %v, want ErrNoHistory", err)
	}

	// Volatile lookup at now routes to node_volatile.
	res := run(t, e, ctx, `uptime=12345`, false)
	if !sameSet(res.Nodes, []string{"qvol01"}) {
		t.Fatalf("volatile-now = %v, want [qvol01]", res.Nodes)
	}
}

func TestIncludeInactive(t *testing.T) {
	e, st, ctx := testEngine(t)
	wipe(t, st, ctx, "qgone01")
	seed(t, st, ctx, "qgone01", qt1, map[string]any{"role": "web"})
	if _, err := st.Pool().Exec(ctx, `UPDATE nodes SET expired=now() WHERE certname='qgone01'`); err != nil {
		t.Fatal(err)
	}
	if res := run(t, e, ctx, `role=web`, false); slices.Contains(res.Nodes, "qgone01") {
		t.Fatal("expired node must be excluded by default")
	}
	if res := run(t, e, ctx, `role=web`, true); !slices.Contains(res.Nodes, "qgone01") {
		t.Fatal("include_inactive must re-include expired node")
	}
}

func mustParse(t *testing.T, s string) *Query {
	t.Helper()
	q, err := Parse(s)
	if err != nil {
		t.Fatal(err)
	}
	return q
}

func sameSet(got, want []string) bool {
	if len(got) != len(want) {
		return false
	}
	m := map[string]bool{}
	for _, g := range got {
		m[g] = true
	}
	for _, w := range want {
		if !m[w] {
			return false
		}
	}
	return true
}
