package monitor

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/ncode/chronicle/internal/store"
)

func setup(t *testing.T) (*Monitor, *store.Store, context.Context) {
	t.Helper()
	dsn := os.Getenv("CHRONICLE_TEST_DB")
	if dsn == "" {
		t.Skip("set CHRONICLE_TEST_DB to run monitor integration tests")
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
	// Global scans -> isolate from other packages' rows.
	if _, err := st.Pool().Exec(ctx, `TRUNCATE nodes, fact_paths RESTART IDENTITY CASCADE`); err != nil {
		t.Fatal(err)
	}
	m := New(st, slog.New(slog.NewTextHandler(io.Discard, nil)))
	m.churnWindow = time.Hour
	m.churnThreshold = 3
	m.cardThreshold = 5
	return m, st, ctx
}

func applyAt(t *testing.T, st *store.Store, ctx context.Context, certname string, at time.Time, facts map[string]any) {
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
	if _, err := st.ApplySnapshot(ctx, certname, at, at, 0, leaves, json.RawMessage(`{}`), true); err != nil {
		t.Fatal(err)
	}
}

func TestChurnAlarm(t *testing.T) {
	m, st, ctx := setup(t)
	base := time.Now().Add(-10 * time.Minute)
	// One path flips value 4 times in the window -> 4 interval opens.
	for i := range 4 {
		applyAt(t, st, ctx, "mon.churn", base.Add(time.Duration(i)*time.Minute),
			map[string]any{"load.1m": json.Number(itoa(i))})
	}
	found, err := m.CheckChurn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !hasKey(found, "load.1m") {
		t.Fatalf("churn scan missed load.1m: %+v", found)
	}
}

func TestCardinalityAlarm(t *testing.T) {
	m, st, ctx := setup(t)
	facts := map[string]any{}
	for i := range 6 {
		facts["p"+itoa(i)] = "x"
	}
	applyAt(t, st, ctx, "mon.card", time.Now(), facts)

	found, err := m.CheckCardinality(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !hasKey(found, "mon.card") {
		t.Fatalf("cardinality scan missed mon.card: %+v", found)
	}
}

func hasKey(fs []Finding, key string) bool {
	for _, f := range fs {
		if f.Key == key {
			return true
		}
	}
	return false
}

func itoa(i int) string { return string(rune('0' + i)) }
