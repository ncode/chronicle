package lifecycle

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/ncode/chronicle/internal/config"
	"github.com/ncode/chronicle/internal/ingest"
	"github.com/ncode/chronicle/internal/store"
	"github.com/ncode/chronicle/internal/wire"
)

func setup(t *testing.T) (*store.Store, *ingest.Service, *Manager, context.Context) {
	t.Helper()
	dsn := os.Getenv("CHRONICLE_TEST_DB")
	if dsn == "" {
		t.Skip("set CHRONICLE_TEST_DB to run lifecycle integration tests")
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
	cfg := &config.ServerConfig{
		MaxSkew: config.Duration(5 * time.Minute), MaxSnapshotByte: 8 << 20,
		MaxLeafCount: 50000, MaxPathLen: 1024, MaxValueBytes: 256 << 10,
		RateLimitPerMin: 1_000_000, MaxConcurrent: 64, VolatilePaths: []string{"uptime"},
	}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	svc, err := ingest.New(st, cfg, log)
	if err != nil {
		t.Fatal(err)
	}
	return st, svc, NewManager(st, log, 24*time.Hour), ctx
}

func wipe(t *testing.T, st *store.Store, ctx context.Context, certname string) {
	t.Helper()
	if _, err := st.Pool().Exec(ctx, `DELETE FROM nodes WHERE certname=$1`, certname); err != nil {
		t.Fatal(err)
	}
}

func push(t *testing.T, svc *ingest.Service, ctx context.Context, certname, tree string, ts, received time.Time) (wire.PushResponse, int) {
	t.Helper()
	return svc.Apply(ctx, certname, &wire.Push{
		ProducerTimestamp: ts, Tree: json.RawMessage(tree),
		Discovery: wire.DiscoveryStatus{Builtin: map[string]string{"os": wire.StatusOK}}, // non-degenerate clean report
	}, received)
}

var (
	lt1 = time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC)
	lt2 = time.Date(2026, 1, 1, 11, 0, 0, 0, time.UTC)
)

func TestRebuildContinuesHistory(t *testing.T) {
	st, svc, _, ctx := setup(t)
	wipe(t, st, ctx, "rebuild.node")

	push(t, svc, ctx, "rebuild.node", `{"os":{"name":"Debian"},"machine_id":"AAAA"}`, lt1, lt1)
	id1, _, _ := st.NodeID(ctx, "rebuild.node")
	// Rebuild: same certname, machine_id flips.
	push(t, svc, ctx, "rebuild.node", `{"os":{"name":"Debian"},"machine_id":"BBBB"}`, lt2, lt2)
	id2, _, _ := st.NodeID(ctx, "rebuild.node")

	if id1 != id2 {
		t.Fatalf("same certname must be one node: %d != %d", id1, id2)
	}
	// machine_id has two intervals (closed AAAA + open BBBB); the change cluster.
	var intervals int
	if err := st.Pool().QueryRow(ctx, `
		SELECT count(*) FROM fact_history fh JOIN fact_paths fp USING (path_id)
		 WHERE fh.node_id=$1 AND fp.path_text='machine_id'`, id1).Scan(&intervals); err != nil {
		t.Fatal(err)
	}
	if intervals != 2 {
		t.Fatalf("rebuild should record a machine_id transition, got %d intervals", intervals)
	}
}

func TestExpiryRoundTrip(t *testing.T) {
	st, svc, mgr, ctx := setup(t)
	wipe(t, st, ctx, "expiry.node")

	// First contact 48h ago.
	old := time.Now().Add(-48 * time.Hour)
	push(t, svc, ctx, "expiry.node", `{"os":{"name":"Debian"}}`, old, old)

	n, err := mgr.Sweep(ctx) // ttl 24h => the 48h-silent node expires
	if err != nil {
		t.Fatal(err)
	}
	if n < 1 || !expired(t, st, ctx, "expiry.node") {
		t.Fatalf("node should be expired after sweep (n=%d)", n)
	}

	// A returning push un-expires it (reversible) and closes nothing.
	now := time.Now()
	if _, status := push(t, svc, ctx, "expiry.node", `{"os":{"name":"Debian"}}`, now, now); status != http.StatusOK {
		t.Fatalf("returning push status %d", status)
	}
	if expired(t, st, ctx, "expiry.node") {
		t.Fatal("returning push must clear expired")
	}
}

func TestDeactivationSealsAndRejects(t *testing.T) {
	st, svc, _, ctx := setup(t)
	wipe(t, st, ctx, "sunset.node")

	push(t, svc, ctx, "sunset.node", `{"os":{"name":"Debian"}}`, lt1, lt1)
	id, _, _ := st.NodeID(ctx, "sunset.node")

	if _, err := st.Deactivate(ctx, "sunset.node"); err != nil {
		t.Fatal(err)
	}
	// Timeline sealed: nothing current.
	now, _ := st.Now(ctx, id)
	if len(now) != 0 {
		t.Fatalf("deactivation must seal open intervals, still open: %+v", now)
	}
	// History retained (keep-forever).
	var rows int
	st.Pool().QueryRow(ctx, `SELECT count(*) FROM fact_history WHERE node_id=$1`, id).Scan(&rows)
	if rows == 0 {
		t.Fatal("deactivation must retain history, not delete it")
	}
	// Further pushes rejected as deactivated.
	resp, status := push(t, svc, ctx, "sunset.node", `{"os":{"name":"Ubuntu"}}`, lt2, lt2)
	if status != http.StatusForbidden || resp.Reason != wire.ReasonDeactivated {
		t.Fatalf("post-deactivation push = %d %+v, want 403 deactivated", status, resp)
	}
}

func expired(t *testing.T, st *store.Store, ctx context.Context, certname string) bool {
	t.Helper()
	var e *time.Time
	if err := st.Pool().QueryRow(ctx, `SELECT expired FROM nodes WHERE certname=$1`, certname).Scan(&e); err != nil {
		t.Fatal(err)
	}
	return e != nil
}
