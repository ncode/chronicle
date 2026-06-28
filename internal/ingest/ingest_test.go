package ingest

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ncode/chronicle/internal/config"
	"github.com/ncode/chronicle/internal/store"
	"github.com/ncode/chronicle/internal/wire"
)

func testService(t *testing.T) (*Service, *store.Store, context.Context) {
	t.Helper()
	dsn := os.Getenv("CHRONICLE_TEST_DB")
	if dsn == "" {
		t.Skip("set CHRONICLE_TEST_DB to run ingest integration tests")
	}
	ctx := context.Background()
	st, err := store.Open(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(st.Close)
	cfg := &config.ServerConfig{
		MaxSkew:         config.Duration(5 * time.Minute),
		MaxSnapshotByte: 8 << 20,
		MaxLeafCount:    50000,
		MaxPathLen:      1024,
		MaxValueBytes:   256 << 10,
		RateLimitPerMin: 1_000_000, // effectively off for these tests
		MaxConcurrent:   64,
		VolatilePaths:   []string{"uptime", "memory.system.*"},
	}
	svc, err := New(st, cfg, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	return svc, st, ctx
}

func wipeNode(t *testing.T, st *store.Store, ctx context.Context, certname string) {
	t.Helper()
	if _, err := st.Pool().Exec(ctx, `DELETE FROM nodes WHERE certname=$1`, certname); err != nil {
		t.Fatal(err)
	}
}

func nodeID(t *testing.T, st *store.Store, ctx context.Context, certname string) int64 {
	t.Helper()
	var id int64
	if err := st.Pool().QueryRow(ctx, `SELECT node_id FROM nodes WHERE certname=$1`, certname).Scan(&id); err != nil {
		t.Fatal(err)
	}
	return id
}

func mkPush(tree string, ts time.Time, status wire.DiscoveryStatus) *wire.Push {
	return &wire.Push{ProducerTimestamp: ts, Tree: json.RawMessage(tree), Discovery: status}
}

var (
	clean = wire.DiscoveryStatus{}
	dirty = wire.DiscoveryStatus{External: map[string]string{"/etc/facts.d/rpm.sh": wire.StatusError}}

	t1 = time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC)
	t2 = time.Date(2026, 1, 1, 11, 0, 0, 0, time.UTC)
	t3 = time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
)

func TestApplyClassifiesDurableAndVolatile(t *testing.T) {
	svc, st, ctx := testService(t)
	wipeNode(t, st, ctx, "classify.example")

	resp, status := svc.Apply(ctx, "classify.example",
		mkPush(`{"os":{"name":"Debian"},"uptime":12345}`, t1, clean), t1)
	if status != http.StatusOK || !resp.Applied {
		t.Fatalf("apply = %d %+v", status, resp)
	}
	if resp.Opened != 1 {
		t.Fatalf("expected 1 durable open (os.name), got %+v", resp)
	}

	id := nodeID(t, st, ctx, "classify.example")
	now, err := st.Now(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if len(now) != 1 || now[0].Path != "os.name" {
		t.Fatalf("durable now = %+v (uptime must not be historized)", now)
	}
	var vol string
	if err := st.Pool().QueryRow(ctx, `SELECT volatile::text FROM node_volatile WHERE node_id=$1`, id).Scan(&vol); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(vol, "uptime") {
		t.Fatalf("volatile blob missing uptime: %s", vol)
	}
}

func TestApplyStaleRejected(t *testing.T) {
	svc, st, ctx := testService(t)
	wipeNode(t, st, ctx, "stale.example")

	if _, status := svc.Apply(ctx, "stale.example", mkPush(`{"os":{"name":"A"}}`, t2, clean), t2); status != http.StatusOK {
		t.Fatalf("first apply status %d", status)
	}
	// Older producer_timestamp -> stale, no watermark advance, no diff.
	resp, status := svc.Apply(ctx, "stale.example", mkPush(`{"os":{"name":"B"}}`, t1, clean), t1)
	if status != http.StatusConflict || resp.Reason != wire.ReasonStale {
		t.Fatalf("stale apply = %d %+v", status, resp)
	}
	id := nodeID(t, st, ctx, "stale.example")
	now, _ := st.Now(ctx, id)
	if len(now) != 1 || string(now[0].Value) != `"A"` {
		t.Fatalf("stale push must not change state: %+v", now)
	}
}

func TestApplySkewRejected(t *testing.T) {
	svc, st, ctx := testService(t)
	wipeNode(t, st, ctx, "skew.example")
	// producer_timestamp 10 min ahead of received, max_skew is 5 min.
	received := t1
	resp, status := svc.Apply(ctx, "skew.example",
		mkPush(`{"os":{"name":"A"}}`, t1.Add(10*time.Minute), clean), received)
	if status != http.StatusConflict || resp.Reason != wire.ReasonSkewed {
		t.Fatalf("skew apply = %d %+v", status, resp)
	}
}

func TestApplyIdempotentReapply(t *testing.T) {
	svc, st, ctx := testService(t)
	wipeNode(t, st, ctx, "idem.example")

	if _, status := svc.Apply(ctx, "idem.example", mkPush(`{"os":{"name":"A"}}`, t2, clean), t2); status != http.StatusOK {
		t.Fatalf("first apply status %d", status)
	}
	// Same producer_timestamp re-applied -> rejected as stale (equal), no-op.
	resp, status := svc.Apply(ctx, "idem.example", mkPush(`{"os":{"name":"A"}}`, t2, clean), t2)
	if status != http.StatusConflict || resp.Reason != wire.ReasonStale {
		t.Fatalf("re-apply = %d %+v", status, resp)
	}
	id := nodeID(t, st, ctx, "idem.example")
	now, _ := st.Now(ctx, id)
	if len(now) != 1 {
		t.Fatalf("re-apply must not duplicate intervals: %+v", now)
	}
}

func TestApplyAbsenceTombstoneVsCarryForward(t *testing.T) {
	svc, st, ctx := testService(t)
	wipeNode(t, st, ctx, "rpm.example")
	id := func() int64 { return nodeID(t, st, ctx, "rpm.example") }

	// t1: node reports rpm_packages.bash + os.name.
	svc.Apply(ctx, "rpm.example", mkPush(`{"os":{"name":"RHEL"},"rpm_packages":{"bash":"5.1"}}`, t1, clean), t1)

	// t2: reinstalled to Debian, rpm script gone, discovery CLEAN -> tombstone rpm.
	resp, status := svc.Apply(ctx, "rpm.example", mkPush(`{"os":{"name":"Debian"}}`, t2, clean), t2)
	if status != http.StatusOK || resp.Tombstoned != 1 {
		t.Fatalf("clean-absence apply = %d %+v (want 1 tombstone)", status, resp)
	}
	if now, _ := st.Now(ctx, id()); len(now) != 1 || now[0].Path != "os.name" {
		t.Fatalf("rpm should be tombstoned: %+v", now)
	}

	// Re-add rpm at t3, then t4 with rpm absent but discovery DIRTY -> carry forward.
	svc.Apply(ctx, "rpm.example", mkPush(`{"os":{"name":"Debian"},"rpm_packages":{"bash":"5.2"}}`, t3, clean), t3)
	t4 := t3.Add(time.Hour)
	resp, status = svc.Apply(ctx, "rpm.example", mkPush(`{"os":{"name":"Debian"}}`, t4, dirty), t4)
	if status != http.StatusOK || resp.Tombstoned != 0 {
		t.Fatalf("dirty-absence apply = %d %+v (want 0 tombstones)", status, resp)
	}
	if now, _ := st.Now(ctx, id()); len(now) != 2 {
		t.Fatalf("rpm should be carried forward on dirty discovery: %+v", now)
	}
}

func TestApplyConcurrentSerialized(t *testing.T) {
	svc, st, ctx := testService(t)
	wipeNode(t, st, ctx, "concurrent.example")

	const n = 12
	var wg sync.WaitGroup
	internal := make([]int32, n) // count of internal errors (should stay 0)
	for i := range n {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			ts := t1.Add(time.Duration(i) * time.Minute)
			tree := `{"os":{"name":"v` + itoa(i) + `"}}`
			_, status := svc.Apply(ctx, "concurrent.example", mkPush(tree, ts, clean), ts)
			// Only OK (applied) or 409 (stale, lost the race) are valid outcomes;
			// 500 would mean a serialization bug (lost update / valid_to<valid_from).
			if status == http.StatusInternalServerError {
				atomic.AddInt32(&internal[i], 1)
			}
		}(i)
	}
	wg.Wait()

	for i := range internal {
		if internal[i] != 0 {
			t.Fatalf("apply %d hit an internal error under concurrency", i)
		}
	}
	id := nodeID(t, st, ctx, "concurrent.example")
	now, err := st.Now(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	// Exactly one current value for os.name, and it is the highest producer_ts.
	if len(now) != 1 || now[0].Path != "os.name" {
		t.Fatalf("expected single open os.name interval, got %+v", now)
	}
	if string(now[0].Value) != `"v`+itoa(n-1)+`"` {
		t.Fatalf("highest producer_ts must win: got %s", now[0].Value)
	}
}

func itoa(i int) string { return string(rune('0'+i/10)) + string(rune('0'+i%10)) }

func TestApplyDedupCollidingPaths(t *testing.T) {
	svc, st, ctx := testService(t)
	wipeNode(t, st, ctx, "collide.example")

	// {"a":{"b":1},"a.b":2} flattens to the SAME leaf path "a.b" twice -> one
	// path_id. Without dedup this trips the unique/PK/CHECK constraints and 500s.
	resp, status := svc.Apply(ctx, "collide.example",
		mkPush(`{"a":{"b":1},"a.b":2}`, t1, clean), t1)
	if status != http.StatusOK || !resp.Applied {
		t.Fatalf("colliding-path push must apply cleanly, got %d %+v", status, resp)
	}
	id := nodeID(t, st, ctx, "collide.example")
	now, _ := st.Now(ctx, id)
	if len(now) != 1 || now[0].Path != "a.b" {
		t.Fatalf("dedup should leave exactly one a.b interval: %+v", now)
	}
}

func TestApplyDegenerateGuardAfterReset(t *testing.T) {
	svc, st, ctx := testService(t)
	wipeNode(t, st, ctx, "degenerate.example")

	// Apply at t2, then clear the watermark (operator recovery).
	if _, status := svc.Apply(ctx, "degenerate.example", mkPush(`{"os":{"name":"A"}}`, t2, clean), t2); status != http.StatusOK {
		t.Fatal("first apply failed")
	}
	if err := st.ResetProducerTS(ctx, "degenerate.example"); err != nil {
		t.Fatal(err)
	}
	// A push older than the node's open interval would close it at valid_to <=
	// valid_from. The degenerate guard must reject it as stale (409), not 500.
	resp, status := svc.Apply(ctx, "degenerate.example", mkPush(`{"os":{"name":"B"}}`, t1, clean), t1)
	if status != http.StatusConflict || resp.Reason != wire.ReasonStale {
		t.Fatalf("degenerate close must be a clean stale reject, got %d %+v", status, resp)
	}
	id := nodeID(t, st, ctx, "degenerate.example")
	now, _ := st.Now(ctx, id)
	if len(now) != 1 || string(now[0].Value) != `"A"` {
		t.Fatalf("rejected push must not change state: %+v", now)
	}
}

func TestApplyOversizedLeafCount(t *testing.T) {
	svc, st, ctx := testService(t)
	svc.cfg.MaxLeafCount = 1 // tighten for this test
	wipeNode(t, st, ctx, "big.example")
	resp, status := svc.Apply(ctx, "big.example",
		mkPush(`{"a":1,"b":2,"c":3}`, t1, clean), t1)
	if status != http.StatusRequestEntityTooLarge || !strings.HasPrefix(resp.Reason, wire.ReasonOversized) {
		t.Fatalf("oversized apply = %d %+v", status, resp)
	}
}

// Structural guarantee: the wire payload has no identity field, so a body cannot
// assert a certname (fact-ingest CN-from-chain). An extra body field is ignored.
func TestPushBodyCannotCarryCertname(t *testing.T) {
	body := `{"producer_timestamp":"2026-01-01T10:00:00Z","tree":{},"certname":"db99.example.com"}`
	p, err := decodePush(strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	// The decoded push exposes no certname; identity must come from the chain.
	b, _ := json.Marshal(p)
	if strings.Contains(string(b), "db99") {
		t.Fatalf("push must not retain a body-asserted certname: %s", b)
	}
}
