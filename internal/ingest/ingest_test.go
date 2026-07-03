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
	st, err := store.Open(ctx, dsn, 0)
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
	// A real push always carries a discovery report; an empty report is now a
	// degenerate reject (task 1.1), so the clean fixture is a non-empty ok report.
	clean = wire.DiscoveryStatus{Builtin: map[string]string{"os": wire.StatusOK}}
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

func TestApplyRejectsCollidingPaths(t *testing.T) {
	svc, st, ctx := testService(t)
	wipeNode(t, st, ctx, "collide.example")

	// {"a":{"b":1},"a.b":2} flattens to the SAME leaf path "a.b" twice. Rather
	// than let map order pick a winner (fabricating history churn), the push is
	// rejected as a bad request naming the colliding path (task 2.2).
	resp, status := svc.Apply(ctx, "collide.example",
		mkPush(`{"a":{"b":1},"a.b":2}`, t1, clean), t1)
	if status != http.StatusBadRequest || !strings.HasPrefix(resp.Reason, wire.ReasonBadRequest) || !strings.Contains(resp.Reason, "a.b") {
		t.Fatalf("colliding-path push must be rejected naming the path, got %d %+v", status, resp)
	}
	// Nothing was written for a node that never got a valid push.
	if _, ok, _ := st.PeekNode(ctx, "collide.example"); ok {
		t.Fatal("rejected colliding push must not create a node")
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

// After a watermark reset the node's LastProducerTS is nil, which LOOKS like
// first contact — but the node exists, so the first-contact clock bound must NOT
// apply (a far-past recovery push is rejected as stale by the overlap guard, not
// skewed), keeping the reset recovery path usable.
func TestResetNodeNotTreatedAsFirstContact(t *testing.T) {
	svc, st, ctx := testService(t)
	wipeNode(t, st, ctx, "reset.example")
	now := time.Now()

	if _, status := svc.Apply(ctx, "reset.example", mkPush(`{"os":{"name":"A"}}`, now.Add(-time.Minute), clean), now); status != http.StatusOK {
		t.Fatalf("establish node = %d", status)
	}
	if err := st.ResetProducerTS(ctx, "reset.example"); err != nil {
		t.Fatal(err)
	}
	// A far-past push after reset: nil watermark, but an existing node → stale
	// (overlaps the open interval), NOT skewed (first-contact bound must not fire).
	resp, status := svc.Apply(ctx, "reset.example", mkPush(`{"os":{"name":"B"}}`, now.Add(-365*24*time.Hour), clean), now)
	if status != http.StatusConflict || resp.Reason != wire.ReasonStale {
		t.Fatalf("post-reset far-past push = %d %+v, want 409 stale (not skewed)", status, resp)
	}
}

// A clean push that flattens to ONLY volatile leaves (zero durable) — e.g. an
// over-broad volatile policy — must NOT tombstone the node's durable history; it
// carries forward while still applying volatile (a932 fleet-wide-wipe guard).
func TestVolatileOnlyPushDoesNotTombstoneDurable(t *testing.T) {
	svc, st, ctx := testService(t)
	wipeNode(t, st, ctx, "volonly.example")

	// Seed a durable fact (os.name) plus a volatile one (uptime is volatile in cfg).
	if _, status := svc.Apply(ctx, "volonly.example", mkPush(`{"os":{"name":"Debian"},"uptime":1}`, t1, clean), t1); status != http.StatusOK {
		t.Fatalf("seed = %d", status)
	}
	id := nodeID(t, st, ctx, "volonly.example")
	if now, _ := st.Now(ctx, id); len(now) != 1 || now[0].Path != "os.name" {
		t.Fatalf("seed durable = %+v, want [os.name]", now)
	}

	// Next pass reports ONLY a volatile fact, clean discovery → zero durable leaves.
	resp, status := svc.Apply(ctx, "volonly.example", mkPush(`{"uptime":2}`, t2, clean), t2)
	if status != http.StatusOK || !resp.Applied {
		t.Fatalf("volatile-only push = %d %+v", status, resp)
	}
	if resp.Tombstoned != 0 {
		t.Fatalf("volatile-only clean push must not tombstone, got %d", resp.Tombstoned)
	}
	if now, _ := st.Now(ctx, id); len(now) != 1 || now[0].Path != "os.name" {
		t.Fatalf("durable history must survive a volatile-only push: %+v", now)
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
