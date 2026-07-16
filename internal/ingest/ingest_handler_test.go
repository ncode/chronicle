package ingest

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/ncode/chronicle/internal/metrics"
	"github.com/ncode/chronicle/internal/store"
	"github.com/ncode/chronicle/internal/wire"
)

// postPush drives handlePush directly with a request carrying a verified mTLS
// chain whose leaf CN is certname (certnameFromChain reads only the CN).
func postPush(svc *Service, certname string, body []byte) *httptest.ResponseRecorder {
	r := httptest.NewRequest("POST", "/v1/push", bytes.NewReader(body))
	if certname != "" {
		r.TLS = &tls.ConnectionState{VerifiedChains: [][]*x509.Certificate{{
			{Subject: pkix.Name{CommonName: certname}},
		}}}
	}
	w := httptest.NewRecorder()
	svc.handlePush(w, r)
	return w
}

func mustJSON(t *testing.T, p *wire.Push) []byte {
	t.Helper()
	b, err := json.Marshal(p)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func lastSeen(t *testing.T, st *store.Store, ctx context.Context, certname string) time.Time {
	t.Helper()
	var ls time.Time
	if err := st.Pool().QueryRow(ctx, `SELECT last_seen FROM nodes WHERE certname=$1`, certname).Scan(&ls); err != nil {
		t.Fatal(err)
	}
	return ls
}

// Every pre-apply guard path returns the right status and increments the reject
// metric with its reason (tasks 5.1, 7.1).
func TestHandlerRejectPathsAndMetrics(t *testing.T) {
	svc, st, ctx := testService(t)
	m := metrics.New()
	svc.SetMetrics(m)
	wipeNode(t, st, ctx, "guard.node")

	good := mustJSON(t, mkPush(`{"os":{"name":"Debian"}}`, t3, clean))

	// no client cert -> 403.
	if w := postPush(svc, "", good); w.Code != 403 {
		t.Fatalf("no-cert = %d, want 403", w.Code)
	}
	// malformed JSON -> 400.
	if w := postPush(svc, "guard.node", []byte(`{not json`)); w.Code != 400 {
		t.Fatalf("bad-json = %d, want 400", w.Code)
	}
	// oversized body -> 413 (tighten the cap for this request).
	svc.cfg.MaxSnapshotByte = 8
	if w := postPush(svc, "guard.node", good); w.Code != 413 {
		t.Fatalf("oversized-body = %d, want 413", w.Code)
	}
	svc.cfg.MaxSnapshotByte = 8 << 20

	for _, reason := range []string{wire.ReasonNoClientCert, wire.ReasonBadRequest, wire.ReasonOversized} {
		if got := testutil.ToFloat64(m.Rejects.WithLabelValues(reason)); got < 1 {
			t.Fatalf("reject reason %q not counted (got %v)", reason, got)
		}
	}
}

// A degenerate empty-tree push (an operator smoke test) is rejected and does NOT
// tombstone the node's durable history or advance its watermark (task 1.1).
func TestEmptyTreePushDoesNotTombstone(t *testing.T) {
	svc, st, ctx := testService(t)
	wipeNode(t, st, ctx, "smoke.node")

	// The HTTP handler anchors received=now, so use near-now producer timestamps
	// (a real agent stamps ≈now; the first-contact past bound rejects old ones).
	seedTS := time.Now().Add(-time.Minute)
	if w := postPush(svc, "smoke.node", mustJSON(t, mkPush(`{"os":{"name":"Debian"},"role":"web"}`, seedTS, clean))); w.Code != 200 {
		t.Fatalf("seed push = %d", w.Code)
	}
	id := nodeID(t, st, ctx, "smoke.node")
	before, _ := st.Now(ctx, id)
	if len(before) != 2 {
		t.Fatalf("seed should open 2 durable facts, got %d", len(before))
	}
	var wmBefore time.Time
	if err := st.Pool().QueryRow(ctx, `SELECT last_producer_ts FROM nodes WHERE node_id=$1`, id).Scan(&wmBefore); err != nil {
		t.Fatal(err)
	}

	// Operator curls only a producer_timestamp: empty tree, with a report.
	empty := &wire.Push{ProducerTimestamp: time.Now(), Tree: json.RawMessage(`{}`), Discovery: clean}
	if w := postPush(svc, "smoke.node", mustJSON(t, empty)); w.Code != 400 {
		t.Fatalf("empty-tree push = %d, want 400 (not a discovery-clean tombstone)", w.Code)
	}
	after, _ := st.Now(ctx, id)
	if len(after) != 2 {
		t.Fatalf("empty-tree push must not tombstone: now has %d facts, want 2", len(after))
	}
	// Watermark must not have advanced on the rejected push.
	var wmAfter time.Time
	if err := st.Pool().QueryRow(ctx, `SELECT last_producer_ts FROM nodes WHERE node_id=$1`, id).Scan(&wmAfter); err != nil {
		t.Fatal(err)
	}
	if !wmAfter.Equal(wmBefore) {
		t.Fatalf("watermark advanced from %s to %s on a rejected push", wmBefore, wmAfter)
	}
}

func TestInvalidDiscoveryStatusDoesNotMutateHistory(t *testing.T) {
	svc, st, ctx := testService(t)
	const certname = "invalid-discovery.node"
	wipeNode(t, st, ctx, certname)

	seedTS := time.Now().Add(-time.Minute)
	seed := mkPush(`{"os":{"name":"Debian"},"role":"web"}`, seedTS, clean)
	if w := postPush(svc, certname, mustJSON(t, seed)); w.Code != 200 {
		t.Fatalf("seed push = %d, want 200", w.Code)
	}
	id := nodeID(t, st, ctx, certname)
	var watermarkBefore time.Time
	if err := st.Pool().QueryRow(ctx, `SELECT last_producer_ts FROM nodes WHERE node_id=$1`, id).Scan(&watermarkBefore); err != nil {
		t.Fatal(err)
	}
	oldContact := time.Now().Add(-30 * 24 * time.Hour)
	if _, err := st.Pool().Exec(ctx, `UPDATE nodes SET last_seen=$2 WHERE node_id=$1`, id, oldContact); err != nil {
		t.Fatal(err)
	}

	malformed := mkPush(
		`{"os":{"name":"Debian"}}`,
		time.Now(),
		wire.DiscoveryStatus{Builtin: map[string]string{"os": "unknown"}},
	)
	if w := postPush(svc, certname, mustJSON(t, malformed)); w.Code != http.StatusBadRequest {
		t.Fatalf("malformed discovery push = %d, want 400", w.Code)
	}

	now, err := st.Now(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if len(now) != 2 {
		t.Fatalf("malformed discovery push changed open facts: %+v", now)
	}
	var total, closed int
	if err := st.Pool().QueryRow(ctx, `
		SELECT count(*), count(*) FILTER (WHERE valid_to <> 'infinity')
		FROM fact_history WHERE node_id=$1`, id).Scan(&total, &closed); err != nil {
		t.Fatal(err)
	}
	if total != 2 || closed != 0 {
		t.Fatalf("history = %d intervals, %d closed; want 2 open intervals", total, closed)
	}
	var watermarkAfter time.Time
	if err := st.Pool().QueryRow(ctx, `SELECT last_producer_ts FROM nodes WHERE node_id=$1`, id).Scan(&watermarkAfter); err != nil {
		t.Fatal(err)
	}
	if !watermarkAfter.Equal(watermarkBefore) {
		t.Fatalf("watermark advanced from %s to %s on malformed discovery", watermarkBefore, watermarkAfter)
	}
	if contact := lastSeen(t, st, ctx, certname); !contact.After(oldContact) {
		t.Fatalf("rejected contact did not advance last_seen beyond %s: %s", oldContact, contact)
	}
}

// A rejected-but-admitted push still advances last_seen, so a node stuck on
// stale rejects is never falsely swept as Expired (task 3.1).
func TestContactOnRejectPreventsExpiry(t *testing.T) {
	svc, st, ctx := testService(t)
	m := metrics.New()
	svc.SetMetrics(m)
	wipeNode(t, st, ctx, "stuck.node")

	// Apply once at a near-now producer_ts so a later lower-ts push is stale.
	firstTS := time.Now().Add(-time.Minute)
	if w := postPush(svc, "stuck.node", mustJSON(t, mkPush(`{"os":{"name":"A"}}`, firstTS, clean))); w.Code != 200 {
		t.Fatalf("first push = %d", w.Code)
	}
	// Simulate a long silence.
	old := time.Now().Add(-30 * 24 * time.Hour)
	if _, err := st.Pool().Exec(ctx, `UPDATE nodes SET last_seen=$2 WHERE certname=$1`, "stuck.node", old); err != nil {
		t.Fatal(err)
	}

	// A stale push (older ts than the watermark) is rejected but registers contact.
	if w := postPush(svc, "stuck.node", mustJSON(t, mkPush(`{"os":{"name":"B"}}`, firstTS.Add(-time.Second), clean))); w.Code != 409 {
		t.Fatalf("stale push = %d, want 409", w.Code)
	}
	if ls := lastSeen(t, st, ctx, "stuck.node"); ls.Before(time.Now().Add(-time.Minute)) {
		t.Fatalf("rejected push must advance last_seen, still %s", ls)
	}
	// The expiry sweep must not expire a node that just made contact.
	if _, err := st.ExpireStale(ctx, 7*24*time.Hour); err != nil {
		t.Fatal(err)
	}
	var expired *time.Time
	if err := st.Pool().QueryRow(ctx, `SELECT expired FROM nodes WHERE certname=$1`, "stuck.node").Scan(&expired); err != nil {
		t.Fatal(err)
	}
	if expired != nil {
		t.Fatal("node stuck on rejects must not be expired after registering contact")
	}
	if got := testutil.ToFloat64(m.Rejects.WithLabelValues(wire.ReasonStale)); got < 1 {
		t.Fatal("stale reject must be counted")
	}
}

// On first contact (no watermark) a snapshot backdated beyond received - max_skew
// is rejected as skewed and writes no node row; a normal in-window first contact
// applies (task 2.1).
func TestFirstContactPastBound(t *testing.T) {
	svc, st, ctx := testService(t)
	wipeNode(t, st, ctx, "backdate.node")

	received := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	// One year in the past on first contact -> skewed reject, no node persisted.
	farPast := received.Add(-365 * 24 * time.Hour)
	resp, status := svc.Apply(ctx, "backdate.node", mkPush(`{"os":{"name":"A"}}`, farPast, clean), received)
	if status != 409 || resp.Reason != wire.ReasonSkewed {
		t.Fatalf("far-past first contact = %d %+v, want 409 skewed", status, resp)
	}
	if _, ok, _ := st.PeekNode(ctx, "backdate.node"); ok {
		t.Fatal("a rejected first-contact push must persist no node row")
	}

	// A first contact within skew applies normally.
	inWindow := received.Add(-time.Minute)
	if _, status := svc.Apply(ctx, "backdate.node", mkPush(`{"os":{"name":"A"}}`, inWindow, clean), received); status != 200 {
		t.Fatalf("in-window first contact = %d, want 200", status)
	}

	// A later, legitimately delayed push for the now-established node is NOT
	// subject to the first-contact bound (only the watermark bounds it).
	delayed := received.Add(time.Hour)
	longAfterCollect := delayed.Add(time.Hour) // producer_ts is an hour before receipt
	if _, status := svc.Apply(ctx, "backdate.node", mkPush(`{"os":{"name":"B"}}`, delayed, clean), longAfterCollect); status != 200 {
		t.Fatalf("delayed non-first-contact push = %d, want 200 (bound must not apply)", status)
	}
}

// Rate-limited excess is load-shedding: 429 without a store write (exempt from
// contact), so it cannot amplify saturation into writes (tasks 3.1, 5.1).
func TestRateLimitExemptFromContact(t *testing.T) {
	svc, st, ctx := testService(t)
	m := metrics.New()
	svc.SetMetrics(m)
	svc.cfg.RateLimitPerMin = 1 // burst of 1
	wipeNode(t, st, ctx, "rl.node")

	if w := postPush(svc, "rl.node", mustJSON(t, mkPush(`{"os":{"name":"A"}}`, time.Now().Add(-time.Minute), clean))); w.Code != 200 {
		t.Fatalf("first push = %d, want 200", w.Code)
	}
	w := postPush(svc, "rl.node", mustJSON(t, mkPush(`{"os":{"name":"A"}}`, time.Now(), clean)))
	if w.Code != 429 {
		t.Fatalf("second push (rate limited) = %d, want 429", w.Code)
	}
	if got := w.Header().Get("Retry-After"); got != "10" {
		t.Fatalf("rate-limit Retry-After = %q, want 10", got)
	}
	if got := testutil.ToFloat64(m.Rejects.WithLabelValues(wire.ReasonRateLimited)); got < 1 {
		t.Fatal("rate-limit reject must be counted")
	}
}
