// Package e2e exercises the whole pipeline end to end: a real facts snapshot
// pushed over facts-ca-style mTLS into the ingest server, landing in the
// temporal store, and read back via the query engine (now / state-at-T / diff).
package e2e

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ncode/facts"

	"github.com/ncode/chronicle/internal/classify"
	"github.com/ncode/chronicle/internal/config"
	"github.com/ncode/chronicle/internal/ingest"
	"github.com/ncode/chronicle/internal/query"
	"github.com/ncode/chronicle/internal/store"
	"github.com/ncode/chronicle/internal/testca"
	"github.com/ncode/chronicle/internal/wire"
)

func testServerConfig() *config.ServerConfig {
	return &config.ServerConfig{
		MaxSkew: config.Duration(5 * time.Minute), MaxSnapshotByte: 16 << 20,
		MaxLeafCount: 200000, MaxPathLen: 2048, MaxValueBytes: 1 << 20,
		RateLimitPerMin: 1_000_000, MaxConcurrent: 64, VolatilePaths: []string{"uptime", "memory.system.*", "load*"},
	}
}

func testPolicyHolder(t *testing.T, patterns []string) *atomic.Pointer[classify.Policy] {
	t.Helper()
	cl, err := classify.New(patterns)
	if err != nil {
		t.Fatal(err)
	}
	var holder atomic.Pointer[classify.Policy]
	holder.Store(cl)
	return &holder
}

func newStore(t *testing.T) (*store.Store, context.Context) {
	t.Helper()
	dsn := os.Getenv("CHRONICLE_TEST_DB")
	if dsn == "" {
		t.Skip("set CHRONICLE_TEST_DB to run e2e tests")
	}
	ctx := context.Background()
	st, err := store.Open(ctx, dsn, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := st.Pool().Exec(ctx, `TRUNCATE nodes, fact_paths RESTART IDENTITY CASCADE`); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(st.Close)
	return st, ctx
}

// startIngest stands up the ingest mux behind a real mTLS server using testca.
func startIngest(t *testing.T, st *store.Store, ca *testca.CA) *httptest.Server {
	t.Helper()
	cfg := testServerConfig()
	dir := t.TempDir()
	cfg.TLS.CACert = ca.WriteCA(t, dir)
	server := ca.IssueServer(t, "chronicle", "127.0.0.1")
	cfg.TLS.ServerCert, cfg.TLS.ServerKey = server.Write(t, dir, "server")

	svc, err := ingest.New(st, cfg, slog.New(slog.NewTextHandler(io.Discard, nil)), testPolicyHolder(t, cfg.VolatilePaths))
	if err != nil {
		t.Fatal(err)
	}
	tc, _, err := ingest.ServerTLSConfig(cfg)
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewUnstartedServer(svc.Handler())
	srv.TLS = tc
	srv.StartTLS()
	t.Cleanup(srv.Close)
	return srv
}

func mtlsClient(ca *testca.CA, clientCert tls.Certificate) *http.Client {
	return &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{
		RootCAs:      ca.Pool(),
		ServerName:   "127.0.0.1",
		Certificates: []tls.Certificate{clientCert},
	}}}
}

func pushSnapshot(t *testing.T, client *http.Client, url string, p wire.Push) (wire.PushResponse, int) {
	t.Helper()
	body, _ := json.Marshal(p)
	resp, err := client.Post(url+"/v1/push", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("push: %v", err)
	}
	defer resp.Body.Close()
	var pr wire.PushResponse
	_ = json.NewDecoder(resp.Body).Decode(&pr)
	return pr, resp.StatusCode
}

// TestRealFactsPushed proves the actual facts library collects a snapshot that
// the ingest server accepts over mTLS and stores.
func TestRealFactsPushed(t *testing.T) {
	st, ctx := newStore(t)
	ca := testca.New(t)
	srv := startIngest(t, st, ca)
	client := mtlsClient(ca, ca.IssueClient(t, "real.node").TLS)

	eng, err := facts.New()
	if err != nil {
		t.Fatal(err)
	}
	snap, _ := eng.Discover(ctx) // partial failures are fine
	if snap == nil {
		t.Fatal("no snapshot from facts.Discover")
	}
	tree, _ := json.Marshal(snap.Tree())

	pr, status := pushSnapshot(t, client, srv.URL,
		wire.Push{ProducerTimestamp: time.Now(), Tree: tree, Discovery: okDisc})
	if status != http.StatusOK || !pr.Applied {
		t.Fatalf("real-facts push = %d %+v", status, pr)
	}
	if pr.Opened == 0 {
		t.Fatal("expected the first real snapshot to open durable intervals")
	}
}

// TestPipelineNowAtTDiff drives a deterministic snapshot through mTLS ingest and
// reads it back via the query engine: now, state-at-T, and node_diff.
func TestPipelineNowAtTDiff(t *testing.T) {
	st, ctx := newStore(t)
	ca := testca.New(t)
	srv := startIngest(t, st, ca)
	client := mtlsClient(ca, ca.IssueClient(t, "pipe.node").TLS)

	eng := &queryEngine{store: st}
	// The mTLS ingest handler anchors received=now, and first contact rejects a
	// producer_timestamp older than max_skew (5m). Use near-now points a few
	// minutes apart so the at-T query still has a genuine window between them.
	base := time.Now().Truncate(time.Second)
	t1 := base.Add(-4 * time.Minute)
	t2 := base.Add(-1 * time.Minute)

	if pr, status := pushSnapshot(t, client, srv.URL, wire.Push{
		ProducerTimestamp: t1, Tree: json.RawMessage(`{"role":"web","os":{"name":"Debian"}}`), Discovery: okDisc,
	}); status != http.StatusOK {
		t.Fatalf("t1 push = %d %+v", status, pr)
	}
	if got := eng.filter(t, ctx, `role=web os.name=Debian`); !equalSet(got, []string{"pipe.node"}) {
		t.Fatalf("now filter = %v", got)
	}

	if pr, status := pushSnapshot(t, client, srv.URL, wire.Push{
		ProducerTimestamp: t2, Tree: json.RawMessage(`{"role":"web","os":{"name":"Ubuntu"}}`), Discovery: okDisc,
	}); status != http.StatusOK {
		t.Fatalf("t2 push = %d %+v", status, pr)
	}

	// now: Ubuntu. at-T(between t1,t2): Debian.
	if got := eng.filter(t, ctx, `os.name=Ubuntu`); !equalSet(got, []string{"pipe.node"}) {
		t.Fatalf("now Ubuntu = %v", got)
	}
	mid := t1.Add(90 * time.Second).Format(time.RFC3339) // between t1 and t2
	if got := eng.filter(t, ctx, `os.name=Debian at `+mid); !equalSet(got, []string{"pipe.node"}) {
		t.Fatalf("at-T Debian = %v", got)
	}

	// node_diff over the window shows os.name closed (Debian) + opened (Ubuntu).
	id, _, _ := st.NodeID(ctx, "pipe.node")
	diff, err := st.Diff(ctx, id, t1.Add(time.Minute), t2.Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	var opened, closed int
	for _, d := range diff {
		if d.Path == "os.name" && d.OpenedInWindow {
			opened++
		}
		if d.Path == "os.name" && d.ClosedInWindow {
			closed++
		}
	}
	if opened != 1 || closed != 1 {
		t.Fatalf("node_diff os.name opened=%d closed=%d", opened, closed)
	}
}

// TestScaleSmoke pushes a fleet's cold-start then a correlated change-burst,
// checking it stays correct and bounded under concurrency. It calls Apply
// directly (the DB write path is what scales, not the TLS handshake).
func TestScaleSmoke(t *testing.T) {
	if testing.Short() {
		t.Skip("scale smoke skipped in -short")
	}
	st, ctx := newStore(t)
	cfg := testServerConfig()
	svc, err := ingest.New(st, cfg, slog.New(slog.NewTextHandler(io.Discard, nil)), testPolicyHolder(t, cfg.VolatilePaths))
	if err != nil {
		t.Fatal(err)
	}

	const fleet = 200
	base := time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)

	// Cold start: every node's first snapshot is an all-INSERT.
	coldStart := time.Now()
	runFleet(t, ctx, svc, fleet, func(i int) (string, wire.Push) {
		return certname(i), wire.Push{
			ProducerTimestamp: base,
			Tree:              json.RawMessage(`{"role":"web","os":{"name":"Debian"},"kernel":"6.1"}`),
			Discovery:         okDisc,
		}
	})
	t.Logf("cold-start %d nodes in %s", fleet, time.Since(coldStart))

	// Correlated change-burst: a fleet-wide patch flips one durable fact at once.
	burst := time.Now()
	runFleet(t, ctx, svc, fleet, func(i int) (string, wire.Push) {
		return certname(i), wire.Push{
			ProducerTimestamp: base.Add(time.Hour),
			Tree:              json.RawMessage(`{"role":"web","os":{"name":"Debian"},"kernel":"6.2"}`),
			Discovery:         okDisc,
		}
	})
	t.Logf("change-burst %d nodes in %s", fleet, time.Since(burst))

	// Correctness: every node now has kernel=6.2 currently.
	var cur int
	if err := st.Pool().QueryRow(ctx, `
		SELECT count(*) FROM current_facts cf
		WHERE cf.path_text='kernel' AND cf.value = '"6.2"'::jsonb`).Scan(&cur); err != nil {
		t.Fatal(err)
	}
	if cur != fleet {
		t.Fatalf("expected %d nodes at kernel 6.2, got %d", fleet, cur)
	}
}

func runFleet(t *testing.T, ctx context.Context, svc *ingest.Service, n int, mk func(i int) (string, wire.Push)) {
	t.Helper()
	var wg sync.WaitGroup
	var failures atomic.Int32
	sem := make(chan struct{}, 32) // bounded concurrency
	for i := range n {
		wg.Add(1)
		sem <- struct{}{}
		go func(i int) {
			defer wg.Done()
			defer func() { <-sem }()
			cn, p := mk(i)
			if _, status := svc.Apply(ctx, cn, &p, p.ProducerTimestamp.Add(time.Minute)); status != http.StatusOK {
				failures.Add(1)
			}
		}(i)
	}
	wg.Wait()
	if f := failures.Load(); f != 0 {
		t.Fatalf("%d/%d fleet pushes failed", f, n)
	}
}

func certname(i int) string { return "fleet-" + strconv.Itoa(i) }

// okDisc is a non-empty clean discovery report; a real agent always sends one,
// and an empty report is now a degenerate reject (task 1.1).
var okDisc = wire.DiscoveryStatus{Builtin: map[string]string{"os": wire.StatusOK}}

func equalSet(got, want []string) bool {
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

// queryEngine is a thin test wrapper to run filter queries via the query engine.
type queryEngine struct{ store *store.Store }

func (e *queryEngine) filter(t *testing.T, ctx context.Context, dsl string) []string {
	t.Helper()
	qe := query.NewEngine(e.store, testPolicyHolder(t, testServerConfig().VolatilePaths))
	q, err := query.Parse(dsl)
	if err != nil {
		t.Fatalf("parse %q: %v", dsl, err)
	}
	res, err := qe.Run(ctx, q, false)
	if err != nil {
		t.Fatalf("run %q: %v", dsl, err)
	}
	return res.Nodes
}
