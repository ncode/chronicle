package query

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/ncode/chronicle/internal/config"
	"github.com/ncode/chronicle/internal/store"
)

func testReadService(t *testing.T) (*Service, *store.Store, context.Context) {
	t.Helper()
	dsn := os.Getenv("CHRONICLE_TEST_DB")
	if dsn == "" {
		t.Skip("set CHRONICLE_TEST_DB to run query service integration tests")
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
	if _, err := st.Pool().Exec(ctx, `TRUNCATE nodes, fact_paths RESTART IDENTITY CASCADE`); err != nil {
		t.Fatal(err)
	}
	cfg := &config.ServerConfig{
		VolatilePaths:  []string{"uptime"},
		AuthFailPerMin: 100,
		StaticTokens: []config.StaticToken{
			{Name: "reader-bot", Token: "r-tok", Role: "reader"},
			{Name: "admin-op", Token: "a-tok", Role: "admin"},
		},
		OIDC: config.OIDC{RolesClaim: "groups"},
	}
	svc, err := NewService(ctx, st, cfg, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	return svc, st, ctx
}

func httpGet(t *testing.T, base, path, tok string) (int, map[string]any) {
	t.Helper()
	req, _ := http.NewRequest(http.MethodGet, base+path, nil)
	if tok != "" {
		req.Header.Set("Authorization", "Bearer "+tok)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var body map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&body)
	return resp.StatusCode, body
}

func httpPost(t *testing.T, base, path, tok string) int {
	t.Helper()
	req, _ := http.NewRequest(http.MethodPost, base+path, nil)
	if tok != "" {
		req.Header.Set("Authorization", "Bearer "+tok)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	return resp.StatusCode
}

// The per-node state endpoint returns current durable facts plus the volatile
// blob, durable-only-with-a-marker at a past T, and hides inactive nodes by
// default (task 4.3).
func TestStateEndpoint(t *testing.T) {
	svc, st, ctx := testReadService(t)
	seed(t, st, ctx, "web01", qt1, map[string]any{"role": "web", "os.name": "Debian"})
	// A later durable change, so there is a genuine past state to reconstruct.
	seed(t, st, ctx, "web01", qt2, map[string]any{"role": "web", "os.name": "Ubuntu"})
	if _, err := st.Pool().Exec(ctx,
		`INSERT INTO node_volatile (node_id, volatile, observed_at)
		 VALUES ((SELECT node_id FROM nodes WHERE certname='web01'), '{"uptime":123}'::jsonb, $1)`, qt2); err != nil {
		t.Fatal(err)
	}

	srv := httptest.NewServer(svc.Handler())
	defer srv.Close()

	// now: current durable facts + volatile blob, marked available.
	code, body := httpGet(t, srv.URL, "/v1/nodes/web01/state", "r-tok")
	if code != 200 {
		t.Fatalf("state now = %d", code)
	}
	if body["volatile_available"] != true {
		t.Fatalf("now must mark volatile available: %+v", body)
	}
	if facts, ok := body["facts"].([]any); !ok || len(facts) != 2 {
		t.Fatalf("now facts = %+v, want 2 durable", body["facts"])
	}
	if vol, ok := body["volatile"].(map[string]any); !ok || vol["uptime"] == nil {
		t.Fatalf("now must include the volatile blob: %+v", body["volatile"])
	}

	// at a past T (between qt1 and qt2): durable-only, volatile explicitly absent.
	past := qt1.Add(30 * 60 * 1_000_000_000) // qt1 + 30m
	code, body = httpGet(t, srv.URL, "/v1/nodes/web01/state?at="+past.Format("2006-01-02T15:04:05Z07:00"), "r-tok")
	if code != 200 {
		t.Fatalf("state at-T = %d", code)
	}
	if body["volatile_available"] != false {
		t.Fatalf("past state must mark volatile unavailable: %+v", body)
	}
	if _, present := body["volatile"]; present {
		t.Fatalf("past state must not fabricate a volatile blob: %+v", body)
	}

	// A node with durable facts but no volatile blob reports volatile_available:false
	// and omits the volatile key (no synthetic empty object).
	seed(t, st, ctx, "novol", qt1, map[string]any{"role": "web"})
	code, body = httpGet(t, srv.URL, "/v1/nodes/novol/state", "r-tok")
	if code != 200 || body["volatile_available"] != false {
		t.Fatalf("no-volatile node: code=%d available=%v", code, body["volatile_available"])
	}
	if _, present := body["volatile"]; present {
		t.Fatalf("no-volatile node must omit the volatile key: %+v", body)
	}

	// Inactive nodes are hidden by default, visible with include_inactive.
	if _, err := st.Deactivate(ctx, "web01"); err != nil {
		t.Fatal(err)
	}
	if code, _ := httpGet(t, srv.URL, "/v1/nodes/web01/state", "r-tok"); code != 404 {
		t.Fatalf("deactivated node must be hidden by default, got %d", code)
	}
	if code, _ := httpGet(t, srv.URL, "/v1/nodes/web01/state?include_inactive=true", "r-tok"); code != 200 {
		t.Fatalf("deactivated node must be visible with include_inactive, got %d", code)
	}
}

// The read/admin mux enforces roles: a reader can read but not deactivate, an
// admin can, and an unauthenticated request is refused (task 7.3).
func TestRoleEnforcement(t *testing.T) {
	svc, st, ctx := testReadService(t)
	seed(t, st, ctx, "role01", qt1, map[string]any{"role": "web"})

	srv := httptest.NewServer(svc.Handler())
	defer srv.Close()

	// Reader can query.
	if code, _ := httpGet(t, srv.URL, "/v1/query?q=role%3Dweb", "r-tok"); code != 200 {
		t.Fatalf("reader query = %d, want 200", code)
	}
	// Reader cannot deactivate.
	if code := httpPost(t, srv.URL, "/v1/admin/deactivate?certname=role01", "r-tok"); code != 403 {
		t.Fatalf("reader deactivate = %d, want 403", code)
	}
	// Admin can deactivate.
	if code := httpPost(t, srv.URL, "/v1/admin/deactivate?certname=role01", "a-tok"); code != 200 {
		t.Fatalf("admin deactivate = %d, want 200", code)
	}
	// Unauthenticated is refused.
	if code, _ := httpGet(t, srv.URL, "/v1/query?q=role%3Dweb", ""); code != 401 {
		t.Fatalf("unauthenticated query = %d, want 401", code)
	}
}

// The node_diff endpoint applies the same inactive-node hiding as the rest of
// the read surface: a deactivated node's diff is 404 by default, visible only
// with include_inactive.
func TestDiffHidesInactiveByDefault(t *testing.T) {
	svc, st, ctx := testReadService(t)
	seed(t, st, ctx, "diff01", qt1, map[string]any{"role": "web"})
	seed(t, st, ctx, "diff01", qt2, map[string]any{"role": "db"}) // a change so the window has content

	srv := httptest.NewServer(svc.Handler())
	defer srv.Close()

	win := "?from=" + qt1.Add(-time.Hour).Format(time.RFC3339) + "&to=" + qt2.Add(time.Hour).Format(time.RFC3339)
	if code, _ := httpGet(t, srv.URL, "/v1/node/diff01/diff"+win, "r-tok"); code != 200 {
		t.Fatalf("active node diff = %d, want 200", code)
	}
	if _, err := st.Deactivate(ctx, "diff01"); err != nil {
		t.Fatal(err)
	}
	if code, _ := httpGet(t, srv.URL, "/v1/node/diff01/diff"+win, "r-tok"); code != 404 {
		t.Fatalf("deactivated node diff = %d, want 404 (hidden by default)", code)
	}
	if code, _ := httpGet(t, srv.URL, "/v1/node/diff01/diff"+win+"&include_inactive=true", "r-tok"); code != 200 {
		t.Fatalf("deactivated node diff with include_inactive = %d, want 200", code)
	}
}
