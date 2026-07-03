package agent

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/ncode/chronicle/internal/config"
	"github.com/ncode/chronicle/internal/wire"
)

// A failed startup limits fetch is retried on later cycles until it succeeds,
// then stops — so a transient outage never pins fallback defaults for the whole
// process lifetime (node-agent spec, task 2.4).
func TestRefreshLimitsRetriesThenStops(t *testing.T) {
	var n atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if n.Add(1) == 1 {
			w.WriteHeader(http.StatusInternalServerError) // first fetch fails
			return
		}
		_ = json.NewEncoder(w).Encode(wire.Limits{MaxSnapshotBytes: 4096, MaxLeafCount: 9, MaxPathLen: 64, MaxValueBytes: 128})
	}))
	defer srv.Close()

	a := &Agent{
		cfg:    &config.AgentConfig{},
		log:    slog.New(slog.NewTextHandler(io.Discard, nil)),
		client: srv.Client(),
		limURL: srv.URL,
		limits: generousLimits(),
	}
	ctx := context.Background()

	a.refreshLimits(ctx) // first cycle: fetch fails, keep generous defaults
	if a.haveLimits {
		t.Fatal("failed fetch must not mark limits as fetched")
	}
	if a.limits.MaxLeafCount != generousLimits().MaxLeafCount {
		t.Fatal("failed fetch must keep generous fallback defaults")
	}

	a.refreshLimits(ctx) // later cycle: fetch succeeds, converge
	if !a.haveLimits || a.limits.MaxLeafCount != 9 {
		t.Fatalf("successful fetch must converge to server limits, got %+v", a.limits)
	}

	a.refreshLimits(ctx) // once fetched, no further server hits
	if got := n.Load(); got != 2 {
		t.Fatalf("expected exactly 2 fetch attempts, got %d", got)
	}
}
