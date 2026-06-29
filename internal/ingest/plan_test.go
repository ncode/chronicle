package ingest

import (
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/ncode/chronicle/internal/classify"
	"github.com/ncode/chronicle/internal/config"
	"github.com/ncode/chronicle/internal/wire"
)

// plan is pure (no DB), so the whole reject contract + the durable/volatile split
// is tested here without Postgres. The DB-backed apply path stays in the
// integration tests (ingest_test.go, e2e).

func planCfg() *config.ServerConfig {
	return &config.ServerConfig{
		MaxSkew:       config.Duration(5 * time.Minute),
		MaxLeafCount:  1000,
		MaxPathLen:    256,
		MaxValueBytes: 4096,
	}
}

func planPolicy(t *testing.T) *classify.Policy {
	t.Helper()
	cl, err := classify.New([]string{"uptime", "load*"})
	if err != nil {
		t.Fatal(err)
	}
	return cl
}

var planReceived = time.Date(2026, 6, 28, 12, 0, 0, 0, time.UTC)

func TestPlanRejects(t *testing.T) {
	cl := planPolicy(t)
	ts := planReceived // within skew

	cases := []struct {
		name       string
		cfg        func(*config.ServerConfig)
		ts         time.Time
		tree       string
		wantReason string
		wantStatus int
	}{
		{"zero timestamp", nil, time.Time{}, `{"os":{"name":"Debian"}}`, wire.ReasonBadRequest, http.StatusBadRequest},
		{"bad json tree", nil, ts, `{not json`, wire.ReasonBadRequest, http.StatusBadRequest},
		{"oversized leaf-count", func(c *config.ServerConfig) { c.MaxLeafCount = 1 }, ts,
			`{"a":1,"b":2}`, "oversized: leaf-count", http.StatusRequestEntityTooLarge},
		{"skewed future", nil, planReceived.Add(10 * time.Minute), `{"os":{"name":"Debian"}}`,
			wire.ReasonSkewed, http.StatusConflict},
		{"oversized path-length", func(c *config.ServerConfig) { c.MaxPathLen = 8 }, ts,
			`{"verylongkey":1}`, "oversized: path-length", http.StatusRequestEntityTooLarge},
		{"oversized value-bytes", func(c *config.ServerConfig) { c.MaxValueBytes = 4 }, ts,
			`{"k":"abcdefgh"}`, "oversized: value-bytes", http.StatusRequestEntityTooLarge},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := planCfg()
			if tc.cfg != nil {
				tc.cfg(cfg)
			}
			push := &wire.Push{ProducerTimestamp: tc.ts, Tree: json.RawMessage(tc.tree)}
			pl, resp, status := plan(cfg, cl, push, planReceived)
			if pl != nil {
				t.Fatalf("want reject, got plan %+v", pl)
			}
			if resp.Reason != tc.wantReason || status != tc.wantStatus {
				t.Fatalf("reject = (%q, %d), want (%q, %d)", resp.Reason, status, tc.wantReason, tc.wantStatus)
			}
		})
	}
}

func TestPlanClassifiesAndSplits(t *testing.T) {
	cl := planPolicy(t)
	push := &wire.Push{
		ProducerTimestamp: planReceived,
		Tree:              json.RawMessage(`{"os":{"name":"Debian"},"uptime":12345,"load":{"1m":"0.4"}}`),
	}
	pl, _, status := plan(planCfg(), cl, push, planReceived)
	if pl == nil {
		t.Fatalf("want a plan, got rejected (status %d)", status)
	}
	if !pl.producerTS.Equal(planReceived) {
		t.Fatalf("producerTS = %s, want %s", pl.producerTS, planReceived)
	}
	// os.name is durable; uptime and load.1m are volatile.
	if len(pl.pending) != 1 || pl.pending[0].path != "os.name" {
		t.Fatalf("pending durable = %+v, want exactly [os.name]", pl.pending)
	}
	var vol map[string]json.RawMessage
	if err := json.Unmarshal(pl.volBlob, &vol); err != nil {
		t.Fatal(err)
	}
	if _, ok := vol["uptime"]; !ok {
		t.Errorf("volatile blob missing uptime: %s", pl.volBlob)
	}
	if _, ok := vol["load.1m"]; !ok {
		t.Errorf("volatile blob missing load.1m: %s", pl.volBlob)
	}
	if len(vol) != 2 {
		t.Errorf("volatile blob = %s, want 2 keys", pl.volBlob)
	}
}

func TestPlanCleanFlag(t *testing.T) {
	cl := planPolicy(t)
	tree := json.RawMessage(`{"os":{"name":"Debian"}}`)

	// Default (no source errors) is clean.
	clean := &wire.Push{ProducerTimestamp: planReceived, Tree: tree}
	if pl, _, _ := plan(planCfg(), cl, clean, planReceived); pl == nil || !pl.clean {
		t.Fatal("default discovery must be clean")
	}

	// A built-in source error makes the pass dirty (carry-forward, not tombstone).
	dirty := &wire.Push{
		ProducerTimestamp: planReceived, Tree: tree,
		Discovery: wire.DiscoveryStatus{Builtin: map[string]string{"networking": wire.StatusError}},
	}
	if pl, _, _ := plan(planCfg(), cl, dirty, planReceived); pl == nil || pl.clean {
		t.Fatal("a source error must make the plan dirty")
	}
}
