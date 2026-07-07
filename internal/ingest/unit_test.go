package ingest

import (
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"io"
	"log/slog"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"golang.org/x/time/rate"

	"github.com/ncode/chronicle/internal/config"
	"github.com/ncode/chronicle/internal/metrics"
	"github.com/ncode/chronicle/internal/wire"
)

func quietLog() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

// recordDirtyStreak extends the per-node streak on dirty passes, resets it on a
// clean pass, and bumps the over-threshold gauge exactly when a node crosses.
func TestDirtyStreak(t *testing.T) {
	m := metrics.New()
	s := &Service{
		cfg:         &config.ServerConfig{DirtyStreakThreshold: 3},
		log:         quietLog(),
		metrics:     m,
		dirtyStreak: map[string]int{},
	}
	// Two dirty passes: below threshold, gauge stays 0.
	s.recordDirtyStreak("n1", false, []string{"networking"})
	s.recordDirtyStreak("n1", false, []string{"networking"})
	if s.dirtyStreak["n1"] != 2 || s.dirtyOver != 0 {
		t.Fatalf("after 2 dirty: streak=%d over=%d, want 2,0", s.dirtyStreak["n1"], s.dirtyOver)
	}
	// Third dirty pass crosses the threshold.
	s.recordDirtyStreak("n1", false, []string{"networking"})
	if s.dirtyStreak["n1"] != 3 || s.dirtyOver != 1 {
		t.Fatalf("after 3 dirty: streak=%d over=%d, want 3,1", s.dirtyStreak["n1"], s.dirtyOver)
	}
	if got := testutil.ToFloat64(m.DirtyNodes); got != 1 {
		t.Fatalf("dirty gauge = %v, want 1", got)
	}
	// A clean pass resets and drops the gauge.
	s.recordDirtyStreak("n1", true, nil)
	if _, ok := s.dirtyStreak["n1"]; ok || s.dirtyOver != 0 {
		t.Fatalf("after clean: present=%v over=%d, want absent,0", ok, s.dirtyOver)
	}
	if got := testutil.ToFloat64(m.DirtyNodes); got != 0 {
		t.Fatalf("dirty gauge = %v, want 0", got)
	}
}

// Every pre-apply reject that passes the load-shedding gates is counted with a
// reason label, not only apply-path rejects (task 5.1). These two paths never
// touch the store, so they are exercised without Postgres.
func TestEarlyRejectsCounted(t *testing.T) {
	newSvc := func(sem chan struct{}) (*Service, *metrics.Metrics) {
		m := metrics.New()
		return &Service{
			cfg:      &config.ServerConfig{RateLimitPerMin: 100, MaxSnapshotByte: 1 << 20},
			log:      quietLog(),
			metrics:  m,
			sem:      sem,
			limiters: map[string]*rate.Limiter{},
		}, m
	}
	withCert := func(cn string) *tls.ConnectionState {
		return &tls.ConnectionState{VerifiedChains: [][]*x509.Certificate{{
			{Subject: pkix.Name{CommonName: cn}},
		}}}
	}

	// No client cert: 403, counted as no-client-cert.
	s, m := newSvc(make(chan struct{}, 1))
	r := httptest.NewRequest("POST", "/v1/push", strings.NewReader("{}"))
	w := httptest.NewRecorder()
	s.handlePush(w, r)
	if w.Code != 403 {
		t.Fatalf("no-cert status = %d, want 403", w.Code)
	}
	if got := testutil.ToFloat64(m.Rejects.WithLabelValues(wire.ReasonNoClientCert)); got != 1 {
		t.Fatalf("no-client-cert reject count = %v, want 1", got)
	}

	// Backpressure: an unbuffered (saturated) semaphore fast-fails with 503,
	// counted as backpressure, WITHOUT touching the store (load-shedding).
	s, m = newSvc(make(chan struct{})) // no receiver: send never ready -> default -> 503
	r = httptest.NewRequest("POST", "/v1/push", strings.NewReader("{}"))
	r.TLS = withCert("node1")
	w = httptest.NewRecorder()
	s.handlePush(w, r)
	if w.Code != 503 {
		t.Fatalf("backpressure status = %d, want 503", w.Code)
	}
	if got := w.Header().Get("Retry-After"); got != "5" {
		t.Fatalf("backpressure Retry-After = %q, want 5", got)
	}
	if got := testutil.ToFloat64(m.Rejects.WithLabelValues(wire.ReasonBackpressure)); got != 1 {
		t.Fatalf("backpressure reject count = %v, want 1", got)
	}
}
