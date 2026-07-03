// Package ingest is the authenticated push endpoint: mTLS CN identity, the
// two-sided clock guard, server-side Durable/Volatile classification, the
// per-node serialized atomic apply, resource caps, and 503 backpressure
// (ADR-0005, 0006, 0007, 0009).
package ingest

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/time/rate"

	"github.com/ncode/chronicle/internal/classify"
	"github.com/ncode/chronicle/internal/config"
	"github.com/ncode/chronicle/internal/metrics"
	"github.com/ncode/chronicle/internal/store"
	"github.com/ncode/chronicle/internal/wire"
)

// Service applies pushes to the store under the ingest contract.
type Service struct {
	store *store.Store
	cfg   *config.ServerConfig
	log   *slog.Logger

	classifier atomic.Pointer[classify.Policy] // hot-swappable volatile policy (task 7.1)
	metrics    *metrics.Metrics                // nil-safe; set by the server

	sem chan struct{} // bounded ingest concurrency (backpressure)

	limMu    sync.Mutex
	limiters map[string]*rate.Limiter // per-certname rate limiters

	// Per-node consecutive-dirty-pass tracking (task 5.2): a node whose pushes
	// stay discovery-dirty has its tombstones frozen by the carry-forward gate
	// (ADR-0009 §1); once the streak crosses the threshold it becomes visible.
	streakMu    sync.Mutex
	dirtyStreak map[string]int // certname -> consecutive dirty applied passes
	dirtyOver   int            // count of certnames at/over the threshold (gauge source)
}

func New(st *store.Store, cfg *config.ServerConfig, log *slog.Logger) (*Service, error) {
	cl, err := classify.New(cfg.VolatilePaths)
	if err != nil {
		return nil, err
	}
	s := &Service{
		store:       st,
		cfg:         cfg,
		log:         log,
		sem:         make(chan struct{}, cfg.MaxConcurrent),
		limiters:    make(map[string]*rate.Limiter),
		dirtyStreak: make(map[string]int),
	}
	s.classifier.Store(cl)
	return s, nil
}

// SetMetrics attaches Prometheus instrumentation (optional).
func (s *Service) SetMetrics(m *metrics.Metrics) { s.metrics = m }

// ReloadVolatilePolicy rebuilds and atomically swaps the volatile classifier
// (SIGHUP-driven hot reload, task 7.1). Unchanged on a bad pattern.
func (s *Service) ReloadVolatilePolicy(patterns []string) error {
	cl, err := classify.New(patterns)
	if err != nil {
		return err
	}
	s.classifier.Store(cl)
	return nil
}

// Handler returns the ingest mux: the push endpoint plus the advertised limits.
func (s *Service) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/push", s.handlePush)
	mux.HandleFunc("GET /v1/limits", s.handleLimits)
	return mux
}

func (s *Service) handleLimits(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(wire.Limits{
		MaxSnapshotBytes: s.cfg.MaxSnapshotByte,
		MaxLeafCount:     s.cfg.MaxLeafCount,
		MaxPathLen:       s.cfg.MaxPathLen,
		MaxValueBytes:    s.cfg.MaxValueBytes,
	})
}

func (s *Service) handlePush(w http.ResponseWriter, r *http.Request) {
	received := time.Now()

	certname, err := certnameFromChain(r)
	if err != nil {
		// Unauthenticated: not counted as contact (no verified certname).
		s.reject(w, http.StatusForbidden, wire.ReasonNoClientCert, "")
		return
	}

	// Per-certname rate limit: one node cannot monopolize ingest. Load-shedding,
	// so exempt from contact (the node's admitted pushes already register it).
	if !s.allow(certname) {
		s.reject(w, http.StatusTooManyRequests, wire.ReasonRateLimited, "10")
		return
	}

	// Backpressure: bounded concurrency, fast-fail rather than unbounded buffer.
	// Also load-shedding: MUST NOT add a store write while the store is saturated.
	select {
	case s.sem <- struct{}{}:
		defer func() { <-s.sem }()
	default:
		s.reject(w, http.StatusServiceUnavailable, wire.ReasonBackpressure, "5")
		return
	}

	// Past this point the push is authenticated and admitted (not load-shed), so
	// every outcome counts as contact for the expiry sweep (lifecycle spec):
	// applied pushes advance last_seen in the apply tx; rejects get a single-row
	// touch here so a node stuck on rejects is never falsely swept as Expired.
	// Body byte cap (the other caps need the decoded tree — enforced in Apply).
	r.Body = http.MaxBytesReader(w, r.Body, s.cfg.MaxSnapshotByte)
	push, err := decodePush(r.Body)
	if err != nil {
		s.markRejectContact(r.Context(), certname)
		if _, ok := errors.AsType[*http.MaxBytesError](err); ok {
			s.reject(w, http.StatusRequestEntityTooLarge, wire.ReasonOversized+": snapshot-bytes", "")
			return
		}
		s.reject(w, http.StatusBadRequest, wire.ReasonBadRequest, "")
		return
	}

	resp, status := s.Apply(r.Context(), certname, push, received)
	s.record(resp, push.ProducerTimestamp, received, time.Since(received))
	if !resp.Applied {
		s.markRejectContact(r.Context(), certname)
	}
	writeResp(w, status, resp)
}

// reject writes a typed reject response (optionally with Retry-After) and counts
// it, so every pre-apply reject path — no-cert, rate-limit, backpressure,
// body-size, malformed JSON — appears in the metrics, not only apply-path
// rejects (task 5.1).
func (s *Service) reject(w http.ResponseWriter, status int, reason, retryAfter string) {
	if retryAfter != "" {
		w.Header().Set("Retry-After", retryAfter)
	}
	s.countReject(reason)
	writeResp(w, status, wire.PushResponse{Reason: reason})
}

// markRejectContact advances last_seen for a rejected-but-admitted push so a
// node stuck on rejects is not swept as Expired (task 3.1). Best-effort: a
// failed touch is logged, never fatal, and no-ops for an unknown/deactivated
// node. `expired` is intentionally NOT cleared here — only an applied push
// un-expires a node.
func (s *Service) markRejectContact(ctx context.Context, certname string) {
	if err := s.store.TouchContact(ctx, certname); err != nil {
		s.log.Warn("touch contact on reject", "certname", certname, "err", err)
	}
}

// record updates Prometheus metrics for one applied-or-rejected apply outcome
// (nil-safe).
func (s *Service) record(resp wire.PushResponse, producerTS, received time.Time, dur time.Duration) {
	if s.metrics == nil {
		return
	}
	if resp.Applied {
		s.metrics.Pushes.WithLabelValues("applied").Inc()
		s.metrics.ApplySec.Observe(dur.Seconds())
		s.metrics.LagSec.Observe(received.Sub(producerTS).Seconds())
		return
	}
	s.countReject(resp.Reason)
}

// countReject increments the rejected-push counters for a typed reason
// (nil-safe). The reason label is collapsed to its prefix so the sub-cause after
// ':' (e.g. "oversized: leaf-count") does not inflate label cardinality.
func (s *Service) countReject(reason string) {
	if s.metrics == nil {
		return
	}
	s.metrics.Pushes.WithLabelValues("rejected").Inc()
	if i := strings.IndexByte(reason, ':'); i >= 0 {
		reason = reason[:i]
	}
	s.metrics.Rejects.WithLabelValues(reason).Inc()
}

// pushPlan is the DB-independent result of one push: the anchored producer time,
// the receive time and skew bound (carried so the store can enforce the
// first-contact past bound under the node lock, which plan() — being DB-free —
// cannot), the durable leaves pending intern, the volatile blob, and the
// discovery-clean flag. Everything in it is computed without a database.
type pushPlan struct {
	producerTS time.Time
	received   time.Time
	maxSkew    time.Duration
	pending    []store.PendingLeaf
	volBlob    json.RawMessage
	clean      bool
}

// plan does all the DB-independent work of one push: anchor and validate the
// producer timestamp, reject a degenerate push (no report / empty tree), decode
// and flatten the tree, enforce the leaf/path/value caps and the skew upper
// bound, and classify + hash every leaf into durable (pending intern) vs
// volatile. It is pure — no context, no store — so the whole reject contract is
// unit-testable without Postgres. A nil plan means the push is rejected; the
// returned (response, status) is what Apply should return.
func plan(cfg *config.ServerConfig, cl *classify.Policy, push *wire.Push, received time.Time) (*pushPlan, wire.PushResponse, int) {
	// Anchor on the node clock at microsecond resolution (Postgres timestamptz
	// resolution) so the staleness guard and stored valid_from/valid_to agree; a
	// missing/zero timestamp is rejected rather than written as year-0001.
	producerTS := push.ProducerTimestamp.Truncate(time.Microsecond)
	if producerTS.IsZero() {
		return planReject(badRequest())
	}

	// Degenerate-push rejection (task 1.1): a push carrying NO discovery report is
	// evidence of nothing and must never be read as a discovery-clean pass that
	// tombstones the node's whole durable history.
	if len(push.Discovery.Builtin) == 0 && len(push.Discovery.External) == 0 {
		return planReject(badRequest())
	}

	tree, err := decodeTree(push.Tree)
	if err != nil {
		return planReject(badRequest())
	}
	// Caps are enforced incrementally during flattening (task 2.3): an over-cap
	// body is abandoned at the first violation, never fully materialized.
	leaves, err := wire.Flatten(tree, wire.FlattenLimits{MaxLeafCount: cfg.MaxLeafCount, MaxPathLen: cfg.MaxPathLen})
	if err != nil {
		if ce, ok := errors.AsType[*wire.CapError](err); ok {
			return planReject(oversized(ce.Which))
		}
		if col, ok := errors.AsType[*wire.CollisionError](err); ok {
			// Name the colliding path so an operator can find the broken producer.
			return planReject(wire.PushResponse{Reason: wire.ReasonBadRequest + ": colliding path " + col.Path}, http.StatusBadRequest)
		}
		return planReject(badRequest())
	}
	// Degenerate-push rejection (task 1.1): an empty/zero-leaf tree would
	// otherwise be applied as a discovery-clean snapshot in which every fact
	// disappeared, tombstoning the node's entire durable history.
	if len(leaves) == 0 {
		return planReject(badRequest())
	}

	// Skew upper bound is server-clock-only — reject early, before any DB work.
	if producerTS.After(received.Add(cfg.MaxSkew.D())) {
		return planReject(wire.PushResponse{Reason: wire.ReasonSkewed}, http.StatusConflict)
	}

	// Classify, hash, and cap-check every leaf WITHOUT interning yet — interning
	// (which grows the shared, never-GC'd fact_paths dictionary) is deferred to
	// Apply, after the per-node guards, so a stale/deactivated/oversized push can
	// never pollute the dictionary or path cache.
	pending := make([]store.PendingLeaf, 0, len(leaves))
	volatile := make(map[string]any, 8)
	for _, lf := range leaves {
		raw, err := json.Marshal(lf.Value)
		if err != nil {
			return planReject(badRequest())
		}
		if len(raw) > cfg.MaxValueBytes {
			return planReject(oversized("value-bytes"))
		}
		if cl.IsVolatile(lf.Path) {
			volatile[lf.Path] = lf.Value
			continue
		}
		pending = append(pending, store.PendingLeaf{Path: lf.Path, FactName: lf.FactName, Value: raw, Hash: store.ValueHash(lf.Value)})
	}
	volBlob, err := json.Marshal(volatile)
	if err != nil {
		return planReject(internalErr())
	}

	// Tombstone gate: only a clean pass that actually carries durable leaves may
	// tombstone absent ones. A clean push that flattens to ZERO durable leaves
	// (e.g. an over-broad volatile policy that classifies every fact volatile, or
	// a discovery malfunction) would otherwise close the Node's entire durable
	// history — the same "evidence of nothing" hazard as an empty tree, at durable
	// granularity. Carry the durable side forward instead (the volatile blob still
	// applies); the discovery-clean signal for the dirty-streak alarm is separate.
	return &pushPlan{
		producerTS: producerTS,
		received:   received,
		maxSkew:    cfg.MaxSkew.D(),
		pending:    pending,
		volBlob:    volBlob,
		clean:      push.Discovery.Clean() && len(pending) > 0,
	}, wire.PushResponse{}, 0
}

// planReject is the rejected-plan return: a nil plan plus the (response, status)
// to surface. The (resp, status) helpers (oversized, internalErr) compose into it.
func planReject(resp wire.PushResponse, status int) (*pushPlan, wire.PushResponse, int) {
	return nil, resp, status
}

// Apply runs the full ingest contract for one push under the given (verified)
// certname. Split from the HTTP layer so it is testable without TLS. The
// DB-independent work is plan()'s; Apply adds the per-node interning and the
// serialized atomic transaction on top of the plan.
func (s *Service) Apply(ctx context.Context, certname string, push *wire.Push, received time.Time) (wire.PushResponse, int) {
	// Snapshot the volatile policy once for the whole push (it is hot-swapped on
	// SIGHUP); plan() is pure, so the policy is passed in rather than loaded inside.
	pl, resp, status := plan(s.cfg, s.classifier.Load(), push, received)
	if pl == nil {
		return resp, status
	}

	// The per-node intern + serialized atomic transaction (and the ordering
	// invariant that keeps it deadlock-free) live in the store. The receive time
	// and skew bound let the store enforce the first-contact past bound under the
	// lock, where the watermark state is authoritative.
	stats, err := s.store.ApplySnapshot(ctx, certname, pl.received, pl.producerTS, pl.maxSkew, pl.pending, pl.volBlob, pl.clean)
	switch {
	case errors.Is(err, store.ErrDeactivated):
		return wire.PushResponse{Reason: wire.ReasonDeactivated}, http.StatusForbidden
	case errors.Is(err, store.ErrStale):
		return wire.PushResponse{Reason: wire.ReasonStale}, http.StatusConflict
	case errors.Is(err, store.ErrSkewed):
		return wire.PushResponse{Reason: wire.ReasonSkewed}, http.StatusConflict
	case err != nil:
		s.log.Error("apply snapshot", "certname", certname, "err", err)
		return internalErr()
	}

	// The streak tracks DISCOVERY-dirty passes (a source erred), not the apply
	// tombstone gate (pl.clean, which also drops to false on a zero-durable push).
	s.recordDirtyStreak(certname, push.Discovery.Clean(), push.Discovery.FailingSources())

	return wire.PushResponse{
		Applied:    true,
		Opened:     stats.Opened,
		Closed:     stats.Closed,
		Tombstoned: stats.Tombstoned,
		Unchanged:  stats.Unchanged,
	}, http.StatusOK
}

// recordDirtyStreak tracks per-node consecutive discovery-dirty applied passes
// (task 5.2). A clean pass resets the streak; a dirty pass extends it. Crossing
// the configured threshold logs once (naming the failing sources) and bumps the
// over-threshold gauge, so an operator can see that the carry-forward gate has
// frozen a node's tombstones rather than that condition staying silent forever.
// ponytail: the streak is a best-effort alarm. The bookkeeping runs after the
// apply commits, so two concurrent applies for one certname could miscount by
// one — acceptable: applies are DB-serialized and per-certname rate-limited, so
// it is rare and self-heals on the next pass.
func (s *Service) recordDirtyStreak(certname string, clean bool, failing []string) {
	threshold := s.cfg.DirtyStreakThreshold
	if threshold <= 0 {
		return // misconfigured (validate() enforces > 0); never drive the gauge negative
	}
	s.streakMu.Lock()
	crossed := false
	switch {
	case clean:
		if s.dirtyStreak[certname] >= threshold {
			s.dirtyOver--
		}
		delete(s.dirtyStreak, certname)
	default:
		s.dirtyStreak[certname]++
		if s.dirtyStreak[certname] == threshold {
			s.dirtyOver++
			crossed = true
		}
	}
	streak := s.dirtyStreak[certname]
	if s.metrics != nil {
		// Set under the lock so the exported gauge cannot lag the protected state.
		s.metrics.DirtyNodes.Set(float64(s.dirtyOver))
	}
	s.streakMu.Unlock()

	if crossed {
		s.log.Warn("node dirty streak crossed threshold; carry-forward gate is freezing tombstones",
			"certname", certname, "streak", streak, "failing_sources", failing)
	}
}

// allow consults (and lazily creates) the per-certname rate limiter.
//
// ponytail: the limiter map is unbounded but bounded by fleet size (~100k small
// structs); evict idle limiters if that ever matters.
func (s *Service) allow(certname string) bool {
	s.limMu.Lock()
	lim, ok := s.limiters[certname]
	if !ok {
		perMin := s.cfg.RateLimitPerMin
		lim = rate.NewLimiter(rate.Every(time.Minute/time.Duration(perMin)), perMin)
		s.limiters[certname] = lim
	}
	s.limMu.Unlock()
	return lim.Allow()
}

func certnameFromChain(r *http.Request) (string, error) {
	if r.TLS == nil || len(r.TLS.VerifiedChains) == 0 || len(r.TLS.VerifiedChains[0]) == 0 {
		return "", fmt.Errorf("no verified client certificate chain")
	}
	cn := r.TLS.VerifiedChains[0][0].Subject.CommonName
	if cn == "" {
		return "", fmt.Errorf("client certificate has empty CN")
	}
	return cn, nil
}

func decodePush(r io.Reader) (*wire.Push, error) {
	// No DisallowUnknownFields: tolerate agent/server version skew (forward-compat).
	var p wire.Push
	if err := json.NewDecoder(r).Decode(&p); err != nil {
		return nil, err
	}
	return &p, nil
}

func decodeTree(raw json.RawMessage) (map[string]any, error) {
	if len(raw) == 0 {
		return map[string]any{}, nil
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	d.UseNumber() // keep 1 vs 1.0 distinct for value_hash
	var m map[string]any
	if err := d.Decode(&m); err != nil {
		return nil, err
	}
	return m, nil
}

func oversized(which string) (wire.PushResponse, int) {
	return wire.PushResponse{Reason: wire.ReasonOversized + ": " + which}, http.StatusRequestEntityTooLarge
}

func badRequest() (wire.PushResponse, int) {
	return wire.PushResponse{Reason: wire.ReasonBadRequest}, http.StatusBadRequest
}

func internalErr() (wire.PushResponse, int) {
	return wire.PushResponse{Reason: wire.ReasonInternal}, http.StatusInternalServerError
}

func writeResp(w http.ResponseWriter, status int, resp wire.PushResponse) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(resp)
}
