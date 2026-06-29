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
}

func New(st *store.Store, cfg *config.ServerConfig, log *slog.Logger) (*Service, error) {
	cl, err := classify.New(cfg.VolatilePaths)
	if err != nil {
		return nil, err
	}
	s := &Service{
		store:    st,
		cfg:      cfg,
		log:      log,
		sem:      make(chan struct{}, cfg.MaxConcurrent),
		limiters: make(map[string]*rate.Limiter),
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
		writeResp(w, http.StatusForbidden, wire.PushResponse{Reason: wire.ReasonNoClientCert})
		return
	}

	// Per-certname rate limit: one node cannot monopolize ingest.
	if !s.allow(certname) {
		w.Header().Set("Retry-After", "10")
		writeResp(w, http.StatusTooManyRequests, wire.PushResponse{Reason: wire.ReasonRateLimited})
		return
	}

	// Backpressure: bounded concurrency, fast-fail rather than unbounded buffer.
	select {
	case s.sem <- struct{}{}:
		defer func() { <-s.sem }()
	default:
		w.Header().Set("Retry-After", "5")
		writeResp(w, http.StatusServiceUnavailable, wire.PushResponse{Reason: wire.ReasonBackpressure})
		return
	}

	// Body byte cap (the other caps need the decoded tree — enforced in Apply).
	r.Body = http.MaxBytesReader(w, r.Body, s.cfg.MaxSnapshotByte)
	push, err := decodePush(r.Body)
	if err != nil {
		if _, ok := errors.AsType[*http.MaxBytesError](err); ok {
			writeResp(w, http.StatusRequestEntityTooLarge,
				wire.PushResponse{Reason: wire.ReasonOversized + ": snapshot-bytes"})
			return
		}
		writeResp(w, http.StatusBadRequest, wire.PushResponse{Reason: wire.ReasonBadRequest})
		return
	}

	resp, status := s.Apply(r.Context(), certname, push, received)
	s.record(resp, push.ProducerTimestamp, received, time.Since(received))
	if status == http.StatusServiceUnavailable {
		w.Header().Set("Retry-After", "5")
	}
	writeResp(w, status, resp)
}

// record updates Prometheus metrics for one push (nil-safe).
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
	s.metrics.Pushes.WithLabelValues("rejected").Inc()
	reason := resp.Reason
	if i := strings.IndexByte(reason, ':'); i >= 0 {
		reason = reason[:i] // collapse "oversized: leaf-count" -> "oversized"
	}
	s.metrics.Rejects.WithLabelValues(reason).Inc()
}

// pendingLeaf is one durable leaf — classified and hashed but not yet interned
// (no path_id). plan() produces these; Apply interns them under the pool.
type pendingLeaf struct {
	path, factName string
	value          json.RawMessage
	hash           [32]byte
}

// pushPlan is the DB-independent result of one push: the anchored producer time,
// the durable leaves pending intern, the volatile blob, and the discovery-clean
// flag. Everything in it is computed without a database.
type pushPlan struct {
	producerTS time.Time
	pending    []pendingLeaf
	volBlob    json.RawMessage
	clean      bool
}

// plan does all the DB-independent work of one push: anchor and validate the
// producer timestamp, decode and flatten the tree, enforce the leaf/path/value
// caps and the skew bound, and classify + hash every leaf into durable (pending
// intern) vs volatile. It is pure — no context, no store — so the whole reject
// contract is unit-testable without Postgres. A nil plan means the push is
// rejected; the returned (response, status) is what Apply should return.
func plan(cfg *config.ServerConfig, cl *classify.Policy, push *wire.Push, received time.Time) (*pushPlan, wire.PushResponse, int) {
	// Anchor on the node clock at microsecond resolution (Postgres timestamptz
	// resolution) so the staleness guard and stored valid_from/valid_to agree; a
	// missing/zero timestamp is rejected rather than written as year-0001.
	producerTS := push.ProducerTimestamp.Truncate(time.Microsecond)
	if producerTS.IsZero() {
		return planReject(wire.PushResponse{Reason: wire.ReasonBadRequest}, http.StatusBadRequest)
	}

	tree, err := decodeTree(push.Tree)
	if err != nil {
		return planReject(wire.PushResponse{Reason: wire.ReasonBadRequest}, http.StatusBadRequest)
	}
	leaves := wire.Flatten(tree)
	if len(leaves) > cfg.MaxLeafCount {
		return planReject(oversized("leaf-count"))
	}

	// Skew upper bound is server-clock-only — reject early, before any DB work.
	if producerTS.After(received.Add(cfg.MaxSkew.D())) {
		return planReject(wire.PushResponse{Reason: wire.ReasonSkewed}, http.StatusConflict)
	}

	// Classify, hash, and cap-check every leaf WITHOUT interning yet — interning
	// (which grows the shared, never-GC'd fact_paths dictionary) is deferred to
	// Apply, after the per-node guards, so a stale/deactivated/oversized push can
	// never pollute the dictionary or path cache.
	pending := make([]pendingLeaf, 0, len(leaves))
	volatile := make(map[string]any, 8)
	for _, lf := range leaves {
		if len(lf.Path) > cfg.MaxPathLen {
			return planReject(oversized("path-length"))
		}
		raw, err := json.Marshal(lf.Value)
		if err != nil {
			return planReject(wire.PushResponse{Reason: wire.ReasonBadRequest}, http.StatusBadRequest)
		}
		if len(raw) > cfg.MaxValueBytes {
			return planReject(oversized("value-bytes"))
		}
		if cl.IsVolatile(lf.Path) {
			volatile[lf.Path] = lf.Value
			continue
		}
		pending = append(pending, pendingLeaf{lf.Path, lf.FactName, raw, store.ValueHash(lf.Value)})
	}
	volBlob, err := json.Marshal(volatile)
	if err != nil {
		return planReject(internalErr())
	}

	return &pushPlan{
		producerTS: producerTS,
		pending:    pending,
		volBlob:    volBlob,
		clean:      push.Discovery.Clean(),
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

	// Cheap non-locking pre-check: reject a deactivated or clearly-stale push
	// BEFORE interning, so a rejected push doesn't grow the shared, never-GC'd
	// dictionary. The authoritative guards re-run under the per-node lock below.
	if peek, ok, perr := s.store.PeekNode(ctx, certname); perr == nil && ok {
		if peek.Deactivated != nil {
			return wire.PushResponse{Reason: wire.ReasonDeactivated}, http.StatusForbidden
		}
		if peek.LastProducerTS != nil && !pl.producerTS.After(*peek.LastProducerTS) {
			return wire.PushResponse{Reason: wire.ReasonStale}, http.StatusConflict
		}
	}

	// Intern on the pool (autocommit) BEFORE opening the per-node tx — never while
	// holding the tx connection, or concurrent lock-waiters could exhaust the pool
	// and deadlock the lock winner mid-intern.
	durable := make([]store.DurableLeaf, 0, len(pl.pending))
	for _, p := range pl.pending {
		pid, err := s.store.InternPath(ctx, p.path, p.factName)
		if err != nil {
			s.log.Error("intern path", "path", p.path, "err", err)
			return internalErr()
		}
		durable = append(durable, store.DurableLeaf{PathID: pid, Value: p.value, Hash: p.hash})
	}

	// Per-node serialized atomic apply (ADR-0009 §3): one tx, lock → authoritative
	// guards → diff → volatile → advance watermark.
	tx, err := s.store.Pool().Begin(ctx)
	if err != nil {
		s.log.Error("begin tx", "certname", certname, "err", err)
		return internalErr()
	}
	defer tx.Rollback(ctx) //nolint:errcheck // rollback after commit is a no-op

	node, err := s.store.LockNode(ctx, tx, certname)
	if err != nil {
		s.log.Error("lock node", "certname", certname, "err", err)
		return internalErr()
	}
	if node.Deactivated != nil {
		return wire.PushResponse{Reason: wire.ReasonDeactivated}, http.StatusForbidden
	}
	// Lower bound (staleness): strictly monotonic per-node producer time.
	if node.LastProducerTS != nil && !pl.producerTS.After(*node.LastProducerTS) {
		return wire.PushResponse{Reason: wire.ReasonStale}, http.StatusConflict
	}

	stats, err := s.store.ApplyDurable(ctx, tx, node.ID, durable, pl.producerTS, pl.clean)
	if err != nil {
		if errors.Is(err, store.ErrStaleApply) {
			return wire.PushResponse{Reason: wire.ReasonStale}, http.StatusConflict
		}
		s.log.Error("apply durable", "certname", certname, "err", err)
		return internalErr()
	}
	if err := s.store.UpsertVolatile(ctx, tx, node.ID, pl.volBlob, pl.producerTS); err != nil {
		s.log.Error("upsert volatile", "certname", certname, "err", err)
		return internalErr()
	}
	if err := s.store.MarkContact(ctx, tx, node.ID, received, &pl.producerTS); err != nil {
		s.log.Error("mark contact", "certname", certname, "err", err)
		return internalErr()
	}
	if err := tx.Commit(ctx); err != nil {
		s.log.Error("commit", "certname", certname, "err", err)
		return internalErr()
	}

	return wire.PushResponse{
		Applied:    true,
		Opened:     stats.Opened,
		Closed:     stats.Closed,
		Tombstoned: stats.Tombstoned,
		Unchanged:  stats.Unchanged,
	}, http.StatusOK
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

func internalErr() (wire.PushResponse, int) {
	return wire.PushResponse{Reason: wire.ReasonInternal}, http.StatusInternalServerError
}

func writeResp(w http.ResponseWriter, status int, resp wire.PushResponse) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(resp)
}
