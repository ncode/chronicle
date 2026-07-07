package query

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/time/rate"

	"github.com/ncode/chronicle/internal/classify"
	"github.com/ncode/chronicle/internal/config"
	"github.com/ncode/chronicle/internal/store"
)

// Engine compiles and runs DSL queries against the temporal store.
type Engine struct {
	store      *store.Store
	classifier *atomic.Pointer[classify.Policy]
}

// NewEngine builds a query Engine over the store with a volatile classifier.
func NewEngine(st *store.Store, classifier *atomic.Pointer[classify.Policy]) *Engine {
	return &Engine{store: st, classifier: classifier}
}

// Service is the read/admin HTTP surface (ADR-0010): DSL query, node_diff, and
// admin actions, authenticated by OIDC/bearer tokens — never node certs.
type Service struct {
	engine *Engine
	auth   atomic.Pointer[Authenticator] // hot-swappable on SIGHUP auth reload (task 4.1)
	store  *store.Store
	log    *slog.Logger

	// Per-source failed-auth limiter (task 4.5): throttles online brute-force of
	// static tokens without touching legitimate authenticated traffic.
	failPerMin int
	failMu     sync.Mutex
	failLim    map[string]*rate.Limiter
	failGlobal *rate.Limiter // shared fallback once failLim is full (bounds memory)
}

// maxAuthFailSources caps the per-source failure-limiter map so an attacker
// spraying distinct source IPs (e.g. an IPv6 /64) can't grow it without bound —
// the read listener requests no client cert, so this path is reachable
// pre-authentication. Beyond the cap, new sources share failGlobal.
const maxAuthFailSources = 8192

// NewService builds the read service, including the volatile classifier (shared
// policy with ingest) and the authenticator.
func NewService(ctx context.Context, st *store.Store, cfg *config.ServerConfig, log *slog.Logger, classifier *atomic.Pointer[classify.Policy]) (*Service, error) {
	auth, err := NewAuthenticator(ctx, cfg)
	if err != nil {
		return nil, err
	}
	s := &Service{
		engine:     NewEngine(st, classifier),
		store:      st,
		log:        log,
		failPerMin: cfg.AuthFailPerMin,
		failLim:    make(map[string]*rate.Limiter),
		failGlobal: rate.NewLimiter(rate.Every(time.Minute/time.Duration(cfg.AuthFailPerMin)), cfg.AuthFailPerMin),
	}
	s.auth.Store(auth)
	return s, nil
}

// ReloadAuth rebuilds and atomically swaps the Authenticator on SIGHUP so a
// removed static token stops working without a restart (task 4.1). The
// static-token half always swaps; a returned error means only the OIDC verifier
// could not be rebuilt (kept fail-closed) and is for logging — token revocation
// still took effect.
func (s *Service) ReloadAuth(ctx context.Context, cfg *config.ServerConfig) error {
	next, err := s.auth.Load().Reload(ctx, cfg)
	s.auth.Store(next) // next is always usable, even when err != nil
	return err
}

// Handler returns the read/admin mux.
func (s *Service) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.Handle("GET /v1/query", s.require(RoleReader, s.handleQuery))
	mux.Handle("GET /v1/nodes/{certname}/state", s.require(RoleReader, s.handleState))
	mux.Handle("GET /v1/node/{certname}/diff", s.require(RoleReader, s.handleDiff))
	mux.Handle("POST /v1/admin/reset-watermark", s.require(RoleAdmin, s.handleResetWatermark))
	mux.Handle("POST /v1/admin/deactivate", s.require(RoleAdmin, s.handleDeactivate))
	return mux
}

func (s *Service) handleDeactivate(w http.ResponseWriter, r *http.Request) {
	certname := r.URL.Query().Get("certname")
	if certname == "" {
		writeErr(w, http.StatusBadRequest, "missing certname")
		return
	}
	sealAt, err := s.store.Deactivate(r.Context(), certname)
	switch {
	case errors.Is(err, store.ErrNodeNotFound):
		writeErr(w, http.StatusNotFound, "unknown certname")
	case err != nil:
		s.log.Error("deactivate", "certname", certname, "err", err)
		writeErr(w, http.StatusInternalServerError, "deactivate failed")
	default:
		s.audit(r, "deactivate", certname)
		writeJSON(w, http.StatusOK, map[string]any{"deactivated": certname, "sealed_at": sealAt})
	}
}

// audit writes an attribution record for a successful terminal/destructive admin
// action (task 4.4): the acting principal, the action, the target, and the time.
// The principal is the operator-assigned static-token name or the OIDC subject —
// never the token secret.
func (s *Service) audit(r *http.Request, action, target string) {
	s.log.Info("admin action",
		"audit", true,
		"principal", principalFrom(r.Context()),
		"action", action,
		"target", target,
		"time", time.Now().UTC().Format(time.RFC3339))
}

// require authenticates the bearer credential, enforces the needed role, and
// threads the authenticated principal into the request context for auditing.
//
// Brute-force resistance (task 4.5): the per-source failure budget is checked
// BEFORE the credential is evaluated. Once a source burns its budget, further
// attempts are refused without checking the token — so an attacker can't keep
// guessing at full rate, and even a correct guess is blocked until the budget
// refills. Legitimate traffic is unaffected: a successful auth never debits the
// budget, so a source presenting a valid token never runs out.
func (s *Service) require(need Role, h http.HandlerFunc) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !s.authAttemptAllowed(r) {
			writeErr(w, http.StatusTooManyRequests, "too many authentication failures")
			return
		}
		have, who, err := s.auth.Load().Authenticate(r.Context(), r)
		if err != nil {
			s.recordAuthFailure(r)
			s.log.Warn("auth rejected", "err", err) // detail to logs, not the client
			writeErr(w, http.StatusUnauthorized, "unauthorized")
			return
		}
		if !allows(have, need) {
			// A valid credential lacking the role is authorization, not a failed
			// auth attempt — it does not debit the brute-force budget.
			writeErr(w, http.StatusForbidden, "insufficient role")
			return
		}
		h(w, r.WithContext(withPrincipal(r.Context(), who)))
	})
}

// failLimiter returns the failure limiter for a request's source, creating one
// while the map has room. Once the map is full (a possible source-IP spray), new
// sources share failGlobal so memory stays bounded.
func (s *Service) failLimiter(r *http.Request) *rate.Limiter {
	src := r.RemoteAddr
	if host, _, err := net.SplitHostPort(src); err == nil {
		src = host
	}
	s.failMu.Lock()
	defer s.failMu.Unlock()
	if lim, ok := s.failLim[src]; ok {
		return lim
	}
	if len(s.failLim) >= maxAuthFailSources {
		return s.failGlobal
	}
	lim := rate.NewLimiter(rate.Every(time.Minute/time.Duration(s.failPerMin)), s.failPerMin)
	s.failLim[src] = lim
	return lim
}

// authAttemptAllowed reports whether the source still has failure budget — a
// peek that does NOT consume, so it can gate before the credential is checked.
func (s *Service) authAttemptAllowed(r *http.Request) bool {
	return s.failLimiter(r).Tokens() >= 1
}

// recordAuthFailure debits one token from the source's failure budget.
func (s *Service) recordAuthFailure(r *http.Request) {
	s.failLimiter(r).Allow()
}

// maxDSLLen bounds the query string so an authenticated reader can't submit a
// pathologically long DSL that burns CPU/memory before compilation.
const maxDSLLen = 8192

func (s *Service) handleQuery(w http.ResponseWriter, r *http.Request) {
	raw := r.URL.Query().Get("q")
	if raw == "" {
		writeErr(w, http.StatusBadRequest, "missing q")
		return
	}
	if len(raw) > maxDSLLen {
		writeErr(w, http.StatusRequestEntityTooLarge, "query too long")
		return
	}
	q, err := Parse(raw)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	includeInactive := r.URL.Query().Get("include_inactive") == "true"
	res, err := s.engine.Run(r.Context(), q, includeInactive)
	if err != nil {
		switch {
		case errors.Is(err, ErrNoHistory):
			writeErr(w, http.StatusUnprocessableEntity, err.Error())
		case errors.Is(err, ErrUnsupported), errors.Is(err, ErrBadQuery):
			writeErr(w, http.StatusBadRequest, err.Error())
		default:
			s.log.Error("run query", "q", raw, "err", err)
			writeErr(w, http.StatusInternalServerError, "query failed")
		}
		return
	}
	writeJSON(w, http.StatusOK, res)
}

func (s *Service) handleDiff(w http.ResponseWriter, r *http.Request) {
	certname := r.PathValue("certname")
	from, err1 := time.Parse(time.RFC3339, r.URL.Query().Get("from"))
	to, err2 := time.Parse(time.RFC3339, r.URL.Query().Get("to"))
	if err1 != nil || err2 != nil {
		writeErr(w, http.StatusBadRequest, "from/to must be RFC3339")
		return
	}
	// Apply the same inactive-node default as the rest of the read surface:
	// expired/deactivated nodes are hidden (404) unless include_inactive, so a
	// reader can't pull an inactive node's history diff by default.
	includeInactive := r.URL.Query().Get("include_inactive") == "true"
	node, ok, err := s.store.PeekNode(r.Context(), certname)
	if err != nil {
		s.log.Error("resolve certname", "certname", certname, "err", err)
		writeErr(w, http.StatusInternalServerError, "lookup failed")
		return
	}
	if !ok || (!includeInactive && (node.Deactivated != nil || node.Expired != nil)) {
		writeErr(w, http.StatusNotFound, "unknown certname")
		return
	}
	rows, err := s.store.Diff(r.Context(), node.ID, from, to)
	if err != nil {
		s.log.Error("node diff", "certname", certname, "err", err)
		writeErr(w, http.StatusInternalServerError, "diff failed")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"certname": certname, "changes": rows})
}

func (s *Service) handleResetWatermark(w http.ResponseWriter, r *http.Request) {
	certname := r.URL.Query().Get("certname")
	if certname == "" {
		writeErr(w, http.StatusBadRequest, "missing certname")
		return
	}
	err := s.store.ResetProducerTS(r.Context(), certname)
	switch {
	case errors.Is(err, store.ErrNodeNotFound):
		writeErr(w, http.StatusNotFound, "unknown certname")
	case err != nil:
		s.log.Error("reset watermark", "certname", certname, "err", err)
		writeErr(w, http.StatusInternalServerError, "reset failed")
	default:
		s.audit(r, "reset-watermark", certname)
		writeJSON(w, http.StatusOK, map[string]string{"reset": certname})
	}
}

// handleState answers "what did node X look like now / at time T" (task 4.3,
// CONTEXT.md's core promise). It applies the read surface's inactive-node
// default: expired/deactivated nodes are hidden unless include_inactive=true.
// Current state returns open durable facts plus the latest volatile blob; state
// at a past T returns durable facts only, with volatile explicitly marked
// unavailable (volatile is latest-only — it has no history at a past instant).
func (s *Service) handleState(w http.ResponseWriter, r *http.Request) {
	certname := r.PathValue("certname")
	includeInactive := r.URL.Query().Get("include_inactive") == "true"

	node, ok, err := s.store.PeekNode(r.Context(), certname)
	if err != nil {
		s.log.Error("resolve certname", "certname", certname, "err", err)
		writeErr(w, http.StatusInternalServerError, "lookup failed")
		return
	}
	// A node hidden by the inactive default is reported as not found so its
	// existence and state are not leaked to a default read.
	if !ok || (!includeInactive && (node.Deactivated != nil || node.Expired != nil)) {
		writeErr(w, http.StatusNotFound, "unknown certname")
		return
	}

	atRaw := r.URL.Query().Get("at")
	if atRaw == "" { // current state: open durable facts + latest volatile blob
		facts, err := s.store.Now(r.Context(), node.ID)
		if err != nil {
			s.log.Error("node state now", "certname", certname, "err", err)
			writeErr(w, http.StatusInternalServerError, "state failed")
			return
		}
		blob, observedAt, hasVol, err := s.store.Volatile(r.Context(), node.ID)
		if err != nil {
			s.log.Error("node volatile", "certname", certname, "err", err)
			writeErr(w, http.StatusInternalServerError, "state failed")
			return
		}
		// volatile_available reflects whether a blob actually exists: at "now"
		// volatile is queryable, but a Node that has never applied a volatile blob
		// has none — don't hand back a synthetic {} the client would read as real.
		resp := map[string]any{"certname": certname, "at": "now", "facts": facts, "volatile_available": hasVol}
		if hasVol {
			resp["volatile"] = blob
			resp["volatile_observed_at"] = observedAt
		}
		writeJSON(w, http.StatusOK, resp)
		return
	}

	at, err := time.Parse(time.RFC3339, atRaw)
	if err != nil {
		writeErr(w, http.StatusBadRequest, "at must be RFC3339")
		return
	}
	facts, err := s.store.StateAt(r.Context(), node.ID, at)
	if err != nil {
		s.log.Error("node state at", "certname", certname, "err", err)
		writeErr(w, http.StatusInternalServerError, "state failed")
		return
	}
	// Volatile is latest-only: it has no value at a past instant. Mark it
	// explicitly unavailable rather than silently omitting it.
	writeJSON(w, http.StatusOK, map[string]any{
		"certname": certname, "at": at, "facts": facts, "volatile_available": false,
	})
}

// ReadServerTLSConfig builds the read listener's TLS: server-TLS with NO client
// certificate requested, so a node identity cert can never be presented as a
// read credential (ADR-0010). Auth is bearer-token only.
func ReadServerTLSConfig(cfg *config.ServerConfig) (*tls.Config, error) {
	cert, err := tls.LoadX509KeyPair(cfg.TLS.ServerCert, cfg.TLS.ServerKey)
	if err != nil {
		return nil, err
	}
	return &tls.Config{
		Certificates: []tls.Certificate{cert},
		ClientAuth:   tls.NoClientCert,
		MinVersion:   tls.VersionTLS12,
	}, nil
}

// principalKey types the request-context slot holding the authenticated
// principal, so auth resolution survives past the auth boundary for auditing.
type principalKey struct{}

func withPrincipal(ctx context.Context, who string) context.Context {
	return context.WithValue(ctx, principalKey{}, who)
}

func principalFrom(ctx context.Context) string {
	if who, ok := ctx.Value(principalKey{}).(string); ok && who != "" {
		return who
	}
	return "unknown"
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}
