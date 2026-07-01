package query

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/ncode/chronicle/internal/classify"
	"github.com/ncode/chronicle/internal/config"
	"github.com/ncode/chronicle/internal/store"
)

// Engine compiles and runs DSL queries against the temporal store.
type Engine struct {
	store      *store.Store
	classifier *classify.Policy
}

// NewEngine builds a query Engine over the store with a volatile classifier.
func NewEngine(st *store.Store, cl *classify.Policy) *Engine {
	return &Engine{store: st, classifier: cl}
}

// Service is the read/admin HTTP surface (ADR-0010): DSL query, node_diff, and
// admin actions, authenticated by OIDC/bearer tokens — never node certs.
type Service struct {
	engine atomic.Pointer[Engine] // hot-swappable on volatile-policy reload (task 7.1)
	store  *store.Store
	auth   *Authenticator
	log    *slog.Logger
}

// NewService builds the read service, including the volatile classifier (shared
// policy with ingest) and the authenticator.
func NewService(ctx context.Context, st *store.Store, cfg *config.ServerConfig, log *slog.Logger) (*Service, error) {
	cl, err := classify.New(cfg.VolatilePaths)
	if err != nil {
		return nil, err
	}
	auth, err := NewAuthenticator(ctx, cfg)
	if err != nil {
		return nil, err
	}
	s := &Service{store: st, auth: auth, log: log}
	s.engine.Store(NewEngine(st, cl))
	return s, nil
}

// ReloadVolatilePolicy rebuilds the query engine's classifier so read-side
// volatile routing / at-rejection stays consistent with ingest after a SIGHUP
// reload (task 7.1, keeps the two endpoints from disagreeing).
func (s *Service) ReloadVolatilePolicy(patterns []string) error {
	cl, err := classify.New(patterns)
	if err != nil {
		return err
	}
	s.engine.Store(NewEngine(s.store, cl))
	return nil
}

// Handler returns the read/admin mux.
func (s *Service) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.Handle("GET /v1/query", s.require(RoleReader, s.handleQuery))
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
		writeJSON(w, http.StatusOK, map[string]any{"deactivated": certname, "sealed_at": sealAt})
	}
}

// require authenticates the bearer credential and enforces the needed role.
func (s *Service) require(need Role, h http.HandlerFunc) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		have, err := s.auth.Authenticate(r.Context(), r)
		if err != nil {
			s.log.Warn("auth rejected", "err", err) // detail to logs, not the client
			writeErr(w, http.StatusUnauthorized, "unauthorized")
			return
		}
		if !allows(have, need) {
			writeErr(w, http.StatusForbidden, "insufficient role")
			return
		}
		h(w, r)
	})
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
	res, err := s.engine.Load().Run(r.Context(), q, includeInactive)
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
	nodeID, ok, err := s.store.NodeID(r.Context(), certname)
	if err != nil {
		s.log.Error("resolve certname", "certname", certname, "err", err)
		writeErr(w, http.StatusInternalServerError, "lookup failed")
		return
	}
	if !ok {
		writeErr(w, http.StatusNotFound, "unknown certname")
		return
	}
	rows, err := s.store.Diff(r.Context(), nodeID, from, to)
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
		writeJSON(w, http.StatusOK, map[string]string{"reset": certname})
	}
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

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}
