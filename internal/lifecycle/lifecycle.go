// Package lifecycle runs the soft-expiry sweep and the terminal deactivation
// sunset (ADR-0011). Identity resolution (certname -> node, create on first
// contact) and the ingest deactivation reject live in store/ingest; this package
// is the periodic sweeper plus a thin deactivation wrapper for the server.
package lifecycle

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/ncode/chronicle/internal/periodic"
	"github.com/ncode/chronicle/internal/store"
)

// Manager periodically marks stale nodes expired.
type Manager struct {
	store *store.Store
	log   *slog.Logger
	ttl   time.Duration
}

func NewManager(st *store.Store, log *slog.Logger, ttl time.Duration) *Manager {
	return &Manager{store: st, log: log, ttl: ttl}
}

// Sweep marks nodes with no contact within ttl as expired (reversible).
func (m *Manager) Sweep(ctx context.Context) (int64, error) {
	if m.ttl <= 0 {
		return 0, fmt.Errorf("expiry ttl must be positive, got %s", m.ttl)
	}
	return m.store.ExpireStale(ctx, m.ttl)
}

// Run sweeps every interval until ctx is cancelled. Errors are logged, not fatal
// (a missed sweep is harmless; the next one catches up).
func (m *Manager) Run(ctx context.Context, interval time.Duration) {
	periodic.Run(ctx, interval, m.log, "expiry sweep", func(ctx context.Context) {
		n, err := m.Sweep(ctx)
		if err != nil {
			m.log.Error("expiry sweep", "err", err)
			return
		}
		if n > 0 {
			m.log.Info("expiry sweep", "newly_expired", n)
		}
	})
}
