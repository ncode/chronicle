// Package monitor raises the operational alarms the dumb-node model pushes to
// the center: high-churn durable paths that should probably be Volatile (task
// 7.3, ADR-0007) and per-node fact_paths cardinality spikes (task 7.2, 7.3,
// ADR-0009 §4). Both are periodic SQL scans — no in-memory accounting.
package monitor

import (
	"context"
	"log/slog"
	"time"

	"github.com/ncode/chronicle/internal/periodic"
	"github.com/ncode/chronicle/internal/store"
)

type Monitor struct {
	store *store.Store
	log   *slog.Logger

	churnWindow    time.Duration // look-back for interval opens
	churnThreshold int64         // opens within the window that trips the alarm
	cardThreshold  int64         // distinct paths per node that trips the alarm
}

// Finding is one alarm row (path/certname + the count that tripped it).
type Finding struct {
	Key   string
	Count int64
}

func New(st *store.Store, log *slog.Logger) *Monitor {
	return &Monitor{
		store:          st,
		log:            log,
		churnWindow:    24 * time.Hour,
		churnThreshold: 5000, // far above a normal durable fact's change rate
		cardThreshold:  5000, // a node normally has a few hundred leaf paths
	}
}

// Run scans on each tick until ctx is cancelled.
func (m *Monitor) Run(ctx context.Context, interval time.Duration) {
	periodic.Run(ctx, interval, m.log, "monitor", func(ctx context.Context) {
		churn, err := m.CheckChurn(ctx)
		if err != nil {
			m.log.Error("churn scan", "err", err)
		}
		for _, f := range churn {
			m.log.Warn("high-churn durable path; consider adding to the volatile policy",
				"path", f.Key, "opens", f.Count, "window", m.churnWindow.String())
		}
		card, err := m.CheckCardinality(ctx)
		if err != nil {
			m.log.Error("cardinality scan", "err", err)
		}
		for _, f := range card {
			m.log.Warn("per-node fact_paths cardinality spike", "certname", f.Key, "paths", f.Count)
		}
	})
}

// CheckChurn flags Durable paths that opened an abnormal number of intervals in
// the window — the signature of a misclassified Volatile fact. The fix is
// forward-only reclassification: add the path to the Volatile policy.
func (m *Monitor) CheckChurn(ctx context.Context) ([]Finding, error) {
	rows, err := m.store.HighChurn(ctx, time.Now().Add(-m.churnWindow), m.churnThreshold)
	if err != nil {
		return nil, err
	}
	var out []Finding
	for _, r := range rows {
		out = append(out, Finding{Key: r.Key, Count: r.Count})
	}
	return out, nil
}

// CheckCardinality flags nodes whose distinct fact_paths count is abnormally
// high — one authenticated node trying to bloat the shared dictionary.
func (m *Monitor) CheckCardinality(ctx context.Context) ([]Finding, error) {
	rows, err := m.store.FactPathCardinality(ctx, m.cardThreshold)
	if err != nil {
		return nil, err
	}
	var out []Finding
	for _, r := range rows {
		out = append(out, Finding{Key: r.Key, Count: r.Count})
	}
	return out, nil
}
