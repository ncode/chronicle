// Package monitor raises the operational alarms the dumb-node model pushes to
// the center: high-churn durable paths that should probably be Volatile (task
// 7.3, ADR-0007) and per-node fact_paths cardinality spikes (task 7.2, 7.3,
// ADR-0009 §4). Both are periodic SQL scans — no in-memory accounting.
package monitor

import (
	"context"
	"log/slog"
	"time"

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
	if interval <= 0 {
		m.log.Error("monitor interval must be positive", "interval", interval)
		return
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
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
		}
	}
}

// CheckChurn flags Durable paths that opened an abnormal number of intervals in
// the window — the signature of a misclassified Volatile fact. The fix is
// forward-only reclassification: add the path to the Volatile policy.
func (m *Monitor) CheckChurn(ctx context.Context) ([]Finding, error) {
	return m.scan(ctx, `
		SELECT fp.path_text, count(*) AS opens
		FROM   fact_history fh JOIN fact_paths fp USING (path_id)
		WHERE  fh.valid_from > $1
		GROUP  BY fp.path_text
		HAVING count(*) >= $2
		ORDER  BY opens DESC
		LIMIT  20`, time.Now().Add(-m.churnWindow), m.churnThreshold)
}

// CheckCardinality flags nodes whose distinct fact_paths count is abnormally
// high — one authenticated node trying to bloat the shared dictionary.
func (m *Monitor) CheckCardinality(ctx context.Context) ([]Finding, error) {
	return m.scan(ctx, `
		SELECT n.certname, count(DISTINCT fh.path_id) AS paths
		FROM   fact_history fh JOIN nodes n USING (node_id)
		GROUP  BY n.certname
		HAVING count(DISTINCT fh.path_id) >= $1
		ORDER  BY paths DESC
		LIMIT  20`, m.cardThreshold)
}

func (m *Monitor) scan(ctx context.Context, sql string, args ...any) ([]Finding, error) {
	rows, err := m.store.Pool().Query(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Finding
	for rows.Next() {
		var f Finding
		if err := rows.Scan(&f.Key, &f.Count); err != nil {
			return nil, err
		}
		out = append(out, f)
	}
	return out, rows.Err()
}
