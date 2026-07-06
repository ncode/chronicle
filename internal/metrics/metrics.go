// Package metrics is chronicle's Prometheus instrumentation (task 7.2): ingest
// rate, reject reasons, apply latency, and ingest lag. It uses a private
// registry so there is no global state.
package metrics

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type Metrics struct {
	reg *prometheus.Registry

	Pushes     *prometheus.CounterVec // result=applied|rejected
	Rejects    *prometheus.CounterVec // reason=stale|skewed|...
	ApplySec   prometheus.Histogram   // apply transaction latency
	LagSec     prometheus.Histogram   // received_at - producer_timestamp on applied pushes
	DirtyNodes prometheus.Gauge       // nodes with a consecutive-dirty streak over threshold
}

// New registers the collectors on a fresh registry.
func New() *Metrics {
	reg := prometheus.NewRegistry()
	m := &Metrics{
		reg: reg,
		Pushes: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "chronicle_ingest_pushes_total",
			Help: "Push attempts by result.",
		}, []string{"result"}),
		Rejects: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "chronicle_ingest_rejects_total",
			Help: "Rejected pushes by reason.",
		}, []string{"reason"}),
		ApplySec: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "chronicle_ingest_apply_seconds",
			Help:    "Per-push apply latency.",
			Buckets: prometheus.DefBuckets,
		}),
		LagSec: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "chronicle_ingest_lag_seconds",
			Help:    "received_at - producer_timestamp for applied pushes.",
			Buckets: []float64{1, 5, 15, 60, 300, 1800, 7200, 86400},
		}),
		DirtyNodes: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "chronicle_ingest_dirty_streak_nodes",
			Help: "Nodes whose consecutive discovery-dirty pass streak is over the alarm threshold (tombstones frozen by the carry-forward gate).",
		}),
	}
	reg.MustRegister(m.Pushes, m.Rejects, m.ApplySec, m.LagSec, m.DirtyNodes)
	return m
}

// Handler serves the metrics in Prometheus text format.
func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.reg, promhttp.HandlerOpts{})
}
