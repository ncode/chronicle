// Package wire is the push payload shared by the agent (producer) and the
// ingest service (consumer). Identity is NOT here — it comes from the verified
// mTLS chain CN, never the body (ADR-0005, fact-ingest spec).
package wire

import (
	"encoding/json"
	"time"
)

// Push is one node snapshot POSTed to chronicle. The Tree is the raw nested fact
// tree (the server decodes it with UseNumber and flattens to leaf paths, so the
// agent stays dumb — ADR-0007). No certname/credential appears here.
type Push struct {
	ProducerTimestamp time.Time       `json:"producer_timestamp"`
	Tree              json.RawMessage `json:"tree"`
	Discovery         DiscoveryStatus `json:"discovery"`
}

// DiscoveryStatus is the per-source report (node-agent spec). Built-in resolvers
// report ok|error per namespace; external facts report ok|error|absent per file.
// The server derives a single discovery-clean gate from it (ADR-0009 §1).
type DiscoveryStatus struct {
	Builtin  map[string]string `json:"builtin,omitempty"`  // namespace -> ok|error
	External map[string]string `json:"external,omitempty"` // file -> ok|error|absent
}

// Status values.
const (
	StatusOK    = "ok"
	StatusError = "error"
)

// Clean reports whether discovery had no source error this pass. An absent
// external file is NOT an error (the script is gone → tombstone-eligible); only
// an explicit error (present-but-failed) makes the pass dirty (carry-forward).
func (d DiscoveryStatus) Clean() bool {
	for _, s := range d.Builtin {
		if s == StatusError {
			return false
		}
	}
	for _, s := range d.External {
		if s == StatusError {
			return false
		}
	}
	return true
}

// FailingSources lists the built-in namespaces and external files that reported
// an error this pass (the reason a pass is dirty). Used for the per-node
// dirty-streak log so an operator can see which sources froze the tombstones.
func (d DiscoveryStatus) FailingSources() []string {
	var out []string
	for name, s := range d.Builtin {
		if s == StatusError {
			out = append(out, name)
		}
	}
	for path, s := range d.External {
		if s == StatusError {
			out = append(out, path)
		}
	}
	return out
}

// PushResponse is the ingest reply (fact-ingest "Response contract"). Applied
// reports success; Reason carries the typed reject cause when Applied is false.
type PushResponse struct {
	Applied    bool   `json:"applied"`
	Reason     string `json:"reason,omitempty"`
	Opened     int    `json:"opened,omitempty"`
	Closed     int    `json:"closed,omitempty"`
	Tombstoned int    `json:"tombstoned,omitempty"`
	Unchanged  int    `json:"unchanged,omitempty"`
}

// Limits are the server-advertised ingest caps the agent pre-checks against
// (node-agent "Bounded Snapshot Payload"). Fetched from GET /v1/limits.
type Limits struct {
	MaxSnapshotBytes int64 `json:"max_snapshot_bytes"`
	MaxLeafCount     int   `json:"max_leaf_count"`
	MaxPathLen       int   `json:"max_path_len"`
	MaxValueBytes    int   `json:"max_value_bytes"`
}

// Reject reasons (typed so the agent and operators can alarm on sustained rejects).
const (
	ReasonStale        = "stale"
	ReasonSkewed       = "skewed"
	ReasonOversized    = "oversized"
	ReasonRateLimited  = "rate-limited"
	ReasonBackpressure = "backpressure"
	ReasonDeactivated  = "deactivated"
	ReasonNoClientCert = "no-client-cert"
	ReasonBadRequest   = "bad-request"
	ReasonInternal     = "internal-error"
)
