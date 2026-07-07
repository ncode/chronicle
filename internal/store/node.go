package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

// ── Node identity (task 6.1; storage primitive used by ingest + lifecycle) ──

// Node is the node row state ingest needs under the per-node lock.
type Node struct {
	ID             int64
	Certname       string
	LastProducerTS *time.Time
	Deactivated    *time.Time
	Expired        *time.Time
}

// lockNode creates the node on first contact and returns its row with a
// FOR UPDATE lock held in tx, serializing concurrent pushes for one certname
// (ADR-0009 §3). Identity is the verified cert CN; same CN = one history. The
// returned `inserted` is true only when this call created the row (genuine first
// contact) — distinguishing a truly-new node from an existing one whose watermark
// was cleared by ResetProducerTS, so the first-contact clock bound does not wedge
// an operator's watermark-reset recovery.
func (s *Store) lockNode(ctx context.Context, tx pgx.Tx, certname string) (Node, bool, error) {
	ct, err := tx.Exec(ctx,
		`INSERT INTO nodes (certname) VALUES ($1) ON CONFLICT (certname) DO NOTHING`,
		certname)
	if err != nil {
		return Node{}, false, fmt.Errorf("ensure node %q: %w", certname, err)
	}
	inserted := ct.RowsAffected() == 1
	n := Node{Certname: certname}
	if err := tx.QueryRow(ctx,
		`SELECT node_id, last_producer_ts, deactivated, expired
		   FROM nodes WHERE certname = $1 FOR UPDATE`, certname,
	).Scan(&n.ID, &n.LastProducerTS, &n.Deactivated, &n.Expired); err != nil {
		return Node{}, false, fmt.Errorf("lock node %q: %w", certname, err)
	}
	return n, inserted, nil
}

// PeekNode reads a node's guard-relevant state without creating or locking it,
// for a cheap pre-check before the authoritative locked apply. ok is false when
// the node does not exist yet (first contact).
func (s *Store) PeekNode(ctx context.Context, certname string) (Node, bool, error) {
	n := Node{Certname: certname}
	err := s.pool.QueryRow(ctx,
		`SELECT node_id, last_producer_ts, deactivated, expired FROM nodes WHERE certname = $1`,
		certname).Scan(&n.ID, &n.LastProducerTS, &n.Deactivated, &n.Expired)
	if errors.Is(err, pgx.ErrNoRows) {
		return Node{}, false, nil
	}
	if err != nil {
		return Node{}, false, err
	}
	return n, true, nil
}

// markContact advances last_seen (server clock) and, when ts is non-nil, the
// applied-snapshot watermark last_producer_ts. Clears expired (a returning push
// un-expires — ADR-0011). Call inside the per-node tx after a successful apply.
func (s *Store) markContact(ctx context.Context, tx pgx.Tx, nodeID int64, now time.Time, producerTS *time.Time) error {
	_, err := tx.Exec(ctx,
		`UPDATE nodes
		    SET last_seen = $2,
		        last_producer_ts = COALESCE($3, last_producer_ts),
		        expired = NULL
		  WHERE node_id = $1`, nodeID, now, producerTS)
	return err
}

// TouchContact advances last_seen for a non-deactivated node WITHOUT applying a
// snapshot, so an authenticated push that was rejected (stale watermark, skew,
// bad request, collision, cap) still registers contact and the node is not
// falsely swept as Expired (ADR-0011, node-lifecycle spec). It runs on the pool
// (autocommit), off the per-node apply tx. It does NOT clear `expired` — only an
// applied push un-expires — and no-ops for an unknown or deactivated certname.
func (s *Store) TouchContact(ctx context.Context, certname string) error {
	_, err := s.pool.Exec(ctx,
		`UPDATE nodes SET last_seen = now() WHERE certname = $1 AND deactivated IS NULL`, certname)
	return err
}

// ErrNodeNotFound is returned by operator actions for an unknown certname.
var ErrNodeNotFound = errors.New("node not found")

// errStaleApply means the push's observation time is not strictly newer than the
// node's newest stored interval, so applying it would create a degenerate
// interval. ApplySnapshot maps it to ErrStale.
var errStaleApply = errors.New("producer_timestamp not after newest stored interval")

// ErrDeactivated, ErrStale, and ErrSkewed are the per-node guard rejections
// ApplySnapshot returns, so the caller maps them to typed responses without
// touching SQL. ErrSkewed is the first-contact past bound (a node with no
// watermark cannot backdate history beyond received_at - max_skew).
var (
	ErrDeactivated = errors.New("node deactivated")
	ErrStale       = errors.New("stale push")
	ErrSkewed      = errors.New("skewed push")
)

// ResetProducerTS clears a node's last_producer_ts so subsequent in-bounds
// pushes are accepted again — the operator recovery for a watermark poisoned by
// a far-future stamp (ADR-0009 §2, task 3.9).
func (s *Store) ResetProducerTS(ctx context.Context, certname string) error {
	ct, err := s.pool.Exec(ctx,
		`UPDATE nodes SET last_producer_ts = NULL WHERE certname = $1`, certname)
	if err != nil {
		return err
	}
	if ct.RowsAffected() == 0 {
		return ErrNodeNotFound
	}
	return nil
}

// ── Lifecycle (ADR-0011, task group 6) ───────────────────────────────────────

// ExpireStale marks nodes `expired` that have had no contact for longer than
// ttl. Soft and reversible: it closes no intervals and deletes nothing (a
// returning push clears `expired` via markContact). Skips already-expired and
// deactivated nodes. Returns the number newly expired.
func (s *Store) ExpireStale(ctx context.Context, ttl time.Duration) (int64, error) {
	cutoff := time.Now().Add(-ttl)
	ct, err := s.pool.Exec(ctx,
		`UPDATE nodes SET expired = now()
		  WHERE last_seen < $1 AND expired IS NULL AND deactivated IS NULL`, cutoff)
	if err != nil {
		return 0, err
	}
	return ct.RowsAffected(), nil
}

// Deactivate is the terminal sunset (ADR-0011): it seals the node's timeline by
// closing every open durable interval and marks the node deactivated, so ingest
// rejects further pushes. History is retained (keep-forever). Idempotent: a
// re-deactivation returns the existing seal time. The seal time is at least
// every open interval's valid_from (so CHECK (valid_from < valid_to) holds even
// if a within-skew future stamp was accepted).
func (s *Store) Deactivate(ctx context.Context, certname string) (time.Time, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return time.Time{}, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	var nodeID int64
	var deactivated *time.Time
	err = tx.QueryRow(ctx,
		`SELECT node_id, deactivated FROM nodes WHERE certname = $1 FOR UPDATE`, certname,
	).Scan(&nodeID, &deactivated)
	if errors.Is(err, pgx.ErrNoRows) {
		return time.Time{}, ErrNodeNotFound
	}
	if err != nil {
		return time.Time{}, err
	}
	if deactivated != nil {
		return *deactivated, nil // already sunset; idempotent
	}

	var sealAt time.Time
	if err := tx.QueryRow(ctx,
		`SELECT GREATEST(now(), COALESCE(max(valid_from), now()) + interval '1 microsecond')
		   FROM fact_history WHERE node_id = $1 AND valid_to = 'infinity'`, nodeID,
	).Scan(&sealAt); err != nil {
		return time.Time{}, err
	}
	if _, err := tx.Exec(ctx,
		`UPDATE fact_history SET valid_to = $2 WHERE node_id = $1 AND valid_to = 'infinity'`,
		nodeID, sealAt); err != nil {
		return time.Time{}, err
	}
	if _, err := tx.Exec(ctx,
		`UPDATE nodes SET deactivated = $2 WHERE node_id = $1`, nodeID, sealAt); err != nil {
		return time.Time{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return time.Time{}, err
	}
	return sealAt, nil
}
