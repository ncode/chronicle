package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
)

// ── Durable apply: the close/open interval primitive (task 2.4) ──────────────

// DurableLeaf is one classified Durable leaf ready to apply: its interned
// path_id, the raw jsonb value bytes, and the content hash.
type DurableLeaf struct {
	PathID int64
	Value  json.RawMessage
	Hash   [32]byte
}

// ApplyStats reports what an apply did (for the push response + metrics).
type ApplyStats struct {
	Opened     int // new interval opened (new fact or changed value)
	Closed     int // old interval closed because the value changed
	Tombstoned int // open interval closed with no replacement (genuine removal)
	Unchanged  int // value matched the open interval; nothing written
}

// applyDurable applies the whole Durable snapshot for one node at time t inside
// the caller's transaction (which already holds the per-node lock and has run
// the staleness/skew guards). It is change-only: unchanged leaves write nothing,
// changed leaves close-then-open, new leaves open. Leaves currently open but
// absent from leaves are tombstoned ONLY when discoveryClean is true; otherwise
// they are carried forward (ADR-0009 §1 absence semantics).
//
// Read-free for present leaves: it reads the node's open set once, diffs in Go,
// and batches the minimal close/open/tombstone statements in one round trip.
func (s *Store) applyDurable(ctx context.Context, tx pgx.Tx, nodeID int64, leaves []DurableLeaf, t time.Time, discoveryClean bool) (ApplyStats, error) {
	var stats ApplyStats

	open, maxValidFrom, err := s.openHashes(ctx, tx, nodeID)
	if err != nil {
		return stats, err
	}
	maxClosedValidTo, err := s.maxClosedValidTo(ctx, tx, nodeID)
	if err != nil {
		return stats, err
	}

	// Reject a push whose observation time would overlap the node's stored
	// history (ADR-0003 no-overlap invariant). Two bounds, both required:
	//
	//   1. t must be strictly after every OPEN interval's valid_from, or closing
	//      one at t would write valid_to <= valid_from and trip
	//      CHECK (valid_from < valid_to).
	//   2. t must be at or after every CLOSED interval's valid_to, or a new
	//      interval opened at t would overlap an already-closed one — two
	//      conflicting values at the same instant. Equality is allowed: intervals
	//      are half-open [valid_from, valid_to), so opening exactly at a closed
	//      valid_to is adjacent, not overlapping.
	//
	// Normally the caller's watermark guard already ensures t is past
	// last_producer_ts. This also covers a watermark cleared by ResetProducerTS on
	// a node whose closed history extends past the reset point (post-reset past
	// push after a full tombstone pass).
	if len(open) > 0 && !t.After(maxValidFrom) {
		return stats, errStaleApply
	}
	if !maxClosedValidTo.IsZero() && t.Before(maxClosedValidTo) {
		return stats, errStaleApply
	}

	// Dedup by PathID (last-wins): a snapshot can flatten two tree shapes to the
	// same leaf path (e.g. {"a":{"b":1},"a.b":2}), which intern to one path_id;
	// queuing both would emit conflicting close/open statements and abort the tx.
	leaves = dedupByPath(leaves)

	const closeSQL = `UPDATE fact_history SET valid_to = $3
	                   WHERE node_id = $1 AND path_id = $2 AND valid_to = 'infinity'`
	const openSQL = `INSERT INTO fact_history (node_id, path_id, value, value_hash, valid_from)
	                 VALUES ($1, $2, $3::jsonb, $4, $5)`

	batch := &pgx.Batch{}
	seen := make(map[int64]struct{}, len(leaves))
	for _, lf := range leaves {
		seen[lf.PathID] = struct{}{}
		cur, isOpen := open[lf.PathID]
		switch {
		case isOpen && cur == lf.Hash:
			stats.Unchanged++ // change-only: nothing to write
		case isOpen:
			batch.Queue(closeSQL, nodeID, lf.PathID, t) // close old…
			batch.Queue(openSQL, nodeID, lf.PathID, string(lf.Value), lf.Hash[:], t)
			stats.Closed++
			stats.Opened++
		default:
			batch.Queue(openSQL, nodeID, lf.PathID, string(lf.Value), lf.Hash[:], t)
			stats.Opened++
		}
	}
	if discoveryClean {
		for pid := range open {
			if _, ok := seen[pid]; ok {
				continue
			}
			batch.Queue(closeSQL, nodeID, pid, t) // tombstone via absence
			stats.Tombstoned++
		}
	}

	if batch.Len() == 0 {
		return stats, nil
	}
	br := tx.SendBatch(ctx, batch)
	defer br.Close()
	for i := 0; i < batch.Len(); i++ {
		if _, err := br.Exec(); err != nil {
			return stats, fmt.Errorf("apply durable batch: %w", err)
		}
	}
	return stats, br.Close()
}

// dedupByPath collapses leaves sharing a path_id to the last occurrence.
func dedupByPath(leaves []DurableLeaf) []DurableLeaf {
	idx := make(map[int64]int, len(leaves))
	out := make([]DurableLeaf, 0, len(leaves))
	for _, lf := range leaves {
		if i, ok := idx[lf.PathID]; ok {
			out[i] = lf // last wins
			continue
		}
		idx[lf.PathID] = len(out)
		out = append(out, lf)
	}
	return out
}

// openHashes reads the (path_id -> value_hash) set of a node's currently-open
// intervals plus the newest open valid_from. Served by fact_history_open_uniq
// (node_id leading, partial on open).
func (s *Store) openHashes(ctx context.Context, tx pgx.Tx, nodeID int64) (map[int64][32]byte, time.Time, error) {
	rows, err := tx.Query(ctx,
		`SELECT path_id, value_hash, valid_from FROM fact_history
		  WHERE node_id = $1 AND valid_to = 'infinity'`, nodeID)
	if err != nil {
		return nil, time.Time{}, fmt.Errorf("read open set: %w", err)
	}
	defer rows.Close()
	open := make(map[int64][32]byte)
	var maxValidFrom time.Time
	for rows.Next() {
		var pid int64
		var h []byte
		var vf time.Time
		if err := rows.Scan(&pid, &h, &vf); err != nil {
			return nil, time.Time{}, err
		}
		if len(h) != 32 {
			return nil, time.Time{}, fmt.Errorf("path %d: value_hash is %d bytes, want 32", pid, len(h))
		}
		open[pid] = [32]byte(h)
		if vf.After(maxValidFrom) {
			maxValidFrom = vf
		}
	}
	return open, maxValidFrom, rows.Err()
}

// maxClosedValidTo returns the greatest valid_to among the node's CLOSED
// intervals (valid_to <> 'infinity'), or the zero time when the node has none.
// It backs the overlap guard's second bound: a new interval must not open before
// the newest closed interval's end. Served by fact_history_node_closed_idx.
func (s *Store) maxClosedValidTo(ctx context.Context, tx pgx.Tx, nodeID int64) (time.Time, error) {
	var vt pgtype.Timestamptz
	if err := tx.QueryRow(ctx,
		`SELECT max(valid_to) FROM fact_history
		  WHERE node_id = $1 AND valid_to <> 'infinity'`, nodeID).Scan(&vt); err != nil {
		return time.Time{}, fmt.Errorf("read max closed valid_to: %w", err)
	}
	if !vt.Valid { // no closed intervals -> SQL NULL
		return time.Time{}, nil
	}
	return vt.Time, nil
}

// ── Volatile apply (task 2.5) ────────────────────────────────────────────────

// upsertVolatile overwrites the node's single latest-only volatile blob.
func (s *Store) upsertVolatile(ctx context.Context, tx pgx.Tx, nodeID int64, blob json.RawMessage, observedAt time.Time) error {
	_, err := tx.Exec(ctx,
		`INSERT INTO node_volatile (node_id, volatile, observed_at)
		 VALUES ($1, $2::jsonb, $3)
		 ON CONFLICT (node_id) DO UPDATE
		   SET volatile = EXCLUDED.volatile, observed_at = EXCLUDED.observed_at`,
		nodeID, string(blob), observedAt)
	return err
}

// ── Per-node atomic apply (ADR-0009 §3) ──────────────────────────────────────

// PendingLeaf is one classified Durable leaf before interning: its dotted path
// and first segment (for the fact_paths dictionary), the raw jsonb value, and the
// content hash. ApplySnapshot interns these into DurableLeaf under the pool.
type PendingLeaf struct {
	Path, FactName string
	Value          json.RawMessage
	Hash           [32]byte
}

// ApplySnapshot is the per-node serialized atomic apply (ADR-0009 §3): it owns
// the whole transaction and the ordering invariant that keeps it deadlock-free.
//
// Interning runs on the pool (autocommit) BEFORE the per-node tx opens — never
// while holding the tx connection, or concurrent lock-waiters could exhaust the
// pool and deadlock the lock winner mid-intern. The single tx then holds the
// FOR UPDATE lock to run the authoritative guards, the durable diff, the volatile
// upsert, and the watermark advance.
//
// The cheap unlocked PeekNode pre-check is a best-effort optimization: it skips
// interning for a push that is ALREADY stale/deactivated, so the common rejected
// case doesn't grow the shared, never-GC'd fact_paths dictionary. It is NOT
// authoritative — a node that turns stale/deactivated between the peek and the
// lock, or a first-contact node the peek can't see, may still intern before the
// locked guards reject it; the per-node cardinality alarm (task 7.2) backstops
// that residual dictionary growth.
//
// Returns ErrDeactivated / ErrStale for the guard rejections; any other error is
// an internal failure with the failing step wrapped.
func (s *Store) ApplySnapshot(ctx context.Context, certname string, received, producerTS time.Time, maxSkew time.Duration, pending []PendingLeaf, volBlob json.RawMessage, clean bool) (ApplyStats, error) {
	// Cheap non-locking pre-check before interning (the authoritative guards
	// re-run under the lock below). It also rejects a far-past first-contact push
	// here, BEFORE interning, so a backdated push cannot grow the shared
	// never-GC'd fact_paths dictionary with orphaned paths (its node row would be
	// rolled back, leaving the interned paths invisible to the cardinality alarm).
	if peek, ok, err := s.PeekNode(ctx, certname); err == nil {
		switch {
		case ok && peek.Deactivated != nil:
			return ApplyStats{}, ErrDeactivated
		case ok && peek.LastProducerTS != nil && !producerTS.After(*peek.LastProducerTS):
			return ApplyStats{}, ErrStale
		case !ok && producerTS.Before(received.Add(-maxSkew)):
			// Node does not exist yet: apparent first contact. The locked guard
			// (keyed on the authoritative `inserted`) is the backstop for a race.
			return ApplyStats{}, ErrSkewed
		}
	}

	// Intern on the pool BEFORE opening the tx — never while holding the tx
	// connection, or concurrent lock-waiters could exhaust the pool and deadlock.
	durable := make([]DurableLeaf, 0, len(pending))
	for _, p := range pending {
		pid, err := s.internPath(ctx, p.Path, p.FactName)
		if err != nil {
			return ApplyStats{}, fmt.Errorf("intern path %q: %w", p.Path, err)
		}
		durable = append(durable, DurableLeaf{PathID: pid, Value: p.Value, Hash: p.Hash})
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return ApplyStats{}, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck // rollback after commit is a no-op

	node, inserted, err := s.lockNode(ctx, tx, certname)
	if err != nil {
		return ApplyStats{}, fmt.Errorf("lock node: %w", err)
	}
	if node.Deactivated != nil {
		return ApplyStats{}, ErrDeactivated
	}
	if node.LastProducerTS != nil && !producerTS.After(*node.LastProducerTS) {
		return ApplyStats{}, ErrStale
	}
	// First-contact past bound (task 2.1): a genuinely new node with a
	// reset/skewed clock could otherwise backdate arbitrarily old history at
	// fleet-wide at-T queries. Keyed on the authoritative `inserted` (this call
	// created the row) — NOT on a nil watermark, which is also true after an
	// operator ResetProducerTS, so a watermark-reset recovery push is not wedged.
	// Rolls back cleanly (the lockNode INSERT is undone with the tx), so a
	// rejected first push persists no node row. Delayed non-first-contact pushes
	// are unaffected (bounded by the watermark / the overlap guard).
	if inserted && producerTS.Before(received.Add(-maxSkew)) {
		return ApplyStats{}, ErrSkewed
	}

	stats, err := s.applyDurable(ctx, tx, node.ID, durable, producerTS, clean)
	if err != nil {
		if errors.Is(err, errStaleApply) {
			return ApplyStats{}, ErrStale
		}
		return ApplyStats{}, fmt.Errorf("apply durable: %w", err)
	}
	if err := s.upsertVolatile(ctx, tx, node.ID, volBlob, producerTS); err != nil {
		return ApplyStats{}, fmt.Errorf("upsert volatile: %w", err)
	}
	if err := s.markContact(ctx, tx, node.ID, received, &producerTS); err != nil {
		return ApplyStats{}, fmt.Errorf("mark contact: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return ApplyStats{}, fmt.Errorf("commit: %w", err)
	}
	return stats, nil
}
