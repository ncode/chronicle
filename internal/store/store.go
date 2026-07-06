// Package store is the PostgreSQL temporal change-only data model (ADR-0003,
// 0004): interned leaf paths, inline jsonb values + value_hash, the fact_history
// temporal table with its open-interval integrity boundary, and the latest-only
// node_volatile blob. It owns the schema and the close/open apply primitive;
// ingest and lifecycle layer policy on top.
package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Store wraps a pgx pool plus an in-process path-interning cache.
type Store struct {
	pool *pgxpool.Pool

	pathMu    sync.RWMutex
	pathCache map[string]int64 // path_text -> path_id
}

// Open dials Postgres and verifies connectivity. maxConns caps the pool so the
// backpressure bound and the migration runner's advisory-lock connection have a
// known, validated ceiling (config.PoolMaxConns); a non-positive value keeps
// pgx's default sizing.
func Open(ctx context.Context, dsn string, maxConns int) (*Store, error) {
	pcfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse postgres dsn: %w", err)
	}
	if maxConns > 0 {
		pcfg.MaxConns = int32(maxConns)
	}
	pool, err := pgxpool.NewWithConfig(ctx, pcfg)
	if err != nil {
		return nil, fmt.Errorf("connect postgres: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping postgres: %w", err)
	}
	return &Store{pool: pool, pathCache: make(map[string]int64)}, nil
}

// Pool exposes the underlying pool for sanctioned raw reads and test setup.
// ApplySnapshot owns the per-node serialized transaction.
func (s *Store) Pool() *pgxpool.Pool { return s.pool }

func (s *Store) Close() { s.pool.Close() }

// ── Path interning (task 2.2) ───────────────────────────────────────────────

// internPath returns the path_id for a leaf path, inserting it if new. Runs on
// the pool (autocommit), independent of any per-node ingest transaction, so path
// creation never contends with node row locks. Cache-first: the path set is
// low-cardinality and never deleted.
//
// ponytail: unbounded cache — fact_paths cardinality is bounded by the fleet's
// distinct leaf paths and the cardinality alarm (task 7.2) catches a runaway.
func (s *Store) internPath(ctx context.Context, pathText, factName string) (int64, error) {
	s.pathMu.RLock()
	id, ok := s.pathCache[pathText]
	s.pathMu.RUnlock()
	if ok {
		return id, nil
	}
	if _, err := s.pool.Exec(ctx,
		`INSERT INTO fact_paths (path_text, fact_name) VALUES ($1, $2)
		 ON CONFLICT (path_text) DO NOTHING`, pathText, factName); err != nil {
		return 0, fmt.Errorf("intern path %q: %w", pathText, err)
	}
	if err := s.pool.QueryRow(ctx,
		`SELECT path_id FROM fact_paths WHERE path_text = $1`, pathText).Scan(&id); err != nil {
		return 0, fmt.Errorf("read path id %q: %w", pathText, err)
	}
	s.pathMu.Lock()
	s.pathCache[pathText] = id
	s.pathMu.Unlock()
	return id, nil
}

// LookupPath returns the path_id for a path_text without creating it (read path).
func (s *Store) LookupPath(ctx context.Context, pathText string) (int64, bool, error) {
	var id int64
	err := s.pool.QueryRow(ctx, `SELECT path_id FROM fact_paths WHERE path_text=$1`, pathText).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, err
	}
	return id, true, nil
}

// NodeID resolves a certname to its node_id (read path).
func (s *Store) NodeID(ctx context.Context, certname string) (int64, bool, error) {
	var id int64
	err := s.pool.QueryRow(ctx, `SELECT node_id FROM nodes WHERE certname=$1`, certname).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, err
	}
	return id, true, nil
}

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

// ErrStaleApply means the push's observation time is not strictly newer than the
// node's newest stored interval, so applying it would create a degenerate
// interval. Ingest maps it to a stale reject (not a 500).
var ErrStaleApply = errors.New("producer_timestamp not after newest stored interval")

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
		return stats, ErrStaleApply
	}
	if !maxClosedValidTo.IsZero() && t.Before(maxClosedValidTo) {
		return stats, ErrStaleApply
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
		if errors.Is(err, ErrStaleApply) {
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

// ── Reconstruction reads (task 2.6) ──────────────────────────────────────────

// FactRow is one reconstructed Durable fact. Open is true for the current value
// (valid_to = 'infinity'); ValidTo is meaningful only when Open is false.
type FactRow struct {
	Path      string          `json:"path"`
	FactName  string          `json:"fact_name"`
	Value     json.RawMessage `json:"value"`
	ValidFrom time.Time       `json:"valid_from"`
	ValidTo   time.Time       `json:"valid_to,omitzero"`
	Open      bool            `json:"open"`
}

// Now returns a node's current Durable facts (open intervals).
func (s *Store) Now(ctx context.Context, nodeID int64) ([]FactRow, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT path_text, fact_name, value, valid_from, 'infinity'::timestamptz
		   FROM current_facts WHERE node_id = $1 ORDER BY path_text`, nodeID)
	if err != nil {
		return nil, err
	}
	return scanFacts(rows)
}

// Volatile returns a node's latest-only volatile blob and its observation time.
// ok is false when the node has never pushed a volatile blob. Volatile facts are
// current-only (ADR-0007), so there is no at-T variant.
func (s *Store) Volatile(ctx context.Context, nodeID int64) (json.RawMessage, time.Time, bool, error) {
	var blob json.RawMessage
	var observedAt time.Time
	err := s.pool.QueryRow(ctx,
		`SELECT volatile, observed_at FROM node_volatile WHERE node_id = $1`, nodeID).
		Scan(&blob, &observedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, time.Time{}, false, nil
	}
	if err != nil {
		return nil, time.Time{}, false, err
	}
	return blob, observedAt, true, nil
}

// StateAt reconstructs a node's Durable facts as of time t.
func (s *Store) StateAt(ctx context.Context, nodeID int64, t time.Time) ([]FactRow, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT path_text, fact_name, value, valid_from, valid_to
		   FROM node_state_at($1, $2) ORDER BY path_text`, nodeID, t)
	if err != nil {
		return nil, err
	}
	return scanFacts(rows)
}

func scanFacts(rows pgx.Rows) ([]FactRow, error) {
	defer rows.Close()
	var out []FactRow
	for rows.Next() {
		var f FactRow
		var vt pgtype.Timestamptz
		if err := rows.Scan(&f.Path, &f.FactName, &f.Value, &f.ValidFrom, &vt); err != nil {
			return nil, err
		}
		f.ValidTo, f.Open = closeTime(vt)
		out = append(out, f)
	}
	return out, rows.Err()
}

// closeTime maps a possibly-infinite timestamptz to (closeTime, open). An
// 'infinity' valid_to means the interval is still open.
func closeTime(vt pgtype.Timestamptz) (time.Time, bool) {
	if vt.InfinityModifier == pgtype.Infinity {
		return time.Time{}, true
	}
	return vt.Time, false
}

// DiffRow is one interval that opened or closed within a Diff window. A pure
// deletion has ClosedInWindow true, OpenedInWindow false, Open false. Open is
// true for an interval that opened in-window and is still current.
type DiffRow struct {
	Path           string          `json:"path"`
	FactName       string          `json:"fact_name"`
	Value          json.RawMessage `json:"value"`
	ValidFrom      time.Time       `json:"valid_from"`
	ValidTo        time.Time       `json:"valid_to,omitzero"`
	Open           bool            `json:"open"`
	OpenedInWindow bool            `json:"opened_in_window"`
	ClosedInWindow bool            `json:"closed_in_window"`
}

// Diff returns the intervals that opened OR closed in [t1, t2) for a node,
// including tombstones (ADR-0003).
func (s *Store) Diff(ctx context.Context, nodeID int64, t1, t2 time.Time) ([]DiffRow, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT path_text, fact_name, value, valid_from, valid_to,
		        opened_in_window, closed_in_window
		   FROM node_diff($1, $2, $3) ORDER BY path_text, valid_from`, nodeID, t1, t2)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []DiffRow
	for rows.Next() {
		var d DiffRow
		var vt pgtype.Timestamptz
		if err := rows.Scan(&d.Path, &d.FactName, &d.Value,
			&d.ValidFrom, &vt, &d.OpenedInWindow, &d.ClosedInWindow); err != nil {
			return nil, err
		}
		d.ValidTo, d.Open = closeTime(vt)
		out = append(out, d)
	}
	return out, rows.Err()
}

type countRow struct {
	Key   string
	Count int64
}

// HighChurn returns durable paths that opened at least threshold intervals
// since the caller-computed lower bound.
func (s *Store) HighChurn(ctx context.Context, since time.Time, threshold int64) ([]countRow, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT fp.path_text, count(*) AS opens
		FROM   fact_history fh JOIN fact_paths fp USING (path_id)
		WHERE  fh.valid_from > $1
		GROUP  BY fp.path_text
		HAVING count(*) >= $2
		ORDER  BY opens DESC
		LIMIT  20`, since, threshold)
	if err != nil {
		return nil, err
	}
	return scanCountRows(rows)
}

// FactPathCardinality returns nodes with at least threshold distinct durable
// fact paths.
func (s *Store) FactPathCardinality(ctx context.Context, threshold int64) ([]countRow, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT n.certname, count(DISTINCT fh.path_id) AS paths
		FROM   fact_history fh JOIN nodes n USING (node_id)
		GROUP  BY n.certname
		HAVING count(DISTINCT fh.path_id) >= $1
		ORDER  BY paths DESC
		LIMIT  20`, threshold)
	if err != nil {
		return nil, err
	}
	return scanCountRows(rows)
}

func scanCountRows(rows pgx.Rows) ([]countRow, error) {
	defer rows.Close()
	var out []countRow
	for rows.Next() {
		var r countRow
		if err := rows.Scan(&r.Key, &r.Count); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}
