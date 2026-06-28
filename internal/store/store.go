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
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
)

// DB is the subset of pgx satisfied by both *pgxpool.Pool and pgx.Tx, so the
// same helper works inside or outside a transaction.
type DB interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// Store wraps a pgx pool plus an in-process path-interning cache.
type Store struct {
	pool *pgxpool.Pool

	pathMu    sync.RWMutex
	pathCache map[string]int64 // path_text -> path_id
}

// Open dials Postgres and verifies connectivity.
func Open(ctx context.Context, dsn string) (*Store, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("connect postgres: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping postgres: %w", err)
	}
	return &Store{pool: pool, pathCache: make(map[string]int64)}, nil
}

// Pool exposes the underlying pool for transaction management by callers (ingest
// opens the per-node serialized tx itself).
func (s *Store) Pool() *pgxpool.Pool { return s.pool }

func (s *Store) Close() { s.pool.Close() }

// ── Path interning (task 2.2) ───────────────────────────────────────────────

// InternPath returns the path_id for a leaf path, inserting it if new. Runs on
// the pool (autocommit), independent of any per-node ingest transaction, so path
// creation never contends with node row locks. Cache-first: the path set is
// low-cardinality and never deleted.
//
// ponytail: unbounded cache — fact_paths cardinality is bounded by the fleet's
// distinct leaf paths and the cardinality alarm (task 7.2) catches a runaway.
func (s *Store) InternPath(ctx context.Context, pathText, factName string) (int64, error) {
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

// LockNode creates the node on first contact and returns its row with a
// FOR UPDATE lock held in tx, serializing concurrent pushes for one certname
// (ADR-0009 §3). Identity is the verified cert CN; same CN = one history.
func (s *Store) LockNode(ctx context.Context, tx pgx.Tx, certname string) (Node, error) {
	if _, err := tx.Exec(ctx,
		`INSERT INTO nodes (certname) VALUES ($1) ON CONFLICT (certname) DO NOTHING`,
		certname); err != nil {
		return Node{}, fmt.Errorf("ensure node %q: %w", certname, err)
	}
	n := Node{Certname: certname}
	if err := tx.QueryRow(ctx,
		`SELECT node_id, last_producer_ts, deactivated, expired
		   FROM nodes WHERE certname = $1 FOR UPDATE`, certname,
	).Scan(&n.ID, &n.LastProducerTS, &n.Deactivated, &n.Expired); err != nil {
		return Node{}, fmt.Errorf("lock node %q: %w", certname, err)
	}
	return n, nil
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

// MarkContact advances last_seen (server clock) and, when ts is non-nil, the
// applied-snapshot watermark last_producer_ts. Clears expired (a returning push
// un-expires — ADR-0011). Call inside the per-node tx after a successful apply.
func (s *Store) MarkContact(ctx context.Context, tx pgx.Tx, nodeID int64, now time.Time, producerTS *time.Time) error {
	_, err := tx.Exec(ctx,
		`UPDATE nodes
		    SET last_seen = $2,
		        last_producer_ts = COALESCE($3, last_producer_ts),
		        expired = NULL
		  WHERE node_id = $1`, nodeID, now, producerTS)
	return err
}

// ErrNodeNotFound is returned by operator actions for an unknown certname.
var ErrNodeNotFound = errors.New("node not found")

// ErrStaleApply means the push's observation time is not strictly newer than the
// node's newest stored interval, so applying it would create a degenerate
// interval. Ingest maps it to a stale reject (not a 500).
var ErrStaleApply = errors.New("producer_timestamp not after newest stored interval")

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
// returning push clears `expired` via MarkContact). Skips already-expired and
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

// ApplyDurable applies the whole Durable snapshot for one node at time t inside
// the caller's transaction (which already holds the per-node lock and has run
// the staleness/skew guards). It is change-only: unchanged leaves write nothing,
// changed leaves close-then-open, new leaves open. Leaves currently open but
// absent from leaves are tombstoned ONLY when discoveryClean is true; otherwise
// they are carried forward (ADR-0009 §1 absence semantics).
//
// Read-free for present leaves: it reads the node's open set once, diffs in Go,
// and batches the minimal close/open/tombstone statements in one round trip.
func (s *Store) ApplyDurable(ctx context.Context, tx pgx.Tx, nodeID int64, leaves []DurableLeaf, t time.Time, discoveryClean bool) (ApplyStats, error) {
	var stats ApplyStats

	open, maxValidFrom, err := s.openHashes(ctx, tx, nodeID)
	if err != nil {
		return stats, err
	}

	// Reject a push that is not strictly newer than the node's newest stored
	// interval: closing at t <= an open interval's valid_from would write
	// valid_to <= valid_from and trip CHECK (valid_from < valid_to). Normally the
	// caller's stale guard already ensures t is past last_producer_ts (>= every
	// valid_from); this also covers a watermark cleared by ResetProducerTS on a
	// node whose open intervals carry a far-future valid_from.
	if len(open) > 0 && !t.After(maxValidFrom) {
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

// ── Volatile apply (task 2.5) ────────────────────────────────────────────────

// UpsertVolatile overwrites the node's single latest-only volatile blob.
func (s *Store) UpsertVolatile(ctx context.Context, tx pgx.Tx, nodeID int64, blob json.RawMessage, observedAt time.Time) error {
	_, err := tx.Exec(ctx,
		`INSERT INTO node_volatile (node_id, volatile, observed_at)
		 VALUES ($1, $2::jsonb, $3)
		 ON CONFLICT (node_id) DO UPDATE
		   SET volatile = EXCLUDED.volatile, observed_at = EXCLUDED.observed_at`,
		nodeID, string(blob), observedAt)
	return err
}

// ── Reconstruction reads (task 2.6) ──────────────────────────────────────────

// FactRow is one reconstructed Durable fact. Open is true for the current value
// (valid_to = 'infinity'); ValidTo is meaningful only when Open is false.
type FactRow struct {
	PathID    int64           `json:"-"`
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
		`SELECT path_id, path_text, fact_name, value, valid_from, 'infinity'::timestamptz
		   FROM current_facts WHERE node_id = $1 ORDER BY path_text`, nodeID)
	if err != nil {
		return nil, err
	}
	return scanFacts(rows)
}

// StateAt reconstructs a node's Durable facts as of time t.
func (s *Store) StateAt(ctx context.Context, nodeID int64, t time.Time) ([]FactRow, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT path_id, path_text, fact_name, value, valid_from, valid_to
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
		if err := rows.Scan(&f.PathID, &f.Path, &f.FactName, &f.Value, &f.ValidFrom, &vt); err != nil {
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
	PathID         int64           `json:"-"`
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
		`SELECT path_id, path_text, fact_name, value, valid_from, valid_to,
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
		if err := rows.Scan(&d.PathID, &d.Path, &d.FactName, &d.Value,
			&d.ValidFrom, &vt, &d.OpenedInWindow, &d.ClosedInWindow); err != nil {
			return nil, err
		}
		d.ValidTo, d.Open = closeTime(vt)
		out = append(out, d)
	}
	return out, rows.Err()
}
