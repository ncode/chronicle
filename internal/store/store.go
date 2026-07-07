// Package store is the PostgreSQL temporal change-only data model (ADR-0003,
// 0004): interned leaf paths, inline jsonb values + value_hash, the fact_history
// temporal table with its open-interval integrity boundary, and the latest-only
// node_volatile blob. It owns the schema and the close/open apply primitive;
// ingest and lifecycle layer policy on top.
package store

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/jackc/pgx/v5"
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
