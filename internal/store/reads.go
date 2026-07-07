package store

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
)

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

// CountRow is one (key, count) result from the monitor scans — a path and its
// interval-open count for HighChurn, a certname and its distinct-path count for
// FactPathCardinality.
type CountRow struct {
	Key   string
	Count int64
}

// HighChurn returns durable paths that opened at least threshold intervals
// since the caller-computed lower bound.
func (s *Store) HighChurn(ctx context.Context, since time.Time, threshold int64) ([]CountRow, error) {
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
func (s *Store) FactPathCardinality(ctx context.Context, threshold int64) ([]CountRow, error) {
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

func scanCountRows(rows pgx.Rows) ([]CountRow, error) {
	defer rows.Close()
	var out []CountRow
	for rows.Next() {
		var r CountRow
		if err := rows.Scan(&r.Key, &r.Count); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}
