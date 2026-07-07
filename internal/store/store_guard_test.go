package store

import (
	"context"
	"errors"
	"testing"
	"time"
)

// applyErr runs one applyDurable and returns its error (the apply helper fatals
// instead), so the reject-path guards are assertable.
func applyErr(s *Store, ctx context.Context, nodeID int64, leaves []DurableLeaf, at time.Time, clean bool) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	if _, err := s.applyDurable(ctx, tx, nodeID, leaves, at, clean); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// A push whose observation time falls inside an already-CLOSED interval must be
// rejected even when the node has no open interval to trip the open-only guard —
// otherwise state-at-T would observe two conflicting values at one instant
// (task 1.2, the closed-interval overlap fix).
func TestClosedIntervalOverlapGuard(t *testing.T) {
	s, ctx := testStore(t)
	node := freshNode(t, s, ctx, "overlap.example")
	at10 := time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC)
	at15 := time.Date(2026, 1, 1, 15, 0, 0, 0, time.UTC)
	at20 := time.Date(2026, 1, 1, 20, 0, 0, 0, time.UTC)

	// Open [10:00, inf), then a clean-absence pass at 20:00 tombstones it: the
	// node now has ONE closed interval [10:00, 20:00) and NO open interval.
	apply(t, s, ctx, node, []DurableLeaf{mkLeaf(t, s, ctx, "os.name", `"A"`)}, at10, true)
	apply(t, s, ctx, node, nil, at20, true)

	// A push at 15:00 falls inside the closed [10:00, 20:00). The old open-only
	// guard would have accepted it (no open interval) and created an overlap; the
	// closed-interval bound rejects it as stale.
	if err := applyErr(s, ctx, node, []DurableLeaf{mkLeaf(t, s, ctx, "os.name", `"B"`)}, at15, true); !errors.Is(err, ErrStaleApply) {
		t.Fatalf("push at 15:00 into closed [10:00,20:00) = %v, want ErrStaleApply", err)
	}

	// A push exactly AT the closed valid_to (20:00) is adjacent, not overlapping
	// (half-open), so it is accepted and opens a fresh interval.
	if err := applyErr(s, ctx, node, []DurableLeaf{mkLeaf(t, s, ctx, "os.name", `"C"`)}, at20, true); err != nil {
		t.Fatalf("push exactly at closed valid_to must be accepted, got %v", err)
	}
	now, _ := s.Now(ctx, node)
	if len(now) != 1 || string(now[0].Value) != `"C"` {
		t.Fatalf("adjacent push should open exactly one C interval: %+v", now)
	}
}

// Migrations run entirely on the advisory-lock connection, so a pool sized to a
// single connection completes rather than self-deadlocking (task 3.2).
func TestMigratePoolMaxConnsOne(t *testing.T) {
	dsn := testDSN(t)
	ctx := context.Background()
	s, err := Open(ctx, dsn, 1)
	if err != nil {
		t.Fatalf("open with pool_max_conns=1: %v", err)
	}
	defer s.Close()
	if err := s.Migrate(ctx); err != nil {
		t.Fatalf("migrate with a single-connection pool must not deadlock: %v", err)
	}
}
