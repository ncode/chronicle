package store

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
)

// testDSN returns the integration database connstring, skipping the test if
// CHRONICLE_TEST_DB is unset.
func testDSN(t *testing.T) string {
	t.Helper()
	dsn := os.Getenv("CHRONICLE_TEST_DB")
	if dsn == "" {
		t.Skip("set CHRONICLE_TEST_DB (a pgx connstring) to run store integration tests")
	}
	return dsn
}

// testStore connects to CHRONICLE_TEST_DB (a pgx connstring) and runs migrations.
// Without that env var the whole suite self-skips, so `go test ./...` stays
// green on a machine with no Postgres.
func testStore(t *testing.T) (*Store, context.Context) {
	t.Helper()
	dsn := testDSN(t)
	ctx := context.Background()
	s, err := Open(ctx, dsn, 0)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := s.Migrate(ctx); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	t.Cleanup(s.Close)
	return s, ctx
}

// freshNode wipes and recreates a node so each test is deterministic on reruns.
func freshNode(t *testing.T, s *Store, ctx context.Context, certname string) int64 {
	t.Helper()
	if _, err := s.pool.Exec(ctx, `DELETE FROM nodes WHERE certname=$1`, certname); err != nil {
		t.Fatalf("wipe node: %v", err)
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	n, _, err := s.lockNode(ctx, tx, certname)
	if err != nil {
		t.Fatalf("lock node: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	return n.ID
}

func mkLeaf(t *testing.T, s *Store, ctx context.Context, path, jsonVal string) DurableLeaf {
	t.Helper()
	name, _, _ := strings.Cut(path, ".")
	pid, err := s.internPath(ctx, path, name)
	if err != nil {
		t.Fatalf("intern %q: %v", path, err)
	}
	v := parseNum(t, jsonVal)
	raw, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return DurableLeaf{PathID: pid, Value: raw, Hash: ValueHash(v)}
}

func apply(t *testing.T, s *Store, ctx context.Context, nodeID int64, leaves []DurableLeaf, at time.Time, clean bool) ApplyStats {
	t.Helper()
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	st, err := s.applyDurable(ctx, tx, nodeID, leaves, at, clean)
	if err != nil {
		t.Fatalf("apply: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit: %v", err)
	}
	return st
}

var (
	t1 = time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC)
	t2 = time.Date(2026, 1, 1, 11, 0, 0, 0, time.UTC)
	t3 = time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
)

func TestApplyIntervalLifecycle(t *testing.T) {
	s, ctx := testStore(t)
	node := freshNode(t, s, ctx, "lifecycle.example")

	// T1: first observation -> one open interval.
	st := apply(t, s, ctx, node, []DurableLeaf{mkLeaf(t, s, ctx, "os.name", `"Debian"`)}, t1, true)
	if st.Opened != 1 || st.Closed != 0 || st.Unchanged != 0 {
		t.Fatalf("first apply stats = %+v", st)
	}

	// T2: changed value -> close old, open new (one transaction).
	st = apply(t, s, ctx, node, []DurableLeaf{mkLeaf(t, s, ctx, "os.name", `"Ubuntu"`)}, t2, true)
	if st.Opened != 1 || st.Closed != 1 {
		t.Fatalf("change apply stats = %+v", st)
	}

	// T3: same value re-applied -> change-only no-op.
	st = apply(t, s, ctx, node, []DurableLeaf{mkLeaf(t, s, ctx, "os.name", `"Ubuntu"`)}, t3, true)
	if st.Unchanged != 1 || st.Opened != 0 || st.Closed != 0 {
		t.Fatalf("unchanged apply stats = %+v", st)
	}

	// Now -> current value is Ubuntu, single open interval.
	now, err := s.Now(ctx, node)
	if err != nil {
		t.Fatal(err)
	}
	if len(now) != 1 || string(now[0].Value) != `"Ubuntu"` {
		t.Fatalf("now = %+v", now)
	}

	// State at a time between T1 and T2 -> Debian.
	at, err := s.StateAt(ctx, node, t1.Add(30*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if len(at) != 1 || string(at[0].Value) != `"Debian"` {
		t.Fatalf("state-at-T = %+v", at)
	}

	// Diff across the change window -> old closed + new opened.
	diff, err := s.Diff(ctx, node, t1.Add(time.Minute), t3)
	if err != nil {
		t.Fatal(err)
	}
	var opened, closed int
	for _, d := range diff {
		if d.OpenedInWindow {
			opened++
		}
		if d.ClosedInWindow {
			closed++
		}
	}
	if opened != 1 || closed != 1 {
		t.Fatalf("diff opened=%d closed=%d rows=%+v", opened, closed, diff)
	}
}

func TestTombstoneOnCleanAbsence(t *testing.T) {
	s, ctx := testStore(t)
	node := freshNode(t, s, ctx, "tombstone.example")

	apply(t, s, ctx, node, []DurableLeaf{mkLeaf(t, s, ctx, "rpm_packages.bash", `"5.1"`)}, t1, true)

	// Clean discovery, leaf gone -> tombstone (close, no reopen).
	st := apply(t, s, ctx, node, nil, t2, true)
	if st.Tombstoned != 1 {
		t.Fatalf("expected 1 tombstone, got %+v", st)
	}
	now, err := s.Now(ctx, node)
	if err != nil {
		t.Fatal(err)
	}
	if len(now) != 0 {
		t.Fatalf("tombstoned fact still current: %+v", now)
	}
	// The tombstone appears in the diff as a close with no matching open.
	diff, err := s.Diff(ctx, node, t1.Add(time.Minute), t3)
	if err != nil {
		t.Fatal(err)
	}
	if len(diff) != 1 || !diff[0].ClosedInWindow || diff[0].OpenedInWindow {
		t.Fatalf("tombstone diff = %+v", diff)
	}
}

func TestCarryForwardOnDirtyDiscovery(t *testing.T) {
	s, ctx := testStore(t)
	node := freshNode(t, s, ctx, "carry.example")

	apply(t, s, ctx, node, []DurableLeaf{mkLeaf(t, s, ctx, "rpm_packages.bash", `"5.1"`)}, t1, true)

	// Discovery FAILED this cycle (clean=false) and the leaf is absent ->
	// carry forward, do NOT tombstone.
	st := apply(t, s, ctx, node, nil, t2, false)
	if st.Tombstoned != 0 {
		t.Fatalf("dirty discovery must not tombstone: %+v", st)
	}
	now, err := s.Now(ctx, node)
	if err != nil {
		t.Fatal(err)
	}
	if len(now) != 1 {
		t.Fatalf("carried-forward fact missing: %+v", now)
	}
}

func TestOpenIntervalIntegrityBoundary(t *testing.T) {
	s, ctx := testStore(t)
	node := freshNode(t, s, ctx, "integrity.example")
	pid, err := s.internPath(ctx, "os.name", "os")
	if err != nil {
		t.Fatal(err)
	}
	h := ValueHash(parseNum(t, `"x"`))
	ins := func(from time.Time) error {
		_, err := s.pool.Exec(ctx,
			`INSERT INTO fact_history (node_id, path_id, value, value_hash, valid_from)
			 VALUES ($1,$2,'"x"'::jsonb,$3,$4)`, node, pid, h[:], from)
		return err
	}
	if err := ins(t1); err != nil {
		t.Fatalf("first open insert: %v", err)
	}
	// Second open interval for the same (node, path) must be rejected by the
	// partial unique index fact_history_open_uniq.
	err = ins(t2)
	if !isPgCode(err, "23505") {
		t.Fatalf("expected unique_violation, got %v", err)
	}
}

func TestCheckConstraintRejectsInvertedInterval(t *testing.T) {
	s, ctx := testStore(t)
	node := freshNode(t, s, ctx, "check.example")
	pid, err := s.internPath(ctx, "os.name", "os")
	if err != nil {
		t.Fatal(err)
	}
	h := ValueHash(parseNum(t, `"x"`))
	// valid_from == valid_to violates CHECK (valid_from < valid_to).
	_, err = s.pool.Exec(ctx,
		`INSERT INTO fact_history (node_id, path_id, value, value_hash, valid_from, valid_to)
		 VALUES ($1,$2,'"x"'::jsonb,$3,$4,$4)`, node, pid, h[:], t1)
	if !isPgCode(err, "23514") {
		t.Fatalf("expected check_violation, got %v", err)
	}
}

func isPgCode(err error, code string) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == code
}
