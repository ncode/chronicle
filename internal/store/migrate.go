package store

import (
	"context"
	"embed"
	"fmt"
	"io/fs"
	"sort"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed migrations/*.sql
var migrationsFS embed.FS

// Migrate applies any embedded migrations not yet recorded, each in its own
// transaction, in version order. Idempotent: already-applied versions are skipped.
//
// ponytail: each migration runs inside one transaction. A future migration that
// needs CREATE INDEX CONCURRENTLY (which cannot run in a tx — task 2.1) will need
// a "-- no-transaction" marker; add that when the second real migration exists.
// migrateLockKey is a fixed advisory-lock key so that, when several stateless
// replicas start against one database, only one runs migrations at a time
// (ADR-0009 §5); the others block here, then find every version already applied.
const migrateLockKey int64 = 0x6368726f6e696c31 // "chronicl1"

func (s *Store) Migrate(ctx context.Context) error {
	// Hold a session advisory lock on a dedicated connection for the whole run,
	// and do ALL migration work on that one connection. Going back to the pool
	// (e.g. s.pool.Exec) while holding this connection would self-deadlock at
	// pool_max_conns=1: the lock winner would block waiting for a second conn the
	// pool cannot hand out (ADR-0009 §5).
	conn, err := s.pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("acquire migration conn: %w", err)
	}
	defer conn.Release()
	if _, err := conn.Exec(ctx, `SELECT pg_advisory_lock($1)`, migrateLockKey); err != nil {
		return fmt.Errorf("acquire migration lock: %w", err)
	}
	defer conn.Exec(ctx, `SELECT pg_advisory_unlock($1)`, migrateLockKey) //nolint:errcheck // best-effort

	if _, err := conn.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS schema_migrations (
			version    int         PRIMARY KEY,
			applied_at timestamptz NOT NULL DEFAULT now()
		)`); err != nil {
		return fmt.Errorf("ensure schema_migrations: %w", err)
	}

	migs, err := loadMigrations()
	if err != nil {
		return err
	}
	for _, m := range migs {
		var exists bool
		if err := conn.QueryRow(ctx,
			`SELECT EXISTS (SELECT 1 FROM schema_migrations WHERE version=$1)`, m.version,
		).Scan(&exists); err != nil {
			return fmt.Errorf("check migration %d: %w", m.version, err)
		}
		if exists {
			continue
		}
		if err := applyOne(ctx, conn, m); err != nil {
			return fmt.Errorf("apply migration %d (%s): %w", m.version, m.name, err)
		}
	}
	return nil
}

func applyOne(ctx context.Context, conn *pgxpool.Conn, m migration) error {
	tx, err := conn.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx) //nolint:errcheck // rollback after commit is a no-op
	if _, err := tx.Exec(ctx, m.sql); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx,
		`INSERT INTO schema_migrations (version) VALUES ($1)`, m.version); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

type migration struct {
	version int
	name    string
	sql     string
}

// loadMigrations reads the embedded migrations/*.sql files.
func loadMigrations() ([]migration, error) {
	return loadMigrationsFS(migrationsFS, "migrations")
}

// loadMigrationsFS reads dir/*.sql from fsys, parsing the leading integer of
// each filename (NNNN_name.sql) as the version, sorted ascending. Two files with
// the same version number are a hard error (an ambiguous, silently-skipped
// migration is worse than a failed startup). Split from loadMigrations so the
// duplicate-version guard is testable with an in-memory FS.
func loadMigrationsFS(fsys fs.FS, dir string) ([]migration, error) {
	entries, err := fs.ReadDir(fsys, dir)
	if err != nil {
		return nil, err
	}
	var migs []migration
	seen := make(map[int]string)
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".sql") {
			continue
		}
		num, _, ok := strings.Cut(e.Name(), "_")
		if !ok {
			return nil, fmt.Errorf("migration %q must be NNNN_name.sql", e.Name())
		}
		v, err := strconv.Atoi(num)
		if err != nil {
			return nil, fmt.Errorf("migration %q has non-numeric version: %w", e.Name(), err)
		}
		if prev, dup := seen[v]; dup {
			return nil, fmt.Errorf("duplicate migration version %d: %q and %q", v, prev, e.Name())
		}
		seen[v] = e.Name()
		b, err := fs.ReadFile(fsys, dir+"/"+e.Name())
		if err != nil {
			return nil, err
		}
		migs = append(migs, migration{version: v, name: e.Name(), sql: string(b)})
	}
	sort.Slice(migs, func(i, j int) bool { return migs[i].version < migs[j].version })
	return migs, nil
}
