package store

import (
	"context"
	"embed"
	"fmt"
	"io/fs"
	"sort"
	"strconv"
	"strings"
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
	// Hold a session advisory lock on a dedicated connection for the whole run.
	conn, err := s.pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("acquire migration conn: %w", err)
	}
	defer conn.Release()
	if _, err := conn.Exec(ctx, `SELECT pg_advisory_lock($1)`, migrateLockKey); err != nil {
		return fmt.Errorf("acquire migration lock: %w", err)
	}
	defer conn.Exec(ctx, `SELECT pg_advisory_unlock($1)`, migrateLockKey) //nolint:errcheck // best-effort

	if _, err := s.pool.Exec(ctx, `
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
		if err := s.pool.QueryRow(ctx,
			`SELECT EXISTS (SELECT 1 FROM schema_migrations WHERE version=$1)`, m.version,
		).Scan(&exists); err != nil {
			return fmt.Errorf("check migration %d: %w", m.version, err)
		}
		if exists {
			continue
		}
		if err := s.applyOne(ctx, m); err != nil {
			return fmt.Errorf("apply migration %d (%s): %w", m.version, m.name, err)
		}
	}
	return nil
}

func (s *Store) applyOne(ctx context.Context, m migration) error {
	tx, err := s.pool.Begin(ctx)
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

// loadMigrations reads the embedded migrations/*.sql files, parsing the leading
// integer of each filename (NNNN_name.sql) as the version, sorted ascending.
func loadMigrations() ([]migration, error) {
	entries, err := fs.ReadDir(migrationsFS, "migrations")
	if err != nil {
		return nil, err
	}
	var migs []migration
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
		b, err := migrationsFS.ReadFile("migrations/" + e.Name())
		if err != nil {
			return nil, err
		}
		migs = append(migs, migration{version: v, name: e.Name(), sql: string(b)})
	}
	sort.Slice(migs, func(i, j int) bool { return migs[i].version < migs[j].version })
	return migs, nil
}
