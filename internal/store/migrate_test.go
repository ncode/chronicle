package store

import (
	"strings"
	"testing"
	"testing/fstest"
)

// The real embedded migrations must load and be strictly version-ordered.
func TestLoadMigrationsOrdered(t *testing.T) {
	migs, err := loadMigrations()
	if err != nil {
		t.Fatalf("loadMigrations: %v", err)
	}
	if len(migs) < 2 {
		t.Fatalf("want at least 2 migrations, got %d", len(migs))
	}
	for i := 1; i < len(migs); i++ {
		if migs[i-1].version >= migs[i].version {
			t.Fatalf("migrations not strictly ascending: %d then %d", migs[i-1].version, migs[i].version)
		}
	}
}

// Two files sharing a version number are a startup error, not a silent skip.
func TestLoadMigrationsRejectsDuplicateVersion(t *testing.T) {
	fsys := fstest.MapFS{
		"m/0001_init.sql":  {Data: []byte("SELECT 1;")},
		"m/0001_again.sql": {Data: []byte("SELECT 2;")},
	}
	_, err := loadMigrationsFS(fsys, "m")
	if err == nil || !strings.Contains(err.Error(), "duplicate migration version 1") {
		t.Fatalf("want duplicate-version error, got %v", err)
	}
}
