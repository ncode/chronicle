package store

import (
	"bytes"
	"os"
	"testing"
)

// TestEmbeddedSchemaMatchesDocs guards against drift between the human-canonical
// schema (docs/schema/v1.sql, cited by the ADRs) and the embedded runnable
// migration. They must stay byte-identical (modulo surrounding whitespace).
func TestEmbeddedSchemaMatchesDocs(t *testing.T) {
	embedded, err := migrationsFS.ReadFile("migrations/0001_init.sql")
	if err != nil {
		t.Fatal(err)
	}
	docs, err := os.ReadFile("../../docs/schema/v1.sql")
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(bytes.TrimSpace(embedded), bytes.TrimSpace(docs)) {
		t.Fatal("internal/store/migrations/0001_init.sql drifted from docs/schema/v1.sql; keep them identical")
	}
}
