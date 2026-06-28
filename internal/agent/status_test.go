package agent

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func TestBuildStatus(t *testing.T) {
	dir := t.TempDir()
	rpm := filepath.Join(dir, "rpm.sh")
	if err := os.WriteFile(rpm, []byte("#!/bin/sh\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	tree := map[string]any{
		"os":         map[string]any{"name": "Debian"},
		"networking": map[string]any{"hostname": "web01"},
	}

	t.Run("clean", func(t *testing.T) {
		st := buildStatus(tree, []string{dir}, nil)
		if !st.Clean() {
			t.Fatal("no error => clean")
		}
		if st.Builtin["os"] != "ok" || st.External[rpm] != "ok" {
			t.Fatalf("ok attribution wrong: %+v", st)
		}
	})

	t.Run("external present-but-failed => error => dirty", func(t *testing.T) {
		derr := errors.Join(fmt.Errorf("parse executable external fact %s: timeout", rpm))
		st := buildStatus(tree, []string{dir}, derr)
		if st.Clean() {
			t.Fatal("present-but-failed source must make the pass dirty")
		}
		if st.External[rpm] != "error" {
			t.Fatalf("expected %s=error, got %+v", rpm, st.External)
		}
	})

	t.Run("builtin resolver failure => namespace error", func(t *testing.T) {
		st := buildStatus(tree, []string{dir}, errors.Join(fmt.Errorf("fact networking: boom")))
		if st.Builtin["networking"] != "error" || st.Clean() {
			t.Fatalf("builtin error attribution wrong: %+v", st)
		}
	})

	t.Run("removed script omitted, stays clean", func(t *testing.T) {
		empty := t.TempDir()
		st := buildStatus(tree, []string{empty}, nil)
		if !st.Clean() {
			t.Fatal("absent file is not an error")
		}
		if _, ok := st.External[filepath.Join(empty, "rpm.sh")]; ok {
			t.Fatal("absent file must be omitted, not listed")
		}
	})

	t.Run("unattributed error still dirty", func(t *testing.T) {
		st := buildStatus(tree, []string{dir}, errors.Join(fmt.Errorf("cache write failed")))
		if st.Clean() {
			t.Fatal("any discovery error must read as dirty (carry-forward)")
		}
	})
}
