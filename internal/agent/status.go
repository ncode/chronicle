package agent

import (
	"os"
	"path/filepath"
	"strings"

	"github.com/ncode/chronicle/internal/wire"
)

// buildStatus assembles the discovery-status report (node-agent spec) from the
// resolved tree, the configured external dirs, and the joined discovery error.
// Built-in namespaces present in the tree are `ok`; external fact-files present
// on disk are `ok`; sources named in the joined error are `error`. A removed
// external file is simply omitted (not `error`), so the server's discovery-clean
// gate tombstones it. Invariant: Clean() == (discoverErr == nil) — any error
// makes the pass dirty (carry-forward), which is exactly the gate the server
// needs (ADR-0009 §1).
func buildStatus(tree map[string]any, externalDirs []string, discoverErr error) wire.DiscoveryStatus {
	st := wire.DiscoveryStatus{
		Builtin:  make(map[string]string, len(tree)),
		External: make(map[string]string),
	}
	for k := range tree {
		st.Builtin[k] = wire.StatusOK
	}

	present := make(map[string]struct{})
	for _, dir := range externalDirs {
		entries, err := os.ReadDir(dir)
		if err != nil {
			if os.IsNotExist(err) {
				continue // genuinely absent dir: facts skips it silently, so do we
			}
			// Unreadable dir (permissions, I/O): we can't tell which facts it
			// would produce, so keep the pass dirty to carry leaves forward
			// rather than risk tombstoning temporarily-inaccessible facts.
			st.Builtin["_discovery"] = wire.StatusError
			continue
		}
		for _, e := range entries {
			if e.IsDir() || !isFactFile(e.Name()) {
				continue
			}
			p := filepath.Join(dir, e.Name())
			st.External[p] = wire.StatusOK
			present[p] = struct{}{}
		}
	}

	if discoverErr != nil {
		for _, e := range flattenErrors(discoverErr) {
			msg := e.Error()
			if name, ok := factNameFromError(msg); ok {
				st.Builtin[name] = wire.StatusError
				continue
			}
			if p, ok := externalPathFromError(msg, present); ok {
				st.External[p] = wire.StatusError
				continue
			}
			st.Builtin["_discovery"] = wire.StatusError // unattributed: still dirty
		}
		if st.Clean() { // belt-and-suspenders: a non-nil error must read as dirty
			st.Builtin["_discovery"] = wire.StatusError
		}
	}
	return st
}

// flattenErrors expands an errors.Join tree into its leaf errors.
func flattenErrors(err error) []error {
	if err == nil {
		return nil
	}
	if u, ok := err.(interface{ Unwrap() []error }); ok {
		var out []error
		for _, e := range u.Unwrap() {
			out = append(out, flattenErrors(e)...)
		}
		return out
	}
	return []error{err}
}

// factNameFromError recognizes the `fact <name>: ...` wrapping facts uses for a
// registered/built-in resolver failure (engine.go).
func factNameFromError(msg string) (string, bool) {
	const prefix = "fact "
	if !strings.HasPrefix(msg, prefix) {
		return "", false
	}
	rest := msg[len(prefix):]
	i := strings.IndexByte(rest, ':')
	if i <= 0 {
		return "", false
	}
	return rest[:i], true
}

// externalPathFromError matches a joined external-fact error to a present file.
func externalPathFromError(msg string, present map[string]struct{}) (string, bool) {
	for p := range present {
		if strings.Contains(msg, p) || strings.Contains(msg, filepath.Base(p)) {
			return p, true
		}
	}
	return "", false
}

// isFactFile is a best-effort filter for external fact sources (observability
// only; the gate's correctness does not depend on it). It skips hidden files and
// known non-fact suffixes; everything else (txt/json/yaml/executable) counts.
func isFactFile(name string) bool {
	if strings.HasPrefix(name, ".") {
		return false
	}
	switch filepath.Ext(name) {
	case ".bak", ".orig", ".rb":
		return false
	}
	return true
}
