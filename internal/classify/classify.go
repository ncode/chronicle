// Package classify is the Durable/Volatile policy (ADR-0007): it decides whether
// a leaf path is a Volatile fact (latest-only, never historised) or Durable
// (versioned over time). Shared by ingest (write-side classification) and query
// (read-side routing and at-T rejection) so the two endpoints never disagree —
// the policy belongs to neither the write path nor the read path.
package classify

import (
	"fmt"
	"regexp"
	"strings"
)

// Policy decides Durable vs Volatile by matching a leaf path against the
// configured Volatile glob patterns (ADR-0007: Durable by default). Patterns are
// compiled once; `*` matches any run of characters (including dots), `?` matches
// one. So `load*` matches load, load.1m; `memory.system.*` matches the subtree;
// `uptime` is exact.
type Policy struct {
	res []*regexp.Regexp
}

// New compiles the Volatile glob patterns into a Policy.
func New(patterns []string) (*Policy, error) {
	res := make([]*regexp.Regexp, 0, len(patterns))
	for _, p := range patterns {
		re, err := regexp.Compile(globToRegex(p))
		if err != nil {
			return nil, fmt.Errorf("volatile pattern %q: %w", p, err)
		}
		res = append(res, re)
	}
	return &Policy{res: res}, nil
}

// IsVolatile reports whether a leaf path matches any Volatile pattern.
func (p *Policy) IsVolatile(path string) bool {
	for _, re := range p.res {
		if re.MatchString(path) {
			return true
		}
	}
	return false
}

func globToRegex(glob string) string {
	var b strings.Builder
	b.WriteByte('^')
	for _, r := range glob {
		switch r {
		case '*':
			b.WriteString("(?s:.*)") // (?s): a newline in a path segment can't dodge the glob
		case '?':
			b.WriteString("(?s:.)")
		default:
			b.WriteString(regexp.QuoteMeta(string(r)))
		}
	}
	b.WriteByte('$')
	return b.String()
}
