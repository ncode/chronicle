package ingest

import (
	"fmt"
	"regexp"
	"strings"
)

// Classifier decides Durable vs Volatile by matching a leaf path against the
// configured Volatile glob patterns (ADR-0007: Durable by default). Patterns are
// compiled once; `*` matches any run of characters (including dots), `?` matches
// one. So `load*` matches load, load.1m; `memory.system.*` matches the subtree;
// `uptime` is exact.
type Classifier struct {
	res []*regexp.Regexp
}

func NewClassifier(patterns []string) (*Classifier, error) {
	res := make([]*regexp.Regexp, 0, len(patterns))
	for _, p := range patterns {
		re, err := regexp.Compile(globToRegex(p))
		if err != nil {
			return nil, fmt.Errorf("volatile pattern %q: %w", p, err)
		}
		res = append(res, re)
	}
	return &Classifier{res: res}, nil
}

// IsVolatile reports whether a leaf path matches any Volatile pattern.
func (c *Classifier) IsVolatile(path string) bool {
	for _, re := range c.res {
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
