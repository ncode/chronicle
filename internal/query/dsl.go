// Package query is the read surface (ADR-0008, 0010): a small purpose-built DSL
// over the temporal views, the node_diff endpoint, and people/automation auth
// (OIDC + static tokens, never node certs).
package query

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// Shape is which of the two DSL forms a query is.
type Shape int

const (
	ShapeFilter  Shape = iota // path=value path=value ...           -> matching nodes
	ShapeGroupBy              // <field> where <terms> group by <field> -> (value, count)
)

// Term is one `path = value` equality. Value is the typed literal (json.Number
// for numerics, string, bool, or nil) so its value_hash matches stored facts.
type Term struct {
	Path  string
	Value any
}

// Query is a parsed DSL statement.
type Query struct {
	Shape      Shape
	Terms      []Term     // filter terms (both shapes)
	GroupField string     // ShapeGroupBy: the group-by path
	At         *time.Time // nil => now
}

// Errors surfaced to callers (mapped to typed HTTP responses).
var (
	ErrBadQuery    = fmt.Errorf("bad query")
	ErrNoHistory   = fmt.Errorf("no history: volatile facts are latest-only")
	ErrUnsupported = fmt.Errorf("unsupported query shape in v1")
)

// Parse parses a DSL string into a Query. Grammar:
//
//	filter   := term (WS term)*
//	groupby  := field "where" term (WS term)* "group" "by" field
//	query    := (filter | groupby) ("at" timestamp)?
//	term     := path "=" literal
//	timestamp:= "now" | RFC3339
func Parse(s string) (*Query, error) {
	toks, err := tokenize(s)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrBadQuery, err)
	}
	if len(toks) == 0 {
		return nil, fmt.Errorf("%w: empty query", ErrBadQuery)
	}

	q := &Query{}

	// Split off a trailing `at <T>` qualifier.
	if i := indexTok(toks, "at"); i >= 0 {
		if i != len(toks)-2 {
			return nil, fmt.Errorf("%w: `at` must be followed by exactly one timestamp", ErrBadQuery)
		}
		at, err := parseTime(toks[i+1])
		if err != nil {
			return nil, fmt.Errorf("%w: %v", ErrBadQuery, err)
		}
		q.At = at
		toks = toks[:i]
	}

	if i := indexTok(toks, "where"); i >= 0 {
		return parseGroupBy(q, toks, i)
	}
	return parseFilter(q, toks)
}

func parseFilter(q *Query, toks []string) (*Query, error) {
	q.Shape = ShapeFilter
	for _, t := range toks {
		term, err := parseTerm(t)
		if err != nil {
			return nil, err
		}
		q.Terms = append(q.Terms, term)
	}
	if len(q.Terms) == 0 {
		return nil, fmt.Errorf("%w: filter needs at least one term", ErrBadQuery)
	}
	return q, nil
}

func parseGroupBy(q *Query, toks []string, whereIdx int) (*Query, error) {
	q.Shape = ShapeGroupBy
	if whereIdx != 1 {
		return nil, fmt.Errorf("%w: expected `<field> where ...`", ErrBadQuery)
	}
	projectField := toks[0]

	gi := indexTok(toks, "group")
	if gi < 0 || gi+2 >= len(toks) || toks[gi+1] != "by" {
		return nil, fmt.Errorf("%w: expected `group by <field>`", ErrBadQuery)
	}
	q.GroupField = toks[gi+2]
	if gi+3 != len(toks) {
		return nil, fmt.Errorf("%w: trailing tokens after group-by field", ErrBadQuery)
	}
	// v1: project field must equal the group-by field (ADR-0008 scope).
	if projectField != q.GroupField {
		return nil, fmt.Errorf("%w: project field %q must equal group-by field %q in v1",
			ErrUnsupported, projectField, q.GroupField)
	}

	for _, t := range toks[whereIdx+1 : gi] {
		term, err := parseTerm(t)
		if err != nil {
			return nil, err
		}
		q.Terms = append(q.Terms, term)
	}
	if len(q.Terms) == 0 {
		return nil, fmt.Errorf("%w: group-by needs a where predicate", ErrBadQuery)
	}
	return q, nil
}

func parseTerm(tok string) (Term, error) {
	i := strings.IndexByte(tok, '=')
	if i <= 0 {
		return Term{}, fmt.Errorf("%w: %q is not path=value", ErrBadQuery, tok)
	}
	return Term{Path: tok[:i], Value: parseLiteral(tok[i+1:])}, nil
}

// parseLiteral types a value: quoted -> string, true|false|null -> bool/nil,
// numeric -> json.Number (so 1 != "1" and the hash matches stored facts), else
// a bare string.
func parseLiteral(s string) any {
	if len(s) >= 2 {
		if (s[0] == '\'' && s[len(s)-1] == '\'') || (s[0] == '"' && s[len(s)-1] == '"') {
			return s[1 : len(s)-1]
		}
	}
	switch s {
	case "true":
		return true
	case "false":
		return false
	case "null":
		return nil
	}
	if _, err := strconv.ParseFloat(s, 64); err == nil {
		return json.Number(s)
	}
	return s
}

func parseTime(s string) (*time.Time, error) {
	if s == "now" {
		return nil, nil
	}
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		return nil, fmt.Errorf("invalid timestamp %q (want RFC3339 or `now`)", s)
	}
	return &t, nil
}

func indexTok(toks []string, kw string) int {
	for i, t := range toks {
		if t == kw {
			return i
		}
	}
	return -1
}

// tokenize splits on whitespace, keeping quoted spans (single or double quotes)
// intact so a value like 'Debian GNU/Linux' stays one token.
func tokenize(s string) ([]string, error) {
	var toks []string
	var cur strings.Builder
	var quote rune
	flush := func() {
		if cur.Len() > 0 {
			toks = append(toks, cur.String())
			cur.Reset()
		}
	}
	for _, r := range s {
		switch {
		case quote != 0:
			cur.WriteRune(r)
			if r == quote {
				quote = 0
			}
		case r == '\'' || r == '"':
			quote = r
			cur.WriteRune(r)
		case r == ' ' || r == '\t' || r == '\n' || r == '\r':
			flush()
		default:
			cur.WriteRune(r)
		}
	}
	if quote != 0 {
		return nil, fmt.Errorf("unbalanced quote")
	}
	flush()
	return toks, nil
}
