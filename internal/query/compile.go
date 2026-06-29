package query

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/ncode/chronicle/internal/ingest"
	"github.com/ncode/chronicle/internal/store"
)

// GroupCount is one row of a group-by result.
type GroupCount struct {
	Value json.RawMessage `json:"value"`
	Count int             `json:"count"`
}

// Result is a DSL query result; exactly one of Nodes/Groups is set per Shape.
type Result struct {
	Shape  Shape        `json:"-"`
	Nodes  []string     `json:"nodes,omitempty"`
	Groups []GroupCount `json:"groups,omitempty"`
}

// argBuf accumulates positional query args and hands out $N placeholders.
type argBuf struct{ args []any }

func (a *argBuf) p(v any) string {
	a.args = append(a.args, v)
	return "$" + strconv.Itoa(len(a.args))
}

// Run executes a parsed query: validate, resolve paths (the only I/O), compile to
// SQL (a pure transformation), then execute. includeInactive re-includes
// deactivated/expired nodes (default excludes them — fact-query spec).
func (e *Engine) Run(ctx context.Context, q *Query, includeInactive bool) (*Result, error) {
	if err := e.checkVolatileHistory(q); err != nil {
		return nil, err
	}
	paths, err := e.resolvePaths(ctx, q)
	if err != nil {
		return nil, err
	}
	sql, args, empty, err := compile(q, e.classifier, paths, includeInactive)
	if err != nil {
		return nil, err
	}
	if empty { // a term/group references an unknown path -> no node can match
		return &Result{Shape: q.Shape}, nil
	}
	switch q.Shape {
	case ShapeFilter:
		return e.execFilter(ctx, sql, args)
	case ShapeGroupBy:
		return e.execGroupBy(ctx, sql, args)
	default:
		return nil, ErrUnsupported
	}
}

// resolvePaths turns every path the query references into its path_id — the only
// database I/O in the read path, hoisted here so compile stays pure.
//
// Resolution is eager (every referenced path, up front). Unlike the old lazy
// short-circuit, a genuine LookupPath DB error surfaces as a query error instead
// of a silent empty result — deliberate: don't mask a failing database. (A plain
// not-found is still just a miss → unknown path → empty result, unchanged.)
//
// ponytail: resolves blindly without classifying. A Volatile path was never
// interned into fact_paths, so LookupPath simply misses and the path is absent
// from the map; compile then routes it to node_volatile on its own. Keeping
// classification solely in compile is the point of the seam — don't re-add it here.
func (e *Engine) resolvePaths(ctx context.Context, q *Query) (map[string]int64, error) {
	paths := make(map[string]int64)
	resolve := func(p string) error {
		if _, done := paths[p]; done {
			return nil
		}
		pid, ok, err := e.store.LookupPath(ctx, p)
		if err != nil {
			return err
		}
		if ok {
			paths[p] = pid
		}
		return nil
	}
	for _, t := range q.Terms {
		if err := resolve(t.Path); err != nil {
			return nil, err
		}
	}
	if q.Shape == ShapeGroupBy {
		if err := resolve(q.GroupField); err != nil {
			return nil, err
		}
	}
	return paths, nil
}

// checkVolatileHistory enforces: a volatile path with an explicit `at <T>` has
// no history (volatile is latest-only — ADR-0007/0008).
func (e *Engine) checkVolatileHistory(q *Query) error {
	if q.At == nil {
		return nil // `now` (or omitted) is fine; volatile routes to node_volatile
	}
	for _, t := range q.Terms {
		if e.classifier.IsVolatile(t.Path) {
			return fmt.Errorf("%w: %q", ErrNoHistory, t.Path)
		}
	}
	if q.Shape == ShapeGroupBy && e.classifier.IsVolatile(q.GroupField) {
		return fmt.Errorf("%w: %q", ErrNoHistory, q.GroupField)
	}
	return nil
}

// compile turns a parsed Query into SQL + positional args. It is a pure function:
// no context, no database, no store — every path_id it needs is supplied in the
// resolved `paths` map. empty is true when a term or group-by references an
// unknown durable path, so the query can match nothing and no SQL should run.
// This is the read surface's test surface: assert the SQL string, no Postgres.
func compile(q *Query, cl *ingest.Classifier, paths map[string]int64, includeInactive bool) (sql string, args []any, empty bool, err error) {
	ab := &argBuf{}
	switch q.Shape {
	case ShapeFilter:
		intersect, empty := buildIntersect(ab, cl, paths, q.Terms, q.At)
		if empty {
			return "", nil, true, nil
		}
		s := "SELECT n.certname FROM nodes n WHERE n.node_id IN (" + intersect + ")" +
			activeClause("n", includeInactive) + " ORDER BY n.certname"
		return s, ab.args, false, nil
	case ShapeGroupBy:
		s, empty := buildGroupBySQL(ab, q, cl, paths, includeInactive)
		if empty {
			return "", nil, true, nil
		}
		return s, ab.args, false, nil
	default:
		return "", nil, false, ErrUnsupported
	}
}

func buildGroupBySQL(ab *argBuf, q *Query, cl *ingest.Classifier, paths map[string]int64, includeInactive bool) (sql string, empty bool) {
	intersect, empty := buildIntersect(ab, cl, paths, q.Terms, q.At)
	if empty {
		return "", true
	}

	var src, selectExpr, groupCols, pathPred, idCol, existPred string
	if cl.IsVolatile(q.GroupField) {
		// at != nil already rejected; now-only volatile group from node_volatile.
		src = "node_volatile gv"
		selectExpr = "gv.volatile -> " + ab.p(q.GroupField)
		groupCols = selectExpr
		idCol = "gv.node_id"
		// Key-existence: don't count nodes lacking the field under a NULL group
		// (`?` distinguishes a missing key from a real JSON null value).
		existPred = "gv.volatile ? " + ab.p(q.GroupField)
	} else {
		pid, ok := paths[q.GroupField]
		if !ok {
			return "", true // unknown group path -> no groups
		}
		selectExpr = "gf.value"
		// Group by value_hash so type-distinct look-alikes (1 vs 1.0) don't merge.
		groupCols = "gf.value_hash, gf.value"
		idCol = "gf.node_id"
		pathPred = "gf.path_id = " + ab.p(pid)
		if q.At == nil {
			src = "current_facts gf"
		} else {
			src = "facts_at(" + ab.p(*q.At) + ") gf" // through the view, not raw fact_history
		}
	}

	var b strings.Builder
	fmt.Fprintf(&b, "SELECT %s, count(*) FROM %s JOIN nodes n ON n.node_id = %s WHERE %s IN (%s)",
		selectExpr, src, idCol, idCol, intersect)
	if pathPred != "" {
		b.WriteString(" AND ")
		b.WriteString(pathPred)
	}
	if existPred != "" {
		b.WriteString(" AND ")
		b.WriteString(existPred)
	}
	b.WriteString(activeClause("n", includeInactive))
	fmt.Fprintf(&b, " GROUP BY %s ORDER BY count(*) DESC, %s", groupCols, selectExpr)
	return b.String(), false
}

// buildIntersect builds an INTERSECT of per-term node_id subqueries. empty is
// true if any durable term references an unknown path (so nothing can match).
func buildIntersect(ab *argBuf, cl *ingest.Classifier, paths map[string]int64, terms []Term, at *time.Time) (sql string, empty bool) {
	parts := make([]string, 0, len(terms))
	for _, t := range terms {
		sub, empty := termSubquery(ab, cl, paths, t, at)
		if empty {
			return "", true
		}
		parts = append(parts, "("+sub+")")
	}
	return strings.Join(parts, " INTERSECT "), false
}

func termSubquery(ab *argBuf, cl *ingest.Classifier, paths map[string]int64, t Term, at *time.Time) (sql string, empty bool) {
	if cl.IsVolatile(t.Path) {
		jsonVal, _ := json.Marshal(t.Value)
		return "SELECT node_id FROM node_volatile WHERE volatile -> " + ab.p(t.Path) +
			" = " + ab.p(string(jsonVal)) + "::jsonb", false
	}
	pid, ok := paths[t.Path]
	if !ok {
		return "", true
	}
	h := store.ValueHash(t.Value)
	if at == nil {
		return "SELECT node_id FROM current_facts WHERE path_id = " + ab.p(pid) +
			" AND value_hash = " + ab.p(h[:]), false
	}
	// at-T goes through facts_at(), not raw fact_history (fact-query spec).
	return "SELECT node_id FROM facts_at(" + ab.p(*at) + ") WHERE path_id = " + ab.p(pid) +
		" AND value_hash = " + ab.p(h[:]), false
}

func (e *Engine) execFilter(ctx context.Context, sql string, args []any) (*Result, error) {
	rows, err := e.store.Pool().Query(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	res := &Result{Shape: ShapeFilter}
	for rows.Next() {
		var cn string
		if err := rows.Scan(&cn); err != nil {
			return nil, err
		}
		res.Nodes = append(res.Nodes, cn)
	}
	return res, rows.Err()
}

func (e *Engine) execGroupBy(ctx context.Context, sql string, args []any) (*Result, error) {
	rows, err := e.store.Pool().Query(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	res := &Result{Shape: ShapeGroupBy}
	for rows.Next() {
		var g GroupCount
		if err := rows.Scan(&g.Value, &g.Count); err != nil {
			return nil, err
		}
		res.Groups = append(res.Groups, g)
	}
	return res, rows.Err()
}

func activeClause(alias string, includeInactive bool) string {
	if includeInactive {
		return ""
	}
	return " AND " + alias + ".deactivated IS NULL AND " + alias + ".expired IS NULL"
}
