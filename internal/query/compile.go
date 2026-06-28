package query

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

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

// Run compiles and executes a parsed query. includeInactive re-includes
// deactivated/expired nodes (default excludes them — fact-query spec).
func (e *Engine) Run(ctx context.Context, q *Query, includeInactive bool) (*Result, error) {
	if err := e.checkVolatileHistory(q); err != nil {
		return nil, err
	}
	switch q.Shape {
	case ShapeFilter:
		return e.runFilter(ctx, q, includeInactive)
	case ShapeGroupBy:
		return e.runGroupBy(ctx, q, includeInactive)
	default:
		return nil, ErrUnsupported
	}
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

func (e *Engine) runFilter(ctx context.Context, q *Query, includeInactive bool) (*Result, error) {
	ab := &argBuf{}
	intersect, empty, err := e.buildIntersect(ctx, ab, q.Terms, q.At)
	if err != nil {
		return nil, err
	}
	if empty { // a term references an unknown path -> no node can match
		return &Result{Shape: ShapeFilter}, nil
	}
	sql := "SELECT n.certname FROM nodes n WHERE n.node_id IN (" + intersect + ")" +
		activeClause("n", includeInactive) + " ORDER BY n.certname"

	rows, err := e.store.Pool().Query(ctx, sql, ab.args...)
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

func (e *Engine) runGroupBy(ctx context.Context, q *Query, includeInactive bool) (*Result, error) {
	ab := &argBuf{}
	intersect, empty, err := e.buildIntersect(ctx, ab, q.Terms, q.At)
	if err != nil {
		return nil, err
	}
	if empty {
		return &Result{Shape: ShapeGroupBy}, nil
	}

	var src, selectExpr, groupCols, pathPred, idCol, existPred string
	if e.classifier.IsVolatile(q.GroupField) {
		// at != nil already rejected; now-only volatile group from node_volatile.
		src = "node_volatile gv"
		selectExpr = "gv.volatile -> " + ab.p(q.GroupField)
		groupCols = selectExpr
		idCol = "gv.node_id"
		// Key-existence: don't count nodes lacking the field under a NULL group
		// (`?` distinguishes a missing key from a real JSON null value).
		existPred = "gv.volatile ? " + ab.p(q.GroupField)
	} else {
		pid, ok, err := e.store.LookupPath(ctx, q.GroupField)
		if err != nil {
			return nil, err
		}
		if !ok {
			return &Result{Shape: ShapeGroupBy}, nil // unknown group path -> no groups
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

	rows, err := e.store.Pool().Query(ctx, b.String(), ab.args...)
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

// buildIntersect builds an INTERSECT of per-term node_id subqueries. empty is
// true if any durable term references an unknown path (so nothing can match).
func (e *Engine) buildIntersect(ctx context.Context, ab *argBuf, terms []Term, at *time.Time) (string, bool, error) {
	parts := make([]string, 0, len(terms))
	for _, t := range terms {
		sub, empty, err := e.termSubquery(ctx, ab, t, at)
		if err != nil {
			return "", false, err
		}
		if empty {
			return "", true, nil
		}
		parts = append(parts, "("+sub+")")
	}
	return strings.Join(parts, " INTERSECT "), false, nil
}

func (e *Engine) termSubquery(ctx context.Context, ab *argBuf, t Term, at *time.Time) (sql string, empty bool, err error) {
	if e.classifier.IsVolatile(t.Path) {
		jsonVal, _ := json.Marshal(t.Value)
		return "SELECT node_id FROM node_volatile WHERE volatile -> " + ab.p(t.Path) +
			" = " + ab.p(string(jsonVal)) + "::jsonb", false, nil
	}
	pid, ok, err := e.store.LookupPath(ctx, t.Path)
	if err != nil {
		return "", false, err
	}
	if !ok {
		return "", true, nil
	}
	h := store.ValueHash(t.Value)
	if at == nil {
		return "SELECT node_id FROM current_facts WHERE path_id = " + ab.p(pid) +
			" AND value_hash = " + ab.p(h[:]), false, nil
	}
	// at-T goes through facts_at(), not raw fact_history (fact-query spec).
	return "SELECT node_id FROM facts_at(" + ab.p(*at) + ") WHERE path_id = " + ab.p(pid) +
		" AND value_hash = " + ab.p(h[:]), false, nil
}

func activeClause(alias string, includeInactive bool) string {
	if includeInactive {
		return ""
	}
	return " AND " + alias + ".deactivated IS NULL AND " + alias + ".expired IS NULL"
}
