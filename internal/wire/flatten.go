package wire

// Leaf is one flattened fact: its dotted leaf path, the first path segment
// (fact_name), and the decoded JSON value (json.Number-aware so 1 != 1.0).
type Leaf struct {
	Path     string
	FactName string
	Value    any
}

// FlattenLimits bounds a flatten walk so an over-cap snapshot is abandoned at
// the first violation instead of being fully materialized and then rejected. A
// zero field means "no limit" (the agent pre-check passes generous locals; the
// server passes its configured caps).
type FlattenLimits struct {
	MaxLeafCount int // max distinct leaf paths (0 = unlimited)
	MaxPathLen   int // max length of a single leaf path (0 = unlimited)
}

// CapError reports that a flatten walk tripped a resource cap. Which names the
// cap ("leaf-count" or "path-length") and matches the oversized reject suffix.
type CapError struct{ Which string }

func (e *CapError) Error() string { return "flatten exceeds cap: " + e.Which }

// CollisionError reports two distinct tree entries flattening to the same leaf
// path (e.g. a literal key "a.b" colliding with a nested a:{b:…}). Rejecting it
// keeps flattening deterministic instead of letting map order pick a winner.
type CollisionError struct{ Path string }

func (e *CollisionError) Error() string { return "colliding leaf path: " + e.Path }

// Flatten walks the nested fact tree to leaf dotted-paths (ADR-0004,
// flatten-in-Go). It recurses through non-empty objects only; a scalar, array,
// null, or empty object is a leaf. An empty object is kept as a leaf (value {})
// so a fact like `foo: {}` is not silently lost. Shared by the agent (payload
// pre-check) and ingest (classification + interning).
//
// The leaf SET and per-leaf values are deterministic — repeated flattens of the
// same tree yield the same paths and values — and two entries that flatten to
// the same leaf path are a *CollisionError (never map-order last-wins). The
// leaf-count/path-length caps in lim abort the walk at the first violation
// (*CapError) so a rejected oversized snapshot is never fully materialized. The
// ORDER of the returned slice follows Go map iteration and is not relied upon
// (the store dedups and diffs by path_id, order-independent).
func Flatten(tree map[string]any, lim FlattenLimits) ([]Leaf, error) {
	out := make([]Leaf, 0, len(tree))
	seen := make(map[string]struct{}, len(tree))
	var rec func(path, factName string, v any) error
	rec = func(path, factName string, v any) error {
		if lim.MaxPathLen > 0 && len(path) > lim.MaxPathLen {
			return &CapError{Which: "path-length"}
		}
		if m, ok := v.(map[string]any); ok && len(m) > 0 {
			for k, sub := range m {
				if err := rec(path+"."+k, factName, sub); err != nil {
					return err
				}
			}
			return nil
		}
		if _, dup := seen[path]; dup {
			return &CollisionError{Path: path}
		}
		seen[path] = struct{}{}
		if lim.MaxLeafCount > 0 && len(out) >= lim.MaxLeafCount {
			return &CapError{Which: "leaf-count"}
		}
		out = append(out, Leaf{Path: path, FactName: factName, Value: v})
		return nil
	}
	for k, v := range tree {
		if err := rec(k, k, v); err != nil { // factName is fixed to the first segment
			return nil, err
		}
	}
	return out, nil
}
