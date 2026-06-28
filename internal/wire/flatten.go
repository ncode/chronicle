package wire

// Leaf is one flattened fact: its dotted leaf path, the first path segment
// (fact_name), and the decoded JSON value (json.Number-aware so 1 != 1.0).
type Leaf struct {
	Path     string
	FactName string
	Value    any
}

// Flatten walks the nested fact tree to leaf dotted-paths (ADR-0004,
// flatten-in-Go). It recurses through non-empty objects only; a scalar, array,
// null, or empty object is a leaf. An empty object is kept as a leaf (value {})
// so a fact like `foo: {}` is not silently lost. Shared by the agent (payload
// pre-check) and ingest (classification + interning).
func Flatten(tree map[string]any) []Leaf {
	var out []Leaf
	var rec func(path, factName string, v any)
	rec = func(path, factName string, v any) {
		if m, ok := v.(map[string]any); ok && len(m) > 0 {
			for k, sub := range m {
				rec(path+"."+k, factName, sub)
			}
			return
		}
		out = append(out, Leaf{Path: path, FactName: factName, Value: v})
	}
	for k, v := range tree {
		rec(k, k, v) // factName is fixed to the first segment
	}
	return out
}
