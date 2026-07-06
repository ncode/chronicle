package wire

import (
	"bytes"
	"encoding/json"
	"sort"
	"testing"
)

func treeOf(t *testing.T, s string) map[string]any {
	t.Helper()
	d := json.NewDecoder(bytes.NewReader([]byte(s)))
	d.UseNumber()
	var m map[string]any
	if err := d.Decode(&m); err != nil {
		t.Fatal(err)
	}
	return m
}

func TestFlatten(t *testing.T) {
	tree := treeOf(t, `{
		"os": {"name": "Debian", "release": {"major": 12}},
		"networking": {"interfaces": {"eth0": {"address": "10.0.0.1"}}},
		"dns": {"servers": ["8.8.8.8", "8.8.4.4"]},
		"uptime": 12345,
		"empty": {}
	}`)
	leaves, err := Flatten(tree, FlattenLimits{})
	if err != nil {
		t.Fatal(err)
	}

	got := map[string]string{}
	names := map[string]string{}
	for _, lf := range leaves {
		raw, _ := json.Marshal(lf.Value)
		got[lf.Path] = string(raw)
		names[lf.Path] = lf.FactName
	}

	want := map[string]string{
		"os.name":                            `"Debian"`,
		"os.release.major":                   `12`,
		"networking.interfaces.eth0.address": `"10.0.0.1"`,
		"dns.servers":                        `["8.8.8.8","8.8.4.4"]`, // array is a leaf
		"uptime":                             `12345`,
		"empty":                              `{}`, // empty object kept as a leaf
	}
	if len(got) != len(want) {
		keys := make([]string, 0, len(got))
		for k := range got {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		t.Fatalf("leaf set = %v, want %d leaves", keys, len(want))
	}
	for p, v := range want {
		if got[p] != v {
			t.Errorf("%s = %s, want %s", p, got[p], v)
		}
	}
	if names["networking.interfaces.eth0.address"] != "networking" {
		t.Errorf("fact_name = %q, want networking", names["networking.interfaces.eth0.address"])
	}
}

// A literal dotted key and a nested path that flatten to the same leaf are a
// deterministic reject, not a map-order last-wins.
func TestFlattenRejectsCollision(t *testing.T) {
	tree := treeOf(t, `{"a":{"b":1},"a.b":2}`)
	_, err := Flatten(tree, FlattenLimits{})
	ce, ok := err.(*CollisionError)
	if !ok {
		t.Fatalf("want *CollisionError, got %v", err)
	}
	if ce.Path != "a.b" {
		t.Fatalf("collision path = %q, want a.b", ce.Path)
	}
}

// The leaf-count and path-length caps are enforced during the walk and abort at
// the first violation with a typed CapError naming the cap.
func TestFlattenCaps(t *testing.T) {
	if _, err := Flatten(treeOf(t, `{"a":1,"b":2,"c":3}`), FlattenLimits{MaxLeafCount: 1}); !isCap(err, "leaf-count") {
		t.Fatalf("want leaf-count cap error, got %v", err)
	}
	if _, err := Flatten(treeOf(t, `{"verylongkey":1}`), FlattenLimits{MaxPathLen: 8}); !isCap(err, "path-length") {
		t.Fatalf("want path-length cap error, got %v", err)
	}
	// A body under the caps is unaffected.
	if _, err := Flatten(treeOf(t, `{"a":1}`), FlattenLimits{MaxLeafCount: 10, MaxPathLen: 10}); err != nil {
		t.Fatalf("within-cap body must not error, got %v", err)
	}
}

func isCap(err error, which string) bool {
	ce, ok := err.(*CapError)
	return ok && ce.Which == which
}

// Flattening the same tree twice yields the same leaf set and values, so an
// unchanged fact can never flip value between pushes.
func TestFlattenDeterministic(t *testing.T) {
	src := `{"os":{"name":"Debian","release":{"major":12}},"uptime":123,"dns":{"servers":["a","b"]}}`
	first, err := Flatten(treeOf(t, src), FlattenLimits{})
	if err != nil {
		t.Fatal(err)
	}
	second, err := Flatten(treeOf(t, src), FlattenLimits{})
	if err != nil {
		t.Fatal(err)
	}
	got := func(ls []Leaf) map[string]string {
		m := map[string]string{}
		for _, lf := range ls {
			raw, _ := json.Marshal(lf.Value)
			m[lf.Path] = string(raw)
		}
		return m
	}
	a, b := got(first), got(second)
	if len(a) != len(b) {
		t.Fatalf("leaf counts differ: %d vs %d", len(a), len(b))
	}
	for p, v := range a {
		if b[p] != v {
			t.Errorf("%s = %s then %s", p, v, b[p])
		}
	}
}
