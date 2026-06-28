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
	leaves := Flatten(tree)

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
