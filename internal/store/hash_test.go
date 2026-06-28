package store

import (
	"encoding/json"
	"strings"
	"testing"
)

// parseNum decodes JSON the way the ingest path must: numbers as json.Number so
// 1 and 1.0 stay distinct.
func parseNum(t *testing.T, s string) any {
	t.Helper()
	d := json.NewDecoder(strings.NewReader(s))
	d.UseNumber()
	var v any
	if err := d.Decode(&v); err != nil {
		t.Fatalf("decode %q: %v", s, err)
	}
	return v
}

func TestValueHashDistinguishesTypesAndNumbers(t *testing.T) {
	// Every literal here must hash distinctly (temporal-store spec: 1 != "1" != 1.0).
	lits := []string{`1`, `"1"`, `1.0`, `1.00`, `2`, `true`, `false`, `null`, `"true"`, `"1.0"`, `[]`, `{}`, `0`}
	seen := map[[32]byte]string{}
	for _, s := range lits {
		h := ValueHash(parseNum(t, s))
		if prev, ok := seen[h]; ok {
			t.Fatalf("collision: %s and %s hash equal", prev, s)
		}
		seen[h] = s
	}
}

func TestValueHashObjectKeyOrderIndependent(t *testing.T) {
	a := ValueHash(parseNum(t, `{"a":1,"b":2,"c":[1,2,3]}`))
	b := ValueHash(parseNum(t, `{"c":[1,2,3],"b":2,"a":1}`))
	if a != b {
		t.Fatal("object key order must not change the hash")
	}
}

func TestValueHashArrayUnambiguousAndOrdered(t *testing.T) {
	// length-prefixing: ["ab","c"] must not collide with ["a","bc"]
	if ValueHash(parseNum(t, `["ab","c"]`)) == ValueHash(parseNum(t, `["a","bc"]`)) {
		t.Fatal("length-prefix failed: arrays collide")
	}
	// arrays are ordered: [1,2] != [2,1]
	if ValueHash(parseNum(t, `[1,2]`)) == ValueHash(parseNum(t, `[2,1]`)) {
		t.Fatal("array element order must matter")
	}
}

func TestValueHashStable(t *testing.T) {
	lit := `{"interfaces":{"eth0":{"mac":"00:11","mtu":1500}}}`
	a := ValueHash(parseNum(t, lit))
	b := ValueHash(parseNum(t, lit))
	if a != b {
		t.Fatal("hash must be deterministic across independent decodes")
	}
}
