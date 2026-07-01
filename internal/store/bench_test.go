package store

import (
	"bytes"
	"encoding/json"
	"testing"
)

func benchVal(b *testing.B, s string) any {
	b.Helper()
	d := json.NewDecoder(bytes.NewReader([]byte(s)))
	d.UseNumber()
	var v any
	if err := d.Decode(&v); err != nil {
		b.Fatal(err)
	}
	return v
}

// BenchmarkValueHash measures the content-address hash computed per durable
// leaf (sha256 + canonical encode). Runs once per durable leaf per push, so
// allocs/op here multiply by fleet leaf volume.
func BenchmarkValueHash(b *testing.B) {
	cases := map[string]string{
		"number": `12345`,
		"string": `"web-01.prod.example.com"`,
		"bool":   `true`,
		"array":  `["8.8.8.8","8.8.4.4","1.1.1.1"]`,
		"object": `{"ip":"10.0.4.21","netmask":"255.255.255.0","mac":"52:54:00:12:34:56","mtu":1500}`,
		"nested": `{"interfaces":{"eth0":{"ip":"10.0.4.21","mtu":1500},"eth1":{"ip":"10.0.8.21","mtu":1500}}}`,
	}
	for name, lit := range cases {
		v := benchVal(b, lit)
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				_ = ValueHash(v)
			}
		})
	}
}
