package wire

import (
	"bytes"
	"encoding/json"
	"testing"
)

func benchTree(b *testing.B, s string) map[string]any {
	b.Helper()
	d := json.NewDecoder(bytes.NewReader([]byte(s)))
	d.UseNumber()
	var m map[string]any
	if err := d.Decode(&m); err != nil {
		b.Fatal(err)
	}
	return m
}

// A node-sized nested tree (~70 leaves) — the per-push Flatten input.
const benchSnapshot = `{
  "os": {"name": "Debian", "family": "Debian", "release": {"major": "12", "minor": "5", "full": "12.5"}},
  "networking": {"hostname": "web-01", "fqdn": "web-01.prod.example.com", "ip": "10.0.4.21", "mac": "52:54:00:12:34:56",
    "interfaces": {
      "eth0": {"ip": "10.0.4.21", "netmask": "255.255.255.0", "mac": "52:54:00:12:34:56", "mtu": 1500},
      "eth1": {"ip": "10.0.8.21", "netmask": "255.255.255.0", "mac": "52:54:00:12:34:57", "mtu": 1500},
      "lo": {"ip": "127.0.0.1", "netmask": "255.0.0.0", "mtu": 65536}}},
  "memory": {"system": {"total_bytes": "16777216000", "available_bytes": "8123456789"}},
  "mountpoints": {"/": {"device": "/dev/sda1", "filesystem": "ext4", "size_bytes": "53687091200"},
    "/var": {"device": "/dev/sda2", "filesystem": "ext4", "size_bytes": "107374182400"}},
  "processors": {"count": "8", "models": ["Xeon E5-2670", "Xeon E5-2670"]},
  "uptime": "12345", "load": {"1m": "0.42", "5m": "0.31", "15m": "0.18"}, "is_virtual": true
}`

// BenchmarkFlatten measures the per-push tree walk that produces leaf paths.
// Runs once per push; allocs here are path-string + leaf-slice churn.
func BenchmarkFlatten(b *testing.B) {
	tree := benchTree(b, benchSnapshot)
	b.ReportAllocs()
	for b.Loop() {
		_, _ = Flatten(tree, FlattenLimits{})
	}
}
