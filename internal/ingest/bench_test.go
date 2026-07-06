package ingest

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/ncode/chronicle/internal/classify"
	"github.com/ncode/chronicle/internal/config"
	"github.com/ncode/chronicle/internal/wire"
)

// nodeSnapshot is a representative single-node facts push (~60 leaves, ~53
// durable: os, networking interfaces, memory, processors, mountpoints, plus
// volatile load/uptime). This is the unit of work the ingest CPU path processes
// per push; TestSnapshotLeafCount pins the exact size PERF_FINDINGS.md cites.
const nodeSnapshot = `{
  "os": {
    "name": "Debian", "family": "Debian", "architecture": "amd64",
    "release": {"major": "12", "minor": "5", "full": "12.5"},
    "distro": {"codename": "bookworm", "description": "Debian GNU/Linux 12 (bookworm)", "id": "Debian"}
  },
  "kernel": "Linux", "kernelrelease": "6.1.0-18-amd64", "kernelversion": "6.1.76",
  "networking": {
    "hostname": "web-01", "fqdn": "web-01.prod.example.com", "domain": "prod.example.com",
    "ip": "10.0.4.21", "ip6": "fe80::5054:ff:fe12:3456", "mac": "52:54:00:12:34:56", "mtu": 1500,
    "interfaces": {
      "eth0": {"ip": "10.0.4.21", "netmask": "255.255.255.0", "mac": "52:54:00:12:34:56", "mtu": 1500},
      "eth1": {"ip": "10.0.8.21", "netmask": "255.255.255.0", "mac": "52:54:00:12:34:57", "mtu": 1500},
      "lo": {"ip": "127.0.0.1", "netmask": "255.0.0.0", "mtu": 65536}
    }
  },
  "processors": {"count": "8", "physicalcount": "1", "models": ["Intel(R) Xeon(R) CPU E5-2670", "Intel(R) Xeon(R) CPU E5-2670"]},
  "memory": {
    "system": {"total_bytes": "16777216000", "available_bytes": "8123456789", "used_bytes": "8653759211"},
    "swap": {"total_bytes": "2147483648", "available_bytes": "2147483648"}
  },
  "mountpoints": {
    "/": {"device": "/dev/sda1", "filesystem": "ext4", "size_bytes": "53687091200", "available_bytes": "21474836480"},
    "/var": {"device": "/dev/sda2", "filesystem": "ext4", "size_bytes": "107374182400", "available_bytes": "85899345920"}
  },
  "disks": {"sda": {"model": "Virtual disk", "size_bytes": "161061273600", "vendor": "QEMU"}},
  "uptime": "12345", "uptime_seconds": "12345", "load": {"1m": "0.42", "5m": "0.31", "15m": "0.18"},
  "timezone": "UTC", "is_virtual": true, "virtual": "kvm",
  "fips_enabled": false, "selinux": false,
  "ssh": {"rsa": {"fingerprints": {"sha256": "SHA256:abc123def456"}}}
}`

// realisticVolatilePatterns mirror a typical deployment: a handful of globs.
var realisticVolatilePatterns = []string{"uptime", "uptime_seconds", "load*", "memory.system.available_bytes", "memory.swap.available_bytes"}

// BenchmarkApplyCPU measures the per-push CPU work in Service.Apply, excluding
// only DB I/O, by calling the real plan(): per-push decodeTree, Flatten (with
// the caps), then per leaf json.Marshal + classify + ValueHash + pending/volatile
// slice build, then the volatile-blob marshal. Calling plan() directly (rather
// than a hand-copied mirror) keeps the benchmark honest as the CPU path evolves.
func BenchmarkApplyCPU(b *testing.B) {
	cl, err := classify.New(realisticVolatilePatterns)
	if err != nil {
		b.Fatal(err)
	}
	cfg := &config.ServerConfig{
		MaxSkew:       config.Duration(5 * time.Minute),
		MaxLeafCount:  50_000,
		MaxPathLen:    1024,
		MaxValueBytes: 256 << 10,
	}
	received := time.Date(2026, 6, 28, 12, 0, 0, 0, time.UTC)
	push := &wire.Push{
		ProducerTimestamp: received,
		Tree:              json.RawMessage(nodeSnapshot),
		Discovery:         wire.DiscoveryStatus{Builtin: map[string]string{"os": wire.StatusOK}},
	}
	b.ReportAllocs()
	for b.Loop() {
		pl, _, _ := plan(cfg, cl, push, received)
		if pl == nil {
			b.Fatal("push unexpectedly rejected")
		}
	}
}

// TestSnapshotLeafCount pins the fixture size PERF_FINDINGS.md cites, so the
// "~N leaves, M durable" claim can't silently drift. Uses the real decode +
// Flatten + classifier so it measures exactly what Apply would see.
func TestSnapshotLeafCount(t *testing.T) {
	tree, err := decodeTree(json.RawMessage(nodeSnapshot))
	if err != nil {
		t.Fatal(err)
	}
	cl, err := classify.New(realisticVolatilePatterns)
	if err != nil {
		t.Fatal(err)
	}
	leaves, err := wire.Flatten(tree, wire.FlattenLimits{})
	if err != nil {
		t.Fatal(err)
	}
	durable := 0
	for _, lf := range leaves {
		if !cl.IsVolatile(lf.Path) {
			durable++
		}
	}
	t.Logf("fixture: %d leaves, %d durable, %d volatile", len(leaves), durable, len(leaves)-durable)
	if len(leaves) < 40 || len(leaves) > 90 {
		t.Fatalf("fixture drifted to %d leaves; update PERF_FINDINGS.md", len(leaves))
	}
}
