package ingest

import (
	"encoding/json"
	"testing"

	"github.com/ncode/chronicle/internal/store"
	"github.com/ncode/chronicle/internal/wire"
)

// nodeSnapshot is a representative single-node facts push (~60 leaves, ~53
// durable: os, networking interfaces, memory, processors, mountpoints, plus
// volatile load/uptime). This is the unit of work the ingest CPU path processes
// per push; TestSnapshotLeafCount pins the exact size the findings doc cites.
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

// pendingLeafBench mirrors the per-leaf struct built inside Apply so the
// benchmark allocates the same slice the real path does.
type pendingLeafBench struct {
	path, factName string
	value          json.RawMessage
	hash           [32]byte
}

// BenchmarkApplyCPU measures the per-push CPU work in Service.Apply, excluding
// only DB I/O. It faithfully mirrors ingest.go Apply: per-push decodeTree (real
// Apply decodes every push), Flatten, then per leaf the MaxPathLen/MaxValueBytes
// guards + json.Marshal + classify + ValueHash + pending/volatile slice build,
// then the volatile-blob marshal. This is a true CPU floor for one push (the DB
// tx is layered on top of it), not a microbenchmark of one function.
func BenchmarkApplyCPU(b *testing.B) {
	raw := json.RawMessage(nodeSnapshot)
	cl, err := NewClassifier(realisticVolatilePatterns)
	if err != nil {
		b.Fatal(err)
	}
	// Representative caps (config.ServerConfig defaults); the checks are cheap len
	// comparisons included for fidelity, not to reject this fixture.
	const maxPathLen, maxValueBytes = 512, 1 << 20
	b.ReportAllocs()
	for b.Loop() {
		tree, err := decodeTree(raw) // per-push decode, as real Apply does
		if err != nil {
			b.Fatal(err)
		}
		leaves := wire.Flatten(tree)
		pending := make([]pendingLeafBench, 0, len(leaves))
		volatile := make(map[string]any, 8)
		for _, lf := range leaves {
			if len(lf.Path) > maxPathLen {
				b.Fatalf("path too long: %s", lf.Path)
			}
			rawVal, err := json.Marshal(lf.Value)
			if err != nil {
				b.Fatal(err)
			}
			if len(rawVal) > maxValueBytes {
				b.Fatal("value too big")
			}
			if cl.IsVolatile(lf.Path) {
				volatile[lf.Path] = lf.Value
				continue
			}
			pending = append(pending, pendingLeafBench{lf.Path, lf.FactName, rawVal, store.ValueHash(lf.Value)})
		}
		if _, err := json.Marshal(volatile); err != nil {
			b.Fatal(err)
		}
		_ = pending
	}
}

// TestSnapshotLeafCount pins the fixture size the findings doc cites, so the
// "~N leaves, M durable" claim can't silently drift. Uses the real decode +
// Flatten + classifier so it measures exactly what Apply would see.
func TestSnapshotLeafCount(t *testing.T) {
	tree, err := decodeTree(json.RawMessage(nodeSnapshot))
	if err != nil {
		t.Fatal(err)
	}
	cl, err := NewClassifier(realisticVolatilePatterns)
	if err != nil {
		t.Fatal(err)
	}
	leaves := wire.Flatten(tree)
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
