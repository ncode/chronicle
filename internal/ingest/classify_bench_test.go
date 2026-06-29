package ingest

import "testing"

// BenchmarkIsVolatile measures the per-leaf classification (regex scan over all
// volatile patterns). Runs once per leaf per push. The "miss" case is worst-case:
// it scans every compiled pattern without an early match.
func BenchmarkIsVolatile(b *testing.B) {
	cl, err := NewClassifier(realisticVolatilePatterns)
	if err != nil {
		b.Fatal(err)
	}
	cases := map[string]string{
		"hit_exact":  "uptime",                             // matches pattern 1
		"hit_glob":   "load.1m",                            // matches "load*"
		"miss":       "networking.interfaces.eth0.address", // scans all patterns, no match
		"miss_short": "os.name",                            // scans all patterns, no match
	}
	for name, path := range cases {
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				_ = cl.IsVolatile(path)
			}
		})
	}
}
