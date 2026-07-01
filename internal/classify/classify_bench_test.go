package classify

import "testing"

// benchPatterns mirror a typical deployment's volatile policy.
var benchPatterns = []string{"uptime", "uptime_seconds", "load*", "memory.system.available_bytes", "memory.swap.available_bytes"}

// BenchmarkIsVolatile measures the per-leaf classification (regex scan over all
// volatile patterns). The "miss" case is worst-case: it scans every compiled
// pattern without an early match.
func BenchmarkIsVolatile(b *testing.B) {
	cl, err := New(benchPatterns)
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
