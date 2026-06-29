package query

import "testing"

// BenchmarkParse measures the DSL parser. Runs once per query — far lower volume
// than the ingest path, so this is a baseline to confirm it is not a concern.
func BenchmarkParse(b *testing.B) {
	cases := map[string]string{
		"filter":  `os.name="Debian" networking.domain="prod.example.com" kernel="Linux"`,
		"groupby": `os.name where os.family="Debian" is_virtual=true group by os.name`,
		"at":      `os.name="Debian" at 2026-06-28T12:00:00Z`,
	}
	for name, q := range cases {
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				if _, err := Parse(q); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
