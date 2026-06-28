package store

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strconv"
)

// ValueHash returns sha256 over a type-tagged, order-independent canonical
// encoding of v. Two values hash equal iff they are the same JSON value, so the
// hash is the change-detection equality key (ADR-0004, temporal-store spec).
//
// Properties:
//   - Type-distinguishing: 1 (number) != "1" (string) != true; tags differ.
//   - 1 != 1.0: numbers must arrive as json.Number (decode with UseNumber), which
//     preserves the literal "1" vs "1.0". A float64 fallback exists but cannot
//     preserve that distinction — the ingest decode path uses UseNumber.
//   - Order-independent: object keys are sorted, so map iteration order and
//     re-serialization never cause spurious churn.
//   - Unambiguous: strings, keys, and scalars are length-prefixed, so
//     concatenations like ["ab","c"] and ["a","bc"] cannot collide.
func ValueHash(v any) [32]byte {
	h := sha256.New()
	canonical(h, v)
	var out [32]byte
	copy(out[:], h.Sum(nil))
	return out
}

func canonical(w io.Writer, v any) {
	switch t := v.(type) {
	case nil:
		_, _ = w.Write([]byte{'n'})
	case bool:
		if t {
			_, _ = w.Write([]byte{'b', 1})
		} else {
			_, _ = w.Write([]byte{'b', 0})
		}
	case json.Number:
		writeTagged(w, '#', []byte(t))
	case float64:
		// ponytail: defensive fallback only. Decoding with UseNumber keeps numbers
		// as json.Number; this path loses the 1-vs-1.0 distinction, so the decode
		// path must not feed float64 here for facts we care about distinguishing.
		writeTagged(w, '#', []byte(strconv.FormatFloat(t, 'g', -1, 64)))
	case string:
		writeTagged(w, 's', []byte(t))
	case []any:
		_, _ = w.Write([]byte{'['})
		writeLen(w, len(t))
		for _, e := range t {
			canonical(w, e)
		}
	case map[string]any:
		_, _ = w.Write([]byte{'{'})
		writeLen(w, len(t))
		keys := make([]string, 0, len(t))
		for k := range t {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			writeTagged(w, 'k', []byte(k))
			canonical(w, t[k])
		}
	default:
		// Unexpected non-JSON type: tag with the Go type so it can never silently
		// collide with a real JSON value.
		writeTagged(w, '?', fmt.Appendf(nil, "%T:%v", v, v))
	}
}

func writeTagged(w io.Writer, tag byte, b []byte) {
	_, _ = w.Write([]byte{tag})
	writeLen(w, len(b))
	_, _ = w.Write(b)
}

func writeLen(w io.Writer, n int) {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], uint64(n))
	_, _ = w.Write(b[:])
}
