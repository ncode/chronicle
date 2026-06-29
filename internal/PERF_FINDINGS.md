# Chronicle — hotpath benchmarks & findings

Host: darwin/arm64, go1.26.4, `-count 6..10`, stable (<2% var). Benchmarks live next to
the code: `internal/{wire,store,ingest,query}/*bench*_test.go`. Reproduce:

```
go test -run '^$' -bench . -benchmem -count 10 ./internal/...
go test -run '^$' -bench '^BenchmarkApplyCPU$' -benchmem -memprofile mem.out ./internal/ingest/
go tool pprof -alloc_objects -top -cum mem.out
```

Adversarially reviewed by Codex (GPT) **and** an independent Claude fork. Several first-pass
claims were wrong and are corrected below — see "Review corrections" at the end.

## Baseline (means)

| Benchmark | ns/op | B/op | allocs/op | Frequency |
|---|---|---|---|---|
| **ApplyCPU** — full per-push CPU, decode-inclusive, no DB | **43,200** | **44,800** | **689** | per push |
| ApplyCPU (post-decode floor, earlier number) | 15,050 | 19,900 | 394 | per push |
| Flatten (standalone, ~50-leaf fixture) | 1,608 | 4,392 | 47 | per push |
| ValueHash/number | 79 | 184 | 5 | per durable leaf |
| ValueHash/string | 81 | 200 | 5 | per durable leaf |
| ValueHash/bool | 60 | 162 | 3 | per durable leaf |
| ValueHash/array(3) | 180 | 248 | 13 | per durable leaf |
| ValueHash/object(4) | 428 | 464 | 29 | per durable leaf |
| ValueHash/nested | 687 | 512 | 43 | per durable leaf |
| IsVolatile/hit_exact | 24 | 0 | 0 | per leaf |
| IsVolatile/hit_glob | 59 | 0 | 0 | per leaf |
| IsVolatile/miss (scans all patterns) | 86 | 0 | 0 | per leaf |
| Parse/filter | 395 | 568 | 16 | per query |
| Parse/groupby | 366 | 488 | 16 | per query |

Fixture: `ingest.nodeSnapshot` = **60 leaves, 53 durable, 7 volatile** (pinned by
`TestSnapshotLeafCount`). One representative single-node facts push.

## Per-push alloc budget — profile-measured (`-alloc_objects -cum`, 689 allocs total)

| Component | alloc share | nature |
|---|---|---|
| **decodeTree** (`json.Decode` → `map[string]any`, UseNumber) | **39.6%** | mostly intrinsic — the push must be parsed |
| **ValueHash** (per durable leaf) | **34.8%** | optimizable — sha256.New 9% + Sum 9% + canonical tiny-writes 16% |
| **Flatten** | **14.4%** | optimizable — `path+"."+k` churn + ungrown `out` |
| **json.Marshal** (per-leaf value + volatile blob) | **10.7%** | needed — raw value is stored |

> The earlier "ValueHash 65%, Flatten 22%" split was measured against the *decode-excluded*
> floor and was misleading. Once the per-push decode is included (as real `Apply` does), decode
> is the single largest allocator and ValueHash drops to ~35%.

## Findings (ranked)

1. **JSON decode is the biggest single allocator (~40%) and is largely unavoidable.**
   `decodeTree` parses the pushed tree into `map[string]any` with `UseNumber`; every nested
   map, boxed `any`, and `json.Number` allocates. You can't skip parsing the input. A faster
   decoder (avoiding `map[string]any`, or a streaming tokenizer) is a real-but-large change —
   not worth it unless a live profile demands it.

2. **ValueHash — ~35% of per-push allocs, the cleanest optimizable target.** Even a single
   number is 5 allocs / 184 B. `sha256.New()` is allocated *per durable leaf*, `h.Sum(nil)`
   allocates the digest, and every `w.Write([]byte{tag})` / `[]byte(json.Number)` / `writeLen`
   slice escapes to heap through `io.Writer`. Fix: `sync.Pool` the hasher (~half) **and** write
   the canonical form into one reused `[]byte` then hash once (the rest). Low risk; the hash
   value is unchanged, so existing `hash_test.go` invariants still gate it.

3. **Flatten — ~14% of per-push allocs.** `path+"."+k` string churn + ungrown `out` slice.
   Pre-size `out` and build paths in a reused buffer. Secondary.

4. **IsVolatile — 0 allocs, CPU-only.** Worst-case miss = 86 ns scanning all compiled regex;
   hit paths (exact + glob) are also 0 allocs (both benched). Most patterns have no glob
   metachars → an exact-match map + a cheap glob matcher would cut the common path. Time, not
   allocs — irrelevant until the path is CPU-bound, which it isn't.

5. **Parse (query): fine.** 250–400 ns, per-query (low volume). No action — YAGNI.

## Not benchmarked — and why (the honest gap)

These run per push/query but are **DB-bound**, so they need an integration benchmark against a
real/`testcontainers` Postgres, not a microbenchmark:

- `store.InternPath` **cache-miss** (store.go:73): `INSERT … ON CONFLICT` + `SELECT`. Cache
  *hits* are a map read under RLock — negligible, not worth benching.
- `store.ApplyDurable` (the open/close/tombstone diff + batch) — the real per-push DB cost.
- `query.buildIntersect` / `termSubquery` / `LookupPath` / SQL exec (compile.go) — per query.

Until these are measured, the DB-dominance claim below is a reasoned estimate, not data.

## The decision (the point of this exercise)

Full per-push CPU is **~43 µs / 689 allocs**. The real `/v1/push` then runs a per-node tx
(PeekNode → begin → LockNode `FOR UPDATE` → ApplyDurable → UpsertVolatile → MarkContact →
commit) = **~5–6 DB round-trips, fixed per push regardless of leaf count** (`InternPath` is
in-process cached, store.go:66, so leaf count drives CPU not round-trips). At ~50–200 µs per
local-Postgres round-trip that's ~0.3–1.2 ms of DB wall time, dominating the 43 µs of CPU by
roughly **7–28×**.

GC angle (Codex): at the stated workload — 100k nodes, 30-min cadence ≈ **56 pushes/sec** —
689 allocs/push is only **~39k allocs/sec**. Go's GC handles that without noticing; alloc
pressure wouldn't plausibly matter until thousands of pushes/sec sustained.

**Verdict: do not optimize these CPU paths now.** The bottleneck is the DB tx, and at realistic
fleet ingest rates the CPU/GC cost is negligible. If a live `/v1/push` CPU+alloc profile under
representative load ever shows otherwise, do them in this order, smallest first:
**(1) ValueHash hasher-pool → (2) ValueHash reused buffer → (3) Flatten pre-size.** Each is
locally verifiable with `benchstat before.txt after.txt` and gated by existing correctness tests.

The next artifact to capture before *any* change: a CPU+alloc profile of the live ingest path,
plus an integration benchmark of the DB paths above to confirm the 7–28× estimate.

## Review corrections (what the adversarial pass caught)

- `BenchmarkApplyCPU` originally excluded the per-push decode + cap checks + volatile marshal →
  it was a CPU *floor*, not faithful. Now decode-inclusive (43 µs, was 15 µs). *(Codex)*
- "~70 leaves" was wrong; it's **60 / 53 durable**, now pinned by a test. *(Codex)*
- "ValueHash ≈ 360 of 394 allocs / 91%" was wrong; profile says ~35% of the faithful path
  (was 65% of the decode-excluded floor). *(Codex + self re-profile)*
- DB-dominance "100–1000×" overstated → ~7–28× with the faithful CPU number; and it's an
  estimate, not measured (DB paths unbenchmarked). *(Codex + self)*
