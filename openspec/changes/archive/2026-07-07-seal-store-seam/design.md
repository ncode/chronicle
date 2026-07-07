# Design — seal the store seam

## Context

Production code already treats `ApplySnapshot` as the store's only write door (ingest calls
nothing else; lifecycle uses `ExpireStale`/`Deactivate`); the exported interval primitives
serve only cross-package test seeding, which bypasses the guards the real path enforces.
The monitor is the only non-store package hand-writing SQL against
`fact_history`/`fact_paths` — the query DSL compiler's raw SQL over views and
`nodes`/`node_volatile` is a deliberate, ADR-0008-sanctioned read path and stays.

Verified by three adversarial passes. Corrections they produced, honored here: `InternPath`
is pool-based (not tx-taking); reseeding must precede unexporting; `Pool()` must stay
exported; `ValueHash` and `PendingLeaf`/`DurableLeaf` stay exported (production-used by
query and ingest).

## Goals / Non-Goals

**Goals:**

- `pgx.Tx` appears in no exported store signature; the five write primitives are
  unexported.
- Cross-package seeds are guard-consistent because they run the real apply.
- Monitor holds no schema knowledge of the temporal tables.

**Non-Goals:**

- No behavior change, no schema change, no new metrics.
- Not hiding `Pool()` (query compiler + readyz ping are sanctioned consumers) — only its
  stale doc comment is corrected.
- Not touching `query/compile.go` or its raw SQL.
- Not introducing any store interface type for mocking (ADR-0012's philosophy: real
  Postgres, no fakes). The seal is about narrowing the concrete surface, not adding a seam.

## Decisions

- **Order: reseed → unexport → move scans.** Reseeding first keeps the suite green at
  every step; unexporting then becomes a compiler-verified no-op; the scan move is
  independent and lands last.
- **Seed helper maps onto the apply.** The existing `seed(certname, at, leaves)` helpers
  become thin wrappers over `ApplySnapshot(ctx, certname, producerTS: at, received: at,
  pending, volatile: {}, clean: true)`. Traps the verifiers pinned:
  - `received` must be ≈ `producerTS` (post-`fix-review-findings`, a first-contact past
    bound rejects `producerTS < received − maxSkew`; passing `time.Now()` as `received`
    for historical seeds would reject them).
  - The volatile blob must be `{}`, never nil (`''::jsonb` errors).
  - The apply also writes a `node_volatile` row and advances `last_producer_ts`; no
    current assertion depends on their absence (verified), but new tests must not assume
    a virgin `node_volatile`.
  - Churn seeding uses strictly increasing timestamps per node — the existing shapes
    (`qt1→qt2`, `base+0..3min`) already comply.
- **Degenerate/setup SQL stays on `Pool()`.** `TRUNCATE`, `DELETE`, and
  `UPDATE nodes SET expired=now()` are test scaffolding, not fact writes; forcing them
  through the apply would be dishonest (they exist to create states the apply forbids).
- **Scan operations take parameters, return store row types.** e.g. churn rows for a
  window, cardinality counts — `monitor.Finding` and the thresholds stay in monitor, so
  no dependency cycle. Precedent: lifecycle over `ExpireStale`/`Deactivate`. Both scans
  keep taking their time bounds as computed-in-Go bind parameters (ADR-0012 documents
  this shape).
- **In-package store tests are untouched** — `store_integration_test.go` is
  `package store` and may keep using the now-unexported primitives to construct edge
  shapes.

## Risks / Trade-offs

- [Conflicts with `fix-review-findings` (~172 changed lines in store.go, same seed call
  sites)] → hard sequencing: implement only after that branch merges; its `LockNode`
  signature change `(Node, bool, error)` and `maxSkew` parameter are the base for this
  work.
- [Seeding through guards is slightly slower (real tx + lock per seed)] → negligible at
  test scale; the suite already runs `-p 1` against one database.
- [A future test needs a shape the apply cannot produce (e.g. an interval chain with a
  deliberately broken invariant)] → write it in-package in `store` (unexported access),
  which is where invariant-violation tests belong anyway.
