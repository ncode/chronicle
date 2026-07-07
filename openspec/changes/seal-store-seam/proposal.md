# Seal the store seam

## Why

The store is the module that owns the temporal discipline, but its exported surface
undersells that depth: interval-level primitives taking `pgx.Tx` are exported with zero
production callers outside `ApplySnapshot`, cross-package tests seed data around the
guards those primitives skip, and the churn monitor is the only non-store package writing
raw SQL against `fact_history`/`fact_paths`. Narrowing the interface makes ADR-0009's "one
transaction, one door" property structural — the compiler, not convention, guarantees every
durable write goes through the per-node serialized apply. Three adversarial reviews (two
Claude refuters, one codex pass) confirmed the facts and found no blocker; codex confirmed
ADR-0009 actively supports seeding through the apply path.

## What Changes

- Re-seed cross-package integration tests (`internal/query/query_integration_test.go`,
  `internal/monitor/monitor_test.go`) through `store.ApplySnapshot` instead of the
  interval-level primitives, so seeded rows are guard-consistent by construction.
- Unexport the five write primitives — `LockNode`, `MarkContact`, `ApplyDurable`,
  `UpsertVolatile` (tx-taking) and `InternPath` (pool-based) — leaving `ApplySnapshot`,
  the lifecycle operations, the reads, `Pool()`, `ValueHash`, and the row/leaf types as
  the store's interface. `pgx.Tx` disappears from every exported signature.
- Move the monitor's two raw scans (high-churn, `fact_paths`-cardinality) behind the
  store's interface as store operations taking window/threshold parameters; the monitor
  keeps thresholds, `Finding`, and alarm logic.
- Fix the stale `Pool()` doc comment (it still says ingest opens the per-node transaction;
  `ApplySnapshot` has owned it since the ingest refactor).
- No runtime behavior change intended. (`store.DB` deletion, originally part of this
  finding, already lands in `juliano/fix-review-findings`.)

## Capabilities

### New Capabilities

(none)

### Modified Capabilities

- `temporal-store`: add the requirement that the whole-snapshot apply is the store's only
  write door (interval primitives internal; external packages — including test seeding —
  write only through it), and that SQL against the temporal tables lives only in the store
  module.

## Impact

- Affected code: `internal/store` (unexports, two new scan operations, comment fix),
  `internal/monitor` (drops its SQL, calls store), the two cross-package test seed
  helpers. `internal/query/compile.go` is untouched — its raw SQL over views/`Pool()` is
  the sanctioned read path (ADR-0008).
- Sequencing: **stacked on `juliano/fix-review-findings`** — that branch rewrites
  `ApplySnapshot`/`LockNode` signatures (adds `maxSkew`, first-contact past bound) and
  updates the same seed call sites. Implementing against `main` guarantees conflicts.
- Order within the change matters: re-seed first (tests stay green through the door),
  then unexport (compiler enforces the seal), then move the monitor scans.
