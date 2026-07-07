# Architecture cleanups: dissolve lifecycle, share the Volatile policy, split store.go

## Why

The periodic architecture review left four candidates; a three-track adversarial
verification (two Claude refuters + codex, all against post-merge `main`) confirmed three
and retired one. The three that hold each remove a shallow or duplicated seam: the
lifecycle module is ~26 lines of passthrough with one importer; the Volatile policy — whose
CONTEXT.md contract is "shared by ingest and query so the two endpoints never disagree" —
is maintained as two independently-swapped copies agreeing only by convention (and no test
enforces it); and `store.go` (757 lines, grown in 3 of the last 5 merges) interleaves
concerns the file's own section banners already separate.

## What Changes

- **Dissolve `internal/lifecycle`**: `Manager.Deactivate` was already deleted in
  fix-review-findings; what remains is a ttl guard + `store.ExpireStale` passthrough and a
  `periodic.Run` wrapper with one importer. Delete the package; wire the expiry sweep
  inline in `cmd/chronicle` (keeping the one-line ttl guard — it is the last defense
  against a negative ttl mass-expiring the fleet); relocate the three Postgres-gated tests
  to `internal/ingest` (they already exercise ingest+store; moving into `store` would be an
  import cycle).
- **One holder for the Volatile policy**: a single policy holder built in `main`
  (gating startup on a bad initial pattern set), passed to both services;
  `query.Engine` becomes a plain struct that loads the policy **once per query run** and
  threads that snapshot through the at-T check and compilation (a mid-request SIGHUP must
  never mix policies); delete both `ReloadVolatilePolicy` methods; `reloadOnHUP` swaps
  once. Adds the missing test: a HUP swap flips write-side classification and read-side
  at-T rejection together.
- **Split `store.go` into four files** (same package, zero interface change): identity +
  lifecycle ops in `node.go`, the whole write path (durable + volatile + `ApplySnapshot`)
  together in `apply.go`, reads + monitor scans in `reads.go`, struct/pool/interning stay
  in `store.go`. The write path stays one file — it is deliberately one write door.
- **Retire the staleness-guard candidate, with teeth**: both verifiers found the `PeekNode`
  pre-check load-bearing (it keeps rejected pushes from growing the never-GC'd
  `fact_paths`, growth invisible to the cardinality alarm). Record the layered-funnel
  design as **ADR-0013** so it is not re-proposed. Residue: unexport
  `ErrStaleApply` → `errStaleApply` (only `ApplySnapshot` consumes it since the ingest-tx
  move) and fix its stale doc comment.

## Capabilities

### New Capabilities

(none)

### Modified Capabilities

- `fact-ingest`: add the requirement that one shared Volatile policy instance serves both
  write-side classification and read-side routing/at-T rejection, swapped atomically as a
  unit, with each push and each query evaluated against a single policy snapshot.

## Impact

- Production files: `internal/lifecycle/` (deleted), `cmd/chronicle/main.go`,
  `internal/ingest/ingest.go`, `internal/query/service.go`, `internal/query/compile.go`,
  `internal/store/` (file split + one unexport), `docs/adr/0013-*.md` (new).
- Test files: `lifecycle_test.go` relocates to `internal/ingest`; ~9 constructor call
  sites across 5 test files gain the holder argument; one new HUP-agreement test.
- No runtime behavior change intended anywhere except the startup-gating of a bad initial
  `VolatilePaths` (previously surfaced as a service-construction error — same outcome,
  earlier and once).
- Sequencing inside the change: lifecycle first (it owns the test file the policy work
  also touches), then the policy holder, then the store split + unexport, then the ADR.
