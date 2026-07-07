# Design — architecture cleanups

## Context

Verified by a three-track adversarial pass (two Claude refuters + codex) against
post-merge `main`. Every scope number below is from that verification, not the original
review draft. The staleness-guard candidate was retired: `PeekNode` is load-bearing for
ADR-0009 §4 (rejected pushes must not grow the never-GC'd `fact_paths`; that growth is
invisible to the cardinality alarm because rejected pushes write no `fact_history` rows).

## Goals / Non-Goals

**Goals:**

- Delete the lifecycle module (~45 net production LOC removed); expiry sweep wired
  directly in `cmd/chronicle` over `periodic.Run` + `store.ExpireStale`.
- The Volatile policy invariant becomes structural: one holder, one swap point, one
  snapshot per operation, plus the missing agreement test.
- `store.go` split along its own section banners into 4 files; `ErrStaleApply` unexported.
- ADR-0013 records the staleness guard as a deliberately layered funnel (do not
  re-propose consolidation).

**Non-Goals:**

- No runtime behavior change (one exception documented in the proposal: invalid initial
  `VolatilePaths` gates startup in `main` instead of service construction — same fatal
  outcome).
- No Clock seam, no mocks (ADR-0012); no new interfaces anywhere.
- `PeekNode`, the locked guards, and `applyDurable`'s overlap backstop are untouched.
- ADR prose that mentions the lifecycle package as history stays as-is (ADRs record
  decisions at a point in time).

## Decisions

- **Lifecycle tests move to `internal/ingest`, not `internal/store`.** `lifecycle_test.go`
  imports `ingest`; store's integration tests are in-package `package store`, so moving
  there is an import cycle. Two of the three tests never used `Manager`; the third changes
  one line (`mgr.Sweep(ctx)` → `st.ExpireStale(ctx, ttl)`).
- **Keep the ttl>0 guard inline in main's sweep closure.** Config validation rejects
  non-positive `expiry_ttl`, but `ExpireStale` computes `cutoff = now − ttl` unguarded — a
  negative ttl would put the cutoff in the future and soft-expire the entire fleet. One
  line of defense-in-depth is cheaper than the incident.
- **Policy holder shape**: `*atomic.Pointer[classify.Policy]` built in `main` before
  either service (startup gates on the initial compile), passed into `ingest.New` and
  `query.NewService`/`NewEngine`. `Engine` becomes a plain struct holding the holder;
  `Run` loads **once at entry** and threads the snapshot through `checkVolatileHistory`
  (signature gains the policy) and `compile` (already takes it). Loading at each use site
  is forbidden — a mid-request reload must not let the at-T check and compilation
  disagree. Ingest keeps its existing snapshot-once-per-push pattern, reading the shared
  holder instead of its own.
- **Reload error semantics preserved**: a failed `classify.New` on reload keeps the old
  policy and logs (today's behavior, now in one place instead of two).
- **Store partition** (write path stays whole — it is the one write door):
  - `store.go`: package doc, `Store` struct + interning cache/mutex, `Open`/`Pool`/`Close`,
    `internPath`/`LookupPath`/`NodeID` (~115 lines)
  - `node.go`: `Node`, `lockNode`, `PeekNode`, `markContact`, `TouchContact`, error vars,
    `ResetProducerTS`, `ExpireStale`, `Deactivate` (~175 lines)
  - `apply.go`: `DurableLeaf`/`ApplyStats`/`applyDurable` + helpers, `upsertVolatile`,
    `PendingLeaf`/`ApplySnapshot` (~295 lines; the apply-ordering comments carry real
    concurrency constraints — move verbatim)
  - `reads.go`: `FactRow`/`Now`/`Volatile`/`StateAt`/`scanFacts`/`closeTime`,
    `DiffRow`/`Diff`, `CountRow`/`HighChurn`/`FactPathCardinality`/`scanCountRows`
    (~170 lines)
- **`ErrStaleApply` → `errStaleApply`** during the split; fix its doc comment
  (`ApplySnapshot` maps it to `ErrStale` now, not ingest). `store_guard_test.go` is
  in-package; one mechanical rename.
- **ADR-0013** documents the four guard layers and why each rejects where it does (plan:
  pre-DB server-clock skew; peek: pre-intern resource bound; locked check: authoritative,
  ADR-0009 §3; overlap bound: schema-invariant backstop covering the post-reset
  nil-watermark path). Modeled on ADR-0012's "reviewed, rejected, do not re-propose" form.

## Risks / Trade-offs

- [Constructor signature changes ripple to ~9 test call sites across 5 files] → mechanical;
  the compiler finds every one.
- [git blame churn on store.go] → accepted; the file accreted a new cluster last week and
  will keep growing — split before it gets worse.
- [Sequencing collision: lifecycle dissolution and policy holder both touch
  lifecycle_test.go / its successor] → strict task order: dissolve first, holder second,
  split third, ADR last.
