# Tasks — seal the store seam

## 1. Sequencing

- [x] 1.1 Confirm `juliano/fix-review-findings` is merged; rebase this change on the
  post-merge `main` (it rewrites `ApplySnapshot`/`LockNode` signatures and the same seed
  call sites)
  - Note: implemented stacked on `juliano/test-http-surface`.

## 2. Re-seed through the write door (tests stay green at every step)

- [x] 2.1 Rewrite `seed()` in `internal/query/query_integration_test.go` as a wrapper over
  `store.ApplySnapshot` (producerTS = received = `at`, volatile blob `{}`, clean=true);
  rewrite the volatile seed (`UpsertVolatile` call) as an apply with a volatile blob
- [x] 2.2 Rewrite the seed helper in `internal/monitor/monitor_test.go` the same way
  (churn shapes = successive applies with strictly increasing timestamps)
- [x] 2.3 Keep `TRUNCATE`/`DELETE`/`UPDATE nodes SET expired=now()` scaffolding on
  `Pool()`; run `make test-db` — full suite green with zero cross-package primitive calls
  - Note: scaffolding retained; `make test-db` green (run by coordinator).

## 3. Unexport the write primitives

- [x] 3.1 Rename `LockNode`→`lockNode`, `InternPath`→`internPath`,
  `MarkContact`→`markContact`, `ApplyDurable`→`applyDurable`,
  `UpsertVolatile`→`upsertVolatile` (in-package callers: `ApplySnapshot`,
  `store_integration_test.go`)
- [x] 3.2 Verify no exported store signature mentions `pgx.Tx`; `make test-db` green
  - Note: exported `pgx.Tx` signature grep is clean; `make test-db` green (run by coordinator).

## 4. Move the monitor scans behind the store

- [x] 4.1 Add store operations for the high-churn scan and the `fact_paths`-cardinality
  scan (window/threshold as parameters, store row types as results; time bounds stay
  Go-computed bind parameters per ADR-0012)
- [x] 4.2 Rewrite `monitor.scan` to call them; delete the raw SQL and the `Pool()` use
  from `internal/monitor`; `monitor.Finding` and thresholds stay in monitor
- [x] 4.3 `make test-db` green; grep confirms `fact_history`/`fact_paths` SQL exists only
  under `internal/store`
  - Note: monitor production SQL moved under `internal/store`; `make test-db` green. Remaining grep hits are the schema itself (`docs/schema/v1.sql`) and sanctioned test scaffolding (`internal/lifecycle/lifecycle_test.go` assertions) — no production SQL outside store.

## 5. Docs and interface hygiene

- [x] 5.1 Fix the stale `Pool()` doc comment (`ApplySnapshot` owns the per-node
  transaction, not ingest)
- [x] 5.2 Confirm still-exported surface is exactly: `Open`, `Store`, `ApplySnapshot`,
  lifecycle ops (`ExpireStale`, `Deactivate`, `ResetProducerTS`), reads (`Now`, `StateAt`,
  `Diff`, `PeekNode` if still public post-rebase), `Pool`, `ValueHash`, `Migrate`, error
  vars, and the row/leaf types (`Node`, `PendingLeaf`, `DurableLeaf`, `ApplyStats`,
  `FactRow`, `DiffRow`)
  - Note: confirmed the five write primitives are gone and no exported store signature mentions `pgx.Tx`; existing production-used read/lifecycle helpers and the two required monitor scan reads remain exported.

## 6. Verify

- [x] 6.1 `make test` (unit, no DB) and `make test-db` (integration) both green
  - Note: both green (run by coordinator). Two branch tests fixed during verification: `TestStateEndpoint` seeded volatile via raw INSERT (now through `seedSnapshot`) and assumed an absent `node_volatile` row (now constructed via explicit scaffolding DELETE).
- [x] 6.2 `make race` green (concurrency-sensitive packages)
  - Note: `make race` green (store + ingest).
- [x] 6.3 Confirm zero runtime behavior change: no SQL text changed except relocation, no
  response or metric changed
