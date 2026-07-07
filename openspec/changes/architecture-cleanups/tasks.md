# Tasks — architecture cleanups

## 1. Dissolve internal/lifecycle

- [x] 1.1 Inline the expiry sweep in `cmd/chronicle/main.go`: `periodic.Run` over
  `st.ExpireStale` with the same logging and a one-line ttl>0 guard (defense-in-depth —
  see design)
- [x] 1.2 Move the three tests from `internal/lifecycle/lifecycle_test.go` into
  `internal/ingest` (package decl; drop `Manager` from setup; `mgr.Sweep(ctx)` →
  `st.ExpireStale(ctx, ttl)`); delete `internal/lifecycle/`
- [x] 1.3 `go build ./... && go vet ./...` green; `make test-db` green

## 2. One holder for the Volatile policy

- [x] 2.1 Build `*atomic.Pointer[classify.Policy]` in `main` before both services;
  startup fails on an invalid initial `VolatilePaths`
- [x] 2.2 `ingest.New` takes the holder; delete `ingest.Service.ReloadVolatilePolicy`;
  push classification keeps its snapshot-once-per-push pattern reading the shared holder
- [x] 2.3 `query.NewService`/`NewEngine` take the holder; `Engine` becomes a plain struct;
  `Run` loads the policy once at entry and threads that snapshot through
  `checkVolatileHistory` and `compile`; delete `query.Service.ReloadVolatilePolicy`
- [x] 2.4 `reloadOnHUP` compiles once and stores once (old policy kept + logged on
  compile failure); update the ~9 constructor call sites across the test files
- [x] 2.5 New agreement test: swap the holder with a pattern newly matching a path,
  assert the next push routes it volatile AND the next `at <past T>` query for it
  returns the typed no-history error (spec: "Reload flips both sides together")
- [x] 2.6 `make test-db` green

## 3. Split store.go + unexport errStaleApply

- [x] 3.1 Split `internal/store/store.go` into `store.go` / `node.go` / `apply.go` /
  `reads.go` per the design partition — pure moves, comments preserved verbatim, no
  signature or behavior changes
- [x] 3.2 Rename `ErrStaleApply` → `errStaleApply`; fix its doc comment
  (`ApplySnapshot` maps it to `ErrStale`, not ingest); update `store_guard_test.go`
- [x] 3.3 `go build ./... && go vet ./...` green; `make test-db` green; grep confirms no
  exported store signature or error changed except the one unexport

## 4. ADR + verify

- [x] 4.1 Write `docs/adr/0013-staleness-guard-is-a-layered-funnel.md`: the four layers
  (plan skew / peek resource bound / locked authoritative / overlap backstop), why each
  rejects where it does, rejected-consolidation record so reviews do not re-propose it
- [x] 4.2 Full ladder: `make test` (no DB), `make test-db`, `make race` all green
- [x] 4.3 Confirm zero runtime behavior change except startup-gating of invalid initial
  `VolatilePaths`
