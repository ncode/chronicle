# Design — test the HTTP surface through its interface

## Context

The pure internals (`query.compile`, `query.dsl`, `ingest.plan`, `Authenticator.Authenticate`,
`allows()`) are exhaustively unit-tested, and store operations are integration-tested. What
was never exercised is the HTTP wiring that composes them. `juliano/fix-review-findings`
introduced the pattern this change extends: `internal/query/query_service_test.go` drives
`httptest.NewServer(svc.Handler())` with static tokens against the Postgres harness;
`internal/ingest/ingest_handler_test.go` drives `handlePush` with a synthesized
`r.TLS = &tls.ConnectionState{VerifiedChains: ...}` (only `Subject.CommonName` is read).

Three adversarial verification passes (two independent Claude refuters, one codex
cross-model review) confirmed the residual gaps this change closes and validated the
approach as free of new seams.

## Goals / Non-Goals

**Goals:**

- Every route in `query.Service.Handler()` has at least one test entering through the
  handler, including its role gate and its failure statuses.
- The error→status mapping in `handleQuery`, `handleDiff`, and the state endpoint is
  pinned by tests (400/404/413/422).
- Ingest load-shed responses (429/503) have their `Retry-After` header asserted.

**Non-Goals:**

- No production code changes (unless a new test exposes a real bug — fixed here, called
  out in review).
- No mock store, no Clock seam (ADR-0012), no OIDC test IdP — static tokens only
  (ADR-0010 layering).
- No re-testing of what `fix-review-findings` already covers (role matrix for
  query/deactivate, 401, state/diff happy paths, ingest 403/400/413/429/503 bodies).
- cmd/chronicle wiring (`reloadOnHUP`, ops mux, shutdown) stays out of scope.

## Decisions

- **Enter through `Handler()`, not handler funcs.** The mux carries the route→role table
  (`require(RoleAdmin, ...)`) — the thing a regression would silently break. Tests that
  call `handleResetWatermark` directly would miss it.
- **No Postgres for pure-wiring cases.** `require` rejects (401/403) and parameter
  parsing (400/413) fire before any store call, so those tests construct the Service and
  call `ServeHTTP` with a `httptest.NewRecorder` — they run in the plain unit suite.
  Only reset-watermark behavior ("a subsequent in-bounds push is accepted") and the 422
  volatile-history case need `CHRONICLE_TEST_DB`.
- **Reset-watermark behavioral assertion goes through the real flow**: poison a node's
  watermark by pushing with a future-but-in-bounds `producer_timestamp`, verify a normal
  push rejects as stale, POST the reset as admin, verify the push is accepted. This tests
  the operator runbook path end to end (ADR-0009 §2).
- **Known trap:** `RateLimitPerMin` must be ≥ 1 in test configs (`allow()` divides by it;
  config validation makes 0 unreachable in production).

## Risks / Trade-offs

- [Stacked on an unmerged branch] → this change's tests extend files that only exist on
  `juliano/fix-review-findings`; rebase after it merges. Do not implement against `main`.
- [Route paths may drift during rebase] → assert against the mux's registered patterns
  (`/v1/admin/reset-watermark`, `/v1/node/{certname}/diff`, `/v1/nodes/{certname}/state`)
  as they exist post-merge; the spec deltas are path-agnostic on purpose.
