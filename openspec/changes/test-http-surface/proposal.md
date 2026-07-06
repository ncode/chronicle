# Test the HTTP surface through its interface

## Why

The interface of both server endpoints is HTTP, but the remaining untested code is exactly
the HTTP glue: routing, role gating, parameter parsing, and error→status mapping. The
internals behind the handlers (`query.compile`, `ingest.plan`, the store operations, the
`Authenticator`) are well tested in isolation — the risk concentrates in the wiring, which
is also the security-sensitive part of the read/admin surface. Three independent
adversarial reviews (two Claude refuters, one codex cross-model pass) confirmed the gap and
found no technical blocker to closing it with the existing test harness — no new seams, no
mocks, no OIDC IdP.

`juliano/fix-review-findings` (unmerged) already covers a large share: role enforcement for
query/deactivate, 401 for unauthenticated, the state and diff endpoints, and ingest's
403/400/413/429/503 reject branches with metrics. This change closes what remains.

## What Changes

- Add HTTP-level tests through `query.Service.Handler()` (httptest + static bearer tokens;
  `CHRONICLE_TEST_DB` only where a handler reaches the store) for the residual gaps:
  - `/v1/admin/reset-watermark`: reader→403, admin→200 with the watermark actually reset,
    missing certname→400.
  - `handleQuery` error→status mapping: missing `q`→400, unparsable DSL→400, DSL longer
    than `maxDSLLen`→413, Volatile-path `at <past T>`→422 (`ErrNoHistory`).
  - `handleDiff`: non-RFC3339 `from`/`to`→400, unknown certname→404.
  - `state` endpoint: non-RFC3339 `at`→400.
- Add `Retry-After` header assertions to the existing ingest 429 (rate-limit) and 503
  (backpressure) handler tests.
- Test-only change: no production behavior changes intended; any bug the new tests expose
  is fixed under this change.

## Capabilities

### New Capabilities

(none)

### Modified Capabilities

- `fact-query`: pin the read/admin HTTP contract as requirements — admin operations
  (deactivate, reset-watermark) are role-gated at the HTTP surface, and DSL/diff/state
  errors map to typed HTTP statuses (400/404/413/422). Behavior already implemented;
  the requirements make it load-bearing.
- `fact-ingest`: pin that load-shed rejects (rate-limit 429, backpressure 503) carry a
  `Retry-After` header. Behavior already implemented.

## Impact

- Affected code: `internal/query/query_service_test.go`, `internal/ingest/ingest_handler_test.go`
  (extend both), no production files.
- Sequencing: **stacked on `juliano/fix-review-findings`** — builds on its
  `query_service_test.go` / `ingest_handler_test.go` helpers and its route set. Rebase this
  change after that branch merges.
- Test infra: role-matrix and error-mapping tests need no Postgres (`require` and param
  parsing reject before any store call); reset-watermark behavior and 422 mapping use the
  existing `CHRONICLE_TEST_DB` harness.
- Aligned with ADR-0012 (injection + real Postgres, no Clock seam) and ADR-0010 (static
  tokens work with zero IdP dependency).
