## 1. Pin concurrent budget behavior

- [x] 1.1 Add deterministic synchronized tests with explicit timestamps for budget admission with zero, partial, full, and refilled tokens; prove concurrent admissions never exceed available capacity without sleeps or a Clock interface.
- [x] 1.2 Add mixed-outcome tests proving failed authentication consumes a reservation while successful authentication and insufficient-role authorization settle immediately without debit.
- [x] 1.3 Preserve HTTP tests for sequential 401→429 lockout and valid-principal propagation; use a blocked downstream handler to prove admitted successes release capacity immediately after authentication, and cover that excess concurrent valid requests may receive 429.
- [x] 1.4 Add a deterministic cross-source fallback test covering in-flight reservation, successful release, and failure debit after the source-map cap.

## 2. Make failure accounting atomic

- [x] 2.1 Replace each raw failure limiter map value with the existing limiter plus per-budget mutex and in-flight reservation state, including the shared fallback.
- [x] 2.2 Reserve capacity atomically before credential evaluation and settle each reservation exactly once immediately after `Authenticate` returns, before role checks or handler invocation.
- [x] 2.3 Debit the limiter only for `Authenticate` errors; preserve 403 authorization handling and successful-request concurrency.
- [x] 2.4 Remove the split `Tokens` peek and ignored post-failure `Allow` result; assert the reservation/token invariant in focused tests.

## 3. Verify security and concurrency

- [x] 3.1 Run `go test ./internal/query` and confirm the HTTP/auth contract remains green.
- [x] 3.2 Run `go test -race ./internal/query` and confirm concurrent budget accounting is race-free.
- [x] 3.3 Run `make test` and `make race` to confirm no read/admin or cross-package regression.
