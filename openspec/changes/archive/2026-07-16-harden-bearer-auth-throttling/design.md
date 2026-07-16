## Context

The read/admin HTTP adapter checks a source's `rate.Limiter.Tokens()` before credential validation, then calls `Allow()` only after validation fails. Those thread-safe calls do not make the compound check/evaluate/debit sequence atomic: concurrent invalid requests can all observe one available token before any request consumes it.

The broader people-auth deepening proposal did not survive adversarial review. `Authenticator` already hides static-token/OIDC validation and reload semantics, while `Service.require` is the single HTTP authorization seam. This design changes only failure-budget accounting at that seam.

## Goals / Non-Goals

**Goals:**

- Bound concurrent invalid credential evaluations by the source's currently available failure budget.
- Consume budget only when authentication fails; successful authentication and insufficient-role authorization remain uncharged.
- Preserve parallel credential checks up to the currently available failure budget and release successful reservations immediately after authentication.
- Retain bounded per-source state and the shared fallback after the map cap.

**Non-Goals:**

- No new auth package, exported interface, generated mock, account lockout, token format, proxy-IP policy, audit sink, or configuration field.
- No change to OIDC/static-token validation, role mapping, principal propagation, SIGHUP reload, or machine-vs-people endpoint separation.
- No serialization of all credential validation for one source; admission remains bounded because validity is unknowable before evaluation.

## Decisions

### Track in-flight reservations beside each rate limiter

Replace each map value with a small failure-budget state containing the existing `rate.Limiter`, a mutex, and an in-flight reservation count. Admission locks only that budget, compares available tokens with existing reservations, and reserves one slot before credential evaluation. Completion releases the reservation and consumes one limiter token only when authentication failed.

The invariant is: available limiter tokens must cover every in-flight request that could still fail. This closes the first-wave race without charging successful requests.

Alternative: hold a per-source mutex across credential validation. Rejected because a shared proxy address could serialize legitimate OIDC/static-token traffic. Alternative: consume before validation and refund success. Rejected because `rate.Limiter` has no simple immediate-token refund interface and reservation cancellation is time-sensitive.

### Keep authentication outside the budget lock and settle immediately

The budget mutex covers only reservation/accounting state. OIDC verification and static-token comparison run after admission with the mutex released. Immediately after `Authenticate` returns, settle the reservation: only authentication errors debit the budget; success releases capacity before role checks or the downstream handler runs. A valid credential denied by role policy remains authorization, not a failed authentication attempt.

Because validity is unknown at admission, more concurrent valid requests than the available budget can transiently receive HTTP 429. This is the unavoidable cost of bounding the concurrent first wave; admitted valid requests still run in parallel and release capacity as soon as authentication completes.

### Preserve source-map and fallback behavior

The existing bounded source map remains. Once it reaches `maxAuthFailSources`, new sources share one fallback failure budget, including its reservation accounting. One post-cap source can therefore temporarily occupy fallback reservations during a slow credential check and cause other post-cap sources to receive 429; success releases that capacity immediately. No `X-Forwarded-For` trust change is included.

### Test the budget state deterministically

Unit-test the reservation/accounting state with synchronized goroutines so the old split peek/debit sequence would fail reliably. Pass explicit timestamps into budget accounting and use `TokensAt`/`AllowN` so refill tests need no sleep or Clock interface. Keep HTTP-level tests for 401/429 mapping, successful principal propagation, and independent sources. Run the package with the race detector.

## Risks / Trade-offs

- [Slow credential checks reserve capacity longer] → reservations intentionally bound concurrent guesses; successful completion releases them without debit.
- [More valid requests arrive concurrently than available budget] → excess requests can receive 429; size the existing budget for expected per-source concurrency and release successful reservations immediately after authentication.
- [A completion path leaks a reservation] → settle once immediately after `Authenticate` and assert the in-flight count returns to zero in tests.
- [Rate refill and fractional tokens complicate admission] → use explicit timestamps with `TokensAt`/`AllowN` under the budget mutex and table-test zero, partial, full, mixed-success, and refill cases.
- [Fallback sources influence one another after map saturation] → document the stronger in-flight coupling and test cross-source success release/failure debit rather than adding unbounded state.

## Migration Plan

Deploy as a behavior-preserving server update. No configuration, client, schema, or stored-data migration is required; rollback is the prior binary.

## Open Questions

None.
