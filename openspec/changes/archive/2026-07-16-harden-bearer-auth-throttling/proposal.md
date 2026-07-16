## Why

Bearer-auth throttling currently peeks at a source's token budget before credential evaluation and debits it afterward. Concurrent invalid requests can all pass the peek before any debit, so the configured burst does not bound the first brute-force wave.

## What Changes

- Make failure-budget admission and outcome accounting atomic for concurrent requests from one source.
- Reserve capacity for in-flight credential checks, debit only failed authentication, and release successful checks immediately after authentication without consuming the failure budget.
- Preserve independent per-source budgets and the bounded shared fallback after the source map reaches its cap.
- Add deterministic concurrent regression coverage and keep the existing sequential lockout/principal checks.
- Do not create a new auth package or interface, move role/principal/reload ownership, or change machine-vs-people authentication.

## Capabilities

### New Capabilities

- None.

### Modified Capabilities

- `fact-query`: Require concurrent bearer-auth failures to obey the configured per-source budget while successful authentication remains uncharged.

## Impact

- Code: auth-failure budget state and middleware ordering in `internal/query/service.go`.
- Tests: deterministic concurrency coverage in `internal/query`, including `-race` verification.
- Contract: all credential checks reserve admission capacity before validity is known; excess concurrent attempts can receive HTTP 429, while admitted successful checks release capacity immediately and consume no failure budget. Role mapping is unchanged.
- Dependencies, routes, configuration, and schema: no changes.
