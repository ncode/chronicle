## 1. History-corruption guards (highs)

- [x] 1.1 Reject degenerate pushes in `plan()` (`internal/ingest/ingest.go`): zero-leaf decoded tree or empty discovery report → typed `bad_request`; unit tests for missing/empty/null `tree`, missing report, and the operator-curl scenario (no tombstones, no volatile wipe, no watermark advance)
- [x] 1.2 Extend the stale guard in `ApplyDurable` (`internal/store/store.go`): reject unless `t` is strictly after `MAX(valid_from)` over open rows AND `t >= MAX(valid_to)` over closed rows (one `GREATEST` query; equality with closed `valid_to` allowed). Integration tests: (a) full-tombstone pass → `ResetProducerTS` → past push rejected; (b) push at 15:00 into a closed `[10:00, 20:00)` interval rejected; (c) push exactly at a closed `valid_to` accepted
- [x] 1.3 Run `make bench` store benchmarks before/after 1.2 and record the delta in the PR description
- [x] 1.4 Await graceful shutdown in `cmd/chronicle/main.go`: move each `srv.Shutdown(drainCtx)` into the errgroup so `main` exits only after drain (pgx pool closes last); test with an in-flight slow request through a real listener

## 2. Clock, canonicalization, and caps (ingest)

- [x] 2.1 First-contact past bound in `ApplySnapshot` under the node lock (`plan()` is DB-free and cannot know watermark state): plan struct carries `received_at` and `max_skew`; reject `producer_timestamp < received_at - max_skew` only when `last_producer_ts` is NULL; integration tests for both sides of the boundary and for a delayed non-first-contact push being unaffected
- [x] 2.2 Collision detection in `internal/wire.Flatten`: colliding canonical paths → typed error naming the path; unit tests for literal-dot vs nested collision and for deterministic output on identical input
- [x] 2.3 Incremental cap enforcement: thread a limits struct from `plan()` into `Flatten`, abort at first leaf-count/path-length violation; test that an over-cap body stops early (assert via leaf-visit counter or allocation bound in a benchmark)
- [x] 2.4 Agent-side: pre-check measures serialized push body bytes (`internal/agent`); limits refetch with backoff on cycles after a failed startup fetch; unit tests for both

## 3. Lifecycle and store hygiene

- [x] 3.1 Contact-on-reject: advance `last_seen` for any authenticated push that reaches decoding or apply — bad-request, collision, cap, skew, and stale-watermark rejects included; rate-limit excess and backpressure 503 exempt (no store writes under load-shedding); keep `expired` clearing only on applied pushes; integration tests: nodes stuck on stale-watermark, skew, and validation rejects are each not expired by the sweep, and the backpressure path performs no store write
- [x] 3.2 Migration runner (`internal/store/migrate.go`): fail on duplicate version numbers; run migrations on the advisory-lock connection; tests: duplicate-version FS errors at startup, and `pool_max_conns=1` migration completes
- [x] 3.3 Document the durable→volatile reclassification limitation (interval close is indistinguishable from removal) in CONTEXT.md per design non-goal
- [x] 3.4 Amend CONTEXT.md's Source entry to match ADR-0009 §1: v1 records a per-source, per-pass discovery report (whole-pass carry-forward gate), not a per-Fact Source; per-leaf provenance remains deferred-with-trigger

## 4. Query service: reload, endpoint, audit, throttle

- [x] 4.1 SIGHUP rebuilds the Authenticator in two halves (`cmd/chronicle/main.go` + `internal/query/auth.go`): static tokens/role mappings swap unconditionally via `atomic.Pointer`; OIDC verifier rebuilt separately, fail-closed on error (keep old verifier, log); tests: removed token stops authenticating after reload without restart, including while OIDC discovery is failing
- [x] 4.2 Remove dead `oidc.jwks_url` knob from `internal/config`; note BREAKING in release notes
- [x] 4.3 Add `GET /v1/nodes/{certname}/state[?at=RFC3339]` wiring `store.Now`/`StateAt` (reader role): handler resolves certname with the lifecycle filter (expired/deactivated excluded unless `include_inactive`), "now" additionally returns the `node_volatile` blob, past-T returns durable-only with an explicit volatile-unavailable marker; integration tests for now (incl. volatile blob), at-T, the marker, and an expired node hidden by default but visible with `include_inactive`
- [x] 4.4 Named static tokens: config becomes `{name, token, role}` (**BREAKING**, release-noted with 4.2); thread the authenticated principal (OIDC sub or token name) into request context; audit-log successful deactivate/reset-watermark with principal, action, target, time; tests assert the audit line and that the token secret never appears in logs
- [x] 4.5 Per-source failure limiter on bearer auth (`x/time/rate`); test: burst of bad tokens throttled, valid token unaffected

## 5. Observability and config validation

- [x] 5.1 Count every ingest reject path (no-cert, rate-limit, backpressure, body-size, bad-JSON, plan rejects) in `pushes_total{result,reason}`; table test iterating every early-return path and asserting the counter
- [x] 5.2 Dirty-streak signal: per-node consecutive-dirty counter (reset on clean apply), gauge of nodes over threshold + log line naming failing sources; configurable threshold default 10; unit test for streak/reset/threshold
- [x] 5.3 `config.validate()`: explicit pool-size knob; require ingest `max_concurrent` + read headroom ≤ pool max conns; startup fails with a clear error otherwise; unit tests

## 6. Cleanup sweep

- [x] 6.1 Delete dead declarations: `store.DB` interface, `wire.StatusAbsent`, `Node.Expired`, write-only `PathID`/`Shape` fields; delete or wire the bypassed `lifecycle.Manager.Deactivate` wrapper and its unreachable ttl guard
- [x] 6.2 Replace hand-rolled `itoa` in `internal/e2e` with `strconv.Itoa`; make `BenchmarkApplyCPU` call the real `plan()` instead of a hand-copied mirror and drop the reference to the nonexistent findings doc
- [x] 6.3 Makefile: `clean` target removes `*.test` binaries; delete the stale root `ingest.test`
- [x] 6.4 Document (comment in Makefile or test README) that DB-backed tests require `-p 1` because of TRUNCATE-based isolation

## 7. Test-gap closure (review highs)

- [x] 7.1 Ingest HTTP guard-path tests: backpressure 503 (+`Retry-After`), rate-limit 429, oversized body 413 — through a real handler with `httptest`
- [x] 7.2 Hot-reload tests: volatile-policy swap (ingest and query agree post-reload), CRL reload fail-closed, authenticator reload (4.1 covers the happy path; add the fail-closed case)
- [x] 7.3 End-to-end role enforcement test on the read/admin mux: reader token can read but not deactivate; admin token can; unauthenticated gets 401 — direct assertions, no proxies

## 8. Verification

- [x] 8.1 `go test -race ./...` and `make test-db` green; `go vet` clean; benchmarks from 1.3 recorded in the PR description
- [x] 8.2 Re-run the review's high-severity reproduction scenarios (empty-tree curl, post-reset past push, closed-interval mid-overlap push, shutdown with in-flight request) and confirm each is now rejected/handled
