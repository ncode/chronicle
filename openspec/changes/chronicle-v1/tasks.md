## 1. Project scaffolding

- [x] 1.1 Initialize the Go module (`go 1.26`) and repo layout: `cmd/chronicle` (server), `cmd/chronicle-agent` (agent), `internal/{store,ingest,agent,query,lifecycle,config}`
- [x] 1.2 Add dependencies: `ncode/facts` (library mode), `ncode/facts-ca`, a PostgreSQL driver (`pgx`), a JWT/OIDC validation lib
- [x] 1.3 Set up structured logging (`slog`), config loading for server + agent, and a Makefile/CI (build, `go vet`, test)

## 2. Storage layer (temporal-store)

- [x] 2.1 Migrations runner that embeds and applies `docs/schema/v1.sql`; later index additions use `CREATE INDEX CONCURRENTLY`
- [x] 2.2 Path interning: get-or-insert into `fact_paths` by `path_text` (`ON CONFLICT DO NOTHING` + select)
- [x] 2.3 `value_hash` computation: sha256 over a type-tagged canonical form so `1 ≠ "1" ≠ 1.0`; define the array/object canonicalization rule (deterministic ordering) to avoid spurious churn
- [x] 2.4 Close/open interval apply primitive: close-old + open-new in one transaction; a no-op when the value is unchanged (change-only dedup)
- [x] 2.5 `node_volatile` upsert (overwrite-in-place, latest-only)
- [x] 2.6 Reconstruction queries + SQL views: `now` (`valid_to='infinity'`), `state-at-T` (`validity @> T`), `diff(T1,T2)` including deletions (closes with no matching open)
- [x] 2.7 Tests: interval correctness, the open-interval integrity boundary (no double-open), `CHECK (valid_from < valid_to)`, diff-includes-deletions

## 3. Ingest service (fact-ingest)

- [x] 3.1 mTLS server (facts-ca CA pool, `RequireAndVerifyClientCert`); derive certname from `VerifiedChains[0][0]` CN — never the body; ignore any body-claimed certname
- [x] 3.2 CRL enforcement at TLS termination (revoked cert cannot connect)
- [x] 3.3 Snapshot decode + resource caps (bytes / leaf count / path length / single-value bytes) → typed reject; per-certname rate limit
- [x] 3.4 Two-sided clock guard: reject `producer_ts <= last_producer_ts` (stale) and `producer_ts > received_at + max_skew` (future, default 5 min); never advance the watermark on reject
- [x] 3.5 Server-side Durable/Volatile classification via a config volatile-path policy (durable by default); route durable→`fact_history`, volatile→`node_volatile`
- [x] 3.6 Absence semantics: parse the discovery-status report; carry an absent durable leaf forward iff a source failed this run (discovery-clean gate, ADR-0009 §1); otherwise tombstone. Per-leaf `ResolvedFact.File` provenance unavailable in the facts public API — deferred-with-trigger
- [x] 3.7 Per-node serialized atomic apply: one transaction — `FOR UPDATE` the node row → guards → classify → absence-scoped close/open diff → volatile upsert → advance `last_producer_ts`; idempotent on re-apply
- [x] 3.8 Backpressure: bounded ingest concurrency; `503 + Retry-After` under saturation
- [x] 3.9 Push response contract (applied-vs-rejected + reason); operator command to reset a poisoned `last_producer_ts`
- [x] 3.10 Tests: CN-from-chain (reject body spoof), stale + future reject, concurrent same-node serialization, rpm_packages absence (reinstall→tombstone vs transient→carry-forward), idempotent re-apply, oversized reject

## 4. Node agent (node-agent)

- [x] 4.1 `facts.Discover()` library-mode integration → snapshot tree
- [x] 4.2 Build the discovery-status report: built-in `{namespace→ok|error}` from the joined error; external `{file→ok|error|absent}` from the joined error + external-dir enumeration. `ResolvedFact.File` per-leaf provenance unavailable in the facts public API → report is per-source; server derives a clean/dirty gate (ADR-0009 §1)
- [x] 4.3 Stamp `producer_timestamp` at collection; assemble the push payload (tree + status report)
- [x] 4.4 mTLS client (facts-ca cert) POST to chronicle; honor server-advertised caps
- [x] 4.5 Timer with per-node jitter; on `503`/failure → bounded jittered backoff → defer to next timer (no durable spool)
- [x] 4.6 Tests: discovery-status accuracy under simulated resolver/script failure; retry/backoff behavior

## 5. Read surface (fact-query)

- [x] 5.1 Read/admin endpoint (server-TLS, no client cert): OIDC bearer-token validation (relying party, JWKS) + static API tokens; map claims → reader/admin role; reject node certs
- [x] 5.2 DSL lexer + parser: shape 1 (compound equality filter), shape 2 (project + filter + group-by), uniform `at <T>` qualifier; define literal typing (quoted→string, bare numerics→number, `true|false|null`)
- [x] 5.3 DSL→SQL compiler: `INTERSECT` of current-row lookups (filter); `GROUP BY` (aggregation); `validity @> T` (at T); volatile-path `at <past>` → typed "no history" error
- [x] 5.4 `node_diff(certname, T1, T2)` endpoint (includes deletions); `include_inactive` opt-in; exclude deactivated/expired from "now" by default
- [x] 5.5 Tests: DSL parse/compile, at-T correctness, volatile-at-past error, authz (node cert rejected; reader vs admin)

## 6. Node lifecycle (node-lifecycle)

- [x] 6.1 Identity resolution: certname → node row, create on first contact, same CN continues one history
- [x] 6.2 Expiry sweep: mark `expired` after N days without contact (default 7); reversible — a returning push resumes; excluded from "now" by default
- [x] 6.3 Deactivation (terminal sunset): operator action closes the node's open intervals at deactivation time + marks `deactivated`; ingest rejects a deactivated certname; return only via a new identity
- [x] 6.4 Tests: rebuild continues history; expiry round-trip (expire → return → resume); deactivation rejects further pushes and seals the timeline

## 7. Observability & config

- [x] 7.1 Config: hot-reloadable volatile-path policy; `max_skew`, caps, rate limits, expiry TTL
- [x] 7.2 Metrics: ingest rate, reject-stale / reject-future rates, apply latency, lag, per-node `fact_paths` cardinality
- [x] 7.3 High-churn alarm: flag durable paths opening intervals on nearly every push (aggregate per-push churn) and suggest reclassification to volatile
- [x] 7.4 Health / readiness endpoints

## 8. End-to-end verification

- [x] 8.1 Integration test against a real Postgres: agent → ingest → store → query (push, now, state-at-T, diff)
- [x] 8.2 Scale smoke: simulated N-node ingest, cold-start enrollment ramp, correlated change-burst (fleet patch) write-rate check
- [x] 8.3 Runbook: migrations, enrollment ramp, backpressure behavior, deactivation procedure, poisoned-watermark reset
