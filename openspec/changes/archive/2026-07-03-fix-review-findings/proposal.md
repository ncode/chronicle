# Fix confirmed findings from the 2026-07 multi-agent review

## Why

An adversarially-verified multi-agent review (9 lenses, every finding attacked by two
independent skeptics) confirmed 36 defects. Three of them silently corrupt the temporal
history — the product's core artifact — via paths a single authenticated request or an
operator recovery action can reach; the rest degrade lifecycle correctness, operability,
and DoS resilience. The mechanical baseline is green (build/vet/race/integration), so this
change is pure hardening: no new features beyond wiring one already-promised read endpoint.

## What Changes

- **Degenerate-push rejection (ingest)**: a push whose decoded `tree` is empty/absent, or
  which carries no discovery report, is rejected with `bad_request` instead of being treated
  as a discovery-clean snapshot that tombstones the node's entire durable history and wipes
  its volatile blob. **BREAKING** for clients that deliberately push empty trees (the stock
  agent never does).
- **Closed-interval overlap guard (store)**: the stale-apply guard considers all intervals,
  not just open ones, so a past-timestamped push after `ResetProducerTS` can no longer insert
  an interval overlapping a closed one (two conflicting values at the same T, forever).
- **Awaited graceful shutdown (server)**: `http.Server.Shutdown` results are awaited before
  process exit; the pgx pool closes only after drain.
- **Contact semantics (lifecycle)**: any authenticated, well-formed push from a
  non-deactivated node counts as contact, so an actively-pushing node that is stuck on
  rejects (e.g. stale watermark) is no longer swept as Expired.
- **First-contact lower clock bound (ingest)**: the clock guard's skew bound also applies
  backwards on first contact, so a node with a reset clock cannot fabricate years of history.
- **Caps during Flatten (ingest)**: leaf-count/path-length caps are enforced incrementally
  during the flatten walk, so rejection never materializes the full leaf set; decode-time
  work stays bounded by `max_snapshot_bytes`.
- **Deterministic leaf canonicalization (ingest/wire)**: colliding flattened paths resolve
  deterministically (collision → reject) instead of Go-map-order last-wins that fabricates
  history churn.
- **Auth reload on SIGHUP (query)**: the Authenticator (static tokens, role mappings) is
  rebuilt on reload like policy and CRL already are; removing a leaked token takes effect
  without restart.
- **Per-node state endpoint (query)**: expose the already-implemented `store.Now`/`StateAt`
  ("what did node X look like now / at T") — CONTEXT.md's core promise, currently dead code.
- **Carry-forward observability (ingest)**: a per-node consecutive-dirty-pass signal
  (metric + log) so the documented whole-pass gate (ADR-0009 §1, deliberately deferred)
  can no longer silently freeze tombstones forever. The gate itself is unchanged.
- **Operability/cleanup sweep**: count every reject path in metrics (with reason);
  attribute and audit admin actions (static tokens become named in config — **BREAKING**
  config shape, `{name, token, role}`); throttle bearer-token auth; migration runner rejects
  duplicate versions and no longer risks pool self-deadlock at `pool_max_conns=1`; agent
  re-fetches server limits instead of pinning defaults forever; agent pre-check measures
  what the server enforces; remove dead config knob `oidc.jwks_url` (**BREAKING** config,
  currently ignored anyway), dead declarations, and the bypassed `lifecycle.Deactivate`
  wrapper; align backpressure bound with the pool size; Makefile `clean` removes `*.test`.
- **Test gaps**: ingest HTTP guard paths (413/429/503), policy/CRL/auth hot-reload
  (including fail-closed), end-to-end role enforcement on the read/admin mux, and
  regression tests for every fix above.

- **Docs alignment**: CONTEXT.md's Source entry is amended to match ADR-0009 §1 (per-source
  per-pass report, not per-Fact Source recording), and the durable→volatile reclassification
  limitation is documented.

Out of scope: per-leaf source provenance (deferred-with-trigger per ADR-0009 §1 pending the
upstream `facts` API); durable→volatile reclassification tombstone semantics are clarified
in design (documented, not re-modeled).

## Capabilities

### New Capabilities

None.

### Modified Capabilities

- `fact-ingest`: absence semantics gain a degenerate-push guard (empty tree / missing report
  ⇒ reject, never tombstone); clock guard gains a first-contact past bound; resource bounds
  enforced pre-materialization; deterministic canonicalization; reject/dirty-streak
  observability.
- `temporal-store`: open-interval integrity boundary extended to reject applies that would
  overlap closed intervals.
- `node-lifecycle`: Expiry's "contact" defined as any authenticated well-formed push from a
  non-deactivated node, not only applied pushes.
- `fact-query`: per-node state (now / at-T) endpoint; authenticator participates in SIGHUP
  reload; admin actions attributed and audited; bearer auth throttled.
- `node-agent`: limits re-fetched with backoff instead of pinned at startup; size pre-check
  aligned with the server's enforcement unit.

## Impact

- Code: `internal/ingest` (plan/decode, metrics), `internal/wire` (flatten), `internal/store`
  (guards, migrate), `internal/query` (service, auth), `internal/lifecycle`, `internal/agent`,
  `cmd/chronicle` (shutdown, SIGHUP), `internal/config` (remove dead knob), Makefile.
- API: new read endpoint (per-node state); degenerate pushes now 400; no other wire changes.
- Config: `oidc.jwks_url` removed (was silently ignored); static tokens become named
  `{name, token, role}` entries. Backpressure/pool sizing knobs gain validation.
- No schema migration is strictly required (overlap guard is query-side); design decides
  whether to also add the deferred `btree_gist` EXCLUDE constraint as defense-in-depth.
- Operational behavior: nodes stuck on rejects stop expiring; SIGHUP now reloads auth.
