# Design — fix-review-findings

## Context

A 9-lens adversarially-verified review confirmed 36 defects against a green mechanical
baseline. The three history-corrupting ones share a shape: guards that validate the common
case but not the degenerate one (empty tree, post-reset past push, unawaited shutdown). The
rest are lifecycle/observability/DoS gaps and dead surface. All fixes stay inside existing
packages; no new dependencies, no schema migration required (one optional constraint is
considered and deferred). Findings, verifier traces, and severities are recorded in the
review output referenced by the proposal.

## Goals / Non-Goals

**Goals:**
- No single authenticated request or operator recovery action can silently corrupt
  `fact_history`.
- Lifecycle and operability semantics match CONTEXT.md (contact, expiry, reload).
- Every reject path is observable; the carry-forward gate cannot freeze silently.
- Close the confirmed test gaps with regression tests per fix.

**Non-Goals:**
- Per-leaf source provenance (ADR-0009 §1: deferred-with-trigger on the upstream `facts`
  API; the dirty-streak signal is the v1 mitigation, not a redesign).
- Re-modeling durable→volatile reclassification. Closing the interval on reclassification is
  retained; the limitation (indistinguishable from removal in history) is documented in
  CONTEXT.md instead of adding a close-reason column for a rare operator action.
- Implementing per-Fact Source recording. CONTEXT.md's Source entry currently overstates v1
  (it promises per-Fact Source; ADR-0009 §1 records why v1 has only a per-source, per-pass
  report) — the doc is amended to match the ADR rather than the code stretched to match the
  doc.
- Schema changes. The `btree_gist` EXCLUDE constraint stays deferred (ADR-0004 rationale
  unchanged); the extended stale guard restores the invariant at apply time.
- Volatile-blob behavior on dirty passes (review verdict split; current overwrite keeps
  "now" fresh, which is volatile facts' only job).

## Decisions

1. **Degenerate pushes are rejected in `plan()`, not patched in the store.**
   `plan()` gains two checks: decoded tree flattens to ≥1 leaf, and the discovery report is
   non-empty. Rejecting at the pure-plan layer keeps `ApplySnapshot` semantics untouched and
   testable without a DB. Alternative — treating empty-tree-plus-empty-report as "dirty"
   (carry forward) — rejected: it hides a malformed client instead of surfacing it, and the
   stock agent never legitimately produces such a push (DryRun errors on an empty snapshot).

2. **Stale guard: `t > MAX(valid_from)` over open rows AND `t >= MAX(valid_to)` over
   closed rows.** `MAX(valid_from)` alone is insufficient — a closed `[10:00, 20:00)` has
   `valid_from = 10:00`, so a push at 15:00 would pass it and still overlap. The guard
   computes both bounds in one query (`GREATEST` of max open `valid_from` + 1 tick and max
   closed `valid_to`); equality with a closed `valid_to` is allowed (half-open adjacency).
   Cost: index-assisted by the PK `(node_id, path_id, valid_from)`; measured in the store
   benchmarks. Alternative — adding the EXCLUDE constraint — rejected for v1: heavier (new
   extension + index on the largest table) and ADR-0004 already deferred it; the apply path
   is the only writer.

3. **Shutdown: `Shutdown` calls join the existing errgroup.**
   Each server goroutine pairs with a `g.Go` watcher that calls `Shutdown(drainCtx)` on
   `gctx.Done()` and returns its error; `main` exits on `g.Wait()`. The drain context stays
   derived from `context.Background()` with the existing 10s timeout. This is the canonical
   pattern; no new machinery.

4. **Contact vs reversal split.** `last_seen` advances for any authenticated push that
   reaches decoding or apply — bad-request, collision, cap, skew, and stale-watermark
   rejects included — via a cheap single-row `UPDATE` on the reject path, outside the apply
   tx. Exempt by design: no-cert (unauthenticated), rate-limit excess (the node's admitted
   pushes register contact), and backpressure `503` (must not add store writes while the
   store is saturated). The `expired` flag still clears only on an applied push. Rationale:
   contact is liveness (expiry input), reversal is data (timeline resumption). Counting only
   applied pushes as contact (status quo) falsely expires live nodes; un-expiring on
   rejected pushes would resurrect a node that contributes no data.

5. **First-contact past bound reuses `max_skew` symmetrically, enforced in
   `ApplySnapshot`.** No new knob. The agent collects and pushes in the same cycle, so
   legitimate first pushes are near `received_at`. A larger dedicated window (e.g. 24h) was
   considered and rejected: no real producer needs it, and it re-opens the backdating hole
   it exists to close. Enforcement lives in `ApplySnapshot` under the node lock — not in
   `plan()`, which is deliberately DB-free and cannot know whether a watermark exists; the
   plan struct carries `received_at` and `max_skew` so the store can apply the bound exactly
   when `last_producer_ts` is NULL. Applying the bound to all pushes was rejected: it would
   change semantics for legitimately delayed non-first-contact snapshots already bounded by
   the watermark.

6. **Flatten collisions reject the push rather than pick a winner.** Detection is a map
   lookup during the existing flatten walk (no extra pass). Deterministic last-wins (sorting
   keys) was considered: it silences the nondeterminism but still silently drops data the
   node reported; a typed reject surfaces the broken producer. The agent side flattens with
   the same shared `internal/wire` code, so a collision is caught at collection time too.

7. **Incremental cap enforcement inside `Flatten`.** `Flatten` gains the (already-existing)
   caps as walk-time checks via a small limits struct passed from `plan()`, aborting on
   first violation. This bounds leaf-set materialization, not decode: the body is fully
   JSON-decoded into `map[string]any` before flattening, so decode-time CPU/memory remains
   linear in the `max_snapshot_bytes`-capped body (with JSON's constant-factor expansion).
   That residual is accepted — it is bounded, and the per-certname rate limit plus the
   concurrency semaphore cap aggregate exposure. Alternative — a token-streaming decoder
   that counts leaves before materialization — rejected as a second parser for a bounded
   problem.

8. **SIGHUP rebuilds the Authenticator in two independently-swapped halves.** `reloadOnHUP`
   already rebuilds policy + CRL; it additionally re-reads the auth config. The static token
   set and role mappings are pure configuration and swap unconditionally (they cannot fail
   to build) via the same `atomic.Pointer` pattern used for the policy. The OIDC verifier is
   rebuilt separately; if IdP discovery fails, only the old verifier is kept (fail-closed,
   logged) — a monolithic keep-the-old-authenticator would let an IdP outage silently block
   revocation of a leaked static token, which is the exact scenario this fix exists for.
   The dead `oidc.jwks_url` knob is removed from `internal/config` (go-oidc discovers JWKS
   from the issuer; the knob never did anything). **BREAKING** only for configs that set it:
   config parsing uses `DisallowUnknownFields`, so startup fails with a clear error and the
   fix is deleting one line.

9. **Per-node state endpoint wires the existing dead code — with the gaps closed.**
   `GET /v1/nodes/{certname}/state` (+ optional `at=<RFC3339>`) on the read mux, reader
   role. `store.Now`/`StateAt` take only a `nodeID` and know nothing of lifecycle state or
   volatile data, so the handler (or an extended store call) must add what the read surface
   requires: resolve certname → node and apply the inactive-node default (expired and
   deactivated excluded unless `include_inactive`, matching the baseline fact-query spec),
   and for "now" also read the `node_volatile` blob so current state is complete. Past-`T`
   responses are durable-only with an explicit volatile-unavailable marker (no volatile path
   is referenced, so the DSL's `at <past>` error does not apply). Alternative — deleting
   `Now`/`StateAt` as dead code — rejected: CONTEXT.md names this as a core promise and the
   implementation plus tests already exist; deleting is a docs-level breaking change to the
   product's stated purpose.

10. **Brute-force resistance = per-source-IP failure limiter on the read listener,** reusing
    `x/time/rate` (already a dependency, same pattern as ingest's per-certname limiter).
    Account lockout rejected (no accounts; tokens are bearer). Audit logging is `slog` on
    the existing logger — principal is threaded from auth middleware into the request
    context; no new audit sink in v1. Static tokens become named in config
    (`{name, token, role}` replacing the raw `token → role` map) so the audit principal is
    the operator-assigned name, never the secret and never a bare role. **BREAKING** config
    (loud at startup via `DisallowUnknownFields`), batched with the `oidc.jwks_url` removal
    in one release note. Logging a token hash prefix instead was considered and rejected:
    unreadable in audit trails and still secret-derived.

11. **Backpressure bound is validated against the pool.** `config.validate()` requires
    `ingest max_concurrent + read headroom <= pool max_conns` (pool size becomes explicit
    in config with a sane default) and fails startup otherwise. Runtime-derived sizing
    was rejected: a startup validation error is simpler and this is exactly what
    `validate()` exists for.

12. **Migration runner: fail fast on duplicate versions; run everything on the one locked
    connection.** Duplicate detection is a map check while scanning the embedded FS. Using
    the advisory-lock connection for the migration work removes the
    `pool_max_conns=1` self-deadlock without a second knob.

13. **Metrics: reject counting moves to a helper on the ingest handler** so every early
    return (cert, rate, backpressure, body size, JSON, plan reject) increments
    `pushes_total{result="rejected", reason=…}`. Reason label values are a small fixed
    enum — no cardinality risk. Dirty-streak: an in-memory per-node counter on the ingest
    service (reset on clean apply), exported as a gauge of nodes-over-threshold plus a log
    line; threshold configurable, default 10 consecutive dirty passes. In-memory (not
    persisted) is accepted: a restart resets streaks, which merely delays the alarm by the
    streak length.

14. **Agent limits refetch**: on failed startup fetch, retry on each push cycle (no new
    timer) until success; keep current fallbacks meanwhile. Pre-check measures
    `len(marshaled push body)` — the unit the server caps — replacing the `len(tree)` proxy.

## Risks / Trade-offs

- [Degenerate-push reject breaks a client that deliberately pushes empty trees] → No such
  client exists in-repo (agent DryRun errors on empty snapshots); the reject is a typed
  `bad_request` with a clear message; release-noted as BREAKING.
- [All-rows `MAX(valid_from)` guard scans closed history] → Bounded by PK index; measured in
  the existing store benchmarks before merge (`make bench`).
- [Contact-on-reject adds a write to the reject path] → It is a single-row update, rate-
  limited upstream by the per-certname limiter; batched/deferred variants are premature.
- [Collision reject could brick a node whose facts legitimately collide] → Collision means
  two sources claim the same canonical path — data loss is already occurring silently
  today; the reject makes it visible. Agent-side pre-flight catches it before the wire.
- [Auth hot-swap during in-flight requests] → Same `atomic.Pointer` discipline as policy:
  a request sees either old or new authenticator, never torn state.
- [IdP outage during reload blocks token revocation] → Eliminated by the two-half swap
  (decision 8): the static-token set swaps unconditionally; only the OIDC verifier can be
  held back.
- [Contact-on-reject writes on paths that never reached the store] → Scoped: decode/plan/
  apply rejects only; rate-limit excess and backpressure are exempt, so the touch cannot
  amplify load-shedding into store writes.
- [Removing `oidc.jwks_url` fails startup for configs that set it] → Intentional
  (`DisallowUnknownFields` already rejects unknown keys loudly); documented in release notes
  with the one-line fix.
- [In-memory dirty-streak resets on restart] → Accepted; alarm delayed by at most one
  streak-length, not lost, since the streak rebuilds from live pushes.

## Migration Plan

Single deploy; no schema migration. Rollback = redeploy previous binary (all fixes are
apply-path/read-path logic). Config changes required before deploy: delete `oidc.jwks_url`
if set; convert static tokens to the named `{name, token, role}` form; optionally set the
new pool-size/dirty-streak knobs (defaults preserve current behavior otherwise).

## Open Questions

None blocking.
