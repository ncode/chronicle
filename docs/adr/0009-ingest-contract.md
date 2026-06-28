# The ingest contract: absence semantics, timestamp bounds, serialization, limits, backpressure

The adversarial red-team (cross-model) confirmed the data model is sound and that every
real gap is at the **ingest edge**. This ADR pins the ingest accept/reject contract. It
extends ADR-0002 (agent) and ADR-0006 (temporal anchor). None of it changes the schema's
shape; all of it is additive.

## 1. Absence semantics — distinguish "removed" from "not observed"

`facts` is a one-shot partial-failure discovery: a transiently-failing resolver or external
script omits its leaves while the rest of the snapshot resolves (`engine.go:126-129`). So a
**missing leaf is ambiguous**, and blindly tombstoning it fabricates a remove-then-re-add
cluster identical to the reprovision signal the timeline exists to detect.

Rule: **carry an absent durable leaf forward iff a source failed this run; otherwise
tombstone it.**

- The agent runs `facts` in **library mode** (`Discover()`, not the CLI) — library mode
  surfaces per-source failures via `errors.Join` (`external.go:144`); the CLI swallows them.
- The push carries a **discovery-status report**: built-in resolvers `{namespace → ok|error}`
  and external sources `{file → ok|error|absent}`. The external set is built from the joined
  error plus a directory enumeration of the external dirs, so "script present-but-failed"
  (carry forward) is unambiguously distinct from "script gone" (tombstone — `external.go:156-158`
  skips an absent source with no error).

**v1 mechanism — a discovery-clean gate (implementation refinement).** The original draft had
chronicle record per-leaf **fact→source provenance** via `ResolvedFact.File` to carry forward
*exactly* the leaves a failed source produced. Implementation against the real `facts` public
API showed this is **not available**: `ResolvedFact` is an `internal/engine` type (not
importable) and its `.File` is populated only for *cache* facts — empty for live external
facts. So v1 derives a single **discovery-clean** boolean from the status report and applies
it as the gate: **tombstone absent durable leaves only when discovery was clean (no built-in
`error`, no external `error`); if any source erred, carry *all* absent leaves forward** and
let them tombstone on a later clean cycle. An `absent` external file is *not* an error, so the
clean-and-gone case still tombstones. This matches every concrete case below; it is only
coarser in a mixed run (one source fails while an unrelated leaf is genuinely dropped), where
it safely *defers* the tombstone rather than risking a fabricated remove-then-re-add. Per-leaf
source scoping is **deferred-with-trigger**: it un-defers if `facts` grows a public per-fact
provenance API (the maintainer owns `facts`), or if chronicle adds a server-side
source-attribution column.

Worked example — external fact `rpm_packages`, host reinstalled RHEL→Debian: the rpm script
is gone, no error is attributed, discovery is clean, so `rpm_packages` is tombstoned (correct),
as part of the same reprovision cluster that flips `os.name`, `machine-id`, install date. A
*transiently* failing rpm script (still present, times out) makes discovery dirty, so the
absent `rpm_packages` leaves are carried forward. (Observer-removed-but-thing-present stays an
accepted ambiguity, disambiguated by the surrounding change cluster; we do not emit a source
inventory in v1 — see [[ADR-0001]] dumb-node principle.)

## 2. producer_timestamp is bounded on *both* sides

ADR-0006 guarded only the lower bound. A forward clock excursion (RTC-before-NTP) or a
malicious far-future stamp would otherwise become "now" and wedge the node out of ingest for
years. Therefore:

- **Reject** `producer_timestamp > received_at + max_skew` (default 5 min, configurable) and
  do **not** advance `last_producer_ts` for a rejected push.
- The push **response** states applied-vs-rejected (and why), so the agent and operators see
  sustained rejects; alarm on sustained per-node rejects.
- An **operator command resets `last_producer_ts`** to recover a node poisoned before this
  guard existed.

## 3. Per-node ingest is serialized and atomic

The `last_producer_ts` compare-and-apply is otherwise an unlocked read-modify-write; two
concurrent/retried pushes for one certname could drop the newer value or write
`valid_to < valid_from`. The partial-unique index guards single-open-interval, not write
*ordering*. Therefore each push is **one transaction** that:

1. takes a per-node lock (`SELECT … FOR UPDATE` the `nodes` row, or `pg_advisory_xact_lock`);
2. evaluates the staleness + skew guards;
3. applies the whole-snapshot diff (all close/open + volatile upsert);
4. advances `last_producer_ts`.

Add `CHECK (valid_from < valid_to)` to `fact_history`. Retry on unique-violation. Re-applying
an identical snapshot (retry after a lost response) is a no-op (value-hash close/open is
idempotent; an equal `producer_timestamp` is rejected by the guard).

## 4. Ingest resource bounds

One authenticated node must not be able to bloat the shared, never-GC'd `fact_paths`
dictionary or inject millions of rows. Enforce hard caps — snapshot bytes, leaf count, path
length, single-value bytes — rejecting oversized pushes with a typed error; a per-certname
rate limit; and an alarm on per-node `fact_paths`-cardinality spikes (reusing the ADR-0007
high-churn machinery).

## 5. Backpressure

When Postgres saturates, ingest **fast-fails `503` + `Retry-After`** under bounded
concurrency (never unbounded-block/buffer to OOM). The agent retries with jittered backoff a
bounded number of times, then defers to its next timer. The data path is loss-tolerant
(gaps are visible, see runbook "unknown coverage"), so no durable spool in v1.
