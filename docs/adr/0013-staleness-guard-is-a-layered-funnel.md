# The staleness guard is a layered funnel

Chronicle rejects stale, skewed, and deactivated pushes at several points instead of
consolidating every guard into one store check. That shape has now been reviewed twice
and kept deliberately: each layer rejects at the cheapest point that still has the
information it needs. This records why, so consolidation is not re-proposed.

The layers are:

- **`ingest.plan` pre-DB skew check.** The HTTP handler captures the server receive time
  and `plan` rejects `producer_timestamp > received_at + max_skew` before any database
  work. This is a trust-boundary check on the server clock, independent of node state.

- **`PeekNode` unlocked pre-intern check.** Before interning pending durable paths,
  `ApplySnapshot` peeks at existing node state and rejects pushes already known to be
  stale, deactivated, or too far in the past for first contact. This protects the shared,
  never-GC'd `fact_paths` dictionary. Growth from rejected pushes is invisible to the
  cardinality alarm because rejected pushes write no `fact_history` rows, so preventing
  avoidable interning is load-bearing.

- **Authoritative under-lock guards.** ADR-0009 section 3 requires one serialized
  per-node transaction. After `lockNode` holds the row lock, `ApplySnapshot` re-checks
  deactivation and watermark staleness and applies the first-contact past bound using the
  authoritative `inserted` result. Keying that bound on `inserted`, not a nil watermark,
  keeps `ResetProducerTS` recovery working after an operator clears `last_producer_ts`.

- **`applyDurable` interval-overlap backstop.** The durable apply step rejects observation
  times that would overlap existing open or closed intervals. This covers the post-reset
  nil-watermark path: even after `ResetProducerTS`, closed history can still extend past a
  proposed recovery push, and the schema invariant must win. `ApplySnapshot` maps this
  internal `errStaleApply` back to `ErrStale`.

## Decision

Keep the staleness guard as a layered funnel. Do not collapse `plan`, `PeekNode`, the
under-lock guards, and `applyDurable` into one check. The duplication is intentional:
each layer has a different cost, state view, and failure boundary.

## Consequences

- Rejects that need no database work stay outside the database.
- Rejects that would otherwise grow `fact_paths` are stopped before interning.
- The row-lock transaction remains the authoritative ADR-0009 section 3 guard.
- The interval-overlap check remains as the schema-invariant backstop, especially for
  watermark-reset recovery paths.

## Deferred / rejected

- **Rejected - consolidating all staleness checks into `ApplySnapshot`.** Reviewed twice
  and rejected both times. It would either keep the `PeekNode` resource-bound check
  anyway, or allow rejected pushes to grow `fact_paths` without visibility in the
  cardinality alarm.
- **Rejected - replacing the inserted-keyed first-contact bound with a nil-watermark
  bound.** A nil watermark also exists after `ResetProducerTS`; using it would wedge the
  operator recovery path.
- **Rejected - removing the `applyDurable` overlap backstop.** The watermark guard is not
  sufficient after resets and tombstone-only history. The schema no-overlap invariant
  needs a local guard at the write primitive.
