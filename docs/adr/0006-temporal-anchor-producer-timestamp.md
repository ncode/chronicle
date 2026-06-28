# Temporal anchor is the node's producer_timestamp, with a reject-stale guard

`valid_from`/`valid_to` are anchored on a `producer_timestamp` the agent stamps at
collection time (the upstream `facts` library provides no timestamp). Chronicle answers
"what was true *on the node* at time T," so the node's clock is the right anchor — not
chronicle's ingest lag.

Out-of-order and late pushes (inherent to node-initiated push, ADR-0002) are handled by a
staleness guard, exactly PuppetDB's `replace-facts!` move: **reject any snapshot whose
`producer_timestamp ≤ nodes.last_producer_ts`.** This makes each node's interval chain
strictly monotonic — a backwards/skewed clock simply has its stale pushes dropped until it
catches up. `received_at` (server clock) is also recorded, for liveness and debugging.

## Scope / consequences

- **Cross-node "fleet state at T" carries clock-skew fuzz** — node A's "T" and node B's "T"
  are slightly different real instants. `received_at` would not fix this (it swaps clock-skew
  fuzz for ingest-lag fuzz). Tolerable at minute-or-coarser resolution; exact cross-node
  causal ordering is a different, much heavier system and is a non-goal.
- **No full bitemporal in v1** — no separate `transaction_time` vs `valid_time`, no
  late-arriving push splitting an existing interval. Reject-stale is correct for *live* push.
  Add bitemporal only if backfilling historical snapshots or correcting the past becomes a
  hard requirement (both violate "pushes only move forward").
