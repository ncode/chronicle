# Durable/Volatile classification: config-driven, durable-by-default, server-side

Every fact path is **Durable by default**. A config-driven list of **Volatile** path-patterns
(`uptime`, `memory.system.available_bytes`, `load*`, …) routes the churny handful to the
latest-only `node_volatile` blob; everything else goes to the temporal `fact_history`.

The split is applied **server-side**: nodes push the full snapshot, chronicle classifies.
This is ADR-0001 again (dumb node, smart center) — policy stays central (edit once, no agent
redeploy across 100k nodes), and the only cost, bandwidth, is a non-issue (~9 Mbit/s
aggregate at 100k nodes / 30 min).

A **high-churn alarm** flags Durable paths that open an interval on almost every push, so a
human adds them to the Volatile list. We do **not** auto-reclassify — its semantics for a
path's existing history are ugly and non-deterministic. Reclassification is **forward-only**
by default: flip the config, new data routes the new way, old history is left as-is (an
optional one-time backfill is possible but not automatic).

## Rejected

- **Auto-detect / reclassify churn** — adaptive machinery, muddy history semantics.
- **Upstream tagging in `facts`** — couples chronicle to a data-source change and pushes
  policy out of chronicle.
- **Agent-side split** — violates dumb-node and forces an agent redeploy to change policy.
