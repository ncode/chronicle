# Physical schema: temporal durable history, volatile blob, no value interning in v1

Snapshots are flattened (in the Go ingest path, never in SQL) to leaf dotted-paths. Paths
are interned in `fact_paths` (low cardinality, clear win). **Durable facts** go to a
temporal `fact_history` table: one row per `(node, path, value)` over `[valid_from,
valid_to)`, where `valid_to = 'infinity'` is the current value. A change closes the open
interval and opens a new one **in one transaction**; a **partial unique index on open
intervals** (`UNIQUE (node_id, path_id) WHERE valid_to='infinity'`) is the integrity
boundary — it guarantees at most one current value per `(node, path)`. **Volatile facts**
go to a single overwrite-in-place JSONB blob per node (`node_volatile`) and are never
historized.

We deliberately do **not** intern fact *values* in v1 — values are stored inline as `jsonb`
plus a `value_hash` equality key (32-byte sha256 over a type-tagged canonical form, so
`1 ≠ "1" ≠ 1.0`; the hash also dodges Postgres's 8191-byte btree-index limit).

Rationale, from PuppetDB's own history: PuppetDB built content-addressed value interning
(migration 64, `rededuplicate-facts`) and then **removed it** (migration 66, `jsonb-facts`)
because the shared value table caused write contention and join cost under concurrent load;
its replacement is the `stable`/`volatile` JSONB-on-factset model with **no history**.
Chronicle's write rate is single-digit rows/sec post-dedup (ADR-0003), so neither the
contention that sank PuppetDB nor the storage pressure that would justify interning applies.
Inline values meet ADR-0003's tens-of-GB/year budget. Interning is a clean later upgrade
gated on *measured* storage — not a thing to pay for now.

## Deferred, with un-defer triggers

- **Value interning (`fact_values`)** — when storage measurably exceeds budget.
- **GiST temporal `EXCLUDE` + `btree_gist` + range index** — when out-of-order writes need
  overlap protection beyond the staleness guard, or deep point-in-time scans need a range index.
- **`path_array text[]` + trigram GiST on `path_text`** — when a query needs array-index
  access or regex `match()` on paths.
- **Partitioning** (current-vs-closed, or RANGE by `valid_from`) — past ~100M `fact_history` rows.
- **Per-push provenance ledger (`snapshots`)** — when liveness beyond `nodes.last_producer_ts` is needed.

## Rejected

- **Full interning now** — solves a contention/storage problem we don't have at our write
  rate; PuppetDB's mig 64→66 reversal is the cautionary tale.
- **Per-node JSONB blob for durable facts (current PuppetDB)** — no per-fact history, which
  is the entire point of chronicle.

Schema draft: [`docs/schema/v1.sql`](../schema/v1.sql).
