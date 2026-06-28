# PostgreSQL with a temporal, change-only fact model

Chronicle stores facts in PostgreSQL using a temporal, change-only model: we record fact
*changes* with validity intervals (`valid_from`/`valid_to`), never periodic full snapshots.

- **"Now"** = facts still valid (`valid_to = infinity`).
- **State at T** = facts valid at T (`valid_from ≤ T < valid_to`) — reconstructed, not stored.
- **Diff(T1, T2)** = intervals that opened (added/changed) *or* closed (removed/changed-away)
  in the window — a pure deletion is a close with no matching open.

This is forced by scale, not chosen for elegance: at 100k nodes, keeping every snapshot is
~35 TB/year; change-only is tens of GB/year. **Volatile facts** (e.g. `uptime`,
`memory.system.available_bytes`, load) are excluded from history and kept latest-only —
historizing them is the dominant bloat source.

Postgres spans the entire 50 → 100k range for this workload (post-dedup, single-digit
writes/sec, tens of GB/year), and delivers cross-node, point-in-time, and diff queries from
plain SQL + indexes — the store the company already runs.

This aligns with **PuppetDB** (PostgreSQL-backed, structured fact storage). Chronicle adds
**per-fact temporal history**, which PuppetDB does not keep (PuppetDB stores the current
factset per node; its history story is reports with a TTL).

Rejected: ClickHouse/Timescale (OLAP we don't need — our reads are indexed point lookups,
not analytical scans), embedded KV (forces hand-building a query/index layer), and a custom
AOF binary format (reinvents the database).
