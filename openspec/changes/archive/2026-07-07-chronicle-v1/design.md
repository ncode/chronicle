# Design — chronicle v1

> The HOW. Architecture distilled from `CONTEXT.md` and `docs/adr/0001–0011`; those
> hold the full rationale. Decisions here cite the ADR they come from and are not
> re-litigated.

## Context

Chronicle is a greenfield Go service plus a node agent. It builds on two existing
`ncode` libraries: **`ncode/facts`** (a Facter port — library + CLI, no network surface,
~250–280 facts/node, one-shot **partial-failure** discovery, and notably **no timestamps**)
and **`ncode/facts-ca`** (a Puppet CA port providing mTLS node identity via RSA client
certs, with a CRL). `facts` discovers; `facts-ca` authenticates; chronicle adds the part
neither has: **per-fact temporal history** in a single PostgreSQL store.

The target is **50 → 100,000 nodes**, a single shared deployment that grows with the
company. The design was hardened by a **cross-model adversarial red-team** (ADR-0009): the
**data model survived intact** — every real gap was at the ingest edge, and **all fixes were
additive** (absence semantics, two-sided clock guard, serialized atomic apply, resource
caps, backpressure). The schema shape (`docs/schema/v1.sql`) did not change.

## Goals / Non-Goals

**Goals**
- A historical, **queryable fact inventory**: *now*, *state-at-T*, and *diff(T1,T2)*.
- **Near-idle nodes** (a periodic `facts` run + an mTLS POST; no resident query engine).
- **Tens-of-GB/year** storage at 100k nodes (change-only, volatile kept latest-only).
- **Authenticated collection** (facts-ca mTLS, CN identity from the verified chain).
- A small purpose-built **read DSL** with a uniform `at <T>` qualifier.

**Non-Goals** (each is *deferred-with-trigger* in an ADR, not rejected forever)
- Value interning (ADR-0004) — inline `jsonb` + `value_hash` in v1.
- OLAP / deep-history rollups (ADR-0003, 0008) — Postgres serves indexed point lookups.
- Cross-node change-feed queries (ADR-0008).
- Full bitemporal — no separate transaction-time/valid-time, no interval splitting (ADR-0006).
- Node lineage across re-identification (ADR-0011).
- HA beyond stateless replicas behind a load balancer (ADR-0009 §5).
- Tamper-evident / signed storage (ADR-0001) — provenance is "authenticated at collection."
- An osquery-style on-node query engine (ADR-0001) — **dumb node, smart center.**

## Decisions

**Dumb-node / smart-center push model (ADR-0002, 0001).** Each node runs a small agent that,
on a self-jittered timer, runs `facts.Discover()` and POSTs the snapshot over facts-ca mTLS;
chronicle is a single ingest endpoint and never reaches out to nodes. *Rationale:* the only
model that reuses facts-ca, keeps nodes near-idle with no inbound port, and removes
node-reachability and credential-fan-out problems. *Rejected:* server-pull (SSH /
chronicle-initiated mTLS), which brings its own auth and a reachability problem.

**Postgres, temporal, change-only (ADR-0003).** Store fact *changes* as validity intervals
(`valid_from`/`valid_to`), never periodic full snapshots: *now* = `valid_to = infinity`,
*state-at-T* = `valid_from ≤ T < valid_to`, *diff* = intervals opened or closed in the window.
*Rationale:* forced by scale — full snapshots are ~35 TB/year at 100k nodes, change-only is
tens of GB/year, and plain SQL+indexes answer all three query shapes. *Rejected:*
ClickHouse/Timescale (OLAP we don't need), embedded KV (hand-build a query layer), custom AOF
(reinvent the database).

**Flatten + intern paths, inline values, NO value interning (ADR-0004).** Snapshots are
flattened to leaf dotted-paths **in Go, never in SQL**; paths are interned in `fact_paths`
(low cardinality, clear win). Durable values are stored **inline** as `jsonb` plus a 32-byte
`value_hash` (sha256 over a type-tagged canonical form, so `1 ≠ "1" ≠ 1.0`, and it dodges
Postgres's 8191-byte btree limit). *Rationale:* PuppetDB built content-addressed value
interning (migration 64) then **removed it** (migration 66) under write contention and join
cost; chronicle's single-digit writes/sec post-dedup hits neither that contention nor the
storage pressure interning would relieve. *Rejected:* full interning now (PuppetDB's
64→66 reversal is the cautionary tale); per-node JSONB blob for durable facts (no per-fact
history — the whole point).

**Node identity is the certificate CN (ADR-0005).** Identity = the Common Name of the
facts-ca client cert (`nodes.certname UNIQUE`), read from the **verified mTLS chain on every
push, never from the snapshot body**. Same certname = same Node = one continuous history; a
rebuild keeping the certname continues the timeline (and surfaces as a forensic change
cluster). *Rationale:* a node cannot assert another's identity; hostname is just a durable
fact. *Rejected:* hardware/machine-id identity (would fragment history on every rebuild).
*Known footgun:* recycling a certname onto a different machine silently merges timelines —
handled operationally (deactivation + revocation), resolved on the disciplined path by
ADR-0011.

**producer_timestamp anchor + two-sided guard (ADR-0006, 0009 §2).** Intervals are anchored
on a `producer_timestamp` the agent stamps at collection (the node clock — chronicle answers
"what was true *on the node* at T," not ingest lag); `received_at` (server clock) is also
recorded for liveness. **Lower bound** (ADR-0006): reject any push with
`producer_timestamp ≤ nodes.last_producer_ts`, making each node's chain strictly monotonic.
**Upper bound** (ADR-0009 §2): reject `producer_timestamp > received_at + max_skew`
(default 5 min) so a forward clock excursion can't wedge a node out of ingest for years; an
operator command can reset a poisoned `last_producer_ts`. *Rationale:* PuppetDB's
`replace-facts!` move; correct for live, forward-only push. *Rejected:* `received_at` as the
anchor (swaps clock-skew fuzz for ingest-lag fuzz); full bitemporal.

**Server-side Durable/Volatile classification (ADR-0007).** Every path is **Durable by
default**; a config-driven list of Volatile path-patterns (`uptime`,
`memory.system.available_bytes`, `load*`, …) routes the churny handful to the latest-only
`node_volatile` blob. The split is applied **server-side** — nodes push the full snapshot,
chronicle classifies. *Rationale:* dumb-node again — policy stays central (edit once, no
agent redeploy across 100k), and the only cost (bandwidth, ~9 Mbit/s aggregate) is a
non-issue. Reclassification is **forward-only**; a high-churn alarm flags Durable paths that
churn every push so a human adds them to the list. *Rejected:* auto-reclassify (muddy history
semantics), upstream tagging in `facts`, agent-side split.

**A small read DSL with `at <T>`, not PQL (ADR-0008).** Reads go through SQL views/functions
that encapsulate the `valid_to = infinity` discipline, plus a small purpose-built DSL over two
shapes: compound equality filter → matching nodes (`role=web os.name=bla`, AND-ed, compiled to
an `INTERSECT` of indexed current-row lookups) and project + filter + group-by
(`role where os.name='bla' group by role`). A uniform **`at <T>`** qualifier (default now)
runs either shape at a point in time. Per-node diff is a separate `node_diff(certname, T1, T2)`
function. *Rationale:* two query shapes don't justify a general AST engine. *Rejected:*
PuppetDB-style PQL/AST compiler (thousands of lines for unneeded generality), raw-SQL-only
(temporal footguns in every caller), GraphQL.

**Machines use mTLS, people use OIDC/tokens (ADR-0010).** Two endpoints, two mechanisms:
nodes hit the **ingest endpoint** with facts-ca mTLS, **push-only** (a node cert can never
read); people and automation hit a **separate read/admin endpoint** with **bearer tokens over
server-TLS, no client certs** — humans via OIDC (chronicle is a relying party validating JWTs
against the company IdP's JWKS, mapping a groups/roles claim to reader/admin), automation via
static API tokens. *Rationale:* certs are right for machine identity but a people-annoyance;
this makes facts-ca's "any CA-signed cert = admin" **structurally irrelevant to reads** (that
endpoint accepts no certs). *Rejected:* certs for humans, chronicle embedding an IdP, one
mechanism for both.

**Deactivation = terminal sunset; Expiry = soft (ADR-0011).** **Expired** (auto, 7 days no
contact) is reversible — a push un-expires it; excluded from "now" by default, closes nothing.
**Deactivated** (operator action) is terminal: chronicle **rejects all further pushes** for
that certname and **seals the timeline** (closes open durable intervals at deactivation time);
the only return is a **new certname**, and the old one is never reused. *Rationale:* resolves
ADR-0005's recycle footgun on the disciplined path. CRL revocation is independent, enforced at
TLS. *Rejected:* reactivation, auto-purge of history (contradicts forensic keep-forever).

**Per-node serialized atomic apply (ADR-0009 §1, §3, §4).** Each push is **one transaction**
that (1) takes a per-node lock (`SELECT … FOR UPDATE` on `nodes`, or `pg_advisory_xact_lock`),
(2) evaluates the staleness + skew guards, (3) applies the whole-snapshot diff (all close/open
+ volatile upsert), (4) advances `last_producer_ts`. *Rationale:* the partial-unique index
guards single-open-interval, not write *ordering*; serialization prevents concurrent/retried
pushes from dropping a newer value or writing `valid_to < valid_from` (also guarded by
`CHECK (valid_from < valid_to)`); re-applying an identical snapshot is a no-op.
**Absence semantics** (§1): a missing durable leaf is **carried forward iff discovery erred
this run; otherwise tombstoned** — driven by the agent's per-source **discovery-status report**
(library-mode `Discover()` surfaces per-source failures via `errors.Join`; the CLI swallows
them), from which the server derives one discovery-clean gate so "script-present-but-failed"
(carry, an `error`) is distinct from "script-gone" (tombstone, an `absent` with no error).
Per-leaf `ResolvedFact.File` provenance proved unavailable in the `facts` public API, so v1
uses the whole-pass gate; per-leaf scoping is deferred-with-trigger (ADR-0009 §1).
**Resource caps** (§4): hard limits on snapshot bytes, leaf count, path length, single-value
bytes, plus a per-certname rate limit and a `fact_paths`-cardinality alarm, so one node can't
bloat the never-GC'd dictionary.

## Risks / Trade-offs

- **Correlated change bursts (a fleet-wide patch flips a durable fact on every node at once)
  spike the write rate** → bounded ingest concurrency + per-node batched whole-snapshot apply
  in one transaction (ADR-0009 §3, §5).
- **Cold start of 100k nodes — every leaf is an all-INSERT with no dedup possible** →
  rate-limited enrollment ramp (ADR-0009 §4) plus `COPY` bulk-load for the first snapshot.
- **A misclassified durable fact churns history on every push** → the high-churn alarm flags
  it for a human; forward-only reclassification adds it to the Volatile list (ADR-0007).
- **Per-push read amplification — applying a snapshot probes the open interval for O(leaves
  per node) paths** → the partial covering index on open intervals
  (`fact_history_open_uniq` / `fact_history_now_idx`, `WHERE valid_to = 'infinity'`) keeps each
  probe an index lookup (ADR-0004, schema).
- **Node clock skew (backward or far-future)** → the two-sided `producer_timestamp` guard
  drops stale and rejects far-future pushes without advancing `last_producer_ts`
  (ADR-0006, 0009 §2).
- **Certname recycle of a never-deactivated CN silently merges two machines** → operational
  discipline; the deactivation sunset permanently retires a properly-sunset certname so it can
  never be recycled (ADR-0005, 0011).
- **Single ingest endpoint is a SPOF** → ingest is stateless; run replicas behind a load
  balancer (ADR-0009 §5). Beyond that, HA is a non-goal in v1.

## Migration Plan

Greenfield deploy — there is nothing to migrate *from*:

1. Stand up PostgreSQL (the single store).
2. Run the **migrations runner**, which applies `docs/schema/v1.sql`. Later index additions
   use **`CREATE INDEX CONCURRENTLY`** so they never block ingest.
3. Deploy the `chronicle` server (ingest + query endpoints) — stateless, replicable behind a
   load balancer.
4. Enroll agents via **facts-ca** (auto-issued client certs) and start `chronicle-agent` on
   nodes, throttled by a **rate-limited first-enrollment ramp** to absorb the all-INSERT
   cold-start.

**Rollback** is trivial because v1 is additive/greenfield: stop the agents, drop the schema.
There is no prior data to migrate back.

## Open Questions

Each is a *deferred-with-trigger* item; the trigger un-defers it.

- **Value interning (`fact_values`)** — un-defers when **measured storage exceeds budget**
  (ADR-0004).
- **GiST temporal index** (`btree_gist` EXCLUDE over closed intervals + composite GiST over
  `(path_id, value_hash, tstzrange)`) — un-defers when **cross-node historical `at <past T>`
  runs at scale** (ADR-0004, 0008, schema).
- **Partitioning** (current-vs-closed, or RANGE by `valid_from`) — un-defers past
  **~100M `fact_history` rows** (ADR-0004).
- **`node_volatile` GIN index** — un-defers when **a real cross-node volatile "now" query
  exists** (schema; until then volatile is read per-node by PK).
- **OLAP read-model** — un-defers for **group-by aggregation over deep history with fleet-wide
  time-bucketing** (tens of millions of rows) (ADR-0003, 0008).
- **Node lineage** (linking a returning machine's new certname to its sunset predecessor) —
  un-defers when **forensic continuity across re-identification** becomes a need (ADR-0011).
- **HA topology specifics** beyond stateless replicas behind a load balancer — un-defers when
  ingest availability needs exceed a single LB tier (ADR-0009 §5).
- **The Volatile-path policy's initial contents** — the seed list of patterns
  (`uptime`, `memory.system.available_bytes`, `load*`, …) needs ratifying against a real
  `facts` snapshot before first deploy (ADR-0007).
