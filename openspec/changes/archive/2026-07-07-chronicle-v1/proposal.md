## Why

Operators have no way to ask "what does the fleet look like now," "what did node X look like
at time T," and "what changed" across a fleet of 50→100,000 nodes — without either an
osquery-style resident query engine that eats the nodes, or naive full-snapshot storage that
balloons to tens of TB/year. chronicle v1 fills that gap: it continuously collects system
facts (via the `ncode/facts` library) and stores their **per-fact temporal history** in
PostgreSQL, keeping nodes near-idle and storage at tens of GB/year by writing only changes.
The full design is ratified in `CONTEXT.md` and `docs/adr/0001–0011`; this change
operationalizes it.

## What Changes

- **NEW** a thin node **agent**: on a timer it runs `facts.Discover()` (library mode), stamps
  a `producer_timestamp`, attaches a per-source discovery-status report, and pushes the
  snapshot to chronicle over facts-ca mTLS. (ADR-0002, 0009)
- **NEW** a **fact-ingest** service: mTLS termination keyed on the verified cert CN (never the
  body); the full ingest contract — absence-vs-not-observed tombstoning scoped to succeeded
  sources, two-sided `producer_timestamp` guard, per-node serialized one-transaction apply,
  server-side Durable/Volatile classification, resource caps, and 503 backpressure.
  (ADR-0005, 0006, 0007, 0009)
- **NEW** a PostgreSQL **temporal store**: change-only schema (`docs/schema/v1.sql`) — interned
  `fact_paths`, inline values + `value_hash`, the `fact_history` temporal table with its
  partial-unique open-interval integrity boundary, and the `node_volatile` latest-only blob.
  No value interning in v1. (ADR-0003, 0004)
- **NEW** a **fact-query** read surface: SQL views/functions + a small purpose-built query DSL
  (compound equality filter; project + filter + group-by) with a uniform `at <T>` qualifier,
  plus a `node_diff` endpoint — on a separate endpoint authenticated by OIDC/bearer tokens for
  people, never node certs. (ADR-0008, 0010)
- **NEW** **node-lifecycle**: CN-based identity, soft reversible Expiry, terminal Deactivation
  (sunset), and CRL enforcement at TLS. (ADR-0005, 0011)

No existing behavior is modified or removed — this is the greenfield v1.

## Capabilities

### New Capabilities

- `node-agent`: the per-node collector — timer-driven `facts` discovery, snapshot assembly
  with `producer_timestamp` and a per-source discovery-status report, mTLS push, retry/backoff.
- `fact-ingest`: the authenticated ingest contract — CN-from-chain identity, absence
  semantics, two-sided clock guard, per-node serialized atomic apply, Durable/Volatile
  classification, resource caps, and backpressure.
- `temporal-store`: the PostgreSQL temporal change-only data model — schema, interval
  open/close semantics, the open-interval integrity boundary, and reconstruction queries
  (now / state-at-T / diff).
- `fact-query`: the read surface — SQL views/functions, the query DSL with `at <T>`, the
  `node_diff` endpoint, and OIDC/token authorization for people.
- `node-lifecycle`: identity, Expiry (soft/reversible), Deactivation (terminal sunset),
  and CRL/revocation enforcement.

### Modified Capabilities

<!-- None — greenfield project, no existing specs in openspec/specs/. -->

## Impact

- **New service + agent** (Go 1.26+): two binaries — `chronicle` (ingest + query server) and
  `chronicle-agent` (node collector).
- **New dependency:** PostgreSQL (the single store, 50→100k). A migrations runner ships the
  `docs/schema/v1.sql` schema.
- **Upstream libraries:** `ncode/facts` (library mode, module dependency) for collection;
  `ncode/facts-ca` for node mTLS identity.
- **External integration:** an OIDC IdP (optional, for human read access; static tokens work
  without it).
- **Non-goals (deferred-with-triggers in the ADRs, not in v1):** value interning, OLAP /
  deep-history rollups, cross-node change-feed queries, full bitemporal, node lineage across
  re-identification, HA beyond stateless replicas behind a load balancer.
