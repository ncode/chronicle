## ADDED Requirements

### Requirement: Whole-snapshot apply is the store's only write door

The store SHALL expose exactly one operation that writes fact data — the whole-snapshot
apply (the per-node serialized transaction of the ingest contract) — alongside the node
lifecycle operations (expire, deactivate, watermark reset). The interval-level write
primitives (node locking, path interning, close/open of durable intervals, volatile
upsert, contact marking) SHALL be internal to the store module and SHALL NOT appear in its
exported interface; no exported store operation SHALL take a database transaction handle.

#### Scenario: External writes inherit the guards
- **WHEN** any package outside the store module writes durable history or volatile state
- **THEN** the write goes through the whole-snapshot apply and inherits the per-node lock,
  the staleness/skew guards, and whole-snapshot atomicity

#### Scenario: Test seeding goes through the write door
- **WHEN** an integration test in another package seeds fact data for a Node
- **THEN** it seeds via the whole-snapshot apply with strictly increasing
  `producer_timestamp` per Node, so seeded history is guard-consistent (no degenerate
  intervals, watermark always advanced)

### Requirement: Temporal-table SQL lives only in the store module

SQL that reads or writes the temporal tables (`fact_history`, `fact_paths`) SHALL live
only in the store module (including the scans backing the high-churn and
path-cardinality alarms); other packages obtain that data through store operations,
supplying their own windows and thresholds. The read surface's compiled DSL SQL over the
temporal views and `nodes`/`node_volatile` (ADR-0008) is a distinct, sanctioned path and
is out of scope for this requirement.

#### Scenario: Churn scan served by the store
- **WHEN** the monitor computes high-churn findings for a window
- **THEN** the `fact_history` scan is executed by a store operation taking the window as a
  parameter, and the monitor applies its thresholds to the returned rows

#### Scenario: Cardinality scan served by the store
- **WHEN** the monitor computes per-Node `fact_paths`-cardinality findings
- **THEN** the scan is executed by a store operation, and the monitor owns only the
  alarm decision
