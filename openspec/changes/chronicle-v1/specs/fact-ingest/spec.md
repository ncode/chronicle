## ADDED Requirements

### Requirement: CN-from-chain identity

The fact-ingest service SHALL derive the Node identity (certname) exclusively from the Common Name of the verified mTLS client-certificate chain presented on each push, and SHALL NEVER read identity from the snapshot body or any request-supplied field. A `certname` value appearing in the request body MUST be ignored. The certname taken from the chain is used to resolve `nodes.certname`; same certname = same Node = one continuous history.

#### Scenario: Identity taken from the verified chain

- **WHEN** a push arrives over mTLS with a client certificate whose CN is `web01.example.com`
- **THEN** the service applies the snapshot to the Node `nodes.certname = 'web01.example.com'`
- **AND** resolves or creates that node row using only the chain CN.

#### Scenario: Body-asserted certname is ignored

- **WHEN** a push from the verified chain CN `web01.example.com` carries a body field claiming the certname is `db99.example.com`
- **THEN** the service ignores the body-asserted value
- **AND** applies the snapshot to `web01.example.com` (the chain CN)
- **AND** a Node cannot assert another Node's identity through the body.

#### Scenario: Missing or unverified client certificate is rejected

- **WHEN** a push arrives without a verified mTLS client-certificate chain (no cert, or a chain that fails facts-ca verification)
- **THEN** the service rejects the push at TLS/authentication
- **AND** does not apply any snapshot or create any node row.

### Requirement: Absence semantics — removed vs not-observed

The fact-ingest service SHALL distinguish a genuinely removed durable leaf from one merely not observed this run, using the per-source discovery-status report carried in the push. The service SHALL derive a single discovery-clean signal from the report — clean iff it contains no built-in `error` and no external `error` entry (an `absent` external file is not an error). When discovery is clean, every absent durable leaf SHALL be tombstoned (its open interval closed at the snapshot's `producer_timestamp`); when discovery is NOT clean, every absent durable leaf SHALL be carried forward (its open interval left untouched) and may tombstone on a later clean pass. The service SHALL NOT blindly tombstone every missing leaf regardless of discovery status, because that would fabricate a remove-then-re-add cluster indistinguishable from a reprovision signal.

> Note (v1, see ADR-0009 §1): per-leaf source provenance (`ResolvedFact.File`) is not available from the `facts` public API, so v1 uses this whole-pass clean/dirty gate rather than scoping carry-forward to exactly the leaves of one failed source. The gate is correct for the single-cause scenarios below and safely conservative (defers, never fabricates) in a mixed run. Per-leaf scoping is deferred-with-trigger.

#### Scenario: Source gone — tombstone (rpm_packages reinstall)

- **WHEN** a Node previously reported `rpm_packages.*` leaves, is reinstalled from RHEL to Debian, the `rpm_packages` external script is now absent, and the discovery-status report attributes no error to that source
- **THEN** the service tombstones the `rpm_packages.*` leaves (closes their open intervals at `producer_timestamp`)
- **AND** does so as part of the same reprovision change-cluster that flips `os.name`, `machine-id`, and the install date.

#### Scenario: Source failed transiently — carry forward

- **WHEN** the `rpm_packages` external script is still present but failed this run (e.g. timed out) and the discovery-status report marks that source as `error`
- **THEN** the service carries the previously-reported `rpm_packages.*` leaves forward
- **AND** leaves their open intervals untouched (no tombstone, no re-open).

#### Scenario: Source succeeded but leaf dropped — tombstone

- **WHEN** discovery is clean (no source reported `error`) but the snapshot no longer contains a previously-open leaf `pkg.foo`
- **THEN** the service tombstones `pkg.foo` (closes its open interval at `producer_timestamp`)
- **AND** treats the clean-discovery-without-the-leaf as a genuine removal.

### Requirement: Two-sided clock guard

The fact-ingest service SHALL bound `producer_timestamp` on both sides relative to the Node watermark and the server clock. It SHALL reject any snapshot whose `producer_timestamp <= nodes.last_producer_ts` (stale / out-of-order) and SHALL reject any snapshot whose `producer_timestamp > received_at + max_skew`, where `max_skew` defaults to 5 minutes and is configurable. A rejected push MUST NOT advance `nodes.last_producer_ts` and MUST NOT apply any snapshot diff. The service SHALL provide an operator action to reset `last_producer_ts` to recover a Node whose watermark was poisoned (e.g. by a far-future stamp accepted before this guard existed).

#### Scenario: Stale snapshot rejected

- **WHEN** a push arrives with `producer_timestamp` less than or equal to the Node's current `last_producer_ts`
- **THEN** the service rejects the push as stale
- **AND** does not advance `last_producer_ts`
- **AND** applies no diff.

#### Scenario: Far-future snapshot rejected

- **WHEN** a push arrives with `producer_timestamp` greater than `received_at + max_skew` (default 5 minutes)
- **THEN** the service rejects the push as skewed
- **AND** does not advance `last_producer_ts`, so the Node is not wedged out of ingest into the future.

#### Scenario: In-bounds snapshot accepted and watermark advances

- **WHEN** a push arrives with `producer_timestamp` strictly greater than `last_producer_ts` and no greater than `received_at + max_skew`
- **THEN** the service applies the snapshot
- **AND** advances `last_producer_ts` to the snapshot's `producer_timestamp`.

#### Scenario: Operator resets a poisoned watermark

- **WHEN** a Node's `last_producer_ts` was poisoned to a far-future value and an operator runs the reset action for that certname
- **THEN** the service resets `last_producer_ts` so subsequent in-bounds pushes are accepted again.

### Requirement: Per-node serialized atomic apply

The fact-ingest service SHALL apply each push within a single database transaction that takes a per-Node lock (either `SELECT ... FOR UPDATE` on the `nodes` row or a `pg_advisory_xact_lock` keyed on the Node), so that concurrent or retried pushes for one certname are serialized. Within that transaction it SHALL, in order: take the lock, evaluate the staleness and skew guards, apply the whole-snapshot close/open diff plus the volatile upsert, and advance `last_producer_ts`. Re-applying an identical snapshot (e.g. an agent retry after a lost response) SHALL be a no-op: the value-hash-driven close/open is idempotent and an equal `producer_timestamp` is rejected by the staleness guard.

#### Scenario: Concurrent pushes for one node are serialized

- **WHEN** two pushes for the same certname arrive concurrently
- **THEN** the per-Node lock serializes them so they apply one at a time
- **AND** the newer `producer_timestamp` is never dropped by the older
- **AND** no interval is written with `valid_to < valid_from`.

#### Scenario: Identical snapshot re-apply is a no-op

- **WHEN** an agent retries a push carrying the same `producer_timestamp` and identical leaf values that were already applied
- **THEN** the equal `producer_timestamp` is rejected by the staleness guard so no duplicate intervals are opened
- **AND** the stored history and `last_producer_ts` are unchanged.

#### Scenario: Whole-snapshot diff is atomic

- **WHEN** applying a snapshot fails partway (e.g. a unique-violation requiring retry) inside the transaction
- **THEN** the transaction rolls back so no partial close/open diff and no watermark advance is persisted
- **AND** the push is retried or rejected as a whole.

### Requirement: Durable/Volatile classification

The fact-ingest service SHALL classify each leaf path server-side via a config-driven volatile-path policy, treating every path as Durable by default and routing only paths matching the configured Volatile patterns to the latest-only blob. Durable leaves SHALL be routed to `fact_history` (temporal, versioned via close/open intervals); Volatile leaves SHALL be routed to the `node_volatile` blob (overwrite-in-place, latest value only). Volatile leaves SHALL NEVER be written to `fact_history`. The classification policy is central (edit once, no agent redeploy) and reclassification is forward-only.

#### Scenario: Durable leaf historized

- **WHEN** a snapshot contains a Durable leaf such as `os.name` whose value differs from the current open interval
- **THEN** the service closes the prior open interval and opens a new one in `fact_history`
- **AND** does not write `os.name` to `node_volatile`.

#### Scenario: Volatile leaf upserted, never historized

- **WHEN** a snapshot contains a Volatile leaf matching a configured pattern such as `uptime` or `memory.system.available_bytes`
- **THEN** the service upserts the value into the Node's `node_volatile` blob (overwrite-in-place)
- **AND** writes no interval for that leaf in `fact_history`.

#### Scenario: Unlisted leaf is Durable by default

- **WHEN** a snapshot contains a leaf path that matches no configured Volatile pattern
- **THEN** the service classifies it as Durable
- **AND** routes it to `fact_history`.

### Requirement: Ingest resource bounds

The fact-ingest service SHALL enforce hard caps on each push — total snapshot bytes, leaf count, individual path length, and single-value bytes — and reject any push exceeding a cap with a typed error, so that one authenticated Node cannot bloat the shared never-GC'd `fact_paths` dictionary or inject millions of rows. The service SHALL enforce a per-certname rate limit, and SHALL alarm on per-Node `fact_paths`-cardinality spikes (reusing the high-churn alarm machinery).

#### Scenario: Oversized snapshot rejected with a typed error

- **WHEN** a push exceeds a configured cap (snapshot bytes, leaf count, path length, or single-value bytes)
- **THEN** the service rejects the push with a typed error identifying which cap was exceeded
- **AND** applies no diff and does not advance `last_producer_ts`.

#### Scenario: Per-certname rate limit enforced

- **WHEN** a single certname pushes faster than its configured rate limit
- **THEN** the service rejects the excess pushes for that certname
- **AND** does not let one Node monopolize ingest.

#### Scenario: Cardinality spike alarms

- **WHEN** a Node introduces an abnormal spike of new `fact_paths` entries in one or successive pushes
- **THEN** the service raises the per-Node cardinality alarm for operator review
- **AND** does not silently absorb the dictionary growth.

### Requirement: Backpressure

Under saturation of the backing store, the fact-ingest service SHALL fast-fail with HTTP `503` and a `Retry-After` header under bounded concurrency, and SHALL NEVER use unbounded buffering or blocking that could exhaust memory. The data path is loss-tolerant (gaps are visible), so there is no durable spool in v1; the agent is expected to retry with jittered backoff a bounded number of times, then defer to its next timer.

#### Scenario: Saturated ingest fast-fails with 503

- **WHEN** the backing Postgres is saturated and the bounded ingest concurrency limit is reached
- **THEN** the service responds `503` with a `Retry-After` header
- **AND** does not buffer or block the push unboundedly.

#### Scenario: No unbounded buffering under load

- **WHEN** pushes arrive faster than the bounded concurrency can apply them
- **THEN** the service sheds load via `503` rather than queueing without bound
- **AND** memory stays bounded.

### Requirement: Response contract

The push response SHALL report whether the snapshot was applied or rejected, and for a rejection SHALL include the reason (e.g. stale, skewed, oversized, rate-limited, backpressure), so the agent and operators can observe sustained rejects and alarm on them. The response SHALL allow distinguishing a transient backpressure reject from a guard reject.

#### Scenario: Applied push reported

- **WHEN** a snapshot is accepted and applied
- **THEN** the response reports it as applied
- **AND** the agent treats the push as successful.

#### Scenario: Rejected push reports a typed reason

- **WHEN** a snapshot is rejected by a guard or a resource cap
- **THEN** the response reports it as rejected with the specific reason (stale, skewed, oversized, rate-limited, or backpressure)
- **AND** the agent and operators can observe and alarm on sustained per-Node rejects.

#### Scenario: Sustained rejects are observable

- **WHEN** a Node's pushes are rejected for a sustained period (e.g. a stuck clock producing only stale stamps)
- **THEN** each response carries the reject reason so monitoring can raise a sustained-reject alarm for that certname.
