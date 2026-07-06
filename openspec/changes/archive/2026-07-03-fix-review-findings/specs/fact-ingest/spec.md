## ADDED Requirements

### Requirement: Degenerate-push rejection

The fact-ingest service SHALL reject, with a typed `bad_request` error, any push whose
decoded fact tree is absent or flattens to zero leaves, and any push that carries no
discovery-status report (no built-in and no external entries). A rejected degenerate push
MUST NOT tombstone any interval, MUST NOT modify the volatile blob, and MUST NOT advance
`nodes.last_producer_ts`. A zero-leaf snapshot or an absent report is evidence of nothing
and SHALL never be interpreted as a discovery-clean pass in which every fact disappeared.

#### Scenario: Missing tree is rejected, not tombstoned

- **WHEN** an authenticated push arrives whose body has no `tree` key (or `tree` is empty/null), such as an operator smoke-test with only a `producer_timestamp`
- **THEN** the service rejects it with a typed `bad_request` error
- **AND** closes no intervals, leaves the volatile blob untouched, and does not advance the watermark.

#### Scenario: Missing discovery report is rejected

- **WHEN** an authenticated push carries a non-empty tree but no discovery-status report at all
- **THEN** the service rejects it with a typed `bad_request` error rather than deriving a vacuously clean signal from the empty report.

### Requirement: Deterministic leaf canonicalization

Flattening a snapshot tree into dotted leaf paths SHALL be deterministic. When two distinct
tree entries flatten to the same leaf path (e.g. a literal key containing a dot colliding
with a nested path), the service SHALL reject the push with a typed `bad_request` error
identifying the colliding path. Identical input trees SHALL always produce identical leaf
sets and values, so repeated pushes of the same snapshot can never fabricate history churn.

#### Scenario: Colliding paths rejected

- **WHEN** a pushed tree contains both a literal key `"a.b"` and a nested `a: {b: …}` so two entries flatten to leaf path `a.b`
- **THEN** the service rejects the push with a typed error naming `a.b`
- **AND** does not let map iteration order decide which value wins.

#### Scenario: Identical pushes are idempotent in content

- **WHEN** the same snapshot tree is flattened twice
- **THEN** both passes produce the same leaf paths and values, so an unchanged fact never flips value between pushes.

### Requirement: Reject and carry-forward observability

Every rejected push SHALL be counted in the ingest metrics with a reason label, including
rejections that occur before the body is decoded (no client certificate, rate-limited,
backpressure, oversized body, malformed JSON) — not only rejections from the apply path.
The service SHALL also surface, per Node, a consecutive-dirty-pass signal (metric and log)
once a configurable streak threshold is crossed, so the whole-pass carry-forward gate
(ADR-0009 §1) cannot silently suppress tombstones for a Node indefinitely.

#### Scenario: Early-path rejects are counted

- **WHEN** a push is rejected before apply — no client cert, rate limit, backpressure `503`, oversized body `413`, or malformed JSON
- **THEN** the rejects metric increments with the corresponding reason label.

#### Scenario: Persistent dirty streak becomes visible

- **WHEN** a Node's pushes are marked discovery-dirty for more than the configured consecutive-pass threshold
- **THEN** the service emits the per-node dirty-streak metric and a log line naming the failing sources
- **AND** an operator can discover that tombstones for that Node have been frozen by the carry-forward gate.

## MODIFIED Requirements

### Requirement: Two-sided clock guard

The fact-ingest service SHALL bound `producer_timestamp` on both sides relative to the Node watermark and the server clock. It SHALL reject any snapshot whose `producer_timestamp <= nodes.last_producer_ts` (stale / out-of-order) and SHALL reject any snapshot whose `producer_timestamp > received_at + max_skew`, where `max_skew` defaults to 5 minutes and is configurable. On first contact (a Node with no watermark) the service SHALL also bound the past: it SHALL reject any snapshot whose `producer_timestamp < received_at - max_skew`, so a Node with a reset or badly skewed clock cannot fabricate arbitrarily old history at fleet-wide at-T queries. A rejected push MUST NOT advance `nodes.last_producer_ts` and MUST NOT apply any snapshot diff. The service SHALL provide an operator action to reset `last_producer_ts` to recover a Node whose watermark was poisoned (e.g. by a far-future stamp accepted before this guard existed).

#### Scenario: Stale snapshot rejected

- **WHEN** a push arrives with `producer_timestamp` less than or equal to the Node's current `last_producer_ts`
- **THEN** the service rejects the push as stale
- **AND** does not advance `last_producer_ts`
- **AND** applies no diff.

#### Scenario: Far-future snapshot rejected

- **WHEN** a push arrives with `producer_timestamp` greater than `received_at + max_skew` (default 5 minutes)
- **THEN** the service rejects the push as skewed
- **AND** does not advance `last_producer_ts`, so the Node is not wedged out of ingest into the future.

#### Scenario: Far-past first contact rejected

- **WHEN** a Node with no watermark (first contact, or after certname turnover) pushes a snapshot whose `producer_timestamp` is earlier than `received_at - max_skew`
- **THEN** the service rejects the push as skewed
- **AND** no backdated history is written for that Node.

#### Scenario: In-bounds snapshot accepted and watermark advances

- **WHEN** a push arrives with `producer_timestamp` strictly greater than `last_producer_ts` and no greater than `received_at + max_skew`
- **THEN** the service applies the snapshot
- **AND** advances `last_producer_ts` to the snapshot's `producer_timestamp`.

#### Scenario: Operator resets a poisoned watermark

- **WHEN** a Node's `last_producer_ts` was poisoned to a far-future value and an operator runs the reset action for that certname
- **THEN** the service resets `last_producer_ts` so subsequent in-bounds pushes are accepted again.

### Requirement: Ingest resource bounds

The fact-ingest service SHALL enforce hard caps on each push — total snapshot bytes, leaf count, individual path length, and single-value bytes — and reject any push exceeding a cap with a typed error, so that one authenticated Node cannot bloat the shared never-GC'd `fact_paths` dictionary or inject millions of rows. Cap enforcement SHALL be incremental: the leaf-count and path-length caps are checked during flattening and the push abandoned at the first violation, so rejection never requires materializing the full leaf set; decode-time work remains bounded by `max_snapshot_bytes` (which caps the request body before decoding). The service SHALL enforce a per-certname rate limit, and SHALL alarm on per-Node `fact_paths`-cardinality spikes (reusing the high-churn alarm machinery).

#### Scenario: Oversized snapshot rejected with a typed error

- **WHEN** a push exceeds a configured cap (snapshot bytes, leaf count, path length, or single-value bytes)
- **THEN** the service rejects the push with a typed error identifying which cap was exceeded
- **AND** applies no diff and does not advance `last_producer_ts`.

#### Scenario: Over-cap push abandoned during flatten

- **WHEN** a maximal in-byte-budget body flattens to more leaves than `max_leaf_count`
- **THEN** the service stops flattening at the cap and rejects the push
- **AND** does not first materialize the full leaf set only to reject it afterwards.

#### Scenario: Per-certname rate limit enforced

- **WHEN** a single certname pushes faster than its configured rate limit
- **THEN** the service rejects the excess pushes for that certname
- **AND** does not let one Node monopolize ingest.

#### Scenario: Cardinality spike alarms

- **WHEN** a Node introduces an abnormal spike of new `fact_paths` entries in one or successive pushes
- **THEN** the service raises the per-Node cardinality alarm for operator review
- **AND** does not silently absorb the dictionary growth.
