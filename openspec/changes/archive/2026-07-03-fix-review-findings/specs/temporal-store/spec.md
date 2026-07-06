## MODIFIED Requirements

### Requirement: Open-interval integrity boundary

The store SHALL guarantee at most one current value per `(node_id, path_id)` via the partial unique index `fact_history_open_uniq` on `(node_id, path_id) WHERE valid_to = 'infinity'`. The store SHALL forbid zero-length and inverted intervals via `CHECK (valid_from < valid_to)`. The apply-time stale guard SHALL consider the Node's entire history, not only its open intervals: a snapshot SHALL be rejected as stale unless its `producer_timestamp` is strictly after the Node's maximum `valid_from` among open intervals AND no earlier than the Node's maximum `valid_to` among closed intervals (starting a new interval exactly at a closed interval's `valid_to` is permitted — intervals are half-open), so that a past-timestamped push arriving after an operator watermark reset can never insert an interval overlapping an already-closed one.

#### Scenario: A second concurrent open interval is rejected
- **WHEN** a write attempts to insert a second row with `valid_to = 'infinity'` for a `(node_id, path_id)` that already has an open interval
- **THEN** the partial unique index `fact_history_open_uniq` rejects the write, so at most one current value per `(node_id, path_id)` can exist

#### Scenario: Zero-length or inverted interval is rejected
- **WHEN** a write attempts to persist an interval with `valid_from >= valid_to` (including `valid_from = valid_to`)
- **THEN** the `CHECK (valid_from < valid_to)` constraint rejects the write

#### Scenario: Post-reset past push cannot overlap closed history
- **WHEN** all of a Node's intervals are closed (e.g. after a full tombstone pass), an operator resets the Node's watermark, and a push then arrives with a `producer_timestamp` that falls inside an already-closed interval — e.g. at 15:00 inside `[10:00, 20:00)`
- **THEN** the apply is rejected as stale because the timestamp is earlier than the Node's maximum closed `valid_to`
- **AND** state-at-T queries can never observe two conflicting values for the same `(node_id, path_id)` at one instant
