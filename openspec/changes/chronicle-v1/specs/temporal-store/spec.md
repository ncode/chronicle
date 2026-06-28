## ADDED Requirements

### Requirement: Change-only intervals

The store SHALL record Durable fact history in `fact_history` as one row per `(node_id, path_id, value)` over the half-open interval `[valid_from, valid_to)`, where `valid_to = 'infinity'` marks the current value. Applying a Durable leaf at observation time `T` (= `valid_from`) MUST be change-only: an unchanged fact (same `value_hash` as the open interval) writes nothing; a changed durable fact closes the open interval at `T` and opens a new interval at `T`, both within one transaction. A disappeared leaf (Tombstone) closes the open interval and opens no replacement.

#### Scenario: Unchanged durable fact writes nothing
- **WHEN** a leaf is applied whose `value_hash` equals the `value_hash` of the node/path's currently open interval (`valid_to = 'infinity'`)
- **THEN** no row is inserted and no open interval is closed, leaving `fact_history` unchanged for that `(node_id, path_id)`

#### Scenario: Changed durable fact closes and opens in one transaction
- **WHEN** a leaf is applied at time `T` whose `value_hash` differs from the node/path's open interval
- **THEN** in a single transaction the open interval's `valid_to` is set to `T` and a new interval is inserted with the new value, `value_hash`, `valid_from = T`, and `valid_to = 'infinity'`

#### Scenario: First-ever observation opens an interval
- **WHEN** a leaf is applied at time `T` for a `(node_id, path_id)` that has no row in `fact_history`
- **THEN** exactly one interval is inserted with `valid_from = T` and `valid_to = 'infinity'`

#### Scenario: Disappeared leaf is tombstoned by absence
- **WHEN** a previously-open leaf is genuinely removed (its succeeded Source no longer produces it)
- **THEN** the open interval is closed by setting `valid_to = T` and no replacement interval is opened

### Requirement: Open-interval integrity boundary

The store SHALL guarantee at most one current value per `(node_id, path_id)` via the partial unique index `fact_history_open_uniq` on `(node_id, path_id) WHERE valid_to = 'infinity'`. The store SHALL forbid zero-length and inverted intervals via `CHECK (valid_from < valid_to)`.

#### Scenario: A second concurrent open interval is rejected
- **WHEN** a write attempts to insert a second row with `valid_to = 'infinity'` for a `(node_id, path_id)` that already has an open interval
- **THEN** the partial unique index `fact_history_open_uniq` rejects the write, so at most one current value per `(node_id, path_id)` can exist

#### Scenario: Zero-length or inverted interval is rejected
- **WHEN** a write attempts to persist an interval with `valid_from >= valid_to` (including `valid_from = valid_to`)
- **THEN** the `CHECK (valid_from < valid_to)` constraint rejects the write

### Requirement: "Now" query

The store SHALL answer current-state queries by selecting open intervals (`valid_to = 'infinity'`). Cross-node "now" lookups of which nodes currently hold a given path/value MUST be served from the open-interval index `fact_history_now_idx` on `(path_id, value_hash, node_id) WHERE valid_to = 'infinity'`.

#### Scenario: Current value of a node's fact
- **WHEN** the current value of `(node_id, path_id)` is requested
- **THEN** the store returns the value of the single interval whose `valid_to = 'infinity'`, or no value if none is open (tombstoned/never observed)

#### Scenario: Cross-node "now" by path and value
- **WHEN** all nodes currently holding a given `(path_id, value_hash)` are requested
- **THEN** the store returns exactly the nodes whose interval for that `(path_id, value_hash)` has `valid_to = 'infinity'`

### Requirement: State-at-T query

The store SHALL reconstruct a point-in-time value for `(node_id, path_id)` at time `T` by selecting the interval whose validity contains `T` (`valid_from <= T < valid_to`, i.e. `tstzrange(valid_from, valid_to) @> T`). Per-node point-in-time reconstruction MUST be indexed in v1 (cross-node historical point-in-time is deferred).

#### Scenario: Value reconstructed at a past time
- **WHEN** the value of `(node_id, path_id)` at time `T` is requested
- **THEN** the store returns the value of the unique interval where `valid_from <= T < valid_to`

#### Scenario: No value existed at T
- **WHEN** time `T` falls before the first interval's `valid_from` or inside a tombstoned gap with no covering interval
- **THEN** the store returns no value for that `(node_id, path_id)` at `T`

#### Scenario: Per-node point-in-time is index-served
- **WHEN** a single node's facts are reconstructed at time `T`
- **THEN** the query is served by the per-node indexes (`fact_history_node_opened_idx` / `fact_history_node_closed_idx`) plus a range filter, without a full table scan

### Requirement: Diff query

The store SHALL answer `Diff(T1, T2)` for a node as the set of intervals that opened (added or changed) OR closed (removed or changed-away) within the window — an interval qualifies if `valid_from` is in `[T1, T2)` OR `valid_to` is in `[T1, T2)`. A pure deletion is a close with no matching open in the window, and such tombstones MUST be included in the diff result.

#### Scenario: Added and changed facts appear as opens
- **WHEN** `Diff(T1, T2)` is requested for a node and a fact was added or changed in the window
- **THEN** the result includes the interval whose `valid_from` falls within `[T1, T2)`

#### Scenario: Pure deletion appears as a close with no matching open
- **WHEN** a fact was tombstoned in the window (open interval closed, no new interval opened)
- **THEN** the result includes that interval by its `valid_to` falling within `[T1, T2)`, even though no open interval matches it in the window

#### Scenario: Unchanged facts are excluded
- **WHEN** `Diff(T1, T2)` is requested and a fact's interval neither opened nor closed within `[T1, T2)`
- **THEN** that fact does not appear in the diff result

### Requirement: Interned paths and inline values

Leaf paths SHALL be interned in `fact_paths` (`path_text` UNIQUE, with `fact_name` as the first segment), and `fact_history` SHALL reference paths by `path_id`. Values SHALL be stored inline as `jsonb` together with a content-addressed `value_hash` (sha256 over a type-tagged canonical form), such that values of differing JSON types are never equal (`1 != "1" != 1.0`) and the hash serves as the equality key. v1 SHALL NOT intern fact values (no `fact_values` table).

#### Scenario: A leaf path is interned once
- **WHEN** a leaf path `path_text` is ingested that already exists in `fact_paths`
- **THEN** the existing `path_id` is reused and no duplicate `fact_paths` row is created (enforced by the `path_text` UNIQUE constraint)

#### Scenario: Type-tagged hash distinguishes look-alike values
- **WHEN** the values `1` (number), `"1"` (string), and `1.0` are each hashed for change detection
- **THEN** each yields a distinct `value_hash`, so a change between any two of them is recorded as a new interval

#### Scenario: Equality is decided by value_hash
- **WHEN** an incoming leaf's `value_hash` equals the open interval's `value_hash`
- **THEN** the value is treated as unchanged and no new interval is written, independent of the inline `jsonb` byte representation

#### Scenario: No value interning table in v1
- **WHEN** the v1 schema is provisioned
- **THEN** values live inline in `fact_history.value` with `fact_history.value_hash` and there is no separate interned `fact_values` table

### Requirement: Volatile store

Volatile facts SHALL be stored latest-only in a single per-node `node_volatile` row holding one `jsonb` blob, overwritten in place on each apply and never historized. There SHALL be exactly one `node_volatile` row per node (primary key `node_id`), and no validity intervals are kept for volatile facts.

#### Scenario: Volatile facts overwrite in place
- **WHEN** a node's volatile facts are applied
- **THEN** the node's `node_volatile.volatile` blob and `observed_at` are replaced via upsert on `node_id`, with no prior volatile value retained

#### Scenario: Volatile facts are never historized
- **WHEN** a volatile fact changes across collection passes
- **THEN** no interval is written to `fact_history` for it and only the single latest `node_volatile` blob exists for that node

#### Scenario: One volatile row per node
- **WHEN** volatile facts are applied for a node that already has a `node_volatile` row
- **THEN** the existing row is updated (no second row is created), enforced by the `node_id` primary key
