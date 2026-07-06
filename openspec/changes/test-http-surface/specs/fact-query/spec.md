## ADDED Requirements

### Requirement: Admin operations are role-gated at the HTTP surface

The read/admin endpoint SHALL expose Deactivation and watermark reset as admin-only HTTP
operations enforced by the role middleware: an unauthenticated request SHALL receive 401, a
reader-role credential SHALL receive 403 with no state change, and an admin-role credential
SHALL execute the operation. A request missing its `certname` parameter SHALL receive 400.

#### Scenario: Reader cannot reset a watermark
- **WHEN** a request bearing a reader-role token POSTs to the watermark-reset operation for
  a Node
- **THEN** the response is 403 and the Node's `last_producer_ts` is unchanged

#### Scenario: Admin resets a watermark
- **WHEN** a request bearing an admin-role token POSTs to the watermark-reset operation for
  a Node whose watermark was poisoned to a far-future value
- **THEN** the response is 200 and a subsequent in-bounds push for that Node is accepted
  again

#### Scenario: Missing certname is rejected
- **WHEN** an admin-role request POSTs to an admin operation without a `certname` parameter
- **THEN** the response is 400 and no state changes

### Requirement: Read errors map to typed HTTP statuses

The read surface SHALL map failures to distinct HTTP statuses so callers can distinguish
them mechanically: a missing or unparsable DSL query SHALL yield 400; a DSL string
exceeding the configured length cap SHALL yield 413; a Volatile-path point-in-time query
(`at <past T>`) SHALL yield 422 carrying the typed no-history error (ADR-0007/0008);
non-RFC3339 temporal parameters (`from`, `to`, `at`) SHALL yield 400; an unknown certname
on a per-node endpoint SHALL yield 404.

#### Scenario: Missing or unparsable DSL is a 400
- **WHEN** a reader GETs the query endpoint with no `q` parameter, or with a `q` the DSL
  parser rejects
- **THEN** the response is 400

#### Scenario: Oversized DSL is a 413
- **WHEN** a reader GETs the query endpoint with a `q` longer than the configured DSL
  length cap
- **THEN** the response is 413

#### Scenario: Volatile path at a past T is a 422
- **WHEN** a reader queries a Volatile fact path with `at <past T>`
- **THEN** the response is 422 and the body carries the typed no-history error

#### Scenario: Malformed temporal parameters are a 400
- **WHEN** a reader supplies a non-RFC3339 `from`/`to` on the diff endpoint or `at` on the
  state endpoint
- **THEN** the response is 400

#### Scenario: Unknown node is a 404
- **WHEN** a reader requests the diff of a certname that does not exist
- **THEN** the response is 404
