# fact-query Specification

## Purpose
TBD - created by archiving change chronicle-v1. Update Purpose after archive.
## Requirements
### Requirement: Reads go through temporal-encapsulating views

All read access SHALL be served by SQL views/functions that encapsulate the
`valid_to='infinity'` open-interval discipline, so callers cannot construct a temporally
incorrect query. The raw `fact_history` table SHALL NOT be the read surface; the DSL, the
`node_diff` endpoint, and the power-user read-only SQL role SHALL all target these
views/functions rather than reconstructing the open-interval predicate by hand.

#### Scenario: Current-state view returns only open rows
- **WHEN** a reader queries the current-state view for a fact path without supplying any
  temporal predicate
- **THEN** only rows whose interval is open (`valid_to = 'infinity'`) are returned, and the
  caller never references `valid_to` to obtain this result

#### Scenario: Power-user SQL role is scoped to the views
- **WHEN** a DBA-issued read-only SQL credential is used for an ad-hoc query
- **THEN** it reaches the data through the same temporal views/functions, so the
  `valid_to='infinity'` discipline is applied identically and cannot be gotten wrong

### Requirement: Compound equality filter resolves to matching nodes

The system SHALL support a compound equality filter of the form `path=value path=value ...`
(space-separated terms, implicitly AND-ed) that returns the set of nodes matching every
term. The filter SHALL be compiled to an `INTERSECT` of indexed current-row lookups, one
indexed lookup per term.

#### Scenario: Two-term AND intersects indexed lookups
- **WHEN** a reader submits `role=web os.name=Debian`
- **THEN** the result is the set of nodes (certnames) that currently have both `role=web`
  and `os.name=Debian`, computed as the `INTERSECT` of the per-term indexed current-row
  lookups

#### Scenario: Single-term filter returns all matching nodes
- **WHEN** a reader submits `role=web`
- **THEN** every node whose current `role` fact equals `web` is returned and no node lacking
  that fact value is included

### Requirement: Project + filter + group-by aggregation

The system SHALL support a `<field> where <predicate> group by <field>` shape that projects
a field, filters current rows by the predicate, and returns an aggregation grouped by the
group-by field. It SHALL be compiled to a filtered `GROUP BY` over current rows.

#### Scenario: Filtered group-by produces counts per group
- **WHEN** a reader submits `role where os.name='Debian' group by role`
- **THEN** the result is an aggregation over the nodes whose current `os.name` equals
  `Debian`, grouped by `role`, returning one row per distinct `role` value

### Requirement: Uniform `at <T>` point-in-time qualifier

Either DSL shape SHALL accept an optional `at <T>` qualifier that evaluates the query over
`validity @> T` instead of current rows; when omitted, the qualifier defaults to now.
Cross-node historical `at <past T>` SHALL be supported (un-indexed in v1, optimized later
by un-deferring the composite GiST temporal index).

#### Scenario: Omitted qualifier defaults to now
- **WHEN** a reader submits `role=web os.name=Debian` with no `at` clause
- **THEN** the query is evaluated against current rows (now), equivalent to `at now`

#### Scenario: Cross-node historical point-in-time
- **WHEN** a reader submits `os.name=Debian at 2026-03-14T06:00:00Z`
- **THEN** the result is the set of nodes whose interval for that fact satisfies
  `validity @> '2026-03-14T06:00:00Z'`, computed across nodes even though the query is
  un-indexed in v1

### Requirement: `at <past>` against a volatile path errors

A query using `at <past T>` against a VOLATILE fact path SHALL return a typed "no history"
error, because volatile facts are latest-only and carry no history. Volatile-path lookups
SHALL route to `node_volatile` rather than the temporal history.

#### Scenario: Historical query on a volatile path is rejected
- **WHEN** a reader submits a query for a volatile path (e.g. `uptime`) with `at <past T>`
- **THEN** the system returns a typed "no history" error rather than a result set or an
  empty set

#### Scenario: Current volatile lookup routes to node_volatile
- **WHEN** a reader looks up a volatile path at now (no past `at`)
- **THEN** the value is served from `node_volatile` (the latest-only store), not from
  `fact_history`

### Requirement: node_diff per-node change endpoint

The system SHALL provide a `node_diff(certname, T1, T2)` function/endpoint that returns a
node's fact changes within the `[T1, T2]` window. The result SHALL include deletions —
intervals that close in the window with no matching open interval (tombstones / sealed
intervals).

#### Scenario: Window changes include a value change
- **WHEN** `node_diff(certname, T1, T2)` is called for a node whose `os.name` changed from
  `Debian` to `Ubuntu` inside the window
- **THEN** the result reports that change for the `os.name` path over `[T1, T2]`

#### Scenario: Deletions are surfaced
- **WHEN** a fact's interval closes inside `[T1, T2]` with no matching open interval
  afterward (a genuine removal / tombstone)
- **THEN** `node_diff` includes that deletion in its result

### Requirement: People/automation authentication on the read endpoint

The read/admin endpoint SHALL authenticate people and automation via OIDC/bearer tokens
(chronicle acting as a relying party validating a JWT against the company IdP's JWKS) or via
static API tokens, over server-TLS without client certificates. It SHALL NOT accept node
identity certificates; a node identity cert can never read.

#### Scenario: Valid bearer token is accepted
- **WHEN** a request to the read endpoint presents a valid OIDC JWT (validated against the
  IdP JWKS) or a valid static API token
- **THEN** the request is authenticated and allowed to proceed to authorization

#### Scenario: Node identity cert is refused for reads
- **WHEN** a request to the read endpoint presents a facts-ca node identity client
  certificate as its credential
- **THEN** the request is rejected and granted no read access

#### Scenario: Static token works without an IdP
- **WHEN** a deployment has no OIDC IdP wired and a request presents a valid static API token
- **THEN** the request authenticates successfully

### Requirement: Chronicle-owned authorization roles

The system SHALL map token claims (e.g. `groups`/`roles`) to chronicle's own reader/admin
roles. It SHALL NOT inherit facts-ca's "any CA-signed cert = admin" semantics; the read/admin
endpoint accepts no certs, so CA admin semantics cannot leak into chronicle authorization.

#### Scenario: Claim maps to reader role
- **WHEN** an authenticated token carries a claim that chronicle's mapping assigns to the
  reader role
- **THEN** the caller is granted reader access and denied admin-only operations

#### Scenario: CA admin semantics never apply to reads
- **WHEN** a credential would be considered admin under facts-ca's "any cert = admin" rule
- **THEN** chronicle ignores that semantic entirely (no cert is accepted on this endpoint)
  and the caller's role is determined solely by chronicle's own claim mapping

### Requirement: Inactive nodes excluded by default

"Now" and state-at-T queries SHALL exclude deactivated and expired nodes by default via a
view-side node-state filter. An `include_inactive` opt-in SHALL re-include them for
historical/forensic queries.

#### Scenario: Expired node hidden from now
- **WHEN** a reader runs a current-state query and a node is expired or deactivated
- **THEN** that node is absent from the default result

#### Scenario: include_inactive re-includes for forensics
- **WHEN** a reader runs the same query with `include_inactive`
- **THEN** expired and deactivated nodes are included in the result for historical/forensic
  inspection

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

