## ADDED Requirements

### Requirement: Per-node state endpoint

The read endpoint SHALL expose a Node's full fact state — current, and as of an arbitrary
past time `T` — for a given certname, answering CONTEXT.md's core question "what did node X
look like at time T". The endpoint SHALL apply the same authentication, authorization, and
inactive-node defaults as the rest of the read surface (expired/deactivated Nodes excluded
unless `include_inactive`). Volatile semantics are explicit, not error-based (no specific
volatile path is referenced, so the DSL's `at <past>` error does not apply): current state
SHALL include the latest volatile blob alongside open durable facts; state at a past `T`
SHALL return durable facts only, and the response SHALL explicitly mark volatile facts as
unavailable in the past rather than silently omitting them.

#### Scenario: Node state now

- **WHEN** an authenticated reader requests the current state of a certname
- **THEN** the endpoint returns every currently-open durable fact and the latest volatile blob for that Node

#### Scenario: Node state at a past T

- **WHEN** an authenticated reader requests the state of a certname at a past time `T`
- **THEN** the endpoint returns exactly the durable facts whose intervals cover `T` (half-open `[valid_from, valid_to)`)
- **AND** the response explicitly marks volatile facts as unavailable at past `T` instead of fabricating or silently omitting them

### Requirement: Admin action attribution and audit

Terminal or destructive admin actions (Deactivation, watermark reset) SHALL be attributable:
the service SHALL resolve and retain the authenticated principal past authentication — the
OIDC subject claim, or the configured name of the static token — and SHALL write an audit
log entry for every successful admin action recording principal, action, target certname,
and time. Static tokens SHALL therefore be configured with operator-assigned names; the
token secret itself MUST NOT be logged.

#### Scenario: Deactivation is audited

- **WHEN** an admin deactivates a certname
- **THEN** the service logs an audit entry with the acting principal, the action, the target certname, and the timestamp
- **AND** the operator of record can later be identified for this terminal action

#### Scenario: Principal survives authentication

- **WHEN** a request authenticates via OIDC or a static token
- **THEN** the resolved principal identity is available to handlers rather than being discarded at the auth boundary

### Requirement: Bearer-credential brute-force resistance

Failed bearer/static-token authentication attempts SHALL be throttled (per source and/or
globally), so the read/admin listener does not permit unbounded online brute force of
static API tokens.

#### Scenario: Repeated auth failures are throttled

- **WHEN** a client presents a stream of invalid bearer tokens
- **THEN** the service throttles further attempts from that source before an online search of the token space becomes feasible
- **AND** legitimate authenticated traffic is not materially affected

## MODIFIED Requirements

### Requirement: People/automation authentication on the read endpoint

The read/admin endpoint SHALL authenticate people and automation via OIDC/bearer tokens
(chronicle acting as a relying party validating a JWT against the company IdP's JWKS) or via
static API tokens, over server-TLS without client certificates. It SHALL NOT accept node
identity certificates; a node identity cert can never read. Authenticator configuration —
the static token set, role mappings, and OIDC settings — SHALL take effect on configuration
reload (SIGHUP) without a process restart, exactly as the volatile policy and CRL already
do, so revoking a leaked static token does not depend on a restart. The static token set and
role mappings SHALL swap unconditionally on reload (they are pure configuration and cannot
fail to build); a failure to rebuild the OIDC verifier (e.g. IdP discovery down) SHALL keep
only the old verifier and MUST NOT prevent the static-token swap, so token revocation is
never blocked by an IdP outage.

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

#### Scenario: Removed token stops working on reload
- **WHEN** an operator removes a static token (or changes role mappings) in the config and sends SIGHUP
- **THEN** the next request presenting the removed token is rejected without a process restart

#### Scenario: Token revocation survives an IdP outage
- **WHEN** an operator removes a leaked static token and sends SIGHUP while the OIDC IdP is unreachable
- **THEN** the static-token swap still takes effect and the leaked token stops authenticating
- **AND** the OIDC verifier keeps its previous instance (fail-closed) with the rebuild failure logged
