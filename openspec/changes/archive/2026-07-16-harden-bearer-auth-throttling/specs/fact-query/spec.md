## ADDED Requirements

### Requirement: Bearer-credential brute-force resistance

Failed bearer-token authentication attempts SHALL be throttled per source before credential evaluation, with bounded source-tracking state and a bounded shared fallback after that state reaches its fixed cap. Admission for concurrent attempts from one source MUST atomically reserve against the currently available failure budget. Because credential validity is unknown at admission, every credential check MUST reserve capacity and an excess request MAY receive HTTP 429 even if its credential would have been valid. Only failed authentication SHALL consume the budget; successful authentication and authorization failures for a valid credential MUST release their reservation immediately after authentication without consuming it.

#### Scenario: Repeated authentication failures are throttled

- **WHEN** a source presents a sequential stream of invalid bearer tokens that exhausts its failure budget
- **THEN** further attempts from that source receive HTTP 429 before credential evaluation until budget refills
- **AND** the invalid attempts admitted before exhaustion receive HTTP 401.

#### Scenario: Concurrent first wave is bounded

- **WHEN** more concurrent invalid bearer-token requests arrive from one source than its currently available failure budget
- **THEN** no more than the available budget are admitted to credential evaluation
- **AND** every excess request receives HTTP 429 without evaluating its credential.

#### Scenario: Successful authentication preserves failure budget

- **WHEN** an admitted request presents a valid bearer credential
- **THEN** authentication proceeds normally and its reserved capacity is released without consuming failure budget
- **AND** at the same logical time, later invalid attempts have no less failure budget because of the successful request.

#### Scenario: Valid credential with insufficient role preserves failure budget

- **WHEN** an admitted request authenticates successfully but lacks the role required by the route
- **THEN** the request receives HTTP 403
- **AND** its reserved capacity is released without consuming failure budget.

#### Scenario: Concurrent valid attempts share admission capacity

- **WHEN** more concurrent valid bearer-token requests arrive from one source than its currently available failure budget
- **THEN** admitted requests authenticate in parallel and release their reservations immediately after authentication
- **AND** excess requests MAY receive HTTP 429 because validity is not known before admission.

#### Scenario: Source tracking remains bounded

- **WHEN** invalid attempts arrive from more distinct sources than the source-map cap
- **THEN** new sources share the bounded fallback budget
- **AND** the server does not allocate unbounded per-source limiter state
- **AND** in-flight reservations, successful release, and failure debit are shared across those fallback sources.
