## ADDED Requirements

### Requirement: Load-shed rejects carry Retry-After

Rate-limited (429) and backpressure (503) push responses SHALL include a `Retry-After`
header, so an agent backs off on the server's schedule instead of retrying immediately.

#### Scenario: Rate-limited push carries Retry-After
- **WHEN** a Node exceeds its per-certname rate limit and receives 429
- **THEN** the response includes a `Retry-After` header

#### Scenario: Backpressure push carries Retry-After
- **WHEN** ingest sheds load under a saturated concurrency bound and receives 503
- **THEN** the response includes a `Retry-After` header
