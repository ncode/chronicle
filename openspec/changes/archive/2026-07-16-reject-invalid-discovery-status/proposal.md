## Why

`DiscoveryStatus.Clean` currently treats every value except the exact string `error` as clean. An authenticated Node can therefore send a non-empty malformed Source report and authorize Tombstones for omitted Durable facts, violating ADR-0009's conservative absence rule.

## What Changes

- Define the contractual external `absent` discovery status alongside `ok` and `error`.
- Validate built-in statuses as `ok|error` and external statuses as `ok|error|absent` before deriving the discovery-clean signal.
- Reject any other status as `bad-request` before path interning or `Store.ApplySnapshot`, preserving history and the Node watermark while retaining normal contact-on-reject handling.
- Add focused unit and PostgreSQL-backed regression coverage for the malformed-but-nonempty report path.
- Do not add a Snapshot facade, change cap enforcement, or alter the staleness funnel.

## Capabilities

### New Capabilities

- None.

### Modified Capabilities

- `fact-ingest`: Constrain Source-status values and reject malformed reports before they can authorize Tombstones.

## Impact

- Code: `internal/wire/wire.go` and `internal/ingest/ingest.go`.
- Tests: `internal/wire`, `internal/ingest` unit tests, and ingest PostgreSQL integration coverage.
- Contract: malformed discovery-status enum values become HTTP 400 `bad-request`; valid Node-agent traffic is unchanged.
- Dependencies and schema: no changes.
