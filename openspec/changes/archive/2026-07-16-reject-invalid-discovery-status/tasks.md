## 1. Pin the malformed-report regression

- [x] 1.1 Add a `DiscoveryStatus` table test covering valid built-in `ok|error`, valid external `ok|error|absent`, invalid values, and mixed reports; assert `ok`/`absent` cleanliness and `error`/invalid dirtiness explicitly.
- [x] 1.2 Extend ingest plan tests with invalid built-in and external statuses; assert a nil plan, HTTP 400, and `bad-request`.
- [x] 1.3 Add a PostgreSQL-backed regression that starts with multiple open Durable facts, submits a newer malformed-but-nonempty report omitting one Fact, and proves no Tombstone or watermark advance occurs while `last_seen` records the rejected contact.

## 2. Enforce the wire contract

- [x] 2.1 Add the external `absent` status and centralized `DiscoveryStatus` validation in `internal/wire`; make cleanliness fail closed for invalid values.
- [x] 2.2 Call discovery-status validation in `ingest.plan` before tree work and map failures to the existing `bad-request` response.

## 3. Verify the focused change

- [x] 3.1 Run `go test ./internal/wire ./internal/agent ./internal/ingest` and confirm all focused tests pass.
- [x] 3.2 Run `make test-db` and confirm the malformed report cannot mutate temporal history.
- [x] 3.3 Run `make test` and confirm no valid Node-agent or ingest behavior regresses.
