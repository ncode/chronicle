## Context

The wire contract stores Source statuses as unrestricted strings. `DiscoveryStatus.Clean` currently returns false only for the exact value `error`, so a non-empty malformed report can be treated as clean, reach `store.ApplySnapshot`, and Tombstone every omitted Durable fact. The existing empty-report and empty-tree guards do not cover this path.

The architecture review's broader Snapshot-exchange proposal did not survive adversarial review. Body, leaf, path, and value caps are aligned, and the shareable flattening mechanics already live in `internal/wire`. This design keeps that ownership and fixes only the history-mutating trust-boundary gap.

## Goals / Non-Goals

**Goals:**

- Make the Source-status vocabulary explicit and validate it before path interning or `Store.ApplySnapshot`.
- Reject malformed reports before path interning, any Snapshot diff, Tombstone, or watermark advance while preserving the existing rejected-push contact update.
- Preserve external `absent` as a clean status even though the stateless Node adapter currently represents a removed Source by omission.
- Leave a defense-in-depth cleanliness check that cannot fail open if another caller skips validation.

**Non-Goals:**

- No new Snapshot package, facade, interface, or adapter.
- No cap, JSON-field, classification, Source-provenance, store, or staleness-funnel redesign.
- No change to valid Node-agent payloads or to the omission-based representation of a removed external Source.

## Decisions

### Validate in `internal/wire`, enforce in `ingest.plan`

`internal/wire` already owns the payload types and status constants. Add the external `absent` constant and a small `DiscoveryStatus` validity operation there. `ingest.plan` calls it after the existing non-empty-report guard and before tree decoding/flattening, path interning, or `Store.ApplySnapshot`.

`Service.Apply` keeps its current reject path: an existing Node's `last_seen` records the authenticated contact, while `last_producer_ts` and Fact history remain unchanged.

Alternative: validate only inside ingest. Rejected because that duplicates wire vocabulary and leaves other callers able to misclassify malformed values.

### Reject invalid enum values as `bad-request`

Built-in values are limited to `ok|error`; external values are limited to `ok|error|absent`. Any other value rejects the push with HTTP 400, applies no Snapshot diff, creates no Tombstone, and leaves `last_producer_ts` unchanged.

Alternative: treat unknown values as dirty and continue. Rejected because it silently accepts a malformed protocol message and can freeze Tombstones indefinitely. Unknown top-level JSON fields remain tolerated; enum semantics are history-significant and require coordinated rollout.

### Keep `Clean` fail-closed

Even though ingest validates first, `Clean` must return false for any value outside the allowed set. This is defense-in-depth for future callers; it does not replace trust-boundary rejection.

### Do not refactor cap enforcement

The Node adapter's cap checks are advisory preflight and ingest is authoritative. Serialized body bytes use the same payload, leaf/path caps share `wire.Flatten`, and value bytes marshal the same flattened value. No cap code or documentation changes in this focused fix.

## Risks / Trade-offs

- [A newer Node adapter introduces a status before the server understands it] → coordinate the enum change in `node-agent` and `fact-ingest`; the older server returns a visible 400 instead of risking history corruption.
- [Previously malformed clients begin failing] → intentional protocol hardening; the response is typed `bad-request` and existing reject metrics expose it.
- [Validation and `Clean` drift] → table-test the full built-in/external value matrix in `internal/wire`.

## Migration Plan

Deploy the server change normally; valid payloads need no migration. Rollback is the prior binary because there is no schema or stored-data change.

## Open Questions

None.
