# node-agent Specification

## Purpose
TBD - created by archiving change chronicle-v1. Update Purpose after archive.
## Requirements
### Requirement: Timer-Driven Library-Mode Collection

The agent SHALL run `facts.Discover()` in LIBRARY mode (never the `facts` CLI) on a configurable collection interval, because library mode surfaces per-source failures via `errors.Join` while the CLI swallows them. Each node SHALL apply per-node jitter to its interval so that a fleet of 50→100,000 nodes does not push in a synchronized thundering herd.

#### Scenario: Periodic collection on the configured interval

- **WHEN** the agent has been started with a configured collection interval
- **THEN** it invokes `facts.Discover()` in library mode once per interval
- **AND** it never shells out to the `facts` CLI to collect a snapshot

#### Scenario: Per-node jitter de-synchronizes the fleet

- **WHEN** two or more nodes start with the same configured interval
- **THEN** each node offsets its collection (and retry) timing by an independently computed per-node jitter
- **AND** the actual push instants are spread across the interval rather than aligned

#### Scenario: Library mode preserves per-source failures

- **WHEN** a resolver or external source fails during `facts.Discover()`
- **THEN** the agent obtains that source's failure from the joined error rather than a silently dropped CLI result
- **AND** the rest of the snapshot that resolved successfully is still collected

### Requirement: Producer Timestamp Stamping

Because the `facts` library attaches no time to a snapshot, the agent SHALL stamp each snapshot with a `producer_timestamp` captured at collection time, and SHALL transmit it as the single producer-asserted time for that push.

#### Scenario: Every snapshot is stamped at collection time

- **WHEN** the agent finishes a `facts.Discover()` pass
- **THEN** it records a `producer_timestamp` taken at the moment of that collection
- **AND** the value travels with the snapshot in the push so chronicle can apply its two-sided skew guard

#### Scenario: Each pass gets its own timestamp

- **WHEN** the agent performs two successive collection passes
- **THEN** the second snapshot carries a distinct `producer_timestamp` reflecting its own collection time
- **AND** a retry of an unchanged snapshot reuses the original pass's `producer_timestamp` rather than minting a new one

### Requirement: Per-Source Discovery-Status Report

The agent SHALL attach a discovery-status report to every push so chronicle can distinguish a genuinely removed source from one that merely failed this pass. For built-in resolvers the report SHALL be `{namespace → ok|error}`; for external facts it SHALL be `{source-file → ok|error|absent}`, built by joining the per-source error from `errors.Join` with a directory enumeration of the configured external dirs, so that "script present-but-failed" (error) is unambiguously distinct from "script gone" (absent). From this report chronicle derives a single discovery-clean signal (no built-in `error` and no external `error`) that gates absence handling.

> Note (v1, see ADR-0009 §1): the `facts` public API does not expose per-fact source provenance — `ResolvedFact` is an internal type and its `.File` is empty for live external facts — so the report is per-source, not per-leaf, and the server applies it as a clean/dirty gate rather than carrying forward exactly the leaves of one failed source. Per-leaf scoping is deferred-with-trigger.

#### Scenario: Built-in resolver status is per-namespace

- **WHEN** a built-in resolver succeeds and another raises an error during discovery
- **THEN** the report marks the succeeding namespace `ok` and the failing namespace `error`
- **AND** both entries are present in the report attached to the push

#### Scenario: Present-but-failed external script reports error

- **WHEN** an external script is still present in a configured external dir but fails (e.g. times out)
- **THEN** the directory enumeration finds the file AND the joined error attributes a failure to it
- **AND** the report marks that source-file `error` (carry-forward), not `absent`

#### Scenario: Removed external script is omitted, not errored

- **WHEN** an external script that previously existed is gone from every configured external dir and no error is attributed to it
- **THEN** the directory enumeration does not find the file, so the report carries no entry for it and attributes no error (a stateless agent cannot mark `absent` without remembering prior contents)
- **AND** with no error in the pass the server treats discovery as clean and tombstones that source's leaves

#### Scenario: Report yields a clean/dirty discovery signal

- **WHEN** the discovery-status report contains at least one built-in `error` or external `error` entry
- **THEN** chronicle treats the whole pass as not-clean and carries absent durable leaves forward
- **AND** when the report has no `error` entry (only `ok`/`absent`) chronicle treats the pass as clean and tombstones absent durable leaves

### Requirement: Authenticated mTLS Push

The agent SHALL POST the snapshot to the single chronicle ingest endpoint over mutual TLS, presenting its `facts-ca`-issued client certificate. Identity SHALL be the certificate alone; the agent SHALL NOT place any Certname, credential, or other identity claim in the request body, because chronicle reads the Certname from the verified mTLS chain.

#### Scenario: Push presents the client certificate over mTLS

- **WHEN** the agent pushes a snapshot
- **THEN** it establishes a mutual-TLS connection presenting its facts-ca-issued client certificate
- **AND** it sends the snapshot to the single chronicle ingest endpoint

#### Scenario: No identity claim in the body

- **WHEN** the agent serializes the push body
- **THEN** the body contains the snapshot, `producer_timestamp`, and discovery-status report only
- **AND** it contains no Certname, password, token, or other credential

#### Scenario: Agent never listens for inbound collection

- **WHEN** the agent is running between collection passes
- **THEN** it opens no inbound listening port and accepts no server-initiated pull
- **AND** all communication is node-initiated push outbound to chronicle

### Requirement: Bounded Retry With Jittered Backoff

When a push fails with HTTP `503` (or any transport failure), the agent SHALL retry the push with jittered backoff, honoring a server-supplied `Retry-After` when present, for a bounded number of attempts. After exhausting the bound the agent SHALL defer the snapshot to its next collection timer and SHALL NOT durably spool it, because the data path is loss-tolerant and gaps are visible to chronicle.

#### Scenario: Retry on 503 with jittered backoff

- **WHEN** a push receives a `503` response
- **THEN** the agent waits a jittered backoff interval (respecting `Retry-After` if provided) and retries the push
- **AND** it stops retrying once it reaches the configured bounded attempt count

#### Scenario: Retry on transport failure

- **WHEN** a push fails at the transport layer (connection refused, reset, or timeout) before a response
- **THEN** the agent retries with jittered backoff under the same bounded attempt count

#### Scenario: Give up to the next timer, no durable spool

- **WHEN** the agent has exhausted its bounded retry attempts without a successful push
- **THEN** it discards the in-flight snapshot and waits for the next collection timer
- **AND** it does not persist the snapshot to a durable on-disk spool for later replay

### Requirement: Bounded Snapshot Payload

The agent SHALL respect chronicle's server-advertised ingest limits (snapshot bytes, leaf count, path length, single-value bytes) and SHALL NOT emit a pathologically large snapshot, so that one node cannot bloat the shared store or trigger an avoidable rejection.

#### Scenario: Respect server-advertised limits

- **WHEN** chronicle advertises ingest resource limits to the agent
- **THEN** the agent keeps each push within those advertised bounds before sending
- **AND** it does not transmit a snapshot exceeding the advertised snapshot-byte, leaf-count, path-length, or single-value-byte caps

#### Scenario: Surface a rejected oversized push

- **WHEN** a push is rejected by chronicle with a typed oversize error
- **THEN** the agent treats the rejection as terminal for that snapshot (it does not blindly retry the same oversized body)
- **AND** it records the rejection so operators can see sustained oversize rejects

