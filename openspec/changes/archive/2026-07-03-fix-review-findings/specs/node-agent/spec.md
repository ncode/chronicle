## MODIFIED Requirements

### Requirement: Bounded Snapshot Payload

The agent SHALL respect chronicle's server-advertised ingest limits (snapshot bytes, leaf count, path length, single-value bytes) and SHALL NOT emit a pathologically large snapshot, so that one node cannot bloat the shared store or trigger an avoidable rejection. The agent's size pre-check SHALL measure the same unit the server enforces — the serialized push body — not a smaller proxy of it, so a snapshot that passes the pre-check is not terminally rejected by the server's byte cap every cycle. When the startup fetch of server limits fails, the agent SHALL retry the fetch with backoff on subsequent cycles rather than pinning fallback defaults for the process lifetime.

#### Scenario: Respect server-advertised limits

- **WHEN** chronicle advertises ingest resource limits to the agent
- **THEN** the agent keeps each push within those advertised bounds before sending
- **AND** it does not transmit a snapshot exceeding the advertised snapshot-byte, leaf-count, path-length, or single-value-byte caps

#### Scenario: Pre-check measures the enforced unit

- **WHEN** the agent evaluates a snapshot against the advertised snapshot-byte cap
- **THEN** it measures the serialized push body it is about to send (the unit the server's cap is applied to)
- **AND** a snapshot that passes the pre-check is not deterministically rejected as oversized by the server

#### Scenario: Limits fetch failure is not permanent

- **WHEN** the agent's startup fetch of server limits fails and the agent falls back to defaults
- **THEN** the agent retries the limits fetch on later cycles with backoff
- **AND** converges to the server's real limits once the server is reachable

#### Scenario: Surface a rejected oversized push

- **WHEN** a push is rejected by chronicle with a typed oversize error
- **THEN** the agent treats the rejection as terminal for that snapshot (it does not blindly retry the same oversized body)
- **AND** it records the rejection so operators can see sustained oversize rejects
