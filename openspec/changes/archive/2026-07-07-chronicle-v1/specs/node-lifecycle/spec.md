## ADDED Requirements

### Requirement: CN-based Node identity

A Node's identity SHALL be the Common Name (the **certname**) of its `facts-ca`-issued client certificate, read from the verified mTLS chain on each push and never from the snapshot body. Same certname SHALL mean the same Node and one continuous history. A rebuild or re-provision that keeps the same certname SHALL continue the Node's existing timeline rather than create a new Node; the rebuild SHALL surface as a cluster of durable-fact changes (e.g. machine-id, SSH host keys, install date, hardware) stamped at the rebuild instant.

#### Scenario: Same certname continues one history

- **WHEN** two pushes arrive carrying the same verified certificate CN at different times
- **THEN** both pushes resolve to the same Node and append to one continuous history rather than creating a second Node

#### Scenario: Rebuild keeping the certname surfaces as a change cluster

- **WHEN** a Node is rebuilt or re-provisioned, keeps the same certname, and pushes a snapshot whose machine-id, SSH host keys, install date, and hardware facts have all changed
- **THEN** chronicle continues the Node's existing timeline and records the changed durable facts as a cluster of interval transitions stamped at the rebuild instant, with no new Node created

#### Scenario: Identity comes from the verified chain, not the body

- **WHEN** a push presents one certname in the verified mTLS chain but asserts a different certname inside the snapshot body
- **THEN** chronicle attributes the snapshot to the certname from the verified chain and ignores the body-asserted identity

### Requirement: Hostname is not identity

Changing a Node's hostname SHALL NOT change its identity. The hostname SHALL be treated as an ordinary durable fact that is versioned over time like any other durable fact, while the Node's identity remains its certname.

#### Scenario: Hostname change keeps the same Node

- **WHEN** a Node renames its hostname and pushes a snapshot under the same certname
- **THEN** chronicle keeps the same Node and one continuous history, and records the hostname change as an ordinary durable-fact interval transition

### Requirement: Soft reversible Expiry

A Node SHALL be marked `expired` automatically after a configurable period without contact (default 7 days). Expiry SHALL be reversible: an expired Node that pushes again SHALL be un-expired and resume its timeline. Expiry SHALL NOT close any intervals and SHALL NOT delete anything. Expired Nodes SHALL be excluded from "now" and state-at-T views by default, with an `include_inactive` opt-in to include them.

#### Scenario: Node expires after the configured silence

- **WHEN** a Node has not made contact for longer than the configured node-ttl (default 7 days)
- **THEN** chronicle marks the Node `expired` without closing any of its open durable intervals and without deleting any history

#### Scenario: Expired Node resumes on next push

- **WHEN** an expired Node pushes a snapshot again
- **THEN** chronicle clears the `expired` state, resumes the Node's existing timeline, and applies the snapshot normally

#### Scenario: Expired Nodes excluded from "now" by default

- **WHEN** a "now" or state-at-T query runs without `include_inactive`
- **THEN** expired Nodes are excluded from the result, but are included when `include_inactive` is set

### Requirement: Terminal Deactivation (sunset)

Deactivation SHALL be a terminal, operator-initiated sunset. Upon deactivation chronicle SHALL reject all further pushes for that certname, SHALL seal the Node's timeline by closing the Node's open durable intervals at the deactivation time, and SHALL retain history (keep-forever) queryable via `include_inactive`. The only way for that machine to return SHALL be a new certificate under a new identity (a new certname); a deactivated certname SHALL be permanently retired and never reused. Reactivating a deactivated Node SHALL NOT be supported.

#### Scenario: Deactivation seals the timeline

- **WHEN** an operator deactivates a Node that has open durable intervals
- **THEN** chronicle closes those open durable intervals at the deactivation time and retains the full history

#### Scenario: Pushes rejected after deactivation

- **WHEN** a push arrives carrying a deactivated certname
- **THEN** chronicle rejects the push and applies no changes to the Node's history

#### Scenario: Deactivated history remains queryable

- **WHEN** a query runs with `include_inactive` after a Node has been deactivated
- **THEN** the deactivated Node's sealed history is returned and is not deleted

#### Scenario: Return requires a new identity

- **WHEN** the same machine is brought back after deactivation
- **THEN** it can only be tracked under a new certificate with a new certname, and the deactivated certname is never reused

### Requirement: CRL enforcement at TLS termination

Certificate revocation SHALL be enforced at TLS termination, independent of a Node's deactivation or expiry state. A Node presenting a `facts-ca`-revoked certificate SHALL NOT be able to establish a connection, regardless of whether its certname is active, expired, or deactivated.

#### Scenario: Revoked cert cannot connect

- **WHEN** a Node presents a certificate that appears on the `facts-ca` CRL
- **THEN** the TLS connection is refused at termination before any push is processed

#### Scenario: Revocation is independent of lifecycle state

- **WHEN** a certificate is revoked for a certname that has not been deactivated and is not expired
- **THEN** the connection is still refused at TLS termination on the basis of revocation alone

### Requirement: No auto-purge of history

Neither Expiry nor Deactivation SHALL auto-delete a Node's history. Chronicle SHALL keep durable-fact history forever for forensic purposes; an expired or deactivated Node's intervals and recorded facts SHALL be retained and remain queryable via `include_inactive`.

#### Scenario: Expiry does not delete history

- **WHEN** a Node is marked `expired`
- **THEN** none of its history is deleted and all of it remains queryable via `include_inactive`

#### Scenario: Deactivation does not delete history

- **WHEN** a Node is deactivated
- **THEN** its history is sealed but retained (keep-forever) and remains queryable via `include_inactive`, with no auto-purge
