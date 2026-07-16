## MODIFIED Requirements

### Requirement: Absence semantics — removed vs not-observed

The fact-ingest service SHALL distinguish a genuinely removed Durable fact from one merely not observed this run, using the per-Source discovery-status report carried in the push. Before deriving cleanliness, the service MUST validate that every built-in status is `ok` or `error` and every external status is `ok`, `error`, or `absent`. A report containing any other value MUST be rejected as `bad-request` before path interning or `Store.ApplySnapshot`; rejection MUST apply no Snapshot diff, create no Tombstone, and leave the Node's `last_producer_ts` unchanged. For an existing Node, normal rejected-push contact handling SHALL still advance `last_seen` to the authenticated receipt time.

For a valid report, the service SHALL derive a single discovery-clean signal — clean iff it contains no built-in `error` and no external `error` entry (an `absent` external Source is not an error). When discovery is clean, every absent Durable fact SHALL be tombstoned (its open interval closed at the Snapshot's `producer_timestamp`); when discovery is NOT clean, every absent Durable fact SHALL be carried forward (its open interval left untouched) and may tombstone on a later clean pass. The service SHALL NOT blindly tombstone every missing Fact regardless of discovery status, because that would fabricate a remove-then-re-add cluster indistinguishable from a reprovision signal.

> Note (v1, see ADR-0009 §1): per-Fact Source provenance (`ResolvedFact.File`) is not available from the `facts` public interface, so v1 uses this whole-pass clean/dirty gate rather than scoping carry-forward to exactly the Facts of one failed Source. The gate is correct for the single-cause scenarios below and safely conservative (defers, never fabricates) in a mixed run. Per-Fact scoping is deferred-with-trigger.

#### Scenario: Source gone — tombstone (rpm_packages reinstall)

- **WHEN** a Node previously reported `rpm_packages.*` Facts, is reinstalled from RHEL to Debian, the `rpm_packages` external script is now absent, and the discovery-status report attributes no error to that Source
- **THEN** the service tombstones the `rpm_packages.*` Facts (closes their open intervals at `producer_timestamp`)
- **AND** does so as part of the same reprovision change-cluster that flips `os.name`, `machine-id`, and the install date.

#### Scenario: Source failed transiently — carry forward

- **WHEN** the `rpm_packages` external script is still present but failed this run (e.g. timed out) and the discovery-status report marks that Source as `error`
- **THEN** the service carries the previously-reported `rpm_packages.*` Facts forward
- **AND** leaves their open intervals untouched (no Tombstone, no re-open).

#### Scenario: Source succeeded but Fact dropped — tombstone

- **WHEN** discovery is clean (no Source reported `error`) but the Snapshot no longer contains a previously-open Fact `pkg.foo`
- **THEN** the service tombstones `pkg.foo` (closes its open interval at `producer_timestamp`)
- **AND** treats the clean discovery without the Fact as a genuine removal.

#### Scenario: Invalid built-in status is rejected before history changes

- **WHEN** a non-empty discovery report assigns a built-in Source any value other than `ok` or `error`
- **THEN** the service rejects the push as `bad-request`
- **AND** applies no Snapshot diff, creates no Tombstone, and leaves `last_producer_ts` unchanged
- **AND** preserves normal contact-on-reject handling for an existing Node.

#### Scenario: Invalid external status is rejected before history changes

- **WHEN** a non-empty discovery report assigns an external Source any value other than `ok`, `error`, or `absent`
- **THEN** the service rejects the push as `bad-request`
- **AND** applies no Snapshot diff, creates no Tombstone, and leaves `last_producer_ts` unchanged
- **AND** preserves normal contact-on-reject handling for an existing Node.
