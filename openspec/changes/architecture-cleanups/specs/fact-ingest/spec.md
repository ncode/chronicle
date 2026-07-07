## ADDED Requirements

### Requirement: One shared Volatile policy for write and read

The server SHALL hold exactly one Volatile policy instance, shared by write-side
classification (ingest) and read-side routing and at-T rejection (query), and SHALL swap
it atomically as a unit on reload — the two endpoints can never disagree by construction.
Each push SHALL be classified against a single policy snapshot taken once for the whole
push, and each query SHALL be evaluated against a single policy snapshot taken once at
query entry, so a concurrent reload never mixes two policies within one operation.

#### Scenario: Reload flips both sides together
- **WHEN** the Volatile policy is reloaded (e.g. on SIGHUP) with a pattern newly matching
  path `p`
- **THEN** subsequent pushes route `p` to the latest-only volatile store AND subsequent
  `at <past T>` queries for `p` reject with the typed no-history error — with no window in
  which the two endpoints disagree

#### Scenario: One snapshot per operation
- **WHEN** a policy reload lands while a push is being classified or a query is being
  evaluated
- **THEN** that in-flight operation completes entirely under the policy snapshot it
  started with

#### Scenario: Invalid patterns fail startup, not reload
- **WHEN** the initial `VolatilePaths` configuration fails to compile
- **THEN** the server fails to start; **WHEN** a reload's patterns fail to compile
- **THEN** the running policy is kept unchanged and the failure is logged
