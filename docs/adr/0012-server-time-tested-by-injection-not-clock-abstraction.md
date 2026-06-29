# Server time is tested by injection and synctest, not a Clock abstraction

Chronicle reads wall-clock time in several places, but it deliberately does **not**
introduce a `Clock` interface threaded through the modules. A periodic architecture
review flagged the missing abstraction; on inspection the abstraction would be shallow —
it would wrap reads that are already injectable, intrinsically database-bound, or must not
be faked. This records why, so the Clock seam is not re-proposed.

The "now" reads fall into kinds, each with its own testability story:

- **Producer time (the temporal anchor, ADR-0006).** Stamped by the agent and carried in
  the push; it is data, not a server clock read. Never server-controlled.

- **Ingest staleness / skew guard.** `Service.Apply(ctx, certname, push, received)` takes
  `received` as a parameter; only the thin HTTP handler reads `time.Now()`. The guard is
  already unit-testable by passing a synthetic `received` (and producer timestamp), as the
  ingest/lifecycle tests do. No seam needed.

- **Periodic sweeps (expiry, monitor).** Two parts: a Go-computed time bound
  (`ExpireStale`'s `cutoff = now − ttl`, `CheckChurn`'s `now − window` — both plain
  `time.Now()` passed as SQL **bind parameters**, *not* SQL `now()`) and a SQL scan that
  applies it (`last_seen < $cutoff`, `valid_from > $window`). A Go clock *could* control the
  bound — but the scan still needs Postgres over seeded rows, and the existing tests already
  pin the boundary by seeding the data (e.g. a 48h-old `last_seen`). So a clock removes
  neither the database dependency nor a test that does not already exist; its value here is
  marginal. The genuine untested gap is the loop *cadence* (tick → work, cancel → exit),
  covered by `testing/synctest` against the shared `internal/periodic.Run` helper — fake
  time, no database, no production Clock.

- **Database clock (`now()` in SQL).** `expired = now()`, the Deactivate seal
  `GREATEST(now(), max(valid_from) + 1µs)`, and `first_seen`/`last_seen` defaults use the
  Postgres clock on purpose, so stored times are consistent with the database that holds
  them. A Go Clock cannot and should not override these.

- **CRL validity window (`ingest/tls.go`).** A security check that rejects expired/future
  CRLs. Faking this clock would defeat the check; it stays the real wall clock.

## Decision

No `Clock` interface. Server time is made testable by (1) parameter injection for the
ingest guard, (2) `testing/synctest` for the periodic loop cadence, and (3) the real
database / real wall clock for the SQL-`now()` and CRL paths, which must not be faked.

## Consequences

- The periodic loop lives once in `internal/periodic` (deduplicated from lifecycle and
  monitor), with the per-tick work injected — the seam that makes the cadence
  synctest-testable, justified by two real callers, not one hypothetical adapter.
- No clock abstraction threads through store/ingest/lifecycle/monitor; SQL-`now()`
  semantics (app/DB clock consistency) are preserved untouched.
- Expiry/churn boundary tests remain integration tests (the scan needs Postgres over seeded
  rows). A Go clock could control the Go-computed bound, but it would neither remove that
  database dependency nor replace the existing seed-based tests.

## Deferred / rejected

- **Rejected — a `Clock` interface across the server modules.** It would be a shallow
  abstraction (wrapping already-injectable, DB-bound, or must-not-fake reads) and fails the
  two-adapters test: the only second "adapter" is a test fake, and the periodic-loop case it
  would serve is already covered by `synctest` with no production seam.
- **Deferred — converting SQL `now()` to app-supplied timestamps.** Only if a future need
  (deterministic replay, explicit app/DB clock-divergence handling) requires it; today the
  database clock is the right source for stored server times.
