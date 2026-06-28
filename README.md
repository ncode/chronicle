# chronicle

A Go service that continuously collects system facts from a fleet of nodes
(50 → 100,000) and stores their **per-fact temporal history** in PostgreSQL, so
you can ask three questions over time:

- **Now** — what does the fleet look like right now?
- **State-at-T** — what did node X look like at time T?
- **Diff** — what changed (including removals) between T1 and T2?

It builds on two sibling libraries: [`ncode/facts`](https://github.com/ncode/facts)
(a Facter port — the collector) and [`ncode/facts-ca`](https://github.com/ncode/facts-ca)
(a Puppet-CA port — mTLS node identity). `facts` discovers; `facts-ca` authenticates;
chronicle adds the part neither has: per-fact temporal history.

> Dumb node, smart center. The full design rationale lives in `CONTEXT.md`,
> `docs/adr/0001–0011`, `docs/schema/v1.sql`, and the `openspec/` change.

## Architecture

```
 chronicle-agent (per node)                    chronicle (server, stateless, replicable)
 ┌───────────────────────┐   facts-ca mTLS     ┌──────────────────────────────────────┐
 │ facts.Discover()      │  POST /v1/push      │ ingest  : CN-from-chain identity,     │
 │ + producer_timestamp  │ ──────────────────► │           two-sided clock guard,      │   ┌────────────┐
 │ + discovery status    │                     │           per-node serialized apply   │──►│ PostgreSQL │
 │ jittered timer, retry │                     │ store   : change-only temporal model  │   │ (temporal) │
 └───────────────────────┘                     │ query   : DSL + node_diff (OIDC/token)│◄──│            │
                                               │ lifecycle: expiry + deactivation      │   └────────────┘
   people / automation  ── bearer token ─────► │ ops     : /healthz /readyz /metrics   │
 (OIDC or static API token, server-TLS, no cert)└──────────────────────────────────────┘
```

- **Change-only storage:** facts are stored as validity intervals
  (`[valid_from, valid_to)`, `valid_to = 'infinity'` = current). Only *changes*
  are written, so 100k nodes cost tens of GB/year, not tens of TB.
- **Durable vs Volatile:** churny facts (`uptime`, free memory, load) are kept
  latest-only and never historized; everything else is versioned.
- **Machines use mTLS, people use tokens:** nodes push over facts-ca mTLS
  (push-only, identity is the verified cert CN); humans/automation read on a
  separate endpoint with OIDC/bearer tokens — a node cert can never read.

## Platform support

The **agent** runs everywhere `ncode/facts` runs (its release tier); the
**server** runs everywhere PostgreSQL-capable Go runs (the same set minus
Plan 9). Both are pure-Go (`CGO_ENABLED=0`), cross-compiled for every target.

| OS | agent | server | CI |
|----|:---:|:---:|---|
| Linux (amd64, arm64) | ✅ | ✅ | native build+test |
| macOS (arm64, amd64) | ✅ | ✅ | native build+test |
| Windows (amd64, arm64) | ✅ | ✅ | native build+test |
| FreeBSD / OpenBSD / NetBSD / DragonFly | ✅ | ✅ | cross-compile |
| illumos (OmniOS) | ✅ | ✅ | cross-compile |
| Plan 9 (amd64) | ✅ | — | cross-compile (agent only) |

GitHub CI does native build+test on the hosted OSes, full Postgres integration on
Linux, and a cross-compile sweep over the whole matrix. The agent is additionally
validated end to end (`facts.Discover()` → snapshot) across the full OS matrix.

## Build & test

```sh
make build            # both binaries
make vet
make test             # unit tests (integration self-skips without a DB)
make test-db          # spin a throwaway Postgres and run the full suite
```

The integration tests need PostgreSQL; set `CHRONICLE_TEST_DB` to a pgx
connstring, or use `make test-db`. They share one database, so run with `-p 1`.

### Agent dry-run (no server needed)

```sh
go run ./cmd/chronicle-agent -dry-run        # discover facts once, print the push payload
```

This is the cross-OS smoke: a non-empty snapshot proves `facts.Discover()` works
on the host OS, and is the basis for the cross-OS agent validation.

## Layout

```
cmd/chronicle/          server (ingest + read/admin + ops listeners)
cmd/chronicle-agent/    per-node collector
internal/store/         PostgreSQL temporal model + migrations
internal/ingest/        mTLS ingest contract
internal/agent/         facts collection + mTLS push
internal/query/         read DSL + node_diff + OIDC/token auth
internal/lifecycle/     expiry sweep + deactivation
internal/{metrics,monitor,config,wire}/
docs/adr/               the 11 accepted architecture decisions
openspec/               the spec-driven change (proposal, specs, design, tasks)
```

See `docs/runbook.md` for operations (migrations, enrollment ramp, backpressure,
deactivation, watermark reset, CRL, alarms).
