# chronicle runbook (v1)

Operational procedures for the chronicle server + agent. Background: `CONTEXT.md`,
`docs/adr/0001–0011`, `docs/schema/v1.sql`.

## Topology

- **`chronicle`** — one stateless binary, replicable behind a load balancer (ADR-0009 §5).
  Three listeners:
  - **ingest** (`ingest_addr`, default `:8443`) — mTLS, nodes push here. `RequireAndVerifyClientCert`.
  - **read/admin** (`read_addr`, default `:8444`) — server-TLS, **no client cert**; people/automation
    authenticate with OIDC bearer tokens or static API tokens (ADR-0010).
  - **ops** (`ops_addr`, default `:9090`) — plain HTTP: `/healthz`, `/readyz`, `/metrics`.
- **`chronicle-agent`** — one per node, pushes over facts-ca mTLS. No inbound port, no spool.
- **PostgreSQL** — the single store.

## 1. Migrations

The server runs embedded migrations on startup (`internal/store/migrations/*.sql`, applied in one
transaction each, idempotent). The first migration is `docs/schema/v1.sql` (kept byte-identical by a
test). To provision a fresh database:

1. Create an empty database and point `database_url` (or `CHRONICLE_DATABASE_URL`) at it.
2. Start `chronicle`; it applies all pending migrations, logging `migrations applied`.

Future index additions ship as new `NNNN_*.sql` migrations and should use
`CREATE INDEX CONCURRENTLY` (which requires a no-transaction migration — add that marker when the
second real migration lands).

**Rollback** (greenfield): stop agents, drop the schema. There is no prior data to migrate back.

## 2. First-enrollment ramp (cold start)

The first snapshot from each node is an all-INSERT (no dedup possible), so enrolling 100k nodes at
once is the heaviest write the system ever sees. Throttle it:

- Roll out `chronicle-agent` in batches, or rely on each agent's **initial jitter** (it waits a
  random `[0, jitter)` before its first push) to spread the herd.
- Watch `chronicle_ingest_apply_seconds` and Postgres load; widen batches as headroom allows.
- The agent's per-node interval jitter keeps steady-state pushes desynchronized.

If ingest saturates during the ramp, it sheds load with `503 + Retry-After` (see §3) and agents back
off — the ramp self-paces. Gaps are visible and harmless (the data path is loss-tolerant).

## 3. Backpressure

Under store saturation the ingest endpoint fast-fails with **HTTP 503 + `Retry-After`** once the
bounded ingest concurrency (`max_concurrent`) is reached — it never buffers without bound. Agents
retry with jittered backoff a bounded number of times, then defer to their next timer.

- Symptom: rising `chronicle_ingest_pushes_total{result="rejected"}` with
  `chronicle_ingest_rejects_total{reason="backpressure"}`.
- Action: scale ingest replicas and/or Postgres; raise `max_concurrent` only if the store can take it.

## 4. Deactivation (terminal sunset)

Deactivation is **terminal** (ADR-0011): it seals the node's timeline and the certname is retired
forever. Use it when a machine is decommissioned.

```
curl -sS -X POST \
  -H "Authorization: Bearer $ADMIN_TOKEN" \
  "https://chronicle:8444/v1/admin/deactivate?certname=web01.example.com"
```

Effect: closes all open durable intervals at the seal time, marks the node `deactivated`, and **rejects
all further pushes** for that certname (`403 deactivated`). History is retained (keep-forever) and
remains queryable with `include_inactive=true`. The only way the machine returns is a **new
certname** (new cert); the old certname is never reused. There is no reactivation.

CRL revocation is independent and enforced at TLS (§6).

## 5. Poisoned watermark reset

A node whose clock jumped far into the future (before the two-sided guard, or via a bug) can poison
its `last_producer_ts`, after which every in-bounds push is rejected as `stale`. Recover it:

```
curl -sS -X POST \
  -H "Authorization: Bearer $ADMIN_TOKEN" \
  "https://chronicle:8444/v1/admin/reset-watermark?certname=web01.example.com"
```

This clears `last_producer_ts`; the next in-bounds push is accepted and re-establishes the watermark.

- Symptom: sustained `chronicle_ingest_rejects_total{reason="stale"}` for one certname.

## 6. Revocation (CRL)

Revocation is enforced at TLS termination, independent of deactivation/expiry. Configure `tls.crl`
to the facts-ca CRL file. After revoking a cert in facts-ca, refresh the CRL on disk and **`SIGHUP`**
the server to reload it (no restart needed). A revoked cert cannot complete the TLS handshake.

## 7. Hot config reload (`SIGHUP`)

`SIGHUP` re-reads the config file and hot-swaps the **volatile-path policy** and the **CRL**. Other
knobs (`max_skew`, caps, rate limits, expiry TTL) take effect on restart.

- Add a churny durable path to `volatile_paths` (see §8) and `SIGHUP` to start routing it to
  `node_volatile` — forward-only; existing history is untouched.

## 8. Alarms

The server logs two operator alarms hourly:

- **High-churn durable path** — a durable path opening intervals on nearly every push is almost
  always a misclassified volatile fact. Add it to `volatile_paths` and `SIGHUP` (§7).
- **Per-node `fact_paths` cardinality spike** — one node introducing thousands of distinct paths is
  trying (accidentally or not) to bloat the shared never-GC'd dictionary. Investigate that certname.

## 9. Health

- `GET /healthz` — liveness (always 200 while the process is up).
- `GET /readyz` — readiness; 200 only when Postgres is reachable, else 503. Use for load-balancer
  health checks.
- `GET /metrics` — Prometheus exposition.
