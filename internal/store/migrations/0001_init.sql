-- chronicle v1 schema — the lazy floor. See docs/adr/0003 (temporal change-only),
-- 0004 (physical schema), 0005 (node identity). Deferred pieces are noted with the
-- trigger that un-defers them. value_hash is computed in the Go ingest path.

-- ── Nodes: identity = facts-ca cert CN (ADR-0005) ───────────────────────────
CREATE TABLE nodes (
    node_id          bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    certname         text        NOT NULL,   -- CN from the verified mTLS chain, never the body
    first_seen       timestamptz NOT NULL DEFAULT now(),
    last_seen        timestamptz NOT NULL DEFAULT now(),  -- server clock, any contact
    last_producer_ts timestamptz,            -- node clock of last APPLIED snapshot (staleness guard)
    deactivated      timestamptz,            -- explicit decommission
    expired          timestamptz,            -- node-ttl sweep (no contact)
    CONSTRAINT nodes_certname_key UNIQUE (certname)
);

-- ── Interned leaf paths (flattened in Go, not SQL) ──────────────────────────
CREATE TABLE fact_paths (
    path_id   bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    path_text text NOT NULL,   -- 'networking.interfaces.eth0.address' (leaf)
    fact_name text NOT NULL,   -- first segment, e.g. 'networking'
    CONSTRAINT fact_paths_path_text_key UNIQUE (path_text)
);
CREATE INDEX fact_paths_name_idx ON fact_paths (fact_name);
-- Deferred: path_array text[] + GIN, trigram GiST on path_text.
--   Add when a query needs array-index access or regex match() on paths.

-- ── Durable facts: the temporal history (the net-new core) ──────────────────
CREATE TABLE fact_history (
    node_id    bigint      NOT NULL REFERENCES nodes(node_id) ON DELETE CASCADE,
    path_id    bigint      NOT NULL REFERENCES fact_paths(path_id),
    value      jsonb       NOT NULL,
    value_hash bytea       NOT NULL,   -- sha256(value_type || canonical bytes); equality key
    valid_from timestamptz NOT NULL,
    valid_to   timestamptz NOT NULL DEFAULT 'infinity',
    PRIMARY KEY (node_id, path_id, valid_from),
    CHECK (valid_from < valid_to),             -- no zero-length/inverted intervals (ADR-0009)
    CHECK (octet_length(value_hash) = 32)      -- value_hash is a 32-byte sha256 digest
);
-- INTEGRITY BOUNDARY: at most one current value per (node, path).
CREATE UNIQUE INDEX fact_history_open_uniq
    ON fact_history (node_id, path_id) WHERE valid_to = 'infinity';
-- cross-node "now": every node where path = value, currently
CREATE INDEX fact_history_now_idx
    ON fact_history (path_id, value_hash, node_id) WHERE valid_to = 'infinity';
-- per-node diff window: intervals opened OR closed in [T1, T2) (a deletion is a pure close)
CREATE INDEX fact_history_node_opened_idx ON fact_history (node_id, valid_from);
CREATE INDEX fact_history_node_closed_idx ON fact_history (node_id, valid_to)
    WHERE valid_to <> 'infinity';
-- per-node point-in-time ("state of node X at T") is served by the above + a range filter.
-- Deferred (un-defers when cross-node historical `at <past T>` runs at scale):
--   btree_gist EXCLUDE (no-overlap over CLOSED intervals) + a composite GiST over
--   (path_id, value_hash, tstzrange(valid_from, valid_to)) for cross-node point-in-time.

-- Applying ONE changed durable leaf at time T (= valid_from), per transaction:
--   1) close the open interval iff the value differs (change-only; no-op if unchanged):
--        UPDATE fact_history SET valid_to = :T
--         WHERE node_id = :n AND path_id = :p AND valid_to = 'infinity'
--           AND value_hash <> :h;
--   2) open a new interval iff none is currently open for (node, path):
--        INSERT INTO fact_history (node_id, path_id, value, value_hash, valid_from)
--        SELECT :n, :p, :v, :h, :T
--         WHERE NOT EXISTS (SELECT 1 FROM fact_history
--                            WHERE node_id = :n AND path_id = :p AND valid_to = 'infinity');
--   disappeared leaf: run step 1 unconditionally (close), skip step 2 (tombstone via absence).

-- ── Volatile facts: latest-only, overwrite-in-place, never historized ────────
CREATE TABLE node_volatile (
    node_id     bigint      NOT NULL PRIMARY KEY REFERENCES nodes(node_id) ON DELETE CASCADE,
    volatile    jsonb       NOT NULL DEFAULT '{}'::jsonb,
    observed_at timestamptz NOT NULL DEFAULT now()
);
-- Deferred (un-defers when a cross-node volatile "now" query actually exists):
--   CREATE INDEX node_volatile_gin ON node_volatile USING gin (volatile jsonb_ops);
--   jsonb_ops indexes both @> containment AND ? key-existence; jsonb_path_ops indexes @>
--   only. Until a cross-node volatile query exists, node_volatile is read per-node by PK.
-- ingest: INSERT ... ON CONFLICT (node_id) DO UPDATE
--         SET volatile = EXCLUDED.volatile, observed_at = EXCLUDED.observed_at;
