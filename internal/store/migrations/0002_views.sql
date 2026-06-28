-- Reconstruction surface (ADR-0003, task 2.6). Views/functions encapsulate the
-- valid_to='infinity' temporal discipline so callers can't get it wrong.

-- "Now": current value of every (node, path).
CREATE VIEW current_facts AS
SELECT fh.node_id, fh.path_id, fp.path_text, fp.fact_name,
       fh.value, fh.value_hash, fh.valid_from
FROM   fact_history fh
JOIN   fact_paths   fp USING (path_id)
WHERE  fh.valid_to = 'infinity';

-- Cross-node point-in-time: every (node, path) value valid at T. Encapsulates
-- the validity predicate so the DSL's `at <T>` reads never touch raw fact_history
-- (fact-query spec). Un-indexed in v1 (a scan); the deferred GiST index optimizes
-- it later without changing this surface.
CREATE FUNCTION facts_at(p_t timestamptz)
RETURNS TABLE (node_id bigint, path_id bigint, value jsonb, value_hash bytea)
LANGUAGE sql STABLE AS $$
    SELECT fh.node_id, fh.path_id, fh.value, fh.value_hash
    FROM   fact_history fh
    WHERE  fh.valid_from <= p_t AND p_t < fh.valid_to;
$$;

-- State-at-T: the interval whose validity contains T (valid_from <= T < valid_to).
CREATE FUNCTION node_state_at(p_node bigint, p_t timestamptz)
RETURNS TABLE (path_id bigint, path_text text, fact_name text,
               value jsonb, valid_from timestamptz, valid_to timestamptz)
LANGUAGE sql STABLE AS $$
    SELECT fh.path_id, fp.path_text, fp.fact_name, fh.value, fh.valid_from, fh.valid_to
    FROM   fact_history fh
    JOIN   fact_paths   fp USING (path_id)
    WHERE  fh.node_id = p_node
      AND  fh.valid_from <= p_t
      AND  p_t < fh.valid_to;
$$;

-- Diff(T1,T2): intervals that opened OR closed in [T1,T2). A pure deletion is a
-- close with no matching open (closed_in_window true, opened_in_window false,
-- valid_to <> 'infinity'); it MUST appear (ADR-0003).
CREATE FUNCTION node_diff(p_node bigint, p_t1 timestamptz, p_t2 timestamptz)
RETURNS TABLE (path_id bigint, path_text text, fact_name text, value jsonb,
               valid_from timestamptz, valid_to timestamptz,
               opened_in_window boolean, closed_in_window boolean)
LANGUAGE sql STABLE AS $$
    SELECT fh.path_id, fp.path_text, fp.fact_name, fh.value, fh.valid_from, fh.valid_to,
           (fh.valid_from >= p_t1 AND fh.valid_from < p_t2) AS opened_in_window,
           (fh.valid_to   >= p_t1 AND fh.valid_to   < p_t2) AS closed_in_window
    FROM   fact_history fh
    JOIN   fact_paths   fp USING (path_id)
    WHERE  fh.node_id = p_node
      AND ((fh.valid_from >= p_t1 AND fh.valid_from < p_t2)
        OR (fh.valid_to   >= p_t1 AND fh.valid_to   < p_t2));
$$;
