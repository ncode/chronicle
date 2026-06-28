# Read surface: a small purpose-built query DSL over SQL views, with a uniform `at T`

Reads go through SQL views/functions (which encapsulate the `valid_to='infinity'` temporal
discipline so callers can't get it wrong) plus a **small, purpose-built query DSL** — not a
general AST query engine like PuppetDB's PQL. The DSL covers two shapes:

1. **Compound equality filter → matching nodes:** `role=web os.name=bla` — space-separated
   `path=value`, implicitly AND-ed, compiled to an `INTERSECT` of indexed current-row lookups.
2. **Project + filter + group-by:** `role where os.name='bla' group by role` — compiled to a
   filtered `GROUP BY` over current rows.

A uniform **`at <T>`** qualifier (default: now) runs either shape over `validity @> T` for
point-in-time, making cross-node forensic queries ("which nodes had `os.name=Debian` the
morning of the incident") first-class. Per-node point-in-time is indexed in v1; cross-node
historical `at <past T>` **un-defers the composite GiST temporal index** from ADR-0004. **Per-node diff** is a separate endpoint/function (`node_diff(certname, T1, T2)`) —
it returns changes, a different result shape, and the per-node case dominates.

Both shapes run on the Postgres temporal schema because they hit **current (or shallow-window)
state** (≤100k rows). The only trigger to add a separate OLAP read-model is group-by
aggregation over **deep history with fleet-wide time-bucketing** (tens of millions of rows).

## Deferred, with triggers

- OR / regex / inequality / range operators (e.g. `kernel.version < 5.10`, which also needs
  semver-aware compare, not lexical); multi-field group-by — when a real query needs them.
- **Rule:** `at <past T>` on a **Volatile** path returns a typed "no history" error — volatile
  facts are latest-only (ADR-0007); volatile-path queries route to `node_volatile`.
- **Cross-node change-feed** ("what changed across the fleet between T1 and T2") — when asked.
- Deep-history rollups / OLAP read-model — see trigger above.
- Power users get a read-only SQL role against the views for anything the DSL can't express.

## Rejected

- **PuppetDB-style PQL / AST compiler** — thousands of lines for generality we don't need.
- **Raw-SQL-only** — temporal footguns (`valid_to='infinity'`) in every caller.
- **GraphQL** — overkill for two query shapes.
