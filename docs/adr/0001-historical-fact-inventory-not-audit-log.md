# Chronicle is a historical fact inventory, not an audit log or query engine

Chronicle stores the history of node facts (collected via `ncode/facts`) to answer three
questions: what does the fleet look like now, what did a node look like at a past time, and
what changed in between.

We explicitly reject two adjacent shapes:

- **Not tamper-evident storage.** `facts-ca` authenticates *collection* over mTLS but does
  not sign fact payloads. True evidentiary storage (detached per-batch signatures) is more
  machinery than our use case (drift + forensic time-view) needs. Provenance is
  "we authenticated the node at collection time," not "this stored snapshot is
  cryptographically non-repudiable."
- **Not an osquery-style on-node query engine.** `facts` is a one-shot discovery pass, not
  a resident query engine. All change-detection and diffing happen centrally, keeping the
  per-node footprint to "a periodic facts run." Principle: **dumb node, smart center.**
