# Collection is node-initiated push over facts-ca mTLS

Each node runs a small, near-idle agent that, on a timer, runs `facts` discovery and POSTs
the resulting snapshot to chronicle over mTLS, authenticated by a `facts-ca`-issued client
certificate. Chronicle is a single ingest endpoint; it never reaches out to nodes.

We chose node-push over server-pull (SSH or chronicle-initiated mTLS) because it:

- matches the Puppet lineage of both libraries (agent check-in);
- is the only model that uses `facts-ca` (SSH brings its own auth);
- keeps nodes near-idle with no inbound listening port;
- lets nodes self-jitter, avoiding a thundering herd at any scale;
- reduces chronicle to an ingest endpoint with no node-reachability problem and no
  credential fan-out to N nodes.

Cost: we must build and ship the agent — neither `ncode/facts` nor `ncode/facts-ca`
provides one. It is small (timer + `facts.Discover()` + mTLS POST).

Target scale: 50 → 100,000 nodes on a single, shared deployment that grows with the company.
