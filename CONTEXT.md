# Chronicle

Chronicle periodically collects system facts from a fleet of nodes and stores their
history, so you can ask what the fleet looks like now, what a node looked like at a past
time, and what changed in between. It is a historical, queryable inventory — not a
tamper-proof audit log and not an on-node query engine.

## Language

**Node**:
A tracked member of the fleet, identified by its Certname. Identity persists across reboots,
re-provisioning, and any fact change (including hostname): same Certname = same Node, one
continuous history. Not synonymous with a hostname or a physical machine.
_Avoid_: host, machine, agent, server

**Certname**:
The Common Name of a Node's `facts-ca`-issued client certificate — chronicle's identity for
a Node. Read from the verified mTLS chain on each push, never from the snapshot body, so a
Node cannot claim another's identity.
_Avoid_: hostname, FQDN, node name

**Fact**:
A single system attribute discovered by the upstream `facts` library, addressed by a
dotted path (e.g. `os.name`, `kernel.version.full`). The value may be a scalar or a
nested subtree.
_Avoid_: attribute, property, metric

**Snapshot**:
The full set of facts discovered from one Node in one collection pass — a nested
`map[string]any` (the upstream `facts.Snapshot`). The `facts` library attaches no time or
identity; chronicle stamps a Snapshot with a time and source Node on receipt.
_Avoid_: report, scan, sample

**Durable fact**:
A Fact whose value is stable and configuration-like (e.g. `os.name`, kernel version,
installed packages, network config, hardware). A change is signal; chronicle versions these
over time. They are the inventory and the forensic timeline.
_Avoid_: config fact

**Volatile fact**:
A Fact whose value changes nearly every collection pass and carries no historical value
(e.g. `uptime`, `memory.system.available_bytes`, load). Chronicle keeps at most the latest
value and never writes it to history — historizing volatile facts is the dominant source of
storage bloat.
_Avoid_: telemetry fact, metric

**Source**:
The resolver or external script that produces a Fact (e.g. the `networking` resolver, or an
external `rpm_packages.sh` script). Chronicle records each Fact's Source so it can tell a
genuine removal from a transient discovery failure.

**Tombstone**:
The closing of a Durable fact's open interval because the Fact was genuinely removed from a
Node — its Source ran successfully without producing it, or the Source itself is gone — as
opposed to merely *not observed* this pass (a failed Source, which carries the interval
forward instead).
_Avoid_: delete, remove (those describe row deletion, not a closed interval)

**Expiry**:
A soft, automatic Node state set after a configured period without contact (default 7 days).
Reversible — the Node resumes if it pushes again. Excluded from "now" by default.

**Deactivation** (sunset):
A terminal, operator-initiated retirement of a Node. Chronicle rejects all further pushes for
that Certname and seals its timeline; the only way back is a new certificate under a new
identity. Distinct from Expiry, which is soft and reversible.
_Avoid_: decommission, sunset (use Deactivation)
