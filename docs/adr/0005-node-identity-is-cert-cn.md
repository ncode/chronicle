# Node identity is the facts-ca certificate CN (certname)

A Node's identity is the Common Name of its `facts-ca`-issued client certificate — its
**certname** — stored as `nodes.certname UNIQUE`. Same certname = same Node = one continuous
history.

Consequences:

- **Rebuild / re-provision keeping the same certname continues the Node's timeline.** The
  rebuild surfaces as a cluster of durable-fact changes (machine-id, SSH host keys, install
  date, possibly hardware) stamped at one instant — exactly what forensic time-view wants.
- **Hostname is not identity.** A Node can rename its hostname and remain the same Node;
  hostname is just another durable fact.
- **Certname is taken from the verified mTLS client-cert chain on each push, never from the
  snapshot body.** This prevents a Node from asserting another Node's identity, and is the
  security teeth of `facts-ca` in chronicle (ADR-0002).

## Rejected / known footgun

- **Hardware/machine-id identity** — rejected: would make every rebuild a *new* Node,
  fragmenting history, the opposite of intent.
- **Footgun:** recycling a certname onto a physically different machine silently merges two
  machines into one timeline. Inherent to CN-identity; handled operationally (node
  deactivation/expiry + `facts-ca` revocation), not by chronicle adding a second identity axis.
