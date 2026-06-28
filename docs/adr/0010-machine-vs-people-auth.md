# Authentication splits by principal: machines use mTLS, people use OIDC/tokens

Chronicle authenticates machines and people with different mechanisms, on different
endpoints.

- **Machines (nodes) → ingest endpoint:** facts-ca mTLS, **push-only**. A node identity cert
  can never read. (ADR-0002.)
- **People & automation → a separate read/admin endpoint:** **bearer tokens over server-TLS,
  no client certs.** Humans authenticate via **OIDC** — chronicle is a *relying party*: it
  validates a JWT against the company IdP's JWKS and maps a `groups`/`roles` claim to a
  chronicle role (reader/admin). Automation/CI uses **static API tokens** (or OIDC
  client-credentials). Chronicle is **not** an identity provider and runs no OAuth flows
  itself — it only validates tokens the company IdP issued.

## Rationale

Client certificates are excellent for *machine* identity (facts-ca auto-enrolls them with no
human in the loop) but a persistent operational annoyance for *people* (manual issuance,
renewal, browser/CLI distribution — the Puppet pain point we are explicitly avoiding).
Splitting by principal lets each use the right mechanism.

Critically, this makes facts-ca's "any CA-signed cert = admin" behavior **structurally
irrelevant to reads**: the read/admin endpoint accepts no certs at all, so the CA's admin
semantics cannot leak into chronicle's authorization. Role mapping (reader/admin) is
chronicle's own, never inherited from the CA.

## Layering for bootstrap

- **Static API tokens** need zero external dependency — usable from the first 50-node
  deployment before any IdP exists.
- **OIDC** is opt-in and becomes the primary human path once the company IdP is wired.
- **Power-user raw SQL** stays a third, independent tier, gated by DBA-issued read-only DB
  credentials.

## Rejected

- **Client certs for human access** — the Puppet annoyance we are deliberately escaping.
- **Chronicle embedding a full IdP / OAuth server** — it is a relying party only.
- **One auth mechanism for both machines and people** — forces one principal type onto the
  wrong tool.
