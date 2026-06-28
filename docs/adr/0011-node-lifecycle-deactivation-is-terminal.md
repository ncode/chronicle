# Node lifecycle: expiry is soft, deactivation is a terminal sunset

Chronicle has two node lifecycle states, with deliberately different finality.

- **Expired (automatic, soft).** Set after a configured period without contact (default
  7 days). **Reversible** — an expired node that pushes again is un-expired and resumes its
  timeline; it simply went quiet. Excluded from "now" / state-at-T by default (a view-side
  node-state filter), with an `include_inactive` opt-in for history. Expiry never closes
  intervals or deletes anything.

- **Deactivated (explicit operator action, terminal).** A **sunset**. Chronicle **rejects all
  further pushes** for that certname, and **seals the node's timeline** by closing its open
  durable intervals at the deactivation time. History is retained (keep-forever) and queryable
  via `include_inactive`. The **only** way for that machine to return is a **new certificate
  under a new identity (certname)** — a deactivated certname is permanently retired and never
  reused.

## Consequences

- **Resolves ADR-0005's certname-recycle footgun on the disciplined path.** A properly
  sunset certname can never be recycled onto another machine — chronicle rejects it forever.
  The residual recycle risk now applies only to a certname that was *never* deactivated, just
  went silent.
- **Accidental deactivation is not reversible.** Recovery is re-enrollment under a new
  certname; the prior history is *sealed, not lost*. This friction is intended — deactivation
  means sunset.
- **Revocation is independent.** facts-ca's CRL is enforced at TLS termination (a revoked
  cert cannot connect) regardless of deactivation state.

## Deferred / rejected

- **Deferred — node lineage:** linking a returning machine's new identity to its sunset
  predecessor. Operators track the mapping out-of-band in v1; add a lineage pointer only if
  forensic continuity across re-identification becomes a need.
- **Rejected — reactivating a deactivated node** (the explicit "real sunset" requirement) and
  **auto-purging** an expired/deactivated node's history (contradicts the forensic
  keep-forever purpose).
