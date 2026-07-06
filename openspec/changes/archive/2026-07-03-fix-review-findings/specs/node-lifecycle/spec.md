## MODIFIED Requirements

### Requirement: Soft reversible Expiry

A Node SHALL be marked `expired` automatically after a configurable period without contact (default 7 days). Contact SHALL mean any authenticated push from a non-deactivated certname that reaches decoding or apply — including pushes rejected by the watermark, skew, or validation guards — so that an actively-pushing Node can never be swept as Expired merely because its pushes are being rejected. Load-shedding rejections (rate-limit excess, backpressure `503`) are exempt: a rate-limited Node's admitted pushes still register contact, and the backpressure path MUST NOT add store writes while the store is saturated. Expiry SHALL be reversible: an expired Node whose push is applied SHALL be un-expired and resume its timeline. Expiry SHALL NOT close any intervals and SHALL NOT delete anything. Expired Nodes SHALL be excluded from "now" and state-at-T views by default, with an `include_inactive` opt-in to include them.

#### Scenario: Node expires after the configured silence

- **WHEN** a Node has not made contact for longer than the configured node-ttl (default 7 days)
- **THEN** chronicle marks the Node `expired` without closing any of its open durable intervals and without deleting any history

#### Scenario: Rejected pushes still count as contact

- **WHEN** a Node pushes on every cycle but each push is rejected — stale watermark, clock skew, or a validation error such as a flatten collision
- **THEN** the Node's contact timestamp still advances and the expiry sweep does not mark it `expired`
- **AND** the condition remains visible to operators through the reject metrics rather than through a false Expiry

#### Scenario: Expired Node resumes on next push

- **WHEN** an expired Node pushes a snapshot again and the push is applied
- **THEN** chronicle clears the `expired` state, resumes the Node's existing timeline, and applies the snapshot normally

#### Scenario: Expired Nodes excluded from "now" by default

- **WHEN** a "now" or state-at-T query runs without `include_inactive`
- **THEN** expired Nodes are excluded from the result, but are included when `include_inactive` is set
