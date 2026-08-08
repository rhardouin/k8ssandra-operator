---
phase: release
wave: Docs
todos: [REL-D-01, REL-D-02, REL-D-03, REL-D-04]
---

# Documentation Wave — User and Operator Guidance

- [ ] **REL-D-01 — Rewrite the existing migration workflow for discovery** → modify: docs/content/en/tasks/migrate/_index.md
  - Requirements: document audience/prerequisites, automatic marker/non-retroactivity, Cassandra 4.0+ and unsupported cases, IP seeds/TCP 9042, credential/TLS configuration, stable normalized spec-order fallback, fresh session per attempted seed, earlier failure/later success, first complete valid authority, skipped remainder, deterministic exhaustion, immutable accepted ordered seed list/snapshot, apply/observe/recover, exact three-keyspace heterogeneous RF example `2/9/2/10/10`, current-plan changes, and external drift zero-DDL behavior. Do not claim skipped bootstrap seeds were validated.
  - Scope: only existing migration page; remove stale advice to pre-normalize RF or treat `externalDatacenters` as arbitrary RF preservation.
  - Tests: validate every field/reason/default against generated CRD, source, and release tests; Hugo build/link check.
  - Acceptance evidence: doc-review mapping from each workflow/example claim to source/test; explicit live-proof limitations paragraph.
  - Depends: Release behavior and names frozen; REL-T-05/06 evidence available.

- [x] **REL-D-02 — Publish finite troubleshooting reasons and recovery actions** → modify: docs/content/en/tasks/troubleshoot/_index.md
  - Requirements: cover every stable reason with phase/condition, retryability, safe corrective action, and evidence commands; distinguish pre-managed rediscovery from post-managed/historical permanent conflict; include Job/image/API/binding/version-skew/drift cases and transition-only Event semantics.
  - Scope: only existing troubleshooting page; no raw Secret/result-body collection instructions.
  - Tests: automated reason-list parity against constants/failure matrix plus command review.
  - Acceptance evidence: zero undocumented reason and zero stale reason; examples use actual status paths.
  - Depends: failure contract and REL-T-01..04.

- [x] **REL-D-03 — Document source credential and TLS security constraints** → modify: docs/content/en/tasks/secure/encryption/_index.md
  - Requirements: dedicated legacy credential Secret exact keys, fully qualified binding semantics, read-only copying/mounting, TLS IP verification, authentication-before-identity trust warning, least-privilege resources, unsupported `secretsProvider: external`, rotation/retry behavior, and prohibition on literal production secrets/target superuser reuse.
  - Scope: only existing encryption/security page; use placeholder Secret data references, never real base64 credentials.
  - Tests: security reviewer checks examples against delivered API/resource manifests and canary/redaction tests.
  - Acceptance evidence: all security boundary claims cite implemented behavior; no unsafe default or auth bypass.
  - Depends: REL-T-04 and generated API.

- [ ] **REL-D-04 — Regenerate current CRD reference and summarize delivered docs** → modify: docs/content/en/reference/crd/k8ssandra-operator-crds-latest/_index.md
  - Requirements: regenerate current reference from final CRD; do not touch historical release snapshots; create/update Forge `docs_summary.md` listing changed docs, workflows, examples, security notes, live-proof limits, and LOW follow-ups.
  - Scope: current CRD reference plus `.forge/cassandra_legacy_rf_discovery/docs_summary.md`; primary target marker is current reference.
  - Tests: clean regeneration, Hugo production build, internal links, and docs diff review.
  - Acceptance evidence: generated reference matches CRD exactly, docs are discoverable through existing indexes, historical docs unchanged.
  - Depends: REL-D-01 through REL-D-03 and final generated CRD.
