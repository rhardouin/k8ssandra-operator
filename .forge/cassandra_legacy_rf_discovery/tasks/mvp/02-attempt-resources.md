---
phase: mvp
wave: B
todos: [MVP-B-04, MVP-B-05, MVP-B-06]
---

# Wave B — Kubernetes Discovery Attempts

- [x] **MVP-B-04 — Build bounded least-privilege attempt resources** → new: controllers/k8ssandra/legacy_rf_discovery_resources.go
  - Requirements: create unique attempt ConfigMap, HMAC Secret, copied read-only credential/TLS Secret, ServiceAccount, resourceName-scoped Role/RoleBinding, and bounded Job in the first current operator-owned DC location; bind UID/generation/marker/ordered seeds with order-sensitive digest/qualified Secret versions/image digest; preserve normalized spec order in worker input; hardened Pod security, deadline, retry, cleanup, and watched-by labels are mandatory.
  - Scope: only `controllers/k8ssandra/legacy_rf_discovery_resources.go`; no shared controller wiring or generated RBAC.
  - Tests: pure desired-resource tests cover names, owners/correlation, mounts, resourceName restrictions, deadlines, security context, and no Secret bytes in non-Secret objects.
  - Acceptance evidence: object snapshots show the worker command `/legacy-rf-discovery` and exact digest-pinned image, never a mutable tag.
  - Depends: PoC contracts and MVP-B-02 executable contract.

- [x] **MVP-B-05 — Resolve and bind the running manager image digest** → new: controllers/k8ssandra/legacy_rf_discovery_image.go
  - Requirements: uncached-read the manager Pod `status.containerStatuses[].imageID`; select the configured manager container; normalize immutable digest reference; reject tag-only, ambiguous, absent, or malformed identity; bind it into attempt and result verification.
  - Scope: only `controllers/k8ssandra/legacy_rf_discovery_image.go`; no registry lookup fallback and no mutable tag use.
  - Tests: table cases cover common OCI imageID forms, wrong container, restart, absent status, ambiguity, and malformed digest.
  - Acceptance evidence: focused tests prove every Job uses the exact running operator digest or blocks.
  - Depends: POC-A-07 protocol binding.

- [x] **MVP-B-06 — Implement attempt ensure/result/cleanup lifecycle** → new: controllers/k8ssandra/legacy_rf_discovery.go
  - Requirements: implement the three-method `DiscoveryAttempts` boundary; direct-read remote resources; verify signed bounded results and bindings; map Job scheduling/image-pull/deadline/result/API failures deterministically; clean stale attempts only when snapshot-loss safety permits; use bounded backoff+jitter through injected seams.
  - Scope: only `controllers/k8ssandra/legacy_rf_discovery.go`; no edits to aggregate controller/datacenter/schema files.
  - Tests: focused controller tests cover idempotent ensure, partial resource creation, result forgery/staleness/oversize, retry bounds, cleanup, transition-only events, and redaction.
  - Acceptance evidence: call-order and fake-client logs prove no managed Cassandra resource or unrelated component is created by this module.
  - Depends: MVP-B-04 and MVP-B-05.
