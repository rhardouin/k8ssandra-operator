---
phase: mvp
wave: B
todos: [MVP-B-07, MVP-B-08, MVP-B-09]
---

# Wave B — Typed Admission

- [x] **MVP-B-07 — Implement typed CREATE-only marker mutation** → modify: apis/k8ssandra/v1alpha1/k8ssandracluster_webhook.go
  - Requirements: inject the reserved marker only on CREATE; reject client-supplied marker; never inject on UPDATE; preserve existing defaulting/validation and receive dependencies without adding package globals.
  - Scope: only `apis/k8ssandra/v1alpha1/k8ssandracluster_webhook.go`; configuration generation waits for Release Wave C.
  - Tests: real webhook envtest in MVP-B-09 covers create/update and admission outage behavior.
  - Acceptance evidence: API-server create returns marker only when server injected it; forged input fails.
  - Depends: POC-A-01/02.

- [x] **MVP-B-08 — Enforce marker immutability and local discovery validation** → modify: apis/k8ssandra/v1alpha1/k8ssandracluster_webhook.go
  - Requirements: reject marker removal/mutation and user ownership; validate only local structure: IP-literal seed syntax, credential reference shape, and unsupported external provider where determinable; leave live/version/topology checks to the controller. Existing unmarked objects must remain unmarked and valid on upgrade.
  - Scope: same file as MVP-B-07, therefore execute after it in this file; no remote API/CQL calls in admission.
  - Tests: update matrix in MVP-B-09 includes pre-feature unmarked object, forged/mutated/removed marker, invalid seeds, supported Cassandra and unsupported provider behavior.
  - Acceptance evidence: webhook tests prove non-retroactivity and deterministic errors without Secret data.
  - Depends: MVP-B-07.

- [x] **MVP-B-09 — Prove admission behavior through real envtest webhooks** → modify: apis/k8ssandra/v1alpha1/k8ssandracluster_webhook_test.go
  - Requirements: exercise API CREATE/UPDATE, not direct method calls; assert injection, forgery rejection, immutable marker, no update injection, failure-closed unavailability, valid legacy unmarked update, and compatibility with no-seed objects.
  - Scope: only `apis/k8ssandra/v1alpha1/k8ssandracluster_webhook_test.go`.
  - Tests: `go test ./apis/k8ssandra/v1alpha1 -run TestK8ssandraClusterWebhook`.
  - Acceptance evidence: focused envtest log identifies each admission transition; no packaged rollout-safety claim yet.
  - Depends: MVP-B-07/08.
