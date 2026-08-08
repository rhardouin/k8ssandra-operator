---
phase: release
wave: C
todos: [REL-C-01, REL-C-02, REL-C-03, REL-C-04, REL-C-05, REL-C-06]
---

# Wave C — Generated Artifacts and Packaged Rollout

- [ ] **REL-C-01 — Build both manager and digest-identical worker in the operator image** → modify: Dockerfile
  - Requirements: compile/install `/manager` and `/legacy-rf-discovery` from the same source/image; preserve non-root runtime and existing build platforms; worker Job receives the exact normalized digest of the running image.
  - Scope: only `Dockerfile`; no mutable secondary image/tag.
  - Tests: local multi-stage build and container inspection for both executable paths/UID/architecture.
  - Acceptance evidence: image inspect plus executable smoke output; digest binding is proved later in e2e.
  - Delivered evidence: the Dockerfile source contains both binaries and the NOTICE copy, but a fresh production build stopped while resolving Docker Hub base images. Validation-only derivative image hashes and the scratch COPY check are harness-only evidence, not production image proof.
  - Depends: MVP complete.

- [x] **REL-C-02 — Generate deepcopy, CRD, RBAC, and fail-closed webhook artifacts** → modify: config/crd/bases/k8ssandra.io_k8ssandraclusters.yaml
  - Requirements: run repository generators after source markers/RBAC annotations are final; include new input/status/condition schema, Job/attempt least privilege, mutating+validating webhook configurations with `failurePolicy: Fail`; inspect all generated diffs and do not edit historical CRD references.
  - Scope: generated `zz_generated.deepcopy.go`, `config/crd/bases/...`, `config/rbac/role.yaml`, `config/webhook/manifests.yaml`, and current generated docs only; primary marker identifies the CRD source artifact.
  - Tests: `make generate manifests`; schema validation and clean second regeneration.
  - Acceptance evidence: zero diff after second generation, explicit generated-file inventory, and API schema admits valid examples/rejects malformed status/input.
  - Depends: REL-C-01; sequential because generated/shared files overlap.

- [x] **REL-C-03 — Preserve RollingUpdate and enable leader election in Kustomize packaging** → modify: config/manager/manager.yaml
  - Requirements: packaged controller Deployment uses `strategy.type: RollingUpdate`; discovery-aware replicas enable controller-runtime leader election; webhook paths remain fail closed. Operators must wait for rollout completion before creating or modifying legacy-RF migration resources.
  - Scope: `config/manager/manager.yaml` and its existing Kustomize patch/config only.
  - Tests: render default/cluster-scope/namespace-scope Kustomize variants and assert strategy/leader-election/webhook policy.
  - Acceptance evidence: rendered Deployment has `RollingUpdate` and leader election in every supported variant.
  - Depends: REL-C-02.

- [x] **REL-C-04 — Mirror RollingUpdate, leader election, RBAC, CRD, and webhooks in Helm** → modify: charts/k8ssandra-operator/templates/deployment.yaml
  - Requirements: Helm output is behaviorally identical to Kustomize for RollingUpdate, leader election, fail-closed mutating/validating admission, generated CRD, worker image, and least-privilege RBAC.
  - Scope: exact Helm deployment/webhook/RBAC/CRD templates and `values.yaml`; execute after Kustomize contract freezes.
  - Tests: render supported chart modes and compare safety fields/resource permissions to Kustomize.
  - Acceptance evidence: checked render diff plus schema validation; no mutable worker-image value exists.
  - Depends: REL-C-03.

- [x] **REL-C-05 — Freeze the three repository-aligned Cassandra lifecycle fixtures** → new: test/testdata/fixtures/legacy-rf-discovery-4.0/k8ssandra.yaml
  - Requirements: follow the existing `remove-local-dc-4.0`, `remove-local-dc-4.1`, and `remove-local-dc-5.0` fixture convention rather than inventing a centralized source-by-target matrix. Create exactly three version-specific legacy-RF fixtures for Cassandra 4.0, 4.1, and 5.0, using the representative patch versions already exercised by the repository: `4.0.17`, `4.1.9`, and `5.0.6`. Cite the existing suite registrations, GitHub Actions test-name matrix, and source fixture paths in a focused fixture-contract test. Do not require SHA-256 image pins unless release engineering introduces that repository-wide policy; do not add DSE/HCD or a Cartesian product.
  - Scope: three `legacy-rf-discovery-{4.0,4.1,5.0}` fixtures plus a focused fixture-contract test; REL-T-05 owns executable test functions/registrations and CI names. No new centralized version-matrix file.
  - Tests: a focused fixture test fails if a fixture is missing, duplicated, or uses a representative patch version other than 4.0.17, 4.1.9, or 5.0.6.
  - Acceptance evidence: exactly three fixtures map 1:1 to 4.0.17, 4.1.9, and 5.0.6 and cite the existing per-test CI convention they extend.
  - Depends: REL-C-04.

- [x] **REL-C-06 — Verify rendered packaging and generated-artifact parity** → new: test/e2e/legacy_rf_packaging_test.go
  - Requirements: assert Kustomize/Helm RollingUpdate, leader election, both failure policies, worker executable/image contract, Role resourceName restrictions, Secret/Job watches, CRD status/input schema, and no historical-doc mutation.
  - Scope: only `test/e2e/legacy_rf_packaging_test.go`; consume rendered output without editing it.
  - Tests: targeted render test plus repository schema validators.
  - Acceptance evidence: artifact report names every rendered variant and exact asserted safety field.
  - Depends: REL-C-02 through REL-C-05; packaging parity itself has no Cassandra-image-digest requirement.
