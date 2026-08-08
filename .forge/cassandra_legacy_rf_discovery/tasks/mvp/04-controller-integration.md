---
phase: mvp
wave: C
todos: [MVP-C-01, MVP-C-02, MVP-C-03, MVP-C-04, MVP-C-05]
---

# Wave C — Sequential Shared Controller Integration

- [x] **MVP-C-01 — Wire direct readers and discovery dependencies at composition root** → integrate: main.go
  - Requirements: construct validated attempt/state/image/clock/backoff dependencies; retain `ManagementApiFactory` lifecycle; inject direct local/remote readers and recorder; enable controller-runtime leader election through existing config without global state.
  - Scope: only `main.go`; do not edit manifests or controller logic here.
  - Tests: startup/unit compilation with nil-dependency rejection and existing manager setup tests.
  - Acceptance evidence: `go test ./...` compile graph plus constructor test demonstrates every external boundary is injected.
  - Depends: all MVP Wave B modules.

- [x] **MVP-C-02 — Register watches for attempts, Secrets, Jobs, and result resources** → integrate: controllers/k8ssandra/k8ssandracluster_controller.go
  - Requirements: extend `SetupWithManager` with watched-by label mappings for referenced credential/TLS Secrets and attempt Job/ConfigMap/Secret changes; enqueue owning cluster without leaking secret data or broad unrelated fan-out.
  - Scope: only watch/registration section of `controllers/k8ssandra/k8ssandracluster_controller.go`.
  - Tests: controller tests verify each resource update enqueues exactly the owner and unrelated resources do not.
  - Acceptance evidence: envtest reconcile counts and mapping assertions.
  - Depends: MVP-C-01.

- [x] **MVP-C-03 — Insert the discovery gate before all managed/support creation** → integrate: controllers/k8ssandra/k8ssandracluster_controller.go
  - Requirements: run after deletion/finalizer/basic Cassandra validation but before superuser, replicated Secret, Reaper, Medusa, Stargate, managed DC, or Cassandra-pod-affecting steps; Pending/Blocked/just-Accepted stops reconciliation; only later accepted hash read-back continues.
  - Scope: only sequential reconcile pipeline and reconciler fields in `controllers/k8ssandra/k8ssandracluster_controller.go`.
  - Tests: existing controller suite asserts no forbidden resource before acceptance, no create in acceptance reconcile, and accepted read-back before progression.
  - Acceptance evidence: ordered reconciliation trace demonstrates exact gate position.
  - Depends: MVP-C-02.

- [x] **MVP-C-04 — Consume accepted seeds and authorize immediately beside DC Create** → integrate: controllers/k8ssandra/datacenters.go
  - Requirements: marked accepted path uses immutable canonical snapshot seeds, never later spec seeds; invoke named final authorization immediately beside `remoteClient.Create`; use direct union survey/history update/read-back ordering; `AlreadyExists` restarts; leave unmarked behavior unchanged.
  - Scope: only `controllers/k8ssandra/datacenters.go`; no schema/admission edits.
  - Tests: datacenter tests cover seed edits after acceptance, every acceptance/create race, reorder/removal/addition, concurrent reconciliation, restart, and AlreadyExists.
  - Acceptance evidence: mock call order proves authorization→immediate Create and exact accepted seeds in desired DC.
  - Depends: MVP-C-03 and MVP-B-13.

- [x] **MVP-C-05 — Add aggregate controller regression and safety tests** → modify: controllers/k8ssandra/k8ssandracluster_controller_test.go
  - Requirements: multi-cluster envtest covers qualification, early gate placement, CAS/read-back, restart, current/history surveys, pre-managed rediscovery, post-managed permanent conflict after historical location removal, Secret/result watches, generation/binding/plan races, and redaction.
  - Scope: only `controllers/k8ssandra/k8ssandracluster_controller_test.go`; reuse existing multi-cluster harness.
  - Tests: `go test -race ./controllers/k8ssandra` with deterministic injected clock/backoff; no real Cassandra.
  - Acceptance evidence: named cases map to every acceptance/create safety invariant and explicitly label envtest limitations.
  - Depends: MVP-C-01 through MVP-C-04.
