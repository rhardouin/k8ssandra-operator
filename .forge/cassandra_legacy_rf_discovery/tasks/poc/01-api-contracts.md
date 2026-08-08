---
phase: poc
wave: A
todos: [POC-A-01, POC-A-02, POC-A-03, POC-A-04]
---

# Wave A — API and Failure Contracts

- [x] **POC-A-01 — Define discovery input and immutable status wire types** → modify: apis/k8ssandra/v1alpha1/k8ssandracluster_types.go
  - Requirements: add optional `legacyCqlCredentialsSecretRef` adjacent to `additionalSeeds`; explicit three-keyspace maps; discovery phase/reason/message/transition/hash; immutable snapshot with ordered accepted seed list, order-sensitive digest, and authoritative endpoint provenance; qualified Secret bindings; worker digest; accepted/current/history locations; monotonic managed-creation tombstone; separate `SystemKeyspaceReplicationReady` condition. Preserve omission and use `int32` RF values. Cassandra runtime scope is `>=4.0.0` with no runtime upper bound.
  - Scope: only `apis/k8ssandra/v1alpha1/k8ssandracluster_types.go`; no grouped speculative config and no user-keyspace/Reaper/Stargate types.
  - Tests: table-driven API round-trip/deep-copy tests are scheduled in POC-A-04.
  - Acceptance evidence: `go test ./apis/k8ssandra/v1alpha1` later proves map omission, status round-trip, condition separation, and RF 9/10 preservation.
  - Depends: none.

- [x] **POC-A-02 — Centralize marker, protocol, keyspace, and reason constants** → modify: apis/k8ssandra/v1alpha1/constants.go
  - Requirements: define reserved discovery marker/version, protocol size/version, three exact keyspace names, supported NTS aliases, condition/reason constants, and deterministic public failure categories without raw-cause fields.
  - Scope: only `apis/k8ssandra/v1alpha1/constants.go`; reuse the `k8ssandra.io/...` namespace and existing enum conventions.
  - Tests: compile-time/API tests in POC-A-04 must cover unique stable values and the complete design failure matrix.
  - Acceptance evidence: a reason-matrix test enumerates admission, unsupported server/source/provider, seeds, Job, Secret/TLS, authn/authz/reachability, identity/topology/schema, result/API/conflict, snapshot/managed-state, and drift reasons.
  - Depends: POC-A-01 type names frozen first.

- [x] **POC-A-03 — Define pure current-plan and create-authorization value contracts** → new: controllers/k8ssandra/legacy_rf_discovery_plan.go
  - Requirements: implement pure `ValidateCurrentPlan(snapshot,currentSpec)` and value builders that allow reorder/removal/non-colliding addition, retain accepted seeds, reject external-name collisions and incompatible expected cluster/server-type changes, require exact supplied `externalDatacenters`, and treat accepted/history locations only as a safety search domain.
  - Scope: only `controllers/k8ssandra/legacy_rf_discovery_plan.go`; no Kubernetes I/O and no generic repository abstraction.
  - Tests: table cases in POC-T-02 include first-location reorder/removal, generation change, seed edits, legal addition, collision, server/cluster change, and exact external set.
  - Acceptance evidence: pure test output proves allowed topology evolution does not invalidate the snapshot or prune history.
  - Depends: POC-A-01.

- [x] **POC-A-04 — Lock API wire and failure-matrix tests** → modify: apis/k8ssandra/v1alpha1/k8ssandracluster_types_test.go
  - Requirements: assert JSON field names, absence versus empty maps, deep copies, qualified Secret binding canonical fields, tombstone/history monotonic representation, separate drift condition, MaxInt32/RF 9/10, and one stable/sanitized/retryability classification for every required failure.
  - Scope: only `apis/k8ssandra/v1alpha1/k8ssandracluster_types_test.go`.
  - Tests: run `go test ./apis/k8ssandra/v1alpha1 -run 'TestLegacy|TestDiscovery'`.
  - Acceptance evidence: focused test log plus reviewed golden JSON contains no Secret bytes/raw causes.
  - Depends: POC-A-01 and POC-A-02.
