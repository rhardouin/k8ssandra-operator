---
phase: mvp
wave: B
todos: [MVP-B-14, MVP-B-15, MVP-B-16, MVP-B-17]
---

# Wave B — Snapshot-Aware Post-Ready Schema Logic

- [x] **MVP-B-14 — Implement pure three-keyspace live prevalidation and managed merge** → new: controllers/k8ssandra/legacy_rf_discovery_schema.go
  - Requirements: parse live maps/NTS aliases/RF `1..MaxInt32`; preserve omission; validate all three external projections exactly before planning any DDL; merge only current desired managed entries; return a complete plan or one drift/error with deterministic precedence.
  - Scope: only `controllers/k8ssandra/legacy_rf_discovery_schema.go`; source protocol package must not own managed planning.
  - Tests: sparse heterogeneous maps, RF 9/10, missing row, strategy/RF errors, external add/remove/value drift, managed reorder/removal/addition, and no-op plan.
  - Acceptance evidence: pure tests show external maps are byte/value unchanged and any one-keyspace error yields no plan.
  - Depends: PoC snapshot/current-plan contracts.

- [x] **MVP-B-15 — Implement marked post-Ready read-all/validate-all/direct-ALTER branch** → modify: controllers/k8ssandra/schemas.go
  - Requirements: retain lifecycle-scoped `ManagementApiFactory`; for marked accepted objects read all three maps before DDL, apply MVP-B-14 plan, call direct `AlterKeyspace` only for changed managed maps, and re-read all three after partial failure. The marked path must be structurally unable to call `EnsureKeyspaceReplication` or create-capable helpers; unmarked path remains unchanged.
  - Scope: only `controllers/k8ssandra/schemas.go`; no discovery worker or controller gate edits.
  - Tests: MVP-B-17 mocks exact Management API call ordering and mutation count.
  - Acceptance evidence: call logs prove zero DDL on any prevalidation/drift error and no create-capable method invocation.
  - Depends: MVP-B-14.

- [x] **MVP-B-16 — Persist external drift as a separate transition-only condition** → modify: controllers/k8ssandra/schemas.go
  - Requirements: keep discovery phase `Accepted`; set `SystemKeyspaceReplicationReady=False/ExternalReplicationDrift`; sanitize actionable message; emit Event only on state transition; issue zero DDL for that reconciliation; restore true only after all-three fresh validation succeeds.
  - Scope: same file as MVP-B-15, execute sequentially; do not overwrite discovery acceptance status.
  - Tests: repeated identical drift, changed drift, recovery, and partial ALTER retry condition behavior.
  - Acceptance evidence: tests show stable transition time/Event count on identical retries and unchanged snapshot hash.
  - Depends: MVP-B-15.

- [x] **MVP-B-17 — Prove all-three prevalidation and zero-DDL drift behavior** → new: controllers/k8ssandra/legacy_rf_discovery_schema_test.go
  - Requirements: cover every error before first ALTER, heterogeneous `system_auth` `2/9/2/10/10`, sparse traces/distributed maps, no-op, managed-only changes, partial second ALTER failure then full reread, condition transitions, and legacy unmarked regression behavior.
  - Scope: only `controllers/k8ssandra/legacy_rf_discovery_schema_test.go`; use existing Management API fake patterns.
  - Tests: `go test -race ./controllers/k8ssandra -run 'TestLegacyRFSchema|TestCheckSchemas'`.
  - Acceptance evidence: mock assertions show exact Get×3 before Alter and zero create/ensure calls for marked objects.
  - Depends: MVP-B-14 through MVP-B-16.
