---
phase: mvp
wave: B
todos: [MVP-B-10, MVP-B-11, MVP-B-12, MVP-B-13]
---

# Wave B — Acceptance and Creation Safety

- [x] **MVP-B-10 — Implement qualification and early state-machine decisions** → modify: controllers/k8ssandra/legacy_rf_discovery.go
  - Requirements: decide `NotRequired/Pending/Blocked/Accepted`; marked Cassandra+seeds qualifies, unmarked/no-seed does not, marked DSE/HCD reports unsupported without preservation claim, external provider/source<4.0 blocks; choose first current operator-owned DC location as attempt provenance only; emit identical status/Event once per transition.
  - Scope: only `controllers/k8ssandra/legacy_rf_discovery.go`, sequential after MVP-B-06.
  - Tests: matrix covers qualification, DiscoveryTooLate, plan changes, recoverable/transient failures, and sanitized corrective actions.
  - Acceptance evidence: state table tests map every design failure to stable reason/retryability/message precedence.
  - Depends: MVP-B-06 and admission contracts.

- [x] **MVP-B-11 — Implement explicit immutable snapshot acceptance** → modify: controllers/k8ssandra/legacy_rf_discovery_status.go
  - Requirements: order uncached control-plane read; UID/marker/generation/seed/Secret/TLS/digest/plan/result validation; direct current+history DC/pod absence survey; optimistic complete status patch; unconditional stop; later uncached read-back/hash verification. Never create in acceptance reconcile.
  - Scope: only `controllers/k8ssandra/legacy_rf_discovery_status.go`; no repository abstraction.
  - Tests: conflicts/races mutate each binding and planned target around acceptance, including seed reorder with unchanged membership; every mismatch discards result and creates nothing.
  - Acceptance evidence: ordered fake calls prove patch→stop→later read-back and no create call in the patching reconcile.
  - Depends: MVP-B-10 and POC-A-10.

- [x] **MVP-B-12 — Implement snapshot-loss and historical-location recovery policy** → modify: controllers/k8ssandra/legacy_rf_discovery_status.go
  - Requirements: missing/corrupt snapshot rediscovery only when tombstone false and direct absence across current+accepted+append-only history is complete; managed tombstone, DC/pod presence, or any unavailable read yields permanent `SnapshotConflict`; never prune location history after removal/decommission.
  - Scope: same status file, sequential after MVP-B-11.
  - Tests: pre-managed corruption rediscovery; post-managed corruption after original location leaves spec permanently blocks; status/tombstone/history monotonicity under conflict/restart.
  - Acceptance evidence: envtest/unit call traces show historical location queried even when absent from current desired topology.
  - Depends: MVP-B-11.

- [x] **MVP-B-13 — Implement named final managed-DC creation authorization** → new: controllers/k8ssandra/legacy_rf_discovery_authorization.go
  - Requirements: fresh uncached CR read; UID/marker/snapshot read-back hash/seed/current-plan validation; direct exact DC+correlated-pod survey over current+accepted+history; monotonic target-history update and read-back; immediate callback/Create using accepted seeds. Any read error blocks; `AlreadyExists` triggers a fresh full survey and is never success.
  - Scope: only `controllers/k8ssandra/legacy_rf_discovery_authorization.go`; integration beside `remoteClient.Create` waits for MVP-C-04.
  - Tests: ordered fake tests cover every race, history update conflict, restart, duplicate reconcile, AlreadyExists, and accepted-seed use.
  - Acceptance evidence: exact call-order assertions and create argument snapshot demonstrate authorization ordering.
  - Depends: MVP-B-11/12 and POC-A-03.
