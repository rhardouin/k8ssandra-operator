---
phase: release
wave: Test
todos: [REL-T-01, REL-T-02, REL-T-03, REL-T-04]
---

# Test Wave — Kubernetes Safety and Version Skew

- [ ] **REL-T-01 — Execute the packaged RollingUpdate sequencing scenario** → new: test/e2e/legacy_rf_upgrade_test.go
  - Requirements: install the last old package, create/retain legacy unmarked behavior, initiate the packaged upgrade, wait for the operator rollout to complete, and only then create a marked migration and prove it gates before DC creation.
  - Scope: only `test/e2e/legacy_rf_upgrade_test.go`; use real rendered Kustomize and Helm packages, not mocked deployment ordering.
  - Tests: execute both packaging paths where CI supports them; assert Deployment/Pod/webhook/API event timeline.
  - Acceptance evidence: timestamped resource timeline shows rollout completion before marked migration creation; manually mixed reconcilers remain explicitly unsupported.
  - Delivered evidence: the harness and rendered-package assertions implement this sequencing, but no fresh production image was available to execute the live old/new scenario.
  - Depends: REL-C-03/04/06.

- [x] **REL-T-02 — Exercise multi-cluster acceptance, restart, and race matrix** → new: test/e2e/legacy_rf_controller_test.go
  - Requirements: real API servers and remote contexts cover accepted-location reorder/removal, non-colliding addition, CAS/read-back, manager restart, concurrent reconciliation, generation/seed membership and order/credential/TLS/target mutation around acceptance and creation, direct-read outage, stale cache, AlreadyExists, and transition-only events. Add two named same-`K8ssandraCluster`-UID recovery timelines: `secret_correction` with credential and TLS variants, and `network_restoration`. Secret correction must show `Pending -> Blocked(AuthenticationRejected|TLSFailed)`, one failed Job, no managed DC, update of the same referenced Secret to a new resourceVersion, Secret-watch requeue, stale-attempt rejection, exactly one replacement Job, `Accepted` patch/read-back, then exactly one managed DC without CR recreation. Network restoration must show `Pending -> Blocked(ContactUnreachable)`, one failed Job, no managed DC, restoration of endpoint reachability only, bounded periodic retry without CR/Secret mutation, one replacement Job, `Accepted` patch/read-back, then one managed DC with the original UID. `SnapshotConflict` and `DiscoveryTooLate` are negative controls that create no retry Job or DC.
  - Scope: only `test/e2e/legacy_rf_controller_test.go`; CQL endpoint may be deterministic fake because Cassandra proof is REL-T-05.
  - Tests: table-driven subtests with bounded Eventually and no `time.Sleep`; recovery cases use fake clock/backoff, record every reconcile, assert ordered status/Event timelines, exactly one Event per transition, no duplicate identical Blocked Event, Job counts `1 -> 2`, UID stability, and DC count `0 -> 1` only after Accepted read-back.
  - Acceptance evidence: per-case resource/event/status snapshots show no managed DC before valid later read-back/authorization; two timestamped timelines prove watch-driven Secret recovery and poll-driven network recovery on the same UID while permanent conflicts remain blocked.
  - Depends: Release Wave C.

- [x] **REL-T-03 — Prove historical tombstone/location recovery boundary** → modify: test/e2e/legacy_rf_controller_test.go
  - Requirements: pre-managed corrupt snapshot plus complete direct absence triggers wholly new attempt; after managed creation, remove original location from current spec and delete/corrupt snapshot, then prove tombstone/history causes permanent block; partial/unavailable historical API survey also blocks.
  - Scope: same test file, sequential after REL-T-02.
  - Tests: restart controller between state transitions to rule out in-memory provenance.
  - Acceptance evidence: status, history, remote API reads, and Job counts prove durable history rather than current-plan-only safety.
  - Depends: REL-T-02.

- [x] **REL-T-04 — Verify qualified bindings, immutable worker digest, and redaction** → modify: test/e2e/legacy_rf_controller_test.go
  - Requirements: mutate context/namespace/name/key/resourceVersion/purpose independently; change TLS/credential Secrets; attempt tag-only/wrong digest; forge/oversize result; seed secrets/raw errors with canaries and scan status, Events, logs, ConfigMaps, and result metadata.
  - Scope: same test file, sequential after REL-T-03.
  - Tests: all binding dimensions and failure outputs have named cases.
  - Acceptance evidence: canary scan is empty and every mismatch blocks before acceptance/creation.
  - Depends: REL-T-03.
