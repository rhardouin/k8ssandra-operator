---
phase: release
wave: Test
todos: [REL-T-05, REL-T-06, REL-T-07, REL-T-08]
---

# Test Wave — Real Cassandra Lifecycle Proof

- [ ] **REL-T-05 — Execute the three named Cassandra lifecycle scenarios** → new: test/e2e/legacy_rf_cassandra_test.go
  - Requirements: execute exactly the registered Cassandra 4.0.17, 4.1.9, and 5.0.6 legacy-RF scenarios through the repository's existing per-test CI model; no source-by-target Cartesian product and no digest prerequisite. Each scenario provisions exactly two one-node source datacenters from one shared definition (`legacy-a`, `legacy-b`), then runs the target lifecycle and verifies ordered fallback discovery, accepted snapshot, target bootstrap, and the post-Ready branch. One designated scenario must prove seed A fails, B succeeds, and C is never contacted. On one designated scenario, execute the named credential/TLS matrix `sourceAuth={off,on} x discoveryCredentialRef={absent,present-valid} x TLS={off,on}`. Source-auth-off accepts all four rows, including a valid supplied reference because Cassandra does not request authentication. Source-auth-on accepts only the two reference-present rows; absent credentials block `AuthenticationRejected` before snapshot/DC. TLS-on success uses an IP-SAN-verifiable certificate. Repeat every logical row with managed-target auth disabled and enabled and require identical source outcomes; target auth must never provide source credentials. Add TLS IP-verification mismatch controls for anonymous and credentialed source modes; both block `TLSFailed` before snapshot/DC.
  - Scope: `test/e2e/legacy_rf_cassandra_test.go`, the three exact suite registrations in `test/e2e/suite_test.go`, and their three exact GitHub Actions test-name entries; reusable fixture mechanics may live in the existing e2e framework only via a separate reviewed task if unavoidable.
  - Tests: CI executes the three named E2E tests separately with per-test timeout/artifact collection; all credential/TLS/target-auth variants assert phase/reason, snapshot/DC presence, source-only credential mounts/inputs, and query completion, with zero skipped scenarios.
  - Acceptance evidence: scenario-keyed results show three declared/executed/passed tests; fallback trace proves A-fails/B-succeeds/C-skipped; the credential/TLS report shows identical target-auth paired outcomes, source-only credential mounts, and positive/negative TLS IP-verification evidence.
  - Depends: REL-C-05 and runnable packaged image.

- [ ] **REL-T-06 — Prove heterogeneous sparse RF preservation and bootstrap boundary** → modify: test/e2e/legacy_rf_cassandra_test.go
  - Requirements: source replication is `system_auth={legacy-a:2,legacy-b:4}`, `system_traces={legacy-a:1}`, and `system_distributed={legacy-b:4}`. Verify the RF greater than 3, heterogeneous per-DC values, sparse omissions, discovery snapshot exact values, accepted ordered seeds, no worker DDL, and live before/after rows across bootstrap. The managed DC must be added at RF 1 without changing any external entry. Do not serialize discovered maps into bootstrap system-replication property. Add named `legacy_flat_annotation_is_ignored`: a marked object carries `k8ssandra.io/initial-system-replication` with a deliberately conflicting sentinel map; before worker success it neither supplies/satisfies snapshot status nor permits managed creation, and after discovery the accepted three-map snapshot comes only from live CQL, the sentinel is absent from bootstrap system-replication input, and live before/after rows match the no-annotation contract.
  - Scope: same lifecycle test file, sequential after REL-T-05.
  - Tests: run for all three named version-line scenarios where fixture topology permits; keep the annotation case on one explicitly named scenario and record annotation, Jobs, status, desired DC bootstrap properties, and CQL rows.
  - Acceptance evidence: CQL before/snapshot/after artifacts are exact per keyspace, retain RF 4, preserve heterogeneous and sparse external entries, and add only the managed RF 1 entry; annotation-present trace proves the Job remains required, no pre-acceptance DC exists, the snapshot is CQL-derived, and the sentinel never enters bootstrap input or live rows.
  - Depends: REL-T-05.

- [ ] **REL-T-07 — Prove post-Ready zero-DDL external drift and managed-only ALTER** → modify: test/e2e/legacy_rf_cassandra_test.go
  - Requirements: after Ready, introduce each external projection drift class separately; verify durable `Accepted`, false drift condition, transition-only Event, and zero operator DDL; restore external assertion, then verify only managed entries are altered; inject partial ALTER failure and verify all-three reread. Continue `legacy_flat_annotation_is_ignored` through post-Ready drift and managed-only reconciliation and prove its DDL decisions and operation trace are identical to accepted snapshot/live CQL inputs; the flat annotation never adds, removes, or changes an ALTER.
  - Scope: same lifecycle test file, sequential after REL-T-06.
  - Tests: capture audit/query or schema snapshots sufficient to distinguish zero DDL from unchanged final state.
  - Acceptance evidence: operation-level evidence shows no `CREATE`, no `EnsureKeyspaceReplication`, and no ALTER during drift, plus an annotation-present/no-annotation operation-trace comparison showing zero influence.
  - Depends: REL-T-06.

- [ ] **REL-T-08 — Record live-proof boundaries and operational artifacts** → new: test/e2e/legacy_rf_evidence.go
  - Requirements: collect scenario name, source/target versions and resolved image references, sanitized attempted-endpoint order and outcomes, accepted/skipped endpoint evidence, CQL rows, snapshot/hash, Jobs/Pods/Events/conditions, operation evidence, and test timings without secrets; explicitly label what the run does not prove: skipped-seed identity/state, authoritative liveness/ring membership, ownership, streaming completion, repair, or data availability.
  - Scope: only `test/e2e/legacy_rf_evidence.go`; deterministic filenames keyed by the three scenario names.
  - Tests: unit test redaction and required-field validation; failing run still emits bounded artifacts.
  - Acceptance evidence: one reviewed artifact bundle per lifecycle row with secret canary scan empty.
  - Depends: REL-T-05 through REL-T-07.

## Delivered lifecycle evidence

- Cassandra 4.0: the terminal replay provisioned five source datacenters, then failed before discovery on the first source `ALTER KEYSPACE`. The helper discarded `cqlsh` stderr, so the Cassandra-side cause is unknown and no 4.0 feature proof exists.
- Cassandra 4.1: the retained artifact is invalid/incomplete (`accepted endpoint must have an accepted attempt`) and has no accepted snapshot. It is not feature proof.
- Cassandra 5.0: the retained historical artifact is valid for its recorded failed/accepted/skipped attempt trace, accepted snapshot/hash, and before/after RF rows. Its explicit `doesNotProve` limits remain binding; in particular it does not prove skipped-seed identity/state, ring health or membership, ownership, streaming, repair, or data availability.

REL-T-05 through REL-T-08 remain unchecked because the complete three-version acceptance contract was not met.
