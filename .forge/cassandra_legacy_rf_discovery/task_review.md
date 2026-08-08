# Document Gap Analysis Report

## Executive Summary

Forge's task plan needs **six targeted corrections before execution**: 4 major delivery or proof gaps and 2 minor compatibility or scheduling gaps. The retained findings concern the clarified seed-fallback behavior, unowned prerequisite artifacts, automatic recovery proof, legacy-annotation compatibility, the authentication/TLS support matrix, and one invalid parallel-work declaration.

The plan is otherwise detailed and substantially traceable. Its strongest parts are the acceptance/read-back ordering, direct absence surveys, post-Ready zero-DDL drift design, redaction, and the separation of local evidence from live Cassandra proof.

This review compares the current working-tree PRD with the task dashboard and all 15 task files (56 unchecked task IDs). A PRFAQ was not supplied and is not part of the requested scope.

### Finding totals

| Severity | Count |
|----------|------:|
| S1 | 0 |
| S2 | 4 |
| S3 | 2 |
| **Total** | **6** |

## Document Inventory

| Document | Title | Status | Last Updated |
|----------|-------|--------|--------------|
| PRD | Legacy System-Keyspace Replication Discovery | Working-tree version; modified relative to Git | Not declared |
| PRFAQ | Not supplied | Out of review scope | N/A |
| Impl Plan | Legacy System-Keyspace RF Discovery Execution Graph plus 15 task files | 56 unchecked tasks; working-tree files are untracked | Not declared |

## Findings

### [S2] Seed handling is stricter than the clarified fallback requirement (ID=RF-TASK-001)

- **Dimension:** D1 Scope Alignment; D3 Technical Reality; D7 Error Handling
- **Documents:** PRD; task dashboard; PoC protocol/tests; MVP worker/tests
- **Gap:** The required behavior is to treat `additionalSeeds` as connection fallbacks: attempt seeds until one returns a complete valid discovery result, then stop. A failed seed must not block if a later seed succeeds, and seeds after the first success need not be contacted. Both the PRD and tasks are currently stricter: the PRD requires every contact point to be contacted and mutually consistent, while the tasks require identity checks for every seed and race complete candidates.
- **Evidence:** `prd.md:90`, `prd.md:99-102`, `prd.md:109`, and `prd.md:151-152` require all contact points to succeed and agree. `tasks/README.md:15` and `tasks/README.md:42` require every seed to pass identity checks. `tasks/poc/02-protocol-contracts.md:30-34`, `tasks/poc/03-contract-tests.md:9-13`, and `tasks/mvp/01-cql-worker.md:23-27` implement and test all-seed validation or candidate racing rather than fallback-until-success.
- **Recommendation:** Rewrite the discovery contract and its tests around fallback semantics. Define a stable attempt order, use a fresh endpoint-pinned session for each attempted seed, accept the first complete valid result, cancel or skip remaining attempts, and block only when no seed produces a complete result. Do not merge or compare observations across seeds.

### [S2] Safety-critical prerequisite artifacts are outside the executable task graph (ID=RF-TASK-006)

- **Dimension:** D4 Priority and Phasing; D9 Assumptions and Dependencies
- **Documents:** MVP worker task; release matrix task; task dashboard
- **Gap:** MVP implementation depends on a grounded driver artifact that does not exist in the reviewed feature directory, and the release matrix depends on an unowned support-policy approval. Neither dependency has a task ID, deliverable owner, or resume state.
- **Evidence:** `tasks/mvp/01-cql-worker.md:14` depends on `dependencies.md` driver grounding. `tasks/release/01-generated-packaging.md:42` depends on Phase 4.7 grounding and supported-release policy approval. The complete graph and dashboard at `tasks/README.md:26-36` and `tasks/README.md:65-76` contain no producing task.
- **Recommendation:** Add numbered prerequisite tasks that produce the grounded driver/API artifact and the approved support-matrix policy, then depend on those task IDs from MVP-B-01 and REL-C-05. If Forge Phase 4.7 owns them externally, link the concrete artifact and completion state in this dashboard.

### [S2] Automatic recovery is assembled from components but never proved end to end (ID=RF-TASK-009)

- **Dimension:** D5 Success Metrics; D7 Recovery; D10 User Journey
- **Documents:** PRD; attempt, safety, watch, and release test tasks
- **Gap:** Backoff, Secret watches, retry classification, and outage cases exist separately, but no named release case proves the full `Blocked -> corrected prerequisite -> retry -> Accepted -> managed DC` journey on the same CR UID.
- **Evidence:** `prd.md:37`, `prd.md:143`, `prd.md:181`, and `prd.md:209` require automatic recovery after Secret or network correction. `tasks/mvp/01-attempt-resources.md:23-27`, `tasks/mvp/02-safety-state.md:9-13`, and `tasks/mvp/03-controller-integration.md:16-20` provide pieces. `tasks/release/02-envtest-safety.md:16-20` includes mutations and outages, but its acceptance evidence establishes only that managed creation does not happen prematurely.
- **Recommendation:** Add two named release cases with status, Event, reconcile, and Job-count timelines: credential/TLS Secret correction and network restoration. Both must eventually create the managed DC without recreating the `K8ssandraCluster`. Limit the recovery promise to retryable reasons; permanent conflicts must remain blocked.

### [S3] The legacy flat replication annotation has no explicit compatibility guard (ID=RF-TASK-011)

- **Dimension:** D2 Requirement Traceability; D7 Compatibility
- **Documents:** PRD; API task; Cassandra lifecycle task
- **Gap:** The tasks create a new status snapshot and avoid writing discovered maps to the bootstrap property, but no explicit test proves that a user-supplied `k8ssandra.io/initial-system-replication` annotation cannot satisfy, bypass, or influence the marked discovery path.
- **Evidence:** `prd.md:66` says the annotation cannot hold the snapshot, and `prd.md:164` says it is not accepted as one. The new snapshot is defined by `tasks/poc/01-api-contracts.md:9-13`; `tasks/release/02-cassandra-lifecycle.md:16-20` covers discovered-map serialization but does not mention the legacy annotation.
- **Recommendation:** Add a focused compatibility test for a marked object carrying the legacy annotation. Prove that it neither satisfies discovery acceptance nor changes the accepted snapshot or subsequent bootstrap/schema behavior.

### [S2] Authenticated, anonymous, and TLS discovery lack an explicit support matrix (ID=RF-TASK-012)

- **Dimension:** D2 Requirement Traceability; D6 Security; D7 Error Handling
- **Documents:** PRD; API and CQL worker tasks; release tests; security documentation task
- **Gap:** The feature must support legacy Cassandra clusters with authentication enabled or disabled, each with TLS enabled or disabled. The credential Secret is optional, but the tasks do not define or prove the resulting 2 x 2 x 2 matrix: source authentication enabled/disabled, discovery credential Secret present/absent, and TLS enabled/disabled. They also do not explicitly prove that managed-target authentication settings cannot substitute for or alter source discovery credentials.
- **Evidence:** `prd.md:92` requires anonymous discovery when the dedicated credential reference is absent and forbids inference from target authentication. `prd.md:94-95` define TLS and endpoint-verification behavior. `tasks/poc/01-api-contracts.md:10` makes the credential reference optional. `tasks/mvp/01-cql-worker.md:9-14` names generic authentication and TLS tests, while `tasks/release/03-documentation.md:23-27` documents the security contract without a complete release proof matrix.
- **Recommendation:** Add a named 8-case matrix covering source authentication on/off, credential Secret present/absent, and TLS on/off. Valid anonymous and authenticated configurations must reach discovery; authentication enabled without credentials must fail explicitly before snapshot acceptance or managed creation. Prove TLS verification in both source-auth modes and prove that target-auth configuration never supplies source credentials.

### [S3] The dashboard declares parallel work across an explicit dependency (ID=RF-TASK-013)

- **Dimension:** D4 Priority and Phasing; D9 Dependencies
- **Documents:** Task dashboard; MVP worker and attempt tasks
- **Gap:** The dashboard says same-prefix MVP `01` files may run in parallel, but MVP-B-04 depends on MVP-B-02 in another MVP `01` file.
- **Evidence:** `tasks/README.md:24`, `tasks/README.md:30`, and `tasks/README.md:70` declare the files parallel. `tasks/mvp/01-attempt-resources.md:14` requires MVP-B-02 first.
- **Recommendation:** Split the wave or show `MVP-B-01/02 -> MVP-B-04` explicitly so dispatch cannot begin resource construction before the executable contract freezes.

## Coverage Matrix

| Dimension | PRD | PRFAQ | Impl Plan | Status |
|-----------|-----|-------|-----------|--------|
| D1: Scope | Current text requires all-seed consistency; clarified need is fallback-until-success | Not supplied | Also requires all-seed checks or racing candidates | GAP |
| D2: Traceability | Detailed FR and acceptance structure | Not supplied | Strong overall; legacy-annotation compatibility and the complete auth/TLS matrix are not explicitly proved | GAP |
| D3: Technical reality | Pre-creation CQL discovery is feasible | Not supplied | Architecture is feasible, but seed execution semantics must be rewritten | GAP |
| D4: Priority/phasing | Safety gates precede managed creation | Not supplied | External prerequisites and one cross-file dependency are unresolved | GAP |
| D5: Success metrics | Automatic recovery is required | Not supplied | Recovery components exist, but the complete recovery journey is not release-tested | GAP |
| D6: Risks/mitigations | Secret handling, TLS, drift, and concurrency risks are explicit | Not supplied | Redaction and binding controls are strong; the auth/TLS support combinations lack explicit proof | GAP |
| D7: Edge cases/errors | Detailed fail-closed behavior | Not supplied | Fallback exhaustion, recovery, legacy annotation, and auth/TLS outcomes need named tests | GAP |
| D8: Terminology | Consistent marker/snapshot/managed/external terminology | Not supplied | Terminology is consistent | OK |
| D9: Dependencies | CQL client, remote RBAC, admission, and supported images required | Not supplied | Driver grounding and support-policy approval have no executable owner; one parallel declaration violates a dependency | GAP |
| D10: User journey | Create, block, correct, accept, start, and reconcile | Not supplied | Main flow is covered; corrected-prerequisite recovery lacks end-to-end release proof | GAP |

## Verified Strengths

- All 56 task IDs are unique; every task file's `todos` front matter matches its checkbox count and order; phase totals match the dashboard.
- Direct uncached current/accepted/history absence checks fail closed on API errors.
- Snapshot status acceptance, unconditional stop, later read-back, final create-time revalidation, and `AlreadyExists` restart are explicitly separated.
- Accepted seeds remain bootstrap authority after later spec edits.
- RF values use `1..MaxInt32`, preserve sparse omission, and include RF 9/10.
- The marked post-Ready branch prevalidates all three keyspaces, uses direct `AlterKeyspace`, and requests operation-level zero-DDL evidence for external drift.
- Stable reasons, transition-only Events, redaction canaries, least-privilege attempt resources, and documentation parity are unusually well planned.
- The plan correctly distinguishes local/unit/envtest/rendered evidence from live Cassandra lifecycle proof and from unproved ring health, ownership, streaming, repair, and data availability.

## Recommendations Summary

1. Rewrite the PRD and tasks around seed fallback-until-first-complete-success semantics before implementing the worker.
2. Give driver grounding and supported-release policy concrete prerequisite tasks or linked completed artifacts.
3. Add end-to-end recovery cases for Secret correction and network restoration.
4. Add the explicit legacy-annotation compatibility guard and the full authentication/credential/TLS matrix.
5. Correct the MVP dashboard dependency so MVP-B-04 cannot start before MVP-B-02.
