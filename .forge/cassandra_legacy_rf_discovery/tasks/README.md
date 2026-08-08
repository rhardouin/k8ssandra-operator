# Legacy System-Keyspace RF Discovery Execution Graph

## Context

This brownfield feature protects new Cassandra migrations with non-empty `additionalSeeds` from the existing RF-3 normalization/create-or-alter path. A discovery-aware admission marker activates a fail-closed, pre-managed-DC workflow that reads `system_auth`, `system_traces`, and `system_distributed` from the legacy cluster, accepts one immutable status snapshot, reads it back, and only then permits managed `CassandraDatacenter` creation. Existing unmarked objects retain legacy behavior. Runtime source scope is Apache Cassandra 4.0.0 or newer; DSE, HCD, FQDN seeds, and `secretsProvider: external` are outside this release.

The observable guarantee is deliberately narrow: the worker issues only CQL `SELECT`; the marked post-Ready path issues zero DDL unless all three external projections match the accepted snapshot. Local source, unit, envtest, rendered-manifest, and generated-schema evidence does **not** prove live bootstrap preservation, ring membership/health, ownership, streaming, repair, or data availability.

## Domain Model

- **Discovery marker:** immutable server-owned version marker injected only on CREATE; it prevents retroactive activation.
- **Canonical seed list:** validated IP literals normalized in spec order, deduplicated by first occurrence, and protected by an order-sensitive immutable digest; accepted seeds remain bootstrap authority after later spec edits.
- **Qualified Secret binding:** purpose, source context, namespace, name, relevant keys, and resource version; bytes never enter status/results.
- **Discovery attempt:** bounded remote Job plus least-privilege support resources, immutable worker digest, and signed result envelope.
- **Authoritative observation:** seeds are attempted lazily in normalized spec order with fresh endpoint-pinned sessions; earlier failures fall back, the first complete valid response alone supplies source/topology/schema/RF data, and later seeds are skipped.
- **Accepted snapshot:** immutable, canonical, hash-bound status record with three presence-preserving RF maps and accepted/historical locations.
- **Managed-creation provenance:** monotonic tombstone and append-only location history used only as an authoritative absence-search domain, never desired topology.
- **Schema drift condition:** separate from durable discovery `Accepted`; any external mismatch causes transition-only evidence and zero operator DDL.

## Architecture

The plan follows the repository's level-based controller pipeline. `pkg/discovery` owns pure source protocol/canonicalization and the worker; `controllers/k8ssandra` owns qualification, Kubernetes attempt lifecycle, explicit status acceptance, current-plan compatibility, creation authorization, and post-Ready replication planning. `ClientCache` gains direct remote readers for authoritative absence checks. Shared controller/composition/generated edits are deferred to sequential Wave C files.

Execution order is numeric. Files with the same prefix may run in parallel and have non-overlapping primary ownership. Within a file, checkboxes execute top-to-bottom unless a task's `Depends` says otherwise.

```text
poc/01 contracts
  -> poc/02 canonical protocol || poc/02 direct readers
  -> poc/03 contract verification
  -> mvp/01 worker || mvp/01 admission
  -> mvp/02 attempts || mvp/02 post-Ready schema
  -> mvp/03 safety state
  -> mvp/04 sequential controller integration
  -> release/01 generated and packaged delivery
  -> release/02 envtest || release/02 Cassandra lifecycle
  -> release/03 documentation
  -> release/04 final verification
```

## Contracts

- Marker injection and validation fail closed. Packaged Kustomize and Helm deployments retain `RollingUpdate`; operators must wait for the rollout to complete before creating or modifying a legacy-RF migration resource.
- Seeds are attempted sequentially in normalized spec order through fresh endpoint-pinned sessions. Failures fall back; the first complete valid response is authoritative and later seeds are never contacted. Discovery blocks only after deterministic exhaustion, and endpoint observations are never merged or compared.
- RF text is ASCII base-10 `1..MaxInt32`; RF 9/10 and sparse keyspace-specific omissions are preserved. All three NTS keyspace rows are mandatory.
- Acceptance orders uncached CR read, binding/plan validation, direct current-plus-history absence survey, optimistic full snapshot status patch, unconditional stop, then later uncached hash read-back.
- Final creation authorization orders fresh CR read, UID/marker/hash/seed/current-plan validation, direct absence survey over current+accepted+history, monotonic history update/read-back, then immediate `Create`. `AlreadyExists` restarts the full survey.
- Snapshot loss can rediscover only when tombstone is false and authoritative current+historical absence is complete. After managed creation or ambiguous reads, it permanently blocks.
- The marked post-Ready branch reads/prevalidates all three keyspaces before any DDL, preserves the external projection exactly, and uses direct `AlterKeyspace` only for changed managed entries. It never calls a create-capable helper.
- Failure output is deterministic, sanitized, transition-driven, and complete across admission, Kubernetes, Job, Secret/TLS, ordered CQL fallback/exhaustion, protocol, acceptance, snapshot, creation, and drift boundaries.

## External Prerequisite Gate

This gate is an executable graph node but is not an implementation checkbox: Forge state sync must not dispatch an implementer for orchestrator-owned grounding.

| Gate | Owner | Artifact and completion state | Resume behavior | Blocks |
|---|---|---|---|---|
| `PRE-G-01 — Ground feature dependencies` | Forge orchestrator, Phase 4.7 `forge-grounder` | `.forge/cassandra_legacy_rf_discovery/dependencies.md`; complete only when present, first line is not `HALT`, the chosen Go CQL driver/version is pinned, endpoint-pinning/auth/TLS APIs are documented, and required `go.mod` changes are recorded | Missing or first-line `HALT` reruns Phase 4.7; a valid artifact is reused | all Phase 5 work that consumes dependencies, explicitly `MVP-B-01` |

## Documentation Plan

Audience: SREs running legacy Cassandra migrations, platform engineers packaging/upgrading the operator, and responders diagnosing blocked discovery.

Existing documentation targets only:

- `docs/content/en/tasks/migrate/_index.md`: qualification/non-retroactivity, Cassandra 4.0+ scope, IP-only seeds and TCP 9042, credential/TLS setup, stable ordered fallback, earlier failure/later success, skipped remainder, deterministic exhaustion, immutable accepted seeds/snapshot, apply/observe/recover workflow, external drift behavior, and RF `2/9/2/10/10` example.
- `docs/content/en/tasks/troubleshoot/_index.md`: finite reason matrix, retryability/corrective actions, status/condition/Event inspection, pre-managed rediscovery versus post-managed permanent block, Job/image/API failures, and redaction-safe evidence collection.
- `docs/content/en/tasks/secure/encryption/_index.md`: dedicated source credentials, fully qualified Secret/TLS bindings, endpoint certificate verification, authentication-before-identity warning, least privilege, unsupported external provider, and no literal secrets.
- `docs/content/en/reference/crd/k8ssandra-operator-crds-latest/_index.md`: regenerate current API reference only; historical release snapshots remain untouched.

The migration page must state that `additionalSeeds` provide bootstrap/connectivity, while discovery snapshots preserve the three external replication assertions; neither is live ring/repair proof. It must also state the `RollingUpdate` rollout-complete requirement and that manually mixed reconcilers are unsupported. Examples may be finalized only after implemented field/reason names are verified.

## Wave-Grouped Task Dashboard

| Phase | Prefix / wave | Parallel groups | Tasks | Dependency |
|---|---|---|---:|---|
| PoC | `01` / A | `01-api-contracts.md` | 4 | none |
| PoC | `02` / A | `02-protocol-contracts.md`, `02-direct-reader-contract.md` | 6 | PoC 01 |
| PoC | `03` / Test | `03-contract-tests.md` | 3 | PoC 02 |
| MVP | `01` / B | `01-cql-worker.md`, `01-admission.md` | 6 | PoC complete and `PRE-G-01` for the worker |
| MVP | `02` / B | `02-attempt-resources.md`, `02-schema-reconciliation.md` | 7 | MVP 01; attempts require `MVP-B-02` |
| MVP | `03` / B | `03-safety-state.md` | 4 | MVP 02 |
| MVP | `04` / C | `04-controller-integration.md` | 5 | MVP 03; sequential shared edits |
| Release | `01` / C | `01-generated-packaging.md` | 6 | MVP complete; sequential generated/shared edits |
| Release | `02` / Test | `02-envtest-safety.md`, `02-cassandra-lifecycle.md` | 8 | Release 01 |
| Release | `03` / Docs | `03-documentation.md` | 4 | implemented behavior + Release 02 evidence |
| Release | `04` / Test | `04-verification.md` | 3 | all prior work |

Totals: PoC 13, MVP 22, Release 21; 56 implementation checkboxes plus 1 external dependency-grounding gate, for 57 execution-graph nodes overall. No task file exceeds 10 tasks, so there is no mandatory hierarchical-delegation candidate. Release lifecycle proof follows the repository's existing three independently selected E2E tests for Cassandra 4.0.17, 4.1.9, and 5.0.6; it is not a source-by-target matrix.

## Resume and Completion Rules

The `todos` front matter in each task file maps 1:1, in order, to its markdown checkboxes. A checkbox is resume ground truth only after its named acceptance evidence exists. External gate state comes only from the grounding artifact above. Do not mark generated, manifest, documentation, or lifecycle tasks complete from source inspection alone. Preserve unrelated working-tree changes and never regenerate historical CRD documentation.

Current delivery evidence and its proof boundaries are recorded in `delivery_evidence.md`.
