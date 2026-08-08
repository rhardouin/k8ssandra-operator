# Architecture Challenge: Legacy System-Keyspace RF Discovery

> **Historical challenge:** the later `task_review.md` changed seed handling after this duel. Any statement below about both proposals honoring the prior first-complete/all-seed contract is superseded by authoritative `design.md` ordered fallback semantics.

## Verdict

Neither proposal is safe unchanged. Both honor the first-complete-response seed rule and avoid comparing or merging RF/topology/schema data from non-authoritative seeds, but both miss a critical mixed-version rollout invariant: a new webhook can inject the discovery marker while an old marker-unaware reconciler remains active and creates a managed `CassandraDatacenter` before snapshot read-back.

## Proposal A Findings

1. **CRITICAL — mixed-version gate bypass:** enabling leader election in the new deployment does not constrain an already-running old binary. A capability/upgrade-order gate is required before marker injection.
2. **HIGH — incomplete snapshot-loss search domain:** the proposal does not durably retain every accepted/created managed location. After a managed DC leaves current spec/status, corruption could cause an incomplete absence survey and unsafe rediscovery.
3. **HIGH — ambiguous Secret/TLS bindings:** binding keys do not fully qualify purpose, context, namespace, name, relevant keys, and resource version, so canonical hashes can alias.
4. **HIGH — vague post-acceptance compatibility:** reorder/removal/addition are described as allowed, but no executable predicate composes current generation, accepted snapshot, external-name collisions, cluster identity, and create-time checks.
5. **HIGH — accepted versus external-drift state conflict:** one phase/reason tuple cannot both keep discovery visibly `Accepted` and report post-Ready `ExternalReplicationDrift` without a separate status domain.
6. **MEDIUM — worker observer contract:** one observation plus one error does not precisely model identity-only success, authoritative candidate success, and ignorable non-authoritative candidate failure.
7. **MEDIUM — mutable worker image identity:** HMAC authenticates possession of a key, not trusted code. The exact worker image digest must be bound into the attempt and result.
8. **MEDIUM — package cohesion:** one `pkg/discovery` package owns endpoint observation, signed protocol, snapshot validation, and post-Ready replication planning; these are separate reasons to change.
9. **MEDIUM — incomplete release coverage:** explicit tests are missing for rolling version skew, first-DC reorder/removal, historical-location snapshot loss, conflicting non-authoritative seed observations, and driver endpoint pinning.
10. **LOW — wave conflicts:** proposed parallel controller tasks overlap in controller files, fixtures, and discovery tests.

## Proposal B Findings

1. **CRITICAL — mixed-version rollout declared unsupported:** normal Helm/Kustomize rolling updates create mixed versions. A concrete capability or rollout mechanism is required.
2. **HIGH — invalid schema-port construction:** a concrete `ManagementApiFacade` cannot be built at process startup; the existing lifecycle requires a factory scoped to a selected Ready DC/pod.
3. **HIGH — ambiguous remote credential reference:** reinterpreting `LocalObjectReference` in a dynamically selected remote context conflicts with Kubernetes and existing API conventions unless context/namespace/watch/RBAC semantics are explicit.
4. **HIGH — underqualified Secret bindings:** name and resource version alone omit context, namespace, purpose, relevant key names, and canonical ordering.
5. **HIGH — deferred CRD wire contract:** the proposal does not choose between a loose keyspace map and three explicit fields. Exactly three presence-preserving maps must be enforceable now.
6. **HIGH — vague current-plan predicate:** accepted targets risk becoming immutable desired topology because the design does not precisely allow reorder/removal/non-colliding additions while rejecting incompatible changes.
7. **MEDIUM — incomplete failure mapping:** infrastructure failures are not mapped to precise public reason, retryability, sanitized corrective action, and deterministic precedence.
8. **MEDIUM — misleading repository abstraction:** `SnapshotRepository` suggests a transaction that cannot include remote absence reads; the non-atomic read/survey/status-patch/read-back sequence must remain explicit.
9. **MEDIUM — speculative grouped config:** a one-child `legacySystemReplicationDiscovery` group is YAGNI without another concrete setting.
10. **MEDIUM — accepted versus drift undefined:** external drift needs a separate condition/status from durable discovery acceptance.
11. **MEDIUM — mutable worker identity:** hardened Pods and HMAC still require a digest-pinned trusted worker image.
12. **MEDIUM — incomplete release plan:** version-skew, topology mutation, historical-location recovery, and the authoritative finite matrix source are not concrete.
13. **LOW — wave ownership collisions:** snapshot/result/admission/docs tasks overlap and need contract freeze plus explicit file ownership.

## Comparative Result

Proposal A is closer to the repository's established level-based reconciliation and is the better base. Proposal B contributes useful ideas: explicit worker protocol boundaries, a narrow no-create post-Ready schema capability, fully qualified bindings, and durable managed-location provenance. Proposal B's repository abstraction, grouped API field, and seven-area hexagonal split are not justified by current scope.

## Required Synthesis Mitigations

1. Add a version-skew safety mechanism that prevents marker injection until every active reconciler is discovery-aware. Test a real old/new rolling upgrade.
2. Persist accepted discovery location plus a canonical, append-only safety search history of managed locations. This is provenance for absence surveys, not desired topology, and must never block legitimate reorder/removal/decommission.
3. Define one pure `ValidateCurrentPlan(snapshot, currentSpec)` contract: allow reorder, removal, and non-colliding additions; reject external-name collisions and incompatible expected cluster/server-type changes; retain accepted seeds regardless of later spec edits.
4. Define named final creation authorization with exact ordering: fresh control-plane read, UID/marker/hash/current-plan validation, direct remote absence reads over the full current-plus-history union, then immediate `Create`. Never create in the same reconcile that accepts the snapshot. Read errors block; `AlreadyExists` restarts the full survey.
5. Fully qualify canonical Secret bindings by context, namespace, name, purpose, relevant keys, and resource version. Keep source credential semantics explicit and do not overload a `LocalObjectReference` across clusters.
6. Keep discovery `Accepted` durable while reporting post-Ready schema drift through a separate condition/status with transition-only Events; drift blocks all operator schema DDL for that attempt.
7. Use lifecycle-accurate minimum interfaces: a per-Ready-DC schema adapter factory, explicit acceptance sequence, and separate source-protocol versus managed-replication-planning responsibilities.
8. Bind an immutable worker image digest into the attempt and signed result. HMAC protects the result channel but does not establish trusted code identity.
9. Publish a complete failure matrix covering stable reason, retryability, sanitized corrective action, and precedence for admission, Secret, TLS, CQL, Job, result, API, conflict, snapshot, and drift failures.
10. Add concrete release gates for rolling version skew, authoritative-first behavior with conflicting non-authoritative observations, endpoint pinning, accepted first-DC reorder/removal, pre-managed rediscovery, post-managed permanent block after historical-location removal, mutation races, image identity, and qualified bindings.
11. Keep the explicit boundary that unit/envtest/rendered-manifest evidence is not proof of live Cassandra bootstrap preservation, ring health, streaming, repair, or data availability.
