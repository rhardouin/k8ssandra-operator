# Analysis: Legacy System-Keyspace Replication Discovery

## Overview

This brownfield feature adds a fail-closed, pre-creation discovery gate for new discovery-aware Apache Cassandra migrations. For a newly admitted `K8ssandraCluster` with non-empty IP-literal `additionalSeeds`, K8ssandra must read the legacy cluster through CQL from the Kubernetes data plane of the first operator-owned Cassandra datacenter in current spec order. Seeds are attempted lazily in normalized spec order with one fresh endpoint-pinned session at a time; the first complete valid response supplies the authoritative identity, topology, schema, version, and replication snapshot, and later seeds are never contacted. Earlier failures do not block while a later fallback succeeds. The persisted snapshot preserves three independent and potentially sparse `NetworkTopologyStrategy` maps for `system_auth`, `system_traces`, and `system_distributed`; RF values from 1 through `2147483647`, including 9 and 10, are valid. After startup, the snapshot is an assertion about external state: external drift blocks all operator schema DDL and is never reverted.

Validation result: **PASS — NO CRITICAL BUSINESS QUESTIONS REMAIN**. The PRD contains five stories, nineteen explicit acceptance criteria, a specified language/version, and no direct contradiction with the supplied tech stack or brownfield conventions after applying the resolved decisions below. Remaining choices are bounded architecture/implementation details.

### Resolved decisions and product boundaries

1. **Discovery location:** The first operator-owned Cassandra datacenter in current spec order hosts discovery. Before the first managed DC exists, generation changes re-resolve that location from current spec. The selected location is provenance, not an immutable topology constraint; after acceptance, managed DC reorder, decommission, or removal remains allowed unless a current managed name collides with an observed external name or another snapshot invariant fails.
2. **Seed authority:** Normalize IPs, deduplicate by first occurrence, preserve spec order, and hash that order. Attempt one seed at a time with a fresh endpoint-pinned session. The first complete valid response is authoritative for the entire snapshot; close failed sessions, accept immediately on success, and never contact later seeds. Wrong-cluster, reachability, auth, TLS, version, topology, schema, or replication failure on an earlier endpoint is relevant only if all fallbacks exhaust. Observations are never merged or compared. Cassandra, not K8ssandra, owns cross-node convergence.
3. **Snapshot recovery:** A missing, malformed, corrupt, or hash-invalid snapshot triggers a new full discovery only while no managed `CassandraDatacenter` exists in any planned/current managed context. Once any managed DC exists, the same condition blocks permanently for that object UID; rediscovery is forbidden.
4. **Compatibility:** Runtime admission supports Apache Cassandra 4.0 or newer. Release proof follows this repository's existing convention: one named E2E scenario for each supported Cassandra line, using the representative patch versions already exercised by CI (`4.0.17`, `4.1.9`, and `5.0.6`). This three-scenario suite does not narrow runtime admission and is not a Cartesian source-by-target matrix.
5. **Authoritative Kubernetes absence:** Fresh CQL always supplies RF data. The final Kubernetes DC-absence check must be authoritative using or minimally extending the existing `ClientCache`; the precise cached/direct-client mechanism is an internal architecture decision, not a product choice.

### Important non-blocking requirements to settle in design

- Define the public field name and Secret key contract for the dedicated discovery credential reference, including missing-key behavior and same-namespace/cross-namespace restrictions.
- Define canonical IP normalization and duplicate handling, especially IPv4-mapped IPv6 forms; preserve first occurrence/spec order and make the digest and propagated bootstrap list byte-for-byte deterministic and order-sensitive.
- Define the internal validation path for post-acceptance `spec.externalDatacenters` edits while preserving the resolved rule that normal managed-DC topology edits remain allowed and current managed names may not collide with observed external names.
- Define discovery Job/result retention, cleanup on deletion, maximum result size, execution/query timeouts, retry ceilings, and backoff bounds.
- Define the stable reason precedence used only after every ordered endpoint attempt fails so status and tests are deterministic.
- Define the trust mechanism for accepting a result resource. UID/generation/hash bindings detect staleness but do not by themselves prevent another principal with write access in the data-plane namespace from forging a result.
- Define whether release-version compatibility accepts only Apache semantic versions or also vendor-suffixed version strings; DSE/HCD remain explicitly unsupported.
- Define the exact required CQL permissions so operators can provision least-privilege legacy credentials.

Happy-path scenarios counted for the edge-case gate: **9** (admission qualification, anonymous discovery, authenticated discovery, TLS discovery, ordered fallback to first complete valid seed, snapshot acceptance/read-back, first DC creation, managed-only ALTER, transient recovery). Required minimum edge cases: **18**. Cataloged below: **38**.

## Stories

### US-1 — Preserve observed replication

- **Role:** SRE
- **Goal:** Discover legacy replication independently for `system_auth`, `system_traces`, and `system_distributed` through `additionalSeeds`.
- **Benefit:** Existing sparse and heterogeneous RF values, including 2, 9, and 10, are not replaced by RF 3 or normalized across keyspaces.
- **Acceptance:** Given a newly marked Apache Cassandra migration with valid reachable seeds and stable supported source state, when discovery completes, then the accepted snapshot stores each keyspace's exact presence-preserving map, emits no DDL, and never routes discovered RFs through the bootstrap-property parser. Covered by AC-001, AC-003, AC-004, AC-006, AC-007.

### US-2 — Fail before managed creation

- **Role:** SRE
- **Goal:** Stop migration before any managed Cassandra resource exists when legacy state is unavailable, inconsistent, unsupported, or ambiguous.
- **Benefit:** K8ssandra cannot mutate or bootstrap against an untrusted source view.
- **Acceptance:** Given a qualifying migration where every ordered endpoint attempt fails contact, auth, TLS, expected-cluster identity, version, schema/topology/keyspace/strategy/replication, or timing validation, when reconciliation exhausts the list, then it creates only discovery support resources, reports deterministic `Blocked`, creates no managed DC/pod, and issues no schema DDL. Any earlier failure is ignored when a later endpoint succeeds. Covered by AC-002, AC-005, AC-006, AC-008, AC-012, AC-015, AC-018, and AC-019.

### US-3 — Keep one durable snapshot

- **Role:** Platform engineer
- **Goal:** Persist one immutable, controller-owned valid snapshot, with replacement allowed only to recover missing/corrupt state before any managed DC exists.
- **Benefit:** Retries, operator restarts, and post-acceptance seed edits cannot replace or normalize a valid accepted snapshot; recovery from snapshot loss is constrained to the pre-managed-DC phase.
- **Acceptance:** Given a valid discovery result bound to current authoritative inputs and no managed DC, when status is optimistically written and read back, then later reconciliation uses the identical valid snapshot and accepted seed set; concurrent or stale attempts cannot create a DC. A missing/corrupt snapshot permits full rediscovery only while no managed DC exists. Covered by AC-002, AC-009, AC-015 plus the resolved snapshot-recovery decision.

### US-4 — Diagnose and recover safely

- **Role:** SRE
- **Goal:** Receive a stable, sanitized reason and corrective action for a blocked migration.
- **Benefit:** Corrected network, Secret, TLS, or source prerequisites recover automatically without recreating the cluster object or leaking secrets.
- **Acceptance:** Given a blocked transient prerequisite, when its referenced Secret changes or periodic retry observes recovery, then full discovery retries with bounded backoff and can proceed; status, Events, logs, and results expose no secret material. Covered by AC-005, AC-013, AC-014, AC-017.

### US-5 — Avoid retroactive activation

- **Role:** Platform engineer
- **Goal:** Apply discovery only to objects created through the new fail-closed admission path.
- **Benefit:** Operator upgrades do not unexpectedly gate or rediscover in-progress clusters.
- **Acceptance:** Given an old unmarked object, an admission outage, a forged/mutated marker, or a marked object already owning a managed DC without a snapshot, when admission or reconciliation evaluates it, then old objects retain legacy behavior, new creation fails during admission outage, marker tampering is rejected, and too-late marked objects block without rediscovery. Covered by AC-012, AC-016, AC-018.

## Tech Stack

- **Language:** Go 1.26.3.
- **Framework/runtime:** controller-runtime 0.23.3; Kubernetes libraries 0.35.5; Kubebuilder CRDs and mutating/validating webhooks; Kubernetes Jobs/ConfigMaps or an equivalently bounded controller-owned result mechanism; Helm and Kustomize packaging.
- **Testing:** Go `testing`; Testify effectively pinned by `replace` to 1.10.0; multi-cluster envtest; Kind-based end-to-end and real Cassandra lifecycle tests.
- **Patterns:** level-based reconciliation; sequential `ReconcileResult` pipeline; manual constructor/struct dependency injection; domain-facing interfaces of at most five methods; optimistic status writes; generated CRD/RBAC/deep-copy artifacts; watched-by labels for cross-context requeues; task-oriented Hugo/Docsy documentation.
- **Brownfield constraints:** gate before existing secret/Reaper/Medusa/datacenter creation; use a separate pre-Ready CQL boundary rather than `ManagementApiFacade`; preserve the unmarked legacy path; never flatten discovered maps into `initial-system-replication` or bootstrap configuration; use direct post-Ready `AlterKeyspace`, never `EnsureKeyspaceReplication`; do not add global mutable state; synchronize Kustomize and Helm webhook/RBAC artifacts.
- **Dependency-grounding constraint:** the repository has no existing CQL session abstraction. Any new CQL driver or executable/runtime dependency must be pinned and API-grounded in Phase 4.7; no version can be inferred here.
- **Contradiction check:** none found between the corrected authoritative PRD, resolved decisions, and supplied stack. Full consistency/fingerprint validation applies within each candidate endpoint attempt; failed attempts are closed, later fallbacks may succeed, and skipped endpoints are unobserved. The absence of a current pre-Ready CQL client, an authoritative remote absence-read path, enabled packaged leader election, and typed mutating webhook are implementation gaps, not contradictions.

## Domain Model

1. **Migration (`K8ssandraCluster`)** — Aggregate root holding desired Cassandra migration configuration, UID/generation, marker, and controller-owned discovery status.
   - Operations: qualify, validate mutation, resolve planned datacenters, transition discovery status, consume accepted snapshot.
2. **Discovery Version Marker** — Server-owned immutable version token proving creation through the discovery-aware admission path.
   - Operations: inject on create, reject forgery, validate immutability, choose compatible reconciliation behavior.
3. **Planned Managed Datacenter** — Desired Cassandra target with name, Kubernetes context, and namespace; the first operator-owned member in current spec order hosts pre-creation discovery.
   - Operations: re-resolve current first location before managed creation, record provenance, check name collisions, check authoritative absence, create only after snapshot read-back, allow later normal reorder/decommission/removal.
4. **Canonical Contact-Point List** — Deduplicated deterministic ordered list of IP-literal seed endpoints on port 9042 plus its order-sensitive digest.
   - Operations: parse, normalize, retain first occurrence/spec order, digest, attempt lazily, propagate the accepted list to bootstrap.
5. **Discovery Credential** — Optional reference to a pre-existing Secret containing legacy CQL username/password, independent of target auth.
   - Operations: resolve, bind resource version, mount/read without returning data, trigger retry on change.
6. **TLS Material** — Referenced trust and optional client identity Secrets used for authenticated, endpoint-verified CQL transport.
   - Operations: resolve, bind resource versions, configure trust/client identity, verify contacted IP.
7. **Discovery Attempt** — One bounded execution tied to current migration inputs and workload identity; it attempts endpoints sequentially until the first complete valid response and stops immediately.
   - Operations: schedule, open one pinned session, validate a complete candidate, close on failure and continue, accept on success, skip the remainder, publish sanitized result, discard if stale, retry after exhaustion where permitted.
8. **Observable Cluster Identity** — Expected cluster name, source release version, partitioner, schema version, and host identities taken solely from the accepted endpoint's complete internally stable response.
   - Operations: read one candidate state before/after, fingerprint, reject instability, continue to the next fallback on failure.
9. **Observable Topology** — Exact case-sensitive mappings among addresses, host IDs, and external datacenter names visible in the authoritative response's `system.local` and `system.peers_v2` view.
   - Operations: fingerprint before/after, detect internal conflicts, derive observed external set, detect current managed-name collision; never union or reconcile topology across seeds.
10. **System Keyspace Replication Map** — Presence-preserving `NetworkTopologyStrategy` map for exactly one supported system keyspace.
    - Operations: read, canonicalize class alias, parse RF, preserve omission, compare external projection, merge managed projection.
11. **Accepted Snapshot** — Immutable status record containing all accepted bindings, identities, topology/schema fingerprints, source version, three separate maps, external set, accepted time, and canonical hash.
    - Operations: validate result bindings, optimistic accept, read back, hash-verify, compare external live state.
12. **Discovery Status** — User-visible phase (`NotRequired`, `Pending`, `Blocked`, `Accepted`), observed generation, stable reason/message, transition time, and optional snapshot hash.
    - Operations: transition, sanitize, emit transition-only Event, preserve Accepted visibility.
13. **Managed Replication Reconciliation** — Post-Ready operation that prevalidates all three keyspaces and changes only managed-datacenter entries.
    - Operations: read all, validate all, compute managed-only deltas, direct ALTER, retry after partial success.

## Relationships

- One Migration has exactly one Discovery Version Marker or is unmarked; an unmarked migration never gains one on update.
- One Migration has one or more Planned Managed Datacenters; the first operator-owned datacenter in current spec order hosts each pre-creation Discovery Attempt. That provenance does not prevent later managed-DC reorder, decommission, or removal.
- One Migration supplies one mutable ordered spec seed list before acceptance; one Accepted Snapshot owns exactly one immutable ordered Canonical Contact-Point List afterward.
- One Discovery Attempt references zero or one Discovery Credential and zero or more TLS Secrets; each accepted result binds their exact resource versions.
- One Discovery Attempt contacts endpoints sequentially until the first complete valid response. Earlier failures contribute only sanitized exhaustion evidence; later endpoints are not contacted.
- One accepted Observable Topology contains one or more observed External Datacenters; each address and host ID maps to exactly one datacenter.
- One Accepted Snapshot contains exactly three System Keyspace Replication Maps, one per supported keyspace. Each map has zero or more exact datacenter-to-RF entries, but the keyspace row itself must exist.
- One Migration has zero or one valid Accepted Snapshot at a time and exactly one current Discovery Status. A replacement snapshot is allowed only after the prior snapshot is missing/corrupt and authoritative checks prove no managed DC exists.
- One Accepted Snapshot authorizes creation of one or more planned managed datacenters only after authoritative read-back and revalidation; it does not own external datacenters.
- One Accepted Snapshot is consumed by many Managed Replication Reconciliation attempts. An attempt may issue zero to three direct ALTERs, but only after all three live maps pass external prevalidation.
- Discovery Credential and TLS Secret changes can enqueue many migration reconciliations; they never mutate an already accepted snapshot.

## External Dependencies

1. **Control-plane Kubernetes API server** — Stores `K8ssandraCluster`, admission mutations/validation, status snapshot, Events, and authoritative UID/generation/resourceVersion. Failures must block; optimistic concurrency is mandatory.
2. **Data-plane Kubernetes API server for each planned/current context** — Creates/observes the short-lived discovery workload/result and checks for managed DC/pod existence. The final absence read must be authoritative through an existing or minimally extended `ClientCache` path; this is an internal implementation requirement, not an unresolved product question.
3. **Kubernetes admission infrastructure** — Invokes fail-closed mutating and validating webhooks. An unavailable webhook must reject new object creation rather than admit an unmarked object.
4. **Legacy Apache Cassandra CQL native transport (TCP 9042)** — Supplies one complete candidate per attempted configured IP. Endpoints are tried in ordered fallback sequence with bounded fresh sessions; the first complete valid response supplies authoritative release version, partitioner, schema, topology, and keyspace replication, and later IPs are skipped.
5. **Managed Cassandra pod Management API** — Existing post-Ready boundary for live `GetKeyspaceReplication` and direct `AlterKeyspace`; it is not usable for discovery and must not expose create-or-alter operations to the discovery-aware branch.
6. **Kubernetes Secret storage/watch API** — Supplies dedicated discovery credentials and TLS trust/client identity material. Secret values are untrusted sensitive input; only resource versions enter results/snapshots.
7. **Discovery workload container image/runtime and registry** — Executes the read-only CQL client in the data plane. Image identity, pull policy/secrets, command, non-root/read-only security context, and availability are operational dependencies.
8. **Controller clock and random/jitter source** — Supplies acceptance/transition timestamps and bounded retry scheduling. It must be injected/faked for deterministic tests.

External library dependencies already present are controller-runtime 0.23.3, Kubernetes libraries 0.35.5, Kubebuilder generation, and Testify 1.10.0. A CQL driver is unresolved and must be grounded separately. No database other than the legacy/managed Cassandra clusters, no cloud-specific API, no DNS dependency for seeds, and no event bus are in scope.

## Invariants

### Migration

- UID and managed-creation state define the replacement boundary: normal updates never replace a valid snapshot; missing/corrupt state may be rediscovered for the same UID only while no managed DC exists, and deletion/recreation starts a new lifetime.
- Before accepted snapshot read-back, no managed `CassandraDatacenter`, Cassandra pod, or non-discovery support resource may be created.
- Unmarked pre-feature objects retain legacy behavior and are never marked retroactively.

### Discovery Version Marker

- The marker is injected only by create admission and is neither client-forgeable nor mutable/removable afterward.
- Every reconciler capable of processing a marked object understands the marker version and enforces the pre-creation gate.

### Planned Managed Datacenter

- Planned names are exact and case-sensitive and cannot equal any observed external datacenter name.
- Creation is allowed only after an authoritative check confirms all planned managed DCs are absent and the accepted snapshot still matches current immutable bindings.
- Before first managed creation, discovery location always follows the first operator-owned Cassandra DC in current spec order; after acceptance, this recorded provenance does not prohibit reorder, decommission, or removal.

### Canonical Contact-Point Set

- Every member is a valid IPv4 or IPv6 literal contacted on port 9042; FQDNs, empty values, and silently dropped values are invalid.
- Formatting, first-occurrence duplicate handling, spec ordering, order-sensitive digesting, and later EndpointSlice/bootstrap propagation are deterministic.
- While its snapshot remains valid, the accepted ordered list is immutable and overrides later spec edits for bootstrap; an allowed pre-managed-DC recovery attempt derives a new list from current spec.

### Discovery Credential

- Source credentials come only from the explicit optional reference; target authentication settings never select, generate, or alter them.
- Credential bytes never appear in result resources, snapshots, status, Events, or logs; only the Secret resource version is persisted.

### TLS Material

- Server identity is verified against the contacted IP; certificate/hostname verification cannot be disabled by discovery.
- Secret bytes, private keys, certificates, and raw TLS errors containing sensitive payloads never leave the workload boundary.

### Discovery Attempt

- An attempt is read-only toward Cassandra, opens at most one endpoint-pinned session at a time, and closes every attempted session on all success/failure paths.
- Its result is accepted only if UID, generation, marker version, seed digest, credential/TLS resource versions, and planned-DC absence still match authoritative state.
- The first configured seed in normalized spec order to return a complete valid response supplies all snapshot data. No partial response survives a before/after fingerprint change or stale binding; an invalid candidate falls back.
- Earlier attempted endpoints may fail; later endpoints are skipped after success. No endpoint observations are compared or merged.

### Observable Cluster Identity

- A complete candidate must report the exact expected cluster name. A null/different name fails that attempt and a later fallback may succeed.
- The accepted response reports one partitioner, one non-null schema version, and a supported Apache Cassandra version of 4.0 or newer; skipped endpoints are unobserved.
- Host IDs are non-null and globally unique within the accepted observable view.
- Before and after authoritative identity/schema fingerprints are identical for the attempt.

### Observable Topology

- Within the authoritative response, an address or host ID never maps to conflicting datacenter names; all identifiers are compared case-sensitively.
- Every datacenter referenced by a replication map exists in accepted observable topology, and every observed datacenter is external at acceptance.
- The topology is explicitly only the stable authoritative seed's observable view; it is never a cross-seed union and never asserts liveness, convergence, or complete ring membership.

### System Keyspace Replication Map

- Exactly the three named keyspaces must exist and use a supported short or fully qualified `NetworkTopologyStrategy` class.
- Every present RF is a base-10 integer in `[1, 2147483647]`; no RF-5 cap, cross-keyspace normalization, or topology-size normalization is allowed.
- Absence remains absence. An omitted datacenter is never synthesized as RF 0, RF 3, or another default.

### Accepted Snapshot

- At most one valid canonical snapshot exists at a time; its canonical hash covers every semantically relevant persisted field. Replacement requires a missing/corrupt prior snapshot and authoritative proof that no managed DC exists.
- Acceptance is an optimistic status write followed by API read-back and a reconciliation stop; DC creation cannot occur in the same unchecked continuation.
- The snapshot is an assertion for comparison, never desired external state to enforce.
- Missing/corrupt snapshot plus zero managed DCs triggers full rediscovery; the same condition plus any managed DC blocks and can never trigger rediscovery.

### Discovery Status

- Phase is always one of `NotRequired`, `Pending`, `Blocked`, or `Accepted`; `Accepted` remains visible while the snapshot is valid.
- Reason/message are stable and sanitized, and transition time/Event emission changes only for a real defined state transition.
- `observedGeneration` identifies the spec generation evaluated for the displayed state.

### Managed Replication Reconciliation

- All three live keyspaces are read and externally prevalidated before the first DDL statement of every attempt.
- Any missing keyspace, changed strategy, or external presence/value drift results in zero DDL for that attempt.
- Only managed projections may change, through direct `AlterKeyspace`; `EnsureKeyspaceReplication`, create-if-missing, and compensation of external state are forbidden.
- After partial managed-only ALTER success, the next retry rereads and prevalidates all three keyspaces from scratch.

## Edge Cases

1. `spec.cassandra` is nil or structurally incomplete when qualification runs.
2. `additionalSeeds` is nil, empty, contains an empty string, whitespace, a hostname/FQDN, an IP with a port, or an IPv6 zone identifier.
3. Duplicate textual IPs canonicalize to one address; IPv4 and IPv4-mapped IPv6 representations collide deterministically.
4. One of several contact points is unreachable, times out, resets during query, or succeeds only intermittently.
5. One seed points to a different cluster: that attempt fails and closes; a later complete valid seed may still be accepted.
6. Anonymous discovery is attempted against a source requiring auth; supplied credentials are rejected or lack system-table permissions.
7. The credential Secret is absent, in the wrong namespace, missing username/password keys, has empty values, is replaced, or changes resourceVersion mid-attempt.
8. `secretsProvider: external` is configured on a qualifying object.
9. A TLS Secret is absent, malformed, expired, untrusted, has a mismatched client keypair, or changes mid-attempt.
10. The certificate is valid for a DNS name but not the contacted IP, or an operator attempts to disable hostname/IP verification.
11. An attempted endpoint reports a null/empty cluster name, partitioner, host ID, schema version, datacenter, or release version; that candidate fails and the next fallback may succeed.
12. A later endpoint would report different partitioner, schema version, source version, topology, or RF maps, but an earlier complete valid response succeeds; the later endpoint is never contacted and K8ssandra makes no cross-seed claim.
13. Within the authoritative response, two addresses claim the same host ID with conflicting datacenters, or one address maps to multiple host IDs/datacenters.
14. One seed returns an incomplete/invalid `system.local` + `system.peers_v2` response while a later seed returns a complete valid response; only the first complete successful response becomes authoritative and no union is formed.
15. Authoritative topology changes between the before and after reads while replication rows remain unchanged.
16. Authoritative schema changes between the before and after reads while topology remains unchanged.
17. One of the authoritative response's three keyspace rows is missing, duplicated/conflicting within that response, null, or unreadable.
18. Strategy class is `SimpleStrategy`, `LocalStrategy`, an unsupported custom class, misspelled, differently cased, or a supported fully qualified alias.
19. RF is absent, null, empty, signed, decimal, whitespace-padded, non-decimal, zero, negative, `2147483647`, or overflows `2147483647`/host `int`.
20. A keyspace omits a known external DC while the other two include it; omission must survive exactly.
21. Replication names a DC not in observable topology, or differs only by case from an observed/planned DC.
22. An observed external DC exactly collides with a planned managed DC, including after planned names change post-acceptance.
23. User-supplied `externalDatacenters` is absent, contains duplicates, is a strict subset/superset, or differs only by case from the observed set.
24. The discovery workload cannot be scheduled, image pull fails, service account/RBAC is denied, it is evicted, or it exceeds its deadline.
25. A result is missing, truncated, oversized, malformed, contains unknown fields, contains secret material, or has a non-canonical hash.
26. A malicious or stale principal writes a plausible result with correct visible bindings but without executing the trusted workload.
27. Migration UID, generation, marker, seeds, credential Secret, TLS Secret, or planned DC set changes between workload creation and result acceptance.
28. Two controller replicas race attempts over the same ordered seed list; optimistic concurrency allows only one valid snapshot to win, while each attempt independently preserves lazy ordered fallback.
29. A managed DC/pod appears in any planned context between the initial absence check and status acceptance.
30. A managed DC appears between snapshot read-back and create; create-time absence revalidation must stop duplicate/unsafe creation.
31. The operator crashes immediately before status write, after status write but before read-back, or after read-back but before DC creation.
32. An accepted snapshot is erased, partially patched, corrupted, or has a hash mismatch: zero managed DCs triggers full rediscovery; any managed DC causes a permanent block for that UID.
33. `additionalSeeds` is reordered, changed, emptied, or removed after acceptance; accepted canonical seeds remain bootstrap inputs.
34. Expected cluster name, server type, planned managed names, or user-supplied external set changes after acceptance; ordinary managed-DC reorder/decommission/removal remains allowed, but current managed-name collision blocks.
35. External replication drifts in one keyspace while another managed entry needs updating; zero DDL is issued.
36. One managed-only ALTER succeeds and the second fails; retry observes both that successful managed change and unchanged external projections before continuing.
37. A supported keyspace disappears after acceptance; no create-or-alter helper is invoked.
38. Admission is unavailable, a client submits the reserved marker, or an update mutates/removes it; creation/update fails closed as applicable.

## Security Boundaries

1. **Kubernetes API client to admission webhook:** The entire `K8ssandraCluster` object is untrusted. Validate reserved-marker ownership, IP literals, Secret references, server/provider compatibility, accepted-snapshot mutations, and cross-field constraints. Mutating and validating configurations must use `failurePolicy: Fail`.
2. **Controller to control-plane API/status:** Status can be stale or tampered with by a sufficiently privileged actor. Re-read UID/generation/resourceVersion, verify canonical snapshot hash and immutability, use optimistic locking, and never trust the generic raw `.status.error` path for driver failures.
3. **Controller to remote data-plane API:** Context/namespace resolution and remote cache data cross a cluster boundary. Use least-privilege credentials, authoritative reads for race-sensitive absence, explicit timeouts, and labels plus cryptographic/canonical bindings because cross-cluster owner references are invalid.
4. **Discovery workload scheduling boundary:** Pod spec, image, command, service account, pull secrets, mounted Secrets, and result channel are security-sensitive. Use an immutable/pinned trusted image, non-root UID, read-only root filesystem, dropped capabilities, no unnecessary token mount, resource/deadline limits, and minimal Job/ConfigMap/Secret RBAC.
5. **Secret API/mount to discovery process:** Username/password, CA, certificate, and private key are sensitive untrusted bytes. Validate required keys and formats, minimize filesystem exposure, never persist values, and bind only Secret resource versions to the result.
6. **Discovery process to configured CQL endpoints:** Contact-point IPs are user-controlled until validated, and authentication precedes Cassandra identity checks. Enforce IP-only port 9042, deadlines, authenticated TLS with IP verification across untrusted networks, read-only queries, and clear documentation that operators must trust endpoints.
7. **CQL responses to parser/canonicalizer:** System-table values, versions, names, maps, and integers are untrusted remote input. Parse and validate one complete candidate per attempted seed; reject nulls, internal conflicts, unsupported classes/versions, unknown DCs, overflow, malformed encodings, and unstable before/after fingerprints. Close failed sessions, stop after the first valid candidate, and never union or compare endpoint data.
8. **Discovery result to controller:** A Kubernetes result object is untrusted even if controller-owned by convention. Validate schema, size, canonical encoding/hash, all attempt bindings, absence of prohibited secret fields, workload identity/authenticity, and freshness before status acceptance.
9. **Accepted snapshot to bootstrap/datacenter builder:** Only accepted canonical seeds may cross this boundary. Per-keyspace replication maps must never reach the flat bootstrap system-replication property or user-writable annotation.
10. **Post-Ready Management API to schema reconciler:** Live replication is untrusted mutable external state. Read all three maps first, compare exact external projections, and expose only direct ALTER capability for managed-only deltas; do not expose create-if-missing through the discovery branch.
11. **Status/Event/log user boundary:** Messages are externally visible and durable. Map raw failures to stable sanitized reason codes and corrective actions; redact credentials, Secret data, keys, certificates, auth payloads, driver DSNs, and overly detailed TLS errors.

## Documentation Needs

### User-facing workflows

1. **New legacy migration workflow:** Update `docs/content/en/tasks/migrate/_index.md` with qualification, admission marker behavior, current-spec first-operator-owned-DC discovery placement, stable ordered seed fallback, first-complete-valid authority, skipped remainder, exhaustion, accepted-snapshot read-back, first managed DC creation, post-Ready external comparison, and managed-only ALTER behavior.
2. **Blocked-to-recovered workflow:** Show how to inspect discovery phase/reason, correct a Secret/network/TLS/source issue, and wait for Secret-triggered or periodic retry without recreating the `K8ssandraCluster`.
3. **Upgrade/non-retroactivity workflow:** Explain that pre-existing unmarked or in-progress objects keep legacy behavior; discovery is not an upgrade migration tool, and marked objects that are already too late block.
4. **External drift workflow:** Explain how drift is detected, why K8ssandra emits no DDL and does not restore external RF, and what the SRE must verify/correct before retry.
5. **Lifecycle verification workflow:** Separate source/rendered-manifest/unit evidence from the required live Cassandra lifecycle proof; explicitly state that discovery does not prove liveness, complete ring membership, streaming, repair, cleanup, or data health.

### Setup, configuration, and migration steps

- Runtime support for Apache Cassandra 4.0 or newer plus the three repository-aligned named E2E scenarios for 4.0.17, 4.1.9, and 5.0.6; unsupported DSE/HCD, pre-4.0 sources, FQDN seeds, and `secretsProvider: external` behavior.
- Discovery-aware webhook installation/availability and fail-closed create behavior; compatible single active reconciler or supported leader-election configuration.
- First operator-owned Cassandra datacenter selection by current spec order and its Kubernetes context/namespace; explain that placement is provenance rather than an immutable topology constraint and that normal later reorder/decommission/removal is allowed. Document data-plane permissions to create/observe the workload/result and reach attempted seeds on TCP 9042.
- IP-literal `additionalSeeds`, normalized first-occurrence/spec-order semantics, order-sensitive accepted-list digest, and the separate existing internode bootstrap reachability requirement.
- Ordered fallback authority: failed attempts do not block while a later seed succeeds; the first complete valid seed supplies all snapshot RF/topology/schema/version data; later seeds are skipped and K8ssandra makes no convergence claim about them.
- Dedicated pre-existing discovery credential Secret, exact keys/namespace rules, least-privilege CQL permissions, and explicit independence from target Cassandra auth/superuser settings.
- TLS trust/client identity Secret setup and IP certificate verification requirements.
- Optional `externalDatacenters` exact-set validation and a warning that it neither discovers nor supplies RF values.
- Snapshot immutability, post-acceptance seed/spec edit behavior, object deletion/recreation consequences, unsupported manual status editing, and the explicit recovery split: rediscover missing/corrupt state only before any managed DC exists; otherwise block.

### API/configuration examples

- A complete validated `K8ssandraCluster` YAML using the delivered credential-reference field, IP seeds, Cassandra server type, planned DC context/namespace, and TLS references; examples must not contain literal credentials.
- `kubectl` examples to create the credential/TLS Secrets from local files or environment input without echoing secret values into documentation or shell history.
- `kubectl get`/JSONPath examples for phase, reason, actionable message, observed generation, accepted snapshot hash, and accepted seed set.
- An RF example proving independent sparse maps and `system_auth` values `2/9/2/10/10`; do not represent it as user-authored desired config.
- Before/after commands for inspecting all three live replication maps, clearly labeled as operational verification rather than K8ssandra configuration.
- A multi-seed example showing A failing, B succeeding, C never being contacted, plus exhaustion behavior when every endpoint fails.
- Examples of unmarked upgrade behavior, `DiscoveryTooLate`, and `ExternalReplicationDrift` with safe remediation.
- Regenerated current CRD reference for every new spec/status field and enum. Do not edit historical release references or extend the stale scaffold sample as the primary example.

### Troubleshooting topics and reasons

- Document the meaning, likely causes, safe diagnostics, and corrective action for every required stable reason: `UnsupportedServerType`, `UnsupportedSourceVersion`, `DiscoveryTooLate`, `InvalidContactPoint`, `ContactUnreachable`, `AuthenticationRejected`, `AuthorizationDenied`, `TLSFailed`, `UnsupportedSecretsProvider`, `IdentityMismatch`, `SchemaDisagreement`, `TopologyInconsistent`, `ManagedDatacenterNameCollision`, `MissingKeyspace`, `UnsupportedStrategy`, `InvalidReplication`, `StaleDiscoveryResult`, `SnapshotConflict`, and `ExternalReplicationDrift`.
- Add operational diagnosis for workload Pending/Failed, image pull, RBAC, result timeout/malformed result, remote context unavailable, admission unavailable, retry/backoff timing, and snapshot hash corruption.
- Explain reason precedence if multiple failures occur and which cases recover automatically versus require object recreation or operator intervention.
- Explain attempted endpoint order, accepted endpoint provenance, deterministic exhausted-fallback reason selection, skipped endpoints, and how a pre-creation missing/corrupt snapshot differs from the permanent post-creation block.
- Link migration troubleshooting from `docs/content/en/tasks/troubleshoot/_index.md`; keep the detailed migration contract in the existing migration page and regenerate the latest CRD reference.

### Security-sensitive documentation constraints

- Warn that CQL authentication occurs before cluster identity is readable; configured IPs must be trusted and authenticated TLS is required when credentials cross an untrusted network.
- Never show literal passwords, Secret data, private keys, embedded certificates, disabled certificate verification, anonymous auth as a production default, wildcard RBAC, or reusable broad Cassandra credentials.
- State the minimal CQL read permissions and Kubernetes RBAC required by controller versus discovery workload.
- Explain result/snapshot trust, status ownership, redaction guarantees and limitations, and why users must not edit controller-owned status or reserved markers.
- Document that external drift is intentionally not remediated automatically and that RF preservation is not evidence of repair, ownership, or data availability.

Documentation workflow count: **5**. Natural homes are the existing migration guide, troubleshooting guide, secure/encryption guidance, and generated latest CRD reference; any new page must be linked from the nearest index.

## Complexity Signal

- **Stories:** 5
- **Entities:** 13
- **External runtime dependencies:** 8
- **Entity invariants:** 40
- **Happy-path scenarios:** 9
- **Edge cases:** 38 (minimum required: 18)
- **Security boundaries:** 11
- **Documentation workflows:** 5
- **Architecture duel threshold:** **met** (`13 >= 6` entities and `5 >= 4` stories).
- **Duel recommended:** **yes**. The feature crosses admission, multi-cluster reconciliation, ordered fallback seed authority, CQL execution, recoverable pre-creation snapshot persistence, race-sensitive resource creation, post-Ready DDL, RBAC/packaging, and live lifecycle verification. **No critical business requirement questions remain**; the duel must preserve the resolved decisions and choose only internal implementation details.
