# Legacy System-Keyspace Replication Discovery

## Document Purpose

Define the product and safety contract for discovering legacy Cassandra system-keyspace replication before K8ssandra creates its first managed `CassandraDatacenter`. The capability prevents K8ssandra from replacing independently configured legacy replication factors with its current RF 3 default.

This document covers Apache Cassandra migrations only. It defines the observable guarantee K8ssandra can make; it does not claim that CQL provides an authoritative node-liveness or ring-membership view.

## Overview

K8ssandra currently computes one replication map for `system_auth`, `system_traces`, and `system_distributed`. Every datacenter listed in `spec.externalDatacenters` receives RF 3. Later reconciliation uses a create-or-alter helper. That behavior is unsafe for a legacy migration because it can both change an external RF and create a missing system keyspace.

For a qualifying new migration with non-empty `spec.cassandra.additionalSeeds`, K8ssandra shall run a read-only, fail-closed CQL discovery phase before it creates any managed `CassandraDatacenter` resource. It shall persist one immutable, controller-owned snapshot containing the separately observed replication map for each supported system keyspace. After acceptance, that snapshot is an assertion about externally owned state, not desired state to reapply: any external drift blocks K8ssandra schema DDL instead of being reverted.

### Legacy `system_auth` example

```sql
CREATE KEYSPACE system_auth WITH replication = {
  'class': 'NetworkTopologyStrategy',
  'dc1': '2',
  'dc2': '9',
  'dc3': '2',
  'dc4': '10',
  'dc5': '10'
};
```

RF 9 and RF 10 are valid inputs to this capability and shall not be normalized or rejected by a feature-specific RF 5 limit.

## Goals

- Prevent K8ssandra from increasing or decreasing an observed external `system_auth` RF.
- Discover `system_auth`, `system_traces`, and `system_distributed` independently without issuing schema DDL.
- Stop before creating any managed `CassandraDatacenter` when the observable legacy state is unavailable, inconsistent, unsupported, or ambiguous.
- Persist a deterministic snapshot before allowing managed Cassandra creation.
- Reconcile only managed-datacenter entries after startup and only while external entries still match the accepted snapshot.
- Recover automatically after a transient prerequisite is corrected.

## Non-Goals

- Discovering or managing user-keyspace replication.
- Supporting DSE, HCD, or Cassandra versions older than 4.0.
- Managing Reaper or Stargate topology or replication from the discovered snapshot.
- Performing repair, cleanup, data streaming, or liveness/ring-health validation.
- Creating a missing supported system keyspace.
- Enforcing or reverting an external-datacenter replication change.
- Supporting concurrent topology or schema edits during discovery or managed-entry reconciliation.
- Supporting `secretsProvider: external` in the first release of discovery.
- Supporting FQDN values in `additionalSeeds` until K8ssandra propagates them to Cassandra bootstrap as well as discovery.

## User Stories

- **US-1:** As an SRE, I want K8ssandra to discover legacy system-keyspace replication from `additionalSeeds` so that a live RF such as 2, 9, or 10 is not replaced by RF 3.
- **US-2:** As an SRE, I want migration to stop before any managed Cassandra resource is created when the legacy state cannot be trusted.
- **US-3:** As a platform engineer, I want one durable snapshot per migration so retries do not rediscover or normalize external state.
- **US-4:** As an SRE, I want a sanitized failure reason and corrective action so a blocked migration can recover automatically.
- **US-5:** As a platform engineer, I want discovery to apply only to new, discovery-aware migration objects, never retroactively to an in-progress cluster during an operator upgrade.

## Feasibility Constraints

The following constraints are part of the feature, not implementation suggestions:

1. The existing Management API facade cannot perform discovery because it requires a Ready managed Cassandra pod. Discovery therefore requires a separate read-only CQL client and a pre-creation reconciliation phase.
2. Discovery shall execute in a short-lived workload in the Kubernetes context and namespace of the first planned managed datacenter. This uses the migration's data-plane network path rather than assuming the control-plane operator can reach the legacy cluster.
3. Discovery acceptance shall be durably persisted and read back from the Kubernetes API before the controller may create any managed `CassandraDatacenter`.
4. The existing `k8ssandra.io/initial-system-replication` annotation cannot store the snapshot: it has one flat DC-to-RF map, is user-writable metadata, and cannot represent three heterogeneous keyspace maps.
5. The discovered external maps shall not be serialized into Cassandra's bootstrap system-replication property. That path cannot represent the three maps independently and must not receive discovered RF values. The release gate shall prove on every supported Cassandra lifecycle that bootstrap leaves the pre-existing three keyspaces unchanged.
6. After startup, discovery-enabled migrations shall use live read/compare/`AlterKeyspace` only. They shall never call `EnsureKeyspaceReplication` or another create-or-alter path for the three supported keyspaces.
7. Automatic qualification requires a new mutating admission path for `K8ssandraCluster` creation. It shall inject the discovery-version marker with failure policy `Fail`; the validating admission path shall reject a client-supplied forged marker and any later marker mutation or removal.

## Functional Requirements

### Qualification and Pre-Creation Gate

- **FR-001:** Discovery shall be required only when all of the following are true:
  - the object was created through the discovery-aware API path and bears an immutable, server-owned discovery-version marker;
  - `spec.cassandra.serverType` is `cassandra`;
  - `spec.cassandra.additionalSeeds` is non-empty;
  - no accepted snapshot exists; and
  - no managed `CassandraDatacenter` exists in any planned target context and namespace.
- **FR-002:** The discovery-version marker shall be established on object creation without user opt-in. Admission shall fail closed when marker injection or validation is unavailable. It shall reject a client-supplied marker and shall never infer or add the marker retroactively to an existing object during operator upgrade.
- **FR-003:** If a marked Cassandra object has non-empty `additionalSeeds` and no accepted snapshot but any planned managed `CassandraDatacenter` or managed Cassandra pod already exists, reconciliation shall block with `DiscoveryTooLate`. It shall not rediscover or continue managed creation. Marked objects without `additionalSeeds` remain outside this gate.
- **FR-004:** Before snapshot acceptance, K8ssandra may create only discovery support resources. It shall not create a managed `CassandraDatacenter` or Cassandra pod.
- **FR-005:** Every operator replica capable of reconciling a marked object shall understand and enforce this gate. Mixed old/new reconcilers are unsupported; deployment shall provide a single active compatible reconciler or equivalent concurrency safety.
- **FR-005a:** A marked DSE or HCD object shall report `NotRequired` with reason `UnsupportedServerType` and retain its existing behavior; this feature makes no RF-preservation claim for it. A marked Cassandra object whose discovered source is older than 4.0 shall block with `UnsupportedSourceVersion` before managed creation.

### Discovery Execution and Connection Contract

- **FR-006:** A short-lived discovery workload shall run in the first planned datacenter's resolved Kubernetes context and namespace. It shall return only canonical, non-secret discovery results through a controller-owned Kubernetes resource; credentials shall never be returned in results or logs. Each result shall be bound to the `K8ssandraCluster` UID, generation, marker version, canonical seed digest, discovery-credential Secret resource version, and referenced TLS Secret resource versions used by that attempt.
- **FR-007:** Each `additionalSeeds` entry shall be an IPv4 or IPv6 literal. After structural validation, discovery shall attempt the canonical endpoints sequentially in stable spec order on CQL native transport port 9042. Each attempted endpoint gets a fresh endpoint-pinned session. A failed attempted endpoint shall not block while a later endpoint can produce a complete valid result; discovery blocks only when no endpoint succeeds. Invalid seed syntax remains a fail-closed preflight error because Cassandra bootstrap would omit it.
- **FR-008:** The canonical ordered contact-point list accepted by discovery shall preserve spec order, deduplicate by first occurrence, use an order-sensitive digest, be stored in the snapshot, and later be propagated as Cassandra additional seeds. A spec change after acceptance shall not change the bootstrap list. The single endpoint that produces the accepted result shall be stored separately as authoritative provenance. Discovery shall not succeed through an endpoint that bootstrap silently omits.
- **FR-009:** Legacy-source authentication is independent of managed-target authentication. A dedicated optional discovery credential Secret reference shall select the only credentials discovery may use; it shall name a pre-existing Secret containing the legacy CQL username and password. When it is absent, discovery shall attempt anonymous CQL. When the source has authentication disabled, discovery may succeed whether the dedicated reference is absent or validly present because Cassandra does not request authentication. K8ssandra shall never generate discovery credentials, infer them from target authentication, or substitute target credentials.
- **FR-010:** `secretsProvider: external` shall block discovery as unsupported in the first release. Supporting it later requires a defined injector contract for the discovery workload.
- **FR-011:** When client encryption is configured, the discovery workload shall consume the referenced trust and client identity material, verify the server certificate against the contacted IP address, and preserve hostname/IP verification. TLS configurations that cannot authenticate the contacted endpoint shall block.
- **FR-012:** Connections and queries shall have bounded per-endpoint and overall deadlines. Discovery shall open at most one fresh endpoint-pinned session at a time, close it on success or failure, and stop without opening any later session after the first complete valid result. Authentication occurs before CQL identity can be read; documentation shall warn that operators must trust attempted contact points and use authenticated TLS when credentials cross an untrusted network.

### Observable Identity, Topology, and Schema Contract

- **FR-013:** Each attempted endpoint shall report the expected Cassandra cluster name: `spec.cassandra.clusterName` when set, otherwise the `K8ssandraCluster` name. A mismatch fails that endpoint and discovery proceeds to the next fallback. Cluster-name equality on the accepted endpoint is a consistency check, not proof of a globally unique cluster identity or validation of skipped endpoints.
- **FR-014:** A complete candidate from one attempted endpoint shall have a non-null partitioner and schema version plus a non-conflicting `system.local` and `system.peers_v2` view. Host IDs shall be non-null and unique; an address or host ID shall not map to conflicting datacenter names. Candidate failure proceeds to the next endpoint.
- **FR-015:** Discovery shall treat the internally stable topology observable from the single accepted endpoint as the known topology. It shall not merge or compare endpoint observations, claim node liveness or authoritative ring membership, or claim knowledge of skipped endpoints or nodes absent from the accepted endpoint's observable system tables.
- **FR-016:** For each candidate attempt, K8ssandra shall read identity, topology, and schema-version fingerprints before and after reading the three replication rows. That candidate succeeds only when its own before/after fingerprints are identical and its observable view has one schema version.
- **FR-017:** Every observed legacy datacenter is external. If any observed datacenter name exactly and case-sensitively equals a planned managed Cassandra datacenter name, discovery shall block with `ManagedDatacenterNameCollision`.
- **FR-018:** `spec.externalDatacenters` is not required and shall not be populated from discovery. If the user supplied it, its exact set shall match the observed external set before acceptance; it shall never supply or override RF values. The snapshot shall not affect Reaper, Stargate, or user-keyspace behavior.

### Replication Validation

- **FR-019:** `system_auth`, `system_traces`, and `system_distributed` shall all exist and use `NetworkTopologyStrategy`. Accepted class names shall be canonicalized only from Cassandra's supported fully qualified or short NetworkTopologyStrategy names.
- **FR-019a:** Discovery shall read the source Cassandra release version from each candidate endpoint it attempts. The accepted endpoint shall report a supported version of 4.0 or newer, and that version shall be bound to the accepted snapshot. Versions from failed or skipped endpoints are not compared.
- **FR-020:** Each present RF shall parse as a base-10 integer in the range 1 through 2,147,483,647. No smaller feature-specific upper bound is allowed; RF 9 and RF 10 shall be accepted.
- **FR-021:** A datacenter omitted from a keyspace replication map means that keyspace has no replica entry for that datacenter. Omission shall remain omission, not RF 0 or RF 3. Present values shall not be normalized across datacenters or keyspaces.
- **FR-022:** A replication entry naming a datacenter absent from the accepted observable topology shall block as ambiguous. Datacenter identifiers are exact and case-sensitive.
- **FR-023:** Discovery shall issue read-only CQL. Missing rows, unsupported strategies, malformed maps, null or conflicting topology, and partial results shall block without schema DDL.

### Snapshot Persistence and Authority

- **FR-024:** The accepted snapshot shall be stored in controller-owned status and contain at least:
  - the `K8ssandraCluster` UID and accepted generation;
  - discovery marker version, accepted canonical seed set and digest, and the resource versions of referenced discovery credential and TLS Secrets;
  - expected cluster name and source Cassandra version;
  - canonical identity, topology, and schema fingerprints;
  - the observed external datacenter set;
  - a separate presence-preserving DC-to-RF map for each supported keyspace;
  - acceptance time and canonical snapshot hash.
- **FR-025:** Snapshot acceptance shall use optimistic concurrency. Immediately before acceptance, the controller shall authoritatively revalidate the object UID/generation/marker, seed digest, referenced Secret resource versions, and absence of every planned managed `CassandraDatacenter`; any mismatch discards the result. After a successful status write, reconciliation shall stop.
- **FR-025a:** Immediately before creating a managed `CassandraDatacenter`, the controller shall re-read authoritative state and revalidate the object UID/generation/marker, accepted snapshot hash, accepted seed digest, compatible planned DC names, and absence of a managed DC. Only then may it create the resource using the accepted canonical seed set.
- **FR-026:** An accepted snapshot is immutable for the lifetime of the `K8ssandraCluster` UID. Changes to `additionalSeeds` after acceptance shall not replace either its replication maps or accepted seed set. A missing, corrupt, conflicting, or user-erased snapshot after managed creation shall block.
- **FR-027:** Changes after acceptance to expected cluster name, server type, or planned managed datacenter names shall be validated against the snapshot. An incompatible change or external-name collision shall block; it shall not trigger rediscovery.
- **FR-028:** The snapshot is the expected external-state assertion for later managed reconciliation. It is not desired state that K8ssandra may enforce against an externally owned datacenter.

### Managed Replication Reconciliation

- **FR-029:** Before any operator-issued system-keyspace `ALTER`, K8ssandra shall read and prevalidate all three live keyspaces. If a keyspace is missing, its strategy changed, or any external presence/value differs from the accepted snapshot, reconciliation shall block and issue no schema DDL.
- **FR-030:** Only when all three external projections still match may K8ssandra merge desired managed-datacenter entries into the current maps and call direct `AlterKeyspace` for keyspaces whose managed projection differs.
- **FR-031:** Discovery-enabled reconciliation shall never call `EnsureKeyspaceReplication`, `CreateKeyspaceIfNotExists`, or an equivalent create-or-alter helper for the three keyspaces.
- **FR-032:** If an `ALTER` succeeds for one keyspace and a later managed-only `ALTER` fails, retry shall re-read and prevalidate all three keyspaces before continuing. K8ssandra shall never compensate by changing external entries.

### Failure, Recovery, and Observability

- **FR-033:** Discovery status shall use the phases `NotRequired`, `Pending`, `Blocked`, and `Accepted`. `Accepted` remains visible for the lifetime of the snapshot; managed readiness is reported by existing conditions.
- **FR-034:** Status shall expose `observedGeneration`, phase, stable reason code, sanitized actionable message, transition time, and accepted snapshot hash where applicable. Kubernetes Events shall be emitted on state transitions, not on every identical retry.
- **FR-035:** Stable reasons shall include at least `UnsupportedServerType`, `UnsupportedSourceVersion`, `DiscoveryTooLate`, `InvalidContactPoint`, `ContactUnreachable`, `AuthenticationRejected`, `AuthorizationDenied`, `TLSFailed`, `UnsupportedSecretsProvider`, `IdentityMismatch`, `SchemaDisagreement`, `TopologyInconsistent`, `ManagedDatacenterNameCollision`, `MissingKeyspace`, `UnsupportedStrategy`, `InvalidReplication`, `StaleDiscoveryResult`, `SnapshotConflict`, and `ExternalReplicationDrift`. Per-endpoint failures remain internal and sanitized while fallbacks remain. After exhaustion, the public reason shall be chosen deterministically from the ordered attempt failures using a documented precedence rule.
- **FR-036:** Transient failures shall be retried with bounded exponential backoff and jitter. Changes to referenced credential or TLS Secrets shall enqueue reconciliation; periodic retry shall still recover from network changes.
- **FR-037:** Status, Events, logs, discovery results, and error payloads shall redact passwords, Secret data, private keys, certificates, and raw authentication payloads.

## Migration Workflow

1. The SRE creates a new Cassandra `K8ssandraCluster` with IP-literal `additionalSeeds`, supported internal Secret handling, an optional dedicated legacy CQL credential Secret reference, and the required TLS configuration.
2. The discovery-aware API path records the immutable version marker. The controller confirms that no managed `CassandraDatacenter` exists.
3. K8ssandra creates a short-lived discovery workload in the first planned datacenter's context and namespace. No Cassandra resource is created.
4. The workload attempts seeds sequentially in canonical spec order. Each attempt uses a fresh endpoint-pinned session and captures one complete stable before/after identity, topology, schema, and per-keyspace replication view.
5. A failed endpoint is closed and the next fallback is attempted. The first complete valid result is accepted immediately and later seeds are skipped. Only exhaustion produces `Blocked` status; full discovery retries after a retryable prerequisite is corrected.
6. A valid result is written to status with optimistic concurrency. Reconciliation stops and reads the accepted snapshot back from the API.
7. Only after read-back validation may K8ssandra create the first managed `CassandraDatacenter`.
8. Later system-keyspace reconciliation first verifies that all external projections still equal the snapshot. It may then alter managed entries only.

## Compatibility

- New discovery-marked Cassandra clusters with non-empty `additionalSeeds` must pass the gate.
- New clusters without `additionalSeeds` retain current behavior and report `NotRequired`.
- Objects created before discovery-aware admission lack the immutable marker and retain current behavior. Discovery is never enabled retroactively during upgrade. Admission outage fails new object creation rather than creating an unmarked object.
- A marked object with an existing managed `CassandraDatacenter` but no snapshot blocks as `DiscoveryTooLate`; `CassandraInitialized` alone is not a safe eligibility predicate.
- DSE, HCD, Cassandra sources older than 4.0, `secretsProvider: external`, and FQDN seeds are unsupported by this release and shall not enter discovery silently.
- Existing flat initial-system-replication annotations are not accepted as discovery snapshots.
- `additionalSeeds` still requires the existing internode path for Cassandra bootstrap in addition to the new TCP 9042 CQL path for discovery.

## Acceptance Criteria

- **AC-001:** Given a new marked Cassandra cluster with valid IP-literal `additionalSeeds` and no managed DC resource, discovery begins automatically without user opt-in, an external-DC list, or an RF matrix.
- **AC-002:** Until snapshot status is successfully persisted and read back, no managed `CassandraDatacenter` or Cassandra pod exists.
- **AC-003:** A stable legacy `system_auth` map containing `dc1=2`, `dc2=9`, `dc3=2`, `dc4=10`, and `dc5=10` is accepted and stored exactly; the other two keyspaces retain their own independent maps.
- **AC-004:** RF values greater than five never pass through a bootstrap-property parser and remain unchanged from the pre-bootstrap read through managed readiness.
- **AC-005:** Unreachable, wrong-cluster, authentication, authorization, TLS, schema, topology, keyspace, strategy, or replication failures block with no managed DC and no schema DDL only when every ordered fallback fails to produce a complete valid result.
- **AC-006:** A missing supported keyspace never invokes a create-or-alter path before or after snapshot acceptance.
- **AC-007:** A datacenter omitted from one keyspace remains omitted from that keyspace; no RF 0 or RF 3 entry is synthesized.
- **AC-008:** A topology or schema fingerprint change within one endpoint attempt discards that candidate, closes its session, and proceeds to the next fallback; no partial snapshot is accepted.
- **AC-009:** An operator restart after acceptance uses the identical stored snapshot and accepted seed set and does not rediscover, including when `additionalSeeds` changes or is removed. Bootstrap consumes the accepted set, not the changed spec.
- **AC-010:** External replication drift after acceptance produces `ExternalReplicationDrift` and zero operator-issued DDL; K8ssandra does not restore the snapshot value.
- **AC-011:** When all external entries still match, K8ssandra may alter managed entries without changing any external entry and without calling `EnsureKeyspaceReplication`.
- **AC-012:** An existing or non-Ready managed DC without an accepted snapshot does not trigger retroactive discovery and blocks if the object is marked.
- **AC-013:** A corrected credential/TLS Secret or restored network causes a retryable blocked migration to retry and proceed to accepted snapshot read-back and managed DC creation without recreating the `K8ssandraCluster`; permanent snapshot/too-late conflicts do not retry.
- **AC-014:** Status, Events, logs, and discovery results contain no credential, key, certificate, or raw authentication material.
- **AC-015:** A stale discovery result caused by a generation, seed, credential Secret, or TLS Secret change is rejected before snapshot acceptance; a concurrent reconcile cannot create a managed DC without repeating the authoritative create-time checks.
- **AC-016:** Admission outage rejects creation of a new `K8ssandraCluster`; a user cannot forge, mutate, or remove the discovery-version marker.
- **AC-017:** Source authentication succeeds or fails solely from the dedicated discovery credential reference and source behavior, independently of managed-target authentication settings. A source with authentication disabled may succeed with the reference absent or validly present; a source requiring authentication blocks without the reference and succeeds with valid referenced credentials.
- **AC-018:** A Cassandra source older than 4.0 blocks with `UnsupportedSourceVersion`; DSE and HCD report `NotRequired/UnsupportedServerType` without claiming RF preservation.
- **AC-019:** Given ordered seeds A, B, and C, when A fails and B returns a complete valid result, B alone supplies the snapshot, A's failure does not block acceptance, and C is never contacted. When all seeds fail, discovery blocks with a deterministic exhausted-fallback reason.

## Verification and Release Gates

The feature is not complete until all of the following pass:

- Table-driven unit tests for ordered fallback, exhaustion, no-later-contact after success, per-attempt identity/topology consistency, before/after fingerprints, NetworkTopologyStrategy aliases, sparse maps, RF 2/9/10, overflow, malformed values, unknown DCs, and managed-name collisions.
- Controller/envtest coverage proving: no `CassandraDatacenter` on every blocked path; status compare-and-swap and read-back; restart persistence; immutable snapshot behavior; Secret-triggered recovery; and no create method available on the discovery interface.
- End-to-end controller recovery timelines proving same-UID credential/TLS Secret correction through a Secret watch and network restoration through periodic retry, each progressing from retryable `Blocked` to `Accepted` read-back and exactly one managed DC; permanent conflicts remain blocked.
- Schema-reconciliation tests proving full three-keyspace prevalidation, direct `AlterKeyspace` only, no DDL on external drift, and no use of `EnsureKeyspaceReplication` for marked migrations.
- A real Cassandra lifecycle test for every Apache Cassandra source/target version combination supported by the release. It shall use one node in each of the two source datacenters `legacy-a` and `legacy-b`, capture all three replication maps before the first managed DC, start the managed DC, wait through Ready and managed-entry reconciliation, and prove that every external presence/value is unchanged while the managed DC is added at RF 1. The sparse source maps are `system_auth={legacy-a:2,legacy-b:4}`, `system_traces={legacy-a:1}`, and `system_distributed={legacy-b:4}`.
- A release-owned authentication matrix covering source authentication on/off, dedicated discovery credentials absent/present, and TLS off/on, with every row repeated under managed-target authentication off/on. It shall prove target authentication never supplies or changes source credentials and include TLS IP-verification failures in anonymous and authenticated modes.
- Marked-object lifecycle coverage proving a conflicting user-supplied `k8ssandra.io/initial-system-replication` annotation neither satisfies/bypasses discovery nor influences the accepted snapshot, bootstrap input, live keyspace rows, or post-Ready DDL decisions.
- Upgrade coverage proving that an old or in-progress object with seeds and an existing non-Ready DC never triggers discovery retroactively.
- Multi-replica/restart coverage proving that concurrent reconciles cannot create a managed DC before authoritative snapshot read-back. The operator deployment shall use leader election or demonstrate equivalent correctness with status compare-and-swap plus create-time revalidation.
- Mutation-race coverage for object generation, seeds, discovery credentials, TLS Secrets, and planned DC resources between discovery, acceptance, and creation.
- Admission coverage proving fail-closed marker injection, rejection of client-forged markers, and marker immutability.
- Security tests for TLS endpoint verification and redaction of all secret material.

Local/unit evidence is not proof of live bootstrap behavior. A release shall not claim external-RF preservation until the lifecycle gate above passes on the actual supported Cassandra images.

## Success Metrics

- Zero operator-issued external RF changes for migrations using this capability.
- Zero managed `CassandraDatacenter` resources created before accepted-snapshot read-back.
- Zero missing supported keyspaces created by the discovery-enabled path.
- Every blocked prerequisite has a stable, sanitized reason and recovers automatically when corrected.
- Every supported heterogeneous or sparse map, including RF 9 and 10, is preserved per keyspace.

## Assumptions and Dependencies

- The legacy source is Apache Cassandra 4.0 or newer and is in the release's tested compatibility matrix.
- A single schema writer operates during discovery and managed-entry reconciliation.
- Configured contact-point IPs are trusted fallbacks on TCP 9042. K8ssandra validates only attempted endpoints and makes no claim that skipped endpoints expose the accepted cluster.
- The first planned datacenter's Kubernetes context can create the discovery workload and reach the legacy CQL endpoints.
- Authenticated legacy discovery uses a dedicated pre-existing referenced Secret; target authentication settings are independent. TLS material is compatible with the discovery client and endpoint verification.
- The controller has remote RBAC for the discovery workload and result resource, and watches or periodically polls their state.
- Discovery-aware mutating and validating admission is registered with failure policy `Fail` for new objects and prevents marker forgery, mutation, or removal.

## Resolved Decisions

- Discovery runs in the first planned datacenter's data plane, not through the post-Ready Management API.
- Authentication uses an explicitly referenced, pre-existing legacy discovery Secret; generated credentials and target-auth settings are not discovery inputs.
- External secret injection and FQDN seeds are deferred rather than left ambiguous.
- Observable identity is the accepted endpoint's expected cluster name plus internally stable partitioner, host IDs, topology, and schema fingerprints; it is not a globally unique identity proof or a cross-endpoint consistency claim.
- Sparse NetworkTopologyStrategy maps preserve omission.
- External drift blocks without DDL; K8ssandra never restores the snapshot value.
- The accepted snapshot remains immutable for the `K8ssandraCluster` UID lifetime.
