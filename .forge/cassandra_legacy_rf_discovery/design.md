# Synthesized Design: Legacy System-Keyspace RF Discovery

## Outcome

Use Proposal A's brownfield-aligned, level-based reconciliation design as the base. Add the challenger's required safety contracts and selected Proposal B boundaries. Do not introduce a repository abstraction, grouped one-field public configuration, generic workflow engine, new CRD, or repository-wide hexagonal layout.

The resulting design adds:

- one fresh worker binary in the existing operator image;
- a small source-discovery/protocol package and a separate managed-replication planning file/package boundary;
- an early controller gate, authoritative status acceptance/read-back, and a named final creation authorization operation;
- direct remote Kubernetes readers retained by the existing `ClientCache`;
- typed mutating/validating admission with a rollout mechanism that prevents old and new reconcilers from overlapping while marker injection is active;
- an immutable accepted snapshot plus separate managed-creation provenance and post-Ready schema condition;
- lazy ordered endpoint-pinned CQL fallback where the first complete valid seed response is authoritative and later seeds are skipped.

## Resolved Product Semantics

1. The discovery workload runs in the first **operator-owned** Cassandra datacenter in current spec order. The chosen location is attempt provenance, not desired topology.
2. Managed datacenter reorder, removal/decommission, and non-colliding addition remain legal after acceptance. They do not trigger rediscovery or invalidate the external snapshot.
3. Canonical seeds preserve normalized spec order and are attempted sequentially with one fresh endpoint-pinned CQL session at a time. A failed attempt closes and falls back; the first complete valid before/keyspaces/after response is the sole source for source version, partitioner, schema, topology, and all three RF maps. Later seeds are never contacted, and observations are neither compared nor merged. Discovery blocks only after exhaustion.
4. A missing or corrupt snapshot may trigger full rediscovery only when authoritative Kubernetes checks plus the durable managed-creation tombstone prove that no managed DC has ever been created. After managed creation, snapshot loss blocks permanently.
5. Runtime source compatibility is Apache Cassandra `4.0.0` or newer with no runtime upper bound. Release tests follow the repository's existing three-test convention with representative patches `4.0.17`, `4.1.9`, and `5.0.6`; they do not introduce a Cartesian source-by-target matrix.
6. RF discovery is always a fresh CQL read from the worker. Kubernetes cache freshness is a separate concern used only for resource reconciliation; safety absence checks use direct API readers.

## Package and File Structure

```text
cmd/legacy-rf-discovery/
  main.go                         # isolated worker process

pkg/discovery/
  errors.go                       # typed internal failures and public reason mapping
  types.go                        # attempt/result/source observation types
  canonical.go                    # IP, RF, fingerprint, canonical encoding and hashing
  protocol.go                     # signed bounded result envelope
  worker.go                       # lazy ordered endpoint fallback and first-valid authority

controllers/k8ssandra/
  legacy_rf_discovery.go          # qualification and level-based state machine
  legacy_rf_discovery_resources.go# Job/Secret/SA/Role/Binding/ConfigMap lifecycle
  legacy_rf_discovery_status.go   # CAS, read-back, tombstone/history, transitions
  legacy_rf_discovery_plan.go     # pure current-plan compatibility and create authorization
  legacy_rf_discovery_test.go
  datacenters.go                  # accepted seeds and immediate pre-create call
  schemas.go                      # accepted-snapshot live prevalidation/direct ALTER
  k8ssandracluster_controller.go  # early gate and watches

apis/k8ssandra/v1alpha1/
  k8ssandracluster_types.go       # credential input, snapshot/status/condition types
  constants.go                    # marker/protocol/reason/keyspace constants
  k8ssandracluster_webhook.go     # marker ownership and immutable validation

pkg/clientcache/cache.go          # cached plus direct remote client pairs
main.go                           # composition root
Dockerfile                        # manager and worker binaries
config/, charts/, docs/, test/e2e/# generated delivery and release gates
```

`pkg/discovery` owns only source observation and the signed worker protocol. Managed desired-map planning stays with the existing schema reconciliation concern so the source protocol package does not acquire a second reason to change.

## Public API Contract

Add a direct optional field adjacent to `additionalSeeds`:

```go
LegacyCqlCredentialsSecretRef *corev1.LocalObjectReference `json:"legacyCqlCredentialsSecretRef,omitempty"`
```

It resolves in the `K8ssandraCluster` namespace on the control-plane cluster. Required keys are exactly `username` and `password`; absence selects anonymous CQL. Cross-namespace references, generated credentials, reuse of target superuser credentials, and `secretsProvider: external` are unsupported.

Use explicit snapshot fields for the three keyspaces rather than a free-form keyspace-keyed map:

```go
type LegacySystemKeyspaceReplication struct {
    SystemAuth        map[string]int32 `json:"systemAuth"`
    SystemTraces      map[string]int32 `json:"systemTraces"`
    SystemDistributed map[string]int32 `json:"systemDistributed"`
}
```

Each map preserves omission. All three keyspace rows are mandatory; an empty DC map is distinct from a missing keyspace row.

Discovery status contains:

- `observedGeneration`, `phase`, `reason`, `message`, and `lastTransitionTime`;
- `snapshotHash` and an accepted immutable snapshot;
- `managedCreationObserved` as a monotonic controller-owned tombstone;
- canonical append-only `managedLocationHistory` used only as a safety search domain;
- a separate `SystemKeyspaceReplicationReady` condition for post-Ready schema assertion/drift.

The discovery phase remains `Accepted` while the snapshot is valid. External drift sets the separate schema condition false with reason `ExternalReplicationDrift`, emits an Event only on transition, and blocks all operator-issued schema DDL for that reconciliation attempt.

The snapshot includes cluster UID, accepted generation, marker/protocol version, canonical accepted seeds and digest, authoritative endpoint, expected cluster name, source version, identity/topology/schema fingerprints, observed external DCs, three replication maps, fully qualified Secret bindings, discovery location, accepted managed locations, immutable worker image digest, acceptance time, and canonical hash.

Fully qualified Secret bindings contain purpose, source context, namespace, name, relevant key names, and resource version. TLS bindings follow the source semantics of the existing referenced TLS fields and are copied read-only into the attempt namespace. Secret bytes never enter the result or status.

## Admission and Version-Skew Safety

The reserved marker is injected only on CREATE by a typed mutating webhook. Client-supplied markers are rejected; update never injects a marker; removal/mutation is rejected. Both mutating and validating configurations use `failurePolicy: Fail`.

Packaged controller deployments retain Kubernetes `RollingUpdate`. Operators must wait for the operator Deployment rollout to complete before creating or modifying a legacy-RF migration resource. New multi-replica Pods use controller-runtime leader election.

This operational sequencing contract is required in both Kustomize and Helm. Rendered-package checks verify `RollingUpdate` and leader-election parity; they do not prove a live old/new upgrade. Manually operated mixed-version reconcilers are unsupported and must not be presented as safe.

## Discovery Execution Protocol

1. The early gate runs after deletion/finalizer/basic Cassandra validation and before superuser, replicated Secret, Reaper, Medusa, Stargate, managed DC, or Cassandra pod creation.
2. It canonicalizes IP literals using `netip`, rejects whitespace/zones/ports/FQDNs, unmaps IPv4-mapped IPv6, deduplicates by first occurrence, preserves spec order, and computes an order-sensitive digest for the accepted list.
3. It resolves the current first operator-owned DC's context and namespace.
4. It authoritatively checks current plus historical managed locations with direct Kubernetes readers. Any read error blocks; cache absence is never treated as authoritative.
5. It creates a unique attempt ConfigMap, HMAC Secret, copied credential/TLS Secret, ServiceAccount, resourceName-scoped Role/RoleBinding, and bounded Job.
6. The Job invokes `/legacy-rf-discovery` from the exact operator image digest. The manager resolves its running image digest from its own Pod `status.containerStatuses[].imageID`, normalizes it to an immutable digest reference, and binds it into attempt/result. Failure to obtain a digest blocks rather than using a mutable tag.
7. The worker opens one fresh endpoint-pinned CQL session for the next seed in normalized spec order. Peer discovery/load balancing is disabled using the Phase 4.7-grounded driver API so a query intended for seed A cannot execute on seed B.
8. That session performs complete reachability/auth/TLS, exact cluster-name, before/keyspaces/after, version, topology, schema, and RF validation. On failure it closes and tries the next endpoint. On success it accepts immediately, closes, and never opens later sessions.
9. The accepted endpoint's fingerprints must be identical before and after the three replication rows. It must report Apache Cassandra `>=4.0.0`, non-null consistent identity/topology/schema data, no managed-name collision, and three existing NTS keyspaces.
10. RF values accept ASCII base-10 digits only in `1..2147483647`, stored as `int32`. Sparse per-keyspace omission is preserved.
11. The worker produces canonical, bounded JSON and HMAC-SHA-256 signs it. The controller constant-time verifies the MAC, digest-pinned image binding, UID/generation/marker/attempt/seed/Secret bindings, payload schema, size, and canonical hash.

The worker issues only `SELECT` queries. Its source-facing interface contains one complete endpoint-attempt operation and no mutation operation. Per-endpoint failures remain internal while a fallback remains; exhaustion maps the ordered failures to one deterministic public reason.

## Snapshot Acceptance Sequence

Acceptance is an explicit controller operation, not a repository abstraction:

1. Read the `K8ssandraCluster` through the uncached control-plane reader.
2. Revalidate UID, current marker, attempt generation, canonical seed digest, Secret bindings, worker digest, current planned names, and result hash.
3. Survey all current planned and historical managed locations through direct remote readers; require no managed DC or correlated managed Cassandra pod.
4. Optimistically patch the complete snapshot and current accepted locations into status.
5. Stop reconciliation unconditionally.
6. On a later reconcile, uncached-read the object and verify the accepted snapshot hash before any consumption.

No managed DC may be created in the reconcile that writes the snapshot.

## Current-Plan Compatibility

One pure `ValidateCurrentPlan(snapshot, currentSpec)` function is authoritative:

- allow reorder, decommission/removal, and non-colliding addition of operator-owned DCs;
- require every current managed DC name to remain case-sensitively disjoint from the accepted external DC set;
- allow accepted generation to differ from current generation when this predicate passes;
- retain the accepted seed set regardless of later `additionalSeeds` edits/removal;
- reject incompatible expected cluster-name or server-type changes;
- if `externalDatacenters` is supplied, require its exact set to match the accepted observed external set; it never supplies RF values;
- never treat accepted/history locations as desired topology.

The append-only location history records locations in which this controller has authorized or observed managed DC creation. It is updated monotonically before/with creation authorization and is not pruned when a DC is decommissioned.

## Final Managed-DC Creation Authorization

Immediately beside `remoteClient.Create(desiredDc)`, run one named operation:

1. uncached-read the cluster;
2. verify UID, marker, snapshot/read-back hash, accepted seed digest, and `ValidateCurrentPlan`;
3. direct-read every exact managed DC and correlated pod location in the union of current plans, accepted locations, and append-only history;
4. if all reads prove absence, update the monotonic location history for the target if necessary and read it back;
5. immediately call `Create` using the accepted canonical seed set.

Any read error blocks. `AlreadyExists` is not treated as success; it causes a fresh full survey. Kubernetes name uniqueness is the final atomic boundary, while leader election, status CAS, tombstone/history, and repeated direct checks provide defense in depth.

## Snapshot-Loss Recovery

- Missing/corrupt snapshot plus `managedCreationObserved=false` and authoritative absence across current plus historical locations: delete stale attempt resources and run a wholly new discovery attempt.
- Missing/corrupt snapshot plus `managedCreationObserved=true`, any managed DC/pod, or an incomplete/unavailable absence survey: permanent `SnapshotConflict`; never rediscover.
- Privileged deletion of the entire status/tombstone and every managed resource is outside the controller's trust model; normal users must not have status-subresource mutation rights, and admission rejects user mutation where applicable.

## Post-Ready Schema Reconciliation

The existing `ManagementApiFactory` remains injected. After selecting a Ready managed DC, reconciliation obtains the facade for that lifecycle; it is not constructed at process startup.

For accepted discovery objects:

1. read all three live keyspace maps before any DDL;
2. require every keyspace to exist and use a supported NTS alias;
3. parse every present RF in `1..MaxInt32` and preserve omission;
4. compare the exact external projection against the snapshot for all three keyspaces;
5. on any mismatch, set the separate schema condition to `ExternalReplicationDrift` and issue zero DDL;
6. otherwise merge only current desired managed entries and call direct `AlterKeyspace` for changed maps;
7. after a partial ALTER failure, retry from fresh reads of all three.

The marked path cannot call `EnsureKeyspaceReplication`, `CreateKeyspaceIfNotExists`, or a create-capable abstraction. The unmarked legacy path remains unchanged.

## Failure Contract

Every boundary failure maps to a closed record containing public reason, retryability, sanitized corrective action, deterministic precedence, and a private wrapped cause. Raw causes, Secret material, certificates, keys, auth payloads, DSNs, and result bodies never enter status or Events.

The implementation plan must enumerate the complete matrix for admission, unsupported server/source/provider, invalid seeds, Job scheduling/image pull/deadline, Secret/TLS material, authentication, authorization, reachability, identity, topology/schema stability, missing/invalid keyspaces, stale/forged/oversize results, API unavailability/conflict, snapshot loss, managed-state presence, and external drift.

Identical phase/reason/message does not update transition time or emit another Event. Recoverable failures use bounded exponential backoff with jitter and Secret/Job/result watches.

## Dependency Injection

Use existing manual construction and only lifecycle-accurate seams:

```go
type DiscoveryAttempts interface {
    Ensure(context.Context, discovery.Attempt) (discovery.AttemptState, error)
    Result(context.Context, discovery.Attempt) (*discovery.SignedResult, error)
    Cleanup(context.Context, discovery.Attempt) error
}

type ManagedDatacenterState interface {
    AssertAbsent(context.Context, []discovery.ManagedLocation, bool) error
}

type EndpointObserver interface {
    DiscoverCandidate(context.Context, netip.AddrPort, Connection) (Candidate, error)
}
```

Retain the existing `ManagementApiFactory`, Kubernetes clients, recorder, clock, and image registry boundaries. Do not add `SnapshotRepository`; status acceptance remains explicit controller code. Constructors reject nil dependencies and invalid bounds. No new package globals are allowed.

## Delivery Waves

### Wave A — contracts

- API input/status/snapshot/condition types and explicit three-keyspace representation.
- Marker, rollout, protocol, canonicalization, qualified Secret binding, tombstone/history, and current-plan contracts.
- Pure IP/RF/hash/HMAC/fingerprint/result validation and complete failure matrix.
- `ClientCache` direct-reader pair contract.

Freeze these contracts before generation or adapter work.

### Wave B — independent adapters

- CQL worker and sequential endpoint-pinned fallback-to-first-valid authority.
- Kubernetes attempt resources, result authentication, digest-pinned worker identity, cleanup, and watches.
- Typed admission and marker immutability.
- Direct managed-state reader plus explicit acceptance/create authorization.
- Snapshot-aware post-Ready read-all/validate-all/direct-ALTER branch.

Assign non-overlapping file ownership; controller integration that shares files is sequential, not falsely parallel.

### Wave C — integration and release proof

- Insert early controller gate and composition-root wiring.
- Accepted seed consumption and final create authorization.
- Dockerfile, RollingUpdate rollout, leader election, RBAC, webhooks, generated CRD/deepcopy, Kustomize/Helm parity, and current CRD docs.
- Multi-cluster envtest for admission, CAS/read-back, restart, tombstone/history, rediscovery boundary, races, recovery, and redaction.
- Real old/new packaged rolling upgrade test.
- Three real Cassandra lifecycle scenarios aligned with the existing 4.0, 4.1, and 5.0 E2E test lines, including sparse maps and `system_auth` RF `2/9/2/10/10`.
- Existing migration, troubleshooting, and encryption/security docs.

## Mandatory Verification Boundaries

Release verification explicitly includes:

- new webhook plus old reconciler version-skew safety;
- ordered A-fails/B-succeeds/C-skipped authority plus deterministic all-seed exhaustion;
- lazy fresh endpoint-pinned sessions proving no driver redirection and no session/query after success;
- first discovery-location DC reorder/removal after acceptance;
- pre-managed snapshot corruption causing rediscovery;
- post-managed corruption after the historical location leaves current spec causing permanent block;
- every generation/seed/Secret/TLS/planned-target race around acceptance and creation;
- immutable worker digest and fully qualified Secret bindings;
- all-three prevalidation and zero DDL on external drift;
- the three repository-aligned Cassandra 4.0.17, 4.1.9, and 5.0.6 lifecycle scenarios.

Local source, unit, envtest, rendered-manifest, and generated-schema evidence is not proof of live Cassandra bootstrap preservation, ring health, ownership, streaming, repair, or data availability.

## Duel Decision Record

**Proposal A position:** Minimize packages and place orchestration in the existing controller with one worker and one discovery package.

**Proposal B position:** Add a scoped hexagonal core and explicit ports for attempts, snapshots, managed state, source CQL, schema, and retries.

**Challenger critique:** Both missed mixed-version rollout safety, historical safety provenance, exact plan compatibility, qualified Secret bindings, accepted-versus-drift status separation, immutable worker identity, and decisive release cases. Proposal B additionally introduced lifecycle-invalid and speculative abstractions.

**Synthesis rationale:** Proposal A best matches this mature brownfield controller. The synthesis adopts Proposal B's narrow source/schema safety ideas only where they correspond to real external lifecycles, adds all challenger mitigations, and keeps non-atomic Kubernetes/status ordering explicit rather than hidden behind repository terminology.
