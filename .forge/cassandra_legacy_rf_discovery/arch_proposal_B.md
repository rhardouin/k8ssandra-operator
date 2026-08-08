# Architecture Proposal B: Extensible, Fail-Closed Legacy RF Discovery

> **Historical proposal:** `task_review.md` superseded this proposal's all-seed validation/concurrent candidate semantics. The authoritative `design.md` now requires lazy sequential fallback in normalized spec order, first complete valid acceptance, skipped remainder, and deterministic exhaustion.

## Constraint Applied

**Constraint:** Favor extensibility through explicit ports/adapters and safety boundaries, without inventing product scope or replacing the repository's established controller structure.

This proposal uses a **scoped hexagonal boundary inside the existing operator**, not a repository-wide Clean Architecture rewrite. The controller remains the level-based orchestrator; Kubernetes admission/status remain the public API; `ClientCache`, `ReconcileResult`, watched-by labels, `ManagementApiFactory`, Kubebuilder generation, and multi-cluster envtest remain the brownfield integration mechanisms. A small transport-neutral `pkg/discovery` core is shared by the controller and a fresh CQL worker. Concrete Kubernetes, CQL, and Management API adapters remain at the edges.

The extensibility constraint changes the design only at seams already proven likely to vary:

- discovery execution is behind a workload port so a later transport does not alter controller policy;
- CQL access is behind a read-only source port so the driver remains replaceable and mockable;
- remote authoritative reads are behind a managed-state port so cache mechanics do not leak into safety policy;
- post-Ready schema access is narrowed to read plus direct alter, making creation unrepresentable;
- canonical result encoding is versioned, but contains no speculative fields or alternate transports.

The following resolved product decisions are preserved exactly: discovery runs in the first planned managed DC's data plane; a dedicated pre-existing Secret supplies optional legacy credentials; FQDN seeds and external secret injection are unsupported; every seed must be reachable and report the expected cluster name; the first complete successful response is authoritative for all other data; sparse maps and RF values through `2147483647` are preserved; a valid accepted snapshot is immutable for the object UID; pre-managed missing/corrupt status may rediscover, post-managed corruption may not; external drift blocks without DDL; DSE/HCD are `NotRequired/UnsupportedServerType`; Cassandra before 4.0 blocks; only the three named system keyspaces are in scope.

## Complexity Assessment

The architecture guide classifies this as **complex** and recommends hexagonal/DDD-style boundaries: 13 entities, 5 stories, 8 external runtime dependencies, concurrent controller/worker operations, 40 invariants, 38 edge cases, and 11 security boundaries. The duel threshold is met by both entity and story counts, and the CQL/Kubernetes anti-corruption boundaries independently justify a duel.

The chosen pattern is **brownfield-aligned controller pipeline plus scoped ports/adapters**:

- aggregate/root: `K8ssandraCluster` and its discovery status;
- application service: discovery gate in `controllers/k8ssandra`;
- domain core: canonicalization, validation, fingerprints, result/snapshot hashing, and managed-only replication planning in `pkg/discovery`;
- driven adapters: CQL source, Kubernetes Job/result resources, uncached managed-state reads, status CAS, and existing Management API;
- driving adapters: typed admission and the existing reconcile pipeline.

An actor model or generic workflow engine is wrong here. Kubernetes reconciliation and Jobs already provide durable scheduling and retry. A new controller/API group for attempts is also unjustified: one bounded Job plus controller-owned support resources is enough.

Complexity is contained to **7 feature Go package areas (3 new, 4 existing), plus root `main.go` wiring** and generated/package manifests. There are **6 new feature-facing interfaces**, each with at most three methods. There are **10 architecture decisions** below.

## Package Structure

```text
apis/k8ssandra/v1alpha1/                     # existing package; public API and admission
  k8ssandracluster_types.go                  # discovery config/status/snapshot/reason types
  constants.go                               # reserved marker/labels/attempt protocol version
  k8ssandracluster_webhook.go                # create marker, create/update validation

pkg/discovery/                               # NEW; transport-neutral policy/core
  errors.go                                  # typed failures and stable reason mapping
  ports.go                                   # feature-facing interfaces
  types.go                                   # attempts, endpoints, observations, snapshots
  canonical.go                               # IP/map canonicalization and bounded integer parsing
  validate.go                                # identity/topology/schema/result validation
  fingerprint.go                             # deterministic digests and before/after comparison
  protocol.go                                # versioned canonical result envelope and MAC verification
  replication.go                            # external prevalidation and managed-only alter plans

pkg/discovery/cql/                           # NEW; CQL adapter selected/pinned in Phase 4.7
  source.go                                  # independent endpoint sessions, read-only queries, closure
  tls.go                                     # mounted trust/client identity, IP verification

cmd/legacy-rf-discovery-worker/              # NEW; fresh process in the operator image
  main.go                                    # parse bounded args/files, run discovery, publish signed result

controllers/k8ssandra/                       # existing application/Kubernetes adapter package
  legacy_rf_discovery.go                     # qualification, attempt lifecycle, status/state machine
  legacy_rf_discovery_kubernetes.go          # Job/SA/Role/Binding/Secret/ConfigMap adapter
  legacy_rf_discovery_snapshot.go            # API status mapping and optimistic acceptance/read-back
  k8ssandracluster_controller.go             # early gate and watches
  datacenters.go                             # accepted seeds and final create-time authorization
  schemas.go                                 # discovery-aware read-all/validate-all/direct-ALTER branch

pkg/clientcache/                             # existing multi-cluster client registry
  cache.go                                   # retain/expose local and remote direct API readers

pkg/cassandra/                               # existing post-Ready Cassandra adapter
  management.go                              # existing GetKeyspaceReplication/AlterKeyspace reused

main.go                                      # construct adapters, inject ports, enable leader election
Dockerfile                                   # build/copy manager and worker binaries
config/ and charts/k8ssandra-operator/       # generated CRD/RBAC/webhooks and matching Helm delivery
docs/content/en/tasks/{migrate,troubleshoot}/# existing user-facing homes
docs/content/en/tasks/secure/encryption/      # credential/TLS constraints
test/e2e/                                    # actual Cassandra lifecycle release gates
```

Dependency direction is acyclic:

```text
apis ──────────────────────────────────────────────────────────────┐
pkg/discovery/cql ──> pkg/discovery <── cmd/worker                 │
controllers/k8ssandra ──> apis + pkg/discovery + clientcache       │
controllers/k8ssandra ──> pkg/cassandra                            │
main ──> controllers + concrete adapters                           │
```

`pkg/discovery` imports neither controller nor API packages. Controllers explicitly map wire/domain snapshots into the Kubernetes status representation. This small duplication prevents worker protocol changes from silently becoming CRD wire-format changes. `pkg/discovery/cql` depends inward on the read-only source contract. No domain package depends on Kubernetes clients or a concrete CQL driver.

### Reusable brownfield abstractions

- `controllers/k8ssandra/k8ssandracluster_controller.go:131-192`: retain the sequential `ReconcileResult` pipeline and insert the gate before `reconcileSuperuserSecret` at lines 149-151.
- `pkg/result`: return `Done`, `RequeueSoon`, or sanitized `Error` consistently; expected discovery blocks do not flow through the raw generic `.status.error` path at controller lines 115-125.
- `pkg/clientcache/cache.go:31-50,138-156`: extend the existing cache instead of creating a second multi-cluster registry.
- `pkg/labels` and `apis/k8ssandra/v1alpha1/constants.go:61-62`: reuse watched-by correlation labels; cross-cluster owner references remain forbidden.
- `pkg/cassandra/management.go:205-246`: reuse live `GetKeyspaceReplication` and direct `AlterKeyspace`; do not reuse `EnsureKeyspaceReplication` at lines 288-317.
- `controllers/k8ssandra/schemas.go:394-402`: reuse the optimistic-lock precedent, strengthened with uncached read-back.
- `pkg/test`, `MultiClusterTestEnv`, and Testify mocks: retain established test and DI patterns.
- existing migration, troubleshooting, encryption, and generated CRD documentation locations: update rather than create a parallel architecture document for users.

### Exact integration seams

- API types: `apis/k8ssandra/v1alpha1/k8ssandracluster_types.go:237-278`, adjacent to `AdditionalSeeds` and client encryption.
- Admission: register the existing typed defaulter and validator at `k8ssandracluster_webhook.go:60-89`; replace the current package-global dependency for the new path with constructed defaulter/validator instances.
- Early gate: `k8ssandracluster_controller.go:131-151`, after nil/basic checks and before any non-discovery resource creation.
- Watches: `k8ssandracluster_controller.go:237-325`, using watched-by labels for result/Job resources and Secret indexes for referenced credential/TLS changes.
- Accepted seed consumption: `datacenters.go:43-71,145-151`; marked objects bypass the flat initial replication/bootstrap path and EndpointSlices use snapshot seeds.
- Final creation gate: immediately before `remoteClient.Create` at `datacenters.go:244-255`.
- Post-Ready schema: branch at `schemas.go:33-46`; replace only the marked path around `schemas.go:168-212` while preserving the unmarked path.
- Direct readers: extend `clientcache/cache.go:20-50,73-92,138-140` so every registered context retains a direct `client.Reader`, not just the cached client.
- Wiring: `main.go:187-243`; build the six ports manually, following existing constructor injection.
- Delivery: regenerate `config/webhook/manifests.yaml`, RBAC, CRDs, deepcopy and current CRD docs; mirror them in Helm templates. Do not edit historical CRD reference snapshots.

## Data Flows

### US-1 — Preserve observed replication

`K8ssandraCluster CREATE` → mutating webhook injects reserved discovery marker → validator accepts IP-only qualifying Cassandra spec → early reconcile gate canonicalizes/deduplicates seeds and resolves the current first planned data plane → `AttemptRuntime` creates a fresh worker Job and scoped support resources → worker independently opens a fresh session to every seed → every seed passes reachability/auth/TLS and exact expected-name check → complete candidate discovery calls race; the first complete valid response becomes authoritative → the worker rereads authoritative identity/topology/schema after the three keyspaces → core verifies identical before/after fingerprints, supported Cassandra version, exact topology, managed-name collisions, an exact observed-set match when `spec.externalDatacenters` is supplied, NTS aliases, sparse maps, and RF `1..2147483647` → signed bounded result → controller verifies bindings/MAC and persists the three maps without DDL → accepted canonical seed set later feeds EndpointSlices, never bootstrap replication properties. Discovery never populates `externalDatacenters`, and that field never supplies RF values.

### US-2 — Fail before managed creation

Qualifying reconcile → `ManagedStateReader.AssertAbsent` uses direct readers in every current planned context → any managed DC/pod, invalid seed, unreachable endpoint, auth/TLS failure, wrong/null expected name, no complete authoritative candidate, unstable authoritative fingerprint, malformed/unknown topology, missing keyspace, unsupported strategy/version, invalid RF, stale/forged result, or admission marker violation → typed `DiscoveryFailure` → sanitized `Blocked` phase/reason/message and transition-only Event → bounded requeue where recoverable → **no superuser/Reaper/Medusa/replicated secret, managed DC, Cassandra pod, or schema DDL**. DSE/HCD instead follow their existing path with `NotRequired/UnsupportedServerType` and no preservation claim.

### US-3 — Keep one durable snapshot

Verified terminal result → fresh control-plane object read plus direct absence survey → compare UID/generation/marker, canonical seed digest, Secret resource versions, TLS resource versions, accepted/current planned-target bindings → optimistic status CAS → stop reconcile → fresh API read-back → hash/UID/marker validation → later reconcile treats the snapshot as immutable. If snapshot is missing/corrupt and the direct survey finds no managed DC/pod, discard support resources and start full discovery. If any managed state exists, return permanent `SnapshotConflict`/`DiscoveryTooLate`; never rediscover. Immediately before each first managed-DC create, repeat fresh object and union-of-accepted/current-target absence checks, then use only accepted seeds.

### US-4 — Diagnose and recover safely

Raw CQL/Kubernetes/driver failure at adapter → classified typed failure with private cause retained only in restricted structured logs → public status/Event receives stable reason and corrective action without endpoint credentials, Secret bytes, certificates, keys, DSNs, or raw auth/TLS payloads → `RetryScheduler` produces bounded exponential delay with jitter → watched credential/TLS Secret resource-version change or periodic requeue starts a wholly new worker attempt → corrected prerequisite reaches acceptance without recreating the cluster. Identical phase/reason does not emit another Event.

### US-5 — Avoid retroactive activation

Create admission available → server-owned marker is injected and validated; create admission unavailable → API request fails closed. Existing unmarked object on upgrade → `NotRequired` and unchanged legacy reconciliation. Client-forged marker or marker mutation/removal → validating webhook rejects. Marked qualifying object with managed state but no accepted snapshot → direct survey → `DiscoveryTooLate`, no attempt. Accepted object → marker/snapshot validation continues for the UID lifetime; ordinary post-accept managed topology edits remain allowed unless a current managed name collides with an observed external name or creation safety fails.

## Architecture Decisions

### D1 — Scoped hexagonal core inside the existing controller package

**Context:** The feature has multiple external boundaries, but the repository is organized by domain and uses a flat sequential reconciler rather than layers.
**Options:** Keep all logic in one controller file; introduce repository-wide Clean Architecture; add one transport-neutral core plus narrow adapters.
**Decision:** Add `pkg/discovery` and `pkg/discovery/cql`, while keeping orchestration and Kubernetes resources in `controllers/k8ssandra`.
**Consequences:** Pure invariants and protocols can be tested independently and reused by worker/controller; mappings add some code. No existing package is refactored.
**Constraint influence:** Extensibility justifies ports only at actual CQL/Kubernetes/schema seams.
**Align vs improve:** Aligns with package-by-domain and manual DI; improves external-boundary testability required by the quality gates.

### D2 — Fresh worker binary in the existing operator image

**Context:** Discovery must run from the first data plane before any managed pod, and the current image contains only `/manager`.
**Options:** Run CQL from the control plane; add a manager subcommand; build a separate worker binary/image; build a separate worker binary into the existing image.
**Decision:** Build `/legacy-rf-discovery-worker` from `cmd/legacy-rf-discovery-worker` and copy it beside `/manager` in the existing image. Each attempt creates a new Job/process and therefore a fresh driver/session lifecycle.
**Consequences:** No second image supply chain; clear executable contract and clean session state. Dockerfile and image tests must prove both binaries exist. A future image split changes only the workload adapter.
**Constraint influence:** The worker port isolates execution placement and image choice.
**Align vs improve:** Aligns with Kubernetes Job operation and existing image registry settings; improves the current single-binary image explicitly rather than assuming a nonexistent command.

The Job uses a per-attempt ServiceAccount and a Role restricted by `resourceNames` to patch one pre-created result ConfigMap. Credentials, TLS files, and a controller-generated 32-byte MAC key are mounted read-only; the worker cannot list Secrets and does not return secret bytes. A canonical JSON result is capped at 512 KiB, protocol-versioned, and HMAC-SHA-256 signed. The controller verifies the MAC plus all visible attempt bindings before accepting it. The worker Job has an active deadline, resource limits, non-root/read-only filesystem, dropped capabilities, and no privilege escalation. Support Secrets and terminal result objects are deleted immediately after ingestion; Jobs use a ten-minute TTL. Public diagnostics live in sanitized status, not retained raw results.

### D3 — Grouped credential API with fixed, local Secret contract

**Context:** Discovery authentication is independent from target authentication; namespace and key behavior must be unambiguous.
**Options:** Reuse `SuperuserSecretRef`; add a loose top-level Secret name; add a grouped discovery config.
**Decision:** Add optional `spec.cassandra.legacySystemReplicationDiscovery.credentialsSecretRef.name`. It is a `LocalObjectReference` to a pre-existing Secret in the first planned DC's resolved namespace/context, with required non-empty keys `username` and `password`. Absence means anonymous discovery. Cross-namespace references and generated credentials are unsupported. Existing `clientEncryptionStores` supplies TLS material; endpoint verification may not be disabled.
**Consequences:** Target auth remains independent; moving the first planned DC requires an equivalent Secret in the newly resolved location before acceptance. The group can host a later compatible config version, but no speculative transport or FQDN fields are added now.
**Constraint influence:** The grouped boundary avoids another breaking top-level field if the concrete discovery contract grows.
**Align vs improve:** Aligns with adjacent `AdditionalSeeds` and `LocalObjectReference`; improves safety by making location and keys exact.

### D4 — Two-stage per-seed checks with first complete authoritative response

**Context:** Resolved requirements require every seed to be reachable and match the expected name, while only one complete response supplies topology/schema/RFs. Cross-seed convergence comparison is explicitly out of scope.
**Options:** Query one seed only; merge all seeds; require all full responses to match; independently check every seed and accept the first complete valid candidate.
**Decision:** The worker opens independent bounded sessions. `CheckContact` validates reachability/auth/TLS and exact expected name for every canonical endpoint. `DiscoverCandidate` runs complete read-before/keyspaces/read-after discovery concurrently; the first complete valid candidate wins. Other endpoints contribute no topology, schema, release, or RF comparison. If none yields a complete candidate, discovery blocks.
**Consequences:** The authoritative endpoint may differ across retries before acceptance; its endpoint is recorded as provenance. Cassandra owns convergence. Tests must model the race and prove no union.
**Constraint influence:** Separate source-port methods keep the resolved policy explicit and allow later source transports without changing validation.
**Align vs improve:** Improves over the ambiguous broad CQL client gap; it does not change existing Cassandra reconciliation.

### D5 — Versioned authenticated result envelope and immutable status snapshot

**Context:** A ConfigMap writer can be stale or malicious, visible UID/generation bindings alone do not authenticate execution, and status is the durable authority.
**Options:** Trust labels; trust Job ownership; create a new CRD; authenticate a bounded result then copy canonical data to status.
**Decision:** Pre-create a result ConfigMap and one-time MAC Secret; accept only the signed versioned envelope bound to cluster UID, generation, marker, attempt ID, canonical seed digest, credential/TLS Secret resource versions, discovery location, and planned-target digest. Map it to controller-owned status using optimistic locking, stop, and read it back authoritatively before use.
**Consequences:** A principal that can read the one-time MAC Secret and patch the result can still forge it; namespace RBAC must prevent that. This limitation is documented. No new CRD/controller is required. Status remains the sole durable snapshot.
**Constraint influence:** Protocol versioning and a result-authentication seam permit compatible evolution without exposing worker internals in status.
**Align vs improve:** Aligns with ConfigMap/watched-label and status-CAS patterns; improves trust and stale-result handling.

### D6 — Dedicated authoritative managed-state reader backed by direct clients

**Context:** Cached remote reads cannot establish absence for the pre-acceptance and pre-create safety gates. Current `ClientCache` retains a local non-cache client but only cached remote clients.
**Options:** Accept cache freshness; create ad hoc clients per reconcile; minimally retain direct readers in `ClientCache`.
**Decision:** Extend `ClientCache` registration to retain a direct `client.Reader` for every remote context, with the existing local non-cache client for empty context. `ManagedStateReader.AssertAbsent` uses these direct readers to GET every exact planned/accepted CassandraDatacenter key and LIST correlated Cassandra pods in each relevant namespace. It never uses informer-cache absence.
**Consequences:** Client registration and tests change; no duplicate rest-config registry is introduced. An unavailable context/API is a blocking observation failure, never interpreted as absence.
**Constraint influence:** The domain port permits different freshness mechanisms later while policy continues to require authoritative absence.
**Align vs improve:** Aligns with `ClientCache`; improves its remote direct-read gap because correctness depends on it.

The check runs (a) before creating an attempt, (b) immediately before snapshot CAS, and (c) immediately before `remoteClient.Create(desiredDc)`. The final check fresh-reads the control-plane object and snapshot, then surveys the union of acceptance-bound targets and current planned targets. It verifies UID, generation where acceptance requires it, immutable marker, snapshot hash, accepted seed digest, compatible current names, credential/TLS bindings for the attempt, and no exact DC or correlated Cassandra pod. For the Kubernetes create race, leader election supplies a single active reconciler and the API server's create is the final atomic name-level arbiter; `AlreadyExists` causes a fresh survey, not success by assumption.

### D7 — Enable leader election for packaged multi-replica safety

**Context:** The binary supports leader election, but Kustomize/Helm default to one replica and Helm does not pass the flag. Status CAS alone does not serialize two reconcilers creating different generation-bound targets.
**Options:** Claim one replica only; design a new distributed lock; package controller-runtime leader election.
**Decision:** Enable controller-runtime leader election in supported packaged deployments and document that mixed old/new reconcilers are unsupported. Preserve status CAS and all final checks as defense in depth.
**Consequences:** RBAC/Lease permissions and Helm/Kustomize parity are release gates; leader failover tests must cover restart between acceptance/read-back/create.
**Constraint influence:** Uses the existing concurrency seam rather than embedding locks into discovery policy.
**Align vs improve:** Aligns with `main.go` support; improves unsafe deployment defaults for this feature.

### D8 — Make create capability absent from discovery and schema ports

**Context:** `EnsureKeyspaceReplication` can create a missing keyspace, violating the contract. The existing Management API facade is too broad for the marked path.
**Options:** Call the existing helper with guards; add flags to it; expose a two-method schema port.
**Decision:** `SystemKeyspaceSchema` exposes only `ReadSystemReplication` and `AlterSystemReplication`. Its adapter delegates to existing `GetKeyspaceReplication` and direct `AlterKeyspace`. Marked reconciliation reads and validates all three maps before computing any alters; missing/strategy/external drift yields zero DDL.
**Consequences:** The unmarked path remains untouched. Partial managed-only ALTER retries reread all three. Compile-time interface shape and mocks prove no create method is available.
**Constraint influence:** A narrow port allows schema assertions to grow without coupling policy to the nine-method facade.
**Align vs improve:** Reuses the existing Management API implementation but improves capability safety for the new branch.

### D9 — Typed state machine, deterministic reason precedence, bounded recovery

**Context:** The current reconcile wrapper copies raw errors to public status/Events; stable sanitized reasons and predictable retries are required.
**Options:** Return ordinary errors; add scattered condition updates; centralize typed state transitions.
**Decision:** Expected discovery outcomes are `DiscoveryFailure` values consumed by one state-transition function. Precedence is: tampering/stale binding → managed state too late/snapshot conflict → unsupported configuration/version → TLS/auth/authz → reachability → identity → topology/schema → keyspace/strategy/RF → workload/result infrastructure. The first applicable reason within that ordering is public; private causes remain wrapped internally.
**Consequences:** Tests can assert exact phase/reason; transition-only Events suppress noise. Retry policy marks immutable/tamper/post-managed conflicts permanent and network/Secret/workload failures retryable with bounded exponential backoff plus jitter.
**Constraint influence:** The failure contract is an extension seam for additional assertions without leaking adapter errors.
**Align vs improve:** Aligns with status-driven reconciliation; deliberately improves raw-error exposure.

### D10 — Preserve public API compatibility and documentation truth boundaries

**Context:** Discovery must be automatic for new marked objects and invisible to unmarked upgrades; generated manifests and live proof are part of correctness.
**Options:** User opt-in field; retrofit existing objects; admission-owned version marker.
**Decision:** The mutating create webhook first rejects a marker already present in the submitted object, then injects an internal reserved annotation/marker (for example `k8ssandra.io/legacy-rf-discovery-version: v1`). The validating update path rejects mutation/removal. Updates never add it. Admission configurations use `failurePolicy: Fail`.
**Consequences:** New qualifying objects gate automatically; old objects retain legacy behavior. Kustomize and Helm must install identical webhooks/RBAC. Docs state that source/unit/manifest evidence is not proof of live bootstrap, ring health, ownership, streaming, repair, or availability.
**Constraint influence:** A versioned marker permits future compatible admission behavior without retroactive inference.
**Align vs improve:** Aligns with typed Kubebuilder admission and existing docs homes; improves setup from validator-only to fail-closed mutator plus validator.

## Error Types

Error contracts are defined before interfaces. Public status consumes only stable `DiscoveryReason` plus sanitized action; raw causes never cross the status/result/logging boundary.

```go
// pkg/discovery/errors.go
type Failure struct {
    Reason    Reason
    Retryable bool
    Action    string // fixed sanitized remediation text
    Cause     error  // private; never serialized
}

func (e *Failure) Error() string
func (e *Failure) Unwrap() error

type ConflictError struct { Operation string; Cause error }       // retry fresh state
type ManagedStatePresentError struct { Targets []ManagedTarget }  // DiscoveryTooLate
type InvalidResultError struct { Kind ResultViolation; Cause error }
type ExternalDriftError struct { Keyspace string; Datacenter string }
```

`Reason` is a closed string type with at least: `UnsupportedServerType`, `UnsupportedSourceVersion`, `DiscoveryTooLate`, `InvalidContactPoint`, `ContactUnreachable`, `AuthenticationRejected`, `AuthorizationDenied`, `TLSFailed`, `UnsupportedSecretsProvider`, `IdentityMismatch`, `SchemaDisagreement`, `TopologyInconsistent`, `ManagedDatacenterNameCollision`, `MissingKeyspace`, `UnsupportedStrategy`, `InvalidReplication`, `StaleDiscoveryResult`, `SnapshotConflict`, and `ExternalReplicationDrift`. Internal-only distinctions such as Job scheduling, timeout, malformed envelope, bad MAC, oversized result, API unavailability, and optimistic conflict map deterministically to these public reasons plus a sanitized action.

Unexpected errors are wrapped at each adapter boundary with operation, object namespaced name, attempt ID, and context name. They must not contain Secret values, full DSNs, raw auth responses, certificate/private-key bytes, or result bodies. Context cancellation/deadline errors remain detectable with `errors.Is`.

## Interfaces

All new feature interfaces are consumer-owned, domain-named, and have at most three methods.

```go
// pkg/discovery/ports.go
type AttemptRuntime interface {
    Reconcile(ctx context.Context, spec AttemptSpec) (AttemptObservation, error)
    Delete(ctx context.Context, key AttemptKey) error
}

// Implemented by the CQL adapter inside the worker. No write/create method exists.
type SourceDiscovery interface {
    CheckContact(ctx context.Context, endpoint Endpoint, expectedCluster string) error
    DiscoverCandidate(ctx context.Context, endpoint Endpoint, expectedCluster string) (SourceObservation, error)
}

type ManagedStateReader interface {
    AssertAbsent(ctx context.Context, cluster ClusterRef, targets []ManagedTarget) error
}

type SnapshotRepository interface {
    LoadFresh(ctx context.Context, cluster ClusterRef) (ClusterState, error)
    Accept(ctx context.Context, expected ClusterStateVersion, snapshot Snapshot) error
    ReadAccepted(ctx context.Context, cluster ClusterRef) (Snapshot, error)
}

type SystemKeyspaceSchema interface {
    ReadSystemReplication(ctx context.Context) (SystemReplication, error)
    AlterSystemReplication(ctx context.Context, keyspace SystemKeyspace, replication ReplicationMap) error
}

type RetryScheduler interface {
    Now() time.Time
    NextDelay(attempt int) time.Duration
}
```

`io.Reader` is additionally constructor-injected for cryptographic nonce/MAC-key generation; it is a standard library interface, not a new feature contract. Concrete adapters may depend on `client.Reader`, `client.StatusWriter`, controller-runtime `client.Client`, a Phase-4.7-grounded CQL driver, and the existing `cassandra.ManagementApiFacade`, but those infrastructure types do not enter the core contracts.

## Types

### Public API representation

```go
type LegacySystemReplicationDiscoveryConfig struct {
    CredentialsSecretRef *corev1.LocalObjectReference `json:"credentialsSecretRef,omitempty"`
}

type LegacySystemReplicationDiscoveryStatus struct {
    Phase              DiscoveryPhase          `json:"phase"`
    Reason             DiscoveryReason         `json:"reason,omitempty"`
    Message            string                  `json:"message,omitempty"`
    ObservedGeneration int64                   `json:"observedGeneration,omitempty"`
    TransitionTime     metav1.Time             `json:"transitionTime,omitempty"`
    SnapshotHash       string                  `json:"snapshotHash,omitempty"`
    Snapshot           *LegacyReplicationSnapshot `json:"snapshot,omitempty"`
}

type LegacyReplicationSnapshot struct {
    ClusterUID             types.UID                         `json:"clusterUid"`
    AcceptedGeneration     int64                             `json:"acceptedGeneration"`
    MarkerVersion          string                            `json:"markerVersion"`
    AcceptedSeeds          []string                          `json:"acceptedSeeds"`
    SeedDigest             string                            `json:"seedDigest"`
    CredentialResourceVersion string                         `json:"credentialResourceVersion,omitempty"`
    TLSResourceVersions    map[string]string                 `json:"tlsResourceVersions,omitempty"`
    ExpectedClusterName    string                            `json:"expectedClusterName"`
    SourceVersion          string                            `json:"sourceVersion"`
    AuthoritativeEndpoint  string                            `json:"authoritativeEndpoint"`
    DiscoveryLocation      ManagedTarget                     `json:"discoveryLocation"`
    AcceptedManagedTargets []ManagedTarget                   `json:"acceptedManagedTargets"`
    IdentityFingerprint    string                            `json:"identityFingerprint"`
    TopologyFingerprint    string                            `json:"topologyFingerprint"`
    SchemaFingerprint      string                            `json:"schemaFingerprint"`
    ExternalDatacenters    []string                          `json:"externalDatacenters"`
    Replication            map[SystemKeyspace]ReplicationMap `json:"replication"`
    AcceptedAt             metav1.Time                       `json:"acceptedAt"`
    Hash                   string                            `json:"hash"`
}
```

The actual API representation should use explicit fields for the three supported keyspaces if Kubebuilder map-key schema generation cannot express the closed keyspace set safely. The domain type remains a closed `SystemKeyspace` enum. Snapshot hashing excludes `Hash` itself, uses sorted canonical seeds/DCs/Secret bindings/targets, and preserves map omission. Status size is checked before CAS.

### Core and worker protocol

```go
type Endpoint struct { Address netip.Addr; Port uint16 }
type AttemptKey struct { ClusterUID types.UID; AttemptID string }
type ClusterRef struct { Namespace, Name string; UID types.UID }
type ManagedTarget struct { Context, Namespace, Name string }
type SecretBinding struct { Name, ResourceVersion string }

type AttemptSpec struct {
    Key AttemptKey
    Generation int64
    MarkerVersion string
    ExpectedClusterName string
    Seeds []Endpoint
    SeedDigest string
    DiscoveryLocation ManagedTarget
    PlannedTargets []ManagedTarget
    Credential *SecretBinding
    TLSBindings []SecretBinding
    Deadline time.Duration
}

type AttemptObservation struct {
    Phase AttemptPhase // Pending, Failed, Complete
    Envelope []byte
    Failure *Failure
}

type SourceObservation struct {
    Endpoint Endpoint
    ClusterName string
    SourceVersion string
    Partitioner string
    SchemaVersion string
    Hosts []ObservedHost
    Before Fingerprints
    Replication SystemReplication
    After Fingerprints
}

type ReplicationMap map[string]int32
type SystemReplication struct {
    SystemAuth ReplicationMap
    SystemTraces ReplicationMap
    SystemDistributed ReplicationMap
}

type ResultEnvelope struct {
    ProtocolVersion string
    Bindings ResultBindings
    Observation SourceObservation
    ContactChecks []ContactCheck
    Signature string
}
```

Canonical IPs use `netip.Addr.Unmap()` so IPv4 and IPv4-mapped IPv6 collide, are deduplicated, sorted by canonical bytes for the accepted set/digest, and rendered without ports/zones. Each worker process creates and closes independent endpoint sessions. RF parsing accepts ASCII base-10 digits only, no sign/whitespace/decimal, range `1..math.MaxInt32`, then stores `int32` so host word size is irrelevant.

The state transition table is explicit:

| Current | Input | Next | Side effect |
|---|---|---|---|
| unmarked/nonqualifying | reconcile | `NotRequired` | legacy path only |
| no snapshot, no managed state | no/active attempt | `Pending` | reconcile support resources |
| `Pending`/retryable `Blocked` | terminal failure | `Blocked` | sanitized status/Event/requeue |
| no snapshot, valid result | CAS + read-back | `Accepted` | persist once, stop reconcile |
| valid `Accepted` | later reconcile | `Accepted` | never rediscover |
| missing/corrupt snapshot, no managed state | reconcile | `Pending` | full new attempt |
| missing/corrupt snapshot, managed state | reconcile | `Blocked` | permanent `SnapshotConflict` |
| marked/no snapshot, managed state | reconcile | `Blocked` | `DiscoveryTooLate` |

## Constructors

Constructors validate non-nil dependencies and bounds; no package globals are introduced.

```go
func NewService(
    attempts AttemptRuntime,
    managedState ManagedStateReader,
    snapshots SnapshotRepository,
    scheduler RetryScheduler,
    nonce io.Reader,
    limits Limits,
) (*Service, error)

func NewWorker(
    source SourceDiscovery,
    publish PublishResultFunc, // worker-local function; update-only fixed ConfigMap
    nonceKey []byte,
    limits Limits,
) (*Worker, error)

func NewKubernetesAttemptRuntime(
    clients *clientcache.ClientCache,
    scheme *runtime.Scheme,
    imageRegistry cassimages.ImageRegistry,
    scheduler RetryScheduler,
) (AttemptRuntime, error)

func NewKubernetesManagedStateReader(
    clients *clientcache.ClientCache,
) (ManagedStateReader, error)

func NewKubernetesSnapshotRepository(
    apiReader client.Reader,
    statusWriter client.StatusWriter,
) (SnapshotRepository, error)

func NewManagementSchemaAdapter(
    facade cassandra.ManagementApiFacade,
) (SystemKeyspaceSchema, error)

func NewCQLSource(config CQLConfig) (SourceDiscovery, error)
```

`PublishResultFunc` is a function type, not a seventh interface, because only the Kubernetes fixed-ConfigMap protocol exists; tests inject a recording function. If it becomes a second real seam, extract it then. `Limits` contains named internal bounds for connection/query/attempt deadline, result size, retry ceiling, and backoff; it has validated defaults and no user-facing knobs in the first release. The CQL driver and its constructor APIs remain unresolved until Phase 4.7 dependency grounding; no version is proposed here.

## Wave Hints

### Wave A — Foundation

- API config/status/snapshot/enums, reserved constants, deepcopy/CRD schema design.
- `pkg/discovery` errors, six ports, core types, canonical encoding, IP/RF parsing, fingerprints, state transition table, reason precedence.
- `ClientCache` direct-reader registration contract and unit tests.
- Worker/result protocol, MAC/hash test vectors, size/deadline constants.
- Admission contract tests for marker ownership and qualification.

Wave A establishes all compile-time contracts before concrete adapters. API generation should occur after the type contract stabilizes, not in parallel with competing type edits.

### Wave B — Core/adapters, parallelizable

- **B1:** CQL source adapter and worker candidate-selection service; driver version/API must come from Phase 4.7.
- **B2:** Kubernetes attempt runtime: scoped SA/Role/Binding, MAC Secret, Job, result ConfigMap, cleanup and watches.
- **B3:** Snapshot repository plus authoritative managed-state reader using direct readers.
- **B4:** Pure result/snapshot validation, hash verification, stale-binding rejection and rediscovery boundary.
- **B5:** Managed replication planner and narrow Management API schema adapter.
- **B6:** Typed mutating/validating admission and immutable marker behavior.
- **B7:** Documentation draft against final API names: migration, troubleshooting, encryption/security, live-proof limitation.

Each B task depends only on Wave A contracts. No B module should import another B adapter.

### Wave C — Integration

- Insert discovery service before secret/component creation in the aggregate reconcile pipeline.
- Wire accepted seed use and final fresh create-time authorization in `datacenters.go`.
- Wire discovery-aware post-Ready schema branch while preserving the unmarked legacy branch.
- Construct adapters in `main.go`; build/copy the worker; enable/package leader election.
- Add result/Job/Secret watches, RBAC, webhook registration, Helm/Kustomize parity, generated CRDs/deepcopies/current docs.
- Add multi-cluster envtest coverage, then real Cassandra lifecycle, restart/multi-replica, mutation-race, auth/TLS/redaction and upgrade gates.

### Hierarchical delegation candidates

- `controllers/k8ssandra` integration and envtest work is expected to exceed 10 tasks because it spans early gating, attempt lifecycle, CAS/read-back, three absence checkpoints, watches, retries, cleanup, accepted seeds, and schema branching. It is a candidate for hierarchical delegation split by owned files.
- Real lifecycle/e2e verification is expected to exceed 10 tasks across source/target matrices, RF `2/9/2/10/10`, sparse maps, restart, multi-replica, mutations, TLS/auth, and upgrade behavior. It is a separate hierarchical candidate.
- Generated CRD/RBAC/webhook/Helm artifacts are mechanical outputs of stabilized source contracts, not independent design tasks; one owner should regenerate and verify them to prevent drift.

No implementation may claim live external-RF preservation from unit/envtest or rendered-manifest evidence alone. Release acceptance requires the PRD's actual supported-image lifecycle matrix and explicit proof that all external presence/value pairs remain unchanged through managed readiness.
