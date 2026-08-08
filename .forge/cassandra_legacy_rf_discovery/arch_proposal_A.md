# Architecture Proposal A: Minimal Brownfield Discovery Gate

> **Historical proposal:** `task_review.md` superseded this proposal's all-seed validation/concurrent candidate semantics. The authoritative `design.md` now requires lazy sequential fallback in normalized spec order, first complete valid acceptance, skipped remainder, and deterministic exhaustion.

## Constraint Applied

**Favor simplicity.** Add one focused domain package and one worker executable package, keep orchestration in the existing `controllers/k8ssandra` package, extend `ClientCache` only enough to expose direct remote clients, and reuse the current reconciliation pipeline, API group, Management API facade, labels, configuration, generation, test harness, and documentation homes.

The proposal deliberately does not add ports/adapters/service/model subpackages, a new CRD, a generic job framework, a global discovery service, or a second container image. The worker is a second binary in the existing operator image because discovery must execute in the data plane and must start with fresh CQL driver state.

Brownfield stance:

- **Align:** package-by-domain layout, sequential `ReconcileResult` pipeline, manual field/constructor injection, Kubebuilder API types and generation, watched-by labels, Testify/envtest, and existing migration docs (`codebase.md:30-47,58-79,90-112`).
- **Improve:** new admission dependencies are typed rather than package globals; discovery errors are sanitized rather than passed to generic `.status.error`; new external seams are narrow interfaces; remote absence reads are uncached. These deviations are required for safety/testability under the align-vs-improve rule (`architecture-guide.md:188-205`).
- **Preserve:** the unmarked path and existing schema behavior remain unchanged. Discovery-aware objects bypass the flat initial-system-replication and create-or-alter paths only where required.

## Complexity Assessment

The analysis contains 13 domain entities, five user stories, eight external dependencies, 38 edge cases, concurrency races, admission, multi-cluster Kubernetes state, and a Cassandra protocol boundary. The guide therefore signals **complex** behavior and requires a duel (`architecture-guide.md:7-29`).

The selected pattern is nevertheless the repository's existing **level-based reconciliation pipeline plus a short-lived worker**, not a new hexagonal package tree. This is a deliberate brownfield simplification: external dependencies receive narrow domain-facing interfaces, while pure validation/canonicalization is isolated in one flat package. The aggregate controller remains the state-machine owner; Kubernetes status remains the durable state.

State progression is explicit:

`NotRequired | Pending(attempt) | Blocked(reason) | Accepted(snapshot)`

Only `Pending` may create discovery support resources. Only a valid `Accepted` snapshot that has been written, authoritatively read back, and revalidated permits managed-DC creation. A valid accepted snapshot never transitions back to discovery. Missing/corrupt status may rediscover only after an uncached scan of the union of planned and status-observed managed locations proves that no managed DC exists.

## Package Structure

Two new Go packages; existing packages are extended in place:

```text
cmd/legacy-rf-discovery/
  main.go                         # worker entrypoint; stdin/file config -> signed ConfigMap result

pkg/discovery/
  errors.go                       # typed failures and stable reason precedence
  types.go                        # worker request/result, observations, snapshot-neutral values
  canonical.go                    # IP/RF/map/fingerprint/canonical JSON/hash validation
  worker.go                       # concurrent fresh per-endpoint CQL observation
  worker_test.go                  # pure and mocked endpoint tests

controllers/k8ssandra/
  legacy_rf_discovery.go          # qualification, Job/support resources, acceptance, status transitions
  legacy_rf_discovery_test.go
  datacenters.go                  # accepted seeds + final authoritative create-time gate
  schemas.go                      # snapshot-aware read-all/compare/direct-ALTER branch
  k8ssandracluster_controller.go  # early gate, watches, injected seams

apis/k8ssandra/v1alpha1/
  k8ssandracluster_types.go       # credential input, status/snapshot/phase/reason types
  constants.go                    # marker, labels, supported keyspace constants
  k8ssandracluster_webhook.go     # create marker and create/update validation

pkg/clientcache/cache.go          # cached and direct client pair per remote context
pkg/config/config.go              # only operator-configurable time/bounds, if exposed
main.go                           # wire reconciler, admission, workload factory
Dockerfile                       # build/copy worker in the existing image
```

Generated CRD/deep-copy/RBAC/webhook/Helm artifacts and current CRD docs are outputs of existing generation flows, not new architecture modules.

Responsibilities and boundaries:

- `pkg/discovery` knows Cassandra observation semantics and canonical wire data, but not `K8ssandraCluster`, controller-runtime reconciliation, or desired managed RF.
- `cmd/legacy-rf-discovery` reads mounted attempt configuration and secrets, creates a fresh endpoint-pinned CQL session for each seed, invokes `pkg/discovery`, signs canonical output, patches only its pre-created result ConfigMap, and exits.
- `controllers/k8ssandra` owns all Kubernetes state transitions, result trust verification, optimistic status persistence, cleanup, backoff, and the no-DC gate.
- API types own only user input and durable controller-owned status. Worker transport types do not become CRD fields automatically.
- `pkg/clientcache` remains the sole multi-cluster client registry. It stores both the manager-backed cached client and a direct `client.New(restConfig, ...)` client for every remote context.
- `pkg/cassandra/management.go` remains the post-Ready adapter. It is not extended for pre-Ready CQL.

Reusable abstractions:

- `K8ssandraClusterReconciler` sequential steps and `pkg/result.ReconcileResult` for early stopping (`codebase.md:34-47`).
- `ClientCache` local cached/uncached resolution and remote context registration (`codebase.md:94-99`).
- watched-by cluster labels and ConfigMap mapping for cross-context requeues (`codebase.md:65,145-155`).
- optimistic status patch style from `controllers/k8ssandra/schemas.go:394-402` (`codebase.md:159-168`).
- direct `GetKeyspaceReplication` and `AlterKeyspace`; never `EnsureKeyspaceReplication` (`codebase.md:170-179`).
- existing `MultiClusterTestEnv`, webhook envtest, Management API mock factory, generation scripts, and migration/troubleshooting docs (`codebase.md:69-88,101-112`).

No circular dependency exists: `apis` and `pkg/discovery` are leaves; `clientcache` depends only on Kubernetes libraries; the controller imports those leaves; `cmd` imports `pkg/discovery`; `main` composes concrete dependencies.

## Data Flows

### US-1 — Preserve observed replication

`K8ssandraCluster CREATE` → mutating webhook injects marker → validating webhook canonicalizes/rejects local input → early reconcile resolves the current first planned DC → controller creates per-attempt support Secret, result ConfigMap, scoped ServiceAccount/Role/RoleBinding, and Job → worker starts all canonical seeds concurrently, using one new driver cluster/session pinned to exactly one IP per seed → every session reads the expected cluster name; each may produce a complete candidate → the first complete valid candidate to finish supplies release/partitioner/schema/topology and all three independent maps → worker waits for all identity checks, rejects any unreachable/mismatched seed, validates authoritative before/after fingerprints, canonicalizes and HMAC-signs the result → controller verifies signature/bindings/size, checks any user-supplied `externalDatacenters` is the exact observed set, and persists the three sparse maps unchanged → accepted canonical seeds later feed EndpointSlices. Output: immutable accepted snapshot; no CQL DDL.

The worker must disable driver peer discovery/load balancing for these sessions (using the exact grounded driver API in Phase 4.7). Otherwise a session configured for seed A could query seed B and violate independent-contact and authority semantics.

### US-2 — Fail before managed creation

Marked qualifying object → early gate runs before `reconcileSuperuserSecret` → uncached existence scan across every planned context/namespace → validation/workload/result failure maps through deterministic reason precedence → sanitized `Blocked` status and transition-only Event → `ReconcileResult.Done` stops all existing resource creation. Output: discovery support resources only; zero managed DC/pod and zero schema DDL.

### US-3 — Keep one durable snapshot

Verified signed result → uncached control-plane re-read → validate UID/generation/marker/seed digest/source Secret RVs/planned set → uncached all-context managed-DC absence scan → optimistic status patch → stop → next reconcile uses uncached API read to hash-verify snapshot → immediately before each first managed-DC `Create`, re-read object and direct remote clients, revalidate snapshot/plan/absence → create. Output: one winning snapshot under concurrent attempts. Conflict or stale result is discarded. Valid snapshots are never replaced; corrupt/missing snapshots rediscover only if the same authoritative scan across the union of planned and status-observed managed locations returns none. A later generation may proceed without rediscovery only when expected name/server type remain compatible, any supplied external set still equals the snapshot, and every current managed name remains disjoint from observed external names; reorder, removal, and addition of non-colliding managed DCs stay allowed.

### US-4 — Diagnose and recover safely

Failure → `DiscoveryFailure` chooses stable reason and safe remediation → status/Event transition (raw driver/auth/TLS errors remain debug-log-only after redaction) → controller schedules bounded exponential backoff with jitter → Secret index/watch or Job/ConfigMap watch requeues earlier → a corrected attempt replaces only pre-acceptance support resources. Output: stable phase/reason/message without secret bytes.

Defaults are named and bounded: 10-second connect, 10-second query, 120-second Job active deadline, Job `backoffLimit: 0`, controller retry starting at 5 seconds and capped at 5 minutes, and a 256 KiB canonical result limit. Configuration is added to `ReconcilerConfig` only if operators need to tune it; clock/jitter/key generation are injected for tests.

### US-5 — Avoid retroactive activation

Create admission → reject reserved client marker → inject marker version `v1` → validating webhook requires supported marker on qualifying create. Update admission → never inject marker; reject removal/mutation/forgery and incompatible accepted-state edits. Reconcile unmarked object → existing path untouched. Marked object with DC/pod but no valid snapshot → `DiscoveryTooLate`, with no rediscovery. DSE/HCD → `NotRequired/UnsupportedServerType` and existing behavior. Output: upgrade-safe non-retroactivity.

### Post-Ready continuation shared by US-1/US-2

Ready managed DC → existing Management API reads all three live maps before any DDL → canonicalize strategies/RFs → compare exact external presence/value projection against snapshot → on any drift/missing keyspace, `ExternalReplicationDrift` or precise failure and zero DDL → otherwise merge only desired managed entries → call direct `AlterKeyspace` for changed managed projections → retry always rereads all three after partial success. Output: external state is asserted, never enforced.

## Architecture Decisions

### D1 — Existing reconciliation pipeline owns the state machine

**Context:** The repository already short-circuits sequential reconcile concerns; the gate must precede all unrelated support resources.

**Options considered:** new controller/CRD; generic workflow engine; one early step in `K8ssandraClusterReconciler`.

**Decision:** Add `reconcileLegacyRfDiscovery` after deletion/finalizer/basic Cassandra validation and before `reconcileSuperuserSecret` (`codebase.md:143-155`). It returns `Done` for Pending, Blocked, and just-Accepted.

**Consequences:** Smallest integration and no extra aggregate ownership. The controller file is focused, but the existing aggregate remains responsible for ordering. **Constraint influence:** simplicity favors one step over a new controller and CRD. **Align** with existing pipeline.

### D2 — Same image, separate worker binary

**Context:** CQL must run in the first data plane; the image currently contains only `/manager` and no CQL abstraction.

**Options considered:** manager subcommand; separate image/service; second binary in the existing image.

**Decision:** Build `/legacy-rf-discovery` from `cmd/legacy-rf-discovery` into the existing operator image. The Job invokes that path explicitly. The binary has no manager initialization or global CQL state.

**Consequences:** One supply-chain artifact and one small executable package; every attempt starts a fresh process. A manager subcommand would entangle flags/setup; a separate image would multiply packaging. **Constraint influence:** simplicity minimizes artifacts while preserving process isolation. **Align** with the existing image registry; **improve** the Dockerfile intentionally.

### D3 — One fresh endpoint-pinned session per seed

**Context:** Normal Cassandra drivers discover peers and load-balance, which can make an apparent request to seed A execute on seed B.

**Options considered:** one multi-host session; sequential sessions; concurrent independent sessions.

**Decision:** Canonical seeds are launched concurrently. Each endpoint observer constructs a brand-new driver cluster/session with only that IP, peer discovery/load balancing disabled or pinned by the grounded API, bounded connect/query timeouts, and guaranteed close. Each reports cluster-name evidence and an optional complete candidate. Completion order selects the first complete valid candidate; non-authoritative endpoints contribute only reachability and exact expected-name evidence and are not compared for other fields.

**Consequences:** Exact resolved authority semantics and bounded latency, at the cost of N short sessions. Candidate-only failures from a non-authoritative endpoint are ignored after that endpoint has proven reachability and exact expected name; they do not enter failure precedence or become cross-seed convergence checks. The worker issues only `SELECT` queries against `system.local`, `system.peers_v2`, and `system_schema.keyspaces`; operator documentation must give the least-privilege role grants verified against every supported source version. No worker pool or reusable session cache is introduced. **Constraint influence:** simplicity chooses direct goroutines plus `errgroup`-style coordination over a framework. **Improve** because the external boundary must be testable and exact.

### D4 — Signed ConfigMap result with least-privilege attempt identity

**Context:** UID/generation/hash bindings detect staleness but a principal able to write the result ConfigMap could forge them.

**Options considered:** trust labels/owner convention; Pod logs; custom CRD; HMAC-authenticated bounded ConfigMap.

**Decision:** The controller generates a cryptographically random 32-byte one-attempt key in a data-plane Secret, pre-creates the uniquely named result ConfigMap, and creates a unique ServiceAccount with a Role limited by `resourceNames` to patch/get that ConfigMap. The Job necessarily mounts its projected ServiceAccount token plus attempt/config/credential/TLS Secrets. The worker signs canonical result bytes with HMAC-SHA-256; the controller reads the key directly, constant-time verifies the signature, then validates all authoritative bindings. Neither key nor source secrets appear in results. Cleanup removes all attempt resources after acceptance, replacement, or cluster deletion.

**Consequences:** A ConfigMap-only writer cannot forge acceptance; compromise of the Job, attempt Secret, controller, or data-plane secret-reading authority remains a documented trust boundary. This adds one ephemeral Secret/SA/Role/RoleBinding but no API type or service. **Constraint influence:** simplicity selects standard Kubernetes resources and stdlib crypto over a new service/CRD. **Improve** over convention-only ownership due to a concrete security invariant.

### D5 — Extend `ClientCache` with paired direct remote clients

**Context:** local uncached reads exist, but production remote clients are manager-cache-backed. FR-025/025a require authoritative remote absence checks.

**Options considered:** tolerate cached reads; reconstruct clients per check from kubeconfig Secrets; store each remote REST config; create and retain a direct client when the remote cluster is registered.

**Decision:** Add `remoteNonCacheClients map[string]client.Client`, `AddClientPair(contextName, cached, direct)`, and `GetRemoteNonCacheClient(contextName)`. Empty context returns the existing local uncached client. `ClientConfigReconciler`, while it already holds `rest.Config`, creates one direct client and registers the pair. Keep `AddClient` for compatibility/tests, but authoritative lookup fails closed if no direct pair exists. Do not silently fall back to cached data.

**Consequences:** Direct API reads are cheap to resolve and consistent across acceptance/create gates; no repeated kubeconfig parsing and no new global cache. Map access must retain the existing lifecycle assumptions or add a small mutex if dynamic registration is concurrent. **Constraint influence:** simplicity makes the smallest cache extension rather than a new multi-cluster client subsystem. **Align** with ClientCache ownership; **improve** its missing authoritative seam.

The controller uses a domain-facing `ManagedDatacenterState` adapter over this API, so business code does not depend on SDK-named methods. `AnyExists` performs direct `Get` for each exact managed location in the caller-provided planned/status-observed union and additionally checks managed Cassandra pods where the too-late contract requires it; any API error other than NotFound blocks.

### D6 — Leader election plus compare-and-swap and create-time revalidation

**Context:** packaged leader election is disabled; two replicas can race different authoritative candidates and DC creation.

**Options considered:** prove lock-free correctness only; add a custom Lease; enable existing controller-runtime leader election and retain defensive CAS/revalidation.

**Decision:** Enable the already-supported leader election in Kustomize/Helm for supported multi-replica operation. Still retain optimistic status write/read-back and direct create-time revalidation because leadership can change and tests require race safety.

**Consequences:** No new locking mechanism. Single-replica installs remain valid; multi-replica has one active reconciler. **Constraint influence:** simplicity reuses the built-in switch. **Align** with existing runtime support, **improve** packaging defaults where replicas exceed one.

### D7 — Status snapshot is the only durable accepted state

**Context:** the flat annotation cannot represent three maps and is user-writable; a separate CRD adds lifecycle/RBAC complexity.

**Options considered:** annotation; result ConfigMap as source of truth; new Snapshot CRD; status.

**Decision:** Persist typed snapshot/status under `K8ssandraClusterStatus`; canonical hash covers UID, generation, marker, canonical seeds/digest, source Secret RVs, expected cluster/source version, fingerprints, external DCs, three maps, accepted time, and format version. After optimistic patch, stop and only consume an authoritative read-back on a later reconcile.

**Consequences:** One aggregate and Kubernetes concurrency semantics. Object-size limits are addressed by bounding result/snapshot input. Admission rejects post-acceptance expected-name/server-type changes and any supplied external set unequal to the snapshot; controller validation handles live/current managed-name collisions while allowing ordinary managed reorder/decommission/addition. Current generation need not equal `AcceptedGeneration` after such a compatible edit, but create-time validation must prove compatibility against the immutable snapshot. **Constraint influence:** simplicity chooses the PRD-required existing aggregate. **Align** with status ownership.

### D8 — Dedicated same-namespace source credential contract

**Context:** Discovery credentials are independent of target auth and must be usable across a control-plane/data-plane boundary.

**Options considered:** reuse superuser/target auth; cross-namespace namespaced reference; dedicated local reference copied ephemerally.

**Decision:** Add `spec.cassandra.legacyCqlCredentialsSecretRef` as an optional `corev1.LocalObjectReference`. It resolves only in the `K8ssandraCluster` namespace on the control-plane cluster. Keys are exactly `username` and `password`; missing or empty keys block as `AuthenticationRejected` without exposing values. The controller copies bytes only into an immutable, per-attempt data-plane support Secret, binds the source resourceVersion, and deletes the copy with the attempt. No cross-namespace reference and no external secrets provider are accepted. Existing client-encryption Secret refs follow the same per-attempt copy pattern and bind their source RVs.

**Consequences:** Explicit public contract, no generated credentials, and no dependency on target auth. The controller briefly handles secret bytes, already an existing trust role. **Constraint influence:** simplicity uses `LocalObjectReference` and fixed keys instead of a new credential CRD. **Align** with API Secret-ref conventions; **improve** isolation from target credentials.

### D9 — Deterministic canonicalization and failure precedence

**Context:** concurrent endpoints can produce multiple failures; retries must not oscillate status reasons.

**Options considered:** first error wins; nested infrastructure errors; fixed domain precedence.

**Decision:** Normalize IPs with `net/netip`: trim is not accepted, zones/ports/FQDNs are rejected, IPv4-mapped IPv6 is unmapped to IPv4, duplicates collapse, and canonical addresses sort lexicographically by 16-byte form plus family. The exact sorted textual set drives digest, worker input, snapshot, and EndpointSlices.

After stale/trust checks, failure precedence is fixed: `SnapshotConflict` → `DiscoveryTooLate` → `StaleDiscoveryResult` → `UnsupportedServerType`/`UnsupportedSecretsProvider` → `InvalidContactPoint` → credential/TLS material validation → `ContactUnreachable` → `AuthenticationRejected` → `AuthorizationDenied` → `TLSFailed` → `IdentityMismatch` → `UnsupportedSourceVersion` → `SchemaDisagreement` → `TopologyInconsistent` → `ManagedDatacenterNameCollision` → `MissingKeyspace` → `UnsupportedStrategy` → `InvalidReplication` → `DiscoveryWorkloadFailed`/`InvalidDiscoveryResult`. Within one reason, the lowest canonical endpoint/keyspace wins the sanitized message.

**Consequences:** Status and tests are deterministic even though authority selection is completion-based. **Constraint influence:** simplicity centralizes one ordered table rather than distributed conditionals. **Improve** safety/testability.

### D10 — Snapshot-aware schema branch reuses the Management API

**Context:** post-Ready live calls already exist, but the nine-method facade is too broad and `EnsureKeyspaceReplication` can create.

**Options considered:** new schema service; extend discovery CQL worker post-Ready; branch existing schema reconciliation and reuse direct methods.

**Decision:** In `schemas.go`, marked/accepted objects use existing `GetKeyspaceReplication` and `AlterKeyspace`. A pure helper in `pkg/discovery` validates class/RF/presence and computes managed-only desired maps. All three reads and external comparisons complete before the first ALTER. Unmarked objects keep the existing path.

**Consequences:** No second runtime transport after Ready and no change to `ManagementApiFacade`. Partial success is retried from fresh reads. **Constraint influence:** simplicity reuses proven calls. **Align** with the existing post-Ready boundary; **improve** by bypassing unsafe create-or-alter only for marked objects.

### D11 — Existing docs and generated artifacts are integration deliverables

**Context:** the current migration guide is stale and Helm/Kustomize webhook/RBAC are separately checked in.

**Options considered:** new standalone architecture doc; update natural task/reference locations.

**Decision:** Update `docs/content/en/tasks/migrate/_index.md`, add/extend discovery remediation in `docs/content/en/tasks/troubleshoot/_index.md`, regenerate current CRD reference, and regenerate/synchronize CRD, deep-copy, RBAC, Kustomize webhook, and Helm artifacts. Historical CRD docs remain untouched.

**Consequences:** Users find the feature in existing workflows; packaging cannot omit the fail-closed marker or Job permissions. Docs must explicitly separate source/local checks from the required real Cassandra lifecycle proof. **Constraint influence:** simplicity avoids a new docs subtree. **Align** with existing documentation structure.

### D12 — Strict Apache release-version syntax

**Context:** Runtime admission is Apache Cassandra 4.0 or newer; accepting arbitrary vendor-suffixed strings could accidentally admit DSE/HCD, while a release test matrix must not become a runtime upper bound.

**Options considered:** extract the first numeric prefix from any string; maintain a finite version allow-list; accept strict Apache numeric release strings with a minimum only.

**Decision:** Parse the entire `release_version` as an Apache numeric `major.minor.patch` value and require `>=4.0.0`; reject vendor prefixes/suffixes rather than guessing provenance. Do not cap the major version in runtime code. Official Apache prerelease syntax, if a supported release ever includes it, must be added deliberately with lifecycle coverage rather than accepted generically.

**Consequences:** DSE/HCD-style version strings fail closed, future stable Apache major versions are not artificially blocked, and behavior is deterministic. Some vendor-repackaged Cassandra distributions with decorated versions are intentionally unsupported until product requirements and lifecycle tests say otherwise. **Constraint influence:** simplicity chooses one strict parser, not a provider registry. **Improve** at an untrusted CQL boundary.

## Error Types

Defined in `pkg/discovery/errors.go` before interfaces:

```go
type Reason string

type Failure struct {
    Reason    Reason
    Retryable bool
    SafeMessage string
    Endpoint  string // canonical, optional; never secret-bearing
    Cause     error  // internal only; never serialized
}

func (f *Failure) Error() string
func (f *Failure) Unwrap() error
```

`Reason` includes every FR-035 value plus `DiscoveryWorkloadFailed` and `InvalidDiscoveryResult` for explicit worker/result edges. Constructors accept safe domain context and wrap raw causes. Only `Reason`, `Retryable`, and `SafeMessage` cross into status/Event/result; `Cause` is logged only through a redacting logger policy.

Sentinels remain limited to caller control flow:

```go
var (
    ErrNoCompleteObservation = errors.New("legacy discovery: no complete observation")
    ErrResultUnauthenticated = errors.New("legacy discovery: result authentication failed")
    ErrSnapshotInvalid       = errors.New("legacy discovery: snapshot invalid")
)
```

Kubernetes Conflict/NotFound and driver errors are wrapped at their boundary and mapped once to `Failure`; they are not compared by message text.

## Interfaces

All interfaces are consumer-owned, domain-facing, and at most three methods:

```go
// In controllers/k8ssandra: lifecycle of one data-plane discovery attempt.
type DiscoveryAttempts interface {
    Ensure(ctx context.Context, attempt discovery.Attempt) (discovery.AttemptState, error)
    Result(ctx context.Context, attempt discovery.Attempt) (*discovery.SignedResult, error)
    Cleanup(ctx context.Context, attempt discovery.Attempt) error
}

// In controllers/k8ssandra: authoritative managed-resource safety boundary.
type ManagedDatacenterState interface {
    AnyExists(ctx context.Context, locations []discovery.ManagedLocation, includePods bool) (bool, error)
}

// In pkg/discovery: one isolated Cassandra endpoint observation.
type EndpointObserver interface {
    Observe(ctx context.Context, endpoint netip.AddrPort, connection Connection) (EndpointObservation, error)
}

// In controllers/k8ssandra: deterministic retry boundary.
type RetrySchedule interface {
    Next(attempt uint32) time.Duration
}

// In controllers/k8ssandra: cryptographic attempt-key boundary.
type AttemptKeys interface {
    New() ([]byte, error)
}
```

Kubernetes `client.Client`, recorder, and existing `ManagementApiFactory` remain existing injected boundaries. `DiscoveryAttempts` is implemented with them; `ManagedDatacenterState` is a thin adapter over direct clients from `ClientCache`. Clock uses the existing Kubernetes clock interface rather than defining another local interface. No interface exposes create-keyspace behavior.

## Types

Public CRD contract in `apis/k8ssandra/v1alpha1`:

```go
type LegacyReplicationDiscoveryPhase string
type LegacyReplicationDiscoveryReason string

type LegacyReplicationDiscoveryStatus struct {
    ObservedGeneration int64
    Phase LegacyReplicationDiscoveryPhase
    Reason LegacyReplicationDiscoveryReason
    Message string
    LastTransitionTime metav1.Time
    SnapshotHash string
    Snapshot *LegacyReplicationSnapshot
}

type LegacyReplicationSnapshot struct {
    FormatVersion string
    ClusterUID types.UID
    AcceptedGeneration int64
    MarkerVersion string
    Seeds []string
    SeedDigest string
    CredentialSecretResourceVersion string
    TLSSecretResourceVersions map[string]string
    ExpectedClusterName string
    SourceVersion string
    IdentityFingerprint string
    TopologyFingerprint string
    SchemaFingerprint string
    ExternalDatacenters []string
    Replication LegacySystemKeyspaceReplication
    AcceptedAt metav1.Time
    Hash string
}

type LegacySystemKeyspaceReplication struct {
    SystemAuth map[string]int32
    SystemTraces map[string]int32
    SystemDistributed map[string]int32
}
```

RF storage is `int32`, allowing exactly 1..2147483647 and avoiding host-`int` ambiguity. Empty maps are valid; a missing keyspace row is not. Separate named fields make three-row completeness explicit and preserve sparse per-keyspace omission.

Internal `pkg/discovery` types:

- `Attempt`: cluster UID/generation/marker, canonical seeds/digest, expected name, source Secret RV bindings, planned DCs, context/namespace, result name, deadline.
- `Connection`: mounted credential/TLS paths and bounded dial/query settings; it never serializes secret bytes.
- `EndpointObservation`: endpoint, exact cluster name, optional complete `Candidate`, and sanitized failure classification.
- `Candidate`: source version, partitioner, schema/identity/topology fingerprints, hosts, external DCs, and three replication maps from one endpoint only.
- `SignedResult`: format version, attempt bindings, authoritative endpoint, canonical candidate, all endpoint identity evidence, completed time, canonical hash, HMAC.
- `AttemptState`: `Pending`, `Succeeded`, `Failed`, with Job identity and sanitized termination data.
- `ManagedLocation`: exact name, context, namespace, derived as the deduplicated union of current spec plans and status-observed managed locations when recovery safety requires both.

Unknown JSON fields, non-canonical ordering, duplicate map keys, oversize payloads, invalid UTF-8, invalid hashes/signatures, and prohibited secret-like fields reject the result. Canonical encoding uses a fixed struct schema and sorted slices; Go map JSON order is not used as the hash contract.

## Constructors

```go
func discovery.NewWorker(observer EndpointObserver, limits Limits) (*Worker, error)
func discovery.NewCqlEndpointObserver(driverConfig GroundedDriverConfig) (*CqlEndpointObserver, error)
func discovery.NewFailure(reason Reason, retryable bool, safeMessage string, cause error) *Failure

func NewKubernetesDiscoveryAttempts(
    clients *clientcache.ClientCache,
    scheme *runtime.Scheme,
    image string,
    keys AttemptKeys,
    limits discovery.Limits,
) (*KubernetesDiscoveryAttempts, error)

func NewManagedDatacenterState(clients *clientcache.ClientCache) (*KubernetesManagedDatacenterState, error)
func NewRetrySchedule(clock clock.Clock, random io.Reader, bounds RetryBounds) (*ExponentialRetrySchedule, error)
func NewClusterDefaulter(markerVersion string) (*K8ssandraClusterDefaulter, error)
func NewClusterValidator(clients *clientcache.ClientCache) (*K8ssandraClusterValidator, error)
```

Constructors reject nil dependencies, empty image/marker, non-positive deadlines, retry min greater than max, and result limits above Kubernetes-safe bounds. `main.go` is the composition root. The worker binary constructs its concrete CQL observer only after Phase 4.7 pins and grounds the driver; no version/API is guessed in this proposal.

## Wave Hints

**Wave A — foundation, sequential generation boundary afterward**

1. API input/status/snapshot/phase/reason types, marker/constants, deep-copy/CRD contract.
2. `pkg/discovery` errors, canonical types, IP/RF/map/fingerprint/hash/HMAC functions, and table-driven tests.
3. Narrow interfaces, limits/retry configuration, ClientCache paired direct-client extension, and unit tests.

**Wave B — core, parallelizable after Wave A**

1. Fresh CQL worker and endpoint observer, including first-complete authority and all-seed identity checks.
2. Kubernetes discovery attempt resources, signed result verification, cleanup, watches, and Secret-trigger mapping.
3. Admission marker/defaulting/validation and real webhook envtests.
4. Snapshot-aware post-Ready three-keyspace prevalidation and managed-only direct ALTER tests.
5. Authoritative absence adapter plus acceptance/create race tests.

**Wave C — integration and release gates**

1. Insert early controller gate, wire constructors in `main.go`, accepted-seed EndpointSlices, final create-time checks, status/Event behavior.
2. Dockerfile, image command, RBAC, leader-election packaging, generated Kustomize/Helm/CRD/deep-copy artifacts.
3. Multi-cluster envtest for no pre-acceptance resources, CAS/read-back, restart, snapshot loss/corruption, races, recovery, redaction.
4. Kind/real-Cassandra lifecycle matrix including sparse maps and `system_auth` RF `2/9/2/10/10`; this is the only proof of live bootstrap preservation.
5. Migration/troubleshooting/security documentation and generated current CRD reference.

`pkg/discovery` worker/validation is likely to exceed 10 implementation/test tasks (endpoint isolation, concurrency, identity, topology, schema, RF parsing, fingerprints, signing, redaction, deadlines, closing sessions) and is a **hierarchical delegation candidate**. The controller integration also exceeds 10 tasks if admission, workload resources, status, watches, and create gates are planned as one file; the planner should split those into separate task files rather than delegate a monolith.

Every Wave B item depends only on Wave A contracts. Wave C is the only wave that edits the shared composition root and generated/package artifacts.
