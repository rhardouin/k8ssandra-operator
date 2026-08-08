# Codebase Map: Legacy System-Keyspace Replication Discovery

Evidence was collected from the `keep-external-dc-rf` working tree on 2026-08-06. The only pre-existing worktree modification observed was `.forge/cassandra_legacy_rf_discovery/prd.md`; this map does not treat that PRD as implemented behavior. Codebase-memory graph discovery identified 808 indexed source files, 92 packages, 1,734 functions/methods, and `main.main` as the executable entry point; exact paths and lines below were then verified against the checkout.

## Folder Structure

| Path | Responsibility | Feature relevance |
|---|---|---|
| `main.go` | Process entry point, scheme registration, controller-runtime manager, multi-cluster client initialization, reconciler/webhook construction. | Wire any new discovery dependency into `K8ssandraClusterReconciler`; register admission and any new API types. |
| `apis/k8ssandra/v1alpha1/` | `K8ssandraCluster` desired/status types, constants, validation/defaulting webhook, generated deep-copy code. | Natural home for discovery credential input, immutable snapshot/status types, phase/reason enums, marker constant, and admission rules. |
| `apis/{config,control,medusa,reaper,replication,stargate,telemetry}/` | Other CRD API groups and generated code. | Existing API style references; no PRD scope belongs in these groups. |
| `controllers/k8ssandra/` | Main aggregate controller split by reconciliation concern: datacenters, schemas, seeds, secrets, cleanup, telemetry, Reaper, Medusa, Stargate. | Primary integration area. A focused `legacy_rf_discovery.go` (plus tests) would fit this package rather than expanding `datacenters.go` or `schemas.go` further. |
| `controllers/{config,control,medusa,reaper,replication,stargate}/` | Controllers for supporting resources and components. | `controllers/config` creates remote cluster clients/caches; `controllers/control` shows delegated task orchestration. No existing generic arbitrary Kubernetes Job controller is available. |
| `controllers/secrets-webhook/` | Custom Pod mutation endpoint for external-secret injection. | Demonstrates manual mutating webhook registration, but the discovery marker belongs with the typed `K8ssandraCluster` webhook instead. |
| `pkg/cassandra/` | Cassandra resource builders, configuration, encryption helpers, Management API facade/factory, replication helpers. | Existing post-Ready schema adapter. New pre-Ready CQL discovery must be a separate boundary; the current facade requires Ready pods. |
| `pkg/clientcache/` | Local cached/uncached clients plus named remote Kubernetes clients. | Use `GetRemoteClient(firstDC.K8sContext)` for the discovery workload/result in the first planned data plane. |
| `pkg/result/` | `ReconcileResult` control-flow abstraction (`Continue`, `Done`, `RequeueSoon`, `Error`). | New controller steps should return this abstraction and stop reconciliation when Pending/Blocked/just-Accepted. |
| `pkg/errors/` | Small typed reason/error abstraction, currently only schema disagreement. | Possible style reference, but PRD reason codes are status-domain values and should not be forced into this underspecified package. |
| `pkg/{annotations,labels,secret,encryption,images,utils,reconciliation,test}/` | Shared object metadata, secret/TLS types, resource utilities, generic reconcile helper, test environment and mocks. | Reuse watched-by labels, secret selectors, hashing, owner metadata, and multi-cluster test harness. |
| `config/` | Kubebuilder-generated CRDs/RBAC/webhooks plus Kustomize deployment overlays and samples. | Generated CRD/RBAC/webhook artifacts must be refreshed; data-plane permissions must include discovery workload/result operations. |
| `charts/k8ssandra-operator/` | Helm chart, generated CRD bundle, RBAC, manager deployment, webhook configurations and values. | Packaged admission/RBAC/image behavior must match Kustomize output; chart webhook templates are checked in separately. |
| `test/e2e/`, `test/framework/`, `test/testdata/` | Kind/multi-cluster end-to-end suite, shared remote-client test framework, fixtures. | Natural home for migration lifecycle, upgrade, restart/concurrency and actual Cassandra-image release gates. |
| `docs/content/en/tasks/migrate/` | Existing user-facing Cassandra migration procedure. | Primary home for the new migration workflow, prerequisites, status/reasons, recovery and security constraints. |
| `docs/content/en/tasks/troubleshoot/` and `docs/content/en/tasks/secure/` | Status/error diagnosis and TLS/secret guidance. | Secondary homes for discovery reason codes and credential/TLS safety. |
| `docs/content/en/reference/crd/` | Generated current and historical CRD references. | Current reference must be regenerated; historical release snapshots must not be rewritten. |
| `scripts/`, `Makefile`, `Dockerfile` | Code/manifests generation, verification, Helm artifact preparation and image build. | A worker executable or manager subcommand requires Dockerfile/build changes; `make manifests generate` owns CRD/RBAC/deep-copy output. |

The repository has no route/controller layer in the HTTP-service sense. Its public ingress is the Kubernetes API: CRDs, controller watches, and admission webhooks. There is no event bus or queue consumer subsystem.

## Architectural Patterns

The primary architecture is a Kubebuilder/controller-runtime operator using level-based reconciliation, with package-by-domain organization rather than strict Clean Architecture.

- `main.go:88-102` registers Kubernetes API groups into a shared runtime scheme. `main.go:165-176` creates the manager and uncached client. `main.go:187-243` conditionally assembles the control-plane client cache, `K8ssandraCluster` reconciler, webhook, secret replication controller and task controller.
- `controllers/k8ssandra/k8ssandracluster_controller.go:100-129` is the Kubernetes reconciliation boundary. It reads and deep-copies the CR, delegates work, records an error Event, and status-patches the object.
- `controllers/k8ssandra/k8ssandracluster_controller.go:131-192` is a sequential orchestration pipeline. Each concern returns `pkg/result.ReconcileResult`; any completed result short-circuits the iteration.
- `controllers/k8ssandra/datacenters.go:43-318` performs desired-state construction, remote reads/creates/updates, Ready gating, post-Ready schema reconciliation, and task delegation. Graph tracing confirms it is called only by the aggregate `reconcile` path and directly calls `checkInitialSystemReplication`, `reconcileSeedsEndpoints`, `checkSchemas`, and remote client operations.
- `controllers/k8ssandra/schemas.go:33-109` constructs the Management API facade only after a managed DC is Ready (`datacenters.go:206-235`). This is the wrong boundary for pre-creation discovery, but the right location for snapshot-aware post-startup read/compare/direct-ALTER behavior.
- `pkg/cassandra/management.go:65-101` defines the Management API port. The concrete facade lists Ready pods (`management.go:136-151`) and then makes per-pod calls. `GetKeyspaceReplication` is at `management.go:231-246`; `AlterKeyspace` is at `management.go:205-229`.
- Multi-cluster is first-class: `controllers/config/clientconfig_controller.go:171-224` constructs and registers controller-runtime `cluster.Cluster` instances and stores their clients; data-plane caches are exposed to other controllers.

The desired discovery flow should preserve the existing split:

1. Admission qualifies only new objects and owns the version marker.
2. A controller-domain discovery orchestrator decides `NotRequired`/`Pending`/`Blocked`/`Accepted`, creates and observes a short-lived workload in the first data plane, validates result bindings, and persists immutable status.
3. The existing datacenter reconciler consumes only an accepted canonical seed set and performs a final authoritative pre-create check.
4. The existing schema reconciler selects a discovery-aware branch after Ready; that branch reads and prevalidates all three keyspaces, then uses direct `AlterKeyspace` for managed-only changes.

Important current behavior and hazards:

- `controllers/k8ssandra/schemas.go:115-161` persists one flat `initial-system-replication` annotation and defaults external DCs to RF 3. It cannot represent three independent sparse maps and must not carry the discovered snapshot.
- `controllers/k8ssandra/schemas.go:168-212` computes one map and calls `EnsureKeyspaceReplication` for every system keyspace.
- `pkg/cassandra/management.go:288-317` proves `EnsureKeyspaceReplication` is create-or-alter: a missing keyspace calls `CreateKeyspaceIfNotExists`. The discovery-enabled path must bypass it completely.
- `pkg/cassandra/config.go:105-122` serializes a single bootstrap system-replication map. Discovered maps must not be routed into this function.
- The Go module includes `github.com/datastax/go-cassandra-native-protocol`, but no current package imports it and there is no existing CQL session/client abstraction. Pre-Ready discovery is therefore net-new, not an extension of an existing CQL adapter.
- Current checked-in deployments default to one replica (`charts/k8ssandra-operator/values.yaml:33-37`, `config/manager/manager.yaml:11`) and the Helm deployment passes no `--leader-elect` argument (`charts/k8ssandra-operator/templates/deployment.yaml:35-38`). `main.go:104-112` supports leader election but defaults it to false. The PRD multi-replica gate cannot assume leader election is active.

## Naming Conventions

- Directories and Go packages are short lowercase domain names: `cassandra`, `k8ssandra`, `clientcache`, `reconciliation`.
- Go files use lowercase snake case for multiword names: `k8ssandracluster_controller.go`, `clientconfig_controller.go`, `result_helper.go`. Generated files use `zz_generated.*`.
- Exported types/functions use PascalCase; unexported functions and variables use camelCase. Reconciler types end in `Reconciler`; setup methods are `SetupWithManager`; resource builders commonly use `New...`, `Create...`, or `MakeDesired...`; reconciliation steps use `reconcile...`, `check...`, `update...`, and `ensure...`.
- Existing acronym casing is established but not Go-initialism-perfect: `ManagementApi`, `K8sContext`, `CassDcName`, `DseKeyspaces`. New code should follow surrounding names instead of introducing parallel `API`/`DC` spellings in the same API surface.
- API types carry explicit JSON camelCase tags and Kubebuilder markers. Enums are named Go string types plus constants, e.g. `ServerDistribution` at `k8ssandracluster_types.go:589-595`; discovery phases/reasons should follow this pattern.
- Kubernetes names, labels and annotations use the `k8ssandra.io/...` prefix and are centralized in `apis/k8ssandra/v1alpha1/constants.go`. Resource correlation should reuse `k8ssandra.io/cluster-name` and `k8ssandra.io/cluster-namespace` (`constants.go:61-62`) so existing map functions can enqueue the owning cluster.
- Errors are normally returned immediately with context and logged at the reconciliation boundary. There is mixed use of standard `fmt.Errorf`, `github.com/pkg/errors`, and the small `pkg/errors.K8ssandraError`; new discovery reason/status mapping should be explicit and sanitized rather than exposing raw driver errors through the generic `.status.error` path.
- Tests use `TestXxx`, descriptive `t.Run("Case", ...)`, and helper functions in lower camel case. Constants for mocked method names live in `pkg/test`.

## Testing Patterns

- Unit and integration tests use Go's `testing` package with `github.com/stretchr/testify/assert`, `require`, and `mock`. The module declares Testify 1.11.1 but replaces it with 1.10.0 because newer Testify breaks envtests (`go.mod:27,40`).
- Controller coverage is primarily multi-cluster `envtest`. `controllers/k8ssandra/k8ssandracluster_controller_test.go:64-134` starts three data planes, injects a fake Management API factory, and registers many isolated namespace-per-test subtests.
- `pkg/test/testenv.go:343-365` creates a randomized namespace and fresh `Framework` per controller test. `pkg/test/testenv.go:384-430` builds CRDs for envtest; `registerApis` at `testenv.go:433-478` registers all required API types.
- Admission is tested with a real envtest webhook server in `apis/k8ssandra/v1alpha1/k8ssandracluster_webhook_test.go:75-185`, including actual API create/update behavior. Marker injection, forged marker rejection and immutability belong there.
- External boundaries use Testify mocks. `pkg/test/mgmtapi.go:22-89` adapts an injected fake factory; generated mocks live in `pkg/mocks/`, with regeneration commands at `Makefile:531-534`.
- Pure helpers generally use table-driven tests (`pkg/cassandra/*_test.go`, `pkg/utils/*_test.go`) and assert exact values/maps. Canonical seed sorting/digesting, sparse-map preservation, RF integer parsing, fingerprinting, result binding, redaction and snapshot hashing should be pure table-driven tests.
- End-to-end tests use standard Go tests plus the shared `test/framework` against Kind/multiple Kubernetes contexts (`test/e2e/suite_test.go:78-128`). `Makefile:174-180` runs selected e2e tests; lifecycle evidence involving real Cassandra belongs here, not in envtest.
- Normal verification is `make test`, which first regenerates manifests/deep copies, formats, vets, lints and sets up envtest, then runs `go test` with atomic coverage (`Makefile:134-163`). E2E is separate.
- Existing tests do use polling and bounded `Eventually`; do not copy the older `time.Sleep` patterns into new unit tests. Inject a clock/backoff/random source where deterministic retry logic is tested.

Feature-specific test placement:

- `apis/k8ssandra/v1alpha1/k8ssandracluster_types_test.go`: enum/type helpers, marker/snapshot invariants.
- `apis/k8ssandra/v1alpha1/k8ssandracluster_webhook_test.go`: mutating injection, fail-closed registration, client-forged marker, marker mutation/removal, seed/server/provider validation.
- `controllers/k8ssandra/legacy_rf_discovery_test.go`: qualification, support resource generation, result binding, optimistic status acceptance, immutable snapshot and all stable reason mappings.
- `controllers/k8ssandra/k8ssandracluster_controller_test.go`: no unrelated support resource/DC before acceptance, accepted read-back before DC creation, too-late detection, Secret/result watches, restart/concurrent reconcile cases.
- `controllers/k8ssandra/schemas_test.go` or focused new tests: all-three prevalidation before any DDL, sparse heterogeneous maps including 2/9/2/10/10, external drift and missing keyspaces cause zero DDL, partial ALTER retry rereads all three.
- `test/e2e/`: actual Cassandra 4.0+ migration/bootstrap, upgrade non-retroactivity, multi-replica/restart and mutation races, TLS/auth/redaction. Envtest cannot prove the PRD's bootstrap non-mutation claim.

## DI Approach

Dependencies are assembled manually; there is no DI container.

- `main.go:187-215` constructs `ClientCache`, then injects `client.Client`, `runtime.Scheme`, `ClientCache`, `ManagementApiFactory`, event recorder and image registry into `K8ssandraClusterReconciler` fields.
- `K8ssandraClusterReconciler` declares those dependencies at `controllers/k8ssandra/k8ssandracluster_controller.go:72-82`. New external boundaries should be added as narrow injected interfaces/factories here (for example, workload/result construction or a clock), with real construction in `main.go` and fakes in tests.
- `pkg/cassandra/management.go` already separates `ManagementApiFactory` from `ManagementApiFacade`; this is the pattern to follow, but discovery should not enlarge the already nine-method facade. A domain-facing discovery interface should remain separate and at most five methods.
- `pkg/clientcache/cache.go:31-50` constructor-injects clients/scheme and resolves local-versus-remote clients by `K8sContext`.
- Reconciler settings are injected through `*config.ReconcilerConfig`; defaults currently come from environment variables in `pkg/config/config.go:18-75`.
- The codebase still has global mutable state: the webhook stores `clientCache` in a package global (`k8ssandracluster_webhook.go:40-42,60-64`), image defaults are globals (`pkg/images/images.go:12-19`), and some test harness state is package-global. New discovery code should not add more globals; typed validator/defaulter structs can receive dependencies directly.

## Documentation Structure

- `README.md:4-6` sends users to the documentation site; it is not the natural place for detailed migration configuration.
- The documentation site is Hugo + Docsy (`docs/README.md:1-22`). Pages use `_index.md`, YAML front matter (`title`, `linkTitle`, `weight`, `description`) and task-oriented headings, fenced YAML/bash examples, Docsy alert shortcodes and Hugo `relref` links.
- The existing migration guide is `docs/content/en/tasks/migrate/_index.md`. It already explains `additionalSeeds`, `externalDatacenters`, system keyspaces, applying a `K8ssandraCluster`, observing bootstrap, and decommissioning (`migrate/_index.md:64-236`). It is the authoritative user-facing home and is currently stale for this PRD: it tells users to pre-normalize system RF and describes `externalDatacenters` as preservation.
- `docs/content/en/tasks/troubleshoot/_index.md:7-39` teaches users to inspect `.status.error` and Events. It should document discovery phase/reason diagnosis and recovery, or link to a migration troubleshooting subsection.
- `docs/content/en/tasks/secure/encryption/_index.md` documents secret creation and client encryption. Discovery documentation must instead use the actual dedicated credential/TLS fields delivered by the feature, warn that authentication precedes identity checks, require endpoint verification, and never put literal passwords in examples.
- Current CRD reference is generated at `docs/content/en/reference/crd/k8ssandra-operator-crds-latest/_index.md`; historical `releases/...` pages are snapshots and should not be edited for a new feature.
- `docs/content/en/reference/crd/_index.md` is the index for generated reference. `docs/content/en/tasks/_index.md` automatically lists task children; no new top-level index is needed if migration remains in the existing page.
- Documentation build commands are `npm run start`, `npm run build:staging`, and `npm run build:production` from `docs/` (`docs/README.md:24-61`).

Documentation required by the PRD should cover: qualification/non-retroactivity; IP-only seeds and TCP 9042; separate optional discovery credentials; supported Cassandra/server/provider versions; TLS hostname/IP verification; immutable accepted seed set/snapshot; exact meanings and remediation for every stable reason; external drift blocking without enforcement; observable-topology limitations; and an explicit statement that configuration/source tests are not proof of ring health, liveness, streaming, repair, or live bootstrap preservation.

## Integration Points

### API schema and generated artifacts

| Location | Exact pattern to follow |
|---|---|
| `apis/k8ssandra/v1alpha1/k8ssandracluster_types.go:37-81` | Extend `K8ssandraClusterSpec` only for cluster-level discovery input if the product contract places the credential ref outside Cassandra; use comments, `+optional`, validation markers and camelCase JSON tags. |
| `apis/k8ssandra/v1alpha1/k8ssandracluster_types.go:237-278` | `CassandraClusterTemplate` owns `additionalSeeds`, `clusterName`, `serverType`, and client encryption. A dedicated legacy discovery credential Secret reference naturally sits adjacent to `AdditionalSeeds`; do not reuse `SuperuserSecretRef` or target `Auth`. |
| `apis/k8ssandra/v1alpha1/k8ssandracluster_types.go:95-113` | Add controller-owned discovery status and immutable snapshot. Existing status already owns `ObservedGeneration`; the discovery status must carry its own observed generation, phase, reason/message, transition time, snapshot hash and separate per-keyspace maps. Preserve omission with maps whose absent keys remain absent. |
| `apis/k8ssandra/v1alpha1/k8ssandracluster_types.go:557-583` | Follow status helper style for transitions, but do not reset `LastTransitionTime` when phase/reason/message are unchanged; the existing generic helper always resets it and is insufficient for transition-only Events. |
| `apis/k8ssandra/v1alpha1/constants.go:3-76` | Add marker/label/annotation constants and supported keyspace identifiers centrally. Keep `SystemKeyspaces` scope at `constants.go:81-83`; discovery must not append Reaper/Stargate keyspaces. |
| `apis/k8ssandra/v1alpha1/zz_generated.deepcopy.go` | Regenerate with `make generate`; never hand-edit nested map/Secret-ref deep-copy code. |
| `config/crd/bases/k8ssandra.io_k8ssandraclusters.yaml`, `charts/k8ssandra-operator/crds/k8ssandra-operator-crds.yaml` | Regenerate from Kubebuilder markers via `make manifests`; chart CRD bundle is copied by `scripts/prepare-helm-release.sh:15-28`. |
| `docs/content/en/reference/crd/k8ssandra-operator-crds-latest/_index.md` | Regenerate current CRD documentation after schema changes; do not change historical release reference pages. |

### Admission and qualification

| Location | Exact pattern to follow |
|---|---|
| `apis/k8ssandra/v1alpha1/k8ssandracluster_webhook.go:60-75` | The typed defaulter exists but setup currently calls only `.WithValidator(...)`. Register `.WithDefaulter(...)`, inject the immutable discovery version marker only on create, and reject a preexisting client-supplied marker in the mutating step. |
| `apis/k8ssandra/v1alpha1/k8ssandracluster_webhook.go:77-89` | Add a `+kubebuilder:webhook` mutating marker with `failurePolicy=fail`, `verbs=create`, and the K8ssandraCluster path; keep the existing validating marker fail-closed. Validate create requires the injected marker for the discovery-aware path. |
| `apis/k8ssandra/v1alpha1/k8ssandracluster_webhook.go:91-141` | Add structural/cross-field create validation: Cassandra pointer safety, IP literals, unsupported external secrets, exact external-DC constraints where admission has enough state. Live Cassandra checks remain controller/worker responsibilities. |
| `apis/k8ssandra/v1alpha1/k8ssandracluster_webhook.go:176-217` | Reject marker mutation/removal and incompatible accepted-snapshot mutations. Preserve non-retroactivity: never add a marker during update/defaulting. |
| `main.go:207-222` | Webhook setup is already wired in control-plane mode; constructor changes belong here if the defaulter/validator need explicit clients/config. |
| `config/webhook/manifests.yaml:1-72` | Generated mutating config currently has only the Pod secret-inject webhook; generation must add the K8ssandraCluster create mutation and keep `failurePolicy: Fail`. |
| `charts/k8ssandra-operator/templates/admissionwebhookconfiguration.yaml:1-35` | Checked-in Helm mutating config likewise covers Pods only; add the generated-equivalent K8ssandraCluster webhook under the control-plane/cluster-scoped conditions. |
| `charts/k8ssandra-operator/templates/validatingwebhookconfiguration.yaml:1-53` | Preserve control-plane validation registration. The data-plane-only branch intentionally omits K8ssandraCluster validation (`validatingwebhookconfiguration.yaml:55-84`). |
| `apis/k8ssandra/v1alpha1/k8ssandracluster_webhook_test.go:75-185` | Extend the real envtest admission suite; test API CREATE/UPDATE rather than only invoking helper methods. |

### Pre-creation reconciliation and workload

| Location | Exact pattern to follow |
|---|---|
| `controllers/k8ssandra/k8ssandracluster_controller.go:131-150` | Insert qualification/discovery gating after deletion/finalizer/basic validation/Cassandra nil checks and **before** `reconcileSuperuserSecret`. FR-004 says only discovery support resources may exist before acceptance; putting the gate only in `reconcileDatacenters` would allow current superuser/Reaper/Medusa/replicated-secret creation first. |
| `controllers/k8ssandra/k8ssandracluster_controller.go:72-82` | Inject narrow discovery workload/result, clock/backoff and hashing dependencies as needed. Avoid a direct driver singleton/global. |
| `controllers/k8ssandra/legacy_rf_discovery.go` (new) | Natural focused controller file for qualification, phase/reason transitions, first-DC context/namespace resolution, Job/ConfigMap reconciliation, result validation, authoritative revalidation, optimistic status acceptance and cleanup. Keep pure canonicalization/fingerprint parsing in a small `pkg/cassandra` or dedicated `pkg/discovery` package only if it has independent tests/reuse. |
| `pkg/clientcache/cache.go:41-50` | Resolve the first planned DC's `K8sContext`; empty context correctly means the local cluster. Namespace resolution should match `controllers/k8ssandra/seeds.go:31-34` and `datacenters.go:79-82`. |
| `controllers/k8ssandra/k8ssandracluster_controller.go:237-325` | Add watches for discovery result ConfigMaps and, if status-driven, `batchv1.Job`. Existing watched-by label mapping at `245-255` and local/remote ConfigMap watches at `295-296,315-318` can already requeue a correctly labelled result. Add equivalent remote Job watches or use bounded polling. Add Secret field indexes/map functions so credential/TLS Secret resource-version changes enqueue the owning cluster. |
| `pkg/labels/labels.go` and `apis/k8ssandra/v1alpha1/constants.go:61-62` | Label support resources with owning cluster name/namespace so existing `clusterLabelFilter` works across contexts. Cross-cluster owner references are invalid; labels plus immutable UID/generation/result bindings must supply ownership semantics. |
| `controllers/k8ssandra/datacenters.go:43-71` | Discovery-enabled objects must bypass the flat `checkInitialSystemReplication` behavior and build DC configs without serializing discovered per-keyspace maps into bootstrap properties. |
| `controllers/k8ssandra/datacenters.go:145-151` | Reconcile EndpointSlices from the accepted canonical seed set, not mutable `spec.additionalSeeds`, after acceptance. Current seed filtering already silently drops non-IP entries at `seeds.go:80-91`; discovery validation must fail instead. |
| `controllers/k8ssandra/datacenters.go:244-255` | Immediately before `remoteClient.Create(ctx, desiredDc)`, re-read authoritative CR/status and revalidate UID, generation, marker, snapshot hash, seed digest, planned names and absence of all planned DCs. This is the final FR-025a gate. A check only at function entry is vulnerable to races. |
| `Dockerfile:1-42` | The image currently builds and copies only `/manager`. If discovery is a separate executable, build/copy it and set the Job command explicitly. If it is a manager subcommand, add explicit command dispatch without weakening the manager entry point. Do not assume an unused binary exists in the image. |
| `config/cass-operator/imageconfig/patch-image-config.yaml:7-33`, `charts/k8ssandra-operator/values.yaml:15-26` | If a distinct discovery image is introduced, add it to ImageConfig generation/defaults and registry overrides. Reusing the operator image avoids a second supply-chain artifact but still needs explicit Job command, pull policy/secrets and a non-root/read-only security context. |

### Result persistence, concurrency and observability

| Location | Exact pattern to follow |
|---|---|
| `controllers/k8ssandra/k8ssandracluster_controller.go:100-128` | Current wrapper always mirrors raw errors into `.status.error` and Warning Events. Discovery failures need sanitized stable status and transition-only Events; do not pass credential/driver payloads into this generic path. |
| `controllers/k8ssandra/schemas.go:394-402` | `versionCheck` demonstrates `client.MergeFromWithOptimisticLock{}` and conflict requeue. Snapshot acceptance needs the same optimistic-lock pattern plus authoritative uncached re-reads and must stop reconciliation after a successful status write/read-back. |
| `pkg/clientcache/cache.go:53-55,138-156` | The cache exposes local cached and uncached clients, but no remote uncached client. Decide explicitly whether authoritative remote DC absence checks can tolerate cached reads; otherwise extend the cache to retain remote direct clients/rest configs. This is a real race-sensitive gap. |
| `main.go:104-112,135-143` and `charts/k8ssandra-operator/templates/deployment.yaml:35-38` | Enable and package leader election for supported multi-replica operation, or prove compare-and-swap plus create-time revalidation under concurrent reconcilers. Current defaults do neither. |
| `controllers/k8ssandra/k8ssandracluster_controller.go:79,97,118` | Reuse the injected event recorder and events RBAC. Emit only when phase/reason changes; include namespaced cluster identity and sanitized corrective action. |
| `pkg/config/config.go:13-75` | Existing delays are configuration-injected. Add bounded discovery retry/backoff settings here only if operators must configure them; otherwise keep named internal defaults and inject clock/random for tests. |

### Post-Ready managed replication

| Location | Exact pattern to follow |
|---|---|
| `controllers/k8ssandra/datacenters.go:206-235` | `checkSchemas` remains correctly gated on a Ready, observed-generation DC. Discovery must happen earlier; snapshot-aware live validation happens here after startup. |
| `controllers/k8ssandra/schemas.go:33-46` | Branch system-keyspace reconciliation on an accepted discovery snapshot before other Reaper/Stargate/user-schema work. A blocked external drift must stop the rest of schema reconciliation. |
| `controllers/k8ssandra/schemas.go:168-212` | Preserve the legacy path for unmarked/non-discovery objects. For accepted discovery objects, replace the one-map `EnsureKeyspaceReplication` loop with read-all-three, prevalidate-all-three, merge managed entries, then direct ALTER only where managed projections differ. |
| `controllers/k8ssandra/schemas.go:359-390` | Existing conversion helper drops `class` and parses values with `strconv.Atoi`; new parsing must first require/canonicalize NetworkTopologyStrategy, reject malformed/null/missing rows, preserve omission, and enforce 1..MaxInt32 independent of host `int` size. |
| `pkg/cassandra/management.go:205-246` | Reuse direct `AlterKeyspace` and live `GetKeyspaceReplication`. Do not use `EnsureKeyspaceReplication` (`288-317`) in the discovery path. Its facade is post-Ready and pod-backed, not the discovery transport. |
| `pkg/cassandra/config.go:105-122` | Keep discovered replication out of `ApplySystemReplication`. The accepted canonical seeds may flow through DC construction, but the three discovered maps may not be flattened into startup properties. |

### RBAC, manifests and configuration

| Location | Exact pattern to follow |
|---|---|
| `controllers/k8ssandra/k8ssandracluster_controller.go:84-98` | Extend Kubebuilder RBAC markers for `batch/jobs` create/get/list/watch/delete (and patch/update only if required), discovery result ConfigMaps, and Secret reads in data-plane namespaces. Current batch access is CronJobs only and Pod access is read-only. |
| `config/rbac/role.yaml:8-63` | Generated role currently grants ConfigMap/Secret CRUD, Pod read, and CronJob CRUD but no Job access. Regenerate; do not hand-edit generated role as the source of truth. |
| `scripts/prepare-helm-release.sh:15-32` | `make manifests` invokes this script to copy generated RBAC rules into both Helm Role and ClusterRole. Verify both cluster-scoped and namespace-scoped charts after regeneration. |
| `config/manager/manager.yaml:11-23`, `charts/k8ssandra-operator/templates/deployment.yaml:11-38`, `charts/k8ssandra-operator/values.yaml:33-37` | Wire leader election/replica behavior consistently through Kustomize and Helm if multi-replica is supported. |
| `config/samples/k8ssandra.io_v1alpha1_k8ssandracluster.yaml:1-7` | The existing scaffold sample is invalid/stale (`foo: bar`) and is not a useful model. Put verified migration examples in the migration docs/tests instead of extending this placeholder without broader cleanup. |

### Other registries and non-integration points

- No HTTP route registry exists beyond controller-runtime webhooks. Admission routes are generated from markers and registered at `SetupK8ssandraClusterWebhookWithManager`; the manual Pod mutation route is registered in `controllers/secrets-webhook/secretswebhook.go:25-33`.
- No event bus/message consumer exists. Kubernetes watches in `SetupWithManager` are the event mechanism.
- Existing scheduled work is Medusa CronJobs and K8ssandra/Cassandra task CRs. Legacy RF discovery is not a recurring cron concern; use a bounded short-lived workload owned/correlated to the cluster.
- There is no auth/policy registry beyond CRD validation, admission, Kubernetes RBAC and secret references. Do not conflate target Cassandra superuser generation with legacy discovery authentication.
- There are no TypeScript/Python-style barrel exports. Go public API is package-level capitalization; API registration is through `init()`/`SchemeBuilder.Register` (`k8ssandracluster_types.go:585-587`) and `AddToScheme` in `main.go:88-102`.

## Codebase Maturity

- Approximate checked-in size excluding `.git`/vendor: 903 files; 250 Go files and about 64,217 Go lines; 91 `_test.go` files. YAML is about 308 files/121,475 lines. Markdown is inflated to about 3.15 million lines by checked-in generated historical CRD references, so it is not a meaningful handwritten-code measure.
- The repository has 1,046 commits on the current history and established conventions across multiple API groups, controllers, Helm/Kustomize delivery, envtest and Kind e2e. It is mature, but some older scaffolding and patterns remain.
- Language/toolchain is Go 1.26.3 from `go.mod`; controller-runtime is 0.23.3 and Kubernetes libraries are 0.35.5. The local tool reports Go 1.26.5. Dependency versions must be grounded separately by the Forge dependency phase; this map does not recommend adding or changing a driver version.
- Strong abstractions to reuse: `K8ssandraClusterReconciler` step pipeline, `ReconcileResult`, `ClientCache`, watched-by labels, Kubebuilder API markers/generation, `ManagementApiFactory` for post-Ready calls, Testify mocks, `MultiClusterTestEnv`, and the migration docs home.
- Maturity caveats that materially affect this feature: the aggregate datacenter reconcile method is already very large; webhook DI uses a package global; status has a generic raw error field; remote uncached clients are unavailable; checked-in Helm webhook templates require synchronization; leader election exists as a flag but is disabled by default and not passed by packaged deployments; no CQL discovery client/workload exists.
- Highest-risk integration errors are: gating after unrelated resources are created; accepting a snapshot without API read-back; using cached remote absence as authoritative without justification; flattening three maps into the initial annotation/bootstrap property; calling `EnsureKeyspaceReplication`; silently dropping invalid/FQDN seeds; assuming the marker is server-owned without a fail-closed mutating path; exposing driver/secret data through `.status.error`, Events or Job result ConfigMaps; and treating local/envtest evidence as proof of live Cassandra bootstrap preservation.
