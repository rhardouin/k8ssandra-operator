package v1alpha1

const (
	ResourceHashAnnotation = "k8ssandra.io/resource-hash"

	// PerNodeConfigHashAnnotation is the annotation used to store the hash of the per-node
	// ConfigMap into the PodTemplateSpec of the CassandraDatacenter resource. By storing the
	// ConfigMap hash, the PodTemplateSpec changes when the ConfigMap changes, thus allowing the
	// changes to the ConfigMap to be properly detected and applied.
	PerNodeConfigHashAnnotation = "k8ssandra.io/per-node-config-hash"

	// InitialSystemReplicationAnnotation provides the initial replication of system keyspaces
	// (system_auth, system_distributed, system_traces) encoded as JSON. This annotation
	// is set on a K8ssandraCluster when it is first created. The value does not change
	// regardless of whether the replication of the system keyspaces changes.
	InitialSystemReplicationAnnotation = "k8ssandra.io/initial-system-replication"

	LegacyRFDiscoveryMarkerAnnotation = "k8ssandra.io/legacy-rf-discovery-version"
	LegacyRFDiscoveryMarkerVersion    = "v1"
	LegacyRFDiscoveryProtocolVersion  = "v1"
	LegacyRFDiscoveryMaxResultBytes   = 1 << 20
	LegacyRFDiscoveryMaxSeeds         = 32

	SystemAuthKeyspace        = "system_auth"
	SystemTracesKeyspace      = "system_traces"
	SystemDistributedKeyspace = "system_distributed"

	NetworkTopologyStrategyClass          = "NetworkTopologyStrategy"
	NetworkTopologyStrategyQualifiedClass = "org.apache.cassandra.locator.NetworkTopologyStrategy"

	// DcReplicationAnnotation tells the operator the replication settings to apply to user
	// keyspaces when adding a DC to an existing cluster. The value should be serialized
	// JSON, e.g., {"dc2": {"ks1": 3, "ks2": 3}}. All user keyspaces must be specified;
	// otherwise, reconciliation will fail with a validation error. If you do not want to
	// replicate a particular keyspace, specify a value of 0. Replication settings can be
	// specified for multiple DCs; however, existing DCs won't be modified, and only the DC
	// currently being added will be updated. Specifying multiple DCs can be useful though
	// if you add multiple DCs to the cluster at once (Note that the CassandraDatacenters
	// are still deployed serially).
	DcReplicationAnnotation = "k8ssandra.io/dc-replication"

	// Deprecated: Use K8ssandraClusterSpec.Cassandra.Rebuild.SourceDC instead.
	// DeprecatedRebuildSourceDcAnnotation tells the operation the DC from which to stream when rebuilding a DC.
	// If not set the operator will choose the first DC. The value for
	// this annotation must specify the name of a CassandraDatacenter whose Ready condition is true.
	DeprecatedRebuildSourceDcAnnotation = "k8ssandra.io/rebuild-src-dc"

	RebuildDcAnnotation = "k8ssandra.io/rebuild-dc"

	NameLabel      = "app.kubernetes.io/name"
	NameLabelValue = "k8ssandra-operator"

	ManagedByLabel = "app.kubernetes.io/managed-by"

	ComponentLabel               = "app.kubernetes.io/component"
	ComponentLabelValueCassandra = "cassandra"
	ComponentLabelValueStargate  = "stargate"
	ComponentLabelValueReaper    = "reaper"
	ComponentLabelTelemetry      = "telemetry"

	PartOfLabel      = "app.kubernetes.io/part-of"
	PartOfLabelValue = "k8ssandra"

	// ReplicatedByLabel is used to label secrets that should be selected for replication by a ReplicatedSecret.
	ReplicatedByLabel      = "k8ssandra.io/replicated-by"
	ReplicatedByLabelValue = "k8ssandracluster-controller"

	// ReplicatedSecretLabel is used to label secrets to identify which ReplicatedSecret created them.
	ReplicatedSecretLabel = "k8ssandra.io/replicated-secret-name"

	CleanedUpByLabel      = "k8ssandra.io/cleaned-up-by"
	CleanedUpByLabelValue = "k8ssandracluster-controller"

	K8ssandraClusterNameLabel      = "k8ssandra.io/cluster-name"
	K8ssandraClusterNamespaceLabel = "k8ssandra.io/cluster-namespace"

	DatacenterLabel = "k8ssandra.io/datacenter"
	// Forces refresh of secrets which relate to roles and authn in Cassandra.
	RefreshAnnotation = "k8ssandra.io/refresh"

	// Annotation to indicate the purpose of a given resource.
	PurposeAnnotation = "k8ssandra.io/purpose"

	// AutomatedUpdateAnnotation is an annotation that allows the Datacenters to be updated even if no changes were done to the K8ssandraCluster spec
	AutomatedUpdateAnnotation = "k8ssandra.io/autoupdate-spec"

	AllowUpdateAlways AllowUpdateType = "always"
	AllowUpdateOnce   AllowUpdateType = "once"

	LegacyRFDiscoveryPhaseNotRequired LegacyRFDiscoveryPhase = "NotRequired"
	LegacyRFDiscoveryPhasePending     LegacyRFDiscoveryPhase = "Pending"
	LegacyRFDiscoveryPhaseBlocked     LegacyRFDiscoveryPhase = "Blocked"
	LegacyRFDiscoveryPhaseAccepted    LegacyRFDiscoveryPhase = "Accepted"

	LegacyRFReasonAdmissionUnavailable           LegacyRFDiscoveryReason = "AdmissionUnavailable"
	LegacyRFReasonMarkerInvalid                  LegacyRFDiscoveryReason = "MarkerInvalid"
	LegacyRFReasonUnsupportedServerType          LegacyRFDiscoveryReason = "UnsupportedServerType"
	LegacyRFReasonUnsupportedSourceVersion       LegacyRFDiscoveryReason = "UnsupportedSourceVersion"
	LegacyRFReasonUnsupportedSecretsProvider     LegacyRFDiscoveryReason = "UnsupportedSecretsProvider"
	LegacyRFReasonDiscoveryTooLate               LegacyRFDiscoveryReason = "DiscoveryTooLate"
	LegacyRFReasonInvalidContactPoint            LegacyRFDiscoveryReason = "InvalidContactPoint"
	LegacyRFReasonJobSchedulingFailed            LegacyRFDiscoveryReason = "JobSchedulingFailed"
	LegacyRFReasonWorkerImageUnavailable         LegacyRFDiscoveryReason = "WorkerImageUnavailable"
	LegacyRFReasonWorkerImagePullFailed          LegacyRFDiscoveryReason = "WorkerImagePullFailed"
	LegacyRFReasonDiscoveryDeadlineExceeded      LegacyRFDiscoveryReason = "DiscoveryDeadlineExceeded"
	LegacyRFReasonCredentialSecretInvalid        LegacyRFDiscoveryReason = "CredentialSecretInvalid"
	LegacyRFReasonTLSMaterialInvalid             LegacyRFDiscoveryReason = "TLSMaterialInvalid"
	LegacyRFReasonAuthenticationRejected         LegacyRFDiscoveryReason = "AuthenticationRejected"
	LegacyRFReasonAuthorizationDenied            LegacyRFDiscoveryReason = "AuthorizationDenied"
	LegacyRFReasonTLSFailed                      LegacyRFDiscoveryReason = "TLSFailed"
	LegacyRFReasonContactUnreachable             LegacyRFDiscoveryReason = "ContactUnreachable"
	LegacyRFReasonIdentityMismatch               LegacyRFDiscoveryReason = "IdentityMismatch"
	LegacyRFReasonSchemaDisagreement             LegacyRFDiscoveryReason = "SchemaDisagreement"
	LegacyRFReasonTopologyInconsistent           LegacyRFDiscoveryReason = "TopologyInconsistent"
	LegacyRFReasonManagedDatacenterNameCollision LegacyRFDiscoveryReason = "ManagedDatacenterNameCollision"
	LegacyRFReasonMissingKeyspace                LegacyRFDiscoveryReason = "MissingKeyspace"
	LegacyRFReasonUnsupportedStrategy            LegacyRFDiscoveryReason = "UnsupportedStrategy"
	LegacyRFReasonInvalidReplication             LegacyRFDiscoveryReason = "InvalidReplication"
	LegacyRFReasonStaleDiscoveryResult           LegacyRFDiscoveryReason = "StaleDiscoveryResult"
	LegacyRFReasonInvalidDiscoveryResult         LegacyRFDiscoveryReason = "InvalidDiscoveryResult"
	LegacyRFReasonForgedDiscoveryResult          LegacyRFDiscoveryReason = "ForgedDiscoveryResult"
	LegacyRFReasonDiscoveryResultTooLarge        LegacyRFDiscoveryReason = "DiscoveryResultTooLarge"
	LegacyRFReasonKubernetesAPIUnavailable       LegacyRFDiscoveryReason = "KubernetesAPIUnavailable"
	LegacyRFReasonKubernetesAPIConflict          LegacyRFDiscoveryReason = "KubernetesAPIConflict"
	LegacyRFReasonSnapshotConflict               LegacyRFDiscoveryReason = "SnapshotConflict"
	LegacyRFReasonManagedStatePresent            LegacyRFDiscoveryReason = "ManagedStatePresent"
	LegacyRFReasonExternalReplicationDrift       LegacyRFDiscoveryReason = "ExternalReplicationDrift"
)

// LegacyRFDiscoveryFailure is the public, sanitized classification of a discovery failure.
// It intentionally has no field capable of carrying a raw cause.
type LegacyRFDiscoveryFailure struct {
	Reason    LegacyRFDiscoveryReason `json:"reason"`
	Retryable bool                    `json:"retryable"`
	Message   string                  `json:"message"`
}

// LegacyRFDiscoveryFailureForReason returns the stable public classification for reason.
func LegacyRFDiscoveryFailureForReason(reason LegacyRFDiscoveryReason) (LegacyRFDiscoveryFailure, bool) {
	retryable, message, found := legacyRFFailureDetails(reason)
	return LegacyRFDiscoveryFailure{Reason: reason, Retryable: retryable, Message: message}, found
}

func legacyRFFailureDetails(reason LegacyRFDiscoveryReason) (bool, string, bool) {
	if retryable, message, found := legacyRFPrerequisiteFailure(reason); found {
		return retryable, message, true
	}
	if retryable, message, found := legacyRFSourceFailure(reason); found {
		return retryable, message, true
	}
	return legacyRFIntegrityFailure(reason)
}

func legacyRFPrerequisiteFailure(reason LegacyRFDiscoveryReason) (bool, string, bool) {
	switch reason {
	case LegacyRFReasonAdmissionUnavailable:
		return true, "Discovery admission is unavailable; retry after restoring the webhook.", true
	case LegacyRFReasonMarkerInvalid:
		return false, "The discovery marker is missing or invalid; recreate the migration through supported admission.", true
	case LegacyRFReasonUnsupportedServerType:
		return false, "Legacy replication discovery supports Apache Cassandra only.", true
	case LegacyRFReasonUnsupportedSourceVersion:
		return false, "The legacy source must run Apache Cassandra 4.0 or newer.", true
	case LegacyRFReasonUnsupportedSecretsProvider:
		return false, "Legacy discovery requires the internal Secrets provider.", true
	case LegacyRFReasonDiscoveryTooLate, LegacyRFReasonManagedStatePresent:
		return false, "Managed Cassandra state already exists; discovery cannot start safely.", true
	case LegacyRFReasonInvalidContactPoint:
		return false, "Use valid IP-literal additional seeds without ports, zones, or whitespace.", true
	case LegacyRFReasonJobSchedulingFailed:
		return true, "The discovery Job could not be scheduled; inspect data-plane capacity and policy.", true
	case LegacyRFReasonWorkerImageUnavailable, LegacyRFReasonWorkerImagePullFailed:
		return true, "The discovery worker image is unavailable; restore image access.", true
	case LegacyRFReasonDiscoveryDeadlineExceeded:
		return true, "Discovery exceeded its bounded deadline; restore prerequisites and retry.", true
	default:
		return false, "", false
	}
}

func legacyRFSourceFailure(reason LegacyRFDiscoveryReason) (bool, string, bool) {
	switch reason {
	case LegacyRFReasonCredentialSecretInvalid:
		return true, "The discovery credential Secret is unavailable or invalid; restore its required keys.", true
	case LegacyRFReasonTLSMaterialInvalid:
		return true, "The discovery TLS material is unavailable or invalid; restore the referenced Secret.", true
	case LegacyRFReasonAuthenticationRejected:
		return true, "The legacy source rejected authentication; verify the dedicated discovery credential.", true
	case LegacyRFReasonAuthorizationDenied:
		return false, "The discovery identity lacks required read permissions on the legacy source.", true
	case LegacyRFReasonTLSFailed:
		return true, "TLS verification failed; verify trust material and certificate IP identity.", true
	case LegacyRFReasonContactUnreachable:
		return true, "No configured contact point completed discovery; restore source network access.", true
	case LegacyRFReasonIdentityMismatch:
		return false, "The contacted source does not match the expected Cassandra cluster identity.", true
	case LegacyRFReasonSchemaDisagreement:
		return true, "The source schema changed during observation; retry after schema agreement is restored.", true
	case LegacyRFReasonTopologyInconsistent:
		return true, "The source topology is incomplete or inconsistent; retry after it stabilizes.", true
	case LegacyRFReasonManagedDatacenterNameCollision:
		return false, "A managed datacenter name collides with an observed external datacenter.", true
	case LegacyRFReasonMissingKeyspace:
		return false, "A required system keyspace is missing from the legacy source.", true
	case LegacyRFReasonUnsupportedStrategy:
		return false, "A required system keyspace does not use NetworkTopologyStrategy.", true
	case LegacyRFReasonInvalidReplication:
		return false, "A required system keyspace has invalid replication metadata.", true
	default:
		return false, "", false
	}
}

func legacyRFIntegrityFailure(reason LegacyRFDiscoveryReason) (bool, string, bool) {
	switch reason {
	case LegacyRFReasonStaleDiscoveryResult, LegacyRFReasonKubernetesAPIConflict:
		return true, "Discovery inputs changed concurrently; retry using current authoritative state.", true
	case LegacyRFReasonInvalidDiscoveryResult, LegacyRFReasonForgedDiscoveryResult, LegacyRFReasonDiscoveryResultTooLarge:
		return true, "The discovery result failed integrity validation; discard it and retry discovery.", true
	case LegacyRFReasonKubernetesAPIUnavailable:
		return true, "Authoritative Kubernetes state is unavailable; restore API access before retrying.", true
	case LegacyRFReasonSnapshotConflict:
		return false, "The accepted discovery snapshot is missing or conflicts with managed-state history.", true
	case LegacyRFReasonExternalReplicationDrift:
		return true, "External system-keyspace replication differs from the accepted snapshot; resolve external drift.", true
	default:
		return false, "", false
	}
}

// TODO Use the accepted values from cass-operator's api instead to prevent drift, once Kubernetes dependencies are updated in k8ssandra-operator
type AllowUpdateType string

var (
	SystemKeyspaces = []string{"system_traces", "system_distributed", "system_auth"}
	DseKeyspaces    = []string{"dse_leases", "dse_perf", "dse_security"}
)
