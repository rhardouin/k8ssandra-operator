package k8ssandra

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/netip"
	"reflect"

	cassdcapi "github.com/k8ssandra/cass-operator/apis/cassandra/v1beta1"
	api "github.com/k8ssandra/k8ssandra-operator/apis/k8ssandra/v1alpha1"
	"github.com/k8ssandra/k8ssandra-operator/pkg/clientcache"
	"github.com/k8ssandra/k8ssandra-operator/pkg/discovery"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// LegacyRFControlPlane performs the two explicit direct control-plane
// operations needed by acceptance and creation authorization.
type LegacyRFControlPlane interface {
	Read(context.Context, types.NamespacedName) (*api.K8ssandraCluster, error)
	PatchStatusCAS(context.Context, *api.K8ssandraCluster, *api.K8ssandraCluster) error
}

type directLegacyRFControlPlane struct {
	client client.Client
}

// NewLegacyRFControlPlane binds safety operations to an uncached control-plane client.
func NewLegacyRFControlPlane(directClient client.Client) (LegacyRFControlPlane, error) {
	if directClient == nil {
		return nil, fmt.Errorf("create legacy RF control plane: direct client is required")
	}
	return &directLegacyRFControlPlane{client: directClient}, nil
}

func (control *directLegacyRFControlPlane) Read(
	ctx context.Context,
	key types.NamespacedName,
) (*api.K8ssandraCluster, error) {
	cluster := &api.K8ssandraCluster{}
	if err := control.client.Get(ctx, key, cluster); err != nil {
		return nil, fmt.Errorf("direct-read K8ssandraCluster %s: %w", key, err)
	}
	return cluster, nil
}

func (control *directLegacyRFControlPlane) PatchStatusCAS(
	ctx context.Context,
	base, updated *api.K8ssandraCluster,
) error {
	return PatchLegacyRFDiscoveryStatusCAS(ctx, control.client, base, updated)
}

// LegacyRFAcceptanceInput binds an acceptance attempt to one cluster version.
type LegacyRFAcceptanceInput struct {
	Key         types.NamespacedName
	ExpectedUID types.UID
	Attempt     discovery.Attempt
	Result      discovery.DiscoveryResult
	Snapshot    *api.LegacyRFSnapshot
	Secrets     LegacyRFSecretState
}

// LegacyRFSecretState authoritatively revalidates credential and TLS bindings.
type LegacyRFSecretState interface {
	ValidateBindings(context.Context, []discovery.SecretBinding) error
}

// LegacyRFAcceptanceOutcome forces the caller to stop after a status patch.
type LegacyRFAcceptanceOutcome struct {
	StatusPatched bool
	Stop          bool
}

// AcceptLegacyRFSnapshot performs direct revalidation, absence survey, and a
// single optimistic status patch. It never reads back or creates managed state.
func AcceptLegacyRFSnapshot(
	ctx context.Context,
	control LegacyRFControlPlane,
	managedState ManagedDatacenterState,
	input LegacyRFAcceptanceInput,
) (LegacyRFAcceptanceOutcome, error) {
	stop := LegacyRFAcceptanceOutcome{Stop: true}
	if control == nil || managedState == nil || input.Secrets == nil {
		return stop, fmt.Errorf("accept legacy RF snapshot: control plane, managed state, and Secret state are required")
	}
	current, err := control.Read(ctx, input.Key)
	if err != nil {
		return stop, legacyRFStatusBoundary(api.LegacyRFReasonKubernetesAPIUnavailable, "read cluster for acceptance", err)
	}
	plan, err := validateLegacyRFAcceptanceBindings(current, input)
	if err != nil {
		return stop, legacyRFStatusBoundary(api.LegacyRFReasonStaleDiscoveryResult, "revalidate acceptance bindings", err)
	}
	domain := LegacyRFManagedSearchDomain(plan.ManagedLocations, input.Snapshot, current.Status.LegacyRFDiscovery)
	if err = managedState.AssertAbsent(ctx, domain, true); err != nil {
		return stop, managedSurveyBoundary("survey before snapshot acceptance", err)
	}
	if err = input.Secrets.ValidateBindings(ctx, input.Attempt.Connection.SecretBindings); err != nil {
		return stop, legacyRFStatusBoundary(api.LegacyRFReasonStaleDiscoveryResult,
			"revalidate Secret bindings immediately before acceptance", err)
	}
	accepted, err := BuildLegacyRFAcceptedStatus(current.Status.LegacyRFDiscovery, input.Snapshot, plan.ManagedLocations)
	if err != nil {
		return stop, legacyRFStatusBoundary(api.LegacyRFReasonSnapshotConflict, "build accepted status", err)
	}
	updated := current.DeepCopy()
	updated.Status.LegacyRFDiscovery = &accepted
	if err = control.PatchStatusCAS(ctx, current, updated); err != nil {
		return stop, legacyRFStatusBoundary(api.LegacyRFReasonKubernetesAPIConflict, "patch accepted status", err)
	}
	stop.StatusPatched = true
	return stop, nil
}

func validateLegacyRFAcceptanceBindings(
	cluster *api.K8ssandraCluster,
	input LegacyRFAcceptanceInput,
) (LegacyRFCurrentPlan, error) {
	if cluster == nil || input.Snapshot == nil || input.ExpectedUID == "" || cluster.UID != input.ExpectedUID {
		return LegacyRFCurrentPlan{}, errors.New("cluster UID or snapshot changed")
	}
	if cluster.Annotations[api.LegacyRFDiscoveryMarkerAnnotation] != input.Attempt.MarkerVersion ||
		input.Attempt.MarkerVersion != api.LegacyRFDiscoveryMarkerVersion {
		return LegacyRFCurrentPlan{}, errors.New("discovery marker changed")
	}
	if cluster.Generation != input.Attempt.Generation || input.Result.Generation != input.Attempt.Generation ||
		input.Snapshot.AcceptedGeneration != input.Attempt.Generation {
		return LegacyRFCurrentPlan{}, errors.New("attempt generation changed")
	}
	if err := validateLegacyRFResultBindings(cluster, input); err != nil {
		return LegacyRFCurrentPlan{}, err
	}
	plan, err := BuildLegacyRFCurrentPlan(cluster)
	if err != nil {
		return LegacyRFCurrentPlan{}, err
	}
	if err = ValidateCurrentPlan(input.Snapshot, plan); err != nil {
		return LegacyRFCurrentPlan{}, err
	}
	if !reflect.DeepEqual(plan.ManagedLocations, input.Snapshot.AcceptedManagedLocations) ||
		len(plan.ManagedLocations) == 0 || plan.ManagedLocations[0] != input.Snapshot.DiscoveryLocation {
		return LegacyRFCurrentPlan{}, errors.New("planned managed locations changed")
	}
	return plan, nil
}

func validateLegacyRFResultBindings(cluster *api.K8ssandraCluster, input LegacyRFAcceptanceInput) error {
	canonical, digest, err := discovery.CanonicalizeSeeds(cluster.Spec.Cassandra.AdditionalSeeds)
	if err != nil {
		return fmt.Errorf("canonicalize current seeds: %w", err)
	}
	if digest != input.Attempt.SeedDigest || digest != input.Result.SeedDigest ||
		digest != input.Snapshot.AcceptedSeedDigest || !reflect.DeepEqual(canonical, input.Attempt.OrderedSeeds) ||
		!reflect.DeepEqual(canonical, input.Result.OrderedSeeds) || !equalAcceptedSeedAddresses(canonical, input.Snapshot.AcceptedSeeds) {
		return errors.New("ordered seed binding changed")
	}
	attemptBindingsDigest, err := discovery.BindingsDigest(input.Attempt.Connection.SecretBindings)
	if err != nil {
		return fmt.Errorf("canonicalize attempt Secret bindings: %w", err)
	}
	resultBindingsDigest, err := discovery.BindingsDigest(input.Result.SecretBindings)
	if err != nil {
		return fmt.Errorf("canonicalize result Secret bindings: %w", err)
	}
	snapshotBindingsDigest, err := discovery.BindingsDigest(fromAPISecretBindings(input.Snapshot.SecretBindings))
	if err != nil {
		return fmt.Errorf("canonicalize snapshot Secret bindings: %w", err)
	}
	if attemptBindingsDigest != resultBindingsDigest || attemptBindingsDigest != snapshotBindingsDigest {
		return errors.New("secret or TLS binding changed")
	}
	if input.Attempt.WorkerImageDigest != input.Result.WorkerImageDigest ||
		input.Attempt.WorkerImageDigest != input.Snapshot.WorkerImageDigest {
		return errors.New("worker image digest changed")
	}
	return validateLegacyRFResultIdentity(cluster, input)
}

func validateLegacyRFResultIdentity(cluster *api.K8ssandraCluster, input LegacyRFAcceptanceInput) error {
	result := input.Result
	snapshot := input.Snapshot
	if err := discovery.ValidateResultHash(result); err != nil {
		return fmt.Errorf("result canonical hash changed: %w", err)
	}
	if result.ClusterUID != string(cluster.UID) || snapshot.ClusterUID != string(cluster.UID) ||
		result.MarkerVersion != input.Attempt.MarkerVersion || result.ProtocolVersion != input.Attempt.ProtocolVersion ||
		snapshot.MarkerVersion != input.Attempt.MarkerVersion || snapshot.ProtocolVersion != input.Attempt.ProtocolVersion {
		return errors.New("result identity binding changed")
	}
	if result.AttemptID != input.Attempt.AttemptID || result.CanonicalHash == "" ||
		snapshot.ExpectedClusterName != cluster.CassClusterName() ||
		result.Authoritative.Endpoint.String() != snapshot.AuthoritativeEndpoint ||
		result.Authoritative.ClusterName != snapshot.ExpectedClusterName ||
		result.Authoritative.SourceVersion != snapshot.SourceVersion ||
		result.Authoritative.Partitioner != snapshot.Partitioner ||
		!reflect.DeepEqual(result.Authoritative.ObservedExternalDCs, snapshot.ObservedExternalDCs) ||
		result.Authoritative.Fingerprints.Identity != snapshot.IdentityFingerprint ||
		result.Authoritative.Fingerprints.Topology != snapshot.TopologyFingerprint ||
		result.Authoritative.Fingerprints.Schema != snapshot.SchemaFingerprint ||
		!reflect.DeepEqual(toAPIAttemptTrace(result.AttemptTrace), snapshot.AttemptTrace) ||
		!legacyRFResultReplicationMatchesSnapshot(result.Authoritative.Replication, snapshot.Replication) {
		return errors.New("result payload binding changed")
	}
	return nil
}

func legacyRFResultReplicationMatchesSnapshot(
	result discovery.SystemKeyspaceObservations,
	snapshot api.LegacySystemKeyspaceReplication,
) bool {
	return reflect.DeepEqual(result.SystemAuth.Replication, snapshot.SystemAuth) &&
		reflect.DeepEqual(result.SystemTraces.Replication, snapshot.SystemTraces) &&
		reflect.DeepEqual(result.SystemDistributed.Replication, snapshot.SystemDistributed)
}

func equalAcceptedSeedAddresses(endpoints []netip.AddrPort, addresses []string) bool {
	if len(endpoints) != len(addresses) {
		return false
	}
	for index := range endpoints {
		if endpoints[index].Addr().String() != addresses[index] {
			return false
		}
	}
	return true
}

func toAPISecretBindings(bindings []discovery.SecretBinding) []api.LegacyRFSecretBinding {
	converted := make([]api.LegacyRFSecretBinding, len(bindings))
	for index, binding := range bindings {
		converted[index] = api.LegacyRFSecretBinding{
			Purpose: binding.Purpose, SourceContext: binding.SourceContext, Namespace: binding.Namespace,
			Name: binding.Name, Keys: append([]string(nil), binding.Keys...), ResourceVersion: binding.ResourceVersion,
		}
	}
	return converted
}

func fromAPISecretBindings(bindings []api.LegacyRFSecretBinding) []discovery.SecretBinding {
	converted := make([]discovery.SecretBinding, len(bindings))
	for index, binding := range bindings {
		converted[index] = discovery.SecretBinding{
			Purpose: binding.Purpose, SourceContext: binding.SourceContext, Namespace: binding.Namespace,
			Name: binding.Name, Keys: append([]string(nil), binding.Keys...), ResourceVersion: binding.ResourceVersion,
		}
	}
	return converted
}

func legacyRFStatusBoundary(reason api.LegacyRFDiscoveryReason, operation string, cause error) error {
	return discovery.NewBoundaryError(reason, fmt.Errorf("%s: %w", operation, cause))
}

func managedSurveyBoundary(operation string, cause error) error {
	var present *ManagedStatePresentError
	if errors.As(cause, &present) {
		return legacyRFStatusBoundary(api.LegacyRFReasonManagedStatePresent, operation, cause)
	}
	return legacyRFStatusBoundary(api.LegacyRFReasonKubernetesAPIUnavailable, operation, cause)
}

func legacyRFSnapshotIsIntact(status *api.LegacyRFDiscoveryStatus) bool {
	if status == nil || status.Phase != api.LegacyRFDiscoveryPhaseAccepted ||
		status.SnapshotHash == "" || status.AcceptedSnapshot == nil || status.AcceptedSnapshot.Hash != status.SnapshotHash {
		return false
	}
	hash, err := LegacyRFSnapshotHash(status.AcceptedSnapshot)
	return err == nil && hash == status.SnapshotHash
}

// LegacyRFSnapshotHash hashes the canonical snapshot content, excluding its hash field.
func LegacyRFSnapshotHash(snapshot *api.LegacyRFSnapshot) (string, error) {
	if snapshot == nil {
		return "", errors.New("hash legacy RF snapshot: snapshot is required")
	}
	canonical := copyLegacyRFSnapshot(snapshot)
	canonical.Hash = ""
	encoded, err := json.Marshal(canonical)
	if err != nil {
		return "", fmt.Errorf("hash legacy RF snapshot: %w", err)
	}
	digest := sha256.Sum256(encoded)
	return hex.EncodeToString(digest[:]), nil
}

// ManagedDatacenterState authoritatively proves managed Cassandra state is absent.
type ManagedDatacenterState interface {
	AssertAbsent(context.Context, []api.LegacyRFManagedLocation, bool) error
}

// ManagedStatePresentError identifies managed state that makes discovery unsafe.
type ManagedStatePresentError struct {
	Location api.LegacyRFManagedLocation
	Kind     string
}

// Error implements error.
func (e *ManagedStatePresentError) Error() string {
	return fmt.Sprintf("managed %s is present at context %q namespace %q name %q",
		e.Kind, e.Location.K8sContext, e.Location.Namespace, e.Location.Name)
}

type directManagedDatacenterState struct {
	clients *clientcache.ClientCache
}

// NewManagedDatacenterState creates an authoritative managed-state survey.
func NewManagedDatacenterState(clients *clientcache.ClientCache) (ManagedDatacenterState, error) {
	if clients == nil {
		return nil, fmt.Errorf("create managed datacenter state: client cache is required")
	}
	return &directManagedDatacenterState{clients: clients}, nil
}

// AssertAbsent checks exact CassandraDatacenter keys and, when requested, their
// correlated Cassandra pods through direct API clients in every supplied location.
func (s *directManagedDatacenterState) AssertAbsent(
	ctx context.Context,
	locations []api.LegacyRFManagedLocation,
	includePods bool,
) error {
	for _, location := range appendUniqueLocations(nil, locations) {
		if err := validateManagedLocation(location); err != nil {
			return err
		}
		directClient, err := s.clients.GetRemoteNonCacheClient(location.K8sContext)
		if err != nil {
			return fmt.Errorf("survey managed location %q: %w", location.K8sContext, err)
		}
		if err := assertDatacenterAbsent(ctx, directClient, location); err != nil {
			return err
		}
		if includePods {
			if err := assertDatacenterPodsAbsent(ctx, directClient, location); err != nil {
				return err
			}
		}
	}
	return nil
}

func assertDatacenterAbsent(
	ctx context.Context,
	reader client.Reader,
	location api.LegacyRFManagedLocation,
) error {
	datacenter := &cassdcapi.CassandraDatacenter{}
	key := types.NamespacedName{Namespace: location.Namespace, Name: location.Name}
	if err := reader.Get(ctx, key, datacenter); err == nil {
		return &ManagedStatePresentError{Location: location, Kind: "CassandraDatacenter"}
	} else if !k8serrors.IsNotFound(err) {
		return fmt.Errorf("read CassandraDatacenter at context %q key %s: %w", location.K8sContext, key, err)
	}
	return nil
}

func assertDatacenterPodsAbsent(
	ctx context.Context,
	reader client.Reader,
	location api.LegacyRFManagedLocation,
) error {
	pods := &corev1.PodList{}
	selector := client.MatchingLabels{
		cassdcapi.DatacenterLabel: cassdcapi.CleanLabelValue(location.Name),
	}
	if err := reader.List(ctx, pods, client.InNamespace(location.Namespace), selector); err != nil {
		return fmt.Errorf("list Cassandra pods at context %q namespace %q: %w",
			location.K8sContext, location.Namespace, err)
	}
	if len(pods.Items) != 0 {
		return &ManagedStatePresentError{Location: location, Kind: "Cassandra pod"}
	}
	return nil
}

func validateManagedLocation(location api.LegacyRFManagedLocation) error {
	if location.Namespace == "" {
		return fmt.Errorf("survey managed location %q: namespace is required", location.K8sContext)
	}
	if location.Name == "" {
		return fmt.Errorf("survey managed location %q: datacenter name is required", location.K8sContext)
	}
	return nil
}

// LegacyRFManagedSearchDomain returns the de-duplicated current, accepted, and
// append-only historical locations that every authoritative survey must inspect.
func LegacyRFManagedSearchDomain(
	current []api.LegacyRFManagedLocation,
	snapshot *api.LegacyRFSnapshot,
	status *api.LegacyRFDiscoveryStatus,
) []api.LegacyRFManagedLocation {
	locations := appendUniqueLocations(nil, current)
	if status != nil {
		locations = appendUniqueLocations(locations, status.CurrentManagedLocations)
	}
	if snapshot != nil {
		locations = appendUniqueLocations(locations, snapshot.AcceptedManagedLocations)
	}
	if status != nil {
		locations = appendUniqueLocations(locations, status.ManagedLocationHistory)
	}
	return locations
}

// BuildLegacyRFAcceptedStatus creates the complete accepted status while
// retaining monotonic creation provenance from the prior status.
func BuildLegacyRFAcceptedStatus(
	previous *api.LegacyRFDiscoveryStatus,
	snapshot *api.LegacyRFSnapshot,
	current []api.LegacyRFManagedLocation,
) (api.LegacyRFDiscoveryStatus, error) {
	if snapshot == nil || snapshot.Hash == "" {
		return api.LegacyRFDiscoveryStatus{}, fmt.Errorf("build accepted legacy RF status: snapshot hash is required")
	}
	canonicalHash, err := LegacyRFSnapshotHash(snapshot)
	if err != nil || canonicalHash != snapshot.Hash {
		return api.LegacyRFDiscoveryStatus{}, fmt.Errorf("build accepted legacy RF status: snapshot content hash does not match")
	}
	if previous != nil && previous.AcceptedSnapshot != nil && previous.SnapshotHash != snapshot.Hash {
		return api.LegacyRFDiscoveryStatus{}, fmt.Errorf("build accepted legacy RF status: accepted snapshot is immutable")
	}

	next := copyLegacyRFDiscoveryStatus(previous)
	next.ObservedGeneration = snapshot.AcceptedGeneration
	next.Phase = api.LegacyRFDiscoveryPhaseAccepted
	next.Reason = ""
	next.Message = ""
	next.RetryCount = 0
	next.SnapshotHash = snapshot.Hash
	next.AcceptedSnapshot = copyLegacyRFSnapshot(snapshot)
	next.CurrentManagedLocations = appendUniqueLocations(nil, current)
	transitionTime := snapshot.AcceptedAt.DeepCopy()
	next.LastTransitionTime = transitionTime
	return next, nil
}

// BuildLegacyRFManagedCreationStatus records creation provenance monotonically.
func BuildLegacyRFManagedCreationStatus(
	previous *api.LegacyRFDiscoveryStatus,
	current []api.LegacyRFManagedLocation,
	authorized api.LegacyRFManagedLocation,
) (api.LegacyRFDiscoveryStatus, error) {
	if previous == nil {
		return api.LegacyRFDiscoveryStatus{}, fmt.Errorf("record managed creation: discovery status is required")
	}
	if err := validateManagedLocation(authorized); err != nil {
		return api.LegacyRFDiscoveryStatus{}, fmt.Errorf("record managed creation: %w", err)
	}

	next := copyLegacyRFDiscoveryStatus(previous)
	next.CurrentManagedLocations = appendUniqueLocations(nil, current)
	next.ManagedLocationHistory = appendUniqueLocations(next.ManagedLocationHistory, []api.LegacyRFManagedLocation{authorized})
	next.ManagedCreationObserved = true
	return next, nil
}

// PatchLegacyRFDiscoveryStatusCAS optimistically patches status. Read-back is a
// separate operation so callers cannot mistake this non-atomic step for acceptance.
func PatchLegacyRFDiscoveryStatusCAS(
	ctx context.Context,
	directClient client.Client,
	base, updated *api.K8ssandraCluster,
) error {
	if directClient == nil || base == nil || updated == nil {
		return fmt.Errorf("patch legacy RF discovery status: client, base, and updated cluster are required")
	}
	if base.UID != updated.UID || base.Namespace != updated.Namespace || base.Name != updated.Name ||
		base.ResourceVersion == "" || base.ResourceVersion != updated.ResourceVersion {
		return fmt.Errorf("patch legacy RF discovery status: object identity and resource version must match")
	}
	if err := validateLegacyRFStatusMonotonic(base.Status.LegacyRFDiscovery, updated.Status.LegacyRFDiscovery); err != nil {
		return err
	}
	patch := client.MergeFromWithOptions(base.DeepCopy(), client.MergeFromWithOptimisticLock{})
	if err := directClient.Status().Patch(ctx, updated, patch); err != nil {
		return fmt.Errorf("patch legacy RF discovery status for %s/%s: %w", updated.Namespace, updated.Name, err)
	}
	return nil
}

// ReadBackLegacyRFAcceptance performs the distinct authoritative read-back step
// and returns the concrete cluster only when both stored hashes match.
func ReadBackLegacyRFAcceptance(
	ctx context.Context,
	reader client.Reader,
	key types.NamespacedName,
	expectedUID types.UID,
	expectedHash string,
) (*api.K8ssandraCluster, error) {
	if reader == nil || expectedHash == "" {
		return nil, fmt.Errorf("read back legacy RF acceptance: reader and expected hash are required")
	}
	current := &api.K8ssandraCluster{}
	if err := reader.Get(ctx, key, current); err != nil {
		return nil, fmt.Errorf("read back legacy RF acceptance for %s: %w", key, err)
	}
	if expectedUID != "" && current.UID != expectedUID {
		return nil, fmt.Errorf("read back legacy RF acceptance for %s: cluster UID changed", key)
	}
	status := current.Status.LegacyRFDiscovery
	if !legacyRFSnapshotIsIntact(status) ||
		status.SnapshotHash != expectedHash || status.AcceptedSnapshot.Hash != expectedHash {
		return nil, fmt.Errorf("read back legacy RF acceptance for %s: snapshot hash mismatch", key)
	}
	return current, nil
}

func validateLegacyRFStatusMonotonic(previous, next *api.LegacyRFDiscoveryStatus) error {
	if previous == nil {
		return nil
	}
	if next == nil {
		return fmt.Errorf("patch legacy RF discovery status: status cannot be removed")
	}
	if previous.ManagedCreationObserved && !next.ManagedCreationObserved {
		return fmt.Errorf("patch legacy RF discovery status: managed creation tombstone cannot be cleared")
	}
	for _, historical := range previous.ManagedLocationHistory {
		if !containsManagedLocation(next.ManagedLocationHistory, historical) {
			return fmt.Errorf("patch legacy RF discovery status: managed location history cannot be pruned")
		}
	}
	if previous.AcceptedSnapshot != nil &&
		(next.AcceptedSnapshot == nil || previous.SnapshotHash != next.SnapshotHash) {
		return fmt.Errorf("patch legacy RF discovery status: accepted snapshot cannot be replaced")
	}
	return nil
}

func containsManagedLocation(locations []api.LegacyRFManagedLocation, expected api.LegacyRFManagedLocation) bool {
	for _, location := range locations {
		if location == expected {
			return true
		}
	}
	return false
}

func copyLegacyRFDiscoveryStatus(status *api.LegacyRFDiscoveryStatus) api.LegacyRFDiscoveryStatus {
	if status == nil {
		return api.LegacyRFDiscoveryStatus{}
	}
	copy := *status
	copy.AcceptedSnapshot = copyLegacyRFSnapshot(status.AcceptedSnapshot)
	copy.CurrentManagedLocations = append([]api.LegacyRFManagedLocation(nil), status.CurrentManagedLocations...)
	copy.ManagedLocationHistory = append([]api.LegacyRFManagedLocation(nil), status.ManagedLocationHistory...)
	if status.LastTransitionTime != nil {
		copy.LastTransitionTime = status.LastTransitionTime.DeepCopy()
	}
	return copy
}

func copyLegacyRFSnapshot(snapshot *api.LegacyRFSnapshot) *api.LegacyRFSnapshot {
	if snapshot == nil {
		return nil
	}
	copy := *snapshot
	copy.AcceptedSeeds = append([]string(nil), snapshot.AcceptedSeeds...)
	copy.AttemptTrace = append([]api.LegacyRFEndpointAttemptSummary(nil), snapshot.AttemptTrace...)
	copy.ObservedExternalDCs = append([]string(nil), snapshot.ObservedExternalDCs...)
	copy.SecretBindings = copyLegacyRFSecretBindings(snapshot.SecretBindings)
	copy.AcceptedManagedLocations = append([]api.LegacyRFManagedLocation(nil), snapshot.AcceptedManagedLocations...)
	copy.Replication.SystemAuth = copyRFMap(snapshot.Replication.SystemAuth)
	copy.Replication.SystemTraces = copyRFMap(snapshot.Replication.SystemTraces)
	copy.Replication.SystemDistributed = copyRFMap(snapshot.Replication.SystemDistributed)
	return &copy
}

func copyLegacyRFSecretBindings(bindings []api.LegacyRFSecretBinding) []api.LegacyRFSecretBinding {
	if bindings == nil {
		return nil
	}
	copy := make([]api.LegacyRFSecretBinding, len(bindings))
	for index := range bindings {
		copy[index] = bindings[index]
		copy[index].Keys = append([]string(nil), bindings[index].Keys...)
	}
	return copy
}

func copyRFMap(values map[string]int32) map[string]int32 {
	if values == nil {
		return nil
	}
	copy := make(map[string]int32, len(values))
	for key, value := range values {
		copy[key] = value
	}
	return copy
}
