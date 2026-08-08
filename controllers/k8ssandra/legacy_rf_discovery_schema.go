package k8ssandra

import (
	"context"
	"fmt"
	"sort"
	"strconv"

	"github.com/go-logr/logr"
	cassdcapi "github.com/k8ssandra/cass-operator/apis/cassandra/v1beta1"
	api "github.com/k8ssandra/k8ssandra-operator/apis/k8ssandra/v1alpha1"
	"github.com/k8ssandra/k8ssandra-operator/pkg/annotations"
	"github.com/k8ssandra/k8ssandra-operator/pkg/cassandra"
	"github.com/k8ssandra/k8ssandra-operator/pkg/result"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type legacyRFSchemaManagement interface {
	GetKeyspaceReplication(string) (map[string]string, error)
	AlterKeyspace(string, map[string]int) error
}

const legacyRFUserCreationGateOwnerAnnotation = "k8ssandra.io/legacy-rf-user-creation-gate"

type legacyRFSchemaLiveReplication struct {
	SystemAuth        map[string]string
	SystemTraces      map[string]string
	SystemDistributed map[string]string
}

type legacyRFManagedProjection struct {
	Desired map[string]int32
	Known   map[string]struct{}
}

type legacyRFSchemaChange struct {
	Keyspace    string
	Replication map[string]int
}

type legacyRFSchemaPlan struct {
	Changes []legacyRFSchemaChange
}

func (r *K8ssandraClusterReconciler) reconcileLegacyRFPreReadySchema(
	ctx context.Context,
	cluster *api.K8ssandraCluster,
	datacenter *cassdcapi.CassandraDatacenter,
	remoteClient client.Client,
	logger logr.Logger,
) result.ReconcileResult {
	managed, eligible, err := legacyRFPreReadyManagedProjection(cluster, datacenter)
	if err != nil {
		return result.Error(err)
	}
	if !eligible {
		return result.Continue()
	}
	if r.ManagementApi == nil {
		return result.Error(fmt.Errorf("create pre-Ready schema client: management API factory is required"))
	}
	managementAPI, err := r.ManagementApi.NewManagementApiFacade(ctx, datacenter, remoteClient, logger)
	if err != nil {
		return result.Error(fmt.Errorf("create pre-Ready schema client: %w", err))
	}
	reconcileResult := r.reconcileAcceptedLegacyRFSchemaWithManaged(cluster, managementAPI, managed, logger)
	if reconcileResult.Completed() || !legacyRFSchemaConditionReady(cluster) {
		return reconcileResult
	}
	if _, err := releaseLegacyRFUserCreationIfReady(ctx, cluster, remoteClient, datacenter); err != nil {
		return result.Error(err)
	}
	return result.RequeueSoon(r.DefaultDelay)
}

func releaseLegacyRFUserCreationIfReady(
	ctx context.Context,
	cluster *api.K8ssandraCluster,
	remoteClient client.Client,
	datacenter *cassdcapi.CassandraDatacenter,
) (bool, error) {
	if !cluster.Spec.IsAuthEnabled() ||
		cluster.Status.LegacyRFDiscovery == nil ||
		cluster.Status.LegacyRFDiscovery.Phase != api.LegacyRFDiscoveryPhaseAccepted ||
		!legacyRFSnapshotIsIntact(cluster.Status.LegacyRFDiscovery) ||
		!legacyRFSchemaConditionReady(cluster) ||
		datacenter.Annotations[cassdcapi.SkipUserCreationAnnotation] != "true" ||
		datacenter.Annotations[legacyRFUserCreationGateOwnerAnnotation] != "true" {
		return false, nil
	}
	if err := releaseLegacyRFUserCreation(ctx, remoteClient, datacenter); err != nil {
		return false, err
	}
	return true, nil
}

func releaseLegacyRFUserCreation(
	ctx context.Context,
	remoteClient client.Client,
	datacenter *cassdcapi.CassandraDatacenter,
) error {
	if datacenter.Annotations[cassdcapi.SkipUserCreationAnnotation] != "true" {
		return nil
	}
	updated := datacenter.DeepCopy()
	delete(updated.Annotations, cassdcapi.SkipUserCreationAnnotation)
	delete(updated.Annotations, legacyRFUserCreationGateOwnerAnnotation)
	if err := remoteClient.Patch(ctx, updated, client.MergeFrom(datacenter)); err != nil {
		return fmt.Errorf("release pre-Ready user creation: %w", err)
	}
	return nil
}

func legacyRFSchemaConditionReady(cluster *api.K8ssandraCluster) bool {
	return cluster.Status.GetConditionStatus(api.SystemKeyspaceReplicationReady) == corev1.ConditionTrue
}

func legacyRFPreReadyManagedProjection(
	cluster *api.K8ssandraCluster,
	datacenter *cassdcapi.CassandraDatacenter,
) (legacyRFManagedProjection, bool, error) {
	if cluster == nil || datacenter == nil ||
		annotations.GetAnnotation(cluster, api.LegacyRFDiscoveryMarkerAnnotation) != api.LegacyRFDiscoveryMarkerVersion ||
		cluster.Status.LegacyRFDiscovery == nil ||
		cluster.Status.LegacyRFDiscovery.Phase != api.LegacyRFDiscoveryPhaseAccepted {
		return legacyRFManagedProjection{}, false, nil
	}
	if datacenter.GetConditionStatus(cassdcapi.DatacenterHealthy) != corev1.ConditionTrue ||
		int32(len(datacenter.Status.NodeStatuses)) < datacenter.Spec.Size {
		return legacyRFManagedProjection{}, false, nil
	}
	if datacenter.Spec.Size < 1 {
		return legacyRFManagedProjection{}, false, fmt.Errorf("prepare pre-Ready managed replication: datacenter size must be positive")
	}
	managed := legacyRFManagedReplication(cluster)
	name := datacenter.DatacenterName()
	managed.Desired[name] = min(datacenter.Spec.Size, 3)
	managed.Known[name] = struct{}{}
	return managed, true, nil
}

type legacyRFExternalDriftError struct {
	Keyspace string
}

func (e *legacyRFExternalDriftError) Error() string {
	return fmt.Sprintf("external replication for keyspace %s differs from accepted snapshot", e.Keyspace)
}

type legacyRFKeyspaceInput struct {
	name     string
	snapshot map[string]int32
	live     map[string]string
}

func buildLegacyRFSchemaPlan(
	snapshot api.LegacySystemKeyspaceReplication,
	live legacyRFSchemaLiveReplication,
	managed legacyRFManagedProjection,
) (*legacyRFSchemaPlan, error) {
	inputs := []legacyRFKeyspaceInput{
		{name: api.SystemAuthKeyspace, snapshot: snapshot.SystemAuth, live: live.SystemAuth},
		{name: api.SystemTracesKeyspace, snapshot: snapshot.SystemTraces, live: live.SystemTraces},
		{name: api.SystemDistributedKeyspace, snapshot: snapshot.SystemDistributed, live: live.SystemDistributed},
	}
	parsed := make([]map[string]int32, len(inputs))
	for index, input := range inputs {
		values, err := parseLegacyRFKeyspace(input.name, input.live)
		if err != nil {
			return nil, err
		}
		if !externalProjectionMatches(input.snapshot, values, managed.Known) {
			return nil, &legacyRFExternalDriftError{Keyspace: input.name}
		}
		parsed[index] = values
	}

	plan := &legacyRFSchemaPlan{}
	for index, input := range inputs {
		desired := mergeLegacyRFReplication(input.snapshot, managed.Desired)
		if !equalLegacyRFMaps(parsed[index], desired) {
			plan.Changes = append(plan.Changes, legacyRFSchemaChange{
				Keyspace: input.name, Replication: legacyRFMapToInt(desired),
			})
		}
	}
	return plan, nil
}

func readLegacyRFSchema(mgmtAPI legacyRFSchemaManagement) (legacyRFSchemaLiveReplication, error) {
	auth, err := mgmtAPI.GetKeyspaceReplication(api.SystemAuthKeyspace)
	if err != nil {
		return legacyRFSchemaLiveReplication{}, fmt.Errorf("read live replication for %s: %w", api.SystemAuthKeyspace, err)
	}
	traces, err := mgmtAPI.GetKeyspaceReplication(api.SystemTracesKeyspace)
	if err != nil {
		return legacyRFSchemaLiveReplication{}, fmt.Errorf("read live replication for %s: %w", api.SystemTracesKeyspace, err)
	}
	distributed, err := mgmtAPI.GetKeyspaceReplication(api.SystemDistributedKeyspace)
	if err != nil {
		return legacyRFSchemaLiveReplication{}, fmt.Errorf("read live replication for %s: %w", api.SystemDistributedKeyspace, err)
	}
	return legacyRFSchemaLiveReplication{
		SystemAuth: auth, SystemTraces: traces, SystemDistributed: distributed,
	}, nil
}

func legacyRFManagedReplication(cluster *api.K8ssandraCluster) legacyRFManagedProjection {
	desiredValues := cassandra.ComputeReplicationFromDatacenters(3, nil, cluster.GetInitializedDatacenters()...)
	desired := make(map[string]int32, len(desiredValues))
	known := make(map[string]struct{}, len(desiredValues)+len(cluster.Status.Datacenters))
	for datacenter, rf := range desiredValues {
		desired[datacenter] = int32(rf)
		known[datacenter] = struct{}{}
	}
	for _, status := range cluster.Status.Datacenters {
		if status.Cassandra != nil && status.Cassandra.DatacenterName != nil && *status.Cassandra.DatacenterName != "" {
			known[*status.Cassandra.DatacenterName] = struct{}{}
		}
	}
	if discoveryStatus := cluster.Status.LegacyRFDiscovery; discoveryStatus != nil {
		for _, location := range discoveryStatus.ManagedLocationHistory {
			if location.DatacenterName != "" {
				known[location.DatacenterName] = struct{}{}
			}
		}
	}
	return legacyRFManagedProjection{Desired: desired, Known: known}
}

func parseLegacyRFKeyspace(keyspace string, live map[string]string) (map[string]int32, error) {
	if len(live) == 0 {
		return nil, fmt.Errorf("validate live replication for %s: keyspace row is missing", keyspace)
	}
	strategy, found := live["class"]
	if !found || (strategy != api.NetworkTopologyStrategyClass && strategy != api.NetworkTopologyStrategyQualifiedClass) {
		return nil, fmt.Errorf("validate live replication for %s: unsupported replication strategy", keyspace)
	}
	result := make(map[string]int32, len(live)-1)
	datacenters := make([]string, 0, len(live)-1)
	for datacenter := range live {
		if datacenter == "class" {
			continue
		}
		datacenters = append(datacenters, datacenter)
	}
	sort.Strings(datacenters)
	for _, datacenter := range datacenters {
		value := live[datacenter]
		rf, err := parseLegacyRFLiveValue(value)
		if err != nil {
			return nil, fmt.Errorf("validate live replication for %s datacenter %q: %w", keyspace, datacenter, err)
		}
		result[datacenter] = rf
	}
	return result, nil
}

func parseLegacyRFLiveValue(value string) (int32, error) {
	if value == "" {
		return 0, fmt.Errorf("replication factor is empty")
	}
	for _, character := range []byte(value) {
		if character < '0' || character > '9' {
			return 0, fmt.Errorf("replication factor must use ASCII base-10 digits")
		}
	}
	parsed, err := strconv.ParseInt(value, 10, 32)
	if err != nil || parsed < 1 {
		return 0, fmt.Errorf("replication factor must be in 1..2147483647")
	}
	return int32(parsed), nil
}

func externalProjectionMatches(snapshot, live map[string]int32, knownManaged map[string]struct{}) bool {
	if len(snapshot) > len(live) {
		return false
	}
	for datacenter, expected := range snapshot {
		if actual, found := live[datacenter]; !found || actual != expected {
			return false
		}
	}
	for datacenter := range live {
		if _, external := snapshot[datacenter]; external {
			continue
		}
		if _, managed := knownManaged[datacenter]; !managed {
			return false
		}
	}
	return true
}

func mergeLegacyRFReplication(external, managed map[string]int32) map[string]int32 {
	merged := make(map[string]int32, len(external)+len(managed))
	for datacenter, rf := range external {
		merged[datacenter] = rf
	}
	for datacenter, rf := range managed {
		merged[datacenter] = rf
	}
	return merged
}

func equalLegacyRFMaps(left, right map[string]int32) bool {
	if len(left) != len(right) {
		return false
	}
	for key, value := range left {
		if right[key] != value {
			return false
		}
	}
	return true
}

func legacyRFMapToInt(values map[string]int32) map[string]int {
	converted := make(map[string]int, len(values))
	for key, value := range values {
		converted[key] = int(value)
	}
	return converted
}
