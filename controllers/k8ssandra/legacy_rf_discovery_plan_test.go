package k8ssandra

import (
	"context"
	"errors"
	"testing"
	"time"

	cassdcapi "github.com/k8ssandra/cass-operator/apis/cassandra/v1beta1"
	api "github.com/k8ssandra/k8ssandra-operator/apis/k8ssandra/v1alpha1"
	"github.com/k8ssandra/k8ssandra-operator/pkg/clientcache"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestValidateLegacyRFCurrentPlan(t *testing.T) {
	snapshot := planSnapshot()
	base := LegacyRFCurrentPlan{
		ExpectedClusterName: "legacy", ServerType: api.ServerDistributionCassandra,
		ManagedDatacenterNames: []string{"managed-a", "managed-b"},
		ManagedLocations: []api.LegacyRFManagedLocation{
			{K8sContext: "plane-a", Namespace: "ns", Name: "managed-a"},
			{K8sContext: "plane-b", Namespace: "ns", Name: "managed-b"},
		},
	}
	tests := []struct {
		name    string
		mutate  func(*LegacyRFCurrentPlan)
		wantErr string
	}{
		{name: "unchanged plan"},
		{name: "managed reorder is legal", mutate: func(plan *LegacyRFCurrentPlan) {
			plan.ManagedDatacenterNames[0], plan.ManagedDatacenterNames[1] = plan.ManagedDatacenterNames[1], plan.ManagedDatacenterNames[0]
			plan.ManagedLocations[0], plan.ManagedLocations[1] = plan.ManagedLocations[1], plan.ManagedLocations[0]
		}},
		{name: "first discovery location removal is legal", mutate: func(plan *LegacyRFCurrentPlan) {
			plan.ManagedDatacenterNames = plan.ManagedDatacenterNames[1:]
			plan.ManagedLocations = plan.ManagedLocations[1:]
		}},
		{name: "non-colliding addition is legal", mutate: func(plan *LegacyRFCurrentPlan) {
			plan.ManagedDatacenterNames = append(plan.ManagedDatacenterNames, "managed-c")
		}},
		{name: "generation and seeds are intentionally irrelevant"},
		{name: "exact external set accepts reorder and duplicates", mutate: func(plan *LegacyRFCurrentPlan) {
			plan.ExternalDatacentersSet = true
			plan.ExternalDatacenters = []string{"legacy-b", "legacy-a", "legacy-a"}
		}},
		{name: "cluster identity change blocks", mutate: func(plan *LegacyRFCurrentPlan) { plan.ExpectedClusterName = "other" }, wantErr: "cluster name changed"},
		{name: "server change blocks", mutate: func(plan *LegacyRFCurrentPlan) { plan.ServerType = api.ServerDistributionDse }, wantErr: "server type changed"},
		{name: "external collision blocks", mutate: func(plan *LegacyRFCurrentPlan) {
			plan.ManagedDatacenterNames = append(plan.ManagedDatacenterNames, "legacy-a")
		}, wantErr: "collides"},
		{name: "external subset blocks", mutate: func(plan *LegacyRFCurrentPlan) {
			plan.ExternalDatacentersSet = true
			plan.ExternalDatacenters = []string{"legacy-a"}
		}, wantErr: "exactly match"},
		{name: "external superset blocks", mutate: func(plan *LegacyRFCurrentPlan) {
			plan.ExternalDatacentersSet = true
			plan.ExternalDatacenters = []string{"legacy-a", "legacy-b", "legacy-c"}
		}, wantErr: "exactly match"},
		{name: "external case change blocks", mutate: func(plan *LegacyRFCurrentPlan) {
			plan.ExternalDatacentersSet = true
			plan.ExternalDatacenters = []string{"legacy-a", "Legacy-b"}
		}, wantErr: "exactly match"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			plan := base
			plan.ManagedDatacenterNames = append([]string(nil), base.ManagedDatacenterNames...)
			plan.ManagedLocations = append([]api.LegacyRFManagedLocation(nil), base.ManagedLocations...)
			if test.mutate != nil {
				test.mutate(&plan)
			}
			err := ValidateCurrentPlan(snapshot, plan)
			if test.wantErr != "" {
				require.ErrorContains(t, err, test.wantErr)
				return
			}
			require.NoError(t, err)
		})
	}
	require.Error(t, ValidateCurrentPlan(nil, base))
}

func TestValidateLegacyRFCurrentPlanBuildsNormalizedValues(t *testing.T) {
	cluster := &api.K8ssandraCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "object-name", Namespace: "cluster-ns"},
		Spec: api.K8ssandraClusterSpec{Cassandra: &api.CassandraClusterTemplate{
			ClusterName: "legacy",
			Datacenters: []api.CassandraDatacenterTemplate{
				{Meta: api.EmbeddedObjectMeta{Name: "dc-a"}, K8sContext: "plane-a", DatacenterOptions: api.DatacenterOptions{DatacenterName: "managed-a"}},
				{Meta: api.EmbeddedObjectMeta{Name: "dc-b", Namespace: "other-ns"}, K8sContext: "plane-b"},
			},
		}},
	}
	plan, err := BuildLegacyRFCurrentPlan(cluster)
	require.NoError(t, err)
	require.Equal(t, "legacy", plan.ExpectedClusterName)
	require.Equal(t, api.ServerDistributionCassandra, plan.ServerType, "empty server type defaults to Cassandra")
	require.Equal(t, []string{"managed-a", "dc-b"}, plan.ManagedDatacenterNames)
	require.Equal(t, []api.LegacyRFManagedLocation{
		{K8sContext: "plane-a", Namespace: "cluster-ns", Name: "dc-a", DatacenterName: "managed-a"},
		{K8sContext: "plane-b", Namespace: "other-ns", Name: "dc-b", DatacenterName: "dc-b"},
	}, plan.ManagedLocations)
	require.Error(t, func() error { _, err := BuildLegacyRFCurrentPlan(nil); return err }())
}

func TestValidateLegacyRFCurrentPlanAuthorizationRetainsAcceptedSafetyState(t *testing.T) {
	snapshot := planSnapshot()
	status := &api.LegacyRFDiscoveryStatus{
		CurrentManagedLocations: []api.LegacyRFManagedLocation{{K8sContext: "old-current", Namespace: "ns", Name: "old"}},
		ManagedLocationHistory:  []api.LegacyRFManagedLocation{{K8sContext: "history", Namespace: "ns", Name: "removed"}},
	}
	current := LegacyRFCurrentPlan{
		ExpectedClusterName: "legacy", ServerType: api.ServerDistributionCassandra,
		ManagedDatacenterNames: []string{"managed-new"},
		ManagedLocations:       []api.LegacyRFManagedLocation{{K8sContext: "new", Namespace: "ns", Name: "managed-new"}},
	}
	authorization, err := BuildLegacyRFCreateAuthorization(snapshot, status, current)
	require.NoError(t, err)
	require.Equal(t, []string{"192.0.2.1", "192.0.2.2"}, authorization.AcceptedSeeds, "accepted seeds survive spec edits")
	require.Equal(t, "seed-hash", authorization.AcceptedSeedDigest)
	require.Equal(t, []api.LegacyRFManagedLocation{
		{K8sContext: "new", Namespace: "ns", Name: "managed-new"},
		{K8sContext: "old-current", Namespace: "ns", Name: "old"},
		{K8sContext: "accepted", Namespace: "ns", Name: "first"},
		{K8sContext: "history", Namespace: "ns", Name: "removed"},
	}, authorization.ManagedSearchDomain)
	authorization.AcceptedSeeds[0] = "changed"
	require.Equal(t, "192.0.2.1", snapshot.AcceptedSeeds[0], "returned seeds must not alias the snapshot")
	require.Error(t, func() error { _, err := BuildLegacyRFCreateAuthorization(snapshot, nil, current); return err }())
}

func TestLegacyRFSnapshotSearchDomainAndStatusCopiesAreHistoricalSafetyOnly(t *testing.T) {
	current := []api.LegacyRFManagedLocation{{K8sContext: "current", Namespace: "ns", Name: "dc"}}
	snapshot := planSnapshot()
	status := &api.LegacyRFDiscoveryStatus{
		CurrentManagedLocations: []api.LegacyRFManagedLocation{{K8sContext: "status", Namespace: "ns", Name: "dc"}},
		ManagedLocationHistory:  []api.LegacyRFManagedLocation{{K8sContext: "history", Namespace: "ns", Name: "dc"}},
	}
	domain := LegacyRFManagedSearchDomain(current, snapshot, status)
	require.Equal(t, []api.LegacyRFManagedLocation{
		current[0], status.CurrentManagedLocations[0], snapshot.AcceptedManagedLocations[0], status.ManagedLocationHistory[0],
	}, domain)

	accepted, err := BuildLegacyRFAcceptedStatus(status, snapshot, current)
	require.NoError(t, err)
	require.Equal(t, api.LegacyRFDiscoveryPhaseAccepted, accepted.Phase)
	require.Equal(t, snapshot.Hash, accepted.SnapshotHash)
	accepted.AcceptedSnapshot.Replication.SystemAuth["legacy-a"] = 99
	require.EqualValues(t, 2, snapshot.Replication.SystemAuth["legacy-a"], "RF maps must remain immutable")

	created, err := BuildLegacyRFManagedCreationStatus(&accepted, current, api.LegacyRFManagedLocation{K8sContext: "created", Namespace: "ns", Name: "dc"})
	require.NoError(t, err)
	require.True(t, created.ManagedCreationObserved)
	require.Contains(t, created.ManagedLocationHistory, api.LegacyRFManagedLocation{K8sContext: "created", Namespace: "ns", Name: "dc"})
	require.Error(t, validateLegacyRFStatusMonotonic(&created, &accepted), "tombstone cannot clear")
	pruned := created
	pruned.ManagedLocationHistory = nil
	require.Error(t, validateLegacyRFStatusMonotonic(&created, &pruned), "history cannot prune")
	replaced := created
	replaced.SnapshotHash = "different"
	require.Error(t, validateLegacyRFStatusMonotonic(&created, &replaced), "snapshot cannot replace")
}

func TestLegacyRFSnapshotAcceptanceReadBackRequiresStoredHashAndUID(t *testing.T) {
	scheme := runtime.NewScheme()
	require.NoError(t, api.AddToScheme(scheme))
	snapshot := planSnapshot()
	cluster := &api.K8ssandraCluster{
		ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "cluster", UID: types.UID("uid"), ResourceVersion: "1"},
		Status: api.K8ssandraClusterStatus{LegacyRFDiscovery: &api.LegacyRFDiscoveryStatus{
			Phase: api.LegacyRFDiscoveryPhaseAccepted, SnapshotHash: snapshot.Hash, AcceptedSnapshot: snapshot,
		}},
	}
	client := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(cluster).WithObjects(cluster).Build()
	key := types.NamespacedName{Namespace: cluster.Namespace, Name: cluster.Name}
	actual, err := ReadBackLegacyRFAcceptance(context.Background(), client, key, cluster.UID, snapshot.Hash)
	require.NoError(t, err)
	require.Equal(t, cluster.UID, actual.UID)
	_, err = ReadBackLegacyRFAcceptance(context.Background(), client, key, types.UID("other"), snapshot.Hash)
	require.ErrorContains(t, err, "UID changed")
	_, err = ReadBackLegacyRFAcceptance(context.Background(), client, key, cluster.UID, "wrong")
	require.ErrorContains(t, err, "hash mismatch")
}

func TestDirectLegacyRFSnapshotManagedStateSurveyUsesAuthoritativeCurrentAndHistory(t *testing.T) {
	scheme := runtime.NewScheme()
	require.NoError(t, api.AddToScheme(scheme))
	require.NoError(t, cassdcapi.AddToScheme(scheme))
	require.NoError(t, corev1.AddToScheme(scheme))
	staleCached := fake.NewClientBuilder().WithScheme(scheme).Build()
	direct := fake.NewClientBuilder().WithScheme(scheme).WithObjects(&cassdcapi.CassandraDatacenter{
		ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "historical-dc"},
	}).Build()
	clients, err := clientcache.NewValidated(staleCached, direct, scheme)
	require.NoError(t, err)
	state, err := NewManagedDatacenterState(clients)
	require.NoError(t, err)

	history := []api.LegacyRFManagedLocation{{Namespace: "ns", Name: "historical-dc"}}
	err = state.AssertAbsent(context.Background(), history, true)
	var present *ManagedStatePresentError
	require.ErrorAs(t, err, &present)
	require.Equal(t, "CassandraDatacenter", present.Kind)
	require.Error(t, staleCached.Get(context.Background(), types.NamespacedName{Namespace: "ns", Name: "historical-dc"}, &cassdcapi.CassandraDatacenter{}), "a stale cached miss cannot authorize rediscovery")

	unknownHistory := []api.LegacyRFManagedLocation{{K8sContext: "unavailable", Namespace: "ns", Name: "removed-dc"}}
	err = state.AssertAbsent(context.Background(), unknownHistory, true)
	require.ErrorContains(t, err, "no known direct client")
	require.Nil(t, func() ManagedDatacenterState { value, _ := NewManagedDatacenterState(nil); return value }())
}

func TestDirectLegacyRFSnapshotManagedStateSurveyBlocksPodOnlyPresenceAndReadErrors(t *testing.T) {
	scheme := runtime.NewScheme()
	require.NoError(t, api.AddToScheme(scheme))
	require.NoError(t, cassdcapi.AddToScheme(scheme))
	require.NoError(t, corev1.AddToScheme(scheme))
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Namespace: "ns", Name: "dc-a-default-sts-0",
		Labels: map[string]string{cassdcapi.DatacenterLabel: cassdcapi.CleanLabelValue("dc-a")},
	}}
	direct := fake.NewClientBuilder().WithScheme(scheme).WithObjects(pod).Build()
	clients, err := clientcache.NewValidated(direct, direct, scheme)
	require.NoError(t, err)
	state, err := NewManagedDatacenterState(clients)
	require.NoError(t, err)

	location := api.LegacyRFManagedLocation{Namespace: "ns", Name: "dc-a"}
	err = state.AssertAbsent(context.Background(), []api.LegacyRFManagedLocation{location, location}, true)
	var present *ManagedStatePresentError
	require.True(t, errors.As(err, &present))
	require.Equal(t, "Cassandra pod", present.Kind)
	require.NoError(t, state.AssertAbsent(context.Background(), []api.LegacyRFManagedLocation{{Namespace: "ns", Name: "missing"}}, true))
	require.Error(t, state.AssertAbsent(context.Background(), []api.LegacyRFManagedLocation{{Name: "missing-namespace"}}, false))
}

func TestLegacyRFSnapshotAcceptanceCASPrecedesAuthoritativeReadBack(t *testing.T) {
	scheme := runtime.NewScheme()
	require.NoError(t, api.AddToScheme(scheme))
	base := &api.K8ssandraCluster{ObjectMeta: metav1.ObjectMeta{
		Namespace: "ns", Name: "cluster", UID: types.UID("uid"), ResourceVersion: "1",
	}}
	client := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(base).WithObjects(base).Build()
	updated := base.DeepCopy()
	snapshot := planSnapshot()
	updated.Status.LegacyRFDiscovery = &api.LegacyRFDiscoveryStatus{
		Phase: api.LegacyRFDiscoveryPhaseAccepted, SnapshotHash: snapshot.Hash, AcceptedSnapshot: snapshot,
	}
	require.NoError(t, PatchLegacyRFDiscoveryStatusCAS(context.Background(), client, base, updated))
	readBack, err := ReadBackLegacyRFAcceptance(
		context.Background(), client, types.NamespacedName{Namespace: "ns", Name: "cluster"}, base.UID, snapshot.Hash,
	)
	require.NoError(t, err)
	require.Equal(t, snapshot.Hash, readBack.Status.LegacyRFDiscovery.SnapshotHash)

	wrongIdentity := updated.DeepCopy()
	wrongIdentity.ResourceVersion = "different"
	require.Error(t, PatchLegacyRFDiscoveryStatusCAS(context.Background(), client, base, wrongIdentity))
}

func planSnapshot() *api.LegacyRFSnapshot {
	snapshot := &api.LegacyRFSnapshot{
		ExpectedClusterName: "legacy", ServerType: api.ServerDistributionCassandra,
		AcceptedSeeds: []string{"192.0.2.1", "192.0.2.2"}, AcceptedSeedDigest: "seed-hash",
		ObservedExternalDCs:      []string{"legacy-a", "legacy-b"},
		AcceptedManagedLocations: []api.LegacyRFManagedLocation{{K8sContext: "accepted", Namespace: "ns", Name: "first"}},
		Replication: api.LegacySystemKeyspaceReplication{
			SystemAuth:        map[string]int32{"legacy-a": 2, "legacy-b": 9},
			SystemTraces:      map[string]int32{"legacy-a": 2},
			SystemDistributed: map[string]int32{"legacy-b": 10},
		},
		AcceptedGeneration: 7, AcceptedAt: metav1.NewTime(time.Unix(123, 0)),
	}
	hash, err := LegacyRFSnapshotHash(snapshot)
	if err != nil {
		panic(err)
	}
	snapshot.Hash = hash
	return snapshot
}
