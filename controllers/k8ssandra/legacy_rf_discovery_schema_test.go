package k8ssandra

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/go-logr/logr"
	cassdcapi "github.com/k8ssandra/cass-operator/apis/cassandra/v1beta1"
	api "github.com/k8ssandra/k8ssandra-operator/apis/k8ssandra/v1alpha1"
	"github.com/k8ssandra/k8ssandra-operator/pkg/clientcache"
	"github.com/k8ssandra/k8ssandra-operator/pkg/config"
	"github.com/k8ssandra/k8ssandra-operator/pkg/mocks"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestLegacyRFSchemaPlanPreservesExternalAndMergesManaged(t *testing.T) {
	snapshot := api.LegacySystemKeyspaceReplication{
		SystemAuth:        map[string]int32{"legacy-a": 2, "legacy-b": 9, "legacy-c": 2, "legacy-d": 10, "legacy-e": 10},
		SystemTraces:      map[string]int32{"legacy-a": 1, "legacy-c": 2},
		SystemDistributed: map[string]int32{"legacy-b": 9},
	}
	live := legacyRFSchemaLiveReplication{
		SystemAuth:        rawLegacyRFReplication(snapshot.SystemAuth),
		SystemTraces:      rawLegacyRFReplication(snapshot.SystemTraces),
		SystemDistributed: rawLegacyRFReplication(snapshot.SystemDistributed),
	}
	managed := legacyRFManagedProjection{
		Desired: map[string]int32{"managed-b": 2, "managed-a": 3},
		Known:   map[string]struct{}{"managed-a": {}, "managed-b": {}, "managed-old": {}},
	}
	live.SystemAuth["managed-old"] = "3"
	live.SystemTraces["managed-a"] = "1"
	live.SystemDistributed["managed-b"] = "1"

	plan, err := buildLegacyRFSchemaPlan(snapshot, live, managed)
	require.NoError(t, err)
	require.Len(t, plan.Changes, 3)
	for _, change := range plan.Changes {
		require.Equal(t, int32(2), snapshot.SystemAuth["legacy-a"], "snapshot maps must not be mutated")
		for dc, rf := range replicationForKeyspace(snapshot, change.Keyspace) {
			require.Equal(t, int(rf), change.Replication[dc], "external values must be byte-for-byte unchanged")
		}
		require.Equal(t, 3, change.Replication["managed-a"])
		require.Equal(t, 2, change.Replication["managed-b"])
		require.NotContains(t, change.Replication, "managed-old")
	}
}

func TestLegacyRFSchemaPlanRejectsEveryPrevalidationErrorWithoutPlan(t *testing.T) {
	valid := legacyRFSchemaLiveReplication{
		SystemAuth:        map[string]string{"class": api.NetworkTopologyStrategyClass, "legacy": "9"},
		SystemTraces:      map[string]string{"class": api.NetworkTopologyStrategyQualifiedClass, "legacy": "1"},
		SystemDistributed: map[string]string{"class": api.NetworkTopologyStrategyClass, "legacy": "10"},
	}
	snapshot := api.LegacySystemKeyspaceReplication{
		SystemAuth: map[string]int32{"legacy": 9}, SystemTraces: map[string]int32{"legacy": 1}, SystemDistributed: map[string]int32{"legacy": 10},
	}
	cases := map[string]func(*legacyRFSchemaLiveReplication){
		"missing row":          func(live *legacyRFSchemaLiveReplication) { live.SystemAuth = nil },
		"unsupported strategy": func(live *legacyRFSchemaLiveReplication) { live.SystemTraces["class"] = "SimpleStrategy" },
		"zero RF":              func(live *legacyRFSchemaLiveReplication) { live.SystemDistributed["legacy"] = "0" },
		"overflow RF":          func(live *legacyRFSchemaLiveReplication) { live.SystemDistributed["legacy"] = "2147483648" },
		"non ASCII RF":         func(live *legacyRFSchemaLiveReplication) { live.SystemDistributed["legacy"] = "١" },
		"external removal":     func(live *legacyRFSchemaLiveReplication) { delete(live.SystemAuth, "legacy") },
		"external value drift": func(live *legacyRFSchemaLiveReplication) { live.SystemTraces["legacy"] = "2" },
		"external addition":    func(live *legacyRFSchemaLiveReplication) { live.SystemDistributed["other"] = "1" },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			live := cloneLegacyRFSchemaLiveReplication(valid)
			mutate(&live)
			plan, err := buildLegacyRFSchemaPlan(snapshot, live, legacyRFManagedProjection{})
			require.Error(t, err)
			require.Nil(t, plan)
		})
	}
}

func TestLegacyRFSchemaPlanNoOp(t *testing.T) {
	snapshot := api.LegacySystemKeyspaceReplication{
		SystemAuth: map[string]int32{"legacy": 9}, SystemTraces: map[string]int32{}, SystemDistributed: map[string]int32{"legacy": 10},
	}
	live := legacyRFSchemaLiveReplication{
		SystemAuth: rawLegacyRFReplication(snapshot.SystemAuth), SystemTraces: rawLegacyRFReplication(snapshot.SystemTraces),
		SystemDistributed: rawLegacyRFReplication(snapshot.SystemDistributed),
	}
	for _, values := range []map[string]string{live.SystemAuth, live.SystemTraces, live.SystemDistributed} {
		values["managed"] = "3"
	}
	plan, err := buildLegacyRFSchemaPlan(snapshot, live, legacyRFManagedProjection{
		Desired: map[string]int32{"managed": 3}, Known: map[string]struct{}{"managed": {}},
	})
	require.NoError(t, err)
	require.Empty(t, plan.Changes)
}

func TestLegacyRFSchemaPlanReportsDeterministicFirstError(t *testing.T) {
	snapshot := api.LegacySystemKeyspaceReplication{
		SystemAuth: map[string]int32{"a": 1, "z": 1}, SystemTraces: map[string]int32{}, SystemDistributed: map[string]int32{},
	}
	live := legacyRFSchemaLiveReplication{
		SystemAuth:   map[string]string{"class": api.NetworkTopologyStrategyClass, "z": "bad", "a": "also-bad"},
		SystemTraces: rawLegacyRFReplication(snapshot.SystemTraces), SystemDistributed: rawLegacyRFReplication(snapshot.SystemDistributed),
	}
	for range 20 {
		plan, err := buildLegacyRFSchemaPlan(snapshot, live, legacyRFManagedProjection{})
		require.Nil(t, plan)
		require.ErrorContains(t, err, `datacenter "a"`)
	}
}

func TestLegacyRFSchemaDriftClassification(t *testing.T) {
	snapshot := api.LegacySystemKeyspaceReplication{
		SystemAuth: map[string]int32{"legacy": 2}, SystemTraces: map[string]int32{}, SystemDistributed: map[string]int32{},
	}
	live := legacyRFSchemaLiveReplication{
		SystemAuth: rawLegacyRFReplication(snapshot.SystemAuth), SystemTraces: rawLegacyRFReplication(snapshot.SystemTraces),
		SystemDistributed: rawLegacyRFReplication(snapshot.SystemDistributed),
	}
	live.SystemAuth["legacy"] = "3"
	_, err := buildLegacyRFSchemaPlan(snapshot, live, legacyRFManagedProjection{})
	var drift *legacyRFExternalDriftError
	require.True(t, errors.As(err, &drift))
	require.Equal(t, api.SystemAuthKeyspace, drift.Keyspace)
}

func TestLegacyRFSchemaDoesNotClassifyResourceNameAsManagedDatacenter(t *testing.T) {
	for _, test := range []struct {
		name                   string
		acceptedDatacenterName string
	}{
		{name: "current snapshot records Cassandra name", acceptedDatacenterName: "managed-logical"},
		{name: "legacy snapshot without Cassandra name fails closed"},
	} {
		t.Run(test.name, func(t *testing.T) {
			cluster := markedLegacyRFSchemaCluster()
			cluster.Spec.Cassandra.Datacenters[0].Meta.Name = "managed-resource"
			cluster.Spec.Cassandra.Datacenters[0].DatacenterName = "managed-logical"
			cluster.Status.Datacenters = map[string]api.K8ssandraStatus{
				"managed-resource": {Cassandra: &cassdcapi.CassandraDatacenterStatus{}},
			}
			location := api.LegacyRFManagedLocation{
				Namespace:      cluster.Namespace,
				Name:           "managed-resource",
				DatacenterName: test.acceptedDatacenterName,
			}
			cluster.Status.LegacyRFDiscovery.AcceptedSnapshot.DiscoveryLocation = location
			cluster.Status.LegacyRFDiscovery.AcceptedSnapshot.AcceptedManagedLocations = []api.LegacyRFManagedLocation{location}
			hash, err := LegacyRFSnapshotHash(cluster.Status.LegacyRFDiscovery.AcceptedSnapshot)
			require.NoError(t, err)
			cluster.Status.LegacyRFDiscovery.AcceptedSnapshot.Hash = hash
			cluster.Status.LegacyRFDiscovery.SnapshotHash = hash

			facade := new(mocks.ManagementApiFacade)
			facade.On("GetKeyspaceReplication", api.SystemAuthKeyspace).Return(map[string]string{
				"class": api.NetworkTopologyStrategyClass, "legacy": "2", "managed-resource": "1",
			}, nil).Once()
			facade.On("GetKeyspaceReplication", api.SystemTracesKeyspace).Return(map[string]string{
				"class": api.NetworkTopologyStrategyClass, "legacy": "2",
			}, nil).Once()
			facade.On("GetKeyspaceReplication", api.SystemDistributedKeyspace).Return(map[string]string{
				"class": api.NetworkTopologyStrategyClass, "legacy": "2",
			}, nil).Once()
			reconciler := &K8ssandraClusterReconciler{
				ReconcilerConfig: &config.ReconcilerConfig{DefaultDelay: time.Second},
			}

			result := reconciler.reconcileAcceptedLegacyRFSchema(cluster, facade, logr.Discard())

			require.True(t, result.IsRequeue())
			require.Equal(t, string(api.LegacyRFReasonExternalReplicationDrift), legacyRFSchemaCondition(cluster).Reason)
			require.Equal(t, []string{
				"GetKeyspaceReplication:" + api.SystemAuthKeyspace,
				"GetKeyspaceReplication:" + api.SystemTracesKeyspace,
				"GetKeyspaceReplication:" + api.SystemDistributedKeyspace,
			}, managementCalls(facade), "an external DC named like the Kubernetes resource must issue zero DDL")
		})
	}
}

func TestLegacyRFSchemaTreatsPlannedButUncreatedDatacenterAsExternalDrift(t *testing.T) {
	cluster := markedLegacyRFSchemaCluster()
	future := api.LegacyRFManagedLocation{
		K8sContext: "plane-b", Namespace: cluster.Namespace, Name: "future-resource", DatacenterName: "future-logical",
	}
	cluster.Status.LegacyRFDiscovery.AcceptedSnapshot.AcceptedManagedLocations = append(
		cluster.Status.LegacyRFDiscovery.AcceptedSnapshot.AcceptedManagedLocations, future,
	)
	hash, err := LegacyRFSnapshotHash(cluster.Status.LegacyRFDiscovery.AcceptedSnapshot)
	require.NoError(t, err)
	cluster.Status.LegacyRFDiscovery.AcceptedSnapshot.Hash = hash
	cluster.Status.LegacyRFDiscovery.SnapshotHash = hash

	managed := legacyRFManagedReplication(cluster)
	require.NotContains(t, managed.Known, future.DatacenterName)
	live := map[string]string{
		"class": api.NetworkTopologyStrategyClass, "legacy": "2", future.DatacenterName: "1",
	}
	facade := new(mocks.ManagementApiFacade)
	setLegacyRFReads(facade, live)
	reconciler := &K8ssandraClusterReconciler{ReconcilerConfig: &config.ReconcilerConfig{DefaultDelay: time.Second}}

	reconcileResult := reconciler.reconcileAcceptedLegacyRFSchema(cluster, facade, logr.Discard())

	require.True(t, reconcileResult.IsRequeue())
	require.Equal(t, string(api.LegacyRFReasonExternalReplicationDrift), legacyRFSchemaCondition(cluster).Reason)
	require.Equal(t, []string{
		"GetKeyspaceReplication:" + api.SystemAuthKeyspace,
		"GetKeyspaceReplication:" + api.SystemTracesKeyspace,
		"GetKeyspaceReplication:" + api.SystemDistributedKeyspace,
	}, managementCalls(facade), "future planned names must not permit DDL before the DC exists")
}

func TestLegacyRFSchemaMarkedPathReadsAllBeforeDriftAndTransitionsConditionOnce(t *testing.T) {
	cluster := markedLegacyRFSchemaCluster()
	facade := new(mocks.ManagementApiFacade)
	setLegacyRFReads(facade, map[string]string{"class": api.NetworkTopologyStrategyClass, "legacy": "3"})
	recorder := &legacyRFEventRecorder{}
	reconciler := &K8ssandraClusterReconciler{ReconcilerConfig: &config.ReconcilerConfig{DefaultDelay: time.Second}, Recorder: recorder}

	result := reconciler.reconcileAcceptedLegacyRFSchema(cluster, facade, logr.Discard())
	require.True(t, result.IsRequeue())
	require.Equal(t, []string{
		"GetKeyspaceReplication:" + api.SystemAuthKeyspace,
		"GetKeyspaceReplication:" + api.SystemTracesKeyspace,
		"GetKeyspaceReplication:" + api.SystemDistributedKeyspace,
	}, managementCalls(facade))
	require.Equal(t, api.LegacyRFDiscoveryPhaseAccepted, cluster.Status.LegacyRFDiscovery.Phase)
	condition := legacyRFSchemaCondition(cluster)
	require.Equal(t, corev1.ConditionFalse, condition.Status)
	require.Equal(t, string(api.LegacyRFReasonExternalReplicationDrift), condition.Reason)
	require.Len(t, recorder.events, 1)
	transitionTime := condition.LastTransitionTime.DeepCopy()

	facade.Calls = nil
	setLegacyRFReads(facade, map[string]string{"class": api.NetworkTopologyStrategyClass, "legacy": "4"})
	result = reconciler.reconcileAcceptedLegacyRFSchema(cluster, facade, logr.Discard())
	require.True(t, result.IsRequeue())
	require.Equal(t, transitionTime, legacyRFSchemaCondition(cluster).LastTransitionTime)
	require.Len(t, recorder.events, 1, "identical public drift state must not emit another event")
	require.Equal(t, cluster.Status.LegacyRFDiscovery.AcceptedSnapshot.Hash, cluster.Status.LegacyRFDiscovery.SnapshotHash)
}

func TestValidatedLegacyRFSchemaSnapshotRejectsContentTamper(t *testing.T) {
	cluster := markedLegacyRFSchemaCluster()
	cluster.Status.LegacyRFDiscovery.AcceptedSnapshot.Replication.SystemAuth["legacy"] = 99

	_, err := validatedLegacyRFSchemaSnapshot(cluster)

	require.ErrorContains(t, err, "snapshot hash mismatch")
}

func TestLegacyRFSchemaMarkedPathUsesOnlyDirectAlterAfterAllReads(t *testing.T) {
	cluster := markedLegacyRFSchemaCluster()
	facade := new(mocks.ManagementApiFacade)
	valid := map[string]string{"class": api.NetworkTopologyStrategyQualifiedClass, "legacy": "2"}
	setLegacyRFReads(facade, valid)
	facade.On("AlterKeyspace", api.SystemAuthKeyspace, map[string]int{"legacy": 2, "managed": 3}).Return(nil).Once()
	facade.On("AlterKeyspace", api.SystemTracesKeyspace, map[string]int{"legacy": 2, "managed": 3}).Return(nil).Once()
	facade.On("AlterKeyspace", api.SystemDistributedKeyspace, map[string]int{"legacy": 2, "managed": 3}).Return(nil).Once()
	reconciler := &K8ssandraClusterReconciler{ReconcilerConfig: &config.ReconcilerConfig{DefaultDelay: time.Second}}

	result := reconciler.reconcileAcceptedLegacyRFSchema(cluster, facade, logr.Discard())
	require.True(t, result.IsRequeue())
	require.Equal(t, []string{
		"GetKeyspaceReplication:" + api.SystemAuthKeyspace,
		"GetKeyspaceReplication:" + api.SystemTracesKeyspace,
		"GetKeyspaceReplication:" + api.SystemDistributedKeyspace,
		"AlterKeyspace:" + api.SystemAuthKeyspace,
	}, managementCalls(facade))
	require.NotEqual(t, corev1.ConditionTrue, cluster.Status.GetConditionStatus(api.SystemKeyspaceReplicationReady))
	for _, call := range facade.Calls {
		require.NotEqual(t, "EnsureKeyspaceReplication", call.Method)
		require.NotEqual(t, "CreateKeyspaceIfNotExists", call.Method)
	}
}

func TestLegacyRFSchemaMarkedPathReadErrorsIssueZeroDDL(t *testing.T) {
	keyspaces := []string{api.SystemAuthKeyspace, api.SystemTracesKeyspace, api.SystemDistributedKeyspace}
	for failedIndex, failedKeyspace := range keyspaces {
		t.Run(failedKeyspace, func(t *testing.T) {
			cluster := markedLegacyRFSchemaCluster()
			facade := new(mocks.ManagementApiFacade)
			for index := 0; index < failedIndex; index++ {
				facade.On("GetKeyspaceReplication", keyspaces[index]).Return(map[string]string{
					"class": api.NetworkTopologyStrategyClass, "legacy": "2",
				}, nil).Once()
			}
			facade.On("GetKeyspaceReplication", failedKeyspace).Return(nil, errors.New("injected read failure")).Once()
			reconciler := &K8ssandraClusterReconciler{ReconcilerConfig: &config.ReconcilerConfig{DefaultDelay: time.Second}}

			result := reconciler.reconcileAcceptedLegacyRFSchema(cluster, facade, logr.Discard())
			require.True(t, result.IsError())
			for _, call := range facade.Calls {
				require.Equal(t, "GetKeyspaceReplication", call.Method)
			}
		})
	}
}

func TestLegacyRFPreReadyManagedProjectionRequiresAcceptedHealthyObservedDatacenter(t *testing.T) {
	baseCluster := markedLegacyRFSchemaCluster()
	baseDatacenter := preReadyLegacyRFDatacenter()
	tests := []struct {
		name   string
		mutate func(*api.K8ssandraCluster, *cassdcapi.CassandraDatacenter)
	}{
		{name: "unmarked", mutate: func(cluster *api.K8ssandraCluster, _ *cassdcapi.CassandraDatacenter) {
			delete(cluster.Annotations, api.LegacyRFDiscoveryMarkerAnnotation)
		}},
		{name: "pending", mutate: func(cluster *api.K8ssandraCluster, _ *cassdcapi.CassandraDatacenter) {
			cluster.Status.LegacyRFDiscovery.Phase = api.LegacyRFDiscoveryPhasePending
		}},
		{name: "unhealthy", mutate: func(_ *api.K8ssandraCluster, datacenter *cassdcapi.CassandraDatacenter) {
			datacenter.Status.Conditions[0].Status = corev1.ConditionFalse
		}},
		{name: "nodes not observed", mutate: func(_ *api.K8ssandraCluster, datacenter *cassdcapi.CassandraDatacenter) {
			datacenter.Status.NodeStatuses = nil
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cluster, datacenter := baseCluster.DeepCopy(), baseDatacenter.DeepCopy()
			test.mutate(cluster, datacenter)
			_, eligible, err := legacyRFPreReadyManagedProjection(cluster, datacenter)
			require.NoError(t, err)
			require.False(t, eligible)
		})
	}

	managed, eligible, err := legacyRFPreReadyManagedProjection(baseCluster, baseDatacenter)
	require.NoError(t, err)
	require.True(t, eligible)
	require.Equal(t, map[string]int32{"managed": 1}, managed.Desired,
		"pre-Ready RF must derive from the actual managed datacenter size")
}

func TestLegacyRFPreReadySchemaReadsAllBeforeExactManagedAlter(t *testing.T) {
	cluster := markedLegacyRFSchemaCluster()
	managed, eligible, err := legacyRFPreReadyManagedProjection(cluster, preReadyLegacyRFDatacenter())
	require.NoError(t, err)
	require.True(t, eligible)
	facade := new(mocks.ManagementApiFacade)
	valid := map[string]string{"class": api.NetworkTopologyStrategyClass, "legacy": "2"}
	setLegacyRFReads(facade, valid)
	for _, keyspace := range []string{api.SystemAuthKeyspace, api.SystemTracesKeyspace, api.SystemDistributedKeyspace} {
		facade.On("AlterKeyspace", keyspace, map[string]int{"legacy": 2, "managed": 1}).Return(nil).Once()
	}
	reconciler := &K8ssandraClusterReconciler{ReconcilerConfig: &config.ReconcilerConfig{DefaultDelay: time.Second}}

	result := reconciler.reconcileAcceptedLegacyRFSchemaWithManaged(cluster, facade, managed, logr.Discard())
	require.True(t, result.IsRequeue())
	require.Equal(t, []string{
		"GetKeyspaceReplication:" + api.SystemAuthKeyspace,
		"GetKeyspaceReplication:" + api.SystemTracesKeyspace,
		"GetKeyspaceReplication:" + api.SystemDistributedKeyspace,
		"AlterKeyspace:" + api.SystemAuthKeyspace,
	}, managementCalls(facade))
}

func TestLegacyRFPreReadyMissingKeyspaceIssuesZeroDDL(t *testing.T) {
	cluster := markedLegacyRFSchemaCluster()
	managed, _, err := legacyRFPreReadyManagedProjection(cluster, preReadyLegacyRFDatacenter())
	require.NoError(t, err)
	facade := new(mocks.ManagementApiFacade)
	facade.On("GetKeyspaceReplication", api.SystemAuthKeyspace).Return(map[string]string{
		"class": api.NetworkTopologyStrategyClass, "legacy": "2",
	}, nil).Once()
	facade.On("GetKeyspaceReplication", api.SystemTracesKeyspace).Return(map[string]string(nil), nil).Once()
	facade.On("GetKeyspaceReplication", api.SystemDistributedKeyspace).Return(map[string]string{
		"class": api.NetworkTopologyStrategyClass, "legacy": "2",
	}, nil).Once()
	reconciler := &K8ssandraClusterReconciler{ReconcilerConfig: &config.ReconcilerConfig{DefaultDelay: time.Second}}

	result := reconciler.reconcileAcceptedLegacyRFSchemaWithManaged(cluster, facade, managed, logr.Discard())
	require.True(t, result.IsError())
	require.Len(t, facade.Calls, 3)
	for _, call := range facade.Calls {
		require.Equal(t, "GetKeyspaceReplication", call.Method)
	}
}

func TestLegacyRFPreReadyConvergesOneAlterPerFreshRead(t *testing.T) {
	cluster := markedLegacyRFSchemaCluster()
	managed, eligible, err := legacyRFPreReadyManagedProjection(cluster, preReadyLegacyRFDatacenter())
	require.NoError(t, err)
	require.True(t, eligible)
	facade := new(mocks.ManagementApiFacade)
	withoutManaged := map[string]string{"class": api.NetworkTopologyStrategyClass, "legacy": "2"}
	withManaged := map[string]string{"class": api.NetworkTopologyStrategyClass, "legacy": "2", "managed": "1"}
	for round := 0; round < 4; round++ {
		auth := withoutManaged
		traces := withoutManaged
		distributed := withoutManaged
		if round >= 1 {
			auth = withManaged
		}
		if round >= 2 {
			traces = withManaged
		}
		if round >= 3 {
			distributed = withManaged
		}
		facade.On("GetKeyspaceReplication", api.SystemAuthKeyspace).Return(auth, nil).Once()
		facade.On("GetKeyspaceReplication", api.SystemTracesKeyspace).Return(traces, nil).Once()
		facade.On("GetKeyspaceReplication", api.SystemDistributedKeyspace).Return(distributed, nil).Once()
	}
	for _, keyspace := range []string{api.SystemAuthKeyspace, api.SystemTracesKeyspace, api.SystemDistributedKeyspace} {
		facade.On("AlterKeyspace", keyspace, map[string]int{"legacy": 2, "managed": 1}).Return(nil).Once()
	}
	reconciler := &K8ssandraClusterReconciler{ReconcilerConfig: &config.ReconcilerConfig{DefaultDelay: time.Second}}

	for round := 0; round < 3; round++ {
		reconcileResult := reconciler.reconcileAcceptedLegacyRFSchemaWithManaged(cluster, facade, managed, logr.Discard())
		require.True(t, reconcileResult.IsRequeue())
		require.False(t, legacyRFSchemaConditionReady(cluster))
	}
	finalResult := reconciler.reconcileAcceptedLegacyRFSchemaWithManaged(cluster, facade, managed, logr.Discard())
	require.False(t, finalResult.Completed())
	require.True(t, legacyRFSchemaConditionReady(cluster))
	require.Equal(t, 15, len(facade.Calls), "each ALTER must be preceded by fresh reads of all three keyspaces")
}

func TestLegacyRFUserCreationGatePreservesAnnotationsUntilSchemaConverges(t *testing.T) {
	cluster := markedLegacyRFSchemaCluster()
	datacenter := preReadyLegacyRFDatacenter()
	datacenter.Annotations = map[string]string{"preserved": "value"}

	applyLegacyRFUserCreationGate(cluster, datacenter)
	require.Equal(t, "value", datacenter.Annotations["preserved"])
	require.Equal(t, "true", datacenter.Annotations[cassdcapi.SkipUserCreationAnnotation])
	require.Equal(t, "true", datacenter.Annotations[legacyRFUserCreationGateOwnerAnnotation])

	now := metav1.Now()
	cluster.Status.SetCondition(api.K8ssandraClusterCondition{
		Type: api.SystemKeyspaceReplicationReady, Status: corev1.ConditionTrue, LastTransitionTime: &now,
	})
	convergedDatacenter := preReadyLegacyRFDatacenter()
	convergedDatacenter.Annotations = map[string]string{"preserved": "value"}
	applyLegacyRFUserCreationGate(cluster, convergedDatacenter)
	require.NotContains(t, convergedDatacenter.Annotations, cassdcapi.SkipUserCreationAnnotation)
	require.NotContains(t, convergedDatacenter.Annotations, legacyRFUserCreationGateOwnerAnnotation)
	require.Equal(t, "value", convergedDatacenter.Annotations["preserved"])

	authDisabled := false
	cluster.Spec.Auth = &authDisabled
	cluster.Spec.ExternalDatacenters = nil
	disabledAuthDatacenter := preReadyLegacyRFDatacenter()
	applyLegacyRFUserCreationGate(cluster, disabledAuthDatacenter)
	require.Equal(t, "true", disabledAuthDatacenter.Annotations[cassdcapi.SkipUserCreationAnnotation])
	require.Equal(t, "true", disabledAuthDatacenter.Annotations[legacyRFUserCreationGateOwnerAnnotation])
}

func TestAcceptedLegacyRFGateOwnershipSurvivesSeedRemoval(t *testing.T) {
	for _, test := range []struct {
		name     string
		auth     bool
		wantGate bool
	}{
		{name: "auth disabled remains permanently gated", auth: false, wantGate: true},
		{name: "auth enabled remains released after validation", auth: true, wantGate: false},
	} {
		t.Run(test.name, func(t *testing.T) {
			cluster := markedLegacyRFSchemaCluster()
			cluster.Spec.Auth = &test.auth
			cluster.Spec.Cassandra.AdditionalSeeds = nil
			now := metav1.Now()
			cluster.Status.SetCondition(api.K8ssandraClusterCondition{
				Type: api.SystemKeyspaceReplicationReady, Status: corev1.ConditionTrue, LastTransitionTime: &now,
			})

			decision := DecideLegacyRFDiscovery(cluster, false, nil)
			require.Equal(t, api.LegacyRFDiscoveryPhaseAccepted, decision.Phase)
			require.Empty(t, decision.AttemptLocations, "accepted state must not rediscover after seed removal")

			desired := &cassdcapi.CassandraDatacenter{ObjectMeta: metav1.ObjectMeta{Name: "managed"}}
			applyLegacyRFUserCreationGate(cluster, desired)

			require.Equal(t, test.wantGate, desired.Annotations[cassdcapi.SkipUserCreationAnnotation] == "true")
			require.Equal(t, test.wantGate, desired.Annotations[legacyRFUserCreationGateOwnerAnnotation] == "true")
		})
	}
}

func TestLegacyRFUserCreationGateDoesNotOwnExistingSkipIntent(t *testing.T) {
	cluster := markedLegacyRFSchemaCluster()
	for _, source := range []string{"user metadata", "secondary datacenter"} {
		t.Run(source, func(t *testing.T) {
			datacenter := preReadyLegacyRFDatacenter()
			datacenter.Annotations = map[string]string{cassdcapi.SkipUserCreationAnnotation: "true"}

			applyLegacyRFUserCreationGate(cluster, datacenter)

			require.Equal(t, "true", datacenter.Annotations[cassdcapi.SkipUserCreationAnnotation])
			require.NotContains(t, datacenter.Annotations, legacyRFUserCreationGateOwnerAnnotation)
		})
	}
}

func TestReleaseLegacyRFUserCreationRequiresConvergenceAndRemovesOnlyGateAnnotation(t *testing.T) {
	scheme := runtime.NewScheme()
	require.NoError(t, cassdcapi.AddToScheme(scheme))
	cluster := markedLegacyRFSchemaCluster()
	datacenter := preReadyLegacyRFDatacenter()
	datacenter.Namespace = "test"
	datacenter.ResourceVersion = "1"
	datacenter.Annotations = map[string]string{
		cassdcapi.SkipUserCreationAnnotation:    "true",
		legacyRFUserCreationGateOwnerAnnotation: "true",
		"preserved":                             "value",
	}
	remoteClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(datacenter.DeepCopy()).Build()

	released, err := releaseLegacyRFUserCreationIfReady(context.Background(), cluster, remoteClient, datacenter)
	require.NoError(t, err)
	require.False(t, released)
	now := metav1.Now()
	cluster.Status.SetCondition(api.K8ssandraClusterCondition{
		Type: api.SystemKeyspaceReplicationReady, Status: corev1.ConditionTrue, LastTransitionTime: &now,
	})
	released, err = releaseLegacyRFUserCreationIfReady(context.Background(), cluster, remoteClient, datacenter)
	require.NoError(t, err)
	require.True(t, released)
	updated := &cassdcapi.CassandraDatacenter{}
	require.NoError(t, remoteClient.Get(context.Background(), client.ObjectKeyFromObject(datacenter), updated))
	require.NotContains(t, updated.Annotations, cassdcapi.SkipUserCreationAnnotation)
	require.NotContains(t, updated.Annotations, legacyRFUserCreationGateOwnerAnnotation)
	require.Equal(t, "value", updated.Annotations["preserved"])
}

func TestReleaseLegacyRFUserCreationPreservesUnownedSkipIntent(t *testing.T) {
	scheme := runtime.NewScheme()
	require.NoError(t, cassdcapi.AddToScheme(scheme))
	cluster := markedLegacyRFSchemaCluster()
	now := metav1.Now()
	cluster.Status.SetCondition(api.K8ssandraClusterCondition{
		Type: api.SystemKeyspaceReplicationReady, Status: corev1.ConditionTrue, LastTransitionTime: &now,
	})
	datacenter := preReadyLegacyRFDatacenter()
	datacenter.Namespace = "test"
	datacenter.ResourceVersion = "1"
	datacenter.Annotations = map[string]string{cassdcapi.SkipUserCreationAnnotation: "true"}
	remoteClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(datacenter.DeepCopy()).Build()

	released, err := releaseLegacyRFUserCreationIfReady(context.Background(), cluster, remoteClient, datacenter)
	require.NoError(t, err)
	require.False(t, released)
	updated := &cassdcapi.CassandraDatacenter{}
	require.NoError(t, remoteClient.Get(context.Background(), client.ObjectKeyFromObject(datacenter), updated))
	require.Equal(t, "true", updated.Annotations[cassdcapi.SkipUserCreationAnnotation])
}

func TestReleaseLegacyRFUserCreationPreservesOwnedGateWhenAuthIsDisabled(t *testing.T) {
	scheme := runtime.NewScheme()
	require.NoError(t, cassdcapi.AddToScheme(scheme))
	cluster := markedLegacyRFSchemaCluster()
	authDisabled := false
	cluster.Spec.Auth = &authDisabled
	now := metav1.Now()
	cluster.Status.SetCondition(api.K8ssandraClusterCondition{
		Type: api.SystemKeyspaceReplicationReady, Status: corev1.ConditionTrue, LastTransitionTime: &now,
	})
	datacenter := preReadyLegacyRFDatacenter()
	datacenter.Namespace = "test"
	datacenter.ResourceVersion = "1"
	datacenter.Annotations = map[string]string{
		cassdcapi.SkipUserCreationAnnotation:    "true",
		legacyRFUserCreationGateOwnerAnnotation: "true",
	}
	remoteClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(datacenter.DeepCopy()).Build()

	released, err := releaseLegacyRFUserCreationIfReady(context.Background(), cluster, remoteClient, datacenter)

	require.NoError(t, err)
	require.False(t, released)
	updated := &cassdcapi.CassandraDatacenter{}
	require.NoError(t, remoteClient.Get(context.Background(), client.ObjectKeyFromObject(datacenter), updated))
	require.Equal(t, "true", updated.Annotations[cassdcapi.SkipUserCreationAnnotation])
	require.Equal(t, "true", updated.Annotations[legacyRFUserCreationGateOwnerAnnotation])
}

func TestReleaseLegacyRFUserCreationRequiresIntactAcceptedSnapshot(t *testing.T) {
	scheme := runtime.NewScheme()
	require.NoError(t, cassdcapi.AddToScheme(scheme))
	cluster := markedLegacyRFSchemaCluster()
	now := metav1.Now()
	cluster.Status.SetCondition(api.K8ssandraClusterCondition{
		Type: api.SystemKeyspaceReplicationReady, Status: corev1.ConditionTrue, LastTransitionTime: &now,
	})
	cluster.Status.LegacyRFDiscovery.AcceptedSnapshot.Replication.SystemAuth["legacy"]++
	datacenter := preReadyLegacyRFDatacenter()
	datacenter.Namespace = "test"
	datacenter.ResourceVersion = "1"
	datacenter.Annotations = map[string]string{
		cassdcapi.SkipUserCreationAnnotation:    "true",
		legacyRFUserCreationGateOwnerAnnotation: "true",
	}
	remoteClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(datacenter.DeepCopy()).Build()

	released, err := releaseLegacyRFUserCreationIfReady(context.Background(), cluster, remoteClient, datacenter)

	require.NoError(t, err)
	require.False(t, released)
	updated := &cassdcapi.CassandraDatacenter{}
	require.NoError(t, remoteClient.Get(context.Background(), client.ObjectKeyFromObject(datacenter), updated))
	require.Equal(t, "true", updated.Annotations[cassdcapi.SkipUserCreationAnnotation])
	require.Equal(t, "true", updated.Annotations[legacyRFUserCreationGateOwnerAnnotation])
}

func TestLegacyRFSchemaConditionTransitionsOnChangedDriftAndRecovery(t *testing.T) {
	cluster := markedLegacyRFSchemaCluster()
	recorder := &legacyRFEventRecorder{}
	reconciler := &K8ssandraClusterReconciler{ReconcilerConfig: &config.ReconcilerConfig{DefaultDelay: time.Second}, Recorder: recorder}

	authDrift := new(mocks.ManagementApiFacade)
	setLegacyRFReads(authDrift, map[string]string{"class": api.NetworkTopologyStrategyClass, "legacy": "3"})
	require.True(t, reconciler.reconcileAcceptedLegacyRFSchema(cluster, authDrift, logr.Discard()).IsRequeue())

	distributedDrift := new(mocks.ManagementApiFacade)
	distributedDrift.On("GetKeyspaceReplication", api.SystemAuthKeyspace).Return(map[string]string{
		"class": api.NetworkTopologyStrategyClass, "legacy": "2",
	}, nil).Once()
	distributedDrift.On("GetKeyspaceReplication", api.SystemTracesKeyspace).Return(map[string]string{
		"class": api.NetworkTopologyStrategyClass, "legacy": "2",
	}, nil).Once()
	distributedDrift.On("GetKeyspaceReplication", api.SystemDistributedKeyspace).Return(map[string]string{
		"class": api.NetworkTopologyStrategyClass, "legacy": "3",
	}, nil).Once()
	require.True(t, reconciler.reconcileAcceptedLegacyRFSchema(cluster, distributedDrift, logr.Discard()).IsRequeue())
	require.Contains(t, legacyRFSchemaCondition(cluster).Message, api.SystemDistributedKeyspace)

	recovered := new(mocks.ManagementApiFacade)
	valid := map[string]string{"class": api.NetworkTopologyStrategyClass, "legacy": "2", "managed": "3"}
	recovered.On("GetKeyspaceReplication", api.SystemAuthKeyspace).Return(valid, nil).Once()
	recovered.On("GetKeyspaceReplication", api.SystemTracesKeyspace).Return(valid, nil).Once()
	recovered.On("GetKeyspaceReplication", api.SystemDistributedKeyspace).Return(valid, nil).Once()
	require.False(t, reconciler.reconcileAcceptedLegacyRFSchema(cluster, recovered, logr.Discard()).Completed())
	require.Equal(t, corev1.ConditionTrue, legacyRFSchemaCondition(cluster).Status)
	require.Len(t, recorder.events, 3)
	require.Equal(t, api.LegacyRFDiscoveryPhaseAccepted, cluster.Status.LegacyRFDiscovery.Phase)
}

func TestLegacyRFSchemaPartialAlterFailureRequiresFullReread(t *testing.T) {
	cluster := markedLegacyRFSchemaCluster()
	facade := new(mocks.ManagementApiFacade)
	valid := map[string]string{"class": api.NetworkTopologyStrategyClass, "legacy": "2"}
	setLegacyRFReads(facade, valid)
	facade.On("AlterKeyspace", api.SystemAuthKeyspace, mock.Anything).Return(nil).Once()
	reconciler := &K8ssandraClusterReconciler{ReconcilerConfig: &config.ReconcilerConfig{DefaultDelay: time.Second}}

	first := reconciler.reconcileAcceptedLegacyRFSchema(cluster, facade, logr.Discard())
	require.True(t, first.IsRequeue())
	require.NotEqual(t, corev1.ConditionTrue, cluster.Status.GetConditionStatus(api.SystemKeyspaceReplicationReady))

	facade.On("GetKeyspaceReplication", api.SystemAuthKeyspace).Return(map[string]string{
		"class": api.NetworkTopologyStrategyClass, "legacy": "2", "managed": "3",
	}, nil).Once()
	facade.On("GetKeyspaceReplication", api.SystemTracesKeyspace).Return(valid, nil).Once()
	facade.On("GetKeyspaceReplication", api.SystemDistributedKeyspace).Return(valid, nil).Once()
	facade.On("AlterKeyspace", api.SystemTracesKeyspace, mock.Anything).Return(errors.New("injected alter failure")).Once()
	second := reconciler.reconcileAcceptedLegacyRFSchema(cluster, facade, logr.Discard())
	require.True(t, second.IsError())
	calls := managementCalls(facade)
	require.Equal(t, "GetKeyspaceReplication:"+api.SystemAuthKeyspace, calls[4])
	require.Equal(t, "GetKeyspaceReplication:"+api.SystemTracesKeyspace, calls[5])
	require.Equal(t, "GetKeyspaceReplication:"+api.SystemDistributedKeyspace, calls[6])
}

func TestCheckSchemasUnmarkedRetainsLegacyEnsurePath(t *testing.T) {
	cluster := markedLegacyRFSchemaCluster()
	delete(cluster.Annotations, api.LegacyRFDiscoveryMarkerAnnotation)
	cluster.Status.LegacyRFDiscovery = nil
	scheme := runtime.NewScheme()
	require.NoError(t, api.AddToScheme(scheme))
	localClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster.DeepCopy()).Build()
	facade := new(mocks.ManagementApiFacade)
	for _, keyspace := range api.SystemKeyspaces {
		facade.On("EnsureKeyspaceReplication", keyspace, map[string]int{"managed": 3}).Return(nil).Once()
	}
	reconciler := &K8ssandraClusterReconciler{
		ReconcilerConfig: &config.ReconcilerConfig{DefaultDelay: time.Second},
		ClientCache:      clientcache.New(localClient, localClient, scheme),
	}

	result := reconciler.updateReplicationOfSystemKeyspaces(context.Background(), cluster, facade, logr.Discard())
	require.NoError(t, result.GetError())
	require.False(t, result.IsRequeue())
	require.False(t, result.Completed())
	require.Len(t, facade.Calls, 3)
	for _, call := range facade.Calls {
		require.Equal(t, "EnsureKeyspaceReplication", call.Method)
	}
}

type legacyRFEventRecorder struct {
	events []string
}

func (r *legacyRFEventRecorder) Eventf(_ runtime.Object, _ runtime.Object, eventType, reason, action, note string, args ...interface{}) {
	r.events = append(r.events, fmt.Sprintf("%s:%s:%s:%s", eventType, reason, action, fmt.Sprintf(note, args...)))
}

func markedLegacyRFSchemaCluster() *api.K8ssandraCluster {
	snapshot := &api.LegacyRFSnapshot{
		ExpectedClusterName: "migration", ServerType: api.ServerDistributionCassandra,
		ObservedExternalDCs: []string{"legacy"},
		Replication: api.LegacySystemKeyspaceReplication{
			SystemAuth: map[string]int32{"legacy": 2}, SystemTraces: map[string]int32{"legacy": 2},
			SystemDistributed: map[string]int32{"legacy": 2},
		},
	}
	hash, err := LegacyRFSnapshotHash(snapshot)
	if err != nil {
		panic(err)
	}
	snapshot.Hash = hash
	initialized := metav1.Now()
	return &api.K8ssandraCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "migration", ResourceVersion: "1", Annotations: map[string]string{
			api.LegacyRFDiscoveryMarkerAnnotation: api.LegacyRFDiscoveryMarkerVersion,
		}},
		Spec: api.K8ssandraClusterSpec{Cassandra: &api.CassandraClusterTemplate{
			ServerType: api.ServerDistributionCassandra, DatacenterOptions: api.DatacenterOptions{ServerVersion: "4.0.18"},
			AdditionalSeeds: []string{"192.0.2.10"},
			Datacenters:     []api.CassandraDatacenterTemplate{{Meta: api.EmbeddedObjectMeta{Name: "managed"}, Size: 3}},
		}},
		Status: api.K8ssandraClusterStatus{
			Datacenters: map[string]api.K8ssandraStatus{"managed": {Cassandra: &cassdcapi.CassandraDatacenterStatus{
				Conditions: []cassdcapi.DatacenterCondition{{Type: cassdcapi.DatacenterInitialized, Status: corev1.ConditionTrue, LastTransitionTime: initialized}},
			}}},
			LegacyRFDiscovery: &api.LegacyRFDiscoveryStatus{Phase: api.LegacyRFDiscoveryPhaseAccepted, SnapshotHash: snapshot.Hash, AcceptedSnapshot: snapshot},
		},
	}
}

func preReadyLegacyRFDatacenter() *cassdcapi.CassandraDatacenter {
	const size int32 = 1
	now := metav1.Now()
	nodes := make(map[string]cassdcapi.CassandraNodeStatus, size)
	for index := int32(0); index < size; index++ {
		nodes[fmt.Sprintf("node-%d", index)] = cassdcapi.CassandraNodeStatus{}
	}
	return &cassdcapi.CassandraDatacenter{
		ObjectMeta: metav1.ObjectMeta{Name: "managed"},
		Spec:       cassdcapi.CassandraDatacenterSpec{Size: size},
		Status: cassdcapi.CassandraDatacenterStatus{
			Conditions:   []cassdcapi.DatacenterCondition{{Type: cassdcapi.DatacenterHealthy, Status: corev1.ConditionTrue, LastTransitionTime: now}},
			NodeStatuses: nodes,
		},
	}
}

func setLegacyRFReads(facade *mocks.ManagementApiFacade, systemAuth map[string]string) {
	facade.On("GetKeyspaceReplication", api.SystemAuthKeyspace).Return(systemAuth, nil).Once()
	facade.On("GetKeyspaceReplication", api.SystemTracesKeyspace).Return(map[string]string{
		"class": api.NetworkTopologyStrategyClass, "legacy": "2",
	}, nil).Once()
	facade.On("GetKeyspaceReplication", api.SystemDistributedKeyspace).Return(map[string]string{
		"class": api.NetworkTopologyStrategyClass, "legacy": "2",
	}, nil).Once()
}

func managementCalls(facade *mocks.ManagementApiFacade) []string {
	calls := make([]string, 0, len(facade.Calls))
	for _, call := range facade.Calls {
		calls = append(calls, call.Method+":"+call.Arguments.String(0))
	}
	return calls
}

func legacyRFSchemaCondition(cluster *api.K8ssandraCluster) api.K8ssandraClusterCondition {
	for _, condition := range cluster.Status.Conditions {
		if condition.Type == api.SystemKeyspaceReplicationReady {
			return condition
		}
	}
	return api.K8ssandraClusterCondition{}
}

func rawLegacyRFReplication(replication map[string]int32) map[string]string {
	result := map[string]string{"class": api.NetworkTopologyStrategyQualifiedClass}
	for dc, rf := range replication {
		result[dc] = strconv.Itoa(int(rf))
	}
	return result
}

func replicationForKeyspace(replication api.LegacySystemKeyspaceReplication, keyspace string) map[string]int32 {
	switch keyspace {
	case api.SystemAuthKeyspace:
		return replication.SystemAuth
	case api.SystemTracesKeyspace:
		return replication.SystemTraces
	default:
		return replication.SystemDistributed
	}
}

func cloneLegacyRFSchemaLiveReplication(in legacyRFSchemaLiveReplication) legacyRFSchemaLiveReplication {
	return legacyRFSchemaLiveReplication{
		SystemAuth: cloneStringMap(in.SystemAuth), SystemTraces: cloneStringMap(in.SystemTraces),
		SystemDistributed: cloneStringMap(in.SystemDistributed),
	}
}

func cloneStringMap(in map[string]string) map[string]string {
	out := make(map[string]string, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}
