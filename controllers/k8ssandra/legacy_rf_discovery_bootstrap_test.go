package k8ssandra

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/go-logr/logr"
	cassdcapi "github.com/k8ssandra/cass-operator/apis/cassandra/v1beta1"
	api "github.com/k8ssandra/k8ssandra-operator/apis/k8ssandra/v1alpha1"
	"github.com/k8ssandra/k8ssandra-operator/pkg/clientcache"
	"github.com/k8ssandra/k8ssandra-operator/pkg/config"
	"github.com/k8ssandra/k8ssandra-operator/pkg/discovery"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestMarkedLegacyRFFlatAnnotationDoesNotSupplyInitialReplication(t *testing.T) {
	cluster := markedLegacyRFSchemaCluster()
	cluster.Annotations[api.InitialSystemReplicationAnnotation] = `{"sentinel":99}`
	reconciler := &K8ssandraClusterReconciler{}

	replication, err := reconciler.checkInitialSystemReplication(context.Background(), cluster, logr.Discard())

	require.NoError(t, err)
	require.Nil(t, replication)
	require.Equal(t, `{"sentinel":99}`, cluster.Annotations[api.InitialSystemReplicationAnnotation])
}

func TestLegacyRFAttemptTracePersistsAndIsBoundToAcceptedSnapshot(t *testing.T) {
	cluster, attempt, result, _ := acceptanceFixture(t)
	result.Authoritative.AttemptIndex = 1
	result.Authoritative.Endpoint = attempt.OrderedSeeds[1]
	result.AttemptTrace = []discovery.EndpointAttemptSummary{
		{AttemptIndex: 0, Endpoint: attempt.OrderedSeeds[0], Outcome: discovery.EndpointAttemptFailed, Reason: api.LegacyRFReasonContactUnreachable},
		{AttemptIndex: 1, Endpoint: attempt.OrderedSeeds[1], Outcome: discovery.EndpointAttemptAccepted},
	}
	result = canonicalAcceptanceResult(t, result)

	snapshot, err := legacyRFSnapshotFromResult(cluster, attempt, result, time.Unix(1, 0))
	require.NoError(t, err)
	require.NotEqual(t, result.CanonicalHash, snapshot.Hash, "snapshot hash must be derived from snapshot content")
	recomputed, err := LegacyRFSnapshotHash(snapshot)
	require.NoError(t, err)
	require.Equal(t, recomputed, snapshot.Hash)
	require.Equal(t, toAPIAttemptTrace(result.AttemptTrace), snapshot.AttemptTrace)
	accepted, err := BuildLegacyRFAcceptedStatus(nil, snapshot, snapshot.AcceptedManagedLocations)
	require.NoError(t, err)
	snapshot.AttemptTrace[0].Endpoint = "changed"
	require.Equal(t, attempt.OrderedSeeds[0].String(), accepted.AcceptedSnapshot.AttemptTrace[0].Endpoint)
	snapshot.AttemptTrace = toAPIAttemptTrace(result.AttemptTrace)

	input := LegacyRFAcceptanceInput{Attempt: attempt, Result: result, Snapshot: snapshot}
	require.NoError(t, validateLegacyRFResultBindings(cluster, input))
	snapshot.AttemptTrace[0].Outcome = api.LegacyRFEndpointAttemptSkipped
	require.ErrorContains(t, validateLegacyRFResultBindings(cluster, input), "payload binding changed")
}

func TestLegacyRFSecretBindingComparisonIsCanonicalAndStrict(t *testing.T) {
	cluster, attempt, result, snapshot := acceptanceFixture(t)
	result.SecretBindings = append([]discovery.SecretBinding(nil), result.SecretBindings...)
	result.SecretBindings[0].Keys = []string{"password", "username"}
	snapshot.SecretBindings = append([]api.LegacyRFSecretBinding(nil), snapshot.SecretBindings...)
	snapshot.SecretBindings[0].Keys = []string{"password", "username"}
	wantAttempt := append([]discovery.SecretBinding(nil), attempt.Connection.SecretBindings...)
	wantAttempt[0].Keys = append([]string(nil), attempt.Connection.SecretBindings[0].Keys...)

	require.NoError(t, validateLegacyRFResultBindings(cluster, LegacyRFAcceptanceInput{
		Attempt: attempt, Result: result, Snapshot: snapshot,
	}))
	require.Equal(t, wantAttempt, attempt.Connection.SecretBindings)

	result.SecretBindings[0].Keys[0] = "token"
	require.ErrorContains(t, validateLegacyRFResultBindings(cluster, LegacyRFAcceptanceInput{
		Attempt: attempt, Result: result, Snapshot: snapshot,
	}), "secret or TLS binding changed")
	result.SecretBindings[0].Keys = []string{"password", "username"}
	snapshot.SecretBindings[0].ResourceVersion = "changed"
	require.ErrorContains(t, validateLegacyRFResultBindings(cluster, LegacyRFAcceptanceInput{
		Attempt: attempt, Result: result, Snapshot: snapshot,
	}), "secret or TLS binding changed")
}

func TestMarkedLegacyRFFlatAnnotationDoesNotEnterBootstrapProperties(t *testing.T) {
	cluster := markedLegacyRFSchemaCluster()
	cluster.Namespace = "control"
	cluster.Annotations[api.InitialSystemReplicationAnnotation] = `{"sentinel":99}`
	storageClass := "test"
	cluster.Spec.Cassandra.Datacenters[0].StorageConfig = &cassdcapi.StorageConfig{
		CassandraDataVolumeClaimSpec: &corev1.PersistentVolumeClaimSpec{StorageClassName: &storageClass},
	}
	scheme := runtime.NewScheme()
	require.NoError(t, api.AddToScheme(scheme))
	localClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster.DeepCopy()).Build()
	reconciler := &K8ssandraClusterReconciler{
		ReconcilerConfig: &config.ReconcilerConfig{},
		ClientCache:      clientcache.New(localClient, localClient, scheme),
	}

	configs, err := reconciler.createDatacenterConfigs(context.Background(), cluster, logr.Discard(), map[string]int{"sentinel": 99})

	require.NoError(t, err)
	require.Len(t, configs, 1)
	for _, option := range configs[0].CassandraConfig.JvmOptions.AdditionalOptions {
		require.False(t, strings.HasPrefix(option, "-Dcassandra.system_distributed_replication="), option)
	}
	_, found := cluster.Annotations[api.InitialSystemReplicationAnnotation]
	require.True(t, found, "marked reconciliation must not delete or rewrite user metadata")
}
