package v1alpha1

import (
	"encoding/json"
	"math"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/yaml"

	cassdcapi "github.com/k8ssandra/cass-operator/apis/cassandra/v1beta1"
	stargateapi "github.com/k8ssandra/k8ssandra-operator/apis/stargate/v1alpha1"
	telemetryapi "github.com/k8ssandra/k8ssandra-operator/apis/telemetry/v1alpha1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestK8ssandraCluster(t *testing.T) {
	t.Run("HasStargates", testK8ssandraClusterHasStargates)
}

func TestLegacyRFDiscoveryAPIWireRoundTrip(t *testing.T) {
	now := metav1.Now()
	cluster := legacyRFDiscoveryAPICluster(now)

	encoded, err := json.Marshal(cluster)
	require.NoError(t, err)
	jsonText := string(encoded)
	assert.Contains(t, jsonText, `"legacyCqlCredentialsSecretRef":{"name":"legacy-cql"}`)
	assert.Contains(t, jsonText, `"legacyCqlTLSSecretRef":{"name":"legacy-cql-tls"}`)
	assert.Contains(t, jsonText, `"systemAuth":null`)
	assert.Contains(t, jsonText, `"systemTraces":{}`)
	assert.Contains(t, jsonText, `"systemDistributed":{"dc10":10,"dc9":9,"dcMax":2147483647}`)
	assert.NotContains(t, strings.ToLower(jsonText), "private-key-material")
	assert.NotContains(t, strings.ToLower(jsonText), "supersecret")
	assert.NotContains(t, strings.ToLower(jsonText), "rawcause")

	var decoded K8ssandraCluster
	require.NoError(t, json.Unmarshal(encoded, &decoded))
	require.NotNil(t, decoded.Spec.Cassandra.LegacyCqlTLSSecretRef)
	assert.Equal(t, "legacy-cql-tls", decoded.Spec.Cassandra.LegacyCqlTLSSecretRef.Name)
	discovery := requireLegacyRFDiscoveryStatus(t, &decoded)
	require.Nil(t, discovery.AcceptedSnapshot.Replication.SystemAuth)
	require.NotNil(t, discovery.AcceptedSnapshot.Replication.SystemTraces)
	assert.Empty(t, discovery.AcceptedSnapshot.Replication.SystemTraces)
	assert.Equal(t, int32(math.MaxInt32), discovery.AcceptedSnapshot.Replication.SystemDistributed["dcMax"])
	assert.True(t, discovery.ManagedCreationObserved)
	assert.Len(t, discovery.ManagedLocationHistory, 2)
	assert.Equal(t, LegacyRFReasonExternalReplicationDrift, LegacyRFDiscoveryReason(decoded.Status.Conditions[0].Reason))

	discovery.AcceptedSnapshot.Replication.SystemDistributed["dc9"] = 1
	assert.Equal(t, int32(9), cluster.Status.LegacyRFDiscovery.AcceptedSnapshot.Replication.SystemDistributed["dc9"])
}

func TestLegacyRFDiscoveryAPIDeepCopy(t *testing.T) {
	cluster := legacyRFDiscoveryAPICluster(metav1.Now())
	copied := cluster.DeepCopy()

	copied.Spec.Cassandra.LegacyCqlCredentialsSecretRef.Name = "changed-credentials"
	copied.Spec.Cassandra.LegacyCqlTLSSecretRef.Name = "changed-tls"

	assert.Equal(t, "legacy-cql", cluster.Spec.Cassandra.LegacyCqlCredentialsSecretRef.Name)
	assert.Equal(t, "legacy-cql-tls", cluster.Spec.Cassandra.LegacyCqlTLSSecretRef.Name)
}

func TestDiscoveryFailureMatrix(t *testing.T) {
	tests := legacyRFFailureMatrix()
	seen := make(map[LegacyRFDiscoveryReason]struct{}, len(tests))
	for _, test := range tests {
		t.Run(string(test.reason), func(t *testing.T) {
			failure, found := LegacyRFDiscoveryFailureForReason(test.reason)
			require.True(t, found)
			assert.Equal(t, test.retryable, failure.Retryable)
			assert.Equal(t, test.reason, failure.Reason)
			assert.NotEmpty(t, failure.Message)
			assert.NotContains(t, strings.ToLower(failure.Message), "password")
			assert.NotContains(t, strings.ToLower(failure.Message), "secret data")
			_, duplicate := seen[test.reason]
			assert.False(t, duplicate, "failure reason values must be unique")
			seen[test.reason] = struct{}{}
		})
	}
	_, found := LegacyRFDiscoveryFailureForReason("UnknownReason")
	assert.False(t, found)
}

func TestDiscoveryProtocolConstants(t *testing.T) {
	assert.Equal(t, "k8ssandra.io/legacy-rf-discovery-version", LegacyRFDiscoveryMarkerAnnotation)
	assert.Equal(t, "v1", LegacyRFDiscoveryMarkerVersion)
	assert.Equal(t, "v1", LegacyRFDiscoveryProtocolVersion)
	assert.Equal(t, 1<<20, LegacyRFDiscoveryMaxResultBytes)
	assert.Equal(t, []string{"system_auth", "system_traces", "system_distributed"}, []string{
		SystemAuthKeyspace, SystemTracesKeyspace, SystemDistributedKeyspace,
	})
	assert.NotEqual(t, NetworkTopologyStrategyClass, NetworkTopologyStrategyQualifiedClass)
	assert.Equal(t, K8ssandraClusterConditionType("SystemKeyspaceReplicationReady"), SystemKeyspaceReplicationReady)
}

func legacyRFDiscoveryAPICluster(now metav1.Time) *K8ssandraCluster {
	location := LegacyRFManagedLocation{
		K8sContext: "data-plane", Namespace: "migration", Name: "dc-new", DatacenterName: "logical-dc-new",
	}
	return &K8ssandraCluster{
		Spec: K8ssandraClusterSpec{Cassandra: &CassandraClusterTemplate{
			LegacyCqlCredentialsSecretRef: &corev1.LocalObjectReference{Name: "legacy-cql"},
			LegacyCqlTLSSecretRef:         &corev1.LocalObjectReference{Name: "legacy-cql-tls"},
		}},
		Status: K8ssandraClusterStatus{
			LegacyRFDiscovery: &LegacyRFDiscoveryStatus{
				ObservedGeneration: 7, Phase: LegacyRFDiscoveryPhaseAccepted, SnapshotHash: "sha256:snapshot",
				LastTransitionTime: &now, ManagedCreationObserved: true,
				CurrentManagedLocations: []LegacyRFManagedLocation{location},
				ManagedLocationHistory:  []LegacyRFManagedLocation{location, {K8sContext: "old", Namespace: "migration", Name: "dc-old"}},
				AcceptedSnapshot:        legacyRFSnapshotForAPITest(now, location),
			},
			Conditions: []K8ssandraClusterCondition{{
				Type: SystemKeyspaceReplicationReady, Status: corev1.ConditionFalse,
				Reason: string(LegacyRFReasonExternalReplicationDrift), Message: "External replication differs from the accepted snapshot.",
			}},
		},
	}
}

func legacyRFSnapshotForAPITest(now metav1.Time, location LegacyRFManagedLocation) *LegacyRFSnapshot {
	return &LegacyRFSnapshot{
		ClusterUID: "uid", AcceptedGeneration: 7, MarkerVersion: LegacyRFDiscoveryMarkerVersion,
		ProtocolVersion: LegacyRFDiscoveryProtocolVersion, AcceptedSeeds: []string{"192.0.2.1", "192.0.2.2"},
		AcceptedSeedDigest: "sha256:seeds", AuthoritativeEndpoint: "192.0.2.2:9042",
		ExpectedClusterName: "legacy", ServerType: ServerDistributionCassandra, SourceVersion: "4.1.8",
		Partitioner: "Murmur3Partitioner", IdentityFingerprint: "identity", TopologyFingerprint: "topology",
		SchemaFingerprint: "schema", ObservedExternalDCs: []string{"dc9", "dc10", "dcMax"},
		Replication: LegacySystemKeyspaceReplication{
			SystemAuth: nil, SystemTraces: map[string]int32{},
			SystemDistributed: map[string]int32{"dc9": 9, "dc10": 10, "dcMax": math.MaxInt32},
		},
		SecretBindings: []LegacyRFSecretBinding{{
			Purpose: "credentials", SourceContext: "control-plane", Namespace: "migration",
			Name: "legacy-cql", Keys: []string{"username", "password"}, ResourceVersion: "42",
		}},
		DiscoveryLocation: location, AcceptedManagedLocations: []LegacyRFManagedLocation{location},
		WorkerImageDigest: "registry.example/operator@sha256:worker", AcceptedAt: now, Hash: "sha256:snapshot",
	}
}

func requireLegacyRFDiscoveryStatus(t *testing.T, cluster *K8ssandraCluster) *LegacyRFDiscoveryStatus {
	t.Helper()
	require.NotNil(t, cluster.Status.LegacyRFDiscovery)
	require.NotNil(t, cluster.Status.LegacyRFDiscovery.AcceptedSnapshot)
	return cluster.Status.LegacyRFDiscovery
}

type legacyRFFailureTest struct {
	reason    LegacyRFDiscoveryReason
	retryable bool
}

func legacyRFFailureMatrix() []legacyRFFailureTest {
	return []legacyRFFailureTest{
		{LegacyRFReasonAdmissionUnavailable, true}, {LegacyRFReasonMarkerInvalid, false},
		{LegacyRFReasonUnsupportedServerType, false}, {LegacyRFReasonUnsupportedSourceVersion, false},
		{LegacyRFReasonUnsupportedSecretsProvider, false}, {LegacyRFReasonDiscoveryTooLate, false},
		{LegacyRFReasonInvalidContactPoint, false}, {LegacyRFReasonJobSchedulingFailed, true},
		{LegacyRFReasonWorkerImageUnavailable, true}, {LegacyRFReasonWorkerImagePullFailed, true},
		{LegacyRFReasonDiscoveryDeadlineExceeded, true}, {LegacyRFReasonCredentialSecretInvalid, true},
		{LegacyRFReasonTLSMaterialInvalid, true}, {LegacyRFReasonAuthenticationRejected, true},
		{LegacyRFReasonAuthorizationDenied, false}, {LegacyRFReasonTLSFailed, true},
		{LegacyRFReasonContactUnreachable, true}, {LegacyRFReasonIdentityMismatch, false},
		{LegacyRFReasonSchemaDisagreement, true}, {LegacyRFReasonTopologyInconsistent, true},
		{LegacyRFReasonManagedDatacenterNameCollision, false}, {LegacyRFReasonMissingKeyspace, false},
		{LegacyRFReasonUnsupportedStrategy, false}, {LegacyRFReasonInvalidReplication, false},
		{LegacyRFReasonStaleDiscoveryResult, true}, {LegacyRFReasonInvalidDiscoveryResult, true},
		{LegacyRFReasonForgedDiscoveryResult, true}, {LegacyRFReasonDiscoveryResultTooLarge, true},
		{LegacyRFReasonKubernetesAPIUnavailable, true}, {LegacyRFReasonKubernetesAPIConflict, true},
		{LegacyRFReasonSnapshotConflict, false}, {LegacyRFReasonManagedStatePresent, false},
		{LegacyRFReasonExternalReplicationDrift, true},
	}
}

func testK8ssandraClusterHasStargates(t *testing.T) {
	t.Run("nil receiver", func(t *testing.T) {
		var kc *K8ssandraCluster = nil
		assert.False(t, kc.HasStargates())
	})
	t.Run("no stargates", func(t *testing.T) {
		kc := K8ssandraCluster{}
		assert.False(t, kc.HasStargates())
	})
	t.Run("cluster-level stargate", func(t *testing.T) {
		kc := K8ssandraCluster{
			Spec: K8ssandraClusterSpec{
				Stargate: &stargateapi.StargateClusterTemplate{
					Size: 3,
				},
			},
		}
		assert.True(t, kc.HasStargates())
	})
	t.Run("dc-level stargate", func(t *testing.T) {
		kc := K8ssandraCluster{
			Spec: K8ssandraClusterSpec{
				Cassandra: &CassandraClusterTemplate{
					Datacenters: []CassandraDatacenterTemplate{
						{
							Size:     3,
							Stargate: nil,
						},
						{
							Size: 3,
							Stargate: &stargateapi.StargateDatacenterTemplate{
								StargateClusterTemplate: stargateapi.StargateClusterTemplate{
									Size: 3,
								},
							},
						},
					},
				},
			},
		}
		assert.True(t, kc.HasStargates())
	})
}

func TestNetworkingConfig_ToCassNetworkingConfig(t *testing.T) {
	tests := []struct {
		name string
		in   *NetworkingConfig
		want *cassdcapi.NetworkingConfig
	}{
		{
			"nil",
			nil,
			nil,
		},
		{
			"empty",
			&NetworkingConfig{},
			&cassdcapi.NetworkingConfig{},
		},
		{
			"host network true",
			&NetworkingConfig{
				HostNetwork: ptr.To(true),
			},
			&cassdcapi.NetworkingConfig{
				HostNetwork: true,
			},
		},
		{
			"host network false",
			&NetworkingConfig{
				HostNetwork: ptr.To(false),
			},
			&cassdcapi.NetworkingConfig{
				HostNetwork: false,
			},
		},
		{
			"host network nil",
			&NetworkingConfig{
				HostNetwork: nil,
			},
			&cassdcapi.NetworkingConfig{
				HostNetwork: false,
			},
		},
		{
			"all set",
			&NetworkingConfig{
				HostNetwork: ptr.To(true),
				NodePort: &cassdcapi.NodePortConfig{
					Native:       1,
					NativeSSL:    2,
					Internode:    3,
					InternodeSSL: 4,
				},
			},
			&cassdcapi.NetworkingConfig{
				HostNetwork: true,
				NodePort: &cassdcapi.NodePortConfig{
					Native:       1,
					NativeSSL:    2,
					Internode:    3,
					InternodeSSL: 4,
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, tt.in.ToCassNetworkingConfig())
		})
	}
}

func TestCassandraDcAndClusterTelemetryMerging(t *testing.T) {
	cluster := &CassandraClusterTemplate{
		DatacenterOptions: DatacenterOptions{
			Telemetry: &telemetryapi.TelemetrySpec{
				Vector: &telemetryapi.VectorSpec{
					Enabled: ptr.To(true),
				},
				Mcac: &telemetryapi.McacTelemetrySpec{
					Enabled: ptr.To(true),
				},
			},
		},
	}
	dc := &CassandraDatacenterTemplate{
		DatacenterOptions: DatacenterOptions{
			Telemetry: &telemetryapi.TelemetrySpec{
				Vector: &telemetryapi.VectorSpec{
					Enabled: ptr.To(false),
				},
			},
		},
	}

	merged := dc.MergeTelemetry(cluster)

	assert.False(t, merged.IsVectorEnabled())
	assert.True(t, merged.IsMcacEnabled())
}

// TestUnmarshallDatacenterMeta checks that user-provided labels and annotations are properly set when unmarshalling
// YAML.
func TestUnmarshallDatacenterMeta(t *testing.T) {
	input := `
metadata:
  labels:
    label1: labelValue1
  annotations:
    annotation1: annotationValue1
  commonLabels:
    commonLabel1: commonLabelValue1
  pods:
    labels:
      podLabel1: podLabelValue1
    annotations:
      podAnnotation1: podAnnotationValue1
  services:
    dcService:
      labels:
        dcSvcLabel1: dcSvcValue1
      annotations:
        dcSvcAnnotation1: dcSvcAnnotationValue1
    seedService:
      labels:
        seedSvcLabel1: seedSvcLabelValue1
      annotations:
        seedSvcAnnotation1: seedSvcAnnotationValue1
    allPodsService:
      labels:
        allPodsSvcLabel1: allPodsSvcLabelValue1
      annotations:
        allPodsSvcAnnotation1: allPodsSvcAnnotationValue1
    additionalSeedService:
      labels:
        addSeedSvcLabel1: addSeedSvcLabelValue1
      annotations:
        addSeedSvcAnnotation1: addSeedSvcAnnotationValue1
    nodePortService:
      labels:
        nodePortSvcLabel1: nodePortSvcLabelValue1
      annotations:
        nodePortSvcAnnotation1: nodePortSvcAnnotationValue1
`
	dc := &CassandraDatacenterTemplate{}
	err := yaml.Unmarshal([]byte(input), dc)
	assert.NoError(t, err)

	assert.Equal(t, "labelValue1", dc.Meta.Labels["label1"])
	assert.Equal(t, "annotationValue1", dc.Meta.Annotations["annotation1"])
	assert.Equal(t, "commonLabelValue1", dc.Meta.CommonLabels["commonLabel1"])
	assert.Equal(t, "podLabelValue1", dc.Meta.Pods.Labels["podLabel1"])
	assert.Equal(t, "podAnnotationValue1", dc.Meta.Pods.Annotations["podAnnotation1"])
	assert.Equal(t, "dcSvcValue1", dc.Meta.ServiceConfig.DatacenterService.Labels["dcSvcLabel1"])
	assert.Equal(t, "dcSvcAnnotationValue1", dc.Meta.ServiceConfig.DatacenterService.Annotations["dcSvcAnnotation1"])
	assert.Equal(t, "seedSvcLabelValue1", dc.Meta.ServiceConfig.SeedService.Labels["seedSvcLabel1"])
	assert.Equal(t, "seedSvcAnnotationValue1", dc.Meta.ServiceConfig.SeedService.Annotations["seedSvcAnnotation1"])
	assert.Equal(t, "allPodsSvcLabelValue1", dc.Meta.ServiceConfig.AllPodsService.Labels["allPodsSvcLabel1"])
	assert.Equal(t, "allPodsSvcAnnotationValue1", dc.Meta.ServiceConfig.AllPodsService.Annotations["allPodsSvcAnnotation1"])
	assert.Equal(t, "addSeedSvcLabelValue1", dc.Meta.ServiceConfig.AdditionalSeedService.Labels["addSeedSvcLabel1"])
	assert.Equal(t, "addSeedSvcAnnotationValue1", dc.Meta.ServiceConfig.AdditionalSeedService.Annotations["addSeedSvcAnnotation1"])
	assert.Equal(t, "nodePortSvcLabelValue1", dc.Meta.ServiceConfig.NodePortService.Labels["nodePortSvcLabel1"])
	assert.Equal(t, "nodePortSvcAnnotationValue1", dc.Meta.ServiceConfig.NodePortService.Annotations["nodePortSvcAnnotation1"])
}

func TestGenerationChanged(t *testing.T) {
	assert := assert.New(t)
	kc := &K8ssandraCluster{
		ObjectMeta: metav1.ObjectMeta{
			Generation: 2,
		},
		Spec: K8ssandraClusterSpec{},
	}

	kc.Status = K8ssandraClusterStatus{
		ObservedGeneration: 0,
	}

	assert.True(kc.GenerationChanged())
	kc.Status.ObservedGeneration = 2
	assert.False(kc.GenerationChanged())
	kc.Generation = 3
	assert.True(kc.GenerationChanged())
}

func TestDcRemoved(t *testing.T) {
	kcOld := createClusterObjWithCassandraConfig("testcluster", "testns")
	kcNew := kcOld.DeepCopy()
	require.False(t, DcRemoved(kcOld.Spec, kcNew.Spec))
	kcOld.Spec.Cassandra.Datacenters = append(kcOld.Spec.Cassandra.Datacenters, CassandraDatacenterTemplate{
		Meta: EmbeddedObjectMeta{
			Name: "dc2",
		},
	})
	require.True(t, DcRemoved(kcOld.Spec, kcNew.Spec))
	kcOld = createClusterObjWithCassandraConfig("testcluster", "testns")
	kcNew = kcOld.DeepCopy()
	kcNew.Spec.Cassandra.Datacenters[0].Meta.Name = "newName"
	require.True(t, DcRemoved(kcOld.Spec, kcNew.Spec))
}

func TestDcAdded(t *testing.T) {
	kcOld := createClusterObjWithCassandraConfig("testcluster", "testns")
	kcNew := kcOld.DeepCopy()
	require.False(t, DcAdded(kcOld.Spec, kcNew.Spec))
	kcNew.Spec.Cassandra.Datacenters = append(kcOld.Spec.Cassandra.Datacenters, CassandraDatacenterTemplate{
		Meta: EmbeddedObjectMeta{
			Name: "dc2",
		},
	})
	require.True(t, DcAdded(kcOld.Spec, kcNew.Spec))

	kcOld = createClusterObjWithCassandraConfig("testcluster", "testns")
	kcNew = kcOld.DeepCopy()
	kcNew.Spec.Cassandra.Datacenters[0].Meta.Name = "newName"
	require.True(t, DcAdded(kcOld.Spec, kcNew.Spec))
}
