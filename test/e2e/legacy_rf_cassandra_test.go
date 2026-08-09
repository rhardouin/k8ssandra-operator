package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	cassdcapi "github.com/k8ssandra/cass-operator/apis/cassandra/v1beta1"
	api "github.com/k8ssandra/k8ssandra-operator/apis/k8ssandra/v1alpha1"
	"github.com/k8ssandra/k8ssandra-operator/test/framework"
	"github.com/stretchr/testify/require"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/yaml"
)

var legacyRFSourceDatacenters = []string{"legacy-a", "legacy-b"}

var legacyRFSystemReplication = api.LegacySystemKeyspaceReplication{
	SystemAuth:        map[string]int32{"legacy-a": 2, "legacy-b": 4},
	SystemTraces:      map[string]int32{"legacy-a": 1},
	SystemDistributed: map[string]int32{"legacy-b": 4},
}

type legacyRFCassandraScenario struct {
	Name       string
	Fixture    string
	Version    string
	Annotation bool
	SourceAuth bool
}

type legacyRFSource struct {
	ID               string
	Auth             bool
	ClusterName      string
	ServiceName      string
	ServiceSelector  string
	ServiceIP        string
	PodName          string
	Image            string
	CredentialSecret string
}

type legacyRFCreateClient interface {
	Create(ctx context.Context, object client.Object, opts ...client.CreateOption) error
}

func waitForLegacyRFCassandraDatacenterWebhook(
	ctx context.Context,
	creator legacyRFCreateClient,
	dc *cassdcapi.CassandraDatacenter,
	timeout, interval time.Duration,
) error {
	return wait.PollUntilContextTimeout(ctx, interval, timeout, true, func(ctx context.Context) (bool, error) {
		err := creator.Create(ctx, dc, client.DryRunAll)
		if err == nil {
			return true, nil
		}
		message := strings.ToLower(err.Error())
		transientAdmissionTransportError := strings.Contains(message, "failed calling webhook") &&
			(strings.Contains(message, "connection refused") || strings.Contains(message, "no endpoints available"))
		if transientAdmissionTransportError {
			return false, nil
		}
		return false, err
	})
}

func legacyRFCassandra40(t *testing.T, ctx context.Context, namespace string, f *framework.E2eFramework) {
	runLegacyRFCassandraLifecycle(t, ctx, namespace, f, legacyRFCassandraScenario{
		Name: "LegacyRFDiscoveryFrom4.0Cluster", Fixture: "legacy-rf-discovery-4.0", Version: "4.0.17", SourceAuth: true,
	})
}

func legacyRFCassandra41(t *testing.T, ctx context.Context, namespace string, f *framework.E2eFramework) {
	runLegacyRFCassandraLifecycle(t, ctx, namespace, f, legacyRFCassandraScenario{
		Name: "LegacyRFDiscoveryFrom4.1Cluster", Fixture: "legacy-rf-discovery-4.1", Version: "4.1.9", Annotation: true,
	})
}

func legacyRFCassandra50(t *testing.T, ctx context.Context, namespace string, f *framework.E2eFramework) {
	runLegacyRFCassandraLifecycle(t, ctx, namespace, f, legacyRFCassandraScenario{
		Name: "LegacyRFDiscoveryFrom5.0Cluster", Fixture: "legacy-rf-discovery-5.0", Version: "5.0.6",
	})
}

func runLegacyRFCassandraLifecycle(
	t *testing.T,
	ctx context.Context,
	namespace string,
	f *framework.E2eFramework,
	scenario legacyRFCassandraScenario,
) {
	started := time.Now()
	evidence := newLegacyRFEvidence(scenario.Name, scenario.Version, "pending", "pending")
	defer func() {
		evidence.Timings["total"] = time.Since(started)
		err := writeLegacyRFEvidence(filepath.Join(repositoryRoot(t), "build", "test"), evidence)
		if t.Failed() {
			t.Logf("lifecycle evidence finalized after failure: %v", err)
			return
		}
		require.NoError(t, err)
	}()
	// The 4.0 scenario runs against an authenticated source, the other two anonymously, so the
	// three scenarios together cover both credential paths of the discovery contract.
	source := provisionLegacyRFSource(t, ctx, namespace, f, scenario, "base", scenario.SourceAuth)
	evidence.SourceImage = source.Image

	recordLegacyRFSourceAlters(evidence)
	recordLegacyRFSourceRows(t, ctx, namespace, f, source, evidence)
	cluster := loadLegacyRFTarget(t, namespace, f.DataPlaneContexts[0], scenario, source)
	if scenario.Annotation {
		setLegacyRFSourceServiceEnabled(t, ctx, namespace, f, source, false)
	}
	require.NoError(t, f.Client.Create(ctx, cluster))
	if scenario.Annotation {
		preAcceptance := waitForLegacyRFPreAcceptanceJob(t, ctx, namespace, f, cluster)
		assertLegacyRFPreAcceptanceBoundary(t, ctx, namespace, f, preAcceptance)
		setLegacyRFSourceServiceEnabled(t, ctx, namespace, f, source, true)
	}
	accepted := waitForLegacyRFAccepted(t, ctx, f.Client, client.ObjectKeyFromObject(cluster))
	assertLegacyRFAcceptedSnapshot(t, accepted, source, scenario, evidence)
	targetPod := waitForLegacyRFTargetReady(t, ctx, namespace, f, cluster)
	evidence.TargetImage = containerImage(t, targetPod, "cassandra")
	assertLegacyRFBootstrapInput(t, ctx, namespace, f, cluster, scenario.Annotation)
	assertLegacyRFSourceRowsAfterBootstrap(t, ctx, namespace, f, source, cluster, evidence)
	runLegacyRFPostReadySchema(t, ctx, namespace, f, source, cluster, evidence)
	collectLegacyRFLiveEvidence(t, ctx, namespace, f, accepted, evidence)
}

func waitForLegacyRFPreAcceptanceJob(
	t *testing.T,
	ctx context.Context,
	namespace string,
	f *framework.E2eFramework,
	cluster *api.K8ssandraCluster,
) *api.K8ssandraCluster {
	key := framework.NewClusterKey(f.DataPlaneContexts[0], namespace, "")
	require.NoError(t, wait.PollUntilContextTimeout(ctx, time.Second, time.Minute, true,
		func(ctx context.Context) (bool, error) {
			jobs := &batchv1.JobList{}
			if err := f.List(ctx, key, jobs); err != nil {
				return false, err
			}
			return len(jobs.Items) > 0, nil
		}))
	observed := &api.K8ssandraCluster{}
	require.NoError(t, f.Client.Get(ctx, client.ObjectKeyFromObject(cluster), observed))
	return observed
}

func recordLegacyRFSourceAlters(evidence *legacyRFEvidence) {
	replication := legacyRFReplicationByKeyspace()
	for _, keyspace := range []string{api.SystemAuthKeyspace, api.SystemTracesKeyspace, api.SystemDistributedKeyspace} {
		evidence.Operations = append(evidence.Operations, legacyRFOperationEvidence{
			At: time.Now().UTC(), Operation: "source ALTER", Keyspace: keyspace,
			Outcome: cqlReplicationDynamic(replication[keyspace]),
		})
	}
}

func provisionLegacyRFSource(
	t *testing.T,
	ctx context.Context,
	namespace string,
	f *framework.E2eFramework,
	scenario legacyRFCassandraScenario,
	sourceID string,
	auth bool,
) legacyRFSource {
	suffix := legacyRFScenarioID(t, scenario) + "-" + sourceID
	source := legacyRFSource{ID: sourceID, Auth: auth, ClusterName: "lrfs-" + suffix}
	source.CredentialSecret = source.ClusterName + "-superuser"
	createLegacyRFSourceSecret(t, ctx, namespace, f, source)
	for index, dcName := range legacyRFSourceDatacenters {
		resourceName := fmt.Sprintf("lrf-%s-%d", suffix, index)
		if index == 0 {
			source.ServiceSelector = dcName
			source.ServiceName, source.ServiceIP = createLegacyRFSourceService(t, ctx, namespace, f, resourceName, dcName)
		}
		dc := newLegacyRFSourceDatacenter(namespace, dcName, scenario.Version, source, auth, index > 0)
		if index == 0 {
			require.NoError(t, waitForLegacyRFCassandraDatacenterWebhook(ctx, f.Client, dc, time.Minute, time.Second))
		}
		require.NoError(t, f.Create(ctx, framework.NewClusterKey(f.DataPlaneContexts[0], namespace, dcName), dc))
		if index == 0 {
			waitForLegacyRFSourceReady(t, ctx, f, namespace, dcName)
			pod := firstLegacyRFSourcePod(t, ctx, namespace, f, source.ServiceSelector)
			source.PodName, source.Image = pod.Name, containerImage(t, pod, "cassandra")
			// Cassandra 4.0 rejects NTS entries for datacenters it has not
			// discovered yet. Stage system_auth on the first datacenter so its
			// LOCAL_ONE authentication remains available while legacy-b joins;
			// the complete two-datacenter map is installed below.
			applyLegacyRFSourceKeyspaceReplication(t, ctx, namespace, f, source,
				api.SystemAuthKeyspace, legacyRFBootstrapSystemAuthReplication(), auth)
		}
		waitForLegacyRFSourceReady(t, ctx, f, namespace, dcName)
	}
	applyLegacyRFSourceReplication(t, ctx, namespace, f, source, auth)
	return source
}

func createLegacyRFSourceSecret(
	t *testing.T,
	ctx context.Context,
	namespace string,
	f *framework.E2eFramework,
	source legacyRFSource,
) {
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: source.CredentialSecret, Namespace: namespace},
		StringData: map[string]string{"username": "legacy-admin", "password": "legacy-rf-e2e-only"}}
	require.NoError(t, f.Create(ctx, framework.NewClusterKey(f.DataPlaneContexts[0], namespace, secret.Name), secret))
}

func legacyRFScenarioID(t *testing.T, scenario legacyRFCassandraScenario) string {
	t.Helper()
	ids := map[string]string{"4.0.17": "40", "4.1.9": "41", "5.0.6": "50"}
	id, found := ids[scenario.Version]
	require.True(t, found, "unsupported Cassandra scenario version %s", scenario.Version)
	return id
}

func createLegacyRFSourceService(
	t *testing.T,
	ctx context.Context,
	namespace string,
	f *framework.E2eFramework,
	serviceName, dcResourceName string,
) (string, string) {
	service := &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: serviceName + "-cql", Namespace: namespace},
		Spec: corev1.ServiceSpec{Selector: map[string]string{cassdcapi.DatacenterLabel: dcResourceName},
			Ports: []corev1.ServicePort{{Name: "cql", Port: 9042, TargetPort: intstr.FromInt(9042)}}}}
	key := framework.NewClusterKey(f.DataPlaneContexts[0], namespace, service.Name)
	require.NoError(t, f.Create(ctx, key, service))
	require.NotEmpty(t, service.Spec.ClusterIP)
	return service.Name, service.Spec.ClusterIP
}

func setLegacyRFSourceServiceEnabled(
	t *testing.T,
	ctx context.Context,
	namespace string,
	f *framework.E2eFramework,
	source legacyRFSource,
	enabled bool,
) {
	key := framework.NewClusterKey(f.DataPlaneContexts[0], namespace, source.ServiceName)
	service := &corev1.Service{}
	require.NoError(t, f.Get(ctx, key, service))
	base := service.DeepCopy()
	service.Spec.Selector = map[string]string{"legacy-rf-disabled": "true"}
	if enabled {
		service.Spec.Selector = map[string]string{cassdcapi.DatacenterLabel: source.ServiceSelector}
	}
	require.NoError(t, f.Patch(ctx, service, client.MergeFrom(base), key))
	require.NoError(t, wait.PollUntilContextTimeout(ctx, time.Second, time.Minute, true,
		func(ctx context.Context) (bool, error) {
			slices := &discoveryv1.EndpointSliceList{}
			if err := f.List(ctx, key, slices, client.MatchingLabels{
				discoveryv1.LabelServiceName: source.ServiceName,
			}); err != nil {
				return false, err
			}
			routed := false
			for _, slice := range slices.Items {
				for _, endpoint := range slice.Endpoints {
					routed = routed || endpoint.Conditions.Ready == nil || *endpoint.Conditions.Ready
				}
			}
			return routed == enabled, nil
		}))
}

func newLegacyRFSourceDatacenter(
	namespace, dcName, version string,
	source legacyRFSource,
	auth, joinsExisting bool,
) *cassdcapi.CassandraDatacenter {
	cassandraConfig := map[string]any{"cluster_name": source.ClusterName, "endpoint_snitch": "GossipingPropertyFileSnitch",
		"auto_snapshot": false, "memtable_flush_writers": 1, "concurrent_compactors": 1,
		"concurrent_reads": 2, "concurrent_writes": 2, "concurrent_counter_writes": 2}
	if auth {
		cassandraConfig["authenticator"] = "PasswordAuthenticator"
		cassandraConfig["authorizer"] = "CassandraAuthorizer"
		cassandraConfig["role_manager"] = "CassandraRoleManager"
	} else {
		cassandraConfig["authenticator"] = "AllowAllAuthenticator"
		cassandraConfig["authorizer"] = "AllowAllAuthorizer"
		cassandraConfig["role_manager"] = "CassandraRoleManager"
	}
	config, _ := json.Marshal(map[string]any{"cassandra-yaml": cassandraConfig,
		"jvm-server-options": map[string]any{"initial_heap_size": 512 << 20, "max_heap_size": 512 << 20}})
	dc := &cassdcapi.CassandraDatacenter{ObjectMeta: metav1.ObjectMeta{Name: dcName, Namespace: namespace},
		Spec: cassdcapi.CassandraDatacenterSpec{Size: 1, ServerVersion: version, ServerType: "cassandra",
			ClusterName: source.ClusterName, DatacenterName: dcName, Config: config,
			AllowMultipleNodesPerWorker: true,
			Resources: corev1.ResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("100m"), corev1.ResourceMemory: resource.MustParse("1Gi")},
				Limits:   corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("1"), corev1.ResourceMemory: resource.MustParse("1Gi")},
			},
			Networking: &cassdcapi.NetworkingConfig{HostNetwork: false},
			StorageConfig: cassdcapi.StorageConfig{CassandraDataVolumeClaimSpec: &corev1.PersistentVolumeClaimSpec{
				AccessModes:      []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
				Resources:        corev1.VolumeResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("256Mi")}},
				StorageClassName: ptr.To(*storageClassFlag),
			}}}}
	if !auth || joinsExisting {
		dc.Annotations = map[string]string{cassdcapi.SkipUserCreationAnnotation: "true"}
	}
	if auth {
		dc.Spec.SuperuserSecretName = source.CredentialSecret
	}
	if joinsExisting {
		dc.Spec.AdditionalSeeds = []string{source.ServiceIP}
	}
	return dc
}

func waitForLegacyRFSourceReady(
	t *testing.T,
	ctx context.Context,
	f *framework.E2eFramework,
	namespace, name string,
) {
	key := framework.NewClusterKey(f.DataPlaneContexts[0], namespace, name)
	require.NoError(t, wait.PollUntilContextTimeout(ctx, 10*time.Second, 15*time.Minute, true,
		func(ctx context.Context) (bool, error) {
			dc := &cassdcapi.CassandraDatacenter{}
			if err := f.Get(ctx, key, dc); err != nil {
				return false, client.IgnoreNotFound(err)
			}
			return dc.GetConditionStatus(cassdcapi.DatacenterReady) == corev1.ConditionTrue &&
				dc.Status.CassandraOperatorProgress == cassdcapi.ProgressReady, nil
		}))
}

func firstLegacyRFSourcePod(
	t *testing.T,
	ctx context.Context,
	namespace string,
	f *framework.E2eFramework,
	dcResourceName string,
) *corev1.Pod {
	pods := &corev1.PodList{}
	key := framework.NewClusterKey(f.DataPlaneContexts[0], namespace, "")
	require.NoError(t, f.List(ctx, key, pods, client.MatchingLabels{cassdcapi.DatacenterLabel: dcResourceName}))
	require.NotEmpty(t, pods.Items)
	sort.Slice(pods.Items, func(left, right int) bool { return pods.Items[left].Name < pods.Items[right].Name })
	return &pods.Items[0]
}

func applyLegacyRFSourceReplication(
	t *testing.T,
	ctx context.Context,
	namespace string,
	f *framework.E2eFramework,
	source legacyRFSource,
	auth bool,
) {
	for keyspace, replication := range legacyRFReplicationByKeyspace() {
		applyLegacyRFSourceKeyspaceReplication(t, ctx, namespace, f, source, keyspace, replication, auth)
	}
}

func applyLegacyRFSourceKeyspaceReplication(
	t *testing.T,
	ctx context.Context,
	namespace string,
	f *framework.E2eFramework,
	source legacyRFSource,
	keyspace string,
	replication map[string]int32,
	auth bool,
) {
	statement := fmt.Sprintf("ALTER KEYSPACE %s WITH replication = %s", keyspace, cqlReplication(replication))
	var output string
	var err error
	if auth {
		output, err = f.ExecuteCql(ctx, f.DataPlaneContexts[0], namespace, source.ClusterName, source.PodName, statement)
	} else {
		output, err = f.ExecuteCqlNoAuth(f.DataPlaneContexts[0], namespace, source.PodName, statement)
	}
	require.NoError(t, err, "alter source replication for %s: %s", keyspace, output)
}

func legacyRFBootstrapSystemAuthReplication() map[string]int32 {
	firstDatacenter := legacyRFSourceDatacenters[0]
	return map[string]int32{firstDatacenter: legacyRFSystemReplication.SystemAuth[firstDatacenter]}
}

func legacyRFReplicationByKeyspace() map[string]map[string]int32 {
	return map[string]map[string]int32{
		api.SystemAuthKeyspace:        legacyRFSystemReplication.SystemAuth,
		api.SystemTracesKeyspace:      legacyRFSystemReplication.SystemTraces,
		api.SystemDistributedKeyspace: legacyRFSystemReplication.SystemDistributed,
	}
}

func cqlReplication(replication map[string]int32) string {
	parts := []string{"'class': 'NetworkTopologyStrategy'"}
	for _, dcName := range legacyRFSourceDatacenters {
		if factor, found := replication[dcName]; found {
			parts = append(parts, fmt.Sprintf("'%s': '%d'", dcName, factor))
		}
	}
	return "{" + strings.Join(parts, ", ") + "}"
}

func loadLegacyRFTarget(
	t *testing.T,
	namespace, dataPlane string,
	scenario legacyRFCassandraScenario,
	source legacyRFSource,
) *api.K8ssandraCluster {
	body, err := os.ReadFile(filepath.Join(repositoryRoot(t), "test", "testdata", "fixtures", scenario.Fixture, "k8ssandra.yaml"))
	require.NoError(t, err)
	cluster := &api.K8ssandraCluster{}
	require.NoError(t, yaml.Unmarshal(body, cluster))
	suffix := legacyRFScenarioID(t, scenario)
	cluster.Name, cluster.Namespace = "legacy-rf-target-"+suffix, namespace
	cluster.ResourceVersion, cluster.UID = "", ""
	cluster.Spec.Cassandra.ClusterName = source.ClusterName
	cluster.Spec.Cassandra.AdditionalSeeds = []string{"192.0.2.10", source.ServiceIP, "192.0.2.12"}
	cluster.Spec.Cassandra.Datacenters[0].Meta.Name = "target-" + suffix
	cluster.Spec.Cassandra.Datacenters[0].K8sContext = dataPlane
	disableLegacyRFHostNetwork(cluster)
	cluster.Spec.Auth = ptr.To(source.Auth)
	if source.Auth {
		cluster.Spec.Cassandra.LegacyCqlCredentialsSecretRef = &corev1.LocalObjectReference{Name: source.CredentialSecret}
	}
	if scenario.Annotation {
		cluster.Annotations = map[string]string{api.InitialSystemReplicationAnnotation: `{"sentinel":99}`}
	}
	return cluster
}

func disableLegacyRFHostNetwork(cluster *api.K8ssandraCluster) {
	if cluster.Spec.Cassandra == nil {
		cluster.Spec.Cassandra = &api.CassandraClusterTemplate{}
	}
	if cluster.Spec.Cassandra.Networking == nil {
		cluster.Spec.Cassandra.Networking = &api.NetworkingConfig{}
	}
	cluster.Spec.Cassandra.Networking.HostNetwork = ptr.To(false)
}

func waitForLegacyRFAccepted(
	t *testing.T,
	ctx context.Context,
	reader client.Reader,
	key client.ObjectKey,
) *api.K8ssandraCluster {
	accepted := &api.K8ssandraCluster{}
	require.NoError(t, wait.PollUntilContextTimeout(ctx, 5*time.Second, 10*time.Minute, true,
		func(ctx context.Context) (bool, error) {
			if err := reader.Get(ctx, key, accepted); err != nil {
				return false, client.IgnoreNotFound(err)
			}
			status := accepted.Status.LegacyRFDiscovery
			return status != nil && status.Phase == api.LegacyRFDiscoveryPhaseAccepted && status.AcceptedSnapshot != nil, nil
		}))
	return accepted.DeepCopy()
}

func assertLegacyRFPreAcceptanceBoundary(
	t *testing.T,
	ctx context.Context,
	namespace string,
	f *framework.E2eFramework,
	cluster *api.K8ssandraCluster,
) {
	require.Nil(t, cluster.Status.LegacyRFDiscovery.AcceptedSnapshot)
	dcName := cluster.Spec.Cassandra.Datacenters[0].Meta.Name
	dcKey := framework.NewClusterKey(f.DataPlaneContexts[0], namespace, dcName)
	require.True(t, errors.IsNotFound(f.Get(ctx, dcKey, &cassdcapi.CassandraDatacenter{})),
		"flat annotation permitted managed creation before live discovery")
}

func assertLegacyRFBootstrapInput(
	t *testing.T,
	ctx context.Context,
	namespace string,
	f *framework.E2eFramework,
	cluster *api.K8ssandraCluster,
	annotationPresent bool,
) {
	dcName := cluster.Spec.Cassandra.Datacenters[0].Meta.Name
	dc := &cassdcapi.CassandraDatacenter{}
	require.NoError(t, f.Get(ctx, framework.NewClusterKey(f.DataPlaneContexts[0], namespace, dcName), dc))
	config := string(dc.Spec.Config)
	require.NotContains(t, config, "sentinel")
	require.NotContains(t, config, "cassandra.system_distributed_replication")
	if annotationPresent {
		current := &api.K8ssandraCluster{}
		require.NoError(t, f.Client.Get(ctx, client.ObjectKeyFromObject(cluster), current))
		require.Equal(t, `{"sentinel":99}`, current.Annotations[api.InitialSystemReplicationAnnotation])
	}
}

func assertLegacyRFAcceptedSnapshot(
	t *testing.T,
	cluster *api.K8ssandraCluster,
	source legacyRFSource,
	scenario legacyRFCassandraScenario,
	evidence *legacyRFEvidence,
) {
	snapshot := cluster.Status.LegacyRFDiscovery.AcceptedSnapshot
	require.Equal(t, scenario.Version, snapshot.SourceVersion)
	require.Equal(t, legacyRFSystemReplication, snapshot.Replication)
	require.Equal(t, source.ServiceIP+":9042", snapshot.AuthoritativeEndpoint)
	require.Equal(t, []string{"192.0.2.10", source.ServiceIP, "192.0.2.12"}, snapshot.AcceptedSeeds)
	require.Len(t, snapshot.AttemptTrace, 3)
	require.Equal(t, api.LegacyRFEndpointAttemptFailed, snapshot.AttemptTrace[0].Outcome)
	require.Equal(t, api.LegacyRFEndpointAttemptAccepted, snapshot.AttemptTrace[1].Outcome)
	require.Equal(t, api.LegacyRFEndpointAttemptSkipped, snapshot.AttemptTrace[2].Outcome)
	require.Equal(t, api.LegacyRFReasonContactUnreachable, snapshot.AttemptTrace[0].Reason)
	if scenario.Annotation {
		require.Equal(t, `{"sentinel":99}`, cluster.Annotations[api.InitialSystemReplicationAnnotation])
	}
	evidence.AcceptedEndpoint = snapshot.AuthoritativeEndpoint
	evidence.SkippedEndpoints = []string{snapshot.AttemptTrace[2].Endpoint}
	for _, attempt := range snapshot.AttemptTrace {
		if attempt.Outcome == api.LegacyRFEndpointAttemptSkipped {
			continue
		}
		evidence.AttemptedEndpoints = append(evidence.AttemptedEndpoints, legacyRFEndpointEvidence{
			Endpoint: attempt.Endpoint, Outcome: strings.ToLower(string(attempt.Outcome)), Reason: string(attempt.Reason),
		})
	}
	evidence.Snapshot, _ = json.Marshal(snapshot)
	evidence.SnapshotHash = snapshot.Hash
}

func runLegacyRFPostReadySchema(
	t *testing.T,
	ctx context.Context,
	namespace string,
	f *framework.E2eFramework,
	source legacyRFSource,
	cluster *api.K8ssandraCluster,
	evidence *legacyRFEvidence,
) {
	managedDC := cluster.Spec.Cassandra.Datacenters[0].Meta.Name
	drifts := []struct {
		name     string
		keyspace string
		mutate   func(map[string]int32)
	}{
		{name: "changed", keyspace: api.SystemAuthKeyspace, mutate: func(values map[string]int32) { values["legacy-a"] = 3 }},
		{name: "missing", keyspace: api.SystemTracesKeyspace, mutate: func(values map[string]int32) { delete(values, "legacy-a") }},
		{name: "unexpected", keyspace: api.SystemDistributedKeyspace, mutate: func(values map[string]int32) { values["legacy-a"] = 1 }},
	}
	for _, drift := range drifts {
		drifted := mergeLegacyRFTestReplication(legacyRFReplicationByKeyspace()[drift.keyspace], managedDC, 1)
		drift.mutate(drifted)
		alterLegacyRFKeyspace(t, ctx, namespace, f, source, drift.keyspace, drifted, source.Auth)
		schemaAfterMutation := queryLegacyRFSchemaVersion(t, ctx, namespace, f, source, source.Auth)
		triggerLegacyRFReconcile(t, ctx, f.Client, client.ObjectKeyFromObject(cluster))
		waitForLegacyRFSchemaCondition(t, ctx, f.Client, client.ObjectKeyFromObject(cluster), corev1.ConditionFalse,
			string(api.LegacyRFReasonExternalReplicationDrift))
		require.Equal(t, schemaAfterMutation, queryLegacyRFSchemaVersion(t, ctx, namespace, f, source, source.Auth),
			"operator issued schema DDL during %s external drift", drift.name)
		evidence.Operations = append(evidence.Operations, legacyRFOperationEvidence{At: time.Now().UTC(),
			Operation: "external drift", Keyspace: drift.keyspace, Outcome: drift.name + ":zero-DDL"})
		restored := mergeLegacyRFTestReplication(legacyRFReplicationByKeyspace()[drift.keyspace], managedDC, 1)
		alterLegacyRFKeyspace(t, ctx, namespace, f, source, drift.keyspace, restored, source.Auth)
		triggerLegacyRFReconcile(t, ctx, f.Client, client.ObjectKeyFromObject(cluster))
		waitForLegacyRFSchemaCondition(t, ctx, f.Client, client.ObjectKeyFromObject(cluster), corev1.ConditionTrue, "")
	}

	managedDrift := mergeLegacyRFTestReplication(legacyRFSystemReplication.SystemAuth, managedDC, 2)
	alterLegacyRFKeyspace(t, ctx, namespace, f, source, api.SystemAuthKeyspace, managedDrift, source.Auth)
	schemaAfterManagedDrift := queryLegacyRFSchemaVersion(t, ctx, namespace, f, source, source.Auth)
	triggerLegacyRFReconcile(t, ctx, f.Client, client.ObjectKeyFromObject(cluster))
	require.NoError(t, wait.PollUntilContextTimeout(ctx, 5*time.Second, 5*time.Minute, true,
		func(context.Context) (bool, error) {
			output := queryLegacyRFReplication(t, ctx, namespace, f, source, api.SystemAuthKeyspace, source.Auth)
			return strings.Contains(output, fmt.Sprintf("'%s': '1'", managedDC)), nil
		}))
	require.NotEqual(t, schemaAfterManagedDrift, queryLegacyRFSchemaVersion(t, ctx, namespace, f, source, source.Auth),
		"managed-only reconciliation did not execute ALTER")
	evidence.Operations = append(evidence.Operations, legacyRFOperationEvidence{At: time.Now().UTC(),
		Operation: "managed-only ALTER", Keyspace: api.SystemAuthKeyspace, Outcome: "external entries preserved"})
}

func mergeLegacyRFTestReplication(external map[string]int32, managedDC string, managedRF int32) map[string]int32 {
	merged := make(map[string]int32, len(external)+1)
	for dcName, factor := range external {
		merged[dcName] = factor
	}
	merged[managedDC] = managedRF
	return merged
}

func alterLegacyRFKeyspace(
	t *testing.T,
	ctx context.Context,
	namespace string,
	f *framework.E2eFramework,
	source legacyRFSource,
	keyspace string,
	replication map[string]int32,
	auth bool,
) {
	statement := fmt.Sprintf("ALTER KEYSPACE %s WITH replication = %s", keyspace, cqlReplicationDynamic(replication))
	var output string
	var err error
	if auth {
		output, err = f.ExecuteCql(ctx, f.DataPlaneContexts[0], namespace, source.ClusterName, source.PodName, statement)
	} else {
		output, err = f.ExecuteCqlNoAuth(f.DataPlaneContexts[0], namespace, source.PodName, statement)
	}
	require.NoError(t, err, "alter live replication for %s: %s", keyspace, output)
}

func cqlReplicationDynamic(replication map[string]int32) string {
	names := make([]string, 0, len(replication))
	for dcName := range replication {
		names = append(names, dcName)
	}
	sort.Strings(names)
	parts := []string{"'class': 'NetworkTopologyStrategy'"}
	for _, dcName := range names {
		parts = append(parts, fmt.Sprintf("'%s': '%d'", dcName, replication[dcName]))
	}
	return "{" + strings.Join(parts, ", ") + "}"
}

func queryLegacyRFSchemaVersion(
	t *testing.T,
	ctx context.Context,
	namespace string,
	f *framework.E2eFramework,
	source legacyRFSource,
	auth bool,
) string {
	query := "SELECT schema_version FROM system.local"
	var output string
	var err error
	if auth {
		output, err = f.ExecuteCql(ctx, f.DataPlaneContexts[0], namespace, source.ClusterName, source.PodName, query)
	} else {
		output, err = f.ExecuteCqlNoAuth(f.DataPlaneContexts[0], namespace, source.PodName, query)
	}
	require.NoError(t, err)
	return output
}

func triggerLegacyRFReconcile(
	t *testing.T,
	ctx context.Context,
	writer client.Client,
	key client.ObjectKey,
) {
	cluster := &api.K8ssandraCluster{}
	require.NoError(t, writer.Get(ctx, key, cluster))
	base := cluster.DeepCopy()
	if cluster.Annotations == nil {
		cluster.Annotations = make(map[string]string)
	}
	cluster.Annotations[api.AutomatedUpdateAnnotation] = string(api.AllowUpdateOnce)
	require.NoError(t, writer.Patch(ctx, cluster, client.MergeFrom(base)))
}

func waitForLegacyRFSchemaCondition(
	t *testing.T,
	ctx context.Context,
	reader client.Reader,
	key client.ObjectKey,
	status corev1.ConditionStatus,
	reason string,
) {
	require.NoError(t, wait.PollUntilContextTimeout(ctx, 3*time.Second, 5*time.Minute, true,
		func(ctx context.Context) (bool, error) {
			cluster := &api.K8ssandraCluster{}
			if err := reader.Get(ctx, key, cluster); err != nil {
				return false, client.IgnoreNotFound(err)
			}
			if cluster.Status.LegacyRFDiscovery == nil ||
				cluster.Status.LegacyRFDiscovery.Phase != api.LegacyRFDiscoveryPhaseAccepted {
				return false, nil
			}
			for _, condition := range cluster.Status.Conditions {
				if condition.Type == api.SystemKeyspaceReplicationReady && condition.Status == status &&
					(reason == "" || condition.Reason == reason) {
					return true, nil
				}
			}
			return false, nil
		}))
}

func recordLegacyRFSourceRows(
	t *testing.T,
	ctx context.Context,
	namespace string,
	f *framework.E2eFramework,
	source legacyRFSource,
	evidence *legacyRFEvidence,
) {
	for keyspace, expected := range legacyRFReplicationByKeyspace() {
		output := queryLegacyRFReplication(t, ctx, namespace, f, source, keyspace, source.Auth)
		assertLegacyRFReplicationOutput(t, output, expected, "")
		encoded, err := json.Marshal(map[string]any{"before": output, "expected": expected})
		require.NoError(t, err)
		evidence.CQLRows[keyspace] = encoded
		evidence.Operations = append(evidence.Operations, legacyRFOperationEvidence{
			At: time.Now().UTC(), Operation: "source SELECT", Keyspace: keyspace, Outcome: strings.TrimSpace(output),
		})
	}
}

func queryLegacyRFReplication(
	t *testing.T,
	ctx context.Context,
	namespace string,
	f *framework.E2eFramework,
	source legacyRFSource,
	keyspace string,
	auth bool,
) string {
	query := fmt.Sprintf("SELECT replication FROM system_schema.keyspaces WHERE keyspace_name = '%s'", keyspace)
	var output string
	var err error
	if auth {
		output, err = f.ExecuteCql(ctx, f.DataPlaneContexts[0], namespace, source.ClusterName, source.PodName, query)
	} else {
		output, err = f.ExecuteCqlNoAuth(f.DataPlaneContexts[0], namespace, source.PodName, query)
	}
	require.NoError(t, err, "query live replication for %s", keyspace)
	return output
}

func assertLegacyRFReplicationOutput(
	t *testing.T,
	output string,
	expected map[string]int32,
	managedDatacenter string,
) {
	for _, dcName := range legacyRFSourceDatacenters {
		factor, found := expected[dcName]
		entryPrefix := fmt.Sprintf("'%s': '", dcName)
		if found {
			require.Contains(t, output, fmt.Sprintf("'%s': '%d'", dcName, factor))
		} else {
			require.NotContains(t, output, entryPrefix, "sparse RF entry %s must remain omitted", dcName)
		}
	}
	if managedDatacenter != "" {
		require.Contains(t, output, fmt.Sprintf("'%s': '1'", managedDatacenter))
	}
}

func waitForLegacyRFTargetReady(
	t *testing.T,
	ctx context.Context,
	namespace string,
	f *framework.E2eFramework,
	cluster *api.K8ssandraCluster,
) *corev1.Pod {
	dcName := cluster.Spec.Cassandra.Datacenters[0].Meta.Name
	key := framework.NewClusterKey(f.DataPlaneContexts[0], namespace, dcName)
	require.NoError(t, wait.PollUntilContextTimeout(ctx, 10*time.Second, 20*time.Minute, true,
		func(ctx context.Context) (bool, error) {
			dc := &cassdcapi.CassandraDatacenter{}
			if err := f.Get(ctx, key, dc); err != nil {
				return false, client.IgnoreNotFound(err)
			}
			return dc.GetConditionStatus(cassdcapi.DatacenterReady) == corev1.ConditionTrue &&
				dc.Status.CassandraOperatorProgress == cassdcapi.ProgressReady, nil
		}))
	pods := &corev1.PodList{}
	require.NoError(t, f.List(ctx, key, pods, client.MatchingLabels{cassdcapi.DatacenterLabel: dcName}))
	require.Len(t, pods.Items, 1)
	return &pods.Items[0]
}

func assertLegacyRFSourceRowsAfterBootstrap(
	t *testing.T,
	ctx context.Context,
	namespace string,
	f *framework.E2eFramework,
	source legacyRFSource,
	cluster *api.K8ssandraCluster,
	evidence *legacyRFEvidence,
) {
	managedDatacenter := cluster.Spec.Cassandra.Datacenters[0].Meta.Name
	for keyspace, expected := range legacyRFReplicationByKeyspace() {
		output := queryLegacyRFReplication(t, ctx, namespace, f, source, keyspace, source.Auth)
		assertLegacyRFReplicationOutput(t, output, expected, managedDatacenter)
		var rows map[string]any
		require.NoError(t, json.Unmarshal(evidence.CQLRows[keyspace], &rows))
		rows["after"] = output
		evidence.CQLRows[keyspace] = marshalLegacyRFEvidenceObject(t, rows)
	}
}

func collectLegacyRFLiveEvidence(
	t *testing.T,
	ctx context.Context,
	namespace string,
	f *framework.E2eFramework,
	cluster *api.K8ssandraCluster,
	evidence *legacyRFEvidence,
) {
	currentCluster := &api.K8ssandraCluster{}
	require.NoError(t, f.Client.Get(ctx, client.ObjectKeyFromObject(cluster), currentCluster))

	jobs := &batchv1.JobList{}
	require.NoError(t, f.List(ctx, framework.NewClusterKey(f.DataPlaneContexts[0], namespace, ""), jobs))
	for index := range jobs.Items {
		job := jobs.Items[index]
		evidence.Jobs = append(evidence.Jobs, marshalLegacyRFEvidenceObject(t, map[string]any{
			"name": job.Name, "active": job.Status.Active, "succeeded": job.Status.Succeeded,
			"failed": job.Status.Failed, "startTime": job.Status.StartTime, "completionTime": job.Status.CompletionTime,
		}))
	}
	pods := &corev1.PodList{}
	require.NoError(t, f.List(ctx, framework.NewClusterKey(f.DataPlaneContexts[0], namespace, ""), pods))
	for index := range pods.Items {
		pod := pods.Items[index]
		images := make(map[string]string, len(pod.Spec.Containers))
		for _, container := range pod.Spec.Containers {
			images[container.Name] = container.Image
		}
		evidence.Pods = append(evidence.Pods, marshalLegacyRFEvidenceObject(t, map[string]any{
			"name": pod.Name, "phase": pod.Status.Phase, "podIP": pod.Status.PodIP, "images": images,
		}))
	}
	events := &corev1.EventList{}
	require.NoError(t, f.List(ctx, framework.NewClusterKey(f.ControlPlaneContext, namespace, ""), events))
	for index := range events.Items {
		event := events.Items[index]
		evidence.Events = append(evidence.Events, marshalLegacyRFEvidenceObject(t, map[string]any{
			"reason": event.Reason, "type": event.Type, "count": event.Count,
			"involvedKind": event.InvolvedObject.Kind, "involvedName": event.InvolvedObject.Name,
		}))
	}
	for _, condition := range currentCluster.Status.Conditions {
		evidence.Conditions = append(evidence.Conditions, marshalLegacyRFEvidenceObject(t, condition))
	}
	evidence.Operations = append(evidence.Operations, legacyRFOperationEvidence{
		At: time.Now().UTC(), Operation: "live CQL snapshot", Outcome: "three keyspaces verified",
	})
}

func marshalLegacyRFEvidenceObject(t *testing.T, value any) json.RawMessage {
	t.Helper()
	body, err := json.Marshal(value)
	require.NoError(t, err)
	return body
}

func containerImage(t *testing.T, pod *corev1.Pod, name string) string {
	t.Helper()
	for _, container := range pod.Spec.Containers {
		if container.Name == name {
			require.NotEmpty(t, container.Image)
			return container.Image
		}
	}
	t.Fatalf("container %s not found in pod %s", name, pod.Name)
	return ""
}

func TestLegacyRFEvidenceValidationAndRedaction(t *testing.T) {
	evidence := completeLegacyRFEvidence(t)
	evidence.Snapshot = json.RawMessage(`{
		"hash":"snapshot-hash",
		"secretData":{"password":"legacy-rf-secret-canary"},
		"replication":{"system_auth":{"legacy-a":2,"legacy-b":4}}
	}`)
	evidence.Operations = []legacyRFOperationEvidence{
		{At: time.Unix(2, 0).UTC(), Operation: "ALTER", Keyspace: "system_auth", Outcome: "succeeded"},
		{At: time.Unix(1, 0).UTC(), Operation: "SELECT", Keyspace: "system_auth", Outcome: "succeeded"},
	}

	root := t.TempDir()
	require.NoError(t, writeLegacyRFEvidence(root, evidence))
	path := filepath.Join(root, evidence.Scenario, "legacy-rf-evidence.json")
	body, err := os.ReadFile(path)
	require.NoError(t, err)
	require.NotContains(t, string(body), "legacy-rf-secret-canary")
	require.Contains(t, string(body), "[REDACTED]")
	require.Less(t, strings.Index(string(body), `"operation": "SELECT"`), strings.Index(string(body), `"operation": "ALTER"`))
	info, err := os.Stat(path)
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o600), info.Mode().Perm())
}

func TestLegacyRFCassandraSuiteAndCIRegistrations(t *testing.T) {
	repository := repositoryRoot(t)
	suite := readContractFile(t, filepath.Join(repository, "test", "e2e", "suite_test.go"))
	workflow := readContractFile(t, filepath.Join(repository, ".github", "workflows", "kind_e2e_tests.yaml"))
	for _, scenario := range []string{
		"LegacyRFDiscoveryFrom4.0Cluster", "LegacyRFDiscoveryFrom4.1Cluster", "LegacyRFDiscoveryFrom5.0Cluster",
	} {
		require.Equal(t, 1, strings.Count(suite, `t.Run("`+scenario+`"`), "suite registration %s", scenario)
		require.Equal(t, 1, strings.Count(workflow, "- "+scenario), "CI registration %s", scenario)
	}
}

func TestLegacyRFSourceTopologyUsesTwoSingleNodeDatacentersAndSparseReplication(t *testing.T) {
	require.Equal(t, []string{"legacy-a", "legacy-b"}, legacyRFSourceDatacenters)
	bootstrapSystemAuth := legacyRFBootstrapSystemAuthReplication()
	require.Equal(t, map[string]int32{"legacy-a": 2}, bootstrapSystemAuth)
	require.Equal(t, "{'class': 'NetworkTopologyStrategy', 'legacy-a': '2'}", cqlReplication(bootstrapSystemAuth))
	require.Equal(t, map[string]int32{"legacy-a": 2, "legacy-b": 4}, legacyRFSystemReplication.SystemAuth)
	require.Equal(t, map[string]int32{"legacy-a": 1}, legacyRFSystemReplication.SystemTraces)
	require.Equal(t, map[string]int32{"legacy-b": 4}, legacyRFSystemReplication.SystemDistributed)
	require.Equal(t, map[string]int32{"legacy-a": 2, "legacy-b": 4, "managed": 1},
		mergeLegacyRFTestReplication(legacyRFSystemReplication.SystemAuth, "managed", 1))
	require.Equal(t, map[string]int32{"legacy-a": 1, "managed": 1},
		mergeLegacyRFTestReplication(legacyRFSystemReplication.SystemTraces, "managed", 1))
	require.Equal(t, map[string]int32{"legacy-b": 4, "managed": 1},
		mergeLegacyRFTestReplication(legacyRFSystemReplication.SystemDistributed, "managed", 1))

	for index, dcName := range legacyRFSourceDatacenters {
		dc := newLegacyRFSourceDatacenter("test", dcName, "4.1.9", legacyRFSource{
			ClusterName: "legacy", CredentialSecret: "legacy-superuser", ServiceIP: "192.0.2.10",
		}, true, index > 0)
		require.Equal(t, int32(1), dc.Spec.Size, "source DC %s must remain a single-node metadata fixture", dcName)
	}
}

func TestDisableLegacyRFHostNetworkInitializesNilClusterNetworking(t *testing.T) {
	cluster := &api.K8ssandraCluster{}
	disableLegacyRFHostNetwork(cluster)
	require.NotNil(t, cluster.Spec.Cassandra.Networking)
	require.NotNil(t, cluster.Spec.Cassandra.Networking.HostNetwork)
	require.False(t, *cluster.Spec.Cassandra.Networking.HostNetwork)
}

func TestLegacyRFEvidenceRejectsMissingRequiredFields(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*legacyRFEvidence)
	}{
		{name: "unknown scenario", mutate: func(e *legacyRFEvidence) { e.Scenario = "other" }},
		{name: "missing version", mutate: func(e *legacyRFEvidence) { e.SourceVersion = "" }},
		{name: "missing attempts", mutate: func(e *legacyRFEvidence) { e.AttemptedEndpoints = nil }},
		{name: "accepted endpoint not attempted", mutate: func(e *legacyRFEvidence) { e.AcceptedEndpoint = "192.0.2.99:9042" }},
		{name: "skipped endpoint was attempted", mutate: func(e *legacyRFEvidence) { e.SkippedEndpoints = []string{"192.0.2.10:9042"} }},
		{name: "missing CQL row", mutate: func(e *legacyRFEvidence) { delete(e.CQLRows, "system_auth") }},
		{name: "missing boundaries", mutate: func(e *legacyRFEvidence) { e.DoesNotProve = nil }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			evidence := completeLegacyRFEvidence(t)
			test.mutate(evidence)
			root := t.TempDir()
			require.Error(t, writeLegacyRFEvidence(root, evidence))
			if legacyRFEvidenceScenarioPattern.MatchString(evidence.Scenario) {
				require.FileExists(t, filepath.Join(root, evidence.Scenario, "legacy-rf-evidence.json"))
			}
		})
	}
}

func TestLegacyRFEvidenceIsBoundedOnFailure(t *testing.T) {
	evidence := completeLegacyRFEvidence(t)
	evidence.Events = []json.RawMessage{json.RawMessage(`{"message":"` + strings.Repeat("x", legacyRFEvidenceMaximumBytes) + `"}`)}
	root := t.TempDir()
	err := writeLegacyRFEvidence(root, evidence)
	require.ErrorContains(t, err, "exceeds")
	path := filepath.Join(root, evidence.Scenario, "legacy-rf-evidence.json")
	require.FileExists(t, path)
	info, statErr := os.Stat(path)
	require.NoError(t, statErr)
	require.Less(t, info.Size(), int64(legacyRFEvidenceMaximumBytes))
}

func completeLegacyRFEvidence(t *testing.T) *legacyRFEvidence {
	t.Helper()
	evidence := newLegacyRFEvidence(
		"LegacyRFDiscoveryFrom4.1Cluster", "4.1.9", "k8ssandra/cass-management-api:4.1.9",
		"k8ssandra/cass-management-api:4.1.9",
	)
	evidence.AttemptedEndpoints = []legacyRFEndpointEvidence{
		{Endpoint: "192.0.2.10:9042", Outcome: "failed", Reason: "ContactUnreachable"},
		{Endpoint: "192.0.2.11:9042", Outcome: "accepted"},
	}
	evidence.AcceptedEndpoint = "192.0.2.11:9042"
	evidence.SkippedEndpoints = []string{"192.0.2.12:9042"}
	evidence.CQLRows = map[string]json.RawMessage{
		"system_auth":        json.RawMessage(`{"legacy-a":"2","legacy-b":"4"}`),
		"system_traces":      json.RawMessage(`{"legacy-a":"1"}`),
		"system_distributed": json.RawMessage(`{"legacy-b":"4"}`),
	}
	evidence.Snapshot = json.RawMessage(`{"hash":"snapshot-hash"}`)
	evidence.SnapshotHash = "snapshot-hash"
	evidence.Jobs = []json.RawMessage{json.RawMessage(`{"name":"discovery-attempt"}`)}
	evidence.Pods = []json.RawMessage{json.RawMessage(`{"name":"source-0"}`)}
	evidence.Events = []json.RawMessage{json.RawMessage(`{"reason":"Accepted"}`)}
	evidence.Conditions = []json.RawMessage{json.RawMessage(`{"type":"LegacyRFDiscoveryReady","status":"True"}`)}
	evidence.Operations = []legacyRFOperationEvidence{{
		At: time.Unix(1, 0).UTC(), Operation: "SELECT", Keyspace: "system_auth", Outcome: "succeeded",
	}}
	evidence.Timings["total"] = time.Second
	return evidence
}
