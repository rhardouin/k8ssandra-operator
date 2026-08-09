package e2e

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"math/big"
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
	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/yaml"
)

var legacyRFSourceDatacenters = []string{"legacy-a", "legacy-b"}

// legacyRFMatrixDatacenter is the single datacenter of the raw credential/TLS matrix sources.
const legacyRFMatrixDatacenter = "matrix-dc"

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
	TLSSecret        string
	WrongTLSSecret   string
	RawStatefulSet   string
}

type legacyRFMatrixRow struct {
	Name string
	// ShortID names the Kubernetes objects for this row. Name stays descriptive for evidence.
	ShortID     string
	SourceAuth  bool
	Credentials bool
	TLS         bool
	TLSMismatch bool
	TargetAuth  bool
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
	// Primary scenarios isolate RF preservation from credential transport. The
	// 4.1 raw matrix below owns authenticated and TLS boundary coverage.
	source := provisionLegacyRFSource(t, ctx, namespace, f, scenario, "base", scenario.SourceAuth, false)
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
	if scenario.Name == "LegacyRFDiscoveryFrom4.1Cluster" {
		runLegacyRFCredentialTLSMatrix(t, ctx, namespace, f, scenario, cluster, evidence)
	}
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
	auth, tlsEnabled bool,
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
			if tlsEnabled {
				source.TLSSecret, source.WrongTLSSecret = createLegacyRFSourceTLS(t, ctx, namespace, f, suffix, source.ServiceIP)
			}
		}
		dc := newLegacyRFSourceDatacenter(namespace, dcName, scenario.Version, source, auth, tlsEnabled, index > 0)
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

func allLegacyRFContainersReady(pod *corev1.Pod) bool {
	if len(pod.Status.ContainerStatuses) == 0 {
		return false
	}
	for _, status := range pod.Status.ContainerStatuses {
		if !status.Ready {
			return false
		}
	}
	return true
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

func provisionLegacyRFMatrixSource(
	t *testing.T,
	ctx context.Context,
	namespace string,
	f *framework.E2eFramework,
	scenario legacyRFCassandraScenario,
	sourceID string,
	auth, tlsEnabled bool,
) legacyRFSource {
	suffix := legacyRFScenarioID(t, scenario) + "-" + sourceID
	name := framework.CleanupForKubernetes("lrfm-" + suffix)
	source := legacyRFSource{ID: sourceID, Auth: auth, ClusterName: name, RawStatefulSet: name,
		CredentialSecret: name + "-superuser", ServiceSelector: name}
	createLegacyRFMatrixCredential(t, ctx, namespace, f, source)
	source.ServiceName, source.ServiceIP = createLegacyRFMatrixService(t, ctx, namespace, f, name)
	if tlsEnabled {
		source.TLSSecret, source.WrongTLSSecret = createLegacyRFSourceTLS(t, ctx, namespace, f, suffix, source.ServiceIP)
	}
	statefulSet := newLegacyRFMatrixStatefulSet(namespace, scenario.Version, source, tlsEnabled)
	require.NoError(t, f.Create(ctx, framework.NewClusterKey(f.DataPlaneContexts[0], namespace, name), statefulSet))
	source.PodName = name + "-0"
	source.Image = statefulSet.Spec.Template.Spec.Containers[0].Image
	waitForLegacyRFMatrixSource(t, ctx, namespace, f, source)
	applyLegacyRFMatrixReplication(t, ctx, namespace, f, source)
	return source
}

// applyLegacyRFMatrixReplication puts the three supported system keyspaces on
// NetworkTopologyStrategy. A freshly bootstrapped node ships them as SimpleStrategy, which
// discovery rightly blocks as UnsupportedStrategy. These rows exercise credential and TLS
// boundaries, so the source must first be a legitimate discovery candidate.
func applyLegacyRFMatrixReplication(
	t *testing.T,
	ctx context.Context,
	namespace string,
	f *framework.E2eFramework,
	source legacyRFSource,
) {
	replication := map[string]int32{legacyRFMatrixDatacenter: 1}
	for _, keyspace := range []string{api.SystemAuthKeyspace, api.SystemTracesKeyspace, api.SystemDistributedKeyspace} {
		// alterLegacyRFKeyspace renders every datacenter in the map. cqlReplication only renders
		// the two base lifecycle datacenters, so it would emit a class-only map here and never
		// install matrix-dc.
		alterLegacyRFKeyspace(t, ctx, namespace, f, source, keyspace, replication, source.Auth)
	}
}

func createLegacyRFMatrixCredential(
	t *testing.T,
	ctx context.Context,
	namespace string,
	f *framework.E2eFramework,
	source legacyRFSource,
) {
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: source.CredentialSecret, Namespace: namespace},
		StringData: map[string]string{"username": "cassandra", "password": "cassandra"}}
	require.NoError(t, f.Create(ctx, framework.NewClusterKey(f.DataPlaneContexts[0], namespace, secret.Name), secret))
}

func createLegacyRFMatrixService(
	t *testing.T,
	ctx context.Context,
	namespace string,
	f *framework.E2eFramework,
	name string,
) (string, string) {
	// This ClusterIP is both the discovery contact point and the additional seed the managed
	// datacenter inherits from the accepted snapshot. cass-operator publishes no seed service for
	// these raw sources, so it must also carry internode traffic or the managed node can never
	// gossip its way into the source ring and never reaches Ready.
	service := &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace}, Spec: corev1.ServiceSpec{
		Selector:                 map[string]string{"legacy-rf-source": name},
		PublishNotReadyAddresses: true,
		Ports: []corev1.ServicePort{
			{Name: "cql", Port: 9042, TargetPort: intstr.FromInt(9042)},
			{Name: "internode", Port: 7000, TargetPort: intstr.FromInt(7000)},
		},
	}}
	require.NoError(t, f.Create(ctx, framework.NewClusterKey(f.DataPlaneContexts[0], namespace, name), service))
	require.NotEmpty(t, service.Spec.ClusterIP)
	createLegacyRFMatrixSeedService(t, ctx, namespace, f, name)
	return name, service.Spec.ClusterIP
}

// createLegacyRFMatrixSeedService publishes the Pod address itself. The CQL Service above is a
// ClusterIP that only forwards 9042, so naming it as the Cassandra seed leaves gossip on 7000
// unanswered and startup fails with "Unable to gossip with any peers". Resolving the seed to the
// Pod address instead makes this single node its own seed, which is how a standalone source boots.
func createLegacyRFMatrixSeedService(
	t *testing.T,
	ctx context.Context,
	namespace string,
	f *framework.E2eFramework,
	name string,
) {
	seedService := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: legacyRFMatrixSeedServiceName(name), Namespace: namespace},
		Spec: corev1.ServiceSpec{
			ClusterIP:                corev1.ClusterIPNone,
			Selector:                 map[string]string{"legacy-rf-source": name},
			PublishNotReadyAddresses: true,
			Ports:                    []corev1.ServicePort{{Name: "internode", Port: 7000, TargetPort: intstr.FromInt(7000)}},
		}}
	require.NoError(t, f.Create(ctx,
		framework.NewClusterKey(f.DataPlaneContexts[0], namespace, seedService.Name), seedService))
}

func legacyRFMatrixSeedServiceName(name string) string {
	return name + "-seed"
}

func newLegacyRFMatrixStatefulSet(
	namespace, version string,
	source legacyRFSource,
	tlsEnabled bool,
) *appsv1.StatefulSet {
	labels := map[string]string{"legacy-rf-source": source.RawStatefulSet}
	config := legacyRFMatrixConfig(source, tlsEnabled)
	replicas := int32(1)
	return &appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{Name: source.RawStatefulSet, Namespace: namespace},
		Spec: appsv1.StatefulSetSpec{ServiceName: source.ServiceName, Replicas: &replicas,
			Selector: &metav1.LabelSelector{MatchLabels: labels},
			Template: corev1.PodTemplateSpec{ObjectMeta: metav1.ObjectMeta{Labels: labels}, Spec: corev1.PodSpec{
				InitContainers: []corev1.Container{legacyRFMatrixConfigBuilder(version, config)},
				Containers:     []corev1.Container{legacyRFMatrixCassandra(version, tlsEnabled)},
				Volumes:        legacyRFMatrixVolumes(tlsEnabled, source),
			}},
		}}
}

func legacyRFMatrixConfig(source legacyRFSource, tlsEnabled bool) string {
	cassandraConfig := map[string]any{
		"cluster_name": source.ClusterName, "endpoint_snitch": "GossipingPropertyFileSnitch", "auto_snapshot": false,
		"authenticator": "AllowAllAuthenticator", "authorizer": "AllowAllAuthorizer", "role_manager": "CassandraRoleManager",
		"memtable_flush_writers": 1, "concurrent_compactors": 1, "concurrent_reads": 2, "concurrent_writes": 2,
	}
	if source.Auth {
		cassandraConfig["authenticator"] = "PasswordAuthenticator"
		cassandraConfig["authorizer"] = "CassandraAuthorizer"
	}
	if tlsEnabled {
		cassandraConfig["client_encryption_options"] = map[string]any{"enabled": true, "optional": true,
			"keystore": "/etc/client-tls/keystore.jks", "keystore_password": "changeit",
			"truststore": "/etc/client-tls/truststore.jks", "truststore_password": "changeit"}
	}
	config, _ := json.Marshal(map[string]any{"cassandra-yaml": cassandraConfig,
		"cluster-info":       map[string]any{"name": source.ClusterName, "seeds": legacyRFMatrixSeedServiceName(source.ServiceName)},
		"datacenter-info":    map[string]any{"name": legacyRFMatrixDatacenter},
		"jvm-server-options": map[string]any{"initial_heap_size": 512 << 20, "max_heap_size": 512 << 20}})
	return string(config)
}

func legacyRFMatrixConfigBuilder(version, config string) corev1.Container {
	return corev1.Container{Name: "server-config-init", Image: "docker.io/datastax/cass-config-builder:1.0-ubi",
		Env: []corev1.EnvVar{
			{Name: "POD_IP", ValueFrom: fieldRef("status.podIP")}, {Name: "HOST_IP", ValueFrom: fieldRef("status.hostIP")},
			{Name: "USE_HOST_IP_FOR_BROADCAST", Value: "false"}, {Name: "RACK_NAME", Value: "default"},
			{Name: "PRODUCT_VERSION", Value: version}, {Name: "PRODUCT_NAME", Value: "cassandra"},
			{Name: "POD_NAME", ValueFrom: fieldRef("metadata.name")}, {Name: "CONFIG_FILE_DATA", Value: config},
		}, VolumeMounts: []corev1.VolumeMount{{Name: "server-config", MountPath: "/config"}}}
}

func fieldRef(path string) *corev1.EnvVarSource {
	return &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: path}}
}

func legacyRFMatrixCassandra(version string, tlsEnabled bool) corev1.Container {
	mounts := []corev1.VolumeMount{{Name: "server-config", MountPath: "/config"},
		{Name: "server-data", MountPath: "/var/lib/cassandra"}, {Name: "server-logs", MountPath: "/var/log/cassandra"},
		{Name: "tmp", MountPath: "/tmp"}}
	if tlsEnabled {
		mounts = append(mounts, corev1.VolumeMount{Name: "client-tls", MountPath: "/etc/client-tls", ReadOnly: true})
	}
	return corev1.Container{Name: "cassandra", Image: "docker.io/k8ssandra/cass-management-api:" + version + "-ubi",
		Env: []corev1.EnvVar{{Name: "POD_NAME", ValueFrom: fieldRef("metadata.name")},
			{Name: "NODE_NAME", ValueFrom: fieldRef("spec.nodeName")}, {Name: "DS_LICENSE", Value: "accept"},
			// No cass-operator drives these raw sources, so the Management API must own the
			// Cassandra lifecycle and start it itself. MGMT_API_NO_KEEP_ALIVE would disable that
			// lifecycle manager and leave port 9042 closed forever.
			{Name: "USE_MGMT_API", Value: "true"},
			{Name: "MGMT_API_EXPLICIT_START", Value: "false"}},
		Ports: []corev1.ContainerPort{{Name: "native", ContainerPort: 9042}, {Name: "mgmt-api-http", ContainerPort: 8080}},
		ReadinessProbe: &corev1.Probe{ProbeHandler: corev1.ProbeHandler{TCPSocket: &corev1.TCPSocketAction{Port: intstr.FromInt(9042)}},
			InitialDelaySeconds: 10, PeriodSeconds: 5},
		Resources: corev1.ResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("100m"),
			corev1.ResourceMemory: resource.MustParse("1Gi")}, Limits: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("1"),
			corev1.ResourceMemory: resource.MustParse("1Gi")}}, VolumeMounts: mounts}
}

func legacyRFMatrixVolumes(tlsEnabled bool, source legacyRFSource) []corev1.Volume {
	volumes := []corev1.Volume{
		{Name: "server-config", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}},
		{Name: "server-data", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}},
		{Name: "server-logs", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}},
		{Name: "tmp", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}},
	}
	if tlsEnabled {
		volumes = append(volumes, corev1.Volume{Name: "client-tls", VolumeSource: corev1.VolumeSource{
			Secret: &corev1.SecretVolumeSource{SecretName: source.TLSSecret + "-server"}}})
	}
	return volumes
}

func waitForLegacyRFMatrixSource(
	t *testing.T,
	ctx context.Context,
	namespace string,
	f *framework.E2eFramework,
	source legacyRFSource,
) {
	require.NoError(t, wait.PollUntilContextTimeout(ctx, 5*time.Second, 10*time.Minute, true,
		func(ctx context.Context) (bool, error) {
			pod := &corev1.Pod{}
			key := framework.NewClusterKey(f.DataPlaneContexts[0], namespace, source.PodName)
			if err := f.Get(ctx, key, pod); err != nil || !allLegacyRFContainersReady(pod) {
				return false, client.IgnoreNotFound(err)
			}
			query := "SELECT cluster_name FROM system.local"
			if source.Auth {
				_, err := f.ExecuteCql(ctx, f.DataPlaneContexts[0], namespace, source.ClusterName, source.PodName, query)
				return err == nil, nil
			}
			_, err := f.ExecuteCqlNoAuth(f.DataPlaneContexts[0], namespace, source.PodName, query)
			return err == nil, nil
		}))
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

func createLegacyRFSourceTLS(
	t *testing.T,
	ctx context.Context,
	namespace string,
	f *framework.E2eFramework,
	suffix, serviceIP string,
) (string, string) {
	base := "legacy-rf-tls-" + suffix
	password := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: base + "-jks-password", Namespace: namespace},
		StringData: map[string]string{"password": "changeit"}}
	require.NoError(t, f.Create(ctx, framework.NewClusterKey(f.DataPlaneContexts[0], namespace, password.Name), password))
	createLegacyRFCertManagerObject(t, ctx, namespace, f, "Issuer", base+"-selfsigned",
		map[string]any{"selfSigned": map[string]any{}})
	createLegacyRFCertManagerObject(t, ctx, namespace, f, "Certificate", base+"-ca", map[string]any{
		"isCA": true, "commonName": base + "-ca", "secretName": base + "-ca",
		"issuerRef": map[string]any{"name": base + "-selfsigned", "kind": "Issuer"},
	})
	waitForLegacyRFSecretKey(t, ctx, namespace, f, base+"-ca", "tls.crt")
	createLegacyRFCertManagerObject(t, ctx, namespace, f, "Issuer", base+"-issuer",
		map[string]any{"ca": map[string]any{"secretName": base + "-ca"}})
	createLegacyRFCertManagerObject(t, ctx, namespace, f, "Certificate", base+"-server", map[string]any{
		"secretName": base + "-server", "ipAddresses": []any{serviceIP},
		"privateKey": map[string]any{"algorithm": "RSA", "encoding": "PKCS1", "size": int64(2048)},
		"keystores": map[string]any{"jks": map[string]any{"create": true,
			"passwordSecretRef": map[string]any{"name": base + "-jks-password", "key": "password"}}},
		"issuerRef": map[string]any{"name": base + "-issuer", "kind": "Issuer"},
	})
	serverSecret := waitForLegacyRFSecretKey(t, ctx, namespace, f, base+"-server", "keystore.jks")
	discoverySecret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: base, Namespace: namespace},
		Data: map[string][]byte{"ca.crt": append([]byte(nil), serverSecret.Data["ca.crt"]...)}}
	require.NotEmpty(t, discoverySecret.Data["ca.crt"])
	require.NoError(t, f.Create(ctx, framework.NewClusterKey(f.DataPlaneContexts[0], namespace, base), discoverySecret))
	wrongName := base + "-wrong"
	wrongSecret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: wrongName, Namespace: namespace},
		Data: map[string][]byte{"ca.crt": newLegacyRFTestCA(t)}}
	require.NoError(t, f.Create(ctx, framework.NewClusterKey(f.DataPlaneContexts[0], namespace, wrongName), wrongSecret))
	return base, wrongName
}

func createLegacyRFCertManagerObject(
	t *testing.T,
	ctx context.Context,
	namespace string,
	f *framework.E2eFramework,
	kind, name string,
	spec map[string]any,
) {
	object := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "cert-manager.io/v1", "kind": kind,
		"metadata": map[string]any{"name": name, "namespace": namespace}, "spec": spec,
	}}
	require.NoError(t, f.Create(ctx, framework.NewClusterKey(f.DataPlaneContexts[0], namespace, name), object))
}

func waitForLegacyRFSecretKey(
	t *testing.T,
	ctx context.Context,
	namespace string,
	f *framework.E2eFramework,
	name, dataKey string,
) *corev1.Secret {
	secret := &corev1.Secret{}
	key := framework.NewClusterKey(f.DataPlaneContexts[0], namespace, name)
	require.NoError(t, wait.PollUntilContextTimeout(ctx, 2*time.Second, 3*time.Minute, true,
		func(ctx context.Context) (bool, error) {
			if err := f.Get(ctx, key, secret); err != nil {
				return false, client.IgnoreNotFound(err)
			}
			return len(secret.Data[dataKey]) > 0, nil
		}))
	return secret.DeepCopy()
}

func newLegacyRFTestCA(t *testing.T) []byte {
	t.Helper()
	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	template := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "wrong-legacy-rf-ca"},
		NotBefore: time.Now().Add(-time.Minute), NotAfter: time.Now().Add(time.Hour),
		IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature}
	certificate, err := x509.CreateCertificate(rand.Reader, template, template, &privateKey.PublicKey, privateKey)
	require.NoError(t, err)
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certificate})
}

func newLegacyRFSourceDatacenter(
	namespace, dcName, version string,
	source legacyRFSource,
	auth, tlsEnabled, joinsExisting bool,
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
	if tlsEnabled {
		cassandraConfig["client_encryption_options"] = map[string]any{
			"enabled": true, "optional": true,
			"keystore": "/etc/client-tls/keystore.jks", "keystore_password": "changeit",
			"truststore": "/etc/client-tls/truststore.jks", "truststore_password": "changeit",
		}
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
	if tlsEnabled {
		dc.Spec.StorageConfig.AdditionalVolumes = cassdcapi.AdditionalVolumesSlice{{
			Name: "client-tls", MountPath: "/etc/client-tls",
			VolumeSource: &corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{SecretName: source.TLSSecret + "-server"}},
		}}
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

func runLegacyRFCredentialTLSMatrix(
	t *testing.T,
	ctx context.Context,
	namespace string,
	f *framework.E2eFramework,
	scenario legacyRFCassandraScenario,
	baselineCluster *api.K8ssandraCluster,
	evidence *legacyRFEvidence,
) {
	deleteLegacyRFTarget(t, ctx, f, baselineCluster)
	pairedOutcomes := make(map[string]api.LegacyRFDiscoveryPhase)
	for _, sourceAuth := range []bool{false, true} {
		for _, tlsEnabled := range []bool{false, true} {
			sourceID := legacyRFSourceID(sourceAuth, tlsEnabled)
			sourceShortID := legacyRFMatrixShortID(sourceAuth, tlsEnabled)
			source := provisionLegacyRFMatrixSource(t, ctx, namespace, f, scenario, sourceShortID, sourceAuth, tlsEnabled)
			for _, credentials := range []bool{false, true} {
				for _, targetAuth := range []bool{false, true} {
					row := legacyRFMatrixRow{Name: legacyRFMatrixRowName(sourceID, credentials, targetAuth),
						ShortID:    legacyRFMatrixRowShortID(sourceShortID, credentials, targetAuth),
						SourceAuth: sourceAuth, Credentials: credentials, TLS: tlsEnabled, TargetAuth: targetAuth}
					phase := runLegacyRFMatrixRow(t, ctx, namespace, f, scenario, source, row, evidence)
					pairKey := fmt.Sprintf("%t/%t/%t", sourceAuth, credentials, tlsEnabled)
					if prior, found := pairedOutcomes[pairKey]; found {
						require.Equal(t, prior, phase, "managed-target auth changed source outcome for %s", pairKey)
					} else {
						pairedOutcomes[pairKey] = phase
					}
				}
			}
			if tlsEnabled {
				for _, targetAuth := range []bool{false, true} {
					row := legacyRFMatrixRow{Name: legacyRFMatrixRowName(sourceID+"-mismatch", sourceAuth, targetAuth),
						ShortID:    legacyRFMatrixRowShortID(sourceShortID+"x", sourceAuth, targetAuth),
						SourceAuth: sourceAuth, Credentials: sourceAuth, TLS: true, TLSMismatch: true, TargetAuth: targetAuth}
					require.Equal(t, api.LegacyRFDiscoveryPhaseBlocked,
						runLegacyRFMatrixRow(t, ctx, namespace, f, scenario, source, row, evidence))
				}
			}
			deleteLegacyRFSource(t, ctx, namespace, f, source)
		}
	}
}

func legacyRFSourceID(sourceAuth, tlsEnabled bool) string {
	authName, tlsName := "anon", "plain"
	if sourceAuth {
		authName = "auth"
	}
	if tlsEnabled {
		tlsName = "tls"
	}
	return authName + "-" + tlsName
}

func legacyRFMatrixRowName(sourceID string, credentials, targetAuth bool) string {
	return fmt.Sprintf("%s-creds-%t-target-auth-%t", sourceID, credentials, targetAuth)
}

// legacyRFMatrixShortID compresses a matrix coordinate into a few characters. Descriptive row
// names stay in the evidence, but Kubernetes names derived from them must keep the generated
// StatefulSet name within the 60 character limit the K8ssandraCluster webhook enforces.
// "n"/"a" is source authentication off/on, "p"/"t" is plaintext/TLS.
func legacyRFMatrixShortID(sourceAuth, tlsEnabled bool) string {
	authPart, tlsPart := "n", "p"
	if sourceAuth {
		authPart = "a"
	}
	if tlsEnabled {
		tlsPart = "t"
	}
	return authPart + tlsPart
}

func legacyRFMatrixRowShortID(sourceShortID string, credentials, targetAuth bool) string {
	return fmt.Sprintf("%s-c%s-t%s", sourceShortID, legacyRFFlag(credentials), legacyRFFlag(targetAuth))
}

func legacyRFFlag(value bool) string {
	if value {
		return "1"
	}
	return "0"
}

func runLegacyRFMatrixRow(
	t *testing.T,
	ctx context.Context,
	namespace string,
	f *framework.E2eFramework,
	scenario legacyRFCassandraScenario,
	source legacyRFSource,
	row legacyRFMatrixRow,
	evidence *legacyRFEvidence,
) api.LegacyRFDiscoveryPhase {
	cluster := loadLegacyRFTarget(t, namespace, f.DataPlaneContexts[0], scenario, source)
	configureLegacyRFTargetRow(t, ctx, namespace, f, cluster, source, row)
	require.NoError(t, f.Client.Create(ctx, cluster))
	expectedPhase, expectedReason := legacyRFExpectedMatrixOutcome(row)
	observed := waitForLegacyRFPhase(t, ctx, f.Client, client.ObjectKeyFromObject(cluster), expectedPhase, expectedReason)
	assertLegacyRFMatrixBoundary(t, ctx, namespace, f, observed, source, row)
	if expectedPhase == api.LegacyRFDiscoveryPhaseAccepted && row.TargetAuth == row.SourceAuth {
		_ = waitForLegacyRFTargetReady(t, ctx, namespace, f, observed)
		output := queryLegacyRFReplication(t, ctx, namespace, f, source, api.SystemAuthKeyspace, row.SourceAuth)
		// Matrix sources own a single datacenter of their own; legacy-a belongs to the base
		// lifecycle sources. The external entry must survive the managed datacenter joining.
		require.Contains(t, output, legacyRFMatrixDatacenter)
	}
	evidence.Operations = append(evidence.Operations, legacyRFOperationEvidence{At: time.Now().UTC(),
		Operation: "credential/TLS matrix", Outcome: fmt.Sprintf("%s:%s", row.Name, expectedPhase)})
	deleteLegacyRFTarget(t, ctx, f, observed)
	return expectedPhase
}

func configureLegacyRFTargetRow(
	t *testing.T,
	ctx context.Context,
	namespace string,
	f *framework.E2eFramework,
	cluster *api.K8ssandraCluster,
	source legacyRFSource,
	row legacyRFMatrixRow,
) {
	cleanName := framework.CleanupForKubernetes("lrf-" + row.ShortID)
	cluster.Name = cleanName
	cluster.Spec.Cassandra.Datacenters[0].Meta.Name = framework.CleanupForKubernetes("t-" + row.ShortID)
	cluster.Spec.Auth = ptr.To(row.TargetAuth)
	cluster.Spec.Cassandra.LegacyCqlCredentialsSecretRef = nil
	if row.Credentials {
		cluster.Spec.Cassandra.LegacyCqlCredentialsSecretRef = &corev1.LocalObjectReference{Name: source.CredentialSecret}
	}
	if row.TLS {
		secretName := source.TLSSecret
		if row.TLSMismatch {
			secretName = source.WrongTLSSecret
		}
		cluster.Spec.Cassandra.LegacyCqlTLSSecretRef = &corev1.LocalObjectReference{Name: secretName}
	}
	if row.TargetAuth {
		secretName := cleanName + "-target-superuser"
		secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: secretName, Namespace: namespace},
			StringData: map[string]string{"username": "managed-admin", "password": "managed-target-e2e-only"}}
		require.NoError(t, f.Create(ctx, framework.NewClusterKey(f.DataPlaneContexts[0], namespace, secretName), secret))
		cluster.Spec.Cassandra.SuperuserSecretRef = corev1.LocalObjectReference{Name: secretName}
	}
}

func legacyRFExpectedMatrixOutcome(row legacyRFMatrixRow) (api.LegacyRFDiscoveryPhase, api.LegacyRFDiscoveryReason) {
	if row.TLSMismatch {
		return api.LegacyRFDiscoveryPhaseBlocked, api.LegacyRFReasonTLSFailed
	}
	if row.SourceAuth && !row.Credentials {
		return api.LegacyRFDiscoveryPhaseBlocked, api.LegacyRFReasonAuthenticationRejected
	}
	return api.LegacyRFDiscoveryPhaseAccepted, ""
}

func waitForLegacyRFPhase(
	t *testing.T,
	ctx context.Context,
	reader client.Reader,
	key client.ObjectKey,
	phase api.LegacyRFDiscoveryPhase,
	reason api.LegacyRFDiscoveryReason,
) *api.K8ssandraCluster {
	observed := &api.K8ssandraCluster{}
	require.NoError(t, wait.PollUntilContextTimeout(ctx, 5*time.Second, 10*time.Minute, true,
		func(ctx context.Context) (bool, error) {
			if err := reader.Get(ctx, key, observed); err != nil {
				return false, client.IgnoreNotFound(err)
			}
			status := observed.Status.LegacyRFDiscovery
			return status != nil && status.Phase == phase && status.Reason == reason, nil
		}))
	return observed.DeepCopy()
}

func assertLegacyRFMatrixBoundary(
	t *testing.T,
	ctx context.Context,
	namespace string,
	f *framework.E2eFramework,
	cluster *api.K8ssandraCluster,
	source legacyRFSource,
	row legacyRFMatrixRow,
) {
	status := cluster.Status.LegacyRFDiscovery
	dcName := cluster.Spec.Cassandra.Datacenters[0].Meta.Name
	dc := &cassdcapi.CassandraDatacenter{}
	dcKey := framework.NewClusterKey(f.DataPlaneContexts[0], namespace, dcName)
	if status.Phase == api.LegacyRFDiscoveryPhaseBlocked {
		require.Nil(t, status.AcceptedSnapshot)
		require.True(t, errors.IsNotFound(f.Get(ctx, dcKey, dc)), "blocked row created managed DC")
		return
	}
	require.NotNil(t, status.AcceptedSnapshot)
	purposes := make(map[string]struct{}, len(status.AcceptedSnapshot.SecretBindings))
	for _, binding := range status.AcceptedSnapshot.SecretBindings {
		purposes[binding.Purpose] = struct{}{}
	}
	_, hasAuth := purposes["auth"]
	_, hasTLS := purposes["tls"]
	require.Equal(t, row.Credentials, hasAuth)
	require.Equal(t, row.TLS, hasTLS)
	require.NotEqual(t, source.CredentialSecret, cluster.Spec.Cassandra.SuperuserSecretRef.Name,
		"managed target credentials must not supply legacy source authentication")
}

func deleteLegacyRFTarget(t *testing.T, ctx context.Context, f *framework.E2eFramework, cluster *api.K8ssandraCluster) {
	key := client.ObjectKeyFromObject(cluster)
	err := f.DeleteK8ssandraCluster(ctx, key, 15*time.Minute, 5*time.Second)
	require.NoError(t, err)
}

func deleteLegacyRFSource(
	t *testing.T,
	ctx context.Context,
	namespace string,
	f *framework.E2eFramework,
	source legacyRFSource,
) {
	if source.RawStatefulSet != "" {
		deleteLegacyRFMatrixSource(t, ctx, namespace, f, source)
		return
	}
	suffix := strings.TrimPrefix(source.ClusterName, "legacy-rf-source-")
	for index := 4; index >= 0; index-- {
		name := fmt.Sprintf("source-%s-%d", suffix, index)
		key := framework.NewClusterKey(f.DataPlaneContexts[0], namespace, name)
		dc := &cassdcapi.CassandraDatacenter{}
		if err := f.Get(ctx, key, dc); err == nil {
			require.NoError(t, f.Delete(ctx, key, dc))
		}
		require.NoError(t, wait.PollUntilContextTimeout(ctx, 5*time.Second, 10*time.Minute, true,
			func(ctx context.Context) (bool, error) {
				err := f.Get(ctx, key, &cassdcapi.CassandraDatacenter{})
				return errors.IsNotFound(err), client.IgnoreNotFound(err)
			}))
	}
}

func deleteLegacyRFMatrixSource(
	t *testing.T,
	ctx context.Context,
	namespace string,
	f *framework.E2eFramework,
	source legacyRFSource,
) {
	statefulSet := &appsv1.StatefulSet{}
	statefulSetKey := framework.NewClusterKey(f.DataPlaneContexts[0], namespace, source.RawStatefulSet)
	if err := f.Get(ctx, statefulSetKey, statefulSet); err == nil {
		require.NoError(t, f.Delete(ctx, statefulSetKey, statefulSet))
	}
	require.NoError(t, wait.PollUntilContextTimeout(ctx, 2*time.Second, 5*time.Minute, true,
		func(ctx context.Context) (bool, error) {
			err := f.Get(ctx, framework.NewClusterKey(f.DataPlaneContexts[0], namespace, source.PodName), &corev1.Pod{})
			return errors.IsNotFound(err), client.IgnoreNotFound(err)
		}))
	service := &corev1.Service{}
	serviceKey := framework.NewClusterKey(f.DataPlaneContexts[0], namespace, source.ServiceName)
	if err := f.Get(ctx, serviceKey, service); err == nil {
		require.NoError(t, f.Delete(ctx, serviceKey, service))
	}
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
		}, true, false, index > 0)
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

func TestLegacyRFCredentialTLSMatrixDefinition(t *testing.T) {
	rows := make([]legacyRFMatrixRow, 0, 20)
	for _, sourceAuth := range []bool{false, true} {
		for _, tlsEnabled := range []bool{false, true} {
			for _, credentials := range []bool{false, true} {
				for _, targetAuth := range []bool{false, true} {
					rows = append(rows, legacyRFMatrixRow{SourceAuth: sourceAuth, TLS: tlsEnabled,
						Credentials: credentials, TargetAuth: targetAuth})
				}
			}
			if tlsEnabled {
				for _, targetAuth := range []bool{false, true} {
					rows = append(rows, legacyRFMatrixRow{SourceAuth: sourceAuth, TLS: true,
						Credentials: sourceAuth, TLSMismatch: true, TargetAuth: targetAuth})
				}
			}
		}
	}
	require.Len(t, rows, 20)
	paired := make(map[string]map[bool]api.LegacyRFDiscoveryPhase)
	for _, row := range rows {
		phase, reason := legacyRFExpectedMatrixOutcome(row)
		if row.TLSMismatch {
			require.Equal(t, api.LegacyRFReasonTLSFailed, reason)
		} else if row.SourceAuth && !row.Credentials {
			require.Equal(t, api.LegacyRFReasonAuthenticationRejected, reason)
		} else {
			require.Equal(t, api.LegacyRFDiscoveryPhaseAccepted, phase)
		}
		key := fmt.Sprintf("%t/%t/%t/%t", row.SourceAuth, row.Credentials, row.TLS, row.TLSMismatch)
		if paired[key] == nil {
			paired[key] = make(map[bool]api.LegacyRFDiscoveryPhase)
		}
		paired[key][row.TargetAuth] = phase
	}
	for key, outcomes := range paired {
		require.Equal(t, outcomes[false], outcomes[true], "target auth changed outcome for %s", key)
	}
}

func TestLegacyRFCredentialTLSMatrixUsesRawPinnedSource(t *testing.T) {
	source := legacyRFSource{Auth: true, ClusterName: "matrix-auth-tls", RawStatefulSet: "matrix-auth-tls",
		ServiceName: "matrix-auth-tls", TLSSecret: "matrix-auth-tls-ca"}
	statefulSet := newLegacyRFMatrixStatefulSet("test", "4.1.9", source, true)

	require.Empty(t, statefulSet.OwnerReferences, "matrix source must not be owned by CassandraDatacenter")
	require.Equal(t, "docker.io/k8ssandra/cass-management-api:4.1.9-ubi",
		statefulSet.Spec.Template.Spec.Containers[0].Image)
	require.Equal(t, map[string]string{"legacy-rf-source": source.RawStatefulSet},
		statefulSet.Spec.Selector.MatchLabels)
	require.Contains(t, statefulSet.Spec.Template.Spec.Volumes, corev1.Volume{Name: "client-tls",
		VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{SecretName: source.TLSSecret + "-server"}}})

	var config map[string]map[string]any
	require.NoError(t, json.Unmarshal([]byte(statefulSet.Spec.Template.Spec.InitContainers[0].Env[7].Value), &config))
	require.Equal(t, "PasswordAuthenticator", config["cassandra-yaml"]["authenticator"])
	require.Equal(t, legacyRFMatrixSeedServiceName(source.ServiceName), config["cluster-info"]["seeds"],
		"a standalone source must seed from the Pod address, not the CQL ClusterIP")
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
