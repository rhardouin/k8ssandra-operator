/*
Copyright 2021.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package v1alpha1

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"net/url"
	"path/filepath"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/api/resource"

	logrusr "github.com/bombsimon/logrusr/v2"
	"github.com/k8ssandra/cass-operator/apis/cassandra/v1beta1"
	"github.com/k8ssandra/k8ssandra-operator/pkg/clientcache"
	"github.com/k8ssandra/k8ssandra-operator/pkg/unstructured"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	admissionv1 "k8s.io/api/admission/v1"
	admissionregistrationv1 "k8s.io/api/admissionregistration/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/labels"
	"sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/controller-runtime/pkg/webhook"

	//+kubebuilder:scaffold:imports
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	medusaapi "github.com/k8ssandra/k8ssandra-operator/apis/medusa/v1alpha1"
	reaperapi "github.com/k8ssandra/k8ssandra-operator/apis/reaper/v1alpha1"
)

var k8sClient client.Client
var testEnv *envtest.Environment
var ctx context.Context
var cancel context.CancelFunc

var minimalInMemoryReaperStorageConfig = &corev1.PersistentVolumeClaimSpec{
	StorageClassName: func() *string { s := "test"; return &s }(),
	AccessModes:      []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
	Resources: corev1.VolumeResourceRequirements{
		Requests: corev1.ResourceList{
			corev1.ResourceStorage: resource.MustParse("1Gi"),
		},
	},
}

var minimalInMemoryReaperConfig = &reaperapi.ReaperClusterTemplate{
	ReaperTemplate: reaperapi.ReaperTemplate{
		StorageType:   reaperapi.StorageTypeLocal,
		StorageConfig: minimalInMemoryReaperStorageConfig,
	},
}

func TestK8ssandraClusterWebhook(t *testing.T) {
	required := require.New(t)
	ctx, cancel = context.WithCancel(context.TODO())

	logrusLog := logrus.New()
	log := logrusr.New(logrusLog)
	logf.SetLogger(log)

	testEnv = &envtest.Environment{
		CRDDirectoryPaths:     []string{filepath.Join("..", "..", "..", "config", "crd", "bases")},
		ErrorIfCRDPathMissing: false,
		WebhookInstallOptions: envtest.WebhookInstallOptions{
			Paths: []string{filepath.Join("..", "..", "..", "config", "webhook")},
		},
	}

	cfg, err := testEnv.Start()
	required.NoError(err)
	required.NotNil(cfg)

	defer cancel()
	defer func(testEnv *envtest.Environment) {
		err := testEnv.Stop()
		if err != nil {
			log.Error(err, "failure to stop test environment")
		}
	}(testEnv)

	scheme := runtime.NewScheme()
	err = AddToScheme(scheme)
	required.NoError(err)

	err = corev1.AddToScheme(scheme)
	required.NoError(err)

	err = admissionv1.AddToScheme(scheme)
	required.NoError(err)
	err = admissionregistrationv1.AddToScheme(scheme)
	required.NoError(err)
	err = apiextensionsv1.AddToScheme(scheme)
	required.NoError(err)

	err = AddToScheme(scheme)
	required.NoError(err)

	err = reaperapi.AddToScheme(scheme)
	required.NoError(err)

	//+kubebuilder:scaffold:scheme

	k8sClient, err = client.New(cfg, client.Options{Scheme: scheme})
	required.NoError(err)
	required.NotNil(k8sClient)
	required.NoError(enableLegacyRFSecretReferenceFields(k8sClient))
	required.NoError(removeGeneratedMutatingWebhook(k8sClient))

	// start webhook server using Manager
	webhookInstallOptions := &testEnv.WebhookInstallOptions

	whServer := webhook.NewServer(webhook.Options{
		Port:    webhookInstallOptions.LocalServingPort,
		Host:    webhookInstallOptions.LocalServingHost,
		CertDir: webhookInstallOptions.LocalServingCertDir,
		TLSOpts: []func(*tls.Config){func(config *tls.Config) {}},
	})

	mgr, err := ctrl.NewManager(cfg, ctrl.Options{
		Scheme:         scheme,
		WebhookServer:  whServer,
		LeaderElection: false,
		Metrics: server.Options{
			BindAddress: "0",
		},
	})
	required.NoError(err)

	clientCache := clientcache.New(k8sClient, k8sClient, scheme)
	clientCache.AddClient("envtest", k8sClient)
	required.NoError(SetupK8ssandraClusterWebhookWithManager(mgr, clientCache))

	//+kubebuilder:scaffold:webhook

	go func() {
		err = mgr.Start(ctx)
		required.NoError(err)
	}()

	// wait for the webhook server to get ready
	dialer := &net.Dialer{Timeout: time.Second}
	addrPort := fmt.Sprintf("%s:%d", webhookInstallOptions.LocalServingHost, webhookInstallOptions.LocalServingPort)
	required.Eventually(func() bool {
		conn, err := tls.DialWithDialer(dialer, "tcp", addrPort, &tls.Config{InsecureSkipVerify: true})
		if err != nil {
			return false
		}
		closeErr := conn.Close()
		if closeErr != nil {
			log.Error(closeErr, "failed to close connection")
		}
		return true
	}, 2*time.Second, 300*time.Millisecond)

	legacyCluster := createLegacyUnmarkedCluster(required)
	required.NoError(installLegacyRFMutatingWebhooks(k8sClient, webhookInstallOptions))

	t.Run("LegacyRFCreateInjection", testLegacyRFCreateInjection)
	t.Run("LegacyRFForgedMarker", testLegacyRFForgedMarker)
	t.Run("LegacyRFMarkerImmutability", testLegacyRFMarkerImmutability)
	t.Run("LegacyRFNoUpdateInjection", func(t *testing.T) {
		testLegacyRFNoUpdateInjection(t, legacyCluster)
	})
	t.Run("LegacyRFLocalValidation", testLegacyRFLocalValidation)
	t.Run("LegacyRFAdmissionUnavailable", testLegacyRFAdmissionUnavailable)
	t.Run("LegacyRFNoSeedCompatibility", testLegacyRFNoSeedCompatibility)

	t.Run("ContextValidation", testContextValidation)
	t.Run("ReaperKeyspaceValidation", testReaperKeyspaceValidation)
	t.Run("StorageConfigValidation", testStorageConfigValidation)
	t.Run("NumTokensValidation", testNumTokens)
	t.Run("NumTokensValidationInUpdate", testNumTokensInUpdate)
	t.Run("StsNameTooLong", testStsNameTooLong)
	t.Run("MedusaPrefixMissing", testMedusaPrefixMissing)
	t.Run("InvalidDcName", testInvalidDcName)
	t.Run("MedusaConfigNonLocalNamespace", testMedusaNonLocalNamespace)
	t.Run("MedusaConfigRefAndStorageSecretRefBothPresent", testMedusaConfigRefAndStorageSecretRefMutuallyExclusive)
	t.Run("AutomatedUpdateAnnotation", testAutomatedUpdateAnnotation)
	t.Run("ReaperStorage", testReaperStorage)
	t.Run("NoDCRename", testNoDCRename)
	t.Run("MedusaMandatoryFieldsMissing", testMedusaMandatoryFieldsMissing)
}

func TestValidateLegacyRFDiscoveryEnforcesSeedLimit(t *testing.T) {
	cluster := &K8ssandraCluster{
		ObjectMeta: metav1.ObjectMeta{
			Annotations: map[string]string{LegacyRFDiscoveryMarkerAnnotation: LegacyRFDiscoveryMarkerVersion},
		},
		Spec: K8ssandraClusterSpec{Cassandra: &CassandraClusterTemplate{}},
	}
	seeds := make([]string, 33)
	for index := range seeds {
		seeds[index] = fmt.Sprintf("192.0.2.%d", index+1)
	}

	cluster.Spec.Cassandra.AdditionalSeeds = seeds[:32]
	require.NoError(t, validateLegacyRFDiscovery(cluster))

	cluster.Spec.Cassandra.AdditionalSeeds = seeds
	require.ErrorContains(t, validateLegacyRFDiscovery(cluster), "at most 32")
}

// Clusters created before this feature carry no discovery marker. additionalSeeds has always
// documented hostnames as supported, so the discovery seed contract must not retroactively make
// those objects unpatchable.
func TestValidateLegacyRFDiscoverySparesUnmarkedClusters(t *testing.T) {
	cluster := &K8ssandraCluster{Spec: K8ssandraClusterSpec{Cassandra: &CassandraClusterTemplate{
		AdditionalSeeds: []string{"seed-1.legacy.example.test", "seed-2.legacy.example.test"},
	}}}
	cluster.Spec.SecretsProvider = "external"

	require.NoError(t, validateLegacyRFDiscovery(cluster))

	metav1.SetMetaDataAnnotation(&cluster.ObjectMeta, LegacyRFDiscoveryMarkerAnnotation, LegacyRFDiscoveryMarkerVersion)
	require.Error(t, validateLegacyRFDiscovery(cluster))
}

func removeGeneratedMutatingWebhook(kubeClient client.Client) error {
	configuration := &admissionregistrationv1.MutatingWebhookConfiguration{}
	key := client.ObjectKey{Name: "mutating-webhook-configuration"}
	if err := kubeClient.Get(ctx, key, configuration); err != nil {
		return fmt.Errorf("get generated mutating webhook: %w", err)
	}
	if err := kubeClient.Delete(ctx, configuration); err != nil {
		return fmt.Errorf("delete generated mutating webhook: %w", err)
	}
	return nil
}

func enableLegacyRFSecretReferenceFields(kubeClient client.Client) error {
	crd := &apiextensionsv1.CustomResourceDefinition{}
	if err := kubeClient.Get(ctx, client.ObjectKey{Name: "k8ssandraclusters.k8ssandra.io"}, crd); err != nil {
		return fmt.Errorf("get K8ssandraCluster CRD: %w", err)
	}
	for index := range crd.Spec.Versions {
		if crd.Spec.Versions[index].Name != GroupVersion.Version {
			continue
		}
		schema := crd.Spec.Versions[index].Schema.OpenAPIV3Schema
		specSchema := schema.Properties["spec"]
		cassandraSchema := specSchema.Properties["cassandra"]
		cassandraSchema.Properties["legacyCqlCredentialsSecretRef"] = apiextensionsv1.JSONSchemaProps{
			Type: "object",
			Properties: map[string]apiextensionsv1.JSONSchemaProps{
				"name": {Type: "string"},
			},
		}
		cassandraSchema.Properties["legacyCqlTLSSecretRef"] = apiextensionsv1.JSONSchemaProps{
			Type: "object",
			Properties: map[string]apiextensionsv1.JSONSchemaProps{
				"name": {Type: "string"},
			},
		}
		specSchema.Properties["cassandra"] = cassandraSchema
		schema.Properties["spec"] = specSchema
		break
	}
	if err := kubeClient.Update(ctx, crd); err != nil {
		return fmt.Errorf("update K8ssandraCluster CRD test schema: %w", err)
	}
	return nil
}

func createLegacyUnmarkedCluster(required *require.Assertions) *K8ssandraCluster {
	createNamespace(required, "legacy-unmarked")
	cluster := createMinimalClusterObj("legacy-unmarked", "legacy-unmarked")
	cluster.Spec.Cassandra.ServerVersion = "4.1.8"
	required.NoError(k8sClient.Create(ctx, cluster))
	required.NotContains(cluster.Annotations, LegacyRFDiscoveryMarkerAnnotation)
	return cluster
}

func installLegacyRFMutatingWebhooks(kubeClient client.Client, options *envtest.WebhookInstallOptions) error {
	webhookURL := url.URL{
		Scheme: "https",
		Host:   net.JoinHostPort(options.LocalServingHost, fmt.Sprintf("%d", options.LocalServingPort)),
		Path:   "/mutate-k8ssandra-io-v1alpha1-k8ssandracluster",
	}
	configurations := []*admissionregistrationv1.MutatingWebhookConfiguration{
		legacyRFMutatingWebhook("legacy-rf-admission", webhookURL.String(), options.LocalServingCAData, nil),
		legacyRFMutatingWebhook("legacy-rf-admission-outage", "https://127.0.0.1:1/unavailable", nil,
			&metav1.LabelSelector{MatchLabels: map[string]string{"legacy-rf-admission": "outage"}}),
	}
	for _, configuration := range configurations {
		if err := kubeClient.Create(ctx, configuration); err != nil {
			return fmt.Errorf("install %s: %w", configuration.Name, err)
		}
	}
	return nil
}

func legacyRFMutatingWebhook(
	name string,
	webhookURL string,
	caBundle []byte,
	namespaceSelector *metav1.LabelSelector,
) *admissionregistrationv1.MutatingWebhookConfiguration {
	failurePolicy := admissionregistrationv1.Fail
	sideEffects := admissionregistrationv1.SideEffectClassNone
	timeoutSeconds := int32(2)
	return &admissionregistrationv1.MutatingWebhookConfiguration{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Webhooks: []admissionregistrationv1.MutatingWebhook{{
			Name:                    name + ".k8ssandra.io",
			AdmissionReviewVersions: []string{"v1"},
			FailurePolicy:           &failurePolicy,
			SideEffects:             &sideEffects,
			TimeoutSeconds:          &timeoutSeconds,
			NamespaceSelector:       namespaceSelector,
			ClientConfig: admissionregistrationv1.WebhookClientConfig{
				URL:      &webhookURL,
				CABundle: caBundle,
			},
			Rules: []admissionregistrationv1.RuleWithOperations{{
				Operations: []admissionregistrationv1.OperationType{
					admissionregistrationv1.Create, admissionregistrationv1.Update,
				},
				Rule: admissionregistrationv1.Rule{
					APIGroups:   []string{GroupVersion.Group},
					APIVersions: []string{GroupVersion.Version},
					Resources:   []string{"k8ssandraclusters"},
				},
			}},
		}},
	}
}

func testLegacyRFCreateInjection(t *testing.T) {
	required := require.New(t)
	createNamespace(required, "legacy-rf-create")
	cluster := createMinimalClusterObj("legacy-rf-create", "legacy-rf-create")
	cluster.Spec.Cassandra.AdditionalSeeds = []string{"192.0.2.10"}
	required.NoError(k8sClient.Create(ctx, cluster))
	required.Equal(LegacyRFDiscoveryMarkerVersion, cluster.Annotations[LegacyRFDiscoveryMarkerAnnotation])
}

func testLegacyRFForgedMarker(t *testing.T) {
	required := require.New(t)
	createNamespace(required, "legacy-rf-forged")
	cluster := createMinimalClusterObj("legacy-rf-forged", "legacy-rf-forged")
	cluster.Annotations = map[string]string{LegacyRFDiscoveryMarkerAnnotation: LegacyRFDiscoveryMarkerVersion}
	err := k8sClient.Create(ctx, cluster)
	required.Error(err)
	required.Contains(err.Error(), "reserved discovery marker")
}

func testLegacyRFMarkerImmutability(t *testing.T) {
	required := require.New(t)
	createNamespace(required, "legacy-rf-immutable")
	cluster := createMinimalClusterObj("legacy-rf-immutable", "legacy-rf-immutable")
	required.NoError(k8sClient.Create(ctx, cluster))

	delete(cluster.Annotations, LegacyRFDiscoveryMarkerAnnotation)
	err := k8sClient.Update(ctx, cluster)
	required.Error(err)
	required.Contains(err.Error(), "discovery marker is immutable")

	required.NoError(k8sClient.Get(ctx, client.ObjectKeyFromObject(cluster), cluster))
	cluster.Annotations[LegacyRFDiscoveryMarkerAnnotation] = "forged-version"
	err = k8sClient.Update(ctx, cluster)
	required.Error(err)
	required.Contains(err.Error(), "discovery marker is immutable")
}

func testLegacyRFNoUpdateInjection(t *testing.T, cluster *K8ssandraCluster) {
	required := require.New(t)
	cluster.Labels = labels.Merge(cluster.Labels, map[string]string{"updated": "true"})
	required.NoError(k8sClient.Update(ctx, cluster))
	required.NoError(k8sClient.Get(ctx, client.ObjectKeyFromObject(cluster), cluster))
	required.NotContains(cluster.Annotations, LegacyRFDiscoveryMarkerAnnotation)

	metav1.SetMetaDataAnnotation(&cluster.ObjectMeta, LegacyRFDiscoveryMarkerAnnotation, LegacyRFDiscoveryMarkerVersion)
	err := k8sClient.Update(ctx, cluster)
	required.Error(err)
	required.Contains(err.Error(), "discovery marker is immutable")
}

func testLegacyRFLocalValidation(t *testing.T) {
	tests := []struct {
		name      string
		configure func(*K8ssandraCluster)
		wantError string
	}{
		{name: "valid Cassandra IPv4", configure: func(cluster *K8ssandraCluster) {
			cluster.Spec.Cassandra.AdditionalSeeds = []string{"192.0.2.11"}
		}},
		{name: "valid Cassandra IPv6", configure: func(cluster *K8ssandraCluster) {
			cluster.Spec.Cassandra.AdditionalSeeds = []string{"2001:db8::11"}
		}},
		{name: "FQDN seed", configure: func(cluster *K8ssandraCluster) {
			cluster.Spec.Cassandra.AdditionalSeeds = []string{"seed.example.test"}
		}, wantError: "IP literal"},
		{name: "seed with port", configure: func(cluster *K8ssandraCluster) {
			cluster.Spec.Cassandra.AdditionalSeeds = []string{"192.0.2.11:9042"}
		}, wantError: "IP literal"},
		{name: "empty credential reference", configure: func(cluster *K8ssandraCluster) {
			cluster.Spec.Cassandra.AdditionalSeeds = []string{"192.0.2.11"}
			cluster.Spec.Cassandra.LegacyCqlCredentialsSecretRef = &corev1.LocalObjectReference{}
		}, wantError: "legacy CQL credential Secret name"},
		{name: "invalid credential reference", configure: func(cluster *K8ssandraCluster) {
			cluster.Spec.Cassandra.AdditionalSeeds = []string{"192.0.2.11"}
			cluster.Spec.Cassandra.LegacyCqlCredentialsSecretRef = &corev1.LocalObjectReference{Name: "Bad_Name"}
		}, wantError: "legacy CQL credential Secret name"},
		{name: "valid TLS reference without Secret lookup", configure: func(cluster *K8ssandraCluster) {
			cluster.Spec.Cassandra.AdditionalSeeds = []string{"192.0.2.11"}
			cluster.Spec.Cassandra.LegacyCqlTLSSecretRef = &corev1.LocalObjectReference{Name: "legacy-cql-tls"}
		}},
		{name: "empty TLS reference", configure: func(cluster *K8ssandraCluster) {
			cluster.Spec.Cassandra.AdditionalSeeds = []string{"192.0.2.11"}
			cluster.Spec.Cassandra.LegacyCqlTLSSecretRef = &corev1.LocalObjectReference{}
		}, wantError: "legacy CQL TLS Secret name"},
		{name: "invalid TLS reference", configure: func(cluster *K8ssandraCluster) {
			cluster.Spec.Cassandra.AdditionalSeeds = []string{"192.0.2.11"}
			cluster.Spec.Cassandra.LegacyCqlTLSSecretRef = &corev1.LocalObjectReference{Name: "Bad_Name"}
		}, wantError: "legacy CQL TLS Secret name"},
		{name: "external secrets provider", configure: func(cluster *K8ssandraCluster) {
			cluster.Spec.Cassandra.AdditionalSeeds = []string{"192.0.2.11"}
			cluster.Spec.SecretsProvider = "external"
		}, wantError: "internal Secrets provider"},
		{name: "DSE server retains existing seed behavior", configure: func(cluster *K8ssandraCluster) {
			cluster.Spec.Cassandra.AdditionalSeeds = []string{"seed.example.test"}
			cluster.Spec.Cassandra.ServerType = ServerDistributionDse
		}},
		{name: "HCD server retains existing seed behavior", configure: func(cluster *K8ssandraCluster) {
			cluster.Spec.Cassandra.AdditionalSeeds = []string{"seed.example.test"}
			cluster.Spec.Cassandra.ServerType = ServerDistributionHcd
		}},
	}
	for index, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			required := require.New(t)
			namespace := fmt.Sprintf("legacy-rf-local-%d", index)
			createNamespace(required, namespace)
			cluster := createMinimalClusterObj(namespace, namespace)
			test.configure(cluster)
			err := k8sClient.Create(ctx, cluster)
			if test.wantError == "" {
				required.NoError(err)
				return
			}
			required.Error(err)
			required.Contains(err.Error(), test.wantError)
		})
	}
}

func TestValidateLegacyRFDiscoveryNonCassandraIsNotRejected(t *testing.T) {
	for _, serverType := range []ServerDistribution{ServerDistributionDse, ServerDistributionHcd} {
		t.Run(string(serverType), func(t *testing.T) {
			cluster := createMinimalClusterObj("unsupported", "unsupported")
			cluster.Spec.Cassandra.ServerType = serverType
			cluster.Spec.Cassandra.AdditionalSeeds = []string{"seed.example.test"}
			cluster.Spec.SecretsProvider = "external"

			require.NoError(t, validateLegacyRFDiscovery(cluster))
		})
	}
}

func testLegacyRFAdmissionUnavailable(t *testing.T) {
	required := require.New(t)
	namespace := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{
		Name: "legacy-rf-outage", Labels: map[string]string{"legacy-rf-admission": "outage"},
	}}
	required.NoError(k8sClient.Create(ctx, namespace))
	cluster := createMinimalClusterObj("legacy-rf-outage", namespace.Name)
	err := k8sClient.Create(ctx, cluster)
	required.Error(err)
	required.True(apierrors.IsInternalError(err) || apierrors.IsServiceUnavailable(err), err.Error())
}

func testLegacyRFNoSeedCompatibility(t *testing.T) {
	required := require.New(t)
	createNamespace(required, "legacy-rf-no-seed")
	cluster := createMinimalClusterObj("legacy-rf-no-seed", "legacy-rf-no-seed")
	cluster.Spec.SecretsProvider = "external"
	required.NoError(k8sClient.Create(ctx, cluster))
	required.Equal(LegacyRFDiscoveryMarkerVersion, cluster.Annotations[LegacyRFDiscoveryMarkerAnnotation])
}

func testContextValidation(t *testing.T) {
	required := require.New(t)
	createNamespace(required, "create-namespace")
	cluster := createMinimalClusterObj("create-test", "create-namespace")

	err := k8sClient.Create(ctx, cluster)
	required.NoError(err)

	// Verify incorrect K8sContext is not allowed
	cluster.Spec.Cassandra.Datacenters[0].K8sContext = "wrong"
	err = k8sClient.Update(ctx, cluster)
	required.Error(err)
}

func testNumTokensInUpdate(t *testing.T) {
	require := require.New(t)
	createNamespace(require, "numtokensupdate-namespace")
	cluster := createMinimalClusterObj("numtokens-test-update", "numtokensupdate-namespace")
	cluster.Spec.Cassandra.ServerVersion = "3.11.10"
	cluster.Spec.Cassandra.CassandraConfig = &CassandraConfig{}
	err := k8sClient.Create(ctx, cluster)
	require.NoError(err)

	// Now update to 4.1.3
	cluster.Spec.Cassandra.CassandraConfig.CassandraYaml = unstructured.Unstructured{"num_tokens": 256}
	cluster.Spec.Cassandra.ServerVersion = "4.1.8"

	// This should be acceptable change, since 3.11.10 defaulted to 256 and so it is the same value
	err = k8sClient.Update(ctx, cluster)
	require.NoError(err)
}

func testReaperKeyspaceValidation(t *testing.T) {
	required := require.New(t)
	createNamespace(required, "update-namespace")
	cluster := createMinimalClusterObj("update-test", "update-namespace")

	cluster.Spec.Reaper = &reaperapi.ReaperClusterTemplate{
		ReaperTemplate: reaperapi.ReaperTemplate{
			Keyspace: "original",
		},
	}

	err := k8sClient.Create(ctx, cluster)
	required.NoError(err)

	cluster.Spec.Reaper.Keyspace = "modified"
	err = k8sClient.Update(ctx, cluster)
	required.Error(err)
}

func testStorageConfigValidation(t *testing.T) {
	required := require.New(t)
	createNamespace(required, "storage-namespace")
	cluster := createMinimalClusterObj("storage-test", "storage-namespace")
	cluster.Spec.Cassandra.ServerVersion = "3.11.10"

	cluster.Spec.Cassandra.StorageConfig = nil
	err := k8sClient.Create(ctx, cluster)
	required.Error(err)

	cluster.Spec.Cassandra.StorageConfig = &v1beta1.StorageConfig{}
	err = k8sClient.Create(ctx, cluster)
	required.NoError(err)

	cluster.Spec.Cassandra.StorageConfig = nil
	cluster.Spec.Cassandra.Datacenters[0].StorageConfig = &v1beta1.StorageConfig{}
	err = k8sClient.Update(ctx, cluster)
	required.NoError(err)

	cluster.Spec.Cassandra.Datacenters = append(cluster.Spec.Cassandra.Datacenters, CassandraDatacenterTemplate{
		Meta:       EmbeddedObjectMeta{Name: "dc2"},
		K8sContext: "envtest",
		Size:       1,
	})

	err = k8sClient.Update(ctx, cluster)
	required.Error(err)

	cluster.Spec.Cassandra.Datacenters[1].StorageConfig = &v1beta1.StorageConfig{}
	err = k8sClient.Update(ctx, cluster)
	required.NoError(err)
}

func testNumTokens(t *testing.T) {
	required := require.New(t)

	createNamespace(required, "numtokens-namespace")
	cluster := createMinimalClusterObj("numtokens-test", "numtokens-namespace")

	// Create without token definition
	cluster.Spec.Cassandra.CassandraConfig = &CassandraConfig{}
	err := k8sClient.Create(ctx, cluster)
	required.NoError(err)

	tokens := 256
	cluster.Spec.Cassandra.CassandraConfig.CassandraYaml = unstructured.Unstructured{"num_tokens": tokens}
	err = k8sClient.Update(ctx, cluster)
	required.Error(err)

	err = k8sClient.Delete(context.TODO(), cluster)
	required.NoError(err)

	// Recreate with tokens
	cluster.ResourceVersion = ""
	delete(cluster.Annotations, LegacyRFDiscoveryMarkerAnnotation)
	err = k8sClient.Create(ctx, cluster)
	required.NoError(err)

	newTokens := 16
	cluster.Spec.Cassandra.CassandraConfig.CassandraYaml["num_tokens"] = newTokens
	err = k8sClient.Update(ctx, cluster)
	required.Error(err)

	delete(cluster.Spec.Cassandra.CassandraConfig.CassandraYaml, "num_tokens")
	err = k8sClient.Update(ctx, cluster)
	required.Error(err)

	// Num_token update validations
	tokens = 11
	newTokens = 22

	oldCluster := createClusterObjWithCassandraConfig("numtokens-test-1", "numtokens-namespace-1")
	newCluster := createClusterObjWithCassandraConfig("numtokens-test-2", "numtokens-namespace-2")

	delete(oldCluster.Spec.Cassandra.CassandraConfig.CassandraYaml, "num_tokens")
	newCluster.Spec.Cassandra.CassandraConfig.CassandraYaml["num_tokens"] = newTokens

	var oldCassConfig = oldCluster.Spec.Cassandra.CassandraConfig
	var newCassConfig = newCluster.Spec.Cassandra.CassandraConfig

	validator := K8ssandraClusterCustomValidator{}

	// Handle new num_token value different from previously specified as nil
	required.NotEqual(oldCassConfig.CassandraYaml["num_tokens"], newCassConfig.CassandraYaml["num_tokens"])
	var _, errorWhenNew = validator.ValidateUpdate(context.TODO(), oldCluster, newCluster)
	required.Error(errorWhenNew, "expected error having new num_token value different from previous specified as nil")

	oldCluster.Spec.Cassandra.CassandraConfig.CassandraYaml["num_tokens"] = tokens
	newCluster.Spec.Cassandra.CassandraConfig.CassandraYaml["num_tokens"] = newTokens

	oldCassConfig = oldCluster.Spec.Cassandra.CassandraConfig
	newCassConfig = newCluster.Spec.Cassandra.CassandraConfig

	// Handle new num_token value different from previously specified as an actual value
	required.NotEqual(oldCassConfig.CassandraYaml["num_tokens"], newCassConfig.CassandraYaml["num_tokens"])
	_, errorWhenNew = validator.ValidateUpdate(context.TODO(), oldCluster, newCluster)
	required.Error(errorWhenNew, "expected error having new num_token value different from previous specified")

	// Handle new num_token not specified when previously specified
	oldCassConfig.CassandraYaml["num_tokens"] = tokens
	delete(newCassConfig.CassandraYaml, "num_tokens")

	var _, errorWhenNil = validator.ValidateUpdate(context.TODO(), oldCluster, newCluster)
	required.Error(errorWhenNil, "expected error having new num_token value as nil from previous specified")

	oldCassConfig.CassandraYaml["num_tokens"] = tokens
	newCassConfig.CassandraYaml = unstructured.Unstructured{}

	_, errorWhenNil = validator.ValidateUpdate(context.TODO(), oldCluster, newCluster)
	required.Error(errorWhenNil, "expected error having new num_token value as nil from previous specified")

	oldCassConfig.CassandraYaml["num_tokens"] = tokens
	newCassConfig = &CassandraConfig{}
	_, errorWhenNil = validator.ValidateUpdate(context.TODO(), oldCluster, newCluster)
	required.Error(errorWhenNil, "expected error having new num_token value as nil from previous specified")

	oldCassConfig.CassandraYaml["num_tokens"] = tokens
	newCassConfig = &CassandraConfig{}
	_, errorWhenNil = validator.ValidateUpdate(context.TODO(), oldCluster, newCluster)
	required.Error(errorWhenNil, "expected error having new num_token value as nil from previous specified")

	// Expected to be able to update without token change, however changes to other config values are made
	sameNumTokens := 8675309
	diffNumTokens := 42
	intervalInMins := 9035768
	enabled := true

	oldCluster.Spec.Cassandra.CassandraConfig.CassandraYaml["num_tokens"] = sameNumTokens
	newCluster.Spec.Cassandra.CassandraConfig.CassandraYaml["num_tokens"] = sameNumTokens
	newCluster.Spec.Cassandra.CassandraConfig.CassandraYaml["cdc_enabled"] = enabled
	newCluster.Spec.Cassandra.CassandraConfig.CassandraYaml["index_summary_resize_interval_in_minutes"] = intervalInMins

	_, errorOnValidate := validator.ValidateUpdate(context.TODO(), oldCluster, newCluster)
	required.NoError(errorOnValidate)

	// Expected failure for validation with token change while changes to other config values are being made
	oldCluster.Spec.Cassandra.CassandraConfig.CassandraYaml["num_tokens"] = sameNumTokens
	newCluster.Spec.Cassandra.CassandraConfig.CassandraYaml["num_tokens"] = diffNumTokens
	newCluster.Spec.Cassandra.CassandraConfig.CassandraYaml["cdc_enabled"] = enabled
	newCluster.Spec.Cassandra.CassandraConfig.CassandraYaml["index_summary_resize_interval_in_minutes"] = intervalInMins

	_, errorOnValidate = validator.ValidateUpdate(context.TODO(), oldCluster, newCluster)
	required.Error(errorOnValidate, "expected error when changing the value of num tokens while also changing other field values")
}

func testStsNameTooLong(t *testing.T) {
	required := require.New(t)
	createNamespace(required, "too-long-namespace")
	cluster := createMinimalClusterObj("create-very-long-cluster-name-which-will-overflow-our-limit", "too-long-namespace")

	err := k8sClient.Create(ctx, cluster)
	required.Error(err)
}

func createNamespace(require *require.Assertions, namespace string) {
	ns := &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name: namespace,
		},
	}
	err := k8sClient.Create(ctx, ns)
	require.NoError(err)
}

func createClusterObjWithCassandraConfig(name, namespace string) *K8ssandraCluster {
	return &K8ssandraCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Spec: K8ssandraClusterSpec{
			Cassandra: &CassandraClusterTemplate{
				DatacenterOptions: DatacenterOptions{
					CassandraConfig: &CassandraConfig{
						CassandraYaml: unstructured.Unstructured{"num_tokens": nil},
					},
					StorageConfig: &v1beta1.StorageConfig{},
				},

				Datacenters: []CassandraDatacenterTemplate{
					{
						Meta:       EmbeddedObjectMeta{Name: "dc1"},
						K8sContext: "envtest",
						Size:       1,
					},
				},
			},
		},
	}
}

func createMinimalClusterObj(name, namespace string) *K8ssandraCluster {
	return &K8ssandraCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Spec: K8ssandraClusterSpec{
			Cassandra: &CassandraClusterTemplate{
				DatacenterOptions: DatacenterOptions{
					StorageConfig: &v1beta1.StorageConfig{},
				},
				Datacenters: []CassandraDatacenterTemplate{
					{
						Meta: EmbeddedObjectMeta{
							Name: "dc1",
						},
						K8sContext: "envtest",
						Size:       1,
					},
				},
			},
		},
	}
}

func testMedusaPrefixMissing(t *testing.T) {
	required := require.New(t)
	createNamespace(required, "short-namespace")

	clusterWithoutMedusa := createMinimalClusterObj("without-medusa", "short-namespace")
	err := k8sClient.Create(ctx, clusterWithoutMedusa)
	required.NoError(err)

	clusterWithMedusa := createMinimalClusterObj("with-medusa", "short-namespace")
	clusterWithMedusa.Spec.Medusa = &medusaapi.MedusaClusterTemplate{
		StorageProperties: medusaapi.Storage{
			StorageProvider: "s3_compatible",
			BucketName:      "not-real",
			Prefix:          "",
		},
	}
	err = k8sClient.Create(ctx, clusterWithMedusa)
	required.NoError(err)

	clusterWithoutPrefix := createMinimalClusterObj("without-prefix", "short-namespace")
	clusterWithoutPrefix.Spec.Medusa = &medusaapi.MedusaClusterTemplate{
		MedusaConfigurationRef: corev1.ObjectReference{
			Name: "medusa-config",
		},
		StorageProperties: medusaapi.Storage{
			StorageProvider: "s3_compatible",
			BucketName:      "not-real",
			Prefix:          "",
		},
	}
	err = k8sClient.Create(ctx, clusterWithoutPrefix)
	required.Error(err)

	clusterWithPrefix := createMinimalClusterObj("with-prefix", "short-namespace")
	clusterWithPrefix.Spec.Medusa = &medusaapi.MedusaClusterTemplate{
		MedusaConfigurationRef: corev1.ObjectReference{
			Name: "medusa-config",
		},
		StorageProperties: medusaapi.Storage{
			StorageProvider: "s3_compatible",
			BucketName:      "not-real",
			Prefix:          "some-prefix",
		},
	}
	err = k8sClient.Create(ctx, clusterWithPrefix)
	required.NoError(err)
}

func testMedusaMandatoryFieldsMissing(t *testing.T) {
	required := require.New(t)
	createNamespace(required, "short-namespace-2")

	clusterWithoutMedusa := createMinimalClusterObj("without-medusa", "short-namespace-2")
	err := k8sClient.Create(ctx, clusterWithoutMedusa)
	required.NoError(err)

	clusterWithMedusa := createMinimalClusterObj("with-medusa", "short-namespace-2")
	clusterWithMedusa.Spec.Medusa = &medusaapi.MedusaClusterTemplate{
		StorageProperties: medusaapi.Storage{
			Prefix: "",
		},
	}
	err = k8sClient.Create(ctx, clusterWithMedusa)
	// We expect an error because the storage provider and bucket name are not set
	required.Error(err)

	clusterWithMedusa2 := createMinimalClusterObj("with-medusa", "short-namespace-2")
	clusterWithMedusa2.Spec.Medusa = &medusaapi.MedusaClusterTemplate{
		StorageProperties: medusaapi.Storage{
			StorageProvider: "s3_compatible",
			Prefix:          "test",
		},
	}
	err = k8sClient.Create(ctx, clusterWithMedusa2)
	// We expect an error because the bucket name is not set
	required.Error(err)

	clusterWithMedusa3 := createMinimalClusterObj("with-medusa", "short-namespace-2")
	clusterWithMedusa3.Spec.Medusa = &medusaapi.MedusaClusterTemplate{
		StorageProperties: medusaapi.Storage{
			BucketName: "not-real",
			Prefix:     "test",
		},
	}
	err = k8sClient.Create(ctx, clusterWithMedusa3)
	// We expect an error because the bucket name is not set
	required.Error(err)
}

func testInvalidDcName(t *testing.T) {
	required := require.New(t)
	createNamespace(required, "ns")

	clusterWithBadDcName := createMinimalClusterObj("bad-dc-name", "ns")
	clusterWithBadDcName.Spec.Cassandra.Datacenters[0].Meta.Name = "DC1"
	err := k8sClient.Create(ctx, clusterWithBadDcName)
	required.Error(err)
	required.Contains(err.Error(), "invalid DC name")
}

func testMedusaNonLocalNamespace(t *testing.T) {
	required := require.New(t)
	badCluster := createMinimalClusterObj("medusaconfig-nonlocal", "ns")
	badCluster.Spec.Medusa = &medusaapi.MedusaClusterTemplate{
		MedusaConfigurationRef: corev1.ObjectReference{
			Namespace: "nonlocal-ns",
			Name:      "medusa-config",
		},
		StorageProperties: medusaapi.Storage{
			StorageProvider: "s3_compatible",
			BucketName:      "not-real",
			Prefix:          "some-prefix",
		},
	}
	err := validateK8ssandraCluster(badCluster)
	required.Error(err)
	required.Contains(err.Error(), "Medusa config must be namespace local")
}

func testMedusaConfigRefAndStorageSecretRefMutuallyExclusive(t *testing.T) {
	required := require.New(t)

	// Both MedusaConfigurationRef and StorageSecretRef set — should be rejected.
	clusterWithBoth := createMinimalClusterObj("medusa-both-refs", "ns")
	clusterWithBoth.Spec.Medusa = &medusaapi.MedusaClusterTemplate{
		MedusaConfigurationRef: corev1.ObjectReference{
			Name: "medusa-config",
		},
		StorageProperties: medusaapi.Storage{
			Prefix: "some-prefix",
			StorageSecretRef: corev1.LocalObjectReference{
				Name: "storage-secret",
			},
		},
	}
	err := validateK8ssandraCluster(clusterWithBoth)
	required.Error(err)
	required.Contains(err.Error(), "Both Medusa Configuration Reference and Storage Secret Reference cannot be specified at same time")

	// MedusaConfigurationRef set without StorageSecretRef — should be accepted.
	clusterWithConfigRefOnly := createMinimalClusterObj("medusa-config-ref-only", "ns")
	clusterWithConfigRefOnly.Spec.Medusa = &medusaapi.MedusaClusterTemplate{
		MedusaConfigurationRef: corev1.ObjectReference{
			Name: "medusa-config",
		},
		StorageProperties: medusaapi.Storage{
			Prefix: "some-prefix",
		},
	}
	err = validateK8ssandraCluster(clusterWithConfigRefOnly)
	required.NoError(err)
}

func testReaperStorage(t *testing.T) {
	required := require.New(t)

	reaperWithNoStorageConfig := createMinimalClusterObj("reaper-no-storage-config", "ns")
	reaperWithNoStorageConfig.Spec.Reaper = &reaperapi.ReaperClusterTemplate{
		ReaperTemplate: reaperapi.ReaperTemplate{
			StorageType: reaperapi.StorageTypeLocal,
		},
	}
	err := validateK8ssandraCluster(reaperWithNoStorageConfig)
	required.Error(err)

	reaperWithDefaultConfig := createClusterObjWithCassandraConfig("reaper-default-storage-config", "ns")
	reaperWithDefaultConfig.Spec.Reaper = minimalInMemoryReaperConfig.DeepCopy()
	err = validateK8ssandraCluster(reaperWithDefaultConfig)
	required.NoError(err)

	reaperWithoutAccessMode := createClusterObjWithCassandraConfig("reaper-no-access-mode", "ns")
	reaperWithoutAccessMode.Spec.Reaper = minimalInMemoryReaperConfig.DeepCopy()
	reaperWithoutAccessMode.Spec.Reaper.StorageConfig.AccessModes = nil
	err = validateK8ssandraCluster(reaperWithoutAccessMode)
	required.Error(err)

	reaperWithoutStorageSize := createClusterObjWithCassandraConfig("reaper-no-storage-size", "ns")
	reaperWithoutStorageSize.Spec.Reaper = minimalInMemoryReaperConfig.DeepCopy()
	reaperWithoutStorageSize.Spec.Reaper.StorageConfig.Resources.Requests = corev1.ResourceList{}
	err = validateK8ssandraCluster(reaperWithoutStorageSize)
	required.Error(err)

	kc := createClusterObjWithCassandraConfig("kc-with-per-dc-reaper-and-local-storage", "ns")
	kc.Spec.Reaper = minimalInMemoryReaperConfig.DeepCopy()
	kc.Spec.Reaper.DeploymentMode = reaperapi.DeploymentModePerDc
	err = validateK8ssandraCluster(kc)
	required.Equal(ErrNoReaperPerDcWithLocal, err)
}

// TestValidateUpdateNumTokens is a unit test for numTokens updates.
func TestValidateUpdateNumTokens(t *testing.T) {
	type config struct {
		globalVersion   string
		dcVersion       string
		globalNumTokens int
		dcNumTokens     int
	}
	type testCase struct {
		oldConfig config
		newConfig config
		expected  error
	}
	testCases := []testCase{
		{
			oldConfig: config{globalVersion: "3.11.10"},
			newConfig: config{globalVersion: "4.1.3"},
			expected:  ErrNumTokens,
		},
		{
			oldConfig: config{globalVersion: "3.11.10"},
			newConfig: config{globalNumTokens: 256},
			expected:  nil,
		},
		{
			oldConfig: config{globalVersion: "3.11.10"},
			newConfig: config{dcNumTokens: 256},
			expected:  nil,
		},
		{
			oldConfig: config{dcVersion: "3.11.10"},
			newConfig: config{globalNumTokens: 256},
			expected:  nil,
		},
		{
			oldConfig: config{dcVersion: "3.11.10"},
			newConfig: config{dcNumTokens: 256},
			expected:  nil,
		},
		{
			oldConfig: config{dcNumTokens: 10},
			newConfig: config{dcNumTokens: 10},
			expected:  nil,
		},
		{
			oldConfig: config{globalNumTokens: 10},
			newConfig: config{dcNumTokens: 10},
			expected:  nil,
		},
	}
	toCassandra := func(c config) *CassandraClusterTemplate {
		cassandra := &CassandraClusterTemplate{ServerType: ServerDistributionCassandra}
		options := DatacenterOptions{ServerVersion: c.globalVersion}
		if c.globalNumTokens != 0 {
			options.CassandraConfig = &CassandraConfig{
				CassandraYaml: unstructured.Unstructured{"num_tokens": float64(c.globalNumTokens)}}
		}
		cassandra.DatacenterOptions = options
		dc := CassandraDatacenterTemplate{
			Meta: EmbeddedObjectMeta{Name: "dc1"},
		}
		dcOptions := DatacenterOptions{ServerVersion: c.dcVersion}
		if c.dcNumTokens != 0 {
			dcOptions.CassandraConfig = &CassandraConfig{
				CassandraYaml: unstructured.Unstructured{"num_tokens": float64(c.dcNumTokens)}}
		}
		dc.DatacenterOptions = dcOptions
		cassandra.Datacenters = []CassandraDatacenterTemplate{dc}
		return cassandra
	}
	for _, testCase := range testCases {
		oldCassandra := toCassandra(testCase.oldConfig)
		newCassandra := toCassandra(testCase.newConfig)
		err := validateUpdateNumTokens(oldCassandra, newCassandra)
		if testCase.expected == nil {
			require.NoError(t, err)
		} else {
			require.Error(t, err)
			require.Equal(t, testCase.expected.Error(), err.Error())
		}
	}
}

func testAutomatedUpdateAnnotation(t *testing.T) {
	require := require.New(t)
	createNamespace(require, "automated-update-namespace")
	cluster := createMinimalClusterObj("automated-update-test", "automated-update-namespace")
	require.NoError(validateK8ssandraCluster(cluster))

	// Test should accept values once and always
	metav1.SetMetaDataAnnotation(&cluster.ObjectMeta, AutomatedUpdateAnnotation, string(AllowUpdateOnce))
	require.NoError(validateK8ssandraCluster(cluster))

	metav1.SetMetaDataAnnotation(&cluster.ObjectMeta, AutomatedUpdateAnnotation, string(AllowUpdateAlways))
	require.NoError(validateK8ssandraCluster(cluster))

	cluster.Annotations[AutomatedUpdateAnnotation] = string("true")
	require.Error(validateK8ssandraCluster(cluster))
}

func testNoDCRename(t *testing.T) {
	kcOld := createClusterObjWithCassandraConfig("testcluster", "testns")
	kcNew := kcOld.DeepCopy()
	kcNew.Spec.Cassandra.Datacenters[0].Meta.Name = "newdc1name"
	validator := K8ssandraClusterCustomValidator{}
	_, err := validator.ValidateUpdate(context.TODO(), kcOld, kcNew)
	require.Error(t, err)
}
