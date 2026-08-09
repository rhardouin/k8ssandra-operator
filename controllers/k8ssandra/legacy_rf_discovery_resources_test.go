package k8ssandra

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/netip"
	"strings"
	"testing"

	api "github.com/k8ssandra/k8ssandra-operator/apis/k8ssandra/v1alpha1"
	"github.com/k8ssandra/k8ssandra-operator/pkg/discovery"
	k8ssandralabels "github.com/k8ssandra/k8ssandra-operator/pkg/labels"
	"github.com/stretchr/testify/require"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestBuildLegacyRFAttemptResourcesIsBoundedAndLeastPrivilege(t *testing.T) {
	attempt := resourceTestAttempt(t)
	key := bytes.Repeat([]byte{7}, legacyRFHMACKeyBytes)
	resources, err := BuildLegacyRFAttemptResources(LegacyRFAttemptResourcesInput{
		ClusterKey: types.NamespacedName{Namespace: "control", Name: "migration"},
		Attempt:    attempt,
		Location:   api.LegacyRFManagedLocation{K8sContext: "data", Namespace: "data-ns", Name: "dc1"},
		HMACKey:    key,
		CopiedSecretData: map[string][]byte{
			legacyRFAuthUsernameKey: []byte("user-canary"),
			legacyRFAuthPasswordKey: []byte("password-canary"),
			legacyRFTLSCAKey:        []byte("certificate-canary"),
		},
	})
	require.NoError(t, err)
	require.Equal(t, "data-ns", resources.Job.Namespace)
	require.Equal(t, resources.ResultConfigMap.Name, resources.Role.Rules[0].ResourceNames[0])
	require.Equal(t, []string{"get", "patch"}, resources.Role.Rules[0].Verbs)
	require.Equal(t, resources.ServiceAccount.Name, resources.Job.Spec.Template.Spec.ServiceAccountName)
	require.True(t, *resources.Job.Spec.Template.Spec.AutomountServiceAccountToken)
	require.EqualValues(t, 0, *resources.Job.Spec.BackoffLimit)
	require.EqualValues(t, legacyRFJobDeadlineSeconds, *resources.Job.Spec.ActiveDeadlineSeconds)
	require.EqualValues(t, legacyRFJobTTLSeconds, *resources.Job.Spec.TTLSecondsAfterFinished)
	require.Equal(t, corev1.RestartPolicyNever, resources.Job.Spec.Template.Spec.RestartPolicy)

	container := resources.Job.Spec.Template.Spec.Containers[0]
	require.Equal(t, []string{"/legacy-rf-discovery"}, container.Command)
	require.Equal(t, attempt.WorkerImageDigest, container.Image)
	require.Contains(t, container.Args, "--result-configmap")
	require.Contains(t, container.Args, resources.ResultConfigMap.Name)
	require.True(t, *container.SecurityContext.RunAsNonRoot)
	require.False(t, *container.SecurityContext.AllowPrivilegeEscalation)
	require.True(t, *container.SecurityContext.ReadOnlyRootFilesystem)
	require.Equal(t, []corev1.Capability{"ALL"}, container.SecurityContext.Capabilities.Drop)
	require.Equal(t, "100m", container.Resources.Requests.Cpu().String())
	require.Equal(t, "64Mi", container.Resources.Requests.Memory().String())
	require.Equal(t, "500m", container.Resources.Limits.Cpu().String())
	require.Equal(t, "256Mi", container.Resources.Limits.Memory().String())
	require.Equal(t, corev1.SeccompProfileTypeRuntimeDefault, resources.Job.Spec.Template.Spec.SecurityContext.SeccompProfile.Type)
	require.EqualValues(t, 65532, *resources.Job.Spec.Template.Spec.SecurityContext.FSGroup)
	for _, volume := range resources.Job.Spec.Template.Spec.Volumes {
		if volume.Secret != nil {
			require.EqualValues(t, 0o440, *volume.Secret.DefaultMode)
		}
	}

	var decoded discovery.Attempt
	require.NoError(t, json.Unmarshal([]byte(resources.AttemptConfigMap.Data[legacyRFAttemptDataKey]), &decoded))
	require.Equal(t, attempt.OrderedSeeds, decoded.OrderedSeeds)
	require.Equal(t, attempt.SeedDigest, decoded.SeedDigest)
	require.Equal(t, attempt.ClusterUID, resources.Job.Annotations[legacyRFClusterUIDAnnotation])
	require.Equal(t, attempt.AttemptID, resources.Job.Annotations[legacyRFAttemptIDAnnotation])
	require.Equal(t, key, resources.HMACSecret.Data[legacyRFHMACDataKey])
	require.Equal(t, []byte("password-canary"), resources.CopiedSecret.Data[legacyRFAuthPasswordKey])
	require.Equal(t, corev1.SecretTypeOpaque, resources.HMACSecret.Type)
	require.Equal(t, corev1.SecretTypeOpaque, resources.CopiedSecret.Type)

	for _, object := range resources.nonSecretObjects() {
		body, marshalErr := json.Marshal(object)
		require.NoError(t, marshalErr)
		require.NotContains(t, string(body), "password-canary")
		require.NotContains(t, string(body), "certificate-canary")
		require.NotContains(t, string(body), string(key))
		require.Empty(t, object.GetOwnerReferences(), "cross-cluster owner references are invalid")
	}
}

func TestBuildLegacyRFAttemptResourcesRejectsUnresolvableImageAndInvalidKey(t *testing.T) {
	attempt := resourceTestAttempt(t)
	tests := []struct {
		name   string
		mutate func(*discovery.Attempt, *[]byte)
	}{
		{name: "malformed digest image", mutate: func(attempt *discovery.Attempt, _ *[]byte) {
			attempt.WorkerImageDigest = "registry.example/operator@sha256:abc"
		}},
		{name: "short HMAC key", mutate: func(_ *discovery.Attempt, key *[]byte) { *key = []byte("short") }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			key := bytes.Repeat([]byte{1}, legacyRFHMACKeyBytes)
			test.mutate(&attempt, &key)
			_, err := BuildLegacyRFAttemptResources(LegacyRFAttemptResourcesInput{ClusterKey: types.NamespacedName{Namespace: "ns", Name: "cluster"}, Attempt: attempt, Location: api.LegacyRFManagedLocation{Namespace: "ns", Name: "dc"}, HMACKey: key})
			require.Error(t, err)
		})
	}
}

func TestBuildLegacyRFAttemptResourcesEnforcesSeedLimitBeforeJobCreation(t *testing.T) {
	attempt := resourceTestAttempt(t)
	seedStrings := make([]string, api.LegacyRFDiscoveryMaxSeeds+1)
	for index := range seedStrings {
		seedStrings[index] = fmt.Sprintf("192.0.2.%d", index+1)
	}
	seeds, digest, err := discovery.CanonicalizeSeeds(seedStrings)
	require.NoError(t, err)
	attempt.OrderedSeeds, attempt.SeedDigest = seeds, digest
	input := LegacyRFAttemptResourcesInput{
		ClusterKey: types.NamespacedName{Namespace: "control", Name: "migration"}, Attempt: attempt,
		Location: api.LegacyRFManagedLocation{Namespace: "data", Name: "dc1"},
		HMACKey:  bytes.Repeat([]byte{1}, legacyRFHMACKeyBytes),
	}

	_, err = BuildLegacyRFAttemptResources(input)
	require.ErrorContains(t, err, "at most 32")

	input.Attempt.OrderedSeeds = seeds[:api.LegacyRFDiscoveryMaxSeeds]
	input.Attempt.SeedDigest, err = discovery.SeedDigest(input.Attempt.OrderedSeeds)
	require.NoError(t, err)
	resources, err := BuildLegacyRFAttemptResources(input)
	require.NoError(t, err)
	require.NotNil(t, resources.Job)
}

func TestBuildLegacyRFAttemptResourcesNormalizesEmptyCopiedSecretData(t *testing.T) {
	attempt := resourceTestAttempt(t)
	attempt.Connection.SecretBindings = nil
	resources, err := BuildLegacyRFAttemptResources(LegacyRFAttemptResourcesInput{
		ClusterKey: types.NamespacedName{Namespace: "control", Name: "migration"},
		Attempt:    attempt, Location: api.LegacyRFManagedLocation{Namespace: "data-ns", Name: "dc1"},
		HMACKey: bytes.Repeat([]byte{7}, legacyRFHMACKeyBytes), CopiedSecretData: map[string][]byte{},
	})
	require.NoError(t, err)
	require.Nil(t, resources.CopiedSecret.Data)
}

func resourceTestAttempt(t *testing.T) discovery.Attempt {
	t.Helper()
	seeds := []netip.AddrPort{netip.MustParseAddrPort("192.0.2.2:9042"), netip.MustParseAddrPort("192.0.2.1:9042")}
	_, digest, err := discovery.CanonicalizeSeeds([]string{"192.0.2.2", "192.0.2.1"})
	require.NoError(t, err)
	return discovery.Attempt{
		ClusterUID: "uid-1", Generation: 7, MarkerVersion: api.LegacyRFDiscoveryMarkerVersion,
		ProtocolVersion: api.LegacyRFDiscoveryProtocolVersion, AttemptID: "attempt-1",
		OrderedSeeds: seeds, SeedDigest: digest,
		WorkerImageDigest: "registry.example/operator@sha256:" + strings.Repeat("a", 64),
		Connection: discovery.Connection{ExpectedClusterName: "legacy", SecretBindings: []discovery.SecretBinding{
			{Purpose: "auth", SourceContext: "source", Namespace: "source-ns", Name: "auth", Keys: []string{"username", "password"}, ResourceVersion: "11"},
			{Purpose: "tls", SourceContext: "source", Namespace: "source-ns", Name: "tls", Keys: []string{"ca.crt"}, ResourceVersion: "12"},
		}},
	}
}

func TestBuildLegacyRFAttemptResourcesCarriesImagePullSecrets(t *testing.T) {
	pullSecrets := []corev1.LocalObjectReference{{Name: "registry-a"}, {Name: "registry-b"}}
	resources, err := BuildLegacyRFAttemptResources(LegacyRFAttemptResourcesInput{
		ClusterKey: types.NamespacedName{Namespace: "control", Name: "migration"},
		Attempt:    resourceTestAttempt(t),
		Location:   api.LegacyRFManagedLocation{Namespace: "data-ns", Name: "dc1"},
		HMACKey:    bytes.Repeat([]byte{7}, legacyRFHMACKeyBytes),
		// A private registry needs these on the worker Pod: it runs under a dedicated
		// ServiceAccount, so it inherits nothing from the manager Deployment.
		ImagePullSecrets: pullSecrets,
	})

	require.NoError(t, err)
	require.Equal(t, pullSecrets, resources.Job.Spec.Template.Spec.ImagePullSecrets)

	withoutSecrets, err := BuildLegacyRFAttemptResources(LegacyRFAttemptResourcesInput{
		ClusterKey: types.NamespacedName{Namespace: "control", Name: "migration"},
		Attempt:    resourceTestAttempt(t),
		Location:   api.LegacyRFManagedLocation{Namespace: "data-ns", Name: "dc1"},
		HMACKey:    bytes.Repeat([]byte{7}, legacyRFHMACKeyBytes),
	})

	require.NoError(t, err)
	require.Empty(t, withoutSecrets.Job.Spec.Template.Spec.ImagePullSecrets)
}

func TestPurgeLegacyRFAttemptResourcesRemovesOnlyCorrelatedAttemptObjects(t *testing.T) {
	clusterKey := types.NamespacedName{Namespace: "control", Name: "migration"}
	attemptLabels := k8ssandralabels.WatchedByK8ssandraClusterLabels(clusterKey)
	attemptLabels[legacyRFComponentLabel] = legacyRFComponentValue
	watchOnlyLabels := k8ssandralabels.WatchedByK8ssandraClusterLabels(clusterKey)
	otherClusterLabels := k8ssandralabels.WatchedByK8ssandraClusterLabels(
		types.NamespacedName{Namespace: "control", Name: "other"})
	otherClusterLabels[legacyRFComponentLabel] = legacyRFComponentValue

	meta := func(name string, labels map[string]string) metav1.ObjectMeta {
		return metav1.ObjectMeta{Name: name, Namespace: "data-ns", Labels: labels}
	}
	scheme := runtime.NewScheme()
	require.NoError(t, corev1.AddToScheme(scheme))
	require.NoError(t, batchv1.AddToScheme(scheme))
	require.NoError(t, rbacv1.AddToScheme(scheme))
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(
		&batchv1.Job{ObjectMeta: meta("attempt-job", attemptLabels)},
		&corev1.Secret{ObjectMeta: meta("attempt-source", attemptLabels)},
		&corev1.ConfigMap{ObjectMeta: meta("attempt-result", attemptLabels)},
		&corev1.ServiceAccount{ObjectMeta: meta("attempt-worker", attemptLabels)},
		&rbacv1.Role{ObjectMeta: meta("attempt-role", attemptLabels)},
		&rbacv1.RoleBinding{ObjectMeta: meta("attempt-writer", attemptLabels)},
		// Replicated Secrets carry the watch labels without the component label. Selecting on
		// the watch labels alone would delete the cluster's superuser credentials.
		&corev1.Secret{ObjectMeta: meta("superuser", watchOnlyLabels)},
		&corev1.Secret{ObjectMeta: meta("other-cluster-source", otherClusterLabels)},
		&corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "unrelated", Namespace: "data-ns"}},
	).Build()

	require.NoError(t, PurgeLegacyRFAttemptResources(context.Background(), fakeClient, "data-ns", clusterKey))

	for _, gone := range []client.Object{
		&batchv1.Job{}, &corev1.Secret{}, &corev1.ConfigMap{},
		&corev1.ServiceAccount{}, &rbacv1.Role{}, &rbacv1.RoleBinding{},
	} {
		names := map[string]string{
			"*v1.Job": "attempt-job", "*v1.Secret": "attempt-source", "*v1.ConfigMap": "attempt-result",
			"*v1.ServiceAccount": "attempt-worker", "*v1.Role": "attempt-role", "*v1.RoleBinding": "attempt-writer",
		}
		name := names[fmt.Sprintf("%T", gone)]
		err := fakeClient.Get(context.Background(), types.NamespacedName{Namespace: "data-ns", Name: name}, gone)
		require.True(t, k8serrors.IsNotFound(err), "%s %s should be deleted, got %v", fmt.Sprintf("%T", gone), name, err)
	}

	for _, kept := range []string{"superuser", "other-cluster-source", "unrelated"} {
		secret := &corev1.Secret{}
		require.NoError(t, fakeClient.Get(context.Background(),
			types.NamespacedName{Namespace: "data-ns", Name: kept}, secret), "%s must survive", kept)
	}
}
