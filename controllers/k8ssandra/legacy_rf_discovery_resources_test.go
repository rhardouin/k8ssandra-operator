package k8ssandra

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/netip"
	"strings"
	"testing"

	api "github.com/k8ssandra/k8ssandra-operator/apis/k8ssandra/v1alpha1"
	"github.com/k8ssandra/k8ssandra-operator/pkg/discovery"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
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
