package k8ssandra

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"

	api "github.com/k8ssandra/k8ssandra-operator/apis/k8ssandra/v1alpha1"
	"github.com/k8ssandra/k8ssandra-operator/pkg/discovery"
	k8ssandralabels "github.com/k8ssandra/k8ssandra-operator/pkg/labels"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	legacyRFAttemptDataKey        = "attempt.json"
	legacyRFResultDataKey         = "result.json"
	legacyRFHMACDataKey           = "hmac-key"
	legacyRFHMACKeyBytes          = 32
	legacyRFJobDeadlineSeconds    = int64(600)
	legacyRFJobTTLSeconds         = int32(600)
	legacyRFAuthUsernameKey       = "auth-username"
	legacyRFAuthPasswordKey       = "auth-password"
	legacyRFTLSCAKey              = "tls-ca.crt"
	legacyRFTLSCertificateKey     = "tls-tls.crt"
	legacyRFTLSPrivateKeyKey      = "tls-tls.key"
	legacyRFClusterUIDAnnotation  = "k8ssandra.io/legacy-rf-cluster-uid"
	legacyRFAttemptIDAnnotation   = "k8ssandra.io/legacy-rf-attempt-id"
	legacyRFGenerationAnnotation  = "k8ssandra.io/legacy-rf-generation"
	legacyRFSeedDigestAnnotation  = "k8ssandra.io/legacy-rf-seed-digest"
	legacyRFMarkerAnnotation      = "k8ssandra.io/legacy-rf-marker"
	legacyRFWorkerImageAnnotation = "k8ssandra.io/legacy-rf-worker-image"
)

// LegacyRFAttemptResourcesInput contains one immutable desired workload request.
type LegacyRFAttemptResourcesInput struct {
	ClusterKey       types.NamespacedName
	Attempt          discovery.Attempt
	Location         api.LegacyRFManagedLocation
	HMACKey          []byte
	CopiedSecretData map[string][]byte
}

// LegacyRFAttemptResources contains every namespaced object for one attempt.
type LegacyRFAttemptResources struct {
	AttemptConfigMap *corev1.ConfigMap
	ResultConfigMap  *corev1.ConfigMap
	HMACSecret       *corev1.Secret
	CopiedSecret     *corev1.Secret
	ServiceAccount   *corev1.ServiceAccount
	Role             *rbacv1.Role
	RoleBinding      *rbacv1.RoleBinding
	Job              *batchv1.Job
}

// BuildLegacyRFAttemptResources builds bounded, correlated, least-privilege attempt objects.
func BuildLegacyRFAttemptResources(input LegacyRFAttemptResourcesInput) (LegacyRFAttemptResources, error) {
	if err := validateAttemptResourceInput(input); err != nil {
		return LegacyRFAttemptResources{}, err
	}
	attemptJSON, err := json.Marshal(input.Attempt)
	if err != nil {
		return LegacyRFAttemptResources{}, fmt.Errorf("encode legacy RF attempt: %w", err)
	}
	metadata := newAttemptMetadata(input)
	resources := newAttemptSupportResources(input, metadata, attemptJSON)
	resources.Job = newAttemptJob(input, metadata, resources)
	return resources, nil
}

func validateAttemptResourceInput(input LegacyRFAttemptResourcesInput) error {
	if input.ClusterKey.Namespace == "" || input.ClusterKey.Name == "" ||
		input.Location.Namespace == "" || input.Attempt.AttemptID == "" || input.Attempt.ClusterUID == "" {
		return errors.New("build legacy RF attempt resources: cluster, location, and attempt identity are required")
	}
	if len(input.HMACKey) != legacyRFHMACKeyBytes {
		return fmt.Errorf("build legacy RF attempt resources: HMAC key must be %d bytes", legacyRFHMACKeyBytes)
	}
	if !resolvableImageReference(input.Attempt.WorkerImageDigest) {
		return errors.New("build legacy RF attempt resources: worker image must be a resolvable reference")
	}
	if len(input.Attempt.OrderedSeeds) == 0 || input.Attempt.SeedDigest == "" {
		return errors.New("build legacy RF attempt resources: ordered seed binding is required")
	}
	if len(input.Attempt.OrderedSeeds) > api.LegacyRFDiscoveryMaxSeeds {
		return fmt.Errorf("build legacy RF attempt resources: ordered seeds must contain at most %d entries", api.LegacyRFDiscoveryMaxSeeds)
	}
	return nil
}

// resolvableImageReference accepts the references the manager image resolver can produce: a
// digest-pinned reference, or the manager's own configured reference when the runtime reports
// no pullable digest. A digest, when present, must still be a well-formed SHA-256 reference.
func resolvableImageReference(image string) bool {
	if image == "" || strings.ContainsAny(image, " \t\r\n") {
		return false
	}
	repository, digest, found := strings.Cut(image, "@")
	if !found {
		return true
	}
	return repository != "" && validSHA256Digest(digest)
}

type attemptMetadata struct {
	namespace   string
	baseName    string
	labels      map[string]string
	annotations map[string]string
}

func newAttemptMetadata(input LegacyRFAttemptResourcesInput) attemptMetadata {
	digest := sha256.Sum256([]byte(input.Attempt.AttemptID))
	suffix := hex.EncodeToString(digest[:6])
	prefix := truncateDNSLabel(input.ClusterKey.Name, 39)
	baseName := fmt.Sprintf("%s-legacy-rf-%s", prefix, suffix)
	labels := k8ssandralabels.WatchedByK8ssandraClusterLabels(input.ClusterKey)
	annotations := map[string]string{
		legacyRFClusterUIDAnnotation:  input.Attempt.ClusterUID,
		legacyRFAttemptIDAnnotation:   input.Attempt.AttemptID,
		legacyRFGenerationAnnotation:  strconv.FormatInt(input.Attempt.Generation, 10),
		legacyRFSeedDigestAnnotation:  input.Attempt.SeedDigest,
		legacyRFMarkerAnnotation:      input.Attempt.MarkerVersion,
		legacyRFWorkerImageAnnotation: input.Attempt.WorkerImageDigest,
	}
	return attemptMetadata{namespace: input.Location.Namespace, baseName: baseName, labels: labels, annotations: annotations}
}

func truncateDNSLabel(value string, maximum int) string {
	value = strings.Trim(strings.ToLower(value), "-")
	if len(value) > maximum {
		value = strings.TrimRight(value[:maximum], "-")
	}
	if value == "" {
		return "cluster"
	}
	return value
}

func newAttemptSupportResources(
	input LegacyRFAttemptResourcesInput,
	metadata attemptMetadata,
	attemptJSON []byte,
) LegacyRFAttemptResources {
	objectMeta := func(suffix string) metav1.ObjectMeta {
		return metav1.ObjectMeta{Name: metadata.baseName + suffix, Namespace: metadata.namespace,
			Labels: copyStringMap(metadata.labels), Annotations: copyStringMap(metadata.annotations)}
	}
	resultName := metadata.baseName + "-result"
	role := &rbacv1.Role{ObjectMeta: objectMeta("-writer"), Rules: []rbacv1.PolicyRule{{
		APIGroups: []string{""}, Resources: []string{"configmaps"}, ResourceNames: []string{resultName},
		Verbs: []string{"get", "patch"},
	}}}
	serviceAccount := &corev1.ServiceAccount{ObjectMeta: objectMeta("-worker")}
	return LegacyRFAttemptResources{
		AttemptConfigMap: &corev1.ConfigMap{ObjectMeta: objectMeta("-attempt"), Data: map[string]string{legacyRFAttemptDataKey: string(attemptJSON)}},
		ResultConfigMap:  &corev1.ConfigMap{ObjectMeta: objectMeta("-result"), Data: map[string]string{}},
		HMACSecret:       &corev1.Secret{ObjectMeta: objectMeta("-hmac"), Type: corev1.SecretTypeOpaque, Immutable: boolValue(true), Data: map[string][]byte{legacyRFHMACDataKey: append([]byte(nil), input.HMACKey...)}},
		CopiedSecret:     &corev1.Secret{ObjectMeta: objectMeta("-source"), Type: corev1.SecretTypeOpaque, Immutable: boolValue(true), Data: copyByteMap(input.CopiedSecretData)},
		ServiceAccount:   serviceAccount,
		Role:             role,
		RoleBinding: &rbacv1.RoleBinding{ObjectMeta: objectMeta("-writer"), RoleRef: rbacv1.RoleRef{
			APIGroup: rbacv1.GroupName, Kind: "Role", Name: role.Name,
		}, Subjects: []rbacv1.Subject{{Kind: "ServiceAccount", Name: serviceAccount.Name, Namespace: metadata.namespace}}},
	}
}

func newAttemptJob(
	input LegacyRFAttemptResourcesInput,
	metadata attemptMetadata,
	resources LegacyRFAttemptResources,
) *batchv1.Job {
	args := workerArguments(input, resources)
	container := corev1.Container{
		Name: "discovery", Image: input.Attempt.WorkerImageDigest, ImagePullPolicy: corev1.PullIfNotPresent,
		Command: []string{"/legacy-rf-discovery"}, Args: args,
		SecurityContext: &corev1.SecurityContext{AllowPrivilegeEscalation: boolValue(false),
			ReadOnlyRootFilesystem: boolValue(true), RunAsNonRoot: boolValue(true),
			Capabilities: &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}}},
		Resources: corev1.ResourceRequirements{
			Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("100m"), corev1.ResourceMemory: resource.MustParse("64Mi")},
			Limits:   corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("500m"), corev1.ResourceMemory: resource.MustParse("256Mi")},
		},
		VolumeMounts: workerVolumeMounts(input),
	}
	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Name: metadata.baseName, Namespace: metadata.namespace,
			Labels: copyStringMap(metadata.labels), Annotations: copyStringMap(metadata.annotations)},
		Spec: batchv1.JobSpec{BackoffLimit: int32Value(0), ActiveDeadlineSeconds: int64Value(legacyRFJobDeadlineSeconds),
			TTLSecondsAfterFinished: int32Value(legacyRFJobTTLSeconds),
			Template: corev1.PodTemplateSpec{ObjectMeta: metav1.ObjectMeta{Labels: copyStringMap(metadata.labels), Annotations: copyStringMap(metadata.annotations)},
				Spec: corev1.PodSpec{ServiceAccountName: resources.ServiceAccount.Name, AutomountServiceAccountToken: boolValue(true),
					RestartPolicy: corev1.RestartPolicyNever, SecurityContext: &corev1.PodSecurityContext{
						RunAsNonRoot: boolValue(true), FSGroup: int64Value(65532),
						SeccompProfile: &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeRuntimeDefault}},
					Containers: []corev1.Container{container}, Volumes: workerVolumes(input, resources)}},
		},
	}
}

func workerArguments(input LegacyRFAttemptResourcesInput, resources LegacyRFAttemptResources) []string {
	args := []string{
		"--attempt", "/attempt/" + legacyRFAttemptDataKey,
		"--hmac-key", "/hmac/" + legacyRFHMACDataKey,
		"--result-configmap", resources.ResultConfigMap.Name,
		"--result-namespace", resources.ResultConfigMap.Namespace,
		"--result-key", legacyRFResultDataKey,
	}
	for _, binding := range input.Attempt.Connection.SecretBindings {
		switch binding.Purpose {
		case "auth":
			args = append(args, "--credentials-dir", "/credentials")
		case "tls":
			args = append(args, "--tls-dir", "/tls")
		}
	}
	return args
}

func workerVolumeMounts(input LegacyRFAttemptResourcesInput) []corev1.VolumeMount {
	mounts := []corev1.VolumeMount{
		{Name: "attempt", MountPath: "/attempt", ReadOnly: true},
		{Name: "hmac", MountPath: "/hmac", ReadOnly: true},
	}
	for _, binding := range input.Attempt.Connection.SecretBindings {
		if binding.Purpose == "auth" {
			mounts = append(mounts, corev1.VolumeMount{Name: "credentials", MountPath: "/credentials", ReadOnly: true})
		}
		if binding.Purpose == "tls" {
			mounts = append(mounts, corev1.VolumeMount{Name: "tls", MountPath: "/tls", ReadOnly: true})
		}
	}
	return mounts
}

func workerVolumes(input LegacyRFAttemptResourcesInput, resources LegacyRFAttemptResources) []corev1.Volume {
	mode := int32(0o440)
	volumes := []corev1.Volume{
		{Name: "attempt", VolumeSource: corev1.VolumeSource{ConfigMap: &corev1.ConfigMapVolumeSource{LocalObjectReference: corev1.LocalObjectReference{Name: resources.AttemptConfigMap.Name}}}},
		{Name: "hmac", VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{SecretName: resources.HMACSecret.Name, DefaultMode: &mode}}},
	}
	for _, binding := range input.Attempt.Connection.SecretBindings {
		if binding.Purpose == "auth" {
			volumes = append(volumes, corev1.Volume{Name: "credentials", VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{SecretName: resources.CopiedSecret.Name, DefaultMode: &mode, Items: []corev1.KeyToPath{{Key: legacyRFAuthUsernameKey, Path: "username"}, {Key: legacyRFAuthPasswordKey, Path: "password"}}}}})
		}
		if binding.Purpose == "tls" {
			volumes = append(volumes, tlsVolume(resources.CopiedSecret.Name, binding.Keys, mode))
		}
	}
	return volumes
}

func tlsVolume(secretName string, keys []string, mode int32) corev1.Volume {
	items := make([]corev1.KeyToPath, 0, len(keys))
	for _, key := range keys {
		if copiedKey, found := copiedSecretKey("tls", key); found {
			items = append(items, corev1.KeyToPath{Key: copiedKey, Path: key})
		}
	}
	return corev1.Volume{Name: "tls", VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{
		SecretName: secretName, DefaultMode: &mode, Items: items,
	}}}
}

func (resources LegacyRFAttemptResources) objects() []client.Object {
	return []client.Object{resources.AttemptConfigMap, resources.ResultConfigMap, resources.HMACSecret,
		resources.CopiedSecret, resources.ServiceAccount, resources.Role, resources.RoleBinding, resources.Job}
}

func (resources LegacyRFAttemptResources) nonSecretObjects() []client.Object {
	return []client.Object{resources.AttemptConfigMap, resources.ResultConfigMap, resources.ServiceAccount,
		resources.Role, resources.RoleBinding, resources.Job}
}

func copyStringMap(source map[string]string) map[string]string {
	copy := make(map[string]string, len(source))
	for key, value := range source {
		copy[key] = value
	}
	return copy
}

func copyByteMap(source map[string][]byte) map[string][]byte {
	if len(source) == 0 {
		return nil
	}
	copy := make(map[string][]byte, len(source))
	for key, value := range source {
		copy[key] = append([]byte(nil), value...)
	}
	return copy
}

func boolValue(value bool) *bool    { return &value }
func int32Value(value int32) *int32 { return &value }
func int64Value(value int64) *int64 { return &value }
