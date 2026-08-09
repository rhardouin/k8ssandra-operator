package e2e

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	api "github.com/k8ssandra/k8ssandra-operator/apis/k8ssandra/v1alpha1"
	k8ssandractrl "github.com/k8ssandra/k8ssandra-operator/controllers/k8ssandra"
	"github.com/k8ssandra/k8ssandra-operator/pkg/discovery"
	"github.com/stretchr/testify/require"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	yamlutil "k8s.io/apimachinery/pkg/util/yaml"
	"sigs.k8s.io/yaml"
)

const packagingImageDigest = "registry.example/k8ssandra-operator@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

func TestLegacyRFPackagingParity(t *testing.T) {
	repository := repositoryRoot(t)
	kustomize := filepath.Join(repository, "bin", "kustomize")
	require.FileExists(t, kustomize)

	variants := []struct {
		name string
		path string
	}{
		{name: "kustomize-default", path: "config/default"},
		{name: "kustomize-namespace", path: "config/deployments/control-plane"},
		{name: "kustomize-cluster", path: "config/deployments/control-plane/cluster-scope"},
	}
	for _, variant := range variants {
		t.Run(variant.name, func(t *testing.T) {
			objects := renderObjects(t, repository, kustomize, "build", variant.path)
			assertRenderedSafety(t, objects, true)
		})
	}

	helmVariants := []struct {
		name         string
		clusterScope bool
		controlPlane bool
	}{
		{name: "helm-namespace", controlPlane: true},
		{name: "helm-cluster", clusterScope: true, controlPlane: true},
		{name: "helm-data-plane", controlPlane: false},
	}
	for _, variant := range helmVariants {
		t.Run(variant.name, func(t *testing.T) {
			objects := renderHelm(t, repository, variant.clusterScope, variant.controlPlane)
			assertRenderedSafety(t, objects, variant.controlPlane)
		})
	}
}

func TestLegacyRFGeneratedCRDSchemaAcceptsInputAndRejectsMalformedStatus(t *testing.T) {
	repository := repositoryRoot(t)
	body, err := os.ReadFile(filepath.Join(repository, "config/crd/bases/k8ssandra.io_k8ssandraclusters.yaml"))
	require.NoError(t, err)
	crd := &apiextensionsv1.CustomResourceDefinition{}
	require.NoError(t, yaml.Unmarshal(body, crd))
	version := crdVersion(t, crd, "v1alpha1")

	valid := map[string]interface{}{
		"apiVersion": "k8ssandra.io/v1alpha1", "kind": "K8ssandraCluster",
		"metadata": map[string]interface{}{"name": "migration"},
		"spec": map[string]interface{}{"cassandra": map[string]interface{}{
			"legacyCqlCredentialsSecretRef": map[string]interface{}{"name": "legacy-auth"},
			"legacyCqlTLSSecretRef":         map[string]interface{}{"name": "legacy-tls"},
		}},
		"status": map[string]interface{}{"legacyRFDiscovery": map[string]interface{}{
			"phase": "Pending", "observedGeneration": int64(1),
		}},
	}
	require.NoError(t, validateOpenAPIValue(*version.Schema.OpenAPIV3Schema, valid, "root"))
	valid["status"].(map[string]interface{})["legacyRFDiscovery"] = "Pending"
	require.Error(t, validateOpenAPIValue(*version.Schema.OpenAPIV3Schema, valid, "root"))
}

func validateOpenAPIValue(definition apiextensionsv1.JSONSchemaProps, value interface{}, path string) error {
	switch definition.Type {
	case "object":
		object, ok := value.(map[string]interface{})
		if !ok {
			return fmt.Errorf("%s must be an object", path)
		}
		for name, child := range object {
			if property, found := definition.Properties[name]; found {
				if err := validateOpenAPIValue(property, child, path+"."+name); err != nil {
					return err
				}
			}
		}
	case "string":
		if _, ok := value.(string); !ok {
			return fmt.Errorf("%s must be a string", path)
		}
	case "integer":
		if _, ok := value.(int64); !ok {
			return fmt.Errorf("%s must be an integer", path)
		}
	}
	return nil
}

func TestLegacyRFWorkerAndAttemptRoleUseOneImmutableImage(t *testing.T) {
	seeds, digest, err := discovery.CanonicalizeSeeds([]string{"192.0.2.10"})
	require.NoError(t, err)
	attempt := discovery.Attempt{
		ClusterUID: "uid", AttemptID: "attempt", OrderedSeeds: seeds, SeedDigest: digest,
		WorkerImageDigest: packagingImageDigest,
	}
	resources, err := k8ssandractrl.BuildLegacyRFAttemptResources(k8ssandractrl.LegacyRFAttemptResourcesInput{
		ClusterKey: types.NamespacedName{Namespace: "control", Name: "migration"}, Attempt: attempt,
		Location: api.LegacyRFManagedLocation{Namespace: "data", Name: "dc1"}, HMACKey: bytes.Repeat([]byte{1}, 32),
	})
	require.NoError(t, err)
	container := resources.Job.Spec.Template.Spec.Containers[0]
	require.Equal(t, packagingImageDigest, container.Image)
	require.Equal(t, []string{"/legacy-rf-discovery"}, container.Command)
	require.Equal(t, []string{resources.ResultConfigMap.Name}, resources.Role.Rules[0].ResourceNames)
	require.ElementsMatch(t, []string{"get", "patch"}, resources.Role.Rules[0].Verbs)
	require.Equal(t, "100m", container.Resources.Requests.Cpu().String())
	require.Equal(t, "64Mi", container.Resources.Requests.Memory().String())
	require.Equal(t, "500m", container.Resources.Limits.Cpu().String())
	require.Equal(t, "256Mi", container.Resources.Limits.Memory().String())
}

func renderHelm(t *testing.T, repository string, clusterScope, controlPlane bool) []*unstructured.Unstructured {
	t.Helper()
	chart := filepath.Join(repository, "charts/k8ssandra-operator")
	require.DirExists(t, filepath.Join(chart, "charts"), "run helm dependency build charts/k8ssandra-operator")
	arguments := []string{"template", "release", chart, "--namespace", "k8ssandra-operator",
		"--set", "cass-operator.disableCertManagerCheck=true",
		"--set", fmt.Sprintf("global.clusterScoped=%t", clusterScope),
		"--set", "global.clusterScopedResources=true",
		"--set-string", `global.commonLabels.app\.kubernetes\.io/created-by=cass-operator`,
		"--set", fmt.Sprintf("controlPlane=%t", controlPlane),
	}
	return renderObjects(t, repository, "helm", arguments...)
}

func renderObjects(t *testing.T, repository, executable string, arguments ...string) []*unstructured.Unstructured {
	t.Helper()
	command := exec.CommandContext(context.Background(), executable, arguments...)
	command.Dir = repository
	output, err := command.CombinedOutput()
	require.NoError(t, err, string(output))
	decoder := yamlutil.NewYAMLOrJSONDecoder(bytes.NewReader(output), 4096)
	objects := make([]*unstructured.Unstructured, 0)
	for {
		object := &unstructured.Unstructured{}
		err = decoder.Decode(object)
		if errors.Is(err, io.EOF) {
			return objects
		}
		require.NoError(t, err)
		if object.GetKind() != "" {
			objects = append(objects, object)
		}
	}
}

func assertRenderedSafety(t *testing.T, objects []*unstructured.Unstructured, expectClusterWebhooks bool) {
	t.Helper()
	deployment := findObject(t, objects, schema.GroupVersionKind{Group: "apps", Version: "v1", Kind: "Deployment"}, "k8ssandra-operator")
	arguments, _, _ := unstructured.NestedStringSlice(deployment.Object, "spec", "template", "spec", "containers", "0", "args")
	if len(arguments) == 0 {
		arguments = containerArguments(t, deployment)
	}
	require.Contains(t, arguments, "--leader-elect")
	assertPodWebhookSelector(t, objects, deployment)
	assertK8ssandraWebhookPolicy(t, objects, expectClusterWebhooks)
	assertWebhookPolicy(t, objects, "vk8ssandracluster.kb.io", expectClusterWebhooks)
	assertControllerRBAC(t, objects)
	assertLeaderElectionRBAC(t, objects, deployment)
	assertEventRecorderRBAC(t, objects)
}

func assertEventRecorderRBAC(t *testing.T, objects []*unstructured.Unstructured) {
	t.Helper()
	controllers := 0
	for _, deployment := range objects {
		if deployment.GetKind() != "Deployment" || !hasControlPlaneLabel(deployment) {
			continue
		}
		controllers++
		serviceAccount, found, err := unstructured.NestedString(
			deployment.Object, "spec", "template", "spec", "serviceAccountName",
		)
		require.NoError(t, err)
		require.True(t, found)
		require.True(t, serviceAccountHasExactRule(
			t, objects, serviceAccount, deployment.GetNamespace(),
			"events.k8s.io", "events", []string{"create", "patch", "update"},
		), "service account %s has no bound modern Event recorder rule", serviceAccount)
	}
	require.Positive(t, controllers)
}

func hasControlPlaneLabel(deployment *unstructured.Unstructured) bool {
	labels, _, _ := unstructured.NestedStringMap(
		deployment.Object, "spec", "template", "metadata", "labels",
	)
	return labels["control-plane"] != ""
}

func serviceAccountHasExactRule(
	t *testing.T,
	objects []*unstructured.Unstructured,
	serviceAccount, namespace, apiGroup, resource string,
	verbs []string,
) bool {
	t.Helper()
	for _, binding := range objects {
		if !bindingTargetsServiceAccount(binding, serviceAccount, namespace) {
			continue
		}
		role := boundRole(objects, binding)
		if role != nil && assertExactRule(t, role, apiGroup, resource, verbs) {
			return true
		}
	}
	return false
}

func assertLeaderElectionRBAC(
	t *testing.T,
	objects []*unstructured.Unstructured,
	deployment *unstructured.Unstructured,
) {
	t.Helper()
	serviceAccount, found, err := unstructured.NestedString(
		deployment.Object, "spec", "template", "spec", "serviceAccountName",
	)
	require.NoError(t, err)
	require.True(t, found)
	for _, binding := range objects {
		if !bindingTargetsServiceAccount(binding, serviceAccount, deployment.GetNamespace()) {
			continue
		}
		role := boundRole(objects, binding)
		if role != nil && assertExactRule(
			t, role, "coordination.k8s.io", "leases",
			[]string{"get", "list", "watch", "create", "update", "patch", "delete"},
		) {
			return
		}
	}
	t.Fatalf("service account %s has no bound leader-election lease role", serviceAccount)
}

func bindingTargetsServiceAccount(binding *unstructured.Unstructured, name, namespace string) bool {
	if binding.GetKind() != "RoleBinding" && binding.GetKind() != "ClusterRoleBinding" {
		return false
	}
	subjects, _, _ := unstructured.NestedSlice(binding.Object, "subjects")
	for _, item := range subjects {
		subject := item.(map[string]interface{})
		subjectNamespace, _ := subject["namespace"].(string)
		if subjectNamespace == "" {
			subjectNamespace = binding.GetNamespace()
		}
		if namespace == "" {
			namespace = subjectNamespace
		}
		if subject["kind"] == "ServiceAccount" && subject["name"] == name && subjectNamespace == namespace {
			return true
		}
	}
	return false
}

func boundRole(objects []*unstructured.Unstructured, binding *unstructured.Unstructured) *unstructured.Unstructured {
	roleKind, _, _ := unstructured.NestedString(binding.Object, "roleRef", "kind")
	roleName, _, _ := unstructured.NestedString(binding.Object, "roleRef", "name")
	for _, object := range objects {
		if object.GetKind() != roleKind || object.GetName() != roleName {
			continue
		}
		if roleKind == "ClusterRole" || object.GetNamespace() == binding.GetNamespace() {
			return object
		}
	}
	return nil
}

func assertExactRule(
	t *testing.T,
	role *unstructured.Unstructured,
	apiGroup, resource string,
	expectedVerbs []string,
) bool {
	t.Helper()
	rules, _, _ := unstructured.NestedSlice(role.Object, "rules")
	for _, item := range rules {
		rule := item.(map[string]interface{})
		if !containsString(stringValues(rule["apiGroups"]), apiGroup) ||
			!containsString(stringValues(rule["resources"]), resource) {
			continue
		}
		require.Equal(t, []string{apiGroup}, stringValues(rule["apiGroups"]))
		require.Equal(t, []string{resource}, stringValues(rule["resources"]))
		require.ElementsMatch(t, expectedVerbs, stringValues(rule["verbs"]))
		require.Len(t, stringValues(rule["verbs"]), len(expectedVerbs))
		return true
	}
	return false
}

func assertPodWebhookSelector(
	t *testing.T,
	objects []*unstructured.Unstructured,
	deployment *unstructured.Unstructured,
) {
	t.Helper()
	webhook, found := findWebhook(t, objects, "mpod.kb.io")
	require.True(t, found)
	require.Equal(t, "Fail", webhook["failurePolicy"])
	rawSelector, ok := webhook["objectSelector"].(map[string]interface{})
	require.True(t, ok)
	selectorSpec := &metav1.LabelSelector{}
	require.NoError(t, runtime.DefaultUnstructuredConverter.FromUnstructured(rawSelector, selectorSpec))
	selector, err := metav1.LabelSelectorAsSelector(selectorSpec)
	require.NoError(t, err)
	require.True(t, selector.Matches(labels.Set{"app.kubernetes.io/created-by": "cass-operator"}))
	require.False(t, selector.Matches(labels.Set{}))
	managerLabels, found, err := unstructured.NestedStringMap(
		deployment.Object, "spec", "template", "metadata", "labels",
	)
	require.NoError(t, err)
	require.True(t, found)
	require.False(t, selector.Matches(labels.Set(managerLabels)))
}

func assertK8ssandraWebhookPolicy(t *testing.T, objects []*unstructured.Unstructured, expected bool) {
	t.Helper()
	webhook, found := findWebhook(t, objects, "mk8ssandracluster.kb.io")
	require.Equal(t, expected, found)
	if !found {
		return
	}
	require.Equal(t, "Fail", webhook["failurePolicy"])
	require.NotContains(t, webhook, "objectSelector")
}

func containerArguments(t *testing.T, deployment *unstructured.Unstructured) []string {
	t.Helper()
	containers, found, err := unstructured.NestedSlice(deployment.Object, "spec", "template", "spec", "containers")
	require.NoError(t, err)
	require.True(t, found)
	require.NotEmpty(t, containers)
	container := containers[0].(map[string]interface{})
	values, _, err := unstructured.NestedStringSlice(container, "args")
	require.NoError(t, err)
	return values
}

func assertWebhookPolicy(t *testing.T, objects []*unstructured.Unstructured, name string, expected bool) {
	t.Helper()
	webhook, found := findWebhook(t, objects, name)
	require.Equal(t, expected, found, name)
	if found {
		require.Equal(t, "Fail", webhook["failurePolicy"])
	}
}

func findWebhook(t *testing.T, objects []*unstructured.Unstructured, name string) (map[string]interface{}, bool) {
	t.Helper()
	for _, object := range objects {
		if object.GetKind() != "MutatingWebhookConfiguration" && object.GetKind() != "ValidatingWebhookConfiguration" {
			continue
		}
		webhooks, _, err := unstructured.NestedSlice(object.Object, "webhooks")
		require.NoError(t, err)
		for _, item := range webhooks {
			webhook := item.(map[string]interface{})
			if webhook["name"] == name {
				return webhook, true
			}
		}
	}
	return nil, false
}

func assertControllerRBAC(t *testing.T, objects []*unstructured.Unstructured) {
	t.Helper()
	required := map[string]bool{"jobs": false, "serviceaccounts": false, "roles": false, "rolebindings": false}
	for _, object := range objects {
		if object.GetKind() != "Role" && object.GetKind() != "ClusterRole" {
			continue
		}
		rules, _, _ := unstructured.NestedSlice(object.Object, "rules")
		for _, item := range rules {
			rule := item.(map[string]interface{})
			for _, resource := range stringValues(rule["resources"]) {
				if _, tracked := required[resource]; tracked {
					required[resource] = true
				}
			}
		}
	}
	for resource, found := range required {
		require.True(t, found, "missing controller RBAC for %s", resource)
	}
}

func stringValues(value interface{}) []string {
	items, ok := value.([]interface{})
	if !ok {
		return nil
	}
	values := make([]string, 0, len(items))
	for _, item := range items {
		if value, ok := item.(string); ok {
			values = append(values, value)
		}
	}
	return values
}

func findObject(t *testing.T, objects []*unstructured.Unstructured, gvk schema.GroupVersionKind, namePart string) *unstructured.Unstructured {
	t.Helper()
	for _, object := range objects {
		if object.GroupVersionKind() == gvk && strings.Contains(object.GetName(), namePart) {
			return object
		}
	}
	t.Fatalf("missing %s containing name %q", gvk.String(), namePart)
	return nil
}

func crdVersion(t *testing.T, crd *apiextensionsv1.CustomResourceDefinition, name string) apiextensionsv1.CustomResourceDefinitionVersion {
	t.Helper()
	for _, version := range crd.Spec.Versions {
		if version.Name == name {
			return version
		}
	}
	t.Fatalf("missing CRD version %s", name)
	return apiextensionsv1.CustomResourceDefinitionVersion{}
}

func repositoryRoot(t *testing.T) string {
	t.Helper()
	root, err := filepath.Abs(filepath.Join("..", ".."))
	require.NoError(t, err)
	return root
}
