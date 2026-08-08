//go:build legacy_rf_kind

package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	api "github.com/k8ssandra/k8ssandra-operator/apis/k8ssandra/v1alpha1"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	yamlutil "k8s.io/apimachinery/pkg/util/yaml"
	"sigs.k8s.io/yaml"
)

const (
	legacyRFKindVersion = "v0.31.0"
	legacyRFKindNode    = "kindest/node:v1.34.3@sha256:08497ee19eace7b4b5348db5c6a1591d7752b164530a36f855cb0f2bdcbadd48"
	legacyRFOldCommit   = "bcabb887f63b07ab9d03da9c30c08d916e8598e6"
	legacyRFOldImage    = "docker.io/library/k8ssandra-operator:legacy-rf-old-v1.32.5"
	legacyRFNewImage    = "docker.io/library/k8ssandra-operator:legacy-rf-current"
	legacyRFCertManager = "https://github.com/cert-manager/cert-manager/releases/download/v1.18.5/cert-manager.yaml"
	legacyRFNamespace   = "k8ssandra-operator"
)

type upgradePackage string

const (
	upgradeKustomize upgradePackage = "kustomize"
	upgradeHelm      upgradePackage = "helm"
)

type upgradeHarness struct {
	t          *testing.T
	repository string
	oldRoot    string
	cluster    string
	kubeconfig string
	kind       string
}

type upgradeTimelineEntry struct {
	At     time.Time
	Action string
}

// TestLegacyRFPackagedUpgradeSafety proves that no marked migration is created
// until the packaged operator rollout has fully completed.
func TestLegacyRFPackagedUpgradeSafety(t *testing.T) {
	repository := repositoryRoot(t)
	kind := filepath.Join(repository, "build", "tools", "kind")
	require.Contains(t, runOutput(t, repository, kind, "version"), legacyRFKindVersion)
	oldRoot := prepareOldWorktree(t, repository)
	require.Equal(t, legacyRFOldCommit, strings.TrimSpace(runOutput(t, oldRoot, "git", "rev-parse", "HEAD")))
	buildUpgradeImages(t, repository, oldRoot)

	for _, packaging := range []upgradePackage{upgradeKustomize, upgradeHelm} {
		packaging := packaging
		t.Run(string(packaging), func(t *testing.T) {
			harness := newUpgradeHarness(t, repository, oldRoot, kind, packaging)
			harness.run(packaging)
		})
	}
}

func prepareOldWorktree(t *testing.T, repository string) string {
	t.Helper()
	if supplied := os.Getenv("LEGACY_RF_OLD_ROOT"); supplied != "" {
		return supplied
	}
	oldRoot := filepath.Join(t.TempDir(), "v1.32.5")
	runOutput(t, repository, "git", "worktree", "add", "--detach", oldRoot, legacyRFOldCommit)
	t.Cleanup(func() {
		command := exec.Command("git", "worktree", "remove", "--force", oldRoot)
		command.Dir = repository
		if output, err := command.CombinedOutput(); err != nil {
			t.Logf("old worktree cleanup failed: %v: %s", err, output)
		}
	})
	return oldRoot
}

func buildUpgradeImages(t *testing.T, repository, oldRoot string) {
	t.Helper()
	runOutput(t, oldRoot, "docker", "build", "--tag", legacyRFOldImage, ".")
	runOutput(t, repository, "docker", "build", "--tag", legacyRFNewImage, ".")
	if _, err := os.Stat(filepath.Join(oldRoot, "charts", "k8ssandra-operator", "charts")); errors.Is(err, os.ErrNotExist) {
		runOutput(t, oldRoot, "helm", "dependency", "build", "charts/k8ssandra-operator")
	}
}

func newUpgradeHarness(
	t *testing.T, repository, oldRoot, kind string, packaging upgradePackage,
) *upgradeHarness {
	t.Helper()
	suffix := strings.ReplaceAll(string(packaging), "_", "-")
	cluster := fmt.Sprintf("legacy-rf-%s-%d", suffix, time.Now().UnixNano()%1_000_000)
	harness := &upgradeHarness{
		t: t, repository: repository, oldRoot: oldRoot, cluster: cluster,
		kubeconfig: filepath.Join(t.TempDir(), "kubeconfig"), kind: kind,
	}
	t.Cleanup(func() {
		command := exec.Command(kind, "delete", "cluster", "--name", cluster)
		command.Dir = repository
		if output, err := command.CombinedOutput(); err != nil {
			t.Logf("Kind cleanup failed: %v: %s", err, output)
		}
	})
	return harness
}

func (h *upgradeHarness) run(packaging upgradePackage) {
	timeline := make([]upgradeTimelineEntry, 0, 12)
	record := func(action string) {
		entry := upgradeTimelineEntry{At: time.Now().UTC(), Action: action}
		timeline = append(timeline, entry)
		h.t.Logf("timeline %s %s", entry.At.Format(time.RFC3339Nano), action)
	}

	h.createCluster(record)
	h.installCertManager(record)
	h.installOldPackage(packaging, record)
	h.assertOldAdmissionBehavior(record)
	h.exposeNewWebhook(packaging, record)
	h.assertAdmissionFailsClosed(record)
	h.replaceManager(packaging, record)
	h.assertNewAdmissionAndGate(record)
	h.assertTimeline(timeline)
}

func (h *upgradeHarness) createCluster(record func(string)) {
	runOutput(h.t, h.repository, h.kind, "create", "cluster", "--name", h.cluster,
		"--image", legacyRFKindNode, "--kubeconfig", h.kubeconfig, "--wait", "180s")
	runOutput(h.t, h.repository, h.kind, "load", "docker-image", "--name", h.cluster,
		legacyRFOldImage, legacyRFNewImage)
	record("pinned Kind cluster created and both immutable build inputs loaded")
}

func (h *upgradeHarness) installCertManager(record func(string)) {
	h.kubectl(nil, "apply", "-f", legacyRFCertManager)
	for _, deployment := range []string{"cert-manager", "cert-manager-cainjector", "cert-manager-webhook"} {
		h.kubectl(nil, "rollout", "status", "deployment/"+deployment, "-n", "cert-manager", "--timeout=180s")
	}
	record("cert-manager v1.18.5 ready")
}

func (h *upgradeHarness) installOldPackage(packaging upgradePackage, record func(string)) {
	if packaging == upgradeHelm {
		h.kubectl(nil, "create", "namespace", legacyRFNamespace)
	}
	objects := h.renderPackage(packaging, h.oldRoot, legacyRFOldImage)
	h.applyObjects(objects)
	h.waitForManagerRollout()
	pod := h.singleManagerPod()
	require.Contains(h.t, pod.Spec.Containers[0].Image, "legacy-rf-old-v1.32.5")
	record("old v1.32.5 package ready with Pod UID " + string(pod.UID))
}

func (h *upgradeHarness) assertOldAdmissionBehavior(record func(string)) {
	h.kubectl([]byte(legacyClusterYAML("retained-old", false)), "apply", "-f", "-")
	cluster := h.getCluster("retained-old")
	require.NotContains(h.t, cluster.GetAnnotations(), api.LegacyRFDiscoveryMarkerAnnotation)
	record("old admission retained an unmarked K8ssandraCluster")
}

func (h *upgradeHarness) exposeNewWebhook(packaging upgradePackage, record func(string)) {
	objects := h.renderPackage(packaging, h.repository, legacyRFNewImage)
	if packaging == upgradeHelm {
		h.deleteReplaceableHelmHooks(objects)
	}
	deployment, remaining := splitManagerDeployment(h.t, objects)
	require.NotNil(h.t, deployment)
	h.applyObjects(remaining)
	record("new packaged webhook/API objects exposed while old manager Pod remained active")
	oldPod := h.singleManagerPod()
	require.Contains(h.t, oldPod.Spec.Containers[0].Image, "legacy-rf-old-v1.32.5")
}

func (h *upgradeHarness) deleteReplaceableHelmHooks(objects []*unstructured.Unstructured) {
	h.t.Helper()
	for _, object := range objects {
		annotations := object.GetAnnotations()
		if annotations["helm.sh/hook"] == "" ||
			!strings.Contains(annotations["helm.sh/hook-delete-policy"], "before-hook-creation") {
			continue
		}
		h.kubectl(nil, "delete", object.GetKind(), object.GetName(), "--namespace", legacyRFNamespace,
			"--ignore-not-found=true", "--wait=true")
	}
}

func (h *upgradeHarness) assertAdmissionFailsClosed(record func(string)) {
	oldUID := string(h.singleManagerPod().UID)
	h.waitForFailsClosedCanary(oldUID)
	for index := 0; index < 3; index++ {
		name := fmt.Sprintf("closed-canary-%d", index)
		output, err := h.kubectlError([]byte(legacyClusterYAML(name, true)), "create", "-f", "-")
		require.Error(h.t, err, "marker admission unexpectedly reopened while the old manager was active")
		require.NotEmpty(h.t, strings.TrimSpace(output))
		require.Equal(h.t, oldUID, string(h.singleManagerPod().UID))
	}
	record("marker injection failed closed while old reconciler was active")
}

func (h *upgradeHarness) waitForFailsClosedCanary(oldUID string) {
	h.t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for attempt := 0; time.Now().Before(deadline); attempt++ {
		name := fmt.Sprintf("overlap-canary-%d", attempt)
		_, err := h.kubectlError([]byte(legacyClusterYAML(name, true)), "create", "-f", "-")
		if err != nil {
			return
		}
		cluster := h.getCluster(name)
		require.NotContains(h.t, cluster.GetAnnotations(), api.LegacyRFDiscoveryMarkerAnnotation,
			"old manager admitted an object with a new-version marker")
		require.Equal(h.t, oldUID, string(h.singleManagerPod().UID))
		h.kubectl(nil, "delete", "k8ssandracluster", name, "-n", legacyRFNamespace, "--wait=true")
		time.Sleep(250 * time.Millisecond)
	}
	h.t.Fatal("new mutating webhook did not become fail-closed within 30s")
}

func (h *upgradeHarness) replaceManager(packaging upgradePackage, record func(string)) {
	oldPod := h.singleManagerPod()
	objects := h.renderPackage(packaging, h.repository, legacyRFNewImage)
	deployment, _ := splitManagerDeployment(h.t, objects)
	require.NotNil(h.t, deployment)
	h.applyDeployment(deployment)
	h.waitForManagerRollout()
	newPod := h.singleManagerPod()
	require.NotEqual(h.t, oldPod.UID, newPod.UID)
	require.Contains(h.t, newPod.Spec.Containers[0].Image, "legacy-rf-current")
	record("new marker-aware manager ready after the operator rollout fully completed")
}

func (h *upgradeHarness) assertNewAdmissionAndGate(record func(string)) {
	h.kubectl([]byte(legacyClusterYAML("gated-new", true)), "create", "-f", "-")
	cluster := h.getCluster("gated-new")
	require.Equal(h.t, api.LegacyRFDiscoveryMarkerVersion,
		cluster.GetAnnotations()[api.LegacyRFDiscoveryMarkerAnnotation])
	record("new admission injected the v1 discovery marker")

	phase := h.waitForDiscoveryPhase("gated-new")
	require.Contains(h.t, []string{"Pending", "Blocked"}, phase)
	output := h.kubectl(nil, "get", "cassandradatacenters.cassandra.datastax.com", "-n", legacyRFNamespace,
		"-o", "jsonpath={.items[*].metadata.name}")
	require.Empty(h.t, strings.TrimSpace(output), "discovery must gate CassandraDatacenter creation")
	record("discovery phase " + phase + " observed with zero CassandraDatacenters")
}

func (h *upgradeHarness) assertTimeline(entries []upgradeTimelineEntry) {
	h.t.Helper()
	require.GreaterOrEqual(h.t, len(entries), 9)
	for index := 1; index < len(entries); index++ {
		require.False(h.t, entries[index].At.Before(entries[index-1].At))
	}
	h.t.Log("the marked migration was created only after the packaged operator rollout completed")
}

func (h *upgradeHarness) renderPackage(
	packaging upgradePackage, root, image string,
) []*unstructured.Unstructured {
	h.t.Helper()
	var output string
	switch packaging {
	case upgradeKustomize:
		output = runOutput(h.t, root, filepath.Join(h.repository, "bin", "kustomize"),
			"build", "config/deployments/control-plane")
	case upgradeHelm:
		output = runOutput(h.t, root, "helm", "template", "release", "charts/k8ssandra-operator",
			"--namespace", legacyRFNamespace, "--include-crds", "--set", "cass-operator.disableCertManagerCheck=true",
			"--set", "image.registry=docker.io", "--set", "image.repository=library/k8ssandra-operator",
			"--set", "image.tag="+strings.TrimPrefix(image, "docker.io/library/k8ssandra-operator:"),
			"--set", "fullnameOverride=k8ssandra-operator")
	default:
		h.t.Fatalf("unsupported package %q", packaging)
	}
	objects := decodeUpgradeObjects(h.t, []byte(output))
	setManagerImage(h.t, objects, image)
	return objects
}

func decodeUpgradeObjects(t *testing.T, body []byte) []*unstructured.Unstructured {
	t.Helper()
	decoder := yamlutil.NewYAMLOrJSONDecoder(bytes.NewReader(body), 4096)
	objects := make([]*unstructured.Unstructured, 0)
	for {
		object := &unstructured.Unstructured{}
		err := decoder.Decode(object)
		if errors.Is(err, io.EOF) {
			return objects
		}
		require.NoError(t, err)
		if object.GetKind() != "" {
			objects = append(objects, object)
		}
	}
}

func setManagerImage(t *testing.T, objects []*unstructured.Unstructured, image string) {
	t.Helper()
	for _, object := range objects {
		if object.GetAPIVersion() != "apps/v1" || object.GetKind() != "Deployment" {
			continue
		}
		containers, found, err := unstructured.NestedSlice(object.Object, "spec", "template", "spec", "containers")
		require.NoError(t, err)
		if !found {
			continue
		}
		for index := range containers {
			container := containers[index].(map[string]interface{})
			if container["name"] == "k8ssandra-operator" {
				container["image"] = image
				require.NoError(t, unstructured.SetNestedSlice(object.Object, containers,
					"spec", "template", "spec", "containers"))
				return
			}
		}
	}
	t.Fatal("rendered package has no k8ssandra-operator container")
}

func splitManagerDeployment(
	t *testing.T, objects []*unstructured.Unstructured,
) (*unstructured.Unstructured, []*unstructured.Unstructured) {
	t.Helper()
	remaining := make([]*unstructured.Unstructured, 0, len(objects)-1)
	var deployment *unstructured.Unstructured
	for _, object := range objects {
		if object.GetAPIVersion() == "apps/v1" && object.GetKind() == "Deployment" &&
			object.GetName() == "k8ssandra-operator" {
			deployment = object
			continue
		}
		remaining = append(remaining, object)
	}
	return deployment, remaining
}

func (h *upgradeHarness) applyObjects(objects []*unstructured.Unstructured) {
	h.t.Helper()
	var manifest bytes.Buffer
	for _, object := range objects {
		body, err := json.Marshal(object.Object)
		require.NoError(h.t, err)
		yamlBody, err := yaml.JSONToYAML(body)
		require.NoError(h.t, err)
		manifest.WriteString("---\n")
		manifest.Write(yamlBody)
	}
	h.kubectl(manifest.Bytes(), "apply", "--server-side", "--force-conflicts",
		"--namespace", legacyRFNamespace, "-f", "-")
}

func (h *upgradeHarness) applyDeployment(deployment *unstructured.Unstructured) {
	h.t.Helper()
	body, err := json.Marshal(deployment.Object)
	require.NoError(h.t, err)
	yamlBody, err := yaml.JSONToYAML(body)
	require.NoError(h.t, err)
	h.kubectl(yamlBody, "apply", "--namespace", legacyRFNamespace, "-f", "-")
}

func (h *upgradeHarness) waitForManagerRollout() {
	h.t.Helper()
	h.kubectl(nil, "rollout", "status", "deployment/k8ssandra-operator", "-n", legacyRFNamespace,
		"--timeout=240s")
}

func (h *upgradeHarness) singleManagerPod() corev1.Pod {
	h.t.Helper()
	active := make([]corev1.Pod, 0, 1)
	for _, pod := range h.managerPods() {
		if pod.DeletionTimestamp == nil && pod.Status.Phase != corev1.PodFailed && pod.Status.Phase != corev1.PodSucceeded {
			active = append(active, pod)
		}
	}
	require.Len(h.t, active, 1)
	return active[0]
}

func (h *upgradeHarness) managerPods() []corev1.Pod {
	h.t.Helper()
	output := h.kubectl(nil, "get", "pods", "-n", legacyRFNamespace,
		"-l", "control-plane=k8ssandra-operator", "-o", "json")
	list := corev1.PodList{}
	require.NoError(h.t, json.Unmarshal([]byte(output), &list))
	return list.Items
}

func (h *upgradeHarness) getCluster(name string) *unstructured.Unstructured {
	h.t.Helper()
	output := h.kubectl(nil, "get", "k8ssandracluster", name, "-n", legacyRFNamespace, "-o", "json")
	cluster := &unstructured.Unstructured{}
	require.NoError(h.t, json.Unmarshal([]byte(output), &cluster.Object))
	return cluster
}

func (h *upgradeHarness) waitForDiscoveryPhase(name string) string {
	h.t.Helper()
	deadline := time.Now().Add(180 * time.Second)
	for time.Now().Before(deadline) {
		cluster := h.getCluster(name)
		phase, found, err := unstructured.NestedString(cluster.Object, "status", "legacyRFDiscovery", "phase")
		require.NoError(h.t, err)
		if found && phase != "" {
			return phase
		}
		time.Sleep(500 * time.Millisecond)
	}
	h.t.Fatal("legacy RF discovery phase was not published within 180s")
	return ""
}

func (h *upgradeHarness) kubectl(stdin []byte, arguments ...string) string {
	h.t.Helper()
	output, err := h.kubectlError(stdin, arguments...)
	require.NoError(h.t, err, output)
	return output
}

func (h *upgradeHarness) kubectlError(stdin []byte, arguments ...string) (string, error) {
	h.t.Helper()
	arguments = append([]string{"--kubeconfig", h.kubeconfig}, arguments...)
	command := exec.CommandContext(context.Background(), "kubectl", arguments...)
	command.Dir = h.repository
	command.Stdin = bytes.NewReader(stdin)
	output, err := command.CombinedOutput()
	return string(output), err
}

func runOutput(t *testing.T, directory, executable string, arguments ...string) string {
	t.Helper()
	command := exec.CommandContext(context.Background(), executable, arguments...)
	command.Dir = directory
	output, err := command.CombinedOutput()
	require.NoError(t, err, "%s %s failed:\n%s", executable, strings.Join(arguments, " "), output)
	return string(output)
}

func legacyClusterYAML(name string, qualify bool) string {
	seeds := ""
	if qualify {
		seeds = "\n    additionalSeeds:\n    - 192.0.2.10"
	}
	return fmt.Sprintf(`apiVersion: k8ssandra.io/v1alpha1
kind: K8ssandraCluster
metadata:
  name: %s
  namespace: %s
spec:
  cassandra:
    serverVersion: "4.1.9"
    datacenters: []%s
`, name, legacyRFNamespace, seeds)
}
