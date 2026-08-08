package e2e

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	api "github.com/k8ssandra/k8ssandra-operator/apis/k8ssandra/v1alpha1"
	"github.com/stretchr/testify/require"
	"sigs.k8s.io/yaml"
)

func TestLegacyRFLifecycleFixtureContract(t *testing.T) {
	repository := repositoryRoot(t)
	fixturesRoot := filepath.Join(repository, "test/testdata/fixtures")
	expected := map[string]string{
		"legacy-rf-discovery-4.0": "4.0.17",
		"legacy-rf-discovery-4.1": "4.1.9",
		"legacy-rf-discovery-5.0": "5.0.6",
	}
	fixturePaths, err := filepath.Glob(filepath.Join(fixturesRoot, "legacy-rf-discovery-*"))
	require.NoError(t, err)
	require.Len(t, fixturePaths, len(expected))
	for _, fixturePath := range fixturePaths {
		name := filepath.Base(fixturePath)
		version, found := expected[name]
		require.True(t, found, "unexpected legacy RF fixture %s", name)
		assertLegacyRFFixtureVersion(t, fixturePath, version, 1)
	}
	assertExistingLifecycleConvention(t, repository, expected)
}

func assertLegacyRFFixtureVersion(t *testing.T, fixturePath, expectedVersion string, expectedDatacenters int) {
	t.Helper()
	body, err := os.ReadFile(filepath.Join(fixturePath, "k8ssandra.yaml"))
	require.NoError(t, err)
	cluster := &api.K8ssandraCluster{}
	require.NoError(t, yaml.Unmarshal(body, cluster))
	require.NotNil(t, cluster.Spec.Cassandra)
	require.Equal(t, expectedVersion, cluster.Spec.Cassandra.ServerVersion)
	require.Len(t, cluster.Spec.Cassandra.Datacenters, expectedDatacenters)
	require.Equal(t, "dc1", cluster.Spec.Cassandra.Datacenters[0].Meta.Name)
	require.FileExists(t, filepath.Join(fixturePath, "kustomization.yaml"))
}

func assertExistingLifecycleConvention(t *testing.T, repository string, expected map[string]string) {
	t.Helper()
	suite := readContractFile(t, filepath.Join(repository, "test/e2e/suite_test.go"))
	workflow := readContractFile(t, filepath.Join(repository, ".github/workflows/kind_e2e_tests.yaml"))
	fixturesRoot := filepath.Join(repository, "test/testdata/fixtures")
	conventions := []struct {
		testName string
		fixture  string
		version  string
	}{
		{testName: "RemoveLocalDcFrom4.0Cluster", fixture: "remove-local-dc-4.0", version: expected["legacy-rf-discovery-4.0"]},
		{testName: "RemoveLocalDcFrom4.1Cluster", fixture: "remove-local-dc-4.1", version: expected["legacy-rf-discovery-4.1"]},
		{testName: "RemoveLocalDcFrom5.0Cluster", fixture: "remove-local-dc-5.0", version: expected["legacy-rf-discovery-5.0"]},
	}
	for _, convention := range conventions {
		require.Equal(t, 1, strings.Count(suite, `t.Run("`+convention.testName+`"`))
		require.Equal(t, 1, strings.Count(workflow, "- "+convention.testName))
		assertLegacyRFFixtureVersion(t, filepath.Join(fixturesRoot, convention.fixture), convention.version, 2)
	}
}

func readContractFile(t *testing.T, path string) string {
	t.Helper()
	body, err := os.ReadFile(path)
	require.NoError(t, err)
	return string(body)
}
