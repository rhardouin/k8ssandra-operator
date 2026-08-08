package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/netip"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	api "github.com/k8ssandra/k8ssandra-operator/apis/k8ssandra/v1alpha1"
	"github.com/k8ssandra/k8ssandra-operator/pkg/discovery"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

type fakeObserver struct {
	candidate discovery.Candidate
	err       error
	calls     []netip.AddrPort
}

func (observer *fakeObserver) DiscoverCandidate(
	ctx context.Context,
	endpoint netip.AddrPort,
	_ discovery.Connection,
) (discovery.Candidate, error) {
	observer.calls = append(observer.calls, endpoint)
	if err := ctx.Err(); err != nil {
		return discovery.Candidate{}, err
	}
	return observer.candidate, observer.err
}

type testClock struct{ now time.Time }

func (clock testClock) Now() time.Time { return clock.now }

type fakeResultPublisher struct {
	targets []resultConfigMapTarget
	bodies  [][]byte
	err     error
}

func (publisher *fakeResultPublisher) Publish(
	_ context.Context,
	target resultConfigMapTarget,
	body []byte,
) error {
	publisher.targets = append(publisher.targets, target)
	publisher.bodies = append(publisher.bodies, append([]byte(nil), body...))
	return publisher.err
}

func TestRunWritesOneCanonicalSignedResultWithoutLoggingSecrets(t *testing.T) {
	directory := t.TempDir()
	authDirectory := filepath.Join(directory, "auth")
	require.NoError(t, os.Mkdir(authDirectory, 0o700))
	require.NoError(t, os.WriteFile(filepath.Join(authDirectory, "username"), []byte("user-canary"), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(authDirectory, "password"), []byte("password-canary"), 0o600))
	key := bytes.Repeat([]byte{7}, 32)
	keyPath := filepath.Join(directory, "hmac-key")
	require.NoError(t, os.WriteFile(keyPath, key, 0o600))
	attempt := validExecutableAttempt(t)
	attempt.Connection.SecretBindings = []discovery.SecretBinding{{
		Purpose: "auth", SourceContext: "source", Namespace: "ns", Name: "credentials",
		Keys: []string{"username", "password"}, ResourceVersion: "7",
	}}
	attemptPath := writeAttempt(t, directory, attempt)
	observer := &fakeObserver{candidate: validExecutableCandidate()}
	publisher := &fakeResultPublisher{}
	dependencies := executableDependencies{
		newObserver: func(options discovery.CQLObserverOptions) (discovery.EndpointObserver, error) {
			require.Equal(t, "user-canary", options.Credentials.Username)
			require.Equal(t, "password-canary", options.Credentials.Password)
			return observer, nil
		},
		clock:     testClock{now: time.Date(2099, 1, 1, 0, 0, 0, 0, time.UTC)},
		publisher: publisher,
	}
	var stdout, stderr bytes.Buffer
	exitCode := run(context.Background(), []string{
		"--attempt", attemptPath, "--hmac-key", keyPath,
		"--result-configmap", "attempt-result", "--result-namespace", "worker-ns", "--result-key", "result.json",
		"--credentials-dir", authDirectory,
	}, &stdout, &stderr, dependencies)
	require.Zero(t, exitCode)
	require.Empty(t, stdout.String())
	require.Empty(t, stderr.String())
	require.Equal(t, []netip.AddrPort{attempt.OrderedSeeds[0]}, observer.calls)

	require.Equal(t, []resultConfigMapTarget{{Namespace: "worker-ns", Name: "attempt-result", Key: "result.json"}}, publisher.targets)
	require.Len(t, publisher.bodies, 1)
	body := publisher.bodies[0]
	_, statErr := os.Stat(filepath.Join(directory, "result.json"))
	require.ErrorIs(t, statErr, os.ErrNotExist, "success must not depend on a Pod-local result file")
	result, err := discovery.ValidateResultEnvelope(body, key, discovery.ExpectedResult{
		SchemaVersion: attempt.ProtocolVersion, ClusterUID: attempt.ClusterUID,
		Generation: attempt.Generation, MarkerVersion: attempt.MarkerVersion,
		ProtocolVersion: attempt.ProtocolVersion, AttemptID: attempt.AttemptID,
		OrderedSeeds: attempt.OrderedSeeds, SeedDigest: attempt.SeedDigest,
		SecretBindings: attempt.Connection.SecretBindings, WorkerImageDigest: attempt.WorkerImageDigest,
		MaximumBytes: api.LegacyRFDiscoveryMaxResultBytes,
	})
	require.NoError(t, err)
	require.EqualValues(t, 10, result.Authoritative.Replication.SystemAuth.Replication["legacy-e"])
	require.NotContains(t, string(body), "user-canary")
	require.NotContains(t, string(body), "password-canary")
}

func TestRunFailsClosedWithStableRedactedErrors(t *testing.T) {
	secretCanaries := []string{"password-canary", "certificate-canary", "hmac-canary", "dsn-canary"}
	tests := []struct {
		name   string
		mutate func(*discovery.Attempt, *runPaths, *fakeObserver, *fakeResultPublisher)
		cancel bool
	}{
		{name: "mutable worker image", mutate: func(attempt *discovery.Attempt, _ *runPaths, _ *fakeObserver, _ *fakeResultPublisher) {
			attempt.WorkerImageDigest = "registry.example/operator:mutable"
		}},
		{name: "missing qualified auth mount", mutate: func(attempt *discovery.Attempt, paths *runPaths, _ *fakeObserver, _ *fakeResultPublisher) {
			attempt.Connection.SecretBindings = []discovery.SecretBinding{{Purpose: "auth", SourceContext: "source", Namespace: "ns", Name: "auth", Keys: []string{"username", "password"}, ResourceVersion: "1"}}
			paths.credentialsDirectory = ""
		}},
		{name: "missing qualified TLS mount", mutate: func(attempt *discovery.Attempt, _ *runPaths, _ *fakeObserver, _ *fakeResultPublisher) {
			attempt.Connection.SecretBindings = []discovery.SecretBinding{{Purpose: "tls", SourceContext: "source", Namespace: "ns", Name: "tls", Keys: []string{"ca.crt"}, ResourceVersion: "1"}}
		}},
		{name: "malformed credentials", mutate: func(attempt *discovery.Attempt, paths *runPaths, _ *fakeObserver, _ *fakeResultPublisher) {
			attempt.Connection.SecretBindings = []discovery.SecretBinding{{Purpose: "auth", SourceContext: "source", Namespace: "ns", Name: "auth", Keys: []string{"username", "password"}, ResourceVersion: "1"}}
			paths.credentialsDirectory = filepath.Join(filepath.Dir(paths.attempt), "auth")
			require.NoError(t, os.Mkdir(paths.credentialsDirectory, 0o700))
			require.NoError(t, os.WriteFile(filepath.Join(paths.credentialsDirectory, "username"), []byte("password-canary"), 0o600))
		}},
		{name: "wrong result key binding", mutate: func(_ *discovery.Attempt, paths *runPaths, _ *fakeObserver, _ *fakeResultPublisher) {
			paths.resultKey = "credentials.json"
		}},
		{name: "publisher update failure", mutate: func(_ *discovery.Attempt, _ *runPaths, _ *fakeObserver, publisher *fakeResultPublisher) {
			publisher.err = errors.New("hmac-canary")
		}},
		{name: "legacy local result flag rejected", mutate: func(_ *discovery.Attempt, paths *runPaths, _ *fakeObserver, _ *fakeResultPublisher) {
			paths.legacyResultPath = filepath.Join(filepath.Dir(paths.attempt), "result.json")
		}},
		{name: "canceled", cancel: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			directory := t.TempDir()
			paths := runPaths{
				attempt: filepath.Join(directory, "attempt.json"), hmacKey: filepath.Join(directory, "hmac-key"),
				resultNamespace: "worker-ns", resultConfigMap: "attempt-result", resultKey: "result.json",
			}
			require.NoError(t, os.WriteFile(paths.hmacKey, bytes.Repeat([]byte{9}, 32), 0o600))
			attempt := validExecutableAttempt(t)
			observer := &fakeObserver{candidate: validExecutableCandidate()}
			publisher := &fakeResultPublisher{}
			if test.mutate != nil {
				test.mutate(&attempt, &paths, observer, publisher)
			}
			encoded, err := json.Marshal(attempt)
			require.NoError(t, err)
			require.NoError(t, os.WriteFile(paths.attempt, encoded, 0o600))
			ctx := context.Background()
			if test.cancel {
				canceled, cancel := context.WithCancel(ctx)
				cancel()
				ctx = canceled
			}
			dependencies := executableDependencies{
				newObserver: func(discovery.CQLObserverOptions) (discovery.EndpointObserver, error) { return observer, nil },
				clock:       testClock{now: time.Date(2099, 1, 1, 0, 0, 0, 0, time.UTC)}, publisher: publisher,
			}
			var stdout, stderr bytes.Buffer
			exitCode := run(ctx, paths.arguments(), &stdout, &stderr, dependencies)
			require.Equal(t, 1, exitCode)
			require.Empty(t, stdout.String())
			require.True(t, strings.HasPrefix(stderr.String(), "legacy RF discovery failed: "))
			for _, canary := range secretCanaries {
				require.NotContains(t, stderr.String(), canary)
			}
			if test.name != "publisher update failure" {
				require.Empty(t, publisher.bodies)
			}
			if paths.legacyResultPath != "" {
				_, statErr := os.Stat(paths.legacyResultPath)
				require.ErrorIs(t, statErr, os.ErrNotExist)
			}
		})
	}
}

func TestConfigMapPublisherPatchesOnlyTheBoundPrecreatedObjectOnce(t *testing.T) {
	scheme := runtime.NewScheme()
	require.NoError(t, corev1.AddToScheme(scheme))
	target := resultConfigMapTarget{Namespace: "worker-ns", Name: "attempt-result", Key: "result.json"}
	precreated := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Namespace: target.Namespace, Name: target.Name}}
	decoy := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Namespace: target.Namespace, Name: "other-result"},
		Data:       map[string]string{"untouched": "value"},
	}
	kubernetesClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(precreated, decoy).Build()
	publisher := configMapResultPublisher{kubernetesClient: kubernetesClient}

	require.NoError(t, publisher.Publish(context.Background(), target, []byte(`{"signed":true}`)))
	actual := &corev1.ConfigMap{}
	require.NoError(t, kubernetesClient.Get(context.Background(), types.NamespacedName{
		Namespace: target.Namespace, Name: target.Name,
	}, actual))
	require.Equal(t, map[string]string{"result.json": `{"signed":true}`}, actual.Data)
	require.NoError(t, kubernetesClient.Get(context.Background(), types.NamespacedName{
		Namespace: decoy.Namespace, Name: decoy.Name,
	}, decoy))
	require.Equal(t, map[string]string{"untouched": "value"}, decoy.Data)
	require.Error(t, publisher.Publish(context.Background(), target, []byte(`{"signed":false}`)))

	missing := resultConfigMapTarget{Namespace: target.Namespace, Name: "missing-result", Key: target.Key}
	require.Error(t, publisher.Publish(context.Background(), missing, []byte(`{"signed":true}`)))
	err := kubernetesClient.Get(context.Background(), types.NamespacedName{
		Namespace: missing.Namespace, Name: missing.Name,
	}, &corev1.ConfigMap{})
	require.True(t, apierrors.IsNotFound(err), "publisher must never create a missing ConfigMap")
}

type runPaths struct {
	attempt, hmacKey, credentialsDirectory      string
	resultNamespace, resultConfigMap, resultKey string
	legacyResultPath                            string
}

func (paths runPaths) arguments() []string {
	arguments := []string{
		"--attempt", paths.attempt, "--hmac-key", paths.hmacKey,
		"--result-namespace", paths.resultNamespace, "--result-configmap", paths.resultConfigMap,
		"--result-key", paths.resultKey,
	}
	if paths.legacyResultPath != "" {
		arguments = append(arguments, "--result", paths.legacyResultPath)
	}
	if paths.credentialsDirectory != "" {
		arguments = append(arguments, "--credentials-dir", paths.credentialsDirectory)
	}
	return arguments
}

func TestParseOptionsRejectsConfigurableSeedLimit(t *testing.T) {
	paths := runPaths{attempt: "/attempt", hmacKey: "/hmac", resultNamespace: "ns", resultConfigMap: "result", resultKey: "result.json"}
	_, err := parseOptions(append(paths.arguments(), "--maximum-seeds", "1"))
	require.Error(t, err)
}

func writeAttempt(t *testing.T, directory string, attempt discovery.Attempt) string {
	t.Helper()
	body, err := json.Marshal(attempt)
	require.NoError(t, err)
	path := filepath.Join(directory, "attempt.json")
	require.NoError(t, os.WriteFile(path, body, 0o600))
	return path
}

func validExecutableAttempt(t *testing.T) discovery.Attempt {
	t.Helper()
	seeds, digest, err := discovery.CanonicalizeSeeds([]string{"192.0.2.10", "192.0.2.11"})
	require.NoError(t, err)
	return discovery.Attempt{
		ClusterUID: "uid", Generation: 1, MarkerVersion: api.LegacyRFDiscoveryMarkerVersion,
		ProtocolVersion: api.LegacyRFDiscoveryProtocolVersion, AttemptID: "attempt",
		OrderedSeeds: seeds, SeedDigest: digest,
		WorkerImageDigest: "registry.example/operator@sha256:" + strings.Repeat("a", 64),
		Connection:        discovery.Connection{ExpectedClusterName: "legacy"},
	}
}

func validExecutableCandidate() discovery.Candidate {
	observation := discovery.SourceObservation{
		ClusterName: "legacy", ServerType: api.ServerDistributionCassandra, SourceVersion: "4.0.0",
		Partitioner: "org.apache.cassandra.dht.Murmur3Partitioner", SchemaVersion: "schema",
		Topology: []discovery.TopologyHost{
			{Address: netip.MustParseAddr("192.0.2.10"), HostID: "host-a", Datacenter: "legacy-a"},
			{Address: netip.MustParseAddr("192.0.2.11"), HostID: "host-b", Datacenter: "legacy-b"},
			{Address: netip.MustParseAddr("192.0.2.12"), HostID: "host-e", Datacenter: "legacy-e"},
		},
	}
	strategy := api.NetworkTopologyStrategyQualifiedClass
	return discovery.Candidate{Before: observation, After: observation, Replication: discovery.SystemKeyspaceObservations{
		SystemAuth:        discovery.KeyspaceObservation{Present: true, Strategy: strategy, Replication: map[string]int32{"legacy-a": 2, "legacy-b": 9, "legacy-e": 10}},
		SystemTraces:      discovery.KeyspaceObservation{Present: true, Strategy: strategy, Replication: map[string]int32{"legacy-a": 2}},
		SystemDistributed: discovery.KeyspaceObservation{Present: true, Strategy: strategy, Replication: map[string]int32{"legacy-b": 2}},
	}}
}
