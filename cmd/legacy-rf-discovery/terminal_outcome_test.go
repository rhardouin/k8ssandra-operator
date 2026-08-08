package main

import (
	"bytes"
	"context"
	"errors"
	"net/netip"
	"os"
	"path/filepath"
	"testing"
	"time"

	api "github.com/k8ssandra/k8ssandra-operator/apis/k8ssandra/v1alpha1"
	"github.com/k8ssandra/k8ssandra-operator/pkg/discovery"
	"github.com/stretchr/testify/require"
)

type deadlineObserver struct{}

func (deadlineObserver) DiscoverCandidate(
	ctx context.Context,
	_ netip.AddrPort,
	_ discovery.Connection,
) (discovery.Candidate, error) {
	<-ctx.Done()
	return discovery.Candidate{}, ctx.Err()
}

func TestRunPublishesSignedTerminalCQLFailuresAndExitsSuccessfully(t *testing.T) {
	reasons := []api.LegacyRFDiscoveryReason{
		api.LegacyRFReasonAuthenticationRejected,
		api.LegacyRFReasonTLSFailed,
		api.LegacyRFReasonContactUnreachable,
		api.LegacyRFReasonIdentityMismatch,
		api.LegacyRFReasonTopologyInconsistent,
		api.LegacyRFReasonSchemaDisagreement,
	}
	for _, reason := range reasons {
		t.Run(string(reason), func(t *testing.T) {
			directory := t.TempDir()
			attempt := validExecutableAttempt(t)
			key := bytes.Repeat([]byte{7}, resultSigningKeyLen)
			paths := runPaths{
				attempt: filepath.Join(directory, "attempt.json"), hmacKey: filepath.Join(directory, "hmac-key"),
				resultNamespace: "worker-ns", resultConfigMap: "attempt-result", resultKey: resultConfigMapKey,
			}
			writeTerminalOutcomeInputs(t, paths, attempt, key)
			publisher := &fakeResultPublisher{}
			observer := &fakeObserver{err: discovery.NewBoundaryError(reason, errors.New("private-canary"))}
			dependencies := executableDependencies{
				newObserver: func(discovery.CQLObserverOptions) (discovery.EndpointObserver, error) { return observer, nil },
				clock:       testClock{now: time.Date(2099, 1, 1, 0, 0, 0, 0, time.UTC)}, publisher: publisher,
			}
			var stdout, stderr bytes.Buffer

			exitCode := run(context.Background(), paths.arguments(), &stdout, &stderr, dependencies)

			require.Zero(t, exitCode)
			require.Empty(t, stdout.String())
			require.Empty(t, stderr.String())
			require.Len(t, publisher.bodies, 1)
			result, err := discovery.ValidateResultEnvelope(publisher.bodies[0], key, expectedExecutableResult(attempt))
			require.NoError(t, err)
			require.Nil(t, result.Authoritative)
			require.NotNil(t, result.Failure)
			require.Equal(t, reason, result.Failure.Reason)
			require.Len(t, result.AttemptTrace, len(attempt.OrderedSeeds))
			require.NotContains(t, string(publisher.bodies[0]), "private-canary")
		})
	}
}

func TestRunFailsWhenTerminalFailureCannotBePublished(t *testing.T) {
	directory := t.TempDir()
	attempt := validExecutableAttempt(t)
	key := bytes.Repeat([]byte{7}, resultSigningKeyLen)
	paths := runPaths{
		attempt: filepath.Join(directory, "attempt.json"), hmacKey: filepath.Join(directory, "hmac-key"),
		resultNamespace: "worker-ns", resultConfigMap: "attempt-result", resultKey: resultConfigMapKey,
	}
	writeTerminalOutcomeInputs(t, paths, attempt, key)
	publisher := &fakeResultPublisher{err: errors.New("Kubernetes unavailable")}
	dependencies := executableDependencies{
		newObserver: func(discovery.CQLObserverOptions) (discovery.EndpointObserver, error) {
			return &fakeObserver{err: discovery.NewBoundaryError(api.LegacyRFReasonAuthenticationRejected, errors.New("rejected"))}, nil
		},
		clock: testClock{now: time.Date(2099, 1, 1, 0, 0, 0, 0, time.UTC)}, publisher: publisher,
	}
	var stdout, stderr bytes.Buffer

	exitCode := run(context.Background(), paths.arguments(), &stdout, &stderr, dependencies)

	require.Equal(t, 1, exitCode)
	require.Len(t, publisher.bodies, 1)
}

func TestRunPublishesInternalDiscoveryDeadlineAndExitsSuccessfully(t *testing.T) {
	directory := t.TempDir()
	attempt := validExecutableAttempt(t)
	key := bytes.Repeat([]byte{7}, resultSigningKeyLen)
	paths := runPaths{
		attempt: filepath.Join(directory, "attempt.json"), hmacKey: filepath.Join(directory, "hmac-key"),
		resultNamespace: "worker-ns", resultConfigMap: "attempt-result", resultKey: resultConfigMapKey,
	}
	writeTerminalOutcomeInputs(t, paths, attempt, key)
	publisher := &fakeResultPublisher{}
	dependencies := executableDependencies{
		newObserver: func(discovery.CQLObserverOptions) (discovery.EndpointObserver, error) { return deadlineObserver{}, nil },
		clock:       discovery.RealClock{}, publisher: publisher,
	}
	var stdout, stderr bytes.Buffer
	arguments := append(paths.arguments(), "--overall-timeout", "1ms")

	exitCode := run(context.Background(), arguments, &stdout, &stderr, dependencies)

	require.Zero(t, exitCode)
	require.Empty(t, stdout.String())
	require.Empty(t, stderr.String())
	require.Len(t, publisher.bodies, 1)
	result, err := discovery.ValidateResultEnvelope(publisher.bodies[0], key, expectedExecutableResult(attempt))
	require.NoError(t, err)
	require.NotNil(t, result.Failure)
	require.Equal(t, api.LegacyRFReasonDiscoveryDeadlineExceeded, result.Failure.Reason)
	require.Equal(t, []discovery.EndpointAttemptSummary{
		{AttemptIndex: 0, Endpoint: attempt.OrderedSeeds[0], Outcome: discovery.EndpointAttemptFailed, Reason: api.LegacyRFReasonDiscoveryDeadlineExceeded},
		{AttemptIndex: 1, Endpoint: attempt.OrderedSeeds[1], Outcome: discovery.EndpointAttemptSkipped},
	}, result.AttemptTrace)
}

func expectedExecutableResult(attempt discovery.Attempt) discovery.ExpectedResult {
	return discovery.ExpectedResult{
		SchemaVersion: attempt.ProtocolVersion, ClusterUID: attempt.ClusterUID, Generation: attempt.Generation,
		MarkerVersion: attempt.MarkerVersion, ProtocolVersion: attempt.ProtocolVersion, AttemptID: attempt.AttemptID,
		OrderedSeeds: attempt.OrderedSeeds, SeedDigest: attempt.SeedDigest,
		SecretBindings: attempt.Connection.SecretBindings, WorkerImageDigest: attempt.WorkerImageDigest,
		MaximumBytes: api.LegacyRFDiscoveryMaxResultBytes,
	}
}

func writeTerminalOutcomeInputs(t *testing.T, paths runPaths, attempt discovery.Attempt, key []byte) {
	t.Helper()
	require.Equal(t, paths.attempt, writeAttempt(t, filepath.Dir(paths.attempt), attempt))
	require.NoError(t, os.WriteFile(paths.hmacKey, key, 0o600))
}
