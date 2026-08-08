package k8ssandra

import (
	"context"
	"net/netip"
	"testing"
	"time"

	api "github.com/k8ssandra/k8ssandra-operator/apis/k8ssandra/v1alpha1"
	"github.com/k8ssandra/k8ssandra-operator/pkg/discovery"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func TestSignedTerminalWorkerFailurePropagatesToControllerStatus(t *testing.T) {
	reasons := []api.LegacyRFDiscoveryReason{
		api.LegacyRFReasonDiscoveryDeadlineExceeded,
		api.LegacyRFReasonAuthenticationRejected,
		api.LegacyRFReasonTLSFailed,
		api.LegacyRFReasonContactUnreachable,
		api.LegacyRFReasonIdentityMismatch,
		api.LegacyRFReasonTopologyInconsistent,
		api.LegacyRFReasonSchemaDisagreement,
	}
	for _, reason := range reasons {
		t.Run(string(reason), func(t *testing.T) {
			manager, direct, attempt := discoveryAttemptsWithEnsuredResources(t)
			resultConfigMap := findResultConfigMap(t, direct)
			hmacSecret := findHMACSecret(t, direct)
			body := signedAttemptFailureResult(t, attempt, hmacSecret.Data[legacyRFHMACDataKey], reason)
			current := &corev1.ConfigMap{}
			require.NoError(t, direct.Get(context.Background(), client.ObjectKeyFromObject(resultConfigMap), current))
			current.Data = map[string]string{legacyRFResultDataKey: string(body)}
			require.NoError(t, direct.Update(context.Background(), current))

			result, err := manager.Result(context.Background(), attempt)

			require.Nil(t, result)
			var boundary *discovery.BoundaryError
			require.ErrorAs(t, err, &boundary)
			require.Equal(t, reason, boundary.PublicFailure().Reason)
			cluster := safetyCluster()
			ApplyLegacyRFDiscoveryDecision(cluster, legacyRFErrorDecision(err), metav1.NewTime(time.Unix(100, 0)), nil)
			require.Equal(t, api.LegacyRFDiscoveryPhaseBlocked, cluster.Status.LegacyRFDiscovery.Phase)
			require.Equal(t, reason, cluster.Status.LegacyRFDiscovery.Reason)
		})
	}
}

func signedAttemptFailureResult(
	t *testing.T,
	attempt discovery.Attempt,
	key []byte,
	reason api.LegacyRFDiscoveryReason,
) []byte {
	t.Helper()
	failure, found := api.LegacyRFDiscoveryFailureForReason(reason)
	require.True(t, found)
	trace := make([]discovery.EndpointAttemptSummary, len(attempt.OrderedSeeds))
	if reason == api.LegacyRFReasonDiscoveryDeadlineExceeded {
		trace[0] = discovery.EndpointAttemptSummary{
			AttemptIndex: 0, Endpoint: attempt.OrderedSeeds[0], Outcome: discovery.EndpointAttemptFailed, Reason: reason,
		}
		for index := 1; index < len(attempt.OrderedSeeds); index++ {
			trace[index] = discovery.EndpointAttemptSummary{
				AttemptIndex: index, Endpoint: attempt.OrderedSeeds[index], Outcome: discovery.EndpointAttemptSkipped,
			}
		}
	} else {
		for index, endpoint := range attempt.OrderedSeeds {
			trace[index] = discovery.EndpointAttemptSummary{
				AttemptIndex: index, Endpoint: endpoint, Outcome: discovery.EndpointAttemptFailed, Reason: reason,
			}
		}
	}
	result := discovery.DiscoveryResult{
		SchemaVersion: attempt.ProtocolVersion, ClusterUID: attempt.ClusterUID, Generation: attempt.Generation,
		MarkerVersion: attempt.MarkerVersion, ProtocolVersion: attempt.ProtocolVersion, AttemptID: attempt.AttemptID,
		OrderedSeeds: append([]netip.AddrPort(nil), attempt.OrderedSeeds...), SeedDigest: attempt.SeedDigest,
		SecretBindings: attempt.Connection.SecretBindings, WorkerImageDigest: attempt.WorkerImageDigest,
		Failure: &failure, AttemptTrace: trace,
	}
	body, err := discovery.SignResult(result, key, api.LegacyRFDiscoveryMaxResultBytes)
	require.NoError(t, err)
	return body
}
