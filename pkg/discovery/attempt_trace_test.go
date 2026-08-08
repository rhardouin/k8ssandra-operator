package discovery

import (
	"context"
	"encoding/json"
	"errors"
	"net/netip"
	"testing"

	api "github.com/k8ssandra/k8ssandra-operator/apis/k8ssandra/v1alpha1"
	"github.com/stretchr/testify/require"
)

func TestWorkerAttemptTraceProvesFailedAcceptedAndSkippedEndpoints(t *testing.T) {
	authFailure := NewBoundaryError(api.LegacyRFReasonAuthenticationRejected, errors.New("password=do-not-persist"))
	observer := &recordingObserver{replies: []observerReply{
		{err: authFailure},
		{candidate: validCandidate("4.0.0", map[string]int32{"legacy-b": 9})},
	}}
	attempt := validAttempt(t)

	result, err := newTestWorker(t, observer).Discover(context.Background(), attempt)

	require.NoError(t, err)
	require.Equal(t, []EndpointAttemptSummary{
		{AttemptIndex: 0, Endpoint: attempt.OrderedSeeds[0], Outcome: EndpointAttemptFailed, Reason: api.LegacyRFReasonAuthenticationRejected},
		{AttemptIndex: 1, Endpoint: attempt.OrderedSeeds[1], Outcome: EndpointAttemptAccepted},
		{AttemptIndex: 2, Endpoint: attempt.OrderedSeeds[2], Outcome: EndpointAttemptSkipped},
	}, result.AttemptTrace)
	encoded, err := json.Marshal(result.AttemptTrace)
	require.NoError(t, err)
	require.NotContains(t, string(encoded), "do-not-persist")
}

func TestWorkerAttemptTraceRecordsBoundedSanitizedAllFailureOutcome(t *testing.T) {
	observer := &recordingObserver{replies: []observerReply{
		{err: errors.New("dsn=cassandra://secret-a")},
		{err: errors.New("password=secret-b")},
		{err: errors.New("certificate=secret-c")},
	}}
	attempt := validAttempt(t)

	result, err := newTestWorker(t, observer).Discover(context.Background(), attempt)

	require.NoError(t, err)
	require.NotNil(t, result.Failure)
	require.Equal(t, api.LegacyRFReasonContactUnreachable, result.Failure.Reason)
	require.Len(t, result.AttemptTrace, len(attempt.OrderedSeeds))
	for index, summary := range result.AttemptTrace {
		require.Equal(t, index, summary.AttemptIndex)
		require.Equal(t, attempt.OrderedSeeds[index], summary.Endpoint)
		require.Equal(t, EndpointAttemptFailed, summary.Outcome)
		require.NotEmpty(t, summary.Reason)
	}
	encoded, marshalErr := json.Marshal(result.AttemptTrace)
	require.NoError(t, marshalErr)
	require.NotContains(t, string(encoded), "secret-")
}

func TestDiscoveryResultRejectsUnboundedOrNonPublicAttemptTrace(t *testing.T) {
	result, _ := validProtocolResult(t)
	result.OrderedSeeds = make([]netip.AddrPort, MaximumAttemptTraceEntries+1)
	result.AttemptTrace = make([]EndpointAttemptSummary, MaximumAttemptTraceEntries+1)

	_, err := SignResult(result, []byte("signing-key"), api.LegacyRFDiscoveryMaxResultBytes)
	require.ErrorContains(t, err, "exceeds 256 entries")

	result, _ = validProtocolResult(t)
	result.Authoritative.AttemptIndex = 1
	result.Authoritative.Endpoint = result.OrderedSeeds[1]
	result.AttemptTrace[0] = EndpointAttemptSummary{AttemptIndex: 0, Endpoint: result.OrderedSeeds[0], Outcome: EndpointAttemptFailed,
		Reason: api.LegacyRFDiscoveryReason("password=do-not-persist")}
	result.AttemptTrace[1] = EndpointAttemptSummary{AttemptIndex: 1, Endpoint: result.OrderedSeeds[1], Outcome: EndpointAttemptAccepted}

	_, err = SignResult(result, []byte("signing-key"), api.LegacyRFDiscoveryMaxResultBytes)
	require.ErrorContains(t, err, "attempt trace outcome is invalid")
	require.NotContains(t, err.Error(), "do-not-persist")
}
