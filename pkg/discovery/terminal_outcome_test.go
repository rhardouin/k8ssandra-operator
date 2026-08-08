package discovery

import (
	"context"
	"errors"
	"net/netip"
	"testing"

	api "github.com/k8ssandra/k8ssandra-operator/apis/k8ssandra/v1alpha1"
	"github.com/stretchr/testify/require"
)

func TestWorkerReturnsExhaustedFallbackAsTerminalFailureData(t *testing.T) {
	attempt := validAttempt(t)
	observer := &recordingObserver{replies: []observerReply{
		{err: errors.New("network unavailable")},
		{err: NewBoundaryError(api.LegacyRFReasonIdentityMismatch, errors.New("wrong cluster"))},
		{err: NewBoundaryError(api.LegacyRFReasonTLSFailed, errors.New("certificate rejected"))},
	}}

	result, err := newTestWorker(t, observer).Discover(context.Background(), attempt)

	require.NoError(t, err)
	require.Nil(t, result.Authoritative)
	require.NotNil(t, result.Failure)
	require.Equal(t, api.LegacyRFReasonIdentityMismatch, result.Failure.Reason,
		"a permanent incompatibility must dominate retryable endpoint failures")
	require.Len(t, result.AttemptTrace, len(attempt.OrderedSeeds))
}

func TestSignedDiscoveryResultRoundTripsExactlyOneTerminalOutcome(t *testing.T) {
	key := []byte("unit-test-signing-key")
	success, expected := validProtocolResult(t)
	failure, found := api.LegacyRFDiscoveryFailureForReason(api.LegacyRFReasonAuthenticationRejected)
	require.True(t, found)
	success.Authoritative = nil
	success.Failure = &failure
	success.AttemptTrace = failedAttemptTrace(success.OrderedSeeds, api.LegacyRFReasonAuthenticationRejected)

	body, err := SignResult(success, key, api.LegacyRFDiscoveryMaxResultBytes)
	require.NoError(t, err)
	actual, err := ValidateResultEnvelope(body, key, expected)
	require.NoError(t, err)
	require.Nil(t, actual.Authoritative)
	require.Equal(t, failure, *actual.Failure)
	require.Equal(t, success.AttemptTrace, actual.AttemptTrace)

	invalid := success
	invalid.Authoritative = &AuthoritativeCandidate{
		AttemptID: success.AttemptID, AttemptIndex: 0, Endpoint: success.OrderedSeeds[0],
	}
	_, err = SignResult(invalid, key, api.LegacyRFDiscoveryMaxResultBytes)
	require.ErrorContains(t, err, "exactly one")

	invalid.Authoritative = nil
	invalid.Failure = nil
	_, err = SignResult(invalid, key, api.LegacyRFDiscoveryMaxResultBytes)
	require.ErrorContains(t, err, "exactly one")
}

func TestSignedDiscoveryResultAcceptsDeadlineFailureWithSkippedFallback(t *testing.T) {
	key := []byte("unit-test-signing-key")
	result, expected := validProtocolResult(t)
	failure, found := api.LegacyRFDiscoveryFailureForReason(api.LegacyRFReasonDiscoveryDeadlineExceeded)
	require.True(t, found)
	result.Authoritative = nil
	result.Failure = &failure
	result.AttemptTrace = []EndpointAttemptSummary{
		{AttemptIndex: 0, Endpoint: result.OrderedSeeds[0], Outcome: EndpointAttemptFailed, Reason: api.LegacyRFReasonContactUnreachable},
		{AttemptIndex: 1, Endpoint: result.OrderedSeeds[1], Outcome: EndpointAttemptFailed, Reason: api.LegacyRFReasonDiscoveryDeadlineExceeded},
		{AttemptIndex: 2, Endpoint: result.OrderedSeeds[2], Outcome: EndpointAttemptSkipped},
	}

	body, err := SignResult(result, key, api.LegacyRFDiscoveryMaxResultBytes)
	require.NoError(t, err)
	actual, err := ValidateResultEnvelope(body, key, expected)
	require.NoError(t, err)
	require.Equal(t, failure, *actual.Failure)
	require.Equal(t, result.AttemptTrace, actual.AttemptTrace)
}

func TestEndpointFailurePrecedenceCoversAuthoritativeSetAndRanksPermanentFirst(t *testing.T) {
	permanent := make(map[api.LegacyRFDiscoveryReason]int)
	retryable := make(map[api.LegacyRFDiscoveryReason]int)
	seenPrecedence := make(map[int]api.LegacyRFDiscoveryReason)
	for reason, precedence := range endpointFailurePrecedenceByReason {
		require.Positive(t, precedence, reason)
		require.NotContains(t, seenPrecedence, precedence, reason)
		seenPrecedence[precedence] = reason
		failure, found := api.LegacyRFDiscoveryFailureForReason(reason)
		require.True(t, found, reason)
		if failure.Retryable {
			retryable[reason] = precedence
		} else {
			permanent[reason] = precedence
		}
	}
	require.NotEmpty(t, permanent)
	require.NotEmpty(t, retryable)
	for permanentReason, permanentPrecedence := range permanent {
		for retryableReason, retryablePrecedence := range retryable {
			require.Less(t, permanentPrecedence, retryablePrecedence,
				"permanent %s must dominate retryable %s", permanentReason, retryableReason)
		}
	}
	_, found := endpointFailurePrecedence(api.LegacyRFReasonInvalidDiscoveryResult)
	require.False(t, found)
}

func failedAttemptTrace(
	endpoints []netip.AddrPort,
	reason api.LegacyRFDiscoveryReason,
) []EndpointAttemptSummary {
	trace := make([]EndpointAttemptSummary, len(endpoints))
	for index, endpoint := range endpoints {
		trace[index] = EndpointAttemptSummary{
			AttemptIndex: index, Endpoint: endpoint, Outcome: EndpointAttemptFailed, Reason: reason,
		}
	}
	return trace
}
