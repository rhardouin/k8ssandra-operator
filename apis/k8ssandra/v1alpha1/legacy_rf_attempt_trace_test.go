package v1alpha1

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLegacyRFSnapshotDeepCopyOwnsAttemptTrace(t *testing.T) {
	snapshot := &LegacyRFSnapshot{AttemptTrace: []LegacyRFEndpointAttemptSummary{
		{AttemptIndex: 0, Endpoint: "192.0.2.1:9042", Outcome: LegacyRFEndpointAttemptFailed, Reason: LegacyRFReasonContactUnreachable},
		{AttemptIndex: 1, Endpoint: "192.0.2.2:9042", Outcome: LegacyRFEndpointAttemptAccepted},
		{AttemptIndex: 2, Endpoint: "192.0.2.3:9042", Outcome: LegacyRFEndpointAttemptSkipped},
	}}

	copy := snapshot.DeepCopy()
	copy.AttemptTrace[0].Endpoint = "changed"

	require.Equal(t, "192.0.2.1:9042", snapshot.AttemptTrace[0].Endpoint)
}
