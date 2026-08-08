package discovery

import (
	"context"
	"errors"
	"math"
	"net/netip"
	"strings"
	"testing"
	"time"

	api "github.com/k8ssandra/k8ssandra-operator/apis/k8ssandra/v1alpha1"
	"github.com/stretchr/testify/require"
)

type fixedClock struct{ now time.Time }

func (clock fixedClock) Now() time.Time { return clock.now }

type observerReply struct {
	candidate Candidate
	err       error
}

type recordingObserver struct {
	replies []observerReply
	calls   []netip.AddrPort
	active  int
	maxOpen int
}

func (observer *recordingObserver) DiscoverCandidate(
	ctx context.Context,
	endpoint netip.AddrPort,
	_ Connection,
) (Candidate, error) {
	observer.calls = append(observer.calls, endpoint)
	observer.active++
	if observer.active > observer.maxOpen {
		observer.maxOpen = observer.active
	}
	defer func() { observer.active-- }()
	if err := ctx.Err(); err != nil {
		return Candidate{}, err
	}
	reply := observer.replies[len(observer.calls)-1]
	return reply.candidate, reply.err
}

func TestCanonicalizeSeeds(t *testing.T) {
	tests := []struct {
		name    string
		raw     []string
		want    []string
		wantErr string
	}{
		{name: "IPv4 and IPv6 preserve order", raw: []string{"192.0.2.1", "2001:db8::1"}, want: []string{"192.0.2.1:9042", "[2001:db8::1]:9042"}},
		{name: "mapped IPv4 deduplicates by first occurrence", raw: []string{"::ffff:192.0.2.1", "192.0.2.1", "192.0.2.2"}, want: []string{"192.0.2.1:9042", "192.0.2.2:9042"}},
		{name: "empty list", raw: nil, wantErr: "at least one"},
		{name: "empty", raw: []string{""}, wantErr: "empty or contain whitespace"},
		{name: "whitespace", raw: []string{" 192.0.2.1"}, wantErr: "whitespace"},
		{name: "FQDN", raw: []string{"seed.example.test"}, wantErr: "IP literal"},
		{name: "port", raw: []string{"192.0.2.1:9042"}, wantErr: "IP literal"},
		{name: "zone", raw: []string{"fe80::1%eth0"}, wantErr: "zone"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			seeds, digest, err := CanonicalizeSeeds(test.raw)
			if test.wantErr != "" {
				require.ErrorContains(t, err, test.wantErr)
				require.Empty(t, seeds)
				require.Empty(t, digest)
				return
			}
			require.NoError(t, err)
			actual := make([]string, len(seeds))
			for index := range seeds {
				actual[index] = seeds[index].String()
			}
			require.Equal(t, test.want, actual)
			require.Len(t, digest, 64)
		})
	}

	_, first, err := CanonicalizeSeeds([]string{"192.0.2.1", "192.0.2.2"})
	require.NoError(t, err)
	_, reordered, err := CanonicalizeSeeds([]string{"192.0.2.2", "192.0.2.1"})
	require.NoError(t, err)
	require.NotEqual(t, first, reordered, "seed digest must be order-sensitive")
}

func TestSeedDigestRejectsNonCanonicalEndpoints(t *testing.T) {
	tests := []netip.AddrPort{
		{},
		netip.MustParseAddrPort("192.0.2.1:9043"),
		netip.AddrPortFrom(netip.MustParseAddr("fe80::1%eth0"), 9042),
	}
	for _, seed := range tests {
		_, err := SeedDigest([]netip.AddrPort{seed})
		require.Error(t, err)
	}
}

func TestParseReplicationFactor(t *testing.T) {
	tests := []struct {
		raw     string
		want    int32
		wantErr bool
	}{
		{raw: "1", want: 1}, {raw: "9", want: 9}, {raw: "10", want: 10},
		{raw: "2147483647", want: math.MaxInt32},
		{raw: "", wantErr: true}, {raw: "0", wantErr: true}, {raw: "-1", wantErr: true},
		{raw: "+1", wantErr: true}, {raw: "1.0", wantErr: true}, {raw: " 1", wantErr: true},
		{raw: "١", wantErr: true}, {raw: "2147483648", wantErr: true},
	}
	for _, test := range tests {
		t.Run(test.raw, func(t *testing.T) {
			actual, err := ParseReplicationFactor(test.raw)
			if test.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, test.want, actual)
		})
	}
}

func TestCanonicalizeNetworkTopologyStrategy(t *testing.T) {
	for _, strategy := range []string{api.NetworkTopologyStrategyClass, api.NetworkTopologyStrategyQualifiedClass} {
		actual, err := CanonicalizeNetworkTopologyStrategy(strategy)
		require.NoError(t, err)
		require.Equal(t, api.NetworkTopologyStrategyQualifiedClass, actual)
	}
	for _, strategy := range []string{"", "SimpleStrategy", "networktopologystrategy"} {
		_, err := CanonicalizeNetworkTopologyStrategy(strategy)
		require.Error(t, err)
	}
}

func TestBindingsAndObservationFingerprintsAreCanonical(t *testing.T) {
	left := []SecretBinding{
		{Purpose: "tls", SourceContext: "source", Namespace: "ns", Name: "tls", Keys: []string{"key", "cert"}, ResourceVersion: "2"},
		{Purpose: "auth", SourceContext: "source", Namespace: "ns", Name: "auth", Keys: []string{"username", "password"}, ResourceVersion: "1"},
	}
	right := []SecretBinding{left[1], left[0]}
	right[0].Keys = []string{"password", "username"}
	leftDigest, err := BindingsDigest(left)
	require.NoError(t, err)
	rightDigest, err := BindingsDigest(right)
	require.NoError(t, err)
	require.Equal(t, leftDigest, rightDigest)
	require.Equal(t, []string{"key", "cert"}, left[0].Keys, "canonicalization must not mutate callers")
	_, err = BindingsDigest([]SecretBinding{{Purpose: "auth", Namespace: "ns", Name: "auth"}})
	require.Error(t, err)

	observation := validCandidate("4.0.0", map[string]int32{"legacy-b": 10}).Before
	reordered := observation
	reordered.Topology = []TopologyHost{observation.Topology[1], observation.Topology[0]}
	first, err := FingerprintObservation(observation)
	require.NoError(t, err)
	second, err := FingerprintObservation(reordered)
	require.NoError(t, err)
	require.Equal(t, first, second)
	duplicate := observation
	duplicate.Topology = append(duplicate.Topology, duplicate.Topology[0])
	_, err = FingerprintObservation(duplicate)
	require.Error(t, err)
}

func TestBoundaryErrorIsTypedSanitizedAndUnwraps(t *testing.T) {
	secretCause := errors.New("password=hunter2")
	err := NewBoundaryError(api.LegacyRFReasonAuthenticationRejected, secretCause)
	require.ErrorIs(t, err, secretCause)
	require.NotContains(t, err.Error(), "hunter2")
	require.Equal(t, api.LegacyRFReasonAuthenticationRejected, err.PublicFailure().Reason)
	unknown := NewBoundaryError(api.LegacyRFDiscoveryReason("unknown"), nil)
	require.Equal(t, api.LegacyRFReasonInvalidDiscoveryResult, unknown.PublicFailure().Reason)
}

func TestResultEnvelopeRoundTripAndRejections(t *testing.T) {
	key := []byte("unit-test-signing-key")
	result, expected := validProtocolResult(t)
	body, err := SignResult(result, key, api.LegacyRFDiscoveryMaxResultBytes)
	require.NoError(t, err)
	actual, err := ValidateResultEnvelope(body, key, expected)
	require.NoError(t, err)
	require.Equal(t, result.AttemptID, actual.AttemptID)
	require.NotEmpty(t, actual.CanonicalHash)

	tests := []struct {
		name   string
		body   func() []byte
		key    []byte
		mutate func(*ExpectedResult)
		reason api.LegacyRFDiscoveryReason
	}{
		{name: "forged MAC", body: func() []byte { copy := append([]byte(nil), body...); copy[len(copy)-2] ^= 1; return copy }, key: key, reason: api.LegacyRFReasonInvalidDiscoveryResult},
		{name: "wrong key", body: func() []byte { return body }, key: []byte("wrong"), reason: api.LegacyRFReasonForgedDiscoveryResult},
		{name: "empty key", body: func() []byte { return body }, reason: api.LegacyRFReasonForgedDiscoveryResult},
		{name: "truncated", body: func() []byte { return body[:len(body)/2] }, key: key, reason: api.LegacyRFReasonInvalidDiscoveryResult},
		{name: "trailing data", body: func() []byte { return append(append([]byte(nil), body...), '\n') }, key: key, reason: api.LegacyRFReasonInvalidDiscoveryResult},
		{name: "oversize", body: func() []byte {
			return append(append([]byte(nil), body...), make([]byte, api.LegacyRFDiscoveryMaxResultBytes)...)
		}, key: key, reason: api.LegacyRFReasonDiscoveryResultTooLarge},
		{name: "generation stale", body: func() []byte { return body }, key: key, mutate: func(value *ExpectedResult) { value.Generation++ }, reason: api.LegacyRFReasonStaleDiscoveryResult},
		{name: "seed order stale", body: func() []byte { return body }, key: key, mutate: func(value *ExpectedResult) {
			value.OrderedSeeds[0], value.OrderedSeeds[1] = value.OrderedSeeds[1], value.OrderedSeeds[0]
		}, reason: api.LegacyRFReasonStaleDiscoveryResult},
		{name: "digest stale", body: func() []byte { return body }, key: key, mutate: func(value *ExpectedResult) { value.SeedDigest = strings.Repeat("0", 64) }, reason: api.LegacyRFReasonStaleDiscoveryResult},
		{name: "binding stale", body: func() []byte { return body }, key: key, mutate: func(value *ExpectedResult) { value.SecretBindings[0].ResourceVersion = "changed" }, reason: api.LegacyRFReasonStaleDiscoveryResult},
		{name: "worker stale", body: func() []byte { return body }, key: key, mutate: func(value *ExpectedResult) { value.WorkerImageDigest = "sha256:changed" }, reason: api.LegacyRFReasonStaleDiscoveryResult},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			want := expected
			want.OrderedSeeds = append([]netip.AddrPort(nil), expected.OrderedSeeds...)
			want.SecretBindings = append([]SecretBinding(nil), expected.SecretBindings...)
			if test.mutate != nil {
				test.mutate(&want)
			}
			_, err := ValidateResultEnvelope(test.body(), test.key, want)
			var boundary *BoundaryError
			require.ErrorAs(t, err, &boundary)
			require.Equal(t, test.reason, boundary.PublicFailure().Reason)
			require.NotContains(t, err.Error(), string(body))
			require.NotContains(t, err.Error(), string(key))
		})
	}
	_, err = SignResult(result, nil, 0)
	require.Error(t, err)
	_, err = SignResult(result, key, 1)
	require.Error(t, err)
}

func TestNewWorkerRejectsInvalidDependenciesAndBounds(t *testing.T) {
	validObserver := &recordingObserver{}
	validClock := fixedClock{now: time.Date(2099, 1, 1, 0, 0, 0, 0, time.UTC)}
	validTimeout := time.Minute
	tests := []struct {
		observer EndpointObserver
		clock    Clock
		timeout  time.Duration
	}{
		{clock: validClock, timeout: validTimeout},
		{observer: validObserver, timeout: validTimeout},
		{observer: validObserver, clock: validClock},
	}
	for _, test := range tests {
		_, err := NewWorker(test.observer, test.clock, test.timeout)
		require.Error(t, err)
	}
}

func TestWorkerEnforcesSharedSeedLimit(t *testing.T) {
	worker, err := NewWorker(&recordingObserver{}, fixedClock{now: time.Now()}, time.Minute)
	require.NoError(t, err)
	attempt := validAttempt(t)
	seeds := make([]netip.AddrPort, api.LegacyRFDiscoveryMaxSeeds+1)
	for index := range seeds {
		seeds[index] = netip.AddrPortFrom(netip.AddrFrom4([4]byte{192, 0, 2, byte(index + 1)}), defaultCQLPort)
	}

	attempt.OrderedSeeds = seeds[:api.LegacyRFDiscoveryMaxSeeds]
	attempt.SeedDigest, err = SeedDigest(attempt.OrderedSeeds)
	require.NoError(t, err)
	require.NoError(t, worker.validateAttempt(attempt))

	attempt.OrderedSeeds = seeds
	attempt.SeedDigest, err = SeedDigest(attempt.OrderedSeeds)
	require.NoError(t, err)
	require.ErrorContains(t, worker.validateAttempt(attempt), "between 1 and 32")
}

func TestWorkerOrderedFallbackAuthority(t *testing.T) {
	auth := NewBoundaryError(api.LegacyRFReasonAuthenticationRejected, errors.New("private auth response"))
	tls := NewBoundaryError(api.LegacyRFReasonTLSFailed, errors.New("private TLS response"))
	wrongCluster := validCandidate("4.0.0", map[string]int32{"legacy-b": 10})
	wrongCluster.Before.ClusterName, wrongCluster.After.ClusterName = "other", "other"
	unstable := validCandidate("4.0.0", map[string]int32{"legacy-b": 10})
	unstable.After.SchemaVersion = "schema-2"

	tests := []struct {
		name        string
		replies     []observerReply
		wantCalls   int
		wantFailure []api.LegacyRFDiscoveryReason
		wantRF      int32
		wantErr     api.LegacyRFDiscoveryReason
	}{
		{name: "first success leaves later seeds unopened", replies: []observerReply{{candidate: validCandidate("4.0.0", map[string]int32{"legacy-b": 9})}}, wantCalls: 1, wantRF: 9},
		{name: "first failure then second success", replies: []observerReply{{err: auth}, {candidate: validCandidate("4.1.2", map[string]int32{"legacy-b": 10})}}, wantCalls: 2, wantFailure: []api.LegacyRFDiscoveryReason{api.LegacyRFReasonAuthenticationRejected}, wantRF: 10},
		{name: "multiple failures then success", replies: []observerReply{{err: errors.New("dial")}, {err: tls}, {candidate: validCandidate("5.0.0", map[string]int32{})}}, wantCalls: 3, wantFailure: []api.LegacyRFDiscoveryReason{api.LegacyRFReasonContactUnreachable, api.LegacyRFReasonTLSFailed}},
		{name: "wrong cluster falls back", replies: []observerReply{{candidate: wrongCluster}, {candidate: validCandidate("4.0.0", map[string]int32{"legacy-b": 1})}}, wantCalls: 2, wantFailure: []api.LegacyRFDiscoveryReason{api.LegacyRFReasonIdentityMismatch}, wantRF: 1},
		{name: "Cassandra 3.x rejected then 4.0 accepted", replies: []observerReply{{candidate: validCandidate("3.11.17", map[string]int32{"legacy-b": 2})}, {candidate: validCandidate("4.0.0", map[string]int32{"legacy-b": 9})}}, wantCalls: 2, wantFailure: []api.LegacyRFDiscoveryReason{api.LegacyRFReasonUnsupportedSourceVersion}, wantRF: 9},
		{name: "schema instability falls back", replies: []observerReply{{candidate: unstable}, {candidate: validCandidate("4.0.1", map[string]int32{"legacy-b": 10})}}, wantCalls: 2, wantFailure: []api.LegacyRFDiscoveryReason{api.LegacyRFReasonSchemaDisagreement}, wantRF: 10},
		{name: "all fail deterministically", replies: []observerReply{{err: errors.New("dial")}, {err: tls}, {err: auth}}, wantCalls: 3, wantFailure: []api.LegacyRFDiscoveryReason{api.LegacyRFReasonContactUnreachable, api.LegacyRFReasonTLSFailed, api.LegacyRFReasonAuthenticationRejected}, wantErr: api.LegacyRFReasonAuthenticationRejected},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			observer := &recordingObserver{replies: test.replies}
			worker := newTestWorker(t, observer)
			attempt := validAttempt(t)
			result, err := worker.Discover(context.Background(), attempt)
			require.Len(t, observer.calls, test.wantCalls)
			require.Equal(t, 1, observer.maxOpen, "only one endpoint session may be active")
			require.Zero(t, observer.active, "the active endpoint call must always close")
			for index, endpoint := range observer.calls {
				require.Equal(t, attempt.OrderedSeeds[index], endpoint, "each query must stay pinned to its attempted endpoint")
			}
			actualFailures := make([]api.LegacyRFDiscoveryReason, 0, len(result.AttemptTrace))
			for _, summary := range result.AttemptTrace {
				if summary.Outcome == EndpointAttemptFailed {
					actualFailures = append(actualFailures, summary.Reason)
				}
			}
			if len(test.wantFailure) == 0 {
				require.Empty(t, actualFailures)
			} else {
				require.Equal(t, test.wantFailure, actualFailures)
			}
			if test.wantErr != "" {
				require.NoError(t, err)
				require.NotNil(t, result.Failure)
				require.Equal(t, test.wantErr, result.Failure.Reason)
				require.Nil(t, result.Authoritative)
				return
			}
			require.NoError(t, err)
			require.NotNil(t, result.Authoritative)
			require.Equal(t, test.wantCalls-1, result.Authoritative.AttemptIndex)
			require.Equal(t, attempt.OrderedSeeds[test.wantCalls-1], result.Authoritative.Endpoint)
			require.Equal(t, test.wantRF, result.Authoritative.Replication.SystemAuth.Replication["legacy-b"])
		})
	}
}

func TestWorkerInvalidCandidateBoundaryFallsBackWithoutMerging(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*Candidate)
		reason api.LegacyRFDiscoveryReason
	}{
		{name: "unsupported server", mutate: func(candidate *Candidate) {
			candidate.Before.ServerType, candidate.After.ServerType = api.ServerDistributionDse, api.ServerDistributionDse
		}, reason: api.LegacyRFReasonUnsupportedServerType},
		{name: "incomplete identity", mutate: func(candidate *Candidate) {
			candidate.Before.Partitioner, candidate.After.Partitioner = "", ""
		}, reason: api.LegacyRFReasonTopologyInconsistent},
		{name: "conflicting topology", mutate: func(candidate *Candidate) {
			candidate.Before.Topology[1].Address = candidate.Before.Topology[0].Address
			candidate.After.Topology[1].Address = candidate.After.Topology[0].Address
		}, reason: api.LegacyRFReasonTopologyInconsistent},
		{name: "duplicate host ID in one datacenter", mutate: func(candidate *Candidate) {
			candidate.Before.Topology[1].Datacenter = candidate.Before.Topology[0].Datacenter
			candidate.After.Topology[1].Datacenter = candidate.After.Topology[0].Datacenter
			candidate.Before.Topology[1].HostID = candidate.Before.Topology[0].HostID
			candidate.After.Topology[1].HostID = candidate.After.Topology[0].HostID
		}, reason: api.LegacyRFReasonTopologyInconsistent},
		{name: "managed name collision", mutate: func(candidate *Candidate) {
			candidate.Before.Topology[0].Datacenter, candidate.After.Topology[0].Datacenter = "managed", "managed"
		}, reason: api.LegacyRFReasonManagedDatacenterNameCollision},
		{name: "missing keyspace", mutate: func(candidate *Candidate) {
			candidate.Replication.SystemAuth.Present = false
		}, reason: api.LegacyRFReasonMissingKeyspace},
		{name: "unsupported strategy", mutate: func(candidate *Candidate) {
			candidate.Replication.SystemTraces.Strategy = "SimpleStrategy"
		}, reason: api.LegacyRFReasonUnsupportedStrategy},
		{name: "null replication", mutate: func(candidate *Candidate) {
			candidate.Replication.SystemDistributed.Replication = nil
		}, reason: api.LegacyRFReasonInvalidReplication},
		{name: "invalid RF", mutate: func(candidate *Candidate) {
			candidate.Replication.SystemAuth.Replication["legacy-a"] = 0
		}, reason: api.LegacyRFReasonInvalidReplication},
		{name: "unknown system auth replication datacenter", mutate: func(candidate *Candidate) {
			candidate.Replication.SystemAuth.Replication["unknown"] = 1
		}, reason: api.LegacyRFReasonInvalidReplication},
		{name: "unknown system traces replication datacenter", mutate: func(candidate *Candidate) {
			candidate.Replication.SystemTraces.Replication["unknown"] = 1
		}, reason: api.LegacyRFReasonInvalidReplication},
		{name: "unknown system distributed replication datacenter", mutate: func(candidate *Candidate) {
			candidate.Replication.SystemDistributed.Replication["unknown"] = 1
		}, reason: api.LegacyRFReasonInvalidReplication},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			invalid := validCandidate("4.0.0", map[string]int32{"legacy-a": 2})
			test.mutate(&invalid)
			authoritative := validCandidate("4.0.0", map[string]int32{"legacy-b": 10})
			observer := &recordingObserver{replies: []observerReply{{candidate: invalid}, {candidate: authoritative}}}
			result, err := newTestWorker(t, observer).Discover(context.Background(), validAttempt(t))
			require.NoError(t, err)
			require.Len(t, observer.calls, 2)
			require.Equal(t, test.reason, result.AttemptTrace[0].Reason)
			require.EqualValues(t, 10, result.Authoritative.Replication.SystemAuth.Replication["legacy-b"])
			require.NotContains(t, result.Authoritative.Replication.SystemAuth.Replication, "legacy-a", "invalid candidate data must not merge into authority")
		})
	}
}

func TestValidateTopologyUsesCassandraDatacenterNameNotResourceName(t *testing.T) {
	hosts := []TopologyHost{{
		Address: netip.MustParseAddr("192.0.2.10"), HostID: "host-a", Datacenter: "managed-resource",
	}}
	managed := []ManagedLocation{{Name: "managed-resource", DatacenterName: "managed-logical"}}

	datacenters, err := validateTopology(hosts, managed)

	require.NoError(t, err)
	require.Equal(t, []string{"managed-resource"}, datacenters,
		"a source DC named like the Kubernetes resource is external when the Cassandra DC override differs")
}

func TestWorkerRejectsInvalidAttemptsAndHonorsCancellation(t *testing.T) {
	observer := &recordingObserver{replies: []observerReply{{candidate: validCandidate("4.0.0", nil)}}}
	worker := newTestWorker(t, observer)
	invalid := validAttempt(t)
	invalid.SeedDigest = "wrong"
	_, err := worker.Discover(context.Background(), invalid)
	var boundary *BoundaryError
	require.ErrorAs(t, err, &boundary)
	require.Equal(t, api.LegacyRFReasonInvalidContactPoint, boundary.PublicFailure().Reason)
	require.Empty(t, observer.calls)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = worker.Discover(ctx, validAttempt(t))
	require.ErrorIs(t, err, context.Canceled)
	require.Empty(t, observer.calls)

	pastWorker, err := NewWorker(observer, fixedClock{now: time.Unix(0, 0)}, time.Second)
	require.NoError(t, err)
	deadlineAttempt := validAttempt(t)
	deadlineResult, err := pastWorker.Discover(context.Background(), deadlineAttempt)
	require.NoError(t, err)
	require.Nil(t, deadlineResult.Authoritative)
	require.NotNil(t, deadlineResult.Failure)
	require.Equal(t, api.LegacyRFReasonDiscoveryDeadlineExceeded, deadlineResult.Failure.Reason)
	require.Equal(t, []EndpointAttemptSummary{
		{AttemptIndex: 0, Endpoint: deadlineAttempt.OrderedSeeds[0], Outcome: EndpointAttemptFailed, Reason: api.LegacyRFReasonDiscoveryDeadlineExceeded},
		{AttemptIndex: 1, Endpoint: deadlineAttempt.OrderedSeeds[1], Outcome: EndpointAttemptSkipped},
		{AttemptIndex: 2, Endpoint: deadlineAttempt.OrderedSeeds[2], Outcome: EndpointAttemptSkipped},
	}, deadlineResult.AttemptTrace)
}

func validAttempt(t *testing.T) Attempt {
	seeds, digest, err := CanonicalizeSeeds([]string{"192.0.2.1", "192.0.2.2", "192.0.2.3"})
	require.NoError(t, err)
	return Attempt{
		ClusterUID: "uid", Generation: 1, MarkerVersion: "v1", ProtocolVersion: "v1", AttemptID: "attempt",
		OrderedSeeds: seeds, SeedDigest: digest, WorkerImageDigest: "sha256:worker",
		Connection: Connection{ExpectedClusterName: "legacy", ManagedLocations: []ManagedLocation{{Name: "managed-resource", DatacenterName: "managed"}}},
	}
}

func newTestWorker(t *testing.T, observer EndpointObserver) *Worker {
	worker, err := NewWorker(observer, fixedClock{now: time.Date(2099, 1, 1, 0, 0, 0, 0, time.UTC)}, time.Minute)
	require.NoError(t, err)
	return worker
}

func validCandidate(version string, systemAuth map[string]int32) Candidate {
	if systemAuth == nil {
		systemAuth = map[string]int32{"legacy-a": 2}
	}
	observation := SourceObservation{
		ClusterName: "legacy", ServerType: api.ServerDistributionCassandra, SourceVersion: version,
		Partitioner: "org.apache.cassandra.dht.Murmur3Partitioner", SchemaVersion: "schema-1",
		Topology: []TopologyHost{
			{Address: netip.MustParseAddr("192.0.2.10"), HostID: "host-a", Datacenter: "legacy-a"},
			{Address: netip.MustParseAddr("192.0.2.11"), HostID: "host-b", Datacenter: "legacy-b"},
		},
	}
	replication := SystemKeyspaceObservations{
		SystemAuth:        KeyspaceObservation{Present: true, Strategy: api.NetworkTopologyStrategyClass, Replication: systemAuth},
		SystemTraces:      KeyspaceObservation{Present: true, Strategy: api.NetworkTopologyStrategyQualifiedClass, Replication: map[string]int32{"legacy-a": 1}},
		SystemDistributed: KeyspaceObservation{Present: true, Strategy: api.NetworkTopologyStrategyClass, Replication: map[string]int32{}},
	}
	return Candidate{Before: observation, Replication: replication, After: observation}
}

func validProtocolResult(t *testing.T) (DiscoveryResult, ExpectedResult) {
	attempt := validAttempt(t)
	candidate, err := validateCandidate(attempt, 0, attempt.OrderedSeeds[0], validCandidate("4.0.0", map[string]int32{"legacy-a": 2, "legacy-b": 10}))
	require.NoError(t, err)
	bindings := []SecretBinding{{Purpose: "auth", SourceContext: "source", Namespace: "ns", Name: "credentials", Keys: []string{"username", "password"}, ResourceVersion: "7"}}
	result := DiscoveryResult{
		SchemaVersion: "1", ClusterUID: attempt.ClusterUID, Generation: attempt.Generation,
		MarkerVersion: attempt.MarkerVersion, ProtocolVersion: attempt.ProtocolVersion, AttemptID: attempt.AttemptID,
		OrderedSeeds: attempt.OrderedSeeds, SeedDigest: attempt.SeedDigest, SecretBindings: bindings,
		WorkerImageDigest: attempt.WorkerImageDigest, Authoritative: &candidate,
		AttemptTrace: []EndpointAttemptSummary{
			{AttemptIndex: 0, Endpoint: attempt.OrderedSeeds[0], Outcome: EndpointAttemptAccepted},
			{AttemptIndex: 1, Endpoint: attempt.OrderedSeeds[1], Outcome: EndpointAttemptSkipped},
			{AttemptIndex: 2, Endpoint: attempt.OrderedSeeds[2], Outcome: EndpointAttemptSkipped},
		},
	}
	expected := ExpectedResult{
		SchemaVersion: result.SchemaVersion, ClusterUID: result.ClusterUID, Generation: result.Generation,
		MarkerVersion: result.MarkerVersion, ProtocolVersion: result.ProtocolVersion, AttemptID: result.AttemptID,
		OrderedSeeds: append([]netip.AddrPort(nil), result.OrderedSeeds...), SeedDigest: result.SeedDigest,
		SecretBindings: append([]SecretBinding(nil), bindings...), WorkerImageDigest: result.WorkerImageDigest,
		MaximumBytes: api.LegacyRFDiscoveryMaxResultBytes,
	}
	return result, expected
}

func TestSignResultDoesNotMutateCallerOwnedSecretBindings(t *testing.T) {
	result, _ := validProtocolResult(t)
	bindings := []SecretBinding{{
		Purpose: "auth", SourceContext: "source", Namespace: "ns", Name: "credentials",
		Keys: []string{"username", "password"}, ResourceVersion: "7",
	}}
	result.SecretBindings = bindings
	want := []SecretBinding{{
		Purpose: "auth", SourceContext: "source", Namespace: "ns", Name: "credentials",
		Keys: []string{"username", "password"}, ResourceVersion: "7",
	}}

	_, err := SignResult(result, []byte("01234567890123456789012345678901"), api.LegacyRFDiscoveryMaxResultBytes)
	require.NoError(t, err)
	require.Equal(t, want, bindings)
	require.Equal(t, want, result.SecretBindings)
}

func FuzzCanonicalizeSeeds(f *testing.F) {
	f.Add("192.0.2.1")
	f.Add("2001:db8::1")
	f.Add("seed.example.test")
	f.Fuzz(func(t *testing.T, raw string) {
		if len(raw) > 256 {
			t.Skip()
		}
		seeds, _, err := CanonicalizeSeeds([]string{raw})
		if err == nil {
			require.Len(t, seeds, 1)
			require.Equal(t, uint16(9042), seeds[0].Port())
		}
	})
}

func FuzzValidateResultEnvelope(f *testing.F) {
	f.Add([]byte(`{}`), []byte("key"))
	f.Add([]byte(`{"result":`), []byte("key"))
	f.Fuzz(func(t *testing.T, body, key []byte) {
		if len(body) > api.LegacyRFDiscoveryMaxResultBytes+1 || len(key) > 256 {
			t.Skip()
		}
		_, _ = ValidateResultEnvelope(body, key, ExpectedResult{MaximumBytes: api.LegacyRFDiscoveryMaxResultBytes})
	})
}
