package discovery

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"net"
	"net/netip"
	"reflect"
	"strings"
	"testing"
	"time"

	gocql "github.com/apache/cassandra-gocql-driver/v2"
	api "github.com/k8ssandra/k8ssandra-operator/apis/k8ssandra/v1alpha1"
	"github.com/stretchr/testify/require"
)

type fakeCQLFactory struct {
	sessions []*fakeCQLSession
	err      error
	calls    []netip.AddrPort
}

func (factory *fakeCQLFactory) Open(
	_ context.Context,
	endpoint netip.AddrPort,
	_ CQLObserverOptions,
) (cqlSession, error) {
	factory.calls = append(factory.calls, endpoint)
	if factory.err != nil {
		return nil, factory.err
	}
	if len(factory.sessions) == 0 {
		return nil, errors.New("unexpected session open")
	}
	session := factory.sessions[0]
	factory.sessions = factory.sessions[1:]
	return session, nil
}

type fakeCQLSession struct {
	endpoint  netip.AddrPort
	responses map[string][]*fakeCQLRows
	queries   []string
	closed    bool
}

func (session *fakeCQLSession) Select(
	_ context.Context,
	statement string,
	_ ...any,
) (cqlRows, error) {
	session.queries = append(session.queries, statement)
	responses := session.responses[statement]
	if len(responses) == 0 {
		return nil, errors.New("unexpected SELECT")
	}
	rows := responses[0]
	session.responses[statement] = responses[1:]
	return rows, nil
}

func (session *fakeCQLSession) Close() { session.closed = true }

type fakeCQLRows struct {
	rows        [][]any
	coordinator netip.AddrPort
	index       int
	closeErr    error
	closed      bool
}

func (rows *fakeCQLRows) Scan(dest ...any) bool {
	if rows.index >= len(rows.rows) {
		return false
	}
	values := rows.rows[rows.index]
	rows.index++
	if len(values) != len(dest) {
		panic("fake row column mismatch")
	}
	for index := range values {
		target := reflect.ValueOf(dest[index]).Elem()
		value := reflect.ValueOf(values[index])
		if value.IsValid() && value.Type().AssignableTo(target.Type()) {
			target.Set(value)
			continue
		}
		if target.Kind() == reflect.Pointer && value.IsValid() && value.Type().AssignableTo(target.Type().Elem()) {
			pointer := reflect.New(target.Type().Elem())
			pointer.Elem().Set(value)
			target.Set(pointer)
			continue
		}
		panic("fake row value mismatch")
	}
	return true
}

func (rows *fakeCQLRows) Coordinator() netip.AddrPort { return rows.coordinator }

func (rows *fakeCQLRows) Close() error {
	rows.closed = true
	return rows.closeErr
}

func TestPinnedClusterConfigUsesEveryGroundedNoEscapeControl(t *testing.T) {
	endpoint := netip.MustParseAddrPort("192.0.2.10:9042")
	tlsConfig := &tls.Config{RootCAs: x509.NewCertPool(), MinVersion: tls.VersionTLS12}
	options := CQLObserverOptions{
		Credentials:    &CQLCredentials{Username: "user-canary", Password: "password-canary"},
		TLSConfig:      tlsConfig,
		ConnectTimeout: 3 * time.Second,
		QueryTimeout:   2 * time.Second,
	}
	config, err := newPinnedClusterConfig(context.Background(), endpoint, options)
	require.NoError(t, err)
	require.Equal(t, []string{"192.0.2.10:9042"}, config.Hosts)
	require.True(t, config.DisableInitialHostLookup)
	require.True(t, config.Events.DisableNodeStatusEvents)
	require.True(t, config.Events.DisableTopologyEvents)
	require.True(t, config.Events.DisableSchemaEvents)
	require.Equal(t, gocql.Disabled, config.Metadata.CacheMode)
	require.Zero(t, config.ReconnectInterval)
	require.Equal(t, 3*time.Second, config.ConnectTimeout)
	require.Equal(t, 2*time.Second, config.Timeout)
	require.Nil(t, config.HostDialer)
	require.NotNil(t, config.Dialer)
	require.NotNil(t, config.PoolConfig.HostSelectionPolicy)
	require.Equal(t, 1, config.ReconnectionPolicy.GetMaxRetries())

	allowed, err := gocql.NewHostInfoFromAddrPort(net.ParseIP("192.0.2.10"), 9042)
	require.NoError(t, err)
	wrongIP, err := gocql.NewHostInfoFromAddrPort(net.ParseIP("192.0.2.11"), 9042)
	require.NoError(t, err)
	wrongPort, err := gocql.NewHostInfoFromAddrPort(net.ParseIP("192.0.2.10"), 9043)
	require.NoError(t, err)
	require.True(t, config.HostFilter.Accept(allowed))
	require.False(t, config.HostFilter.Accept(wrongIP))
	require.False(t, config.HostFilter.Accept(wrongPort))

	authenticator, ok := config.Authenticator.(gocql.PasswordAuthenticator)
	require.True(t, ok)
	require.Equal(t, []string{"org.apache.cassandra.auth.PasswordAuthenticator"}, authenticator.AllowedAuthenticators)
	require.Equal(t, endpoint.Addr().String(), config.SslOpts.ServerName)
	require.False(t, config.SslOpts.InsecureSkipVerify)
	require.NotSame(t, tlsConfig, config.SslOpts.Config)
}

func TestPinnedDialerRejectsEveryEndpointEscapeBeforeDelegating(t *testing.T) {
	delegate := &recordingDialer{}
	dialer := pinnedDialer{target: "192.0.2.10:9042", delegate: delegate}
	for _, request := range []struct{ network, address string }{
		{network: "udp", address: "192.0.2.10:9042"},
		{network: "tcp", address: "192.0.2.11:9042"},
		{network: "tcp", address: "192.0.2.10:9043"},
	} {
		_, err := dialer.DialContext(context.Background(), request.network, request.address)
		require.Error(t, err)
	}
	require.Empty(t, delegate.calls)
	_, _ = dialer.DialContext(context.Background(), "tcp", "192.0.2.10:9042")
	require.Equal(t, []string{"tcp|192.0.2.10:9042"}, delegate.calls)
}

type recordingDialer struct{ calls []string }

func (dialer *recordingDialer) DialContext(_ context.Context, network, address string) (net.Conn, error) {
	dialer.calls = append(dialer.calls, network+"|"+address)
	return nil, errors.New("expected test dial failure")
}

func TestCQLObserverReadsOnlyThePinnedCompleteCandidateAndClosesResources(t *testing.T) {
	endpoint := netip.MustParseAddrPort("192.0.2.10:9042")
	session, allRows := validFakeCQLSession(t, endpoint)
	factory := &fakeCQLFactory{sessions: []*fakeCQLSession{session}}
	observer, err := newCQLObserver(validCQLObserverOptions(), factory)
	require.NoError(t, err)

	candidate, err := observer.DiscoverCandidate(context.Background(), endpoint, Connection{ExpectedClusterName: "legacy"})
	require.NoError(t, err)
	require.True(t, session.closed)
	for _, rows := range allRows {
		require.True(t, rows.closed)
	}
	for _, query := range session.queries {
		require.True(t, strings.HasPrefix(query, "SELECT "), query)
	}
	require.Equal(t, []string{localObservationQuery, peersObservationQuery, keyspaceReplicationQuery, localObservationQuery, peersObservationQuery}, session.queries)
	require.Equal(t, "4.0.0", candidate.Before.SourceVersion)
	require.EqualValues(t, 2, candidate.Replication.SystemAuth.Replication["legacy-a"])
	require.EqualValues(t, 9, candidate.Replication.SystemAuth.Replication["legacy-b"])
	require.EqualValues(t, 2, candidate.Replication.SystemTraces.Replication["legacy-a"])
	require.EqualValues(t, 10, candidate.Replication.SystemDistributed.Replication["legacy-b"])
	require.EqualValues(t, 10, candidate.Replication.SystemDistributed.Replication["legacy-e"])
}

func TestCQLObserverRejectsTopologyBeyondBoundBeforeAccumulatingIt(t *testing.T) {
	endpoint := netip.MustParseAddrPort("192.0.2.10:9042")
	session, rows := validFakeCQLSession(t, endpoint)
	peerRows := rows[1]
	template := append([]any(nil), peerRows.rows[0]...)
	peerRows.rows = make([][]any, MaximumTopologyHosts)
	for index := range peerRows.rows {
		peerRows.rows[index] = append([]any(nil), template...)
	}
	observer, err := newCQLObserver(validCQLObserverOptions(), &fakeCQLFactory{sessions: []*fakeCQLSession{session}})
	require.NoError(t, err)

	_, err = observer.DiscoverCandidate(context.Background(), endpoint, Connection{ExpectedClusterName: "legacy"})

	var boundary *BoundaryError
	require.ErrorAs(t, err, &boundary)
	require.Equal(t, api.LegacyRFReasonTopologyInconsistent, boundary.PublicFailure().Reason)
	require.NotContains(t, boundary.PublicFailure().Message, endpoint.String())
	require.True(t, peerRows.closed)
	require.True(t, session.closed)
}

func TestCQLObserverOpensAndClosesFreshSessionsLazilyAcrossFallback(t *testing.T) {
	firstEndpoint := netip.MustParseAddrPort("192.0.2.10:9042")
	secondEndpoint := netip.MustParseAddrPort("192.0.2.11:9042")
	thirdEndpoint := netip.MustParseAddrPort("192.0.2.12:9042")
	first, _ := validFakeCQLSession(t, firstEndpoint)
	wrongCluster := "wrong-cluster"
	first.responses[localObservationQuery][0].rows[0][0] = &wrongCluster
	second, _ := validFakeCQLSession(t, secondEndpoint)
	third, _ := validFakeCQLSession(t, thirdEndpoint)
	factory := &fakeCQLFactory{sessions: []*fakeCQLSession{first, second, third}}
	observer, err := newCQLObserver(validCQLObserverOptions(), factory)
	require.NoError(t, err)
	worker, err := NewWorker(observer, fixedClock{now: time.Date(2099, 1, 1, 0, 0, 0, 0, time.UTC)}, time.Minute)
	require.NoError(t, err)
	attempt := validAttempt(t)
	attempt.OrderedSeeds = []netip.AddrPort{firstEndpoint, secondEndpoint, thirdEndpoint}
	attempt.SeedDigest, err = SeedDigest(attempt.OrderedSeeds)
	require.NoError(t, err)

	result, err := worker.Discover(context.Background(), attempt)
	require.NoError(t, err)
	require.Equal(t, []netip.AddrPort{firstEndpoint, secondEndpoint}, factory.calls)
	require.Equal(t, secondEndpoint, result.Authoritative.Endpoint)
	require.True(t, first.closed)
	require.True(t, second.closed)
	require.False(t, third.closed)
	require.Len(t, factory.sessions, 1, "the session after first success must never be opened")
}

func TestNewCQLObserverRejectsUnsafeConfiguration(t *testing.T) {
	_, err := NewCQLObserver(validCQLObserverOptions())
	require.NoError(t, err)
	unsafeOptions := []CQLObserverOptions{
		{},
		{ConnectTimeout: time.Second},
		{ConnectTimeout: time.Second, QueryTimeout: time.Second, Credentials: &CQLCredentials{Username: "user"}},
		{ConnectTimeout: time.Second, QueryTimeout: time.Second, TLSConfig: &tls.Config{InsecureSkipVerify: true}}, //nolint:gosec // verifies rejection
		{ConnectTimeout: time.Second, QueryTimeout: time.Second, TLSConfig: &tls.Config{}},
	}
	for _, options := range unsafeOptions {
		_, err := NewCQLObserver(options)
		require.Error(t, err)
	}
}

func TestCQLObserverFailsClosedForRedirectionNullIdentityAndCloseErrors(t *testing.T) {
	endpoint := netip.MustParseAddrPort("192.0.2.10:9042")
	tests := []struct {
		name   string
		mutate func(*fakeCQLSession, []*fakeCQLRows)
		reason api.LegacyRFDiscoveryReason
	}{
		{name: "query redirected", reason: api.LegacyRFReasonTopologyInconsistent, mutate: func(_ *fakeCQLSession, rows []*fakeCQLRows) {
			rows[0].coordinator = netip.MustParseAddrPort("192.0.2.11:9042")
		}},
		{name: "null local identity", reason: api.LegacyRFReasonTopologyInconsistent, mutate: func(_ *fakeCQLSession, rows []*fakeCQLRows) {
			rows[0].rows[0][0] = (*string)(nil)
		}},
		{name: "iterator close failure", reason: api.LegacyRFReasonContactUnreachable, mutate: func(_ *fakeCQLSession, rows []*fakeCQLRows) {
			rows[0].closeErr = errors.New("password=close-canary")
		}},
		{name: "peer schema conflict", reason: api.LegacyRFReasonSchemaDisagreement, mutate: func(_ *fakeCQLSession, rows []*fakeCQLRows) {
			conflicting, err := gocql.ParseUUID("44444444-4444-4444-4444-444444444444")
			require.NoError(t, err)
			rows[1].rows[0][3] = &conflicting
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			session, rows := validFakeCQLSession(t, endpoint)
			test.mutate(session, rows)
			observer, err := newCQLObserver(validCQLObserverOptions(), &fakeCQLFactory{sessions: []*fakeCQLSession{session}})
			require.NoError(t, err)
			_, err = observer.DiscoverCandidate(context.Background(), endpoint, Connection{ExpectedClusterName: "legacy"})
			var boundary *BoundaryError
			require.ErrorAs(t, err, &boundary)
			require.Equal(t, test.reason, boundary.PublicFailure().Reason)
			require.NotContains(t, err.Error(), "close-canary")
			require.True(t, session.closed)
		})
	}
}

func TestCQLObserverClassifiesSessionFailuresWithoutLeakingTheirCause(t *testing.T) {
	tests := []struct {
		name   string
		cause  error
		reason api.LegacyRFDiscoveryReason
	}{
		{name: "authentication", cause: codedRequestError{code: gocql.ErrCodeCredentials, text: "password=auth-canary"}, reason: api.LegacyRFReasonAuthenticationRejected},
		{name: "authorization", cause: codedRequestError{code: gocql.ErrCodeUnauthorized, text: "password=authorization-canary"}, reason: api.LegacyRFReasonAuthorizationDenied},
		{name: "TLS", cause: x509.UnknownAuthorityError{}, reason: api.LegacyRFReasonTLSFailed},
		{name: "reachability", cause: errors.New("dsn=reachability-canary"), reason: api.LegacyRFReasonContactUnreachable},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			observer, err := newCQLObserver(validCQLObserverOptions(), &fakeCQLFactory{err: test.cause})
			require.NoError(t, err)
			_, err = observer.DiscoverCandidate(context.Background(), netip.MustParseAddrPort("192.0.2.10:9042"), Connection{ExpectedClusterName: "legacy"})
			var boundary *BoundaryError
			require.ErrorAs(t, err, &boundary)
			require.Equal(t, test.reason, boundary.PublicFailure().Reason)
			require.NotContains(t, err.Error(), "canary")
		})
	}
}

type codedRequestError struct {
	code int
	text string
}

func (err codedRequestError) Error() string   { return err.text }
func (err codedRequestError) Code() int       { return err.code }
func (err codedRequestError) Message() string { return err.text }

func validCQLObserverOptions() CQLObserverOptions {
	return CQLObserverOptions{ConnectTimeout: 3 * time.Second, QueryTimeout: 2 * time.Second}
}

func validFakeCQLSession(t *testing.T, endpoint netip.AddrPort) (*fakeCQLSession, []*fakeCQLRows) {
	t.Helper()
	localID, err := gocql.ParseUUID("11111111-1111-1111-1111-111111111111")
	require.NoError(t, err)
	peerID, err := gocql.ParseUUID("22222222-2222-2222-2222-222222222222")
	require.NoError(t, err)
	thirdPeerID, err := gocql.ParseUUID("55555555-5555-5555-5555-555555555555")
	require.NoError(t, err)
	schemaID, err := gocql.ParseUUID("33333333-3333-3333-3333-333333333333")
	require.NoError(t, err)
	cluster, release := "legacy", "4.0.0"
	partitioner, localDC, peerDC := "org.apache.cassandra.dht.Murmur3Partitioner", "legacy-a", "legacy-b"
	thirdPeerDC := "legacy-e"
	localAddress, peerAddress := net.ParseIP("192.0.2.10"), net.ParseIP("192.0.2.11")
	thirdPeerAddress := net.ParseIP("192.0.2.12")
	localRow := []any{&cluster, &release, &partitioner, &schemaID, &localID, &localDC, &localAddress}
	peerRow := []any{&peerAddress, &peerID, &peerDC, &schemaID, &release}
	thirdPeerRow := []any{&thirdPeerAddress, &thirdPeerID, &thirdPeerDC, &schemaID, &release}
	strategy := api.NetworkTopologyStrategyQualifiedClass
	keyspaces := [][]any{
		{api.SystemAuthKeyspace, map[string]string{"class": strategy, "legacy-a": "2", "legacy-b": "9"}},
		{api.SystemTracesKeyspace, map[string]string{"class": strategy, "legacy-a": "2"}},
		{api.SystemDistributedKeyspace, map[string]string{"class": strategy, "legacy-b": "10", "legacy-e": "10"}},
	}
	rows := []*fakeCQLRows{
		{rows: [][]any{localRow}, coordinator: endpoint},
		{rows: [][]any{peerRow, thirdPeerRow}, coordinator: endpoint},
		{rows: keyspaces, coordinator: endpoint},
		{rows: [][]any{localRow}, coordinator: endpoint},
		{rows: [][]any{peerRow, thirdPeerRow}, coordinator: endpoint},
	}
	return &fakeCQLSession{endpoint: endpoint, responses: map[string][]*fakeCQLRows{
		localObservationQuery:    {rows[0], rows[3]},
		peersObservationQuery:    {rows[1], rows[4]},
		keyspaceReplicationQuery: {rows[2]},
	}}, rows
}
