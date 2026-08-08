package discovery

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"strings"
	"time"

	gocql "github.com/apache/cassandra-gocql-driver/v2"
	api "github.com/k8ssandra/k8ssandra-operator/apis/k8ssandra/v1alpha1"
)

const (
	passwordAuthenticator = "org.apache.cassandra.auth.PasswordAuthenticator"
	localObservationQuery = "SELECT cluster_name, release_version, partitioner, schema_version, host_id, " +
		"data_center, broadcast_address FROM system.local"
	peersObservationQuery = "SELECT peer, host_id, data_center, schema_version, release_version " +
		"FROM system.peers_v2"
	keyspaceReplicationQuery = "SELECT keyspace_name, replication FROM system_schema.keyspaces " +
		"WHERE keyspace_name IN (?, ?, ?)"
)

// CQLCredentials contains optional dedicated legacy-source credentials.
type CQLCredentials struct {
	Username string
	Password string
}

// CQLObserverOptions configures bounded source connections and queries.
type CQLObserverOptions struct {
	Credentials    *CQLCredentials
	TLSConfig      *tls.Config
	ConnectTimeout time.Duration
	QueryTimeout   time.Duration
}

// CQLObserver observes one endpoint through a fresh endpoint-pinned session.
type CQLObserver struct {
	options CQLObserverOptions
	factory cqlSessionFactory
}

// NewCQLObserver constructs an observer backed by the grounded Apache driver.
func NewCQLObserver(options CQLObserverOptions) (*CQLObserver, error) {
	return newCQLObserver(options, apacheCQLSessionFactory{})
}

func newCQLObserver(options CQLObserverOptions, factory cqlSessionFactory) (*CQLObserver, error) {
	if factory == nil {
		return nil, errors.New("CQL session factory is required")
	}
	if err := validateCQLObserverOptions(options); err != nil {
		return nil, err
	}
	return &CQLObserver{options: options, factory: factory}, nil
}

func validateCQLObserverOptions(options CQLObserverOptions) error {
	if options.ConnectTimeout <= 0 || options.QueryTimeout <= 0 {
		return errors.New("CQL connect and query timeouts must be positive")
	}
	if options.Credentials != nil &&
		(options.Credentials.Username == "" || options.Credentials.Password == "") {
		return errors.New("CQL username and password must both be non-empty")
	}
	if options.TLSConfig != nil {
		if options.TLSConfig.InsecureSkipVerify {
			return errors.New("CQL TLS certificate verification cannot be disabled")
		}
		if options.TLSConfig.RootCAs == nil {
			return errors.New("CQL TLS trusted roots are required")
		}
	}
	return nil
}

// DiscoverCandidate reads one complete before/keyspaces/after candidate.
func (observer *CQLObserver) DiscoverCandidate(
	ctx context.Context,
	endpoint netip.AddrPort,
	connection Connection,
) (Candidate, error) {
	if err := validateObserverRequest(ctx, endpoint, connection); err != nil {
		return Candidate{}, err
	}
	session, err := observer.factory.Open(ctx, endpoint, observer.options)
	if err != nil {
		return Candidate{}, classifyCQLError(err)
	}
	defer session.Close()

	before, err := observer.readSourceObservation(ctx, session, endpoint, connection.ExpectedClusterName)
	if err != nil {
		return Candidate{}, err
	}
	replication, err := observer.readSystemReplication(ctx, session, endpoint)
	if err != nil {
		return Candidate{}, err
	}
	after, err := observer.readSourceObservation(ctx, session, endpoint, connection.ExpectedClusterName)
	if err != nil {
		return Candidate{}, err
	}
	return Candidate{Before: before, Replication: replication, After: after}, nil
}

func validateObserverRequest(ctx context.Context, endpoint netip.AddrPort, connection Connection) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("endpoint observation canceled: %w", err)
	}
	if !endpoint.IsValid() || endpoint.Addr().Zone() != "" || endpoint.Port() != defaultCQLPort {
		return NewBoundaryError(api.LegacyRFReasonInvalidContactPoint,
			errors.New("endpoint is not a canonical CQL address"))
	}
	if connection.ExpectedClusterName == "" {
		return NewBoundaryError(api.LegacyRFReasonIdentityMismatch,
			errors.New("expected cluster name is empty"))
	}
	return nil
}

func (observer *CQLObserver) readSourceObservation(
	ctx context.Context,
	session cqlSession,
	endpoint netip.AddrPort,
	expectedCluster string,
) (SourceObservation, error) {
	local, err := observer.readLocal(ctx, session, endpoint, expectedCluster)
	if err != nil {
		return SourceObservation{}, err
	}
	peers, err := observer.readPeers(ctx, session, endpoint, local)
	if err != nil {
		return SourceObservation{}, err
	}
	local.Topology = append(local.Topology, peers...)
	return local, nil
}

func (observer *CQLObserver) readLocal(
	ctx context.Context,
	session cqlSession,
	endpoint netip.AddrPort,
	expectedCluster string,
) (observation SourceObservation, returnedErr error) {
	rows, err := selectPinned(ctx, session, endpoint, localObservationQuery)
	if err != nil {
		return SourceObservation{}, err
	}
	defer closeCQLRows(rows, &returnedErr)

	var cluster, release, partitioner, datacenter *string
	var schemaID, hostID *gocql.UUID
	var address *net.IP
	if !rows.Scan(&cluster, &release, &partitioner, &schemaID, &hostID, &datacenter, &address) {
		return SourceObservation{}, NewBoundaryError(api.LegacyRFReasonTopologyInconsistent,
			errors.New("system.local returned no complete row"))
	}
	if rows.Scan(&cluster, &release, &partitioner, &schemaID, &hostID, &datacenter, &address) {
		return SourceObservation{}, NewBoundaryError(api.LegacyRFReasonTopologyInconsistent,
			errors.New("system.local returned multiple rows"))
	}
	return buildLocalObservation(cluster, release, partitioner, schemaID, hostID, datacenter, address, expectedCluster)
}

func buildLocalObservation(
	cluster, release, partitioner *string,
	schemaID, hostID *gocql.UUID,
	datacenter *string,
	address *net.IP,
	expectedCluster string,
) (SourceObservation, error) {
	if cluster == nil || release == nil || partitioner == nil || schemaID == nil || hostID == nil ||
		datacenter == nil || address == nil || *cluster == "" || *release == "" || *partitioner == "" ||
		*datacenter == "" {
		return SourceObservation{}, NewBoundaryError(api.LegacyRFReasonTopologyInconsistent,
			errors.New("system.local contains null or empty identity fields"))
	}
	if *cluster != expectedCluster {
		return SourceObservation{}, NewBoundaryError(api.LegacyRFReasonIdentityMismatch,
			errors.New("source cluster name differs from expected cluster name"))
	}
	if err := validateSourceVersion(*release); err != nil {
		return SourceObservation{}, err
	}
	topology, err := newTopologyHost(*address, hostID.String(), *datacenter)
	if err != nil {
		return SourceObservation{}, err
	}
	return SourceObservation{
		ClusterName: *cluster, ServerType: api.ServerDistributionCassandra,
		SourceVersion: *release, Partitioner: *partitioner, SchemaVersion: schemaID.String(),
		Topology: []TopologyHost{topology},
	}, nil
}

func (observer *CQLObserver) readPeers(
	ctx context.Context,
	session cqlSession,
	endpoint netip.AddrPort,
	local SourceObservation,
) (peers []TopologyHost, returnedErr error) {
	rows, err := selectPinned(ctx, session, endpoint, peersObservationQuery)
	if err != nil {
		return nil, err
	}
	defer closeCQLRows(rows, &returnedErr)
	for {
		peer, found, err := scanPeer(rows, local)
		if err != nil {
			return nil, err
		}
		if !found {
			return peers, nil
		}
		if len(peers) >= MaximumTopologyHosts-1 {
			return nil, NewBoundaryError(api.LegacyRFReasonTopologyInconsistent,
				fmt.Errorf("source topology exceeds %d hosts", MaximumTopologyHosts))
		}
		peers = append(peers, peer)
	}
}

func scanPeer(rows cqlRows, local SourceObservation) (TopologyHost, bool, error) {
	var address *net.IP
	var hostID, schemaID *gocql.UUID
	var datacenter, release *string
	if !rows.Scan(&address, &hostID, &datacenter, &schemaID, &release) {
		return TopologyHost{}, false, nil
	}
	if address == nil || hostID == nil || datacenter == nil || schemaID == nil || release == nil ||
		*datacenter == "" || *release == "" {
		return TopologyHost{}, false, NewBoundaryError(api.LegacyRFReasonTopologyInconsistent,
			errors.New("system.peers_v2 contains null or empty identity fields"))
	}
	if schemaID.String() != local.SchemaVersion || *release != local.SourceVersion {
		return TopologyHost{}, false, NewBoundaryError(api.LegacyRFReasonSchemaDisagreement,
			errors.New("system.peers_v2 disagrees with system.local"))
	}
	peer, err := newTopologyHost(*address, hostID.String(), *datacenter)
	return peer, true, err
}

func newTopologyHost(address net.IP, hostID, datacenter string) (TopologyHost, error) {
	parsed, valid := netip.AddrFromSlice(address)
	if !valid || parsed.IsUnspecified() || hostID == "" || datacenter == "" {
		return TopologyHost{}, NewBoundaryError(api.LegacyRFReasonTopologyInconsistent,
			errors.New("source topology host is incomplete"))
	}
	return TopologyHost{Address: parsed.Unmap(), HostID: hostID, Datacenter: datacenter}, nil
}

func (observer *CQLObserver) readSystemReplication(
	ctx context.Context,
	session cqlSession,
	endpoint netip.AddrPort,
) (replication SystemKeyspaceObservations, returnedErr error) {
	rows, err := selectPinned(ctx, session, endpoint, keyspaceReplicationQuery,
		api.SystemAuthKeyspace, api.SystemTracesKeyspace, api.SystemDistributedKeyspace)
	if err != nil {
		return SystemKeyspaceObservations{}, err
	}
	defer closeCQLRows(rows, &returnedErr)
	seen := make(map[string]struct{}, 3)
	for {
		name, observation, found, err := scanKeyspace(rows)
		if err != nil {
			return SystemKeyspaceObservations{}, err
		}
		if !found {
			break
		}
		if _, duplicate := seen[name]; duplicate {
			return SystemKeyspaceObservations{}, invalidReplicationError("duplicate keyspace row")
		}
		seen[name] = struct{}{}
		if err = assignKeyspaceObservation(&replication, name, observation); err != nil {
			return SystemKeyspaceObservations{}, err
		}
	}
	return replication, nil
}

func scanKeyspace(rows cqlRows) (string, KeyspaceObservation, bool, error) {
	var name *string
	var rawReplication map[string]string
	if !rows.Scan(&name, &rawReplication) {
		return "", KeyspaceObservation{}, false, nil
	}
	if name == nil || *name == "" || rawReplication == nil {
		return "", KeyspaceObservation{}, false, invalidReplicationError("keyspace row is null")
	}
	strategy, found := rawReplication["class"]
	if !found {
		return "", KeyspaceObservation{}, false, invalidReplicationError("keyspace strategy is missing")
	}
	canonicalStrategy, err := CanonicalizeNetworkTopologyStrategy(strategy)
	if err != nil {
		return "", KeyspaceObservation{}, false, NewBoundaryError(api.LegacyRFReasonUnsupportedStrategy, err)
	}
	factors := make(map[string]int32, len(rawReplication)-1)
	for datacenter, rawFactor := range rawReplication {
		if datacenter == "class" {
			continue
		}
		factor, err := ParseReplicationFactor(rawFactor)
		if err != nil || datacenter == "" {
			return "", KeyspaceObservation{}, false, invalidReplicationError("keyspace RF entry is invalid")
		}
		factors[datacenter] = factor
	}
	return *name, KeyspaceObservation{Present: true, Strategy: canonicalStrategy, Replication: factors}, true, nil
}

func assignKeyspaceObservation(
	replication *SystemKeyspaceObservations,
	name string,
	observation KeyspaceObservation,
) error {
	switch name {
	case api.SystemAuthKeyspace:
		replication.SystemAuth = observation
	case api.SystemTracesKeyspace:
		replication.SystemTraces = observation
	case api.SystemDistributedKeyspace:
		replication.SystemDistributed = observation
	default:
		return invalidReplicationError("unexpected keyspace row")
	}
	return nil
}

func invalidReplicationError(message string) error {
	return NewBoundaryError(api.LegacyRFReasonInvalidReplication, errors.New(message))
}

func selectPinned(
	ctx context.Context,
	session cqlSession,
	endpoint netip.AddrPort,
	statement string,
	values ...any,
) (cqlRows, error) {
	rows, err := session.Select(ctx, statement, values...)
	if err != nil {
		return nil, classifyCQLError(err)
	}
	if rows == nil {
		return nil, NewBoundaryError(api.LegacyRFReasonContactUnreachable,
			errors.New("CQL SELECT returned no iterator"))
	}
	if rows.Coordinator() != endpoint {
		cause := errors.New("CQL SELECT escaped the attempted endpoint")
		if closeErr := rows.Close(); closeErr != nil {
			cause = errors.Join(cause, fmt.Errorf("close redirected CQL iterator: %w", closeErr))
		}
		return nil, NewBoundaryError(api.LegacyRFReasonTopologyInconsistent,
			cause)
	}
	return rows, nil
}

func closeCQLRows(rows cqlRows, returnedErr *error) {
	if err := rows.Close(); err != nil && *returnedErr == nil {
		*returnedErr = classifyCQLError(err)
	}
}

type cqlSessionFactory interface {
	Open(context.Context, netip.AddrPort, CQLObserverOptions) (cqlSession, error)
}

type cqlSession interface {
	Select(context.Context, string, ...any) (cqlRows, error)
	Close()
}

type cqlRows interface {
	Scan(...any) bool
	Coordinator() netip.AddrPort
	Close() error
}

type apacheCQLSessionFactory struct{}

func (apacheCQLSessionFactory) Open(
	ctx context.Context,
	endpoint netip.AddrPort,
	options CQLObserverOptions,
) (cqlSession, error) {
	config, err := newPinnedClusterConfig(ctx, endpoint, options)
	if err != nil {
		return nil, err
	}
	session, err := config.CreateSession()
	if err != nil {
		return nil, fmt.Errorf("create endpoint-pinned CQL session: %w", err)
	}
	return apacheCQLSession{session: session}, nil
}

func newPinnedClusterConfig(
	ctx context.Context,
	endpoint netip.AddrPort,
	options CQLObserverOptions,
) (*gocql.ClusterConfig, error) {
	if err := validateCQLObserverOptions(options); err != nil {
		return nil, err
	}
	if !endpoint.IsValid() || endpoint.Port() != defaultCQLPort || endpoint.Addr().Zone() != "" {
		return nil, errors.New("endpoint is not a canonical CQL address")
	}
	connectTimeout, err := timeoutWithinContext(ctx, options.ConnectTimeout)
	if err != nil {
		return nil, err
	}
	target := net.JoinHostPort(endpoint.Addr().String(), fmt.Sprint(defaultCQLPort))
	config := gocql.NewCluster(target)
	configurePinnedRouting(config, endpoint, ctx, target, connectTimeout)
	config.ConnectTimeout = connectTimeout
	config.Timeout = options.QueryTimeout
	config.WriteTimeout = options.QueryTimeout
	configureAuthenticationAndTLS(config, endpoint, options)
	return config, nil
}

func configurePinnedRouting(
	config *gocql.ClusterConfig,
	endpoint netip.AddrPort,
	ctx context.Context,
	target string,
	connectTimeout time.Duration,
) {
	config.DisableInitialHostLookup = true
	config.Events.DisableNodeStatusEvents = true
	config.Events.DisableTopologyEvents = true
	config.Events.DisableSchemaEvents = true
	config.Metadata.CacheMode = gocql.Disabled
	config.HostFilter = gocql.HostFilterFunc(func(host *gocql.HostInfo) bool {
		return host.ConnectAddress().Equal(net.IP(endpoint.Addr().AsSlice())) && host.Port() == int(defaultCQLPort)
	})
	config.PoolConfig.HostSelectionPolicy = gocql.RoundRobinHostPolicy()
	config.ReconnectInterval = 0
	config.ReconnectionPolicy = &gocql.ConstantReconnectionPolicy{MaxRetries: 1}
	config.NumConns = 1
	config.Dialer = pinnedDialer{
		target: target, parent: ctx,
		delegate: &net.Dialer{Timeout: connectTimeout},
	}
	config.HostDialer = nil
}

func configureAuthenticationAndTLS(
	config *gocql.ClusterConfig,
	endpoint netip.AddrPort,
	options CQLObserverOptions,
) {
	if options.Credentials != nil {
		config.Authenticator = gocql.PasswordAuthenticator{
			Username: options.Credentials.Username, Password: options.Credentials.Password,
			AllowedAuthenticators: []string{passwordAuthenticator},
		}
	}
	if options.TLSConfig != nil {
		tlsConfig := options.TLSConfig.Clone()
		tlsConfig.ServerName = endpoint.Addr().String()
		tlsConfig.InsecureSkipVerify = false
		config.SslOpts = &gocql.SslOptions{Config: tlsConfig}
	}
}

func timeoutWithinContext(ctx context.Context, configured time.Duration) (time.Duration, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	deadline, found := ctx.Deadline()
	if !found {
		return configured, nil
	}
	remaining := time.Until(deadline)
	if remaining <= 0 {
		return 0, context.DeadlineExceeded
	}
	if remaining < configured {
		return remaining, nil
	}
	return configured, nil
}

type pinnedDialer struct {
	target   string
	parent   context.Context
	delegate gocql.Dialer
}

func (dialer pinnedDialer) DialContext(ctx context.Context, network, address string) (net.Conn, error) {
	if network != "tcp" || address != dialer.target {
		return nil, fmt.Errorf("reject CQL endpoint escape to %q over %q", address, network)
	}
	if dialer.delegate == nil {
		return nil, errors.New("bounded CQL network dialer is required")
	}
	if dialer.parent == nil {
		return dialer.delegate.DialContext(ctx, network, address)
	}
	bounded, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(dialer.parent, cancel)
	defer func() {
		stop()
		cancel()
	}()
	return dialer.delegate.DialContext(bounded, network, address)
}

type apacheCQLSession struct{ session *gocql.Session }

func (session apacheCQLSession) Select(
	ctx context.Context,
	statement string,
	values ...any,
) (cqlRows, error) {
	if !allowedSelect(statement) {
		return nil, errors.New("CQL observer rejected a non-read-only statement")
	}
	iterator := session.session.Query(statement, values...).IterContext(ctx)
	return apacheCQLRows{iterator: iterator}, nil
}

func (session apacheCQLSession) Close() { session.session.Close() }

func allowedSelect(statement string) bool {
	switch statement {
	case localObservationQuery, peersObservationQuery, keyspaceReplicationQuery:
		return strings.HasPrefix(statement, "SELECT ")
	default:
		return false
	}
}

type apacheCQLRows struct{ iterator *gocql.Iter }

func (rows apacheCQLRows) Scan(dest ...any) bool { return rows.iterator.Scan(dest...) }

func (rows apacheCQLRows) Coordinator() netip.AddrPort {
	host := rows.iterator.Host()
	if host == nil {
		return netip.AddrPort{}
	}
	address, valid := netip.AddrFromSlice(host.ConnectAddress())
	if !valid || host.Port() < 1 || host.Port() > 65535 {
		return netip.AddrPort{}
	}
	return netip.AddrPortFrom(address.Unmap(), uint16(host.Port()))
}

func (rows apacheCQLRows) Close() error { return rows.iterator.Close() }

func classifyCQLError(err error) *BoundaryError {
	if err == nil {
		return NewBoundaryError(api.LegacyRFReasonContactUnreachable, errors.New("CQL operation failed"))
	}
	var boundary *BoundaryError
	if errors.As(err, &boundary) {
		return boundary
	}
	var request gocql.RequestError
	if errors.As(err, &request) {
		switch request.Code() {
		case gocql.ErrCodeCredentials:
			return NewBoundaryError(api.LegacyRFReasonAuthenticationRejected, err)
		case gocql.ErrCodeUnauthorized:
			return NewBoundaryError(api.LegacyRFReasonAuthorizationDenied, err)
		}
	}
	if isTLSError(err) {
		return NewBoundaryError(api.LegacyRFReasonTLSFailed, err)
	}
	return NewBoundaryError(api.LegacyRFReasonContactUnreachable, err)
}

func isTLSError(err error) bool {
	var certificateVerification *tls.CertificateVerificationError
	var unknownAuthority x509.UnknownAuthorityError
	var hostname x509.HostnameError
	var recordHeader tls.RecordHeaderError
	return errors.As(err, &certificateVerification) || errors.As(err, &unknownAuthority) ||
		errors.As(err, &hostname) || errors.As(err, &recordHeader)
}
