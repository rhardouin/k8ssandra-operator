package discovery

import (
	"context"
	"errors"
	"fmt"
	"net/netip"
	"sort"
	"time"

	"github.com/Masterminds/semver/v3"
	api "github.com/k8ssandra/k8ssandra-operator/apis/k8ssandra/v1alpha1"
)

const minimumSourceVersion = "4.0.0"

// RealClock supplies wall-clock time to a production Worker.
type RealClock struct{}

// Now returns the current time.
func (RealClock) Now() time.Time {
	return time.Now()
}

// Worker executes bounded, ordered fallback discovery.
type Worker struct {
	observer       EndpointObserver
	clock          Clock
	overallTimeout time.Duration
}

// NewWorker constructs a worker with injected observation and time boundaries.
func NewWorker(observer EndpointObserver, clock Clock, overallTimeout time.Duration) (*Worker, error) {
	if observer == nil {
		return nil, errors.New("endpoint observer is required")
	}
	if clock == nil {
		return nil, errors.New("clock is required")
	}
	if overallTimeout <= 0 {
		return nil, errors.New("overall discovery timeout must be positive")
	}
	return &Worker{observer: observer, clock: clock, overallTimeout: overallTimeout}, nil
}

// Discover attempts canonical endpoints lazily and accepts the first complete candidate.
func (worker *Worker) Discover(ctx context.Context, attempt Attempt) (WorkerResult, error) {
	if err := worker.validateAttempt(attempt); err != nil {
		return WorkerResult{}, NewBoundaryError(api.LegacyRFReasonInvalidContactPoint, err)
	}
	deadlineContext, cancel := context.WithDeadline(ctx, worker.clock.Now().Add(worker.overallTimeout))
	defer cancel()

	internalFailures := make([]attemptFailure, 0, len(attempt.OrderedSeeds))
	attemptTrace := make([]EndpointAttemptSummary, 0, len(attempt.OrderedSeeds))
	for index, endpoint := range attempt.OrderedSeeds {
		if err := deadlineContext.Err(); err != nil {
			return worker.interruptedResult(ctx, attempt, attemptTrace)
		}
		candidate, err := worker.observer.DiscoverCandidate(deadlineContext, endpoint, attempt.Connection)
		if err != nil {
			if deadlineContext.Err() != nil {
				return worker.interruptedResult(ctx, attempt, attemptTrace)
			}
			internalFailures = append(internalFailures, makeAttemptFailure(index, endpoint, err))
			attemptTrace = append(attemptTrace, failureAttemptSummary(internalFailures[len(internalFailures)-1]))
			continue
		}
		authoritative, err := validateCandidate(attempt, index, endpoint, candidate)
		if err != nil {
			internalFailures = append(internalFailures, makeAttemptFailure(index, endpoint, err))
			attemptTrace = append(attemptTrace, failureAttemptSummary(internalFailures[len(internalFailures)-1]))
			continue
		}
		attemptTrace = append(attemptTrace, EndpointAttemptSummary{AttemptIndex: index, Endpoint: endpoint, Outcome: EndpointAttemptAccepted})
		for skippedIndex := index + 1; skippedIndex < len(attempt.OrderedSeeds); skippedIndex++ {
			attemptTrace = append(attemptTrace, EndpointAttemptSummary{AttemptIndex: skippedIndex, Endpoint: attempt.OrderedSeeds[skippedIndex], Outcome: EndpointAttemptSkipped})
		}
		return WorkerResult{
			Authoritative: &authoritative,
			AttemptTrace:  attemptTrace,
		}, nil
	}
	failure := exhaustedFailure(internalFailures).PublicFailure()
	return WorkerResult{Failure: &failure, AttemptTrace: attemptTrace}, nil
}

func (worker *Worker) validateAttempt(attempt Attempt) error {
	if attempt.ClusterUID == "" || attempt.AttemptID == "" || attempt.Generation < 1 ||
		attempt.MarkerVersion == "" || attempt.ProtocolVersion == "" || attempt.WorkerImageDigest == "" {
		return errors.New("attempt has incomplete immutable bindings")
	}
	if len(attempt.OrderedSeeds) == 0 || len(attempt.OrderedSeeds) > api.LegacyRFDiscoveryMaxSeeds {
		return fmt.Errorf("attempt seed count must be between 1 and %d", api.LegacyRFDiscoveryMaxSeeds)
	}
	digest, err := SeedDigest(attempt.OrderedSeeds)
	if err != nil {
		return err
	}
	if digest != attempt.SeedDigest {
		return errors.New("attempt seed digest does not match ordered seeds")
	}
	return nil
}

func (worker *Worker) interruptedResult(
	parent context.Context,
	attempt Attempt,
	attemptTrace []EndpointAttemptSummary,
) (WorkerResult, error) {
	if parent.Err() != nil {
		return WorkerResult{}, fmt.Errorf("discovery canceled: %w", parent.Err())
	}
	failure, _ := api.LegacyRFDiscoveryFailureForReason(api.LegacyRFReasonDiscoveryDeadlineExceeded)
	trace := append([]EndpointAttemptSummary(nil), attemptTrace...)
	deadlineIndex := len(trace)
	if deadlineIndex < len(attempt.OrderedSeeds) {
		trace = append(trace, EndpointAttemptSummary{
			AttemptIndex: deadlineIndex, Endpoint: attempt.OrderedSeeds[deadlineIndex],
			Outcome: EndpointAttemptFailed, Reason: api.LegacyRFReasonDiscoveryDeadlineExceeded,
		})
		for index := deadlineIndex + 1; index < len(attempt.OrderedSeeds); index++ {
			trace = append(trace, EndpointAttemptSummary{
				AttemptIndex: index, Endpoint: attempt.OrderedSeeds[index], Outcome: EndpointAttemptSkipped,
			})
		}
	}
	return WorkerResult{Failure: &failure, AttemptTrace: trace}, nil
}

func makeAttemptFailure(index int, endpoint netip.AddrPort, err error) attemptFailure {
	boundary := normalizeBoundaryError(err)
	return attemptFailure{
		public: endpointAttemptFailure{
			AttemptIndex: index,
			Endpoint:     endpoint,
			Reason:       boundary.PublicFailure().Reason,
		},
		boundary: boundary,
	}
}

func failureAttemptSummary(failure attemptFailure) EndpointAttemptSummary {
	return EndpointAttemptSummary{AttemptIndex: failure.public.AttemptIndex, Endpoint: failure.public.Endpoint,
		Outcome: EndpointAttemptFailed, Reason: failure.public.Reason}
}

func validateCandidate(
	attempt Attempt,
	index int,
	endpoint netip.AddrPort,
	candidate Candidate,
) (AuthoritativeCandidate, error) {
	before, err := FingerprintObservation(candidate.Before)
	if err != nil {
		return AuthoritativeCandidate{}, NewBoundaryError(api.LegacyRFReasonTopologyInconsistent, err)
	}
	after, err := FingerprintObservation(candidate.After)
	if err != nil {
		return AuthoritativeCandidate{}, NewBoundaryError(api.LegacyRFReasonTopologyInconsistent, err)
	}
	if !equalFingerprints(before, after) {
		return AuthoritativeCandidate{}, NewBoundaryError(api.LegacyRFReasonSchemaDisagreement,
			errors.New("source fingerprints changed during observation"))
	}
	if candidate.Before.ClusterName != attempt.Connection.ExpectedClusterName {
		return AuthoritativeCandidate{}, NewBoundaryError(api.LegacyRFReasonIdentityMismatch,
			errors.New("source cluster name differs from expected cluster name"))
	}
	if candidate.Before.ServerType != api.ServerDistributionCassandra {
		return AuthoritativeCandidate{}, NewBoundaryError(api.LegacyRFReasonUnsupportedServerType,
			errors.New("source server type is not Apache Cassandra"))
	}
	if err = validateSourceVersion(candidate.Before.SourceVersion); err != nil {
		return AuthoritativeCandidate{}, err
	}
	externalDatacenters, err := validateTopology(candidate.Before.Topology, attempt.Connection.ManagedLocations)
	if err != nil {
		return AuthoritativeCandidate{}, err
	}
	if err = validateReplication(candidate.Replication, externalDatacenters); err != nil {
		return AuthoritativeCandidate{}, err
	}
	return AuthoritativeCandidate{
		AttemptID: attempt.AttemptID, AttemptIndex: index, Endpoint: endpoint,
		ClusterName:         candidate.Before.ClusterName,
		SourceVersion:       candidate.Before.SourceVersion,
		Partitioner:         candidate.Before.Partitioner,
		SchemaVersion:       candidate.Before.SchemaVersion,
		ObservedExternalDCs: externalDatacenters,
		Fingerprints:        before, Replication: candidate.Replication,
	}, nil
}

func validateSourceVersion(raw string) error {
	version, err := semver.NewVersion(raw)
	minimum := semver.MustParse(minimumSourceVersion)
	if err != nil || version.LessThan(minimum) {
		return NewBoundaryError(api.LegacyRFReasonUnsupportedSourceVersion,
			fmt.Errorf("source version is unsupported: %q", raw))
	}
	return nil
}

func validateTopology(hosts []TopologyHost, managed []ManagedLocation) ([]string, error) {
	addresses := make(map[netip.Addr]TopologyHost, len(hosts))
	hostIDs := make(map[string]TopologyHost, len(hosts))
	datacenters := make(map[string]struct{})
	managedNames := make(map[string]struct{}, len(managed))
	for _, location := range managed {
		if location.DatacenterName == "" {
			return nil, NewBoundaryError(api.LegacyRFReasonInvalidDiscoveryResult,
				errors.New("managed Cassandra datacenter name is required"))
		}
		managedNames[location.DatacenterName] = struct{}{}
	}
	for _, host := range hosts {
		address := host.Address.Unmap()
		if prior, found := addresses[address]; found &&
			(prior.HostID != host.HostID || prior.Datacenter != host.Datacenter) {
			return nil, NewBoundaryError(api.LegacyRFReasonTopologyInconsistent,
				errors.New("one source address has conflicting topology identities"))
		}
		if prior, found := hostIDs[host.HostID]; found &&
			(prior.Datacenter != host.Datacenter || prior.Address.Unmap() != address) {
			return nil, NewBoundaryError(api.LegacyRFReasonTopologyInconsistent,
				errors.New("one source host ID maps to conflicting addresses or datacenters"))
		}
		if _, collision := managedNames[host.Datacenter]; collision {
			return nil, NewBoundaryError(api.LegacyRFReasonManagedDatacenterNameCollision,
				errors.New("observed and managed datacenter names collide"))
		}
		addresses[address], hostIDs[host.HostID] = host, host
		datacenters[host.Datacenter] = struct{}{}
	}
	ordered := make([]string, 0, len(datacenters))
	for datacenter := range datacenters {
		ordered = append(ordered, datacenter)
	}
	sort.Strings(ordered)
	return ordered, nil
}

func validateReplication(replication SystemKeyspaceObservations, datacenters []string) error {
	known := make(map[string]struct{}, len(datacenters))
	for _, datacenter := range datacenters {
		known[datacenter] = struct{}{}
	}
	keyspaces := []struct {
		name        string
		observation KeyspaceObservation
	}{
		{api.SystemAuthKeyspace, replication.SystemAuth},
		{api.SystemTracesKeyspace, replication.SystemTraces},
		{api.SystemDistributedKeyspace, replication.SystemDistributed},
	}
	for _, keyspace := range keyspaces {
		if err := validateKeyspace(keyspace.name, keyspace.observation, known); err != nil {
			return err
		}
	}
	return nil
}

func validateKeyspace(name string, observation KeyspaceObservation, known map[string]struct{}) error {
	if !observation.Present {
		return NewBoundaryError(api.LegacyRFReasonMissingKeyspace,
			fmt.Errorf("required keyspace %s is missing", name))
	}
	if _, err := CanonicalizeNetworkTopologyStrategy(observation.Strategy); err != nil {
		return NewBoundaryError(api.LegacyRFReasonUnsupportedStrategy,
			fmt.Errorf("keyspace %s strategy is unsupported: %w", name, err))
	}
	if observation.Replication == nil {
		return NewBoundaryError(api.LegacyRFReasonInvalidReplication,
			fmt.Errorf("keyspace %s replication map is null", name))
	}
	for datacenter, factor := range observation.Replication {
		if datacenter == "" || factor < 1 {
			return NewBoundaryError(api.LegacyRFReasonInvalidReplication,
				fmt.Errorf("keyspace %s contains an invalid replication entry", name))
		}
		if _, found := known[datacenter]; !found {
			return NewBoundaryError(api.LegacyRFReasonInvalidReplication,
				fmt.Errorf("keyspace %s references an unobserved datacenter", name))
		}
	}
	return nil
}
