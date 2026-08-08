// Package discovery defines the read-only legacy Cassandra discovery protocol.
package discovery

import (
	"context"
	"net/netip"
	"time"

	api "github.com/k8ssandra/k8ssandra-operator/apis/k8ssandra/v1alpha1"
)

const (
	defaultCQLPort             uint16 = 9042
	MaximumAttemptTraceEntries int    = 256
	// MaximumTopologyHosts bounds system.peers_v2 accumulation per source observation.
	MaximumTopologyHosts int = 4096
)

// SecretBinding identifies source Secret metadata without containing Secret data.
type SecretBinding struct {
	Purpose         string   `json:"purpose"`
	SourceContext   string   `json:"sourceContext"`
	Namespace       string   `json:"namespace"`
	Name            string   `json:"name"`
	Keys            []string `json:"keys"`
	ResourceVersion string   `json:"resourceVersion"`
}

// ManagedLocation identifies a managed Cassandra resource search location and
// the Cassandra datacenter name owned by that resource.
type ManagedLocation struct {
	K8sContext     string `json:"k8sContext"`
	Namespace      string `json:"namespace"`
	Name           string `json:"name"`
	DatacenterName string `json:"datacenterName,omitempty"`
}

// Connection contains non-secret inputs required to observe one endpoint.
type Connection struct {
	ExpectedClusterName string            `json:"expectedClusterName"`
	SecretBindings      []SecretBinding   `json:"secretBindings,omitempty"`
	ManagedLocations    []ManagedLocation `json:"managedLocations"`
}

// Attempt binds a worker invocation to one immutable controller request.
type Attempt struct {
	ClusterUID        string           `json:"clusterUID"`
	Generation        int64            `json:"generation"`
	MarkerVersion     string           `json:"markerVersion"`
	ProtocolVersion   string           `json:"protocolVersion"`
	AttemptID         string           `json:"attemptID"`
	OrderedSeeds      []netip.AddrPort `json:"orderedSeeds"`
	SeedDigest        string           `json:"seedDigest"`
	WorkerImageDigest string           `json:"workerImageDigest"`
	Connection        Connection       `json:"connection"`
}

// TopologyHost is one host in the endpoint's observable topology.
type TopologyHost struct {
	Address    netip.Addr `json:"address"`
	HostID     string     `json:"hostID"`
	Datacenter string     `json:"datacenter"`
}

// SourceObservation is one internally consistent source read.
type SourceObservation struct {
	ClusterName   string                 `json:"clusterName"`
	ServerType    api.ServerDistribution `json:"serverType"`
	SourceVersion string                 `json:"sourceVersion"`
	Partitioner   string                 `json:"partitioner"`
	SchemaVersion string                 `json:"schemaVersion"`
	Topology      []TopologyHost         `json:"topology"`
}

// KeyspaceObservation preserves row presence, strategy, and sparse replication.
type KeyspaceObservation struct {
	Present     bool             `json:"present"`
	Strategy    string           `json:"strategy"`
	Replication map[string]int32 `json:"replication"`
}

// SystemKeyspaceObservations contains exactly the three supported keyspace rows.
type SystemKeyspaceObservations struct {
	SystemAuth        KeyspaceObservation `json:"systemAuth"`
	SystemTraces      KeyspaceObservation `json:"systemTraces"`
	SystemDistributed KeyspaceObservation `json:"systemDistributed"`
}

// Candidate is one complete endpoint attempt: state before, keyspaces, then state after.
type Candidate struct {
	Before      SourceObservation          `json:"before"`
	Replication SystemKeyspaceObservations `json:"replication"`
	After       SourceObservation          `json:"after"`
}

// CandidateFingerprints contains deterministic hashes of the accepted source view.
type CandidateFingerprints struct {
	Identity string `json:"identity"`
	Topology string `json:"topology"`
	Schema   string `json:"schema"`
}

// AuthoritativeCandidate records the sole accepted endpoint and its provenance.
type AuthoritativeCandidate struct {
	AttemptID           string                     `json:"attemptID"`
	AttemptIndex        int                        `json:"attemptIndex"`
	Endpoint            netip.AddrPort             `json:"endpoint"`
	ClusterName         string                     `json:"clusterName"`
	SourceVersion       string                     `json:"sourceVersion"`
	Partitioner         string                     `json:"partitioner"`
	SchemaVersion       string                     `json:"schemaVersion"`
	ObservedExternalDCs []string                   `json:"observedExternalDatacenters"`
	Fingerprints        CandidateFingerprints      `json:"fingerprints"`
	Replication         SystemKeyspaceObservations `json:"replication"`
}

// endpointAttemptFailure records one sanitized failed fallback attempt.
type endpointAttemptFailure struct {
	AttemptIndex int                         `json:"attemptIndex"`
	Endpoint     netip.AddrPort              `json:"endpoint"`
	Reason       api.LegacyRFDiscoveryReason `json:"reason"`
}

// EndpointAttemptOutcome is the sanitized public result of one ordered endpoint attempt.
type EndpointAttemptOutcome string

const (
	EndpointAttemptFailed   EndpointAttemptOutcome = "Failed"
	EndpointAttemptAccepted EndpointAttemptOutcome = "Accepted"
	EndpointAttemptSkipped  EndpointAttemptOutcome = "Skipped"
)

// EndpointAttemptSummary records only bounded endpoint identity and public outcome provenance.
type EndpointAttemptSummary struct {
	AttemptIndex int                         `json:"attemptIndex"`
	Endpoint     netip.AddrPort              `json:"endpoint"`
	Outcome      EndpointAttemptOutcome      `json:"outcome"`
	Reason       api.LegacyRFDiscoveryReason `json:"reason,omitempty"`
}

// WorkerResult is the concrete outcome of ordered fallback discovery.
type WorkerResult struct {
	Authoritative *AuthoritativeCandidate       `json:"authoritative,omitempty"`
	Failure       *api.LegacyRFDiscoveryFailure `json:"failure,omitempty"`
	AttemptTrace  []EndpointAttemptSummary      `json:"attemptTrace,omitempty"`
}

// EndpointObserver performs one complete endpoint-pinned, read-only attempt.
// Implementations must open a fresh session, issue only reads, and close it before return.
type EndpointObserver interface {
	DiscoverCandidate(context.Context, netip.AddrPort, Connection) (Candidate, error)
}

// Clock supplies time for deterministic deadline construction.
type Clock interface {
	Now() time.Time
}
