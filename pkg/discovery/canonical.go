package discovery

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/netip"
	"sort"
	"strconv"
	"strings"

	api "github.com/k8ssandra/k8ssandra-operator/apis/k8ssandra/v1alpha1"
)

// CanonicalizeSeeds validates, normalizes, and deduplicates contact points in spec order.
func CanonicalizeSeeds(rawSeeds []string) ([]netip.AddrPort, string, error) {
	canonical := make([]netip.AddrPort, 0, len(rawSeeds))
	seen := make(map[netip.Addr]struct{}, len(rawSeeds))
	for index, rawSeed := range rawSeeds {
		address, err := canonicalAddress(rawSeed)
		if err != nil {
			return nil, "", fmt.Errorf("canonicalize contact point %d: %w", index, err)
		}
		if _, duplicate := seen[address]; duplicate {
			continue
		}
		seen[address] = struct{}{}
		canonical = append(canonical, netip.AddrPortFrom(address, defaultCQLPort))
	}
	if len(canonical) == 0 {
		return nil, "", errors.New("at least one contact point is required")
	}
	digest, err := SeedDigest(canonical)
	if err != nil {
		return nil, "", fmt.Errorf("digest canonical contact points: %w", err)
	}
	return canonical, digest, nil
}

func canonicalAddress(raw string) (netip.Addr, error) {
	if raw == "" || strings.TrimSpace(raw) != raw {
		return netip.Addr{}, errors.New("contact point must not be empty or contain whitespace")
	}
	address, err := netip.ParseAddr(raw)
	if err != nil {
		return netip.Addr{}, fmt.Errorf("contact point must be an IP literal without a port: %w", err)
	}
	if address.Zone() != "" {
		return netip.Addr{}, errors.New("IPv6 zone identifiers are not supported")
	}
	return address.Unmap(), nil
}

// SeedDigest computes an order-sensitive digest of canonical endpoint strings.
func SeedDigest(seeds []netip.AddrPort) (string, error) {
	canonical := make([]string, len(seeds))
	for index, seed := range seeds {
		if !seed.IsValid() || seed.Addr().Zone() != "" || seed.Port() != defaultCQLPort {
			return "", fmt.Errorf("seed %d is not a canonical CQL endpoint", index)
		}
		canonical[index] = netip.AddrPortFrom(seed.Addr().Unmap(), defaultCQLPort).String()
	}
	encoded, err := json.Marshal(canonical)
	if err != nil {
		return "", fmt.Errorf("encode ordered seeds: %w", err)
	}
	return digestBytes(encoded), nil
}

// ParseReplicationFactor parses an ASCII decimal RF in the Cassandra int32 range.
func ParseReplicationFactor(raw string) (int32, error) {
	if raw == "" {
		return 0, errors.New("replication factor is empty")
	}
	for _, character := range []byte(raw) {
		if character < '0' || character > '9' {
			return 0, errors.New("replication factor must contain ASCII decimal digits only")
		}
	}
	value, err := strconv.ParseInt(raw, 10, 32)
	if err != nil || value < 1 {
		return 0, errors.New("replication factor must be between 1 and 2147483647")
	}
	return int32(value), nil
}

// CanonicalizeNetworkTopologyStrategy accepts only Cassandra's supported NTS aliases.
func CanonicalizeNetworkTopologyStrategy(strategy string) (string, error) {
	switch strategy {
	case api.NetworkTopologyStrategyClass, api.NetworkTopologyStrategyQualifiedClass:
		return api.NetworkTopologyStrategyQualifiedClass, nil
	default:
		return "", fmt.Errorf("unsupported replication strategy %q", strategy)
	}
}

// BindingsDigest deterministically hashes fully qualified Secret metadata.
func BindingsDigest(bindings []SecretBinding) (string, error) {
	canonical, err := canonicalBindings(bindings)
	if err != nil {
		return "", err
	}
	encoded, err := json.Marshal(canonical)
	if err != nil {
		return "", fmt.Errorf("encode qualified Secret bindings: %w", err)
	}
	return digestBytes(encoded), nil
}

func canonicalBindings(bindings []SecretBinding) ([]SecretBinding, error) {
	canonical := make([]SecretBinding, len(bindings))
	for index, binding := range bindings {
		if binding.Purpose == "" || binding.Namespace == "" || binding.Name == "" || binding.ResourceVersion == "" {
			return nil, fmt.Errorf("binding %d is not fully qualified", index)
		}
		canonical[index] = binding
		canonical[index].Keys = append([]string(nil), binding.Keys...)
		sort.Strings(canonical[index].Keys)
	}
	sort.Slice(canonical, func(left, right int) bool {
		return bindingKey(canonical[left]) < bindingKey(canonical[right])
	})
	return canonical, nil
}

func bindingKey(binding SecretBinding) string {
	return strings.Join([]string{
		binding.Purpose, binding.SourceContext, binding.Namespace,
		binding.Name, binding.ResourceVersion, strings.Join(binding.Keys, "\x00"),
	}, "\x00")
}

// FingerprintObservation hashes identity, topology, and schema independently.
func FingerprintObservation(observation SourceObservation) (CandidateFingerprints, error) {
	if err := validateObservationFields(observation); err != nil {
		return CandidateFingerprints{}, err
	}
	identity, err := json.Marshal(struct {
		ClusterName   string                 `json:"clusterName"`
		ServerType    api.ServerDistribution `json:"serverType"`
		SourceVersion string                 `json:"sourceVersion"`
		Partitioner   string                 `json:"partitioner"`
	}{observation.ClusterName, observation.ServerType, observation.SourceVersion, observation.Partitioner})
	if err != nil {
		return CandidateFingerprints{}, fmt.Errorf("encode source identity: %w", err)
	}
	topology, err := canonicalTopology(observation.Topology)
	if err != nil {
		return CandidateFingerprints{}, err
	}
	schema, err := json.Marshal(struct {
		SchemaVersion string `json:"schemaVersion"`
	}{observation.SchemaVersion})
	if err != nil {
		return CandidateFingerprints{}, fmt.Errorf("encode schema identity: %w", err)
	}
	return CandidateFingerprints{
		Identity: digestBytes(identity), Topology: digestBytes(topology), Schema: digestBytes(schema),
	}, nil
}

func validateObservationFields(observation SourceObservation) error {
	if observation.ClusterName == "" || observation.ServerType == "" || observation.SourceVersion == "" ||
		observation.Partitioner == "" || observation.SchemaVersion == "" {
		return errors.New("source observation has incomplete identity or schema fields")
	}
	if len(observation.Topology) == 0 {
		return errors.New("source observation has no topology hosts")
	}
	return nil
}

func canonicalTopology(hosts []TopologyHost) ([]byte, error) {
	canonical := append([]TopologyHost(nil), hosts...)
	for index := range canonical {
		if !canonical[index].Address.IsValid() || canonical[index].Address.Zone() != "" ||
			canonical[index].HostID == "" || canonical[index].Datacenter == "" {
			return nil, fmt.Errorf("topology host %d is incomplete", index)
		}
		canonical[index].Address = canonical[index].Address.Unmap()
	}
	sort.Slice(canonical, func(left, right int) bool {
		return topologyKey(canonical[left]) < topologyKey(canonical[right])
	})
	for index := 1; index < len(canonical); index++ {
		if topologyKey(canonical[index-1]) == topologyKey(canonical[index]) {
			return nil, errors.New("source topology contains a duplicate host observation")
		}
	}
	encoded, err := json.Marshal(canonical)
	if err != nil {
		return nil, fmt.Errorf("encode source topology: %w", err)
	}
	return encoded, nil
}

func topologyKey(host TopologyHost) string {
	return host.Address.String() + "\x00" + host.HostID + "\x00" + host.Datacenter
}

func equalFingerprints(left, right CandidateFingerprints) bool {
	return left == right
}

func equalBindings(left, right []SecretBinding) (bool, error) {
	leftDigest, err := BindingsDigest(left)
	if err != nil {
		return false, err
	}
	rightDigest, err := BindingsDigest(right)
	if err != nil {
		return false, err
	}
	return bytes.Equal([]byte(leftDigest), []byte(rightDigest)), nil
}

func digestBytes(value []byte) string {
	digest := sha256.Sum256(value)
	return hex.EncodeToString(digest[:])
}
