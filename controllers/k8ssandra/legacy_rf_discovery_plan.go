package k8ssandra

import (
	"fmt"

	api "github.com/k8ssandra/k8ssandra-operator/apis/k8ssandra/v1alpha1"
)

// LegacyRFCurrentPlan is the normalized subset of current desired state relevant to
// accepted legacy replication discovery.
type LegacyRFCurrentPlan struct {
	ExpectedClusterName    string
	ServerType             api.ServerDistribution
	ManagedDatacenterNames []string
	ManagedLocations       []api.LegacyRFManagedLocation
	ExternalDatacenters    []string
	ExternalDatacentersSet bool
}

// LegacyRFCreateAuthorization is immutable input for the final managed-DC creation check.
type LegacyRFCreateAuthorization struct {
	AcceptedSeeds       []string
	AcceptedSeedDigest  string
	ManagedSearchDomain []api.LegacyRFManagedLocation
}

// BuildLegacyRFCurrentPlan derives a validation value from the current cluster object.
func BuildLegacyRFCurrentPlan(cluster *api.K8ssandraCluster) (LegacyRFCurrentPlan, error) {
	if cluster == nil || cluster.Spec.Cassandra == nil {
		return LegacyRFCurrentPlan{}, fmt.Errorf("build legacy RF current plan: Cassandra spec is required")
	}

	plan := LegacyRFCurrentPlan{
		ExpectedClusterName:    cluster.CassClusterName(),
		ServerType:             effectiveServerType(cluster.Spec.Cassandra.ServerType),
		ExternalDatacenters:    append([]string(nil), cluster.Spec.ExternalDatacenters...),
		ExternalDatacentersSet: cluster.Spec.ExternalDatacenters != nil,
	}
	for _, datacenter := range cluster.Spec.Cassandra.Datacenters {
		name := datacenter.CassDcName()
		namespace := datacenter.Meta.Namespace
		if namespace == "" {
			namespace = cluster.Namespace
		}
		plan.ManagedDatacenterNames = append(plan.ManagedDatacenterNames, name)
		plan.ManagedLocations = append(plan.ManagedLocations, api.LegacyRFManagedLocation{
			K8sContext: datacenter.K8sContext, Namespace: namespace, Name: datacenter.Meta.Name,
			DatacenterName: name,
		})
	}
	return plan, nil
}

// ValidateCurrentPlan verifies that current desired state remains compatible with snapshot.
func ValidateCurrentPlan(snapshot *api.LegacyRFSnapshot, currentSpec LegacyRFCurrentPlan) error {
	if snapshot == nil {
		return fmt.Errorf("validate legacy RF current plan: accepted snapshot is required")
	}
	if currentSpec.ExpectedClusterName != snapshot.ExpectedClusterName {
		return fmt.Errorf("validate legacy RF current plan: expected cluster name changed")
	}
	if currentSpec.ServerType != snapshot.ServerType {
		return fmt.Errorf("validate legacy RF current plan: server type changed")
	}
	if collision := firstCollision(currentSpec.ManagedDatacenterNames, snapshot.ObservedExternalDCs); collision != "" {
		return fmt.Errorf("validate legacy RF current plan: managed datacenter %q collides with external topology", collision)
	}
	if currentSpec.ExternalDatacentersSet && !sameStringSet(currentSpec.ExternalDatacenters, snapshot.ObservedExternalDCs) {
		return fmt.Errorf("validate legacy RF current plan: externalDatacenters must exactly match the accepted external set")
	}
	return nil
}

// BuildLegacyRFCreateAuthorization returns retained seeds and the complete safety search domain.
func BuildLegacyRFCreateAuthorization(
	snapshot *api.LegacyRFSnapshot,
	status *api.LegacyRFDiscoveryStatus,
	currentSpec LegacyRFCurrentPlan,
) (LegacyRFCreateAuthorization, error) {
	if err := ValidateCurrentPlan(snapshot, currentSpec); err != nil {
		return LegacyRFCreateAuthorization{}, err
	}
	if status == nil {
		return LegacyRFCreateAuthorization{}, fmt.Errorf("build legacy RF create authorization: discovery status is required")
	}

	locations := append([]api.LegacyRFManagedLocation(nil), currentSpec.ManagedLocations...)
	locations = appendUniqueLocations(locations, status.CurrentManagedLocations)
	locations = appendUniqueLocations(locations, snapshot.AcceptedManagedLocations)
	locations = appendUniqueLocations(locations, status.ManagedLocationHistory)
	return LegacyRFCreateAuthorization{
		AcceptedSeeds:       append([]string(nil), snapshot.AcceptedSeeds...),
		AcceptedSeedDigest:  snapshot.AcceptedSeedDigest,
		ManagedSearchDomain: locations,
	}, nil
}

func effectiveServerType(serverType api.ServerDistribution) api.ServerDistribution {
	if serverType == "" {
		return api.ServerDistributionCassandra
	}
	return serverType
}

func firstCollision(managed, external []string) string {
	externalNames := make(map[string]struct{}, len(external))
	for _, name := range external {
		externalNames[name] = struct{}{}
	}
	for _, name := range managed {
		if _, found := externalNames[name]; found {
			return name
		}
	}
	return ""
}

func sameStringSet(left, right []string) bool {
	leftSet := stringSet(left)
	rightSet := stringSet(right)
	if len(leftSet) != len(rightSet) {
		return false
	}
	for value := range leftSet {
		if _, found := rightSet[value]; !found {
			return false
		}
	}
	return true
}

func stringSet(values []string) map[string]struct{} {
	set := make(map[string]struct{}, len(values))
	for _, value := range values {
		set[value] = struct{}{}
	}
	return set
}

func appendUniqueLocations(
	locations []api.LegacyRFManagedLocation,
	additional []api.LegacyRFManagedLocation,
) []api.LegacyRFManagedLocation {
	seen := make(map[api.LegacyRFManagedLocation]struct{}, len(locations)+len(additional))
	for _, location := range locations {
		seen[location] = struct{}{}
	}
	for _, location := range additional {
		if _, found := seen[location]; !found {
			locations = append(locations, location)
			seen[location] = struct{}{}
		}
	}
	return locations
}
