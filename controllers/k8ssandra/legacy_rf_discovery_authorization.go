package k8ssandra

import (
	"context"
	"errors"
	"fmt"

	api "github.com/k8ssandra/k8ssandra-operator/apis/k8ssandra/v1alpha1"
	"github.com/k8ssandra/k8ssandra-operator/pkg/discovery"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
)

// LegacyRFManagedCreateCallback performs the single managed datacenter create
// using the immutable accepted seed set supplied by authorization.
type LegacyRFManagedCreateCallback func(context.Context, []string) error

// LegacyRFManagedCreationInput binds one final authorization to an accepted snapshot.
type LegacyRFManagedCreationInput struct {
	Key                  types.NamespacedName
	ExpectedUID          types.UID
	ExpectedSnapshotHash string
	Target               api.LegacyRFManagedLocation
	Create               LegacyRFManagedCreateCallback
}

// AuthorizeLegacyRFManagedCreation performs the named final safety operation
// immediately before invoking the managed datacenter create callback.
func AuthorizeLegacyRFManagedCreation(
	ctx context.Context,
	control LegacyRFControlPlane,
	managedState ManagedDatacenterState,
	input LegacyRFManagedCreationInput,
) error {
	if control == nil || managedState == nil || input.Create == nil {
		return fmt.Errorf("authorize managed creation: control plane, managed state, and create callback are required")
	}
	current, authorization, err := readLegacyRFCreateAuthorization(ctx, control, input)
	if err != nil {
		return err
	}
	if err = managedState.AssertAbsent(ctx, authorization.ManagedSearchDomain, true); err != nil {
		return managedSurveyBoundary("survey immediately before managed creation", err)
	}
	if managedCreationStatusNeedsUpdate(current.Status.LegacyRFDiscovery, input.Target) {
		current, err = recordLegacyRFManagedCreation(ctx, control, current, input.Target)
		if err != nil {
			return err
		}
		if err = verifyLegacyRFManagedCreationReadBack(current, input); err != nil {
			return legacyRFStatusBoundary(api.LegacyRFReasonKubernetesAPIConflict, "verify managed creation history", err)
		}
	}
	if err = input.Create(ctx, append([]string(nil), authorization.AcceptedSeeds...)); err != nil {
		if apierrors.IsAlreadyExists(err) {
			return resurveyLegacyRFAfterAlreadyExists(ctx, control, managedState, input, err)
		}
		return fmt.Errorf("create authorized managed datacenter: %w", err)
	}
	return nil
}

func readLegacyRFCreateAuthorization(
	ctx context.Context,
	control LegacyRFControlPlane,
	input LegacyRFManagedCreationInput,
) (*api.K8ssandraCluster, LegacyRFCreateAuthorization, error) {
	current, err := control.Read(ctx, input.Key)
	if err != nil {
		return nil, LegacyRFCreateAuthorization{}, legacyRFStatusBoundary(
			api.LegacyRFReasonKubernetesAPIUnavailable, "read cluster before managed creation", err)
	}
	status := current.Status.LegacyRFDiscovery
	if err = validateLegacyRFCreateReadBack(current, status, input); err != nil {
		return nil, LegacyRFCreateAuthorization{}, legacyRFStatusBoundary(
			api.LegacyRFReasonSnapshotConflict, "validate accepted snapshot before managed creation", err)
	}
	plan, err := BuildLegacyRFCurrentPlan(current)
	if err != nil {
		return nil, LegacyRFCreateAuthorization{}, legacyRFStatusBoundary(
			api.LegacyRFReasonStaleDiscoveryResult, "build current plan before managed creation", err)
	}
	if !containsManagedLocation(plan.ManagedLocations, input.Target) {
		return nil, LegacyRFCreateAuthorization{}, legacyRFStatusBoundary(
			api.LegacyRFReasonStaleDiscoveryResult, "validate managed creation target", errors.New("target is not in the current plan"))
	}
	authorization, err := BuildLegacyRFCreateAuthorization(status.AcceptedSnapshot, status, plan)
	if err != nil {
		return nil, LegacyRFCreateAuthorization{}, legacyRFStatusBoundary(
			api.LegacyRFReasonStaleDiscoveryResult, "validate current plan before managed creation", err)
	}
	return current, authorization, nil
}

func validateLegacyRFCreateReadBack(
	cluster *api.K8ssandraCluster,
	status *api.LegacyRFDiscoveryStatus,
	input LegacyRFManagedCreationInput,
) error {
	if cluster == nil || cluster.UID != input.ExpectedUID || input.ExpectedUID == "" {
		return errors.New("cluster UID changed")
	}
	if cluster.Annotations[api.LegacyRFDiscoveryMarkerAnnotation] != api.LegacyRFDiscoveryMarkerVersion {
		return errors.New("discovery marker changed")
	}
	if !legacyRFSnapshotIsIntact(status) || status.SnapshotHash != input.ExpectedSnapshotHash {
		return errors.New("accepted snapshot read-back hash changed")
	}
	_, digest, err := discovery.CanonicalizeSeeds(status.AcceptedSnapshot.AcceptedSeeds)
	if err != nil {
		return fmt.Errorf("canonicalize accepted seeds: %w", err)
	}
	if digest != status.AcceptedSnapshot.AcceptedSeedDigest {
		return errors.New("accepted seed digest changed")
	}
	return validateManagedLocation(input.Target)
}

func managedCreationStatusNeedsUpdate(
	status *api.LegacyRFDiscoveryStatus,
	target api.LegacyRFManagedLocation,
) bool {
	return status == nil || !status.ManagedCreationObserved || !containsManagedLocation(status.ManagedLocationHistory, target)
}

func recordLegacyRFManagedCreation(
	ctx context.Context,
	control LegacyRFControlPlane,
	current *api.K8ssandraCluster,
	target api.LegacyRFManagedLocation,
) (*api.K8ssandraCluster, error) {
	plan, err := BuildLegacyRFCurrentPlan(current)
	if err != nil {
		return nil, legacyRFStatusBoundary(api.LegacyRFReasonStaleDiscoveryResult, "build plan for history update", err)
	}
	nextStatus, err := BuildLegacyRFManagedCreationStatus(current.Status.LegacyRFDiscovery, plan.ManagedLocations, target)
	if err != nil {
		return nil, legacyRFStatusBoundary(api.LegacyRFReasonSnapshotConflict, "build managed creation history", err)
	}
	updated := current.DeepCopy()
	updated.Status.LegacyRFDiscovery = &nextStatus
	if err = control.PatchStatusCAS(ctx, current, updated); err != nil {
		return nil, legacyRFStatusBoundary(api.LegacyRFReasonKubernetesAPIConflict, "patch managed creation history", err)
	}
	readBack, err := control.Read(ctx, types.NamespacedName{Namespace: current.Namespace, Name: current.Name})
	if err != nil {
		return nil, legacyRFStatusBoundary(api.LegacyRFReasonKubernetesAPIUnavailable, "read back managed creation history", err)
	}
	return readBack, nil
}

func verifyLegacyRFManagedCreationReadBack(
	cluster *api.K8ssandraCluster,
	input LegacyRFManagedCreationInput,
) error {
	status := cluster.Status.LegacyRFDiscovery
	if err := validateLegacyRFCreateReadBack(cluster, status, input); err != nil {
		return err
	}
	if !status.ManagedCreationObserved || !containsManagedLocation(status.ManagedLocationHistory, input.Target) {
		return errors.New("managed creation history update was not persisted")
	}
	return nil
}

func resurveyLegacyRFAfterAlreadyExists(
	ctx context.Context,
	control LegacyRFControlPlane,
	managedState ManagedDatacenterState,
	input LegacyRFManagedCreationInput,
	createErr error,
) error {
	_, authorization, err := readLegacyRFCreateAuthorization(ctx, control, input)
	if err != nil {
		return fmt.Errorf("managed create returned AlreadyExists; fresh authorization failed: %w", err)
	}
	if err = managedState.AssertAbsent(ctx, authorization.ManagedSearchDomain, true); err != nil {
		return fmt.Errorf("managed create returned AlreadyExists; fresh survey failed: %w", err)
	}
	return fmt.Errorf("managed create returned AlreadyExists after a fresh full survey: %w", createErr)
}
