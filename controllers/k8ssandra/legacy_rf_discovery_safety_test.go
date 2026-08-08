package k8ssandra

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/netip"
	"strings"
	"testing"
	"time"

	"github.com/go-logr/logr"
	api "github.com/k8ssandra/k8ssandra-operator/apis/k8ssandra/v1alpha1"
	"github.com/k8ssandra/k8ssandra-operator/pkg/clientcache"
	"github.com/k8ssandra/k8ssandra-operator/pkg/discovery"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

type fixedDiscoveryClock struct{ now time.Time }

func (clock fixedDiscoveryClock) Now() time.Time { return clock.now }

type fixedDiscoveryBackoff struct{ delay time.Duration }

func (backoff fixedDiscoveryBackoff) Delay(int32) time.Duration { return backoff.delay }

type recordingDiscoveryBackoff struct {
	delay    time.Duration
	failures []int32
}

func (backoff *recordingDiscoveryBackoff) Delay(failures int32) time.Duration {
	backoff.failures = append(backoff.failures, failures)
	return backoff.delay
}

func requestKeys(requests []reconcile.Request) []types.NamespacedName {
	keys := make([]types.NamespacedName, len(requests))
	for index := range requests {
		keys[index] = requests[index].NamespacedName
	}
	return keys
}

func drainEvents(recorder *record.FakeRecorder) []string {
	events := make([]string, 0)
	for {
		select {
		case event := <-recorder.Events:
			events = append(events, event)
		default:
			return events
		}
	}
}

func TestDecideLegacyRFDiscoveryQualificationAndFailurePrecedence(t *testing.T) {
	tests := []struct {
		name       string
		mutate     func(*api.K8ssandraCluster)
		failure    error
		managed    bool
		wantPhase  api.LegacyRFDiscoveryPhase
		wantReason api.LegacyRFDiscoveryReason
		wantPlace  api.LegacyRFManagedLocation
	}{
		{name: "marked Cassandra with seeds qualifies", wantPhase: api.LegacyRFDiscoveryPhasePending,
			wantPlace: api.LegacyRFManagedLocation{K8sContext: "plane-a", Namespace: "data", Name: "dc-a", DatacenterName: "managed-a"}},
		{name: "unmarked is not required", mutate: func(cluster *api.K8ssandraCluster) {
			delete(cluster.Annotations, api.LegacyRFDiscoveryMarkerAnnotation)
		}, wantPhase: api.LegacyRFDiscoveryPhaseNotRequired},
		{name: "marked without seeds is not required", mutate: func(cluster *api.K8ssandraCluster) {
			cluster.Spec.Cassandra.AdditionalSeeds = nil
		}, wantPhase: api.LegacyRFDiscoveryPhaseNotRequired},
		{name: "accepted missing snapshot conflicts after seeds are removed", mutate: func(cluster *api.K8ssandraCluster) {
			cluster.Spec.Cassandra.AdditionalSeeds = nil
			cluster.Status.LegacyRFDiscovery = &api.LegacyRFDiscoveryStatus{Phase: api.LegacyRFDiscoveryPhaseAccepted}
		}, wantPhase: api.LegacyRFDiscoveryPhaseBlocked, wantReason: api.LegacyRFReasonSnapshotConflict},
		{name: "accepted corrupt snapshot conflicts after seeds are removed", mutate: func(cluster *api.K8ssandraCluster) {
			_, _, _, snapshot := acceptanceFixture(t)
			cluster.Spec.Cassandra.AdditionalSeeds = nil
			cluster.Status.LegacyRFDiscovery = acceptedStatus(snapshot)
			cluster.Status.LegacyRFDiscovery.AcceptedSnapshot.Replication.SystemAuth["legacy-a"]++
		}, wantPhase: api.LegacyRFDiscoveryPhaseBlocked, wantReason: api.LegacyRFReasonSnapshotConflict},
		{name: "DSE retains existing behavior without preservation", mutate: func(cluster *api.K8ssandraCluster) {
			cluster.Spec.Cassandra.ServerType = api.ServerDistributionDse
		}, wantPhase: api.LegacyRFDiscoveryPhaseNotRequired, wantReason: api.LegacyRFReasonUnsupportedServerType},
		{name: "HCD retains existing behavior without preservation", mutate: func(cluster *api.K8ssandraCluster) {
			cluster.Spec.Cassandra.ServerType = api.ServerDistributionHcd
		}, wantPhase: api.LegacyRFDiscoveryPhaseNotRequired, wantReason: api.LegacyRFReasonUnsupportedServerType},
		{name: "external Secrets provider blocks", mutate: func(cluster *api.K8ssandraCluster) {
			cluster.Spec.SecretsProvider = "external"
		}, wantPhase: api.LegacyRFDiscoveryPhaseBlocked, wantReason: api.LegacyRFReasonUnsupportedSecretsProvider},
		{name: "managed state is too late", managed: true, wantPhase: api.LegacyRFDiscoveryPhaseBlocked,
			wantReason: api.LegacyRFReasonDiscoveryTooLate},
		{name: "source below 4 is permanent", failure: discovery.NewBoundaryError(api.LegacyRFReasonUnsupportedSourceVersion, errors.New("3.11.17 password-canary")),
			wantPhase: api.LegacyRFDiscoveryPhaseBlocked, wantReason: api.LegacyRFReasonUnsupportedSourceVersion},
		{name: "transient source failure blocks the completed attempt", failure: discovery.NewBoundaryError(api.LegacyRFReasonTLSFailed, errors.New("certificate-canary")),
			wantPhase: api.LegacyRFDiscoveryPhaseBlocked, wantReason: api.LegacyRFReasonTLSFailed},
		{name: "accepted remains accepted after managed creation", managed: true, mutate: func(cluster *api.K8ssandraCluster) {
			_, _, _, snapshot := acceptanceFixture(t)
			cluster.Status.LegacyRFDiscovery = acceptedStatus(snapshot)
		}, wantPhase: api.LegacyRFDiscoveryPhaseAccepted},
		{name: "accepted incompatible plan blocks", mutate: func(cluster *api.K8ssandraCluster) {
			_, _, _, snapshot := acceptanceFixture(t)
			cluster.Status.LegacyRFDiscovery = acceptedStatus(snapshot)
			cluster.Spec.Cassandra.ClusterName = "changed"
		}, wantPhase: api.LegacyRFDiscoveryPhaseBlocked, wantReason: api.LegacyRFReasonSnapshotConflict},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cluster := safetyCluster()
			if test.mutate != nil {
				test.mutate(cluster)
			}
			decision := DecideLegacyRFDiscovery(cluster, test.managed, test.failure)
			require.Equal(t, test.wantPhase, decision.Phase)
			require.Equal(t, test.wantReason, decision.Reason)
			if test.wantPlace == (api.LegacyRFManagedLocation{}) {
				require.Empty(t, decision.AttemptLocations)
			} else {
				require.NotEmpty(t, decision.AttemptLocations)
				require.Equal(t, test.wantPlace, decision.AttemptLocations[0])
			}
			require.NotContains(t, decision.Message, "canary")
		})
	}
}

func TestDecideLegacyRFDiscoveryEnforcesSeedLimitBeforeAttempt(t *testing.T) {
	cluster := safetyCluster()
	seeds := make([]string, api.LegacyRFDiscoveryMaxSeeds+1)
	for index := range seeds {
		seeds[index] = fmt.Sprintf("192.0.2.%d", index+1)
	}

	cluster.Spec.Cassandra.AdditionalSeeds = seeds[:api.LegacyRFDiscoveryMaxSeeds]
	require.Equal(t, api.LegacyRFDiscoveryPhasePending, DecideLegacyRFDiscovery(cluster, false, nil).Phase)

	cluster.Spec.Cassandra.AdditionalSeeds = seeds
	decision := DecideLegacyRFDiscovery(cluster, false, nil)
	require.Equal(t, api.LegacyRFDiscoveryPhaseBlocked, decision.Phase)
	require.Equal(t, api.LegacyRFReasonInvalidContactPoint, decision.Reason)
}

func TestLegacyRFFailurePhaseAndRetrySchedulingAreIndependent(t *testing.T) {
	tests := []struct {
		name        string
		reason      api.LegacyRFDiscoveryReason
		wantRequeue bool
	}{
		{name: "authentication can retry", reason: api.LegacyRFReasonAuthenticationRejected, wantRequeue: true},
		{name: "TLS can retry", reason: api.LegacyRFReasonTLSFailed, wantRequeue: true},
		{name: "network can retry", reason: api.LegacyRFReasonContactUnreachable, wantRequeue: true},
		{name: "Job scheduling can retry", reason: api.LegacyRFReasonJobSchedulingFailed, wantRequeue: true},
		{name: "image pull can retry", reason: api.LegacyRFReasonWorkerImagePullFailed, wantRequeue: true},
		{name: "API conflict can retry", reason: api.LegacyRFReasonKubernetesAPIConflict, wantRequeue: true},
		{name: "snapshot conflict is permanent", reason: api.LegacyRFReasonSnapshotConflict},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			decision := legacyRFFailureDecision(test.reason)
			require.Equal(t, api.LegacyRFDiscoveryPhaseBlocked, decision.Phase)
			require.Equal(t, test.wantRequeue, decision.Retryable)

			cluster := safetyCluster()
			runtime := &legacyRFControllerRuntime{
				clock: fixedDiscoveryClock{now: time.Unix(100, 0)}, recorder: record.NewFakeRecorder(2),
				backoff: fixedDiscoveryBackoff{delay: 7 * time.Second},
			}
			stopped := runtime.stop(cluster, decision)
			output, err := stopped.Output()
			require.NoError(t, err)
			require.Equal(t, test.wantRequeue, stopped.IsRequeue())
			if test.wantRequeue {
				require.Equal(t, 7*time.Second, output.RequeueAfter)
			} else {
				require.Zero(t, output.RequeueAfter)
			}
		})
	}
}

func TestLegacyRFRetryBackoffUsesConsecutiveFailureHistory(t *testing.T) {
	cluster := safetyCluster()
	backoff := &recordingDiscoveryBackoff{delay: time.Second}
	runtime := &legacyRFControllerRuntime{
		clock: fixedDiscoveryClock{now: time.Unix(100, 0)}, recorder: record.NewFakeRecorder(4), backoff: backoff,
	}
	decision := legacyRFFailureDecision(api.LegacyRFReasonContactUnreachable)

	runtime.stop(cluster, decision)
	runtime.stop(cluster, decision)

	require.Equal(t, []int32{0, 1}, backoff.failures)
}

func TestAcceptedReadBackFailurePreservesAcceptedStatus(t *testing.T) {
	scheme := runtime.NewScheme()
	require.NoError(t, api.AddToScheme(scheme))
	cached := fake.NewClientBuilder().WithScheme(scheme).Build()
	direct := fake.NewClientBuilder().WithScheme(scheme).Build()
	clients, err := clientcache.NewValidated(cached, direct, scheme)
	require.NoError(t, err)
	cluster := safetyCluster()
	_, _, _, snapshot := acceptanceFixture(t)
	cluster.Status.LegacyRFDiscovery = acceptedStatus(snapshot)
	backoff := &recordingDiscoveryBackoff{delay: time.Second}
	runtime := &legacyRFControllerRuntime{
		clients: clients, clock: fixedDiscoveryClock{now: time.Unix(100, 0)},
		recorder: record.NewFakeRecorder(2), backoff: backoff,
	}

	reconcileResult := runtime.ReconcileGate(context.Background(), cluster, logr.Discard())

	require.True(t, reconcileResult.IsRequeue())
	require.Equal(t, api.LegacyRFDiscoveryPhaseAccepted, cluster.Status.LegacyRFDiscovery.Phase)
	require.Equal(t, snapshot, cluster.Status.LegacyRFDiscovery.AcceptedSnapshot)
	require.Empty(t, cluster.Status.LegacyRFDiscovery.Reason)
	require.Equal(t, []int32{0}, backoff.failures)
}

func TestAcceptedCleanupFailurePreservesAcceptedStatus(t *testing.T) {
	scheme := runtime.NewScheme()
	require.NoError(t, api.AddToScheme(scheme))
	cluster := safetyCluster()
	_, _, _, snapshot := acceptanceFixture(t)
	cluster.Status.LegacyRFDiscovery = acceptedStatus(snapshot)
	cached := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster).Build()
	direct := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster).Build()
	clients, err := clientcache.NewValidated(cached, direct, scheme)
	require.NoError(t, err)
	backoff := &recordingDiscoveryBackoff{delay: time.Second}
	runtime := &legacyRFControllerRuntime{
		clients: clients, clock: fixedDiscoveryClock{now: time.Unix(100, 0)}, keyReader: strings.NewReader("unused"),
		recorder: record.NewFakeRecorder(2), backoff: backoff,
	}

	reconcileResult := runtime.ReconcileGate(context.Background(), cluster, logr.Discard())

	require.True(t, reconcileResult.IsRequeue())
	require.Equal(t, api.LegacyRFDiscoveryPhaseAccepted, cluster.Status.LegacyRFDiscovery.Phase)
	require.Equal(t, snapshot, cluster.Status.LegacyRFDiscovery.AcceptedSnapshot)
	require.Empty(t, cluster.Status.LegacyRFDiscovery.Reason)
	require.Equal(t, []int32{0}, backoff.failures)
}

func TestSecretWatchImmediatelyEnqueuesBlockedDiscovery(t *testing.T) {
	scheme := runtime.NewScheme()
	require.NoError(t, api.AddToScheme(scheme))
	cluster := safetyCluster()
	cluster.Spec.Cassandra.LegacyCqlCredentialsSecretRef = &corev1.LocalObjectReference{Name: "legacy-auth"}
	cluster.Status.LegacyRFDiscovery = &api.LegacyRFDiscoveryStatus{
		Phase: api.LegacyRFDiscoveryPhaseBlocked, Reason: api.LegacyRFReasonAuthenticationRejected,
	}

	indexedClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster).
		WithIndex(&api.K8ssandraCluster{}, legacyRFCQLCredentialsSecretRefIndex, legacyRFCQLCredentialsSecretRefValues).
		WithIndex(&api.K8ssandraCluster{}, legacyRFCQLTLSSecretRefIndex, legacyRFCQLTLSSecretRefValues).Build()
	requests := legacyRFReferencedSecretRequests(context.Background(), indexedClient,
		&corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: cluster.Namespace, Name: "legacy-auth"}})
	require.Equal(t, []types.NamespacedName{{Namespace: cluster.Namespace, Name: cluster.Name}}, requestKeys(requests))
}

func TestRetryTransitionIsBlockedThenPendingAndSuppressesIdenticalEvents(t *testing.T) {
	cluster := safetyCluster()
	recorder := record.NewFakeRecorder(4)
	blocked := legacyRFFailureDecision(api.LegacyRFReasonContactUnreachable)
	pending := LegacyRFDiscoveryDecision{Phase: api.LegacyRFDiscoveryPhasePending}

	require.True(t, ApplyLegacyRFDiscoveryDecision(cluster, blocked, metav1.NewTime(time.Unix(100, 0)), recorder))
	require.False(t, ApplyLegacyRFDiscoveryDecision(cluster, blocked, metav1.NewTime(time.Unix(150, 0)), recorder))
	require.True(t, ApplyLegacyRFDiscoveryDecision(cluster, pending, metav1.NewTime(time.Unix(200, 0)), recorder))
	require.False(t, ApplyLegacyRFDiscoveryDecision(cluster, pending, metav1.NewTime(time.Unix(250, 0)), recorder))
	require.Equal(t, api.LegacyRFDiscoveryPhasePending, cluster.Status.LegacyRFDiscovery.Phase)
	require.Equal(t, metav1.NewTime(time.Unix(200, 0)), *cluster.Status.LegacyRFDiscovery.LastTransitionTime)
	require.Len(t, drainEvents(recorder), 2)
}

func TestRetryCleanupRequiresAuthoritativeManagedStateAbsence(t *testing.T) {
	attempt := discovery.Attempt{Connection: discovery.Connection{ManagedLocations: []discovery.ManagedLocation{
		{K8sContext: "plane-a", Namespace: "data", Name: "dc-a"},
	}}}
	managed := &traceManagedState{}
	runtime := &legacyRFControllerRuntime{managed: managed}

	require.NoError(t, runtime.AllowCleanup(context.Background(), attempt))
	require.Equal(t, 1, managed.calls)

	managed.err = errors.New("API unavailable")
	require.Error(t, runtime.AllowCleanup(context.Background(), attempt))
}

func TestPermanentBlockedStatusDoesNotResolveInputsOrCreateRetryAttempt(t *testing.T) {
	for _, mutate := range []func(*api.K8ssandraCluster){
		func(*api.K8ssandraCluster) {},
		func(cluster *api.K8ssandraCluster) { cluster.Spec.Cassandra.AdditionalSeeds = nil },
		func(cluster *api.K8ssandraCluster) {
			delete(cluster.Annotations, api.LegacyRFDiscoveryMarkerAnnotation)
		},
	} {
		cluster := safetyCluster()
		decision := legacyRFFailureDecision(api.LegacyRFReasonSnapshotConflict)
		ApplyLegacyRFDiscoveryDecision(cluster, decision, metav1.NewTime(time.Unix(100, 0)), nil)
		mutate(cluster)
		runtime := &legacyRFControllerRuntime{}

		stopped := runtime.ReconcileGate(context.Background(), cluster, logr.Discard())
		require.True(t, stopped.IsDone())
		require.False(t, stopped.IsRequeue())
		require.Equal(t, api.LegacyRFDiscoveryPhaseBlocked, cluster.Status.LegacyRFDiscovery.Phase)
		require.Equal(t, api.LegacyRFReasonSnapshotConflict, cluster.Status.LegacyRFDiscovery.Reason)
	}
}

func TestRetryCleanupUsesFailedAttemptGenerationAcrossCurrentInputChanges(t *testing.T) {
	cluster := safetyCluster()
	failedGeneration := cluster.Generation
	failedLocation := api.LegacyRFManagedLocation{
		K8sContext: "plane-a", Namespace: "old-data", Name: "old-dc", DatacenterName: "old-managed",
	}
	status := &api.LegacyRFDiscoveryStatus{
		ObservedGeneration: failedGeneration, Phase: api.LegacyRFDiscoveryPhaseBlocked,
		Reason: api.LegacyRFReasonContactUnreachable, CurrentManagedLocations: []api.LegacyRFManagedLocation{failedLocation},
	}
	cluster.Generation++
	current := discovery.Attempt{
		ClusterUID: string(cluster.UID), Generation: cluster.Generation,
		AttemptID:         legacyRFAttemptID(cluster.UID, cluster.Generation),
		WorkerImageDigest: "operator@sha256:" + strings.Repeat("b", 64),
		Connection: discovery.Connection{ManagedLocations: []discovery.ManagedLocation{{
			K8sContext: "plane-b", Namespace: "new-data", Name: "new-dc", DatacenterName: "new-managed",
		}}},
	}

	failed := legacyRFFailedAttemptForCleanup(current, status)
	require.Equal(t, failedGeneration, failed.Generation)
	require.Equal(t, legacyRFAttemptID(cluster.UID, failedGeneration), failed.AttemptID)
	require.NotEqual(t, current.AttemptID, failed.AttemptID)
	require.Equal(t, []discovery.ManagedLocation{{
		K8sContext: "plane-a", Namespace: "old-data", Name: "old-dc", DatacenterName: "old-managed",
	}}, failed.Connection.ManagedLocations, "cleanup must target the failed attempt's persisted location")

	runtime := &legacyRFControllerRuntime{
		clock: fixedDiscoveryClock{now: time.Unix(200, 0)}, recorder: record.NewFakeRecorder(2),
		backoff: fixedDiscoveryBackoff{delay: time.Second},
	}
	waiting := runtime.waitForRetryCleanup(cluster, status.Reason, failedGeneration)
	require.True(t, waiting.IsRequeue())
	require.Equal(t, failedGeneration, cluster.Status.LegacyRFDiscovery.ObservedGeneration)
	failedAgain := legacyRFFailedAttemptForCleanup(current, cluster.Status.LegacyRFDiscovery)
	require.Equal(t, failed.AttemptID, failedAgain.AttemptID)
}

func TestApplyLegacyRFDiscoveryDecisionEmitsOnlyOnTransition(t *testing.T) {
	cluster := safetyCluster()
	recorder := record.NewFakeRecorder(4)
	now := metav1.NewTime(time.Unix(100, 0))
	decision := DecideLegacyRFDiscovery(cluster, false,
		discovery.NewBoundaryError(api.LegacyRFReasonTLSFailed, errors.New("password-canary")))
	require.True(t, ApplyLegacyRFDiscoveryDecision(cluster, decision, now, recorder))
	require.False(t, ApplyLegacyRFDiscoveryDecision(cluster, decision, metav1.NewTime(time.Unix(200, 0)), recorder))
	require.Equal(t, now, *cluster.Status.LegacyRFDiscovery.LastTransitionTime)
	event := <-recorder.Events
	require.Contains(t, event, string(api.LegacyRFReasonTLSFailed))
	require.NotContains(t, event, "password-canary")
	select {
	case duplicate := <-recorder.Events:
		t.Fatalf("unexpected duplicate event: %s", duplicate)
	default:
	}
}

func TestAcceptLegacyRFSnapshotOrdersReadSurveyPatchAndStops(t *testing.T) {
	cluster, attempt, result, snapshot := acceptanceFixture(t)
	trace := []string{}
	control := &traceLegacyRFControl{current: cluster, trace: &trace}
	managed := &traceManagedState{trace: &trace}
	secrets := &traceLegacyRFSecretState{trace: &trace}
	outcome, err := AcceptLegacyRFSnapshot(context.Background(), control, managed, LegacyRFAcceptanceInput{
		Key: types.NamespacedName{Namespace: cluster.Namespace, Name: cluster.Name}, ExpectedUID: cluster.UID,
		Attempt: attempt, Result: result, Snapshot: snapshot, Secrets: secrets,
	})
	require.NoError(t, err)
	require.True(t, outcome.StatusPatched)
	require.True(t, outcome.Stop)
	require.Equal(t, []string{"read", "survey:plane-a/data/dc-a", "secrets", "patch"}, trace)
	require.Equal(t, api.LegacyRFDiscoveryPhaseAccepted, control.current.Status.LegacyRFDiscovery.Phase)
	require.Equal(t, snapshot.Hash, control.current.Status.LegacyRFDiscovery.SnapshotHash)
}

func TestAcceptLegacyRFSnapshotRejectsSecretRotationImmediatelyBeforeCAS(t *testing.T) {
	cluster, attempt, result, snapshot := acceptanceFixture(t)
	trace := []string{}
	_, err := AcceptLegacyRFSnapshot(context.Background(), &traceLegacyRFControl{current: cluster, trace: &trace}, &traceManagedState{trace: &trace}, LegacyRFAcceptanceInput{
		Key: types.NamespacedName{Namespace: cluster.Namespace, Name: cluster.Name}, ExpectedUID: cluster.UID,
		Attempt: attempt, Result: result, Snapshot: snapshot,
		Secrets: &traceLegacyRFSecretState{trace: &trace, err: errors.New("Secret resourceVersion changed")},
	})
	require.Error(t, err)
	require.Equal(t, api.LegacyRFReasonStaleDiscoveryResult, boundaryReason(t, err))
	require.Equal(t, []string{"read", "survey:plane-a/data/dc-a", "secrets"}, trace)
	require.NotContains(t, trace, "patch")
}

func TestAcceptedSnapshotContentTamperBlocksEveryConsumer(t *testing.T) {
	cluster, _, _, snapshot := acceptanceFixture(t)
	cluster.Status.LegacyRFDiscovery = acceptedStatus(snapshot)
	cluster.Status.LegacyRFDiscovery.AcceptedSnapshot.Replication.SystemAuth["legacy-a"]++

	decision := DecideLegacyRFDiscovery(cluster, false, nil)
	require.Equal(t, api.LegacyRFDiscoveryPhaseBlocked, decision.Phase)
	require.Equal(t, api.LegacyRFReasonSnapshotConflict, decision.Reason)
	require.False(t, legacyRFSnapshotIsIntact(cluster.Status.LegacyRFDiscovery))
}

func TestAcceptLegacyRFSnapshotRejectsEveryBindingRaceBeforePatch(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*api.K8ssandraCluster, *discovery.Attempt, *discovery.DiscoveryResult, *api.LegacyRFSnapshot)
	}{
		{name: "UID", mutate: func(cluster *api.K8ssandraCluster, _ *discovery.Attempt, _ *discovery.DiscoveryResult, _ *api.LegacyRFSnapshot) {
			cluster.UID = "replacement"
		}},
		{name: "marker", mutate: func(cluster *api.K8ssandraCluster, _ *discovery.Attempt, _ *discovery.DiscoveryResult, _ *api.LegacyRFSnapshot) {
			cluster.Annotations[api.LegacyRFDiscoveryMarkerAnnotation] = "v2"
		}},
		{name: "generation", mutate: func(cluster *api.K8ssandraCluster, _ *discovery.Attempt, _ *discovery.DiscoveryResult, _ *api.LegacyRFSnapshot) {
			cluster.Generation++
		}},
		{name: "seed reorder", mutate: func(cluster *api.K8ssandraCluster, _ *discovery.Attempt, _ *discovery.DiscoveryResult, _ *api.LegacyRFSnapshot) {
			cluster.Spec.Cassandra.AdditionalSeeds[0], cluster.Spec.Cassandra.AdditionalSeeds[1] = cluster.Spec.Cassandra.AdditionalSeeds[1], cluster.Spec.Cassandra.AdditionalSeeds[0]
		}},
		{name: "Secret", mutate: func(_ *api.K8ssandraCluster, attempt *discovery.Attempt, _ *discovery.DiscoveryResult, _ *api.LegacyRFSnapshot) {
			attempt.Connection.SecretBindings[0].ResourceVersion = "changed"
		}},
		{name: "TLS", mutate: func(_ *api.K8ssandraCluster, _ *discovery.Attempt, result *discovery.DiscoveryResult, _ *api.LegacyRFSnapshot) {
			result.SecretBindings[1].ResourceVersion = "changed"
		}},
		{name: "worker digest", mutate: func(_ *api.K8ssandraCluster, _ *discovery.Attempt, _ *discovery.DiscoveryResult, snapshot *api.LegacyRFSnapshot) {
			snapshot.WorkerImageDigest = "other@sha256:" + strings.Repeat("b", 64)
		}},
		{name: "planned target", mutate: func(cluster *api.K8ssandraCluster, _ *discovery.Attempt, _ *discovery.DiscoveryResult, _ *api.LegacyRFSnapshot) {
			cluster.Spec.Cassandra.Datacenters[0].Meta.Name = "renamed"
		}},
		{name: "result hash", mutate: func(_ *api.K8ssandraCluster, _ *discovery.Attempt, result *discovery.DiscoveryResult, _ *api.LegacyRFSnapshot) {
			result.CanonicalHash = "changed"
		}},
		{name: "authoritative topology", mutate: func(_ *api.K8ssandraCluster, _ *discovery.Attempt, result *discovery.DiscoveryResult, _ *api.LegacyRFSnapshot) {
			result.Authoritative.ObservedExternalDCs = []string{"different"}
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cluster, attempt, result, snapshot := acceptanceFixture(t)
			expectedUID := cluster.UID
			test.mutate(cluster, &attempt, &result, snapshot)
			trace := []string{}
			control := &traceLegacyRFControl{current: cluster, trace: &trace}
			_, err := AcceptLegacyRFSnapshot(context.Background(), control, &traceManagedState{trace: &trace}, LegacyRFAcceptanceInput{
				Key: types.NamespacedName{Namespace: cluster.Namespace, Name: cluster.Name}, ExpectedUID: expectedUID,
				Attempt: attempt, Result: result, Snapshot: snapshot, Secrets: &traceLegacyRFSecretState{trace: &trace},
			})
			require.Error(t, err)
			require.NotContains(t, trace, "patch")
		})
	}
}

func TestAcceptedLegacyRFSnapshotMissingOrCorruptBlocksWithoutRediscovery(t *testing.T) {
	tests := []struct {
		name   string
		status func() *api.LegacyRFDiscoveryStatus
	}{
		{name: "missing", status: func() *api.LegacyRFDiscoveryStatus {
			return &api.LegacyRFDiscoveryStatus{Phase: api.LegacyRFDiscoveryPhaseAccepted, SnapshotHash: "lost"}
		}},
		{name: "corrupt", status: func() *api.LegacyRFDiscoveryStatus {
			_, _, _, snapshot := acceptanceFixture(t)
			status := acceptedStatus(snapshot)
			status.AcceptedSnapshot.Replication.SystemAuth["legacy-a"]++
			return status
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cluster := safetyCluster()
			cluster.Status.LegacyRFDiscovery = test.status()
			decision := DecideLegacyRFDiscovery(cluster, false, nil)
			require.Equal(t, api.LegacyRFDiscoveryPhaseBlocked, decision.Phase)
			require.Equal(t, api.LegacyRFReasonSnapshotConflict, decision.Reason)
			require.False(t, decision.Retryable)
			require.Empty(t, decision.AttemptLocations)
		})
	}
}

func TestAuthorizeLegacyRFManagedCreationHasExactOrderingAndAcceptedSeeds(t *testing.T) {
	cluster, _, _, snapshot := acceptanceFixture(t)
	cluster.Status.LegacyRFDiscovery = acceptedStatus(snapshot)
	trace := []string{}
	control := &traceLegacyRFControl{current: cluster, trace: &trace}
	managed := &traceManagedState{trace: &trace}
	target := api.LegacyRFManagedLocation{K8sContext: "plane-a", Namespace: "data", Name: "dc-a", DatacenterName: "managed-a"}
	var createdSeeds []string
	err := AuthorizeLegacyRFManagedCreation(context.Background(), control, managed, LegacyRFManagedCreationInput{
		Key: types.NamespacedName{Namespace: cluster.Namespace, Name: cluster.Name}, ExpectedUID: cluster.UID,
		ExpectedSnapshotHash: snapshot.Hash, Target: target,
		Create: func(_ context.Context, seeds []string) error {
			trace = append(trace, "create:"+strings.Join(seeds, ","))
			createdSeeds = append([]string(nil), seeds...)
			return nil
		},
	})
	require.NoError(t, err)
	require.Equal(t, snapshot.AcceptedSeeds, createdSeeds)
	require.Equal(t, []string{
		"read", "survey:plane-a/data/dc-a", "patch", "read",
		"create:192.0.2.10,192.0.2.11",
	}, trace)
	require.True(t, control.current.Status.LegacyRFDiscovery.ManagedCreationObserved)
	require.Contains(t, control.current.Status.LegacyRFDiscovery.ManagedLocationHistory, target)
}

func TestAuthorizeLegacyRFManagedCreationResurveysAlreadyExistsAndNeverSucceeds(t *testing.T) {
	cluster, _, _, snapshot := acceptanceFixture(t)
	cluster.Status.LegacyRFDiscovery = acceptedStatus(snapshot)
	target := api.LegacyRFManagedLocation{K8sContext: "plane-a", Namespace: "data", Name: "dc-a", DatacenterName: "managed-a"}
	trace := []string{}
	managed := &traceManagedState{trace: &trace}
	err := AuthorizeLegacyRFManagedCreation(context.Background(), &traceLegacyRFControl{current: cluster, trace: &trace}, managed, LegacyRFManagedCreationInput{
		Key: types.NamespacedName{Namespace: cluster.Namespace, Name: cluster.Name}, ExpectedUID: cluster.UID,
		ExpectedSnapshotHash: snapshot.Hash, Target: target,
		Create: func(context.Context, []string) error {
			trace = append(trace, "create")
			return apierrors.NewAlreadyExists(schema.GroupResource{Group: "cassandra.datastax.com", Resource: "cassandradatacenters"}, target.Name)
		},
	})
	require.Error(t, err)
	require.Equal(t, 2, managed.calls)
	require.Equal(t, "survey:plane-a/data/dc-a", trace[len(trace)-1])
}

func TestAuthorizeLegacyRFManagedCreationBlocksRacesAndDuplicateRestartIsIdempotent(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*api.K8ssandraCluster)
	}{
		{name: "UID changed", mutate: func(cluster *api.K8ssandraCluster) { cluster.UID = "replacement" }},
		{name: "marker changed", mutate: func(cluster *api.K8ssandraCluster) {
			cluster.Annotations[api.LegacyRFDiscoveryMarkerAnnotation] = "v2"
		}},
		{name: "snapshot hash changed", mutate: func(cluster *api.K8ssandraCluster) {
			cluster.Status.LegacyRFDiscovery.SnapshotHash = "changed"
		}},
		{name: "seed digest changed", mutate: func(cluster *api.K8ssandraCluster) {
			cluster.Status.LegacyRFDiscovery.AcceptedSnapshot.AcceptedSeedDigest = "changed"
		}},
		{name: "plan collision", mutate: func(cluster *api.K8ssandraCluster) {
			cluster.Spec.Cassandra.Datacenters[0].DatacenterName = "legacy-a"
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cluster, _, _, snapshot := acceptanceFixture(t)
			cluster.Status.LegacyRFDiscovery = acceptedStatus(snapshot)
			expectedUID := cluster.UID
			test.mutate(cluster)
			created := false
			err := AuthorizeLegacyRFManagedCreation(context.Background(), &traceLegacyRFControl{current: cluster}, &traceManagedState{}, LegacyRFManagedCreationInput{
				Key: types.NamespacedName{Namespace: cluster.Namespace, Name: cluster.Name}, ExpectedUID: expectedUID,
				ExpectedSnapshotHash: snapshot.Hash, Target: api.LegacyRFManagedLocation{K8sContext: "plane-a", Namespace: "data", Name: "dc-a", DatacenterName: "managed-a"},
				Create: func(context.Context, []string) error { created = true; return nil },
			})
			require.Error(t, err)
			require.False(t, created)
		})
	}

	cluster, _, _, snapshot := acceptanceFixture(t)
	cluster.Status.LegacyRFDiscovery = acceptedStatus(snapshot)
	target := api.LegacyRFManagedLocation{K8sContext: "plane-a", Namespace: "data", Name: "dc-a", DatacenterName: "managed-a"}
	createdStatus, err := BuildLegacyRFManagedCreationStatus(cluster.Status.LegacyRFDiscovery, snapshot.AcceptedManagedLocations, target)
	require.NoError(t, err)
	cluster.Status.LegacyRFDiscovery = &createdStatus
	trace := []string{}
	err = AuthorizeLegacyRFManagedCreation(context.Background(), &traceLegacyRFControl{current: cluster, trace: &trace}, &traceManagedState{trace: &trace}, LegacyRFManagedCreationInput{
		Key: types.NamespacedName{Namespace: cluster.Namespace, Name: cluster.Name}, ExpectedUID: cluster.UID,
		ExpectedSnapshotHash: snapshot.Hash, Target: target,
		Create: func(context.Context, []string) error { trace = append(trace, "create"); return nil },
	})
	require.NoError(t, err)
	require.Equal(t, []string{"read", "survey:plane-a/data/dc-a", "create"}, trace,
		"restart after the durable history patch must not rewrite status")
}

func TestAuthorizeLegacyRFManagedCreationRejectsTargetOutsideCurrentPlan(t *testing.T) {
	cluster, _, _, snapshot := acceptanceFixture(t)
	cluster.Status.LegacyRFDiscovery = acceptedStatus(snapshot)
	created := false
	err := AuthorizeLegacyRFManagedCreation(context.Background(), &traceLegacyRFControl{current: cluster}, &traceManagedState{}, LegacyRFManagedCreationInput{
		Key: types.NamespacedName{Namespace: cluster.Namespace, Name: cluster.Name}, ExpectedUID: cluster.UID,
		ExpectedSnapshotHash: snapshot.Hash,
		Target:               api.LegacyRFManagedLocation{K8sContext: "other", Namespace: "data", Name: "not-planned"},
		Create:               func(context.Context, []string) error { created = true; return nil },
	})
	require.Error(t, err)
	require.False(t, created)
}

func TestAuthorizeLegacyRFManagedCreationStopsOnHistoryConflictAndReadBackFailure(t *testing.T) {
	tests := []struct {
		name    string
		control func(*api.K8ssandraCluster) *traceLegacyRFControl
	}{
		{name: "history patch conflict", control: func(cluster *api.K8ssandraCluster) *traceLegacyRFControl {
			return &traceLegacyRFControl{current: cluster, patchErr: errors.New("status conflict")}
		}},
		{name: "history read-back unavailable", control: func(cluster *api.K8ssandraCluster) *traceLegacyRFControl {
			return &traceLegacyRFControl{current: cluster, failReadCall: 2, readErr: errors.New("API unavailable")}
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cluster, _, _, snapshot := acceptanceFixture(t)
			cluster.Status.LegacyRFDiscovery = acceptedStatus(snapshot)
			created := false
			err := AuthorizeLegacyRFManagedCreation(context.Background(), test.control(cluster), &traceManagedState{}, LegacyRFManagedCreationInput{
				Key: types.NamespacedName{Namespace: cluster.Namespace, Name: cluster.Name}, ExpectedUID: cluster.UID,
				ExpectedSnapshotHash: snapshot.Hash,
				Target:               api.LegacyRFManagedLocation{K8sContext: "plane-a", Namespace: "data", Name: "dc-a", DatacenterName: "managed-a"},
				Create:               func(context.Context, []string) error { created = true; return nil },
			})
			require.Error(t, err)
			require.False(t, created)
		})
	}
}

type traceLegacyRFControl struct {
	current      *api.K8ssandraCluster
	trace        *[]string
	err          error
	patchErr     error
	readErr      error
	failReadCall int
	readCalls    int
}

func (control *traceLegacyRFControl) Read(_ context.Context, _ types.NamespacedName) (*api.K8ssandraCluster, error) {
	control.readCalls++
	if control.trace != nil {
		*control.trace = append(*control.trace, "read")
	}
	if control.failReadCall == control.readCalls && control.readErr != nil {
		return nil, control.readErr
	}
	if control.err != nil {
		return nil, control.err
	}
	return control.current.DeepCopy(), nil
}

func (control *traceLegacyRFControl) PatchStatusCAS(_ context.Context, _, updated *api.K8ssandraCluster) error {
	if control.trace != nil {
		*control.trace = append(*control.trace, "patch")
	}
	if control.err != nil {
		return control.err
	}
	if control.patchErr != nil {
		return control.patchErr
	}
	control.current = updated.DeepCopy()
	control.current.ResourceVersion = "next"
	return nil
}

type traceManagedState struct {
	trace *[]string
	err   error
	calls int
}

type traceLegacyRFSecretState struct {
	trace *[]string
	err   error
}

func (state *traceLegacyRFSecretState) ValidateBindings(_ context.Context, _ []discovery.SecretBinding) error {
	if state.trace != nil {
		*state.trace = append(*state.trace, "secrets")
	}
	return state.err
}

func (state *traceManagedState) AssertAbsent(_ context.Context, locations []api.LegacyRFManagedLocation, _ bool) error {
	state.calls++
	for _, location := range locations {
		if state.trace != nil {
			*state.trace = append(*state.trace, fmt.Sprintf("survey:%s/%s/%s", location.K8sContext, location.Namespace, location.Name))
		}
	}
	return state.err
}

func safetyCluster() *api.K8ssandraCluster {
	return &api.K8ssandraCluster{
		ObjectMeta: metav1.ObjectMeta{Namespace: "control", Name: "migration", UID: "uid-1", ResourceVersion: "7", Generation: 7,
			Annotations: map[string]string{api.LegacyRFDiscoveryMarkerAnnotation: api.LegacyRFDiscoveryMarkerVersion}},
		Spec: api.K8ssandraClusterSpec{Cassandra: &api.CassandraClusterTemplate{
			ClusterName: "legacy", ServerType: api.ServerDistributionCassandra,
			AdditionalSeeds: []string{"192.0.2.10", "192.0.2.11"},
			Datacenters:     []api.CassandraDatacenterTemplate{{Meta: api.EmbeddedObjectMeta{Name: "dc-a", Namespace: "data"}, K8sContext: "plane-a", DatacenterOptions: api.DatacenterOptions{DatacenterName: "managed-a"}}},
		}},
	}
}

func acceptanceFixture(t *testing.T) (*api.K8ssandraCluster, discovery.Attempt, discovery.DiscoveryResult, *api.LegacyRFSnapshot) {
	t.Helper()
	cluster := safetyCluster()
	seeds, digest, err := discovery.CanonicalizeSeeds(cluster.Spec.Cassandra.AdditionalSeeds)
	require.NoError(t, err)
	bindings := []discovery.SecretBinding{
		{Purpose: "auth", SourceContext: "", Namespace: "control", Name: "auth", Keys: []string{"username", "password"}, ResourceVersion: "11"},
		{Purpose: "tls", SourceContext: "plane-a", Namespace: "data", Name: "tls", Keys: []string{"ca.crt"}, ResourceVersion: "12"},
	}
	attempt := discovery.Attempt{ClusterUID: string(cluster.UID), Generation: cluster.Generation,
		MarkerVersion: api.LegacyRFDiscoveryMarkerVersion, ProtocolVersion: api.LegacyRFDiscoveryProtocolVersion,
		AttemptID: "attempt-1", OrderedSeeds: seeds, SeedDigest: digest, WorkerImageDigest: "operator@sha256:" + strings.Repeat("a", 64),
		Connection: discovery.Connection{ExpectedClusterName: cluster.CassClusterName(), SecretBindings: bindings,
			ManagedLocations: []discovery.ManagedLocation{{K8sContext: "plane-a", Namespace: "data", Name: "dc-a", DatacenterName: "managed-a"}}},
	}
	result := discovery.DiscoveryResult{SchemaVersion: attempt.ProtocolVersion, ClusterUID: attempt.ClusterUID,
		Generation: attempt.Generation, MarkerVersion: attempt.MarkerVersion, ProtocolVersion: attempt.ProtocolVersion,
		AttemptID: attempt.AttemptID, OrderedSeeds: append([]netip.AddrPort(nil), seeds...), SeedDigest: digest,
		SecretBindings: append([]discovery.SecretBinding(nil), bindings...), WorkerImageDigest: attempt.WorkerImageDigest,
		Authoritative: &discovery.AuthoritativeCandidate{AttemptID: attempt.AttemptID,
			AttemptIndex: 0, Endpoint: seeds[0], ClusterName: "legacy", SourceVersion: "4.1.8",
			ObservedExternalDCs: []string{"legacy-a", "legacy-b"}},
		AttemptTrace: []discovery.EndpointAttemptSummary{
			{AttemptIndex: 0, Endpoint: seeds[0], Outcome: discovery.EndpointAttemptAccepted},
			{AttemptIndex: 1, Endpoint: seeds[1], Outcome: discovery.EndpointAttemptSkipped},
		},
	}
	snapshot := planSnapshot()
	snapshot.ClusterUID = string(cluster.UID)
	snapshot.AcceptedGeneration = cluster.Generation
	snapshot.MarkerVersion = attempt.MarkerVersion
	snapshot.ProtocolVersion = attempt.ProtocolVersion
	snapshot.AcceptedSeeds = []string{"192.0.2.10", "192.0.2.11"}
	snapshot.AcceptedSeedDigest = digest
	snapshot.AuthoritativeEndpoint = seeds[0].String()
	snapshot.AttemptTrace = toAPIAttemptTrace(result.AttemptTrace)
	snapshot.ExpectedClusterName = cluster.CassClusterName()
	snapshot.SourceVersion = "4.1.8"
	snapshot.SecretBindings = []api.LegacyRFSecretBinding{
		{Purpose: "auth", SourceContext: "", Namespace: "control", Name: "auth", Keys: []string{"username", "password"}, ResourceVersion: "11"},
		{Purpose: "tls", SourceContext: "plane-a", Namespace: "data", Name: "tls", Keys: []string{"ca.crt"}, ResourceVersion: "12"},
	}
	snapshot.DiscoveryLocation = api.LegacyRFManagedLocation{K8sContext: "plane-a", Namespace: "data", Name: "dc-a", DatacenterName: "managed-a"}
	snapshot.AcceptedManagedLocations = []api.LegacyRFManagedLocation{snapshot.DiscoveryLocation}
	snapshot.WorkerImageDigest = attempt.WorkerImageDigest
	snapshot.Hash, err = LegacyRFSnapshotHash(snapshot)
	require.NoError(t, err)
	result.Authoritative.Replication = discovery.SystemKeyspaceObservations{
		SystemAuth:        discovery.KeyspaceObservation{Present: true, Replication: copyRFMap(snapshot.Replication.SystemAuth)},
		SystemTraces:      discovery.KeyspaceObservation{Present: true, Replication: copyRFMap(snapshot.Replication.SystemTraces)},
		SystemDistributed: discovery.KeyspaceObservation{Present: true, Replication: copyRFMap(snapshot.Replication.SystemDistributed)},
	}
	result = canonicalAcceptanceResult(t, result)
	return cluster, attempt, result, snapshot
}

func canonicalAcceptanceResult(t *testing.T, result discovery.DiscoveryResult) discovery.DiscoveryResult {
	t.Helper()
	body, err := discovery.SignResult(result, []byte("01234567890123456789012345678901"), api.LegacyRFDiscoveryMaxResultBytes)
	require.NoError(t, err)
	var envelope discovery.ResultEnvelope
	require.NoError(t, json.Unmarshal(body, &envelope))
	return envelope.Result
}

func acceptedStatus(snapshot *api.LegacyRFSnapshot) *api.LegacyRFDiscoveryStatus {
	return &api.LegacyRFDiscoveryStatus{ObservedGeneration: snapshot.AcceptedGeneration,
		Phase: api.LegacyRFDiscoveryPhaseAccepted, SnapshotHash: snapshot.Hash,
		AcceptedSnapshot: copyLegacyRFSnapshot(snapshot), CurrentManagedLocations: append([]api.LegacyRFManagedLocation(nil), snapshot.AcceptedManagedLocations...)}
}
