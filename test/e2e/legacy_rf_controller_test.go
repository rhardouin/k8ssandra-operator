package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/netip"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-logr/logr"
	cassdcapi "github.com/k8ssandra/cass-operator/apis/cassandra/v1beta1"
	api "github.com/k8ssandra/k8ssandra-operator/apis/k8ssandra/v1alpha1"
	k8ssandractrl "github.com/k8ssandra/k8ssandra-operator/controllers/k8ssandra"
	"github.com/k8ssandra/k8ssandra-operator/pkg/clientcache"
	"github.com/k8ssandra/k8ssandra-operator/pkg/discovery"
	"github.com/stretchr/testify/require"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
)

const releaseWorkerDigest = "registry.example/k8ssandra-operator@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

func TestLegacyRFMultiClusterAcceptanceRestartAndRaceMatrix(t *testing.T) {
	harness := startLegacyRFEnvtest(t, 2)
	ctx := context.Background()
	t.Run("acceptance read-back then exactly one managed datacenter", func(t *testing.T) { testReleaseAcceptance(t, ctx, harness) })
	t.Run("concurrent acceptance has one optimistic winner", func(t *testing.T) { testReleaseAcceptanceRace(t, ctx, harness) })
	t.Run("acceptance binding races stop before status or datacenter", func(t *testing.T) { testReleaseBindingRaces(t, ctx, harness) })
	t.Run("accepted plan reorder removal and non-colliding addition remain compatible", func(t *testing.T) { testReleasePlans(t, ctx, harness) })
	t.Run("direct read outage and stale cache fail closed", func(t *testing.T) { testAuthoritativeDirectReads(t, ctx, harness) })
	t.Run("managed creation AlreadyExists race is resurveyd", func(t *testing.T) { testAlreadyExistsRace(t, ctx, harness) })
	t.Run("permanent conflicts create no retry Job or datacenter", func(t *testing.T) { testPermanentNoRetryControls(t, ctx, harness) })
	t.Run("secret_correction", func(t *testing.T) {
		for _, variant := range []struct {
			name   string
			reason api.LegacyRFDiscoveryReason
		}{
			{name: "credential", reason: api.LegacyRFReasonAuthenticationRejected},
			{name: "tls", reason: api.LegacyRFReasonTLSFailed},
		} {
			t.Run(variant.name, func(t *testing.T) { testSameUIDRecovery(t, ctx, harness, variant.reason, variant.name) })
		}
	})
	t.Run("network_restoration", func(t *testing.T) {
		testSameUIDRecovery(t, ctx, harness, api.LegacyRFReasonContactUnreachable, "network")
	})
}

func testReleaseAcceptance(t *testing.T, ctx context.Context, harness *legacyRFEnvtestHarness) {
	fixture := createReleaseFixture(t, ctx, harness, "acceptance")
	outcome, err := k8ssandractrl.AcceptLegacyRFSnapshot(ctx, fixture.control, fixture.managed, fixture.input())
	require.NoError(t, err, "private cause: %v", errors.Unwrap(err))
	require.True(t, outcome.Stop)
	require.True(t, outcome.StatusPatched)
	assertDatacenterCount(t, ctx, harness.data[0], fixture.namespace, 0)
	accepted, err := k8ssandractrl.ReadBackLegacyRFAcceptance(ctx, harness.control,
		client.ObjectKeyFromObject(fixture.cluster), fixture.cluster.UID, fixture.snapshot.Hash)
	require.NoError(t, err)
	created := 0
	err = k8ssandractrl.AuthorizeLegacyRFManagedCreation(ctx, fixture.control, fixture.managed,
		k8ssandractrl.LegacyRFManagedCreationInput{Key: client.ObjectKeyFromObject(accepted), ExpectedUID: accepted.UID,
			ExpectedSnapshotHash: fixture.snapshot.Hash, Target: fixture.location, Create: func(ctx context.Context, seeds []string) error {
				created++
				require.Equal(t, []string{"192.0.2.10", "192.0.2.11"}, seeds)
				return createReleaseDatacenter(ctx, harness.data[0], fixture.location)
			}})
	require.NoError(t, err)
	require.Equal(t, 1, created)
	assertDatacenterCount(t, ctx, harness.data[0], fixture.namespace, 1)
}

func createReleaseDatacenter(ctx context.Context, target client.Client, location api.LegacyRFManagedLocation) error {
	return target.Create(ctx, &cassdcapi.CassandraDatacenter{
		ObjectMeta: metav1.ObjectMeta{Name: location.Name, Namespace: location.Namespace},
		Spec:       cassdcapi.CassandraDatacenterSpec{ClusterName: "managed", ServerType: "cassandra", ServerVersion: "4.1.9", Size: 1},
	})
}

func testReleaseAcceptanceRace(t *testing.T, ctx context.Context, harness *legacyRFEnvtestHarness) {
	fixture := createReleaseFixture(t, ctx, harness, "acceptance-race")
	var wait sync.WaitGroup
	errorsSeen := make(chan error, 2)
	for range 2 {
		wait.Add(1)
		go func() {
			defer wait.Done()
			_, err := k8ssandractrl.AcceptLegacyRFSnapshot(ctx, fixture.control, fixture.managed, fixture.input())
			errorsSeen <- err
		}()
	}
	wait.Wait()
	close(errorsSeen)
	successes := 0
	for err := range errorsSeen {
		if err == nil {
			successes++
		}
	}
	require.Equal(t, 1, successes)
	assertDatacenterCount(t, ctx, harness.data[0], fixture.namespace, 0)
}

func testReleaseBindingRaces(t *testing.T, ctx context.Context, harness *legacyRFEnvtestHarness) {
	mutations := []struct {
		name string
		fn   func(*releaseFixture)
	}{
		{"uid", func(f *releaseFixture) { f.expectedUID = "replacement" }},
		{"generation", func(f *releaseFixture) { f.attempt.Generation++ }},
		{"seed membership", func(f *releaseFixture) { f.attempt.OrderedSeeds = f.attempt.OrderedSeeds[:1] }},
		{"seed order", func(f *releaseFixture) {
			f.result.OrderedSeeds[0], f.result.OrderedSeeds[1] = f.result.OrderedSeeds[1], f.result.OrderedSeeds[0]
		}},
		{"credential", func(f *releaseFixture) { f.result.SecretBindings[0].ResourceVersion = "stale" }},
		{"tls", func(f *releaseFixture) { f.snapshot.SecretBindings[1].ResourceVersion = "stale" }},
		{"secret rotation", func(f *releaseFixture) { f.secrets.rotate("auth", "rotated") }},
		{"worker digest", func(f *releaseFixture) { f.snapshot.WorkerImageDigest = "operator:mutable" }},
		{"target", func(f *releaseFixture) { f.snapshot.AcceptedManagedLocations[0].Name = "other" }},
	}
	for _, mutation := range mutations {
		t.Run(mutation.name, func(t *testing.T) {
			fixture := createReleaseFixture(t, ctx, harness, "race-"+sanitizeName(mutation.name))
			mutation.fn(fixture)
			_, err := k8ssandractrl.AcceptLegacyRFSnapshot(ctx, fixture.control, fixture.managed, fixture.input())
			require.Error(t, err)
			require.Nil(t, getCluster(t, ctx, harness.control, client.ObjectKeyFromObject(fixture.cluster)).Status.LegacyRFDiscovery)
			assertDatacenterCount(t, ctx, harness.data[0], fixture.namespace, 0)
		})
	}
}

func testReleasePlans(t *testing.T, ctx context.Context, harness *legacyRFEnvtestHarness) {
	fixture := createReleaseFixture(t, ctx, harness, "plan")
	plans := []k8ssandractrl.LegacyRFCurrentPlan{
		{ExpectedClusterName: "legacy", ServerType: api.ServerDistributionCassandra, ManagedDatacenterNames: []string{"dc-b", "dc-a"}},
		{ExpectedClusterName: "legacy", ServerType: api.ServerDistributionCassandra, ManagedDatacenterNames: []string{"dc-b"}},
		{ExpectedClusterName: "legacy", ServerType: api.ServerDistributionCassandra, ManagedDatacenterNames: []string{"dc-a", "dc-c"}},
	}
	for _, plan := range plans {
		require.NoError(t, k8ssandractrl.ValidateCurrentPlan(fixture.snapshot, plan))
	}
}

type staleDatacenterClient struct{ client.Client }

func (stale staleDatacenterClient) Get(ctx context.Context, key client.ObjectKey, object client.Object, options ...client.GetOption) error {
	if _, isDatacenter := object.(*cassdcapi.CassandraDatacenter); isDatacenter {
		return k8serrors.NewNotFound(schema.GroupResource{Group: "cassandra.datastax.com", Resource: "cassandradatacenters"}, key.Name)
	}
	return stale.Client.Get(ctx, key, object, options...)
}

type unavailableDirectClient struct{ client.Client }

func (unavailableDirectClient) Get(context.Context, client.ObjectKey, client.Object, ...client.GetOption) error {
	return errors.New("direct-api-canary unavailable")
}

func testAuthoritativeDirectReads(t *testing.T, ctx context.Context, harness *legacyRFEnvtestHarness) {
	staleFixture := createReleaseFixture(t, ctx, harness, "stale-cache")
	require.NoError(t, createReleaseDatacenter(ctx, harness.data[0], staleFixture.location))
	staleCache, err := clientcache.NewValidated(harness.control, harness.control, harness.scheme)
	require.NoError(t, err)
	require.NoError(t, staleCache.AddClientPair("plane-a", staleDatacenterClient{harness.data[0]}, harness.data[0]))
	state, err := k8ssandractrl.NewManagedDatacenterState(staleCache)
	require.NoError(t, err)
	var present *k8ssandractrl.ManagedStatePresentError
	require.ErrorAs(t, state.AssertAbsent(ctx, []api.LegacyRFManagedLocation{staleFixture.location}, false), &present)

	outageFixture := createReleaseFixture(t, ctx, harness, "direct-outage")
	outageCache, err := clientcache.NewValidated(harness.control, harness.control, harness.scheme)
	require.NoError(t, err)
	require.NoError(t, outageCache.AddClientPair("plane-a", harness.data[0], unavailableDirectClient{harness.data[0]}))
	state, err = k8ssandractrl.NewManagedDatacenterState(outageCache)
	require.NoError(t, err)
	require.Error(t, state.AssertAbsent(ctx, []api.LegacyRFManagedLocation{outageFixture.location}, false))
}

func testAlreadyExistsRace(t *testing.T, ctx context.Context, harness *legacyRFEnvtestHarness) {
	fixture := createReleaseFixture(t, ctx, harness, "already-exists")
	_, err := k8ssandractrl.AcceptLegacyRFSnapshot(ctx, fixture.control, fixture.managed, fixture.input())
	require.NoError(t, err)
	accepted := getCluster(t, ctx, harness.control, client.ObjectKeyFromObject(fixture.cluster))
	created := 0
	err = k8ssandractrl.AuthorizeLegacyRFManagedCreation(ctx, fixture.control, fixture.managed,
		k8ssandractrl.LegacyRFManagedCreationInput{Key: client.ObjectKeyFromObject(accepted), ExpectedUID: accepted.UID,
			ExpectedSnapshotHash: fixture.snapshot.Hash, Target: fixture.location, Create: func(ctx context.Context, _ []string) error {
				created++
				require.NoError(t, createReleaseDatacenter(ctx, harness.data[0], fixture.location))
				return k8serrors.NewAlreadyExists(schema.GroupResource{Group: "cassandra.datastax.com", Resource: "cassandradatacenters"}, fixture.location.Name)
			}})
	require.Error(t, err)
	require.Equal(t, 1, created)
	assertDatacenterCount(t, ctx, harness.data[0], fixture.namespace, 1)
}

func testPermanentNoRetryControls(t *testing.T, ctx context.Context, harness *legacyRFEnvtestHarness) {
	for _, reason := range []api.LegacyRFDiscoveryReason{api.LegacyRFReasonSnapshotConflict, api.LegacyRFReasonDiscoveryTooLate} {
		t.Run(string(reason), func(t *testing.T) {
			fixture := createReleaseFixture(t, ctx, harness, "permanent-"+strings.ToLower(string(reason)))
			cluster := fixture.cluster.DeepCopy()
			if reason == api.LegacyRFReasonSnapshotConflict {
				cluster.Status.LegacyRFDiscovery = &api.LegacyRFDiscoveryStatus{Phase: api.LegacyRFDiscoveryPhaseAccepted,
					SnapshotHash: fixture.snapshot.Hash, AcceptedSnapshot: fixture.snapshot.DeepCopy()}
				cluster.Spec.Cassandra.ClusterName = "changed"
			} else {
				cluster.Status.Datacenters = map[string]api.K8ssandraStatus{"existing": {}}
			}
			decision := k8ssandractrl.DecideLegacyRFDiscovery(cluster, false, nil)
			require.Equal(t, api.LegacyRFDiscoveryPhaseBlocked, decision.Phase)
			require.Equal(t, reason, decision.Reason)
			require.False(t, decision.Retryable)
			assertTimelineJobCount(t, ctx, harness.data[0], fixture.namespace, 0)
			assertDatacenterCount(t, ctx, harness.data[0], fixture.namespace, 0)
		})
	}
}

type timelineClock struct {
	now time.Time
}

func (clock *timelineClock) Now() time.Time { return clock.now }

func (clock *timelineClock) Advance(duration time.Duration) { clock.now = clock.now.Add(duration) }

type timelineImageResolver struct{}

func (timelineImageResolver) Resolve(context.Context) (string, error) {
	return releaseWorkerDigest, nil
}

func testSameUIDRecovery(
	t *testing.T,
	ctx context.Context,
	harness *legacyRFEnvtestHarness,
	reason api.LegacyRFDiscoveryReason,
	variant string,
) {
	fixture := createTimelineFixture(t, ctx, harness, variant)
	clock := &timelineClock{now: time.Unix(100, 0)}
	recorder := record.NewFakeRecorder(20)
	integration, err := k8ssandractrl.NewLegacyRFControllerIntegration(
		harness.cache, timelineImageResolver{}, clock, bytes.NewReader(bytes.Repeat([]byte{7}, 256)), recorder,
		k8ssandractrl.NewBoundedDiscoveryBackoff(time.Second, 8*time.Second, func(delay time.Duration) time.Duration { return delay }),
	)
	require.NoError(t, err)

	cluster := getCluster(t, ctx, harness.control, client.ObjectKeyFromObject(fixture.cluster))
	integration.ReconcileGate(ctx, cluster, logr.Discard())
	persistTimelineStatus(t, ctx, harness.control, cluster)
	require.Equal(t, api.LegacyRFDiscoveryPhasePending, cluster.Status.LegacyRFDiscovery.Phase,
		"reason=%s message=%s", cluster.Status.LegacyRFDiscovery.Reason, cluster.Status.LegacyRFDiscovery.Message)
	firstJob := onlyTimelineJob(t, ctx, harness.data[0], fixture.namespace)
	firstAttempt := readTimelineAttempt(t, ctx, harness.data[0], fixture.namespace)
	markTimelineJobFailed(t, ctx, harness.data[0], firstJob)
	assertDatacenterCount(t, ctx, harness.data[0], fixture.namespace, 0)

	blocked := k8ssandractrl.DecideLegacyRFDiscovery(cluster, false, discovery.NewBoundaryError(reason, errors.New("private-canary")))
	require.True(t, k8ssandractrl.ApplyLegacyRFDiscoveryDecision(cluster, blocked, metav1.NewTime(clock.Now()), recorder))
	require.False(t, k8ssandractrl.ApplyLegacyRFDiscoveryDecision(cluster, blocked, metav1.NewTime(clock.Now()), recorder))
	persistTimelineStatus(t, ctx, harness.control, cluster)
	require.Equal(t, api.LegacyRFDiscoveryPhaseBlocked, cluster.Status.LegacyRFDiscovery.Phase)

	if variant == "network" {
		clock.Advance(time.Second)
	} else {
		updateTimelineSecretAndObserveWatch(t, ctx, harness.control, fixture.namespace, variant)
	}
	cluster = getCluster(t, ctx, harness.control, client.ObjectKeyFromObject(cluster))
	managedState, err := k8ssandractrl.NewManagedDatacenterState(harness.cache)
	require.NoError(t, err)
	require.NoError(t, managedState.AssertAbsent(ctx, []api.LegacyRFManagedLocation{fixture.location}, true))
	cleanupResult := integration.ReconcileGate(ctx, cluster, logr.Discard())
	require.NoError(t, cleanupResult.GetError())
	require.True(t, cleanupResult.IsRequeue())
	require.Equal(t, api.LegacyRFDiscoveryPhaseBlocked, cluster.Status.LegacyRFDiscovery.Phase,
		"reason=%s message=%s", cluster.Status.LegacyRFDiscovery.Reason, cluster.Status.LegacyRFDiscovery.Message)
	cleanupOutput, err := cleanupResult.Output()
	require.NoError(t, err)
	require.Positive(t, cleanupOutput.RequeueAfter)
	require.LessOrEqual(t, cleanupOutput.RequeueAfter, 8*time.Second)
	require.NoError(t, completeEnvtestJobGarbageCollection(ctx, harness.data[0], fixture.namespace))
	require.Eventually(t, func() bool {
		cluster = getCluster(t, ctx, harness.control, client.ObjectKeyFromObject(cluster))
		retryResult := integration.ReconcileGate(ctx, cluster, logr.Discard())
		if retryResult.GetError() != nil || cluster.Status.LegacyRFDiscovery.Phase == api.LegacyRFDiscoveryPhaseBlocked {
			return false
		}
		if cluster.Status.LegacyRFDiscovery.Phase != api.LegacyRFDiscoveryPhasePending {
			return false
		}
		persistTimelineStatus(t, ctx, harness.control, cluster)
		return true
	}, 5*time.Second, 10*time.Millisecond)
	secondJob := onlyTimelineJob(t, ctx, harness.data[0], fixture.namespace)
	require.NotEqual(t, firstJob.UID, secondJob.UID)
	require.Equal(t, fixture.cluster.UID, cluster.UID)
	require.Equal(t, fixture.cluster.Generation, cluster.Generation)

	attempt := readTimelineAttempt(t, ctx, harness.data[0], fixture.namespace)
	if variant != "network" {
		assertStaleTimelineAttemptRejected(t, ctx, harness.data[0], fixture.namespace, firstAttempt, attempt)
	}
	publishTimelineResult(t, ctx, harness.data[0], fixture.namespace, attempt)
	markTimelineJobSucceeded(t, ctx, harness.data[0], secondJob)
	cluster = getCluster(t, ctx, harness.control, client.ObjectKeyFromObject(cluster))
	acceptResult := integration.ReconcileGate(ctx, cluster, logr.Discard())
	acceptErr := acceptResult.GetError()
	require.NoError(t, acceptErr, "private cause: %v", errors.Unwrap(acceptErr))
	require.NotEqual(t, api.LegacyRFDiscoveryPhaseBlocked, cluster.Status.LegacyRFDiscovery.Phase,
		"reason=%s message=%s", cluster.Status.LegacyRFDiscovery.Reason, cluster.Status.LegacyRFDiscovery.Message)
	accepted := getCluster(t, ctx, harness.control, client.ObjectKeyFromObject(cluster))
	require.Equal(t, api.LegacyRFDiscoveryPhaseAccepted, accepted.Status.LegacyRFDiscovery.Phase,
		"local phase=%s reason=%s message=%s", cluster.Status.LegacyRFDiscovery.Phase,
		cluster.Status.LegacyRFDiscovery.Reason, cluster.Status.LegacyRFDiscovery.Message)
	assertDatacenterCount(t, ctx, harness.data[0], fixture.namespace, 0)
	authorizeTimelineDatacenter(t, ctx, harness, fixture, accepted)
	assertDatacenterCount(t, ctx, harness.data[0], fixture.namespace, 1)
	assertTimelineEvents(t, recorder, reason)
}

func completeEnvtestJobGarbageCollection(ctx context.Context, target client.Client, namespace string) error {
	jobs := &batchv1.JobList{}
	if err := target.List(ctx, jobs, client.InNamespace(namespace)); err != nil {
		return fmt.Errorf("list terminating envtest Jobs: %w", err)
	}
	for index := range jobs.Items {
		job := &jobs.Items[index]
		if job.DeletionTimestamp == nil || !containsString(job.Finalizers, "orphan") {
			continue
		}
		job.Finalizers = removeString(job.Finalizers, "orphan")
		if err := target.Update(ctx, job); err != nil {
			return fmt.Errorf("complete envtest Job garbage collection: %w", err)
		}
	}
	return nil
}

func containsString(values []string, expected string) bool {
	for _, value := range values {
		if value == expected {
			return true
		}
	}
	return false
}

func removeString(values []string, removed string) []string {
	kept := values[:0]
	for _, value := range values {
		if value != removed {
			kept = append(kept, value)
		}
	}
	return kept
}

func createTimelineFixture(t *testing.T, ctx context.Context, harness *legacyRFEnvtestHarness, variant string) *releaseFixture {
	t.Helper()
	name := "recovery-" + variant
	namespace := "legacy-rf-" + name
	createNamespace(t, ctx, harness.control, namespace)
	for _, target := range harness.data {
		createNamespace(t, ctx, target, namespace)
	}
	cluster := releaseCluster(name)
	cluster.Namespace = namespace
	if variant == "credential" {
		createTimelineSecret(t, ctx, harness.control, namespace, "credentials", map[string][]byte{"username": []byte("old"), "password": []byte("old")})
		cluster.Spec.Cassandra.LegacyCqlCredentialsSecretRef = &corev1.LocalObjectReference{Name: "credentials"}
	}
	if variant == "tls" {
		createTimelineSecret(t, ctx, harness.control, namespace, "tls", map[string][]byte{"ca.crt": []byte("old-ca")})
		cluster.Spec.Cassandra.LegacyCqlTLSSecretRef = &corev1.LocalObjectReference{Name: "tls"}
	}
	require.NoError(t, harness.control.Create(ctx, cluster))
	cluster = getCluster(t, ctx, harness.control, client.ObjectKeyFromObject(cluster))
	datacenter := &cluster.Spec.Cassandra.Datacenters[0]
	return &releaseFixture{cluster: cluster, namespace: namespace,
		location: api.LegacyRFManagedLocation{K8sContext: "plane-a", Namespace: namespace, Name: "dc-a",
			DatacenterName: datacenter.CassDcName()}}
}

func createTimelineSecret(t *testing.T, ctx context.Context, target client.Client, namespace, name string, data map[string][]byte) {
	t.Helper()
	require.NoError(t, target.Create(ctx, &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace}, Data: data}))
}

func persistTimelineStatus(t *testing.T, ctx context.Context, target client.Client, cluster *api.K8ssandraCluster) {
	t.Helper()
	require.NoError(t, target.Status().Update(ctx, cluster))
}

func onlyTimelineJob(t *testing.T, ctx context.Context, target client.Reader, namespace string) *batchv1.Job {
	t.Helper()
	jobs := &batchv1.JobList{}
	require.NoError(t, target.List(ctx, jobs, client.InNamespace(namespace)))
	require.Len(t, jobs.Items, 1)
	return jobs.Items[0].DeepCopy()
}

func assertTimelineJobCount(t *testing.T, ctx context.Context, target client.Reader, namespace string, expected int) {
	t.Helper()
	jobs := &batchv1.JobList{}
	require.NoError(t, target.List(ctx, jobs, client.InNamespace(namespace)))
	require.Len(t, jobs.Items, expected)
}

func markTimelineJobFailed(t *testing.T, ctx context.Context, target client.Client, job *batchv1.Job) {
	t.Helper()
	now := metav1.Now()
	job.Status.Failed = 1
	job.Status.StartTime = &now
	job.Status.Conditions = []batchv1.JobCondition{
		{Type: batchv1.JobFailureTarget, Status: corev1.ConditionTrue, Reason: "FakeCQL"},
		{Type: batchv1.JobFailed, Status: corev1.ConditionTrue, Reason: "FakeCQL"},
	}
	require.NoError(t, target.Status().Update(ctx, job))
}

func markTimelineJobSucceeded(t *testing.T, ctx context.Context, target client.Client, job *batchv1.Job) {
	t.Helper()
	current := &batchv1.Job{}
	require.NoError(t, target.Get(ctx, client.ObjectKeyFromObject(job), current))
	current.Status.Failed = 0
	current.Status.Succeeded = 1
	now := metav1.Now()
	current.Status.StartTime = &now
	current.Status.CompletionTime = &now
	current.Status.Conditions = []batchv1.JobCondition{
		{Type: batchv1.JobSuccessCriteriaMet, Status: corev1.ConditionTrue},
		{Type: batchv1.JobComplete, Status: corev1.ConditionTrue},
	}
	require.NoError(t, target.Status().Update(ctx, current))
}

func updateTimelineSecretAndObserveWatch(
	t *testing.T,
	ctx context.Context,
	target client.Client,
	namespace, variant string,
) {
	t.Helper()
	watchClient, ok := target.(client.WithWatch)
	require.True(t, ok)
	watchContext, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	stream, err := watchClient.Watch(watchContext, &corev1.SecretList{}, client.InNamespace(namespace))
	require.NoError(t, err)
	defer stream.Stop()
	name := map[string]string{"credential": "credentials", "tls": "tls"}[variant]
	secret := &corev1.Secret{}
	require.NoError(t, target.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, secret))
	oldVersion := secret.ResourceVersion
	secret.Data[map[string]string{"credential": "password", "tls": "ca.crt"}[variant]] = []byte("corrected")
	require.NoError(t, target.Update(ctx, secret))
	for {
		select {
		case event := <-stream.ResultChan():
			changed, matched := event.Object.(*corev1.Secret)
			if matched && changed.Name == name && changed.ResourceVersion != oldVersion {
				return
			}
		case <-watchContext.Done():
			t.Fatal("Secret watch did not observe the corrected resourceVersion")
		}
	}
}

func readTimelineAttempt(t *testing.T, ctx context.Context, target client.Reader, namespace string) discovery.Attempt {
	t.Helper()
	configMaps := &corev1.ConfigMapList{}
	require.NoError(t, target.List(ctx, configMaps, client.InNamespace(namespace)))
	for _, configMap := range configMaps.Items {
		if body := configMap.Data["attempt.json"]; body != "" {
			attempt := discovery.Attempt{}
			require.NoError(t, json.Unmarshal([]byte(body), &attempt))
			return attempt
		}
	}
	t.Fatal("attempt ConfigMap was not found")
	return discovery.Attempt{}
}

func publishTimelineResult(
	t *testing.T,
	ctx context.Context,
	target client.Client,
	namespace string,
	attempt discovery.Attempt,
) {
	t.Helper()
	key := timelineHMACKey(t, ctx, target, namespace)
	result := timelineDiscoveryResult(attempt)
	body, err := discovery.SignResult(result, key, api.LegacyRFDiscoveryMaxResultBytes)
	require.NoError(t, err)
	configMaps := &corev1.ConfigMapList{}
	require.NoError(t, target.List(ctx, configMaps, client.InNamespace(namespace)))
	for index := range configMaps.Items {
		configMap := &configMaps.Items[index]
		if strings.HasSuffix(configMap.Name, "-result") {
			if configMap.Data == nil {
				configMap.Data = make(map[string]string, 1)
			}
			configMap.Data["result.json"] = string(body)
			require.NoError(t, target.Update(ctx, configMap))
			return
		}
	}
	t.Fatal("result ConfigMap was not found")
}

func timelineDiscoveryResult(attempt discovery.Attempt) discovery.DiscoveryResult {
	return discovery.DiscoveryResult{SchemaVersion: attempt.ProtocolVersion, ClusterUID: attempt.ClusterUID,
		Generation: attempt.Generation, MarkerVersion: attempt.MarkerVersion, ProtocolVersion: attempt.ProtocolVersion,
		AttemptID: attempt.AttemptID, OrderedSeeds: attempt.OrderedSeeds, SeedDigest: attempt.SeedDigest,
		SecretBindings: cloneTimelineBindings(attempt.Connection.SecretBindings), WorkerImageDigest: attempt.WorkerImageDigest,
		Authoritative: releaseAuthority(attempt), AttemptTrace: []discovery.EndpointAttemptSummary{
			{AttemptIndex: 0, Endpoint: attempt.OrderedSeeds[0], Outcome: discovery.EndpointAttemptAccepted},
			{AttemptIndex: 1, Endpoint: attempt.OrderedSeeds[1], Outcome: discovery.EndpointAttemptSkipped},
		}}
}

func cloneTimelineBindings(bindings []discovery.SecretBinding) []discovery.SecretBinding {
	cloned := append([]discovery.SecretBinding(nil), bindings...)
	for index := range cloned {
		cloned[index].Keys = append([]string(nil), cloned[index].Keys...)
	}
	return cloned
}

func assertStaleTimelineAttemptRejected(
	t *testing.T,
	ctx context.Context,
	target client.Reader,
	namespace string,
	staleAttempt, currentAttempt discovery.Attempt,
) {
	t.Helper()
	key := timelineHMACKey(t, ctx, target, namespace)
	body, err := discovery.SignResult(timelineDiscoveryResult(staleAttempt), key, api.LegacyRFDiscoveryMaxResultBytes)
	require.NoError(t, err)
	_, err = discovery.ValidateResultEnvelope(body, key, timelineExpectedResult(currentAttempt))
	assertBoundaryReason(t, err, api.LegacyRFReasonStaleDiscoveryResult)
}

func timelineExpectedResult(attempt discovery.Attempt) discovery.ExpectedResult {
	return discovery.ExpectedResult{SchemaVersion: attempt.ProtocolVersion, ClusterUID: attempt.ClusterUID,
		Generation: attempt.Generation, MarkerVersion: attempt.MarkerVersion, ProtocolVersion: attempt.ProtocolVersion,
		AttemptID: attempt.AttemptID, OrderedSeeds: attempt.OrderedSeeds, SeedDigest: attempt.SeedDigest,
		SecretBindings: cloneTimelineBindings(attempt.Connection.SecretBindings), WorkerImageDigest: attempt.WorkerImageDigest,
		MaximumBytes: api.LegacyRFDiscoveryMaxResultBytes}
}

func timelineHMACKey(t *testing.T, ctx context.Context, target client.Reader, namespace string) []byte {
	t.Helper()
	secrets := &corev1.SecretList{}
	require.NoError(t, target.List(ctx, secrets, client.InNamespace(namespace)))
	for _, secret := range secrets.Items {
		if key := secret.Data["hmac-key"]; len(key) != 0 {
			return append([]byte(nil), key...)
		}
	}
	t.Fatal("HMAC Secret was not found")
	return nil
}

func authorizeTimelineDatacenter(
	t *testing.T,
	ctx context.Context,
	harness *legacyRFEnvtestHarness,
	fixture *releaseFixture,
	accepted *api.K8ssandraCluster,
) {
	t.Helper()
	control, err := k8ssandractrl.NewLegacyRFControlPlane(harness.control)
	require.NoError(t, err)
	managed, err := k8ssandractrl.NewManagedDatacenterState(harness.cache)
	require.NoError(t, err)
	err = k8ssandractrl.AuthorizeLegacyRFManagedCreation(ctx, control, managed,
		k8ssandractrl.LegacyRFManagedCreationInput{Key: client.ObjectKeyFromObject(accepted), ExpectedUID: accepted.UID,
			ExpectedSnapshotHash: accepted.Status.LegacyRFDiscovery.SnapshotHash, Target: fixture.location,
			Create: func(ctx context.Context, _ []string) error {
				return createReleaseDatacenter(ctx, harness.data[0], fixture.location)
			}})
	require.NoError(t, err)
}

func assertTimelineEvents(t *testing.T, recorder *record.FakeRecorder, reason api.LegacyRFDiscoveryReason) {
	t.Helper()
	events := make([]string, 0, 4)
	for {
		select {
		case event := <-recorder.Events:
			events = append(events, event)
		default:
			require.Len(t, events, 3)
			require.Equal(t, 1, strings.Count(strings.Join(events, "\n"), string(reason)))
			require.NotContains(t, strings.Join(events, "\n"), "private-canary")
			return
		}
	}
}

func TestLegacyRFHistoricalTombstoneAndLocationBoundary(t *testing.T) {
	harness := startLegacyRFEnvtest(t, 2)
	ctx := context.Background()

	t.Run("pre-managed corruption blocks without rediscovery", func(t *testing.T) {
		fixture := createReleaseFixture(t, ctx, harness, "pre-managed")
		firstJob := createHistoricalAttemptJob(t, ctx, harness.data[0], fixture, fixture.attempt)
		fixture.cluster.Status.LegacyRFDiscovery = &api.LegacyRFDiscoveryStatus{
			Phase: api.LegacyRFDiscoveryPhaseAccepted, SnapshotHash: "corrupt",
			ManagedLocationHistory: []api.LegacyRFManagedLocation{fixture.location},
		}
		decision := k8ssandractrl.DecideLegacyRFDiscovery(fixture.cluster, false, nil)
		require.Equal(t, api.LegacyRFDiscoveryPhaseBlocked, decision.Phase)
		require.Equal(t, api.LegacyRFReasonSnapshotConflict, decision.Reason)
		require.Empty(t, decision.AttemptLocations)
		require.NotEmpty(t, firstJob.UID)
		assertTimelineJobCount(t, ctx, harness.data[0], fixture.namespace, 1)
	})

	t.Run("restart preserves removed historical location tombstone", func(t *testing.T) {
		fixture := createReleaseFixture(t, ctx, harness, "post-managed")
		_, err := k8ssandractrl.AcceptLegacyRFSnapshot(ctx, fixture.control, fixture.managed, fixture.input())
		require.NoError(t, err, "private cause: %v", errors.Unwrap(err))
		accepted := getCluster(t, ctx, harness.control, client.ObjectKeyFromObject(fixture.cluster))
		err = k8ssandractrl.AuthorizeLegacyRFManagedCreation(ctx, fixture.control, fixture.managed,
			k8ssandractrl.LegacyRFManagedCreationInput{
				Key: client.ObjectKeyFromObject(accepted), ExpectedUID: accepted.UID,
				ExpectedSnapshotHash: fixture.snapshot.Hash, Target: fixture.location,
				Create: func(context.Context, []string) error { return nil },
			})
		require.NoError(t, err)

		persisted := getCluster(t, ctx, harness.control, client.ObjectKeyFromObject(fixture.cluster))
		persisted.Spec.Cassandra.Datacenters = []api.CassandraDatacenterTemplate{{
			Meta: api.EmbeddedObjectMeta{Name: "dc-new", Namespace: fixture.namespace}, K8sContext: "plane-b", Size: 1,
		}}
		require.NoError(t, harness.control.Update(ctx, persisted))
		persisted = getCluster(t, ctx, harness.control, client.ObjectKeyFromObject(fixture.cluster))
		persisted.Status.LegacyRFDiscovery.AcceptedSnapshot = nil
		persisted.Status.LegacyRFDiscovery.SnapshotHash = "corrupt"
		require.NoError(t, harness.control.Status().Update(ctx, persisted))

		restartedControl, err := k8ssandractrl.NewLegacyRFControlPlane(harness.control)
		require.NoError(t, err)
		restarted := getCluster(t, ctx, harness.control, client.ObjectKeyFromObject(fixture.cluster))
		decision := k8ssandractrl.DecideLegacyRFDiscovery(restarted, false, nil)
		require.Equal(t, api.LegacyRFDiscoveryPhaseBlocked, decision.Phase)
		require.Equal(t, api.LegacyRFReasonSnapshotConflict, decision.Reason)
		_, err = restartedControl.Read(ctx, client.ObjectKeyFromObject(restarted))
		require.NoError(t, err)
		require.Contains(t, restarted.Status.LegacyRFDiscovery.ManagedLocationHistory, fixture.location)
		assertTimelineJobCount(t, ctx, harness.data[0], fixture.namespace, 0)
	})

	t.Run("unavailable historical context blocks", func(t *testing.T) {
		fixture := createReleaseFixture(t, ctx, harness, "history-unavailable")
		fixture.cluster.Status.LegacyRFDiscovery = &api.LegacyRFDiscoveryStatus{
			Phase: api.LegacyRFDiscoveryPhaseAccepted, SnapshotHash: "corrupt",
			ManagedLocationHistory: []api.LegacyRFManagedLocation{{K8sContext: "missing", Namespace: "old", Name: "dc-old"}},
		}
		decision := k8ssandractrl.DecideLegacyRFDiscovery(fixture.cluster, false, nil)
		require.Equal(t, api.LegacyRFDiscoveryPhaseBlocked, decision.Phase)
		require.Equal(t, api.LegacyRFReasonSnapshotConflict, decision.Reason)
		assertTimelineJobCount(t, ctx, harness.data[0], fixture.namespace, 0)
	})
}

func createHistoricalAttemptJob(
	t *testing.T,
	ctx context.Context,
	target client.Client,
	fixture *releaseFixture,
	attempt discovery.Attempt,
) *batchv1.Job {
	t.Helper()
	attempt.Connection.SecretBindings = nil
	resources, err := k8ssandractrl.BuildLegacyRFAttemptResources(k8ssandractrl.LegacyRFAttemptResourcesInput{
		ClusterKey: client.ObjectKeyFromObject(fixture.cluster), Attempt: attempt, Location: fixture.location,
		HMACKey: bytes.Repeat([]byte{9}, 32),
	})
	require.NoError(t, err)
	require.NoError(t, target.Create(ctx, resources.Job))
	current := &batchv1.Job{}
	require.NoError(t, target.Get(ctx, client.ObjectKeyFromObject(resources.Job), current))
	return current
}

func TestLegacyRFQualifiedBindingsDigestAndRedaction(t *testing.T) {
	key := []byte("01234567890123456789012345678901")
	attempt, result := releaseProtocolFixture(t)
	body, err := discovery.SignResult(result, key, api.LegacyRFDiscoveryMaxResultBytes)
	require.NoError(t, err)
	valid := expectedReleaseResult(attempt)
	_, err = discovery.ValidateResultEnvelope(body, key, valid)
	require.NoError(t, err)

	mutations := []struct {
		name   string
		mutate func(*discovery.ExpectedResult)
	}{
		{name: "context", mutate: func(e *discovery.ExpectedResult) { e.SecretBindings[0].SourceContext = "other" }},
		{name: "namespace", mutate: func(e *discovery.ExpectedResult) { e.SecretBindings[0].Namespace = "other" }},
		{name: "name", mutate: func(e *discovery.ExpectedResult) { e.SecretBindings[0].Name = "other" }},
		{name: "key", mutate: func(e *discovery.ExpectedResult) { e.SecretBindings[0].Keys = []string{"token"} }},
		{name: "resourceVersion", mutate: func(e *discovery.ExpectedResult) { e.SecretBindings[0].ResourceVersion = "other" }},
		{name: "purpose", mutate: func(e *discovery.ExpectedResult) { e.SecretBindings[0].Purpose = "other" }},
		{name: "tls Secret", mutate: func(e *discovery.ExpectedResult) { e.SecretBindings[1].ResourceVersion = "other" }},
		{name: "credential Secret", mutate: func(e *discovery.ExpectedResult) { e.SecretBindings[0].ResourceVersion = "other" }},
		{name: "tag only image", mutate: func(e *discovery.ExpectedResult) { e.WorkerImageDigest = "registry.example/operator:latest" }},
		{name: "wrong digest", mutate: func(e *discovery.ExpectedResult) {
			e.WorkerImageDigest = "registry.example/operator@sha256:" + strings.Repeat("b", 64)
		}},
	}
	for _, mutation := range mutations {
		t.Run(mutation.name, func(t *testing.T) {
			expected := expectedReleaseResult(attempt)
			mutation.mutate(&expected)
			_, err := discovery.ValidateResultEnvelope(body, key, expected)
			assertBoundaryReason(t, err, api.LegacyRFReasonStaleDiscoveryResult)
		})
	}

	t.Run("forged and oversize", func(t *testing.T) {
		forged := append([]byte(nil), body...)
		forged[len(forged)/2] ^= 1
		_, err := discovery.ValidateResultEnvelope(forged, key, valid)
		require.Error(t, err)
		oversize := make([]byte, api.LegacyRFDiscoveryMaxResultBytes+1)
		_, err = discovery.ValidateResultEnvelope(oversize, key, valid)
		assertBoundaryReason(t, err, api.LegacyRFReasonDiscoveryResultTooLarge)
	})

	t.Run("canaries never enter public artifacts", func(t *testing.T) {
		cluster := releaseCluster("redaction")
		cluster.Namespace = "redaction"
		cluster.UID = "redaction-uid"
		cluster.Generation = 1
		failure := discovery.NewBoundaryError(api.LegacyRFReasonAuthenticationRejected,
			errors.New("password-canary certificate-canary seed-canary raw-error-canary"))
		decision := k8ssandractrl.DecideLegacyRFDiscovery(cluster, false, failure)
		recorder := record.NewFakeRecorder(2)
		k8ssandractrl.ApplyLegacyRFDiscoveryDecision(cluster, decision,
			metav1.NewTime(time.Unix(100, 0)), recorder)
		event := <-recorder.Events
		capturedLogs := bytes.NewBufferString(cluster.Status.LegacyRFDiscovery.Message + "\n" + event)
		redactionAttempt, redactionResult, _ := releaseAcceptanceFixture(t, cluster)
		resources, err := k8ssandractrl.BuildLegacyRFAttemptResources(k8ssandractrl.LegacyRFAttemptResourcesInput{
			ClusterKey: client.ObjectKeyFromObject(cluster), Attempt: redactionAttempt,
			Location: api.LegacyRFManagedLocation{K8sContext: "plane-a", Namespace: "redaction", Name: "dc-a"},
			HMACKey:  key, CopiedSecretData: map[string][]byte{"password": []byte("password-canary"), "ca.crt": []byte("certificate-canary")},
		})
		require.NoError(t, err)
		redactedBody, err := discovery.SignResult(redactionResult, key, api.LegacyRFDiscoveryMaxResultBytes)
		require.NoError(t, err)
		resources.ResultConfigMap.Data = map[string]string{"result.json": string(redactedBody)}
		attemptConfigMap, err := json.Marshal(resources.AttemptConfigMap)
		require.NoError(t, err)
		resultConfigMap, err := json.Marshal(resources.ResultConfigMap)
		require.NoError(t, err)
		resultMetadata, err := json.Marshal(map[string]interface{}{
			"labels": resources.ResultConfigMap.Labels, "annotations": resources.ResultConfigMap.Annotations,
		})
		require.NoError(t, err)
		artifacts := []string{cluster.Status.LegacyRFDiscovery.Message, event, capturedLogs.String(), string(body),
			string(attemptConfigMap), string(resultConfigMap), string(resultMetadata)}
		encoded, err := json.Marshal(cluster.Status)
		require.NoError(t, err)
		artifacts = append(artifacts, string(encoded))
		for _, artifact := range artifacts {
			for _, canary := range []string{"password-canary", "certificate-canary", "seed-canary", "raw-error-canary"} {
				require.NotContains(t, artifact, canary)
			}
		}
		require.NotContains(t, strings.Join(artifacts, "\n"), "password-canary")
	})
}

type legacyRFEnvtestHarness struct {
	control client.Client
	data    []client.Client
	cache   *clientcache.ClientCache
	scheme  *runtime.Scheme
}

func startLegacyRFEnvtest(t *testing.T, dataPlanes int) *legacyRFEnvtestHarness {
	t.Helper()
	scheme := runtime.NewScheme()
	require.NoError(t, api.AddToScheme(scheme))
	require.NoError(t, cassdcapi.AddToScheme(scheme))
	require.NoError(t, corev1.AddToScheme(scheme))
	require.NoError(t, batchv1.AddToScheme(scheme))
	require.NoError(t, rbacv1.AddToScheme(scheme))
	environments := make([]*envtest.Environment, dataPlanes+1)
	clients := make([]client.Client, dataPlanes+1)
	for index := range environments {
		environments[index] = &envtest.Environment{CRDDirectoryPaths: []string{
			filepathFromRepository(t, "config/crd/bases"),
			filepathFromRepository(t, "build/crd/cass-operator"),
		}, ErrorIfCRDPathMissing: true}
		config, err := environments[index].Start()
		require.NoError(t, err)
		clients[index], err = client.NewWithWatch(config, client.Options{Scheme: scheme})
		require.NoError(t, err)
	}
	t.Cleanup(func() {
		for _, environment := range environments {
			require.NoError(t, environment.Stop())
		}
	})
	cache, err := clientcache.NewValidated(clients[0], clients[0], scheme)
	require.NoError(t, err)
	for index := 1; index < len(clients); index++ {
		require.NoError(t, cache.AddClientPair(fmt.Sprintf("plane-%c", 'a'+index-1), clients[index], clients[index]))
	}
	return &legacyRFEnvtestHarness{control: clients[0], data: clients[1:], cache: cache, scheme: scheme}
}

func filepathFromRepository(t *testing.T, relative string) string {
	t.Helper()
	return repositoryRoot(t) + "/" + relative
}

type releaseFixture struct {
	cluster     *api.K8ssandraCluster
	attempt     discovery.Attempt
	result      discovery.DiscoveryResult
	snapshot    *api.LegacyRFSnapshot
	control     k8ssandractrl.LegacyRFControlPlane
	managed     k8ssandractrl.ManagedDatacenterState
	location    api.LegacyRFManagedLocation
	namespace   string
	expectedUID types.UID
	secrets     *releaseSecretState
}

type releaseSecretState struct {
	bindings map[string]discovery.SecretBinding
}

func newReleaseSecretState(bindings []discovery.SecretBinding) *releaseSecretState {
	state := &releaseSecretState{bindings: make(map[string]discovery.SecretBinding, len(bindings))}
	for _, binding := range bindings {
		binding.Keys = append([]string(nil), binding.Keys...)
		state.bindings[binding.Purpose] = binding
	}
	return state
}

func (state *releaseSecretState) ValidateBindings(_ context.Context, bindings []discovery.SecretBinding) error {
	if len(bindings) != len(state.bindings) {
		return fmt.Errorf("Secret binding count changed")
	}
	for _, binding := range bindings {
		expected, found := state.bindings[binding.Purpose]
		if !found || expected.SourceContext != binding.SourceContext || expected.Namespace != binding.Namespace ||
			expected.Name != binding.Name || expected.ResourceVersion != binding.ResourceVersion ||
			!slices.Equal(expected.Keys, binding.Keys) {
			return fmt.Errorf("%s Secret binding changed", binding.Purpose)
		}
	}
	return nil
}

func (state *releaseSecretState) rotate(purpose, resourceVersion string) {
	binding := state.bindings[purpose]
	binding.ResourceVersion = resourceVersion
	state.bindings[purpose] = binding
}

func createReleaseFixture(t *testing.T, ctx context.Context, harness *legacyRFEnvtestHarness, name string) *releaseFixture {
	t.Helper()
	namespace := "legacy-rf-" + sanitizeName(name)
	createNamespace(t, ctx, harness.control, namespace)
	for _, dataClient := range harness.data {
		createNamespace(t, ctx, dataClient, namespace)
	}
	cluster := releaseCluster(name)
	cluster.Namespace = namespace
	require.NoError(t, harness.control.Create(ctx, cluster))
	cluster = getCluster(t, ctx, harness.control, client.ObjectKeyFromObject(cluster))
	attempt, result, snapshot := releaseAcceptanceFixture(t, cluster)
	control, err := k8ssandractrl.NewLegacyRFControlPlane(harness.control)
	require.NoError(t, err)
	managed, err := k8ssandractrl.NewManagedDatacenterState(harness.cache)
	require.NoError(t, err)
	return &releaseFixture{cluster: cluster, attempt: attempt, result: result, snapshot: snapshot,
		control: control, managed: managed, location: snapshot.DiscoveryLocation,
		namespace: namespace, expectedUID: cluster.UID, secrets: newReleaseSecretState(attempt.Connection.SecretBindings)}
}

func (fixture *releaseFixture) input() k8ssandractrl.LegacyRFAcceptanceInput {
	return k8ssandractrl.LegacyRFAcceptanceInput{Key: client.ObjectKeyFromObject(fixture.cluster),
		ExpectedUID: fixture.expectedUID, Attempt: fixture.attempt, Result: fixture.result, Snapshot: fixture.snapshot,
		Secrets: fixture.secrets}
}

func releaseCluster(name string) *api.K8ssandraCluster {
	return &api.K8ssandraCluster{ObjectMeta: metav1.ObjectMeta{Name: name,
		Annotations: map[string]string{api.LegacyRFDiscoveryMarkerAnnotation: api.LegacyRFDiscoveryMarkerVersion}},
		Spec: api.K8ssandraClusterSpec{Cassandra: &api.CassandraClusterTemplate{
			ClusterName: "legacy", ServerType: api.ServerDistributionCassandra,
			AdditionalSeeds: []string{"192.0.2.10", "192.0.2.11"},
			Datacenters:     []api.CassandraDatacenterTemplate{{Meta: api.EmbeddedObjectMeta{Name: "dc-a"}, K8sContext: "plane-a", Size: 1}},
		}}}
}

func releaseAcceptanceFixture(t *testing.T, cluster *api.K8ssandraCluster) (discovery.Attempt, discovery.DiscoveryResult, *api.LegacyRFSnapshot) {
	t.Helper()
	seeds, digest, err := discovery.CanonicalizeSeeds(cluster.Spec.Cassandra.AdditionalSeeds)
	require.NoError(t, err)
	bindings := releaseBindings()
	datacenter := &cluster.Spec.Cassandra.Datacenters[0]
	location := api.LegacyRFManagedLocation{K8sContext: "plane-a", Namespace: cluster.Namespace, Name: "dc-a",
		DatacenterName: datacenter.CassDcName()}
	attempt := discovery.Attempt{ClusterUID: string(cluster.UID), Generation: cluster.Generation,
		MarkerVersion: api.LegacyRFDiscoveryMarkerVersion, ProtocolVersion: api.LegacyRFDiscoveryProtocolVersion,
		AttemptID: "attempt-1", OrderedSeeds: seeds, SeedDigest: digest, WorkerImageDigest: releaseWorkerDigest,
		Connection: discovery.Connection{ExpectedClusterName: "legacy", SecretBindings: bindings,
			ManagedLocations: []discovery.ManagedLocation{{K8sContext: location.K8sContext, Namespace: location.Namespace,
				Name: location.Name, DatacenterName: location.DatacenterName}}}}
	result := discovery.DiscoveryResult{SchemaVersion: attempt.ProtocolVersion, ClusterUID: attempt.ClusterUID,
		Generation: attempt.Generation, MarkerVersion: attempt.MarkerVersion, ProtocolVersion: attempt.ProtocolVersion,
		AttemptID: attempt.AttemptID, OrderedSeeds: seeds, SeedDigest: digest, SecretBindings: releaseBindings(),
		WorkerImageDigest: releaseWorkerDigest,
		Authoritative:     releaseAuthority(attempt)}
	result.AttemptTrace = []discovery.EndpointAttemptSummary{
		{AttemptIndex: 0, Endpoint: seeds[0], Outcome: discovery.EndpointAttemptAccepted},
		{AttemptIndex: 1, Endpoint: seeds[1], Outcome: discovery.EndpointAttemptSkipped},
	}
	key := []byte("release-fixture-signing-key")
	body, err := discovery.SignResult(result, key, api.LegacyRFDiscoveryMaxResultBytes)
	require.NoError(t, err)
	result, err = discovery.ValidateResultEnvelope(body, key, timelineExpectedResult(attempt))
	require.NoError(t, err)
	snapshot := releaseSnapshot(t, cluster, attempt, result, location)
	return attempt, result, snapshot
}

func releaseBindings() []discovery.SecretBinding {
	return []discovery.SecretBinding{
		{Purpose: "auth", SourceContext: "control", Namespace: "source", Name: "credentials", Keys: []string{"password", "username"}, ResourceVersion: "11"},
		{Purpose: "tls", SourceContext: "plane-a", Namespace: "source", Name: "tls", Keys: []string{"ca.crt"}, ResourceVersion: "12"},
	}
}

func releaseAuthority(attempt discovery.Attempt) *discovery.AuthoritativeCandidate {
	replication := discovery.SystemKeyspaceObservations{
		SystemAuth:        discovery.KeyspaceObservation{Present: true, Strategy: "NetworkTopologyStrategy", Replication: map[string]int32{"legacy-a": 2}},
		SystemTraces:      discovery.KeyspaceObservation{Present: true, Strategy: "NetworkTopologyStrategy", Replication: map[string]int32{"legacy-a": 2}},
		SystemDistributed: discovery.KeyspaceObservation{Present: true, Strategy: "NetworkTopologyStrategy", Replication: map[string]int32{"legacy-a": 2}},
	}
	return &discovery.AuthoritativeCandidate{AttemptID: attempt.AttemptID, AttemptIndex: 0,
		Endpoint: attempt.OrderedSeeds[0], ClusterName: "legacy", SourceVersion: "4.1.9",
		Partitioner: "Murmur3Partitioner", ObservedExternalDCs: []string{"legacy-a"},
		Fingerprints: discovery.CandidateFingerprints{Identity: "identity", Topology: "topology", Schema: "schema"},
		Replication:  replication}
}

func releaseSnapshot(t *testing.T, cluster *api.K8ssandraCluster, attempt discovery.Attempt, result discovery.DiscoveryResult, location api.LegacyRFManagedLocation) *api.LegacyRFSnapshot {
	t.Helper()
	authority := result.Authoritative
	snapshot := &api.LegacyRFSnapshot{ClusterUID: string(cluster.UID), AcceptedGeneration: cluster.Generation,
		MarkerVersion: attempt.MarkerVersion, ProtocolVersion: attempt.ProtocolVersion,
		AcceptedSeeds: []string{"192.0.2.10", "192.0.2.11"}, AcceptedSeedDigest: attempt.SeedDigest,
		AuthoritativeEndpoint: authority.Endpoint.String(), ExpectedClusterName: "legacy",
		AttemptTrace: []api.LegacyRFEndpointAttemptSummary{
			{AttemptIndex: 0, Endpoint: attempt.OrderedSeeds[0].String(), Outcome: api.LegacyRFEndpointAttemptAccepted},
			{AttemptIndex: 1, Endpoint: attempt.OrderedSeeds[1].String(), Outcome: api.LegacyRFEndpointAttemptSkipped},
		},
		ServerType: api.ServerDistributionCassandra, SourceVersion: authority.SourceVersion, Partitioner: authority.Partitioner,
		IdentityFingerprint: "identity", TopologyFingerprint: "topology", SchemaFingerprint: "schema",
		ObservedExternalDCs: []string{"legacy-a"}, Replication: api.LegacySystemKeyspaceReplication{
			SystemAuth: map[string]int32{"legacy-a": 2}, SystemTraces: map[string]int32{"legacy-a": 2},
			SystemDistributed: map[string]int32{"legacy-a": 2}},
		SecretBindings: apiBindings(attempt.Connection.SecretBindings), DiscoveryLocation: location,
		AcceptedManagedLocations: []api.LegacyRFManagedLocation{location}, WorkerImageDigest: releaseWorkerDigest,
		AcceptedAt: metav1.NewTime(time.Unix(100, 0))}
	hash, err := k8ssandractrl.LegacyRFSnapshotHash(snapshot)
	require.NoError(t, err)
	snapshot.Hash = hash
	return snapshot
}

func apiBindings(bindings []discovery.SecretBinding) []api.LegacyRFSecretBinding {
	result := make([]api.LegacyRFSecretBinding, len(bindings))
	for index, binding := range bindings {
		result[index] = api.LegacyRFSecretBinding{Purpose: binding.Purpose, SourceContext: binding.SourceContext,
			Namespace: binding.Namespace, Name: binding.Name, Keys: append([]string(nil), binding.Keys...),
			ResourceVersion: binding.ResourceVersion}
	}
	return result
}

func releaseProtocolFixture(t *testing.T) (discovery.Attempt, discovery.DiscoveryResult) {
	t.Helper()
	cluster := releaseCluster("protocol")
	cluster.UID = "uid-1"
	cluster.Generation = 7
	attempt, result, _ := releaseAcceptanceFixture(t, cluster)
	return attempt, result
}

func expectedReleaseResult(attempt discovery.Attempt) discovery.ExpectedResult {
	return discovery.ExpectedResult{SchemaVersion: attempt.ProtocolVersion, ClusterUID: attempt.ClusterUID,
		Generation: attempt.Generation, MarkerVersion: attempt.MarkerVersion, ProtocolVersion: attempt.ProtocolVersion,
		AttemptID: attempt.AttemptID, OrderedSeeds: append([]netip.AddrPort(nil), attempt.OrderedSeeds...),
		SeedDigest: attempt.SeedDigest, SecretBindings: releaseBindings(), WorkerImageDigest: releaseWorkerDigest,
		MaximumBytes: api.LegacyRFDiscoveryMaxResultBytes}
}

func assertBoundaryReason(t *testing.T, err error, expected api.LegacyRFDiscoveryReason) {
	t.Helper()
	var boundary *discovery.BoundaryError
	require.ErrorAs(t, err, &boundary)
	require.Equal(t, expected, boundary.PublicFailure().Reason)
}

func createNamespace(t *testing.T, ctx context.Context, target client.Client, name string) {
	t.Helper()
	err := target.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: name}})
	if err != nil && !strings.Contains(err.Error(), "already exists") {
		require.NoError(t, err)
	}
}

func getCluster(t *testing.T, ctx context.Context, target client.Reader, key types.NamespacedName) *api.K8ssandraCluster {
	t.Helper()
	cluster := &api.K8ssandraCluster{}
	require.NoError(t, target.Get(ctx, key, cluster))
	return cluster
}

func assertDatacenterCount(t *testing.T, ctx context.Context, target client.Reader, namespace string, expected int) {
	t.Helper()
	list := &cassdcapi.CassandraDatacenterList{}
	require.NoError(t, target.List(ctx, list, client.InNamespace(namespace)))
	require.Len(t, list.Items, expected)
}

func sanitizeName(name string) string {
	return strings.ReplaceAll(strings.ToLower(name), " ", "-")
}
