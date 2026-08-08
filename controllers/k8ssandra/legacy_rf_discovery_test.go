package k8ssandra

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/netip"
	"strings"
	"testing"
	"time"

	api "github.com/k8ssandra/k8ssandra-operator/apis/k8ssandra/v1alpha1"
	"github.com/k8ssandra/k8ssandra-operator/pkg/clientcache"
	"github.com/k8ssandra/k8ssandra-operator/pkg/discovery"
	"github.com/stretchr/testify/require"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

type staticImageResolver struct {
	image string
	err   error
}

func (resolver staticImageResolver) Resolve(context.Context) (string, error) {
	return resolver.image, resolver.err
}

type cleanupSafetyFunc func(context.Context, discovery.Attempt) error

func (function cleanupSafetyFunc) AllowCleanup(ctx context.Context, attempt discovery.Attempt) error {
	return function(ctx, attempt)
}

func TestDirectDiscoveryAttemptsEnsureResumesPartialCreationIdempotently(t *testing.T) {
	attempt := resourceTestAttempt(t)
	attempt.Connection.ManagedLocations = []discovery.ManagedLocation{{Namespace: "data-ns", Name: "dc1"}}
	scheme := discoveryAttemptScheme(t)
	auth := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "auth", Namespace: "source-ns", ResourceVersion: "11"}, Data: map[string][]byte{"username": []byte("user-canary"), "password": []byte("password-canary")}}
	tls := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "tls", Namespace: "source-ns", ResourceVersion: "12"}, Data: map[string][]byte{"ca.crt": []byte("certificate-canary")}}
	direct := fake.NewClientBuilder().WithScheme(scheme).WithObjects(auth, tls).Build()
	bindAttemptToCurrentSecrets(t, direct, &attempt)
	cache, err := clientcache.NewValidated(direct, direct, scheme)
	require.NoError(t, err)
	require.NoError(t, cache.AddClientPair("source", direct, direct))
	manager, err := NewDiscoveryAttempts(cache, types.NamespacedName{Namespace: "control", Name: "migration"},
		staticImageResolver{image: attempt.WorkerImageDigest}, bytes.NewReader(bytes.Repeat([]byte{5}, 64)),
		cleanupSafetyFunc(func(context.Context, discovery.Attempt) error { return nil }), record.NewFakeRecorder(10),
		NewBoundedDiscoveryBackoff(time.Second, 8*time.Second, func(delay time.Duration) time.Duration { return delay }))
	require.NoError(t, err)

	state, err := manager.Ensure(context.Background(), attempt)
	require.NoError(t, err)
	require.Equal(t, DiscoveryAttemptPending, state.Phase)
	require.Equal(t, time.Second, state.RequeueAfter)
	firstObjects := listAttemptObjects(t, direct)
	require.Len(t, firstObjects, 8)
	require.NoError(t, direct.Delete(context.Background(), findJob(t, direct)))
	require.NoError(t, direct.Delete(context.Background(), findRole(t, direct)))
	state, err = manager.Ensure(context.Background(), attempt)
	require.NoError(t, err)
	require.Equal(t, DiscoveryAttemptPending, state.Phase)
	require.Len(t, listAttemptObjects(t, direct), 8)

	resultConfigMap := findResultConfigMap(t, direct)
	for _, canary := range []string{"user-canary", "password-canary", "certificate-canary"} {
		for _, object := range firstObjects {
			if _, secret := object.(*corev1.Secret); !secret {
				require.NotContains(t, objectString(t, object), canary)
			}
		}
	}
	require.NotEmpty(t, resultConfigMap.Name)
}

func TestDirectDiscoveryAttemptsEnsureAcceptsOnlyOneStableResultPayload(t *testing.T) {
	t.Run("legitimate population and idempotent reread", func(t *testing.T) {
		manager, direct, attempt := discoveryAttemptsWithEnsuredResources(t)
		resultConfigMap := findResultConfigMap(t, direct)
		body := signedAttemptResult(t, attempt, findHMACSecret(t, direct).Data[legacyRFHMACDataKey])
		resultConfigMap.Data = map[string]string{}
		resultConfigMap.Data[legacyRFResultDataKey] = string(body)
		require.NoError(t, direct.Update(context.Background(), resultConfigMap))

		_, err := manager.Ensure(context.Background(), attempt)
		require.NoError(t, err)
		sealed := findResultConfigMap(t, direct)
		require.NotEmpty(t, sealed.Annotations[legacyRFResultDigestAnnotation])
		_, err = manager.Ensure(context.Background(), attempt)
		require.NoError(t, err)

		sealed.Data[legacyRFResultDataKey] = "conflicting-payload"
		require.NoError(t, direct.Update(context.Background(), sealed))
		_, err = manager.Ensure(context.Background(), attempt)
		require.Error(t, err)
	})

	for _, test := range []struct {
		name   string
		mutate func(*corev1.ConfigMap)
	}{
		{name: "foreign data key", mutate: func(result *corev1.ConfigMap) {
			result.Data[legacyRFResultDataKey] = "payload"
			result.Data["foreign"] = "payload"
		}},
		{name: "metadata tamper", mutate: func(result *corev1.ConfigMap) {
			result.Labels[api.K8ssandraClusterNameLabel] = "other"
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			manager, direct, attempt := discoveryAttemptsWithEnsuredResources(t)
			resultConfigMap := findResultConfigMap(t, direct)
			if resultConfigMap.Data == nil {
				resultConfigMap.Data = map[string]string{}
			}
			test.mutate(resultConfigMap)
			require.NoError(t, direct.Update(context.Background(), resultConfigMap))
			_, err := manager.Ensure(context.Background(), attempt)
			require.Error(t, err)
		})
	}
}

func TestDirectDiscoveryAttemptsEnsureRejectsMissingEstablishedResultChannel(t *testing.T) {
	manager, direct, attempt := discoveryAttemptsWithEnsuredResources(t)
	require.NoError(t, direct.Delete(context.Background(), findResultConfigMap(t, direct)))

	_, err := manager.Ensure(context.Background(), attempt)
	require.Error(t, err)
}

func TestDirectDiscoveryAttemptsEnsureIgnoresServerManagedMetadata(t *testing.T) {
	manager, direct, attempt := discoveryAttemptsWithEnsuredResources(t)
	configMaps := &corev1.ConfigMapList{}
	require.NoError(t, direct.List(context.Background(), configMaps, client.InNamespace("data-ns")))
	var attemptConfigMap *corev1.ConfigMap
	for index := range configMaps.Items {
		if strings.HasSuffix(configMaps.Items[index].Name, "-attempt") {
			attemptConfigMap = &configMaps.Items[index]
			break
		}
	}
	require.NotNil(t, attemptConfigMap)
	attemptConfigMap.UID = types.UID("api-server-uid")
	attemptConfigMap.CreationTimestamp = metav1.NewTime(time.Unix(1_700_000_000, 0))
	attemptConfigMap.ManagedFields = []metav1.ManagedFieldsEntry{{Manager: "kube-apiserver", Operation: metav1.ManagedFieldsOperationUpdate}}
	require.NoError(t, direct.Update(context.Background(), attemptConfigMap))

	_, err := manager.Ensure(context.Background(), attempt)
	require.NoError(t, err)
}

func TestAttemptObjectMatchingNormalizesOnlyEmptySecretData(t *testing.T) {
	desired := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "source", Namespace: "data-ns"}, Type: corev1.SecretTypeOpaque, Data: map[string][]byte{}}
	current := desired.DeepCopy()
	current.Data = nil
	current.UID = types.UID("api-server-uid")
	require.True(t, attemptObjectMatchesDesired(desired, current))

	current.Data = map[string][]byte{"foreign": []byte("value")}
	require.False(t, attemptObjectMatchesDesired(desired, current))
}

func TestDirectDiscoveryAttemptsValidatesSignedResultAndRejectsForgeryStalenessAndOversize(t *testing.T) {
	manager, direct, attempt := discoveryAttemptsWithEnsuredResources(t)
	resultConfigMap := findResultConfigMap(t, direct)
	hmacSecret := findHMACSecret(t, direct)
	valid := signedAttemptResult(t, attempt, hmacSecret.Data[legacyRFHMACDataKey])
	staleAttempt := attempt
	staleAttempt.Generation++
	stale := signedAttemptResult(t, staleAttempt, hmacSecret.Data[legacyRFHMACDataKey])

	tests := []struct {
		name   string
		body   []byte
		reason api.LegacyRFDiscoveryReason
	}{
		{name: "forged", body: forgeSignedResult(t, valid), reason: api.LegacyRFReasonForgedDiscoveryResult},
		{name: "stale", body: stale, reason: api.LegacyRFReasonStaleDiscoveryResult},
		{name: "oversize", body: bytes.Repeat([]byte("x"), api.LegacyRFDiscoveryMaxResultBytes+1), reason: api.LegacyRFReasonDiscoveryResultTooLarge},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			current := &corev1.ConfigMap{}
			require.NoError(t, direct.Get(context.Background(), client.ObjectKeyFromObject(resultConfigMap), current))
			if current.Data == nil {
				current.Data = map[string]string{}
			}
			current.Data[legacyRFResultDataKey] = string(test.body)
			require.NoError(t, direct.Update(context.Background(), current))
			_, err := manager.Result(context.Background(), attempt)
			var boundary *discovery.BoundaryError
			require.ErrorAs(t, err, &boundary)
			require.Equal(t, test.reason, boundary.PublicFailure().Reason)
			require.NotContains(t, err.Error(), string(test.body))
		})
	}

	current := &corev1.ConfigMap{}
	require.NoError(t, direct.Get(context.Background(), client.ObjectKeyFromObject(resultConfigMap), current))
	if current.Data == nil {
		current.Data = map[string]string{}
	}
	current.Data[legacyRFResultDataKey] = string(valid)
	require.NoError(t, direct.Update(context.Background(), current))
	result, err := manager.Result(context.Background(), attempt)
	require.NoError(t, err)
	require.Equal(t, attempt.WorkerImageDigest, result.WorkerImageDigest)
}

func TestDiscoveryAttemptFailuresAreSanitizedTransitionOnlyAndRetriesAreBounded(t *testing.T) {
	manager, direct, attempt := discoveryAttemptsWithEnsuredResources(t)
	resultConfigMap := findResultConfigMap(t, direct)
	current := &corev1.ConfigMap{}
	require.NoError(t, direct.Get(context.Background(), client.ObjectKeyFromObject(resultConfigMap), current))
	current.Data = map[string]string{legacyRFResultDataKey: "password-canary malformed"}
	require.NoError(t, direct.Update(context.Background(), current))
	for range 2 {
		_, err := manager.Result(context.Background(), attempt)
		require.Error(t, err)
		require.NotContains(t, err.Error(), "password-canary")
	}
	select {
	case event := <-manager.recorder.(*record.FakeRecorder).Events:
		require.NotContains(t, event, "password-canary")
	default:
		t.Fatal("expected one failure transition event")
	}
	select {
	case duplicate := <-manager.recorder.(*record.FakeRecorder).Events:
		t.Fatalf("unexpected duplicate transition event: %s", duplicate)
	default:
	}

	backoff := NewBoundedDiscoveryBackoff(time.Second, 8*time.Second, func(delay time.Duration) time.Duration { return delay + time.Second })
	require.Equal(t, 2*time.Second, backoff.Delay(0))
	require.Equal(t, 8*time.Second, backoff.Delay(99))
	require.Equal(t, api.LegacyRFReasonWorkerImagePullFailed, boundaryReason(t, classifyPodFailures([]corev1.Pod{{Status: corev1.PodStatus{ContainerStatuses: []corev1.ContainerStatus{{State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "ImagePullBackOff"}}}}}}})))
	require.Equal(t, api.LegacyRFReasonJobSchedulingFailed, boundaryReason(t, classifyPodFailures([]corev1.Pod{{Status: corev1.PodStatus{Conditions: []corev1.PodCondition{{Type: corev1.PodScheduled, Status: corev1.ConditionFalse, Reason: "Unschedulable"}}}}})))
}

func TestDiscoveryJitterUsesInjectedEntropy(t *testing.T) {
	jitter := NewDiscoveryJitter(bytes.NewReader(make([]byte, 8)))
	require.NotNil(t, jitter)
	require.Equal(t, 4*time.Second, jitter(8*time.Second), "zero entropy selects the lower half of the jitter range")
	require.Zero(t, jitter(0))
	require.Nil(t, NewDiscoveryJitter(nil))
}

func TestDirectDiscoveryAttemptsCleanupRequiresSafetyAndRemovesOnlyCorrelatedResources(t *testing.T) {
	manager, direct, attempt := discoveryAttemptsWithEnsuredResources(t)
	manager.cleanupSafety = cleanupSafetyFunc(func(context.Context, discovery.Attempt) error { return errors.New("snapshot loss unsafe") })
	complete, err := manager.Cleanup(context.Background(), attempt)
	require.Error(t, err)
	require.False(t, complete)
	require.Len(t, listAttemptObjects(t, direct), 8)
	manager.cleanupSafety = cleanupSafetyFunc(func(context.Context, discovery.Attempt) error { return nil })
	complete, err = manager.Cleanup(context.Background(), attempt)
	require.NoError(t, err)
	require.False(t, complete, "issuing deletes must end the reconcile before replacement")
	require.Empty(t, listAttemptObjects(t, direct))
	complete, err = manager.Cleanup(context.Background(), attempt)
	require.NoError(t, err)
	require.True(t, complete, "a later direct-read pass proves cleanup complete")
}

func TestDirectDiscoveryAttemptsCleanupWaitsForTerminatingJob(t *testing.T) {
	manager, direct, attempt := discoveryAttemptsWithEnsuredResources(t)
	job := findJob(t, direct)
	job.Finalizers = []string{"test.k8ssandra.io/delayed-deletion"}
	require.NoError(t, direct.Update(context.Background(), job))

	complete, err := manager.Cleanup(context.Background(), attempt)
	require.NoError(t, err)
	require.False(t, complete)
	terminating := &batchv1.Job{}
	require.NoError(t, direct.Get(context.Background(), client.ObjectKeyFromObject(job), terminating))
	require.NotNil(t, terminating.DeletionTimestamp)

	complete, err = manager.Cleanup(context.Background(), attempt)
	require.NoError(t, err)
	require.False(t, complete, "a terminating Job is still present")
	require.NoError(t, direct.Get(context.Background(), client.ObjectKeyFromObject(job), terminating))
	terminating.Finalizers = nil
	require.NoError(t, direct.Update(context.Background(), terminating))

	complete, err = manager.Cleanup(context.Background(), attempt)
	require.NoError(t, err)
	require.True(t, complete)
}

func TestDirectDiscoveryAttemptsCleanupWaitsForCorrelatedPods(t *testing.T) {
	manager, direct, attempt := discoveryAttemptsWithEnsuredResources(t)
	complete, err := manager.Cleanup(context.Background(), attempt)
	require.NoError(t, err)
	require.False(t, complete)

	location := api.LegacyRFManagedLocation{Namespace: "data-ns", Name: "dc1"}
	metadata := newAttemptMetadata(LegacyRFAttemptResourcesInput{
		ClusterKey: manager.clusterKey, Attempt: attempt, Location: location,
	})
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Name: "old-worker", Namespace: location.Namespace,
		Labels: metadata.labels, Annotations: metadata.annotations,
	}}
	require.NoError(t, direct.Create(context.Background(), pod))

	complete, err = manager.Cleanup(context.Background(), attempt)
	require.NoError(t, err)
	require.False(t, complete, "an old correlated worker Pod prevents replacement")
	require.Error(t, direct.Get(context.Background(), client.ObjectKeyFromObject(pod), &corev1.Pod{}))

	complete, err = manager.Cleanup(context.Background(), attempt)
	require.NoError(t, err)
	require.True(t, complete)
}

func TestAcceptedCleanupRemovesCredentialsAndSupportResourcesWithoutLosingAuthorization(t *testing.T) {
	manager, direct, attempt := discoveryAttemptsWithEnsuredResources(t)
	cluster := safetyCluster()
	cluster.UID = types.UID(attempt.ClusterUID)
	cluster.Generation = attempt.Generation
	cluster.Spec.Cassandra.AdditionalSeeds = []string{attempt.OrderedSeeds[0].Addr().String(), attempt.OrderedSeeds[1].Addr().String()}
	snapshot := planSnapshot()
	snapshot.ClusterUID = attempt.ClusterUID
	snapshot.AcceptedGeneration = attempt.Generation
	snapshot.MarkerVersion = attempt.MarkerVersion
	snapshot.ProtocolVersion = attempt.ProtocolVersion
	snapshot.AcceptedSeeds = []string{attempt.OrderedSeeds[0].Addr().String(), attempt.OrderedSeeds[1].Addr().String()}
	snapshot.AcceptedSeedDigest = attempt.SeedDigest
	snapshot.SecretBindings = toAPISecretBindings(attempt.Connection.SecretBindings)
	snapshot.WorkerImageDigest = attempt.WorkerImageDigest
	snapshot.AcceptedManagedLocations = []api.LegacyRFManagedLocation{{Namespace: "data-ns", Name: "dc1"}}
	snapshot.DiscoveryLocation = snapshot.AcceptedManagedLocations[0]
	hash, err := LegacyRFSnapshotHash(snapshot)
	require.NoError(t, err)
	snapshot.Hash = hash
	cluster.Status.LegacyRFDiscovery = acceptedStatus(snapshot)
	runtime := &legacyRFControllerRuntime{
		clients: manager.clients, control: &traceLegacyRFControl{current: cluster}, managed: &traceManagedState{err: errors.New("accepted cleanup must not require absence")},
		image: manager.imageResolver, keyReader: manager.keyReader, recorder: manager.recorder, backoff: manager.backoff,
	}

	complete, err := runtime.cleanupAcceptedAttempt(context.Background(), cluster)
	require.NoError(t, err)
	require.False(t, complete, "deletion is asynchronous")
	require.Empty(t, listAttemptObjects(t, direct), "copied credentials and support resources must be gone")
	complete, err = runtime.cleanupAcceptedAttempt(context.Background(), cluster)
	require.NoError(t, err)
	require.True(t, complete)
	require.True(t, legacyRFSnapshotIsIntact(cluster.Status.LegacyRFDiscovery))
	plan, err := BuildLegacyRFCurrentPlan(cluster)
	require.NoError(t, err)
	_, err = BuildLegacyRFCreateAuthorization(snapshot, cluster.Status.LegacyRFDiscovery, plan)
	require.NoError(t, err)
}

func discoveryAttemptsWithEnsuredResources(t *testing.T) (*directDiscoveryAttempts, client.Client, discovery.Attempt) {
	t.Helper()
	attempt := resourceTestAttempt(t)
	attempt.AttemptID = legacyRFAttemptID(types.UID(attempt.ClusterUID), attempt.Generation)
	attempt.Connection.ManagedLocations = []discovery.ManagedLocation{{Namespace: "data-ns", Name: "dc1"}}
	scheme := discoveryAttemptScheme(t)
	auth := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "auth", Namespace: "source-ns", ResourceVersion: "11"}, Data: map[string][]byte{"username": []byte("user"), "password": []byte("password")}}
	tls := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "tls", Namespace: "source-ns", ResourceVersion: "12"}, Data: map[string][]byte{"ca.crt": []byte("ca")}}
	direct := fake.NewClientBuilder().WithScheme(scheme).WithObjects(auth, tls).Build()
	bindAttemptToCurrentSecrets(t, direct, &attempt)
	cache, err := clientcache.NewValidated(direct, direct, scheme)
	require.NoError(t, err)
	require.NoError(t, cache.AddClientPair("source", direct, direct))
	manager, err := NewDiscoveryAttempts(cache, types.NamespacedName{Namespace: "control", Name: "migration"}, staticImageResolver{image: attempt.WorkerImageDigest}, bytes.NewReader(bytes.Repeat([]byte{3}, 64)), cleanupSafetyFunc(func(context.Context, discovery.Attempt) error { return nil }), record.NewFakeRecorder(10), NewBoundedDiscoveryBackoff(time.Second, 8*time.Second, func(delay time.Duration) time.Duration { return delay }))
	require.NoError(t, err)
	_, err = manager.Ensure(context.Background(), attempt)
	require.NoError(t, err)
	return manager, direct, attempt
}

func signedAttemptResult(t *testing.T, attempt discovery.Attempt, key []byte) []byte {
	t.Helper()
	result := discovery.DiscoveryResult{
		SchemaVersion: attempt.ProtocolVersion, ClusterUID: attempt.ClusterUID, Generation: attempt.Generation,
		MarkerVersion: attempt.MarkerVersion, ProtocolVersion: attempt.ProtocolVersion, AttemptID: attempt.AttemptID,
		OrderedSeeds: attempt.OrderedSeeds, SeedDigest: attempt.SeedDigest, SecretBindings: attempt.Connection.SecretBindings,
		WorkerImageDigest: attempt.WorkerImageDigest,
		Authoritative:     &discovery.AuthoritativeCandidate{AttemptID: attempt.AttemptID, AttemptIndex: 0, Endpoint: attempt.OrderedSeeds[0]},
		AttemptTrace: append([]discovery.EndpointAttemptSummary{
			{AttemptIndex: 0, Endpoint: attempt.OrderedSeeds[0], Outcome: discovery.EndpointAttemptAccepted},
		}, skippedAttemptTrace(attempt.OrderedSeeds[1:], 1)...),
	}
	body, err := discovery.SignResult(result, key, api.LegacyRFDiscoveryMaxResultBytes)
	require.NoError(t, err)
	return body
}

func skippedAttemptTrace(endpoints []netip.AddrPort, offset int) []discovery.EndpointAttemptSummary {
	trace := make([]discovery.EndpointAttemptSummary, len(endpoints))
	for index, endpoint := range endpoints {
		trace[index] = discovery.EndpointAttemptSummary{AttemptIndex: offset + index, Endpoint: endpoint, Outcome: discovery.EndpointAttemptSkipped}
	}
	return trace
}

func forgeSignedResult(t *testing.T, body []byte) []byte {
	t.Helper()
	var envelope discovery.ResultEnvelope
	require.NoError(t, json.Unmarshal(body, &envelope))
	mac, err := base64.RawStdEncoding.DecodeString(envelope.MAC)
	require.NoError(t, err)
	mac[0] ^= 0xff
	envelope.MAC = base64.RawStdEncoding.EncodeToString(mac)
	forged, err := json.Marshal(envelope)
	require.NoError(t, err)
	return forged
}

func boundaryReason(t *testing.T, err error) api.LegacyRFDiscoveryReason {
	t.Helper()
	var boundary *discovery.BoundaryError
	require.ErrorAs(t, err, &boundary)
	return boundary.PublicFailure().Reason
}

func discoveryAttemptScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	scheme := runtime.NewScheme()
	require.NoError(t, corev1.AddToScheme(scheme))
	require.NoError(t, batchv1.AddToScheme(scheme))
	require.NoError(t, rbacv1.AddToScheme(scheme))
	return scheme
}

func bindAttemptToCurrentSecrets(t *testing.T, reader client.Reader, attempt *discovery.Attempt) {
	t.Helper()
	for index := range attempt.Connection.SecretBindings {
		binding := &attempt.Connection.SecretBindings[index]
		secret := &corev1.Secret{}
		require.NoError(t, reader.Get(context.Background(), types.NamespacedName{Namespace: binding.Namespace, Name: binding.Name}, secret))
		binding.ResourceVersion = secret.ResourceVersion
	}
}

func listAttemptObjects(t *testing.T, reader client.Client) []client.Object {
	t.Helper()
	objects := []client.Object{}
	lists := []client.ObjectList{&corev1.ConfigMapList{}, &corev1.SecretList{}, &corev1.ServiceAccountList{}, &rbacv1.RoleList{}, &rbacv1.RoleBindingList{}, &batchv1.JobList{}}
	for _, list := range lists {
		require.NoError(t, reader.List(context.Background(), list, client.InNamespace("data-ns")))
		objects = append(objects, itemsFromList(list)...)
	}
	return objects
}

func itemsFromList(list client.ObjectList) []client.Object {
	switch typed := list.(type) {
	case *corev1.ConfigMapList:
		result := make([]client.Object, len(typed.Items))
		for index := range typed.Items {
			result[index] = &typed.Items[index]
		}
		return result
	case *corev1.SecretList:
		result := make([]client.Object, len(typed.Items))
		for index := range typed.Items {
			result[index] = &typed.Items[index]
		}
		return result
	case *corev1.ServiceAccountList:
		result := make([]client.Object, len(typed.Items))
		for index := range typed.Items {
			result[index] = &typed.Items[index]
		}
		return result
	case *rbacv1.RoleList:
		result := make([]client.Object, len(typed.Items))
		for index := range typed.Items {
			result[index] = &typed.Items[index]
		}
		return result
	case *rbacv1.RoleBindingList:
		result := make([]client.Object, len(typed.Items))
		for index := range typed.Items {
			result[index] = &typed.Items[index]
		}
		return result
	case *batchv1.JobList:
		result := make([]client.Object, len(typed.Items))
		for index := range typed.Items {
			result[index] = &typed.Items[index]
		}
		return result
	default:
		return nil
	}
}

func findResultConfigMap(t *testing.T, reader client.Client) *corev1.ConfigMap {
	t.Helper()
	list := &corev1.ConfigMapList{}
	require.NoError(t, reader.List(context.Background(), list, client.InNamespace("data-ns")))
	for index := range list.Items {
		if strings.HasSuffix(list.Items[index].Name, "-result") {
			return &list.Items[index]
		}
	}
	t.Fatal("result ConfigMap not found")
	return nil
}

func findHMACSecret(t *testing.T, reader client.Client) *corev1.Secret {
	t.Helper()
	list := &corev1.SecretList{}
	require.NoError(t, reader.List(context.Background(), list, client.InNamespace("data-ns")))
	for index := range list.Items {
		if strings.HasSuffix(list.Items[index].Name, "-hmac") {
			return &list.Items[index]
		}
	}
	t.Fatal("HMAC Secret not found")
	return nil
}

func findJob(t *testing.T, reader client.Client) *batchv1.Job {
	t.Helper()
	list := &batchv1.JobList{}
	require.NoError(t, reader.List(context.Background(), list, client.InNamespace("data-ns")))
	require.Len(t, list.Items, 1)
	return &list.Items[0]
}

func findRole(t *testing.T, reader client.Client) *rbacv1.Role {
	t.Helper()
	list := &rbacv1.RoleList{}
	require.NoError(t, reader.List(context.Background(), list, client.InNamespace("data-ns")))
	require.Len(t, list.Items, 1)
	return &list.Items[0]
}

func objectString(t *testing.T, object client.Object) string {
	t.Helper()
	body, err := json.Marshal(object)
	require.NoError(t, err)
	return string(body)
}
