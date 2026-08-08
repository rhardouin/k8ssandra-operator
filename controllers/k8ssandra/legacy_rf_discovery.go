package k8ssandra

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"reflect"
	"strings"
	"time"

	api "github.com/k8ssandra/k8ssandra-operator/apis/k8ssandra/v1alpha1"
	"github.com/k8ssandra/k8ssandra-operator/pkg/clientcache"
	"github.com/k8ssandra/k8ssandra-operator/pkg/discovery"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	legacyRFFailureReasonAnnotation = "k8ssandra.io/legacy-rf-failure-reason"
	legacyRFResultDigestAnnotation  = "k8ssandra.io/legacy-rf-result-digest"
)

type legacyRFEventSink interface {
	Event(object runtime.Object, eventType, reason, message string)
}

// LegacyRFDiscoveryDecision is one sanitized, level-based controller decision.
type LegacyRFDiscoveryDecision struct {
	Phase            api.LegacyRFDiscoveryPhase
	Reason           api.LegacyRFDiscoveryReason
	Message          string
	Retryable        bool
	AttemptLocations []api.LegacyRFManagedLocation
}

// DecideLegacyRFDiscovery qualifies a cluster and maps its current observation
// to a stable public state without exposing an underlying error.
func DecideLegacyRFDiscovery(
	cluster *api.K8ssandraCluster,
	managedStatePresent bool,
	attemptFailure error,
) LegacyRFDiscoveryDecision {
	accepted := cluster != nil && cluster.Status.LegacyRFDiscovery != nil &&
		cluster.Status.LegacyRFDiscovery.Phase == api.LegacyRFDiscoveryPhaseAccepted
	if accepted && !legacyRFSnapshotIsIntact(cluster.Status.LegacyRFDiscovery) {
		return legacyRFFailureDecision(api.LegacyRFReasonSnapshotConflict)
	}
	if !accepted && !legacyRFDiscoveryQualifies(cluster) {
		return LegacyRFDiscoveryDecision{Phase: api.LegacyRFDiscoveryPhaseNotRequired}
	}
	if cluster.Spec.Cassandra == nil {
		return legacyRFFailureDecision(api.LegacyRFReasonInvalidDiscoveryResult)
	}
	if failure := legacyRFPrerequisiteDecision(cluster); failure != nil {
		return *failure
	}
	plan, err := BuildLegacyRFCurrentPlan(cluster)
	if err != nil || len(plan.ManagedLocations) == 0 {
		return legacyRFFailureDecision(api.LegacyRFReasonInvalidDiscoveryResult)
	}
	if accepted := acceptedLegacyRFDecision(cluster, plan); accepted != nil {
		return *accepted
	}
	if managedStatePresent || len(cluster.Status.Datacenters) != 0 ||
		cluster.Status.GetConditionStatus(api.CassandraInitialized) == corev1.ConditionTrue {
		return legacyRFFailureDecision(api.LegacyRFReasonDiscoveryTooLate)
	}
	if attemptFailure != nil {
		return legacyRFErrorDecision(attemptFailure)
	}
	return LegacyRFDiscoveryDecision{
		Phase:            api.LegacyRFDiscoveryPhasePending,
		AttemptLocations: append([]api.LegacyRFManagedLocation(nil), plan.ManagedLocations...),
	}
}

func legacyRFDiscoveryQualifies(cluster *api.K8ssandraCluster) bool {
	if cluster == nil || cluster.Spec.Cassandra == nil || len(cluster.Spec.Cassandra.AdditionalSeeds) == 0 {
		return false
	}
	marker, found := cluster.Annotations[api.LegacyRFDiscoveryMarkerAnnotation]
	return found && marker != ""
}

func legacyRFPrerequisiteDecision(
	cluster *api.K8ssandraCluster,
) *LegacyRFDiscoveryDecision {
	marker := cluster.Annotations[api.LegacyRFDiscoveryMarkerAnnotation]
	if marker != api.LegacyRFDiscoveryMarkerVersion {
		return pointerToDecision(legacyRFFailureDecision(api.LegacyRFReasonMarkerInvalid))
	}
	if effectiveServerType(cluster.Spec.Cassandra.ServerType) != api.ServerDistributionCassandra {
		failure, _ := api.LegacyRFDiscoveryFailureForReason(api.LegacyRFReasonUnsupportedServerType)
		return &LegacyRFDiscoveryDecision{
			Phase: api.LegacyRFDiscoveryPhaseNotRequired, Reason: failure.Reason, Message: failure.Message,
		}
	}
	if cluster.Spec.UseExternalSecrets() {
		return pointerToDecision(legacyRFFailureDecision(api.LegacyRFReasonUnsupportedSecretsProvider))
	}
	if len(cluster.Spec.Cassandra.AdditionalSeeds) > api.LegacyRFDiscoveryMaxSeeds {
		return pointerToDecision(legacyRFFailureDecision(api.LegacyRFReasonInvalidContactPoint))
	}
	return nil
}

func acceptedLegacyRFDecision(
	cluster *api.K8ssandraCluster,
	plan LegacyRFCurrentPlan,
) *LegacyRFDiscoveryDecision {
	status := cluster.Status.LegacyRFDiscovery
	if status == nil || status.Phase != api.LegacyRFDiscoveryPhaseAccepted {
		return nil
	}
	if !legacyRFSnapshotIsIntact(status) {
		return pointerToDecision(legacyRFFailureDecision(api.LegacyRFReasonSnapshotConflict))
	}
	if err := ValidateCurrentPlan(status.AcceptedSnapshot, plan); err != nil {
		reason := api.LegacyRFReasonSnapshotConflict
		if firstCollision(plan.ManagedDatacenterNames, status.AcceptedSnapshot.ObservedExternalDCs) != "" {
			reason = api.LegacyRFReasonManagedDatacenterNameCollision
		}
		return pointerToDecision(legacyRFFailureDecision(reason))
	}
	return &LegacyRFDiscoveryDecision{Phase: api.LegacyRFDiscoveryPhaseAccepted}
}

func legacyRFErrorDecision(err error) LegacyRFDiscoveryDecision {
	var boundary *discovery.BoundaryError
	if errors.As(err, &boundary) {
		failure := boundary.PublicFailure()
		return decisionFromFailure(failure)
	}
	return legacyRFFailureDecision(api.LegacyRFReasonInvalidDiscoveryResult)
}

func legacyRFFailureDecision(reason api.LegacyRFDiscoveryReason) LegacyRFDiscoveryDecision {
	failure, found := api.LegacyRFDiscoveryFailureForReason(reason)
	if !found {
		failure, _ = api.LegacyRFDiscoveryFailureForReason(api.LegacyRFReasonInvalidDiscoveryResult)
	}
	return decisionFromFailure(failure)
}

func decisionFromFailure(failure api.LegacyRFDiscoveryFailure) LegacyRFDiscoveryDecision {
	return LegacyRFDiscoveryDecision{
		Phase: api.LegacyRFDiscoveryPhaseBlocked, Reason: failure.Reason,
		Message: failure.Message, Retryable: failure.Retryable,
	}
}

func pointerToDecision(decision LegacyRFDiscoveryDecision) *LegacyRFDiscoveryDecision {
	return &decision
}

// ApplyLegacyRFDiscoveryDecision records a sanitized status and Event only when
// phase, reason, or message changes. It returns whether a transition occurred.
func ApplyLegacyRFDiscoveryDecision(
	cluster *api.K8ssandraCluster,
	decision LegacyRFDiscoveryDecision,
	now metav1.Time,
	recorder legacyRFEventSink,
) bool {
	if cluster == nil {
		return false
	}
	previous := cluster.Status.LegacyRFDiscovery
	transitioned := previous == nil || previous.Phase != decision.Phase ||
		previous.Reason != decision.Reason || previous.Message != decision.Message
	next := copyLegacyRFDiscoveryStatus(previous)
	next.ObservedGeneration = cluster.Generation
	next.Phase, next.Reason, next.Message = decision.Phase, decision.Reason, decision.Message
	if len(decision.AttemptLocations) != 0 {
		next.CurrentManagedLocations = appendUniqueLocations(nil, decision.AttemptLocations)
	}
	if transitioned {
		next.LastTransitionTime = now.DeepCopy()
	}
	cluster.Status.LegacyRFDiscovery = &next
	if transitioned && recorder != nil {
		eventType := corev1.EventTypeNormal
		if decision.Reason != "" {
			eventType = corev1.EventTypeWarning
		}
		recorder.Event(cluster, eventType, legacyRFDecisionEventReason(decision), decision.Message)
	}
	return transitioned
}

func legacyRFDecisionEventReason(decision LegacyRFDiscoveryDecision) string {
	if decision.Reason != "" {
		return string(decision.Reason)
	}
	return "LegacyRFDiscovery" + string(decision.Phase)
}

// DiscoveryAttemptPhase describes the observable state of one worker attempt.
type DiscoveryAttemptPhase string

const (
	// DiscoveryAttemptPending means support resources exist but no verified result is available.
	DiscoveryAttemptPending DiscoveryAttemptPhase = "Pending"
	// DiscoveryAttemptComplete means the Job reports successful completion.
	DiscoveryAttemptComplete DiscoveryAttemptPhase = "Complete"
)

// DiscoveryAttemptState is the concrete bounded polling state returned by Ensure.
type DiscoveryAttemptState struct {
	Phase        DiscoveryAttemptPhase
	RequeueAfter time.Duration
}

// DiscoveryAttempts owns the direct Kubernetes lifecycle of one discovery attempt.
type DiscoveryAttempts interface {
	Ensure(context.Context, discovery.Attempt) (DiscoveryAttemptState, error)
	Result(context.Context, discovery.Attempt) (*discovery.DiscoveryResult, error)
	Cleanup(context.Context, discovery.Attempt) (bool, error)
}

// DiscoveryCleanupSafety authorizes deletion only after snapshot-loss checks pass.
type DiscoveryCleanupSafety interface {
	AllowCleanup(context.Context, discovery.Attempt) error
}

// DiscoveryBackoff supplies bounded retry delays without sleeping in reconciliation.
type DiscoveryBackoff interface {
	Delay(int32) time.Duration
}

type boundedDiscoveryBackoff struct {
	initial time.Duration
	maximum time.Duration
	jitter  func(time.Duration) time.Duration
}

// NewDiscoveryJitter returns a half-to-full jitter function backed by injected entropy.
func NewDiscoveryJitter(reader io.Reader) func(time.Duration) time.Duration {
	if reader == nil {
		return nil
	}
	return func(delay time.Duration) time.Duration {
		if delay <= 0 {
			return 0
		}
		minimum := delay / 2
		window := delay - minimum
		var entropy [8]byte
		if _, err := io.ReadFull(reader, entropy[:]); err != nil {
			return minimum
		}
		return minimum + time.Duration(binary.BigEndian.Uint64(entropy[:])%uint64(window))
	}
}

// NewBoundedDiscoveryBackoff constructs exponential backoff capped before and after jitter.
func NewBoundedDiscoveryBackoff(
	initial, maximum time.Duration,
	jitter func(time.Duration) time.Duration,
) DiscoveryBackoff {
	if initial <= 0 || maximum < initial || jitter == nil {
		return nil
	}
	return &boundedDiscoveryBackoff{initial: initial, maximum: maximum, jitter: jitter}
}

func (backoff *boundedDiscoveryBackoff) Delay(failures int32) time.Duration {
	delay := backoff.initial
	for index := int32(0); index < failures && delay < backoff.maximum; index++ {
		if delay > backoff.maximum/2 {
			delay = backoff.maximum
			break
		}
		delay *= 2
	}
	jittered := backoff.jitter(delay)
	if jittered < 0 {
		return 0
	}
	if jittered > backoff.maximum {
		return backoff.maximum
	}
	return jittered
}

type directDiscoveryAttempts struct {
	clients       *clientcache.ClientCache
	clusterKey    types.NamespacedName
	imageResolver ManagerImageResolver
	keyReader     io.Reader
	cleanupSafety DiscoveryCleanupSafety
	recorder      legacyRFEventSink
	backoff       DiscoveryBackoff
}

// NewDiscoveryAttempts creates a direct-read, bounded attempt lifecycle manager.
func NewDiscoveryAttempts(
	clients *clientcache.ClientCache,
	clusterKey types.NamespacedName,
	imageResolver ManagerImageResolver,
	keyReader io.Reader,
	cleanupSafety DiscoveryCleanupSafety,
	recorder legacyRFEventSink,
	backoff DiscoveryBackoff,
) (*directDiscoveryAttempts, error) {
	if clients == nil || imageResolver == nil || keyReader == nil || cleanupSafety == nil || recorder == nil || backoff == nil {
		return nil, errors.New("create discovery attempts: every dependency is required")
	}
	if clusterKey.Namespace == "" || clusterKey.Name == "" {
		return nil, errors.New("create discovery attempts: cluster key is required")
	}
	return &directDiscoveryAttempts{clients: clients, clusterKey: clusterKey, imageResolver: imageResolver,
		keyReader: keyReader, cleanupSafety: cleanupSafety, recorder: recorder, backoff: backoff}, nil
}

func (manager *directDiscoveryAttempts) Ensure(
	ctx context.Context,
	attempt discovery.Attempt,
) (DiscoveryAttemptState, error) {
	location, directClient, err := manager.attemptClient(attempt)
	if err != nil {
		return DiscoveryAttemptState{}, err
	}
	if err = manager.verifyWorkerImage(ctx, attempt); err != nil {
		return DiscoveryAttemptState{}, err
	}
	secretData, err := manager.copyBoundSecrets(ctx, attempt.Connection.SecretBindings)
	if err != nil {
		return DiscoveryAttemptState{}, err
	}
	key, err := manager.ensureHMACKey(ctx, directClient, attempt, location)
	if err != nil {
		return DiscoveryAttemptState{}, err
	}
	resources, err := BuildLegacyRFAttemptResources(LegacyRFAttemptResourcesInput{
		ClusterKey: manager.clusterKey, Attempt: attempt, Location: location,
		HMACKey: key, CopiedSecretData: secretData,
	})
	if err != nil {
		return DiscoveryAttemptState{}, fmt.Errorf("ensure discovery attempt: %w", err)
	}
	if err = ensureAttemptObjects(ctx, directClient, resources.objects()); err != nil {
		return DiscoveryAttemptState{}, manager.apiFailure("ensure attempt resources", err)
	}
	state, err := manager.jobState(ctx, directClient, resources.Job)
	if err != nil {
		if transitionErr := manager.recordFailureByKey(ctx, directClient, client.ObjectKeyFromObject(resources.ResultConfigMap), err); transitionErr != nil {
			return DiscoveryAttemptState{}, transitionErr
		}
	}
	return state, err
}

func (manager *directDiscoveryAttempts) Result(
	ctx context.Context,
	attempt discovery.Attempt,
) (*discovery.DiscoveryResult, error) {
	location, directClient, err := manager.attemptClient(attempt)
	if err != nil {
		return nil, err
	}
	if err = manager.verifyWorkerImage(ctx, attempt); err != nil {
		return nil, err
	}
	metadata := newAttemptMetadata(LegacyRFAttemptResourcesInput{ClusterKey: manager.clusterKey, Attempt: attempt, Location: location})
	resultConfigMap := &corev1.ConfigMap{}
	resultKey := types.NamespacedName{Namespace: location.Namespace, Name: metadata.baseName + "-result"}
	if err = directClient.Get(ctx, resultKey, resultConfigMap); err != nil {
		return nil, manager.apiFailure("read discovery result", err)
	}
	body := []byte(resultConfigMap.Data[legacyRFResultDataKey])
	if len(body) == 0 {
		return nil, nil
	}
	key, err := manager.readHMACKey(ctx, directClient, metadata)
	if err != nil {
		return nil, err
	}
	result, err := discovery.ValidateResultEnvelope(body, key, expectedAttemptResult(attempt))
	if err != nil {
		if transitionErr := manager.recordFailureTransition(ctx, directClient, resultConfigMap, err); transitionErr != nil {
			return nil, transitionErr
		}
		return nil, err
	}
	if result.Failure != nil {
		err = discovery.NewBoundaryError(result.Failure.Reason, errors.New("worker reported terminal discovery failure"))
		if transitionErr := manager.recordFailureTransition(ctx, directClient, resultConfigMap, err); transitionErr != nil {
			return nil, transitionErr
		}
		return nil, err
	}
	return &result, nil
}

func (manager *directDiscoveryAttempts) Cleanup(ctx context.Context, attempt discovery.Attempt) (bool, error) {
	if err := manager.cleanupSafety.AllowCleanup(ctx, attempt); err != nil {
		return false, fmt.Errorf("cleanup discovery attempt: safety check failed: %w", err)
	}
	location, directClient, err := manager.attemptClient(attempt)
	if err != nil {
		return false, err
	}
	metadata := newAttemptMetadata(LegacyRFAttemptResourcesInput{ClusterKey: manager.clusterKey, Attempt: attempt, Location: location})
	resources := cleanupObjects(metadata)
	complete := true
	for _, object := range resources {
		present, deleteErr := deleteCorrelatedObject(ctx, directClient, object)
		if deleteErr != nil {
			return false, manager.apiFailure("delete discovery attempt resource", deleteErr)
		}
		if present {
			complete = false
		}
	}
	podsPresent, err := cleanupCorrelatedPods(ctx, directClient, metadata)
	if err != nil {
		return false, manager.apiFailure("delete discovery attempt Pods", err)
	}
	if podsPresent {
		complete = false
	}
	return complete, nil
}

func cleanupCorrelatedPods(
	ctx context.Context,
	directClient client.Client,
	metadata attemptMetadata,
) (bool, error) {
	pods := &corev1.PodList{}
	if err := directClient.List(ctx, pods, client.InNamespace(metadata.namespace), client.MatchingLabels(metadata.labels)); err != nil {
		return false, fmt.Errorf("list correlated discovery Pods: %w", err)
	}
	present := false
	for index := range pods.Items {
		pod := &pods.Items[index]
		if pod.Annotations[legacyRFAttemptIDAnnotation] != metadata.annotations[legacyRFAttemptIDAnnotation] ||
			pod.Annotations[legacyRFClusterUIDAnnotation] != metadata.annotations[legacyRFClusterUIDAnnotation] {
			continue
		}
		present = true
		if err := directClient.Delete(ctx, pod); err != nil && !k8serrors.IsNotFound(err) {
			return false, fmt.Errorf("delete correlated discovery Pod %s: %w", client.ObjectKeyFromObject(pod), err)
		}
	}
	return present, nil
}

func (manager *directDiscoveryAttempts) attemptClient(
	attempt discovery.Attempt,
) (api.LegacyRFManagedLocation, client.Client, error) {
	if len(attempt.Connection.ManagedLocations) == 0 {
		return api.LegacyRFManagedLocation{}, nil, discovery.NewBoundaryError(
			api.LegacyRFReasonKubernetesAPIUnavailable, errors.New("attempt has no discovery location"))
	}
	first := attempt.Connection.ManagedLocations[0]
	location := api.LegacyRFManagedLocation{
		K8sContext: first.K8sContext, Namespace: first.Namespace, Name: first.Name,
		DatacenterName: first.DatacenterName,
	}
	directClient, err := manager.clients.GetRemoteNonCacheClient(location.K8sContext)
	if err != nil {
		return location, nil, discovery.NewBoundaryError(api.LegacyRFReasonKubernetesAPIUnavailable,
			fmt.Errorf("resolve direct discovery client: %w", err))
	}
	return location, directClient, nil
}

func (manager *directDiscoveryAttempts) verifyWorkerImage(ctx context.Context, attempt discovery.Attempt) error {
	running, err := manager.imageResolver.Resolve(ctx)
	if err != nil {
		return discovery.NewBoundaryError(api.LegacyRFReasonWorkerImageUnavailable,
			fmt.Errorf("resolve running worker image: %w", err))
	}
	if running != attempt.WorkerImageDigest {
		return discovery.NewBoundaryError(api.LegacyRFReasonWorkerImageUnavailable,
			errors.New("attempt worker image does not match running manager digest"))
	}
	return nil
}

func (manager *directDiscoveryAttempts) copyBoundSecrets(
	ctx context.Context,
	bindings []discovery.SecretBinding,
) (map[string][]byte, error) {
	copied := make(map[string][]byte)
	for _, binding := range bindings {
		directClient, err := manager.clients.GetRemoteNonCacheClient(binding.SourceContext)
		if err != nil {
			return nil, manager.secretFailure(binding, err)
		}
		secret := &corev1.Secret{}
		key := types.NamespacedName{Namespace: binding.Namespace, Name: binding.Name}
		if err = directClient.Get(ctx, key, secret); err != nil || secret.ResourceVersion != binding.ResourceVersion {
			if err == nil {
				err = errors.New("qualified Secret resource version changed")
			}
			return nil, manager.secretFailure(binding, err)
		}
		if err = copyBindingKeys(copied, binding, secret.Data); err != nil {
			return nil, manager.secretFailure(binding, err)
		}
	}
	return copied, nil
}

func copyBindingKeys(destination map[string][]byte, binding discovery.SecretBinding, source map[string][]byte) error {
	for _, key := range binding.Keys {
		value, found := source[key]
		if !found || len(value) == 0 {
			return fmt.Errorf("required key %q is missing", key)
		}
		target, found := copiedSecretKey(binding.Purpose, key)
		if !found {
			return fmt.Errorf("unsupported key %q for purpose %q", key, binding.Purpose)
		}
		destination[target] = append([]byte(nil), value...)
	}
	return nil
}

func copiedSecretKey(purpose, key string) (string, bool) {
	known := map[string]string{
		"auth/username": legacyRFAuthUsernameKey, "auth/password": legacyRFAuthPasswordKey,
		"tls/ca.crt": legacyRFTLSCAKey, "tls/tls.crt": legacyRFTLSCertificateKey,
		"tls/tls.key": legacyRFTLSPrivateKeyKey,
	}
	value, found := known[purpose+"/"+key]
	return value, found
}

func (manager *directDiscoveryAttempts) secretFailure(binding discovery.SecretBinding, cause error) error {
	reason := api.LegacyRFReasonCredentialSecretInvalid
	if binding.Purpose == "tls" {
		reason = api.LegacyRFReasonTLSMaterialInvalid
	}
	return discovery.NewBoundaryError(reason, fmt.Errorf("read bound %s Secret: %w", binding.Purpose, cause))
}

func (manager *directDiscoveryAttempts) ensureHMACKey(
	ctx context.Context,
	directClient client.Client,
	attempt discovery.Attempt,
	location api.LegacyRFManagedLocation,
) ([]byte, error) {
	metadata := newAttemptMetadata(LegacyRFAttemptResourcesInput{ClusterKey: manager.clusterKey, Attempt: attempt, Location: location})
	key, err := manager.readHMACKey(ctx, directClient, metadata)
	if err == nil {
		return key, nil
	}
	if !k8serrors.IsNotFound(errors.Unwrap(err)) {
		return nil, err
	}
	key = make([]byte, legacyRFHMACKeyBytes)
	if _, err = io.ReadFull(manager.keyReader, key); err != nil {
		return nil, discovery.NewBoundaryError(api.LegacyRFReasonKubernetesAPIUnavailable,
			fmt.Errorf("generate attempt authentication key: %w", err))
	}
	return key, nil
}

func (manager *directDiscoveryAttempts) readHMACKey(
	ctx context.Context,
	directClient client.Reader,
	metadata attemptMetadata,
) ([]byte, error) {
	secret := &corev1.Secret{}
	key := types.NamespacedName{Namespace: metadata.namespace, Name: metadata.baseName + "-hmac"}
	if err := directClient.Get(ctx, key, secret); err != nil {
		return nil, fmt.Errorf("read discovery HMAC Secret: %w", err)
	}
	value := secret.Data[legacyRFHMACDataKey]
	if len(value) != legacyRFHMACKeyBytes {
		return nil, discovery.NewBoundaryError(api.LegacyRFReasonForgedDiscoveryResult,
			errors.New("discovery HMAC key is missing or malformed"))
	}
	return append([]byte(nil), value...), nil
}

func ensureAttemptObjects(ctx context.Context, directClient client.Client, desired []client.Object) error {
	if err := rejectMissingEstablishedResultConfigMap(ctx, directClient, desired); err != nil {
		return err
	}
	for _, object := range desired {
		current, ok := object.DeepCopyObject().(client.Object)
		if !ok {
			return fmt.Errorf("copy desired object %T", object)
		}
		err := directClient.Get(ctx, client.ObjectKeyFromObject(object), current)
		if k8serrors.IsNotFound(err) {
			if err = directClient.Create(ctx, object); err != nil && !k8serrors.IsAlreadyExists(err) {
				return fmt.Errorf("create %T %s: %w", object, client.ObjectKeyFromObject(object), err)
			}
			continue
		}
		if err != nil {
			return fmt.Errorf("read %T %s: %w", object, client.ObjectKeyFromObject(object), err)
		}
		if current.GetAnnotations()[legacyRFAttemptIDAnnotation] != object.GetAnnotations()[legacyRFAttemptIDAnnotation] ||
			current.GetAnnotations()[legacyRFClusterUIDAnnotation] != object.GetAnnotations()[legacyRFClusterUIDAnnotation] {
			return fmt.Errorf("existing %T has conflicting attempt ownership", object)
		}
		if desiredResult, ok := legacyRFResultConfigMap(object); ok {
			currentResult, currentOK := current.(*corev1.ConfigMap)
			if !currentOK {
				return fmt.Errorf("existing result ConfigMap has unexpected type %T", current)
			}
			if err = ensureStableResultConfigMap(ctx, directClient, desiredResult, currentResult); err != nil {
				return err
			}
			continue
		}
		if !attemptObjectMatchesDesired(object, current) {
			return fmt.Errorf("existing %T %s has conflicting desired state", object, client.ObjectKeyFromObject(object))
		}
	}
	return nil
}

func attemptObjectMatchesDesired(desired, current client.Object) bool {
	if !containsStringMap(current.GetLabels(), desired.GetLabels()) ||
		!containsStringMap(current.GetAnnotations(), desired.GetAnnotations()) ||
		!apiequality.Semantic.DeepDerivative(desired.GetOwnerReferences(), current.GetOwnerReferences()) ||
		!apiequality.Semantic.DeepDerivative(desired.GetFinalizers(), current.GetFinalizers()) {
		return false
	}
	switch desiredObject := desired.(type) {
	case *corev1.ConfigMap:
		currentObject, ok := current.(*corev1.ConfigMap)
		return ok && reflect.DeepEqual(desiredObject.Data, currentObject.Data) &&
			reflect.DeepEqual(desiredObject.BinaryData, currentObject.BinaryData) &&
			reflect.DeepEqual(desiredObject.Immutable, currentObject.Immutable)
	case *corev1.Secret:
		currentObject, ok := current.(*corev1.Secret)
		return ok && desiredObject.Type == currentObject.Type &&
			equalByteMaps(desiredObject.Data, currentObject.Data) &&
			reflect.DeepEqual(desiredObject.StringData, currentObject.StringData) &&
			reflect.DeepEqual(desiredObject.Immutable, currentObject.Immutable)
	case *corev1.ServiceAccount:
		currentObject, ok := current.(*corev1.ServiceAccount)
		return ok && apiequality.Semantic.DeepDerivative(desiredObject.ImagePullSecrets, currentObject.ImagePullSecrets) &&
			apiequality.Semantic.DeepDerivative(desiredObject.AutomountServiceAccountToken, currentObject.AutomountServiceAccountToken)
	case *rbacv1.Role:
		currentObject, ok := current.(*rbacv1.Role)
		return ok && apiequality.Semantic.DeepDerivative(desiredObject.Rules, currentObject.Rules)
	case *rbacv1.RoleBinding:
		currentObject, ok := current.(*rbacv1.RoleBinding)
		return ok && reflect.DeepEqual(desiredObject.RoleRef, currentObject.RoleRef) &&
			apiequality.Semantic.DeepDerivative(desiredObject.Subjects, currentObject.Subjects)
	case *batchv1.Job:
		currentObject, ok := current.(*batchv1.Job)
		return ok && apiequality.Semantic.DeepDerivative(desiredObject.Spec, currentObject.Spec)
	default:
		return false
	}
}

func equalByteMaps(left, right map[string][]byte) bool {
	return len(left) == 0 && len(right) == 0 || reflect.DeepEqual(left, right)
}

func containsStringMap(current, desired map[string]string) bool {
	for key, value := range desired {
		if current[key] != value {
			return false
		}
	}
	return true
}

func rejectMissingEstablishedResultConfigMap(
	ctx context.Context,
	directClient client.Reader,
	desired []client.Object,
) error {
	var result client.Object
	for _, object := range desired {
		if _, ok := legacyRFResultConfigMap(object); ok {
			result = object
			break
		}
	}
	if result == nil {
		return nil
	}
	current := &corev1.ConfigMap{}
	if err := directClient.Get(ctx, client.ObjectKeyFromObject(result), current); err == nil {
		return nil
	} else if !k8serrors.IsNotFound(err) {
		return fmt.Errorf("read discovery result ConfigMap: %w", err)
	}
	for _, object := range desired {
		if object == result {
			continue
		}
		current, ok := object.DeepCopyObject().(client.Object)
		if !ok {
			return fmt.Errorf("copy desired object %T", object)
		}
		if err := directClient.Get(ctx, client.ObjectKeyFromObject(object), current); err == nil {
			return errors.New("established discovery attempt is missing its result ConfigMap")
		} else if !k8serrors.IsNotFound(err) {
			return fmt.Errorf("survey discovery attempt for missing result ConfigMap: %w", err)
		}
	}
	return nil
}

func legacyRFResultConfigMap(object client.Object) (*corev1.ConfigMap, bool) {
	configMap, ok := object.(*corev1.ConfigMap)
	return configMap, ok && strings.HasSuffix(configMap.Name, "-result") && len(configMap.Data) == 0
}

func ensureStableResultConfigMap(
	ctx context.Context,
	directClient client.Client,
	desired, current *corev1.ConfigMap,
) error {
	if !reflect.DeepEqual(desired.Labels, current.Labels) ||
		!reflect.DeepEqual(desired.OwnerReferences, current.OwnerReferences) ||
		!reflect.DeepEqual(desired.Finalizers, current.Finalizers) ||
		!reflect.DeepEqual(desired.Immutable, current.Immutable) || len(current.BinaryData) != 0 {
		return errors.New("existing result ConfigMap has conflicting metadata or binary data")
	}
	for key, value := range desired.Annotations {
		if current.Annotations[key] != value {
			return errors.New("existing result ConfigMap has conflicting attempt binding")
		}
	}
	for key := range current.Annotations {
		if _, expected := desired.Annotations[key]; !expected &&
			key != legacyRFFailureReasonAnnotation && key != legacyRFResultDigestAnnotation {
			return errors.New("existing result ConfigMap has foreign annotations")
		}
	}
	if len(current.Data) == 0 {
		if current.Annotations[legacyRFResultDigestAnnotation] != "" {
			return errors.New("existing result ConfigMap lost its sealed payload")
		}
		return nil
	}
	payload, found := current.Data[legacyRFResultDataKey]
	if !found || len(current.Data) != 1 || payload == "" || len(payload) > api.LegacyRFDiscoveryMaxResultBytes {
		return errors.New("existing result ConfigMap has invalid worker data")
	}
	digestBytes := sha256.Sum256([]byte(payload))
	digest := hex.EncodeToString(digestBytes[:])
	sealed := current.Annotations[legacyRFResultDigestAnnotation]
	if sealed != "" {
		if sealed != digest {
			return errors.New("existing result ConfigMap payload conflicts with sealed result")
		}
		return nil
	}
	base := current.DeepCopy()
	if current.Annotations == nil {
		current.Annotations = map[string]string{}
	}
	current.Annotations[legacyRFResultDigestAnnotation] = digest
	if err := directClient.Patch(ctx, current, client.MergeFrom(base)); err != nil {
		return fmt.Errorf("seal discovery result ConfigMap payload: %w", err)
	}
	return nil
}

func (manager *directDiscoveryAttempts) jobState(
	ctx context.Context,
	directClient client.Client,
	desired *batchv1.Job,
) (DiscoveryAttemptState, error) {
	job := &batchv1.Job{}
	if err := directClient.Get(ctx, client.ObjectKeyFromObject(desired), job); err != nil {
		return DiscoveryAttemptState{}, manager.apiFailure("read discovery Job", err)
	}
	if job.Status.Succeeded > 0 {
		return DiscoveryAttemptState{Phase: DiscoveryAttemptComplete}, nil
	}
	if failure := classifyJobFailure(ctx, directClient, job); failure != nil {
		return DiscoveryAttemptState{}, failure
	}
	return DiscoveryAttemptState{Phase: DiscoveryAttemptPending, RequeueAfter: manager.backoff.Delay(job.Status.Failed)}, nil
}

func classifyJobFailure(ctx context.Context, reader client.Reader, job *batchv1.Job) error {
	for _, condition := range job.Status.Conditions {
		if condition.Type == batchv1.JobFailed && condition.Status == corev1.ConditionTrue {
			reason := api.LegacyRFReasonJobSchedulingFailed
			if condition.Reason == "DeadlineExceeded" {
				reason = api.LegacyRFReasonDiscoveryDeadlineExceeded
			}
			return discovery.NewBoundaryError(reason, errors.New("discovery Job failed"))
		}
	}
	pods := &corev1.PodList{}
	if err := reader.List(ctx, pods, client.InNamespace(job.Namespace), client.MatchingLabels{"job-name": job.Name}); err != nil {
		return discovery.NewBoundaryError(api.LegacyRFReasonKubernetesAPIUnavailable,
			fmt.Errorf("list discovery Job pods: %w", err))
	}
	return classifyPodFailures(pods.Items)
}

func classifyPodFailures(pods []corev1.Pod) error {
	for _, pod := range pods {
		for _, status := range pod.Status.ContainerStatuses {
			if status.State.Waiting != nil &&
				(status.State.Waiting.Reason == "ImagePullBackOff" || status.State.Waiting.Reason == "ErrImagePull") {
				return discovery.NewBoundaryError(api.LegacyRFReasonWorkerImagePullFailed,
					errors.New("discovery worker image pull failed"))
			}
		}
		for _, condition := range pod.Status.Conditions {
			if condition.Type == corev1.PodScheduled && condition.Status == corev1.ConditionFalse && condition.Reason == "Unschedulable" {
				return discovery.NewBoundaryError(api.LegacyRFReasonJobSchedulingFailed,
					errors.New("discovery worker is unschedulable"))
			}
		}
	}
	return nil
}

func expectedAttemptResult(attempt discovery.Attempt) discovery.ExpectedResult {
	return discovery.ExpectedResult{
		SchemaVersion: attempt.ProtocolVersion, ClusterUID: attempt.ClusterUID,
		Generation: attempt.Generation, MarkerVersion: attempt.MarkerVersion,
		ProtocolVersion: attempt.ProtocolVersion, AttemptID: attempt.AttemptID,
		OrderedSeeds: attempt.OrderedSeeds, SeedDigest: attempt.SeedDigest,
		SecretBindings: attempt.Connection.SecretBindings, WorkerImageDigest: attempt.WorkerImageDigest,
		MaximumBytes: api.LegacyRFDiscoveryMaxResultBytes,
	}
}

func (manager *directDiscoveryAttempts) recordFailureByKey(
	ctx context.Context,
	directClient client.Client,
	key types.NamespacedName,
	err error,
) error {
	resultConfigMap := &corev1.ConfigMap{}
	if getErr := directClient.Get(ctx, key, resultConfigMap); getErr != nil {
		return manager.apiFailure("read discovery failure transition", getErr)
	}
	return manager.recordFailureTransition(ctx, directClient, resultConfigMap, err)
}

func (manager *directDiscoveryAttempts) recordFailureTransition(
	ctx context.Context,
	directClient client.Client,
	resultConfigMap *corev1.ConfigMap,
	err error,
) error {
	var boundary *discovery.BoundaryError
	if !errors.As(err, &boundary) {
		return nil
	}
	failure := boundary.PublicFailure()
	if resultConfigMap.Annotations[legacyRFFailureReasonAnnotation] == string(failure.Reason) {
		return nil
	}
	base := resultConfigMap.DeepCopy()
	if resultConfigMap.Annotations == nil {
		resultConfigMap.Annotations = map[string]string{}
	}
	resultConfigMap.Annotations[legacyRFFailureReasonAnnotation] = string(failure.Reason)
	if patchErr := directClient.Patch(ctx, resultConfigMap, client.MergeFrom(base)); patchErr != nil {
		return manager.apiFailure("record discovery failure transition", patchErr)
	}
	manager.recorder.Event(resultConfigMap, corev1.EventTypeWarning, string(failure.Reason), failure.Message)
	return nil
}

func (manager *directDiscoveryAttempts) apiFailure(operation string, cause error) error {
	reason := api.LegacyRFReasonKubernetesAPIUnavailable
	if k8serrors.IsConflict(cause) || k8serrors.IsAlreadyExists(cause) {
		reason = api.LegacyRFReasonKubernetesAPIConflict
	}
	return discovery.NewBoundaryError(reason, fmt.Errorf("%s: %w", operation, cause))
}

func cleanupObjects(metadata attemptMetadata) []client.Object {
	objectMeta := func(suffix string) metav1.ObjectMeta {
		return metav1.ObjectMeta{Name: metadata.baseName + suffix, Namespace: metadata.namespace,
			Annotations: copyStringMap(metadata.annotations)}
	}
	return []client.Object{
		&batchv1.Job{ObjectMeta: objectMeta("")},
		&corev1.ConfigMap{ObjectMeta: objectMeta("-attempt")},
		&corev1.ConfigMap{ObjectMeta: objectMeta("-result")},
		&corev1.Secret{ObjectMeta: objectMeta("-hmac")},
		&corev1.Secret{ObjectMeta: objectMeta("-source")},
		&corev1.ServiceAccount{ObjectMeta: objectMeta("-worker")},
		&rbacv1.RoleBinding{ObjectMeta: objectMeta("-writer")},
		&rbacv1.Role{ObjectMeta: objectMeta("-writer")},
	}
}

func deleteCorrelatedObject(ctx context.Context, directClient client.Client, desired client.Object) (bool, error) {
	current, ok := desired.DeepCopyObject().(client.Object)
	if !ok {
		return false, fmt.Errorf("copy cleanup object %T", desired)
	}
	if err := directClient.Get(ctx, client.ObjectKeyFromObject(desired), current); err != nil {
		if k8serrors.IsNotFound(err) {
			return false, nil
		}
		return false, fmt.Errorf("read cleanup object %T: %w", desired, err)
	}
	if current.GetAnnotations()[legacyRFAttemptIDAnnotation] != desired.GetAnnotations()[legacyRFAttemptIDAnnotation] ||
		current.GetAnnotations()[legacyRFClusterUIDAnnotation] != desired.GetAnnotations()[legacyRFClusterUIDAnnotation] {
		return false, fmt.Errorf("refuse to delete uncorrelated %T", desired)
	}
	if err := directClient.Delete(ctx, current); err != nil && !k8serrors.IsNotFound(err) {
		return false, fmt.Errorf("delete correlated %T: %w", desired, err)
	}
	return true, nil
}
