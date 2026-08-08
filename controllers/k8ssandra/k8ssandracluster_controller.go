/*
Copyright 2021.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package k8ssandra

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	stderrors "errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"

	"github.com/go-logr/logr"
	cassdcapi "github.com/k8ssandra/cass-operator/apis/cassandra/v1beta1"
	cassimages "github.com/k8ssandra/cass-operator/pkg/images"
	api "github.com/k8ssandra/k8ssandra-operator/apis/k8ssandra/v1alpha1"
	medusaapi "github.com/k8ssandra/k8ssandra-operator/apis/medusa/v1alpha1"
	reaperapi "github.com/k8ssandra/k8ssandra-operator/apis/reaper/v1alpha1"
	stargateapi "github.com/k8ssandra/k8ssandra-operator/apis/stargate/v1alpha1"
	"github.com/k8ssandra/k8ssandra-operator/pkg/cassandra"
	"github.com/k8ssandra/k8ssandra-operator/pkg/clientcache"
	"github.com/k8ssandra/k8ssandra-operator/pkg/config"
	"github.com/k8ssandra/k8ssandra-operator/pkg/discovery"
	"github.com/k8ssandra/k8ssandra-operator/pkg/labels"
	"github.com/k8ssandra/k8ssandra-operator/pkg/result"
	"github.com/k8ssandra/k8ssandra-operator/pkg/utils"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/cluster"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"
)

type legacyRFControllerRuntime struct {
	clients   *clientcache.ClientCache
	control   LegacyRFControlPlane
	managed   ManagedDatacenterState
	image     ManagerImageResolver
	clock     discovery.Clock
	keyReader io.Reader
	recorder  legacyRFEventSink
	backoff   DiscoveryBackoff
}

const (
	legacyRFCQLCredentialsSecretRefIndex = ".spec.cassandra.legacyCqlCredentialsSecretRef.name"
	legacyRFCQLTLSSecretRefIndex         = ".spec.cassandra.legacyCqlTLSSecretRef.name"
)

// NewLegacyRFControllerIntegration validates and constructs all discovery boundaries.
func NewLegacyRFControllerIntegration(clients *clientcache.ClientCache, image ManagerImageResolver, clock discovery.Clock, keyReader io.Reader, recorder legacyRFEventSink, backoff DiscoveryBackoff) (LegacyRFControllerIntegration, error) {
	if clients == nil || image == nil || clock == nil || keyReader == nil || recorder == nil || backoff == nil {
		return nil, stderrors.New("create legacy RF controller integration: every dependency is required")
	}
	control, err := NewLegacyRFControlPlane(clients.GetLocalNonCacheClient())
	if err != nil {
		return nil, err
	}
	managed, err := NewManagedDatacenterState(clients)
	if err != nil {
		return nil, err
	}
	return &legacyRFControllerRuntime{clients: clients, control: control, managed: managed, image: image, clock: clock, keyReader: keyReader, recorder: recorder, backoff: backoff}, nil
}

// indexK8ssandraClusterByMedusaConfigRef registers a field index on K8ssandraCluster so that
// the controller can efficiently query which clusters reference a given MedusaConfiguration.
func indexK8ssandraClusterByMedusaConfigRef(ctx context.Context, mgr ctrl.Manager) error {
	return mgr.GetFieldIndexer().IndexField(
		ctx,
		&api.K8ssandraCluster{},
		MedusaConfigurationRefIndex,
		func(obj client.Object) []string {
			kc := obj.(*api.K8ssandraCluster)
			if kc.Spec.Medusa == nil || kc.Spec.Medusa.MedusaConfigurationRef.Name == "" {
				return nil
			}
			return []string{kc.Spec.Medusa.MedusaConfigurationRef.Name}
		},
	)
}

func indexK8ssandraClusterByLegacyRFSecretRefs(ctx context.Context, mgr ctrl.Manager) error {
	if err := mgr.GetFieldIndexer().IndexField(ctx, &api.K8ssandraCluster{}, legacyRFCQLCredentialsSecretRefIndex,
		legacyRFCQLCredentialsSecretRefValues); err != nil {
		return err
	}
	return mgr.GetFieldIndexer().IndexField(ctx, &api.K8ssandraCluster{}, legacyRFCQLTLSSecretRefIndex,
		legacyRFCQLTLSSecretRefValues)
}

func legacyRFCQLCredentialsSecretRefValues(object client.Object) []string {
	cluster := object.(*api.K8ssandraCluster)
	if cluster.Spec.Cassandra == nil || cluster.Spec.Cassandra.LegacyCqlCredentialsSecretRef == nil {
		return nil
	}
	return []string{cluster.Spec.Cassandra.LegacyCqlCredentialsSecretRef.Name}
}

func legacyRFCQLTLSSecretRefValues(object client.Object) []string {
	cluster := object.(*api.K8ssandraCluster)
	if cluster.Spec.Cassandra == nil || cluster.Spec.Cassandra.LegacyCqlTLSSecretRef == nil {
		return nil
	}
	return []string{cluster.Spec.Cassandra.LegacyCqlTLSSecretRef.Name}
}

// K8ssandraClusterReconciler reconciles a K8ssandraCluster object
type K8ssandraClusterReconciler struct {
	*config.ReconcilerConfig
	client.Client
	Scheme        *runtime.Scheme
	ClientCache   *clientcache.ClientCache
	ManagementApi cassandra.ManagementApiFactory
	Recorder      events.EventRecorder
	// ImageRegistry provides access to container image settings loaded from ImageConfig (v1beta2).
	ImageRegistry cassimages.ImageRegistry
	// LegacyRFDiscovery gates marked migrations and authorizes their managed DC creation.
	LegacyRFDiscovery LegacyRFControllerIntegration
}

// LegacyRFControllerIntegration is the narrow aggregate-controller boundary for
// pre-create discovery and final managed datacenter authorization.
type LegacyRFControllerIntegration interface {
	ReconcileGate(context.Context, *api.K8ssandraCluster, logr.Logger) result.ReconcileResult
	AuthorizeManagedCreation(context.Context, *api.K8ssandraCluster, api.LegacyRFManagedLocation, LegacyRFManagedCreateCallback) error
}

// +kubebuilder:rbac:groups=k8ssandra.io,namespace="k8ssandra",resources=k8ssandraclusters;clientconfigs,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=config.k8ssandra.io,namespace="k8ssandra",resources=clientconfigs,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=k8ssandra.io,namespace="k8ssandra",resources=k8ssandraclusters/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=k8ssandra.io,namespace="k8ssandra",resources=k8ssandraclusters/finalizers,verbs=update
// +kubebuilder:rbac:groups=cassandra.datastax.com,namespace="k8ssandra",resources=cassandradatacenters,verbs=get;list;watch;create;update;delete;patch
// +kubebuilder:rbac:groups=control.k8ssandra.io,namespace="k8ssandra",resources=cassandratasks,verbs=get;list;watch;create;update;delete;patch
// +kubebuilder:rbac:groups=stargate.k8ssandra.io,namespace="k8ssandra",resources=stargates,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=reaper.k8ssandra.io,namespace="k8ssandra",resources=reapers,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core,namespace="k8ssandra",resources=pods,verbs=get;list;watch;delete
// +kubebuilder:rbac:groups=core,namespace="k8ssandra",resources=secrets,verbs=get;list;watch
// +kubebuilder:rbac:groups=core,namespace="k8ssandra",resources=endpoints;endpoints/restricted,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=discovery.k8s.io,namespace="k8ssandra",resources=endpointslices;endpointslices/restricted,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=monitoring.coreos.com,namespace="k8ssandra",resources=servicemonitors,verbs=get;list;watch;create;update;patch;delete;deletecollection
// +kubebuilder:rbac:groups=core,namespace="k8ssandra",resources=configmaps,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core,namespace="k8ssandra",resources=serviceaccounts,verbs=get;list;watch;create;delete
// +kubebuilder:rbac:groups="",namespace="k8ssandra",resources=events,verbs=create;patch
// +kubebuilder:rbac:groups=events.k8s.io,namespace="k8ssandra",resources=events,verbs=create;patch;update
// +kubebuilder:rbac:groups=batch,namespace="k8ssandra",resources=cronjobs,verbs=get;list;watch;create;update;delete
// +kubebuilder:rbac:groups=batch,namespace="k8ssandra",resources=jobs,verbs=get;list;watch;create;delete
// +kubebuilder:rbac:groups=rbac.authorization.k8s.io,namespace="k8ssandra",resources=roles;rolebindings,verbs=get;list;watch;create;delete

func (r *K8ssandraClusterReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx).WithValues("K8ssandraCluster", req.NamespacedName)

	kc := &api.K8ssandraCluster{}
	err := r.Get(ctx, req.NamespacedName, kc)
	if err != nil {
		if errors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{RequeueAfter: r.DefaultDelay}, err
	}

	kc = kc.DeepCopy()
	patch := client.MergeFrom(kc.DeepCopy())
	result, err := r.reconcile(ctx, kc, logger)
	if kc.GetDeletionTimestamp() == nil {
		if err != nil {
			kc.Status.Error = err.Error()
			r.Recorder.Eventf(kc, nil, corev1.EventTypeWarning, "ReconcileError", "ReconcileError", err.Error())
		} else {
			kc.Status.Error = "None"
		}
		if patchErr := r.Status().Patch(ctx, kc, patch); patchErr != nil {
			logger.Error(patchErr, "failed to update k8ssandracluster status")
		} else {
			logger.Info("updated k8ssandracluster status")
		}
	}
	return result, err
}

func (r *K8ssandraClusterReconciler) reconcile(ctx context.Context, kc *api.K8ssandraCluster, kcLogger logr.Logger) (ctrl.Result, error) {
	if recResult := r.checkDeletion(ctx, kc, kcLogger); recResult.Completed() {
		return recResult.Output()
	}

	if recResult := r.checkFinalizer(ctx, kc, kcLogger); recResult.Completed() {
		return recResult.Output()
	}

	if err := validateK8ssandraCluster(*kc); err != nil {
		return reconcile.Result{}, err
	}

	if kc.Spec.Cassandra == nil {
		// TODO handle the scenario of CassandraClusterTemplate being set to nil after having a non-nil value
		return ctrl.Result{}, nil
	}

	if r.LegacyRFDiscovery != nil {
		if recResult := r.LegacyRFDiscovery.ReconcileGate(ctx, kc, kcLogger); recResult.Completed() {
			return recResult.Output()
		}
	}

	// Reconcile the ReplicatedSecret and superuserSecret first (otherwise CassandraDatacenter will not start)

	if recResult := r.reconcileSuperuserSecret(ctx, kc, kcLogger); recResult.Completed() {
		return recResult.Output()
	}

	if recResult := r.reconcileReaperSecrets(ctx, kc, kcLogger); recResult.Completed() {
		return recResult.Output()
	}

	if medusaSecretResult := r.reconcileMedusaSecrets(ctx, kc, kcLogger); medusaSecretResult.Completed() {
		return medusaSecretResult.Output()
	}

	kcLogger.Info("Reconciling replicated secrets")

	if recResult := r.reconcileReplicatedSecret(ctx, kc, kcLogger); recResult.Completed() {
		return recResult.Output()
	}

	if medusaRecResult := r.reconcileMedusaReplicatedSecret(ctx, kc, kcLogger); medusaRecResult.Completed() {
		return medusaRecResult.Output()
	}

	var actualDcs []*cassdcapi.CassandraDatacenter
	if recResult, dcs := r.reconcileDatacenters(ctx, kc, kcLogger); recResult.Completed() {
		return recResult.Output()
	} else {
		actualDcs = dcs
	}

	kcLogger.Info("All DCs reconciled")

	if recResult := r.afterCassandraReconciled(ctx, kc, actualDcs, kcLogger); recResult.Completed() {
		return recResult.Output()
	}

	if res := updateStatus(ctx, r.Client, kc); res.Completed() {
		return res.Output()
	}

	kcLogger.Info("Finished reconciling the k8ssandracluster")

	return result.Done().Output()
}

func (r *K8ssandraClusterReconciler) afterCassandraReconciled(ctx context.Context, kc *api.K8ssandraCluster, dcs []*cassdcapi.CassandraDatacenter, logger logr.Logger) result.ReconcileResult {
	for i, dcTemplate := range kc.Spec.Cassandra.Datacenters {
		dc := dcs[i]
		dcKey := utils.GetKey(dc)
		logger := logger.WithValues("CassandraDatacenter", dcKey)
		logger.Info("Reconciling Stargate and Reaper for dc " + dc.DatacenterName())
		if remoteClient, err := r.ClientCache.GetRemoteClient(dcTemplate.K8sContext); err != nil {
			logger.Error(err, "Failed to get remote client")
			return result.Error(err)
		} else if recResult := r.reconcileCassandraDCTelemetry(ctx, kc, dcTemplate, dc, logger, remoteClient); recResult.Completed() {
			return recResult
		} else if recResult := r.reconcileStargate(ctx, kc, dcTemplate, dc, logger, remoteClient); recResult.Completed() {
			return recResult
		} else if recResult := r.reconcileReaper(ctx, kc, dcTemplate, dc, logger, remoteClient); recResult.Completed() {
			return recResult
		}
	}
	return result.Continue()
}

func updateStatus(ctx context.Context, r client.Client, kc *api.K8ssandraCluster) result.ReconcileResult {
	if AllowUpdate(kc) {
		if metav1.HasAnnotation(kc.ObjectMeta, api.AutomatedUpdateAnnotation) {
			if kc.Annotations[api.AutomatedUpdateAnnotation] == string(api.AllowUpdateOnce) {
				delete(kc.Annotations, api.AutomatedUpdateAnnotation)
				if err := r.Update(ctx, kc); err != nil {
					return result.Error(err)
				}
			}
		}
		kc.Status.SetConditionStatus(api.ClusterRequiresUpdate, corev1.ConditionFalse)
	}

	kc.Status.ObservedGeneration = kc.Generation
	if err := r.Status().Update(ctx, kc); err != nil {
		return result.Error(err)
	}

	return result.Continue()
}

// SetupWithManager sets up the controller with the Manager.
func (r *K8ssandraClusterReconciler) SetupWithManager(ctx context.Context, mgr ctrl.Manager, clusters []cluster.Cluster) error {
	if err := indexK8ssandraClusterByMedusaConfigRef(ctx, mgr); err != nil {
		return err
	}
	if err := indexK8ssandraClusterByLegacyRFSecretRefs(ctx, mgr); err != nil {
		return err
	}
	cb := ctrl.NewControllerManagedBy(mgr).
		For(&api.K8ssandraCluster{}, builder.WithPredicates(predicate.Or(predicate.GenerationChangedPredicate{}, predicate.AnnotationChangedPredicate{})))

	clusterLabelFilter := func(_ context.Context, mapObj client.Object) []reconcile.Request {
		return legacyRFOwnerRequests(mapObj)
	}

	// Use a more specific filter for Endpoints because we are only interested in one particular service.
	endpointsFilter := func(ctx context.Context, mapObj client.Object) []reconcile.Request {
		requests := make([]reconcile.Request, 0)

		kcName := labels.GetLabel(mapObj, api.K8ssandraClusterNameLabel)
		kcNamespace := labels.GetLabel(mapObj, api.K8ssandraClusterNamespaceLabel)

		if kcName != "" && kcNamespace != "" && strings.HasSuffix(mapObj.GetName(), "all-pods-service") {
			requests = append(requests, reconcile.Request{NamespacedName: types.NamespacedName{Namespace: kcNamespace, Name: kcName}})
		}
		return requests
	}

	// When a MedusaConfiguration changes, find all K8ssandraCluster objects that reference it by name via the field index and enqueue them for reconciliation.
	medusaConfigFilter := func(ctx context.Context, obj client.Object) []reconcile.Request {
		kcList := &api.K8ssandraClusterList{}
		if err := r.List(ctx, kcList,
			client.InNamespace(obj.GetNamespace()),
			client.MatchingFields{MedusaConfigurationRefIndex: obj.GetName()},
		); err != nil {
			return nil
		}
		requests := make([]reconcile.Request, len(kcList.Items))
		for i, kc := range kcList.Items {
			requests[i] = reconcile.Request{NamespacedName: types.NamespacedName{
				Namespace: kc.Namespace,
				Name:      kc.Name,
			}}
		}
		return requests
	}

	cb = cb.Watches(&cassdcapi.CassandraDatacenter{},
		handler.EnqueueRequestsFromMapFunc(clusterLabelFilter))
	cb = cb.Watches(&stargateapi.Stargate{},
		handler.EnqueueRequestsFromMapFunc(clusterLabelFilter))
	cb = cb.Watches(&reaperapi.Reaper{},
		handler.EnqueueRequestsFromMapFunc(clusterLabelFilter))
	cb = cb.Watches(&corev1.ConfigMap{},
		handler.EnqueueRequestsFromMapFunc(clusterLabelFilter))
	cb = cb.Watches(&discoveryv1.EndpointSlice{},
		handler.EnqueueRequestsFromMapFunc(endpointsFilter))
	cb = cb.Watches(&medusaapi.MedusaConfiguration{},
		handler.EnqueueRequestsFromMapFunc(medusaConfigFilter), builder.WithPredicates(predicate.GenerationChangedPredicate{}))

	for _, c := range clusters {
		cb = cb.WatchesRawSource(source.Kind(c.GetCache(), &cassdcapi.CassandraDatacenter{},
			handler.TypedEnqueueRequestsFromMapFunc(func(ctx context.Context, obj *cassdcapi.CassandraDatacenter) []reconcile.Request {
				return clusterLabelFilter(ctx, obj)
			})))
		cb = cb.WatchesRawSource(source.Kind(c.GetCache(), &stargateapi.Stargate{},
			handler.TypedEnqueueRequestsFromMapFunc(func(ctx context.Context, obj *stargateapi.Stargate) []reconcile.Request {
				return clusterLabelFilter(ctx, obj)
			})))
		cb = cb.WatchesRawSource(source.Kind(c.GetCache(), &reaperapi.Reaper{},
			handler.TypedEnqueueRequestsFromMapFunc(func(ctx context.Context, obj *reaperapi.Reaper) []reconcile.Request {
				return clusterLabelFilter(ctx, obj)
			})))
		cb = cb.WatchesRawSource(source.Kind(c.GetCache(), &corev1.ConfigMap{},
			handler.TypedEnqueueRequestsFromMapFunc(func(ctx context.Context, obj *corev1.ConfigMap) []reconcile.Request {
				return clusterLabelFilter(ctx, obj)
			})))
		cb = cb.WatchesRawSource(source.Kind(c.GetCache(), &discoveryv1.EndpointSlice{},
			handler.TypedEnqueueRequestsFromMapFunc(func(ctx context.Context, obj *discoveryv1.EndpointSlice) []reconcile.Request {
				return clusterLabelFilter(ctx, obj)
			})))
	}
	return r.addLegacyRFDiscoveryWatches(cb, clusters).Complete(r)
}

func (r *K8ssandraClusterReconciler) addLegacyRFDiscoveryWatches(
	controller *builder.Builder,
	clusters []cluster.Cluster,
) *builder.Builder {
	controller = controller.Watches(&batchv1.Job{},
		handler.EnqueueRequestsFromMapFunc(func(_ context.Context, object client.Object) []reconcile.Request {
			return legacyRFOwnerRequests(object)
		}))
	controller = controller.Watches(&corev1.Secret{},
		handler.EnqueueRequestsFromMapFunc(func(ctx context.Context, object client.Object) []reconcile.Request {
			requests := legacyRFOwnerRequests(object)
			return appendUniqueRequests(requests, legacyRFReferencedSecretRequests(ctx, r.Client, object))
		}))
	for _, dataPlane := range clusters {
		controller = controller.WatchesRawSource(source.Kind(dataPlane.GetCache(), &batchv1.Job{},
			handler.TypedEnqueueRequestsFromMapFunc(func(_ context.Context, object *batchv1.Job) []reconcile.Request {
				return legacyRFOwnerRequests(object)
			})))
		controller = controller.WatchesRawSource(source.Kind(dataPlane.GetCache(), &corev1.Secret{},
			handler.TypedEnqueueRequestsFromMapFunc(func(_ context.Context, object *corev1.Secret) []reconcile.Request {
				return legacyRFOwnerRequests(object)
			})))
	}
	return controller
}

func legacyRFOwnerRequests(object client.Object) []reconcile.Request {
	name := labels.GetLabel(object, api.K8ssandraClusterNameLabel)
	namespace := labels.GetLabel(object, api.K8ssandraClusterNamespaceLabel)
	if name == "" || namespace == "" {
		return nil
	}
	return []reconcile.Request{{NamespacedName: types.NamespacedName{Namespace: namespace, Name: name}}}
}

func legacyRFReferencedSecretRequests(ctx context.Context, reader client.Reader, object client.Object) []reconcile.Request {
	requests := make([]reconcile.Request, 0, 1)
	for _, field := range []string{legacyRFCQLCredentialsSecretRefIndex, legacyRFCQLTLSSecretRefIndex} {
		clusters := &api.K8ssandraClusterList{}
		if err := reader.List(ctx, clusters, client.InNamespace(object.GetNamespace()), client.MatchingFields{field: object.GetName()}); err != nil {
			log.FromContext(ctx).Error(err, "Failed to map legacy RF Secret reference", "field", field,
				"namespace", object.GetNamespace(), "secret", object.GetName())
			continue
		}
		for index := range clusters.Items {
			cluster := &clusters.Items[index]
			requests = appendUniqueRequests(requests, []reconcile.Request{{NamespacedName: types.NamespacedName{Namespace: cluster.Namespace, Name: cluster.Name}}})
		}
	}
	return requests
}

func appendUniqueRequests(current, additional []reconcile.Request) []reconcile.Request {
	seen := make(map[types.NamespacedName]struct{}, len(current)+len(additional))
	for _, request := range current {
		seen[request.NamespacedName] = struct{}{}
	}
	for _, request := range additional {
		if _, found := seen[request.NamespacedName]; !found {
			current = append(current, request)
			seen[request.NamespacedName] = struct{}{}
		}
	}
	return current
}

func (runtime *legacyRFControllerRuntime) ReconcileGate(ctx context.Context, cluster *api.K8ssandraCluster, _ logr.Logger) result.ReconcileResult {
	previousStatus := cluster.Status.LegacyRFDiscovery
	retryBlockedAttempt := false
	if previousStatus != nil && previousStatus.Phase == api.LegacyRFDiscoveryPhaseBlocked {
		failure, found := api.LegacyRFDiscoveryFailureForReason(previousStatus.Reason)
		if !found || !failure.Retryable {
			return result.Done()
		}
		retryBlockedAttempt = true
	}
	decision := DecideLegacyRFDiscovery(cluster, false, nil)
	if decision.Phase == api.LegacyRFDiscoveryPhaseNotRequired {
		if decision.Reason != "" {
			ApplyLegacyRFDiscoveryDecision(cluster, decision, metav1.NewTime(runtime.clock.Now()), runtime.recorder)
		}
		if retryBlockedAttempt {
			return result.Done()
		}
		return result.Continue()
	}
	if decision.Phase == api.LegacyRFDiscoveryPhaseAccepted {
		return runtime.reconcileAccepted(ctx, cluster)
	}
	if decision.Phase == api.LegacyRFDiscoveryPhaseBlocked {
		return runtime.stop(cluster, decision)
	}
	attempt, err := runtime.buildAttempt(ctx, cluster)
	if err != nil {
		return runtime.stopAttempt(cluster, decision, err)
	}
	attempts, err := NewDiscoveryAttempts(runtime.clients, client.ObjectKeyFromObject(cluster), runtime.image, runtime.keyReader, runtime, runtime.recorder, runtime.backoff)
	if err != nil {
		return result.Error(err)
	}
	if retryBlockedAttempt {
		failedAttempt := legacyRFFailedAttemptForCleanup(attempt, previousStatus)
		cleanupComplete, cleanupErr := attempts.Cleanup(ctx, failedAttempt)
		if cleanupErr != nil {
			return runtime.stopAttempt(cluster, decision, cleanupErr)
		}
		if !cleanupComplete {
			return runtime.waitForRetryCleanup(cluster, previousStatus.Reason, previousStatus.ObservedGeneration)
		}
	}
	state, err := attempts.Ensure(ctx, attempt)
	if err != nil {
		return runtime.stopAttempt(cluster, decision, err)
	}
	if state.Phase != DiscoveryAttemptComplete {
		ApplyLegacyRFDiscoveryDecision(cluster, decision, metav1.NewTime(runtime.clock.Now()), runtime.recorder)
		return result.RequeueSoon(state.RequeueAfter)
	}
	discovered, err := attempts.Result(ctx, attempt)
	if err != nil || discovered == nil {
		return runtime.stopAttempt(cluster, decision, err)
	}
	snapshot, err := legacyRFSnapshotFromResult(cluster, attempt, *discovered, runtime.clock.Now())
	if err != nil {
		return runtime.stopAttempt(cluster, decision, err)
	}
	_, err = AcceptLegacyRFSnapshot(ctx, runtime.control, runtime.managed, LegacyRFAcceptanceInput{Key: client.ObjectKeyFromObject(cluster), ExpectedUID: cluster.UID, Attempt: attempt, Result: *discovered, Snapshot: snapshot, Secrets: runtime})
	if err != nil {
		return runtime.stopAttempt(cluster, decision, err)
	}
	return result.Done()
}

func (runtime *legacyRFControllerRuntime) reconcileAccepted(
	ctx context.Context,
	cluster *api.K8ssandraCluster,
) result.ReconcileResult {
	status := cluster.Status.LegacyRFDiscovery
	accepted, err := ReadBackLegacyRFAcceptance(ctx, runtime.clients.GetLocalNonCacheClient(),
		client.ObjectKeyFromObject(cluster), cluster.UID, status.SnapshotHash)
	if err != nil {
		return runtime.requeue(cluster, true)
	}
	cleanupComplete, err := runtime.cleanupAcceptedAttempt(ctx, accepted)
	if err != nil {
		return runtime.requeue(cluster, true)
	}
	if !cleanupComplete {
		return runtime.requeue(cluster, false)
	}
	return result.Continue()
}

func (runtime *legacyRFControllerRuntime) stopAttempt(
	cluster *api.K8ssandraCluster,
	pending LegacyRFDiscoveryDecision,
	err error,
) result.ReconcileResult {
	decision := legacyRFErrorDecision(err)
	decision.AttemptLocations = pending.AttemptLocations
	return runtime.stop(cluster, decision)
}

func (runtime *legacyRFControllerRuntime) stop(cluster *api.K8ssandraCluster, decision LegacyRFDiscoveryDecision) result.ReconcileResult {
	ApplyLegacyRFDiscoveryDecision(cluster, decision, metav1.NewTime(runtime.clock.Now()), runtime.recorder)
	if decision.Retryable {
		return runtime.requeue(cluster, true)
	}
	return result.Done()
}

func (runtime *legacyRFControllerRuntime) requeue(cluster *api.K8ssandraCluster, newFailure bool) result.ReconcileResult {
	failures := int32(0)
	if cluster.Status.LegacyRFDiscovery != nil {
		failures = cluster.Status.LegacyRFDiscovery.RetryCount
		if newFailure {
			failures++
			cluster.Status.LegacyRFDiscovery.RetryCount = failures
		}
	}
	if failures > 0 {
		failures--
	}
	return result.RequeueSoon(runtime.backoff.Delay(failures))
}

func (runtime *legacyRFControllerRuntime) waitForRetryCleanup(
	cluster *api.K8ssandraCluster,
	reason api.LegacyRFDiscoveryReason,
	failedGeneration int64,
) result.ReconcileResult {
	decision := legacyRFFailureDecision(reason)
	ApplyLegacyRFDiscoveryDecision(cluster, decision, metav1.NewTime(runtime.clock.Now()), runtime.recorder)
	cluster.Status.LegacyRFDiscovery.ObservedGeneration = failedGeneration
	return runtime.requeue(cluster, false)
}

func (runtime *legacyRFControllerRuntime) buildAttempt(ctx context.Context, cluster *api.K8ssandraCluster) (discovery.Attempt, error) {
	seeds, digest, err := discovery.CanonicalizeSeeds(cluster.Spec.Cassandra.AdditionalSeeds)
	if err != nil {
		return discovery.Attempt{}, discovery.NewBoundaryError(api.LegacyRFReasonInvalidContactPoint, err)
	}
	plan, err := BuildLegacyRFCurrentPlan(cluster)
	if err != nil {
		return discovery.Attempt{}, err
	}
	bindings, err := runtime.credentialBindings(ctx, cluster)
	if err != nil {
		return discovery.Attempt{}, err
	}
	image, err := runtime.image.Resolve(ctx)
	if err != nil {
		return discovery.Attempt{}, discovery.NewBoundaryError(api.LegacyRFReasonWorkerImageUnavailable, err)
	}
	locations := make([]discovery.ManagedLocation, len(plan.ManagedLocations))
	for i, location := range plan.ManagedLocations {
		locations[i] = discovery.ManagedLocation{
			K8sContext: location.K8sContext, Namespace: location.Namespace, Name: location.Name,
			DatacenterName: location.DatacenterName,
		}
	}
	return discovery.Attempt{ClusterUID: string(cluster.UID), Generation: cluster.Generation, MarkerVersion: api.LegacyRFDiscoveryMarkerVersion, ProtocolVersion: api.LegacyRFDiscoveryProtocolVersion, AttemptID: legacyRFAttemptID(cluster.UID, cluster.Generation), OrderedSeeds: seeds, SeedDigest: digest, WorkerImageDigest: image, Connection: discovery.Connection{ExpectedClusterName: cluster.CassClusterName(), SecretBindings: bindings, ManagedLocations: locations}}, nil
}

func legacyRFAttemptID(uid types.UID, generation int64) string {
	identity := sha256.Sum256([]byte(string(uid) + ":" + strconv.FormatInt(generation, 10)))
	return hex.EncodeToString(identity[:12])
}

func legacyRFFailedAttemptForCleanup(
	current discovery.Attempt,
	status *api.LegacyRFDiscoveryStatus,
) discovery.Attempt {
	failed := current
	if status == nil || status.ObservedGeneration == 0 {
		return failed
	}
	failed.Generation = status.ObservedGeneration
	failed.AttemptID = legacyRFAttemptID(types.UID(current.ClusterUID), failed.Generation)
	if len(status.CurrentManagedLocations) != 0 {
		failed.Connection.ManagedLocations = make([]discovery.ManagedLocation, len(status.CurrentManagedLocations))
		for index, location := range status.CurrentManagedLocations {
			failed.Connection.ManagedLocations[index] = discovery.ManagedLocation{
				K8sContext: location.K8sContext, Namespace: location.Namespace, Name: location.Name,
				DatacenterName: location.DatacenterName,
			}
		}
	}
	return failed
}

func (runtime *legacyRFControllerRuntime) credentialBindings(ctx context.Context, cluster *api.K8ssandraCluster) ([]discovery.SecretBinding, error) {
	bindings := make([]discovery.SecretBinding, 0, 2)
	reference := cluster.Spec.Cassandra.LegacyCqlCredentialsSecretRef
	if reference != nil {
		binding, err := runtime.secretBinding(ctx, cluster.Namespace, reference.Name, "auth", []string{"username", "password"})
		if err != nil {
			return nil, err
		}
		bindings = append(bindings, binding)
	}
	tlsReference := cluster.Spec.Cassandra.LegacyCqlTLSSecretRef
	if tlsReference == nil {
		return bindings, nil
	}
	secret := &corev1.Secret{}
	if err := runtime.clients.GetLocalNonCacheClient().Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: tlsReference.Name}, secret); err != nil {
		return nil, discovery.NewBoundaryError(api.LegacyRFReasonTLSMaterialInvalid, err)
	}
	keys := []string{"ca.crt"}
	certificate, privateKey := len(secret.Data["tls.crt"]) != 0, len(secret.Data["tls.key"]) != 0
	if len(secret.Data["ca.crt"]) == 0 || certificate != privateKey {
		return nil, discovery.NewBoundaryError(api.LegacyRFReasonTLSMaterialInvalid, stderrors.New("TLS Secret requires ca.crt and an optional tls.crt/tls.key pair"))
	}
	if certificate {
		keys = append(keys, "tls.crt", "tls.key")
	}
	bindings = append(bindings, discovery.SecretBinding{Purpose: "tls", Namespace: cluster.Namespace, Name: tlsReference.Name, Keys: keys, ResourceVersion: secret.ResourceVersion})
	return bindings, nil
}

func (runtime *legacyRFControllerRuntime) secretBinding(ctx context.Context, namespace, name, purpose string, keys []string) (discovery.SecretBinding, error) {
	secret := &corev1.Secret{}
	if err := runtime.clients.GetLocalNonCacheClient().Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, secret); err != nil {
		return discovery.SecretBinding{}, discovery.NewBoundaryError(api.LegacyRFReasonCredentialSecretInvalid, err)
	}
	for _, key := range keys {
		if len(secret.Data[key]) == 0 {
			return discovery.SecretBinding{}, discovery.NewBoundaryError(api.LegacyRFReasonCredentialSecretInvalid, stderrors.New("credential Secret is missing required keys"))
		}
	}
	return discovery.SecretBinding{Purpose: purpose, Namespace: namespace, Name: name, Keys: append([]string(nil), keys...), ResourceVersion: secret.ResourceVersion}, nil
}

func (runtime *legacyRFControllerRuntime) ValidateBindings(ctx context.Context, bindings []discovery.SecretBinding) error {
	for _, binding := range bindings {
		directClient, err := runtime.clients.GetRemoteNonCacheClient(binding.SourceContext)
		if err != nil {
			return fmt.Errorf("resolve direct client for %s Secret: %w", binding.Purpose, err)
		}
		secret := &corev1.Secret{}
		key := types.NamespacedName{Namespace: binding.Namespace, Name: binding.Name}
		if err = directClient.Get(ctx, key, secret); err != nil {
			return fmt.Errorf("read %s Secret %s: %w", binding.Purpose, key, err)
		}
		if secret.ResourceVersion != binding.ResourceVersion {
			return fmt.Errorf("%s Secret %s resourceVersion changed", binding.Purpose, key)
		}
		if err = copyBindingKeys(make(map[string][]byte), binding, secret.Data); err != nil {
			return fmt.Errorf("validate %s Secret %s: %w", binding.Purpose, key, err)
		}
	}
	return nil
}

func (runtime *legacyRFControllerRuntime) AllowCleanup(ctx context.Context, attempt discovery.Attempt) error {
	locations := make([]api.LegacyRFManagedLocation, len(attempt.Connection.ManagedLocations))
	for index, location := range attempt.Connection.ManagedLocations {
		locations[index] = api.LegacyRFManagedLocation{
			K8sContext: location.K8sContext, Namespace: location.Namespace, Name: location.Name,
			DatacenterName: location.DatacenterName,
		}
	}
	if err := runtime.managed.AssertAbsent(ctx, locations, true); err != nil {
		return managedSurveyBoundary("survey before retry cleanup", err)
	}
	return nil
}

func (runtime *legacyRFControllerRuntime) cleanupAcceptedAttempt(
	ctx context.Context,
	cluster *api.K8ssandraCluster,
) (bool, error) {
	if cluster == nil || !legacyRFSnapshotIsIntact(cluster.Status.LegacyRFDiscovery) {
		return false, discovery.NewBoundaryError(api.LegacyRFReasonSnapshotConflict,
			stderrors.New("accepted snapshot is unavailable for cleanup"))
	}
	attempt, err := attemptFromAcceptedSnapshot(cluster.Status.LegacyRFDiscovery.AcceptedSnapshot)
	if err != nil {
		return false, discovery.NewBoundaryError(api.LegacyRFReasonSnapshotConflict, err)
	}
	attempts, err := NewDiscoveryAttempts(runtime.clients, client.ObjectKeyFromObject(cluster), runtime.image,
		runtime.keyReader, acceptedCleanupSafety{}, runtime.recorder, runtime.backoff)
	if err != nil {
		return false, err
	}
	return attempts.Cleanup(ctx, attempt)
}

type acceptedCleanupSafety struct{}

func (acceptedCleanupSafety) AllowCleanup(context.Context, discovery.Attempt) error { return nil }

func attemptFromAcceptedSnapshot(snapshot *api.LegacyRFSnapshot) (discovery.Attempt, error) {
	if snapshot == nil {
		return discovery.Attempt{}, stderrors.New("accepted snapshot is required")
	}
	seeds, digest, err := discovery.CanonicalizeSeeds(snapshot.AcceptedSeeds)
	if err != nil || digest != snapshot.AcceptedSeedDigest {
		return discovery.Attempt{}, stderrors.New("accepted seed binding is invalid")
	}
	locations := make([]discovery.ManagedLocation, len(snapshot.AcceptedManagedLocations))
	for index, location := range snapshot.AcceptedManagedLocations {
		locations[index] = discovery.ManagedLocation{
			K8sContext: location.K8sContext, Namespace: location.Namespace, Name: location.Name,
			DatacenterName: location.DatacenterName,
		}
	}
	return discovery.Attempt{
		ClusterUID: snapshot.ClusterUID, Generation: snapshot.AcceptedGeneration,
		MarkerVersion: snapshot.MarkerVersion, ProtocolVersion: snapshot.ProtocolVersion,
		AttemptID:    legacyRFAttemptID(types.UID(snapshot.ClusterUID), snapshot.AcceptedGeneration),
		OrderedSeeds: seeds, SeedDigest: digest, WorkerImageDigest: snapshot.WorkerImageDigest,
		Connection: discovery.Connection{ExpectedClusterName: snapshot.ExpectedClusterName,
			SecretBindings: fromAPISecretBindings(snapshot.SecretBindings), ManagedLocations: locations},
	}, nil
}

func (runtime *legacyRFControllerRuntime) AuthorizeManagedCreation(ctx context.Context, cluster *api.K8ssandraCluster, target api.LegacyRFManagedLocation, create LegacyRFManagedCreateCallback) error {
	status := cluster.Status.LegacyRFDiscovery
	if status == nil {
		return stderrors.New("authorize managed creation: discovery status is required")
	}
	return AuthorizeLegacyRFManagedCreation(ctx, runtime.control, runtime.managed, LegacyRFManagedCreationInput{Key: client.ObjectKeyFromObject(cluster), ExpectedUID: cluster.UID, ExpectedSnapshotHash: status.SnapshotHash, Target: target, Create: create})
}

func legacyRFSnapshotFromResult(cluster *api.K8ssandraCluster, attempt discovery.Attempt, discovered discovery.DiscoveryResult, acceptedAt time.Time) (*api.LegacyRFSnapshot, error) {
	authority := discovered.Authoritative
	if authority == nil {
		return nil, discovery.NewBoundaryError(api.LegacyRFReasonInvalidDiscoveryResult, stderrors.New("discovery result has no authoritative candidate"))
	}
	plan, err := BuildLegacyRFCurrentPlan(cluster)
	if err != nil || len(plan.ManagedLocations) == 0 {
		return nil, discovery.NewBoundaryError(api.LegacyRFReasonInvalidDiscoveryResult, stderrors.New("discovery result has no managed location"))
	}
	acceptedSeeds := make([]string, len(attempt.OrderedSeeds))
	for index, seed := range attempt.OrderedSeeds {
		acceptedSeeds[index] = seed.Addr().String()
	}
	snapshot := &api.LegacyRFSnapshot{ClusterUID: string(cluster.UID), AcceptedGeneration: cluster.Generation, MarkerVersion: attempt.MarkerVersion, ProtocolVersion: attempt.ProtocolVersion, AcceptedSeeds: acceptedSeeds, AcceptedSeedDigest: attempt.SeedDigest, AuthoritativeEndpoint: authority.Endpoint.String(), AttemptTrace: toAPIAttemptTrace(discovered.AttemptTrace), ExpectedClusterName: authority.ClusterName, ServerType: api.ServerDistributionCassandra, SourceVersion: authority.SourceVersion, Partitioner: authority.Partitioner, IdentityFingerprint: authority.Fingerprints.Identity, TopologyFingerprint: authority.Fingerprints.Topology, SchemaFingerprint: authority.Fingerprints.Schema, ObservedExternalDCs: authority.ObservedExternalDCs, Replication: api.LegacySystemKeyspaceReplication{SystemAuth: authority.Replication.SystemAuth.Replication, SystemTraces: authority.Replication.SystemTraces.Replication, SystemDistributed: authority.Replication.SystemDistributed.Replication}, SecretBindings: toAPISecretBindings(attempt.Connection.SecretBindings), DiscoveryLocation: plan.ManagedLocations[0], AcceptedManagedLocations: plan.ManagedLocations, WorkerImageDigest: attempt.WorkerImageDigest, AcceptedAt: metav1.NewTime(acceptedAt)}
	snapshot.Hash, err = LegacyRFSnapshotHash(snapshot)
	if err != nil {
		return nil, discovery.NewBoundaryError(api.LegacyRFReasonInvalidDiscoveryResult, err)
	}
	return snapshot, nil
}

func toAPIAttemptTrace(trace []discovery.EndpointAttemptSummary) []api.LegacyRFEndpointAttemptSummary {
	converted := make([]api.LegacyRFEndpointAttemptSummary, len(trace))
	for index, summary := range trace {
		converted[index] = api.LegacyRFEndpointAttemptSummary{AttemptIndex: summary.AttemptIndex,
			Endpoint: summary.Endpoint.String(), Outcome: api.LegacyRFEndpointAttemptOutcome(summary.Outcome), Reason: summary.Reason}
	}
	return converted
}
