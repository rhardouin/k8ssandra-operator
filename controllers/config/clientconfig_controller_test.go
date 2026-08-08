package config

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/k8ssandra/k8ssandra-operator/pkg/clientcache"
	testutils "github.com/k8ssandra/k8ssandra-operator/pkg/test"
	"github.com/k8ssandra/k8ssandra-operator/pkg/utils"
	"github.com/k8ssandra/k8ssandra-operator/test/framework"
	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/cluster"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	"sigs.k8s.io/controller-runtime/pkg/manager"

	configapi "github.com/k8ssandra/k8ssandra-operator/apis/config/v1beta1"
)

const (
	timeout  = time.Second * 5
	interval = time.Millisecond * 10
)

var (
	testEnv     *testutils.MultiClusterTestEnv
	scheme      *runtime.Scheme
	logger      logr.Logger
	cancelCalls atomic.Int64
	// secretFilter map[types.NamespacedName]types.NamespacedName
	reconciler *ClientConfigReconciler
	usedMgr    manager.Manager
	kubeConfig []byte
)

func TestClientConfigReconciler(t *testing.T) {
	ctx := testutils.TestSetup(t)
	ctx, cancel := context.WithCancel(ctx)
	testEnv = &testutils.MultiClusterTestEnv{}
	cancelCalls.Store(0)
	shutDownFunc := func() {
		// We don't want to actually call the shutdown in these tests - we just want to know the cancel function was called
		cancelCalls.Add(1)
	}
	// secretFilter = make(map[types.NamespacedName]types.NamespacedName)
	reconciler = &ClientConfigReconciler{}
	err := testEnv.Start(ctx, t, func(mgr manager.Manager, clientCache *clientcache.ClientCache, clusters []cluster.Cluster) error {
		scheme = mgr.GetScheme()
		logger = mgr.GetLogger()
		reconciler.ClientCache = clientCache
		reconciler.Scheme = scheme
		// reconciler.secretFilter = secretFilter
		usedMgr = mgr
		return reconciler.SetupWithManager(mgr, shutDownFunc)
	}, nil)
	if err != nil {
		t.Fatalf("failed to start test environment: %s", err)
	}

	user, err := testEnv.GetControlPlaneEnvTest().ControlPlane.AddUser(envtest.User{
		Name:   "envtest-admin",
		Groups: []string{"system:masters"},
	}, nil)
	if err != nil {
		t.Fatalf("failed to create envtest user: %v", err)
	}

	kubeConfig, err = user.KubeConfig()
	if err != nil {
		t.Fatalf("failed to get envtest kubeconfig: %v", err)
	}

	defer testEnv.Stop(t)
	defer cancel()

	// Secret controller tests
	t.Run("InitClientConfigs", testEnv.ControllerTest(ctx, testInitClientConfigs))
	t.Run("DirectClientConstructionFailure", testEnv.ControllerTest(ctx, testDirectClientConstructionFailure))
	t.Run("ManagerRegistrationFailure", testEnv.ControllerTest(ctx, testManagerRegistrationFailure))
	t.Run("SecretModification", testEnv.ControllerTest(ctx, testSecretModification))
	t.Run("ClientConfigDeletion", testEnv.ControllerTest(ctx, testConfigDeletion))
	t.Run("SecretDeletion", testEnv.ControllerTest(ctx, testSecretDeletion))
}

func insertKubeConfigSecret(ctx context.Context, localClient client.Client, namespace string) (*corev1.Secret, error) {
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      "test-kubeconfig-secret",
		},
		Type: "Opaque",
		Data: map[string][]byte{
			"kubeconfig": kubeConfig,
		},
	}

	err := localClient.Create(ctx, secret)
	return secret, err
}

func insertClientConfig(ctx context.Context, localClient client.Client, namespace, name, secretName string) (*configapi.ClientConfig, error) {
	clientConfig := &configapi.ClientConfig{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      name,
		},
		Spec: configapi.ClientConfigSpec{
			KubeConfigSecret: corev1.LocalObjectReference{
				Name: secretName,
			},
		},
	}

	err := localClient.Create(ctx, clientConfig)
	return clientConfig, err
}

func testInitClientConfigs(t *testing.T, ctx context.Context, f *framework.Framework, namespace string) {
	assert := assert.New(t)

	secret, err := insertKubeConfigSecret(ctx, f.Client, namespace)
	assert.NoError(err)

	clientConfig, err := insertClientConfig(ctx, f.Client, namespace, "envtest", secret.Name)
	assert.NoError(err)

	t.Log("Verify initClientConfig loads the clusters correctly")
	clusters, err := reconciler.InitClientConfigs(ctx, usedMgr, namespace)
	assert.NoError(err)
	assert.Equal(1, len(clusters))
	cachedClient, err := reconciler.ClientCache.GetRemoteClient("envtest")
	assert.NoError(err)
	directClient, err := reconciler.ClientCache.GetRemoteNonCacheClient("envtest")
	assert.NoError(err, "InitClientConfigs must register a direct client for fail-closed API reads")
	assert.NotSame(cachedClient, directClient, "cached and direct clients must be distinct instances")

	secretKey := types.NamespacedName{Namespace: namespace, Name: secret.Name}
	clientConfigKey := types.NamespacedName{Namespace: namespace, Name: clientConfig.Name}

	t.Log("Verify that the client updated secret and clientConfig with hashes")
	assert.Eventually(func() bool {
		currentConfig := &configapi.ClientConfig{}
		err = f.Client.Get(ctx, clientConfigKey, currentConfig)
		if err != nil {
			return false
		}

		reconciler.filterMutex.RLock()
		_, found := reconciler.secretFilter[secretKey]
		reconciler.filterMutex.RUnlock()
		return metav1.HasAnnotation(currentConfig.ObjectMeta, KubeSecretHashAnnotation) &&
			metav1.HasAnnotation(currentConfig.ObjectMeta, ClientConfigHashAnnotation) &&
			found
	}, timeout, interval)

	t.Log("Create clientConfig which has incorrect context-name")
	_, err = insertClientConfig(ctx, f.Client, namespace, "envtest-failed", secret.Name)
	assert.NoError(err)

	_, err = reconciler.InitClientConfigs(ctx, usedMgr, namespace)
	assert.Error(err)
}

func testDirectClientConstructionFailure(t *testing.T, ctx context.Context, f *framework.Framework, namespace string) {
	localCache, err := clientcache.NewValidated(
		reconciler.ClientCache.GetLocalClient(),
		reconciler.ClientCache.GetLocalNonCacheClient(),
		scheme,
	)
	assert.NoError(t, err)

	secret, err := insertKubeConfigSecret(ctx, f.Client, namespace)
	assert.NoError(t, err)
	clientConfig, err := insertClientConfig(ctx, f.Client, namespace, "envtest", secret.Name)
	assert.NoError(t, err)

	testReconciler := &ClientConfigReconciler{
		ClientCache:  localCache,
		Scheme:       scheme,
		secretFilter: make(map[types.NamespacedName]types.NamespacedName),
		newDirectClient: func(*rest.Config, client.Options) (client.Client, error) {
			return nil, errors.New("injected construction failure")
		},
	}

	_, err = testReconciler.initAdditionalClusterConfig(ctx, *clientConfig, usedMgr, nil)
	assert.ErrorContains(t, err, `create direct client for context "envtest": injected construction failure`)
	_, cachedErr := localCache.GetRemoteClient("envtest")
	assert.Error(t, cachedErr, "failed construction must not leave a cached registration")
	_, directErr := localCache.GetRemoteNonCacheClient("envtest")
	assert.Error(t, directErr, "failed construction must not leave a direct registration")
}

type managerWithAddError struct {
	manager.Manager
}

func (managerWithAddError) Add(manager.Runnable) error {
	return errors.New("injected manager registration failure")
}

func testManagerRegistrationFailure(t *testing.T, ctx context.Context, f *framework.Framework, namespace string) {
	localCache, err := clientcache.NewValidated(
		reconciler.ClientCache.GetLocalClient(),
		reconciler.ClientCache.GetLocalNonCacheClient(),
		scheme,
	)
	assert.NoError(t, err)

	secret, err := insertKubeConfigSecret(ctx, f.Client, namespace)
	assert.NoError(t, err)
	clientConfig, err := insertClientConfig(ctx, f.Client, namespace, "envtest", secret.Name)
	assert.NoError(t, err)

	testReconciler := &ClientConfigReconciler{
		ClientCache:  localCache,
		Scheme:       scheme,
		secretFilter: make(map[types.NamespacedName]types.NamespacedName),
	}
	_, err = testReconciler.initAdditionalClusterConfig(
		ctx,
		*clientConfig,
		managerWithAddError{Manager: usedMgr},
		nil,
	)
	assert.ErrorContains(t, err, `register cached cluster for context "envtest": injected manager registration failure`)
	_, cachedErr := localCache.GetRemoteClient("envtest")
	assert.Error(t, cachedErr, "failed manager registration must not leave a cached registration")
	_, directErr := localCache.GetRemoteNonCacheClient("envtest")
	assert.Error(t, directErr, "failed manager registration must not leave a direct registration")
}

func testSecretModification(t *testing.T, ctx context.Context, f *framework.Framework, namespace string) {
	// Intentionally different from previous test. This wants to test the internal behavior more, without
	// relying on the controller itself creating "necessary" requirements

	assert := assert.New(t)
	reconciler.filterMutex.Lock()
	reconciler.secretFilter = make(map[types.NamespacedName]types.NamespacedName)
	reconciler.filterMutex.Unlock()

	secret, err := insertKubeConfigSecret(ctx, f.Client, namespace)
	assert.NoError(err)

	clientConfig := &configapi.ClientConfig{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      "kind-k8ssandra-0",
		},
		Spec: configapi.ClientConfigSpec{
			KubeConfigSecret: corev1.LocalObjectReference{
				Name: secret.Name,
			},
		},
	}
	configHash := utils.DeepHashString(clientConfig.Spec)
	secretHash := utils.DeepHashString(secret.Data)
	metav1.SetMetaDataAnnotation(&clientConfig.ObjectMeta, KubeSecretHashAnnotation, secretHash)
	metav1.SetMetaDataAnnotation(&clientConfig.ObjectMeta, ClientConfigHashAnnotation, configHash)

	err = f.Client.Create(ctx, clientConfig)
	assert.NoError(err)

	// Now update the secretFilter
	secretKey := types.NamespacedName{Namespace: namespace, Name: secret.Name}
	clientConfigKey := types.NamespacedName{Namespace: namespace, Name: clientConfig.Name}
	reconciler.filterMutex.Lock()
	reconciler.secretFilter[secretKey] = clientConfigKey
	reconciler.filterMutex.Unlock()

	// Store currentCount of cancelFunc
	currentCount := cancelCalls.Load()

	secretCurrent := &corev1.Secret{}
	err = f.Client.Get(ctx, secretKey, secretCurrent)
	assert.NoError(err)

	secretCurrent.Data["new-key"] = []byte("excellent")
	err = f.Client.Update(ctx, secretCurrent)
	assert.NoError(err)

	assert.Eventually(func() bool {
		return cancelCalls.Load() > currentCount
	}, timeout, interval)
}

func testConfigDeletion(t *testing.T, ctx context.Context, f *framework.Framework, namespace string) {
	assert := assert.New(t)

	t.Log("Insert clientConfig and secrets, load them to reconciler")
	secret, err := insertKubeConfigSecret(ctx, f.Client, namespace)
	assert.NoError(err)

	clientConfig, err := insertClientConfig(ctx, f.Client, namespace, "envtest", secret.Name)
	assert.NoError(err)

	_, err = reconciler.InitClientConfigs(ctx, usedMgr, namespace)
	assert.NoError(err)

	// Store currentCount of cancelFunc
	currentCount := cancelCalls.Load()

	t.Log("Delete ClientConfig and wait for shutdown call")
	err = f.Client.Delete(ctx, clientConfig)
	assert.NoError(err)

	assert.Eventually(func() bool {
		return cancelCalls.Load() > currentCount
	}, timeout, interval)
}

func testSecretDeletion(t *testing.T, ctx context.Context, f *framework.Framework, namespace string) {
	assert := assert.New(t)

	t.Log("Insert clientConfig and secrets, load them to reconciler")
	secret, err := insertKubeConfigSecret(ctx, f.Client, namespace)
	assert.NoError(err)

	_, err = insertClientConfig(ctx, f.Client, namespace, "envtest", secret.Name)
	assert.NoError(err)

	_, err = reconciler.InitClientConfigs(ctx, usedMgr, namespace)
	assert.NoError(err)

	// Store currentCount of cancelFunc
	currentCount := cancelCalls.Load()

	t.Log("Delete Secret and wait for shutdown call")
	err = f.Client.Delete(ctx, secret)
	assert.NoError(err)

	assert.Eventually(func() bool {
		return cancelCalls.Load() > currentCount
	}, timeout, interval)
}
