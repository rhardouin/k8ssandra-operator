package clientcache

import (
	"testing"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestDirectClientCacheValidatesDependencies(t *testing.T) {
	scheme := runtime.NewScheme()
	require.NoError(t, corev1.AddToScheme(scheme))
	client := fake.NewClientBuilder().WithScheme(scheme).Build()
	tests := []struct {
		name   string
		cached bool
		direct bool
		scheme bool
	}{
		{name: "cached missing", direct: true, scheme: true},
		{name: "direct missing", cached: true, scheme: true},
		{name: "scheme missing", cached: true, direct: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var cached, direct = client, client
			var configuredScheme = scheme
			if !test.cached {
				cached = nil
			}
			if !test.direct {
				direct = nil
			}
			if !test.scheme {
				configuredScheme = nil
			}
			actual, err := NewValidated(cached, direct, configuredScheme)
			require.Error(t, err)
			require.Nil(t, actual)
			require.Nil(t, New(cached, direct, configuredScheme))
		})
	}
}

func TestDirectClientCacheSeparatesCachedAndAuthoritativeReaders(t *testing.T) {
	scheme := runtime.NewScheme()
	require.NoError(t, corev1.AddToScheme(scheme))
	staleCached := fake.NewClientBuilder().WithScheme(scheme).Build()
	authoritative := fake.NewClientBuilder().WithScheme(scheme).WithObjects(&corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "managed-state"},
	}).Build()
	cache, err := NewValidated(staleCached, authoritative, scheme)
	require.NoError(t, err)
	require.Same(t, staleCached, cache.GetLocalClient())
	require.Same(t, authoritative, cache.GetLocalNonCacheClient())

	remoteCached := fake.NewClientBuilder().WithScheme(scheme).Build()
	remoteDirect := fake.NewClientBuilder().WithScheme(scheme).WithObjects(&corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "remote-managed-state"},
	}).Build()
	require.NoError(t, cache.AddClientPair("plane-a", remoteCached, remoteDirect))
	cached, err := cache.GetRemoteClient("plane-a")
	require.NoError(t, err)
	direct, err := cache.GetRemoteNonCacheClient("plane-a")
	require.NoError(t, err)
	require.Same(t, remoteCached, cached)
	require.Same(t, remoteDirect, direct)
	require.NotSame(t, cached, direct)
	remoteKey := types.NamespacedName{Namespace: "ns", Name: "remote-managed-state"}
	require.Error(t, cached.Get(t.Context(), remoteKey, &corev1.ConfigMap{}), "remote cached absence is not authoritative")
	require.NoError(t, direct.Get(t.Context(), remoteKey, &corev1.ConfigMap{}))

	key := types.NamespacedName{Namespace: "ns", Name: "managed-state"}
	require.Error(t, cache.GetLocalClient().Get(t.Context(), key, &corev1.ConfigMap{}), "stale cached absence is not authoritative")
	require.NoError(t, cache.GetLocalNonCacheClient().Get(t.Context(), key, &corev1.ConfigMap{}))
}

func TestDirectClientCacheReplacesRemotePairsTogether(t *testing.T) {
	scheme := runtime.NewScheme()
	require.NoError(t, corev1.AddToScheme(scheme))
	local := fake.NewClientBuilder().WithScheme(scheme).Build()
	cache, err := NewValidated(local, local, scheme)
	require.NoError(t, err)

	oldCached := fake.NewClientBuilder().WithScheme(scheme).Build()
	oldDirect := fake.NewClientBuilder().WithScheme(scheme).Build()
	require.NoError(t, cache.AddClientPair("plane-a", oldCached, oldDirect))
	newCached := fake.NewClientBuilder().WithScheme(scheme).Build()
	newDirect := fake.NewClientBuilder().WithScheme(scheme).Build()
	require.NoError(t, cache.AddClientPair("plane-a", newCached, newDirect))

	actualCached, err := cache.GetRemoteClient("plane-a")
	require.NoError(t, err)
	actualDirect, err := cache.GetRemoteNonCacheClient("plane-a")
	require.NoError(t, err)
	require.Same(t, newCached, actualCached)
	require.Same(t, newDirect, actualDirect)
}

func TestDirectClientCacheFailsClosedForIncompleteRemotePairs(t *testing.T) {
	scheme := runtime.NewScheme()
	require.NoError(t, corev1.AddToScheme(scheme))
	client := fake.NewClientBuilder().WithScheme(scheme).Build()
	cache, err := NewValidated(client, client, scheme)
	require.NoError(t, err)

	tests := []struct {
		name   string
		cached bool
		direct bool
	}{
		{name: "context empty", cached: true, direct: true},
		{name: "cached missing", direct: true},
		{name: "direct missing", cached: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			contextName := "plane-" + test.name
			if test.name == "context empty" {
				contextName = ""
			}
			var cached, direct = client, client
			if !test.cached {
				cached = nil
			}
			if !test.direct {
				direct = nil
			}
			require.Error(t, cache.AddClientPair(contextName, cached, direct))
		})
	}
	_, err = cache.GetRemoteNonCacheClient("unknown")
	require.ErrorContains(t, err, "no known direct client")
	_, err = cache.GetRemoteClient("unknown")
	require.Error(t, err)

	cache.AddClient("cached-only", client)
	_, err = cache.GetRemoteNonCacheClient("cached-only")
	require.Error(t, err, "direct reads must never fall back to a cached client")
}
