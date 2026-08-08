package k8ssandra

import (
	"context"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/stretchr/testify/require"
)

func TestNormalizeManagerImageID(t *testing.T) {
	digest := "sha256:" + strings.Repeat("a", 64)
	tests := []struct {
		name, imageID, configuredImage, want string
		wantErr                              bool
	}{
		{name: "docker pullable reference", imageID: "docker-pullable://registry.example/operator@" + digest, want: "registry.example/operator@" + digest},
		{name: "containerd pullable reference", imageID: "containerd://registry.example/operator@" + digest, want: "registry.example/operator@" + digest},
		{name: "plain pullable reference", imageID: "registry.example/operator@" + digest, want: "registry.example/operator@" + digest},
		{name: "digest only rejects mutable configured image", imageID: "docker://" + digest, wantErr: true},
		{name: "digest only retains configured immutable manifest", imageID: "containerd://sha256:" + strings.Repeat("c", 64), configuredImage: "registry.example/operator@" + digest, want: "registry.example/operator@" + digest},
		{name: "tag only", imageID: "docker://registry.example/operator:latest", wantErr: true},
		{name: "malformed digest", imageID: "docker://registry.example/operator@sha256:abc", wantErr: true},
		{name: "uppercase digest", imageID: "docker://registry.example/operator@sha256:" + strings.Repeat("A", 64), wantErr: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			configuredImage := test.configuredImage
			if configuredImage == "" {
				configuredImage = "registry.example/operator:v1"
			}
			got, err := normalizeManagerImageID(test.imageID, configuredImage)
			if test.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, test.want, got)
		})
	}
}

func TestManagerImageResolverSelectsExactConfiguredContainer(t *testing.T) {
	digest := "sha256:" + strings.Repeat("b", 64)
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "operator-0", Namespace: "operator-system"},
		Spec: corev1.PodSpec{Containers: []corev1.Container{
			{Name: "manager", Image: "registry.example/operator:v1"},
			{Name: "proxy", Image: "registry.example/proxy:v1"},
		}},
		Status: corev1.PodStatus{ContainerStatuses: []corev1.ContainerStatus{
			{Name: "manager", ImageID: "containerd://registry.example/operator@" + digest, RestartCount: 4},
			{Name: "proxy", ImageID: "containerd://registry.example/proxy@sha256:" + strings.Repeat("c", 64)},
		}},
	}
	scheme := runtime.NewScheme()
	require.NoError(t, corev1.AddToScheme(scheme))
	reader := fake.NewClientBuilder().WithScheme(scheme).WithObjects(pod).Build()
	resolver, err := NewManagerImageResolver(reader, types.NamespacedName{Namespace: pod.Namespace, Name: pod.Name}, "manager")
	require.NoError(t, err)
	image, err := resolver.Resolve(context.Background())
	require.NoError(t, err)
	require.Equal(t, "registry.example/operator@"+digest, image)
}

func TestManagerImageResolverRejectsMissingWrongAndAmbiguousStatus(t *testing.T) {
	digest := "sha256:" + strings.Repeat("d", 64)
	tests := []struct {
		name     string
		statuses []corev1.ContainerStatus
	}{
		{name: "absent status"},
		{name: "wrong container", statuses: []corev1.ContainerStatus{{Name: "proxy", ImageID: "docker://" + digest}}},
		{name: "ambiguous", statuses: []corev1.ContainerStatus{{Name: "manager", ImageID: "docker://" + digest}, {Name: "manager", ImageID: "docker://" + digest}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "operator-0", Namespace: "operator-system"}, Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "manager", Image: "registry.example/operator:v1"}}}, Status: corev1.PodStatus{ContainerStatuses: test.statuses}}
			scheme := runtime.NewScheme()
			require.NoError(t, corev1.AddToScheme(scheme))
			resolver, err := NewManagerImageResolver(fake.NewClientBuilder().WithScheme(scheme).WithObjects(pod).Build(), types.NamespacedName{Namespace: pod.Namespace, Name: pod.Name}, "manager")
			require.NoError(t, err)
			_, err = resolver.Resolve(context.Background())
			require.Error(t, err)
		})
	}
}
