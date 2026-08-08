package k8ssandra

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// ManagerImageResolver resolves the immutable image running the configured manager container.
type ManagerImageResolver interface {
	Resolve(context.Context) (string, error)
}

type directManagerImageResolver struct {
	reader        client.Reader
	podKey        types.NamespacedName
	containerName string
}

// NewManagerImageResolver creates an uncached manager Pod image resolver.
func NewManagerImageResolver(
	reader client.Reader,
	podKey types.NamespacedName,
	containerName string,
) (ManagerImageResolver, error) {
	if reader == nil {
		return nil, errors.New("create manager image resolver: direct reader is required")
	}
	if podKey.Namespace == "" || podKey.Name == "" || containerName == "" {
		return nil, errors.New("create manager image resolver: Pod key and container name are required")
	}
	return &directManagerImageResolver{reader: reader, podKey: podKey, containerName: containerName}, nil
}

func (resolver *directManagerImageResolver) Resolve(ctx context.Context) (string, error) {
	pod := &corev1.Pod{}
	if err := resolver.reader.Get(ctx, resolver.podKey, pod); err != nil {
		return "", fmt.Errorf("resolve running manager image: read Pod %s: %w", resolver.podKey, err)
	}
	configuredImage, err := configuredContainerImage(pod, resolver.containerName)
	if err != nil {
		return "", err
	}
	imageID, err := runningContainerImageID(pod, resolver.containerName)
	if err != nil {
		return "", err
	}
	return normalizeManagerImageID(imageID, configuredImage)
}

func configuredContainerImage(pod *corev1.Pod, name string) (string, error) {
	images := make([]string, 0, 1)
	for _, container := range pod.Spec.Containers {
		if container.Name == name {
			images = append(images, container.Image)
		}
	}
	if len(images) != 1 || images[0] == "" {
		return "", fmt.Errorf("resolve running manager image: configured container %q is absent or ambiguous", name)
	}
	return images[0], nil
}

func runningContainerImageID(pod *corev1.Pod, name string) (string, error) {
	imageIDs := make([]string, 0, 1)
	for _, status := range pod.Status.ContainerStatuses {
		if status.Name == name && status.ImageID != "" {
			imageIDs = append(imageIDs, status.ImageID)
		}
	}
	if len(imageIDs) != 1 {
		return "", fmt.Errorf("resolve running manager image: status for container %q is absent or ambiguous", name)
	}
	return imageIDs[0], nil
}

func normalizeManagerImageID(imageID, configuredImage string) (string, error) {
	identity := stripRuntimeScheme(imageID)
	repository, digest, found := strings.Cut(identity, "@")
	if !found {
		if configuredRepository, configuredDigest, configured := strings.Cut(configuredImage, "@"); configured &&
			configuredRepository != "" && validSHA256Digest(configuredDigest) {
			return configuredRepository + "@" + configuredDigest, nil
		}
		return "", errors.New("resolve running manager image: digest-only identity requires an immutable configured image")
	}
	if repository == "" || !validSHA256Digest(digest) || strings.ContainsAny(identity, " \t\r\n") {
		return "", errors.New("resolve running manager image: image identity is not an immutable SHA-256 reference")
	}
	return repository + "@" + digest, nil
}

func stripRuntimeScheme(identity string) string {
	if separator := strings.Index(identity, "://"); separator >= 0 {
		return identity[separator+3:]
	}
	return identity
}

func validSHA256Digest(digest string) bool {
	encoded := strings.TrimPrefix(digest, "sha256:")
	decoded, err := hex.DecodeString(encoded)
	return strings.HasPrefix(digest, "sha256:") && len(encoded) == 64 && len(decoded) == 32 &&
		err == nil && encoded == strings.ToLower(encoded)
}
