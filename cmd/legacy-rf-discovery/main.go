package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"strings"
	"syscall"
	"time"

	api "github.com/k8ssandra/k8ssandra-operator/apis/k8ssandra/v1alpha1"
	"github.com/k8ssandra/k8ssandra-operator/pkg/discovery"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	maximumAttemptBytes = 1 << 20
	maximumSecretBytes  = 1 << 16
	resultSigningKeyLen = 32
	resultConfigMapKey  = "result.json"
)

type executableDependencies struct {
	newObserver func(discovery.CQLObserverOptions) (discovery.EndpointObserver, error)
	clock       discovery.Clock
	publisher   resultPublisher
}

type executableOptions struct {
	attemptPath          string
	hmacKeyPath          string
	credentialsDirectory string
	tlsDirectory         string
	resultTarget         resultConfigMapTarget
	connectTimeout       time.Duration
	queryTimeout         time.Duration
	overallTimeout       time.Duration
}

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()
	dependencies := executableDependencies{
		newObserver: newProductionObserver, clock: discovery.RealClock{},
		publisher: inClusterResultPublisher{},
	}
	os.Exit(run(ctx, os.Args[1:], os.Stdout, os.Stderr, dependencies))
}

func newProductionObserver(options discovery.CQLObserverOptions) (discovery.EndpointObserver, error) {
	return discovery.NewCQLObserver(options)
}

func run(
	ctx context.Context,
	arguments []string,
	_ io.Writer,
	stderr io.Writer,
	dependencies executableDependencies,
) int {
	if err := execute(ctx, arguments, dependencies); err != nil {
		_, _ = fmt.Fprintf(stderr, "legacy RF discovery failed: %s\n", sanitizedMessage(err))
		return 1
	}
	return 0
}

func execute(ctx context.Context, arguments []string, dependencies executableDependencies) error {
	if dependencies.newObserver == nil || dependencies.clock == nil || dependencies.publisher == nil {
		return errors.New("worker dependencies are invalid")
	}
	options, err := parseOptions(arguments)
	if err != nil {
		return err
	}
	attempt, err := readAttempt(options.attemptPath)
	if err != nil {
		return err
	}
	if !isResolvableImageReference(attempt.WorkerImageDigest) {
		return errors.New("worker image binding is invalid")
	}
	observerOptions, err := readObserverOptions(attempt.Connection.SecretBindings, options)
	if err != nil {
		return err
	}
	observer, err := dependencies.newObserver(observerOptions)
	if err != nil {
		return fmt.Errorf("construct endpoint observer: %w", err)
	}
	return discoverAndPublish(ctx, attempt, options, observer, dependencies.clock, dependencies.publisher)
}

func discoverAndPublish(
	ctx context.Context,
	attempt discovery.Attempt,
	options executableOptions,
	observer discovery.EndpointObserver,
	clock discovery.Clock,
	publisher resultPublisher,
) error {
	worker, err := discovery.NewWorker(observer, clock, options.overallTimeout)
	if err != nil {
		return fmt.Errorf("construct discovery worker: %w", err)
	}
	workerResult, err := worker.Discover(ctx, attempt)
	if err != nil {
		return err
	}
	key, err := readExactSecret(options.hmacKeyPath, resultSigningKeyLen)
	if err != nil {
		return fmt.Errorf("read result signing key: %w", err)
	}
	body, err := signedResultBytes(attempt, workerResult, key)
	if err != nil {
		return err
	}
	if err = publisher.Publish(ctx, options.resultTarget, body); err != nil {
		return fmt.Errorf("publish signed discovery result: %w", err)
	}
	return nil
}

// isResolvableImageReference mirrors the controller-side binding rule: the worker runs whichever
// reference the manager image resolver produced. That is a digest-pinned reference when the
// runtime exposes one, and otherwise the manager's own configured reference. A digest, when
// present, must still be a well-formed SHA-256 digest.
func isResolvableImageReference(value string) bool {
	if value == "" || strings.ContainsAny(value, " \t\r\n") {
		return false
	}
	separator := strings.LastIndexByte(value, '@')
	if separator < 0 {
		return true
	}
	if separator == 0 {
		return false
	}
	digest := value[separator+1:]
	if !strings.HasPrefix(digest, "sha256:") {
		return false
	}
	encoded := strings.TrimPrefix(digest, "sha256:")
	decoded, err := hex.DecodeString(encoded)
	return err == nil && len(encoded) == 64 && len(decoded) == 32 && encoded == strings.ToLower(encoded)
}

func parseOptions(arguments []string) (executableOptions, error) {
	options := executableOptions{}
	flags := flag.NewFlagSet("legacy-rf-discovery", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	flags.StringVar(&options.attemptPath, "attempt", "", "mounted attempt JSON")
	flags.StringVar(&options.hmacKeyPath, "hmac-key", "", "mounted HMAC key")
	flags.StringVar(&options.resultTarget.Namespace, "result-namespace", "", "pre-created result ConfigMap namespace")
	flags.StringVar(&options.resultTarget.Name, "result-configmap", "", "pre-created result ConfigMap name")
	flags.StringVar(&options.resultTarget.Key, "result-key", "", "pre-created result ConfigMap data key")
	flags.StringVar(&options.credentialsDirectory, "credentials-dir", "", "mounted credential directory")
	flags.StringVar(&options.tlsDirectory, "tls-dir", "", "mounted TLS directory")
	flags.DurationVar(&options.connectTimeout, "connect-timeout", 10*time.Second, "per-session connect timeout")
	flags.DurationVar(&options.queryTimeout, "query-timeout", 10*time.Second, "per-query timeout")
	flags.DurationVar(&options.overallTimeout, "overall-timeout", time.Minute, "overall fallback timeout")
	if err := flags.Parse(arguments); err != nil || flags.NArg() != 0 {
		return executableOptions{}, errors.New("worker arguments are invalid")
	}
	if options.attemptPath == "" || options.hmacKeyPath == "" {
		return executableOptions{}, errors.New("required worker paths are missing")
	}
	if err := options.resultTarget.validate(); err != nil {
		return executableOptions{}, err
	}
	if options.connectTimeout <= 0 || options.connectTimeout > time.Minute ||
		options.queryTimeout <= 0 || options.queryTimeout > time.Minute ||
		options.overallTimeout <= 0 || options.overallTimeout > 10*time.Minute {
		return executableOptions{}, errors.New("worker bounds are invalid")
	}
	return options, nil
}

func readAttempt(path string) (discovery.Attempt, error) {
	body, err := readBoundedFile(path, maximumAttemptBytes)
	if err != nil {
		return discovery.Attempt{}, fmt.Errorf("read attempt input: %w", err)
	}
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	var attempt discovery.Attempt
	if err = decoder.Decode(&attempt); err != nil {
		return discovery.Attempt{}, errors.New("attempt input is malformed")
	}
	if err = decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return discovery.Attempt{}, errors.New("attempt input has trailing data")
	}
	return attempt, nil
}

func readObserverOptions(
	bindings []discovery.SecretBinding,
	options executableOptions,
) (discovery.CQLObserverOptions, error) {
	if _, err := discovery.BindingsDigest(bindings); err != nil {
		return discovery.CQLObserverOptions{}, fmt.Errorf("validate qualified Secret bindings: %w", err)
	}
	authBinding, tlsBinding, err := classifyBindings(bindings)
	if err != nil {
		return discovery.CQLObserverOptions{}, err
	}
	observerOptions := discovery.CQLObserverOptions{
		ConnectTimeout: options.connectTimeout, QueryTimeout: options.queryTimeout,
	}
	observerOptions.Credentials, err = readCredentials(authBinding, options.credentialsDirectory)
	if err != nil {
		return discovery.CQLObserverOptions{}, err
	}
	observerOptions.TLSConfig, err = readTLSConfig(tlsBinding, options.tlsDirectory)
	if err != nil {
		return discovery.CQLObserverOptions{}, err
	}
	return observerOptions, nil
}

func classifyBindings(
	bindings []discovery.SecretBinding,
) (authBinding, tlsBinding *discovery.SecretBinding, returnedErr error) {
	for index := range bindings {
		binding := &bindings[index]
		switch binding.Purpose {
		case "auth":
			if authBinding != nil || !equalKeys(binding.Keys, []string{"password", "username"}) {
				return nil, nil, errors.New("credential Secret binding is invalid")
			}
			authBinding = binding
		case "tls":
			if tlsBinding != nil || !validTLSKeys(binding.Keys) {
				return nil, nil, errors.New("TLS Secret binding is invalid")
			}
			tlsBinding = binding
		default:
			return nil, nil, errors.New("secret binding purpose is unsupported")
		}
	}
	return authBinding, tlsBinding, nil
}

func equalKeys(actual, expected []string) bool {
	copyOfActual := append([]string(nil), actual...)
	sort.Strings(copyOfActual)
	return equalSortedStrings(copyOfActual, expected)
}

func validTLSKeys(keys []string) bool {
	if len(keys) == 1 {
		return keys[0] == "ca.crt"
	}
	return len(keys) == 3 && equalStringSets(keys, []string{"ca.crt", "tls.crt", "tls.key"})
}

func equalStringSets(actual, expected []string) bool {
	left, right := append([]string(nil), actual...), append([]string(nil), expected...)
	sort.Strings(left)
	sort.Strings(right)
	return equalSortedStrings(left, right)
}

func equalSortedStrings(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

func readCredentials(
	binding *discovery.SecretBinding,
	directory string,
) (*discovery.CQLCredentials, error) {
	if binding == nil {
		if directory != "" {
			return nil, errors.New("credential mount has no qualified binding")
		}
		return nil, nil
	}
	if directory == "" {
		return nil, errors.New("qualified credential mount is missing")
	}
	username, err := readMountedSecret(directory, "username")
	if err != nil {
		return nil, errors.New("credential mount is invalid")
	}
	password, err := readMountedSecret(directory, "password")
	if err != nil {
		return nil, errors.New("credential mount is invalid")
	}
	if len(username) == 0 || len(password) == 0 {
		return nil, errors.New("credential mount is invalid")
	}
	return &discovery.CQLCredentials{Username: string(username), Password: string(password)}, nil
}

func readTLSConfig(binding *discovery.SecretBinding, directory string) (*tls.Config, error) {
	if binding == nil {
		if directory != "" {
			return nil, errors.New("TLS mount has no qualified binding")
		}
		return nil, nil
	}
	if directory == "" {
		return nil, errors.New("qualified TLS mount is missing")
	}
	caPEM, err := readMountedSecret(directory, "ca.crt")
	if err != nil {
		return nil, errors.New("TLS mount is invalid")
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(caPEM) {
		return nil, errors.New("TLS mount is invalid")
	}
	config := &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12}
	if len(binding.Keys) == 3 {
		certificatePEM, certErr := readMountedSecret(directory, "tls.crt")
		privateKeyPEM, keyErr := readMountedSecret(directory, "tls.key")
		if certErr != nil || keyErr != nil {
			return nil, errors.New("TLS mount is invalid")
		}
		certificate, err := tls.X509KeyPair(certificatePEM, privateKeyPEM)
		if err != nil {
			return nil, errors.New("TLS mount is invalid")
		}
		config.Certificates = []tls.Certificate{certificate}
	}
	return config, nil
}

func readMountedSecret(directory, name string) ([]byte, error) {
	if filepath.Base(directory) == "." || filepath.Base(directory) == string(filepath.Separator) {
		return nil, errors.New("secret mount directory is invalid")
	}
	return readBoundedFile(filepath.Join(directory, name), maximumSecretBytes)
}

func readExactSecret(path string, expectedLength int) ([]byte, error) {
	body, err := readBoundedFile(path, expectedLength)
	if err != nil || len(body) != expectedLength {
		return nil, errors.New("mounted secret has invalid length")
	}
	return body, nil
}

func readBoundedFile(path string, maximumBytes int) (body []byte, returnedErr error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, errors.New("mounted file is unavailable")
	}
	defer func() {
		if err := file.Close(); err != nil && returnedErr == nil {
			returnedErr = errors.New("mounted file cannot be closed")
		}
	}()
	body, err = io.ReadAll(io.LimitReader(file, int64(maximumBytes)+1))
	if err != nil {
		return nil, errors.New("mounted file cannot be read")
	}
	if len(body) > maximumBytes {
		return nil, errors.New("mounted file exceeds its size bound")
	}
	return body, nil
}

func signedResultBytes(
	attempt discovery.Attempt,
	workerResult discovery.WorkerResult,
	key []byte,
) ([]byte, error) {
	result := discovery.DiscoveryResult{
		SchemaVersion: attempt.ProtocolVersion, ClusterUID: attempt.ClusterUID,
		Generation: attempt.Generation, MarkerVersion: attempt.MarkerVersion,
		ProtocolVersion: attempt.ProtocolVersion, AttemptID: attempt.AttemptID,
		OrderedSeeds: attempt.OrderedSeeds, SeedDigest: attempt.SeedDigest,
		SecretBindings: attempt.Connection.SecretBindings, WorkerImageDigest: attempt.WorkerImageDigest,
		Authoritative: workerResult.Authoritative, Failure: workerResult.Failure,
		AttemptTrace: workerResult.AttemptTrace,
	}
	body, err := discovery.SignResult(result, key, api.LegacyRFDiscoveryMaxResultBytes)
	if err != nil {
		return nil, fmt.Errorf("sign discovery result: %w", err)
	}
	return body, nil
}

type resultConfigMapTarget struct {
	Namespace string
	Name      string
	Key       string
}

func (target resultConfigMapTarget) validate() error {
	if len(validation.IsDNS1123Label(target.Namespace)) != 0 ||
		len(validation.IsDNS1123Subdomain(target.Name)) != 0 || target.Key != resultConfigMapKey {
		return errors.New("result ConfigMap binding is invalid")
	}
	return nil
}

type resultPublisher interface {
	Publish(context.Context, resultConfigMapTarget, []byte) error
}

type inClusterResultPublisher struct{}

func (inClusterResultPublisher) Publish(
	ctx context.Context,
	target resultConfigMapTarget,
	body []byte,
) error {
	config, err := ctrl.GetConfig()
	if err != nil {
		return fmt.Errorf("load in-cluster Kubernetes configuration: %w", err)
	}
	scheme := runtime.NewScheme()
	if err = corev1.AddToScheme(scheme); err != nil {
		return fmt.Errorf("register ConfigMap API: %w", err)
	}
	kubernetesClient, err := client.New(config, client.Options{Scheme: scheme})
	if err != nil {
		return fmt.Errorf("construct Kubernetes result client: %w", err)
	}
	return configMapResultPublisher{kubernetesClient: kubernetesClient}.Publish(ctx, target, body)
}

type configMapResultPublisher struct{ kubernetesClient client.Client }

func (publisher configMapResultPublisher) Publish(
	ctx context.Context,
	target resultConfigMapTarget,
	body []byte,
) error {
	if err := target.validate(); err != nil {
		return err
	}
	if publisher.kubernetesClient == nil || len(body) == 0 || len(body) > api.LegacyRFDiscoveryMaxResultBytes {
		return errors.New("result publisher input is invalid")
	}
	configMap := &corev1.ConfigMap{}
	key := types.NamespacedName{Namespace: target.Namespace, Name: target.Name}
	if err := publisher.kubernetesClient.Get(ctx, key, configMap); err != nil {
		return fmt.Errorf("get pre-created result ConfigMap: %w", err)
	}
	if len(configMap.Data) != 0 {
		return errors.New("pre-created result ConfigMap is not empty")
	}
	original := configMap.DeepCopy()
	if configMap.Data == nil {
		configMap.Data = make(map[string]string, 1)
	}
	configMap.Data[target.Key] = string(body)
	if err := publisher.kubernetesClient.Patch(ctx, configMap, client.MergeFrom(original)); err != nil {
		return fmt.Errorf("patch pre-created result ConfigMap: %w", err)
	}
	return nil
}

func sanitizedMessage(err error) string {
	var boundary *discovery.BoundaryError
	if errors.As(err, &boundary) {
		return boundary.Error()
	}
	if errors.Is(err, context.Canceled) {
		return "discovery was canceled"
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return "discovery deadline was exceeded"
	}
	return "worker input or output validation failed"
}
