package discovery

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/netip"

	api "github.com/k8ssandra/k8ssandra-operator/apis/k8ssandra/v1alpha1"
)

// DiscoveryResult is the authenticated worker result payload.
type DiscoveryResult struct {
	SchemaVersion     string                        `json:"schemaVersion"`
	ClusterUID        string                        `json:"clusterUID"`
	Generation        int64                         `json:"generation"`
	MarkerVersion     string                        `json:"markerVersion"`
	ProtocolVersion   string                        `json:"protocolVersion"`
	AttemptID         string                        `json:"attemptID"`
	OrderedSeeds      []netip.AddrPort              `json:"orderedSeeds"`
	SeedDigest        string                        `json:"seedDigest"`
	SecretBindings    []SecretBinding               `json:"secretBindings,omitempty"`
	WorkerImageDigest string                        `json:"workerImageDigest"`
	Authoritative     *AuthoritativeCandidate       `json:"authoritative,omitempty"`
	Failure           *api.LegacyRFDiscoveryFailure `json:"failure,omitempty"`
	AttemptTrace      []EndpointAttemptSummary      `json:"attemptTrace"`
	CanonicalHash     string                        `json:"canonicalHash"`
}

// ResultEnvelope contains canonical result bytes authenticated with HMAC-SHA-256.
type ResultEnvelope struct {
	Result DiscoveryResult `json:"result"`
	MAC    string          `json:"mac"`
}

// ExpectedResult binds validation to current authoritative controller inputs.
type ExpectedResult struct {
	SchemaVersion     string
	ClusterUID        string
	Generation        int64
	MarkerVersion     string
	ProtocolVersion   string
	AttemptID         string
	OrderedSeeds      []netip.AddrPort
	SeedDigest        string
	SecretBindings    []SecretBinding
	WorkerImageDigest string
	MaximumBytes      int
}

// SignResult canonicalizes, hashes, and authenticates one bounded result envelope.
func SignResult(result DiscoveryResult, key []byte, maximumBytes int) ([]byte, error) {
	if len(key) == 0 {
		return nil, errors.New("result signing key is empty")
	}
	canonical, err := normalizeResult(result)
	if err != nil {
		return nil, fmt.Errorf("canonicalize discovery result: %w", err)
	}
	canonical.CanonicalHash, err = resultHash(canonical)
	if err != nil {
		return nil, err
	}
	payload, err := json.Marshal(canonical)
	if err != nil {
		return nil, fmt.Errorf("encode discovery result: %w", err)
	}
	mac := hmac.New(sha256.New, key)
	if _, err = mac.Write(payload); err != nil {
		return nil, fmt.Errorf("authenticate discovery result: %w", err)
	}
	envelope, err := json.Marshal(ResultEnvelope{
		Result: canonical,
		MAC:    base64.RawStdEncoding.EncodeToString(mac.Sum(nil)),
	})
	if err != nil {
		return nil, fmt.Errorf("encode signed discovery result: %w", err)
	}
	if len(envelope) > boundedResultSize(maximumBytes) {
		return nil, fmt.Errorf("signed discovery result exceeds %d bytes", boundedResultSize(maximumBytes))
	}
	return envelope, nil
}

// ValidateResultEnvelope rejects untrusted bytes before returning a consumable result.
func ValidateResultEnvelope(body, key []byte, expected ExpectedResult) (DiscoveryResult, error) {
	if len(body) > boundedResultSize(expected.MaximumBytes) {
		return DiscoveryResult{}, NewBoundaryError(api.LegacyRFReasonDiscoveryResultTooLarge,
			errors.New("result exceeded configured size bound"))
	}
	if len(key) == 0 {
		return DiscoveryResult{}, NewBoundaryError(api.LegacyRFReasonForgedDiscoveryResult,
			errors.New("result verification key is empty"))
	}
	envelope, err := decodeEnvelope(body)
	if err != nil {
		return DiscoveryResult{}, NewBoundaryError(api.LegacyRFReasonInvalidDiscoveryResult, err)
	}
	canonical, err := normalizeResult(envelope.Result)
	if err != nil {
		return DiscoveryResult{}, NewBoundaryError(api.LegacyRFReasonInvalidDiscoveryResult, err)
	}
	if err = verifyCanonicalEncoding(body, ResultEnvelope{Result: canonical, MAC: envelope.MAC}); err != nil {
		return DiscoveryResult{}, NewBoundaryError(api.LegacyRFReasonInvalidDiscoveryResult, err)
	}
	if err = verifyMAC(canonical, envelope.MAC, key); err != nil {
		return DiscoveryResult{}, NewBoundaryError(api.LegacyRFReasonForgedDiscoveryResult, err)
	}
	if err = verifyResultHash(canonical); err != nil {
		return DiscoveryResult{}, NewBoundaryError(api.LegacyRFReasonInvalidDiscoveryResult, err)
	}
	if err = validateExpectedResult(canonical, expected); err != nil {
		return DiscoveryResult{}, NewBoundaryError(api.LegacyRFReasonStaleDiscoveryResult, err)
	}
	return canonical, nil
}

func decodeEnvelope(body []byte) (ResultEnvelope, error) {
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	var envelope ResultEnvelope
	if err := decoder.Decode(&envelope); err != nil {
		return ResultEnvelope{}, fmt.Errorf("decode result envelope: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return ResultEnvelope{}, errors.New("result envelope has trailing data")
	}
	if envelope.MAC == "" {
		return ResultEnvelope{}, errors.New("result envelope has no MAC")
	}
	return envelope, nil
}

func normalizeResult(result DiscoveryResult) (DiscoveryResult, error) {
	bindings, err := canonicalBindings(result.SecretBindings)
	if err != nil {
		return DiscoveryResult{}, err
	}
	result.SecretBindings = bindings
	result.OrderedSeeds = append([]netip.AddrPort(nil), result.OrderedSeeds...)
	result.AttemptTrace = append([]EndpointAttemptSummary(nil), result.AttemptTrace...)
	if err := validateAttemptTrace(result); err != nil {
		return DiscoveryResult{}, err
	}
	return result, nil
}

func validateAttemptTrace(result DiscoveryResult) error {
	if len(result.AttemptTrace) > MaximumAttemptTraceEntries {
		return fmt.Errorf("attempt trace exceeds %d entries", MaximumAttemptTraceEntries)
	}
	if len(result.AttemptTrace) != len(result.OrderedSeeds) {
		return errors.New("attempt trace must cover every ordered seed")
	}
	if (result.Authoritative == nil) == (result.Failure == nil) {
		return errors.New("discovery result must contain exactly one terminal outcome")
	}
	if result.Failure != nil {
		return validateFailureAttemptTrace(result)
	}
	acceptedIndex := result.Authoritative.AttemptIndex
	for index, summary := range result.AttemptTrace {
		if summary.AttemptIndex != index || summary.Endpoint != result.OrderedSeeds[index] {
			return errors.New("attempt trace endpoint provenance is invalid")
		}
		switch {
		case index < acceptedIndex && summary.Outcome == EndpointAttemptFailed && isEndpointAttemptReason(summary.Reason):
		case index == acceptedIndex && summary.Outcome == EndpointAttemptAccepted && summary.Reason == "":
		case index > acceptedIndex && summary.Outcome == EndpointAttemptSkipped && summary.Reason == "":
		default:
			return errors.New("attempt trace outcome is invalid")
		}
	}
	return nil
}

func validateFailureAttemptTrace(result DiscoveryResult) error {
	expectedFailure, found := api.LegacyRFDiscoveryFailureForReason(result.Failure.Reason)
	if !found || *result.Failure != expectedFailure {
		return errors.New("terminal discovery failure is invalid")
	}
	if result.Failure.Reason == api.LegacyRFReasonDiscoveryDeadlineExceeded {
		return validateDeadlineAttemptTrace(result)
	}
	if len(result.AttemptTrace) != len(result.OrderedSeeds) {
		return errors.New("terminal failure trace must cover every ordered seed")
	}
	internal := make([]attemptFailure, len(result.AttemptTrace))
	for index, summary := range result.AttemptTrace {
		if summary.AttemptIndex != index || summary.Endpoint != result.OrderedSeeds[index] ||
			summary.Outcome != EndpointAttemptFailed {
			return errors.New("terminal failure trace provenance is invalid")
		}
		if _, found := endpointFailurePrecedence(summary.Reason); !found {
			return errors.New("terminal failure trace reason is invalid")
		}
		internal[index] = attemptFailure{
			public:   endpointAttemptFailure{AttemptIndex: index, Endpoint: summary.Endpoint, Reason: summary.Reason},
			boundary: NewBoundaryError(summary.Reason, errors.New("endpoint attempt failed")),
		}
	}
	if exhaustedFailure(internal).PublicFailure().Reason != result.Failure.Reason {
		return errors.New("terminal discovery failure does not match endpoint precedence")
	}
	return nil
}

func validateDeadlineAttemptTrace(result DiscoveryResult) error {
	deadlineSeen := false
	for index, summary := range result.AttemptTrace {
		if summary.AttemptIndex != index || summary.Endpoint != result.OrderedSeeds[index] {
			return errors.New("deadline failure trace provenance is invalid")
		}
		switch {
		case !deadlineSeen && summary.Outcome == EndpointAttemptFailed && isEndpointAttemptReason(summary.Reason):
		case !deadlineSeen && summary.Outcome == EndpointAttemptFailed && summary.Reason == api.LegacyRFReasonDiscoveryDeadlineExceeded:
			deadlineSeen = true
		case deadlineSeen && summary.Outcome == EndpointAttemptSkipped && summary.Reason == "":
		default:
			return errors.New("deadline failure trace outcome is invalid")
		}
	}
	if !deadlineSeen {
		return errors.New("deadline failure trace has no deadline outcome")
	}
	return nil
}

func isEndpointAttemptReason(reason api.LegacyRFDiscoveryReason) bool {
	_, found := endpointFailurePrecedence(reason)
	return found
}

func verifyCanonicalEncoding(body []byte, envelope ResultEnvelope) error {
	canonical, err := json.Marshal(envelope)
	if err != nil {
		return fmt.Errorf("encode canonical result envelope: %w", err)
	}
	if !bytes.Equal(body, canonical) {
		return errors.New("result envelope is not canonically encoded")
	}
	return nil
}

func verifyMAC(result DiscoveryResult, encodedMAC string, key []byte) error {
	provided, err := base64.RawStdEncoding.DecodeString(encodedMAC)
	if err != nil {
		return errors.New("result MAC is malformed")
	}
	payload, err := json.Marshal(result)
	if err != nil {
		return fmt.Errorf("encode result for MAC verification: %w", err)
	}
	expected := hmac.New(sha256.New, key)
	if _, err = expected.Write(payload); err != nil {
		return fmt.Errorf("compute result MAC: %w", err)
	}
	if !hmac.Equal(provided, expected.Sum(nil)) {
		return errors.New("result MAC does not match")
	}
	return nil
}

func verifyResultHash(result DiscoveryResult) error {
	expected, err := resultHash(result)
	if err != nil {
		return err
	}
	if !hmac.Equal([]byte(result.CanonicalHash), []byte(expected)) {
		return errors.New("canonical result hash does not match")
	}
	return nil
}

// ValidateResultHash recomputes the canonical content hash of a validated result value.
func ValidateResultHash(result DiscoveryResult) error {
	canonical, err := normalizeResult(result)
	if err != nil {
		return err
	}
	return verifyResultHash(canonical)
}

func resultHash(result DiscoveryResult) (string, error) {
	result.CanonicalHash = ""
	encoded, err := json.Marshal(result)
	if err != nil {
		return "", fmt.Errorf("encode canonical result hash input: %w", err)
	}
	return digestBytes(encoded), nil
}

func validateExpectedResult(result DiscoveryResult, expected ExpectedResult) error {
	if result.SchemaVersion != expected.SchemaVersion || result.ClusterUID != expected.ClusterUID ||
		result.Generation != expected.Generation || result.MarkerVersion != expected.MarkerVersion ||
		result.ProtocolVersion != expected.ProtocolVersion || result.AttemptID != expected.AttemptID {
		return errors.New("result identity binding is stale")
	}
	if result.SeedDigest != expected.SeedDigest || !equalSeeds(result.OrderedSeeds, expected.OrderedSeeds) {
		return errors.New("result ordered-seed binding is stale")
	}
	bindingsMatch, err := equalBindings(result.SecretBindings, expected.SecretBindings)
	if err != nil {
		return fmt.Errorf("compare result Secret bindings: %w", err)
	}
	if !bindingsMatch || result.WorkerImageDigest != expected.WorkerImageDigest {
		return errors.New("result Secret or worker binding is stale")
	}
	if result.Authoritative != nil && (result.Authoritative.AttemptID != result.AttemptID ||
		result.Authoritative.AttemptIndex < 0 ||
		result.Authoritative.AttemptIndex >= len(result.OrderedSeeds) ||
		result.Authoritative.Endpoint != result.OrderedSeeds[result.Authoritative.AttemptIndex]) {
		return errors.New("authoritative endpoint provenance is stale")
	}
	return nil
}

func equalSeeds(left, right []netip.AddrPort) bool {
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

func boundedResultSize(configured int) int {
	if configured <= 0 || configured > api.LegacyRFDiscoveryMaxResultBytes {
		return api.LegacyRFDiscoveryMaxResultBytes
	}
	return configured
}
