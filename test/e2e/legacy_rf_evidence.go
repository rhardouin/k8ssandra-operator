package e2e

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"
)

const legacyRFEvidenceMaximumBytes = 1 << 20

var legacyRFEvidenceScenarioPattern = regexp.MustCompile(`^LegacyRFDiscoveryFrom(4\.0|4\.1|5\.0)Cluster$`)

var legacyRFScenarioVersions = map[string]string{
	"LegacyRFDiscoveryFrom4.0Cluster": "4.0.17",
	"LegacyRFDiscoveryFrom4.1Cluster": "4.1.9",
	"LegacyRFDiscoveryFrom5.0Cluster": "5.0.6",
}

// legacyRFEvidence is the sanitized, deterministic record emitted by a Cassandra lifecycle run.
// It deliberately records proof boundaries next to positive observations so that the artifact
// cannot be mistaken for ring, streaming, repair, ownership, or availability evidence.
type legacyRFEvidence struct {
	Scenario           string                      `json:"scenario"`
	SourceVersion      string                      `json:"sourceVersion"`
	TargetVersion      string                      `json:"targetVersion"`
	SourceImage        string                      `json:"sourceImage"`
	TargetImage        string                      `json:"targetImage"`
	AttemptedEndpoints []legacyRFEndpointEvidence  `json:"attemptedEndpoints"`
	AcceptedEndpoint   string                      `json:"acceptedEndpoint"`
	SkippedEndpoints   []string                    `json:"skippedEndpoints"`
	CQLRows            map[string]json.RawMessage  `json:"cqlRows"`
	Snapshot           json.RawMessage             `json:"snapshot"`
	SnapshotHash       string                      `json:"snapshotHash"`
	Jobs               []json.RawMessage           `json:"jobs"`
	Pods               []json.RawMessage           `json:"pods"`
	Events             []json.RawMessage           `json:"events"`
	Conditions         []json.RawMessage           `json:"conditions"`
	Operations         []legacyRFOperationEvidence `json:"operations"`
	Timings            map[string]time.Duration    `json:"timings"`
	DoesNotProve       []string                    `json:"doesNotProve"`
	ValidationError    string                      `json:"validationError,omitempty"`
}

type legacyRFEndpointEvidence struct {
	Endpoint string `json:"endpoint"`
	Outcome  string `json:"outcome"`
	Reason   string `json:"reason,omitempty"`
}

type legacyRFOperationEvidence struct {
	At        time.Time `json:"at"`
	Operation string    `json:"operation"`
	Keyspace  string    `json:"keyspace,omitempty"`
	Outcome   string    `json:"outcome"`
}

func newLegacyRFEvidence(scenario, version, sourceImage, targetImage string) *legacyRFEvidence {
	return &legacyRFEvidence{
		Scenario: scenario, SourceVersion: version, TargetVersion: version,
		SourceImage: sourceImage, TargetImage: targetImage,
		CQLRows: make(map[string]json.RawMessage), Timings: make(map[string]time.Duration),
		DoesNotProve: []string{
			"skipped-seed identity or state", "authoritative liveness or complete ring membership",
			"data ownership", "streaming completion", "repair completion", "data availability",
		},
	}
}

func (e *legacyRFEvidence) validate() error {
	if e == nil {
		return errors.New("legacy RF evidence is required")
	}
	expectedVersion, found := legacyRFScenarioVersions[e.Scenario]
	if !found {
		return fmt.Errorf("unsupported legacy RF evidence scenario %q", e.Scenario)
	}
	if e.SourceVersion != expectedVersion || e.TargetVersion != expectedVersion || e.SourceImage == "" || e.TargetImage == "" {
		return fmt.Errorf("scenario %s requires source and target version %s and resolved images", e.Scenario, expectedVersion)
	}
	if err := validateLegacyRFEndpoints(e); err != nil {
		return err
	}
	if !hasExactLegacyRFCQLRows(e.CQLRows) || len(e.Snapshot) == 0 || e.SnapshotHash == "" {
		return errors.New("three CQL rows and an accepted snapshot/hash are required")
	}
	if !hasExactLegacyRFProofBoundaries(e.DoesNotProve) {
		return errors.New("all live-proof boundaries are required")
	}
	return nil
}

func validateLegacyRFEndpoints(evidence *legacyRFEvidence) error {
	accepted := false
	attempted := make(map[string]struct{}, len(evidence.AttemptedEndpoints))
	for _, attempt := range evidence.AttemptedEndpoints {
		if attempt.Endpoint == "" || attempt.Outcome == "" {
			return errors.New("endpoint evidence requires endpoint and outcome")
		}
		attempted[attempt.Endpoint] = struct{}{}
		accepted = accepted || attempt.Endpoint == evidence.AcceptedEndpoint && attempt.Outcome == "accepted"
	}
	if !accepted {
		return errors.New("accepted endpoint must have an accepted attempt")
	}
	for _, skipped := range evidence.SkippedEndpoints {
		if skipped == "" {
			return errors.New("skipped endpoint cannot be empty")
		}
		if _, found := attempted[skipped]; found {
			return errors.New("skipped endpoint cannot also be attempted")
		}
	}
	return nil
}

func hasExactLegacyRFCQLRows(rows map[string]json.RawMessage) bool {
	if len(rows) != 3 {
		return false
	}
	for _, keyspace := range []string{"system_auth", "system_traces", "system_distributed"} {
		if len(rows[keyspace]) == 0 || !json.Valid(rows[keyspace]) {
			return false
		}
	}
	return true
}

func hasExactLegacyRFProofBoundaries(boundaries []string) bool {
	required := newLegacyRFEvidence("", "", "", "").DoesNotProve
	if len(boundaries) != len(required) {
		return false
	}
	actual := make(map[string]struct{}, len(boundaries))
	for _, boundary := range boundaries {
		actual[boundary] = struct{}{}
	}
	for _, boundary := range required {
		if _, found := actual[boundary]; !found {
			return false
		}
	}
	return true
}

func writeLegacyRFEvidence(root string, evidence *legacyRFEvidence) error {
	validationErr := evidence.validate()
	if evidence == nil || !legacyRFEvidenceScenarioPattern.MatchString(evidence.Scenario) {
		return fmt.Errorf("validate lifecycle evidence: %w", validationErr)
	}
	sanitized, err := sanitizeLegacyRFEvidence(evidence)
	if err != nil {
		return err
	}
	if validationErr != nil {
		sanitized.ValidationError = validationErr.Error()
	}
	if err = persistLegacyRFEvidence(root, sanitized); err != nil {
		return err
	}
	if validationErr != nil {
		return fmt.Errorf("validate lifecycle evidence: %w", validationErr)
	}
	return nil
}

func persistLegacyRFEvidence(root string, evidence *legacyRFEvidence) error {
	body, err := json.MarshalIndent(evidence, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal lifecycle evidence: %w", err)
	}
	oversized := len(body) > legacyRFEvidenceMaximumBytes
	if oversized {
		evidence.Jobs, evidence.Pods, evidence.Events, evidence.Conditions = nil, nil, nil, nil
		evidence.Operations, evidence.CQLRows, evidence.Snapshot = nil, nil, nil
		evidence.ValidationError = fmt.Sprintf("artifact exceeded %d bytes and verbose fields were omitted", legacyRFEvidenceMaximumBytes)
		body, err = json.MarshalIndent(evidence, "", "  ")
		if err != nil {
			return fmt.Errorf("marshal bounded lifecycle evidence: %w", err)
		}
	}
	if len(body) > legacyRFEvidenceMaximumBytes {
		return fmt.Errorf("lifecycle evidence exceeds %d bytes", legacyRFEvidenceMaximumBytes)
	}
	directory := filepath.Join(root, evidence.Scenario)
	if err = os.MkdirAll(directory, 0o750); err != nil {
		return fmt.Errorf("create lifecycle evidence directory: %w", err)
	}
	path := filepath.Join(directory, "legacy-rf-evidence.json")
	if err = os.WriteFile(path, append(body, '\n'), 0o600); err != nil {
		return fmt.Errorf("write lifecycle evidence: %w", err)
	}
	if oversized {
		return fmt.Errorf("lifecycle evidence exceeds %d bytes", legacyRFEvidenceMaximumBytes)
	}
	return nil
}

func sanitizeLegacyRFEvidence(evidence *legacyRFEvidence) (*legacyRFEvidence, error) {
	body, err := json.Marshal(evidence)
	if err != nil {
		return nil, fmt.Errorf("marshal lifecycle evidence for sanitization: %w", err)
	}
	var value any
	if err = json.Unmarshal(body, &value); err != nil {
		return nil, fmt.Errorf("decode lifecycle evidence for sanitization: %w", err)
	}
	redacted := redactLegacyRFValue("", value)
	sanitizedBody, err := json.Marshal(redacted)
	if err != nil {
		return nil, fmt.Errorf("marshal sanitized lifecycle evidence: %w", err)
	}
	var sanitized legacyRFEvidence
	if err = json.Unmarshal(sanitizedBody, &sanitized); err != nil {
		return nil, fmt.Errorf("decode sanitized lifecycle evidence: %w", err)
	}
	sort.SliceStable(sanitized.Operations, func(i, j int) bool {
		return sanitized.Operations[i].At.Before(sanitized.Operations[j].At)
	})
	return &sanitized, nil
}

func redactLegacyRFValue(key string, value any) any {
	if legacyRFSensitiveKey(key) {
		return "[REDACTED]"
	}
	switch typed := value.(type) {
	case map[string]any:
		for childKey, child := range typed {
			typed[childKey] = redactLegacyRFValue(childKey, child)
		}
	case []any:
		for index := range typed {
			typed[index] = redactLegacyRFValue(key, typed[index])
		}
	case string:
		if legacyRFSecretCanary(typed) {
			return "[REDACTED]"
		}
	}
	return value
}

func legacyRFSensitiveKey(key string) bool {
	normalized := strings.ToLower(strings.ReplaceAll(key, "_", ""))
	for _, fragment := range []string{"password", "privatekey", "tlskey", "secretdata", "authpayload"} {
		if strings.Contains(normalized, fragment) {
			return true
		}
	}
	return false
}

func legacyRFSecretCanary(value string) bool {
	normalized := strings.ToLower(value)
	return strings.Contains(normalized, "-----begin private key-----") ||
		strings.Contains(normalized, "legacy-rf-secret-canary") ||
		strings.Contains(normalized, "password=")
}
