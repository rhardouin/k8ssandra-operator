package discovery

import (
	"errors"
	"fmt"

	api "github.com/k8ssandra/k8ssandra-operator/apis/k8ssandra/v1alpha1"
)

// BoundaryError retains a private cause while exposing only a stable public failure.
type BoundaryError struct {
	public api.LegacyRFDiscoveryFailure
	cause  error
}

// NewBoundaryError maps a private cause to a stable, sanitized failure contract.
func NewBoundaryError(reason api.LegacyRFDiscoveryReason, cause error) *BoundaryError {
	public, found := api.LegacyRFDiscoveryFailureForReason(reason)
	if !found {
		public, _ = api.LegacyRFDiscoveryFailureForReason(api.LegacyRFReasonInvalidDiscoveryResult)
	}
	if cause == nil {
		cause = errors.New("discovery boundary failed")
	}
	return &BoundaryError{public: public, cause: cause}
}

// Error returns only the sanitized public message.
func (e *BoundaryError) Error() string {
	return e.public.Message
}

// Unwrap exposes the private cause only to internal error inspection.
func (e *BoundaryError) Unwrap() error {
	return e.cause
}

// PublicFailure returns a value that is safe for status and Events.
func (e *BoundaryError) PublicFailure() api.LegacyRFDiscoveryFailure {
	return e.public
}

func normalizeBoundaryError(err error) *BoundaryError {
	var boundary *BoundaryError
	if errors.As(err, &boundary) {
		return boundary
	}
	return NewBoundaryError(api.LegacyRFReasonContactUnreachable,
		fmt.Errorf("endpoint observation failed: %w", err))
}

func exhaustedFailure(failures []attemptFailure) *BoundaryError {
	if len(failures) == 0 {
		return NewBoundaryError(api.LegacyRFReasonInvalidContactPoint,
			errors.New("discovery has no canonical contact points"))
	}
	selected := failures[0]
	for _, failure := range failures[1:] {
		if failurePrecedence(failure.boundary.public.Reason) < failurePrecedence(selected.boundary.public.Reason) {
			selected = failure
		}
	}
	return NewBoundaryError(selected.boundary.public.Reason,
		fmt.Errorf("ordered endpoint fallback exhausted: %w", selected.boundary))
}

func failurePrecedence(reason api.LegacyRFDiscoveryReason) int {
	precedence, found := endpointFailurePrecedence(reason)
	if !found {
		return int(^uint(0) >> 1)
	}
	return precedence
}

// endpointFailurePrecedenceByReason is the authoritative endpoint business-failure set.
// Permanent incompatibilities sort before failures that can be retried.
var endpointFailurePrecedenceByReason = map[api.LegacyRFDiscoveryReason]int{
	api.LegacyRFReasonUnsupportedServerType:          10,
	api.LegacyRFReasonUnsupportedSourceVersion:       20,
	api.LegacyRFReasonAuthorizationDenied:            30,
	api.LegacyRFReasonIdentityMismatch:               40,
	api.LegacyRFReasonManagedDatacenterNameCollision: 50,
	api.LegacyRFReasonMissingKeyspace:                60,
	api.LegacyRFReasonUnsupportedStrategy:            70,
	api.LegacyRFReasonInvalidReplication:             80,
	api.LegacyRFReasonAuthenticationRejected:         100,
	api.LegacyRFReasonTLSFailed:                      110,
	api.LegacyRFReasonSchemaDisagreement:             120,
	api.LegacyRFReasonTopologyInconsistent:           130,
	api.LegacyRFReasonContactUnreachable:             140,
}

func endpointFailurePrecedence(reason api.LegacyRFDiscoveryReason) (int, bool) {
	precedence, found := endpointFailurePrecedenceByReason[reason]
	return precedence, found
}

type attemptFailure struct {
	public   endpointAttemptFailure
	boundary *BoundaryError
}
