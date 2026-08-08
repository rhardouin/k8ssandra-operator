package e2e

import (
	"context"
	"errors"
	"testing"
	"time"

	cassdcapi "github.com/k8ssandra/cass-operator/apis/cassandra/v1beta1"
	"github.com/stretchr/testify/require"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type sequencedLegacyRFCreate struct {
	errors  []error
	calls   int
	objects []client.Object
	options []client.CreateOptions
}

func (c *sequencedLegacyRFCreate) Create(_ context.Context, object client.Object, opts ...client.CreateOption) error {
	c.calls++
	c.objects = append(c.objects, object)
	options := client.CreateOptions{}
	options.ApplyOptions(opts)
	c.options = append(c.options, options)
	if c.calls <= len(c.errors) {
		return c.errors[c.calls-1]
	}
	return nil
}

func TestWaitForLegacyRFWebhookReadyRetriesOnlyTransientAdmissionTransportErrors(t *testing.T) {
	dc := &cassdcapi.CassandraDatacenter{ObjectMeta: metav1.ObjectMeta{Name: "legacy-a", Namespace: "test"}}
	transient := errors.New(`Internal error occurred: failed calling webhook "mcassandradatacenter.kb.io": dial tcp 10.96.0.10:443: connect: connection refused`)

	singleCreate := &sequencedLegacyRFCreate{errors: []error{transient, nil}}
	require.ErrorIs(t, singleCreate.Create(context.Background(), dc), transient)
	require.Equal(t, 1, singleCreate.calls, "a single create cannot cross transient webhook unavailability")

	retryingCreate := &sequencedLegacyRFCreate{errors: []error{transient, nil}}
	require.NoError(t, waitForLegacyRFCassandraDatacenterWebhook(
		context.Background(), retryingCreate, dc, time.Second, time.Millisecond,
	))
	require.Equal(t, 2, retryingCreate.calls)
	for index := range retryingCreate.options {
		require.Equal(t, []string{metav1.DryRunAll}, retryingCreate.options[index].DryRun)
		require.Same(t, dc, retryingCreate.objects[index])
	}
}

func TestWaitForLegacyRFWebhookReadyRetriesNoEndpointsAndStopsOnOtherErrors(t *testing.T) {
	dc := &cassdcapi.CassandraDatacenter{ObjectMeta: metav1.ObjectMeta{Name: "legacy-a", Namespace: "test"}}
	noEndpoints := errors.New(`failed calling webhook "mcassandradatacenter.kb.io": no endpoints available for service "cass-operator-webhook-service"`)
	validation := apierrors.NewInvalid(
		schema.GroupKind{Group: "cassandra.datastax.com", Kind: "CassandraDatacenter"},
		dc.Name,
		field.ErrorList{field.Invalid(field.NewPath("spec"), nil, "invalid test value")},
	)
	creator := &sequencedLegacyRFCreate{errors: []error{noEndpoints, validation, nil}}

	err := waitForLegacyRFCassandraDatacenterWebhook(
		context.Background(), creator, dc, time.Second, time.Millisecond,
	)
	require.ErrorIs(t, err, validation)
	require.Equal(t, 2, creator.calls, "non-transient validation errors must stop polling")
	for index := range creator.options {
		require.Equal(t, []string{metav1.DryRunAll}, creator.options[index].DryRun)
	}
}
