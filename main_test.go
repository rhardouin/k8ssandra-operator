package main

import (
	"flag"
	"testing"

	api "github.com/k8ssandra/k8ssandra-operator/apis/k8ssandra/v1alpha1"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/runtime"
)

type recordingStructuredEvent struct {
	regarding runtime.Object
	related   runtime.Object
	eventType string
	reason    string
	action    string
	note      string
	args      []interface{}
}

func (r *recordingStructuredEvent) Eventf(
	regarding runtime.Object,
	related runtime.Object,
	eventType, reason, action, note string,
	args ...interface{},
) {
	r.regarding, r.related = regarding, related
	r.eventType, r.reason, r.action, r.note = eventType, reason, action, note
	r.args = args
}

func TestLeaderElectionFlagDefaultsDisabledAndSupportsExplicitEnable(t *testing.T) {
	flags := flag.NewFlagSet("manager", flag.ContinueOnError)
	enabled := false
	bindLeaderElectionFlag(flags, &enabled)
	require.NoError(t, flags.Parse(nil))
	require.False(t, enabled)

	flags = flag.NewFlagSet("manager", flag.ContinueOnError)
	bindLeaderElectionFlag(flags, &enabled)
	require.NoError(t, flags.Parse([]string{"--leader-elect"}))
	require.True(t, enabled)
}

func TestLegacyRFStructuredEventRecorderPreservesEventSemantics(t *testing.T) {
	target := &recordingStructuredEvent{}
	recorder := legacyRFStructuredEventRecorder{recorder: target}
	cluster := &api.K8ssandraCluster{}

	recorder.Event(cluster, "Warning", "TLSFailed", "public 100% message")

	require.Same(t, cluster, target.regarding)
	require.Nil(t, target.related)
	require.Equal(t, "Warning", target.eventType)
	require.Equal(t, "TLSFailed", target.reason)
	require.Equal(t, "LegacyRFDiscovery", target.action)
	require.Equal(t, "%s", target.note)
	require.Equal(t, []interface{}{"public 100% message"}, target.args)
}
