package main

import (
	"flag"
	"testing"

	"github.com/stretchr/testify/require"
)

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
