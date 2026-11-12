package main

import (
	"flag"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBootstrapRuntimeFlagIsRegistered(t *testing.T) {
	t.Parallel()
	f := flag.Lookup("raftBootstrapPeerConfigDir")
	require.NotNil(t, f)
	getter, ok := f.Value.(flag.Getter)
	require.True(t, ok)
	require.Empty(t, getter.Get())
	require.Empty(t, f.DefValue)
	require.Empty(t, defaultFlags[raftBootstrapFlags]().peerConfigDir)
}
