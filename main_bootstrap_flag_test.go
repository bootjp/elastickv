package main

import (
	"flag"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBootstrapRuntimeFlagIsRegistered(t *testing.T) {
	f := flag.Lookup("raftBootstrapPeerConfigDir")
	require.NotNil(t, f)
	getter, ok := f.Value.(flag.Getter)
	require.True(t, ok)
	require.Equal(t, *raftBootstrapPeerConfigDir, getter.Get())
	require.Empty(t, f.DefValue)
}
