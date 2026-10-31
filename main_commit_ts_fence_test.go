package main

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCommitTSFenceStartupRequiresReplicatedCatalogBootstrap(t *testing.T) {
	require.NoError(t, validateCommitTSFenceStartup(false))
	require.ErrorContains(t, validateCommitTSFenceStartup(true), "requires replicated catalog bootstrap")
}

func TestRuntimeInputsRejectCommitTSFenceBeforeOpeningStores(t *testing.T) {
	previous := *commitTSFence
	*commitTSFence = true
	t.Cleanup(func() { *commitTSFence = previous })

	_, _, _, _, err := resolveValidatedRuntimeInputs()
	require.ErrorContains(t, err, "requires replicated catalog bootstrap")
}
