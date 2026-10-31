package main

import (
	"testing"

	"github.com/bootjp/elastickv/distribution"
	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/store"
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

func TestCatalogSetupRejectsPreviouslyActivatedCommitTSFence(t *testing.T) {
	st, err := store.NewPebbleStore(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, st.Close()) })
	fsm := kv.NewKvFSMWithHLC(st, kv.NewHLC())
	require.Nil(t, fsm.Apply(kv.EncodeCommitTSFenceActivation()))

	engine := distribution.NewEngine()
	engine.UpdateRoute(nil, nil, 1)
	_, err = setupDistributionCatalog(t.Context(), []*raftGroupRuntime{{spec: groupSpec{id: 1}, store: st}}, engine)
	require.ErrorContains(t, err, "active commit-ts fence")

	version, err := distribution.NewCatalogStore(st).Version(t.Context())
	require.NoError(t, err)
	require.Zero(t, version)
}
