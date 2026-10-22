package main

import (
	"context"
	"flag"
	"testing"

	"github.com/bootjp/elastickv/distribution"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

func TestAutoSplitCanonicalFlagsAreRegistered(t *testing.T) {
	t.Parallel()

	for _, name := range []string{"autoSplitCooldown", "autoSplitThreshold", "autoSplitMaxPerCycle"} {
		require.NotNil(t, flag.CommandLine.Lookup(name), name)
	}
	for _, name := range []string{"autoSplitSplitCooldown", "autoSplitThresholdOpsMin", "autoSplitMaxSplitsPerCycle"} {
		require.Nil(t, flag.CommandLine.Lookup(name), name)
	}
}

func TestSetupDistributionWatcherAndAutoSplitReturnsNilRuntimeWhenDisabled(t *testing.T) {
	oldAutoSplit := *autoSplitFlags.Enabled
	*autoSplitFlags.Enabled = false
	t.Cleanup(func() {
		*autoSplitFlags.Enabled = oldAutoSplit
	})

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	catalog := distribution.NewCatalogStore(store.NewMVCCStore())
	_, err := catalog.Save(ctx, 0, []distribution.RouteDescriptor{{
		RouteID: 1,
		Start:   []byte(""),
		End:     nil,
		GroupID: 1,
		State:   distribution.RouteStateActive,
	}})
	require.NoError(t, err)

	var eg errgroup.Group
	runtime, err := setupDistributionWatcherAndAutoSplit(ctx, &eg, catalog, distribution.NewEngine(), nil, nil, nil, nil)
	require.NoError(t, err)
	require.Nil(t, runtime)

	cancel()
	require.NoError(t, eg.Wait())
}
