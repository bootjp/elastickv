package main

import (
	"bytes"
	"context"
	"flag"
	"math"
	"testing"

	"github.com/bootjp/elastickv/distribution"
	"github.com/bootjp/elastickv/distribution/autosplit"
	"github.com/bootjp/elastickv/keyviz"
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

func TestEffectiveKeyVizBucketsPerRouteAutoSplitImpliedDefault(t *testing.T) {
	t.Parallel()

	require.Equal(t, 16, effectiveKeyVizBucketsPerRoute(true, false, keyviz.DefaultKeyBucketsPerRoute, 16))
	require.Equal(t, 1, effectiveKeyVizBucketsPerRoute(true, true, keyviz.DefaultKeyBucketsPerRoute, 16))
	require.Equal(t, 8, effectiveKeyVizBucketsPerRoute(true, false, 8, 16))
	require.Equal(t, 1, effectiveKeyVizBucketsPerRoute(false, false, keyviz.DefaultKeyBucketsPerRoute, 16))
}

func TestValidateAutoSplitSamplerConfigAllowsExplicitBucketsWithUnusedInvalidDefault(t *testing.T) {
	oldAutoSplit := *autoSplit
	oldExplicit := keyvizKeyBucketsPerRouteExplicit
	oldKeyBuckets := *keyvizKeyBucketsPerRoute
	oldDefaultBuckets := *autoSplitDefaultBuckets
	t.Cleanup(func() {
		*autoSplit = oldAutoSplit
		keyvizKeyBucketsPerRouteExplicit = oldExplicit
		*keyvizKeyBucketsPerRoute = oldKeyBuckets
		*autoSplitDefaultBuckets = oldDefaultBuckets
	})

	*autoSplit = true
	*keyvizKeyBucketsPerRoute = 8
	*autoSplitDefaultBuckets = keyviz.DefaultKeyBucketsPerRoute
	keyvizKeyBucketsPerRouteExplicit = true
	require.NoError(t, validateAutoSplitSamplerConfig(autosplit.DefaultConfig()))

	keyvizKeyBucketsPerRouteExplicit = false
	*keyvizKeyBucketsPerRoute = keyviz.DefaultKeyBucketsPerRoute
	require.ErrorContains(t, validateAutoSplitSamplerConfig(autosplit.DefaultConfig()), "--autoSplitDefaultBuckets")
}

func TestSetupDistributionWatcherAndAutoSplitReturnsNilRuntimeWhenDisabled(t *testing.T) {
	oldAutoSplit := *autoSplit
	*autoSplit = false
	t.Cleanup(func() {
		*autoSplit = oldAutoSplit
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

func TestAutoSplitLeaderGateUsesCatalogKeyLeadership(t *testing.T) {
	t.Parallel()

	leader := &fakeAutoSplitKeyLeader{catalogLeader: true}
	gate := autoSplitLeaderGate(leader)

	require.True(t, gate())
	require.True(t, bytes.Equal(distribution.CatalogVersionKey(), leader.lastKey))
	require.False(t, leader.defaultLeaderCalled)
}

func TestAutoSplitLeaderGateFallsBackToDefaultLeader(t *testing.T) {
	t.Parallel()

	leader := &fakeAutoSplitLeader{leader: true}
	gate := autoSplitLeaderGate(leader)

	require.True(t, gate())
	require.True(t, leader.called)
}

func TestValidateAutoSplitDetectorConfigRejectsNonFiniteFloats(t *testing.T) {
	t.Parallel()

	valid := autosplit.DefaultConfig()
	tests := []struct {
		name   string
		mutate func(*autosplit.Config)
	}{
		{name: "write weight NaN", mutate: func(cfg *autosplit.Config) { cfg.WriteWeight = math.NaN() }},
		{name: "read weight positive infinity", mutate: func(cfg *autosplit.Config) { cfg.ReadWeight = math.Inf(1) }},
		{name: "threshold negative infinity", mutate: func(cfg *autosplit.Config) { cfg.ThresholdOpsMin = math.Inf(-1) }},
		{name: "top key share NaN", mutate: func(cfg *autosplit.Config) { cfg.TopKeyShare = math.NaN() }},
		{name: "absolute floor positive infinity", mutate: func(cfg *autosplit.Config) { cfg.TopKeyAbsoluteFloor = math.Inf(1) }},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			cfg := valid
			tc.mutate(&cfg)
			require.Error(t, validateAutoSplitDetectorConfig(cfg))
		})
	}
}

type fakeAutoSplitLeader struct {
	leader bool
	called bool
}

func (f *fakeAutoSplitLeader) IsLeader() bool {
	f.called = true
	return f.leader
}

type fakeAutoSplitKeyLeader struct {
	catalogLeader       bool
	defaultLeaderCalled bool
	lastKey             []byte
}

func (f *fakeAutoSplitKeyLeader) IsLeader() bool {
	f.defaultLeaderCalled = true
	return false
}

func (f *fakeAutoSplitKeyLeader) IsLeaderForKey(key []byte) bool {
	f.lastKey = append(f.lastKey[:0], key...)
	return f.catalogLeader
}
