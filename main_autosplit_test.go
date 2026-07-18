package main

import (
	"bytes"
	"flag"
	"testing"

	"github.com/bootjp/elastickv/distribution"
	"github.com/bootjp/elastickv/keyviz"
	"github.com/stretchr/testify/require"
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
