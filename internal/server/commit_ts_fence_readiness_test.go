package server

import (
	"context"
	"testing"
	"time"

	"github.com/bootjp/elastickv/internal/clusterbootstrap"
	"github.com/bootjp/elastickv/internal/noderuntime"
	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
)

func TestCommitTSFenceRuntimesReady(t *testing.T) {
	domain := &clusterbootstrap.BootstrapDomain{}
	ready := func() bool { return true }
	blocked := func() bool { return false }
	cases := []struct {
		name     string
		runtimes []*clusterbootstrap.GroupRuntime
		want     bool
	}{
		{name: "no runtimes"},
		{name: "nil runtime", runtimes: []*clusterbootstrap.GroupRuntime{nil}},
		{name: "legacy runtime", runtimes: []*clusterbootstrap.GroupRuntime{{GroupRuntime: &noderuntime.GroupRuntime{PublicReady: ready}}}},
		{name: "missing callback", runtimes: []*clusterbootstrap.GroupRuntime{{GroupRuntime: &noderuntime.GroupRuntime{}, BootstrapDomain: domain}}},
		{name: "not ready", runtimes: []*clusterbootstrap.GroupRuntime{{GroupRuntime: &noderuntime.GroupRuntime{PublicReady: blocked}, BootstrapDomain: domain}}},
		{name: "mixed domains", runtimes: []*clusterbootstrap.GroupRuntime{{GroupRuntime: &noderuntime.GroupRuntime{PublicReady: ready}, BootstrapDomain: domain}, {GroupRuntime: &noderuntime.GroupRuntime{PublicReady: ready}, BootstrapDomain: &clusterbootstrap.BootstrapDomain{}}}},
		{name: "missing group", runtimes: []*clusterbootstrap.GroupRuntime{{GroupRuntime: &noderuntime.GroupRuntime{PublicReady: ready}, BootstrapDomain: domain}, nil}},
		{name: "all protected groups ready", runtimes: []*clusterbootstrap.GroupRuntime{{GroupRuntime: &noderuntime.GroupRuntime{PublicReady: ready}, BootstrapDomain: domain}, {GroupRuntime: &noderuntime.GroupRuntime{PublicReady: ready}, BootstrapDomain: domain}}, want: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, commitTSFenceRuntimesReady(tc.runtimes))
		})
	}
}

func TestPublicGateWaitsForEveryCommitTSFenceActivation(t *testing.T) {
	runtimes := make([]*clusterbootstrap.GroupRuntime, 2)
	fsms := make([]kv.FSM, 2)
	for i := range runtimes {
		st, err := store.NewPebbleStore(t.TempDir())
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, st.Close()) })
		runtimes[i] = &clusterbootstrap.GroupRuntime{GroupRuntime: &noderuntime.GroupRuntime{Spec: noderuntime.GroupSpec{ID: uint64(i + 1)}, Store: st}}
		fsms[i] = kv.NewKvFSMWithHLC(st, kv.NewHLC())
	}
	gate := &clusterbootstrap.StartupPublicKVGate{}
	active, err := commitTSFenceRuntimesActive(t.Context(), runtimes)
	require.NoError(t, err)
	require.False(t, active)
	require.Nil(t, fsms[0].Apply(kv.EncodeCommitTSFenceActivation()))
	active, err = commitTSFenceRuntimesActive(t.Context(), runtimes)
	require.NoError(t, err)
	require.False(t, active, "one active group cannot open public traffic")

	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- markPublicGateReadyAfterCommitTSFence(ctx, gate, runtimes, true) }()
	select {
	case err := <-done:
		t.Fatalf("gate opened before all groups activated: %v", err)
	case <-time.After(2 * clusterbootstrap.BootstrapReadinessRetryInterval):
	}
	require.True(t, gate.Blocked())
	require.Nil(t, fsms[1].Apply(kv.EncodeCommitTSFenceActivation()))
	require.NoError(t, <-done)
	require.False(t, gate.Blocked())
}

func TestCommitTSFenceActivationWaitFailsClosed(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	require.ErrorIs(t, waitForCommitTSFenceActivation(ctx, nil), context.Canceled)
	require.ErrorIs(t, waitForCommitTSFenceActivation(ctx, []*clusterbootstrap.GroupRuntime{{GroupRuntime: &noderuntime.GroupRuntime{}}}), context.Canceled)
	gate := &clusterbootstrap.StartupPublicKVGate{}
	require.ErrorIs(t, markPublicGateReadyAfterCommitTSFence(ctx, gate, nil, true), context.Canceled)
	require.True(t, gate.Blocked())
	require.NoError(t, markPublicGateReadyAfterCommitTSFence(ctx, gate, nil, false))
	require.False(t, gate.Blocked())
}

// The dedicated TSO group never applies the KV activation entry: its runtime
// store is the TSO control store and its ShardGroup has no KV Store, so the
// coordinator never proposes the marker there. Readiness must only wait for
// the KV data groups, but it must still require at least one of them.
func TestCommitTSFenceReadinessIgnoresDedicatedTSOGroup(t *testing.T) {
	newRuntime := func(id uint64) (*clusterbootstrap.GroupRuntime, kv.FSM) {
		st, err := store.NewPebbleStore(t.TempDir())
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, st.Close()) })
		return &clusterbootstrap.GroupRuntime{GroupRuntime: &noderuntime.GroupRuntime{Spec: noderuntime.GroupSpec{ID: id}, Store: st}}, kv.NewKvFSMWithHLC(st, kv.NewHLC())
	}
	tso, _ := newRuntime(noderuntime.DedicatedTSORaftGroupID)
	data, dataFSM := newRuntime(1)

	active, err := commitTSFenceRuntimesActive(t.Context(), []*clusterbootstrap.GroupRuntime{tso})
	require.NoError(t, err)
	require.False(t, active, "a TSO-only node has no KV data group to fence")

	runtimes := []*clusterbootstrap.GroupRuntime{tso, data}
	active, err = commitTSFenceRuntimesActive(t.Context(), runtimes)
	require.NoError(t, err)
	require.False(t, active)

	require.Nil(t, dataFSM.Apply(kv.EncodeCommitTSFenceActivation()))
	active, err = commitTSFenceRuntimesActive(t.Context(), runtimes)
	require.NoError(t, err)
	require.True(t, active, "the TSO control store never carries the KV marker")

	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	gate := &clusterbootstrap.StartupPublicKVGate{}
	require.NoError(t, markPublicGateReadyAfterCommitTSFence(ctx, gate, runtimes, true))
	require.False(t, gate.Blocked())
}
