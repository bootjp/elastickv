package main

import (
	"context"
	"testing"
	"time"

	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
)

func TestCommitTSFenceRuntimesReady(t *testing.T) {
	domain := &bootstrapDomain{}
	ready := func() bool { return true }
	blocked := func() bool { return false }
	cases := []struct {
		name     string
		runtimes []*raftGroupRuntime
		want     bool
	}{
		{name: "no runtimes"},
		{name: "nil runtime", runtimes: []*raftGroupRuntime{nil}},
		{name: "legacy runtime", runtimes: []*raftGroupRuntime{{publicReady: ready}}},
		{name: "missing callback", runtimes: []*raftGroupRuntime{{bootstrapDomain: domain}}},
		{name: "not ready", runtimes: []*raftGroupRuntime{{bootstrapDomain: domain, publicReady: blocked}}},
		{name: "mixed domains", runtimes: []*raftGroupRuntime{{bootstrapDomain: domain, publicReady: ready}, {bootstrapDomain: &bootstrapDomain{}, publicReady: ready}}},
		{name: "missing group", runtimes: []*raftGroupRuntime{{bootstrapDomain: domain, publicReady: ready}, nil}},
		{name: "all protected groups ready", runtimes: []*raftGroupRuntime{{bootstrapDomain: domain, publicReady: ready}, {bootstrapDomain: domain, publicReady: ready}}, want: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, commitTSFenceRuntimesReady(tc.runtimes))
		})
	}
}

func TestPublicGateWaitsForEveryCommitTSFenceActivation(t *testing.T) {
	runtimes := make([]*raftGroupRuntime, 2)
	fsms := make([]kv.FSM, 2)
	for i := range runtimes {
		st, err := store.NewPebbleStore(t.TempDir())
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, st.Close()) })
		runtimes[i] = &raftGroupRuntime{spec: groupSpec{id: uint64(i + 1)}, store: st}
		fsms[i] = kv.NewKvFSMWithHLC(st, kv.NewHLC())
	}
	gate := &startupPublicKVGate{}
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
	case <-time.After(2 * bootstrapReadinessRetryInterval):
	}
	require.True(t, gate.blocked())
	require.Nil(t, fsms[1].Apply(kv.EncodeCommitTSFenceActivation()))
	require.NoError(t, <-done)
	require.False(t, gate.blocked())
}

func TestCommitTSFenceActivationWaitFailsClosed(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	require.ErrorIs(t, waitForCommitTSFenceActivation(ctx, nil), context.Canceled)
	require.ErrorIs(t, waitForCommitTSFenceActivation(ctx, []*raftGroupRuntime{{}}), context.Canceled)
	gate := &startupPublicKVGate{}
	require.ErrorIs(t, markPublicGateReadyAfterCommitTSFence(ctx, gate, nil, true), context.Canceled)
	require.True(t, gate.blocked())
	require.NoError(t, markPublicGateReadyAfterCommitTSFence(ctx, gate, nil, false))
	require.False(t, gate.blocked())
}

// The dedicated TSO group never applies the KV activation entry: its runtime
// store is the TSO control store and its ShardGroup has no KV Store, so the
// coordinator never proposes the marker there. Readiness must only wait for
// the KV data groups, but it must still require at least one of them.
func TestCommitTSFenceReadinessIgnoresDedicatedTSOGroup(t *testing.T) {
	newRuntime := func(id uint64) (*raftGroupRuntime, kv.FSM) {
		st, err := store.NewPebbleStore(t.TempDir())
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, st.Close()) })
		return &raftGroupRuntime{spec: groupSpec{id: id}, store: st}, kv.NewKvFSMWithHLC(st, kv.NewHLC())
	}
	tso, _ := newRuntime(dedicatedTSORaftGroupID)
	data, dataFSM := newRuntime(1)

	active, err := commitTSFenceRuntimesActive(t.Context(), []*raftGroupRuntime{tso})
	require.NoError(t, err)
	require.False(t, active, "a TSO-only node has no KV data group to fence")

	runtimes := []*raftGroupRuntime{tso, data}
	active, err = commitTSFenceRuntimesActive(t.Context(), runtimes)
	require.NoError(t, err)
	require.False(t, active)

	require.Nil(t, dataFSM.Apply(kv.EncodeCommitTSFenceActivation()))
	active, err = commitTSFenceRuntimesActive(t.Context(), runtimes)
	require.NoError(t, err)
	require.True(t, active, "the TSO control store never carries the KV marker")

	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	gate := &startupPublicKVGate{}
	require.NoError(t, markPublicGateReadyAfterCommitTSFence(ctx, gate, runtimes, true))
	require.False(t, gate.blocked())
}
