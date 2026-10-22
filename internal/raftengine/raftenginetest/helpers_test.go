package raftenginetest_test

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/bootjp/elastickv/internal/monoclock"
	"github.com/bootjp/elastickv/internal/raftengine"
	etcdengine "github.com/bootjp/elastickv/internal/raftengine/etcd"
	"github.com/bootjp/elastickv/internal/raftengine/raftenginetest"
	"github.com/stretchr/testify/require"
)

func TestExactBaseInterfaces(t *testing.T) {
	for _, tc := range []struct {
		value  any
		target reflect.Type
	}{
		{&raftenginetest.StubEngine{}, reflect.TypeFor[raftengine.Engine]()},
		{&raftenginetest.StaticLeaderView{}, reflect.TypeFor[raftengine.LeaderView]()},
	} {
		require.Equal(t, tc.target.NumMethod(), reflect.TypeOf(tc.value).NumMethod())
		assertNoOptionalCapabilities(t, tc.value)
	}
	assertNoOptionalCapabilities(t, &raftenginetest.RecordingProposer{})
	assertNoOptionalCapabilities(t, raftenginetest.ApplyingProposer(&raftenginetest.TestStateMachine{}))
}

func assertNoOptionalCapabilities(t *testing.T, value any) {
	t.Helper()
	_, lease := value.(raftengine.LeaseProvider)
	_, admin := value.(raftengine.Admin)
	_, health := value.(raftengine.HealthReader)
	_, startup := value.(raftengine.StartupBarrier)
	require.False(t, lease)
	require.False(t, admin)
	require.False(t, health)
	require.False(t, startup)
}

func TestStubEngineConfiguration(t *testing.T) {
	ctx := context.Background()
	sentinel := errors.New("configured error")
	s := &raftenginetest.StubEngine{
		StaticLeaderView: raftenginetest.StaticLeaderView{
			StateValue: raftengine.StateLeader, LeaderValue: raftengine.LeaderInfo{ID: "n1"}, ReadIndex: 17,
		},
		StatusValue:  raftengine.Status{AppliedIndex: 13},
		Config:       raftengine.Configuration{Servers: []raftengine.Server{{ID: "n1"}}},
		SnapshotStep: 23,
		ProposeFunc:  func(context.Context, []byte) (*raftengine.ProposalResult, error) { return nil, sentinel },
	}
	require.Equal(t, raftengine.StateLeader, s.State())
	require.Equal(t, "n1", s.Leader().ID)
	require.NoError(t, s.VerifyLeader(ctx))
	idx, err := s.LinearizableRead(ctx)
	require.NoError(t, err)
	require.Equal(t, uint64(17), idx)
	_, err = s.ProposeAdmin(ctx, nil)
	require.ErrorIs(t, err, sentinel)
	require.Equal(t, uint64(13), s.Status().AppliedIndex)
	require.Equal(t, uint64(23), s.SnapshotEvery())
	cfg, err := s.Configuration(ctx)
	require.NoError(t, err)
	cfg.Servers[0].ID = "changed"
	require.Equal(t, "n1", s.Config.Servers[0].ID)
	s.ReadErr, s.VerifyErr, s.ConfigErr = sentinel, sentinel, sentinel
	_, err = s.LinearizableRead(ctx)
	require.ErrorIs(t, err, sentinel)
	require.ErrorIs(t, s.VerifyLeader(ctx), sentinel)
	_, err = s.Configuration(ctx)
	require.ErrorIs(t, err, sentinel)
	s.VerifyErr = nil
	s.StateValue = raftengine.StateFollower
	require.ErrorIs(t, s.VerifyLeader(ctx), raftengine.ErrNotLeader)
}

func TestProposersOwnBytesAndSerialize(t *testing.T) {
	p := &raftenginetest.RecordingProposer{}
	sm := &raftenginetest.TestStateMachine{}
	applying := raftenginetest.ApplyingProposer(sm)
	ctx := context.Background()
	data := []byte("original")
	_, err := p.Propose(ctx, data)
	require.NoError(t, err)
	data[0] = 'X'
	calls := p.Calls()
	require.Equal(t, []byte("original"), calls[0].Data)
	calls[0].Data[0] = 'Y'
	require.Equal(t, []byte("original"), p.Calls()[0].Data)
	var wg sync.WaitGroup
	for range 20 {
		wg.Go(func() {
			if _, err := p.ProposeAdmin(ctx, []byte("admin")); err != nil {
				t.Error(err)
			}
			if _, err := applying.ProposeAdmin(ctx, []byte("applied")); err != nil {
				t.Error(err)
			}
		})
	}
	wg.Wait()
	require.Len(t, p.Calls(), 21)
	require.True(t, p.Calls()[1].Admin)
	require.Len(t, sm.Applied(), 20)
	result, err := applying.Propose(ctx, []byte("last"))
	require.NoError(t, err)
	require.Equal(t, uint64(21), result.CommitIndex)
	require.Equal(t, "last", result.Response)
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	for _, proposer := range []raftengine.Proposer{p, applying, &raftenginetest.StubEngine{}} {
		_, err = proposer.Propose(cancelled, nil)
		require.ErrorIs(t, err, context.Canceled)
	}
	require.Len(t, p.Calls(), 21)
	require.Len(t, sm.Applied(), 21)
}

func TestLeaseCapabilityIsOptIn(t *testing.T) {
	s := &raftenginetest.StubLeaseEngine{LeaseDurationValue: time.Second, AppliedIndexValue: 7, QuorumAck: monoclock.Now()}
	var callbacks int
	deregister := s.RegisterLeaderLossCallback(func() { callbacks++ })
	s.NotifyLeaderLoss()
	deregister()
	deregister()
	s.NotifyLeaderLoss()
	require.Equal(t, 1, callbacks)
	require.Equal(t, time.Second, s.LeaseDuration())
	require.Equal(t, uint64(7), s.AppliedIndex())
	require.Equal(t, s.QuorumAck, s.LastQuorumAck())
}

func TestNewSingleNodeWithRealFactory(t *testing.T) {
	sm := &raftenginetest.TestStateMachine{}
	factory := etcdengine.NewFactory(etcdengine.FactoryConfig{TickInterval: 10 * time.Millisecond, HeartbeatTick: 1, ElectionTick: 10})
	var result *raftengine.FactoryResult
	t.Run("owner", func(t *testing.T) {
		result = raftenginetest.NewSingleNode(t, factory, sm)
		require.Equal(t, 1, raftenginetest.WaitForLeader(t, time.Second,
			&raftenginetest.StaticLeaderView{StateValue: raftengine.StateFollower}, result.Engine))
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		applied, err := result.Engine.Propose(ctx, []byte("example"))
		require.NoError(t, err)
		require.Equal(t, "example", applied.Response)
	})
	require.Equal(t, raftengine.StateShutdown, result.Engine.State())
}
