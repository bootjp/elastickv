package main

import (
	"context"
	"flag"
	"testing"

	"github.com/bootjp/elastickv/adapter"
	"github.com/bootjp/elastickv/distribution"
	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/bootjp/elastickv/kv"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type startupWatermarkEngine struct {
	capabilityConfigEngine
	payload []byte
	index   uint64
	err     error
}

func (*startupWatermarkEngine) State() raftengine.State { return raftengine.StateLeader }

func (e *startupWatermarkEngine) ProposeAdmin(_ context.Context, payload []byte) (*raftengine.ProposalResult, error) {
	e.payload = append([]byte(nil), payload...)
	return &raftengine.ProposalResult{CommitIndex: e.index}, e.err
}

func TestNewStartupShardStoreReadWatermarkOption(t *testing.T) {
	t.Parallel()
	for _, enabled := range []bool{false, true} {
		t.Run(map[bool]string{false: "disabled", true: "enabled"}[enabled], func(t *testing.T) {
			t.Parallel()
			engine := distribution.NewEngine()
			require.NoError(t, engine.ApplySnapshot(distribution.CatalogSnapshot{
				Version: 1,
				Routes: []distribution.RouteDescriptor{
					{RouteID: 1, Start: []byte("a"), End: []byte("b"), GroupID: 1, State: distribution.RouteStateActive},
					{RouteID: 2, Start: []byte("b"), GroupID: 2, State: distribution.RouteStateActive},
				},
			}))
			leading, lagging := store.NewMVCCStore(), store.NewMVCCStore()
			t.Cleanup(func() { require.NoError(t, leading.Close()) })
			t.Cleanup(func() { require.NoError(t, lagging.Close()) })
			require.NoError(t, leading.ApplyMutationsRaftAt(t.Context(), nil, nil, 30, 30, 1))
			require.NoError(t, lagging.PutAt(t.Context(), []byte("b-key"), []byte("value"), 10, 0))
			proposalErr := errors.New("watermark proposal refused")
			raft := &startupWatermarkEngine{err: proposalErr}
			shards := newStartupShardStore(engine, map[uint64]*kv.ShardGroup{
				1: {Store: leading},
				2: {Store: lagging, Engine: raft},
			}, enabled)
			t.Cleanup(func() { require.NoError(t, shards.Close()) })
			require.Equal(t, uint64(30), shards.LastCommitTS())
			value, err := shards.GetAt(t.Context(), []byte("b-key"), shards.LastCommitTS())
			if enabled {
				require.ErrorIs(t, err, proposalErr)
				require.Equal(t, kv.EncodeWatermarkAdvance(30), raft.payload)
				return
			}
			require.NoError(t, err)
			require.Equal(t, []byte("value"), value)
			require.Nil(t, raft.payload)
		})
	}
}

func TestInternalReadWatermarkOptions(t *testing.T) {
	t.Parallel()
	proposalErr := errors.New("watermark proposal refused")
	tests := []struct {
		name    string
		enabled bool
		wrapped bool
		err     error
	}{
		{name: "disabled"},
		{name: "enabled engine fallback", enabled: true},
		{name: "enabled wrapped proposer", enabled: true, wrapped: true},
		{name: "proposal error", enabled: true, err: proposalErr},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			raft := &startupWatermarkEngine{index: 17}
			selected := &startupWatermarkEngine{index: 23, err: tc.err}
			rt := &raftGroupRuntime{spec: groupSpec{id: 7}, engine: raft}
			groups := map[uint64]*kv.ShardGroup{}
			if tc.wrapped || tc.err != nil {
				group := &kv.ShardGroup{Engine: selected}
				_ = kv.NewLeaderProxyForShardGroup(group)
				require.NotSame(t, selected, group.Proposer())
				groups[rt.spec.id] = group
			}
			clock := kv.NewHLC()
			opts := []adapter.InternalOption{adapter.WithInternalAdminProposer(raft)}
			opts = append(opts, internalReadWatermarkOptions(tc.enabled, rt, groups)...)
			server := adapter.NewInternalWithEngine(nil, raft, clock, nil, opts...)
			payload := kv.EncodeWatermarkAdvance(clock.Next())
			resp, err := server.ForwardAdminProposal(t.Context(), &pb.ForwardAdminProposalRequest{Payload: payload})
			switch {
			case !tc.enabled:
				require.Equal(t, codes.FailedPrecondition, status.Code(err))
				require.EqualError(t, err, "rpc error: code = FailedPrecondition desc = read watermark advance is not enabled on this node")
				require.Nil(t, raft.payload, "must not fall back to the admin proposer")
			case tc.err != nil:
				require.ErrorIs(t, err, tc.err)
				require.Equal(t, payload, selected.payload)
				require.Nil(t, raft.payload)
			case tc.wrapped:
				require.NoError(t, err)
				require.Equal(t, selected.index, resp.GetCommitIndex())
				require.Equal(t, payload, selected.payload)
				require.Nil(t, raft.payload)
			default:
				require.NoError(t, err)
				require.Equal(t, raft.index, resp.GetCommitIndex())
				require.Equal(t, payload, raft.payload)
			}
		})
	}
}

func TestReadWatermarkStartupDefaultIsDisabled(t *testing.T) {
	t.Parallel()
	require.Equal(t, "false", flag.Lookup("readWatermarkAdvance").DefValue)
	require.Nil(t, internalReadWatermarkOptions(false, nil, nil), "disabled startup must not resolve a proposer")
}
