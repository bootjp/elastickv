package adapter

import (
	"context"
	"testing"

	"github.com/bootjp/elastickv/distribution"
	"github.com/bootjp/elastickv/kv"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestOnlineSplitCutoverConfigurationChange(t *testing.T) {
	for _, tc := range []struct {
		name       string
		group      uint64
		afterReady bool
	}{{"source", 1, false}, {"target", 2, false}, {"source-after-ready", 1, true}, {"target-after-ready", 2, true}} {
		t.Run(tc.name, func(t *testing.T) {
			r := newResplitAcceptanceRuntime(t)
			changed := false
			r.migrate(t, 1, 2, "m", func(job distribution.SplitJob) {
				if job.Phase != distribution.SplitJobPhaseCutover || changed {
					return
				}
				if tc.afterReady && job.TargetStagedReadinessState != distribution.SplitJobBarrierArmed {
					return
				}
				p, ok := r.clients[tc.group].internal.migrationProposer.(*ownershipRuntimeProposer)
				require.True(t, ok)
				p.index++
				leader, ok := r.clients[tc.group].internal.leader.(ownershipRuntimeLeader)
				require.True(t, ok)
				cfg := leader.configuration
				require.NoError(t, cfg.RecordAppliedConfiguration(p.index))
				changed = true
			})
			require.True(t, changed)
			r.read(t, "n", "original-n")
			require.NoError(t, r.put(t, 2, "x", "after", 1000000000000000000))
			r.read(t, "x", "after")
		})
	}
}

type handoffCancelStore struct {
	store.MVCCStore
	cancel context.CancelFunc
}

func (s handoffCancelStore) OnlineMigrationHandoffActive(context.Context, []byte) bool {
	s.cancel()
	return true
}

func TestOnlineRawHandoffCancellationKeepsProof(t *testing.T) {
	eng := distribution.NewEngine()
	require.NoError(t, eng.ApplySnapshot(distribution.CatalogSnapshot{Version: 1, Routes: []distribution.RouteDescriptor{{RouteID: 1, GroupID: 1, State: distribution.RouteStateWriteFenced}}}))
	txn := &fenceAdmissionTransaction{}
	coord := kv.NewShardedCoordinator(eng, map[uint64]*kv.ShardGroup{1: {Txn: txn}}, 1, kv.NewHLC(), nil)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	server := NewGRPCServer(handoffCancelStore{store.NewMVCCStore(), cancel}, coord)
	_, err := server.RawPut(ctx, &pb.RawPutRequest{Key: []byte("x"), Value: []byte("v")})
	require.Equal(t, codes.Unavailable, status.Code(err))
	require.Zero(t, txn.calls)
}

func TestOnlineSplitLegacyDeleteAfterBase(t *testing.T) {
	r := newResplitAcceptanceRuntime(t)
	key := store.LegacyListMetaDeltaKey([]byte("x"), 8, 0)
	value := store.MarshalListMetaDelta(store.ListMetaDelta{LenDelta: 1})
	require.NoError(t, r.groups[1].Store.PutAt(t.Context(), key, value, 8, 0))
	snap, err := r.server.catalog.Snapshot(t.Context())
	require.NoError(t, err)
	started, err := r.client.StartSplitMigration(t.Context(), &pb.StartSplitMigrationRequest{ExpectedCatalogVersion: snap.Version, RouteId: 1, TargetGroupId: 2, SplitKey: []byte("m")})
	require.NoError(t, err)
	deleted := false
	for range 250 {
		require.NoError(t, r.server.RunSplitJobRunnerOnce(t.Context()))
		job, found, e := r.server.catalog.SplitJob(t.Context(), started.GetJobId())
		require.NoError(t, e)
		require.True(t, found)
		if job.Phase == distribution.SplitJobPhaseFence && !deleted {
			current, e := r.server.catalog.Snapshot(t.Context())
			require.NoError(t, e)
			_, e = r.groups[1].Txn.Commit(t.Context(), []*pb.Request{{Ts: 35, ObservedRouteVersion: kv.EncodeObservedRouteVersion(current.Version), Mutations: []*pb.Mutation{{Op: pb.Op_DEL, Key: key}}}})
			require.NoError(t, e)
			session, e := store.ReadOnlineMigrationSession(t.Context(), r.groups[1].Store, job.JobID)
			require.NoError(t, e)
			require.NotEmpty(t, session.Invalid)
			deleted = true
		}
		if job.Phase == distribution.SplitJobPhaseAbandoned {
			require.True(t, deleted)
			_, e = r.groups[1].Store.GetAt(t.Context(), key, ^uint64(0))
			require.ErrorIs(t, e, store.ErrKeyNotFound)
			return
		}
	}
	t.Fatal("expected an abandoned migration preserving the acknowledged delete")
}

type onlineSealTimingClient struct {
	*resplitAcceptanceClient
	afterExport func(*pb.ExportRangeVersionsRequest)
	afterSeal   func()
}

func (c *onlineSealTimingClient) ExportRangeVersions(ctx context.Context, req *pb.ExportRangeVersionsRequest, opts ...grpc.CallOption) (grpc.ServerStreamingClient[pb.ExportRangeVersionsResponse], error) {
	stream, err := c.resplitAcceptanceClient.ExportRangeVersions(ctx, req, opts...)
	if err == nil && req.GetOnlineJobId() != 0 {
		c.afterExport(req)
	}
	return stream, err
}

func (c *onlineSealTimingClient) ApplyTargetStagedReadiness(ctx context.Context, req *pb.TargetStagedReadinessRequest, opts ...grpc.CallOption) (*pb.TargetStagedReadinessResponse, error) {
	resp, err := c.resplitAcceptanceClient.ApplyTargetStagedReadiness(ctx, req, opts...)
	if err == nil && req.GetOnlineSeal() {
		c.afterSeal()
	}
	return resp, err
}

func TestOnlineSplitTargetChangeWithUnimportedSealedTail(t *testing.T) {
	r := newResplitAcceptanceRuntime(t)
	written, changed := false, false
	source := &onlineSealTimingClient{resplitAcceptanceClient: r.clients[1]}
	source.afterExport = func(req *pb.ExportRangeVersionsRequest) {
		job, found, err := r.server.catalog.SplitJob(t.Context(), req.GetOnlineJobId())
		require.NoError(t, err)
		if !found || written || job.Phase != distribution.SplitJobPhaseFence || !job.TransferPreflightDone {
			return
		}
		require.NoError(t, r.put(t, 1, "x", "last-acknowledged", 30))
		written = true
	}
	source.afterSeal = func() {
		if changed {
			return
		}
		require.True(t, written)
		p, ok := r.clients[2].internal.migrationProposer.(*ownershipRuntimeProposer)
		require.True(t, ok)
		leader, ok := r.clients[2].internal.leader.(ownershipRuntimeLeader)
		require.True(t, ok)
		p.index++
		require.NoError(t, leader.configuration.RecordAppliedConfiguration(p.index))
		changed = true
	}
	r.server.splitMigrationClientFactory = func(context.Context, distribution.SplitJob, uint64) (SplitMigrationClient, SplitMigrationClient, error) {
		return source, r.clients[2], nil
	}
	r.server.splitMigrationGroupFactory = func(_ context.Context, id uint64) (SplitMigrationClient, error) {
		if id == 1 {
			return source, nil
		}
		return r.clients[id], nil
	}
	job := r.migrate(t, 1, 2, "m")
	require.True(t, changed)
	require.Equal(t, uint64(1), job.OnlineSealSequence)
	require.Equal(t, job.OnlineSealSequence, job.OnlineSequence)
	r.read(t, "x", "last-acknowledged")
}
