package adapter

import (
	"context"
	"testing"

	"github.com/bootjp/elastickv/distribution"
	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/bootjp/elastickv/kv"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type mockInternalLeader struct {
	raftengine.LeaderView
}

func (mockInternalLeader) State() raftengine.State {
	return raftengine.StateLeader
}

func (mockInternalLeader) Leader() raftengine.LeaderInfo {
	return raftengine.LeaderInfo{ID: "n1", Address: "127.0.0.1:50051"}
}

func (mockInternalLeader) VerifyLeader(context.Context) error {
	return nil
}

func (mockInternalLeader) LinearizableRead(context.Context) (uint64, error) {
	return 1, nil
}

type applyingMigrationProposer struct {
	fsm   raftengine.StateMachine
	calls uint64
}

func (p *applyingMigrationProposer) Propose(_ context.Context, data []byte) (*raftengine.ProposalResult, error) {
	p.calls++
	return &raftengine.ProposalResult{
		CommitIndex: p.calls,
		Response:    p.fsm.Apply(data),
	}, nil
}

func (p *applyingMigrationProposer) ProposeAdmin(ctx context.Context, data []byte) (*raftengine.ProposalResult, error) {
	return p.Propose(ctx, data)
}

type captureExportRangeVersionsStream struct {
	grpc.ServerStream
	ctx       context.Context
	responses []*pb.ExportRangeVersionsResponse
}

func (s *captureExportRangeVersionsStream) Context() context.Context {
	if s.ctx != nil {
		return s.ctx
	}
	return context.Background()
}

func (s *captureExportRangeVersionsStream) Send(resp *pb.ExportRangeVersionsResponse) error {
	s.responses = append(s.responses, resp)
	return nil
}

func TestInternalExportRangeVersionsUsesStoreAndRouteFilter(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	st := store.NewMVCCStore()
	require.NoError(t, st.PutAt(ctx, []byte("a"), []byte("va"), 10, 0))
	require.NoError(t, st.PutAt(ctx, []byte("z"), []byte("vz"), 10, 0))
	require.NoError(t, st.PutAt(ctx, []byte("!txn|int|a"), []byte("intent"), 10, 0))
	internal := NewInternalWithEngine(nil, mockInternalLeader{}, nil, nil, WithInternalStore(st))
	stream := &captureExportRangeVersionsStream{ctx: ctx}

	err := internal.ExportRangeVersions(&pb.ExportRangeVersionsRequest{
		MaxCommitTs:          20,
		RouteStart:           []byte("a"),
		RouteEnd:             []byte("b"),
		KeyFamily:            distribution.MigrationFamilyUser,
		ExcludeKnownInternal: true,
		RangeStart:           []byte(""),
		RangeEnd:             []byte("z"),
		MaxScannedBytes:      1 << 20,
		ExcludePrefixes:      [][]byte{[]byte("!custom|")},
	}, stream)
	require.NoError(t, err)
	require.Len(t, stream.responses, 1)
	require.True(t, stream.responses[0].GetDone())
	require.Empty(t, stream.responses[0].GetNextCursor())
	require.Equal(t, []*pb.MVCCVersion{
		{Key: []byte("a"), CommitTs: 10, Value: []byte("va"), KeyFamily: distribution.MigrationFamilyUser},
	}, stream.responses[0].GetVersions())
}

func TestInternalExportRangeVersionsRejectsUnboundedExport(t *testing.T) {
	t.Parallel()

	internal := NewInternalWithEngine(nil, mockInternalLeader{}, nil, nil, WithInternalStore(store.NewMVCCStore()))
	stream := &captureExportRangeVersionsStream{ctx: context.Background()}

	err := internal.ExportRangeVersions(&pb.ExportRangeVersionsRequest{
		KeyFamily: distribution.MigrationFamilyUser,
	}, stream)
	require.Error(t, err)
	require.Equal(t, codes.InvalidArgument, status.Code(err))
}

func TestInternalImportRangeVersionsAppliesStoreBatch(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	st := store.NewMVCCStore()
	clock := kv.NewHLC()
	proposer := &applyingMigrationProposer{
		fsm: kv.NewKvFSMWithHLC(st, clock),
	}
	internal := NewInternalWithEngine(nil, mockInternalLeader{}, clock, nil,
		WithInternalStore(st),
		WithInternalMigrationProposer(proposer),
	)

	resp, err := internal.ImportRangeVersions(ctx, &pb.ImportRangeVersionsRequest{
		JobId:     7,
		BracketId: 3,
		BatchSeq:  1,
		Cursor:    []byte("cursor-1"),
		Versions: []*pb.MVCCVersion{
			{Key: []byte("k"), CommitTs: 30, Value: []byte("v"), ExpireAt: 100},
		},
	})
	require.NoError(t, err)
	require.Equal(t, []byte("cursor-1"), resp.GetAckedCursor())
	require.Equal(t, uint64(1), proposer.calls)

	staged := distribution.MigrationStagedDataKey(7, []byte("k"))
	got, err := st.GetAt(ctx, staged, 30)
	require.NoError(t, err)
	require.Equal(t, []byte("v"), got)
	_, err = st.GetAt(ctx, []byte("k"), 30)
	require.ErrorIs(t, err, store.ErrKeyNotFound)
	floor, err := st.MigrationHLCFloor(ctx, 7)
	require.NoError(t, err)
	require.Equal(t, uint64(30), floor)
	require.GreaterOrEqual(t, clock.Current(), uint64(30))
}
