package adapter

import (
	"context"
	"testing"

	"github.com/bootjp/elastickv/distribution"
	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/bootjp/elastickv/internal/s3keys"
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

func testPrefixScanEnd(prefix []byte) []byte {
	out := append([]byte(nil), prefix...)
	for i := len(out) - 1; i >= 0; i-- {
		if out[i] != 0xFF {
			out[i]++
			return out[:i+1]
		}
	}
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

	stream = &captureExportRangeVersionsStream{ctx: context.Background()}
	err = internal.ExportRangeVersions(&pb.ExportRangeVersionsRequest{
		MaxCommitTs: 20,
		KeyFamily:   distribution.MigrationFamilyUser,
	}, stream)
	require.Error(t, err)
	require.Equal(t, codes.InvalidArgument, status.Code(err))
}

func TestInternalExportRangeVersionsUsesDecodedS3BucketRouteFilter(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	st := store.NewMVCCStore()
	internal := NewInternalWithEngine(nil, mockInternalLeader{}, nil, nil, WithInternalStore(st))

	for _, tc := range []struct {
		name       string
		family     uint32
		prefix     string
		keyFor     func(string) []byte
		value      []byte
		routeStart []byte
		routeEnd   []byte
	}{
		{
			name:       "bucket meta",
			family:     distribution.MigrationFamilyS3BucketMeta,
			prefix:     s3keys.BucketMetaPrefix,
			keyFor:     s3keys.BucketMetaKey,
			value:      []byte("meta"),
			routeStart: s3keys.RouteKey("bucket-b", 0, ""),
			routeEnd:   s3keys.RouteKey("bucket-c", 0, ""),
		},
		{
			name:       "bucket generation",
			family:     distribution.MigrationFamilyS3BucketGeneration,
			prefix:     s3keys.BucketGenerationPrefix,
			keyFor:     s3keys.BucketGenerationKey,
			value:      []byte("generation"),
			routeStart: s3keys.RouteKey("bucket-b", 0, ""),
			routeEnd:   s3keys.RouteKey("bucket-c", 0, ""),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			inRouteKey := tc.keyFor("bucket-b")
			outRouteKey := tc.keyFor("bucket-a")
			require.NoError(t, st.PutAt(ctx, inRouteKey, tc.value, 10, 0))
			require.NoError(t, st.PutAt(ctx, outRouteKey, []byte("skip"), 10, 0))

			stream := &captureExportRangeVersionsStream{ctx: ctx}
			err := internal.ExportRangeVersions(&pb.ExportRangeVersionsRequest{
				MaxCommitTs:     20,
				RouteStart:      tc.routeStart,
				RouteEnd:        tc.routeEnd,
				KeyFamily:       tc.family,
				RangeStart:      []byte(tc.prefix),
				RangeEnd:        testPrefixScanEnd([]byte(tc.prefix)),
				MaxScannedBytes: 1 << 20,
			}, stream)
			require.NoError(t, err)
			require.Len(t, stream.responses, 1)
			require.Equal(t, []*pb.MVCCVersion{
				{Key: inRouteKey, CommitTs: 10, Value: tc.value, KeyFamily: tc.family},
			}, stream.responses[0].GetVersions())
		})
	}
}

func TestInternalExportRangeVersionsDecodedS3EmptyRouteEndIsUnbounded(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	st := store.NewMVCCStore()
	internal := NewInternalWithEngine(nil, mockInternalLeader{}, nil, nil, WithInternalStore(st))

	inRouteKey := s3keys.BucketMetaKey("bucket-z")
	outRouteKey := s3keys.BucketMetaKey("bucket-a")
	require.NoError(t, st.PutAt(ctx, inRouteKey, []byte("meta-z"), 10, 0))
	require.NoError(t, st.PutAt(ctx, outRouteKey, []byte("skip"), 10, 0))

	stream := &captureExportRangeVersionsStream{ctx: ctx}
	err := internal.ExportRangeVersions(&pb.ExportRangeVersionsRequest{
		MaxCommitTs:     20,
		RouteStart:      s3keys.RouteKey("bucket-z", 0, ""),
		RouteEnd:        []byte{},
		KeyFamily:       distribution.MigrationFamilyS3BucketMeta,
		RangeStart:      []byte(s3keys.BucketMetaPrefix),
		RangeEnd:        testPrefixScanEnd([]byte(s3keys.BucketMetaPrefix)),
		MaxScannedBytes: 1 << 20,
	}, stream)
	require.NoError(t, err)
	require.Len(t, stream.responses, 1)
	require.Equal(t, []*pb.MVCCVersion{
		{Key: inRouteKey, CommitTs: 10, Value: []byte("meta-z"), KeyFamily: distribution.MigrationFamilyS3BucketMeta},
	}, stream.responses[0].GetVersions())
}

func TestInternalExportRangeVersionsIncludesS3BucketAuxiliaryForBucketRouteIntersection(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	st := store.NewMVCCStore()
	internal := NewInternalWithEngine(nil, mockInternalLeader{}, nil, nil, WithInternalStore(st))

	const bucket = "bucket-b"
	for _, tc := range []struct {
		name   string
		family uint32
		prefix string
		key    []byte
		value  []byte
	}{
		{
			name:   "bucket meta",
			family: distribution.MigrationFamilyS3BucketMeta,
			prefix: s3keys.BucketMetaPrefix,
			key:    s3keys.BucketMetaKey(bucket),
			value:  []byte("meta"),
		},
		{
			name:   "bucket generation",
			family: distribution.MigrationFamilyS3BucketGeneration,
			prefix: s3keys.BucketGenerationPrefix,
			key:    s3keys.BucketGenerationKey(bucket),
			value:  []byte("generation"),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.NoError(t, st.PutAt(ctx, tc.key, tc.value, 10, 0))

			stream := &captureExportRangeVersionsStream{ctx: ctx}
			err := internal.ExportRangeVersions(&pb.ExportRangeVersionsRequest{
				MaxCommitTs:     20,
				RouteStart:      s3keys.RouteKey(bucket, 7, "m"),
				RouteEnd:        s3keys.RouteKey(bucket, 7, "z"),
				KeyFamily:       tc.family,
				RangeStart:      []byte(tc.prefix),
				RangeEnd:        testPrefixScanEnd([]byte(tc.prefix)),
				MaxScannedBytes: 1 << 20,
			}, stream)
			require.NoError(t, err)
			require.Len(t, stream.responses, 1)
			require.Equal(t, []*pb.MVCCVersion{
				{Key: tc.key, CommitTs: 10, Value: tc.value, KeyFamily: tc.family},
			}, stream.responses[0].GetVersions())
		})
	}
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
