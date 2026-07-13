package adapter

import (
	"context"
	"testing"

	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

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
	internal := NewInternalWithEngine(nil, nil, nil, nil, WithInternalStore(st))
	stream := &captureExportRangeVersionsStream{ctx: ctx}

	err := internal.ExportRangeVersions(&pb.ExportRangeVersionsRequest{
		MaxCommitTs: 20,
		RouteStart:  []byte("a"),
		RouteEnd:    []byte("b"),
	}, stream)
	require.NoError(t, err)
	require.Len(t, stream.responses, 1)
	require.True(t, stream.responses[0].GetDone())
	require.Empty(t, stream.responses[0].GetNextCursor())
	require.Equal(t, []*pb.MVCCVersion{
		{Key: []byte("a"), CommitTs: 10, Value: []byte("va")},
	}, stream.responses[0].GetVersions())
}

func TestInternalImportRangeVersionsAppliesStoreBatch(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	st := store.NewMVCCStore()
	internal := NewInternalWithEngine(nil, nil, nil, nil, WithInternalStore(st))

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

	got, err := st.GetAt(ctx, []byte("k"), 30)
	require.NoError(t, err)
	require.Equal(t, []byte("v"), got)
	floor, err := st.MigrationHLCFloor(ctx, 7)
	require.NoError(t, err)
	require.Equal(t, uint64(30), floor)
}
