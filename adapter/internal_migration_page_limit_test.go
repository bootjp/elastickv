package adapter

import (
	"bytes"
	"context"
	"testing"

	"github.com/bootjp/elastickv/distribution"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

type countingPageExportStore struct {
	store.MVCCStore
	calls int
}

func (s *countingPageExportStore) ExportVersions(ctx context.Context, opts store.ExportVersionsOptions) (store.ExportVersionsResult, error) {
	s.calls++
	return s.MVCCStore.ExportVersions(ctx, opts)
}

func TestInternalExportPageLimitDoesNotScanAheadAndResumes(t *testing.T) {
	st := &countingPageExportStore{MVCCStore: store.NewMVCCStore()}
	t.Cleanup(func() { require.NoError(t, st.Close()) })
	for _, key := range []string{"a", "b", "c"} {
		require.NoError(t, st.PutAt(t.Context(), []byte(key), bytes.Repeat([]byte(key), 1024), 10, 0))
	}
	internal := NewInternalWithEngine(nil, mockInternalLeader{}, nil, nil, WithInternalStore(st))
	req := &pb.ExportRangeVersionsRequest{RangeStart: []byte("a"), RangeEnd: []byte("z"), RouteStart: []byte("a"), RouteEnd: []byte("z"), MaxCommitTs: 20, ChunkBytes: 1024, KeyFamily: distribution.MigrationFamilyUser}
	full := &captureExportRangeVersionsStream{ctx: t.Context()}
	require.NoError(t, internal.ExportRangeVersions(req, full))
	require.Greater(t, len(full.responses), 1, "exercise a multi-page family")
	var expected []*pb.MVCCVersion
	for _, page := range full.responses {
		expected = append(expected, page.Versions...)
	}
	for _, limit := range []uint32{1, 2} {
		t.Run(map[uint32]string{1: "one-page", 2: "two-pages"}[limit], func(t *testing.T) {
			next := proto.CloneOf(req)
			next.MaxPages = limit
			var actual []*pb.MVCCVersion
			for attempt := 0; attempt <= len(full.responses); attempt++ {
				before := st.calls
				stream := &captureExportRangeVersionsStream{ctx: t.Context()}
				require.NoError(t, internal.ExportRangeVersions(next, stream))
				require.NotEmpty(t, stream.responses)
				require.LessOrEqual(t, len(stream.responses), int(limit))
				require.Equal(t, len(stream.responses), st.calls-before, "do not scan a discarded page after the last requested response")
				for _, page := range stream.responses {
					actual = append(actual, page.Versions...)
				}
				last := stream.responses[len(stream.responses)-1]
				if last.Done {
					require.Equal(t, expected, actual)
					return
				}
				require.NotEmpty(t, last.NextCursor)
				require.NotEqual(t, next.Cursor, last.NextCursor)
				next.Cursor = last.NextCursor
			}
			t.Fatal("limited export did not complete")
		})
	}
}
