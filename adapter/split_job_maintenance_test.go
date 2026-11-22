package adapter

import (
	"bytes"
	"context"
	"fmt"
	"testing"

	"github.com/bootjp/elastickv/distribution"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

func TestSplitMaintenancePreservesCursorAndLegacyBudgets(t *testing.T) {
	for _, online := range []bool{false, true} {
		t.Run(fmt.Sprint(online), func(t *testing.T) {
			job := distribution.SplitJob{JobID: 7, SplitKey: []byte("m")}
			versions, size := defaultSplitPromotionMaxVersions, defaultSplitPromotionMaxBytes
			if online {
				job.OnlineCaptureVersion = store.OnlineMigrationVersion
				versions, size = 256, 512*1024
			}
			client := &splitPromotionClientStub{responses: []*pb.PromoteStagedVersionsResponse{{NextCursor: []byte("next")}, {Done: true}}}
			require.NoError(t, promoteSplitJobTarget(context.Background(), client, job))
			require.Len(t, client.requests, 2)
			require.Empty(t, client.requests[0].GetCursor())
			require.Equal(t, []byte("next"), client.requests[1].GetCursor())
			for _, req := range client.requests {
				require.Equal(t, versions, req.GetMaxVersions())
				require.Equal(t, size, req.GetMaxBytes())
				require.Equal(t, defaultSplitPromotionMaxScanned, req.GetMaxScannedBytes())
			}
			cleanup := splitMigrationCleanupRequest(job, []byte("z"), distribution.MigrationBracket{Start: []byte("m"), End: []byte("z")}, []byte("saved"), 123)
			require.Equal(t, versions, cleanup.GetMaxVersions())
			require.Equal(t, size, cleanup.GetMaxBytes())
			require.Equal(t, defaultSplitPromotionMaxScanned, cleanup.GetMaxScannedBytes())
			require.Equal(t, []byte("saved"), cleanup.GetCursor())
			require.Equal(t, uint64(123), cleanup.GetMaxCommitTs())
		})
	}
}

func TestOnlineSplitMultipleMaintenancePagesPreserveValues(t *testing.T) {
	t.Parallel()
	r := newResplitAcceptanceRuntime(t)
	mutations := make([]*pb.Mutation, 700)
	value := bytes.Repeat([]byte("v"), 1024)
	for i := range mutations {
		mutations[i] = &pb.Mutation{Op: pb.Op_PUT, Key: fmt.Appendf(nil, "n%04d", i), Value: value}
	}
	_, err := r.groups[1].Txn.Commit(t.Context(), []*pb.Request{{Ts: 100, Mutations: mutations}})
	require.NoError(t, err)
	promotions := &recordOnlinePromotions{SplitPromotionClient: r.clients[2]}
	r.server.splitPromotionClientFactory = func(context.Context, distribution.SplitJob) (SplitPromotionClient, error) { return promotions, nil }
	job := r.migrate(t, 1, 2, "m")
	require.Positive(t, job.OnlineCaptureVersion)
	require.GreaterOrEqual(t, promotions.calls, 3, "actual FSM promotion must cross multiple bounded pages")
	for _, mutation := range mutations {
		r.read(t, string(mutation.GetKey()), string(value))
	}
}

type recordOnlinePromotions struct {
	SplitPromotionClient
	calls int
}

func (c *recordOnlinePromotions) PromoteStagedVersions(ctx context.Context, req *pb.PromoteStagedVersionsRequest, opts ...grpc.CallOption) (*pb.PromoteStagedVersionsResponse, error) {
	c.calls++
	return c.SplitPromotionClient.PromoteStagedVersions(ctx, req, opts...)
}
