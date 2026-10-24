package adapter

import (
	"context"
	"testing"

	"github.com/bootjp/elastickv/distribution"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestDistributionServerJobReadsHonorStartupGate(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	catalog := distribution.NewCatalogStore(store.NewMVCCStore())
	job := sampleDistributionSplitJob(13)
	require.NoError(t, catalog.CreateSplitJob(ctx, job))
	blocked := true
	server := NewDistributionServer(nil, catalog, WithDistributionReadGate(func() bool { return blocked }))
	_, err := server.GetSplitJob(ctx, &pb.GetSplitJobRequest{JobId: job.JobID})
	require.Equal(t, codes.Unavailable, status.Code(err))
	_, err = server.GetSplitJob(ctx, &pb.GetSplitJobRequest{JobId: 99})
	require.Equal(t, codes.Unavailable, status.Code(err), "catch-up must not return a stale NotFound")
	_, err = server.ListSplitJobs(ctx, &pb.ListSplitJobsRequest{})
	require.Equal(t, codes.Unavailable, status.Code(err))
	blocked = false
	got, err := server.GetSplitJob(ctx, &pb.GetSplitJobRequest{JobId: job.JobID})
	require.NoError(t, err)
	require.Equal(t, job.JobID, got.GetJob().GetJobId())
	listed, err := server.ListSplitJobs(ctx, &pb.ListSplitJobsRequest{})
	require.NoError(t, err)
	require.Len(t, listed.GetJobs(), 1)
	_, err = server.GetSplitJob(ctx, &pb.GetSplitJobRequest{JobId: 99})
	require.Equal(t, codes.NotFound, status.Code(err))
}
