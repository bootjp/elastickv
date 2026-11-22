package loadtest

import (
	"context"
	"sync/atomic"
	"testing"

	pb "github.com/bootjp/elastickv/proto"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

type cancellingAuditClient struct {
	pb.RawKVClient
	cancel context.CancelFunc
	calls  atomic.Int64
}

func (c *cancellingAuditClient) RawGet(context.Context, *pb.RawGetRequest, ...grpc.CallOption) (*pb.RawGetResponse, error) {
	c.calls.Add(1)
	c.cancel()
	return &pb.RawGetResponse{}, nil
}
func TestAuditCancellationStopsRemainingCalls(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	client := &cancellingAuditClient{cancel: cancel}
	runner, err := New(configForTest(), client, client, client)
	require.NoError(t, err)
	runner.acknowledged = make([]uint64, 10000)
	result := runner.Audit(ctx)
	require.LessOrEqual(t, client.calls.Load(), int64(runner.config.Concurrency))
	require.Less(t, result.Attempts, len(runner.acknowledged)*len(runner.clients))
	require.Zero(t, result.Success)
}
