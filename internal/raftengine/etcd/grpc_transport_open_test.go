package etcd

import (
	"context"
	"testing"
	"time"

	pb "github.com/bootjp/elastickv/proto"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

type openingRaftClient struct {
	pb.EtcdRaftClient
	open func(context.Context) (pb.EtcdRaft_SendStreamClient, error)
}

func (c openingRaftClient) SendStream(ctx context.Context, _ ...grpc.CallOption) (pb.EtcdRaft_SendStreamClient, error) {
	return c.open(ctx)
}

func TestPeerStreamOpeningCallerCancellation(t *testing.T) {
	for _, returnStream := range []bool{false, true} {
		t.Run(map[bool]string{false: "blocked", true: "success_races_cancellation"}[returnStream], func(t *testing.T) {
			transport := NewGRPCTransport(nil)
			t.Cleanup(func() { require.NoError(t, transport.Close()) })
			const address = "peer:2"
			transport.markPeerStreamSupported(address)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			started := make(chan context.Context, 1)
			client := openingRaftClient{open: func(streamCtx context.Context) (pb.EtcdRaft_SendStreamClient, error) {
				started <- streamCtx
				<-streamCtx.Done()
				if returnStream {
					return &testRaftSendStreamClient{ctx: streamCtx}, nil
				}
				return nil, streamCtx.Err()
			}}
			result := make(chan error, 1)
			go func() {
				_, err := transport.openPeerStream(ctx, address, client)
				result <- err
			}()
			select {
			case <-started:
			case <-time.After(5 * time.Second):
				t.Fatal("stream creation did not start")
			}
			cancel()
			select {
			case err := <-result:
				require.ErrorIs(t, err, context.Canceled)
			case <-time.After(5 * time.Second):
				t.Fatal("stream creation ignored caller cancellation")
			}
			cached, err := transport.cachedStream(address)
			require.NoError(t, err)
			require.Nil(t, cached)
		})
	}
}

func TestPeerStreamSurvivesOpeningCallerDeadline(t *testing.T) {
	transport := NewGRPCTransport(nil)
	t.Cleanup(func() { require.NoError(t, transport.Close()) })
	const address = "peer:2"
	transport.markPeerStreamSupported(address)
	client := &testEtcdRaftClient{}
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	opened, err := transport.openPeerStream(ctx, address, client)
	require.NoError(t, err)
	<-ctx.Done()
	require.NoError(t, opened.stream.Context().Err())
	cached, err := transport.cachedStream(address)
	require.NoError(t, err)
	require.Same(t, opened, cached)
}
