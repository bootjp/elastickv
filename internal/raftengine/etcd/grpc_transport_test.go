package etcd

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	pb "github.com/bootjp/elastickv/proto"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
	raftpb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

func TestTransportContextAppliesTimeoutWhenUnset(t *testing.T) {
	ctx, cancel := transportContext(context.Background(), 3*time.Second)
	t.Cleanup(cancel)

	deadline, ok := ctx.Deadline()
	require.True(t, ok)
	require.InDelta(t, 3*time.Second, time.Until(deadline), float64(200*time.Millisecond))
}

func TestTransportContextPreservesExistingDeadline(t *testing.T) {
	parent, cancel := context.WithTimeout(context.Background(), time.Second)
	t.Cleanup(cancel)

	ctx, childCancel := transportContext(parent, 3*time.Second)
	t.Cleanup(childCancel)

	parentDeadline, ok := parent.Deadline()
	require.True(t, ok)
	deadline, ok := ctx.Deadline()
	require.True(t, ok)
	require.Equal(t, parentDeadline, deadline)
}

func TestSplitSnapshotMessageReusesClonedPayload(t *testing.T) {
	msg := raftpb.Message{
		Type: raftpb.MsgSnap,
		Snapshot: &raftpb.Snapshot{
			Data: []byte("snapshot"),
			Metadata: raftpb.SnapshotMetadata{
				Index: 9,
				Term:  3,
			},
		},
	}

	header, payload, err := splitSnapshotMessage(msg)
	require.NoError(t, err)
	require.NotEmpty(t, header)
	require.Equal(t, []byte("snapshot"), payload)

	payload[0] = 'S'
	require.Equal(t, byte('S'), msg.Snapshot.Data[0])
}

func TestNewSnapshotSpoolUsesConfiguredDir(t *testing.T) {
	dir := t.TempDir()
	spool, err := newSnapshotSpool(dir)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, spool.Close())
	})

	require.Equal(t, dir, filepath.Dir(spool.path))
	_, err = os.Stat(spool.path)
	require.NoError(t, err)
}

func TestReceiveSnapshotStreamRejectsPrematureEOF(t *testing.T) {
	metadata := raftpb.Message{
		Type: raftpb.MsgSnap,
		To:   2,
		Snapshot: &raftpb.Snapshot{
			Metadata: raftpb.SnapshotMetadata{
				Index: 7,
				Term:  3,
			},
		},
	}
	raw, err := metadata.Marshal()
	require.NoError(t, err)

	transport := NewGRPCTransport(nil)
	stream := &testSendSnapshotServer{
		chunks: []*pb.EtcdRaftSnapshotChunk{{
			Metadata: raw,
			Chunk:    []byte("partial"),
		}},
	}

	_, err = transport.receiveSnapshotStream(stream)
	require.Error(t, err)
	require.True(t, errors.Is(err, errSnapshotStreamShort))
}

func TestReceiveSnapshotStreamRejectsDuplicateMetadata(t *testing.T) {
	metadata := raftpb.Message{
		Type: raftpb.MsgSnap,
		To:   2,
		Snapshot: &raftpb.Snapshot{
			Metadata: raftpb.SnapshotMetadata{
				Index: 7,
				Term:  3,
			},
		},
	}
	raw, err := metadata.Marshal()
	require.NoError(t, err)

	transport := NewGRPCTransport(nil)
	stream := &testSendSnapshotServer{
		chunks: []*pb.EtcdRaftSnapshotChunk{
			{Metadata: raw, Chunk: []byte("a")},
			{Metadata: raw, Chunk: []byte("b"), Final: true},
		},
	}

	_, err = transport.receiveSnapshotStream(stream)
	require.Error(t, err)
	require.True(t, errors.Is(err, errSnapshotMetadataDuplicate))
}

func TestClientForDeduplicatesConcurrentDial(t *testing.T) {
	transport := NewGRPCTransport([]Peer{{
		NodeID:  2,
		ID:      "n2",
		Address: "127.0.0.1:65530",
	}})
	t.Cleanup(func() {
		require.NoError(t, transport.Close())
	})

	oldNewClient := grpcNewClient
	var callCount atomic.Int32
	grpcNewClient = func(target string, opts ...grpc.DialOption) (*grpc.ClientConn, error) {
		callCount.Add(1)
		return oldNewClient(target, opts...)
	}
	t.Cleanup(func() {
		grpcNewClient = oldNewClient
	})

	const callers = 8
	var wg sync.WaitGroup
	wg.Add(callers)
	clients := make([]pb.EtcdRaftClient, callers)
	errCh := make(chan error, callers)
	for i := 0; i < callers; i++ {
		go func(idx int) {
			defer wg.Done()
			client, err := transport.clientFor(2)
			if err != nil {
				errCh <- err
				return
			}
			clients[idx] = client
		}(i)
	}
	wg.Wait()
	close(errCh)

	for err := range errCh {
		require.NoError(t, err)
	}
	require.Equal(t, int32(1), callCount.Load())
	for i := 1; i < callers; i++ {
		require.Equal(t, clients[0], clients[i])
	}
}

type testSendSnapshotServer struct {
	chunks []*pb.EtcdRaftSnapshotChunk
	index  int
}

func (s *testSendSnapshotServer) Recv() (*pb.EtcdRaftSnapshotChunk, error) {
	if s.index >= len(s.chunks) {
		return nil, io.EOF
	}
	chunk := s.chunks[s.index]
	s.index++
	return chunk, nil
}

func (*testSendSnapshotServer) SendAndClose(*pb.EtcdRaftAck) error {
	return nil
}

func (*testSendSnapshotServer) SetHeader(metadata.MD) error {
	return nil
}

func (*testSendSnapshotServer) SendHeader(metadata.MD) error {
	return nil
}

func (*testSendSnapshotServer) SetTrailer(metadata.MD) {}

func (*testSendSnapshotServer) Context() context.Context {
	return context.Background()
}

func (*testSendSnapshotServer) SendMsg(any) error {
	return nil
}

func (*testSendSnapshotServer) RecvMsg(any) error {
	return nil
}
