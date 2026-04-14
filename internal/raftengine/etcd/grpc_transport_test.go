package etcd

import (
	"bytes"
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

func TestNewGRPCTransportDerivesPeerNodeIDs(t *testing.T) {
	transport := NewGRPCTransport([]Peer{{
		ID:      "n2",
		Address: "127.0.0.1:65530",
	}})

	peer, err := transport.peerFor(DeriveNodeID("n2"))
	require.NoError(t, err)
	require.Equal(t, "n2", peer.ID)
	require.Equal(t, "127.0.0.1:65530", peer.Address)
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

// --- applyBridgeMode tests ---

func TestApplyBridgeModePassesNonTokenUnchanged(t *testing.T) {
	transport := NewGRPCTransport(nil)

	// Non-token payload must pass through unchanged.
	msg := raftpb.Message{
		Snapshot: &raftpb.Snapshot{Data: []byte("legacy full payload")},
	}
	patched, err := transport.applyBridgeMode(msg)
	require.NoError(t, err)
	require.Equal(t, []byte("legacy full payload"), patched.Snapshot.Data)
}

func TestApplyBridgeModeNoReaderIsNoop(t *testing.T) {
	transport := NewGRPCTransport(nil)

	// A token with no readFSMPayload callback set → passthrough.
	token := encodeSnapshotToken(42, 0xDEADBEEF)
	msg := raftpb.Message{
		Snapshot: &raftpb.Snapshot{Data: token},
	}
	patched, err := transport.applyBridgeMode(msg)
	require.NoError(t, err)
	require.Equal(t, token, patched.Snapshot.Data)
}

func TestApplyBridgeModeReconstructsPayload(t *testing.T) {
	fsmSnapDir := t.TempDir()
	payload := []byte("bridge mode payload data 12345")
	crc, _ := writeFSMFileForTest(t, fsmSnapDir, 42, payload)

	transport := NewGRPCTransport(nil)
	transport.SetFSMPayloadReader(func(index uint64) ([]byte, error) {
		return readFSMSnapshotPayload(fsmSnapPath(fsmSnapDir, index))
	})

	token := encodeSnapshotToken(42, crc)
	msg := raftpb.Message{
		Snapshot: &raftpb.Snapshot{
			Data:     token,
			Metadata: raftpb.SnapshotMetadata{Index: 42},
		},
	}
	patched, err := transport.applyBridgeMode(msg)
	require.NoError(t, err)
	require.Equal(t, payload, patched.Snapshot.Data)
	// Metadata must be preserved.
	require.Equal(t, uint64(42), patched.Snapshot.Metadata.Index)
}

func TestApplyBridgeModeReaderError(t *testing.T) {
	transport := NewGRPCTransport(nil)
	transport.SetFSMPayloadReader(func(_ uint64) ([]byte, error) {
		return nil, ErrFSMSnapshotNotFound
	})

	token := encodeSnapshotToken(99, 0xABCD)
	msg := raftpb.Message{
		Snapshot: &raftpb.Snapshot{Data: token},
	}
	_, err := transport.applyBridgeMode(msg)
	require.ErrorIs(t, err, ErrFSMSnapshotNotFound)
}

// TestSendSnapshotReaderChunksSmallPayloadPreservesData is a regression test
// for a bug where a payload smaller than one chunk size was silently dropped.
// readSnapshotChunk returns (data, io.EOF) for a short final read, and the old
// code treated that the same as an empty reader, sending an empty chunk.
func TestSendSnapshotReaderChunksSmallPayloadPreservesData(t *testing.T) {
	payload := []byte("tiny payload under one chunk")
	header := []byte("header-bytes")

	client := &testSnapshotSendClient{}
	err := sendSnapshotReaderChunks(client, header, bytes.NewReader(payload), defaultSnapshotChunkSize)
	require.NoError(t, err)

	require.Len(t, client.chunks, 1)
	require.Equal(t, header, client.chunks[0].Metadata)
	require.Equal(t, payload, client.chunks[0].Chunk)
	require.True(t, client.chunks[0].Final)
}

func TestSendSnapshotReaderChunksEmptyPayloadSendsHeaderOnly(t *testing.T) {
	header := []byte("header-bytes")

	client := &testSnapshotSendClient{}
	err := sendSnapshotReaderChunks(client, header, bytes.NewReader(nil), defaultSnapshotChunkSize)
	require.NoError(t, err)

	require.Len(t, client.chunks, 1)
	require.Equal(t, header, client.chunks[0].Metadata)
	require.Empty(t, client.chunks[0].Chunk)
	require.True(t, client.chunks[0].Final)
}

// testSnapshotSendClient captures chunks sent via sendSnapshotReaderChunks / sendSnapshotChunks.
type testSnapshotSendClient struct {
	chunks []*pb.EtcdRaftSnapshotChunk
}

func (c *testSnapshotSendClient) Send(chunk *pb.EtcdRaftSnapshotChunk) error {
	c.chunks = append(c.chunks, chunk)
	return nil
}

func (c *testSnapshotSendClient) CloseAndRecv() (*pb.EtcdRaftAck, error) {
	return &pb.EtcdRaftAck{}, nil
}

func (*testSnapshotSendClient) Header() (metadata.MD, error) { return nil, nil }
func (*testSnapshotSendClient) Trailer() metadata.MD         { return nil }
func (*testSnapshotSendClient) CloseSend() error             { return nil }
func (*testSnapshotSendClient) Context() context.Context     { return context.Background() }
func (*testSnapshotSendClient) SendMsg(any) error            { return nil }
func (*testSnapshotSendClient) RecvMsg(any) error            { return nil }
