package etcd

import (
	"bytes"
	"context"
	"io"
	"net"
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
	patched, err := transport.applyBridgeMode(context.Background(), msg)
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
	patched, err := transport.applyBridgeMode(context.Background(), msg)
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
	patched, err := transport.applyBridgeMode(context.Background(), msg)
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
	_, err := transport.applyBridgeMode(context.Background(), msg)
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

// testEtcdRaftClient is a minimal mock of pb.EtcdRaftClient that routes
// SendSnapshot calls to a pre-wired testSnapshotSendClient.
type testEtcdRaftClient struct {
	stream *testSnapshotSendClient
}

func (c *testEtcdRaftClient) Send(_ context.Context, _ *pb.EtcdRaftMessage, _ ...grpc.CallOption) (*pb.EtcdRaftAck, error) {
	return &pb.EtcdRaftAck{}, nil
}

func (c *testEtcdRaftClient) SendSnapshot(_ context.Context, _ ...grpc.CallOption) (pb.EtcdRaft_SendSnapshotClient, error) {
	return c.stream, nil
}

// injectClient pre-populates the transport's client cache for the given peer
// address so calls to clientFor return the mock without dialling.
func injectClient(t *testing.T, transport *GRPCTransport, address string, client pb.EtcdRaftClient) {
	t.Helper()
	transport.mu.Lock()
	transport.clients[address] = client
	transport.mu.Unlock()
}

// --- sendSnapshotReaderChunks multi-chunk tests ---

func TestSendSnapshotReaderChunksMultiChunk(t *testing.T) {
	// 12-byte payload with chunkSize=4 → 3 chunks.
	chunkSize := 4
	payload := []byte("1234abcd5678")
	header := []byte("meta")

	client := &testSnapshotSendClient{}
	err := sendSnapshotReaderChunks(client, header, bytes.NewReader(payload), chunkSize)
	require.NoError(t, err)

	require.Len(t, client.chunks, 3)

	require.Equal(t, header, client.chunks[0].Metadata)
	require.Equal(t, []byte("1234"), client.chunks[0].Chunk)
	require.False(t, client.chunks[0].Final)

	require.Empty(t, client.chunks[1].Metadata)
	require.Equal(t, []byte("abcd"), client.chunks[1].Chunk)
	require.False(t, client.chunks[1].Final)

	require.Empty(t, client.chunks[2].Metadata)
	require.Equal(t, []byte("5678"), client.chunks[2].Chunk)
	require.True(t, client.chunks[2].Final)
}

func TestSendSnapshotReaderChunksExactBoundary(t *testing.T) {
	// 8-byte payload with chunkSize=4 → exactly 2 chunks.
	chunkSize := 4
	payload := []byte("12345678")
	header := []byte("hdr")

	client := &testSnapshotSendClient{}
	err := sendSnapshotReaderChunks(client, header, bytes.NewReader(payload), chunkSize)
	require.NoError(t, err)

	require.Len(t, client.chunks, 2)
	require.Equal(t, header, client.chunks[0].Metadata)
	require.Equal(t, []byte("1234"), client.chunks[0].Chunk)
	require.False(t, client.chunks[0].Final)

	require.Equal(t, []byte("5678"), client.chunks[1].Chunk)
	require.True(t, client.chunks[1].Final)
}

// TestSendSnapshotReaderChunksTrailingPartialChunk regressions a production
// failure where payloads whose length was not a whole multiple of chunkSize
// had the trailing partial chunk silently dropped. The old loop emitted the
// last full chunk with Final=true and returned, leaving the partial in
// `next` unsent. Receivers observed payload_bytes truncated to the previous
// boundary and FSM.Restore hit readRestoreEntry with unexpected EOF.
//
// In the reproduction the exactly-on-boundary case already passed, so the
// pre-existing TestSendSnapshotReaderChunksExactBoundary missed the bug.
// This test fixes the coverage gap: chunkSize=4 with a 9-byte payload yields
// two full chunks plus a 1-byte tail that must arrive.
func TestSendSnapshotReaderChunksTrailingPartialChunk(t *testing.T) {
	chunkSize := 4
	payload := []byte("12345678X")
	header := []byte("hdr")

	client := &testSnapshotSendClient{}
	err := sendSnapshotReaderChunks(client, header, bytes.NewReader(payload), chunkSize)
	require.NoError(t, err)

	require.Len(t, client.chunks, 3, "expected two full chunks plus a trailing partial")

	require.Equal(t, header, client.chunks[0].Metadata)
	require.Equal(t, []byte("1234"), client.chunks[0].Chunk)
	require.False(t, client.chunks[0].Final)

	require.Equal(t, []byte("5678"), client.chunks[1].Chunk)
	require.False(t, client.chunks[1].Final)

	require.Equal(t, []byte("X"), client.chunks[2].Chunk)
	require.True(t, client.chunks[2].Final)

	var delivered []byte
	for _, c := range client.chunks {
		delivered = append(delivered, c.Chunk...)
	}
	require.Equal(t, payload, delivered)
}

// --- streamFSMSnapshot tests ---

// TestStreamFSMSnapshotOverGRPCRestoresFollowerFSM exercises the full
// sender-to-receiver streaming path over a real gRPC server and then drives
// the received bytes through StateMachine.Restore on a fresh follower FSM,
// simulating the deployment scenario the bug manifested in (a follower whose
// data directory was wiped and then received a snapshot from the leader).
//
// The reproducer uses a 3-entry testStateMachine serialization whose total
// byte size (31 bytes) is not a whole multiple of the forced chunkSize (8).
// Before the trailing-partial fix in sendSnapshotReaderChunks the receiver
// observed the stream truncated to 24 bytes (3 full chunks) and the
// subsequent Restore failed with io.ErrUnexpectedEOF while reading the last
// length-prefixed item. The assertion chain is therefore:
//
//  1. sender streams the full .fsm file contents,
//  2. receiver accumulates all chunks and reconstructs the message,
//  3. a *fresh* receiver FSM restores from msg.Snapshot.Data and exposes
//     exactly the entries that were serialized on the sender side.
//
// (3) is the real acceptance criterion — (1) and (2) are stepping stones.
func TestStreamFSMSnapshotOverGRPCRestoresFollowerFSM(t *testing.T) {
	// Seed a sender-side state machine with known entries and serialize it
	// through its own Snapshot() codec so the payload format matches what
	// Restore expects.
	senderFSM := &testStateMachine{}
	for _, e := range [][]byte{[]byte("alpha"), []byte("bravo"), []byte("charlie")} {
		senderFSM.Apply(e)
	}
	snap, err := senderFSM.Snapshot()
	require.NoError(t, err)
	var buf bytes.Buffer
	_, err = snap.WriteTo(&buf)
	require.NoError(t, err)
	require.NoError(t, snap.Close())
	payload := buf.Bytes()

	dir := t.TempDir()
	crc, _ := writeFSMFileForTest(t, dir, 77, payload)

	// Pick a chunk size that guarantees a trailing partial — without this
	// shape the pre-fix code path would have exited normally and the test
	// would be useless as a regression guard.
	const chunkSize = 8
	require.NotZero(t, len(payload)%chunkSize,
		"payload length %d must not be a whole multiple of chunkSize %d or this test would no longer cover the trailing-partial bug",
		len(payload), chunkSize)

	// Real gRPC server with receiver transport.
	lis, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = lis.Close() })

	recvTransport := NewGRPCTransport(nil)
	recvTransport.snapshotChunkSize = chunkSize
	recvTransport.SetSpoolDir(t.TempDir())

	receiverFSM := &testStateMachine{}
	restoredCh := make(chan error, 1)
	recvTransport.SetHandler(func(_ context.Context, msg raftpb.Message) error {
		if msg.Snapshot == nil {
			restoredCh <- errors.New("nil snapshot on receiver")
			return nil
		}
		restoredCh <- receiverFSM.Restore(bytes.NewReader(msg.Snapshot.Data))
		return nil
	})

	server := grpc.NewServer()
	recvTransport.Register(server)
	t.Cleanup(server.Stop)
	go func() { _ = server.Serve(lis) }()

	sendTransport := NewGRPCTransport([]Peer{{NodeID: 2, Address: lis.Addr().String()}})
	sendTransport.snapshotChunkSize = chunkSize
	t.Cleanup(func() { require.NoError(t, sendTransport.Close()) })

	openFn := func(index uint64) (io.ReadCloser, error) {
		return openFSMSnapshotPayloadReader(fsmSnapPath(dir, index))
	}
	msg := raftpb.Message{
		Type: raftpb.MsgSnap, From: 1, To: 2,
		Snapshot: &raftpb.Snapshot{
			Data:     encodeSnapshotToken(77, crc),
			Metadata: raftpb.SnapshotMetadata{Index: 77, Term: 5},
		},
	}
	require.NoError(t, sendTransport.streamFSMSnapshot(context.Background(), msg, 77, openFn))

	select {
	case restoreErr := <-restoredCh:
		require.NoError(t, restoreErr,
			"fresh follower FSM must restore cleanly; a short-read EOF here means the trailing partial chunk was dropped in transit")
	case <-time.After(5 * time.Second):
		t.Fatal("receiver never ran Restore")
	}

	require.Equal(t, senderFSM.Applied(), receiverFSM.Applied(),
		"receiver FSM state after Restore must equal sender FSM state")
}

// TestStreamFSMSnapshotOverGRPCAtChunkBoundary pins the exact-multiple case
// so a future refactor cannot regress it while fixing the non-aligned case.
func TestStreamFSMSnapshotOverGRPCAtChunkBoundary(t *testing.T) {
	dir := t.TempDir()
	payload := []byte("AAAAAAAABBBBBBBB") // 16 bytes == 2 × chunkSize(8)
	crc, _ := writeFSMFileForTest(t, dir, 91, payload)

	lis, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = lis.Close() })

	recvTransport := NewGRPCTransport(nil)
	recvTransport.snapshotChunkSize = 8
	recvTransport.SetSpoolDir(t.TempDir())
	gotCh := make(chan raftpb.Message, 1)
	recvTransport.SetHandler(func(_ context.Context, msg raftpb.Message) error {
		gotCh <- msg
		return nil
	})

	server := grpc.NewServer()
	recvTransport.Register(server)
	t.Cleanup(server.Stop)
	go func() { _ = server.Serve(lis) }()

	sendTransport := NewGRPCTransport([]Peer{{NodeID: 2, Address: lis.Addr().String()}})
	sendTransport.snapshotChunkSize = 8
	t.Cleanup(func() { require.NoError(t, sendTransport.Close()) })

	openFn := func(index uint64) (io.ReadCloser, error) {
		return openFSMSnapshotPayloadReader(fsmSnapPath(dir, index))
	}
	msg := raftpb.Message{
		Type: raftpb.MsgSnap, From: 1, To: 2,
		Snapshot: &raftpb.Snapshot{
			Data:     encodeSnapshotToken(91, crc),
			Metadata: raftpb.SnapshotMetadata{Index: 91, Term: 5},
		},
	}
	require.NoError(t, sendTransport.streamFSMSnapshot(context.Background(), msg, 91, openFn))

	select {
	case got := <-gotCh:
		require.Equal(t, payload, got.Snapshot.Data)
	case <-time.After(5 * time.Second):
		t.Fatal("receiver never observed the snapshot message")
	}
}

func TestStreamFSMSnapshotSendsPayload(t *testing.T) {
	dir := t.TempDir()
	payload := []byte("stream fsm snapshot payload data for test")
	crc, _ := writeFSMFileForTest(t, dir, 55, payload)

	sendClient := &testSnapshotSendClient{}
	transport := NewGRPCTransport([]Peer{{NodeID: 3, Address: "host:2"}})
	injectClient(t, transport, "host:2", &testEtcdRaftClient{stream: sendClient})

	openFn := func(index uint64) (io.ReadCloser, error) {
		return openFSMSnapshotPayloadReader(fsmSnapPath(dir, index))
	}

	msg := raftpb.Message{
		Type: raftpb.MsgSnap,
		To:   3,
		Snapshot: &raftpb.Snapshot{
			Data:     encodeSnapshotToken(55, crc),
			Metadata: raftpb.SnapshotMetadata{Index: 55, Term: 2},
		},
	}

	err := transport.streamFSMSnapshot(context.Background(), msg, 55, openFn)
	require.NoError(t, err)

	require.NotEmpty(t, sendClient.chunks)

	// Metadata must appear only in the first chunk.
	require.NotEmpty(t, sendClient.chunks[0].Metadata)
	for _, c := range sendClient.chunks[1:] {
		require.Empty(t, c.Metadata)
	}

	// Reconstruct and compare the streamed payload.
	var got []byte
	for _, c := range sendClient.chunks {
		got = append(got, c.Chunk...)
	}
	require.Equal(t, payload, got)
	require.True(t, sendClient.chunks[len(sendClient.chunks)-1].Final)
}

func TestStreamFSMSnapshotFileNotFound(t *testing.T) {
	dir := t.TempDir()

	sendClient := &testSnapshotSendClient{}
	transport := NewGRPCTransport([]Peer{{NodeID: 4, Address: "host:3"}})
	injectClient(t, transport, "host:3", &testEtcdRaftClient{stream: sendClient})

	openFn := func(index uint64) (io.ReadCloser, error) {
		return openFSMSnapshotPayloadReader(fsmSnapPath(dir, index))
	}

	msg := raftpb.Message{
		Type: raftpb.MsgSnap,
		To:   4,
		Snapshot: &raftpb.Snapshot{
			Metadata: raftpb.SnapshotMetadata{Index: 999, Term: 1},
		},
	}

	err := transport.streamFSMSnapshot(context.Background(), msg, 999, openFn)
	require.ErrorIs(t, err, ErrFSMSnapshotNotFound)
	require.Empty(t, sendClient.chunks)
}

// --- dispatchSnapshot routing tests ---

func TestDispatchSnapshotTokenRoutesToStream(t *testing.T) {
	// When snapshot.Data is a token and openFSMPayload is set, dispatchSnapshot
	// must route to streamFSMSnapshot (chunked streaming path) — not bridge mode.
	dir := t.TempDir()
	payload := []byte("dispatch token route test payload data 12345")
	crc, _ := writeFSMFileForTest(t, dir, 42, payload)

	sendClient := &testSnapshotSendClient{}
	transport := NewGRPCTransport([]Peer{{NodeID: 2, Address: "fake:1"}})
	injectClient(t, transport, "fake:1", &testEtcdRaftClient{stream: sendClient})
	transport.SetFSMPayloadOpener(func(index uint64) (io.ReadCloser, error) {
		return openFSMSnapshotPayloadReader(fsmSnapPath(dir, index))
	})

	msg := raftpb.Message{
		Type: raftpb.MsgSnap,
		To:   2,
		Snapshot: &raftpb.Snapshot{
			Data:     encodeSnapshotToken(42, crc),
			Metadata: raftpb.SnapshotMetadata{Index: 42, Term: 1},
		},
	}

	err := transport.dispatchSnapshot(context.Background(), msg)
	require.NoError(t, err)

	// Chunks must carry the FSM payload (not the raw token bytes).
	var got []byte
	for _, c := range sendClient.chunks {
		got = append(got, c.Chunk...)
	}
	require.Equal(t, payload, got)
}

func TestDispatchSnapshotNonTokenRoutesToBridge(t *testing.T) {
	// When snapshot.Data is NOT a token (legacy full payload), dispatchSnapshot
	// must forward it unchanged via the bridge (sendSnapshot) path.
	legacy := []byte("legacy full fsm payload not a token")

	sendClient := &testSnapshotSendClient{}
	transport := NewGRPCTransport([]Peer{{NodeID: 2, Address: "fake:1"}})
	injectClient(t, transport, "fake:1", &testEtcdRaftClient{stream: sendClient})

	msg := raftpb.Message{
		Type: raftpb.MsgSnap,
		To:   2,
		Snapshot: &raftpb.Snapshot{
			Data:     legacy,
			Metadata: raftpb.SnapshotMetadata{Index: 1, Term: 1},
		},
	}

	err := transport.dispatchSnapshot(context.Background(), msg)
	require.NoError(t, err)

	// The legacy payload must reach the receiver unmodified.
	var got []byte
	for _, c := range sendClient.chunks {
		got = append(got, c.Chunk...)
	}
	require.Equal(t, legacy, got)
}

func TestDispatchSnapshotTokenNoOpenerFallsBackToBridge(t *testing.T) {
	// Token snapshot with NO openFSMPayload set → falls back to bridge mode.
	// Without a readFSMPayload either, applyBridgeMode is a passthrough, so
	// the raw token bytes are sent. This verifies the fallback branch is taken.
	dir := t.TempDir()
	payload := []byte("bridge fallback test payload data here")
	crc, _ := writeFSMFileForTest(t, dir, 77, payload)
	token := encodeSnapshotToken(77, crc)

	sendClient := &testSnapshotSendClient{}
	transport := NewGRPCTransport([]Peer{{NodeID: 2, Address: "fake:1"}})
	injectClient(t, transport, "fake:1", &testEtcdRaftClient{stream: sendClient})
	// No SetFSMPayloadOpener / SetFSMPayloadReader → passthrough bridge.

	msg := raftpb.Message{
		Type: raftpb.MsgSnap,
		To:   2,
		Snapshot: &raftpb.Snapshot{
			Data:     token,
			Metadata: raftpb.SnapshotMetadata{Index: 77, Term: 1},
		},
	}

	err := transport.dispatchSnapshot(context.Background(), msg)
	require.NoError(t, err)

	// Token bytes forwarded as-is (no opener wired).
	var got []byte
	for _, c := range sendClient.chunks {
		got = append(got, c.Chunk...)
	}
	require.Equal(t, token, got)
}
