package etcd

import (
	"bufio"
	"context"
	"io"
	"log/slog"
	"sync"
	"time"

	internalutil "github.com/bootjp/elastickv/internal"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/cockroachdb/errors"
	raftpb "go.etcd.io/raft/v3/raftpb"
	"golang.org/x/sync/singleflight"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const defaultSnapshotChunkSize = 16 << 20

const defaultDispatchTimeout = 5 * time.Second
const defaultSnapshotDispatchTimeout = 30 * time.Minute

// defaultBridgeMaterializeLimit caps the number of bridge-mode snapshot
// materializations that may hold memory concurrently. Each call allocates up
// to fsmMaxInMemPayload; limiting concurrency bounds the aggregate allocation.
const defaultBridgeMaterializeLimit = 1

var (
	errTransportPeerUnknown      = errors.New("etcd raft peer is not configured")
	errTransportHandlerNil       = errors.New("etcd raft transport handler is not configured")
	errSnapshotMetadataNil       = errors.New("etcd raft snapshot metadata is required")
	errSnapshotMetadataDuplicate = errors.New("etcd raft snapshot metadata was sent more than once")
	errSnapshotMessageNil        = errors.New("etcd raft snapshot message is required")
	errSnapshotStreamShort       = errors.New("etcd raft snapshot stream closed before final chunk")
	// errStreamNotSupported is returned by getOrOpenStream when the remote peer
	// responded with codes.Unimplemented on a previous SendStream attempt.
	// dispatchRegular falls back to the unary Send path on this error.
	errStreamNotSupported = errors.New("etcd raft peer does not support SendStream")
)

// peerStream holds a long-lived client-streaming gRPC stream to one peer.
// cancel tears down the stream context; the transport deletes the entry on
// any Send error and re-opens on the next dispatch attempt.
type peerStream struct {
	stream pb.EtcdRaft_SendStreamClient
	cancel context.CancelFunc
}

var grpcNewClient = grpc.NewClient

type MessageHandler func(context.Context, raftpb.Message) error

type GRPCTransport struct {
	pb.UnimplementedEtcdRaftServer

	mu                sync.RWMutex
	peers             map[uint64]Peer
	clients           map[string]pb.EtcdRaftClient
	conns             map[string]*grpc.ClientConn
	handler           MessageHandler
	snapshotChunkSize int
	spoolDir          string
	fsmSnapDir        string
	// readFSMPayload is the fallback bridge callback that materialises the full
	// FSM payload into memory. Used only when openFSMPayload is not set.
	readFSMPayload func(index uint64) ([]byte, error)
	// openFSMPayload is the preferred bridge callback that opens the .fsm file
	// for streaming without allocating a large buffer.
	openFSMPayload func(index uint64) (io.ReadCloser, error)
	dialGroup      singleflight.Group
	// bridgeSem limits concurrent bridge-mode snapshot materializations so
	// that aggregate in-memory allocation stays bounded even when multiple
	// dispatch workers run simultaneously.
	bridgeSem chan struct{}

	// streamsMu protects streams and noStream.
	// Invariant: never hold streamsMu and t.mu simultaneously. All paths that
	// need both release the first before acquiring the second:
	//   getOrOpenStream: releases streamsMu before calling clientFor (t.mu), then re-acquires streamsMu.
	//   UpsertPeer/RemovePeer: release t.mu before calling closeStream/clearNoStream (streamsMu).
	// Using RWMutex so concurrent reads on the hot dispatch path do not contend.
	streamsMu sync.RWMutex
	// streams holds one long-lived SendStream RPC per peer node ID.
	// Each entry is owned by the single per-peer multiplexing dispatch goroutine.
	streams map[uint64]*peerStream
	// noStream records peers that returned codes.Unimplemented on SendStream;
	// dispatchRegular falls back to unary Send for those peers.
	noStream map[uint64]struct{}
}

func NewGRPCTransport(peers []Peer) *GRPCTransport {
	peerMap := make(map[uint64]Peer, len(peers))
	for _, peer := range peers {
		if peer.NodeID == 0 {
			peer.NodeID = DeriveNodeID(peer.ID)
		}
		if peer.ID == "" {
			peer.ID = peer.Address
		}
		if peer.NodeID == 0 || peer.Address == "" {
			continue
		}
		peerMap[peer.NodeID] = peer
	}
	return &GRPCTransport{
		peers:             peerMap,
		clients:           make(map[string]pb.EtcdRaftClient),
		conns:             make(map[string]*grpc.ClientConn),
		snapshotChunkSize: defaultSnapshotChunkSize,
		bridgeSem:         make(chan struct{}, defaultBridgeMaterializeLimit),
		streams:           make(map[uint64]*peerStream),
		noStream:          make(map[uint64]struct{}),
	}
}

func (t *GRPCTransport) Register(server grpc.ServiceRegistrar) {
	if t == nil || server == nil {
		return
	}
	pb.RegisterEtcdRaftServer(server, t)
}

func (t *GRPCTransport) SetHandler(handler MessageHandler) {
	if t == nil {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	t.handler = handler
}

func (t *GRPCTransport) SetSpoolDir(dir string) {
	if t == nil {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	t.spoolDir = dir
}

func (t *GRPCTransport) SetFSMSnapDir(dir string) {
	if t == nil {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	t.fsmSnapDir = dir
}

func (t *GRPCTransport) SetFSMPayloadReader(fn func(index uint64) ([]byte, error)) {
	if t == nil {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	t.readFSMPayload = fn
}

// SetFSMPayloadOpener registers the callback used by the bridge mode to stream
// FSM snapshot payloads directly from disk without materialising the full
// payload in memory.
func (t *GRPCTransport) SetFSMPayloadOpener(fn func(index uint64) (io.ReadCloser, error)) {
	if t == nil {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	t.openFSMPayload = fn
}

func (t *GRPCTransport) UpsertPeer(peer Peer) {
	if t == nil || peer.NodeID == 0 {
		return
	}
	t.mu.Lock()
	var closedNodeIDs []uint64
	if existing, ok := t.peers[peer.NodeID]; ok && existing.Address != "" && existing.Address != peer.Address {
		closedNodeIDs = t.closePeerConnLocked(existing.Address)
	}
	t.peers[peer.NodeID] = peer
	t.mu.Unlock()

	// Clear stream state outside mu (never hold streamsMu and t.mu simultaneously).
	for _, id := range closedNodeIDs {
		t.closeStream(id)
		t.clearNoStream(id)
	}
	// Always clear noStream for the upserted peer so that a same-address
	// upgrade (peer binary updated without changing address) re-probes
	// streaming support on the next dispatch rather than staying stuck on
	// the unary fallback indefinitely.
	t.clearNoStream(peer.NodeID)
}

func (t *GRPCTransport) RemovePeer(nodeID uint64) {
	if t == nil || nodeID == 0 {
		return
	}
	t.mu.Lock()
	var closedNodeIDs []uint64
	if peer, ok := t.peers[nodeID]; ok {
		// Collect affected nodeIDs before deleting: closePeerConnLocked iterates
		// t.peers to find all nodes using the address, so nodeID must still be
		// present at this point or it won't be included in the cleanup list.
		closedNodeIDs = t.closePeerConnLocked(peer.Address)
		delete(t.peers, nodeID)
	}
	t.mu.Unlock()

	// Tear down stream state outside mu (never hold streamsMu and t.mu simultaneously).
	for _, id := range closedNodeIDs {
		t.closeStream(id)
		t.clearNoStream(id)
	}
}

func (t *GRPCTransport) Close() error {
	if t == nil {
		return nil
	}
	// Cancel all streams before closing connections so in-flight Send calls
	// fail cleanly rather than blocking on a half-closed TCP connection.
	t.closeAllStreams()

	t.mu.Lock()
	defer t.mu.Unlock()

	var err error
	for addr, conn := range t.conns {
		delete(t.conns, addr)
		delete(t.clients, addr)
		err = errors.CombineErrors(err, errors.WithStack(conn.Close()))
	}
	return errors.WithStack(err)
}

// closePeerConnLocked closes the gRPC connection for address and returns the
// node IDs that were using it. Callers should clear stream/noStream state for
// those IDs after releasing t.mu, so the next dispatch re-probes streaming
// support (e.g. after a peer upgrade at the same address).
// Caller must hold t.mu.
func (t *GRPCTransport) closePeerConnLocked(address string) []uint64 {
	if address == "" {
		return nil
	}
	var nodeIDs []uint64
	for id, peer := range t.peers {
		if peer.Address == address {
			nodeIDs = append(nodeIDs, id)
		}
	}
	conn, ok := t.conns[address]
	if !ok {
		delete(t.clients, address)
		return nodeIDs
	}
	delete(t.conns, address)
	delete(t.clients, address)
	if err := conn.Close(); err != nil {
		slog.Warn("failed to close etcd raft peer connection", "address", address, "error", err)
	}
	return nodeIDs
}

func (t *GRPCTransport) Dispatch(ctx context.Context, msg raftpb.Message) error {
	if t == nil {
		return nil
	}
	if isSnapshotMsg(msg) {
		return t.dispatchSnapshot(ctx, msg)
	}
	return t.dispatchRegular(ctx, msg)
}

func isSnapshotMsg(msg raftpb.Message) bool {
	return msg.Type == raftpb.MsgSnap || (msg.Snapshot != nil && len(msg.Snapshot.Data) > 0)
}

func (t *GRPCTransport) dispatchSnapshot(ctx context.Context, msg raftpb.Message) error {
	ctx, cancel := transportContext(ctx, defaultSnapshotDispatchTimeout)
	defer cancel()

	// Prefer streaming when the snapshot holds a token and an opener is wired.
	// This avoids materialising the full FSM payload in memory on the sender.
	if msg.Snapshot != nil && isSnapshotToken(msg.Snapshot.Data) {
		t.mu.RLock()
		openFn := t.openFSMPayload
		t.mu.RUnlock()
		if openFn != nil {
			tok, err := decodeSnapshotToken(msg.Snapshot.Data)
			if err != nil {
				return errors.WithStack(err)
			}
			return t.streamFSMSnapshot(ctx, msg, tok.Index, openFn)
		}
	}

	// Fallback: materialise the payload (no opener wired, or non-token snapshot).
	patched, err := t.applyBridgeMode(ctx, msg)
	if err != nil {
		return err
	}
	return t.sendSnapshot(ctx, patched)
}

// streamFSMSnapshot streams the .fsm payload file directly to the peer using
// chunked gRPC without loading the full content into memory.
func (t *GRPCTransport) streamFSMSnapshot(ctx context.Context, msg raftpb.Message, index uint64, openFn func(uint64) (io.ReadCloser, error)) error {
	rc, err := openFn(index)
	if err != nil {
		return err
	}
	defer func() {
		if closeErr := rc.Close(); closeErr != nil {
			slog.Warn("failed to close FSM snapshot reader", "index", index, "error", closeErr)
		}
	}()

	header, err := snapshotMessageHeader(msg)
	if err != nil {
		return err
	}
	client, err := t.clientFor(msg.To)
	if err != nil {
		return err
	}
	stream, err := client.SendSnapshot(ctx)
	if err != nil {
		return errors.WithStack(err)
	}
	if err := sendSnapshotReaderChunks(stream, header, rc, t.chunkSize()); err != nil {
		return err
	}
	_, err = stream.CloseAndRecv()
	return errors.WithStack(err)
}

// applyBridgeMode implements the Phase 1 bridge: when MemoryStorage holds a
// token, the .fsm file is read back into []byte so all receivers (including
// legacy nodes that predate Phase 2) get a standard full-payload MsgSnap.
// This allocation is transient — freed after the send — and only occurs when
// a slow follower needs a snapshot, not on every periodic creation.
//
// bridgeSem caps the number of concurrent materializations so that aggregate
// in-memory allocation stays bounded across all dispatch workers.
func (t *GRPCTransport) applyBridgeMode(ctx context.Context, msg raftpb.Message) (raftpb.Message, error) {
	if msg.Snapshot == nil || !isSnapshotToken(msg.Snapshot.Data) {
		return msg, nil
	}
	t.mu.RLock()
	readFn := t.readFSMPayload
	t.mu.RUnlock()
	if readFn == nil {
		return msg, nil
	}

	tok, err := decodeSnapshotToken(msg.Snapshot.Data)
	if err != nil {
		return msg, errors.WithStack(err)
	}

	// Acquire the bridge semaphore before allocating the payload buffer.
	// Block until a slot is available or the dispatch context is cancelled.
	select {
	case t.bridgeSem <- struct{}{}:
		defer func() { <-t.bridgeSem }()
	case <-ctx.Done():
		return msg, errors.WithStack(ctx.Err())
	}

	payload, err := readFn(tok.Index)
	if err != nil {
		return msg, errors.WithStack(err)
	}

	snapCopy := *msg.Snapshot
	snapCopy.Data = payload
	msg.Snapshot = &snapCopy
	return msg, nil
}

func (t *GRPCTransport) dispatchRegular(ctx context.Context, msg raftpb.Message) error {
	if err := ctx.Err(); err != nil {
		return errors.WithStack(err)
	}

	raw, err := msg.Marshal()
	if err != nil {
		return errors.WithStack(err)
	}

	stream, err := t.getOrOpenStream(ctx, msg.To)
	if err != nil {
		if errors.Is(err, errStreamNotSupported) {
			return t.dispatchUnary(ctx, raw, msg.To)
		}
		return err
	}

	if err := stream.Send(&pb.EtcdRaftMessage{Message: raw}); err != nil {
		t.closeStream(msg.To)
		return errors.WithStack(err)
	}
	return nil
}

// dispatchUnary sends a single Raft message via the legacy unary Send RPC.
// Used as a fallback when the remote peer does not support SendStream.
func (t *GRPCTransport) dispatchUnary(ctx context.Context, raw []byte, to uint64) error {
	ctx, cancel := transportContext(ctx, defaultDispatchTimeout)
	defer cancel()
	client, err := t.clientFor(to)
	if err != nil {
		return err
	}
	_, err = client.Send(ctx, &pb.EtcdRaftMessage{Message: raw})
	return errors.WithStack(err)
}

func (t *GRPCTransport) DispatchSnapshotSpool(ctx context.Context, msg raftpb.Message, spool *snapshotSpool) error {
	if t == nil {
		return nil
	}
	if spool == nil {
		return t.Dispatch(ctx, msg)
	}
	ctx, cancel := transportContext(ctx, defaultSnapshotDispatchTimeout)
	defer cancel()
	return t.sendSnapshotSpool(ctx, msg, spool)
}

func (t *GRPCTransport) SendSnapshot(stream pb.EtcdRaft_SendSnapshotServer) error {
	msg, err := t.receiveSnapshotStream(stream)
	if err != nil {
		return err
	}
	if err := t.handle(stream.Context(), msg); err != nil {
		return err
	}
	return errors.WithStack(stream.SendAndClose(&pb.EtcdRaftAck{}))
}

func (t *GRPCTransport) Send(ctx context.Context, req *pb.EtcdRaftMessage) (*pb.EtcdRaftAck, error) {
	if req == nil {
		return &pb.EtcdRaftAck{}, nil
	}
	var msg raftpb.Message
	if err := msg.Unmarshal(req.Message); err != nil {
		return nil, errors.WithStack(err)
	}
	if err := t.handle(ctx, msg); err != nil {
		return nil, err
	}
	return &pb.EtcdRaftAck{}, nil
}

// SendStream is the server-side handler for the client-streaming SendStream RPC.
// The client sends a sequence of Raft messages over one long-lived stream;
// this handler processes each one and closes with a single EtcdRaftAck.
// Transient backpressure (errStepQueueFull) is logged and skipped rather than
// tearing down the stream — identical to how the unary Send handler behaves.
func (t *GRPCTransport) SendStream(stream pb.EtcdRaft_SendStreamServer) error {
	for {
		req, err := stream.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return errors.WithStack(stream.SendAndClose(&pb.EtcdRaftAck{}))
			}
			return errors.WithStack(err)
		}
		var msg raftpb.Message
		if err := msg.Unmarshal(req.Message); err != nil {
			return errors.WithStack(err)
		}
		if err := t.handle(stream.Context(), msg); err != nil {
			if errors.Is(err, errStepQueueFull) {
				slog.Warn("etcd raft SendStream: step queue full, dropping message",
					"type", msg.Type.String(),
					"from", msg.From,
					"to", msg.To,
				)
				continue
			}
			return err
		}
	}
}

func (t *GRPCTransport) sendSnapshot(ctx context.Context, msg raftpb.Message) error {
	client, err := t.clientFor(msg.To)
	if err != nil {
		return err
	}

	header, payload, err := splitSnapshotMessage(msg)
	if err != nil {
		return err
	}

	stream, err := client.SendSnapshot(ctx)
	if err != nil {
		return errors.WithStack(err)
	}

	if err := sendSnapshotChunks(stream, header, payload, t.chunkSize()); err != nil {
		return err
	}
	_, err = stream.CloseAndRecv()
	return errors.WithStack(err)
}

func (t *GRPCTransport) sendSnapshotSpool(ctx context.Context, msg raftpb.Message, spool *snapshotSpool) error {
	client, err := t.clientFor(msg.To)
	if err != nil {
		return err
	}
	header, err := snapshotMessageHeader(msg)
	if err != nil {
		return err
	}
	reader, err := spool.Reader()
	if err != nil {
		return err
	}

	stream, err := client.SendSnapshot(ctx)
	if err != nil {
		return errors.WithStack(err)
	}
	if err := sendSnapshotReaderChunks(stream, header, reader, t.chunkSize()); err != nil {
		return err
	}
	_, err = stream.CloseAndRecv()
	return errors.WithStack(err)
}

func (t *GRPCTransport) clientFor(to uint64) (pb.EtcdRaftClient, error) {
	peer, err := t.peerFor(to)
	if err != nil {
		return nil, err
	}

	t.mu.RLock()
	client, ok := t.clients[peer.Address]
	t.mu.RUnlock()
	if ok {
		return client, nil
	}

	value, err, _ := t.dialGroup.Do(peer.Address, func() (any, error) {
		t.mu.RLock()
		client, ok := t.clients[peer.Address]
		t.mu.RUnlock()
		if ok {
			return client, nil
		}

		conn, err := grpcNewClient(peer.Address, internalutil.GRPCDialOptions()...)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		client = pb.NewEtcdRaftClient(conn)

		t.mu.Lock()
		defer t.mu.Unlock()
		if existing, ok := t.clients[peer.Address]; ok {
			_ = conn.Close()
			return existing, nil
		}
		t.conns[peer.Address] = conn
		t.clients[peer.Address] = client
		return client, nil
	})
	if err != nil {
		return nil, errors.WithStack(err)
	}
	client, ok = value.(pb.EtcdRaftClient)
	if !ok {
		return nil, errors.New("etcd raft transport dial returned unexpected client type")
	}
	return client, nil
}

func (t *GRPCTransport) chunkSize() int {
	if t.snapshotChunkSize > 0 {
		return t.snapshotChunkSize
	}
	return defaultSnapshotChunkSize
}

func splitSnapshotMessage(msg raftpb.Message) ([]byte, []byte, error) {
	if msg.Snapshot == nil {
		return nil, nil, errors.WithStack(errSnapshotMessageNil)
	}
	header, err := snapshotMessageHeader(msg)
	if err != nil {
		return nil, nil, err
	}
	return header, msg.Snapshot.Data, nil
}

func snapshotMessageHeader(msg raftpb.Message) ([]byte, error) {
	if msg.Snapshot == nil {
		return nil, errors.WithStack(errSnapshotMessageNil)
	}
	metadata := msg
	snapshotCopy := *msg.Snapshot
	metadata.Snapshot = &snapshotCopy
	metadata.Snapshot.Data = nil
	header, err := metadata.Marshal()
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return header, nil
}

func sendSnapshotChunks(stream pb.EtcdRaft_SendSnapshotClient, header []byte, payload []byte, chunkSize int) error {
	if len(payload) == 0 {
		return sendSnapshotChunk(stream, &pb.EtcdRaftSnapshotChunk{Metadata: header, Final: true})
	}
	for offset := 0; offset < len(payload); offset += chunkSize {
		end := offset + chunkSize
		if end > len(payload) {
			end = len(payload)
		}
		chunk := &pb.EtcdRaftSnapshotChunk{
			Chunk: payload[offset:end],
			Final: end == len(payload),
		}
		if offset == 0 {
			chunk.Metadata = header
		}
		if err := sendSnapshotChunk(stream, chunk); err != nil {
			return err
		}
	}
	return nil
}

func sendSnapshotChunk(stream pb.EtcdRaft_SendSnapshotClient, chunk *pb.EtcdRaftSnapshotChunk) error {
	if err := stream.Send(chunk); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func sendSnapshotReaderChunks(stream pb.EtcdRaft_SendSnapshotClient, header []byte, reader io.Reader, chunkSize int) error {
	if chunkSize <= 0 {
		chunkSize = defaultSnapshotChunkSize
	}
	buffered := bufio.NewReaderSize(reader, chunkSize)
	current, err := readSnapshotChunk(buffered, chunkSize)
	if err != nil {
		if errors.Is(err, io.EOF) {
			// The entire payload fit in one read (or reader was empty).
			// current may be nil for an empty reader, or the full payload for
			// a small snapshot. Include it so the receiver does not get an
			// empty snapshot.Data.
			return sendSnapshotChunk(stream, &pb.EtcdRaftSnapshotChunk{
				Metadata: header,
				Chunk:    current,
				Final:    true,
			})
		}
		return errors.WithStack(err)
	}
	first := true
	for {
		next, nextErr := readSnapshotChunk(buffered, chunkSize)
		final := errors.Is(nextErr, io.EOF)
		if nextErr != nil && !final {
			return errors.WithStack(nextErr)
		}
		chunk := &pb.EtcdRaftSnapshotChunk{
			Chunk: append([]byte(nil), current...),
			Final: final,
		}
		if first {
			chunk.Metadata = header
			first = false
		}
		if err := sendSnapshotChunk(stream, chunk); err != nil {
			return err
		}
		if final {
			return nil
		}
		current = next
	}
}

func readSnapshotChunk(reader *bufio.Reader, chunkSize int) ([]byte, error) {
	chunk := make([]byte, chunkSize)
	n, err := io.ReadFull(reader, chunk)
	switch {
	case err == nil:
		return chunk[:n], nil
	case errors.Is(err, io.ErrUnexpectedEOF):
		// io.ReadFull reports ErrUnexpectedEOF on the final short read from an
		// otherwise healthy stream. Treat that as the last chunk rather than a
		// corrupted snapshot so the trailing partial payload is still sent.
		return chunk[:n], io.EOF
	case errors.Is(err, io.EOF):
		return nil, io.EOF
	default:
		return nil, errors.WithStack(err)
	}
}

func transportContext(ctx context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	if _, ok := ctx.Deadline(); ok {
		return ctx, func() {}
	}
	if timeout <= 0 {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, timeout)
}

func (t *GRPCTransport) peerFor(nodeID uint64) (Peer, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	peer, ok := t.peers[nodeID]
	if !ok {
		return Peer{}, errors.Wrapf(errTransportPeerUnknown, "node_id=%d", nodeID)
	}
	return peer, nil
}

func (t *GRPCTransport) peerByIdentity(id string, address string) (Peer, bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	for _, peer := range t.peers {
		if id != "" && peer.ID != id {
			continue
		}
		if address != "" && peer.Address != address {
			continue
		}
		return peer, true
	}
	return Peer{}, false
}

func (t *GRPCTransport) handle(ctx context.Context, msg raftpb.Message) error {
	t.mu.RLock()
	handler := t.handler
	t.mu.RUnlock()
	if handler == nil {
		return errors.WithStack(errTransportHandlerNil)
	}
	return errors.WithStack(handler(ctx, msg))
}

func (t *GRPCTransport) receiveSnapshotStream(stream pb.EtcdRaft_SendSnapshotServer) (raftpb.Message, error) {
	var metadata raftpb.Message
	seenMetadata := false
	t.mu.RLock()
	spoolDir := t.spoolDir
	t.mu.RUnlock()

	spool, err := newSnapshotSpool(spoolDir)
	if err != nil {
		return raftpb.Message{}, err
	}
	defer func() {
		_ = spool.Close()
	}()

	for {
		chunk, err := stream.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return raftpb.Message{}, errors.WithStack(errSnapshotStreamShort)
			}
			return raftpb.Message{}, errors.WithStack(err)
		}
		seen, err := appendSnapshotChunk(&metadata, spool, chunk, seenMetadata)
		if err != nil {
			return raftpb.Message{}, err
		}
		seenMetadata = seen
		if chunk.Final {
			return buildSnapshotMessage(metadata, spool, seenMetadata)
		}
	}
}

func appendSnapshotChunk(metadata *raftpb.Message, payload io.Writer, chunk *pb.EtcdRaftSnapshotChunk, seenMetadata bool) (bool, error) {
	if len(chunk.Metadata) > 0 {
		if seenMetadata {
			return false, errors.WithStack(errSnapshotMetadataDuplicate)
		}
		if err := metadata.Unmarshal(chunk.Metadata); err != nil {
			return false, errors.WithStack(err)
		}
		seenMetadata = true
	}
	if len(chunk.Chunk) > 0 {
		if _, err := payload.Write(chunk.Chunk); err != nil {
			return false, errors.WithStack(err)
		}
	}
	return seenMetadata, nil
}

// getOrOpenStream returns the live SendStream to nodeID, opening one if
// necessary. Returns errStreamNotSupported if the peer previously returned
// codes.Unimplemented; callers should fall back to the unary Send path.
//
// ctx is the per-peer dispatch context: the stream's lifetime is derived from
// it so that a cancelled context (peer removed, engine shutdown) unblocks any
// pending stream.Send without waiting for a TCP timeout.
//
// Lock ordering: this method acquires streamsMu without holding t.mu; it
// calls clientFor (which may acquire t.mu) only after releasing streamsMu.
func (t *GRPCTransport) getOrOpenStream(ctx context.Context, nodeID uint64) (pb.EtcdRaft_SendStreamClient, error) {
	// Fast path: stream already open or peer known to be unary-only.
	t.streamsMu.RLock()
	_, skip := t.noStream[nodeID]
	ps, ok := t.streams[nodeID]
	t.streamsMu.RUnlock()

	if skip {
		return nil, errStreamNotSupported
	}
	if ok {
		return ps.stream, nil
	}

	// Need a new stream. Dial without holding streamsMu to avoid lock-order
	// inversion with t.mu (clientFor acquires t.mu internally).
	client, err := t.clientFor(nodeID)
	if err != nil {
		return nil, err
	}

	// Re-acquire streamsMu to install the stream atomically.
	t.streamsMu.Lock()
	defer t.streamsMu.Unlock()

	// Re-check: another goroutine or closeStream may have raced.
	if _, skip = t.noStream[nodeID]; skip {
		return nil, errStreamNotSupported
	}
	if ps, ok = t.streams[nodeID]; ok {
		return ps.stream, nil
	}

	streamCtx, cancel := context.WithCancel(ctx)
	stream, err := client.SendStream(streamCtx)
	if err != nil {
		cancel()
		if status.Code(err) == codes.Unimplemented {
			t.noStream[nodeID] = struct{}{}
			return nil, errStreamNotSupported
		}
		return nil, errors.WithStack(err)
	}
	t.streams[nodeID] = &peerStream{stream: stream, cancel: cancel}
	return stream, nil
}

// closeStream tears down the SendStream for nodeID. Safe to call with no
// stream open. The caller must not hold streamsMu.
func (t *GRPCTransport) closeStream(nodeID uint64) {
	t.streamsMu.Lock()
	ps, ok := t.streams[nodeID]
	if ok {
		delete(t.streams, nodeID)
	}
	t.streamsMu.Unlock()
	if ok {
		ps.cancel()
	}
}

// clearNoStream removes nodeID from the noStream set so that the next
// dispatch attempt will probe for SendStream support again (e.g. after an
// upgrade that adds streaming to a previously unary-only peer).
func (t *GRPCTransport) clearNoStream(nodeID uint64) {
	t.streamsMu.Lock()
	delete(t.noStream, nodeID)
	t.streamsMu.Unlock()
}

// closeAllStreams cancels every open stream and resets the streams/noStream
// maps. Called by Close before tearing down the underlying connections.
func (t *GRPCTransport) closeAllStreams() {
	t.streamsMu.Lock()
	old := t.streams
	t.streams = make(map[uint64]*peerStream)
	t.noStream = make(map[uint64]struct{})
	t.streamsMu.Unlock()
	for _, ps := range old {
		ps.cancel()
	}
}

func buildSnapshotMessage(metadata raftpb.Message, spool *snapshotSpool, seenMetadata bool) (raftpb.Message, error) {
	if !seenMetadata || metadata.Snapshot == nil {
		return raftpb.Message{}, errors.WithStack(errSnapshotMetadataNil)
	}
	// RawNode.Step still consumes snapshot payloads as an in-memory []byte, so
	// the transport can only delay materialization until the full stream has
	// been received and bounded on disk.
	payload, err := spool.Bytes()
	if err != nil {
		return raftpb.Message{}, err
	}
	metadata.Snapshot.Data = payload
	return metadata, nil
}
