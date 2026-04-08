package etcd

import (
	"bufio"
	"context"
	"io"
	"sync"
	"time"

	internalutil "github.com/bootjp/elastickv/internal"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/cockroachdb/errors"
	raftpb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/grpc"
)

const defaultSnapshotChunkSize = 1 << 20

const defaultDispatchTimeout = 5 * time.Second
const defaultSnapshotDispatchTimeout = 30 * time.Minute

var (
	errTransportPeerUnknown = errors.New("etcd raft peer is not configured")
	errTransportHandlerNil  = errors.New("etcd raft transport handler is not configured")
	errSnapshotMetadataNil  = errors.New("etcd raft snapshot metadata is required")
	errSnapshotMessageNil   = errors.New("etcd raft snapshot message is required")
	errSnapshotStreamShort  = errors.New("etcd raft snapshot stream closed before final chunk")
)

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
}

func NewGRPCTransport(peers []Peer) *GRPCTransport {
	peerMap := make(map[uint64]Peer, len(peers))
	for _, peer := range peers {
		peerMap[peer.NodeID] = peer
	}
	return &GRPCTransport{
		peers:             peerMap,
		clients:           make(map[string]pb.EtcdRaftClient),
		conns:             make(map[string]*grpc.ClientConn),
		snapshotChunkSize: defaultSnapshotChunkSize,
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

func (t *GRPCTransport) Close() error {
	if t == nil {
		return nil
	}
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

func (t *GRPCTransport) Dispatch(ctx context.Context, msg raftpb.Message) error {
	if t == nil {
		return nil
	}
	if msg.Type == raftpb.MsgSnap || (msg.Snapshot != nil && len(msg.Snapshot.Data) > 0) {
		ctx, cancel := transportContext(ctx, defaultSnapshotDispatchTimeout)
		defer cancel()
		return t.sendSnapshot(ctx, msg)
	}
	ctx, cancel := transportContext(ctx, defaultDispatchTimeout)
	defer cancel()

	raw, err := msg.Marshal()
	if err != nil {
		return errors.WithStack(err)
	}
	client, err := t.clientFor(msg.To)
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
	return t.handle(stream.Context(), msg)
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

	conn, err := grpc.NewClient(peer.Address, internalutil.GRPCDialOptions()...)
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
			return sendSnapshotChunk(stream, &pb.EtcdRaftSnapshotChunk{Metadata: header, Final: true})
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
			return false, errors.WithStack(errSnapshotMetadataNil)
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

func buildSnapshotMessage(metadata raftpb.Message, spool *snapshotSpool, seenMetadata bool) (raftpb.Message, error) {
	if !seenMetadata || metadata.Snapshot == nil {
		return raftpb.Message{}, errors.WithStack(errSnapshotMetadataNil)
	}
	payload, err := spool.Bytes()
	if err != nil {
		return raftpb.Message{}, err
	}
	metadata.Snapshot.Data = payload
	return metadata, nil
}
