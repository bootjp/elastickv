package etcd

import (
	"bufio"
	"context"
	"io"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	internalutil "github.com/bootjp/elastickv/internal"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/cockroachdb/errors"
	raftpb "go.etcd.io/raft/v3/raftpb"
	"golang.org/x/sync/singleflight"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

const defaultSnapshotChunkSize = 16 << 20

const defaultDispatchTimeout = 5 * time.Second
const defaultSnapshotDispatchTimeout = 30 * time.Minute
const defaultSendStreamReprobeInterval = 30 * time.Second
const sendStreamEnabledEnvVar = "ELASTICKV_RAFT_SEND_STREAM"

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
	errSnapshotDispatchBusy      = errors.New("etcd raft snapshot dispatch already in progress")
	errReceivedFSMSnapshotStale  = errors.New("etcd raft received fsm snapshot is stale")
	errPeerStreamClosed          = errors.New("etcd raft SendStream closed")
	errSendStreamDisabled        = errors.New("etcd raft SendStream is disabled")
)

var grpcNewClient = grpc.NewClient

type MessageHandler func(context.Context, raftpb.Message) error

// TransportStats is a monotonic snapshot of successful streaming transport
// activity. The counters are observational only: no dispatch or retry decision
// depends on them.
type TransportStats struct {
	SendStreamOpens      uint64
	SendStreamReconnects uint64
	SendStreamMessages   uint64
	SnapshotStreamSends  uint64
	SnapshotPayloadBytes uint64
}

type peerStream struct {
	mu     sync.Mutex
	stream pb.EtcdRaft_SendStreamClient
	cancel context.CancelFunc
	gate   context.Context
	done   chan struct{}
	errMu  sync.Mutex
	err    error
}

func newPeerStream(stream pb.EtcdRaft_SendStreamClient, cancel context.CancelFunc, gate context.Context) *peerStream {
	peer := &peerStream{
		stream: stream,
		cancel: cancel,
		gate:   gate,
		done:   make(chan struct{}),
	}
	go peer.watchTerminal()
	return peer
}

func (s *peerStream) watchTerminal() {
	var ack pb.EtcdRaftAck
	err := s.stream.RecvMsg(&ack)
	if err == nil {
		err = errPeerStreamClosed
	}
	s.errMu.Lock()
	s.err = err
	s.errMu.Unlock()
	close(s.done)
}

func (s *peerStream) terminalErr() error {
	select {
	case <-s.done:
	default:
		return nil
	}
	s.errMu.Lock()
	defer s.errMu.Unlock()
	return s.err
}

func (s *peerStream) disabledErr() error {
	if s != nil && s.gate != nil && s.gate.Err() != nil {
		return errors.WithStack(sendStreamDisabledError())
	}
	return nil
}

type GRPCTransport struct {
	pb.UnimplementedEtcdRaftServer

	mu                  sync.RWMutex
	peers               map[uint64]Peer
	clients             map[string]pb.EtcdRaftClient
	conns               map[string]*grpc.ClientConn
	streams             map[string]*peerStream
	streamSupported     map[string]bool
	streamUnsupported   map[string]bool
	streamUnsupportedAt map[string]time.Time
	streamOpenedBefore  map[string]struct{}
	sendStreamDisabled  bool
	sendStreamCtx       context.Context
	sendStreamCancel    context.CancelFunc
	handler             MessageHandler
	snapshotChunkSize   int
	spoolDir            string
	fsmSnapDir          string
	prepareFSMWrite     func(index uint64) error
	protectFSMWrite     func(index uint64) bool
	unprotectFSMWrite   func(index uint64)
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
	bridgeSem       chan struct{}
	snapshotSendSem chan struct{}
<<<<<<< HEAD
=======

	sendStreamOpenCount      atomic.Uint64
	sendStreamReconnectCount atomic.Uint64
	sendStreamMessageCount   atomic.Uint64
	snapshotStreamSendCount  atomic.Uint64
	snapshotPayloadByteCount atomic.Uint64
>>>>>>> origin/design/hotspot-split-m2-promotion-complete
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
	sendStreamCtx, sendStreamCancel := context.WithCancel(context.Background())
	sendStreamDisabled := !sendStreamEnabledFromEnv()
	if sendStreamDisabled {
		sendStreamCancel()
	}
	return &GRPCTransport{
		peers:               peerMap,
		clients:             make(map[string]pb.EtcdRaftClient),
		conns:               make(map[string]*grpc.ClientConn),
		streams:             make(map[string]*peerStream),
		streamSupported:     make(map[string]bool),
		streamUnsupported:   make(map[string]bool),
		streamUnsupportedAt: make(map[string]time.Time),
		streamOpenedBefore:  make(map[string]struct{}),
		sendStreamDisabled:  sendStreamDisabled,
		sendStreamCtx:       sendStreamCtx,
		sendStreamCancel:    sendStreamCancel,
		snapshotChunkSize:   defaultSnapshotChunkSize,
		bridgeSem:           make(chan struct{}, defaultBridgeMaterializeLimit),
		snapshotSendSem:     make(chan struct{}, 1),
	}
}

// Stats returns a point-in-time copy of the transport's monotonic streaming
// counters. It is safe to call concurrently with dispatch.
func (t *GRPCTransport) Stats() TransportStats {
	if t == nil {
		return TransportStats{}
	}
	return TransportStats{
		SendStreamOpens:      t.sendStreamOpenCount.Load(),
		SendStreamReconnects: t.sendStreamReconnectCount.Load(),
		SendStreamMessages:   t.sendStreamMessageCount.Load(),
		SnapshotStreamSends:  t.snapshotStreamSendCount.Load(),
		SnapshotPayloadBytes: t.snapshotPayloadByteCount.Load(),
	}
}

func (t *GRPCTransport) recordSnapshotStreamSend(payloadBytes uint64) {
	t.snapshotStreamSendCount.Add(1)
	t.snapshotPayloadByteCount.Add(payloadBytes)
}

func snapshotPayloadByteCount(size int64) uint64 {
	if size <= 0 {
		return 0
	}
	return uint64(size) //nolint:gosec // size is explicitly checked non-negative above.
}

func sendStreamEnabledFromEnv() bool {
	raw := strings.TrimSpace(os.Getenv(sendStreamEnabledEnvVar))
	if raw == "" {
		return true
	}
	enabled, err := strconv.ParseBool(raw)
	if err != nil {
		slog.Warn("invalid ELASTICKV_RAFT_SEND_STREAM; using default", "value", raw, "default", true)
		return true
	}
	return enabled
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

func (t *GRPCTransport) SetFSMSnapshotPrepare(fn func(index uint64) error) {
	if t == nil {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	t.prepareFSMWrite = fn
}

func (t *GRPCTransport) SetFSMSnapshotProtection(protectFn func(index uint64) bool, unprotectFn func(index uint64)) {
	if t == nil {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	t.protectFSMWrite = protectFn
	t.unprotectFSMWrite = unprotectFn
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

// SetSendStreamEnabled toggles outbound regular Raft message streaming.
// Disabling it closes cached streams so later dispatches use unary Send.
func (t *GRPCTransport) SetSendStreamEnabled(enabled bool) {
	if t == nil {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if enabled {
		if !t.sendStreamDisabled {
			return
		}
		t.sendStreamCtx, t.sendStreamCancel = context.WithCancel(context.Background())
		t.sendStreamDisabled = false
		return
	}
	if !t.sendStreamDisabled && t.sendStreamCancel != nil {
		t.sendStreamCancel()
	}
	t.sendStreamDisabled = true
	for address := range t.streams {
		t.closePeerStreamLocked(address)
	}
}

func (t *GRPCTransport) UpsertPeer(peer Peer) {
	if t == nil || peer.NodeID == 0 {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()

	if existing, ok := t.peers[peer.NodeID]; ok && existing.Address != "" && existing.Address != peer.Address {
		t.closePeerConnLocked(existing.Address)
	}
	t.peers[peer.NodeID] = peer
}

func (t *GRPCTransport) RemovePeer(nodeID uint64) {
	if t == nil || nodeID == 0 {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()

	peer, ok := t.peers[nodeID]
	if !ok {
		return
	}
	delete(t.peers, nodeID)
	t.closePeerConnLocked(peer.Address)
}

func (t *GRPCTransport) Close() error {
	if t == nil {
		return nil
	}
	t.mu.Lock()
	defer t.mu.Unlock()

	var err error
	for addr, stream := range t.streams {
		delete(t.streams, addr)
		stream.cancel()
	}
	for addr, conn := range t.conns {
		delete(t.conns, addr)
		delete(t.clients, addr)
		err = errors.CombineErrors(err, errors.WithStack(conn.Close()))
	}
	return errors.WithStack(err)
}

func (t *GRPCTransport) closePeerConnLocked(address string) {
	if address == "" {
		return
	}
	t.closePeerStreamLocked(address)
	delete(t.streamSupported, address)
	delete(t.streamUnsupported, address)
	delete(t.streamUnsupportedAt, address)
	conn, ok := t.conns[address]
	if !ok {
		delete(t.clients, address)
		return
	}
	delete(t.conns, address)
	delete(t.clients, address)
	if err := conn.Close(); err != nil {
		slog.Warn("failed to close etcd raft peer connection", "address", address, "error", err)
	}
}

func (t *GRPCTransport) closePeerStream(address string, expected *peerStream) {
	if address == "" {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	stream, ok := t.streams[address]
	if !ok || (expected != nil && stream != expected) {
		return
	}
	t.closePeerStreamLocked(address)
}

func (t *GRPCTransport) closePeerStreamLocked(address string) {
	stream, ok := t.streams[address]
	if !ok {
		return
	}
	delete(t.streams, address)
	stream.cancel()
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
	return msg.GetType() == raftpb.MsgSnap || (msg.Snapshot != nil && len(msg.Snapshot.Data) > 0)
}

func (t *GRPCTransport) dispatchSnapshot(ctx context.Context, msg raftpb.Message) error {
	ctx, cancel := transportContext(ctx, defaultSnapshotDispatchTimeout)
	defer cancel()
	if !t.tryAcquireSnapshotSend() {
		return errors.WithStack(errors.Mark(status.Error(codes.ResourceExhausted, errSnapshotDispatchBusy.Error()), errSnapshotDispatchBusy))
	}
	defer t.releaseSnapshotSend()

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

func (t *GRPCTransport) tryAcquireSnapshotSend() bool {
	if t == nil || t.snapshotSendSem == nil {
		return true
	}
	select {
	case t.snapshotSendSem <- struct{}{}:
		return true
	default:
		return false
	}
}

func (t *GRPCTransport) releaseSnapshotSend() {
	if t == nil || t.snapshotSendSem == nil {
		return
	}
	select {
	case <-t.snapshotSendSem:
	default:
	}
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
	client, err := t.clientFor(msg.GetTo())
	if err != nil {
		return err
	}
	stream, err := client.SendSnapshot(ctx)
	if err != nil {
		return errors.WithStack(err)
	}
	// Count payload bytes so a sender-side total can be correlated against the
	// receiver-side total when a follower fails to restore. A mismatch points
	// at transport truncation; a match points at a format/parsing issue.
	counter := &countingReadCloser{inner: rc}
	if err := sendSnapshotReaderChunks(stream, header, counter, t.chunkSize()); err != nil {
		return err
	}
	if _, err := stream.CloseAndRecv(); err != nil {
		return errors.WithStack(err)
	}
	t.recordSnapshotStreamSend(snapshotPayloadByteCount(counter.n))
	slog.Info("etcd raft snapshot stream sent",
		"index", index,
		"to", msg.GetTo(),
		"payload_bytes", counter.n,
	)
	return nil
}

type countingReadCloser struct {
	inner io.ReadCloser
	n     int64
}

func (c *countingReadCloser) Read(p []byte) (int, error) {
	n, err := c.inner.Read(p)
	c.n += int64(n)
	return n, err //nolint:wrapcheck // preserve io.EOF sentinel identity for callers
}

func (c *countingReadCloser) Close() error {
	return c.inner.Close() //nolint:wrapcheck // caller expects the underlying close error verbatim
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
	ctx, cancel := transportContext(ctx, defaultDispatchTimeout)
	defer cancel()

	raw, err := proto.Marshal(&msg)
	if err != nil {
		return errors.WithStack(err)
	}
	peer, err := t.peerFor(msg.GetTo())
	if err != nil {
		return err
	}
	client, err := t.clientForPeer(peer)
	if err != nil {
		return err
	}
	req := &pb.EtcdRaftMessage{Message: raw}
	if isPriorityMsg(msg.GetType()) || !t.sendStreamEnabledNow() || !t.allowPeerStreamProbe(peer.Address, time.Now()) {
		err := t.dispatchRegularUnary(ctx, client, req)
		t.closePeerConnOnRetryableDialError(peer.Address, err)
		return err
	}
	err = t.dispatchRegularStream(ctx, peer.Address, client, req)
	if err == nil {
		return nil
	}
	if grpcStatusCode(err) == codes.Unimplemented {
		t.markPeerStreamUnsupported(peer.Address)
		err := t.dispatchRegularUnary(ctx, client, req)
		t.closePeerConnOnRetryableDialError(peer.Address, err)
		return err
	}
	if isSendStreamDisabled(err) {
		err := t.dispatchRegularUnary(ctx, client, req)
		t.closePeerConnOnRetryableDialError(peer.Address, err)
		return err
	}
	t.closePeerConnOnRetryableDialError(peer.Address, err)
	return errors.WithStack(err)
}

func (t *GRPCTransport) dispatchRegularUnary(ctx context.Context, client pb.EtcdRaftClient, req *pb.EtcdRaftMessage) error {
	_, err := client.Send(ctx, req)
	return errors.WithStack(err)
}

func (t *GRPCTransport) closePeerConnOnRetryableDialError(address string, err error) {
	if grpcStatusCode(err) != codes.Unavailable {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	t.closePeerConnLocked(address)
}

func (t *GRPCTransport) dispatchRegularStream(ctx context.Context, address string, client pb.EtcdRaftClient, req *pb.EtcdRaftMessage) error {
	stream, err := t.streamFor(ctx, address, client)
	if err != nil {
		return err
	}
	stopCancel := context.AfterFunc(ctx, func() {
		t.closePeerStream(address, stream)
	})
	defer stopCancel()

	stream.mu.Lock()
	if err := stream.terminalErr(); err != nil {
		stream.mu.Unlock()
		t.closePeerStream(address, stream)
		if disabledErr := stream.disabledErr(); disabledErr != nil {
			return disabledErr
		}
		return errors.WithStack(err)
	}
	err = stream.stream.Send(req)
	if err == nil {
		t.sendStreamMessageCount.Add(1)
		err = stream.terminalErr()
	}
	stream.mu.Unlock()
	if err == nil && ctx.Err() != nil {
		err = ctx.Err()
	}
	if err != nil {
		t.closePeerStream(address, stream)
		if disabledErr := stream.disabledErr(); disabledErr != nil {
			return disabledErr
		}
		return errors.WithStack(err)
	}
	return nil
}

func (t *GRPCTransport) streamFor(ctx context.Context, address string, client pb.EtcdRaftClient) (*peerStream, error) {
	stream, err := t.cachedStream(address)
	if err != nil || stream != nil {
		return stream, err
	}
	if !t.allowPeerStreamProbe(address, time.Now()) {
		return nil, errors.WithStack(status.Error(codes.Unimplemented, "etcd raft SendStream is not supported by peer"))
	}

	value, err, _ := t.dialGroup.Do("stream:"+address, func() (any, error) {
		return t.openPeerStream(ctx, address, client)
	})
	if err != nil {
		return nil, errors.WithStack(err)
	}
	stream, ok := value.(*peerStream)
	if !ok {
		return nil, errors.New("etcd raft transport stream group returned unexpected stream type")
	}
	return stream, nil
}

func (t *GRPCTransport) cachedStream(address string) (*peerStream, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	if t.sendStreamDisabled {
		return nil, errors.WithStack(sendStreamDisabledError())
	}
	stream, ok := t.streams[address]
	if ok {
		return stream, nil
	}
	return nil, nil
}

func (t *GRPCTransport) openPeerStream(ctx context.Context, address string, client pb.EtcdRaftClient) (*peerStream, error) {
	stream, err := t.cachedStream(address)
	if err != nil || stream != nil {
		return stream, err
	}
	if !t.allowPeerStreamProbe(address, time.Now()) {
		return nil, errors.WithStack(status.Error(codes.Unimplemented, "etcd raft SendStream is not supported by peer"))
	}
	if err := t.probeSendStream(ctx, address, client); err != nil {
		return nil, err
	}

	streamCtx, cancel, gateCtx, err := t.sendStreamContext(context.Background())
	if err != nil {
		return nil, err
	}
	sendStream, err := client.SendStream(streamCtx)
	if err != nil {
		cancel()
		return nil, wrapSendStreamContextErr(err, gateCtx)
	}
	opened := newPeerStream(sendStream, cancel, gateCtx)

	t.mu.Lock()
	defer t.mu.Unlock()
	if t.sendStreamDisabled {
		cancel()
		return nil, errors.WithStack(sendStreamDisabledError())
	}
	if existing, ok := t.streams[address]; ok {
		cancel()
		return existing, nil
	}
	t.streams[address] = opened
	if _, reopened := t.streamOpenedBefore[address]; reopened {
		t.sendStreamReconnectCount.Add(1)
	} else {
		t.streamOpenedBefore[address] = struct{}{}
	}
	t.sendStreamOpenCount.Add(1)
	go func() {
		<-opened.done
		t.closePeerStream(address, opened)
	}()
	return opened, nil
}

func (t *GRPCTransport) probeSendStream(ctx context.Context, address string, client pb.EtcdRaftClient) error {
	probeCtx, cancel, gateCtx, err := t.sendStreamContext(ctx)
	if err != nil {
		return err
	}
	defer cancel()
	t.mu.RLock()
	supported := t.streamSupported[address]
	t.mu.RUnlock()
	if supported {
		return nil
	}
	if !t.allowPeerStreamProbe(address, time.Now()) {
		return errors.WithStack(status.Error(codes.Unimplemented, "etcd raft SendStream is not supported by peer"))
	}

	stream, err := client.SendStream(probeCtx)
	if err != nil {
		if grpcStatusCode(err) == codes.Unimplemented {
			t.markPeerStreamUnsupported(address)
		}
		return wrapSendStreamContextErr(err, gateCtx)
	}
	if _, err := stream.CloseAndRecv(); err != nil {
		if grpcStatusCode(err) == codes.Unimplemented {
			t.markPeerStreamUnsupported(address)
		}
		return wrapSendStreamContextErr(err, gateCtx)
	}
	t.markPeerStreamSupported(address)
	return nil
}

func (t *GRPCTransport) peerStreamUnsupported(address string) bool {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.streamUnsupported[address]
}

func (t *GRPCTransport) sendStreamEnabledNow() bool {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return !t.sendStreamDisabled
}

func (t *GRPCTransport) sendStreamContext(parent context.Context) (context.Context, context.CancelFunc, context.Context, error) {
	t.mu.RLock()
	if t.sendStreamDisabled {
		t.mu.RUnlock()
		return nil, nil, nil, errors.WithStack(sendStreamDisabledError())
	}
	gateCtx := t.sendStreamCtx
	t.mu.RUnlock()
	if gateCtx == nil {
		gateCtx = context.Background()
	}

	ctx, cancel := context.WithCancel(gateCtx)
	if parent == nil {
		return ctx, cancel, gateCtx, nil
	}
	stopParentCancel := context.AfterFunc(parent, cancel)
	return ctx, func() {
		stopParentCancel()
		cancel()
	}, gateCtx, nil
}

func wrapSendStreamContextErr(err error, gateCtx context.Context) error {
	if err == nil {
		return nil
	}
	if gateCtx != nil && gateCtx.Err() != nil {
		return errors.WithStack(sendStreamDisabledError())
	}
	return errors.WithStack(err)
}

func sendStreamDisabledError() error {
	return errors.WithStack(errors.Mark(status.Error(codes.Unavailable, errSendStreamDisabled.Error()), errSendStreamDisabled))
}

func isSendStreamDisabled(err error) bool {
	return errors.Is(err, errSendStreamDisabled)
}

func (t *GRPCTransport) allowPeerStreamProbe(address string, now time.Time) bool {
	t.mu.RLock()
	unsupported := t.streamUnsupported[address]
	markedAt := t.streamUnsupportedAt[address]
	t.mu.RUnlock()
	if !unsupported {
		return true
	}
	if markedAt.IsZero() || now.Before(markedAt.Add(defaultSendStreamReprobeInterval)) {
		return false
	}

	t.mu.Lock()
	defer t.mu.Unlock()
	currentMarkedAt := t.streamUnsupportedAt[address]
	if !t.streamUnsupported[address] {
		return true
	}
	if currentMarkedAt.IsZero() || now.Before(currentMarkedAt.Add(defaultSendStreamReprobeInterval)) {
		return false
	}
	delete(t.streamUnsupported, address)
	delete(t.streamUnsupportedAt, address)
	return true
}

func (t *GRPCTransport) markPeerStreamSupported(address string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.streamSupported[address] = true
	delete(t.streamUnsupported, address)
	delete(t.streamUnsupportedAt, address)
}

func (t *GRPCTransport) markPeerStreamUnsupported(address string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.streamUnsupported[address] = true
	t.streamUnsupportedAt[address] = time.Now()
	delete(t.streamSupported, address)
	t.closePeerStreamLocked(address)
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
		// If receive finalized the snapshot as a .fsm file (token in
		// Snapshot.Data), the engine refused to accept it into raft —
		// likely a transient context cancel or closed engine. Remove the
		// on-disk file so retries at later indexes don't leak orphan .fsm
		// payloads into fsmSnapDir until the next startup runs cleanup.
		// Same-index retries are already safe because os.Rename atomically
		// replaces the prior file.
		t.removeOrphanedFSMSnapshot(msg)
		return err
	}
	return errors.WithStack(stream.SendAndClose(&pb.EtcdRaftAck{}))
}

func (t *GRPCTransport) SendStream(stream pb.EtcdRaft_SendStreamServer) error {
	for {
		req, err := stream.Recv()
		switch {
		case errors.Is(err, io.EOF):
			return errors.WithStack(stream.SendAndClose(&pb.EtcdRaftAck{}))
		case err != nil:
			return errors.WithStack(err)
		case req == nil:
			continue
		}
		if err := t.handleRaftRequest(stream.Context(), req); err != nil {
			return err
		}
	}
}

// removeOrphanedFSMSnapshot deletes the .fsm file that
// receiveSnapshotStream finalized for `msg`, if any. Used by
// SendSnapshot when the engine handler (`t.handle`) fails after the
// receive succeeded. A non-nil return means the snapshot was not accepted into
// raft, so the file is unreferenced and safe to remove.
//
// Best-effort: a cleanup failure here is logged but not returned because
// the original apply error is the actionable signal; orphans get swept
// by cleanupStaleFSMSnaps at the next engine restart even if Remove
// races with another process.
func (t *GRPCTransport) removeOrphanedFSMSnapshot(msg raftpb.Message) {
	if msg.Snapshot == nil || !isSnapshotToken(msg.Snapshot.Data) {
		return
	}
	tok, err := decodeSnapshotToken(msg.Snapshot.Data)
	if err != nil {
		return
	}
	t.mu.RLock()
	fsmSnapDir := t.fsmSnapDir
	unprotectFn := t.unprotectFSMWrite
	t.mu.RUnlock()
	if unprotectFn != nil {
		defer unprotectFn(tok.Index)
	}
	if fsmSnapDir == "" {
		return
	}
	path := fsmSnapPath(fsmSnapDir, tok.Index)
	if rmErr := os.Remove(path); rmErr != nil && !os.IsNotExist(rmErr) {
		slog.Warn("failed to remove orphaned fsm snapshot file after apply failure",
			"path", path,
			"index", tok.Index,
			"err", rmErr,
		)
	}
}

func (t *GRPCTransport) Send(ctx context.Context, req *pb.EtcdRaftMessage) (*pb.EtcdRaftAck, error) {
	if req == nil {
		return &pb.EtcdRaftAck{}, nil
	}
	if err := t.handleRaftRequest(ctx, req); err != nil {
		return nil, err
	}
	return &pb.EtcdRaftAck{}, nil
}

func (t *GRPCTransport) handleRaftRequest(ctx context.Context, req *pb.EtcdRaftMessage) error {
	var msg raftpb.Message
	if err := proto.Unmarshal(req.Message, &msg); err != nil {
		return errors.WithStack(err)
	}
	if err := t.handle(ctx, msg); err != nil {
		return err
	}
	return nil
}

func (t *GRPCTransport) sendSnapshot(ctx context.Context, msg raftpb.Message) error {
	client, err := t.clientFor(msg.GetTo())
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
	if _, err := stream.CloseAndRecv(); err != nil {
		return errors.WithStack(err)
	}
	t.recordSnapshotStreamSend(uint64(len(payload)))
	return nil
}

func (t *GRPCTransport) sendSnapshotSpool(ctx context.Context, msg raftpb.Message, spool *snapshotSpool) error {
	client, err := t.clientFor(msg.GetTo())
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
	if _, err := stream.CloseAndRecv(); err != nil {
		return errors.WithStack(err)
	}
	t.recordSnapshotStreamSend(snapshotPayloadByteCount(spool.size))
	return nil
}

func (t *GRPCTransport) clientFor(to uint64) (pb.EtcdRaftClient, error) {
	peer, err := t.peerFor(to)
	if err != nil {
		return nil, err
	}
	return t.clientForPeer(peer)
}

func (t *GRPCTransport) clientForPeer(peer Peer) (pb.EtcdRaftClient, error) {
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

func grpcStatusCode(err error) codes.Code {
	if err == nil {
		return codes.OK
	}
	return status.Code(err)
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
	header, err := proto.Marshal(&metadata)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return header, nil
}

func sendSnapshotChunks(stream pb.EtcdRaft_SendSnapshotClient, header []byte, payload []byte, chunkSize int) error {
	if err := sendSnapshotChunk(stream, &pb.EtcdRaftSnapshotChunk{Metadata: header}); err != nil {
		return err
	}
	if len(payload) == 0 {
		return sendSnapshotChunk(stream, &pb.EtcdRaftSnapshotChunk{Final: true})
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
	if err := sendSnapshotChunk(stream, &pb.EtcdRaftSnapshotChunk{Metadata: header}); err != nil {
		return err
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
				Chunk: current,
				Final: true,
			})
		}
		return errors.WithStack(err)
	}
	return streamReaderChunks(stream, nil, buffered, current, chunkSize)
}

// streamReaderChunks drains buffered starting from `current` (the first full
// chunk already read by the caller) and emits gRPC chunks. When the payload
// length is not a whole multiple of chunkSize the last readSnapshotChunk call
// returns (partial, io.EOF) via io.ErrUnexpectedEOF: in that case `current`
// is still a full chunk that must be flushed as non-final and `next` is the
// trailing partial that must be emitted as the final chunk. Emitting only
// `current` with Final=true would silently drop the trailing bytes and
// truncate the stream to the previous chunkSize boundary on the receiver.
func streamReaderChunks(stream pb.EtcdRaft_SendSnapshotClient, header []byte, buffered *bufio.Reader, current []byte, chunkSize int) error {
	first := true
	for {
		next, nextErr := readSnapshotChunk(buffered, chunkSize)
		final := errors.Is(nextErr, io.EOF)
		if nextErr != nil && !final {
			return nextErr
		}
		isLast := final && len(next) == 0
		metadata := header
		if !first {
			metadata = nil
		}
		first = false
		if err := sendSnapshotChunk(stream, &pb.EtcdRaftSnapshotChunk{
			Metadata: metadata,
			Chunk:    current,
			Final:    isLast,
		}); err != nil {
			return err
		}
		if isLast {
			return nil
		}
		if final {
			return sendSnapshotChunk(stream, &pb.EtcdRaftSnapshotChunk{
				Chunk: next,
				Final: true,
			})
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

// snapshotSpoolPlacement returns the snapshot receive paths/callback under the
// transport lock. When fsmSnapDir is wired, the spool itself is placed inside it so
// FinalizeAsFSMFile's rename stays intra-filesystem and cannot fail with
// EXDEV. Standard engine wiring puts both under cfg.DataDir, but the
// receive code should not assume that. The legacy fallback path
// (fsmSnapDir == "") keeps the spool in spoolDir because it never renames
// — Bytes() materializes the payload in place.
func (t *GRPCTransport) snapshotSpoolPlacement() (
	placement string,
	fsmSnapDir string,
	prepareFn func(uint64) error,
	protectFn func(uint64) bool,
	unprotectFn func(uint64),
) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	fsmSnapDir = t.fsmSnapDir
	prepareFn = t.prepareFSMWrite
	protectFn = t.protectFSMWrite
	unprotectFn = t.unprotectFSMWrite
	if fsmSnapDir != "" {
		return fsmSnapDir, fsmSnapDir, prepareFn, protectFn, unprotectFn
	}
	return t.spoolDir, "", prepareFn, protectFn, unprotectFn
}

func (t *GRPCTransport) receiveSnapshotStream(stream pb.EtcdRaft_SendSnapshotServer) (raftpb.Message, error) {
	spoolPlacement, fsmSnapDir, prepareFn, protectFn, unprotectFn := t.snapshotSpoolPlacement()
	metadata, firstPayloadChunk, preparedFSMWrite, err := receiveSnapshotMetadata(stream, fsmSnapDir, prepareFn)
	if err != nil {
		return raftpb.Message{}, err
	}
	protection, err := protectReceivedSnapshotMetadata(metadata, fsmSnapDir, protectFn, unprotectFn)
	if err != nil {
		return raftpb.Message{}, err
	}
	defer protection.releaseUnlessHandedOff()
	spool, err := newReceiveSnapshotSpool(spoolPlacement)
	if err != nil {
		return raftpb.Message{}, err
	}
	defer func() {
		// Log rather than swallow: a Close failure here points at a
		// half-written spool file we couldn't clean up (disk full,
		// permission flip mid-stream, …). Once FinalizeAsFSMFile has
		// transferred ownership, Close is a no-op so this only fires on
		// the unhappy paths that actually need an operator to look.
		if closeErr := spool.Close(); closeErr != nil {
			slog.Warn("snapshot spool close failed",
				"spool_dir", spoolPlacement,
				"err", closeErr,
			)
		}
	}()

	msg, payloadBytes, err := drainSnapshotChunksFrom(
		stream,
		spool,
		fsmSnapDir,
		prepareFn,
		protectFn,
		unprotectFn,
		metadata,
		firstPayloadChunk,
		preparedFSMWrite,
		protection.protected,
	)
	if err != nil {
		return raftpb.Message{}, err
	}
	protection.handoff()
	index := uint64(0)
	if msg.Snapshot != nil {
		index = msg.Snapshot.GetMetadata().GetIndex()
	}
	slog.Info("etcd raft snapshot stream received",
		"index", index,
		"from", msg.GetFrom(),
		"payload_bytes", payloadBytes,
		"format", snapshotDataFormatLabel(msg.Snapshot),
	)
	return msg, nil
}

type receivedSnapshotProtection struct {
	index       uint64
	protected   bool
	handedOff   bool
	unprotectFn func(uint64)
}

func protectReceivedSnapshotMetadata(
	metadata raftpb.Message,
	fsmSnapDir string,
	protectFn func(uint64) bool,
	unprotectFn func(uint64),
) (receivedSnapshotProtection, error) {
	if fsmSnapDir == "" || metadata.Snapshot == nil {
		return receivedSnapshotProtection{}, nil
	}
	index := metadata.Snapshot.GetMetadata().GetIndex()
	if index == 0 || protectFn == nil {
		return receivedSnapshotProtection{}, nil
	}
	if !protectFn(index) {
		return receivedSnapshotProtection{}, errors.WithStack(errReceivedFSMSnapshotStale)
	}
	return receivedSnapshotProtection{
		index:       index,
		protected:   true,
		unprotectFn: unprotectFn,
	}, nil
}

func (p *receivedSnapshotProtection) handoff() {
	if p == nil {
		return
	}
	p.handedOff = true
}

func (p *receivedSnapshotProtection) releaseUnlessHandedOff() {
	if p == nil || !p.protected || p.handedOff || p.unprotectFn == nil {
		return
	}
	p.unprotectFn(p.index)
}

// drainSnapshotChunks consumes the SendSnapshot stream into spool, computes
// CRC32C over the payload bytes as they hit disk, and on the final chunk
// hands off to finalizeReceivedSnapshot — which decides between the
// streaming-token path (rename to fsmSnapDir/<index>.fsm + 17-byte token
// in Snapshot.Data) and the legacy materialize fallback. Extracted from
// receiveSnapshotStream so that function stays under cyclop's complexity
// budget.
func drainSnapshotChunks(
	stream pb.EtcdRaft_SendSnapshotServer,
	spool *snapshotSpool,
	fsmSnapDir string,
	prepareFn func(uint64) error,
	protectFn func(uint64) bool,
	unprotectFn func(uint64),
) (raftpb.Message, int64, error) {
	var metadata raftpb.Message
	return drainSnapshotChunksFrom(stream, spool, fsmSnapDir, prepareFn, protectFn, unprotectFn, metadata, nil, false, false)
}

func drainSnapshotChunksFrom(
	stream pb.EtcdRaft_SendSnapshotServer,
	spool *snapshotSpool,
	fsmSnapDir string,
	prepareFn func(uint64) error,
	protectFn func(uint64) bool,
	unprotectFn func(uint64),
	metadata raftpb.Message,
	firstPayloadChunk *pb.EtcdRaftSnapshotChunk,
	preparedFSMWrite bool,
	preprotected bool,
) (raftpb.Message, int64, error) {
	seenMetadata := metadata.Snapshot != nil
	// Wrap spool with crc32CWriter so the CRC accumulates as bytes hit
	// disk. The CRC is only meaningful when we have an fsmSnapDir to
	// finalize into; the legacy fallback path discards it. Cost is
	// hashing speed (~GB/s on modern x86 with SSE 4.2 PCLMULQDQ), well
	// above gRPC stream throughput so the wrapper is invisible in
	// profiles.
	crcWriter := newCRC32CWriter(spool)

	var payloadBytes int64
	for {
		chunk, err := nextSnapshotChunk(stream, &firstPayloadChunk)
		if err != nil {
			return raftpb.Message{}, 0, err
		}
		seen, err := appendSnapshotChunkMetadata(&metadata, chunk, seenMetadata)
		if err != nil {
			return raftpb.Message{}, 0, err
		}
		seenMetadata = seen
		if !seenMetadata && len(chunk.Chunk) > 0 {
			return raftpb.Message{}, 0, errors.WithStack(errSnapshotMetadataNil)
		}
		if !preparedFSMWrite {
			preparedFSMWrite = maybePrepareReceivedFSMSnapshotWrite(metadata, fsmSnapDir, prepareFn, seenMetadata)
		}
		if err := writeSnapshotChunkPayload(crcWriter, chunk); err != nil {
			return raftpb.Message{}, 0, err
		}
		payloadBytes += int64(len(chunk.Chunk))
		if chunk.Final {
			msg, err := finalizeReceivedSnapshot(metadata, spool, crcWriter.Sum32(), fsmSnapDir, protectFn, unprotectFn, seenMetadata, preprotected)
			if err != nil {
				return raftpb.Message{}, 0, err
			}
			return msg, payloadBytes, nil
		}
	}
}

func nextSnapshotChunk(
	stream pb.EtcdRaft_SendSnapshotServer,
	firstPayloadChunk **pb.EtcdRaftSnapshotChunk,
) (*pb.EtcdRaftSnapshotChunk, error) {
	if *firstPayloadChunk != nil {
		chunk := *firstPayloadChunk
		*firstPayloadChunk = nil
		return chunk, nil
	}
	return recvSnapshotChunk(stream)
}

func receiveSnapshotMetadata(
	stream pb.EtcdRaft_SendSnapshotServer,
	fsmSnapDir string,
	prepareFn func(uint64) error,
) (raftpb.Message, *pb.EtcdRaftSnapshotChunk, bool, error) {
	var metadata raftpb.Message
	seenMetadata := false
	for {
		chunk, err := recvSnapshotChunk(stream)
		if err != nil {
			return raftpb.Message{}, nil, false, err
		}
		seen, err := appendSnapshotChunkMetadata(&metadata, chunk, seenMetadata)
		if err != nil {
			return raftpb.Message{}, nil, false, err
		}
		seenMetadata = seen
		if !seenMetadata && len(chunk.Chunk) > 0 {
			return raftpb.Message{}, nil, false, errors.WithStack(errSnapshotMetadataNil)
		}
		if seenMetadata {
			prepared := maybePrepareReceivedFSMSnapshotWrite(metadata, fsmSnapDir, prepareFn, true)
			firstPayloadChunk := &pb.EtcdRaftSnapshotChunk{
				Chunk: chunk.Chunk,
				Final: chunk.Final,
			}
			return metadata, firstPayloadChunk, prepared, nil
		}
		if chunk.Final {
			return raftpb.Message{}, nil, false, errors.WithStack(errSnapshotMetadataNil)
		}
	}
}

func recvSnapshotChunk(stream pb.EtcdRaft_SendSnapshotServer) (*pb.EtcdRaftSnapshotChunk, error) {
	chunk, err := stream.Recv()
	if err != nil {
		if errors.Is(err, io.EOF) {
			return nil, errors.WithStack(errSnapshotStreamShort)
		}
		return nil, errors.WithStack(err)
	}
	return chunk, nil
}

// finalizeReceivedSnapshot picks between the streaming-token path (when an
// fsmSnapDir is wired and the snapshot's metadata index is non-zero) and the
// legacy in-memory path. The streaming path renames the spool file in place
// to fsmSnapPath(fsmSnapDir, index), embeds a 17-byte EKVT token in
// Snapshot.Data, and lets restoreSnapshotState read the payload off disk via
// io.Reader — heap usage stays flat regardless of FSM size, eliminating the
// 1.35-GiB-FSM × 2.5-GiB-container OOM hazard observed in the 2026-05-08
// incident. The legacy path is preserved for tests and legacy receivers
// that have not wired a snapshot directory.
func finalizeReceivedSnapshot(
	metadata raftpb.Message,
	spool *snapshotSpool,
	crc32c uint32,
	fsmSnapDir string,
	protectFn func(uint64) bool,
	unprotectFn func(uint64),
	seenMetadata bool,
	preprotected bool,
) (raftpb.Message, error) {
	if !seenMetadata || metadata.Snapshot == nil {
		return raftpb.Message{}, errors.WithStack(errSnapshotMetadataNil)
	}
	index := metadata.Snapshot.GetMetadata().GetIndex()
	if fsmSnapDir == "" || index == 0 {
		// Legacy fallback: full materialization. Used by tests that don't wire an
		// fsmSnapDir and by the index=0 edge case (no canonical filename to
		// rename to).
		return buildSnapshotMessage(metadata, spool, seenMetadata)
	}
	protected, err := protectReceivedSnapshotForFinalize(index, preprotected, protectFn)
	if err != nil {
		return raftpb.Message{}, err
	}
	if err := spool.FinalizeAsFSMFile(fsmSnapDir, index, crc32c); err != nil {
		releaseFinalizeSnapshotProtection(index, protected, preprotected, unprotectFn)
		return raftpb.Message{}, err
	}
	metadata.Snapshot.Data = encodeSnapshotToken(index, crc32c)
	return metadata, nil
}

func protectReceivedSnapshotForFinalize(index uint64, preprotected bool, protectFn func(uint64) bool) (bool, error) {
	if preprotected {
		return true, nil
	}
	if protectFn == nil {
		return false, nil
	}
	if !protectFn(index) {
		return false, errors.WithStack(errReceivedFSMSnapshotStale)
	}
	return true, nil
}

func releaseFinalizeSnapshotProtection(index uint64, protected bool, preprotected bool, unprotectFn func(uint64)) {
	if !protected || preprotected || unprotectFn == nil {
		return
	}
	unprotectFn(index)
}

func maybePrepareReceivedFSMSnapshotWrite(
	metadata raftpb.Message,
	fsmSnapDir string,
	prepareFn func(uint64) error,
	seenMetadata bool,
) bool {
	if fsmSnapDir == "" || !seenMetadata || metadata.Snapshot == nil {
		return false
	}
	index := metadata.Snapshot.GetMetadata().GetIndex()
	if index == 0 {
		return false
	}
	prepareReceivedFSMSnapshotWrite(fsmSnapDir, index, prepareFn)
	return true
}

func prepareReceivedFSMSnapshotWrite(fsmSnapDir string, index uint64, prepareFn func(uint64) error) {
	var err error
	if prepareFn != nil {
		err = prepareFn(index)
	} else {
		err = prepareFSMSnapshotWrite("", fsmSnapDir, index)
	}
	if err != nil {
		slog.Warn("failed to prepare received fsm snapshot write",
			"index", index,
			"error", err,
		)
	}
}

// snapshotDataFormatLabel exists purely for the structured log line on the
// receiver — it lets an operator distinguish a streaming-token receive
// (small heap, payload on disk) from a legacy materialization (heap holds
// the full payload) at a glance, without grepping for byte counts.
func snapshotDataFormatLabel(snap *raftpb.Snapshot) string {
	if snap == nil {
		return "nil"
	}
	if isSnapshotToken(snap.Data) {
		return "token"
	}
	return "inline"
}

func appendSnapshotChunkMetadata(metadata *raftpb.Message, chunk *pb.EtcdRaftSnapshotChunk, seenMetadata bool) (bool, error) {
	if len(chunk.Metadata) > 0 {
		if seenMetadata {
			return false, errors.WithStack(errSnapshotMetadataDuplicate)
		}
		if err := proto.Unmarshal(chunk.Metadata, metadata); err != nil {
			return false, errors.WithStack(err)
		}
		seenMetadata = true
	}
	return seenMetadata, nil
}

func writeSnapshotChunkPayload(payload io.Writer, chunk *pb.EtcdRaftSnapshotChunk) error {
	if len(chunk.Chunk) > 0 {
		if _, err := payload.Write(chunk.Chunk); err != nil {
			return errors.WithStack(err)
		}
	}
	return nil
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
