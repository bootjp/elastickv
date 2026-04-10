package etcd

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"log/slog"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/cockroachdb/errors"
	etcdstorage "go.etcd.io/etcd/server/v3/storage"
	etcdraft "go.etcd.io/raft/v3"
	raftpb "go.etcd.io/raft/v3/raftpb"
)

const (
	defaultTickInterval      = 10 * time.Millisecond
	defaultHeartbeatTick     = 1
	defaultElectionTick      = 10
	defaultMaxInflightMsg    = 256
	defaultMaxSizePerMsg     = 1 << 20
	defaultDispatchWorkers   = 4
	defaultSnapshotEvery     = 10_000
	defaultAdminPollInterval = 10 * time.Millisecond
	defaultMaxPendingConfigs = 64
	unknownLastContact       = time.Duration(-1)

	proposalEnvelopeVersion  = byte(0x01)
	readContextVersion       = byte(0x02)
	confChangeContextVersion = byte(0x03)
	envelopeHeaderSize       = 9
	confChangeFixedSize      = 21
)

var (
	errNilEngine                  = errors.New("raft engine is not configured")
	errClosed                     = errors.New("etcd raft engine is closed")
	errNotLeader                  = errors.New("etcd raft engine is not leader")
	errNodeIDRequired             = errors.New("etcd raft node id is required")
	errDataDirRequired            = errors.New("etcd raft data dir is required")
	errStateMachineUnset          = errors.New("etcd raft state machine is not configured")
	errSnapshotRequired           = errors.New("etcd raft snapshot payload is required")
	errStepQueueFull              = errors.New("etcd raft inbound step queue is full")
	errClusterMismatch            = errors.New("etcd raft persisted cluster does not match configured peers")
	errConfigIndexMismatch        = errors.New("etcd raft configuration index does not match")
	errConfChangeContextTooLarge  = errors.New("etcd raft conf change context is too large")
	errLeadershipTransferTarget   = errors.New("etcd raft leadership transfer target is required")
	errLeadershipTransferNotReady = errors.New("etcd raft leadership transfer target is not available")
	errTooManyPendingConfigs      = errors.New("etcd raft engine has too many pending config changes")
)

type Snapshot interface {
	// Snapshot is an owned export handle from the state machine. Callers are
	// responsible for closing it after WriteTo completes.
	WriteTo(w io.Writer) (int64, error)
	Close() error
}

type StateMachine interface {
	Apply(data []byte) any
	// Snapshot should capture a stable export handle quickly. Expensive snapshot
	// serialization belongs in Snapshot.WriteTo, which the engine can run off
	// the main raft loop.
	Snapshot() (Snapshot, error)
	Restore(r io.Reader) error
}

type OpenConfig struct {
	NodeID         uint64
	LocalID        string
	LocalAddress   string
	DataDir        string
	Peers          []Peer
	Bootstrap      bool
	Transport      *GRPCTransport
	TickInterval   time.Duration
	ElectionTick   int
	HeartbeatTick  int
	StateMachine   StateMachine
	MaxSizePerMsg  uint64
	MaxInflightMsg int
}

type Engine struct {
	nodeID       uint64
	localID      string
	localAddress string
	dataDir      string
	tickInterval time.Duration

	storage   *etcdraft.MemoryStorage
	rawNode   *etcdraft.RawNode
	persist   etcdstorage.Storage
	fsm       StateMachine
	peers     map[uint64]Peer
	transport *GRPCTransport

	nextRequestID atomic.Uint64

	proposeCh      chan proposalRequest
	readCh         chan readRequest
	adminCh        chan adminRequest
	stepCh         chan raftpb.Message
	dispatchCh     chan dispatchRequest
	dispatchStopCh chan struct{}
	dispatchCtx    context.Context
	dispatchCancel context.CancelFunc
	snapshotReqCh  chan snapshotRequest
	snapshotResCh  chan snapshotResult
	snapshotStopCh chan struct{}
	closeCh        chan struct{}
	doneCh         chan struct{}
	startedCh      chan struct{}

	leaderReady  chan struct{}
	leaderOnce   sync.Once
	startOnce    sync.Once
	closeOnce    sync.Once
	dispatchOnce sync.Once
	snapshotOnce sync.Once
	dispatchWG   sync.WaitGroup
	snapshotWG   sync.WaitGroup

	mu          sync.RWMutex
	pending     sync.Mutex
	status      raftengine.Status
	config      raftengine.Configuration
	runErr      error
	closed      bool
	applied     uint64
	configIndex atomic.Uint64

	lastLeaderContactAt   time.Time
	lastLeaderContactFrom uint64

	// Restore swaps the underlying store state and must not race with the short
	// critical section that publishes a newly persisted local snapshot.
	snapshotMu sync.Mutex

	dispatchDropCount  atomic.Uint64
	dispatchErrorCount atomic.Uint64

	pendingProposals map[uint64]proposalRequest
	pendingReads     map[uint64]readRequest
	pendingConfigs   map[uint64]adminRequest
	snapshotInFlight bool
	dispatchFn       func(context.Context, dispatchRequest) error
}

type adminAction byte

const (
	adminActionAddVoter adminAction = iota + 1
	adminActionRemoveServer
	adminActionTransferLeadership
)

type adminRequest struct {
	ctx       context.Context
	id        uint64
	action    adminAction
	peer      Peer
	prevIndex uint64
	done      chan adminResult
}

type adminResult struct {
	index uint64
	peer  Peer
	err   error
}

type proposalRequest struct {
	ctx     context.Context
	id      uint64
	payload []byte
	done    chan proposalResult
}

type proposalResult struct {
	result *raftengine.ProposalResult
	err    error
}

type readRequest struct {
	ctx         context.Context
	id          uint64
	done        chan readResult
	target      uint64
	waitApplied bool
}

type readResult struct {
	index uint64
	err   error
}

type snapshotRequest struct {
	index    uint64
	snapshot Snapshot
}

type snapshotResult struct {
	index uint64
	err   error
}

type dispatchRequest struct {
	msg raftpb.Message
}

// Open starts the etcd/raft backend.
//
// Single-node bootstrap waits for local leadership so callers can use the
// engine immediately. Multi-node startup returns after the local node is
// running; leadership is established asynchronously through raft transport.
func Open(ctx context.Context, cfg OpenConfig) (*Engine, error) {
	cfg = normalizeConfig(cfg)
	if err := validateConfig(cfg); err != nil {
		return nil, err
	}

	localPeer, peers, err := normalizePeers(cfg.NodeID, cfg.LocalID, cfg.LocalAddress, cfg.Peers)
	if err != nil {
		return nil, err
	}
	cfg.NodeID = localPeer.NodeID
	cfg.LocalID = localPeer.ID
	cfg.LocalAddress = localPeer.Address

	disk, err := openDiskState(cfg, peers)
	if err != nil {
		return nil, err
	}
	rawNode, err := newRawNode(cfg, disk.Storage)
	if err != nil {
		_ = closePersist(disk.Persist)
		return nil, err
	}

	peerMap := make(map[uint64]Peer, len(peers))
	for _, peer := range peers {
		peerMap[peer.NodeID] = peer
	}
	var dispatchCtx context.Context
	var dispatchCancel context.CancelFunc
	if cfg.Transport != nil {
		dispatchCtx, dispatchCancel = context.WithCancel(context.Background())
	}
	opened := false
	defer func() {
		if opened || dispatchCancel == nil {
			return
		}
		dispatchCancel()
	}()

	engine := &Engine{
		nodeID:           cfg.NodeID,
		localID:          cfg.LocalID,
		localAddress:     cfg.LocalAddress,
		dataDir:          cfg.DataDir,
		tickInterval:     cfg.TickInterval,
		storage:          disk.Storage,
		rawNode:          rawNode,
		persist:          disk.Persist,
		fsm:              cfg.StateMachine,
		peers:            peerMap,
		transport:        cfg.Transport,
		proposeCh:        make(chan proposalRequest),
		readCh:           make(chan readRequest),
		adminCh:          make(chan adminRequest),
		stepCh:           make(chan raftpb.Message, defaultMaxInflightMsg),
		closeCh:          make(chan struct{}),
		doneCh:           make(chan struct{}),
		startedCh:        make(chan struct{}),
		leaderReady:      make(chan struct{}),
		config:           configurationFromConfState(peerMap, disk.LocalSnap.Metadata.ConfState),
		applied:          maxAppliedIndex(disk.LocalSnap),
		dispatchCtx:      dispatchCtx,
		dispatchCancel:   dispatchCancel,
		pendingProposals: map[uint64]proposalRequest{},
		pendingReads:     map[uint64]readRequest{},
		pendingConfigs:   map[uint64]adminRequest{},
	}
	engine.configIndex.Store(maxAppliedIndex(disk.LocalSnap))
	engine.initTransport(cfg)
	engine.initSnapshotWorker()
	engine.refreshStatus()

	go engine.run()

	openedEngine, err := waitForOpen(ctx, engine, len(peers) == 1)
	if err != nil {
		return nil, err
	}
	opened = true
	return openedEngine, nil
}

func (e *Engine) initTransport(cfg OpenConfig) {
	if e.transport == nil {
		return
	}
	// Transport listeners may already be accepting RPCs when Open is called.
	// Gate inbound delivery on startedCh so messages are not enqueued until the
	// local run loop has completed startup.
	e.dispatchCh = make(chan dispatchRequest, dispatchQueueSize(cfg.MaxInflightMsg))
	e.dispatchStopCh = make(chan struct{})
	e.transport.SetSpoolDir(cfg.DataDir)
	e.transport.SetHandler(e.handleTransportMessage)
	e.startDispatchWorkers()
}

func (e *Engine) initSnapshotWorker() {
	if e.persist == nil {
		return
	}
	// Local snapshot persistence is only wired for disk-backed engines. When
	// persist is nil the prototype intentionally leaves snapshot workers nil
	// and maybePersistLocalSnapshot becomes a no-op.
	e.snapshotReqCh = make(chan snapshotRequest)
	e.snapshotResCh = make(chan snapshotResult, 1)
	e.snapshotStopCh = make(chan struct{})
	e.startSnapshotWorker()
}

func newRawNode(cfg OpenConfig, storage *etcdraft.MemoryStorage) (*etcdraft.RawNode, error) {
	rawNode, err := etcdraft.NewRawNode(&etcdraft.Config{
		ID:                        cfg.NodeID,
		ElectionTick:              cfg.ElectionTick,
		HeartbeatTick:             cfg.HeartbeatTick,
		Storage:                   storage,
		MaxSizePerMsg:             cfg.MaxSizePerMsg,
		MaxCommittedSizePerReady:  cfg.MaxSizePerMsg,
		MaxInflightMsgs:           cfg.MaxInflightMsg,
		CheckQuorum:               true,
		PreVote:                   true,
		ReadOnlyOption:            etcdraft.ReadOnlySafe,
		DisableProposalForwarding: true,
	})
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return rawNode, nil
}

func waitForOpen(ctx context.Context, engine *Engine, waitForLeader bool) (*Engine, error) {
	select {
	case <-ctx.Done():
		_ = engine.Close()
		return nil, errors.WithStack(ctx.Err())
	case <-engine.openReady(waitForLeader):
		return engine, nil
	case <-engine.doneCh:
		if err := engine.currentError(); err != nil {
			return nil, err
		}
		return nil, errors.WithStack(errClosed)
	}
}

func (e *Engine) Close() error {
	if e == nil {
		return nil
	}
	e.closeOnce.Do(func() {
		close(e.closeCh)
	})
	<-e.doneCh
	if err := e.currentError(); err != nil && !errors.Is(err, errClosed) {
		return err
	}
	return nil
}

func (e *Engine) Propose(ctx context.Context, data []byte) (*raftengine.ProposalResult, error) {
	if err := contextErr(ctx); err != nil {
		return nil, err
	}
	if e == nil {
		return nil, errors.WithStack(errNilEngine)
	}

	req := proposalRequest{
		ctx:     ctx,
		id:      e.nextID(),
		payload: append([]byte(nil), data...),
		done:    make(chan proposalResult, 1),
	}

	select {
	case <-ctx.Done():
		return nil, errors.WithStack(ctx.Err())
	case <-e.doneCh:
		return nil, e.currentErrorOrClosed()
	case e.proposeCh <- req:
	}

	select {
	case <-ctx.Done():
		e.cancelPendingProposal(req.id)
		return nil, errors.WithStack(ctx.Err())
	case res := <-req.done:
		if res.err != nil {
			return nil, res.err
		}
		return res.result, nil
	}
}

func (e *Engine) State() raftengine.State {
	if e == nil {
		return raftengine.StateUnknown
	}
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.status.State
}

func (e *Engine) Leader() raftengine.LeaderInfo {
	if e == nil {
		return raftengine.LeaderInfo{}
	}
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.status.Leader
}

func (e *Engine) VerifyLeader(ctx context.Context) error {
	_, err := e.submitRead(ctx, false)
	return err
}

func (e *Engine) CheckServing(ctx context.Context) error {
	if err := contextErr(ctx); err != nil {
		return err
	}
	if e == nil {
		return errors.WithStack(errNilEngine)
	}
	if e.State() != raftengine.StateLeader {
		return errors.WithStack(errNotLeader)
	}
	return nil
}

func (e *Engine) LinearizableRead(ctx context.Context) (uint64, error) {
	return e.submitRead(ctx, true)
}

func (e *Engine) submitRead(ctx context.Context, waitApplied bool) (uint64, error) {
	if err := contextErr(ctx); err != nil {
		return 0, err
	}
	if e == nil {
		return 0, errors.WithStack(errNilEngine)
	}
	req := readRequest{
		ctx:         ctx,
		id:          e.nextID(),
		done:        make(chan readResult, 1),
		waitApplied: waitApplied,
	}

	select {
	case <-ctx.Done():
		return 0, errors.WithStack(ctx.Err())
	case <-e.doneCh:
		return 0, e.currentErrorOrClosed()
	case e.readCh <- req:
	}

	select {
	case <-ctx.Done():
		e.cancelPendingRead(req.id)
		return 0, errors.WithStack(ctx.Err())
	case res := <-req.done:
		if res.err != nil {
			return 0, res.err
		}
		return res.index, nil
	}
}

func (e *Engine) Status() raftengine.Status {
	if e == nil {
		return raftengine.Status{State: raftengine.StateUnknown}
	}
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.status
}

func (e *Engine) Configuration(ctx context.Context) (raftengine.Configuration, error) {
	if err := contextErr(ctx); err != nil {
		return raftengine.Configuration{}, err
	}
	if e == nil {
		return raftengine.Configuration{}, errors.WithStack(errNilEngine)
	}
	return e.currentConfiguration(), nil
}

func (e *Engine) AddVoter(ctx context.Context, id string, address string, prevIndex uint64) (uint64, error) {
	peer, err := e.resolveAdminPeer(id, address)
	if err != nil {
		return 0, err
	}
	result, err := e.submitAdmin(ctx, adminActionAddVoter, peer, prevIndex)
	if err != nil {
		return 0, err
	}
	return result.index, nil
}

func (e *Engine) RemoveServer(ctx context.Context, id string, prevIndex uint64) (uint64, error) {
	result, err := e.submitAdmin(ctx, adminActionRemoveServer, Peer{ID: id}, prevIndex)
	if err != nil {
		return 0, err
	}
	return result.index, nil
}

func (e *Engine) TransferLeadership(ctx context.Context) error {
	result, err := e.submitAdmin(ctx, adminActionTransferLeadership, Peer{}, 0)
	if err != nil {
		return err
	}
	return e.waitForLeadershipTransfer(ctx, result.peer)
}

func (e *Engine) TransferLeadershipToServer(ctx context.Context, id string, address string) error {
	result, err := e.submitAdmin(ctx, adminActionTransferLeadership, Peer{ID: id, Address: address}, 0)
	if err != nil {
		return err
	}
	return e.waitForLeadershipTransfer(ctx, result.peer)
}

func (e *Engine) submitAdmin(ctx context.Context, action adminAction, peer Peer, prevIndex uint64) (adminResult, error) {
	if err := contextErr(ctx); err != nil {
		return adminResult{}, err
	}
	if e == nil {
		return adminResult{}, errors.WithStack(errNilEngine)
	}
	req := adminRequest{
		ctx:       ctx,
		id:        e.nextID(),
		action:    action,
		peer:      peer,
		prevIndex: prevIndex,
		done:      make(chan adminResult, 1),
	}

	select {
	case <-ctx.Done():
		return adminResult{}, errors.WithStack(ctx.Err())
	case <-e.doneCh:
		return adminResult{}, e.currentErrorOrClosed()
	case e.adminCh <- req:
	}

	select {
	case <-ctx.Done():
		e.cancelPendingConfig(req.id)
		return adminResult{}, errors.WithStack(ctx.Err())
	case res := <-req.done:
		if res.err != nil {
			return adminResult{}, res.err
		}
		return res, nil
	}
}

func (e *Engine) waitForLeadershipTransfer(ctx context.Context, target Peer) error {
	if target.NodeID == 0 {
		return errors.WithStack(errLeadershipTransferTarget)
	}
	ticker := time.NewTicker(defaultAdminPollInterval)
	defer ticker.Stop()

	for {
		if err := contextErr(ctx); err != nil {
			return err
		}
		status := e.Status()
		if status.State != raftengine.StateLeader && status.Leader.ID == target.ID {
			return nil
		}

		select {
		case <-ctx.Done():
			return errors.WithStack(ctx.Err())
		case <-e.doneCh:
			return e.currentErrorOrClosed()
		case <-ticker.C:
		}
	}
}

func (e *Engine) run() {
	defer close(e.doneCh)

	ticker := time.NewTicker(e.tickInterval)
	defer ticker.Stop()

	if err := e.startup(); err != nil {
		e.fail(err)
		return
	}
	e.markStarted()

	for {
		if err := e.drainReady(); err != nil {
			e.fail(err)
			return
		}

		ok, err := e.handleEvent(ticker.C)
		if err != nil {
			e.fail(err)
			return
		}
		if !ok {
			e.shutdown()
			return
		}
	}
}

func (e *Engine) startup() error {
	if err := e.drainReady(); err != nil {
		return err
	}
	if e.State() == raftengine.StateLeader {
		return nil
	}
	if !e.shouldCampaignSingleNode() {
		return nil
	}
	if err := e.rawNode.Campaign(); err != nil {
		return errors.WithStack(err)
	}
	return e.drainReady()
}

func (e *Engine) handleEvent(tick <-chan time.Time) (bool, error) {
	select {
	case <-e.closeCh:
		return false, nil
	case <-tick:
		e.rawNode.Tick()
	case req := <-e.proposeCh:
		e.handleProposal(req)
	case req := <-e.readCh:
		e.handleRead(req)
	case req := <-e.adminCh:
		e.handleAdmin(req)
	case msg := <-e.stepCh:
		e.handleStep(msg)
	case result := <-e.snapshotResCh:
		if err := e.handleSnapshotResult(result); err != nil {
			return false, err
		}
	}
	return true, nil
}

func (e *Engine) handleProposal(req proposalRequest) {
	if err := contextErr(req.ctx); err != nil {
		req.done <- proposalResult{err: err}
		return
	}
	if e.State() != raftengine.StateLeader {
		req.done <- proposalResult{err: errors.WithStack(errNotLeader)}
		return
	}
	e.storePendingProposal(req)
	if err := e.rawNode.Propose(encodeProposalEnvelope(req.id, req.payload)); err != nil {
		e.cancelPendingProposal(req.id)
		req.done <- proposalResult{err: errors.WithStack(err)}
	}
}

func (e *Engine) handleRead(req readRequest) {
	if err := contextErr(req.ctx); err != nil {
		req.done <- readResult{err: err}
		return
	}
	if e.State() != raftengine.StateLeader {
		req.done <- readResult{err: errors.WithStack(errNotLeader)}
		return
	}
	e.storePendingRead(req)
	e.rawNode.ReadIndex(encodeReadContext(req.id))
}

func (e *Engine) handleAdmin(req adminRequest) {
	if err := contextErr(req.ctx); err != nil {
		req.done <- adminResult{err: err}
		return
	}
	if e.State() != raftengine.StateLeader {
		req.done <- adminResult{err: errors.WithStack(errNotLeader)}
		return
	}
	currentIndex := e.currentConfigIndex()
	if req.prevIndex != 0 && req.prevIndex != currentIndex {
		req.done <- adminResult{err: errors.Wrapf(errConfigIndexMismatch, "got %d want %d", req.prevIndex, currentIndex)}
		return
	}

	switch req.action {
	case adminActionAddVoter:
		e.handleAddVoter(req)
	case adminActionRemoveServer:
		e.handleRemoveServer(req)
	case adminActionTransferLeadership:
		e.handleTransferLeadership(req)
	default:
		req.done <- adminResult{err: errors.New("unknown admin action")}
	}
}

func (e *Engine) handleAddVoter(req adminRequest) {
	contextBytes, err := encodeConfChangeContext(req.id, req.peer)
	if err != nil {
		req.done <- adminResult{err: err}
		return
	}
	cc := raftpb.ConfChange{
		Type:    raftpb.ConfChangeAddNode,
		NodeID:  req.peer.NodeID,
		Context: contextBytes,
	}
	if err := e.storePendingConfig(req); err != nil {
		req.done <- adminResult{err: err}
		return
	}
	if err := e.rawNode.ProposeConfChange(cc); err != nil {
		e.cancelPendingConfig(req.id)
		req.done <- adminResult{err: errors.WithStack(err)}
	}
}

func (e *Engine) handleRemoveServer(req adminRequest) {
	peer, ok := e.peerForID(req.peer.ID)
	if !ok {
		req.done <- adminResult{err: errors.Wrapf(errTransportPeerUnknown, "id=%q", req.peer.ID)}
		return
	}
	contextBytes, err := encodeConfChangeContext(req.id, peer)
	if err != nil {
		req.done <- adminResult{err: err}
		return
	}
	cc := raftpb.ConfChange{
		Type:    raftpb.ConfChangeRemoveNode,
		NodeID:  peer.NodeID,
		Context: contextBytes,
	}
	if err := e.storePendingConfig(req); err != nil {
		req.done <- adminResult{err: err}
		return
	}
	if err := e.rawNode.ProposeConfChange(cc); err != nil {
		e.cancelPendingConfig(req.id)
		req.done <- adminResult{err: errors.WithStack(err)}
	}
}

func (e *Engine) handleTransferLeadership(req adminRequest) {
	target, err := e.resolveTransferTarget(req.peer)
	if err != nil {
		req.done <- adminResult{err: err}
		return
	}
	e.rawNode.TransferLeader(target.NodeID)
	req.done <- adminResult{peer: target}
}

func (e *Engine) drainReady() error {
	for e.rawNode.HasReady() {
		rd := e.rawNode.Ready()
		if err := e.persistReady(rd); err != nil {
			return err
		}
		if err := e.sendMessages(rd.Messages); err != nil {
			return err
		}
		if err := e.applyCommitted(rd.CommittedEntries); err != nil {
			return err
		}
		e.handleReadStates(rd.ReadStates)
		e.rawNode.Advance(rd)
		if err := e.maybePersistLocalSnapshot(); err != nil {
			return err
		}
		e.refreshStatus()
		e.resolveReadyReads()
	}
	e.refreshStatus()
	return nil
}

func (e *Engine) persistReady(rd etcdraft.Ready) error {
	if !readyNeedsPersistence(rd) {
		return nil
	}
	if err := e.applyReady(rd); err != nil {
		return err
	}
	if e.persist == nil {
		return nil
	}
	return persistReadyToWAL(e.persist, rd)
}

func (e *Engine) applyReady(rd etcdraft.Ready) error {
	if err := e.applyReadySnapshot(rd.Snapshot); err != nil {
		return err
	}
	if err := e.applyReadyEntries(rd.Entries); err != nil {
		return err
	}
	return e.applyReadyHardState(rd.HardState)
}

func (e *Engine) handleStep(msg raftpb.Message) {
	if e.rawNode == nil {
		return
	}
	e.recordLeaderContact(msg)
	if err := e.rawNode.Step(msg); err != nil {
		if errors.Is(err, etcdraft.ErrStepPeerNotFound) {
			return
		}
		e.fail(errors.WithStack(err))
	}
}

func (e *Engine) sendMessages(messages []raftpb.Message) error {
	for _, msg := range messages {
		if e.skipDispatchMessage(msg) {
			continue
		}
		if err := e.enqueueDispatchMessage(msg); err != nil {
			return err
		}
	}
	return nil
}

func (e *Engine) skipDispatchMessage(msg raftpb.Message) bool {
	if msg.To == 0 || msg.To == e.nodeID || etcdraft.IsLocalMsg(msg.Type) {
		return true
	}
	return e.transport == nil || e.dispatchCh == nil
}

func (e *Engine) enqueueDispatchMessage(msg raftpb.Message) error {
	if len(e.dispatchCh) >= cap(e.dispatchCh) {
		e.recordDroppedDispatch(msg)
		return nil
	}
	dispatchReq := prepareDispatchRequest(msg)
	select {
	case e.dispatchCh <- dispatchReq:
		return nil
	default:
		_ = dispatchReq.Close()
		e.recordDroppedDispatch(msg)
		return nil
	}
}

func (e *Engine) applyReadySnapshot(snapshot raftpb.Snapshot) error {
	if etcdraft.IsEmptySnap(snapshot) {
		return nil
	}
	// etcdraft.IsEmptySnap only validates the raft metadata. This backend also
	// requires FSM payload bytes so it can restore local state before applying
	// the metadata snapshot to MemoryStorage.
	if len(snapshot.Data) == 0 {
		return errors.WithStack(errSnapshotRequired)
	}
	// Snapshot application is intentionally synchronous with the raft loop: the
	// local FSM must reflect the incoming raft snapshot before Ready can advance
	// and later committed entries can be applied safely.
	e.snapshotMu.Lock()
	defer e.snapshotMu.Unlock()
	if err := e.fsm.Restore(bytes.NewReader(snapshot.Data)); err != nil {
		return errors.WithStack(err)
	}
	if err := e.storage.ApplySnapshot(snapshot); err != nil {
		return errors.WithStack(err)
	}
	e.applied = snapshot.Metadata.Index
	e.setConfigurationFromConfState(snapshot.Metadata.ConfState, snapshot.Metadata.Index)
	return nil
}

func (e *Engine) applyReadyEntries(entries []raftpb.Entry) error {
	if len(entries) == 0 {
		return nil
	}
	return errors.WithStack(e.storage.Append(entries))
}

func (e *Engine) applyReadyHardState(hardState raftpb.HardState) error {
	if etcdraft.IsEmptyHardState(hardState) {
		return nil
	}
	return errors.WithStack(e.storage.SetHardState(hardState))
}

func (e *Engine) maybePersistLocalSnapshot() error {
	if e.applied == 0 || e.snapshotInFlight {
		return nil
	}
	if e.persist == nil || e.snapshotReqCh == nil {
		// Snapshot persistence is optional in this prototype; engines without a
		// disk-backed etcd storage simply skip local snapshot publication.
		return nil
	}

	current, err := e.storage.Snapshot()
	if err != nil {
		return errors.WithStack(err)
	}
	if e.applied <= current.Metadata.Index || e.applied-current.Metadata.Index < defaultSnapshotEvery {
		return nil
	}
	snapshot, err := e.fsm.Snapshot()
	if err != nil {
		return errors.WithStack(err)
	}

	req := snapshotRequest{
		index:    e.applied,
		snapshot: snapshot,
	}
	return e.enqueueSnapshotRequest(req)
}

func (e *Engine) enqueueSnapshotRequest(req snapshotRequest) error {
	select {
	case <-e.doneCh:
		_ = req.snapshot.Close()
		return e.currentErrorOrClosed()
	case <-e.snapshotStopCh:
		_ = req.snapshot.Close()
		return e.currentErrorOrClosed()
	case e.snapshotReqCh <- req:
		e.snapshotInFlight = true
		return nil
	}
}

func (e *Engine) applyCommitted(entries []raftpb.Entry) error {
	for _, entry := range entries {
		switch entry.Type {
		case raftpb.EntryNormal:
			response := e.applyNormalEntry(entry)
			e.applied = entry.Index
			e.resolveProposal(entry.Index, entry.Data, response)
		case raftpb.EntryConfChange:
			var cc raftpb.ConfChange
			if err := cc.Unmarshal(entry.Data); err != nil {
				return errors.WithStack(err)
			}
			confState := e.rawNode.ApplyConfChange(cc)
			if err := e.persistConfigSnapshot(entry.Index, *confState); err != nil {
				return err
			}
			e.applyConfigChange(cc.Type, cc.NodeID, cc.Context, entry.Index)
			e.applied = entry.Index
		case raftpb.EntryConfChangeV2:
			var cc raftpb.ConfChangeV2
			if err := cc.Unmarshal(entry.Data); err != nil {
				return errors.WithStack(err)
			}
			confState := e.rawNode.ApplyConfChange(cc)
			if err := e.persistConfigSnapshot(entry.Index, *confState); err != nil {
				return err
			}
			e.applyConfigChangeV2(cc, entry.Index)
			e.applied = entry.Index
		default:
			e.applied = entry.Index
		}
	}
	return nil
}

func (e *Engine) applyNormalEntry(entry raftpb.Entry) any {
	if len(entry.Data) == 0 {
		return nil
	}
	_, payload, ok := decodeProposalEnvelope(entry.Data)
	if !ok {
		return nil
	}
	return e.fsm.Apply(payload)
}

func (e *Engine) resolveProposal(commitIndex uint64, data []byte, response any) {
	id, _, ok := decodeProposalEnvelope(data)
	if !ok {
		return
	}
	req, ok := e.popPendingProposal(id)
	if !ok {
		return
	}
	req.done <- proposalResult{
		result: &raftengine.ProposalResult{
			CommitIndex: commitIndex,
			Response:    response,
		},
	}
}

func (e *Engine) applyConfigChange(changeType raftpb.ConfChangeType, nodeID uint64, context []byte, index uint64) {
	e.applyConfigPeerChange(changeType, nodeID, context)
	e.setConfigIndex(index)
	e.resolveConfigChange(index, context)
}

func (e *Engine) applyConfigChangeV2(cc raftpb.ConfChangeV2, index uint64) {
	for _, change := range cc.Changes {
		e.applyConfigPeerChange(change.Type, change.NodeID, cc.Context)
	}
	e.setConfigIndex(index)
	e.resolveConfigChange(index, cc.Context)
}

func (e *Engine) applyConfigPeerChange(changeType raftpb.ConfChangeType, nodeID uint64, context []byte) {
	_, peer, ok := decodeConfChangeContext(context)
	switch changeType {
	case raftpb.ConfChangeAddNode:
		e.applyAddedPeer(nodeID, peer, ok)
	case raftpb.ConfChangeRemoveNode:
		e.applyRemovedPeer(nodeID, peer, ok)
	case raftpb.ConfChangeUpdateNode:
		e.applyUpdatedPeer(peer, ok)
	case raftpb.ConfChangeAddLearnerNode:
		// Phase 3 only exposes voter membership changes. Ignore learner metadata
		// if it appears in a replayed log; startup validation still rejects joint
		// consensus snapshots and learner-only cluster states for now.
	}
}

func (e *Engine) applyAddedPeer(nodeID uint64, peer Peer, ok bool) {
	if ok {
		e.upsertPeer(peer)
		return
	}
	if nodeID == 0 {
		return
	}
	e.upsertPeer(Peer{
		NodeID:  nodeID,
		ID:      strconv.FormatUint(nodeID, 10),
		Address: "",
	})
}

func (e *Engine) applyRemovedPeer(nodeID uint64, peer Peer, ok bool) {
	if ok && peer.NodeID != 0 {
		e.removePeer(peer.NodeID)
		return
	}
	if nodeID != 0 {
		e.removePeer(nodeID)
	}
}

func (e *Engine) applyUpdatedPeer(peer Peer, ok bool) {
	if ok {
		e.upsertPeer(peer)
	}
}

func (e *Engine) persistConfigSnapshot(index uint64, confState raftpb.ConfState) error {
	if upToDate, err := e.configSnapshotUpToDate(index, confState); err != nil {
		return err
	} else if upToDate {
		return nil
	}

	payload, err := e.snapshotPayload()
	if err != nil {
		return err
	}

	// Keep disk publication serialized with snapshot restores. If a newer
	// follower snapshot restored concurrently while an older config snapshot was
	// still being written to disk, a restart could regress to the stale payload.
	e.snapshotMu.Lock()
	defer e.snapshotMu.Unlock()

	if upToDate, err := e.configSnapshotUpToDate(index, confState); err != nil {
		return err
	} else if upToDate {
		return nil
	}

	snap, err := e.createConfigSnapshot(index, confState, payload)
	if err != nil {
		return err
	}
	if err := e.persistCreatedSnapshot(snap); err != nil {
		return err
	}
	return nil
}

func (e *Engine) snapshotPayload() ([]byte, error) {
	snapshot, err := e.fsm.Snapshot()
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return snapshotBytesAndClose(snapshot, e.dataDir)
}

func (e *Engine) configSnapshotUpToDate(index uint64, confState raftpb.ConfState) (bool, error) {
	current, err := e.storage.Snapshot()
	if err != nil {
		return false, errors.WithStack(err)
	}
	if current.Metadata.Index > index {
		return true, nil
	}
	if current.Metadata.Index < index {
		return false, nil
	}
	return equalConfState(current.Metadata.ConfState, confState), nil
}

func (e *Engine) createConfigSnapshot(index uint64, confState raftpb.ConfState, payload []byte) (raftpb.Snapshot, error) {
	snap, err := e.storage.CreateSnapshot(index, &confState, payload)
	if err == nil {
		return snap, nil
	}
	switch {
	case errors.Is(err, etcdraft.ErrSnapOutOfDate):
		return raftpb.Snapshot{}, nil
	case errors.Is(err, etcdraft.ErrCompacted):
		return raftpb.Snapshot{}, nil
	default:
		return raftpb.Snapshot{}, errors.WithStack(err)
	}
}

func (e *Engine) persistCreatedSnapshot(snap raftpb.Snapshot) error {
	if etcdraft.IsEmptySnap(snap) || e.persist == nil {
		return nil
	}
	if err := e.persist.SaveSnap(snap); err != nil {
		return errors.WithStack(err)
	}
	if err := e.persist.Release(snap); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func (e *Engine) handleReadStates(states []etcdraft.ReadState) {
	for _, state := range states {
		id, ok := decodeReadContext(state.RequestCtx)
		if !ok {
			continue
		}
		req, ok := e.pendingRead(id)
		if !ok {
			continue
		}
		req.target = state.Index
		if !req.waitApplied {
			e.popPendingRead(id)
			req.done <- readResult{index: state.Index}
			continue
		}
		e.storePendingRead(req)
	}
}

func (e *Engine) resolveReadyReads() {
	for _, req := range e.readyReads(e.applied) {
		req.done <- readResult{index: req.target}
	}
}

func (e *Engine) refreshStatus() {
	previous := e.Status().State
	basic := e.rawNode.BasicStatus()
	lastLogIndex, _ := e.storage.LastIndex()
	snapshot, _ := e.storage.Snapshot()
	config := e.currentConfiguration()

	state := translateState(basic.RaftState)
	leader := e.leaderInfo(basic.Lead)

	status := raftengine.Status{
		State:             state,
		Leader:            leader,
		Term:              basic.Term,
		CommitIndex:       basic.Commit,
		AppliedIndex:      e.applied,
		LastLogIndex:      lastLogIndex,
		LastSnapshotIndex: snapshot.Metadata.Index,
		FSMPending:        pendingEntries(basic.Commit, e.applied),
		NumPeers:          numRemoteServers(config.Servers, e.localID),
		LastContact:       lastContactFor(state, basic.Lead, e.lastLeaderContactFrom, e.lastLeaderContactAt),
	}

	e.mu.Lock()
	e.status = status
	if e.closed {
		e.status.State = raftengine.StateShutdown
	}
	e.mu.Unlock()

	if status.State == raftengine.StateLeader {
		e.leaderOnce.Do(func() { close(e.leaderReady) })
	}
	if previous == raftengine.StateLeader && status.State != raftengine.StateLeader {
		e.failPending(errors.WithStack(errNotLeader))
	}
}

func (e *Engine) markStarted() {
	e.startOnce.Do(func() { close(e.startedCh) })
}

func (e *Engine) openReady(waitForLeader bool) <-chan struct{} {
	if waitForLeader {
		return e.leaderReady
	}
	return e.startedCh
}

func (e *Engine) shutdown() {
	e.mu.Lock()
	e.closed = true
	e.status.State = raftengine.StateShutdown
	e.mu.Unlock()
	e.stopDispatchWorkers()
	e.stopSnapshotWorker()
	_ = closePersist(e.persist)
	_ = e.transport.Close()
	e.failPending(errors.WithStack(errClosed))
}

func (e *Engine) fail(err error) {
	e.mu.Lock()
	if err != nil {
		e.runErr = err
	}
	e.closed = true
	e.status.State = raftengine.StateShutdown
	e.mu.Unlock()
	e.stopDispatchWorkers()
	e.stopSnapshotWorker()
	_ = closePersist(e.persist)
	_ = e.transport.Close()
	e.failPending(e.currentErrorOrClosed())
}

func (e *Engine) failPending(err error) {
	proposals, reads, configs := e.drainPending()
	for _, req := range proposals {
		req.done <- proposalResult{err: err}
	}
	for _, req := range reads {
		req.done <- readResult{err: err}
	}
	for _, req := range configs {
		req.done <- adminResult{err: err}
	}
}

func (e *Engine) storePendingProposal(req proposalRequest) {
	e.pending.Lock()
	defer e.pending.Unlock()
	e.pendingProposals[req.id] = req
}

func (e *Engine) popPendingProposal(id uint64) (proposalRequest, bool) {
	e.pending.Lock()
	defer e.pending.Unlock()
	req, ok := e.pendingProposals[id]
	if ok {
		delete(e.pendingProposals, id)
	}
	return req, ok
}

func (e *Engine) cancelPendingProposal(id uint64) {
	if id == 0 {
		return
	}
	e.pending.Lock()
	delete(e.pendingProposals, id)
	e.pending.Unlock()
}

func (e *Engine) storePendingRead(req readRequest) {
	e.pending.Lock()
	defer e.pending.Unlock()
	e.pendingReads[req.id] = req
}

func (e *Engine) pendingRead(id uint64) (readRequest, bool) {
	e.pending.Lock()
	defer e.pending.Unlock()
	req, ok := e.pendingReads[id]
	return req, ok
}

func (e *Engine) popPendingRead(id uint64) (readRequest, bool) {
	e.pending.Lock()
	defer e.pending.Unlock()
	req, ok := e.pendingReads[id]
	if ok {
		delete(e.pendingReads, id)
	}
	return req, ok
}

func (e *Engine) cancelPendingRead(id uint64) {
	if id == 0 {
		return
	}
	e.pending.Lock()
	delete(e.pendingReads, id)
	e.pending.Unlock()
}

func (e *Engine) storePendingConfig(req adminRequest) error {
	e.pending.Lock()
	defer e.pending.Unlock()
	if len(e.pendingConfigs) >= defaultMaxPendingConfigs {
		return errors.WithStack(errTooManyPendingConfigs)
	}
	e.pendingConfigs[req.id] = req
	return nil
}

func (e *Engine) popPendingConfig(id uint64) (adminRequest, bool) {
	e.pending.Lock()
	defer e.pending.Unlock()
	req, ok := e.pendingConfigs[id]
	if ok {
		delete(e.pendingConfigs, id)
	}
	return req, ok
}

func (e *Engine) cancelPendingConfig(id uint64) {
	if id == 0 {
		return
	}
	e.pending.Lock()
	delete(e.pendingConfigs, id)
	e.pending.Unlock()
}

func (e *Engine) readyReads(applied uint64) []readRequest {
	e.pending.Lock()
	defer e.pending.Unlock()

	ready := make([]readRequest, 0)
	for id, req := range e.pendingReads {
		if req.target == 0 || req.target > applied {
			continue
		}
		delete(e.pendingReads, id)
		ready = append(ready, req)
	}
	return ready
}

func (e *Engine) drainPending() ([]proposalRequest, []readRequest, []adminRequest) {
	e.pending.Lock()
	defer e.pending.Unlock()

	proposals := make([]proposalRequest, 0, len(e.pendingProposals))
	for id, req := range e.pendingProposals {
		delete(e.pendingProposals, id)
		proposals = append(proposals, req)
	}
	reads := make([]readRequest, 0, len(e.pendingReads))
	for id, req := range e.pendingReads {
		delete(e.pendingReads, id)
		reads = append(reads, req)
	}
	configs := make([]adminRequest, 0, len(e.pendingConfigs))
	for id, req := range e.pendingConfigs {
		delete(e.pendingConfigs, id)
		configs = append(configs, req)
	}
	return proposals, reads, configs
}

func (e *Engine) currentError() error {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.runErr
}

func (e *Engine) currentErrorOrClosed() error {
	e.mu.RLock()
	defer e.mu.RUnlock()
	if e.runErr != nil {
		return e.runErr
	}
	return errors.WithStack(errClosed)
}

func (e *Engine) currentConfiguration() raftengine.Configuration {
	e.mu.RLock()
	defer e.mu.RUnlock()
	cfg := raftengine.Configuration{
		Servers: append([]raftengine.Server(nil), e.config.Servers...),
	}
	return cfg
}

func (e *Engine) currentConfigIndex() uint64 {
	return e.configIndex.Load()
}

func (e *Engine) shouldCampaignSingleNode() bool {
	cfg := e.currentConfiguration()
	return len(cfg.Servers) == 1 && cfg.Servers[0].ID == e.localID
}

func (e *Engine) resolveAdminPeer(id string, address string) (Peer, error) {
	if e.transport != nil {
		if peer, ok := e.transport.peerByIdentity(id, address); ok {
			return peer, nil
		}
	}
	e.mu.RLock()
	for _, peer := range e.peers {
		if id != "" && peer.ID != id {
			continue
		}
		if address != "" && peer.Address != address {
			continue
		}
		e.mu.RUnlock()
		return peer, nil
	}
	e.mu.RUnlock()
	return normalizePeer(Peer{ID: id, Address: address})
}

func (e *Engine) setConfigIndex(index uint64) {
	for {
		current := e.configIndex.Load()
		if index <= current {
			return
		}
		if e.configIndex.CompareAndSwap(current, index) {
			return
		}
	}
}

func (e *Engine) setConfigurationFromConfState(conf raftpb.ConfState, index uint64) {
	e.mu.Lock()
	e.config = configurationFromConfState(e.peers, conf)
	e.mu.Unlock()
	e.setConfigIndex(index)
}

func (e *Engine) nextID() uint64 {
	return e.nextRequestID.Add(1)
}

func normalizeConfig(cfg OpenConfig) OpenConfig {
	cfg = normalizeIdentity(cfg)
	cfg = normalizePeersConfig(cfg)
	cfg = normalizeTimingConfig(cfg)
	cfg = normalizeLimitConfig(cfg)
	return cfg
}

func normalizeIdentity(cfg OpenConfig) OpenConfig {
	if cfg.NodeID == 0 && cfg.LocalID != "" {
		cfg.NodeID = DeriveNodeID(cfg.LocalID)
	}
	if cfg.LocalID == "" && cfg.NodeID != 0 {
		cfg.LocalID = strconv.FormatUint(cfg.NodeID, 10)
	}
	return cfg
}

func normalizePeersConfig(cfg OpenConfig) OpenConfig {
	if len(cfg.Peers) == 0 && cfg.LocalAddress != "" && cfg.LocalID != "" {
		cfg.Peers = []Peer{{
			NodeID:  cfg.NodeID,
			ID:      cfg.LocalID,
			Address: cfg.LocalAddress,
		}}
	}
	return cfg
}

func normalizeTimingConfig(cfg OpenConfig) OpenConfig {
	if cfg.TickInterval <= 0 {
		cfg.TickInterval = defaultTickInterval
	}
	if cfg.HeartbeatTick <= 0 {
		cfg.HeartbeatTick = defaultHeartbeatTick
	}
	if cfg.ElectionTick <= 0 {
		cfg.ElectionTick = defaultElectionTick
	}
	return cfg
}

func normalizeLimitConfig(cfg OpenConfig) OpenConfig {
	if cfg.MaxInflightMsg <= 0 {
		cfg.MaxInflightMsg = defaultMaxInflightMsg
	}
	if cfg.MaxSizePerMsg == 0 {
		cfg.MaxSizePerMsg = defaultMaxSizePerMsg
	}
	return cfg
}

func validateConfig(cfg OpenConfig) error {
	switch {
	case cfg.NodeID == 0:
		return errors.WithStack(errNodeIDRequired)
	case cfg.DataDir == "":
		return errors.WithStack(errDataDirRequired)
	case cfg.StateMachine == nil:
		return errors.WithStack(errStateMachineUnset)
	case cfg.ElectionTick <= cfg.HeartbeatTick:
		return errors.New("election tick must be greater than heartbeat tick")
	default:
		return nil
	}
}

func translateState(state etcdraft.StateType) raftengine.State {
	switch state {
	case etcdraft.StateFollower:
		return raftengine.StateFollower
	case etcdraft.StatePreCandidate:
		return raftengine.StateCandidate
	case etcdraft.StateCandidate:
		return raftengine.StateCandidate
	case etcdraft.StateLeader:
		return raftengine.StateLeader
	default:
		return raftengine.StateUnknown
	}
}

func pendingEntries(commitIndex uint64, appliedIndex uint64) uint64 {
	if commitIndex <= appliedIndex {
		return 0
	}
	return commitIndex - appliedIndex
}

func configurationFromConfState(peers map[uint64]Peer, conf raftpb.ConfState) raftengine.Configuration {
	if len(conf.Voters) == 0 {
		return raftengine.Configuration{}
	}

	servers := make([]raftengine.Server, 0, len(conf.Voters))
	for _, nodeID := range conf.Voters {
		peer, ok := peers[nodeID]
		if ok {
			servers = append(servers, raftengine.Server{
				ID:       peer.ID,
				Address:  peer.Address,
				Suffrage: "voter",
			})
			continue
		}
		servers = append(servers, raftengine.Server{
			ID:       strconv.FormatUint(nodeID, 10),
			Address:  "",
			Suffrage: "voter",
		})
	}
	return raftengine.Configuration{Servers: servers}
}

func numRemoteServers(servers []raftengine.Server, localID string) uint64 {
	var count uint64
	for _, server := range servers {
		if server.ID == localID {
			continue
		}
		count++
	}
	return count
}

func lastContactFor(state raftengine.State, leaderNodeID uint64, lastFrom uint64, lastAt time.Time) time.Duration {
	if state == raftengine.StateLeader {
		return 0
	}
	if state != raftengine.StateFollower {
		return unknownLastContact
	}
	if lastAt.IsZero() {
		return unknownLastContact
	}
	if leaderNodeID != 0 && lastFrom != leaderNodeID {
		return unknownLastContact
	}
	return time.Since(lastAt)
}

func contextErr(ctx context.Context) error {
	if ctx == nil {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func readyNeedsPersistence(rd etcdraft.Ready) bool {
	return !etcdraft.IsEmptySnap(rd.Snapshot) || len(rd.Entries) > 0 || !etcdraft.IsEmptyHardState(rd.HardState)
}

func maxAppliedIndex(snapshot raftpb.Snapshot) uint64 {
	return snapshot.Metadata.Index
}

func (e *Engine) enqueueStep(ctx context.Context, msg raftpb.Message) error {
	select {
	case <-ctx.Done():
		return errors.WithStack(ctx.Err())
	case <-e.doneCh:
		return e.currentErrorOrClosed()
	case e.stepCh <- msg:
		return nil
	default:
		return errors.WithStack(errStepQueueFull)
	}
}

func (e *Engine) handleTransportMessage(ctx context.Context, msg raftpb.Message) error {
	select {
	case <-ctx.Done():
		return errors.WithStack(ctx.Err())
	case <-e.doneCh:
		return e.currentErrorOrClosed()
	case <-e.startedCh:
		return e.enqueueStep(ctx, msg)
	}
}

func (e *Engine) recordLeaderContact(msg raftpb.Message) {
	if !isLeaderContactMessage(msg.Type) || msg.From == 0 {
		return
	}
	e.lastLeaderContactFrom = msg.From
	e.lastLeaderContactAt = time.Now()
}

func isLeaderContactMessage(msgType raftpb.MessageType) bool {
	return msgType == raftpb.MsgApp ||
		msgType == raftpb.MsgHeartbeat ||
		msgType == raftpb.MsgSnap ||
		msgType == raftpb.MsgReadIndexResp
}

func (e *Engine) startDispatchWorkers() {
	if e.dispatchCh == nil {
		return
	}
	for range defaultDispatchWorkers {
		e.dispatchWG.Add(1)
		go e.runDispatchWorker()
	}
}

func (e *Engine) runDispatchWorker() {
	defer e.dispatchWG.Done()
	for {
		select {
		case <-e.dispatchStopCh:
			return
		case req := <-e.dispatchCh:
			if err := e.dispatchTransport(req); err != nil {
				count := e.dispatchErrorCount.Add(1)
				if shouldLogDispatchEvent(count) {
					slog.Warn("etcd raft outbound dispatch failed",
						"node_id", e.nodeID,
						"to", req.msg.To,
						"type", req.msg.Type.String(),
						"dispatch_error_count", count,
						"err", err,
					)
				}
				_ = req.Close()
				continue
			}
			_ = req.Close()
		}
	}
}

func (e *Engine) stopDispatchWorkers() {
	e.dispatchOnce.Do(func() {
		if e.dispatchCancel != nil {
			e.dispatchCancel()
		}
		if e.dispatchStopCh != nil {
			close(e.dispatchStopCh)
		}
		e.dispatchWG.Wait()
	})
}

func (e *Engine) startSnapshotWorker() {
	if e.snapshotReqCh == nil {
		return
	}
	e.snapshotWG.Add(1)
	go e.runSnapshotWorker()
}

func (e *Engine) runSnapshotWorker() {
	defer e.snapshotWG.Done()
	for {
		select {
		case <-e.snapshotStopCh:
			return
		case req := <-e.snapshotReqCh:
			result := snapshotResult{
				index: req.index,
				err:   e.persistSnapshot(req),
			}
			select {
			case <-e.snapshotStopCh:
				return
			case e.snapshotResCh <- result:
			}
		}
	}
}

func (e *Engine) stopSnapshotWorker() {
	e.snapshotOnce.Do(func() {
		if e.snapshotStopCh != nil {
			close(e.snapshotStopCh)
		}
		e.snapshotWG.Wait()
	})
}

func (e *Engine) leaderInfo(leaderNodeID uint64) raftengine.LeaderInfo {
	if leaderNodeID == 0 {
		return raftengine.LeaderInfo{}
	}
	e.mu.RLock()
	peer, ok := e.peers[leaderNodeID]
	e.mu.RUnlock()
	if ok {
		return raftengine.LeaderInfo{ID: peer.ID, Address: peer.Address}
	}
	return raftengine.LeaderInfo{ID: strconv.FormatUint(leaderNodeID, 10)}
}

func (e *Engine) resolveConfigChange(index uint64, context []byte) {
	id, peer, ok := decodeConfChangeContext(context)
	if !ok {
		return
	}
	req, ok := e.popPendingConfig(id)
	if !ok {
		return
	}
	req.done <- adminResult{
		index: index,
		peer:  peer,
	}
}

func (e *Engine) peerForID(id string) (Peer, bool) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	for _, peer := range e.peers {
		if peer.ID == id {
			return peer, true
		}
	}
	if id == e.localID {
		return Peer{
			NodeID:  e.nodeID,
			ID:      e.localID,
			Address: e.localAddress,
		}, true
	}
	for _, server := range e.config.Servers {
		if server.ID != id {
			continue
		}
		return Peer{
			NodeID:  DeriveNodeID(server.ID),
			ID:      server.ID,
			Address: server.Address,
		}, true
	}
	return Peer{}, false
}

func (e *Engine) resolveTransferTarget(target Peer) (Peer, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	if len(e.config.Servers) == 0 {
		return Peer{}, errors.WithStack(errLeadershipTransferNotReady)
	}
	if target.ID == "" && target.Address == "" {
		return e.defaultTransferTargetLocked()
	}
	return e.namedTransferTargetLocked(target)
}

func (e *Engine) defaultTransferTargetLocked() (Peer, error) {
	for _, server := range e.config.Servers {
		if server.ID == e.localID {
			continue
		}
		return peerFromServer(e.peers, server), nil
	}
	return Peer{}, errors.WithStack(errLeadershipTransferNotReady)
}

func (e *Engine) namedTransferTargetLocked(target Peer) (Peer, error) {
	for _, server := range e.config.Servers {
		if target.ID != "" && server.ID != target.ID {
			continue
		}
		if target.Address != "" && server.Address != target.Address {
			continue
		}
		if server.ID == e.localID {
			return Peer{}, errors.WithStack(errLeadershipTransferTarget)
		}
		return peerFromServer(e.peers, server), nil
	}
	if target.ID != "" {
		return Peer{}, errors.Wrapf(errTransportPeerUnknown, "id=%q", target.ID)
	}
	return Peer{}, errors.Wrapf(errTransportPeerUnknown, "address=%q", target.Address)
}

func (e *Engine) upsertPeer(peer Peer) {
	if peer.NodeID == 0 {
		peer.NodeID = DeriveNodeID(peer.ID)
	}
	if peer.ID == "" {
		peer.ID = strconv.FormatUint(peer.NodeID, 10)
	}

	e.mu.Lock()
	if e.peers == nil {
		e.peers = make(map[uint64]Peer)
	}
	e.peers[peer.NodeID] = peer
	e.config.Servers = upsertConfigServer(e.peers, e.config.Servers, raftengine.Server{
		ID:       peer.ID,
		Address:  peer.Address,
		Suffrage: "voter",
	})
	e.mu.Unlock()

	if e.transport != nil {
		e.transport.UpsertPeer(peer)
	}
}

func (e *Engine) removePeer(nodeID uint64) {
	if nodeID == 0 {
		return
	}

	e.mu.Lock()
	peer, ok := e.peers[nodeID]
	if ok {
		delete(e.peers, nodeID)
	}
	e.config.Servers = removeConfigServer(e.peers, e.config.Servers, nodeID, peer.ID)
	e.mu.Unlock()

	if e.transport != nil {
		e.transport.RemovePeer(nodeID)
	}
}

func encodeProposalEnvelope(id uint64, payload []byte) []byte {
	out := make([]byte, envelopeHeaderSize+len(payload))
	out[0] = proposalEnvelopeVersion
	binary.BigEndian.PutUint64(out[1:envelopeHeaderSize], id)
	copy(out[envelopeHeaderSize:], payload)
	return out
}

func decodeProposalEnvelope(data []byte) (uint64, []byte, bool) {
	if len(data) < envelopeHeaderSize || data[0] != proposalEnvelopeVersion {
		return 0, nil, false
	}
	return binary.BigEndian.Uint64(data[1:envelopeHeaderSize]), data[envelopeHeaderSize:], true
}

func dispatchQueueSize(maxInflight int) int {
	size := maxInflight
	if size <= 0 {
		size = defaultMaxInflightMsg
	}
	return size * defaultDispatchWorkers
}

func shouldLogDispatchEvent(count uint64) bool {
	return count == 1 || count%128 == 0
}

func (e *Engine) recordDroppedDispatch(msg raftpb.Message) {
	count := e.dispatchDropCount.Add(1)
	if shouldLogDispatchEvent(count) {
		slog.Warn("dropping etcd raft outbound message",
			"node_id", e.nodeID,
			"to", msg.To,
			"type", msg.Type.String(),
			"drop_count", count,
		)
	}
}

func (e *Engine) dispatchTransport(req dispatchRequest) error {
	ctx := e.dispatchCtx
	if ctx == nil {
		ctx = context.Background()
	}
	if e.dispatchFn != nil {
		return e.dispatchFn(ctx, req)
	}
	return e.transport.Dispatch(ctx, req.msg)
}

func prepareDispatchRequest(msg raftpb.Message) dispatchRequest {
	return dispatchRequest{msg: cloneDispatchMessage(msg)}
}

func (r dispatchRequest) Close() error {
	return nil
}

func cloneDispatchMessage(msg raftpb.Message) raftpb.Message {
	cloned := msg
	cloned.Entries = cloneDispatchEntries(msg.Entries)
	cloned.Snapshot = cloneDispatchSnapshot(msg.Snapshot)
	cloned.Context = append([]byte(nil), msg.Context...)
	cloned.Responses = cloneDispatchMessages(msg.Responses)
	return cloned
}

func cloneDispatchMessages(messages []raftpb.Message) []raftpb.Message {
	if len(messages) == 0 {
		return nil
	}
	cloned := make([]raftpb.Message, len(messages))
	for i, msg := range messages {
		cloned[i] = cloneDispatchMessage(msg)
	}
	return cloned
}

func cloneDispatchEntries(entries []raftpb.Entry) []raftpb.Entry {
	if len(entries) == 0 {
		return nil
	}
	cloned := make([]raftpb.Entry, len(entries))
	for i, entry := range entries {
		cloned[i] = entry
		cloned[i].Data = append([]byte(nil), entry.Data...)
	}
	return cloned
}

func cloneDispatchSnapshot(snapshot *raftpb.Snapshot) *raftpb.Snapshot {
	if snapshot == nil {
		return nil
	}
	cloned := *snapshot
	cloned.Data = append([]byte(nil), snapshot.Data...)
	cloned.Metadata.ConfState = cloneDispatchConfState(snapshot.Metadata.ConfState)
	return &cloned
}

func cloneDispatchConfState(conf raftpb.ConfState) raftpb.ConfState {
	conf.Voters = append([]uint64(nil), conf.Voters...)
	conf.Learners = append([]uint64(nil), conf.Learners...)
	conf.VotersOutgoing = append([]uint64(nil), conf.VotersOutgoing...)
	conf.LearnersNext = append([]uint64(nil), conf.LearnersNext...)
	return conf
}

func (e *Engine) handleSnapshotResult(result snapshotResult) error {
	e.snapshotInFlight = false
	if result.err != nil {
		return result.err
	}
	return e.maybePersistLocalSnapshot()
}

func (e *Engine) persistSnapshot(req snapshotRequest) error {
	payload, err := snapshotBytesAndClose(req.snapshot, e.dataDir)
	if err != nil {
		return err
	}

	// Keep restore and local snapshot publication serialized end-to-end so a
	// follower snapshot cannot swap in newer FSM state while an older local
	// snapshot is still being published to raft storage and disk.
	e.snapshotMu.Lock()
	defer e.snapshotMu.Unlock()

	current, err := e.storage.Snapshot()
	if err != nil {
		return errors.WithStack(err)
	}
	if req.index <= current.Metadata.Index {
		return nil
	}

	_, err = persistLocalSnapshotPayload(e.storage, e.persist, req.index, payload)
	if err != nil {
		switch {
		case errors.Is(err, etcdraft.ErrCompacted):
			return nil
		case errors.Is(err, etcdraft.ErrUnavailable):
			return nil
		case errors.Is(err, etcdraft.ErrSnapOutOfDate):
			return nil
		default:
			return err
		}
	}
	return nil
}

func encodeReadContext(id uint64) []byte {
	out := make([]byte, envelopeHeaderSize)
	out[0] = readContextVersion
	binary.BigEndian.PutUint64(out[1:envelopeHeaderSize], id)
	return out
}

func decodeReadContext(data []byte) (uint64, bool) {
	if len(data) != envelopeHeaderSize || data[0] != readContextVersion {
		return 0, false
	}
	return binary.BigEndian.Uint64(data[1:envelopeHeaderSize]), true
}

func encodeConfChangeContext(id uint64, peer Peer) ([]byte, error) {
	idLen, err := uint16Len(len(peer.ID))
	if err != nil {
		return nil, err
	}
	addressLen, err := uint16Len(len(peer.Address))
	if err != nil {
		return nil, err
	}

	out := make([]byte, confChangeFixedSize+len(peer.ID)+len(peer.Address))
	out[0] = confChangeContextVersion
	binary.BigEndian.PutUint64(out[1:9], id)
	binary.BigEndian.PutUint64(out[9:17], peer.NodeID)
	binary.BigEndian.PutUint16(out[17:19], idLen)
	binary.BigEndian.PutUint16(out[19:21], addressLen)
	offset := confChangeFixedSize
	copy(out[offset:offset+len(peer.ID)], peer.ID)
	offset += len(peer.ID)
	copy(out[offset:offset+len(peer.Address)], peer.Address)
	return out, nil
}

func decodeConfChangeContext(data []byte) (uint64, Peer, bool) {
	if len(data) < confChangeFixedSize || data[0] != confChangeContextVersion {
		return 0, Peer{}, false
	}
	idLen := int(binary.BigEndian.Uint16(data[17:19]))
	addressLen := int(binary.BigEndian.Uint16(data[19:21]))
	if len(data) != confChangeFixedSize+idLen+addressLen {
		return 0, Peer{}, false
	}
	offset := confChangeFixedSize
	peerID := string(data[offset : offset+idLen])
	offset += idLen
	address := string(data[offset : offset+addressLen])
	return binary.BigEndian.Uint64(data[1:9]), Peer{
		NodeID:  binary.BigEndian.Uint64(data[9:17]),
		ID:      peerID,
		Address: address,
	}, true
}

func uint16Len(n int) (uint16, error) {
	if n < 0 || n > 0xffff {
		return 0, errors.WithStack(errConfChangeContextTooLarge)
	}
	return uint16(n), nil
}

func peerFromServer(peers map[uint64]Peer, server raftengine.Server) Peer {
	if nodeID, ok := configServerNodeID(peers, server); ok {
		if peer, ok := peers[nodeID]; ok {
			return peer
		}
		return Peer{
			NodeID:  nodeID,
			ID:      server.ID,
			Address: server.Address,
		}
	}
	return Peer{
		NodeID:  DeriveNodeID(server.ID),
		ID:      server.ID,
		Address: server.Address,
	}
}

func upsertConfigServer(peers map[uint64]Peer, servers []raftengine.Server, server raftengine.Server) []raftengine.Server {
	out := append([]raftengine.Server(nil), servers...)
	for i := range out {
		if out[i].ID != server.ID {
			continue
		}
		out[i] = server
		sortConfigServers(peers, out)
		return out
	}
	out = append(out, server)
	sortConfigServers(peers, out)
	return out
}

func removeConfigServer(peers map[uint64]Peer, servers []raftengine.Server, nodeID uint64, serverID string) []raftengine.Server {
	if len(servers) == 0 {
		return nil
	}
	out := make([]raftengine.Server, 0, len(servers))
	for _, server := range servers {
		if serverID != "" && server.ID == serverID {
			continue
		}
		if nodeID != 0 && serverNodeID(peers, server) == nodeID {
			continue
		}
		out = append(out, server)
	}
	sortConfigServers(peers, out)
	return out
}

func sortConfigServers(peers map[uint64]Peer, servers []raftengine.Server) {
	sort.Slice(servers, func(i, j int) bool {
		left := serverNodeID(peers, servers[i])
		right := serverNodeID(peers, servers[j])
		if left == right {
			return servers[i].ID < servers[j].ID
		}
		return left < right
	})
}

func serverNodeID(peers map[uint64]Peer, server raftengine.Server) uint64 {
	if nodeID, ok := configServerNodeID(peers, server); ok {
		return nodeID
	}
	return DeriveNodeID(server.ID)
}

func configServerNodeID(peers map[uint64]Peer, server raftengine.Server) (uint64, bool) {
	for nodeID, peer := range peers {
		if server.ID != "" && peer.ID == server.ID {
			return nodeID, true
		}
		if server.Address != "" && peer.Address == server.Address {
			return nodeID, true
		}
	}
	return 0, false
}

func equalConfState(left raftpb.ConfState, right raftpb.ConfState) bool {
	return equalUint64s(left.Voters, right.Voters) &&
		equalUint64s(left.Learners, right.Learners) &&
		equalUint64s(left.VotersOutgoing, right.VotersOutgoing) &&
		equalUint64s(left.LearnersNext, right.LearnersNext) &&
		left.AutoLeave == right.AutoLeave
}

func equalUint64s(left []uint64, right []uint64) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}
