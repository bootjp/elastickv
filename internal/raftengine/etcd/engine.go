package etcd

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"
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
	defaultTickInterval   = 10 * time.Millisecond
	defaultHeartbeatTick  = 1
	defaultElectionTick   = 10
	defaultMaxInflightMsg = 256
	defaultMaxSizePerMsg  = 1 << 20
	unknownLastContact    = time.Duration(-1)

	proposalEnvelopeVersion = byte(0x01)
	readContextVersion      = byte(0x02)
	envelopeHeaderSize      = 9
)

var (
	errNilEngine         = errors.New("raft engine is not configured")
	errClosed            = errors.New("etcd raft engine is closed")
	errNotLeader         = errors.New("etcd raft engine is not leader")
	errNodeIDRequired    = errors.New("etcd raft node id is required")
	errDataDirRequired   = errors.New("etcd raft data dir is required")
	errStateMachineUnset = errors.New("etcd raft state machine is not configured")
	errSnapshotRequired  = errors.New("etcd raft snapshot payload is required")
	errClusterMismatch   = errors.New("etcd raft persisted cluster does not match configured peers")
)

type Snapshot interface {
	// Snapshot is an owned export handle from the state machine. Callers are
	// responsible for closing it after WriteTo completes.
	WriteTo(w io.Writer) (int64, error)
	Close() error
}

type StateMachine interface {
	Apply(data []byte) any
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

	proposeCh chan proposalRequest
	readCh    chan readRequest
	stepCh    chan raftpb.Message
	closeCh   chan struct{}
	doneCh    chan struct{}
	startedCh chan struct{}

	leaderReady chan struct{}
	leaderOnce  sync.Once
	startOnce   sync.Once
	closeOnce   sync.Once

	mu      sync.RWMutex
	status  raftengine.Status
	config  raftengine.Configuration
	runErr  error
	closed  bool
	applied uint64

	pendingProposals map[uint64]proposalRequest
	pendingReads     map[uint64]readRequest
}

type proposalRequest struct {
	id      uint64
	payload []byte
	done    chan proposalResult
}

type proposalResult struct {
	result *raftengine.ProposalResult
	err    error
}

type readRequest struct {
	id     uint64
	done   chan readResult
	target uint64
}

type readResult struct {
	index uint64
	err   error
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

	engine := &Engine{
		nodeID:       cfg.NodeID,
		localID:      cfg.LocalID,
		localAddress: cfg.LocalAddress,
		dataDir:      cfg.DataDir,
		tickInterval: cfg.TickInterval,
		storage:      disk.Storage,
		rawNode:      rawNode,
		persist:      disk.Persist,
		fsm:          cfg.StateMachine,
		peers:        peerMap,
		transport:    cfg.Transport,
		proposeCh:    make(chan proposalRequest),
		readCh:       make(chan readRequest),
		stepCh:       make(chan raftpb.Message, defaultMaxInflightMsg),
		closeCh:      make(chan struct{}),
		doneCh:       make(chan struct{}),
		startedCh:    make(chan struct{}),
		leaderReady:  make(chan struct{}),
		config: raftengine.Configuration{
			Servers: configServersForPeers(peers),
		},
		applied:          maxAppliedIndex(disk.Storage, disk.LocalSnap),
		pendingProposals: map[uint64]proposalRequest{},
		pendingReads:     map[uint64]readRequest{},
	}
	if engine.transport != nil {
		// Transport listeners may already be accepting RPCs when Open is called.
		// Gate inbound delivery on startedCh so messages are not enqueued until the
		// local run loop has completed startup.
		engine.transport.SetHandler(engine.handleTransportMessage)
	}
	engine.refreshStatus()

	go engine.run()

	return waitForOpen(ctx, engine, len(peers) == 1)
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
	if err := contextErr(ctx); err != nil {
		return 0, err
	}
	if e == nil {
		return 0, errors.WithStack(errNilEngine)
	}
	req := readRequest{
		id:   e.nextID(),
		done: make(chan readResult, 1),
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
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.config, nil
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

		if !e.handleEvent(ticker.C) {
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
	if len(e.peers) > 1 {
		return nil
	}
	if err := e.rawNode.Campaign(); err != nil {
		return errors.WithStack(err)
	}
	return e.drainReady()
}

func (e *Engine) handleEvent(tick <-chan time.Time) bool {
	select {
	case <-e.closeCh:
		return false
	case <-tick:
		e.rawNode.Tick()
	case req := <-e.proposeCh:
		e.handleProposal(req)
	case req := <-e.readCh:
		e.handleRead(req)
	case msg := <-e.stepCh:
		e.handleStep(msg)
	}
	return true
}

func (e *Engine) handleProposal(req proposalRequest) {
	if e.State() != raftengine.StateLeader {
		req.done <- proposalResult{err: errors.WithStack(errNotLeader)}
		return
	}
	e.pendingProposals[req.id] = req
	if err := e.rawNode.Propose(encodeProposalEnvelope(req.id, req.payload)); err != nil {
		delete(e.pendingProposals, req.id)
		req.done <- proposalResult{err: errors.WithStack(err)}
	}
}

func (e *Engine) handleRead(req readRequest) {
	if e.State() != raftengine.StateLeader {
		req.done <- readResult{err: errors.WithStack(errNotLeader)}
		return
	}
	e.pendingReads[req.id] = req
	e.rawNode.ReadIndex(encodeReadContext(req.id))
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
	if err := e.rawNode.Step(msg); err != nil {
		e.fail(errors.WithStack(err))
	}
}

func (e *Engine) sendMessages(messages []raftpb.Message) error {
	for _, msg := range messages {
		if msg.To == 0 || msg.To == e.nodeID || etcdraft.IsLocalMsg(msg.Type) {
			continue
		}
		if e.transport == nil {
			continue
		}
		if err := e.transport.Dispatch(context.Background(), msg); err != nil {
			continue
		}
	}
	return nil
}

func (e *Engine) applyReadySnapshot(snapshot raftpb.Snapshot) error {
	if etcdraft.IsEmptySnap(snapshot) {
		return nil
	}
	if len(snapshot.Data) == 0 {
		return errors.WithStack(errSnapshotRequired)
	}
	if err := e.fsm.Restore(bytes.NewReader(snapshot.Data)); err != nil {
		return errors.WithStack(err)
	}
	metaSnapshot := snapshot
	metaSnapshot.Data = nil
	if err := e.storage.ApplySnapshot(metaSnapshot); err != nil {
		return errors.WithStack(err)
	}
	e.applied = metaSnapshot.Metadata.Index
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
	if e.applied == 0 || e.persist == nil {
		return nil
	}

	current, err := e.storage.Snapshot()
	if err != nil {
		return errors.WithStack(err)
	}
	if e.applied <= current.Metadata.Index {
		return nil
	}
	snapshot, err := persistLocalSnapshot(e.storage, e.persist, e.fsm, e.applied)
	if err != nil {
		return err
	}
	return e.refreshLocalSnapshot(snapshot)
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
			e.rawNode.ApplyConfChange(cc)
			e.applied = entry.Index
		case raftpb.EntryConfChangeV2:
			var cc raftpb.ConfChangeV2
			if err := cc.Unmarshal(entry.Data); err != nil {
				return errors.WithStack(err)
			}
			e.rawNode.ApplyConfChange(cc)
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
	req, ok := e.pendingProposals[id]
	if !ok {
		return
	}
	delete(e.pendingProposals, id)
	req.done <- proposalResult{
		result: &raftengine.ProposalResult{
			CommitIndex: commitIndex,
			Response:    response,
		},
	}
}

func (e *Engine) handleReadStates(states []etcdraft.ReadState) {
	for _, state := range states {
		id, ok := decodeReadContext(state.RequestCtx)
		if !ok {
			continue
		}
		req, ok := e.pendingReads[id]
		if !ok {
			continue
		}
		req.target = state.Index
		e.pendingReads[id] = req
	}
}

func (e *Engine) resolveReadyReads() {
	for id, req := range e.pendingReads {
		if req.target == 0 || req.target > e.applied {
			continue
		}
		delete(e.pendingReads, id)
		req.done <- readResult{index: req.target}
	}
}

func (e *Engine) refreshStatus() {
	basic := e.rawNode.BasicStatus()
	lastLogIndex, _ := e.storage.LastIndex()
	snapshot, _ := e.storage.Snapshot()

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
		NumPeers:          numRemotePeers(e.peers),
		LastContact:       lastContactFor(state),
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
	_ = closePersist(e.persist)
	_ = e.transport.Close()
	e.failPending(e.currentErrorOrClosed())
}

func (e *Engine) failPending(err error) {
	for id, req := range e.pendingProposals {
		delete(e.pendingProposals, id)
		req.done <- proposalResult{err: err}
	}
	for id, req := range e.pendingReads {
		delete(e.pendingReads, id)
		req.done <- readResult{err: err}
	}
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

func numRemotePeers(peers map[uint64]Peer) uint64 {
	var count uint64
	for range peers {
		count++
	}
	if count == 0 {
		return 0
	}
	return count - 1
}

func lastContactFor(state raftengine.State) time.Duration {
	if state == raftengine.StateLeader {
		return 0
	}
	return unknownLastContact
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

func maxAppliedIndex(storage *etcdraft.MemoryStorage, snapshot raftpb.Snapshot) uint64 {
	if storage == nil {
		return snapshot.Metadata.Index
	}
	lastApplied := snapshot.Metadata.Index
	if hardState, _, err := storage.InitialState(); err == nil && hardState.Commit > lastApplied {
		lastApplied = hardState.Commit
	}
	return lastApplied
}

func (e *Engine) refreshLocalSnapshot(snapshot raftpb.Snapshot) error {
	if etcdraft.IsEmptySnap(snapshot) {
		return nil
	}
	return nil
}

func (e *Engine) enqueueStep(ctx context.Context, msg raftpb.Message) error {
	select {
	case <-ctx.Done():
		return errors.WithStack(ctx.Err())
	case <-e.doneCh:
		return e.currentErrorOrClosed()
	case e.stepCh <- msg:
		return nil
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

func (e *Engine) leaderInfo(leaderNodeID uint64) raftengine.LeaderInfo {
	if leaderNodeID == 0 {
		return raftengine.LeaderInfo{}
	}
	if peer, ok := e.peers[leaderNodeID]; ok {
		return raftengine.LeaderInfo{ID: peer.ID, Address: peer.Address}
	}
	return raftengine.LeaderInfo{ID: strconv.FormatUint(leaderNodeID, 10)}
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
