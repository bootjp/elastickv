package etcd

import (
	"context"
	"encoding/binary"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/cockroachdb/errors"
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
	errSingleNodeOnly    = errors.New("etcd raft phase 1 prototype only supports a single local voter")
)

type StateMachine interface {
	Apply(data []byte) any
}

type OpenConfig struct {
	NodeID         uint64
	LocalID        string
	LocalAddress   string
	DataDir        string
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
	statePath    string
	tickInterval time.Duration

	storage *etcdraft.MemoryStorage
	rawNode *etcdraft.RawNode
	fsm     StateMachine

	nextRequestID atomic.Uint64

	proposeCh chan proposalRequest
	readCh    chan readRequest
	closeCh   chan struct{}
	doneCh    chan struct{}

	leaderReady chan struct{}
	leaderOnce  sync.Once
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

// Open starts the Phase 1 etcd/raft prototype for a single local voter.
//
// This prototype intentionally blocks until the local node becomes leader or
// ctx expires. Multi-node startup and follower transport are deferred to later
// migration phases.
func Open(ctx context.Context, cfg OpenConfig) (*Engine, error) {
	cfg = normalizeConfig(cfg)
	if err := validateConfig(cfg); err != nil {
		return nil, err
	}

	state, err := loadOrCreateState(cfg.DataDir, cfg.NodeID)
	if err != nil {
		return nil, err
	}
	if err := validateSingleNodeState(state, cfg.NodeID); err != nil {
		return nil, err
	}

	storage, err := newMemoryStorage(state)
	if err != nil {
		return nil, err
	}

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

	engine := &Engine{
		nodeID:       cfg.NodeID,
		localID:      cfg.LocalID,
		localAddress: cfg.LocalAddress,
		dataDir:      cfg.DataDir,
		statePath:    stateFilePath(cfg.DataDir),
		tickInterval: cfg.TickInterval,
		storage:      storage,
		rawNode:      rawNode,
		fsm:          cfg.StateMachine,
		proposeCh:    make(chan proposalRequest),
		readCh:       make(chan readRequest),
		closeCh:      make(chan struct{}),
		doneCh:       make(chan struct{}),
		leaderReady:  make(chan struct{}),
		config: raftengine.Configuration{
			Servers: []raftengine.Server{{
				ID:       cfg.LocalID,
				Address:  cfg.LocalAddress,
				Suffrage: "voter",
			}},
		},
		applied:          state.Snapshot.Metadata.Index,
		pendingProposals: map[uint64]proposalRequest{},
		pendingReads:     map[uint64]readRequest{},
	}
	engine.refreshStatus()

	go engine.run()

	select {
	case <-ctx.Done():
		_ = engine.Close()
		return nil, errors.WithStack(ctx.Err())
	case <-engine.leaderReady:
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
		if err := e.applyCommitted(rd.CommittedEntries); err != nil {
			return err
		}
		e.handleReadStates(rd.ReadStates)
		e.rawNode.Advance(rd)
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
	if err := e.applyReadySnapshot(rd.Snapshot); err != nil {
		return err
	}
	if err := e.applyReadyEntries(rd.Entries); err != nil {
		return err
	}
	if err := e.applyReadyHardState(rd.HardState); err != nil {
		return err
	}

	state, err := persistedStateFromStorage(e.storage)
	if err != nil {
		return err
	}
	return saveStateFile(e.statePath, state)
}

func readyNeedsPersistence(rd etcdraft.Ready) bool {
	return len(rd.Entries) > 0 || !etcdraft.IsEmptyHardState(rd.HardState) || !etcdraft.IsEmptySnap(rd.Snapshot)
}

func (e *Engine) applyReadySnapshot(snapshot raftpb.Snapshot) error {
	if etcdraft.IsEmptySnap(snapshot) {
		return nil
	}
	if len(snapshot.Data) > 0 {
		return errors.New("snapshot restore is not implemented for the etcd prototype")
	}
	if err := e.storage.ApplySnapshot(snapshot); err != nil {
		return errors.WithStack(err)
	}
	e.applied = snapshot.Metadata.Index
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
	leader := raftengine.LeaderInfo{}
	switch {
	case basic.Lead == e.nodeID:
		leader = raftengine.LeaderInfo{ID: e.localID, Address: e.localAddress}
	case basic.Lead != etcdraft.None:
		leader = raftengine.LeaderInfo{ID: strconv.FormatUint(basic.Lead, 10)}
	}

	status := raftengine.Status{
		State:             state,
		Leader:            leader,
		Term:              basic.Term,
		CommitIndex:       basic.Commit,
		AppliedIndex:      e.applied,
		LastLogIndex:      lastLogIndex,
		LastSnapshotIndex: snapshot.Metadata.Index,
		FSMPending:        pendingEntries(basic.Commit, e.applied),
		NumPeers:          0,
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

func (e *Engine) shutdown() {
	e.mu.Lock()
	e.closed = true
	e.status.State = raftengine.StateShutdown
	e.mu.Unlock()
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
	if cfg.LocalID == "" && cfg.NodeID != 0 {
		cfg.LocalID = strconv.FormatUint(cfg.NodeID, 10)
	}
	if cfg.TickInterval <= 0 {
		cfg.TickInterval = defaultTickInterval
	}
	if cfg.HeartbeatTick <= 0 {
		cfg.HeartbeatTick = defaultHeartbeatTick
	}
	if cfg.ElectionTick <= 0 {
		cfg.ElectionTick = defaultElectionTick
	}
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

func validateSingleNodeState(state persistedState, nodeID uint64) error {
	conf := state.Snapshot.Metadata.ConfState
	if len(conf.Voters) != 1 || conf.Voters[0] != nodeID {
		return errors.WithStack(errSingleNodeOnly)
	}
	if len(conf.VotersOutgoing) > 0 || len(conf.Learners) > 0 || len(conf.LearnersNext) > 0 || conf.AutoLeave {
		return errors.WithStack(errSingleNodeOnly)
	}
	for _, entry := range state.Entries {
		if entry.Type == raftpb.EntryConfChange || entry.Type == raftpb.EntryConfChangeV2 {
			return errors.WithStack(errSingleNodeOnly)
		}
	}
	return nil
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
