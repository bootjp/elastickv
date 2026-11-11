package etcd

import (
	"log/slog"
	"slices"

	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/cockroachdb/errors"
	etcdraft "go.etcd.io/raft/v3"
	raftpb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

type pipelineSnapshotStage uint8

const (
	pipelineCheckpointCapture pipelineSnapshotStage = iota
	pipelineCheckpointSave
	pipelineCheckpointRelease
	pipelineReceivedSave
	pipelineReceivedRestore
	pipelineReceivedRelease
	pipelineReceivedDiscard
)

type pipelineSnapshotPhase uint8

const (
	pipelineCheckpointFront pipelineSnapshotPhase = iota
	pipelineReceivedFront
	pipelineReceivedFinish
	pipelineReceivedPublish
)

// Metadata and inventory are owner-captured immutable tuples. Payload is filled
// exclusively by the disk actor, then handed back through its real completion.
type pipelineSnapshotPlan struct {
	snapshot   *raftpb.Snapshot
	message    *raftpb.Message
	peers      []Peer
	env        pipelineConfigurationEnvironment
	files      *pipelineSnapshotFiles
	lease      *pipelineSnapshotLease
	permits    []*storageWorkPermit
	frontReply chan error
	appendTask *pipelineTask
	accepted   bool // owner only, except after a reply/completion
	deferred   bool // apply actor, handed back by completion
}

type pipelineSnapshotRequest struct {
	phase pipelineSnapshotPhase
	plan  *pipelineSnapshotPlan
	index uint64
	reply chan error
}

func validatePipelineSnapshotIngress(message *raftpb.Message) error {
	if message.GetType() != raftpb.MsgSnap {
		if message.Snapshot != nil {
			return errors.WithStack(errInvalidStorageWork)
		}
		return nil
	}
	snapshot := message.Snapshot
	if etcdraft.IsEmptySnap(snapshot) || len(snapshot.GetData()) == 0 || len(snapshot.GetMetadata().GetConfState().GetVoters()) == 0 {
		return errors.WithStack(errSnapshotRequired)
	}
	if isSnapshotToken(snapshot.GetData()) {
		token, err := decodeSnapshotToken(snapshot.GetData())
		if err != nil {
			return err
		}
		if token.Index != snapshot.GetMetadata().GetIndex() {
			return errors.WithStack(ErrFSMSnapshotTokenInvalid)
		}
	}
	return validatePipelineSnapshotDescriptor(snapshot)
}

func validatePipelineSnapshotDescriptor(snapshot *raftpb.Snapshot) error {
	if err := validatePipelineConfigurationState(snapshot.GetMetadata().GetConfState()); err != nil {
		return err
	}
	metadata, err := pipelineNormalizationMetadata(snapshot)
	if err != nil {
		return err
	}
	if pipelineSnapshotAccounting(&metadata, true) > pipelineControlBytes {
		return errors.WithStack(errStorageWorkInvalidReservation)
	}
	return nil
}

func (e *Engine) newPipelineSnapshotPlan(message *raftpb.Message) (*pipelineSnapshotPlan, error) {
	return e.newPipelineSnapshotPlanWithLease(message, nil)
}

func (e *Engine) newPipelineSnapshotPlanWithLease(message *raftpb.Message, lease *pipelineSnapshotLease) (*pipelineSnapshotPlan, error) {
	p := e.storagePipeline
	e.initPipelineSnapshotFiles()
	size := uint64(pipelineTaskOverhead)
	if message != nil {
		if err := validatePipelineSnapshotDescriptor(message.GetSnapshot()); err != nil {
			return nil, err
		}
		// The receive front owns both its detached message and snapshot clone.
		size = pipelineAddBytes(size, pipelineCloneMessageBytes(message))
		size = pipelineAddBytes(size, pipelineSnapshotAccounting(message.Snapshot, true))
	}
	permit, err := p.snapshotBudget.tryAcquire(size)
	if err != nil {
		return nil, errors.Wrap(errStoragePipelineResource, "snapshot ownership reservation")
	}
	if lease == nil {
		lease, err = e.newPipelineSnapshotLease(message)
		if err != nil {
			permit.Release()
			return nil, err
		}
	}
	plan := &pipelineSnapshotPlan{message: message, lease: lease, files: p.snapshotFiles, permits: []*storageWorkPermit{permit},
		env: pipelineSnapshotEnvironment(pipelineConfigurationEnvironment{fsm: e.fsm, dataDir: e.dataDir, fsmSnapDir: e.fsmSnapDir, maxWALFiles: e.maxWALFiles})}
	p.snapshotPins[plan] = 0
	if message != nil {
		owned := cloneDispatchMessage(*message)
		plan.message = &owned
		plan.snapshot = proto.CloneOf(owned.Snapshot)
		p.snapshotPins[plan] = plan.snapshot.GetMetadata().GetIndex()
		plan.files.pin(plan.snapshot.GetMetadata().GetIndex())
	}
	return plan, nil
}

func (e *Engine) newPipelineSnapshotLease(message *raftpb.Message) (*pipelineSnapshotLease, error) {
	var retire func()
	if message != nil && isSnapshotToken(message.GetSnapshot().GetData()) {
		index, files := message.GetSnapshot().GetMetadata().GetIndex(), e.storagePipeline.snapshotFiles
		retire = func() { files.releaseReceived(index) }
	}
	return e.storagePipeline.snapshotResources.add(retire)
}

func (e *Engine) maybeQueuePipelineCheckpoint() error {
	p := e.storagePipeline
	if !p.canQueueCheckpoint(e.applied) {
		return nil
	}
	index, err := e.storedSnapshotIndex()
	if err != nil {
		return errors.WithStack(err)
	}
	if e.applied <= index || e.applied-index < e.snapshotThreshold() {
		return nil
	}
	p.checkpointAttempt = e.applied
	plan, err := e.newPipelineSnapshotPlan(nil)
	if err != nil {
		return err
	}
	p.checkpoint = plan
	return e.queuePipelineSnapshotApply(plan, pipelineCheckpointCapture)
}

func (p *engineStoragePipeline) canQueueCheckpoint(applied uint64) bool {
	bytes, work := p.bulkOwnership.debt()
	return bytes == 0 && work == 0 && p.started && !p.quiescing && p.checkpoint == nil && p.receiving == nil && applied != 0 && p.checkpointAttempt != applied
}

func (e *Engine) queuePipelineSnapshotApply(plan *pipelineSnapshotPlan, stage pipelineSnapshotStage) error {
	p := e.storagePipeline
	permit, err := p.applyBudget.tryAcquire(pipelineTaskOverhead)
	if err != nil {
		return errors.Wrap(errStoragePipelineResource, "snapshot apply reservation")
	}
	task := &pipelineTask{permit: permit, snapshotPlan: plan, snapshotStage: stage, diskFence: p.diskIssued}
	task.work = storageWork{token: task, retainedBytes: pipelineTaskOverhead, run: func() (any, error) {
		if err := p.firstFailure(nil); err != nil {
			return nil, err
		}
		_, err := runStorageWork(func() (any, error) { return nil, p.executePipelineSnapshotApply(plan, stage) })
		if err != nil {
			err = p.firstFailure(err)
		}
		return nil, err
	}}
	p.applyDeferred = append(p.applyDeferred, task)
	p.applyOutstanding++
	return nil
}

func (p *engineStoragePipeline) snapshotRequest(request pipelineSnapshotRequest) error {
	request.reply = make(chan error, 1)
	p.apply.completions <- storageWorkCompletion{token: &request}
	return <-request.reply
}

func (p *engineStoragePipeline) executePipelineSnapshotApply(plan *pipelineSnapshotPlan, stage pipelineSnapshotStage) error {
	if stage == pipelineReceivedRestore {
		return p.restorePipelineSnapshot(plan)
	}
	if stage != pipelineCheckpointCapture {
		return errors.WithStack(errInvalidStorageWork)
	}
	err := p.snapshotRequest(pipelineSnapshotRequest{phase: pipelineCheckpointFront, plan: plan, index: p.applyCursor})
	if errors.Is(err, raftengine.ErrSnapshotDeferred) {
		plan.deferred = true
		return nil
	}
	if err != nil {
		return err
	}
	snapshot, err := plan.env.fsm.Snapshot()
	if errors.Is(err, raftengine.ErrSnapshotDeferred) {
		plan.deferred = true
		return nil
	}
	if err != nil {
		return errors.WithStack(err)
	}
	plan.lease.snapshot = snapshot
	if writer, ok := plan.env.fsm.(raftengine.AppliedIndexWriter); ok {
		return errors.WithStack(writer.SetDurableAppliedIndex(plan.snapshot.GetMetadata().GetIndex()))
	}
	return nil
}

func (e *Engine) handlePipelineSnapshotRequest(request *pipelineSnapshotRequest) error {
	p := e.storagePipeline
	if failure := p.firstFailure(nil); failure != nil {
		request.reply <- failure
		return failure
	}
	switch request.phase {
	case pipelineCheckpointFront:
		err := e.capturePipelineSnapshotTuple(request.plan, request.index)
		request.reply <- err
		if errors.Is(err, raftengine.ErrSnapshotDeferred) {
			return nil
		}
		return err
	case pipelineReceivedFront:
		return e.frontPipelineReceivedSnapshot(request)
	case pipelineReceivedFinish:
		request.plan.frontReply = request.reply
		if err := e.queuePipelineSnapshotDisk(request.plan, pipelineReceivedRelease); err != nil {
			request.reply <- err
			return err
		}
		return nil
	case pipelineReceivedPublish:
		err := e.publishPipelineReceivedSnapshot(request.plan)
		request.reply <- err
		return err
	}
	request.reply <- errInvalidStorageWork
	return errors.WithStack(errInvalidStorageWork)
}

func (e *Engine) capturePipelineSnapshotTuple(plan *pipelineSnapshotPlan, index uint64) error {
	if e.pipelineSnapshotPrefixPending(index) {
		return errors.WithStack(raftengine.ErrSnapshotDeferred)
	}
	if err := e.ensureSnapshotConfigurationHistory(); err != nil {
		return err
	}
	if err := e.prunePipelineConfigurationHistory(); err != nil {
		return err
	}
	state, err := e.snapshotConfigurations.at(index)
	if err != nil {
		return err
	}
	term, err := e.storage.Term(index)
	if err != nil {
		return errors.WithStack(err)
	}
	peers, err := snapshotPeerInventory(e.peers, state)
	if err != nil {
		return err
	}
	snapshot := &raftpb.Snapshot{Metadata: &raftpb.SnapshotMetadata{Index: proto.Uint64(index), Term: proto.Uint64(term), ConfState: state}}
	permit, err := e.storagePipeline.snapshotBudget.tryAcquire(pipelineConfigurationBytes(snapshot) + pipelinePeerInventoryBytes(peers))
	if err != nil {
		return errors.Wrap(errStoragePipelineResource, "snapshot tuple reservation")
	}
	plan.snapshot, plan.peers = snapshot, peers
	plan.permits = append(plan.permits, permit)
	e.storagePipeline.snapshotPins[plan] = index
	plan.files.pin(index)
	return nil
}

func (e *Engine) pipelineSnapshotPrefixPending(index uint64) bool {
	replay, pending := e.snapshotConfigurationReplayIndex, e.pendingConfChangeIndex.Load()
	return index == 0 || (replay != 0 && replay <= index) || (pending != 0 && pending <= index)
}

func snapshotPeerInventory(peers map[uint64]Peer, state *raftpb.ConfState) ([]Peer, error) {
	ids := namespaceApplyMembers(confStateValue(state))
	owned := make([]Peer, 0, len(ids))
	for _, id := range ids {
		peer, ok := peers[id]
		if !ok || peer.Address == "" {
			return nil, errors.Wrapf(errPeerAddressRequired, "snapshot member=%d", id)
		}
		peer.Suffrage = SuffrageLearner
		if slices.Contains(state.GetVoters(), id) || slices.Contains(state.GetVotersOutgoing(), id) {
			peer.Suffrage = SuffrageVoter
		}
		owned = append(owned, peer)
	}
	return owned, nil
}

func pipelinePeerInventoryBytes(peers []Peer) uint64 {
	var size uint64
	for _, peer := range peers {
		size += uint64(len(peer.ID)+len(peer.Address)) + pipelineEntryOverhead
	}
	return size
}

func (e *Engine) queuePipelineSnapshotDisk(plan *pipelineSnapshotPlan, stage pipelineSnapshotStage) error {
	p := e.storagePipeline
	size := pipelineAddBytes(pipelineTaskOverhead, pipelineConfigurationBytes(plan.snapshot))
	permit, err := p.diskBulk.tryAcquire(size)
	if err != nil {
		return errors.Wrap(errStoragePipelineResource, "snapshot disk reservation")
	}
	task := &pipelineTask{permit: permit, snapshotPlan: plan, snapshotStage: stage}
	p.diskIssued++
	task.diskSequence = p.diskIssued
	task.work = storageWork{token: task, retainedBytes: size, run: func() (any, error) {
		if failure := p.firstFailure(nil); failure != nil {
			return nil, failure
		}
		_, err := runStorageWork(func() (any, error) { return nil, p.executePipelineSnapshotDisk(plan, stage) })
		if err != nil {
			err = p.firstFailure(err)
		}
		return nil, err
	}}
	if stage == pipelineReceivedSave {
		plan.appendTask = task
	}
	p.diskDeferred = append(p.diskDeferred, task)
	p.diskOutstanding++
	return nil
}

func (p *engineStoragePipeline) executePipelineSnapshotDisk(plan *pipelineSnapshotPlan, stage pipelineSnapshotStage) error {
	switch stage {
	case pipelineCheckpointSave:
		return p.savePipelineCheckpoint(plan)
	case pipelineCheckpointRelease:
		if err := p.persist.Release(plan.snapshot); err != nil {
			return errors.WithStack(err)
		}
	case pipelineReceivedSave:
		return p.savePipelineReceivedSnapshot(plan)
	case pipelineReceivedRelease:
		return p.finishPipelineReceivedSnapshot(plan)
	case pipelineReceivedDiscard:
		return plan.lease.close()
	case pipelineCheckpointCapture, pipelineReceivedRestore:
		return errors.WithStack(errInvalidStorageWork)
	}
	if err := plan.files.purge(plan.env); err != nil {
		slog.Warn("async snapshot cleanup failed", "error", err)
	}
	return nil
}

func (p *engineStoragePipeline) savePipelineCheckpoint(plan *pipelineSnapshotPlan) error {
	if err := writePipelineCheckpoint(plan); err != nil {
		return err
	}
	return errors.WithStack(p.persist.SaveSnap(plan.snapshot))
}

func (p *engineStoragePipeline) finishPipelineReceivedSnapshot(plan *pipelineSnapshotPlan) error {
	if err := p.releasePipelineReceivedSnapshot(plan); err != nil {
		return err
	}
	if err := plan.files.purge(plan.env); err != nil {
		slog.Warn("async snapshot cleanup failed", "error", err)
	}
	return nil
}

func writePipelineCheckpoint(plan *pipelineSnapshotPlan) (err error) {
	defer func() { err = errors.CombineErrors(err, plan.lease.close()) }()
	if err := plan.files.prepare(plan.env, plan.snapshot.GetMetadata().GetIndex()); err != nil {
		return err
	}
	if plan.env.fsmSnapDir == "" {
		return errors.Wrap(errInvalidStorageWork, "async checkpoint requires file backing")
	}
	plan.files.mu.Lock()
	defer plan.files.mu.Unlock()
	crc, err := writeFSMSnapshotFile(plan.lease.snapshot, plan.env.fsmSnapDir, plan.snapshot.GetMetadata().GetIndex())
	if err != nil {
		return err
	}
	plan.snapshot.Data = encodeSnapshotToken(plan.snapshot.GetMetadata().GetIndex(), crc)
	return nil
}

func (e *Engine) completePipelineSnapshotApply(task *pipelineTask, err error) error {
	plan := task.snapshotPlan
	if err != nil {
		return err
	}
	if failure := e.storagePipeline.firstFailure(nil); failure != nil {
		return failure
	}
	if task.snapshotStage == pipelineCheckpointCapture {
		if plan.deferred {
			e.retirePipelineSnapshot(plan)
			return nil
		}
		return e.queuePipelineSnapshotDisk(plan, pipelineCheckpointSave)
	}
	if task.snapshotStage == pipelineReceivedRestore {
		if !plan.accepted {
			return e.queuePipelineSnapshotDisk(plan, pipelineReceivedDiscard)
		}
		e.retirePipelineSnapshot(plan)
		return nil
	}
	return errors.WithStack(errInvalidStorageWork)
}

func (e *Engine) completePipelineSnapshotDisk(task *pipelineTask, err error) error {
	p, plan := e.storagePipeline, task.snapshotPlan
	p.diskOutstanding--
	p.diskRetired = maxUint64(p.diskRetired, task.diskSequence)
	if task.snapshotStage != pipelineReceivedSave {
		defer task.retire()
	}
	if failure := p.firstFailure(nil); failure != nil {
		err = failure
	}
	if task.snapshotStage == pipelineReceivedSave || task.snapshotStage == pipelineReceivedRelease {
		plan.frontReply <- err
		return err
	}
	if err != nil {
		return err
	}
	if task.snapshotStage == pipelineCheckpointSave {
		return e.publishPipelineCheckpoint(plan)
	}
	e.retirePipelineSnapshot(plan)
	return nil
}

func (e *Engine) publishPipelineCheckpoint(plan *pipelineSnapshotPlan) error {
	if err := e.pipelineMemoryStorageMutable(); err != nil {
		return err
	}
	snapshot := plan.snapshot
	current, err := e.storedSnapshotIndex()
	if err != nil {
		return errors.WithStack(err)
	}
	if current < snapshot.GetMetadata().GetIndex() {
		if _, err := e.storage.CreateSnapshot(snapshot.GetMetadata().GetIndex(), snapshot.GetMetadata().GetConfState(), snapshot.GetData()); err != nil {
			return errors.WithStack(err)
		}
		if err := e.storage.Compact(snapshot.GetMetadata().GetIndex()); err != nil {
			return errors.WithStack(err)
		}
		if e.storagePipeline.snapshotView != nil {
			e.storagePipeline.snapshotView.publish(snapshot, false)
		}
	}
	return e.queuePipelineSnapshotDisk(plan, pipelineCheckpointRelease)
}

func (e *Engine) retirePipelineSnapshot(plan *pipelineSnapshotPlan) {
	p := e.storagePipeline
	permits, task, lease := plan.permits, plan.appendTask, plan.lease
	if index := p.snapshotPins[plan]; index != 0 {
		plan.files.unpin(index)
	}
	delete(p.snapshotPins, plan)
	if task != nil {
		task.retire()
	}
	p.snapshotResources.forget(lease)
	if p.checkpoint == plan {
		p.checkpoint = nil
	}
	if p.receiving == plan {
		p.receiving = nil
	}
	if p.normalizingReceived == plan {
		p.normalizingReceived = nil
	}
	*plan = pipelineSnapshotPlan{}
	for _, permit := range permits {
		permit.Release()
	}
}

func (e *Engine) retirePipelineSnapshotPins() {
	p := e.storagePipeline
	for plan := range p.snapshotPins {
		e.retirePipelineSnapshot(plan)
	}
	if p.checkpoint != nil {
		e.retirePipelineSnapshot(p.checkpoint)
	}
}

func (e *Engine) routePipelineApplyResponses(responses []*raftpb.Message) error {
	for _, response := range responses {
		entries := response.GetEntries()
		if response.GetType() == raftpb.MsgStorageApplyResp && len(entries) > 0 && entries[len(entries)-1].GetIndex() <= e.storagePipeline.publishedSnapshotFloor {
			continue // Actual snapshot application has already covered this range.
		}
		if err := e.routePipelineResponses([]*raftpb.Message{response}); err != nil {
			return err
		}
	}
	return nil
}
