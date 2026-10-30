package etcd

import (
	"bytes"
	"io"

	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/cockroachdb/errors"
	"go.etcd.io/raft/v3/confchange"
	raftpb "go.etcd.io/raft/v3/raftpb"
	"go.etcd.io/raft/v3/tracker"
)

func (e *Engine) queuePipelineReceivedFront(message *raftpb.Message) error {
	return e.queuePipelineReceivedFrontWithLease(message, nil)
}

func (e *Engine) queuePipelineReceivedFrontWithLease(message *raftpb.Message, lease *pipelineSnapshotLease) error {
	if err := validatePipelineIngress(message); err != nil {
		return err
	}
	owned := cloneDispatchMessage(*message)
	plan, err := e.newPipelineSnapshotPlanWithLease(&owned, lease)
	if err != nil {
		return err
	}
	return e.queuePipelineSnapshotApply(plan, pipelineReceivedRestore)
}

func (e *Engine) frontPipelineReceivedSnapshot(request *pipelineSnapshotRequest) error {
	p, plan := e.storagePipeline, request.plan
	state := plan.snapshot.GetMetadata().GetConfState()
	_, _, err := confchange.Restore(confchange.Changer{Tracker: tracker.MakeProgressTracker(defaultMaxInflightMsg, 0), LastIndex: plan.snapshot.GetMetadata().GetIndex()}, state)
	if err != nil {
		request.reply <- err
		return errors.WithStack(err)
	}
	plan.peers, err = snapshotPeerInventory(e.peers, state)
	if err != nil {
		request.reply <- err
		return err
	}
	permit, err := p.snapshotBudget.tryAcquire(pipelinePeerInventoryBytes(plan.peers) + pipelineTaskOverhead)
	if err != nil {
		request.reply <- err
		return errors.Wrap(errStoragePipelineResource, "received snapshot inventory")
	}
	plan.permits = append(plan.permits, permit)
	plan.frontReply = request.reply
	p.receiving = plan
	e.snapshotTransition.Store(true)
	e.ackTracker.reset()
	e.singleNodeLeaderAckMonoNs.Store(0)
	e.fireLeaderLossCallbacks()
	e.recordLeaderContact(*plan.message)
	if err := e.rawNode.Step(plan.message); err != nil {
		request.reply <- err
		return errors.WithStack(err)
	}
	if err := e.foldPipelineReady(); err != nil {
		request.reply <- err
		return err
	}
	if plan.appendTask == nil {
		p.receiving = nil
		e.snapshotTransition.Store(false)
		request.reply <- nil
	}
	return nil
}

func (e *Engine) queuePipelineReceivedSave(message *raftpb.Message) error {
	p := e.storagePipeline
	plan := p.receiving
	if plan == nil || message.GetSnapshot().GetMetadata().GetIndex() != plan.snapshot.GetMetadata().GetIndex() {
		return errors.WithStack(errInvalidStorageWork)
	}
	plan.message = message
	plan.snapshot = message.Snapshot
	plan.accepted = true
	detachPipelineMessage(message)
	// RawNode restore has physically replaced its unstable entry slice. Earlier
	// actor borrows keep their independent reservations until real completion.
	p.ledger.releaseEpoch()
	view := *message
	view.Snapshot = nil
	if err := p.ledger.retain(&view); err != nil {
		return err
	}
	return e.queuePipelineSnapshotDisk(plan, pipelineReceivedSave)
}

func (p *engineStoragePipeline) savePipelineReceivedSnapshot(plan *pipelineSnapshotPlan) error {
	snapshot := plan.snapshot
	if isSnapshotToken(snapshot.GetData()) {
		token, err := decodeSnapshotToken(snapshot.GetData())
		if err != nil {
			return err
		}
		if token.Index != snapshot.GetMetadata().GetIndex() {
			return errors.WithStack(ErrFSMSnapshotTokenInvalid)
		}
		plan.lease.reader, err = plan.files.openReceived(plan.env, token.Index, token.CRC32C)
		if err != nil {
			return err
		}
	} else {
		plan.lease.reader = io.NopCloser(bytes.NewReader(snapshot.GetData()))
	}
	return persistAsyncStorageAppend(p.persist, plan.message)
}

func (p *engineStoragePipeline) restorePipelineSnapshot(plan *pipelineSnapshotPlan) error {
	if err := p.snapshotRequest(pipelineSnapshotRequest{phase: pipelineReceivedFront, plan: plan}); err != nil {
		return err
	}
	if !plan.accepted {
		return nil
	}
	if err := plan.env.fsm.Restore(plan.lease.reader); err != nil {
		return errors.WithStack(err)
	}
	// Verify/read all bytes even if a valid FSM parser stops before EOF. The
	// token's full checksum was verified on disk before any FSM side effect.
	if _, err := io.Copy(io.Discard, plan.lease.reader); err != nil {
		return errors.WithStack(err)
	}
	if err := recoverNamespaceConfiguration(plan.env.fsm, *plan.snapshot); err != nil {
		return err
	}
	index := plan.snapshot.GetMetadata().GetIndex()
	if writer, ok := plan.env.fsm.(raftengine.AppliedIndexWriter); ok {
		if err := writer.SetDurableAppliedIndex(index); err != nil {
			return errors.WithStack(err)
		}
	}
	if err := p.snapshotRequest(pipelineSnapshotRequest{phase: pipelineReceivedFinish, plan: plan}); err != nil {
		return err
	}
	p.applyCursor, p.receivedApplyFloor = index, index
	return p.snapshotRequest(pipelineSnapshotRequest{phase: pipelineReceivedPublish, plan: plan})
}

func (p *engineStoragePipeline) releasePipelineReceivedSnapshot(plan *pipelineSnapshotPlan) error {
	if err := plan.lease.close(); err != nil {
		return err
	}
	if err := savePersistedPeers(plan.env.dataDir, plan.snapshot.GetMetadata().GetIndex(), plan.peers); err != nil {
		return err
	}
	return errors.WithStack(p.persist.Release(plan.snapshot))
}

func (e *Engine) publishPipelineReceivedSnapshot(plan *pipelineSnapshotPlan) error {
	p := e.storagePipeline
	if err := e.flushHeldSnapshotDisk(); err != nil {
		return err
	}
	snapshot := plan.snapshot
	if err := e.storage.ApplySnapshot(snapshot); err != nil {
		return errors.WithStack(err)
	}
	if err := e.storage.Append(plan.message.Entries); err != nil {
		return errors.WithStack(err)
	}
	if err := e.applyReadyHardState(&raftpb.HardState{Term: plan.message.Term, Vote: plan.message.Vote, Commit: plan.message.Commit}); err != nil {
		return err
	}
	e.snapshotConfigurations.reset(snapshot.GetMetadata().GetIndex(), snapshot.GetMetadata().GetConfState())
	e.snapshotConfigurationReplayIndex = 0
	p.retireConfigurationHistory()
	e.applied = snapshot.GetMetadata().GetIndex()
	e.appliedIndex.Store(e.applied)
	e.clearPendingConfChange(e.applied)
	state := confStateValue(snapshot.GetMetadata().GetConfState())
	e.refreshVoterCache(state)
	e.setConfigurationFromConfState(state, e.applied)
	e.snapshotTransition.Store(false)
	p.publishedSnapshotFloor = e.applied
	p.receiving = nil
	if err := e.routePipelineResponses(plan.message.Responses); err != nil {
		return err
	}
	e.alarmIfJoinedAsVoter(state)
	return e.flushPublishedSnapshotDisk()
}

func (e *Engine) flushHeldSnapshotDisk() error {
	p := e.storagePipeline
	held := p.heldSnapshotDisk
	p.heldSnapshotDisk = nil
	// Publish earlier FIFO append receipts before installing the new storage
	// base. Later appends remain retained until the base has been installed.
	for i, completion := range held {
		task, ok := completion.token.(*pipelineTask)
		if !ok {
			p.heldSnapshotDisk = append(p.heldSnapshotDisk, held[i:]...)
			return errors.WithStack(errInvalidStorageWork)
		}
		if task.diskSequence > p.receiving.appendTask.diskSequence {
			p.heldSnapshotDisk = append(p.heldSnapshotDisk, held[i:]...)
			return nil
		}
		receiving := p.receiving
		p.receiving = nil
		err := e.completePipelineDisk(completion)
		p.receiving = receiving
		if err != nil {
			p.heldSnapshotDisk = append(p.heldSnapshotDisk, held[i+1:]...)
			return err
		}
	}
	return nil
}

func (e *Engine) retireHeldSnapshotDisk() {
	p := e.storagePipeline
	for _, completion := range p.heldSnapshotDisk {
		completion.Release()
		if task, ok := completion.token.(*pipelineTask); ok {
			task.retire()
			p.diskOutstanding--
		}
	}
	p.heldSnapshotDisk = nil
}

func (e *Engine) flushPublishedSnapshotDisk() error {
	p := e.storagePipeline
	if p.receiving != nil {
		return nil
	}
	held := p.heldSnapshotDisk
	p.heldSnapshotDisk = nil
	for i, completion := range held {
		if err := e.completePipelineDisk(completion); err != nil {
			p.heldSnapshotDisk = append(p.heldSnapshotDisk, held[i+1:]...)
			return err
		}
	}
	return nil
}
