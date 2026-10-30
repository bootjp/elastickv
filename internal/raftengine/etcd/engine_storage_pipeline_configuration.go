package etcd

import (
	"path/filepath"

	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/cockroachdb/errors"
	raftpb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

type pipelineConfigurationPhase uint8

const (
	pipelineConfigurationFront pipelineConfigurationPhase = iota
	pipelineConfigurationPeers
	pipelineConfigurationSave
	pipelineConfigurationRelease
	pipelineConfigurationPublish
)

type pipelineConfigurationEnvironment struct {
	fsm                 StateMachine
	dataDir, fsmSnapDir string
	maxWALFiles         int
}

// Only the owner constructs this plan. All protocol state and peer addresses
// remain immutable until the enclosing apply task's completion is retired.
type pipelineConfigurationPlan struct {
	configApplyPlan
	v1                       *raftpb.ConfChange
	v2                       *raftpb.ConfChangeV2
	term                     uint64
	skipPersistence, capture bool
}

type pipelineConfigurationRequest struct {
	phase    pipelineConfigurationPhase
	task     *pipelineTask
	entry    *raftpb.Entry
	prefix   []normalApplyReceipt
	plan     *pipelineConfigurationPlan
	snapshot *raftpb.Snapshot
	reply    chan pipelineConfigurationReply
}

type pipelineConfigurationReply struct {
	plan *pipelineConfigurationPlan
	err  error
}

type pipelineConfigurationRetention struct {
	index  uint64
	permit *storageWorkPermit
}

func (p *engineStoragePipeline) retireConfigurationHistory() {
	for _, retained := range p.configurationHistory {
		retained.permit.Release()
	}
	p.configurationHistory = nil
}

func pipelineConfigurationBytes(message proto.Message) uint64 {
	size := proto.Size(message)
	if size < 0 {
		return pipelineControlBytes + 1
	}
	return uint64(size) + pipelineTaskOverhead
}

func (e *Engine) recordPipelineConfiguration(plan configApplyPlan) error {
	p := e.storagePipeline
	if err := e.prunePipelineConfigurationHistory(); err != nil {
		return err
	}
	permit, err := p.configurationHistoryBudget.tryAcquire(pipelineConfigurationBytes(&plan.confState))
	if err != nil {
		return errors.Wrap(errStoragePipelineResource, "retained configuration history limit")
	}
	if err := e.recordSnapshotConfiguration(plan); err != nil {
		permit.Release()
		return err
	}
	p.configurationHistory = append(p.configurationHistory, pipelineConfigurationRetention{index: plan.index, permit: permit})
	return e.prunePipelineConfigurationHistory()
}

func (e *Engine) prunePipelineConfigurationHistory() error {
	p := e.storagePipeline
	snapshot, err := e.storage.Snapshot()
	if err != nil {
		return errors.WithStack(err)
	}
	e.snapshotConfigurations.pruneThrough(snapshot.GetMetadata().GetIndex())
	if len(e.snapshotConfigurations.entries) == 0 {
		return nil
	}
	base := e.snapshotConfigurations.entries[0].index
	for len(p.configurationHistory) > 0 && p.configurationHistory[0].index < base {
		p.configurationHistory[0].permit.Release()
		p.configurationHistory[0] = pipelineConfigurationRetention{}
		p.configurationHistory = p.configurationHistory[1:]
	}
	return nil
}

func (p *engineStoragePipeline) configurationRequest(request pipelineConfigurationRequest) (*pipelineConfigurationPlan, error) {
	request.reply = make(chan pipelineConfigurationReply, 1)
	// Typed callbacks share the apply stream's FIFO, but are not work receipts:
	// the enclosing task remains executing and retains both reservations.
	p.apply.completions <- storageWorkCompletion{token: &request}
	reply := <-request.reply
	return reply.plan, reply.err
}

func (p *engineStoragePipeline) executePipelineConfiguration(task *pipelineTask, entry *raftpb.Entry) error {
	prefix := task.receipts
	task.receipts = nil
	plan, err := p.configurationRequest(pipelineConfigurationRequest{
		phase: pipelineConfigurationFront, task: task, entry: entry, prefix: prefix,
	})
	if err != nil {
		return err
	}
	if err := recordNamespaceConfiguration(task.configEnv.fsm, plan.index, plan.confState); err != nil {
		return err
	}
	request := pipelineConfigurationRequest{task: task, plan: plan}
	if !plan.skipPersistence {
		request.phase = pipelineConfigurationPeers
		if _, err := p.configurationRequest(request); err != nil {
			return err
		}
		if err := p.persistPipelineConfiguration(request); err != nil {
			return err
		}
	}
	request.phase = pipelineConfigurationPublish
	if _, err := p.configurationRequest(request); err != nil {
		return err
	}
	if plan.index > p.applyCursor {
		p.applyCursor = plan.index
	}
	return nil
}

func (p *engineStoragePipeline) persistPipelineConfiguration(request pipelineConfigurationRequest) error {
	if !request.plan.capture {
		return nil
	}
	snapshot, err := capturePipelineConfiguration(request.task.configEnv, request.plan)
	if err != nil {
		return err
	}
	if snapshot == nil {
		return nil
	}
	request.snapshot = snapshot
	request.phase = pipelineConfigurationSave
	if _, err := p.configurationRequest(request); err != nil {
		return err
	}
	request.phase = pipelineConfigurationRelease
	_, err = p.configurationRequest(request)
	return err
}

func capturePipelineConfiguration(env pipelineConfigurationEnvironment, plan *pipelineConfigurationPlan) (*raftpb.Snapshot, error) {
	snapshot, err := env.fsm.Snapshot()
	if err != nil {
		// Only capture itself can defer. Close, serialization and durable-index
		// errors are fatal even if they happen to wrap the same sentinel.
		if errors.Is(err, raftengine.ErrSnapshotDeferred) {
			return nil, nil
		}
		return nil, errors.WithStack(err)
	}
	var payload []byte
	if env.fsmSnapDir == "" {
		payload, err = snapshotBytesAndClose(snapshot, env.dataDir)
	} else {
		var crc uint32
		crc, err = writeFSMSnapshotFile(snapshot, env.fsmSnapDir, plan.index)
		closeErr := snapshot.Close()
		if err == nil {
			err = closeErr
		}
		payload = encodeSnapshotToken(plan.index, crc)
	}
	if err != nil {
		return nil, errors.WithStack(err)
	}
	if writer, ok := env.fsm.(raftengine.AppliedIndexWriter); ok {
		if err := writer.SetDurableAppliedIndex(plan.index); err != nil {
			return nil, errors.WithStack(err)
		}
	}
	return &raftpb.Snapshot{Data: payload, Metadata: &raftpb.SnapshotMetadata{
		Index: proto.Uint64(plan.index), Term: proto.Uint64(plan.term), ConfState: proto.CloneOf(&plan.confState),
	}}, nil
}

func (e *Engine) handlePipelineConfiguration(request pipelineConfigurationRequest) error {
	if request.phase == pipelineConfigurationFront {
		if err := e.publishPipelineReceipts(request.prefix); err != nil {
			request.reply <- pipelineConfigurationReply{err: err}
			return err
		}
	}
	if err := e.storagePipeline.firstFailure(nil); err != nil {
		request.reply <- pipelineConfigurationReply{err: err}
		return err
	}
	if request.phase == pipelineConfigurationFront {
		plan, err := e.frontPipelineConfiguration(request)
		request.reply <- pipelineConfigurationReply{plan: plan, err: err}
		return err
	}
	if request.plan == nil || request.plan != e.storagePipeline.configuration {
		request.reply <- pipelineConfigurationReply{err: errInvalidStorageWork}
		return errors.WithStack(errInvalidStorageWork)
	}
	if request.phase == pipelineConfigurationPublish {
		err := e.publishPipelineConfiguration(request.plan)
		request.reply <- pipelineConfigurationReply{err: err}
		return err
	}
	if err := e.queuePipelineConfigurationDisk(request); err != nil {
		request.reply <- pipelineConfigurationReply{err: err}
		return err
	}
	return nil
}

func (e *Engine) frontPipelineConfiguration(request pipelineConfigurationRequest) (*pipelineConfigurationPlan, error) {
	plan := &pipelineConfigurationPlan{}
	var err error
	switch request.entry.GetType() {
	case raftpb.EntryConfChange:
		plan.v1 = &raftpb.ConfChange{}
		if err := proto.Unmarshal(request.entry.GetData(), plan.v1); err != nil {
			return nil, errors.WithStack(err)
		}
	case raftpb.EntryConfChangeV2:
		plan.v2 = &raftpb.ConfChangeV2{}
		if err := proto.Unmarshal(request.entry.GetData(), plan.v2); err != nil {
			return nil, errors.WithStack(err)
		}
	case raftpb.EntryNormal:
		return nil, errors.WithStack(errInvalidStorageWork)
	default:
		return nil, errors.WithStack(errInvalidStorageWork)
	}
	e.configurationTransition.Store(true)
	e.ackTracker.reset()
	e.singleNodeLeaderAckMonoNs.Store(0)
	e.fireLeaderLossCallbacks()
	if plan.v1 != nil {
		plan.configApplyPlan, err = e.transitionConfChange(request.entry.GetIndex(), *plan.v1)
	} else {
		plan.configApplyPlan, err = e.transitionConfChangeV2(request.entry.GetIndex(), *plan.v2)
	}
	if err != nil {
		return nil, err
	}
	return e.finishPipelineConfigurationPlan(request.task, plan)
}

func (e *Engine) finishPipelineConfigurationPlan(task *pipelineTask, plan *pipelineConfigurationPlan) (*pipelineConfigurationPlan, error) {
	size := pipelineConfigurationBytes(&plan.confState)
	if plan.v1 != nil {
		size += pipelineConfigurationBytes(plan.v1)
	} else {
		size += pipelineConfigurationBytes(plan.v2) + uint64(len(plan.v2.GetChanges()))*pipelineEntryOverhead
	}
	for _, peer := range plan.peers {
		size += uint64(len(peer.ID) + len(peer.Address) + pipelineEntryOverhead)
	}
	permit, err := e.storagePipeline.configurationBudget.tryAcquire(size)
	if err != nil {
		return nil, errors.Wrap(errStoragePipelineResource, "configuration plan reservation")
	}
	task.configPermits = append(task.configPermits, permit)
	plan.term, err = e.storage.Term(plan.index)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	snapshot, err := e.storage.Snapshot()
	if err != nil {
		return nil, errors.WithStack(err)
	}
	plan.skipPersistence = e.currentConfigIndex() >= plan.index
	plan.capture = plan.index >= e.applied && plan.index > snapshot.GetMetadata().GetIndex()
	e.storagePipeline.configuration = plan
	return plan, e.foldPipelineReady()
}

func (e *Engine) queuePipelineConfigurationDisk(request pipelineConfigurationRequest) error {
	p := e.storagePipeline
	size := uint64(pipelineTaskOverhead)
	if request.snapshot != nil {
		size += pipelineConfigurationBytes(request.snapshot)
	}
	budget := p.diskControl
	if request.phase == pipelineConfigurationSave {
		budget = p.diskBulk
	}
	permit, err := budget.tryAcquire(size)
	if err != nil {
		return errors.Wrap(errStoragePipelineResource, "configuration disk reservation")
	}
	task := &pipelineTask{permit: permit, configDisk: &request}
	env := request.task.configEnv
	task.work = storageWork{token: task, retainedBytes: size, run: func() (any, error) {
		if err := p.firstFailure(nil); err != nil {
			return nil, err
		}
		_, err := runStorageWork(func() (any, error) { return nil, runPipelineConfigurationDisk(p, env, request) })
		if err != nil {
			err = p.firstFailure(err)
		}
		return nil, err
	}}
	p.diskIssued++
	task.diskSequence = p.diskIssued
	p.diskDeferred = append(p.diskDeferred, task)
	p.diskOutstanding++
	return nil
}

func runPipelineConfigurationDisk(p *engineStoragePipeline, env pipelineConfigurationEnvironment, request pipelineConfigurationRequest) error {
	switch request.phase {
	case pipelineConfigurationPeers:
		if err := writeCurrentPersistedPeers(env.dataDir, request.plan.index, request.plan.peers); err != nil {
			return err
		}
		if request.plan.capture && env.fsmSnapDir != "" {
			return prepareFSMSnapshotWriteProtected(filepath.Join(env.dataDir, snapDirName), env.fsmSnapDir, request.plan.index, nil)
		}
		return nil
	case pipelineConfigurationSave:
		return errors.WithStack(p.persist.SaveSnap(request.snapshot))
	case pipelineConfigurationRelease:
		if err := p.persist.Release(request.snapshot); err != nil {
			return errors.WithStack(err)
		}
		snapshotDiskPhase{persist: p.persist, dataDir: env.dataDir, fsmSnapDir: env.fsmSnapDir, maxWALFiles: env.maxWALFiles}.purge()
		return nil
	case pipelineConfigurationFront, pipelineConfigurationPublish:
		return errors.WithStack(errInvalidStorageWork)
	}
	return errors.WithStack(errInvalidStorageWork)
}

func (e *Engine) completePipelineConfigurationDisk(request *pipelineConfigurationRequest, err error) error {
	if failure := e.storagePipeline.firstFailure(nil); failure != nil {
		err = failure
	}
	if err == nil && request.phase == pipelineConfigurationSave {
		snapshot := request.snapshot
		_, err = e.storage.CreateSnapshot(snapshot.GetMetadata().GetIndex(), snapshot.GetMetadata().GetConfState(), snapshot.GetData())
	}
	request.reply <- pipelineConfigurationReply{err: err}
	return errors.WithStack(err)
}

func (e *Engine) publishPipelineConfiguration(plan *pipelineConfigurationPlan) error {
	if err := e.recordPipelineConfiguration(plan.configApplyPlan); err != nil {
		return err
	}
	if plan.v1 != nil {
		cc := plan.v1
		e.publishConfigChange(cc.GetType(), cc.GetNodeId(), cc.GetContext(), plan.index, plan.confState)
	} else {
		e.publishConfigChangeV2(*plan.v2, plan.index, plan.confState)
	}
	e.clearPendingConfChange(plan.index)
	e.setApplied(plan.index)
	e.ackTracker.reset()
	e.singleNodeLeaderAckMonoNs.Store(0)
	e.configurationTransition.Store(false)
	e.storagePipeline.configuration = nil
	if !e.storagePipeline.quiescing || plan.index <= e.storagePipeline.shutdownCommit {
		if plan.v1 != nil {
			e.resolveConfigChange(plan.index, plan.v1.GetContext())
		} else {
			e.resolveConfigChange(plan.index, plan.v2.GetContext())
		}
	}
	e.alarmIfJoinedAsVoter(plan.confState)
	return nil
}
