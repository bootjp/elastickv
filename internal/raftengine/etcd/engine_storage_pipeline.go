package etcd

import (
	"context"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/cockroachdb/errors"
	etcdstorage "go.etcd.io/etcd/server/v3/storage"
	etcdraft "go.etcd.io/raft/v3"
	raftpb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

var (
	errStoragePipelineResource = errors.New("asynchronous storage prototype terminal resource limit")
)

const (
	pipelineBulkBytes        = 512 << 20
	pipelineControlBytes     = 8 << 20
	pipelineBulkItems        = 128
	pipelineControlItems     = 256
	pipelineActorQueue       = 16
	pipelineEntryRoom        = 128 << 20
	pipelineUncommittedBytes = 256 << 20
	pipelineTaskOverhead     = 512
	pipelineEntryOverhead    = 256
	pipelineResponseOverhead = 64
)

type pipelineIngress struct {
	message raftpb.Message
	permit  *storageWorkPermit
	lease   *pipelineSnapshotLease
}

type pipelineTask struct {
	work          storageWork
	permit        *storageWorkPermit
	message       *raftpb.Message
	inputs        []normalApplyInput
	receipts      []normalApplyReceipt // written by apply actor, read after completion
	startup       bool
	diskSequence  uint64
	diskFence     uint64
	configDisk    *pipelineConfigurationRequest
	configEnv     pipelineConfigurationEnvironment
	configPermits []*storageWorkPermit
	snapshotPlan  *pipelineSnapshotPlan
	snapshotStage pipelineSnapshotStage
}

// engineStoragePipeline is an unactivated runtime. Its own
// queues have finite byte AND item reservations across deferred, executing and
// unconsumed completion states. These are not the full ingress/unstable/outbound
// atomic-Ready debt contract, nor a bound on durable MemoryStorage or FSM memory.
// Only the owner accesses the ledger, deferred queues and lifecycle counters.
type engineStoragePipeline struct {
	persist etcdstorage.Storage
	disk    *storageWorker
	apply   *storageWorker
	ledger  *asyncUnstableLedger

	diskBulk, diskControl, applyBudget *storageWorkBudget
	diskQueueBudget, applyQueueBudget  *storageWorkBudget
	proposalBudget, bulkIngressBudget  *storageWorkBudget
	priorityIngressBudget, readyBudget *storageWorkBudget
	outboundBulk, outboundControl      *storageWorkBudget
	bulk, priority                     chan pipelineIngress
	bulkPaused                         atomic.Bool
	stopping                           atomic.Bool
	ingressMu                          sync.Mutex
	failureMu                          sync.Mutex
	failure                            error

	diskDeferred, applyDeferred       []*pipelineTask
	diskOutstanding, applyOutstanding int
	diskIssued, diskRetired           uint64
	shutdownCommit                    uint64
	applyCursor                       uint64 // apply actor only, seeded before first submission
	failedReady                       *etcdraft.Ready
	failedReadyPermit                 *storageWorkPermit
	startupQueued, startupDone        bool
	campaignChecked, started          bool
	quiescing, actorsQuiesced         bool
	configurationBudget               *storageWorkBudget
	configurationHistoryBudget        *storageWorkBudget
	configurationHistory              []pipelineConfigurationRetention
	configuration                     *pipelineConfigurationPlan
	snapshotResources                 *pipelineSnapshotResources
	snapshotFiles                     *pipelineSnapshotFiles
	snapshotBudget                    *storageWorkBudget
	snapshotIngressBudget             *storageWorkBudget
	snapshotPins                      map[*pipelineSnapshotPlan]uint64
	checkpoint                        *pipelineSnapshotPlan
	checkpointAttempt                 uint64
	receiving                         *pipelineSnapshotPlan
	heldSnapshotDisk                  []storageWorkCompletion
	receivedApplyFloor                uint64 // apply actor only
	publishedSnapshotFloor            uint64 // owner only
}

func newEngineStoragePipeline(persist etcdstorage.Storage) (*engineStoragePipeline, error) {
	if persist == nil {
		return nil, errors.WithStack(errInvalidAsyncStorageAppend)
	}
	apply, err := newStorageWorker(pipelineActorQueue)
	if err != nil {
		return nil, err
	}
	resources := &pipelineSnapshotResources{}
	disk, err := newFinalizingStorageWorker(pipelineActorQueue, func() error {
		<-apply.done
		return resources.closeAll()
	})
	if err != nil {
		apply.quiesce()
		<-apply.done
		return nil, err
	}
	p := &engineStoragePipeline{
		persist: persist, disk: disk, apply: apply,
		ledger:                newAsyncUnstableLedger(newStorageWorkBudget(pipelineBulkBytes, pipelineBulkItems)),
		diskBulk:              newStorageWorkBudget(pipelineBulkBytes, pipelineBulkItems),
		diskControl:           newStorageWorkBudget(pipelineControlBytes, pipelineControlItems),
		applyBudget:           newStorageWorkBudget(pipelineBulkBytes, pipelineBulkItems),
		diskQueueBudget:       newStorageWorkBudget(pipelineBulkBytes+pipelineControlBytes, pipelineBulkItems+pipelineControlItems),
		applyQueueBudget:      newStorageWorkBudget(pipelineBulkBytes, pipelineBulkItems),
		proposalBudget:        newStorageWorkBudget(pipelineBulkBytes, pipelineBulkItems),
		bulkIngressBudget:     newStorageWorkBudget(pipelineBulkBytes, pipelineBulkItems),
		priorityIngressBudget: newStorageWorkBudget(pipelineControlBytes, pipelineControlItems),
		outboundBulk:          newStorageWorkBudget(pipelineBulkBytes, pipelineBulkItems),
		outboundControl:       newStorageWorkBudget(pipelineControlBytes, pipelineControlItems),
		// One owner-held Ready is charged while routing it. On a terminal
		// routing failure it stays retained until RawNode disposal and joins.
		readyBudget:                newStorageWorkBudget(pipelineBulkBytes, 1),
		bulk:                       make(chan pipelineIngress, pipelineBulkItems),
		priority:                   make(chan pipelineIngress, pipelineControlItems),
		configurationBudget:        newStorageWorkBudget(pipelineControlBytes, pipelineControlItems),
		configurationHistoryBudget: newStorageWorkBudget(pipelineControlBytes, pipelineControlItems),
		snapshotResources:          resources,
		snapshotBudget:             newStorageWorkBudget(pipelineBulkBytes, pipelineActorQueue),
		snapshotIngressBudget:      newStorageWorkBudget(pipelineBulkBytes, pipelineActorQueue),
		snapshotPins:               make(map[*pipelineSnapshotPlan]uint64),
	}
	return p, nil
}

func (e *Engine) reserveProposalPayload(bytes uint64) (*storageWorkPermit, error) {
	if e.storagePipeline == nil {
		return nil, nil
	}
	return e.reservePipelineProposal(bytes)
}

// reservePipelineProposal precedes caller copies. The caller owns this permit
// through its return; the unstable ledger separately owns RawNode references.
func (e *Engine) reservePipelineProposal(bytes uint64) (*storageWorkPermit, error) {
	p := e.storagePipeline
	if p.stopping.Load() || e.isClosing() {
		return nil, e.currentErrorOrClosed()
	}
	if p.bulkPaused.Load() {
		return nil, errors.WithStack(errStorageWorkBackpressure)
	}
	if bytes > (math.MaxUint64-pipelineTaskOverhead)/3 {
		return nil, errors.WithStack(errStorageWorkInvalidReservation)
	}
	return p.proposalBudget.tryAcquire(bytes*3 + pipelineTaskOverhead)
}

// enqueuePipelineStep admits immutable transport-owned input, not authenticated
// local completion tokens. Neither a full queue nor a byte limit waits on disk.
func (e *Engine) enqueuePipelineStep(ctx context.Context, message raftpb.Message) error {
	if err := contextErr(ctx); err != nil {
		return err
	}
	if err := validatePipelineIngress(&message); err != nil {
		return err
	}
	p := e.storagePipeline
	p.ingressMu.Lock()
	defer p.ingressMu.Unlock()
	if p.stopping.Load() || e.isClosing() {
		return e.currentErrorOrClosed()
	}
	return e.admitPipelineStep(message)
}

func validatePipelineIngress(message *raftpb.Message) error {
	if etcdraft.IsLocalMsg(message.GetType()) || etcdraft.IsLocalMsgTarget(message.GetFrom()) ||
		etcdraft.IsLocalMsgTarget(message.GetTo()) || len(message.Responses) > 0 {
		return errors.WithStack(etcdraft.ErrStepLocalMsg)
	}
	return validatePipelineSnapshotIngress(message)
}

// Called with ingressMu held, including the stop-admission check.
func (e *Engine) admitPipelineStep(message raftpb.Message) error {
	p := e.storagePipeline
	budget, ch := p.bulkIngressBudget, p.bulk
	// Append acknowledgments are control input, not bulk log payload. They
	// must remain pumpable while data/deferred admission is paused.
	if isInboundPriorityMsg(message.GetType()) || message.GetType() == raftpb.MsgAppResp {
		budget, ch = p.priorityIngressBudget, p.priority
	} else if p.bulkPaused.Load() {
		e.stepQueueFullCount.Add(1)
		return errors.WithStack(errStepQueueFull)
	}
	if message.GetType() == raftpb.MsgSnap {
		budget = p.snapshotIngressBudget
	}
	permit, err := budget.tryAcquire(pipelineMessageBytes(&message))
	if err == nil {
		detachPipelineMessage(&message)
		lease, err := p.snapshotIngressLease(&message)
		if err != nil {
			permit.Release()
			return err
		}
		select {
		case ch <- pipelineIngress{message: message, permit: permit, lease: lease}:
			return nil
		default:
			p.snapshotResources.forget(lease)
			permit.Release()
		}
	}
	e.stepQueueFullCount.Add(1)
	return errors.WithStack(errStepQueueFull)
}

func (p *engineStoragePipeline) firstFailure(err error) error {
	p.failureMu.Lock()
	defer p.failureMu.Unlock()
	if p.failure == nil && err != nil {
		p.failure = err
	}
	return p.failure
}

// All opaque local responses are routed here, in their original order. Each
// self Step is folded before another mutation or a following remote response.
func (e *Engine) routePipelineResponses(messages []*raftpb.Message) error {
	for _, message := range messages {
		if failure := e.storagePipeline.firstFailure(nil); failure != nil {
			return failure
		}
		if message == nil {
			return errors.WithStack(errInvalidStorageWork)
		}
		if message.GetTo() == e.nodeID {
			if err := e.stepPipelineResponse(message); err != nil {
				return err
			}
		} else if err := e.sendPipelineMessage(message); err != nil {
			return err
		}
	}
	return nil
}

func (e *Engine) stepPipelineResponse(message *raftpb.Message) error {
	term := e.rawNode.BasicStatus().GetTerm()
	if err := e.rawNode.Step(message); err != nil {
		return errors.WithStack(err)
	}
	if message.GetType() == raftpb.MsgStorageAppendResp {
		e.storagePipeline.ledger.stabilize(message, term)
	}
	return e.foldPipelineReady()
}

func (e *Engine) sendPipelineMessage(message *raftpb.Message) error {
	if e.skipDispatchMessage(*message) {
		return nil
	}
	return e.enqueueDispatchMessage(*message)
}

func pipelineMessageBytes(message *raftpb.Message) uint64 {
	// Include detached pointer slices, input/receipt descriptors and task state,
	// not just serialized payload. Payload backing retention is also ledgered.
	size := proto.Size(message)
	if size < 0 {
		return math.MaxUint64
	}
	headers := pipelineMessageHeaders(message)
	if uint64(size) > math.MaxUint64-headers {
		return math.MaxUint64
	}
	return uint64(size) + headers
}

func pipelineMessageHeaders(message *raftpb.Message) uint64 {
	size := uint64(pipelineTaskOverhead) + uint64(len(message.Entries))*pipelineEntryOverhead + uint64(len(message.Responses))*pipelineResponseOverhead
	for _, response := range message.Responses {
		if response != nil {
			size += pipelineMessageHeaders(response)
		}
	}
	return size
}

func (e *Engine) foldPipelineReady() error {
	p := e.storagePipeline
	for e.rawNode.HasReady() {
		rd := e.rawNode.Ready()
		// Ready has already accepted upstream messages. Keep their ownership
		// even if sizing or reservation fails before routing can begin.
		p.failedReady = &rd
		var size uint64 = pipelineTaskOverhead
		for _, message := range rd.Messages {
			bytes := pipelineMessageBytes(message)
			if bytes > math.MaxUint64-size {
				return errors.Wrap(errStoragePipelineResource, "atomic Ready size overflow")
			}
			size += bytes
		}
		permit, err := p.readyBudget.tryAcquire(size)
		if err != nil {
			// Upstream has no append-Ready size bound. Full ingress/atomic
			// overshoot admission is still an activation gate for this runtime.
			return errors.Wrap(errStoragePipelineResource, "atomic Ready exceeds prototype routing headroom")
		}
		p.failedReadyPermit = permit
		if err := e.routePipelineReady(rd); err != nil {
			return err
		}
		p.failedReady, p.failedReadyPermit = nil, nil
		permit.Release()
	}
	return nil
}

func (e *Engine) routePipelineReady(rd etcdraft.Ready) error {
	for _, message := range rd.Messages {
		if err := e.routePipelineReadyMessage(message); err != nil {
			return err
		}
	}
	e.handlePipelineReadStates(rd.ReadStates)
	return nil
}

func (e *Engine) routePipelineReadyMessage(message *raftpb.Message) error {
	if message == nil {
		return errors.WithStack(errInvalidStorageWork)
	}
	if message.GetType() == raftpb.MsgStorageAppend {
		return e.queuePipelineAppend(message)
	}
	if message.GetType() == raftpb.MsgStorageApply {
		return e.queuePipelineApply(message)
	}
	if message.GetTo() == e.nodeID {
		return errors.Wrap(errInvalidStorageWork, "unexpected Ready self response")
	}
	return e.sendPipelineMessage(message)
}

func (e *Engine) handlePipelineReadStates(states []etcdraft.ReadState) {
	// Even VerifyLeader's read receipt waits for the actually applied prefix.
	for _, state := range states {
		if id, ok := decodeReadContext(state.RequestCtx); ok {
			if req, found := e.pendingRead(id); found {
				req.target, req.waitApplied = state.Index, true
				e.storePendingRead(req)
			}
		}
	}
}

func detachPipelineMessage(message *raftpb.Message) {
	// Detach pointer arrays without copying user data. Task lifetime must not
	// pin an unrelated unstable suffix through an upstream slice's capacity.
	message.Entries = append([]*raftpb.Entry(nil), message.Entries...)
	message.Responses = append([]*raftpb.Message(nil), message.Responses...)
	for _, response := range message.Responses {
		if response != nil {
			detachPipelineMessage(response)
		}
	}
}

func (e *Engine) queuePipelineAppend(message *raftpb.Message) error {
	p := e.storagePipeline
	if !etcdraft.IsEmptySnap(message.Snapshot) {
		return e.queuePipelineReceivedSave(message)
	}
	if err := p.ledger.retain(message); err != nil {
		return errors.Wrap(errStoragePipelineResource, err.Error())
	}
	budget := p.diskBulk
	if len(message.Entries) == 0 {
		budget = p.diskControl
	}
	permit, err := budget.tryAcquire(pipelineMessageBytes(message))
	if err != nil {
		return errors.Wrap(errStoragePipelineResource, err.Error())
	}
	detachPipelineMessage(message)
	work, err := asyncStorageAppendWork(p.persist, message)
	if err != nil {
		permit.Release()
		return err
	}
	task := &pipelineTask{permit: permit, message: message}
	run := work.run
	work.run = func() (any, error) {
		if err := p.firstFailure(nil); err != nil {
			return nil, err
		}
		value, err := runStorageWork(run)
		if err != nil {
			err = p.firstFailure(err)
		}
		return value, err
	}
	work.token, work.retainedBytes = task, pipelineMessageBytes(message)
	task.work = work
	p.diskIssued++
	task.diskSequence = p.diskIssued
	p.diskDeferred = append(p.diskDeferred, task)
	p.diskOutstanding++
	return nil
}

func (e *Engine) queuePipelineApply(message *raftpb.Message) error {
	p := e.storagePipeline
	if message.GetTo() != etcdraft.LocalApplyThread || !etcdraft.IsEmptySnap(message.Snapshot) {
		return errors.WithStack(errInvalidStorageWork)
	}
	permit, err := p.applyBudget.tryAcquire(pipelineMessageBytes(message))
	if err != nil {
		return errors.Wrap(errStoragePipelineResource, err.Error())
	}
	detachPipelineMessage(message)
	task := &pipelineTask{permit: permit, message: message, inputs: make([]normalApplyInput, len(message.Entries)), diskFence: p.diskIssued}
	task.configEnv = pipelineConfigurationEnvironment{fsm: e.fsm, dataDir: e.dataDir, fsmSnapDir: e.fsmSnapDir, maxWALFiles: e.maxWALFiles}
	for i, entry := range message.Entries {
		task.inputs[i] = e.prepareNormalApply(*entry, false)
	}
	task.work = storageWork{token: task, retainedBytes: pipelineMessageBytes(message), run: func() (any, error) {
		_, err := runStorageWork(func() (any, error) {
			return nil, p.executePipelineApply(task)
		})
		if err != nil {
			err = p.firstFailure(err)
		}
		return nil, err
	}}
	p.applyDeferred = append(p.applyDeferred, task)
	p.applyOutstanding++
	return nil
}

func (p *engineStoragePipeline) executePipelineApply(task *pipelineTask) error {
	for i, input := range task.inputs {
		if err := p.firstFailure(nil); err != nil {
			return err
		}
		if input.index <= p.receivedApplyFloor {
			// Unlike cold-start volatile replay, a live received snapshot has
			// already restored this whole prefix, including volatile state.
			continue
		}
		if task.message.Entries[i].GetType() != raftpb.EntryNormal {
			if err := p.executePipelineConfiguration(task, task.message.Entries[i]); err != nil {
				return err
			}
			continue
		}
		input.duplicate = input.index <= p.applyCursor
		receipt := executeNormalApply(input)
		if receipt.err != nil {
			return receipt.err
		}
		task.receipts = append(task.receipts, receipt)
		if input.index > p.applyCursor {
			p.applyCursor = input.index
		}
	}
	return nil
}

func flushPipelineTasks(worker *storageWorker, queue *[]*pipelineTask, budget *storageWorkBudget, fence uint64) error {
	for len(*queue) > 0 {
		task := (*queue)[0]
		if task.diskFence > fence {
			return nil
		}
		if err := worker.trySubmit(task.work, budget); err != nil {
			if errors.Is(err, errStorageWorkBackpressure) {
				return nil
			}
			return err
		}
		(*queue)[0] = nil
		*queue = (*queue)[1:]
	}
	return nil
}

func (p *engineStoragePipeline) flush() error {
	if err := flushPipelineTasks(p.disk, &p.diskDeferred, p.diskQueueBudget, math.MaxUint64); err != nil {
		return err
	}
	return flushPipelineTasks(p.apply, &p.applyDeferred, p.applyQueueBudget, p.diskRetired)
}

func (task *pipelineTask) retire() {
	permit := task.permit
	for _, configPermit := range task.configPermits {
		configPermit.Release()
	}
	*task = pipelineTask{}
	permit.Release()
}

func (e *Engine) completePipelineDisk(completion storageWorkCompletion) error {
	p := e.storagePipeline
	task, ok := completion.token.(*pipelineTask)
	if !ok || task == nil {
		return errors.WithStack(errInvalidStorageWork)
	}
	if task.snapshotPlan != nil {
		defer completion.Release()
		return e.completePipelineSnapshotDisk(task, completion.err)
	}
	if p.receiving != nil && task.configDisk == nil && completion.err == nil {
		p.heldSnapshotDisk = append(p.heldSnapshotDisk, completion)
		p.diskRetired = maxUint64(p.diskRetired, task.diskSequence)
		return nil
	}
	defer completion.Release()
	defer task.retire()
	p.diskOutstanding--
	p.diskRetired = maxUint64(p.diskRetired, task.diskSequence)
	return e.publishPipelineAppend(task, completion.err)
}

func (e *Engine) publishPipelineAppend(task *pipelineTask, err error) error {
	p := e.storagePipeline
	if task.configDisk != nil {
		return e.completePipelineConfigurationDisk(task.configDisk, err)
	}
	if err != nil {
		return err
	}
	if failure := p.firstFailure(nil); failure != nil {
		return failure
	}
	message := task.message
	// MemoryStorage publication is owner-only and follows real Save/Sync.
	if err := e.storage.Append(message.Entries); err != nil {
		return errors.WithStack(err)
	}
	hs := &raftpb.HardState{Term: message.Term, Vote: message.Vote, Commit: message.Commit}
	if err := e.applyReadyHardState(hs); err != nil {
		return err
	}
	return e.routePipelineResponses(message.Responses)
}

func (e *Engine) completePipelineApply(completion storageWorkCompletion) error {
	p := e.storagePipeline
	defer completion.Release()
	task, ok := completion.token.(*pipelineTask)
	if !ok || task == nil {
		return errors.WithStack(errInvalidStorageWork)
	}
	defer task.retire()
	p.applyOutstanding--
	if task.snapshotPlan != nil {
		return e.completePipelineSnapshotApply(task, completion.err)
	}
	if err := e.publishPipelineReceipts(task.receipts); err != nil {
		return err
	}
	if completion.err != nil {
		return completion.err
	}
	if task.startup {
		p.startupDone = true
		return nil
	}
	if p.firstFailure(nil) != nil {
		return nil
	}
	return e.routePipelineApplyResponses(task.message.Responses)
}

func (e *Engine) publishPipelineReceipts(receipts []normalApplyReceipt) error {
	p := e.storagePipeline
	// Preserve completed prefix verdicts even if this task (or the other actor)
	// subsequently failed. The failed entry never has a successful receipt.
	for _, receipt := range receipts {
		if p.quiescing && receipt.index > p.shutdownCommit && !receipt.duplicate {
			// Drain real FSM effects, but shutdown did not confirm the caller's
			// outcome when this entry only became committed during quiescence.
			e.setApplied(receipt.index)
			continue
		}
		if err := e.publishNormalApply(receipt); err != nil {
			return err
		}
	}
	return nil
}

func (e *Engine) stepPipelineOwner(message *raftpb.Message) error {
	if message.GetType() == raftpb.MsgSnap || !etcdraft.IsEmptySnap(message.Snapshot) {
		return e.queuePipelineReceivedFront(message)
	}
	e.recordLeaderContact(*message)
	if err := e.rawNode.Step(message); err != nil {
		if errors.Is(err, etcdraft.ErrStepPeerNotFound) {
			return nil
		}
		return errors.WithStack(err)
	}
	e.recordQuorumAck(*message)
	return nil
}

// startupPipeline is invoked only after replay's real actor receipts and all
// resulting self responses have drained. No owner-side durable FSM call occurs.
func (e *Engine) startupPipeline() error {
	p := e.storagePipeline
	if !p.campaignChecked {
		p.campaignChecked = true
		if e.rawNode.BasicStatus().RaftState != etcdraft.StateLeader && e.shouldCampaignSingleNode() {
			return errors.WithStack(e.rawNode.Campaign())
		}
	}
	if !p.startupQueued {
		return e.queuePipelineStartup()
	} else if p.startupDone {
		p.started = true
		e.markStarted()
	}
	return nil
}

func (e *Engine) queuePipelineStartup() error {
	p := e.storagePipeline
	permit, err := p.applyBudget.tryAcquire(pipelineTaskOverhead)
	if err != nil {
		return errors.Wrap(errStoragePipelineResource, "startup receipt reservation")
	}
	fsm, index := e.fsm, e.applied
	task := &pipelineTask{permit: permit, startup: true, diskFence: p.diskIssued}
	task.work = storageWork{token: task, retainedBytes: pipelineTaskOverhead, run: func() (any, error) {
		return nil, p.runPipelineStartup(fsm, index)
	}}
	p.applyDeferred = append(p.applyDeferred, task)
	p.applyOutstanding++
	p.startupQueued = true
	return nil
}

func (p *engineStoragePipeline) runPipelineStartup(fsm StateMachine, index uint64) error {
	if err := p.firstFailure(nil); err != nil {
		return err
	}
	_, err := runStorageWork(func() (any, error) {
		if writer, ok := fsm.(raftengine.AppliedIndexWriter); ok && index != 0 {
			return nil, writer.SetDurableAppliedIndex(index)
		}
		return nil, nil
	})
	if err != nil {
		return p.firstFailure(err)
	}
	return nil
}

func (e *Engine) beginPipelineShutdown(err error) {
	p := e.storagePipeline
	if !p.quiescing {
		p.shutdownCommit = e.rawNode.BasicStatus().GetCommit()
	}
	if err != nil {
		err = p.firstFailure(err)
	}
	p.ingressMu.Lock()
	p.stopping.Store(true)
	p.ingressMu.Unlock()
	p.bulkPaused.Store(true)
	p.quiescing = true
	e.publishShutdown(err)
	// A failure after graceful authority revocation still needs its first cause.
	if err != nil {
		e.mu.Lock()
		if e.runErr == nil {
			e.runErr = err
		}
		e.mu.Unlock()
	}
}

func retirePipelineDeferred(queue *[]*pipelineTask, outstanding *int) {
	for _, task := range *queue {
		if task.configDisk != nil {
			task.configDisk.reply <- pipelineConfigurationReply{err: errStorageWorkerClosed}
		}
		if task.snapshotPlan != nil && (task.snapshotStage == pipelineReceivedSave || task.snapshotStage == pipelineReceivedRelease) {
			task.snapshotPlan.frontReply <- errStorageWorkerClosed
		}
		task.retire()
		*outstanding--
	}
	*queue = nil
}

func (p *engineStoragePipeline) stopActorsOnFailure() {
	retirePipelineDeferred(&p.diskDeferred, &p.diskOutstanding)
	retirePipelineDeferred(&p.applyDeferred, &p.applyOutstanding)
	p.disk.quiesce()
	p.apply.quiesce()
	p.actorsQuiesced = true
}

func (p *engineStoragePipeline) updateAdmission() {
	diskBytes, diskItems := p.diskBulk.usage()
	applyBytes, applyItems := p.applyBudget.usage()
	p.bulkPaused.Store(p.quiescing || !p.started || len(p.diskDeferred) > 0 || len(p.applyDeferred) > 0 ||
		diskBytes > pipelineBulkBytes-pipelineEntryRoom || applyBytes > pipelineBulkBytes-pipelineEntryRoom ||
		diskItems >= pipelineBulkItems-1 || applyItems >= pipelineBulkItems-1)
}

func (p *engineStoragePipeline) retireIngress() {
	for _, ch := range []chan pipelineIngress{p.bulk, p.priority} {
		for {
			select {
			case input := <-ch:
				input.permit.Release()
			default:
				goto next
			}
		}
	next:
	}
}

// runStoragePipeline never calls Advance, fail, shutdown or a synchronous
// snapshot helper. The outer Engine.run owns doneCh. Even on fatal shutdown,
// storage is not closed until both actors deliver all actual completions.
func (e *Engine) runStoragePipeline() {
	p := e.storagePipeline
	p.applyCursor = e.applied
	e.initPipelineSnapshotFiles()
	ticker := time.NewTicker(e.tickInterval)
	defer ticker.Stop()
	completions := pipelineCompletionStreams{disk: p.disk.completions, apply: p.apply.completions}
	for {
		again, err := e.preparePipelineTurn()
		if err != nil {
			e.beginPipelineShutdown(err)
			continue
		}
		if again {
			continue
		}
		if completions.disk == nil && completions.apply == nil {
			e.finishStoragePipeline()
			return
		}
		p.updateAdmission()
		if !p.quiescing {
			e.refreshStatus()
			e.resolveReadyReads()
			e.handlePipelineAdmin()
		}
		if err := e.selectPipelineEvent(ticker.C, &completions); err != nil {
			e.beginPipelineShutdown(err)
		}
	}
}

func (e *Engine) preparePipelineTurn() (bool, error) {
	p := e.storagePipeline
	if failure := p.firstFailure(nil); failure != nil {
		e.beginPipelineShutdown(failure)
		if !p.actorsQuiesced {
			e.retireHeldSnapshotDisk()
			p.stopActorsOnFailure()
		}
		return false, nil
	}
	if e.isClosing() && !p.quiescing {
		e.beginPipelineShutdown(nil)
	}
	if err := e.foldPipelineReady(); err != nil {
		return false, err
	}
	return e.progressPipelineActors()
}

func (e *Engine) progressPipelineActors() (bool, error) {
	p := e.storagePipeline
	if p.actorsQuiesced {
		return false, nil
	}
	if err := e.maybeQueuePipelineCheckpoint(); err != nil {
		return false, err
	}
	if err := p.flush(); err != nil {
		return false, err
	}
	if p.diskOutstanding != 0 || p.applyOutstanding != 0 || e.rawNode.HasReady() {
		return false, nil
	}
	if p.quiescing {
		p.disk.quiesce()
		p.apply.quiesce()
		p.actorsQuiesced = true
		return false, nil
	}
	if !p.started {
		return true, e.startupPipeline()
	}
	return false, nil
}

func (e *Engine) finishStoragePipeline() {
	p := e.storagePipeline
	// Closed completion channels precede worker done; join the final epilogue.
	<-p.disk.done
	<-p.apply.done
	e.rawNode = nil
	e.retirePipelineSnapshotPins()
	p.retireConfigurationHistory()
	e.snapshotConfigurations = snapshotConfigurationHistory{}
	p.configuration = nil
	p.failedReady = nil
	p.failedReadyPermit.Release()
	p.failedReadyPermit = nil
	p.ledger.discard()
	p.retireIngress()
	e.finishShutdown(e.currentErrorOrClosed())
}

type pipelineCompletionStreams struct {
	disk, apply <-chan storageWorkCompletion
}

type pipelineInputStreams struct {
	tick           <-chan time.Time
	proposal       <-chan proposalRequest
	read           <-chan readRequest
	bulk, priority <-chan pipelineIngress
	reports        <-chan dispatchReport
	close          <-chan struct{}
}

func (e *Engine) pipelineInputs(tick <-chan time.Time) pipelineInputStreams {
	p := e.storagePipeline
	var inputs pipelineInputStreams
	if p.quiescing {
		return inputs
	}
	inputs.close, inputs.priority, inputs.reports = e.closeCh, p.priority, e.dispatchReportCh
	if p.started {
		inputs.tick, inputs.read = tick, e.readCh
	}
	if !p.bulkPaused.Load() {
		inputs.proposal, inputs.bulk = e.proposeCh, p.bulk
	}
	return inputs
}

func (e *Engine) selectPipelineEvent(tick <-chan time.Time, completions *pipelineCompletionStreams) error {
	inputs := e.pipelineInputs(tick)
	select {
	case <-inputs.close:
		e.beginPipelineShutdown(nil)
	case <-inputs.tick:
		e.tickPipeline()
	case request := <-inputs.proposal:
		e.handlePipelineProposal(request)
	case request := <-inputs.read:
		e.handlePipelineRead(request)
	case input := <-inputs.priority:
		return e.consumePipelineIngress(input)
	case input := <-inputs.bulk:
		return e.consumePipelineIngress(input)
	case report := <-inputs.reports:
		e.reportPipelineDispatch(report)
	case completion, ok := <-completions.disk:
		return e.consumePipelineDisk(completion, ok, completions)
	case completion, ok := <-completions.apply:
		return e.consumePipelineApply(completion, ok, completions)
	}
	return nil
}

func (e *Engine) handlePipelineAdmin() {
	select {
	case request := <-e.adminCh:
		if e.configurationTransition.Load() {
			request.done <- adminResult{err: errors.WithStack(errMembershipConfChangePending)}
		} else {
			e.handleAdmin(request)
		}
	default:
	}
}

func (e *Engine) tickPipeline() {
	if e.storagePipeline.firstFailure(nil) == nil {
		e.storagePipeline.checkpointAttempt = 0
		e.rawNode.Tick()
	}
}

func (e *Engine) reportPipelineDispatch(report dispatchReport) {
	if e.storagePipeline.firstFailure(nil) == nil {
		e.handleDispatchReport(report)
	}
}

func (e *Engine) consumePipelineDisk(completion storageWorkCompletion, open bool, streams *pipelineCompletionStreams) error {
	if !open {
		streams.disk = nil
		return nil
	}
	if _, ok := completion.token.(pipelineSnapshotFinalization); ok {
		return completion.err
	}
	return e.completePipelineDisk(completion)
}

func (e *Engine) consumePipelineApply(completion storageWorkCompletion, open bool, streams *pipelineCompletionStreams) error {
	if !open {
		streams.apply = nil
		return nil
	}
	if request, ok := completion.token.(*pipelineSnapshotRequest); ok {
		return e.handlePipelineSnapshotRequest(request)
	}
	if request, ok := completion.token.(*pipelineConfigurationRequest); ok {
		return e.handlePipelineConfiguration(*request)
	}
	return e.completePipelineApply(completion)
}

func (e *Engine) handlePipelineProposal(request proposalRequest) {
	if failure := e.storagePipeline.firstFailure(nil); failure != nil {
		request.done <- proposalResult{err: failure}
		return
	}
	e.handleProposal(request)
}

func (e *Engine) handlePipelineRead(request readRequest) {
	if failure := e.storagePipeline.firstFailure(nil); failure != nil {
		request.done <- readResult{err: failure}
		return
	}
	e.handleRead(request)
}

func (e *Engine) consumePipelineIngress(input pipelineIngress) error {
	defer input.permit.Release()
	if err := e.storagePipeline.firstFailure(nil); err != nil {
		return err
	}
	var err error
	if input.lease != nil {
		err = e.queuePipelineReceivedFrontWithLease(&input.message, input.lease)
	} else {
		err = e.stepPipelineOwner(&input.message)
	}
	if err != nil {
		return err
	}
	// Transfer ownership to emitted work/unstable before ingress retirement.
	return e.foldPipelineReady()
}
