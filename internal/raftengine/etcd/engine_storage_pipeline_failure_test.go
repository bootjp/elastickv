package etcd

import (
	"bytes"
	"context"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/bootjp/elastickv/internal/testutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
	etcdstorage "go.etcd.io/etcd/server/v3/storage"
	etcdraft "go.etcd.io/raft/v3"
	raftpb "go.etcd.io/raft/v3/raftpb"
)

// Faults are armed only by named later user payloads. Every other operation,
// including election/baseline durability and real WAL Close, still delegates.
type pipelineFailureStorage struct {
	etcdstorage.Storage
	payload              []byte
	saveError, syncError error
	closeError           error
	entered              chan uint64
	release              chan struct{}
	returned             chan struct{}
	once                 sync.Once
	syncIndex            uint64 // Disk actor only.
	writes               map[string]chan uint64
	pipeline             *engineStoragePipeline
	active               atomic.Bool
	closeCalls           atomic.Int32
	closedBeforeActors   atomic.Bool
	closedDuringIO       atomic.Bool
}

func pipelineFailureEntryIndex(entries []*raftpb.Entry, payload []byte) uint64 {
	for _, entry := range entries {
		_, data, ok := decodeProposalEnvelope(entry.GetData())
		if ok && bytes.Equal(data, payload) {
			return entry.GetIndex()
		}
	}
	return 0
}

func (s *pipelineFailureStorage) Save(hs *raftpb.HardState, entries []*raftpb.Entry) error {
	index := pipelineFailureEntryIndex(entries, s.payload)
	if index != 0 && s.saveError != nil {
		return s.fail(index, s.saveError)
	}
	if err := s.Storage.Save(hs, entries); err != nil {
		return err
	}
	if index != 0 {
		s.syncIndex = index
	}
	s.notifySaved(entries)
	return nil
}

func (s *pipelineFailureStorage) notifySaved(entries []*raftpb.Entry) {
	for _, entry := range entries {
		_, payload, ok := decodeProposalEnvelope(entry.GetData())
		if ch := s.writes[string(payload)]; ok && ch != nil {
			ch <- entry.GetIndex()
		}
	}
}

func (s *pipelineFailureStorage) Sync() error {
	if s.syncIndex != 0 && s.syncError != nil {
		return s.fail(s.syncIndex, s.syncError)
	}
	return s.Storage.Sync()
}

func (s *pipelineFailureStorage) fail(index uint64, err error) error {
	s.active.Store(true)
	defer s.active.Store(false)
	defer close(s.returned)
	s.entered <- index
	<-s.release
	return err
}

func pipelineFailureChannelClosed(ch <-chan struct{}) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
}

func (s *pipelineFailureStorage) Close() error {
	s.closeCalls.Add(1)
	if !pipelineFailureChannelClosed(s.pipeline.disk.done) || !pipelineFailureChannelClosed(s.pipeline.apply.done) {
		s.closedBeforeActors.Store(true)
	}
	if s.active.Load() {
		s.closedDuringIO.Store(true)
	}
	if err := s.Storage.Close(); err != nil {
		return err
	}
	return s.closeError
}

func (s *pipelineFailureStorage) unblock() {
	s.once.Do(func() { close(s.release) })
}

type pipelineFailureHalt struct{ err error }

func (h pipelineFailureHalt) HaltApply() error { return h.err }

type pipelineFailureFSM struct {
	*asyncStorageFSM
	holdPayload []byte
	failPayload []byte
	failError   error
	panicApply  bool
	entered     chan uint64
	release     chan struct{}
	once        sync.Once
	active      atomic.Bool
	mu          sync.Mutex
	attempts    []string
}

func (f *pipelineFailureFSM) Apply(payload []byte) any {
	f.mu.Lock()
	f.attempts = append(f.attempts, string(payload))
	f.mu.Unlock()
	if bytes.Equal(payload, f.holdPayload) {
		f.active.Store(true)
		defer f.active.Store(false)
		f.entered <- f.applyIndex.Load()
		<-f.release
	}
	if bytes.Equal(payload, f.failPayload) {
		if f.panicApply {
			panic(f.failError)
		}
		return pipelineFailureHalt{err: f.failError}
	}
	return f.asyncStorageFSM.Apply(payload)
}

func (f *pipelineFailureFSM) attempted() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.attempts...)
}

func (f *pipelineFailureFSM) unblock() {
	f.once.Do(func() { close(f.release) })
}

func newPipelineFailureRuntime(t *testing.T, disk *pipelineFailureStorage, fsm *pipelineFailureFSM,
	firstError error,
) (*asyncStorageRuntime, *asyncStorageSaveGate) {
	t.Helper()
	gate := &asyncStorageSaveGate{expectedCloseError: firstError}
	h := newNativeStorageRuntime(t, fsm.asyncStorageFSM, gate, newStorageRawNode, func(e *Engine) {
		disk.Storage = gate
		pipeline, err := newEngineStoragePipeline(disk)
		require.NoError(t, err)
		disk.pipeline = pipeline
		e.persist, e.fsm, e.storagePipeline = disk, fsm, pipeline
	})
	// Client lifetime must not share dispatchCancel: fatal teardown cancels
	// transport before publishing the original error to pending clients.
	clientCtx, cancelClient := context.WithCancel(context.Background())
	h.ctx = clientCtx
	t.Cleanup(func() {
		disk.unblock()
		fsm.unblock()
		cancelClient()
	})
	return h, gate
}

func newPipelineFailureDisk(payload []byte) *pipelineFailureStorage {
	return &pipelineFailureStorage{
		payload: payload, entered: make(chan uint64, 1), release: make(chan struct{}), returned: make(chan struct{}),
	}
}

func newPipelineFailureFSM(payload []byte) *pipelineFailureFSM {
	return &pipelineFailureFSM{
		asyncStorageFSM: &asyncStorageFSM{}, holdPayload: payload,
		entered: make(chan uint64, 1), release: make(chan struct{}),
	}
}

func pipelineFailureInvalidation(h *asyncStorageRuntime) <-chan struct{} {
	invalidated := make(chan struct{})
	var once sync.Once
	h.engine.RegisterLeaderLossCallback(func() { once.Do(func() { close(invalidated) }) })
	return invalidated
}

func requirePipelineFailureQuiescing(t *testing.T, h *asyncStorageRuntime, disk *pipelineFailureStorage,
	invalidated <-chan struct{}, firstError error,
) {
	t.Helper()
	testutil.Closed(t, invalidated, time.Second, "fatal error did not revoke authority while the other actor was held")
	require.Equal(t, raftengine.StateShutdown, h.engine.State())
	require.True(t, h.engine.LastQuorumAck().IsZero())
	require.ErrorIs(t, h.engine.Err(), firstError)
	require.ErrorIs(t, h.engine.storagePipeline.firstFailure(nil), firstError)
	require.Zero(t, disk.closeCalls.Load(), "storage Close preceded the executing actor's return")
	requireAsyncStorageSignalPending(t, h.engine.Done(), "Engine shutdown with another actor held")
}

func awaitPipelineFailureProposal(t *testing.T, done <-chan proposalResult) proposalResult {
	t.Helper()
	select {
	case result := <-done:
		return result
	case <-time.After(time.Second):
		t.Fatal("proposal caller did not receive its actual runtime verdict")
		return proposalResult{}
	}
}

func requirePipelineFailureProposal(t *testing.T, done <-chan proposalResult, firstError error) {
	t.Helper()
	result := awaitPipelineFailureProposal(t, done)
	require.ErrorIs(t, result.err, firstError)
	require.Nil(t, result.result, "failed entry received a fabricated successful receipt")
}

func requirePipelineFailureBudgetHeld(t *testing.T, budget *storageWorkBudget) {
	t.Helper()
	bytes, items := budget.usage()
	require.Positive(t, bytes, "executing ownership lost its byte reservation before the real return")
	require.Positive(t, items, "executing ownership lost its item reservation before the real return")
}

func requirePipelineFailureRetired(t *testing.T, h *asyncStorageRuntime, disk *pipelineFailureStorage, firstError error) {
	t.Helper()
	testutil.Closed(t, h.engine.Done(), time.Second, "fatal owner did not join after real operations returned")
	h.proposals.Wait()
	p := h.engine.storagePipeline
	testutil.Closed(t, p.disk.done, time.Second, "disk actor was not joined")
	testutil.Closed(t, p.apply.done, time.Second, "apply actor was not joined")
	testutil.Closed(t, disk.returned, time.Second, "the real faulting disk operation did not return")
	require.Equal(t, int32(1), disk.closeCalls.Load())
	require.False(t, disk.closedBeforeActors.Load(), "storage Close was called before both actors completed")
	require.False(t, disk.closedDuringIO.Load())
	require.ErrorIs(t, h.engine.Err(), firstError)
	require.ErrorIs(t, h.engine.Close(), firstError, "Close must preserve the original fatal cause")
	require.ErrorIs(t, p.firstFailure(nil), firstError)
	require.Nil(t, h.engine.rawNode)
	require.Zero(t, p.diskOutstanding)
	require.Zero(t, p.applyOutstanding)
	require.Empty(t, p.diskDeferred)
	require.Empty(t, p.applyDeferred)
	require.Empty(t, p.bulk)
	require.Empty(t, p.priority)
	require.Nil(t, p.failedReady)
	require.Nil(t, p.failedReadyPermit)
	require.Empty(t, p.ledger.entries)
	for name, budget := range map[string]*storageWorkBudget{
		"disk bulk": p.diskBulk, "disk control": p.diskControl, "apply": p.applyBudget,
		"disk queue": p.diskQueueBudget, "apply queue": p.applyQueueBudget, "proposal": p.proposalBudget,
		"bulk ingress": p.bulkIngressBudget, "control ingress": p.priorityIngressBudget,
		"Ready": p.readyBudget, "unstable": p.ledger.budget,
	} {
		bytes, items := budget.usage()
		require.Zero(t, bytes, "%s bytes were not retired", name)
		require.Zero(t, items, "%s items were not retired", name)
	}
}

func TestEngineExplicitAsyncStoragePipelineDiskFatalWaitsForActualApply(t *testing.T) {
	for _, operation := range []string{"Save", "required Sync"} {
		t.Run(operation, func(t *testing.T) {
			runPipelineDiskFatal(t, operation == "required Sync")
		})
	}
}

func runPipelineDiskFatal(t *testing.T, syncFailure bool) {
	t.Helper()
	baseline, prefix, failed := []byte("baseline"), []byte("inflight-prefix"), []byte("disk-failure")
	firstError, closeError := errors.New("first disk failure"), errors.New("later storage close failure")
	disk, fsm := newPipelineFailureDisk(failed), newPipelineFailureFSM(prefix)
	disk.closeError = closeError
	if syncFailure {
		disk.syncError = firstError
	} else {
		disk.saveError = firstError
	}
	h, gate := newPipelineFailureRuntime(t, disk, fsm, firstError)
	before := establishExplicitAsyncStorageLeader(t, h, fsm.asyncStorageFSM, gate, baseline)
	invalidated := pipelineFailureInvalidation(h)
	goodDone := h.propose(prefix)
	goodIndex := asyncStorageAwait(t, h, fsm.entered, "committed prefix held in actual FSM Apply")
	require.Greater(t, goodIndex, before)
	require.GreaterOrEqual(t, gate.commit.Load(), goodIndex)
	badDone := h.propose(failed)
	badIndex := asyncStorageAwait(t, h, disk.entered, "later user Save/required Sync fault")
	require.Greater(t, badIndex, goodIndex)
	requireAsyncStorageProposalPending(t, goodDone)
	requireAsyncStorageProposalPending(t, badDone)
	require.Equal(t, before, h.engine.AppliedIndex())
	require.Equal(t, [][]byte{baseline}, fsm.Applied())
	if syncFailure {
		require.GreaterOrEqual(t, gate.lastIndex.Load(), badIndex, "required Sync failure must follow real WAL Save")
	} else {
		require.Less(t, gate.lastIndex.Load(), badIndex, "failed Save must not be recorded as saved")
	}
	disk.unblock()
	requirePipelineFailureQuiescing(t, h, disk, invalidated, firstError)
	require.True(t, fsm.active.Load(), "fatal disk error fabricated completion of the held FSM")
	requirePipelineFailureBudgetHeld(t, h.engine.storagePipeline.applyBudget)
	requirePipelineFailureBudgetHeld(t, h.engine.storagePipeline.applyQueueBudget)
	requirePipelineFailureBudgetHeld(t, h.engine.storagePipeline.ledger.budget)
	requireAsyncStorageProposalPending(t, goodDone)
	fsm.unblock()
	good := awaitPipelineFailureProposal(t, goodDone)
	require.NoError(t, good.err)
	require.NotNil(t, good.result)
	require.Equal(t, goodIndex, good.result.CommitIndex)
	require.Equal(t, string(prefix), good.result.Response)
	requirePipelineFailureProposal(t, badDone, firstError)
	requirePipelineFailureRetired(t, h, disk, firstError)
	require.NotErrorIs(t, h.engine.Err(), closeError)
	require.Equal(t, goodIndex, h.engine.AppliedIndex())
	require.Equal(t, [][]byte{baseline, prefix}, fsm.Applied())
	require.Equal(t, []string{string(baseline), string(prefix)}, fsm.attempted())
	t.Logf("real disk fatal preserved successful prefix=%d, failed index=%d had no ACK, both actors joined and all budgets retired", goodIndex, badIndex)
}

// Do not forward follower append ACKs until all three genuine entries are
// present. One real cumulative ACK then commits good/bad/never in one batch.
func pipelineFailureDeliverWithoutAppendAck(t *testing.T, h *asyncStorageRuntime, msg raftpb.Message) *raftpb.Message {
	t.Helper()
	require.Equal(t, uint64(2), msg.GetTo())
	require.NoError(t, h.follower.Step(&msg))
	var latest *raftpb.Message
	for h.follower.HasReady() {
		rd := h.follower.Ready()
		require.True(t, etcdraft.IsEmptySnap(rd.Snapshot))
		require.NoError(t, h.storage.Append(rd.Entries))
		if !etcdraft.IsEmptyHardState(rd.HardState) {
			require.NoError(t, h.storage.SetHardState(rd.HardState))
		}
		for _, response := range rd.Messages {
			if response.GetType() == raftpb.MsgAppResp {
				if latest == nil || response.GetIndex() > latest.GetIndex() {
					latest = response
				}
			} else {
				require.NoError(t, h.engine.handleTransportMessage(h.ctx, *response))
			}
		}
		h.follower.Advance(rd)
	}
	return latest
}

func proposePipelineFailureWithoutFollower(t *testing.T, h *asyncStorageRuntime, payload []byte,
	written <-chan uint64,
) (<-chan proposalResult, uint64) {
	t.Helper()
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	done := h.propose(payload)
	for {
		select {
		case index := <-written:
			return done, index
		case result := <-done:
			// This precise admission error precedes RawNode proposal. Never
			// retry a request with a possibly replicated/unknown outcome.
			require.ErrorIs(t, result.err, errStorageWorkBackpressure)
			require.Nil(t, result.result)
			runtime.Gosched()
			done = h.propose(payload)
		case <-h.engine.Done():
			t.Fatalf("engine stopped before native batch proposal: %v", h.engine.Err())
		case <-deadline.C:
			t.Fatal("native batch proposal did not reach real WAL Save")
		}
	}
}

func commitPipelineFailureBatch(t *testing.T, h *asyncStorageRuntime, tail uint64) {
	t.Helper()
	awaitPipelineFailureLocalPublication(t, h, tail)
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	for {
		select {
		case msg := <-h.outbound:
			ack := pipelineFailureDeliverWithoutAppendAck(t, h, msg)
			if ack != nil && ack.GetIndex() == tail {
				require.False(t, ack.GetReject())
				require.NoError(t, h.engine.handleTransportMessage(h.ctx, *ack))
				return
			}
		case <-h.engine.Done():
			t.Fatalf("engine stopped assembling native batch: %v", h.engine.Err())
		case <-deadline.C:
			t.Fatal("native follower did not acknowledge the actual batch tail")
		}
	}
}

func awaitPipelineFailureLocalPublication(t *testing.T, h *asyncStorageRuntime, tail uint64) {
	t.Helper()
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	for {
		// MemoryStorage is concurrent-safe. Its publication follows real
		// Save/Sync; the same owner completes its local append response
		// before it can consume our next cumulative follower ACK. A Save
		// notification alone would allow a split 2+1 committed apply batch.
		last, err := h.engine.storage.LastIndex()
		require.NoError(t, err)
		if last >= tail {
			return
		}
		select {
		case <-h.engine.Done():
			t.Fatalf("engine stopped before local durable publication: %v", h.engine.Err())
		case <-deadline.C:
			t.Fatal("local owner did not publish the real durable batch tail")
		default:
			runtime.Gosched()
		}
	}
}

func TestEngineExplicitAsyncStoragePipelineApplyFatalPreservesCompletedPrefix(t *testing.T) {
	for _, mode := range []string{"HaltApply", "panic"} {
		t.Run(mode, func(t *testing.T) {
			runPipelineApplyFatal(t, mode == "panic")
		})
	}
}

func runPipelineApplyFatal(t *testing.T, panicApply bool) {
	t.Helper()
	baseline, good, bad, never := []byte("baseline"), []byte("batch-good"), []byte("batch-bad"), []byte("batch-never")
	secondary := []byte("secondary-held-Save")
	fsmError, diskError := errors.New("first FSM failure"), errors.New("later disk failure")
	wantError := fsmError
	if panicApply {
		wantError = errStorageWorkerPanic
	}
	disk, fsm := newPipelineFailureDisk(secondary), newPipelineFailureFSM(good)
	disk.saveError = diskError
	disk.writes = map[string]chan uint64{}
	for _, payload := range [][]byte{good, bad, never} {
		disk.writes[string(payload)] = make(chan uint64, 1)
	}
	fsm.failPayload, fsm.failError, fsm.panicApply = bad, fsmError, panicApply
	h, gate := newPipelineFailureRuntime(t, disk, fsm, wantError)
	before := establishExplicitAsyncStorageLeader(t, h, fsm.asyncStorageFSM, gate, baseline)
	invalidated := pipelineFailureInvalidation(h)
	goodDone, goodIndex := proposePipelineFailureWithoutFollower(t, h, good, disk.writes[string(good)])
	badDone, badIndex := proposePipelineFailureWithoutFollower(t, h, bad, disk.writes[string(bad)])
	neverDone, neverIndex := proposePipelineFailureWithoutFollower(t, h, never, disk.writes[string(never)])
	require.Equal(t, goodIndex+1, badIndex)
	require.Equal(t, badIndex+1, neverIndex)
	commitPipelineFailureBatch(t, h, neverIndex)
	require.Equal(t, goodIndex, asyncStorageAwait(t, h, fsm.entered, "first entry held inside actual batched FSM Apply"))
	require.GreaterOrEqual(t, gate.commit.Load(), neverIndex)
	_, applyItems := h.engine.storagePipeline.applyBudget.usage()
	// This counts outstanding reservations, not the task's actual entry range.
	// It does not by itself prove all three entries share one apply task.
	require.Equal(t, 1, applyItems, "one apply task is held at this observation")
	secondaryDone := h.propose(secondary)
	require.Greater(t, asyncStorageAwait(t, h, disk.entered, "other actor held in actual Save"), neverIndex)
	for _, done := range []<-chan proposalResult{goodDone, badDone, neverDone, secondaryDone} {
		requireAsyncStorageProposalPending(t, done)
	}
	require.Equal(t, before, h.engine.AppliedIndex())
	fsm.unblock()
	requirePipelineFailureQuiescing(t, h, disk, invalidated, wantError)
	require.True(t, disk.active.Load(), "FSM fatal fabricated completion of a real executing Save")
	requirePipelineFailureBudgetHeld(t, h.engine.storagePipeline.diskBulk)
	requirePipelineFailureBudgetHeld(t, h.engine.storagePipeline.diskQueueBudget)
	requirePipelineFailureBudgetHeld(t, h.engine.storagePipeline.ledger.budget)
	result := awaitPipelineFailureProposal(t, goodDone)
	require.NoError(t, result.err)
	require.NotNil(t, result.result)
	require.Equal(t, goodIndex, result.result.CommitIndex)
	require.Equal(t, string(good), result.result.Response)
	require.Equal(t, goodIndex, h.engine.AppliedIndex())
	disk.unblock()
	for _, done := range []<-chan proposalResult{badDone, neverDone, secondaryDone} {
		requirePipelineFailureProposal(t, done, wantError)
	}
	requirePipelineFailureRetired(t, h, disk, wantError)
	require.NotErrorIs(t, h.engine.Err(), diskError, "later actor failure replaced the first fatal error")
	if panicApply {
		require.ErrorContains(t, h.engine.Err(), fsmError.Error(), "panic receipt lost the actual failing FSM marker")
	}
	require.Equal(t, goodIndex, h.engine.AppliedIndex())
	require.Equal(t, [][]byte{baseline, good}, fsm.Applied())
	require.Equal(t, []string{string(baseline), string(good), string(bad)}, fsm.attempted())
	t.Logf("completed client prefix=%d survived failed entry=%d; later entry=%d unexecuted/unacknowledged, first error and all budget retirement preserved", goodIndex, badIndex, neverIndex)
}
