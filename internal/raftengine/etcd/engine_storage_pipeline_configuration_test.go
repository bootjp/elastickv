package etcd

import (
	"bytes"
	"context"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
	etcdstorage "go.etcd.io/etcd/server/v3/storage"
	etcdraft "go.etcd.io/raft/v3"
	raftpb "go.etcd.io/raft/v3/raftpb"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

type pipelineConfigurationGate struct {
	stage   string
	entered chan uint64
	release chan struct{}
	once    sync.Once
	failure error
}

func (g *pipelineConfigurationGate) visit(stage string, index uint64) error {
	if g.stage != stage {
		return nil
	}
	g.entered <- index
	<-g.release
	return g.failure
}

func (g *pipelineConfigurationGate) unblock() { g.once.Do(func() { close(g.release) }) }

type pipelineConfigurationFSM struct {
	*asyncStorageFSM
	gate          *pipelineConfigurationGate
	configuration atomic.Uint64
	deferSnapshot bool
	mu            sync.Mutex
	order         []string
}

func (f *pipelineConfigurationFSM) Apply(payload []byte) any {
	f.mu.Lock()
	f.order = append(f.order, string(payload))
	f.mu.Unlock()
	return f.asyncStorageFSM.Apply(payload)
}

func (f *pipelineConfigurationFSM) RecordAppliedConfiguration(index uint64) error {
	if err := f.gate.visit("record", index); err != nil {
		return err
	}
	f.configuration.Store(index)
	f.mu.Lock()
	f.order = append(f.order, "configuration")
	f.mu.Unlock()
	return nil
}

func (f *pipelineConfigurationFSM) AppliedConfigurationIndex() uint64 { return f.configuration.Load() }

func (f *pipelineConfigurationFSM) Snapshot() (Snapshot, error) {
	if err := f.gate.visit("capture", f.configuration.Load()); err != nil {
		return nil, err
	}
	if f.deferSnapshot {
		return nil, raftengine.ErrSnapshotDeferred
	}
	return f.asyncStorageFSM.Snapshot()
}

func (f *pipelineConfigurationFSM) SetDurableAppliedIndex(index uint64) error {
	if f.configuration.Load() == index {
		if err := f.gate.visit("durable", index); err != nil {
			return err
		}
	}
	return f.asyncStorageFSM.SetDurableAppliedIndex(index)
}

type pipelineConfigurationStorage struct {
	etcdstorage.Storage
	gate      *pipelineConfigurationGate
	tailSaved chan uint64
}

func (s *pipelineConfigurationStorage) Save(hs *raftpb.HardState, entries []*raftpb.Entry) error {
	if err := s.Storage.Save(hs, entries); err != nil {
		return err
	}
	for _, entry := range entries {
		_, data, ok := decodeProposalEnvelope(entry.GetData())
		if ok && bytes.Equal(data, []byte("after-configuration")) {
			s.tailSaved <- entry.GetIndex()
		}
	}
	return nil
}

func (s *pipelineConfigurationStorage) SaveSnap(snapshot *raftpb.Snapshot) error {
	if err := s.gate.visit("save", snapshot.GetMetadata().GetIndex()); err != nil {
		return err
	}
	return s.Storage.SaveSnap(snapshot)
}

func (s *pipelineConfigurationStorage) Release(snapshot *raftpb.Snapshot) error {
	if err := s.gate.visit("release", snapshot.GetMetadata().GetIndex()); err != nil {
		return err
	}
	return s.Storage.Release(snapshot)
}

func newPipelineConfigurationRuntime(t *testing.T, stage string, failure error, deferred bool) (*asyncStorageRuntime, *pipelineConfigurationFSM, *pipelineConfigurationGate, *pipelineConfigurationStorage) {
	t.Helper()
	return newPipelineConfigurationRuntimeWithDisk(t, stage, failure, deferred, nil)
}

func newPipelineConfigurationRuntimeWithDisk(t *testing.T, stage string, failure error, deferred bool,
	configure func(*Engine, *pipelineConfigurationStorage),
) (*asyncStorageRuntime, *pipelineConfigurationFSM, *pipelineConfigurationGate, *pipelineConfigurationStorage) {
	t.Helper()
	gate := &pipelineConfigurationGate{stage: stage, failure: failure, entered: make(chan uint64, 1), release: make(chan struct{})}
	fsm := &pipelineConfigurationFSM{asyncStorageFSM: &asyncStorageFSM{}, gate: gate, deferSnapshot: deferred}
	disk := &pipelineConfigurationStorage{gate: gate, tailSaved: make(chan uint64, 4)}
	walGate := &asyncStorageSaveGate{expectedCloseError: failure}
	h := newNativeStorageRuntime(t, fsm.asyncStorageFSM, walGate, newStorageRawNode, func(e *Engine) {
		disk.Storage = walGate
		pipeline, err := newEngineStoragePipeline(disk)
		require.NoError(t, err)
		e.fsm, e.persist, e.storagePipeline = fsm, disk, pipeline
		dispatch := e.dispatchFn
		e.dispatchFn = func(ctx context.Context, request dispatchRequest) error {
			if request.msg.GetTo() != 2 {
				return nil // The new learner is intentionally offline; the two voters still form the native quorum.
			}
			return dispatch(ctx, request)
		}
		if configure != nil {
			configure(e, disk)
		}
	})
	t.Cleanup(gate.unblock)
	establishExplicitAsyncStorageLeader(t, h, fsm.asyncStorageFSM, walGate, []byte("before-configuration"))
	return h, fsm, gate, disk
}

func proposeNativePipelineConfiguration(t *testing.T, h *asyncStorageRuntime, v2 bool) {
	t.Helper()
	data, err := encodeConfChangeContext(0, Peer{NodeID: 3, ID: "n3", Address: "127.0.0.1:7013"})
	require.NoError(t, err)
	if v2 {
		err = h.follower.ProposeConfChange(&raftpb.ConfChangeV2{Changes: []*raftpb.ConfChangeSingle{
			{Type: confChangeTypePtr(raftpb.ConfChangeAddLearnerNode), NodeId: proto.Uint64(3)},
		}, Context: data})
	} else {
		err = h.follower.ProposeConfChange(&raftpb.ConfChange{
			Type: confChangeTypePtr(raftpb.ConfChangeAddLearnerNode), NodeId: proto.Uint64(3), Context: data,
		})
	}
	require.NoError(t, err)
	h.flushFollower(t)
}

func TestEnginePipelineConfigurationHeldStages(t *testing.T) {
	for _, v2 := range []bool{false, true} {
		version := "v1"
		if v2 {
			version = "v2"
		}
		for _, stage := range []string{"record", "capture", "durable", "save", "release"} {
			t.Run(version+"/"+stage, func(t *testing.T) {
				h, fsm, gate, disk := newPipelineConfigurationRuntime(t, stage, nil, false)
				before := h.engine.AppliedIndex()
				proposeNativePipelineConfiguration(t, h, v2)
				index := asyncStorageAwait(t, h, gate.entered, "real configuration phase")
				require.Greater(t, index, before)
				tail := h.propose([]byte("after-configuration"))
				if stage == "record" {
					require.Greater(t, asyncStorageAwait(t, h, disk.tailSaved, "durable later normal entry"), index)
				}
				assertBlocked := func() {
					require.Equal(t, before, h.engine.AppliedIndex())
					require.Less(t, h.engine.currentConfigIndex(), index)
					require.True(t, h.engine.LastQuorumAck().IsZero(), "old published membership cannot confer a lease during transition")
					require.Equal(t, [][]byte{[]byte("before-configuration")}, fsm.Applied())
					requireAsyncStorageProposalPending(t, tail)
				}
				h.verifyBlockedHeartbeats(t, assertBlocked)
				gate.unblock()
				result := asyncStorageAwait(t, h, tail, "tail after durable configuration publication")
				require.NoError(t, result.err)
				require.Greater(t, result.result.CommitIndex, index)
				require.Equal(t, index, h.engine.currentConfigIndex())
				fsm.mu.Lock()
				order := append([]string(nil), fsm.order...)
				fsm.mu.Unlock()
				require.Equal(t, []string{"before-configuration", "configuration", "after-configuration"}, order)
				require.NoError(t, h.engine.Close())
				requirePipelineConfigurationReopen(t, h.engine.dataDir, index)
			})
		}
	}
}

func requirePipelineConfigurationReopen(t *testing.T, dir string, index uint64) {
	t.Helper()
	restored := &testStateMachine{}
	disk, err := loadWalState(zap.NewNop(), filepath.Join(dir, walDirName), filepath.Join(dir, snapDirName), filepath.Join(dir, fsmSnapDirName), restored, nil)
	require.NoError(t, err)
	require.Equal(t, index, disk.LocalSnap.GetMetadata().GetIndex())
	require.Equal(t, []uint64{3}, disk.LocalSnap.GetMetadata().GetConfState().GetLearners())
	require.Equal(t, [][]byte{[]byte("before-configuration")}, restored.Applied())
	require.NoError(t, closePersist(disk.Persist))
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	e, err := Open(ctx, OpenConfig{NodeID: 1, LocalID: "n1", LocalAddress: "127.0.0.1:7011", DataDir: dir, StateMachine: &testStateMachine{}})
	require.NoError(t, err)
	require.NoError(t, e.Close())
}

func TestEnginePipelineConfigurationFailuresQuiesce(t *testing.T) {
	for _, stage := range []string{"record", "capture", "durable", "save", "release"} {
		t.Run(stage, func(t *testing.T) {
			failure := errors.New("configuration " + stage + " failed")
			h, fsm, gate, _ := newPipelineConfigurationRuntime(t, stage, failure, false)
			before := h.engine.AppliedIndex()
			proposeNativePipelineConfiguration(t, h, false)
			asyncStorageAwait(t, h, gate.entered, "held failing configuration phase")
			gate.unblock()
			select {
			case <-h.engine.Done():
			case <-time.After(5 * time.Second):
				t.Fatal("configuration handshake did not quiesce after actual failure")
			}
			require.ErrorIs(t, h.engine.Err(), failure)
			require.Equal(t, before, h.engine.AppliedIndex())
			require.Equal(t, [][]byte{[]byte("before-configuration")}, fsm.Applied())
			require.Zero(t, h.engine.storagePipeline.diskOutstanding)
			require.Zero(t, h.engine.storagePipeline.applyOutstanding)
			used, items := h.engine.storagePipeline.configurationBudget.usage()
			require.Zero(t, used)
			require.Zero(t, items)
		})
	}
}

func TestEnginePipelineConfigurationCloseWhileCallbackNeedsOwner(t *testing.T) {
	h, _, gate, _ := newPipelineConfigurationRuntime(t, "record", nil, false)
	proposeNativePipelineConfiguration(t, h, true)
	asyncStorageAwait(t, h, gate.entered, "configuration record before shutdown")
	h.engine.requestShutdown()
	requireAsyncStorageSignalPending(t, h.engine.Done(), "shutdown before real configuration record return")
	gate.unblock()
	select {
	case <-h.engine.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("quiescing owner stopped pumping configuration callbacks")
	}
	require.NoError(t, h.engine.Err())
}

func TestEnginePipelineConfigurationDeferredCaptureReplaysOnOpen(t *testing.T) {
	for _, v2 := range []bool{false, true} {
		name := "v1"
		if v2 {
			name = "v2"
		}
		t.Run(name, func(t *testing.T) {
			h, _, gate, _ := newPipelineConfigurationRuntime(t, "record", nil, true)
			proposeNativePipelineConfiguration(t, h, v2)
			index := asyncStorageAwait(t, h, gate.entered, "deferred configuration")
			gate.unblock()
			result := asyncStorageAwait(t, h, h.propose([]byte("after-configuration")), "tail after deferred configuration")
			require.NoError(t, result.err)
			require.Equal(t, index, h.engine.currentConfigIndex())
			current, err := h.engine.storage.Snapshot()
			require.NoError(t, err)
			require.Equal(t, uint64(1), current.GetMetadata().GetIndex())
			require.Empty(t, current.GetMetadata().GetConfState().GetLearners())
			require.NoError(t, h.engine.Close())
			fsm := &testStateMachine{}
			e, err := Open(t.Context(), OpenConfig{NodeID: 1, LocalID: "n1", LocalAddress: "127.0.0.1:7011", DataDir: h.engine.dataDir, StateMachine: fsm})
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, e.Close()) })
			require.Eventually(t, func() bool { return e.AppliedIndex() >= result.result.CommitIndex }, 5*time.Second, time.Millisecond)
			require.Equal(t, [][]byte{[]byte("before-configuration"), []byte("after-configuration")}, fsm.Applied())
			require.Equal(t, index, e.currentConfigIndex())
			e.mu.RLock()
			learner := e.isLearnerNode[3]
			e.mu.RUnlock()
			require.True(t, learner)
		})
	}
}

func TestEnginePipelineConfigurationAdminReceiptFollowsPublication(t *testing.T) {
	h, _, gate, _ := newPipelineConfigurationRuntime(t, "save", nil, false)
	done := make(chan adminResult, 1)
	go func() {
		index, err := h.engine.AddLearner(h.ctx, "n3", "127.0.0.1:7013", 0)
		done <- adminResult{index: index, err: err}
	}()
	index := asyncStorageAwait(t, h, gate.entered, "admin configuration actual SaveSnap")
	select {
	case result := <-done:
		t.Fatalf("admin receipt preceded real durability/publication: %+v", result)
	default:
	}
	gate.unblock()
	result := asyncStorageAwait(t, h, done, "admin configuration published")
	require.NoError(t, result.err)
	require.Equal(t, index, result.index)
	require.Equal(t, index, h.engine.currentConfigIndex())
	require.GreaterOrEqual(t, h.engine.AppliedIndex(), index)
	require.False(t, h.engine.configurationTransition.Load())
}

func TestEnginePipelineConfigurationHistoryReservationAndRetirement(t *testing.T) {
	storage := etcdraft.NewMemoryStorage()
	base := raftTestSnapshot(1, 1, []uint64{1}, []byte("base"))
	require.NoError(t, storage.ApplySnapshot(&base))
	var entries []*raftpb.Entry
	for index := uint64(2); index <= 4; index++ {
		entries = append(entries, &raftpb.Entry{Index: proto.Uint64(index), Term: proto.Uint64(1)})
	}
	require.NoError(t, storage.Append(entries))
	p := &engineStoragePipeline{configurationHistoryBudget: newStorageWorkBudget(1<<20, 2)}
	e := &Engine{storage: storage, storagePipeline: p}
	e.snapshotConfigurations.reset(1, base.GetMetadata().GetConfState())
	state := raftpb.ConfState{Voters: []uint64{1}, Learners: []uint64{2}}
	require.NoError(t, e.recordPipelineConfiguration(configApplyPlan{index: 2, confState: state}))
	require.NoError(t, e.recordPipelineConfiguration(configApplyPlan{index: 3, confState: state}))
	require.ErrorIs(t, e.recordPipelineConfiguration(configApplyPlan{index: 4, confState: state}), errStoragePipelineResource)
	_, items := p.configurationHistoryBudget.usage()
	require.Equal(t, 2, items)
	_, err := storage.CreateSnapshot(4, &state, []byte("current"))
	require.NoError(t, err)
	require.NoError(t, e.recordPipelineConfiguration(configApplyPlan{index: 4, confState: state}))
	require.Len(t, e.snapshotConfigurations.entries, 1)
	_, items = p.configurationHistoryBudget.usage()
	require.Equal(t, 1, items)
	p.retireConfigurationHistory()
	used, items := p.configurationHistoryBudget.usage()
	require.Zero(t, used)
	require.Zero(t, items)
}

func TestEnginePipelineConfigurationFrontWaitsForActualEarlierApply(t *testing.T) {
	h, fsm, gate, _ := newPipelineConfigurationRuntime(t, "record", nil, false)
	fsm.payload = []byte("earlier-held-apply")
	fsm.applyEntered = make(chan uint64, 1)
	fsm.applyRelease = make(chan struct{})
	fsm.applyReturned = make(chan struct{})
	earlier := h.propose(fsm.payload)
	priorIndex := asyncStorageAwait(t, h, fsm.applyEntered, "actual earlier Apply")
	proposeNativePipelineConfiguration(t, h, true)
	h.verifyBlockedHeartbeats(t, func() {
		select {
		case index := <-gate.entered:
			t.Fatalf("configuration %d overtook held earlier Apply %d", index, priorIndex)
		default:
		}
		requireAsyncStorageProposalPending(t, earlier)
	})
	fsm.unblock()
	index := asyncStorageAwait(t, h, gate.entered, "configuration after actual earlier Apply")
	require.Greater(t, index, priorIndex)
	select {
	case result := <-earlier:
		require.NoError(t, result.err)
		require.Equal(t, priorIndex, result.result.CommitIndex)
	default:
		t.Fatal("configuration transition preceded the earlier successful prefix publication")
	}
	gate.unblock()
}

func TestEnginePipelineConfigurationDeferralIsCaptureOnly(t *testing.T) {
	for _, stage := range []string{"record", "durable", "save", "release"} {
		t.Run(stage, func(t *testing.T) {
			h, _, gate, _ := newPipelineConfigurationRuntime(t, stage, raftengine.ErrSnapshotDeferred, false)
			before := h.engine.AppliedIndex()
			proposeNativePipelineConfiguration(t, h, true)
			asyncStorageAwait(t, h, gate.entered, "non-capture deferred sentinel")
			gate.unblock()
			select {
			case <-h.engine.Done():
			case <-time.After(5 * time.Second):
				t.Fatal("a non-capture failure was treated as benign snapshot deferral")
			}
			require.ErrorIs(t, h.engine.Err(), raftengine.ErrSnapshotDeferred)
			require.Equal(t, before, h.engine.AppliedIndex())
		})
	}
}

func TestEnginePipelineConfigurationHeldRecordJoinsAfterDiskFatal(t *testing.T) {
	failure := errors.New("disk failed while configuration Record was executing")
	fault := newPipelineFailureDisk([]byte("after-configuration"))
	fault.saveError = failure
	h, _, gate, _ := newPipelineConfigurationRuntimeWithDisk(t, "record", failure, false, func(e *Engine, disk *pipelineConfigurationStorage) {
		fault.Storage, fault.pipeline = disk, e.storagePipeline
		e.persist, e.storagePipeline.persist = fault, fault
	})
	// Fatal teardown cancels transport first; keep the client alive to observe
	// the actual storage verdict instead of racing a shared context cancellation.
	clientCtx, cancelClient := context.WithCancel(context.Background())
	h.ctx = clientCtx
	t.Cleanup(cancelClient)
	t.Cleanup(fault.unblock)
	proposeNativePipelineConfiguration(t, h, false)
	asyncStorageAwait(t, h, gate.entered, "held real configuration Record")
	invalidated := pipelineFailureInvalidation(h)
	tail := h.propose([]byte("after-configuration"))
	asyncStorageAwait(t, h, fault.entered, "real later Save fault")
	fault.unblock()
	requirePipelineFailureQuiescing(t, h, fault, invalidated, failure)
	requirePipelineFailureBudgetHeld(t, h.engine.storagePipeline.configurationBudget)
	requireAsyncStorageSignalPending(t, h.engine.Done(), "config Record still executing after disk fatal")
	gate.unblock()
	requirePipelineFailureProposal(t, tail, failure)
	requirePipelineFailureRetired(t, h, fault, failure)
	used, items := h.engine.storagePipeline.configurationBudget.usage()
	require.Zero(t, used)
	require.Zero(t, items)
}

func TestEnginePipelineConfigurationSelfRemovalPublishesAndJoins(t *testing.T) {
	for _, v2 := range []bool{false, true} {
		name := "v1"
		if v2 {
			name = "v2"
		}
		t.Run(name, func(t *testing.T) {
			h, _, gate, _ := newPipelineConfigurationRuntime(t, "save", nil, false)
			context, err := encodeConfChangeContext(0, Peer{NodeID: 1, ID: "n1", Address: "127.0.0.1:7011"})
			require.NoError(t, err)
			if v2 {
				err = h.follower.ProposeConfChange(&raftpb.ConfChangeV2{Context: context, Changes: []*raftpb.ConfChangeSingle{
					{Type: confChangeTypePtr(raftpb.ConfChangeRemoveNode), NodeId: proto.Uint64(1)},
				}})
			} else {
				err = h.follower.ProposeConfChange(&raftpb.ConfChange{Context: context,
					Type: confChangeTypePtr(raftpb.ConfChangeRemoveNode), NodeId: proto.Uint64(1)})
			}
			require.NoError(t, err)
			h.flushFollower(t)
			index := asyncStorageAwait(t, h, gate.entered, "self-removal SaveSnap")
			require.Less(t, h.engine.currentConfigIndex(), index)
			require.True(t, h.engine.LastQuorumAck().IsZero())
			gate.unblock()
			require.Eventually(t, func() bool { return h.engine.currentConfigIndex() == index }, 5*time.Second, time.Millisecond)
			require.GreaterOrEqual(t, h.engine.AppliedIndex(), index)
			h.engine.mu.RLock()
			_, present := h.engine.peers[1]
			h.engine.mu.RUnlock()
			require.False(t, present)
			require.NoError(t, h.engine.Close())
			for _, budget := range []*storageWorkBudget{h.engine.storagePipeline.configurationBudget, h.engine.storagePipeline.configurationHistoryBudget} {
				used, items := budget.usage()
				require.Zero(t, used)
				require.Zero(t, items)
			}
		})
	}
}
