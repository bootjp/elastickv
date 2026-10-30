package etcd

import (
	"testing"

	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/server/v3/mock/mockstorage"
	etcdstorage "go.etcd.io/etcd/server/v3/storage"
	etcdraft "go.etcd.io/raft/v3"
	raftpb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

const (
	configPhaseBaseIndex = uint64(1)
	configPhaseIndex     = uint64(2)
	configPhaseRequestID = uint64(17)
)

type configPhaseFSM struct {
	testStateMachine
	visit              func(string) error
	configurationIndex uint64
	durableIndex       uint64
}

func (f *configPhaseFSM) RecordAppliedConfiguration(index uint64) error {
	if err := f.visit("record"); err != nil {
		return err
	}
	f.configurationIndex = index
	return nil
}

func (f *configPhaseFSM) AppliedConfigurationIndex() uint64 { return f.configurationIndex }

func (f *configPhaseFSM) Snapshot() (Snapshot, error) {
	if err := f.visit("snapshot"); err != nil {
		return nil, err
	}
	return &testSnapshot{data: []byte("configuration-prefix")}, nil
}

func (f *configPhaseFSM) SetDurableAppliedIndex(index uint64) error {
	if err := f.visit("bump"); err != nil {
		return err
	}
	f.durableIndex = index
	return nil
}

type configPhaseStorage struct {
	etcdstorage.Storage
	visit func(string) error
}

func (s *configPhaseStorage) SaveSnap(snapshot *raftpb.Snapshot) error {
	if err := s.visit("save"); err != nil {
		return err
	}
	return s.Storage.SaveSnap(snapshot)
}

func (s *configPhaseStorage) Release(snapshot *raftpb.Snapshot) error {
	if err := s.visit("release"); err != nil {
		return err
	}
	return s.Storage.Release(snapshot)
}

type configPhaseFixture struct {
	engine        *Engine
	entry         raftpb.Entry
	done          chan adminResult
	configuration raftengine.Configuration
	joining       Peer
}

func newConfigPhaseFixture(t *testing.T, v2 bool, fsm StateMachine, persist etcdstorage.Storage) configPhaseFixture {
	t.Helper()
	local := Peer{NodeID: 1, ID: "n1", Address: "127.0.0.1:7001", Suffrage: SuffrageVoter}
	joining := Peer{NodeID: 2, ID: "n2", Address: "127.0.0.1:7002"}
	context, err := encodeConfChangeContext(configPhaseRequestID, joining)
	require.NoError(t, err)
	entry := raftpb.Entry{Index: uint64Ptr(configPhaseIndex), Term: uint64Ptr(1)}
	if v2 {
		entry.Type = entryTypePtr(raftpb.EntryConfChangeV2)
		entry.Data, err = proto.Marshal(&raftpb.ConfChangeV2{
			Changes: []*raftpb.ConfChangeSingle{{Type: confChangeTypePtr(raftpb.ConfChangeAddLearnerNode), NodeId: uint64Ptr(joining.NodeID)}},
			Context: context,
		})
	} else {
		entry.Type = entryTypePtr(raftpb.EntryConfChange)
		entry.Data, err = proto.Marshal(&raftpb.ConfChange{
			Type: confChangeTypePtr(raftpb.ConfChangeAddLearnerNode), NodeId: uint64Ptr(joining.NodeID), Context: context,
		})
	}
	require.NoError(t, err)
	storage := etcdraft.NewMemoryStorage()
	snapshot := raftTestSnapshot(configPhaseBaseIndex, 1, []uint64{local.NodeID}, []byte("baseline"))
	require.NoError(t, storage.ApplySnapshot(&snapshot))
	require.NoError(t, storage.Append([]*raftpb.Entry{&entry}))
	require.NoError(t, storage.SetHardState(&raftpb.HardState{Term: uint64Ptr(1), Commit: uint64Ptr(configPhaseIndex)}))
	done := make(chan adminResult, 1)
	e := &Engine{
		nodeID: local.NodeID, localID: local.ID,
		storage: storage, rawNode: mustRawNode(t, storage, local.NodeID),
		fsm: fsm, persist: persist, dataDir: t.TempDir(),
		peers: map[uint64]Peer{local.NodeID: local}, voterCount: 1,
		applied:        configPhaseBaseIndex,
		pendingConfigs: map[uint64]adminRequest{configPhaseRequestID: {id: configPhaseRequestID, done: done}},
	}
	e.config = configurationFromConfState(e.peers, confStateValue(snapshot.GetMetadata().GetConfState()))
	e.configIndex.Store(configPhaseBaseIndex)
	e.appliedIndex.Store(configPhaseBaseIndex)
	e.markPendingConfChange(configPhaseIndex)
	return configPhaseFixture{engine: e, entry: entry, done: done, configuration: e.currentConfiguration(), joining: joining}
}

func (f configPhaseFixture) requireUnpublished(t *testing.T) {
	t.Helper()
	e := f.engine
	require.Equal(t, f.configuration, e.currentConfiguration())
	require.Equal(t, configPhaseBaseIndex, e.currentConfigIndex())
	require.Len(t, e.peers, 1)
	require.NotContains(t, e.peers, f.joining.NodeID)
	require.Equal(t, 1, e.voterCount)
	require.Empty(t, e.isLearnerNode)
	require.Equal(t, configPhaseBaseIndex, e.applied)
	require.Equal(t, configPhaseBaseIndex, e.AppliedIndex())
	require.Equal(t, configPhaseIndex, e.pendingConfChangeIndex.Load())
	require.Contains(t, e.pendingConfigs, configPhaseRequestID)
	select {
	case result := <-f.done:
		t.Fatalf("configuration receipt before persistence success: %+v", result)
	default:
	}
}

func (f configPhaseFixture) requirePublished(t *testing.T) {
	t.Helper()
	e := f.engine
	require.Equal(t, configPhaseIndex, e.currentConfigIndex())
	require.Equal(t, configPhaseIndex, e.applied)
	require.Equal(t, configPhaseIndex, e.AppliedIndex())
	require.False(t, e.hasPendingConfChange())
	require.Empty(t, e.pendingConfigs)
	require.Equal(t, f.joining, e.peers[f.joining.NodeID])
	require.Equal(t, 1, e.voterCount)
	require.True(t, e.isLearnerNode[f.joining.NodeID])
	want := configurationFromConfState(e.peers, raftpb.ConfState{Voters: []uint64{1}, Learners: []uint64{f.joining.NodeID}})
	require.Equal(t, want, e.currentConfiguration())
	select {
	case result := <-f.done:
		require.NoError(t, result.err)
		require.Equal(t, configPhaseIndex, result.index)
		require.Equal(t, f.joining.NodeID, result.peer.NodeID)
	default:
		t.Fatal("configuration publication did not deliver its admin receipt")
	}
	require.Empty(t, f.done, "admin receipt must be delivered exactly once")
}

func TestEngineConfigPhasesPersistenceAndPublication(t *testing.T) {
	failure := errors.New("injected configuration phase failure")
	for _, version := range []string{"v1", "v2"} {
		t.Run(version, func(t *testing.T) {
			for _, stage := range []string{"record", "snapshot", "bump", "save", "release", "success", "deferred"} {
				t.Run(stage, func(t *testing.T) {
					testConfigPhasePersistence(t, version == "v2", stage, failure)
				})
			}
		})
	}
}

func testConfigPhasePersistence(t *testing.T, v2 bool, failureStage string, failure error) {
	t.Helper()
	var fixture configPhaseFixture
	var events []string
	visit := func(stage string) error {
		fixture.requireUnpublished(t)
		events = append(events, stage)
		if stage == failureStage {
			return failure
		}
		if stage == "snapshot" && failureStage == "deferred" {
			return raftengine.ErrSnapshotDeferred
		}
		return nil
	}
	fsm := &configPhaseFSM{visit: visit, configurationIndex: configPhaseBaseIndex, durableIndex: configPhaseBaseIndex}
	persist := &configPhaseStorage{Storage: mockstorage.NewStorageRecorder(""), visit: visit}
	fixture = newConfigPhaseFixture(t, v2, fsm, persist)
	err := fixture.engine.applyCommitted([]raftpb.Entry{fixture.entry})
	sequence := []string{"record", "snapshot", "bump", "save", "release"}
	if failureStage == "success" || failureStage == "deferred" {
		require.NoError(t, err)
		fixture.requirePublished(t)
		if failureStage == "deferred" {
			sequence = sequence[:2]
			require.Equal(t, configPhaseBaseIndex, fsm.durableIndex)
		} else {
			require.Equal(t, configPhaseIndex, fsm.durableIndex)
		}
		require.Equal(t, configPhaseIndex, fsm.configurationIndex)
	} else {
		require.ErrorIs(t, err, failure)
		fixture.requireUnpublished(t)
		for i, stage := range sequence {
			if stage == failureStage {
				sequence = sequence[:i+1]
				break
			}
		}
	}
	require.Equal(t, sequence, events)
}

func TestEngineConfigPhasesTransitionOwnsPlan(t *testing.T) {
	for _, version := range []string{"v1", "v2"} {
		t.Run(version, func(t *testing.T) {
			visit := func(stage string) error {
				t.Fatalf("owner transition invoked persistence callback %q", stage)
				return nil
			}
			fsm := &configPhaseFSM{visit: visit}
			persist := &configPhaseStorage{Storage: mockstorage.NewStorageRecorder(""), visit: visit}
			fixture := newConfigPhaseFixture(t, version == "v2", fsm, persist)
			var plan configApplyPlan
			var err error
			if version == "v2" {
				var cc raftpb.ConfChangeV2
				require.NoError(t, proto.Unmarshal(fixture.entry.GetData(), &cc))
				plan, err = fixture.engine.transitionConfChangeV2(configPhaseIndex, cc)
			} else {
				var cc raftpb.ConfChange
				require.NoError(t, proto.Unmarshal(fixture.entry.GetData(), &cc))
				plan, err = fixture.engine.transitionConfChange(configPhaseIndex, cc)
			}
			require.NoError(t, err)
			fixture.requireUnpublished(t)
			require.Equal(t, configPhaseIndex, plan.index)
			require.Equal(t, []uint64{1}, plan.confState.GetVoters())
			require.Equal(t, []uint64{fixture.joining.NodeID}, plan.confState.GetLearners())
			require.Contains(t, fixture.engine.rawNode.Status().Config.Learners, fixture.joining.NodeID)
			learner := fixture.joining
			learner.Suffrage = SuffrageLearner
			wantPeers := []Peer{fixture.engine.peers[1], learner}
			require.Equal(t, wantPeers, plan.peers)
			// Later owner transitions and peer-cache replacements cannot change
			// the already captured configuration prefix.
			fixture.engine.rawNode.ApplyConfChange(&raftpb.ConfChange{
				Type: confChangeTypePtr(raftpb.ConfChangeRemoveNode), NodeId: uint64Ptr(fixture.joining.NodeID),
			})
			fixture.engine.peers[1] = Peer{NodeID: 1, Address: "replacement"}
			require.Equal(t, []uint64{1}, plan.confState.GetVoters())
			require.Equal(t, []uint64{fixture.joining.NodeID}, plan.confState.GetLearners())
			require.Equal(t, wantPeers, plan.peers)
		})
	}
}
