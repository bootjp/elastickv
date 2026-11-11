package etcd

import (
	"path/filepath"
	"testing"

	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/server/v3/mock/mockstorage"
	etcdraft "go.etcd.io/raft/v3"
	raftpb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

func fullSnapshotTestConfiguration() *raftpb.ConfState {
	return &raftpb.ConfState{
		Voters: []uint64{1, 2}, VotersOutgoing: []uint64{1, 3},
		Learners: []uint64{4}, LearnersNext: []uint64{3}, AutoLeave: proto.Bool(true),
	}
}

func mutateSnapshotTestConfiguration(state *raftpb.ConfState) {
	state.Voters[0] = 11
	state.VotersOutgoing[0] = 12
	state.Learners[0] = 13
	state.LearnersNext[0] = 14
	*state.AutoLeave = false
}

func TestSnapshotConfigurationHistoryOwnsFullConfState(t *testing.T) {
	for _, operation := range []string{"reset", "record"} {
		t.Run(operation, func(t *testing.T) {
			var history snapshotConfigurationHistory
			input := fullSnapshotTestConfiguration()
			want := proto.CloneOf(input)
			if operation == "reset" {
				history.reset(10, input)
			} else {
				history.reset(1, &raftpb.ConfState{Voters: []uint64{1}})
				require.NoError(t, history.record(10, input))
			}
			mutateSnapshotTestConfiguration(input)
			captured, err := history.at(15)
			require.NoError(t, err)
			require.True(t, proto.Equal(want, captured))
			mutateSnapshotTestConfiguration(captured)
			again, err := history.at(15)
			require.NoError(t, err)
			require.True(t, proto.Equal(want, again), "lookup must return an owned clone")
		})
	}
}

func TestSnapshotConfigurationHistoryRejectsUnknownAndInvalidOrder(t *testing.T) {
	state := &raftpb.ConfState{Voters: []uint64{1}}
	var history snapshotConfigurationHistory
	require.ErrorIs(t, history.record(10, state), errSnapshotConfigurationPrefix)
	_, err := history.at(10)
	require.ErrorIs(t, err, errSnapshotConfigurationPrefix)
	history.reset(10, state)
	require.NoError(t, history.record(9, &raftpb.ConfState{Voters: []uint64{2}}))
	require.NoError(t, history.record(10, proto.CloneOf(state)))
	require.Len(t, history.entries, 1)
	_, err = history.at(9)
	require.ErrorIs(t, err, errSnapshotConfigurationPrefix)
	newState := &raftpb.ConfState{Voters: []uint64{1}, Learners: []uint64{2}}
	require.NoError(t, history.record(20, newState))
	for _, index := range []uint64{10, 15, 20} {
		require.ErrorIs(t, history.record(index, state), errSnapshotConfigurationPrefix)
	}
	require.NoError(t, history.record(20, proto.CloneOf(newState)))
	require.Len(t, history.entries, 2)
	for _, tc := range []struct {
		index uint64
		want  *raftpb.ConfState
	}{{10, state}, {19, state}, {20, newState}, {100, newState}} {
		got, err := history.at(tc.index)
		require.NoError(t, err)
		require.True(t, proto.Equal(tc.want, got))
	}
}

func TestSnapshotConfigurationHistoryNilFailsClosed(t *testing.T) {
	var history snapshotConfigurationHistory
	history.reset(0, nil)
	for _, index := range []uint64{0, 1, 100} {
		got, err := history.at(index)
		require.ErrorIs(t, err, errSnapshotConfigurationPrefix)
		require.Nil(t, got)
	}
	require.ErrorIs(t, history.record(1, nil), errSnapshotConfigurationPrefix)
	require.Len(t, history.entries, 1, "reject nil without adding an apparently known prefix")
	state := &raftpb.ConfState{Voters: []uint64{1}}
	require.NoError(t, history.record(1, state))
	got, err := history.at(1)
	require.NoError(t, err)
	require.True(t, proto.Equal(state, got))
	for _, index := range []uint64{0, 1, 2} {
		require.ErrorIs(t, history.record(index, nil), errSnapshotConfigurationPrefix)
	}
	require.Len(t, history.entries, 2)
}

func TestSnapshotConfigurationHistoryPrunesCoveredPrefix(t *testing.T) {
	for _, tc := range []struct {
		name    string
		through uint64
		indexes []uint64
	}{
		{"before", 9, []uint64{10, 20, 30}},
		{"base", 10, []uint64{10, 20, 30}},
		{"between", 19, []uint64{10, 20, 30}},
		{"exact", 20, []uint64{20, 30}},
		{"gap", 25, []uint64{20, 30}},
		{"last", 30, []uint64{30}},
		{"beyond", ^uint64(0), []uint64{30}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var history snapshotConfigurationHistory
			history.reset(10, fullSnapshotTestConfiguration())
			require.NoError(t, history.record(20, &raftpb.ConfState{Voters: []uint64{2}}))
			require.NoError(t, history.record(30, &raftpb.ConfState{Voters: []uint64{3}}))
			captured, err := history.at(10)
			require.NoError(t, err)
			oldEntries := history.entries
			history.pruneThrough(tc.through)
			var indexes []uint64
			for _, entry := range history.entries {
				indexes = append(indexes, entry.index)
				got, err := history.at(entry.index)
				require.NoError(t, err)
				require.True(t, proto.Equal(entry.state, got))
			}
			require.Equal(t, tc.indexes, indexes)
			if len(indexes) < len(oldEntries) {
				require.NotSame(t, &oldEntries[len(oldEntries)-len(indexes)], &history.entries[0],
					"pruning must release the superseded backing array")
			}
			_, err = history.at(indexes[0] - 1)
			require.ErrorIs(t, err, errSnapshotConfigurationPrefix)
			history.pruneThrough(tc.through)
			require.Len(t, history.entries, len(indexes), "repeated prune is idempotent")
			history.reset(40, &raftpb.ConfState{Voters: []uint64{4}})
			require.True(t, proto.Equal(fullSnapshotTestConfiguration(), captured), "prune/reset cannot alter an owned capture")
		})
	}
	var empty snapshotConfigurationHistory
	empty.pruneThrough(^uint64(0))
	require.Empty(t, empty.entries)
}

func TestSnapshotConfigurationHistoryDoesNotPublishPersistenceFailure(t *testing.T) {
	failure := errors.New("configuration persistence failed")
	for _, v2 := range []bool{false, true} {
		for _, stage := range []string{"record", "snapshot", "bump", "save", "release"} {
			name := "v1/" + stage
			if v2 {
				name = "v2/" + stage
			}
			t.Run(name, func(t *testing.T) {
				visit := func(current string) error {
					if current == stage {
						return failure
					}
					return nil
				}
				fixture := newConfigPhaseFixture(t, v2, &configPhaseFSM{visit: visit},
					&configPhaseStorage{Storage: mockstorage.NewStorageRecorder(""), visit: visit})
				require.ErrorIs(t, fixture.engine.applyCommitted([]raftpb.Entry{fixture.entry}), failure)
				fixture.requireUnpublished(t)
				require.Len(t, fixture.engine.snapshotConfigurations.entries, 1)
				state, err := fixture.engine.snapshotConfigurations.at(configPhaseIndex)
				require.NoError(t, err)
				require.Equal(t, []uint64{1}, state.GetVoters())
				require.Empty(t, state.GetLearners(), "failed persistence cannot enter snapshot configuration history")
			})
		}
	}
}

func TestSnapshotConfigurationHistorySeedsOnlySelectedStorageSnapshot(t *testing.T) {
	storage := etcdraft.NewMemoryStorage()
	selected := raftTestSnapshot(10, 1, []uint64{1}, []byte("selected"))
	require.NoError(t, storage.ApplySnapshot(&selected))
	e := &Engine{storage: storage, applied: 30, peers: map[uint64]Peer{
		1: {NodeID: 1}, 2: {NodeID: 2, Suffrage: SuffrageLearner},
	}}
	e.configIndex.Store(20)
	state, err := e.snapshotConfigurationAt(15)
	require.NoError(t, err)
	require.True(t, proto.Equal(selected.GetMetadata().GetConfState(), state))
	require.Equal(t, uint64(10), e.snapshotConfigurations.entries[0].index)
}

func TestSnapshotConfigurationHistoryReceivedSnapshotResetsBase(t *testing.T) {
	fixture := newConfigPhaseFixture(t, false, &dummyFSM{}, mockstorage.NewStorageRecorder(""))
	e := fixture.engine
	e.peers[fixture.joining.NodeID] = fixture.joining
	e.snapshotConfigurations.reset(1, &raftpb.ConfState{Voters: []uint64{1}})
	e.snapshotConfigurationReplayIndex = 4
	require.NoError(t, e.snapshotConfigurations.record(2, fullSnapshotTestConfiguration()))
	captured, err := e.snapshotConfigurations.at(2)
	require.NoError(t, err)
	incoming := raftTestSnapshot(10, 2, []uint64{1}, []byte("received"))
	incoming.Metadata.ConfState.Learners = []uint64{fixture.joining.NodeID}
	require.NoError(t, e.applyReadySnapshot(&incoming))
	require.Zero(t, e.snapshotConfigurationReplayIndex)
	require.Len(t, e.snapshotConfigurations.entries, 1)
	_, err = e.snapshotConfigurations.at(9)
	require.ErrorIs(t, err, errSnapshotConfigurationPrefix)
	state, err := e.snapshotConfigurations.at(10)
	require.NoError(t, err)
	require.True(t, proto.Equal(incoming.GetMetadata().GetConfState(), state))
	incoming.Metadata.ConfState.Learners[0] = 99
	state, err = e.snapshotConfigurations.at(10)
	require.NoError(t, err)
	require.Equal(t, []uint64{fixture.joining.NodeID}, state.GetLearners())
	require.True(t, proto.Equal(fullSnapshotTestConfiguration(), captured))
}

func TestSnapshotConfigurationCapturePendingReplayBoundary(t *testing.T) {
	for _, tc := range []struct {
		name    string
		pending uint64
		deferIt bool
	}{
		{"none", 0, false},
		{"earlier", 4, true},
		{"equal", 5, true},
		{"future", 6, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fsm := &countingSnapshotStateMachine{}
			storage := etcdraft.NewMemoryStorage()
			base := raftTestSnapshot(1, 1, []uint64{1}, []byte("base"))
			require.NoError(t, storage.ApplySnapshot(&base))
			e := &Engine{storage: storage, fsm: fsm, applied: 5}
			e.pendingConfChangeIndex.Store(tc.pending)
			request, err := e.captureLocalSnapshotRequest(5)
			if tc.deferIt {
				require.ErrorIs(t, err, raftengine.ErrSnapshotDeferred)
				require.Nil(t, request.snapshot)
				require.Zero(t, fsm.snapshotCalls.Load(), "do not capture the ahead FSM before configuration replay catches up")
				return
			}
			require.NoError(t, err)
			require.NotNil(t, request.snapshot)
			require.NoError(t, request.snapshot.Close())
			require.Equal(t, uint32(1), fsm.snapshotCalls.Load())
			require.Equal(t, uint64(5), request.index)
			require.Equal(t, []uint64{1}, request.confState.GetVoters())
		})
	}
}

func TestSnapshotConfigurationCaptureWaitsForLastSplitReplay(t *testing.T) {
	fsm := &countingSnapshotStateMachine{}
	storage := etcdraft.NewMemoryStorage()
	base := raftTestSnapshot(1, 1, []uint64{1}, []byte("base"))
	require.NoError(t, storage.ApplySnapshot(&base))
	e := &Engine{storage: storage, fsm: fsm, applied: 5}
	e.markPendingConfChange(2)
	e.markPendingConfChange(4)
	e.clearPendingConfChange(2)
	_, err := e.captureLocalSnapshotRequest(5)
	require.ErrorIs(t, err, raftengine.ErrSnapshotDeferred)
	require.Zero(t, fsm.snapshotCalls.Load())
	require.NoError(t, e.ensureSnapshotConfigurationHistory())
	state := &raftpb.ConfState{Voters: []uint64{1}, Learners: []uint64{2}}
	require.NoError(t, e.snapshotConfigurations.record(4, state))
	e.clearPendingConfChange(4)
	request, err := e.captureLocalSnapshotRequest(5)
	require.NoError(t, err)
	require.NoError(t, request.snapshot.Close())
	require.Equal(t, uint32(1), fsm.snapshotCalls.Load())
	require.True(t, proto.Equal(state, request.confState))
}

func TestSnapshotConfigurationCaptureUncommittedTailDoesNotMaskReplay(t *testing.T) {
	fsm := &countingSnapshotStateMachine{}
	storage := etcdraft.NewMemoryStorage()
	base := raftTestSnapshot(1, 1, []uint64{1}, []byte("base"))
	require.NoError(t, storage.ApplySnapshot(&base))
	var entries []*raftpb.Entry
	for index := uint64(2); index <= 6; index++ {
		entry := &raftpb.Entry{Index: proto.Uint64(index), Term: proto.Uint64(1)}
		if index%2 == 0 {
			entry.Type = entryTypePtr(raftpb.EntryConfChange)
		}
		entries = append(entries, entry)
	}
	require.NoError(t, storage.Append(entries))
	require.NoError(t, storage.SetHardState(&raftpb.HardState{Term: proto.Uint64(1), Commit: proto.Uint64(5)}))
	e := &Engine{storage: storage, fsm: fsm, applied: 5}
	require.NoError(t, e.restorePendingConfChangeFenceFromStorage(1))
	require.Equal(t, uint64(6), e.pendingConfChangeIndex.Load(), "the highest fence also covers uncommitted entries")
	e.clearPendingConfChange(2)
	e.snapshotConfigurations.reset(1, &raftpb.ConfState{Voters: []uint64{1}})
	require.NoError(t, e.snapshotConfigurations.record(2, &raftpb.ConfState{Voters: []uint64{1}, Learners: []uint64{2}}))
	request, err := e.captureLocalSnapshotRequest(5)
	if request.snapshot != nil {
		require.NoError(t, request.snapshot.Close())
	}
	require.ErrorIs(t, err, raftengine.ErrSnapshotDeferred,
		"uncommitted conf 6 cannot mask committed conf 4 that still must replay before capturing FSM prefix 5")
	require.Zero(t, fsm.snapshotCalls.Load())
	state := raftpb.ConfState{Voters: []uint64{1}, Learners: []uint64{2, 3}}
	require.NoError(t, e.recordSnapshotConfiguration(configApplyPlan{index: 4, confState: state}))
	require.Zero(t, e.snapshotConfigurationReplayIndex)
	request, err = e.captureLocalSnapshotRequest(5)
	require.NoError(t, err, "the uncommitted configuration after the captured prefix must not block it once committed replay completes")
	require.NoError(t, request.snapshot.Close())
	require.True(t, proto.Equal(&state, request.confState))
}

func TestSnapshotConfigurationWorkerPreservesFullJointState(t *testing.T) {
	for _, format := range []string{"inline", "file"} {
		t.Run(format, func(t *testing.T) {
			storage := etcdraft.NewMemoryStorage()
			base := raftTestSnapshot(1, 1, []uint64{1}, []byte("base"))
			require.NoError(t, storage.ApplySnapshot(&base))
			require.NoError(t, storage.Append([]*raftpb.Entry{{Index: proto.Uint64(2), Term: proto.Uint64(1)}}))
			e := &Engine{
				storage: storage, persist: mockstorage.NewStorageRecorder(""),
				fsm: &countingSnapshotStateMachine{}, dataDir: t.TempDir(), applied: 2,
			}
			if format == "file" {
				e.fsmSnapDir = filepath.Join(e.dataDir, fsmSnapDirName)
			}
			e.snapshotConfigurations.reset(1, base.GetMetadata().GetConfState())
			require.NoError(t, e.snapshotConfigurations.record(2, fullSnapshotTestConfiguration()))
			request, err := e.captureLocalSnapshotRequest(2)
			require.NoError(t, err)
			mutateSnapshotTestConfiguration(e.snapshotConfigurations.entries[1].state)
			require.NoError(t, e.persistLocalSnapshot(request))
			current, err := storage.Snapshot()
			require.NoError(t, err)
			require.True(t, proto.Equal(fullSnapshotTestConfiguration(), current.GetMetadata().GetConfState()),
				"voters, outgoing voters, learners, next learners and AutoLeave must survive the worker")
			require.Equal(t, format == "file", isSnapshotToken(current.GetData()))
		})
	}
}

func TestSnapshotConfigurationSynchronousDeferralKeepsBoundedHistory(t *testing.T) {
	for _, v2 := range []bool{false, true} {
		name := "v1"
		if v2 {
			name = "v2"
		}
		t.Run(name, func(t *testing.T) {
			fsm := &configPhaseFSM{visit: func(stage string) error {
				if stage == "snapshot" {
					return raftengine.ErrSnapshotDeferred
				}
				return nil
			}}
			fixture := newConfigPhaseFixture(t, v2, fsm, mockstorage.NewStorageRecorder(""))
			e := fixture.engine
			require.NoError(t, e.applyCommitted([]raftpb.Entry{fixture.entry}))
			fixture.requirePublished(t)
			for index := uint64(3); index <= 66; index++ {
				entry := fixture.entry
				entry.Index = proto.Uint64(index)
				if index%2 != 0 {
					var err error
					if v2 {
						entry.Data, err = proto.Marshal(&raftpb.ConfChangeV2{Changes: []*raftpb.ConfChangeSingle{
							{Type: confChangeTypePtr(raftpb.ConfChangeRemoveNode), NodeId: proto.Uint64(fixture.joining.NodeID)},
						}})
					} else {
						entry.Data, err = proto.Marshal(&raftpb.ConfChange{
							Type: confChangeTypePtr(raftpb.ConfChangeRemoveNode), NodeId: proto.Uint64(fixture.joining.NodeID),
						})
					}
					require.NoError(t, err)
				}
				require.NoError(t, e.storage.Append([]*raftpb.Entry{&entry}))
				require.NoError(t, e.applyCommitted([]raftpb.Entry{entry}))
				require.Len(t, e.snapshotConfigurations.entries, 1, "deferred synchronous checkpoints must not retain every configuration")
				require.Equal(t, index, e.snapshotConfigurations.entries[0].index)
			}
			selected, err := e.storage.Snapshot()
			require.NoError(t, err)
			require.Equal(t, configPhaseBaseIndex, selected.GetMetadata().GetIndex(), "every snapshot remained deferred")
			require.Empty(t, selected.GetMetadata().GetConfState().GetLearners())
			state, err := e.snapshotConfigurationAt(e.applied)
			require.NoError(t, err)
			require.Equal(t, []uint64{fixture.joining.NodeID}, state.GetLearners())
		})
	}
}
