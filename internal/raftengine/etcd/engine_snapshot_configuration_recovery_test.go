package etcd

import (
	"context"
	"io"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/server/v3/mock/mockstorage"
	raftpb "go.etcd.io/raft/v3/raftpb"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

type recoveryConfigurationSnapshotFSM struct {
	testStateMachine
	deferred bool
}

func (f *recoveryConfigurationSnapshotFSM) Snapshot() (Snapshot, error) {
	if f.deferred {
		return nil, raftengine.ErrSnapshotDeferred
	}
	return f.testStateMachine.Snapshot()
}

func TestSnapshotConfigurationNativeWALRecoveryAfterDeferredChange(t *testing.T) {
	for _, version := range []string{"v1", "v2"} {
		t.Run(version, func(t *testing.T) {
			for _, format := range []string{"inline", "file"} {
				t.Run(format, func(t *testing.T) {
					testSnapshotConfigurationNativeWALRecovery(t, version == "v2", format == "file")
				})
			}
		})
	}
}

func testSnapshotConfigurationNativeWALRecovery(t *testing.T, v2, file bool) {
	t.Helper()
	fsm := &recoveryConfigurationSnapshotFSM{}
	fixture := newConfigPhaseFixture(t, v2, fsm, mockstorage.NewStorageRecorder(""))
	e := fixture.engine
	e.raftCutoverIndex = orInertCutover(nil)
	disk, err := openDiskState(OpenConfig{DataDir: e.dataDir, Bootstrap: true, StateMachine: fsm}, []Peer{e.peers[1]})
	require.NoError(t, err)
	closed := false
	t.Cleanup(func() {
		if !closed {
			require.NoError(t, closePersist(disk.Persist))
		}
	})
	e.storage, e.persist = disk.Storage, disk.Persist
	if file {
		e.fsmSnapDir = filepath.Join(e.dataDir, fsmSnapDirName)
	}
	configHardState := &raftpb.HardState{Term: proto.Uint64(1), Commit: proto.Uint64(configPhaseIndex)}
	require.NoError(t, disk.Persist.Save(configHardState, []*raftpb.Entry{&fixture.entry}))
	require.NoError(t, disk.Storage.Append([]*raftpb.Entry{&fixture.entry}))
	require.NoError(t, disk.Storage.SetHardState(configHardState))
	e.rawNode = mustRawNode(t, disk.Storage, e.nodeID)
	fsm.deferred = true
	require.NoError(t, e.applyCommitted([]raftpb.Entry{fixture.entry}))
	fixture.requirePublished(t)
	before, err := e.storage.Snapshot()
	require.NoError(t, err)
	require.Equal(t, configPhaseBaseIndex, before.GetMetadata().GetIndex())
	require.Empty(t, before.GetMetadata().GetConfState().GetLearners())

	const checkpoint = configPhaseIndex + 1
	payload := []byte("ordinary-data-after-deferred-configuration")
	entry := raftpb.Entry{Index: proto.Uint64(checkpoint), Term: proto.Uint64(1), Data: encodeProposalEnvelope(99, payload)}
	hardState := &raftpb.HardState{Term: proto.Uint64(1), Commit: proto.Uint64(checkpoint)}
	require.NoError(t, disk.Persist.Save(hardState, []*raftpb.Entry{&entry}))
	require.NoError(t, disk.Storage.Append([]*raftpb.Entry{&entry}))
	require.NoError(t, disk.Storage.SetHardState(hardState))
	require.NoError(t, e.applyCommitted([]raftpb.Entry{entry}))
	fsm.deferred = false
	e.snapshotEvery = 1
	e.snapshotReqCh = make(chan snapshotRequest, 1)
	e.snapshotStopCh = make(chan struct{})
	e.doneCh = make(chan struct{})
	require.NoError(t, e.maybePersistLocalSnapshot())
	var request snapshotRequest
	select {
	case request = <-e.snapshotReqCh:
	default:
		t.Fatal("ordinary checkpoint was not captured")
	}
	require.Equal(t, checkpoint, request.index)
	require.NoError(t, e.persistLocalSnapshot(request))
	require.NoError(t, closePersist(disk.Persist))
	closed = true

	// Read the actual WAL-selected boundary, not just the in-memory snapshot.
	restored := &testStateMachine{}
	selected, err := loadWalState(zap.NewNop(), filepath.Join(e.dataDir, walDirName),
		filepath.Join(e.dataDir, snapDirName), filepath.Join(e.dataDir, fsmSnapDirName), restored, nil)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, closePersist(selected.Persist)) })
	wantConf := &raftpb.ConfState{Voters: []uint64{1}, Learners: []uint64{fixture.joining.NodeID}, AutoLeave: proto.Bool(false)}
	require.Equal(t, checkpoint, selected.LocalSnap.GetMetadata().GetIndex())
	require.True(t, proto.Equal(wantConf, selected.LocalSnap.GetMetadata().GetConfState()),
		"want=%v got=%v", wantConf, selected.LocalSnap.GetMetadata().GetConfState())
	require.Equal(t, file, isSnapshotToken(selected.LocalSnap.GetData()))
	require.Equal(t, [][]byte{payload}, restored.Applied())
	require.NoError(t, closePersist(selected.Persist))
	selected.Persist = nil

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	reopenedFSM := &testStateMachine{}
	reopened, err := Open(ctx, OpenConfig{
		NodeID: e.nodeID, LocalID: e.localID, LocalAddress: e.peers[1].Address,
		DataDir: e.dataDir, StateMachine: reopenedFSM,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, reopened.Close()) })
	current, err := reopened.storage.Snapshot()
	require.NoError(t, err)
	require.Equal(t, checkpoint, current.GetMetadata().GetIndex())
	require.True(t, proto.Equal(wantConf, current.GetMetadata().GetConfState()))
	require.GreaterOrEqual(t, reopened.AppliedIndex(), checkpoint)
	require.Equal(t, [][]byte{payload}, reopenedFSM.Applied())
	require.Equal(t, configurationFromConfState(e.peers, confStateValue(wantConf)), reopened.currentConfiguration())
}

type aheadConfigurationRecoveryFSM struct {
	testStateMachine
	have     uint64
	restores atomic.Uint64
}

func (f *aheadConfigurationRecoveryFSM) LastAppliedIndex() (uint64, bool, error) {
	return f.have, true, nil
}

func (f *aheadConfigurationRecoveryFSM) ParseSnapshotHeader(io.Reader) (uint64, uint64, error) {
	return 0, 0, nil
}

func (f *aheadConfigurationRecoveryFSM) ApplySnapshotHeader(uint64, uint64) {}

func (f *aheadConfigurationRecoveryFSM) Restore(reader io.Reader) error {
	f.restores.Add(1)
	return f.testStateMachine.Restore(reader)
}

func TestSnapshotConfigurationNativeReplayDoesNotSnapshotAheadFSM(t *testing.T) {
	for _, v2 := range []bool{false, true} {
		name := "v1"
		if v2 {
			name = "v2"
		}
		t.Run(name, func(t *testing.T) {
			fixture := newConfigPhaseFixture(t, v2, &testStateMachine{}, mockstorage.NewStorageRecorder(""))
			e := fixture.engine
			disk, err := openDiskState(OpenConfig{DataDir: e.dataDir, Bootstrap: true, StateMachine: &testStateMachine{}}, []Peer{e.peers[1]})
			require.NoError(t, err)
			closed := false
			t.Cleanup(func() {
				if !closed {
					require.NoError(t, closePersist(disk.Persist))
				}
			})
			const have = configPhaseIndex + 1
			payload := []byte("already-durable-at-index-three")
			tail := &raftpb.Entry{Index: proto.Uint64(have), Term: proto.Uint64(1), Data: encodeProposalEnvelope(99, payload)}
			require.NoError(t, disk.Persist.Save(&raftpb.HardState{Term: proto.Uint64(1), Commit: proto.Uint64(have)},
				[]*raftpb.Entry{&fixture.entry, tail}))
			require.NoError(t, closePersist(disk.Persist))
			closed = true
			require.NoError(t, savePersistedPeers(e.dataDir, configPhaseBaseIndex, []Peer{e.peers[1]}))

			fsm := &aheadConfigurationRecoveryFSM{have: have}
			fsm.Apply(payload)
			observer := &recordingObs{}
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			opened, err := Open(ctx, OpenConfig{
				NodeID: 1, LocalID: e.localID, LocalAddress: e.peers[1].Address,
				DataDir: e.dataDir, StateMachine: fsm, ColdStartObserver: observer,
			})
			require.NoError(t, err)
			defer func() { require.NoError(t, opened.Close()) }()
			require.Zero(t, fsm.restores.Load(), "the native token skip gate must keep the ahead FSM")
			observer.mu.Lock()
			skipped := append([]uint64(nil), observer.skipped...)
			observer.mu.Unlock()
			require.Equal(t, []uint64{have - configPhaseBaseIndex}, skipped)
			require.GreaterOrEqual(t, opened.AppliedIndex(), have)
			require.Equal(t, [][]byte{payload}, fsm.Applied(), "normal replay must not duplicate the durable tail")
			current, err := opened.storage.Snapshot()
			require.NoError(t, err)
			require.NotEqual(t, configPhaseIndex, current.GetMetadata().GetIndex(),
				"configuration replay at index 2 must not serialize the already-ahead FSM at index 3 under snapshot metadata index 2")
		})
	}
}
