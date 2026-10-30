package etcd

import (
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/bootjp/elastickv/internal/testutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/server/v3/mock/mockstorage"
	etcdstorage "go.etcd.io/etcd/server/v3/storage"
	etcdraft "go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

func TestEngineSnapshotDiskPhasesLocal(t *testing.T) {
	for _, format := range []string{"inline", "file"} {
		t.Run(format, func(t *testing.T) {
			for _, stage := range []string{"bump", "save", "release", "success"} {
				t.Run(stage, func(t *testing.T) {
					testSnapshotDiskLocal(t, format == "file", stage)
				})
			}
		})
	}
}

func testSnapshotDiskLocal(t *testing.T, file bool, failureStage string) {
	t.Helper()
	e := localSnapshotEngine(t, nil, nil, configPhaseIndex)
	e.applied = configPhaseBaseIndex
	e.appliedIndex.Store(configPhaseBaseIndex)
	e.protectedReceivedFSMSnaps = map[uint64]int{configPhaseBaseIndex: 1, configPhaseIndex + 1: 1}
	walDir := snapshotDiskPurgeFixture(t, e)
	failure := errors.New("injected snapshot disk phase failure")
	var events []string
	visit := func(stage string) error {
		snapshotDiskRequireLocked(t, e)
		require.Equal(t, configPhaseBaseIndex, e.AppliedIndex())
		require.Equal(t, configPhaseBaseIndex, e.applied)
		require.Equal(t, map[uint64]int{configPhaseBaseIndex: 1, configPhaseIndex + 1: 1}, e.protectedReceivedFSMSnaps)
		require.Len(t, listWALFiles(t, walDir), 2, "purge must follow successful Release")
		first, err := e.storage.FirstIndex()
		require.NoError(t, err)
		if stage == "release" {
			require.Equal(t, configPhaseIndex+1, first, "local compaction must precede Release")
		} else {
			require.Equal(t, configPhaseBaseIndex, first, "local compaction must follow SaveSnap")
		}
		events = append(events, stage)
		if stage == failureStage {
			return failure
		}
		return nil
	}
	fsm := &configPhaseFSM{visit: visit, durableIndex: configPhaseBaseIndex}
	e.fsm = fsm
	e.persist = &configPhaseStorage{Storage: mockstorage.NewStorageRecorder(""), visit: visit}
	payload := []byte("local snapshot prefix")
	var err error
	if file {
		err = e.persistLocalSnapshot(snapshotRequest{index: configPhaseIndex, snapshot: &testSnapshot{data: payload}})
		// The existing file-capture phase precedes the durable-index callback.
		// A failed bump must prevent raft SaveSnap/Release, not undo capture.
		require.FileExists(t, fsmSnapPath(e.fsmSnapDir, configPhaseIndex))
	} else {
		err = e.persistLocalSnapshotPayload(configPhaseIndex, payload)
	}
	snapshotDiskRequireResult(t, err, failure, failureStage, []string{"bump", "save", "release"}, events)
	if failureStage == "bump" {
		require.Equal(t, configPhaseBaseIndex, fsm.durableIndex)
	} else {
		require.Equal(t, configPhaseIndex, fsm.durableIndex)
	}
	current, snapshotErr := e.storage.Snapshot()
	require.NoError(t, snapshotErr)
	if failureStage == "bump" || failureStage == "save" {
		require.Zero(t, current.GetMetadata().GetIndex(), "failed SaveSnap must not publish MemoryStorage snapshot")
	} else {
		require.Equal(t, configPhaseIndex, current.GetMetadata().GetIndex())
		if file {
			token, decodeErr := decodeSnapshotToken(current.GetData())
			require.NoError(t, decodeErr)
			require.Equal(t, configPhaseIndex, token.Index)
			got, readErr := readFSMSnapshotPayload(fsmSnapPath(e.fsmSnapDir, token.Index))
			require.NoError(t, readErr)
			require.Equal(t, payload, got)
		} else {
			require.Equal(t, payload, current.GetData())
		}
	}
	snapshotDiskRequireCleanup(t, e, walDir, failureStage == "success")
	require.Equal(t, configPhaseBaseIndex, e.AppliedIndex(), "snapshot persistence must not publish owner applied state")
}

func TestEngineSnapshotDiskPhasesConfiguration(t *testing.T) {
	for _, version := range []string{"v1", "v2"} {
		t.Run(version, func(t *testing.T) {
			for _, stage := range []string{"bump", "save", "release", "success"} {
				t.Run(stage, func(t *testing.T) {
					testSnapshotDiskConfiguration(t, version == "v2", stage)
				})
			}
		})
	}
}

func testSnapshotDiskConfiguration(t *testing.T, v2 bool, failureStage string) {
	t.Helper()
	var fixture configPhaseFixture
	var walDir string
	var events []string
	failure := errors.New("injected configuration snapshot disk failure")
	visit := func(stage string) error {
		fixture.requireUnpublished(t)
		if stage != "record" {
			snapshotDiskRequireLocked(t, fixture.engine)
		}
		require.Equal(t, map[uint64]int{configPhaseBaseIndex: 1, configPhaseIndex + 1: 1}, fixture.engine.protectedReceivedFSMSnaps)
		require.Len(t, listWALFiles(t, walDir), 2, "purge must follow successful Release")
		first, err := fixture.engine.storage.FirstIndex()
		require.NoError(t, err)
		require.Equal(t, configPhaseIndex, first, "configuration persistence must not compact MemoryStorage")
		events = append(events, stage)
		if stage == failureStage {
			return failure
		}
		return nil
	}
	fsm := &configPhaseFSM{visit: visit, configurationIndex: configPhaseBaseIndex, durableIndex: configPhaseBaseIndex}
	persist := &configPhaseStorage{Storage: mockstorage.NewStorageRecorder(""), visit: visit}
	fixture = newConfigPhaseFixture(t, v2, fsm, persist)
	fixture.engine.protectedReceivedFSMSnaps = map[uint64]int{configPhaseBaseIndex: 1, configPhaseIndex + 1: 1}
	walDir = snapshotDiskPurgeFixture(t, fixture.engine)
	err := fixture.engine.applyCommitted([]raftpb.Entry{fixture.entry})
	snapshotDiskRequireResult(t, err, failure, failureStage, []string{"record", "snapshot", "bump", "save", "release"}, events)
	if failureStage == "success" {
		fixture.requirePublished(t)
	} else {
		fixture.requireUnpublished(t)
	}
	snapshotDiskRequireCleanup(t, fixture.engine, walDir, failureStage == "success")
}

func TestEngineSnapshotDiskPhasesNoCallbacks(t *testing.T) {
	for _, path := range []string{"created", "local"} {
		t.Run(path, func(t *testing.T) {
			e := localSnapshotEngine(t, nil, nil, configPhaseIndex)
			e.fsm = &configPhaseFSM{visit: func(stage string) error {
				t.Fatalf("disk phase invoked FSM callback %q", stage)
				return nil
			}}
			e.protectedReceivedFSMSnaps = map[uint64]int{configPhaseBaseIndex: 1}
			var events []string
			e.persist = &configPhaseStorage{Storage: mockstorage.NewStorageRecorder(""), visit: func(stage string) error {
				events = append(events, stage)
				return nil
			}}
			disk := e.snapshotDiskPhase()
			e.snapshotMu.Lock()
			defer e.snapshotMu.Unlock()
			if path == "created" {
				require.NoError(t, disk.persistCreated(appliedIndexTestSnapshot(configPhaseIndex, []byte("snapshot"))))
			} else {
				snapshot := appliedIndexTestSnapshot(configPhaseIndex, []byte("snapshot"))
				before, err := e.storage.Snapshot()
				require.NoError(t, err)
				require.NoError(t, disk.save(&snapshot))
				require.NoError(t, disk.release(&snapshot))
				disk.purge()
				after, err := e.storage.Snapshot()
				require.NoError(t, err)
				require.Equal(t, before, after, "disk operations must not publish MemoryStorage")
			}
			require.Equal(t, []string{"save", "release"}, events)
			require.Equal(t, map[uint64]int{configPhaseBaseIndex: 1}, e.protectedReceivedFSMSnaps)
			require.Zero(t, e.AppliedIndex())
			require.Zero(t, e.currentConfigIndex())
		})
	}
}

func snapshotDiskPurgeFixture(t *testing.T, e *Engine) string {
	t.Helper()
	e.maxWALFiles = 1
	dir := filepath.Join(e.dataDir, walDirName)
	require.NoError(t, os.MkdirAll(dir, 0o700))
	writeDummyWALSegment(t, dir, 0, 0)
	writeDummyWALSegment(t, dir, 1, 1)
	return dir
}

func snapshotDiskRequireLocked(t *testing.T, e *Engine) {
	t.Helper()
	if e.snapshotMu.TryLock() {
		e.snapshotMu.Unlock()
		t.Fatal("snapshotMu was released across synchronous snapshot phases")
	}
}

func snapshotDiskRequireResult(t *testing.T, err, failure error, failureStage string, sequence, events []string) {
	t.Helper()
	if failureStage == "success" {
		require.NoError(t, err)
	} else {
		require.ErrorIs(t, err, failure)
		for i, stage := range sequence {
			if stage == failureStage {
				sequence = sequence[:i+1]
				break
			}
		}
	}
	require.Equal(t, sequence, events)
}

func snapshotDiskRequireCleanup(t *testing.T, e *Engine, walDir string, success bool) {
	t.Helper()
	if success {
		require.Len(t, listWALFiles(t, walDir), 1)
		require.Equal(t, map[uint64]int{configPhaseIndex + 1: 1}, e.protectedReceivedFSMSnaps)
	} else {
		require.Len(t, listWALFiles(t, walDir), 2)
		require.Equal(t, map[uint64]int{configPhaseBaseIndex: 1, configPhaseIndex + 1: 1}, e.protectedReceivedFSMSnaps)
	}
}

type snapshotPhaseCallbackStorage struct {
	etcdstorage.Storage
	beforeSave    func(*raftpb.Snapshot) error
	beforeRelease func(*raftpb.Snapshot) error
}

func (s *snapshotPhaseCallbackStorage) SaveSnap(snapshot *raftpb.Snapshot) error {
	if err := s.beforeSave(snapshot); err != nil {
		return err
	}
	return s.Storage.SaveSnap(snapshot)
}

func (s *snapshotPhaseCallbackStorage) Release(snapshot *raftpb.Snapshot) error {
	if err := s.beforeRelease(snapshot); err != nil {
		return err
	}
	return s.Storage.Release(snapshot)
}

type snapshotPhaseCompactionFault struct {
	*etcdraft.MemoryStorage
	err error
}

func (s *snapshotPhaseCompactionFault) Compact(index uint64) error {
	if s.err != nil {
		return s.err
	}
	return s.MemoryStorage.Compact(index)
}

func TestLocalSnapshotPersistenceInterleavings(t *testing.T) {
	failure := errors.New("injected snapshot persistence failure")
	for _, tc := range []struct {
		name        string
		mutate      func(*testing.T, *etcdraft.MemoryStorage)
		err         error
		release     bool
		memoryIndex uint64
		firstIndex  uint64
		memoryTerm  uint64
	}{
		{name: "success", release: true, memoryIndex: 2, firstIndex: 3, memoryTerm: 1},
		{name: "save_error", err: failure, firstIndex: 1},
		{name: "release_error", err: failure, release: true, memoryIndex: 2, firstIndex: 3, memoryTerm: 1},
		{name: "compaction_error", err: failure, memoryIndex: 2, firstIndex: 1, memoryTerm: 1},
		{
			name: "publication_out_of_date", err: etcdraft.ErrSnapOutOfDate, memoryIndex: 3, firstIndex: 1, memoryTerm: 1,
			mutate: func(t *testing.T, storage *etcdraft.MemoryStorage) {
				t.Helper()
				_, err := storage.CreateSnapshot(3, &raftpb.ConfState{Voters: []uint64{9}}, []byte("newer"))
				require.NoError(t, err)
			},
		},
		{
			name: "compaction_already_done", release: true, memoryIndex: 2, firstIndex: 3, memoryTerm: 1,
			mutate: func(t *testing.T, storage *etcdraft.MemoryStorage) {
				t.Helper()
				require.NoError(t, storage.Compact(configPhaseIndex))
			},
		},
		{
			name: "configuration_changed_during_save", release: true, memoryIndex: 2, firstIndex: 3, memoryTerm: 1,
			mutate: func(t *testing.T, storage *etcdraft.MemoryStorage) {
				t.Helper()
				_, err := storage.CreateSnapshot(1, &raftpb.ConfState{Voters: []uint64{9}}, []byte("different prefix"))
				require.NoError(t, err)
			},
		},
		{
			name: "term_changed_during_save", release: true, memoryIndex: 2, firstIndex: 3, memoryTerm: 9,
			mutate: func(t *testing.T, storage *etcdraft.MemoryStorage) {
				t.Helper()
				require.NoError(t, storage.Append(entryPointers([]raftpb.Entry{testEntry(2, 9, nil), testEntry(3, 9, nil)})))
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			storage := snapshotPhaseMemoryStorage(t)
			fault := &snapshotPhaseCompactionFault{MemoryStorage: storage}
			var saved *raftpb.Snapshot
			var captured *raftpb.Snapshot
			var events []string
			persist := &snapshotPhaseCallbackStorage{
				Storage: mockstorage.NewStorageRecorder(""),
				beforeSave: func(snapshot *raftpb.Snapshot) error {
					events = append(events, "save")
					saved = snapshot
					var ok bool
					captured, ok = proto.Clone(snapshot).(*raftpb.Snapshot)
					require.True(t, ok)
					require.Equal(t, configPhaseIndex, captured.GetMetadata().GetIndex())
					require.Equal(t, uint64(1), captured.GetMetadata().GetTerm())
					require.Equal(t, []uint64{1}, captured.GetMetadata().GetConfState().GetVoters())
					require.Equal(t, []byte("prefix"), captured.GetData())
					if tc.mutate != nil {
						tc.mutate(t, storage)
					}
					if tc.name == "compaction_error" {
						fault.err = failure
					}
					if tc.name == "save_error" {
						return failure
					}
					return nil
				},
				beforeRelease: func(snapshot *raftpb.Snapshot) error {
					events = append(events, "release")
					require.Same(t, saved, snapshot, "Release must use the snapshot passed to SaveSnap")
					require.True(t, proto.Equal(captured, snapshot), "Release must not re-read newer MemoryStorage metadata")
					first, err := storage.FirstIndex()
					require.NoError(t, err)
					require.Equal(t, configPhaseIndex+1, first)
					if tc.name == "release_error" {
						return failure
					}
					return nil
				},
			}
			var result raftpb.Snapshot
			var err error
			if tc.name == "compaction_error" {
				var snapshot raftpb.Snapshot
				snapshot, err = buildLocalSnapshot(storage, configPhaseIndex, []byte("prefix"))
				require.NoError(t, err)
				result, err = persistCapturedLocalSnapshot(fault, persist, snapshot)
			} else {
				result, err = persistLocalSnapshotPayload(storage, persist, configPhaseIndex, []byte("prefix"))
			}
			if tc.err == nil {
				require.NoError(t, err)
				require.True(t, proto.Equal(captured, &result))
			} else {
				require.ErrorIs(t, err, tc.err)
				require.True(t, proto.Equal(&raftpb.Snapshot{}, &result), "failure must return an empty snapshot")
			}
			wantEvents := []string{"save"}
			if tc.release {
				wantEvents = append(wantEvents, "release")
			}
			require.Equal(t, wantEvents, events)
			snapshotPhaseRequireMemoryState(t, storage, tc.memoryIndex, tc.memoryTerm, tc.firstIndex)
		})
	}
}

func snapshotPhaseRequireMemoryState(t *testing.T, storage *etcdraft.MemoryStorage, index, term, firstIndex uint64) {
	t.Helper()
	current, err := storage.Snapshot()
	require.NoError(t, err)
	require.Equal(t, index, current.GetMetadata().GetIndex())
	if index != 0 {
		require.Equal(t, term, current.GetMetadata().GetTerm())
	}
	first, err := storage.FirstIndex()
	require.NoError(t, err)
	require.Equal(t, firstIndex, first)
}

func snapshotPhaseMemoryStorage(t *testing.T) *etcdraft.MemoryStorage {
	t.Helper()
	storage := etcdraft.NewMemoryStorage()
	baseline := appliedIndexTestSnapshot(0, nil)
	require.NoError(t, storage.ApplySnapshot(&baseline))
	require.NoError(t, storage.Append(entryPointers([]raftpb.Entry{
		testEntry(1, 1, nil), testEntry(2, 1, nil), testEntry(3, 1, nil),
	})))
	return storage
}

func TestLocalSnapshotPersistenceBuildErrors(t *testing.T) {
	for _, compacted := range []bool{false, true} {
		t.Run(map[bool]string{false: "unavailable", true: "compacted"}[compacted], func(t *testing.T) {
			storage := snapshotPhaseMemoryStorage(t)
			index, wantErr := uint64(4), etcdraft.ErrUnavailable
			if compacted {
				require.NoError(t, storage.Compact(configPhaseIndex))
				index, wantErr = configPhaseBaseIndex, etcdraft.ErrCompacted
			}
			persist := mockstorage.NewStorageRecorder("")
			snapshot, err := persistLocalSnapshotPayload(storage, persist, index, []byte("prefix"))
			require.ErrorIs(t, err, wantErr)
			require.True(t, proto.Equal(&raftpb.Snapshot{}, &snapshot))
			require.Empty(t, persist.Action(), "failed capture must prevent every disk operation")
		})
	}
}

func TestEngineSnapshotDiskPhasesLocalPublicationOutOfDate(t *testing.T) {
	e := localSnapshotEngine(t, nil, &recordingAppliedIndexFSM{}, configPhaseIndex+1)
	e.appliedIndex.Store(configPhaseBaseIndex)
	e.protectedReceivedFSMSnaps = map[uint64]int{configPhaseBaseIndex: 1, configPhaseIndex + 1: 1}
	walDir := snapshotDiskPurgeFixture(t, e)
	e.persist = &snapshotPhaseCallbackStorage{
		Storage: mockstorage.NewStorageRecorder(""),
		beforeSave: func(*raftpb.Snapshot) error {
			_, err := e.storage.CreateSnapshot(configPhaseIndex+1, nil, []byte("newer"))
			return err
		},
		beforeRelease: func(*raftpb.Snapshot) error {
			t.Fatal("publication error must prevent Release")
			return nil
		},
	}
	// The Engine historically treats these stale-snapshot errors as benign
	// no-ops, releasing covered pins but skipping the success-only disk purge.
	require.NoError(t, e.persistLocalSnapshotPayload(configPhaseIndex, []byte("prefix")))
	require.Equal(t, map[uint64]int{configPhaseIndex + 1: 1}, e.protectedReceivedFSMSnaps)
	require.Len(t, listWALFiles(t, walDir), 2)
	require.Equal(t, configPhaseBaseIndex, e.AppliedIndex())
	snapshotPhaseRequireMemoryState(t, e.storage, configPhaseIndex+1, 1, 1)
}

func TestEngineSnapshotDiskPhasesLocalWaitsForRelease(t *testing.T) {
	e := localSnapshotEngine(t, nil, &recordingAppliedIndexFSM{}, configPhaseIndex)
	walDir := snapshotDiskPurgeFixture(t, e)
	e.protectedReceivedFSMSnaps = map[uint64]int{configPhaseBaseIndex: 1, configPhaseIndex + 1: 1}
	saveEntered, allowSave := make(chan struct{}), make(chan struct{})
	releaseEntered, allowRelease := make(chan struct{}), make(chan struct{})
	unblockSave := sync.OnceFunc(func() { close(allowSave) })
	unblockRelease := sync.OnceFunc(func() { close(allowRelease) })
	e.persist = &snapshotPhaseCallbackStorage{
		Storage: mockstorage.NewStorageRecorder(""),
		beforeSave: func(*raftpb.Snapshot) error {
			close(saveEntered)
			<-allowSave
			return nil
		},
		beforeRelease: func(*raftpb.Snapshot) error {
			close(releaseEntered)
			<-allowRelease
			return nil
		},
	}
	e.snapshotReqCh = make(chan snapshotRequest, 1)
	e.snapshotResCh = make(chan snapshotResult, 1)
	e.snapshotStopCh = make(chan struct{})
	e.startSnapshotWorker()
	t.Cleanup(func() {
		unblockSave()
		unblockRelease()
		e.stopSnapshotWorker()
	})
	require.NoError(t, e.enqueueSnapshotRequest(snapshotRequest{index: configPhaseIndex, snapshot: &testSnapshot{data: []byte("prefix")}}))
	testutil.Closed(t, saveEntered, time.Second, "SaveSnap did not start")
	require.Empty(t, e.snapshotResCh, "worker must not publish a receipt before SaveSnap completes")
	snapshotDiskRequireLocked(t, e)
	current, err := e.storage.Snapshot()
	require.NoError(t, err)
	require.Zero(t, current.GetMetadata().GetIndex())
	unblockSave()
	testutil.Closed(t, releaseEntered, time.Second, "Release did not start")
	require.Empty(t, e.snapshotResCh, "MemoryStorage publication alone must not publish a worker receipt")
	snapshotDiskRequireLocked(t, e)
	current, err = e.storage.Snapshot()
	require.NoError(t, err)
	require.Equal(t, configPhaseIndex, current.GetMetadata().GetIndex())
	first, err := e.storage.FirstIndex()
	require.NoError(t, err)
	require.Equal(t, configPhaseIndex+1, first)
	snapshotDiskRequireCleanup(t, e, walDir, false)
	unblockRelease()
	select {
	case result := <-e.snapshotResCh:
		require.NoError(t, result.err)
		require.Equal(t, configPhaseIndex, result.index)
	case <-time.After(time.Second):
		t.Fatal("worker did not publish its receipt after Release")
	}
	snapshotDiskRequireCleanup(t, e, walDir, true)
}
