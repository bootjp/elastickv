package etcd

import (
	"io"
	"sync"
	"testing"
	"time"

	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/coreos/go-semver/semver"
	"github.com/stretchr/testify/require"
	etcdraft "go.etcd.io/raft/v3"
	raftpb "go.etcd.io/raft/v3/raftpb"
)

// applyIndexOrderRecorder is shared between the recording FSM and
// the recording persist storage so the test can assert the
// crash-ordering invariant (SetDurableAppliedIndex MUST run before
// persist.SaveSnap). Both record into a single ordered slice keyed
// by event kind; the test reads it back to verify the sequence.
type applyIndexOrderRecorder struct {
	mu     sync.Mutex
	events []orderEvent
}

type orderEvent struct {
	kind  string // "bump" | "save"
	index uint64
}

func (r *applyIndexOrderRecorder) record(kind string, idx uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events = append(r.events, orderEvent{kind: kind, index: idx})
}

func (r *applyIndexOrderRecorder) snapshot() []orderEvent {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]orderEvent, len(r.events))
	copy(out, r.events)
	return out
}

// recordingAppliedIndexFSM implements StateMachine +
// raftengine.AppliedIndexWriter. It records every
// SetDurableAppliedIndex call into the shared recorder.
type recordingAppliedIndexFSM struct {
	rec      *applyIndexOrderRecorder
	failNext bool
	failErr  error
}

func (f *recordingAppliedIndexFSM) Apply(_ []byte) any          { return nil }
func (f *recordingAppliedIndexFSM) Snapshot() (Snapshot, error) { return nil, io.EOF }
func (f *recordingAppliedIndexFSM) Restore(_ io.Reader) error   { return nil }

func (f *recordingAppliedIndexFSM) SetDurableAppliedIndex(idx uint64) error {
	if f.failNext {
		f.failNext = false
		return f.failErr
	}
	f.rec.record("bump", idx)
	return nil
}

// recordingPersistStorage is a minimal etcdstorage.Storage stand-in
// that records SaveSnap calls into the shared recorder. The hook
// only calls SaveSnap + Release; the rest are stubs.
type recordingPersistStorage struct {
	rec             *applyIndexOrderRecorder
	saveStarted     chan struct{}
	saveStartedOnce sync.Once
	saveRelease     <-chan struct{}
}

func (p *recordingPersistStorage) SaveSnap(snap raftpb.Snapshot) error {
	if p.saveStarted != nil {
		p.saveStartedOnce.Do(func() { close(p.saveStarted) })
	}
	if p.saveRelease != nil {
		<-p.saveRelease
	}
	if p.rec != nil {
		p.rec.record("save", snap.Metadata.Index)
	}
	return nil
}

func (p *recordingPersistStorage) Save(_ raftpb.HardState, _ []raftpb.Entry) error { return nil }
func (p *recordingPersistStorage) Release(_ raftpb.Snapshot) error                 { return nil }
func (p *recordingPersistStorage) Sync() error                                     { return nil }
func (p *recordingPersistStorage) Close() error                                    { return nil }
func (p *recordingPersistStorage) MinimalEtcdVersion() *semver.Version             { return nil }

func TestPersistReadyWithSnapshotHoldsSnapshotMuThroughSaveSnap(t *testing.T) {
	saveStarted := make(chan struct{})
	releaseSave := make(chan struct{})
	e := &Engine{
		storage:    etcdraft.NewMemoryStorage(),
		fsm:        &recordingAppliedIndexFSM{},
		persist:    &recordingPersistStorage{saveStarted: saveStarted, saveRelease: releaseSave},
		dataDir:    t.TempDir(),
		fsmSnapDir: t.TempDir(),
	}
	e.protectReceivedFSMSnapshot(7)
	rd := etcdraft.Ready{
		Snapshot: raftpb.Snapshot{
			Data: []byte("payload"),
			Metadata: raftpb.SnapshotMetadata{
				ConfState: raftpb.ConfState{Voters: []uint64{1}},
				Index:     7,
				Term:      1,
			},
		},
	}

	persistDone := make(chan error, 1)
	go func() {
		persistDone <- e.persistReady(rd)
	}()

	select {
	case <-saveStarted:
	case <-time.After(time.Second):
		t.Fatal("SaveSnap did not start")
	}

	prepareDone := make(chan error, 1)
	go func() {
		prepareDone <- e.prepareFSMSnapshotWriteLocked(8)
	}()

	select {
	case err := <-prepareDone:
		t.Fatalf("snapshot prepare finished before SaveSnap released snapshotMu: %v", err)
	case <-time.After(100 * time.Millisecond):
	}

	close(releaseSave)

	select {
	case err := <-persistDone:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("persistReady did not finish after SaveSnap was released")
	}
	select {
	case err := <-prepareDone:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("snapshot prepare did not finish after SaveSnap was released")
	}
	require.Empty(t, e.protectedReceivedFSMSnaps)
}

// TestRecordingFSM_SatisfiesAppliedIndexWriter is a compile-time-
// adjacent assertion: the recording FSM MUST satisfy the writer
// seam so the engine hook actually fires for it.
func TestRecordingFSM_SatisfiesAppliedIndexWriter(t *testing.T) {
	rec := &applyIndexOrderRecorder{}
	var f any = &recordingAppliedIndexFSM{rec: rec}
	_, ok := f.(raftengine.AppliedIndexWriter)
	require.True(t, ok, "recordingAppliedIndexFSM must implement raftengine.AppliedIndexWriter")
}

// TestPersistCreatedSnapshot_BumpsAppliedIndex exercises Site 1 of
// the persist hook. We invoke (*Engine).persistCreatedSnapshot
// directly; the engine MUST call SetDurableAppliedIndex
// (snap.Metadata.Index) BEFORE SaveSnap.
func TestPersistCreatedSnapshot_BumpsAppliedIndex(t *testing.T) {
	rec := &applyIndexOrderRecorder{}
	fsm := &recordingAppliedIndexFSM{rec: rec}
	persist := &recordingPersistStorage{rec: rec}
	e := &Engine{fsm: fsm, persist: persist}

	snap := raftpb.Snapshot{Metadata: raftpb.SnapshotMetadata{Index: 42, Term: 1}}
	require.NoError(t, e.persistCreatedSnapshot(snap))

	require.Equal(t, []orderEvent{
		{kind: "bump", index: 42},
		{kind: "save", index: 42},
	}, rec.snapshot(),
		"hook MUST call SetDurableAppliedIndex BEFORE SaveSnap")
}

// TestPersistCreatedSnapshot_NilFSMNoOp covers the legacy / test-
// fake case: an FSM that does NOT implement AppliedIndexWriter
// silently no-ops; snapshot persist still runs.
func TestPersistCreatedSnapshot_NilFSMNoOp(t *testing.T) {
	rec := &applyIndexOrderRecorder{}
	persist := &recordingPersistStorage{rec: rec}
	// testStateMachine (defined in engine_test.go) is the canonical
	// non-AppliedIndexWriter FSM used by other tests in this package.
	e := &Engine{fsm: &testStateMachine{}, persist: persist}

	snap := raftpb.Snapshot{Metadata: raftpb.SnapshotMetadata{Index: 17, Term: 1}}
	require.NoError(t, e.persistCreatedSnapshot(snap))

	require.Equal(t, []orderEvent{
		{kind: "save", index: 17},
	}, rec.snapshot(),
		"legacy FSM path: snapshot persist still happens, just without the meta-key bump")
}

// TestPersistCreatedSnapshot_BumpErrorAborts checks the ordering
// invariant under failure: if SetDurableAppliedIndex returns an
// error, the engine MUST surface it AND NOT call SaveSnap. This
// preserves the (metaAppliedIndex < snapshot pointer impossible)
// crash invariant from PR #910 design §6.
func TestPersistCreatedSnapshot_BumpErrorAborts(t *testing.T) {
	rec := &applyIndexOrderRecorder{}
	fsm := &recordingAppliedIndexFSM{rec: rec, failNext: true, failErr: io.ErrShortBuffer}
	persist := &recordingPersistStorage{rec: rec}
	e := &Engine{fsm: fsm, persist: persist}

	snap := raftpb.Snapshot{Metadata: raftpb.SnapshotMetadata{Index: 99, Term: 1}}
	err := e.persistCreatedSnapshot(snap)
	require.Error(t, err, "bump failure MUST be surfaced to caller")
	require.Empty(t, rec.snapshot(),
		"failed bump MUST NOT have recorded; SaveSnap MUST NOT have run")
}

// --- Site 2: persistLocalSnapshotPayload (steady-state hot path) ---
//
// These mirror the Site 1 tests above but exercise the engine's
// SnapshotCount-triggered local-snapshot path. The hook sits inside
// e.persistLocalSnapshotPayload (engine.go:4060), under
// e.snapshotMu.Lock(), BEFORE the free-function persistLocalSnapshotPayload
// (wal_store.go:519) which is what actually calls persist.SaveSnap.

// localSnapshotEngine constructs an *Engine suitable for testing
// e.persistLocalSnapshotPayload in isolation. We need:
//   - e.fsm: implements AppliedIndexWriter so the bump hook fires
//   - e.persist: implements etcdstorage.Storage so SaveSnap can run
//     (via the free-function persistLocalSnapshotPayload calling it)
//   - e.storage: a real *etcdraft.MemoryStorage so e.storage.Snapshot()
//     and buildLocalSnapshot return sensible values
//   - e.dataDir / e.fsmSnapDir: temp dirs so the post-persist purge
//     does not panic on Stat
func localSnapshotEngine(t *testing.T, rec *applyIndexOrderRecorder, fsm *recordingAppliedIndexFSM, applied uint64) *Engine {
	t.Helper()
	storage := etcdraft.NewMemoryStorage()
	// Seed the storage with enough entries so buildLocalSnapshot's
	// storage.Term(applied) call succeeds. Without this, the free
	// persistLocalSnapshotPayload short-circuits at Term lookup before
	// reaching persist.SaveSnap, and the test cannot observe the save.
	entries := make([]raftpb.Entry, applied)
	for i := uint64(0); i < applied; i++ {
		entries[i] = raftpb.Entry{Index: i + 1, Term: 1, Data: []byte{}}
	}
	require.NoError(t, storage.Append(entries))
	persist := &recordingPersistStorage{rec: rec}
	return &Engine{
		fsm:        fsm,
		persist:    persist,
		storage:    storage,
		dataDir:    t.TempDir(),
		fsmSnapDir: t.TempDir(),
	}
}

// TestPersistLocalSnapshotPayload_BumpsAppliedIndex is the
// happy-path test for Site 2. The engine MUST call
// SetDurableAppliedIndex(index) BEFORE the free-function
// persistLocalSnapshotPayload — which is what eventually invokes
// persist.SaveSnap.
func TestPersistLocalSnapshotPayload_BumpsAppliedIndex(t *testing.T) {
	rec := &applyIndexOrderRecorder{}
	fsm := &recordingAppliedIndexFSM{rec: rec}
	const index uint64 = 123
	e := localSnapshotEngine(t, rec, fsm, index)

	require.NoError(t, e.persistLocalSnapshotPayload(index, []byte("payload-stub")))

	// Exact slice match (matches Site 1 style + closes claude[bot]
	// round-3 note #4 / coderabbit round-2 nit). A spurious third
	// event — e.g. an accidental double SaveSnap — would fail this
	// assertion; a GreaterOrEqual + positional check would not.
	require.Equal(t, []orderEvent{
		{kind: "bump", index: index},
		{kind: "save", index: index},
	}, rec.snapshot(),
		"hook MUST call SetDurableAppliedIndex BEFORE SaveSnap exactly once each")
}

// TestPersistLocalSnapshotPayload_BumpErrorAborts mirrors Site 1's
// crash-ordering test for Site 2: a failed SetDurableAppliedIndex
// MUST surface the error AND prevent persist.SaveSnap from running.
func TestPersistLocalSnapshotPayload_BumpErrorAborts(t *testing.T) {
	rec := &applyIndexOrderRecorder{}
	fsm := &recordingAppliedIndexFSM{rec: rec, failNext: true, failErr: io.ErrShortBuffer}
	const index uint64 = 456
	e := localSnapshotEngine(t, rec, fsm, index)

	err := e.persistLocalSnapshotPayload(index, []byte("payload-stub"))
	require.Error(t, err, "bump failure MUST be surfaced to caller")
	require.Empty(t, rec.snapshot(),
		"failed bump MUST NOT have recorded; SaveSnap MUST NOT have run")
}
