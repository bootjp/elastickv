package etcd

import (
	"io"
	"sync"
	"testing"

	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/coreos/go-semver/semver"
	"github.com/stretchr/testify/require"
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
	rec *applyIndexOrderRecorder
}

func (p *recordingPersistStorage) SaveSnap(snap raftpb.Snapshot) error {
	p.rec.record("save", snap.Metadata.Index)
	return nil
}

func (p *recordingPersistStorage) Save(_ raftpb.HardState, _ []raftpb.Entry) error { return nil }
func (p *recordingPersistStorage) Release(_ raftpb.Snapshot) error                 { return nil }
func (p *recordingPersistStorage) Sync() error                                     { return nil }
func (p *recordingPersistStorage) Close() error                                    { return nil }
func (p *recordingPersistStorage) MinimalEtcdVersion() *semver.Version             { return nil }

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
