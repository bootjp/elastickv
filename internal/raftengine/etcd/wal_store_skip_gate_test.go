package etcd

import (
	"bytes"
	"encoding/binary"
	"io"
	"os"
	"sync"
	"testing"

	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
	raftpb "go.etcd.io/raft/v3/raftpb"
)

// --- test doubles ---

// skipGateFSM is a state machine that satisfies StateMachine,
// AppliedIndexReader, and SnapshotHeaderApplier — the three
// interfaces the skip gate consults. The applied field controls
// the gate's decision; the header* fields capture observed
// ApplySnapshotHeader calls so assertions can verify the header
// preservation contract.
type skipGateFSM struct {
	applied        uint64
	appliedPresent bool
	appliedErr     error
	parseErr       error
	restoredHeader bool
	parsedCeiling  uint64
	appliedCeiling uint64
	appliedCutover uint64
	bodyBytes      []byte
}

func (f *skipGateFSM) Apply(_ []byte) any          { return nil }
func (f *skipGateFSM) Snapshot() (Snapshot, error) { return nil, io.EOF }
func (f *skipGateFSM) Restore(r io.Reader) error {
	data, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	f.bodyBytes = data
	return nil
}

func (f *skipGateFSM) LastAppliedIndex() (uint64, bool, error) {
	return f.applied, f.appliedPresent, f.appliedErr
}

func (f *skipGateFSM) ParseSnapshotHeader(r io.Reader) (uint64, uint64, error) {
	if f.parseErr != nil {
		return 0, 0, f.parseErr
	}
	// Mimic the real kvFSM contract: parse only the header. The skip path
	// must not drain multi-GiB snapshot bodies just to seed header state.
	hdrLen := 16
	hdr := make([]byte, hdrLen)
	if n, _ := io.ReadFull(r, hdr); n == hdrLen && bytes.HasPrefix(hdr, []byte("EKVTHLC1")) {
		f.parsedCeiling = binary.BigEndian.Uint64(hdr[8:16])
	}
	return f.parsedCeiling, 0, nil
}

func (f *skipGateFSM) ApplySnapshotHeader(ceiling, cutover uint64) {
	f.restoredHeader = true
	f.appliedCeiling = ceiling
	f.appliedCutover = cutover
}

// recordingObs is a ColdStartObserver test double that records every
// callback for later assertion.
type recordingObs struct {
	mu        sync.Mutex
	skipped   []uint64 // gap values reported via RestoreSkipped
	executed  []uint64 // gap values reported via RestoreExecuted
	fallbacks []string // reasons reported via RestoreFallback
}

func (o *recordingObs) RestoreSkipped(snapIndex, have uint64) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.skipped = append(o.skipped, have-snapIndex)
}

func (o *recordingObs) RestoreExecuted(snapIndex, have uint64) {
	o.mu.Lock()
	defer o.mu.Unlock()
	// Mirror the production monitoring.ColdStartObserver semantics:
	// the executed gauge stores |snapIndex - have| because the gate
	// shifted to comparing against the committed-tail (codex P2 #934
	// round 3) and the FSM-ahead-of-snapshot case must not underflow
	// into ~2^64. Claude #934 round 5 caught the test double drift.
	var gap uint64
	if have >= snapIndex {
		gap = have - snapIndex
	} else {
		gap = snapIndex - have
	}
	o.executed = append(o.executed, gap)
}

func (o *recordingObs) RestoreFallback(_ uint64, reason string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.fallbacks = append(o.fallbacks, reason)
}

// --- skip-gate tests ---

// TestSkipGate_SkipsWhenFSMFreshEnough verifies the happy path:
// FSM applied >= snap.Index → skip taken → openAndRestoreFSMSnapshot
// is NOT called (FSM body stays empty), but ApplySnapshotHeader IS
// called with the parsed ceiling.
func TestSkipGate_SkipsWhenFSMFreshEnough(t *testing.T) {
	dir := t.TempDir()
	const (
		ceilingMs  uint64 = 1700_000_000_000
		snapIndex  uint64 = 100
		appliedIdx uint64 = 200
	)
	// Write a fake .fsm with a v1 header (16 bytes: EKVTHLC1 + ceilingMs BE).
	payload := make([]byte, 16)
	copy(payload[:8], "EKVTHLC1")
	binary.BigEndian.PutUint64(payload[8:], ceilingMs)
	crc, _ := writeFSMFileForTest(t, dir, snapIndex, payload)

	fsm := &skipGateFSM{applied: appliedIdx, appliedPresent: true}
	tok := snapshotToken{Index: snapIndex, CRC32C: crc}
	snap := raftpb.Snapshot{
		Data:     encodeSnapshotToken(tok.Index, tok.CRC32C),
		Metadata: testSnapshotMetadata(snapIndex, 0, nil),
	}
	obs := &recordingObs{}
	_, gateErr := restoreSnapshotState(fsm, snap, snap.GetMetadata().GetIndex(), dir, obs, nil)
	require.NoError(t, gateErr)

	require.Empty(t, fsm.bodyBytes, "skip path MUST NOT call fsm.Restore")
	require.True(t, fsm.restoredHeader, "skip path MUST call ApplySnapshotHeader")
	require.Equal(t, ceilingMs, fsm.appliedCeiling, "ceiling MUST survive the skip")
	require.Equal(t, []uint64{appliedIdx - snapIndex}, obs.skipped, "observer MUST record one skip with the gap-ahead value")
	require.Empty(t, obs.executed)
	require.Empty(t, obs.fallbacks)
}

// TestSkipGate_ExecutesWhenFSMStale verifies that when applied <
// snap.Index, the gate does NOT skip — full restore runs, observer
// records the executed outcome with gap-behind, header is restored
// via fsm.Restore (which the fake captures as bodyBytes).
func TestSkipGate_ExecutesWhenFSMStale(t *testing.T) {
	dir := t.TempDir()
	const (
		snapIndex  uint64 = 200
		appliedIdx uint64 = 100
	)
	payload := []byte("fsm-body-bytes")
	crc, _ := writeFSMFileForTest(t, dir, snapIndex, payload)

	fsm := &skipGateFSM{applied: appliedIdx, appliedPresent: true}
	snap := raftpb.Snapshot{
		Data:     encodeSnapshotToken(snapIndex, crc),
		Metadata: testSnapshotMetadata(snapIndex, 0, nil),
	}
	obs := &recordingObs{}
	_, gateErr := restoreSnapshotState(fsm, snap, snap.GetMetadata().GetIndex(), dir, obs, nil)
	require.NoError(t, gateErr)

	require.Equal(t, payload, fsm.bodyBytes, "executed path MUST call fsm.Restore with full payload")
	require.False(t, fsm.restoredHeader, "ApplySnapshotHeader MUST NOT fire on the executed path")
	require.Empty(t, obs.skipped)
	require.Equal(t, []uint64{snapIndex - appliedIdx}, obs.executed)
	require.Empty(t, obs.fallbacks)
}

// TestSkipGate_ReturnsEffectiveAppliedOnSkip pins codex P1 #934
// round 2. The skip path MUST return `have` so Engine.Open can seed
// e.applied above snapshot.Index, preventing applyCommitted from
// re-delivering the snapshot..have tail.
func TestSkipGate_ReturnsEffectiveAppliedOnSkip(t *testing.T) {
	dir := t.TempDir()
	const (
		ceilingMs  uint64 = 1700_000_000_000
		snapIndex  uint64 = 100
		appliedIdx uint64 = 200
	)
	payload := make([]byte, 16)
	copy(payload[:8], "EKVTHLC1")
	binary.BigEndian.PutUint64(payload[8:], ceilingMs)
	crc, _ := writeFSMFileForTest(t, dir, snapIndex, payload)

	fsm := &skipGateFSM{applied: appliedIdx, appliedPresent: true}
	snap := raftpb.Snapshot{
		Data:     encodeSnapshotToken(snapIndex, crc),
		Metadata: testSnapshotMetadata(snapIndex, 0, nil),
	}
	effective, err := restoreSnapshotState(fsm, snap, snap.GetMetadata().GetIndex(), dir, nil, nil)
	require.NoError(t, err)
	require.Equal(t, appliedIdx, effective,
		"skip path MUST return the FSM's durable applied index so Engine.Open seeds e.applied above snapshot.Index")
}

// TestSkipGate_EmitsAfterSuccess pins coderabbit Major #934 (emit-
// after-success). reportColdStart must run AFTER the
// applyHeaderStateOnSkip / openAndRestoreFSMSnapshot path completes,
// not before — otherwise a CRC/header failure would still register
// a successful skip/execute outcome in the soak metrics.
func TestSkipGate_EmitsAfterSuccess(t *testing.T) {
	dir := t.TempDir()
	const snapIndex uint64 = 100
	// Inject a CRC that won't match the on-disk footer so
	// applyHeaderStateOnSkip fails.
	crc, _ := writeFSMFileForTest(t, dir, snapIndex, []byte("body"))
	wrongCRC := crc ^ 0xFFFFFFFF

	fsm := &skipGateFSM{applied: snapIndex + 50, appliedPresent: true}
	snap := raftpb.Snapshot{
		Data:     encodeSnapshotToken(snapIndex, wrongCRC),
		Metadata: testSnapshotMetadata(snapIndex, 0, nil),
	}
	obs := &recordingObs{}
	_, err := restoreSnapshotState(fsm, snap, snap.GetMetadata().GetIndex(), dir, obs, nil)
	require.Error(t, err, "CRC mismatch must surface")
	require.Empty(t, obs.skipped, "skip metric MUST NOT fire when applyHeaderStateOnSkip errors")
	require.Empty(t, obs.executed, "execute metric MUST NOT fire either")
	require.Empty(t, obs.fallbacks)
}

// TestColdStartReplayTarget verifies the committed replay target caps at
// hardState.Commit so a follower carrying an uncommitted WAL suffix does not
// report or initialize from entries raft cannot deliver yet.
func TestColdStartReplayTarget(t *testing.T) {
	t.Parallel()
	mkSnap := func(idx uint64) raftpb.Snapshot {
		return raftTestSnapshot(idx, 0, nil, nil)
	}
	cases := []struct {
		name     string
		snap     raftpb.Snapshot
		hs       raftpb.HardState
		expected uint64
	}{
		{"hardState.Commit equals snapshot (fresh follower)", mkSnap(100), testHardState(0, 100), 100},
		{"hardState.Commit below snapshot (pre-replay)", mkSnap(100), testHardState(0, 50), 100},
		{"hardState.Commit above snapshot (post-replay)", mkSnap(100), testHardState(0, 150), 150},
		{"hardState.Commit zero", mkSnap(100), testHardState(0, 0), 100},
	}
	for _, c := range cases {
		got := coldStartReplayTarget(c.snap, c.hs)
		if got != c.expected {
			t.Errorf("%s: got %d, want %d", c.name, got, c.expected)
		}
	}
}

// TestSkipGate_SkipsWhenWALCarriesPostSnapshotTail verifies a node can skip
// the multi-GiB snapshot restore even when the committed WAL has entries past
// the FSM's durable applied index. Engine.Open seeds e.applied with `have`,
// then RawNode/applyCommitted replays only the needed tail and drops
// data-mutating duplicates at or below `have`.
func TestSkipGate_SkipsWhenWALCarriesPostSnapshotTail(t *testing.T) {
	dir := t.TempDir()
	const (
		snapIndex    uint64 = 100
		appliedIdx   uint64 = 150
		replayTarget uint64 = 200
		ceilingMs    uint64 = 1700_000_000_002
	)
	payload := make([]byte, 16, 16+len("body-bytes-after-header"))
	copy(payload[:8], "EKVTHLC1")
	binary.BigEndian.PutUint64(payload[8:], ceilingMs)
	payload = append(payload, []byte("body-bytes-after-header")...)
	crc, _ := writeFSMFileForTest(t, dir, snapIndex, payload)

	fsm := &skipGateFSM{applied: appliedIdx, appliedPresent: true}
	snap := raftpb.Snapshot{
		Data:     encodeSnapshotToken(snapIndex, crc),
		Metadata: testSnapshotMetadata(snapIndex, 0, nil),
	}
	obs := &recordingObs{}
	effective, gateErr := restoreSnapshotState(fsm, snap, replayTarget, dir, obs, nil)
	require.NoError(t, gateErr)

	require.Empty(t, fsm.bodyBytes, "skip path MUST NOT call fsm.Restore")
	require.True(t, fsm.restoredHeader)
	require.Equal(t, ceilingMs, fsm.appliedCeiling)
	require.Equal(t, appliedIdx, effective)
	require.Equal(t, []uint64{appliedIdx - snapIndex}, obs.skipped)
	require.Empty(t, obs.executed)
	require.Empty(t, obs.fallbacks)
}

// TestSkipGate_FallbackMissingMeta covers the strictly-additive
// fallback when the FSM reports the meta key missing.
func TestSkipGate_FallbackMissingMeta(t *testing.T) {
	dir := t.TempDir()
	payload := []byte("payload")
	crc, _ := writeFSMFileForTest(t, dir, 50, payload)
	fsm := &skipGateFSM{appliedPresent: false} // missing
	snap := raftpb.Snapshot{
		Data:     encodeSnapshotToken(50, crc),
		Metadata: testSnapshotMetadata(50, 0, nil),
	}
	obs := &recordingObs{}
	_, gateErr := restoreSnapshotState(fsm, snap, snap.GetMetadata().GetIndex(), dir, obs, nil)
	require.NoError(t, gateErr)
	require.Equal(t, payload, fsm.bodyBytes, "missing meta MUST fall back to full restore")
	require.Equal(t, []string{"missing_meta"}, obs.fallbacks)
}

// TestSkipGate_FallbackReadErr covers the LastAppliedIndex-error
// path. Engine MUST NOT propagate the error (we collapse to false
// → fallback) — over-restoring is strictly safer.
func TestSkipGate_FallbackReadErr(t *testing.T) {
	dir := t.TempDir()
	payload := []byte("payload")
	crc, _ := writeFSMFileForTest(t, dir, 50, payload)
	fsm := &skipGateFSM{appliedErr: io.ErrUnexpectedEOF}
	snap := raftpb.Snapshot{
		Data:     encodeSnapshotToken(50, crc),
		Metadata: testSnapshotMetadata(50, 0, nil),
	}
	obs := &recordingObs{}
	_, gateErr := restoreSnapshotState(fsm, snap, snap.GetMetadata().GetIndex(), dir, obs, nil)
	require.NoError(t, gateErr)
	require.Equal(t, payload, fsm.bodyBytes, "read_err MUST fall back to full restore")
	require.Equal(t, []string{"read_err"}, obs.fallbacks)
}

// TestSkipGate_FallbackNotReader covers the legacy FSM path: the
// FSM does not implement AppliedIndexReader, so the gate cannot
// even attempt a decision.
func TestSkipGate_FallbackNotReader(t *testing.T) {
	dir := t.TempDir()
	payload := []byte("payload")
	crc, _ := writeFSMFileForTest(t, dir, 50, payload)
	fsm := &dummyFSM{} // no LastAppliedIndex method
	snap := raftpb.Snapshot{
		Data:     encodeSnapshotToken(50, crc),
		Metadata: testSnapshotMetadata(50, 0, nil),
	}
	obs := &recordingObs{}
	_, gateErr := restoreSnapshotState(fsm, snap, snap.GetMetadata().GetIndex(), dir, obs, nil)
	require.NoError(t, gateErr)
	require.NotEmpty(t, fsm.restored, "not_reader MUST fall back to full restore")
	require.Equal(t, []string{"not_reader"}, obs.fallbacks)
}

// --- applyHeaderStateOnSkip CRC failure modes ---

// TestApplyHeaderStateOnSkip_TruncatedFile asserts the size check
// (step 1) catches an empty file and surfaces ErrFSMSnapshotTooSmall
// WITHOUT mutating FSM state.
func TestApplyHeaderStateOnSkip_TruncatedFile(t *testing.T) {
	dir := t.TempDir()
	path := fsmSnapPath(dir, 1)
	require.NoError(t, os.WriteFile(path, []byte{}, 0o600)) //nolint:mnd

	fsm := &skipGateFSM{}
	err := applyHeaderStateOnSkip(fsm, path, 0xDEADBEEF)
	require.ErrorIs(t, err, ErrFSMSnapshotTooSmall)
	require.False(t, fsm.restoredHeader, "FSM state MUST NOT mutate on verification failure")
}

// TestApplyHeaderStateOnSkip_WrongTokenCRC asserts step 2 catches a
// footer-vs-token mismatch.
func TestApplyHeaderStateOnSkip_WrongTokenCRC(t *testing.T) {
	dir := t.TempDir()
	crc, path := writeFSMFileForTest(t, dir, 1, []byte("payload-bytes"))
	_ = crc

	fsm := &skipGateFSM{}
	err := applyHeaderStateOnSkip(fsm, path, 0xBADC0FFE)
	require.ErrorIs(t, err, ErrFSMSnapshotTokenCRC)
	require.False(t, fsm.restoredHeader, "FSM state MUST NOT mutate on verification failure")
}

// TestApplyHeaderStateOnSkip_DoesNotScanBody asserts skip startup cost is
// bounded by the header, not the snapshot body. Body corruption is still
// caught by the full restore path, where the body is actually consumed.
func TestApplyHeaderStateOnSkip_DoesNotScanBody(t *testing.T) {
	dir := t.TempDir()
	const ceilingMs uint64 = 1700_000_000_001
	payload := make([]byte, 16, 16+len("payload-bytes"))
	copy(payload[:8], "EKVTHLC1")
	binary.BigEndian.PutUint64(payload[8:], ceilingMs)
	payload = append(payload, []byte("payload-bytes")...)
	crc, path := writeFSMFileForTest(t, dir, 1, payload)

	// Flip a byte after the fixed 16-byte header. The footer still
	// reads as `crc`, but the on-the-wire content no longer matches
	// it. The skip path should still succeed because it never uses body
	// bytes; the full restore path remains responsible for body CRC.
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	require.NoError(t, err)
	defer f.Close()
	var b [1]byte
	_, err = f.ReadAt(b[:], 16)
	require.NoError(t, err)
	b[0] ^= 0x01
	_, err = f.WriteAt(b[:], 16)
	require.NoError(t, err)

	fsm := &skipGateFSM{}
	err = applyHeaderStateOnSkip(fsm, path, crc)
	require.NoError(t, err)
	require.True(t, fsm.restoredHeader)
	require.Equal(t, ceilingMs, fsm.appliedCeiling)
}

// --- kvFSM header preservation contract ---

// TestSkipGate_KVFSMHeaderRoundTrip verifies the production kvFSM
// satisfies the SnapshotHeaderApplier contract: ParseSnapshotHeader
// returns the v1 ceiling, ApplySnapshotHeader sets f.hlc and
// f.restoredCutover. The cold-start skip path threads them through.
func TestSkipGate_KVFSMHeaderRoundTrip(t *testing.T) {
	const ceilingMs uint64 = 1700_000_000_000

	// 16-byte v1 header followed by inner-store payload.
	const v1HeaderLen = 16
	suffix := []byte("inner-store-bytes-here")
	payload := make([]byte, v1HeaderLen, v1HeaderLen+len(suffix))
	copy(payload[:8], "EKVTHLC1")
	binary.BigEndian.PutUint64(payload[8:], ceilingMs)
	payload = append(payload, suffix...)

	dir := t.TempDir()
	crc, path := writeFSMFileForTest(t, dir, 42, payload)

	// We build a real *kv.kvFSM via NewKvFSMWithHLC so the type-
	// assert in applyHeaderStateOnSkip sees the production type.
	hlc := kv.NewHLC()
	st := store.NewMVCCStore()
	fsm := kv.NewKvFSMWithHLC(st, hlc)
	sm, ok := fsm.(StateMachine)
	require.True(t, ok, "kvFSM must satisfy StateMachine")
	require.NoError(t, applyHeaderStateOnSkip(sm, path, crc))
	require.Equal(t, int64(ceilingMs), hlc.PhysicalCeiling(),
		"applyHeaderStateOnSkip MUST set the HLC ceiling on the production kvFSM")

	// Sanity: the file path is intact (we didn't accidentally delete
	// it via a wrong-cleanup); future test runs can re-open if needed.
	_, statErr := os.Stat(path)
	require.NoError(t, statErr)

	// Also confirm restoredCutover is 0 for v1 (no Stage 8a cutover
	// in the header). RestoredCutover is the public accessor.
	type cutoverer interface {
		RestoredCutover() uint64
	}
	cov, ok := fsm.(cutoverer)
	require.True(t, ok, "kvFSM must expose RestoredCutover()")
	require.Equal(t, uint64(0), cov.RestoredCutover(),
		"v1 header MUST result in restoredCutover=0")
}
