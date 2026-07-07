package main

import (
	"bytes"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/bootjp/elastickv/internal/encryption"
	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/store"
)

// stubGapEngine satisfies encryptionGapEngine for the
// runSidecarBehindRaftLogGuard tests.
type stubGapEngine struct {
	appliedIndex uint64
	scanner      encryption.EncryptionRelevantScanner
}

func (s *stubGapEngine) AppliedIndex() uint64 { return s.appliedIndex }
func (s *stubGapEngine) EncryptionScanner() encryption.EncryptionRelevantScanner {
	return s.scanner
}

// stubScanner is a fake encryption.EncryptionRelevantScanner that
// returns a fixed verdict. Lets the guard tests exercise the
// hit / no-hit / error branches without a real raftengine.
type stubScanner struct {
	hit bool
	err error
}

func (s *stubScanner) HasEncryptionRelevantEntryInRange(_, _ uint64) (bool, error) {
	return s.hit, s.err
}

type stubRestoredCutoverStateMachine struct {
	cutover uint64
}

func (s *stubRestoredCutoverStateMachine) Apply([]byte) any { return nil }
func (s *stubRestoredCutoverStateMachine) Snapshot() (raftengine.Snapshot, error) {
	return nil, nil
}
func (s *stubRestoredCutoverStateMachine) Restore(io.Reader) error { return nil }
func (s *stubRestoredCutoverStateMachine) RestoredCutover() uint64 {
	return s.cutover
}

type testCutoverSource struct {
	cutover uint64
}

func (s testCutoverSource) RaftEnvelopeCutoverIndex() uint64 { return s.cutover }

func snapshotPayloadWithCutover(t *testing.T, cutover uint64) []byte {
	t.Helper()
	fsm := kv.NewKvFSMWithHLC(store.NewMVCCStore(), kv.NewHLC(), kv.WithCutoverSource(testCutoverSource{cutover: cutover}))
	snap, err := fsm.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	defer snap.Close()
	var buf bytes.Buffer
	if _, err := snap.WriteTo(&buf); err != nil {
		t.Fatalf("snapshot WriteTo: %v", err)
	}
	return buf.Bytes()
}

func TestCheckEnvelopeCutoverDivergenceSnapshotPayload_SidecarZeroRefusesSnapshotCutover(t *testing.T) {
	payload := snapshotPayloadWithCutover(t, 99)
	err := checkEnvelopeCutoverDivergenceSnapshotPayload(bytes.NewReader(payload), 0, 1)
	if !errors.Is(err, encryption.ErrEnvelopeCutoverDivergence) {
		t.Fatalf("checkEnvelopeCutoverDivergenceSnapshotPayload error = %v, want ErrEnvelopeCutoverDivergence", err)
	}
}

// writeMinimalSidecar writes a valid §5.1 sidecar with the
// supplied RaftAppliedIndex into a freshly created temp dir, and
// returns the sidecar path.
func writeMinimalSidecar(t *testing.T, raftAppliedIdx uint64) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, encryption.SidecarFilename)
	sc := &encryption.Sidecar{
		Version:          encryption.SidecarVersion,
		RaftAppliedIndex: raftAppliedIdx,
		Keys:             map[string]encryption.SidecarKey{},
	}
	if err := encryption.WriteSidecar(path, sc); err != nil {
		t.Fatalf("WriteSidecar: %v", err)
	}
	return path
}

func writeRaftCutoverSidecarForStartup(t *testing.T, activeRaft uint32, localEpoch uint16, cutover uint64) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, encryption.SidecarFilename)
	sc := &encryption.Sidecar{
		Version:                  encryption.SidecarVersion,
		RaftAppliedIndex:         cutover,
		StorageEnvelopeActive:    true,
		RaftEnvelopeCutoverIndex: cutover,
		Active:                   encryption.ActiveKeys{Storage: testRegDEKID, Raft: activeRaft},
		Keys: map[string]encryption.SidecarKey{
			strconv.FormatUint(uint64(testRegDEKID), 10): {
				Purpose:    encryption.SidecarPurposeStorage,
				Wrapped:    []byte("wrapped-storage"),
				Created:    "2026-07-07T00:00:00Z",
				LocalEpoch: localEpoch,
			},
			strconv.FormatUint(uint64(activeRaft), 10): {
				Purpose: encryption.SidecarPurposeRaft,
				Wrapped: []byte("wrapped-raft"),
				Created: "2026-07-07T00:00:00Z",
			},
		},
	}
	if err := encryption.WriteSidecar(path, sc); err != nil {
		t.Fatalf("WriteSidecar: %v", err)
	}
	return path
}

// TestCheckSidecarBehindRaftLog_DisabledNoop pins the
// fast-skip when --encryption-enabled is off. Even with a stale
// sidecar on disk, the guard MUST return nil and never read it.
func TestCheckSidecarBehindRaftLog_DisabledNoop(t *testing.T) {
	sidecarPath := writeMinimalSidecar(t, 0) // way behind any engine
	err := checkSidecarBehindRaftLog(nil, 1, sidecarPath, false)
	if err != nil {
		t.Fatalf("guard must skip when encryptionEnabled=false; got %v", err)
	}
}

// TestCheckSidecarBehindRaftLog_NoSidecarPathNoop pins the
// empty-path fast-skip.
func TestCheckSidecarBehindRaftLog_NoSidecarPathNoop(t *testing.T) {
	err := checkSidecarBehindRaftLog(nil, 1, "", true)
	if err != nil {
		t.Fatalf("guard must skip on empty sidecar path; got %v", err)
	}
}

// TestCheckSidecarBehindRaftLog_SidecarAbsentNoop pins the
// "no on-disk sidecar" fast-skip. A configured sidecar path with
// no file means bootstrap hasn't committed — no gap to refuse on.
func TestCheckSidecarBehindRaftLog_SidecarAbsentNoop(t *testing.T) {
	dir := t.TempDir()
	err := checkSidecarBehindRaftLog(nil, 1, filepath.Join(dir, "nonexistent.json"), true)
	if err != nil {
		t.Fatalf("guard must skip when sidecar file is absent; got %v", err)
	}
}

// TestCheckSidecarBehindRaftLog_SidecarStatError surfaces a real
// I/O error (path with NUL byte) as a wrapped error rather than
// silently classifying it as "sidecar absent".
func TestCheckSidecarBehindRaftLog_SidecarStatError(t *testing.T) {
	err := checkSidecarBehindRaftLog(nil, 1, "/tmp/elastickv-test/\x00invalid", true)
	if err == nil {
		t.Fatal("guard must surface I/O error from sidecar stat")
	}
	if errors.Is(err, os.ErrNotExist) {
		t.Errorf("invalid path must NOT be silently treated as not-exist: %v", err)
	}
}

// TestCheckSidecarBehindRaftLog_NoRuntimes returns nil when the
// runtimes slice is empty or no entry matches the default group
// id. Production callers always supply at least the default
// group's runtime, but the defensive return prevents a nil-deref
// on misconfigured shard maps.
func TestCheckSidecarBehindRaftLog_NoRuntimes(t *testing.T) {
	sidecarPath := writeMinimalSidecar(t, 10)
	err := checkSidecarBehindRaftLog(nil, 1, sidecarPath, true)
	if err != nil {
		t.Fatalf("guard must skip when no runtimes match default group; got %v", err)
	}
}

// TestCheckSidecarBehindRaftLog_NilEngineFailsClosed verifies
// that a present default-group runtime whose engine has not been
// constructed (nil engine field after buildShardGroups) is
// reported as an error rather than silently passing the guard.
// Rationale: at this point in startup the runtime existed but
// the engine opener failed without surfacing an error, so the
// node never finished coming up. Silently returning nil here
// would let the guard pass on a node that cannot serve, defeating
// the §9.1 fail-closed contract.
func TestCheckSidecarBehindRaftLog_NilEngineFailsClosed(t *testing.T) {
	sidecarPath := writeMinimalSidecar(t, 10)
	rt := &raftGroupRuntime{spec: groupSpec{id: 1}} // engine field stays nil
	err := checkSidecarBehindRaftLog([]*raftGroupRuntime{rt}, 1, sidecarPath, true)
	if err == nil {
		t.Fatal("guard must fail-closed when default-group engine is nil")
	}
	if errors.Is(err, encryption.ErrSidecarBehindRaftLog) {
		t.Errorf("nil-engine must NOT surface as ErrSidecarBehindRaftLog; got %v", err)
	}
}

// TestRunSidecarBehindRaftLogGuard_CaughtUp pins the
// "sidecar already past engine" no-op path in the per-engine
// inner function.
func TestRunSidecarBehindRaftLogGuard_CaughtUp(t *testing.T) {
	sidecarPath := writeMinimalSidecar(t, 100) // ahead of engine
	gap := &stubGapEngine{
		appliedIndex: 50,
		scanner:      &stubScanner{hit: true}, // would fire if consulted
	}
	err := runSidecarBehindRaftLogGuard(gap, sidecarPath, 1)
	if err != nil {
		t.Fatalf("guard must pass when sidecar is caught up; got %v", err)
	}
}

// TestRunSidecarBehindRaftLogGuard_GapNotCovered pins the
// "behind but harmless" path.
func TestRunSidecarBehindRaftLogGuard_GapNotCovered(t *testing.T) {
	sidecarPath := writeMinimalSidecar(t, 10)
	gap := &stubGapEngine{
		appliedIndex: 50,
		scanner:      &stubScanner{hit: false},
	}
	err := runSidecarBehindRaftLogGuard(gap, sidecarPath, 1)
	if err != nil {
		t.Fatalf("guard must pass when gap has no relevant entries; got %v", err)
	}
}

// TestRunSidecarBehindRaftLogGuard_GapCovered pins the fire
// path: gap covers a relevant entry → ErrSidecarBehindRaftLog
// with the sidecar path + default_group annotation that
// operators see in the log line.
func TestRunSidecarBehindRaftLogGuard_GapCovered(t *testing.T) {
	sidecarPath := writeMinimalSidecar(t, 10)
	gap := &stubGapEngine{
		appliedIndex: 50,
		scanner:      &stubScanner{hit: true},
	}
	err := runSidecarBehindRaftLogGuard(gap, sidecarPath, 1)
	if !errors.Is(err, encryption.ErrSidecarBehindRaftLog) {
		t.Fatalf("guard must fire ErrSidecarBehindRaftLog when gap covers a relevant entry; got %v", err)
	}
}

// TestRunSidecarBehindRaftLogGuard_SidecarIndexZero_SkipsTransitionally
// pins the Stage 6C-2d skip-when-zero gate: until the §6.3 applier
// advances `sidecar.raft_applied_index` on Apply, an encrypted
// sidecar persists with index=0 and firing the guard against
// (0, engine.applied] would refuse every restart of an encrypted
// cluster on historical bootstrap/rotation entries. The guard MUST
// return nil for this transitional case even when the scanner
// would otherwise classify the range as relevant (hit=true).
func TestRunSidecarBehindRaftLogGuard_SidecarIndexZero_SkipsTransitionally(t *testing.T) {
	sidecarPath := writeMinimalSidecar(t, 0) // applier-side advancement not yet shipped
	gap := &stubGapEngine{
		appliedIndex: 50,
		scanner:      &stubScanner{hit: true}, // would fire if consulted
	}
	err := runSidecarBehindRaftLogGuard(gap, sidecarPath, 1)
	if err != nil {
		t.Fatalf("guard MUST skip when sidecar.raft_applied_index=0 (applier-side advancement is a 6C-2e follow-up); got %v", err)
	}
}

// TestRunSidecarBehindRaftLogGuard_ScannerError pins the
// scanner-error propagation path: the wrapped error is NOT
// marked with ErrSidecarBehindRaftLog (operator triages
// scanner failure separately).
func TestRunSidecarBehindRaftLogGuard_ScannerError(t *testing.T) {
	sidecarPath := writeMinimalSidecar(t, 10)
	scanErr := errors.New("simulated WAL corruption")
	gap := &stubGapEngine{
		appliedIndex: 50,
		scanner:      &stubScanner{err: scanErr},
	}
	err := runSidecarBehindRaftLogGuard(gap, sidecarPath, 1)
	if err == nil {
		t.Fatal("scanner error must propagate, got nil")
	}
	if errors.Is(err, encryption.ErrSidecarBehindRaftLog) {
		t.Errorf("scanner error must NOT be classified as ErrSidecarBehindRaftLog; got %v", err)
	}
	if !errors.Is(err, scanErr) {
		t.Errorf("original scanner error must be in chain; got %v", err)
	}
}

// TestChainEncryptionStartupGuard_PropagatesPrevError verifies
// that a non-nil prevErr short-circuits before the guard runs.
// Pins the cyclop-reduction shape: caller is "single if err !=
// nil" downstream of the chain helper.
func TestChainEncryptionStartupGuard_PropagatesPrevError(t *testing.T) {
	prev := errors.New("build failed")
	got := chainEncryptionStartupGuard(prev, nil, 0, "", false)
	if !errors.Is(got, prev) {
		t.Fatalf("chain must propagate prev error verbatim; got %v", got)
	}
}

// TestChainEncryptionStartupGuard_NilPrevRunsGuard verifies
// the other half: nil prevErr forwards to checkSidecarBehindRaftLog.
func TestChainEncryptionStartupGuard_NilPrevRunsGuard(t *testing.T) {
	got := chainEncryptionStartupGuard(nil, nil, 0, "", false)
	if got != nil {
		t.Fatalf("chain with nil prev and skipped guard must return nil; got %v", got)
	}
}

func TestCheckEnvelopeCutoverDivergenceStartupGuard_Fires(t *testing.T) {
	t.Parallel()
	sidecarPath := writeRaftCutoverSidecarForStartup(t, 8, 3, 100)
	rt := &raftGroupRuntime{
		spec:         groupSpec{id: 1},
		stateMachine: &stubRestoredCutoverStateMachine{cutover: 200},
	}
	err := checkEnvelopeCutoverDivergenceStartupGuard([]*raftGroupRuntime{rt}, 1, sidecarPath, true)
	if !errors.Is(err, encryption.ErrEnvelopeCutoverDivergence) {
		t.Fatalf("cutover guard must fire ErrEnvelopeCutoverDivergence, got %v", err)
	}
}

func TestCheckEnvelopeCutoverDivergenceStartupGuard_Match(t *testing.T) {
	t.Parallel()
	sidecarPath := writeRaftCutoverSidecarForStartup(t, 8, 3, 100)
	rt := &raftGroupRuntime{
		spec:         groupSpec{id: 1},
		stateMachine: &stubRestoredCutoverStateMachine{cutover: 100},
	}
	if err := checkEnvelopeCutoverDivergenceStartupGuard([]*raftGroupRuntime{rt}, 1, sidecarPath, true); err != nil {
		t.Fatalf("matching snapshot/sidecar cutover must pass, got %v", err)
	}
}

func TestCheckEnvelopeCutoverDivergenceSnapshotPayload_Fires(t *testing.T) {
	t.Parallel()
	payload := snapshotPayloadWithCutover(t, 200)
	err := checkEnvelopeCutoverDivergenceSnapshotPayload(bytes.NewReader(payload), 100, 1)
	if !errors.Is(err, encryption.ErrEnvelopeCutoverDivergence) {
		t.Fatalf("snapshot payload cutover guard must fire ErrEnvelopeCutoverDivergence, got %v", err)
	}
}

func TestCheckEnvelopeCutoverDivergenceSnapshotPayload_Match(t *testing.T) {
	t.Parallel()
	payload := snapshotPayloadWithCutover(t, 100)
	if err := checkEnvelopeCutoverDivergenceSnapshotPayload(bytes.NewReader(payload), 100, 1); err != nil {
		t.Fatalf("matching snapshot payload/sidecar cutover must pass, got %v", err)
	}
}

func TestCheckEnvelopeCutoverDivergenceStartupGuard_PreBootstrapSkips(t *testing.T) {
	t.Parallel()
	sidecarPath := writeMinimalSidecar(t, 1)
	rt := &raftGroupRuntime{
		spec:         groupSpec{id: 1},
		stateMachine: &stubRestoredCutoverStateMachine{cutover: 0},
	}
	if err := checkEnvelopeCutoverDivergenceStartupGuard([]*raftGroupRuntime{rt}, 1, sidecarPath, true); err != nil {
		t.Fatalf("pre-bootstrap sidecar must skip cutover divergence guard, got %v", err)
	}
}
