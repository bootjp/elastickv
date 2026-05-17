package encryption_test

import (
	"errors"
	"strings"
	"testing"

	"github.com/bootjp/elastickv/internal/encryption"
	"github.com/bootjp/elastickv/internal/encryption/fsmwire"
)

// TestIsEncryptionRelevantOpcode_AllRangeMembers pins the
// §5.5 predicate against every byte in the [OpEncryptionMin,
// OpEncryptionMax] range AND a sampling of out-of-range bytes
// (including the boundaries one below and one above the range).
// The predicate is the load-bearing input to the
// ErrSidecarBehindRaftLog guard; a regression that widens or
// narrows the range silently changes the security guarantee.
func TestIsEncryptionRelevantOpcode_AllRangeMembers(t *testing.T) {
	// Every byte in the OpEncryption range MUST be relevant.
	//
	// Iterate via int so the loop terminates even if a future
	// stage sets OpEncryptionMax = 0xFF; a byte-typed loop
	// counter would wrap to 0x00 on opcode++ at 0xFF and the
	// loop would never exit. The design reserves 0x06 / 0x07
	// for Stage 6E, so the range is expected to grow and a
	// byte-typed loop is a latent overflow risk.
	for i := int(fsmwire.OpEncryptionMin); i <= int(fsmwire.OpEncryptionMax); i++ {
		opcode := byte(i)
		if !encryption.IsEncryptionRelevantOpcode(opcode) {
			t.Errorf("opcode 0x%02X in [0x%02X, 0x%02X] must be encryption-relevant",
				opcode, fsmwire.OpEncryptionMin, fsmwire.OpEncryptionMax)
		}
	}
	// The byte one below the range must NOT be relevant.
	if fsmwire.OpEncryptionMin > 0 {
		below := fsmwire.OpEncryptionMin - 1
		if encryption.IsEncryptionRelevantOpcode(below) {
			t.Errorf("opcode 0x%02X (one below OpEncryptionMin) must NOT be encryption-relevant", below)
		}
	}
	// The byte one above the range must NOT be relevant.
	if fsmwire.OpEncryptionMax < 0xFF {
		above := fsmwire.OpEncryptionMax + 1
		if encryption.IsEncryptionRelevantOpcode(above) {
			t.Errorf("opcode 0x%02X (one above OpEncryptionMax) must NOT be encryption-relevant", above)
		}
	}
}

// TestIsEncryptionRelevantOpcode_KnownRanges pins the explicit
// opcode values mentioned in §5.5: 0x03 (registration), 0x04
// (bootstrap), 0x05 (rotation), plus 0x06/0x07 reserved slots.
// Spelling them out by name catches a regression where someone
// changes the constants but leaves the predicate range alone.
func TestIsEncryptionRelevantOpcode_KnownRanges(t *testing.T) {
	for _, tc := range []struct {
		name   string
		opcode byte
	}{
		{"OpRegistration_0x03", fsmwire.OpRegistration},
		{"OpBootstrap_0x04", fsmwire.OpBootstrap},
		{"OpRotation_0x05", fsmwire.OpRotation},
		// Reserved slots in the OpEncryption range. No named
		// constant yet (Stage 6E will assign them); pinning the
		// raw bytes here ensures the predicate still treats them
		// as relevant when 6E lands the wire-format extension
		// and a future reader is reminded the range is reserved-
		// not-just-currently-3-opcodes.
		{"Reserved_0x06", 0x06},
		{"Reserved_0x07", 0x07},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if !encryption.IsEncryptionRelevantOpcode(tc.opcode) {
				t.Errorf("known encryption opcode 0x%02X must be relevant", tc.opcode)
			}
		})
	}
	// 0x00, 0x01, 0x02 (non-encryption FSM opcodes) MUST NOT be
	// relevant. The exact non-encryption opcode space is project-
	// specific; what matters here is that bytes below 0x03 are
	// never in the encryption range.
	for _, op := range []byte{0x00, 0x01, 0x02} {
		if encryption.IsEncryptionRelevantOpcode(op) {
			t.Errorf("non-encryption opcode 0x%02X must NOT be relevant", op)
		}
	}
}

// fakeScanner implements EncryptionRelevantScanner with a
// predetermined verdict. Lets the guard tests exercise the
// hit/no-hit/error branches without needing a real raftengine.
type fakeScanner struct {
	hit                bool
	err                error
	lastStart, lastEnd uint64
	calls              int
}

func (f *fakeScanner) HasEncryptionRelevantEntryInRange(start, end uint64) (bool, error) {
	f.calls++
	f.lastStart, f.lastEnd = start, end
	return f.hit, f.err
}

// TestGuardSidecarBehindRaftLog_CaughtUp verifies the no-op
// fast path: sidecar's applied index is equal to (or ahead of)
// the engine's. The scanner is NEVER called in this branch.
func TestGuardSidecarBehindRaftLog_CaughtUp(t *testing.T) {
	for _, tc := range []struct {
		name                  string
		sidecarIdx, engineIdx uint64
	}{
		{"equal", 42, 42},
		{"sidecar_ahead", 100, 42},
		{"both_zero", 0, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			scanner := &fakeScanner{hit: true} // would fire IF consulted
			err := encryption.GuardSidecarBehindRaftLog(tc.sidecarIdx, tc.engineIdx, scanner)
			if err != nil {
				t.Errorf("expected nil for caught-up case (%d >= %d), got %v",
					tc.sidecarIdx, tc.engineIdx, err)
			}
			if scanner.calls != 0 {
				t.Errorf("scanner must NOT be called when sidecar is caught up; got %d calls", scanner.calls)
			}
		})
	}
}

// TestGuardSidecarBehindRaftLog_GapNotCovered verifies the
// "behind but harmless" path: there is a gap but it contains no
// encryption-relevant entries. The guard MUST pass.
func TestGuardSidecarBehindRaftLog_GapNotCovered(t *testing.T) {
	scanner := &fakeScanner{hit: false}
	err := encryption.GuardSidecarBehindRaftLog(10, 50, scanner)
	if err != nil {
		t.Fatalf("gap (10, 50] with no encryption-relevant entries must pass; got %v", err)
	}
	if scanner.calls != 1 {
		t.Errorf("scanner must be consulted exactly once on the gap path; got %d", scanner.calls)
	}
	if scanner.lastStart != 10 || scanner.lastEnd != 50 {
		t.Errorf("scanner called with (%d, %d]; want (10, 50]", scanner.lastStart, scanner.lastEnd)
	}
}

// TestGuardSidecarBehindRaftLog_GapCovered verifies the fire
// path: there is a gap AND it covers an encryption-relevant
// entry. The guard MUST fire ErrSidecarBehindRaftLog and the
// error annotation MUST include both indices.
func TestGuardSidecarBehindRaftLog_GapCovered(t *testing.T) {
	scanner := &fakeScanner{hit: true}
	err := encryption.GuardSidecarBehindRaftLog(10, 50, scanner)
	if !errors.Is(err, encryption.ErrSidecarBehindRaftLog) {
		t.Fatalf("gap covering encryption-relevant entry must fire ErrSidecarBehindRaftLog; got %v", err)
	}
	msg := err.Error()
	for _, want := range []string{"sidecar_applied_index=10", "engine_applied_index=50"} {
		if !strings.Contains(msg, want) {
			t.Errorf("error annotation must include %q; got %q", want, msg)
		}
	}
}

// TestGuardSidecarBehindRaftLog_ScannerError verifies that
// scanner failures propagate as wrapped errors that are NOT
// marked with ErrSidecarBehindRaftLog. The operator triages a
// scanner failure (e.g., WAL corruption) differently from a
// gap-coverage refusal.
func TestGuardSidecarBehindRaftLog_ScannerError(t *testing.T) {
	scannerErr := errors.New("simulated WAL corruption")
	scanner := &fakeScanner{err: scannerErr}
	err := encryption.GuardSidecarBehindRaftLog(10, 50, scanner)
	if err == nil {
		t.Fatal("scanner error must propagate, got nil")
	}
	if errors.Is(err, encryption.ErrSidecarBehindRaftLog) {
		t.Errorf("scanner error must NOT be classified as ErrSidecarBehindRaftLog; got %v", err)
	}
	if !errors.Is(err, scannerErr) {
		t.Errorf("scanner error must be in the error chain via errors.Is; got %v", err)
	}
}

// TestGuardSidecarBehindRaftLog_NilScanner verifies the
// fail-closed posture when no scanner is supplied: even though
// the gap might be harmless, we cannot prove it is, so refuse.
// A nil scanner in production is a wiring bug; the guard's job
// is to be loud about it rather than silently accept the gap.
func TestGuardSidecarBehindRaftLog_NilScanner(t *testing.T) {
	err := encryption.GuardSidecarBehindRaftLog(10, 50, nil)
	if !errors.Is(err, encryption.ErrSidecarBehindRaftLog) {
		t.Fatalf("nil scanner with non-empty gap MUST fail closed with ErrSidecarBehindRaftLog; got %v", err)
	}
}

// TestGuardSidecarBehindRaftLog_NilScanner_CaughtUp verifies
// that a nil scanner on the caught-up path returns nil. The
// scanner is not consulted when there's no gap, so its absence
// is irrelevant — refusing here would force every non-encrypted
// caller to wire a scanner just to get past the guard.
func TestGuardSidecarBehindRaftLog_NilScanner_CaughtUp(t *testing.T) {
	err := encryption.GuardSidecarBehindRaftLog(42, 42, nil)
	if err != nil {
		t.Errorf("caught-up case with nil scanner must pass; got %v", err)
	}
}
