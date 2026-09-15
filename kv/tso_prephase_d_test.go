package kv

import (
	"context"
	stderrors "errors"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

const testPhaseDActivationMs = int64(1_700_000_000_000)

func testPhaseDFloor() uint64 {
	return uint64(testPhaseDActivationMs) << HLCLogicalBits
}

// TestPhaseDActivationMillisReadsThePhysicalHalf pins that the activation
// instant comes from the floor itself. The floor is the highest timestamp
// issued before the marker applied, so no separate activation record is needed
// — and reading the wrong half would compare a wall clock against a number
// 65536 times too large, which would leave the window open forever.
func TestPhaseDActivationMillisReadsThePhysicalHalf(t *testing.T) {
	t.Parallel()

	require.Equal(t, testPhaseDActivationMs, PhaseDActivationMillis(testPhaseDFloor()))
	// The logical counter carries no wall time and must not shift the instant.
	require.Equal(t, testPhaseDActivationMs,
		PhaseDActivationMillis(testPhaseDFloor()|0xFFFF))
}

// TestPrePhaseDStartAdmissibleClosesAfterTheWindow is §4.2: the attacker's
// leverage is the PREPARE, not the COMMIT. A transaction genuinely in flight
// across the marker can only stay preparable for its lock TTL, so once that
// window shuts no NEW pre-Phase-D intent can be created — and the commit
// carve-out then only ever applies to intents that genuinely predate it.
func TestPrePhaseDStartAdmissibleClosesAfterTheWindow(t *testing.T) {
	t.Parallel()

	floor := testPhaseDFloor()
	window := prePhaseDAdmissionWindow

	require.True(t, PrePhaseDStartAdmissible(floor, testPhaseDActivationMs, window),
		"a transaction preparing at the moment of activation is legitimate")
	require.True(t, PrePhaseDStartAdmissible(floor,
		testPhaseDActivationMs+window.Milliseconds()-1, window),
		"still inside the window")
	require.False(t, PrePhaseDStartAdmissible(floor,
		testPhaseDActivationMs+window.Milliseconds(), window),
		"at the boundary the window is shut: the lock TTL has fully elapsed")
	require.False(t, PrePhaseDStartAdmissible(floor,
		testPhaseDActivationMs+window.Milliseconds()+1, window))
}

// Before Phase D ever activates there is no window to be outside of, and
// nothing has been read at a post-marker timestamp, so nothing is refused.
func TestPrePhaseDStartAdmissibleWithNoActivation(t *testing.T) {
	t.Parallel()

	require.True(t, PrePhaseDStartAdmissible(0, testPhaseDActivationMs, time.Minute))
}

// The window has to outlast the longest legitimate lock, or a transaction
// holding a max-TTL lock across the marker is stranded.
func TestPrePhaseDAdmissionWindowOutlastsTheLongestLock(t *testing.T) {
	t.Parallel()

	require.Greater(t, prePhaseDAdmissionWindow,
		time.Duration(maxTxnLockTTLms)*time.Millisecond,
		"the window must exceed the longest lock TTL, or a legitimate long-running "+
			"transaction is refused before its lock could even expire")
	require.Equal(t, prePhaseDAdmissionGrace,
		prePhaseDAdmissionWindow-time.Duration(maxTxnLockTTLms)*time.Millisecond,
		"the excess is exactly the documented grace, not an accident of arithmetic")
}

// TestClassifyPrePhaseDCommitUsesTheRecordWhenItCanReadIt is §4.1.
//
// Where the primary record is readable it is decisive in BOTH directions: a
// matching record admits regardless of the window, and a missing or disagreeing
// one refuses regardless of it, because the claim is then positively
// contradicted rather than merely unproven.
func TestClassifyPrePhaseDCommitUsesTheRecordWhenItCanReadIt(t *testing.T) {
	t.Parallel()

	const claimed = uint64(4242)

	t.Run("a matching record admits even after the window shut", func(t *testing.T) {
		t.Parallel()
		require.NoError(t, ClassifyPrePhaseDCommit(
			PrimaryCommitEvidence{Readable: true, Found: true, CommitTS: claimed},
			claimed, false, false))
	})

	t.Run("a disagreeing record refuses even inside the window", func(t *testing.T) {
		t.Parallel()
		err := ClassifyPrePhaseDCommit(
			PrimaryCommitEvidence{Readable: true, Found: true, CommitTS: claimed + 1},
			claimed, false, true)
		require.ErrorIs(t, err, ErrPrePhaseDCommitUnproven)
	})

	t.Run("no record at all refuses even inside the window", func(t *testing.T) {
		t.Parallel()
		err := ClassifyPrePhaseDCommit(
			PrimaryCommitEvidence{Readable: true, Found: false},
			claimed, false, true)
		require.ErrorIs(t, err, ErrPrePhaseDCommitUnproven)
	})
}

// TestClassifyPrePhaseDCommitFallsBackToTheWindow covers the case §4.1 alone
// cannot answer: the primary lives on another group, so there is no evidence to
// draw a verdict from and refusing outright would break legitimate cross-shard
// legacy resolution.
func TestClassifyPrePhaseDCommitFallsBackToTheWindow(t *testing.T) {
	t.Parallel()

	const claimed = uint64(4242)
	remote := PrimaryCommitEvidence{Readable: false}

	require.NoError(t, ClassifyPrePhaseDCommit(remote, claimed, false, true),
		"inside the window a remote-primary resolution is still admitted")
	require.ErrorIs(t,
		ClassifyPrePhaseDCommit(remote, claimed, false, false),
		ErrPrePhaseDWindowClosed)
}

// An ABORT synthesises its timestamp rather than replaying a recorded one, so
// no record can ever support it. It is always a window decision, even when the
// primary is local — requiring a record there would refuse every legitimate
// abort resolution.
func TestClassifyPrePhaseDCommitTreatsAbortAsAWindowDecision(t *testing.T) {
	t.Parallel()

	const claimed = uint64(4242)
	// Readable and with no matching record: a COMMIT would be refused here.
	local := PrimaryCommitEvidence{Readable: true, Found: false}

	require.NoError(t, ClassifyPrePhaseDCommit(local, claimed, true, true),
		"an abort's timestamp is synthesised, so the absent record proves nothing")
	require.ErrorIs(t,
		ClassifyPrePhaseDCommit(local, claimed, true, false),
		ErrPrePhaseDWindowClosed)

	// And the same evidence DOES refuse a commit, which is what makes the
	// distinction meaningful rather than a blanket exemption.
	require.ErrorIs(t,
		ClassifyPrePhaseDCommit(local, claimed, false, true),
		ErrPrePhaseDCommitUnproven)
}

// stubPrePhaseDAllocator is a TimestampAllocator that reports a pre-Phase-D
// verdict and a fixed floor, so the wiring can be driven without a Raft group.
type stubPrePhaseDAllocator struct {
	floor     uint64
	prePhaseD bool
}

func (s *stubPrePhaseDAllocator) Next(context.Context) (uint64, error) { return 1, nil }
func (s *stubPrePhaseDAllocator) PhaseDActive() bool                   { return true }
func (s *stubPrePhaseDAllocator) PhaseDRequired() bool                 { return true }
func (s *stubPrePhaseDAllocator) PhaseDFloor() uint64                  { return s.floor }

func (s *stubPrePhaseDAllocator) ValidateDurableTimestamp(context.Context, uint64) error {
	if s.prePhaseD {
		return errors.Wrap(stderrors.Join(ErrTSOTimestampInvalid, ErrTSOTimestampPrePhaseD), "stub")
	}
	return nil
}

// TestForwardedStartTimestampClosesAfterTheAdmissionWindow is the wiring for
// §4.2.
//
// The carve-out's whole proof was attacker-supplied: a caller could PREPARE at
// an arbitrary pre-Phase-D start, creating an intent there, and then COMMIT at
// another pre-D timestamp that the resolution carve-out accepted. Bounding
// intent CREATION is what removes the first step.
func TestForwardedStartTimestampClosesAfterTheAdmissionWindow(t *testing.T) {
	t.Parallel()

	alloc := &stubPrePhaseDAllocator{floor: testPhaseDFloor(), prePhaseD: true}
	restore := prePhaseDNowMillis
	t.Cleanup(func() { prePhaseDNowMillis = restore })

	// Inside the window: a transaction that began before the marker is still
	// entitled to prepare.
	prePhaseDNowMillis = func() int64 { return testPhaseDActivationMs }
	require.NoError(t, ValidateForwardedTxnStartTimestamp(
		context.Background(), alloc, 1, "test"))

	// Past it, no transaction can still legitimately be preparing at a pre-D
	// start, so a fresh pre-D intent cannot be created.
	prePhaseDNowMillis = func() int64 {
		return testPhaseDActivationMs + prePhaseDAdmissionWindow.Milliseconds() + 1
	}
	err := ValidateForwardedTxnStartTimestamp(context.Background(), alloc, 1, "test")
	require.ErrorIs(t, err, ErrPrePhaseDWindowClosed)
}

// A post-Phase-D start is unaffected in either direction: the window governs
// only the pre-D carve-out, not ordinary validation.
func TestForwardedStartTimestampAfterPhaseDIsUnaffectedByTheWindow(t *testing.T) {
	t.Parallel()

	alloc := &stubPrePhaseDAllocator{floor: testPhaseDFloor(), prePhaseD: false}
	restore := prePhaseDNowMillis
	t.Cleanup(func() { prePhaseDNowMillis = restore })
	prePhaseDNowMillis = func() int64 {
		return testPhaseDActivationMs + prePhaseDAdmissionWindow.Milliseconds()*10
	}

	require.NoError(t, ValidateForwardedTxnStartTimestamp(
		context.Background(), alloc, 1, "test"),
		"a timestamp group 0 actually issued is valid whenever it arrives")
}

// An allocator that cannot report the floor leaves the window OPEN. The bound
// exists to narrow a carve-out; a missing signal must not turn it into a
// refusal that strands legitimate legacy resolution.
func TestForwardedStartTimestampWithoutAFloorSourceStaysOpen(t *testing.T) {
	t.Parallel()

	require.True(t, prePhaseDStartWithinWindow(&floorlessAllocator{}))
}

type floorlessAllocator struct{}

func (floorlessAllocator) Next(context.Context) (uint64, error) { return 1, nil }
