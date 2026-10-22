package kv

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

func pinCommand(operation replicatedPinOperation, generation uint64) replicatedPinCommand {
	return replicatedPinCommand{
		Operation: operation, Identity: BackupIdentity{Epoch: 1, Generation: generation},
		PinID: backupTrackerTestPinID(1), ReadTS: 10, Deadline: 100, ExpectedDeadline: 100,
	}
}

func applyPinCommand(t *testing.T, s replicatedBackupPins, command replicatedPinCommand, changed bool) replicatedBackupPins {
	t.Helper()
	next, didChange, err := s.transition(command)
	require.NoError(t, err)
	require.Equal(t, changed, didChange)
	return next
}

func TestReplicatedBackupPinRetiresMissingBeforeAdmission(t *testing.T) {
	t.Parallel()
	for _, operation := range []replicatedPinOperation{replicatedPinRelease, replicatedPinExpire} {
		t.Run(map[replicatedPinOperation]string{replicatedPinRelease: "release", replicatedPinExpire: "expire"}[operation], func(t *testing.T) {
			t.Parallel()
			s := replicatedBackupPins{Epoch: 1, Capacity: 1}
			s = applyPinCommand(t, s, pinCommand(operation, 5), true)
			require.Equal(t, uint64(5), s.RetiredThrough)
			require.Equal(t, uint64(1), s.PinGeneration)
			s = applyPinCommand(t, s, pinCommand(operation, 5), false)
			for _, generation := range []uint64{1, 5} {
				next, changed, err := s.transition(pinCommand(replicatedPinAdmit, generation))
				require.ErrorIs(t, err, ErrReplicatedBackupPin)
				require.False(t, changed)
				require.Equal(t, s, next)
			}
			s = applyPinCommand(t, s, pinCommand(replicatedPinAdmit, 6), true)
			require.Len(t, s.Active, 1)
		})
	}
}

func TestReplicatedBackupPinExtendAndExpiryOrdering(t *testing.T) {
	t.Parallel()
	initial := applyPinCommand(t, replicatedBackupPins{Epoch: 1}, pinCommand(replicatedPinAdmit, 1), true)
	extend := pinCommand(replicatedPinExtend, 1)
	extend.Deadline = 200
	extended := applyPinCommand(t, initial, extend, true)
	staleExpiry := applyPinCommand(t, extended, pinCommand(replicatedPinExpire, 1), false)
	require.Equal(t, extended, staleExpiry)
	require.Zero(t, staleExpiry.RetiredThrough)
	require.Equal(t, uint64(10), staleExpiry.oldest(), "a passed deadline alone never removes retention")

	expiry := pinCommand(replicatedPinExpire, 1)
	expiry.ExpectedDeadline = 200
	expired := applyPinCommand(t, extended, expiry, true)
	require.Zero(t, expired.oldest())
	require.Equal(t, uint64(3), expired.PinGeneration)
	_, _, err := expired.transition(extend)
	require.ErrorIs(t, err, ErrReplicatedBackupPin, "Extend cannot resurrect a retired identity")
	require.Equal(t, int64(100), initial.Active[1].Deadline, "transitions must not mutate their input")
}

func TestReplicatedBackupPinHigherRetirementPreservesActiveOlderPin(t *testing.T) {
	t.Parallel()
	s := applyPinCommand(t, replicatedBackupPins{Epoch: 1}, pinCommand(replicatedPinAdmit, 1), true)
	s = applyPinCommand(t, s, pinCommand(replicatedPinRelease, 5), true)
	s = applyPinCommand(t, s, pinCommand(replicatedPinAdmit, 1), false)
	extend := pinCommand(replicatedPinExtend, 1)
	extend.Deadline = 200
	s = applyPinCommand(t, s, extend, true)
	require.Equal(t, uint64(5), s.RetiredThrough)
	require.Equal(t, int64(200), s.Active[1].Deadline)
	_, _, err := s.transition(pinCommand(replicatedPinAdmit, 2))
	require.ErrorIs(t, err, ErrReplicatedBackupPin)
	s = applyPinCommand(t, s, pinCommand(replicatedPinRelease, 1), true)
	require.Empty(t, s.Active)
	require.Equal(t, uint64(5), s.RetiredThrough)
}

func TestReplicatedBackupPinIdentityFloorAndCapacity(t *testing.T) {
	t.Parallel()
	s := replicatedBackupPins{Epoch: 1, Capacity: 1, ReadFloor: 10}
	s = applyPinCommand(t, s, pinCommand(replicatedPinAdmit, 1), true)
	_, _, err := s.transition(pinCommand(replicatedPinAdmit, 2))
	require.ErrorIs(t, err, ErrTooManyActiveBackups)
	for _, operation := range []replicatedPinOperation{replicatedPinAdmit, replicatedPinExtend, replicatedPinRelease, replicatedPinExpire} {
		command := pinCommand(operation, 1)
		command.Identity.Epoch = 2
		_, _, err = s.transition(command)
		require.ErrorIs(t, err, ErrReplicatedBackupPin)
		command.Identity.Epoch = 1
		command.PinID = backupTrackerTestPinID(2)
		_, _, err = s.transition(command)
		require.ErrorIs(t, err, ErrReplicatedBackupPin)
	}
	s = applyPinCommand(t, s, pinCommand(replicatedPinRelease, 1), true)
	below := pinCommand(replicatedPinAdmit, 2)
	below.ReadTS = 9
	_, _, err = s.transition(below)
	require.ErrorIs(t, err, ErrReplicatedBackupPin)
	duplicate := pinCommand(replicatedPinAdmit, 2)
	s = applyPinCommand(t, s, duplicate, true)
	duplicate.Deadline++
	_, _, err = s.transition(duplicate)
	require.ErrorIs(t, err, ErrReplicatedBackupPin, "initial admission must never renew")
}

func TestReplicatedBackupPinGenerationDoesNotWrap(t *testing.T) {
	t.Parallel()
	s := replicatedBackupPins{Epoch: 1, PinGeneration: math.MaxUint64}
	next, changed, err := s.transition(pinCommand(replicatedPinRelease, 1))
	require.ErrorIs(t, err, ErrReplicatedBackupPin)
	require.False(t, changed)
	require.Equal(t, s, next)
}
