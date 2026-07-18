package kv

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestActiveTimestampTrackerOldest(t *testing.T) {
	tracker := NewActiveTimestampTracker()

	first := tracker.Pin(30)
	second := tracker.Pin(20)
	third := tracker.Pin(40)
	defer first.Release()
	defer second.Release()
	defer third.Release()

	require.Equal(t, uint64(20), tracker.Oldest())

	second.Release()
	require.Equal(t, uint64(30), tracker.Oldest())
}

func TestActiveTimestampTrackerOldestIncludesBackupPins(t *testing.T) {
	tracker := NewActiveTimestampTracker(WithActiveTimestampTrackerSweepInterval(0))

	token := tracker.Pin(30)
	defer token.Release()

	pinID := backupTrackerTestPinID(1)
	require.NoError(t, tracker.PinWithDeadline(pinID, 20, time.Now().Add(time.Hour)))
	require.Equal(t, uint64(20), tracker.Oldest())

	tracker.ReleaseBackupPin(pinID)
	require.Equal(t, uint64(30), tracker.Oldest())
}

func TestActiveTimestampTrackerBackupPinExpiry(t *testing.T) {
	tracker := NewActiveTimestampTracker(WithActiveTimestampTrackerSweepInterval(0))
	now := time.UnixMilli(3000)

	pinID := backupTrackerTestPinID(1)
	require.NoError(t, tracker.PinWithDeadline(pinID, 20, now.Add(-time.Millisecond)))
	require.Equal(t, uint64(0), tracker.Oldest())
	require.Equal(t, 1, tracker.ActiveBackupPinCount())

	tracker.reapExpiredBackupPins(now)
	require.Equal(t, 0, tracker.ActiveBackupPinCount())
	require.Equal(t, uint64(0), tracker.Oldest())
}

func TestActiveTimestampTrackerBackupPinExtendMovesDeadline(t *testing.T) {
	tracker := NewActiveTimestampTracker(WithActiveTimestampTrackerSweepInterval(0))
	pinID := backupTrackerTestPinID(1)
	now := time.Now()
	firstDeadline := now.Add(time.Hour)
	secondDeadline := now.Add(2 * time.Hour)

	require.NoError(t, tracker.PinWithDeadline(pinID, 20, firstDeadline))
	require.NoError(t, tracker.Extend(pinID, secondDeadline))

	got, ok := tracker.BackupPinDeadline(pinID)
	require.True(t, ok)
	require.Equal(t, secondDeadline, got)
	tracker.reapExpiredBackupPins(firstDeadline.Add(time.Millisecond))
	require.Equal(t, 1, tracker.ActiveBackupPinCount())
}

func TestActiveTimestampTrackerBackupPinExtendKeepsLaterDeadline(t *testing.T) {
	t.Parallel()

	tracker := NewActiveTimestampTracker(WithActiveTimestampTrackerSweepInterval(0))
	defer tracker.Close()
	pinID := backupTrackerTestPinID(1)
	laterDeadline := time.Now().Add(time.Hour)
	retriedDeadline := laterDeadline.Add(-time.Minute)
	require.NoError(t, tracker.PinWithDeadline(pinID, 42, laterDeadline))

	require.NoError(t, tracker.Extend(pinID, retriedDeadline))

	got, ok := tracker.BackupPinDeadline(pinID)
	require.True(t, ok)
	require.Equal(t, laterDeadline, got)
}

func TestActiveTimestampTrackerBackupPinExtendMissingIsInvalid(t *testing.T) {
	tracker := NewActiveTimestampTracker(WithActiveTimestampTrackerSweepInterval(0))

	require.ErrorIs(t, tracker.Extend(backupTrackerTestPinID(1), time.UnixMilli(5000)), ErrInvalidBackupPin)
	require.Equal(t, 0, tracker.ActiveBackupPinCount())
}

func TestActiveTimestampTrackerBackupPinExtendExpiredIsInvalid(t *testing.T) {
	tracker := NewActiveTimestampTracker(WithActiveTimestampTrackerSweepInterval(0))
	pinID := backupTrackerTestPinID(1)
	now := time.Now()

	require.NoError(t, tracker.PinWithDeadline(pinID, 20, now.Add(-time.Millisecond)))
	require.ErrorIs(t, tracker.Extend(pinID, now.Add(time.Hour)), ErrInvalidBackupPin)
	require.Equal(t, 0, tracker.ActiveBackupPinCount())
	require.Equal(t, uint64(0), tracker.Oldest())
}

func TestActiveTimestampTrackerBackupPinReleaseIsIdempotent(t *testing.T) {
	tracker := NewActiveTimestampTracker(WithActiveTimestampTrackerSweepInterval(0))
	pinID := backupTrackerTestPinID(1)

	require.NoError(t, tracker.PinWithDeadline(pinID, 20, time.UnixMilli(5000)))
	tracker.ReleaseBackupPin(pinID)
	tracker.ReleaseBackupPin(pinID)

	require.Equal(t, 0, tracker.ActiveBackupPinCount())
	require.Equal(t, uint64(0), tracker.Oldest())
}

func TestActiveTimestampTrackerBackupPinLimit(t *testing.T) {
	tracker := NewActiveTimestampTracker(
		WithActiveTimestampTrackerSweepInterval(0),
		WithActiveTimestampTrackerMaxBackupPins(1),
	)
	first := backupTrackerTestPinID(1)
	second := backupTrackerTestPinID(2)

	now := time.Now()
	require.NoError(t, tracker.PinWithDeadline(first, 20, now.Add(time.Hour)))
	require.NoError(t, tracker.PinWithDeadline(first, 25, now.Add(2*time.Hour)))
	require.ErrorIs(t, tracker.PinWithDeadline(second, 30, now.Add(3*time.Hour)), ErrTooManyActiveBackups)
	require.Equal(t, 1, tracker.ActiveBackupPinCount())
	require.Equal(t, uint64(25), tracker.Oldest())
}

func TestActiveTimestampTrackerBackupPinLimitCountsLogicalPinIDs(t *testing.T) {
	tracker := NewActiveTimestampTracker(
		WithActiveTimestampTrackerSweepInterval(0),
		WithActiveTimestampTrackerMaxBackupPins(1),
	)
	first := backupTrackerTestPinID(1)
	second := backupTrackerTestPinID(2)
	deadline := time.Now().Add(time.Hour)

	require.NoError(t, tracker.PinWithDeadlineForGroup(first, 1, 20, deadline))
	require.NoError(t, tracker.PinWithDeadlineForGroup(first, 2, 20, deadline))
	require.Equal(t, 2, tracker.ActiveBackupPinCount())
	require.ErrorIs(t, tracker.PinWithDeadlineForGroup(second, 3, 30, deadline), ErrTooManyActiveBackups)
}

func TestActiveTimestampTrackerBackupPinLimitReapsExpiredPinsFirst(t *testing.T) {
	tracker := NewActiveTimestampTracker(
		WithActiveTimestampTrackerSweepInterval(0),
		WithActiveTimestampTrackerMaxBackupPins(1),
	)
	first := backupTrackerTestPinID(1)
	second := backupTrackerTestPinID(2)
	now := time.Now()

	require.NoError(t, tracker.PinWithDeadline(first, 20, now.Add(-time.Millisecond)))
	require.NoError(t, tracker.PinWithDeadline(second, 30, now.Add(time.Hour)))
	require.Equal(t, 1, tracker.ActiveBackupPinCount())
	require.Equal(t, uint64(30), tracker.Oldest())
}

func TestActiveTimestampTrackerBackupPinsAreScopedByRaftGroup(t *testing.T) {
	tracker := NewActiveTimestampTracker(WithActiveTimestampTrackerSweepInterval(0))
	pinID := backupTrackerTestPinID(1)
	deadline := time.Now().Add(time.Hour)

	require.NoError(t, tracker.PinWithDeadlineForGroup(pinID, 1, 20, deadline))
	require.NoError(t, tracker.PinWithDeadlineForGroup(pinID, 2, 20, deadline))
	require.Equal(t, 2, tracker.ActiveBackupPinCount())

	tracker.ReleaseBackupPinForGroup(pinID, 1)
	_, ok := tracker.BackupPinDeadlineForGroup(pinID, 1)
	require.False(t, ok)
	got, ok := tracker.BackupPinDeadlineForGroup(pinID, 2)
	require.True(t, ok)
	require.Equal(t, deadline, got)
	require.Equal(t, 1, tracker.ActiveBackupPinCount())
}

func TestActiveTimestampTrackerRejectsInvalidBackupPins(t *testing.T) {
	tracker := NewActiveTimestampTracker(WithActiveTimestampTrackerSweepInterval(0))
	validID := backupTrackerTestPinID(1)

	require.ErrorIs(t, tracker.PinWithDeadline(BackupPinID{}, 20, time.UnixMilli(5000)), ErrInvalidBackupPin)
	require.ErrorIs(t, tracker.PinWithDeadline(validID, 0, time.UnixMilli(5000)), ErrInvalidBackupPin)
	require.ErrorIs(t, tracker.PinWithDeadline(validID, ^uint64(0), time.UnixMilli(5000)), ErrInvalidBackupPin)
	require.ErrorIs(t, tracker.PinWithDeadline(validID, 20, time.Time{}), ErrInvalidBackupPin)
	require.ErrorIs(t, tracker.Extend(BackupPinID{}, time.UnixMilli(5000)), ErrInvalidBackupPin)
	require.ErrorIs(t, tracker.Extend(validID, time.Time{}), ErrInvalidBackupPin)
}

func TestActiveTimestampTrackerCloseIsIdempotent(t *testing.T) {
	tracker := NewActiveTimestampTracker()

	tracker.Close()
	tracker.Close()
}

func backupTrackerTestPinID(seed byte) BackupPinID {
	var id BackupPinID
	for i := range id {
		id[i] = seed + byte(i)
	}
	return id
}
