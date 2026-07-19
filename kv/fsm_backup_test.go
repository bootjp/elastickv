package kv

import (
	"testing"
	"time"

	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestApplyBackupUsesSharedTracker(t *testing.T) {
	tracker := NewActiveTimestampTracker(WithActiveTimestampTrackerSweepInterval(0))
	fsm := newBackupTestFSM(t, tracker)
	pinID := backupTrackerTestPinID(1)
	now := time.Now()
	firstDeadline := time.UnixMilli(now.Add(time.Hour).UnixMilli())
	secondDeadline := time.UnixMilli(now.Add(2 * time.Hour).UnixMilli())

	require.NoError(t, haltApplyOf(fsm.Apply(EncodeBackupPinEntry(BackupPinEntry{
		PinID:    pinID,
		ReadTS:   42,
		Deadline: firstDeadline,
	}))))
	require.Equal(t, 1, tracker.ActiveBackupPinCount())
	require.Equal(t, uint64(42), tracker.Oldest())

	require.NoError(t, haltApplyOf(fsm.Apply(EncodeBackupExtendEntry(BackupExtendEntry{
		PinID:    pinID,
		ReadTS:   42,
		Deadline: secondDeadline,
	}))))
	gotDeadline, ok := tracker.BackupPinDeadline(pinID)
	require.True(t, ok)
	require.Equal(t, secondDeadline, gotDeadline)

	require.NoError(t, haltApplyOf(fsm.Apply(EncodeBackupReleaseEntry(BackupReleaseEntry{PinID: pinID}))))
	require.Equal(t, 0, tracker.ActiveBackupPinCount())
	require.Equal(t, uint64(0), tracker.Oldest())
}

func TestApplyBackupWithoutTrackerHalts(t *testing.T) {
	fsm, ok := NewKvFSMWithHLC(store.NewMVCCStore(), NewHLC()).(*kvFSM)
	require.True(t, ok)

	err := haltApplyOf(fsm.Apply(EncodeBackupPinEntry(BackupPinEntry{
		PinID:    backupTrackerTestPinID(1),
		ReadTS:   42,
		Deadline: time.UnixMilli(5000),
	})))
	require.True(t, errors.Is(err, ErrBackupApply), "err = %v", err)
}

func TestApplyBackupUnknownSubtypeHalts(t *testing.T) {
	fsm := newBackupTestFSM(t, NewActiveTimestampTracker(WithActiveTimestampTrackerSweepInterval(0)))

	err := haltApplyOf(fsm.Apply([]byte{raftEncodeBackup, 0xff}))
	require.True(t, errors.Is(err, ErrBackupApply), "err = %v", err)
	require.True(t, errors.Is(err, ErrBackupWireSubtype), "err = %v", err)
}

func TestApplyBackupInvalidPinReturnsNonFatalError(t *testing.T) {
	fsm := newBackupTestFSM(t, NewActiveTimestampTracker(WithActiveTimestampTrackerSweepInterval(0)))

	resp := fsm.Apply(EncodeBackupPinEntry(BackupPinEntry{
		PinID:    BackupPinID{},
		ReadTS:   42,
		Deadline: time.UnixMilli(5000),
	}))
	require.NoError(t, haltApplyOf(resp))
	respErr, ok := resp.(error)
	require.True(t, ok)
	require.ErrorIs(t, respErr, ErrInvalidBackupPin)
}

func TestApplyBackupLimitDoesNotDropCommittedPins(t *testing.T) {
	tracker := NewActiveTimestampTracker(
		WithActiveTimestampTrackerSweepInterval(0),
		WithActiveTimestampTrackerMaxBackupPins(1),
	)
	fsm := newBackupTestFSM(t, tracker)
	require.NoError(t, haltApplyOf(fsm.Apply(EncodeBackupPinEntry(BackupPinEntry{
		PinID:    backupTrackerTestPinID(1),
		ReadTS:   42,
		Deadline: time.Now().Add(time.Hour),
	}))))

	resp := fsm.Apply(EncodeBackupPinEntry(BackupPinEntry{
		PinID:    backupTrackerTestPinID(2),
		ReadTS:   43,
		Deadline: time.Now().Add(2 * time.Hour),
	}))

	require.NoError(t, haltApplyOf(resp))
	require.Nil(t, resp)
	require.Equal(t, 2, tracker.ActiveBackupPinCount())
	require.Equal(t, uint64(42), tracker.Oldest())
}

func TestApplyBackupMissingExtendRestoresCommittedFence(t *testing.T) {
	tracker := NewActiveTimestampTracker(WithActiveTimestampTrackerSweepInterval(0))
	fsm := newBackupTestFSM(t, tracker)

	resp := fsm.Apply(EncodeBackupExtendEntry(BackupExtendEntry{
		PinID:    backupTrackerTestPinID(1),
		ReadTS:   42,
		Deadline: time.Now().Add(time.Hour),
	}))

	require.NoError(t, haltApplyOf(resp))
	require.Nil(t, resp)
	require.Equal(t, 1, tracker.ActiveBackupPinCount())
	require.Equal(t, uint64(42), tracker.Oldest())
}

func TestApplyBackupExpiredExtendRestoresCommittedFence(t *testing.T) {
	tracker := NewActiveTimestampTracker(WithActiveTimestampTrackerSweepInterval(0))
	fsm := newBackupTestFSM(t, tracker)
	pinID := backupTrackerTestPinID(1)
	require.NoError(t, tracker.PinWithDeadline(pinID, 42, time.Now().Add(-time.Millisecond)))
	tracker.reapExpiredBackupPins(time.Now())
	require.Equal(t, 0, tracker.ActiveBackupPinCount())

	resp := fsm.Apply(EncodeBackupExtendEntry(BackupExtendEntry{
		PinID:    pinID,
		ReadTS:   42,
		Deadline: time.Now().Add(time.Hour),
	}))

	require.NoError(t, haltApplyOf(resp))
	require.Nil(t, resp)
	require.Equal(t, 1, tracker.ActiveBackupPinCount())
	require.Equal(t, uint64(42), tracker.Oldest())
}

func TestApplyBackupZeroDeadlineReturnsNonFatalError(t *testing.T) {
	fsm := newBackupTestFSM(t, NewActiveTimestampTracker(WithActiveTimestampTrackerSweepInterval(0)))

	resp := fsm.Apply(EncodeBackupPinEntry(BackupPinEntry{
		PinID:    backupTrackerTestPinID(1),
		ReadTS:   42,
		Deadline: time.Time{},
	}))

	require.NoError(t, haltApplyOf(resp))
	respErr, ok := resp.(error)
	require.True(t, ok)
	require.ErrorIs(t, respErr, ErrInvalidBackupPin)
}

func TestApplyBackupPinsAreScopedByRaftGroup(t *testing.T) {
	tracker := NewActiveTimestampTracker(WithActiveTimestampTrackerSweepInterval(0))
	fsm1 := newBackupTestFSMWithGroup(t, tracker, 1)
	fsm2 := newBackupTestFSMWithGroup(t, tracker, 2)
	pinID := backupTrackerTestPinID(1)
	entry := BackupPinEntry{
		PinID:    pinID,
		ReadTS:   42,
		Deadline: time.Now().Add(time.Hour),
	}

	require.NoError(t, haltApplyOf(fsm1.Apply(EncodeBackupPinEntry(entry))))
	require.NoError(t, haltApplyOf(fsm2.Apply(EncodeBackupPinEntry(entry))))
	require.Equal(t, 2, tracker.ActiveBackupPinCount())

	require.NoError(t, haltApplyOf(fsm1.Apply(EncodeBackupReleaseEntry(BackupReleaseEntry{PinID: pinID}))))
	require.Equal(t, 1, tracker.ActiveBackupPinCount())
	_, ok := tracker.BackupPinDeadlineForGroup(pinID, 1)
	require.False(t, ok)
	_, ok = tracker.BackupPinDeadlineForGroup(pinID, 2)
	require.True(t, ok)
}

func TestBackupPayloadIsVolatileOnly(t *testing.T) {
	fsm := &kvFSM{}
	pinID := backupTrackerTestPinID(1)

	require.True(t, fsm.IsVolatileOnlyPayload(EncodeBackupPinEntry(BackupPinEntry{
		PinID:    pinID,
		ReadTS:   42,
		Deadline: time.UnixMilli(5000),
	})))
	require.False(t, fsm.IsVolatileOnlyPayload(nil))
	require.False(t, fsm.IsVolatileOnlyPayload([]byte{raftEncodeSingle}))
	require.False(t, fsm.IsVolatileOnlyPayload([]byte{0x03}))
}

func newBackupTestFSM(t *testing.T, tracker *ActiveTimestampTracker) *kvFSM {
	t.Helper()
	fsm, ok := NewKvFSMWithHLCAndTracker(store.NewMVCCStore(), NewHLC(), tracker).(*kvFSM)
	require.True(t, ok)
	return fsm
}

func newBackupTestFSMWithGroup(t *testing.T, tracker *ActiveTimestampTracker, groupID uint64) *kvFSM {
	t.Helper()
	fsm, ok := NewKvFSMWithHLCAndTracker(
		store.NewMVCCStore(),
		NewHLC(),
		tracker,
		WithRouteHistory(nil, groupID),
	).(*kvFSM)
	require.True(t, ok)
	return fsm
}
