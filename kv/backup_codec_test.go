package kv

import (
	"testing"
	"time"

	"github.com/bootjp/elastickv/internal/encryption/fsmwire"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func TestBackupCodecRoundTrip(t *testing.T) {
	pinID := backupTrackerTestPinID(7)
	deadline := time.UnixMilli(1780000000123)

	pinWire := EncodeBackupPinEntry(BackupPinEntry{
		PinID:    pinID,
		ReadTS:   42,
		Deadline: deadline,
	})
	require.Len(t, pinWire, backupPinEntryLen)
	gotPin, err := decodeBackupEntry(pinWire)
	require.NoError(t, err)
	require.Equal(t, backupSubtypePin, gotPin.subtype)
	require.Equal(t, BackupPinEntry{PinID: pinID, ReadTS: 42, Deadline: deadline}, gotPin.pin)

	extendWire := EncodeBackupExtendEntry(BackupExtendEntry{
		PinID:    pinID,
		Deadline: deadline.Add(time.Second),
	})
	require.Len(t, extendWire, backupExtendEntryLen)
	gotExtend, err := decodeBackupEntry(extendWire)
	require.NoError(t, err)
	require.Equal(t, backupSubtypeExtend, gotExtend.subtype)
	require.Equal(t, BackupExtendEntry{PinID: pinID, Deadline: deadline.Add(time.Second)}, gotExtend.extend)

	releaseWire := EncodeBackupReleaseEntry(BackupReleaseEntry{PinID: pinID})
	require.Len(t, releaseWire, backupReleaseEntryLen)
	gotRelease, err := decodeBackupEntry(releaseWire)
	require.NoError(t, err)
	require.Equal(t, backupSubtypeRelease, gotRelease.subtype)
	require.Equal(t, BackupReleaseEntry{PinID: pinID}, gotRelease.release)
}

func TestBackupCodecReserveRoundTrip(t *testing.T) {
	pinID := backupTrackerTestPinID(4)
	deadline := time.UnixMilli(9000)

	reserve, err := decodeBackupEntry(EncodeBackupReserveEntry(BackupReserveEntry{
		PinID: pinID, ReadTS: 88, Deadline: deadline,
	}))
	require.NoError(t, err)
	require.Equal(t, backupSubtypeReserve, reserve.subtype)
	require.Equal(t, pinID, reserve.pin.PinID)
	require.Equal(t, uint64(88), reserve.pin.ReadTS)
	require.Equal(t, deadline, reserve.pin.Deadline)

	unreserve, err := decodeBackupEntry(EncodeBackupUnreserveEntry(BackupUnreserveEntry{PinID: pinID}))
	require.NoError(t, err)
	require.Equal(t, backupSubtypeUnreserve, unreserve.subtype)
	require.Equal(t, pinID, unreserve.release.PinID)
}

func TestBackupCodecRejectsMalformedWire(t *testing.T) {
	pinID := backupTrackerTestPinID(1)
	valid := EncodeBackupPinEntry(BackupPinEntry{
		PinID:    pinID,
		ReadTS:   42,
		Deadline: time.UnixMilli(5000),
	})

	_, err := decodeBackupEntry(nil)
	require.ErrorIs(t, err, ErrBackupWireMalformed)
	_, err = decodeBackupEntry([]byte{raftEncodeBatch, backupSubtypeRelease})
	require.ErrorIs(t, err, ErrBackupWireMalformed)
	_, err = decodeBackupEntry(valid[:len(valid)-1])
	require.ErrorIs(t, err, ErrBackupWireMalformed)
	_, err = decodeBackupEntry([]byte{raftEncodeBackup, 0xff})
	require.ErrorIs(t, err, ErrBackupWireSubtype)
}

func TestBackupCodecZeroDeadlineDecodesToZeroTime(t *testing.T) {
	pinID := backupTrackerTestPinID(1)

	gotPin, err := decodeBackupEntry(EncodeBackupPinEntry(BackupPinEntry{
		PinID:    pinID,
		ReadTS:   42,
		Deadline: time.Time{},
	}))
	require.NoError(t, err)
	require.True(t, gotPin.pin.Deadline.IsZero())

	gotExtend, err := decodeBackupEntry(EncodeBackupExtendEntry(BackupExtendEntry{
		PinID:    pinID,
		Deadline: time.Time{},
	}))
	require.NoError(t, err)
	require.True(t, gotExtend.extend.Deadline.IsZero())
}

func TestBackupEnvelopeOpcodeDoesNotCollide(t *testing.T) {
	require.Equal(t, byte(0x0e), raftEncodeBackup)
	require.Greater(t, raftEncodeBackup, fsmwire.OpEncryptionMax)
	require.NotContains(t, []byte{raftEncodeSingle, raftEncodeBatch, raftEncodeHLCLease}, raftEncodeBackup)
	require.NotContains(t, []byte{0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d}, raftEncodeBackup)

	err := proto.Unmarshal([]byte{raftEncodeBackup, 0x00}, &pb.Request{})
	require.Error(t, err, "0x0e must remain an invalid proto wire start byte")
}
