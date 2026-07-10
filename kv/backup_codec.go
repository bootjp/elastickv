package kv

import (
	"encoding/binary"
	"math"
	"time"

	"github.com/cockroachdb/errors"
)

const (
	raftEncodeBackup byte = 0x0e

	backupSubtypePin     byte = 0x01
	backupSubtypeExtend  byte = 0x02
	backupSubtypeRelease byte = 0x03

	backupPinIDBytes = 16
	backupUint64Size = 8

	backupEnvelopeHeaderLen = 2
	backupPinEntryLen       = backupEnvelopeHeaderLen + backupPinIDBytes + backupUint64Size + backupUint64Size
	backupExtendEntryLen    = backupEnvelopeHeaderLen + backupPinIDBytes + backupUint64Size
	backupReleaseEntryLen   = backupEnvelopeHeaderLen + backupPinIDBytes

	backupPinIDStart        = backupEnvelopeHeaderLen
	backupPinIDEnd          = backupPinIDStart + backupPinIDBytes
	backupReadTSStart       = backupPinIDEnd
	backupReadTSEnd         = backupReadTSStart + backupUint64Size
	backupDeadlineStart     = backupReadTSEnd
	backupDeadlineEnd       = backupDeadlineStart + backupUint64Size
	backupExtendMillisStart = backupPinIDEnd
	backupExtendMillisEnd   = backupExtendMillisStart + backupUint64Size
)

var (
	ErrBackupWireMalformed = errors.New("backup fsm wire payload is malformed")
	ErrBackupWireSubtype   = errors.New("backup fsm wire subtype is unknown")
)

type BackupPinEntry struct {
	PinID    BackupPinID
	ReadTS   uint64
	Deadline time.Time
}

type BackupExtendEntry struct {
	PinID    BackupPinID
	Deadline time.Time
}

type BackupReleaseEntry struct {
	PinID BackupPinID
}

type backupEntry struct {
	subtype byte
	pin     BackupPinEntry
	extend  BackupExtendEntry
	release BackupReleaseEntry
}

func EncodeBackupPinEntry(entry BackupPinEntry) []byte {
	out := make([]byte, backupPinEntryLen)
	out[0] = raftEncodeBackup
	out[1] = backupSubtypePin
	copy(out[backupPinIDStart:backupPinIDEnd], entry.PinID[:])
	binary.BigEndian.PutUint64(out[backupReadTSStart:backupReadTSEnd], entry.ReadTS)
	binary.BigEndian.PutUint64(out[backupDeadlineStart:backupDeadlineEnd], backupDeadlineMillis(entry.Deadline))
	return out
}

func EncodeBackupExtendEntry(entry BackupExtendEntry) []byte {
	out := make([]byte, backupExtendEntryLen)
	out[0] = raftEncodeBackup
	out[1] = backupSubtypeExtend
	copy(out[backupPinIDStart:backupPinIDEnd], entry.PinID[:])
	binary.BigEndian.PutUint64(out[backupExtendMillisStart:backupExtendMillisEnd], backupDeadlineMillis(entry.Deadline))
	return out
}

func EncodeBackupReleaseEntry(entry BackupReleaseEntry) []byte {
	out := make([]byte, backupReleaseEntryLen)
	out[0] = raftEncodeBackup
	out[1] = backupSubtypeRelease
	copy(out[backupPinIDStart:backupPinIDEnd], entry.PinID[:])
	return out
}

func decodeBackupEntry(data []byte) (backupEntry, error) {
	if len(data) < backupEnvelopeHeaderLen || data[0] != raftEncodeBackup {
		return backupEntry{}, errors.WithStack(ErrBackupWireMalformed)
	}
	return decodeBackupPayload(data[1:])
}

func decodeBackupPayload(data []byte) (backupEntry, error) {
	if len(data) < backupEnvelopeHeaderLen-1 {
		return backupEntry{}, errors.WithStack(ErrBackupWireMalformed)
	}
	switch data[0] {
	case backupSubtypePin:
		if len(data) != backupPinEntryLen-1 {
			return backupEntry{}, errors.WithStack(ErrBackupWireMalformed)
		}
		var id BackupPinID
		copy(id[:], data[backupPinIDStart-1:backupPinIDEnd-1])
		return backupEntry{
			subtype: backupSubtypePin,
			pin: BackupPinEntry{
				PinID:    id,
				ReadTS:   binary.BigEndian.Uint64(data[backupReadTSStart-1 : backupReadTSEnd-1]),
				Deadline: backupDeadlineFromMillis(binary.BigEndian.Uint64(data[backupDeadlineStart-1 : backupDeadlineEnd-1])),
			},
		}, nil
	case backupSubtypeExtend:
		if len(data) != backupExtendEntryLen-1 {
			return backupEntry{}, errors.WithStack(ErrBackupWireMalformed)
		}
		var id BackupPinID
		copy(id[:], data[backupPinIDStart-1:backupPinIDEnd-1])
		return backupEntry{
			subtype: backupSubtypeExtend,
			extend: BackupExtendEntry{
				PinID:    id,
				Deadline: backupDeadlineFromMillis(binary.BigEndian.Uint64(data[backupExtendMillisStart-1 : backupExtendMillisEnd-1])),
			},
		}, nil
	case backupSubtypeRelease:
		if len(data) != backupReleaseEntryLen-1 {
			return backupEntry{}, errors.WithStack(ErrBackupWireMalformed)
		}
		var id BackupPinID
		copy(id[:], data[backupPinIDStart-1:backupPinIDEnd-1])
		return backupEntry{
			subtype: backupSubtypeRelease,
			release: BackupReleaseEntry{
				PinID: id,
			},
		}, nil
	default:
		return backupEntry{}, errors.WithStack(ErrBackupWireSubtype)
	}
}

func backupDeadlineMillis(deadline time.Time) uint64 {
	ms := deadline.UnixMilli()
	if ms <= 0 {
		return 0
	}
	return uint64(ms)
}

func backupDeadlineFromMillis(ms uint64) time.Time {
	if ms == 0 {
		return time.Time{}
	}
	if ms > math.MaxInt64 {
		ms = math.MaxInt64
	}
	return time.UnixMilli(int64(ms)) //nolint:gosec // clamped to MaxInt64 above.
}
