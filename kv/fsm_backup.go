package kv

import "github.com/cockroachdb/errors"

var ErrBackupApply = errors.New("backup fsm apply failed")

func (f *kvFSM) applyBackup(data []byte) any {
	if f.readTracker == nil {
		return haltErr(errors.Wrap(ErrBackupApply, "kv/fsm: backup entry arrived but no ActiveTimestampTracker is wired"))
	}
	entry, err := decodeBackupPayload(data)
	if err != nil {
		return haltErr(errors.Wrap(errors.Mark(err, ErrBackupApply), "kv/fsm: decode backup entry"))
	}
	switch entry.subtype {
	case backupSubtypePin:
		err = f.readTracker.ApplyPinWithDeadlineForGroup(entry.pin.PinID, f.shardGroupID, entry.pin.ReadTS, entry.pin.Deadline)
	case backupSubtypeExtend:
		err = f.readTracker.ApplyExtendForGroup(entry.extend.PinID, f.shardGroupID, entry.extend.Deadline)
	case backupSubtypeRelease:
		f.readTracker.ReleaseBackupPinForGroup(entry.release.PinID, f.shardGroupID)
		return nil
	default:
		err = ErrBackupWireSubtype
	}
	if err != nil {
		if errors.Is(err, ErrInvalidBackupPin) {
			return err
		}
		return haltErr(errors.Wrap(errors.Mark(err, ErrBackupApply), "kv/fsm: apply backup entry"))
	}
	return nil
}
