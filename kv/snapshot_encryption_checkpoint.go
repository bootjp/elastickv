package kv

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"io"

	"github.com/bootjp/elastickv/internal/encryption"
	"github.com/cockroachdb/errors"
)

var encryptionCheckpointMagic = []byte("EKVENCP1")

const encryptionCheckpointBytes = 8 + 8 + 32
const snapshotCheckpointHeaderLen = hlcSnapshotV2HeaderLen + encryptionCheckpointBytes

func decodeSnapshotCheckpoint(payload []byte) (encryption.SnapshotCheckpoint, error) {
	var checkpoint encryption.SnapshotCheckpoint
	if len(payload) < hlcSnapshotV2MinPayload+len(encryptionCheckpointMagic) ||
		!bytes.Equal(payload[hlcSnapshotV2MinPayload:hlcSnapshotV2MinPayload+len(encryptionCheckpointMagic)], encryptionCheckpointMagic) {
		return checkpoint, nil
	}
	if len(payload) != hlcSnapshotV2MinPayload+encryptionCheckpointBytes {
		return checkpoint, errors.New("snapshot encryption checkpoint: invalid length")
	}
	checkpoint.Index = binary.BigEndian.Uint64(payload[24:32])
	copy(checkpoint.State[:], payload[32:64])
	if checkpoint.Index == 0 {
		return checkpoint, errors.New("snapshot encryption checkpoint: zero index")
	}
	return checkpoint, nil
}

func (f *kvFSM) captureEncryptionCheckpoint(index uint64) error {
	source, ok := f.encryption.(interface {
		SnapshotCheckpoint() (encryption.SnapshotCheckpoint, error)
	})
	if !ok || index == 0 {
		return nil
	}
	checkpoint, err := source.SnapshotCheckpoint()
	if err != nil {
		return errors.Wrap(err, "capture encryption snapshot checkpoint")
	}
	// During WAL replay the sidecar can already represent a later entry.
	// Do not claim that future state in a snapshot of an earlier prefix.
	if checkpoint.Index == index {
		if sink, ok := f.store.(interface {
			SaveEncryptionSnapshotCheckpoint(encryption.SnapshotCheckpoint) error
		}); ok {
			if err := sink.SaveEncryptionSnapshotCheckpoint(checkpoint); err != nil {
				return errors.Wrap(err, "persist applied encryption checkpoint")
			}
		}
		f.encryptionCheckpoint = checkpoint
	}
	return nil
}

// RestoreSnapshotHeader is used only after the engine has verified the whole
// physical snapshot against its Raft token. Parse errors leave state untouched.
func (f *kvFSM) RestoreSnapshotHeader(r io.Reader) error {
	ceiling, cutover, checkpoint, err := ReadSnapshotHeaderWithCheckpoint(bufio.NewReader(r))
	if err != nil {
		return err
	}
	checkpoint, err = f.checkpointForDurableFSM(checkpoint, true)
	if err != nil {
		return err
	}
	f.ApplySnapshotHeader(ceiling, cutover)
	f.encryptionCheckpoint = checkpoint
	return nil
}

func (f *kvFSM) checkpointForDurableFSM(snapshot encryption.SnapshotCheckpoint, skip bool) (encryption.SnapshotCheckpoint, error) {
	source, ok := f.store.(interface {
		LoadEncryptionSnapshotCheckpoint() (encryption.SnapshotCheckpoint, bool, error)
	})
	if !ok {
		return snapshot, nil
	}
	stored, present, err := source.LoadEncryptionSnapshotCheckpoint()
	if err != nil {
		return snapshot, errors.Wrap(err, "load durable FSM encryption checkpoint")
	}
	if !present {
		return snapshot, nil
	}
	if stored == snapshot {
		return stored, nil
	}
	if !skip || stored.Index <= snapshot.Index {
		return snapshot, errors.New("encryption checkpoint: physical snapshot header and FSM record disagree")
	}
	applied, present, err := f.LastAppliedIndex()
	if err != nil {
		return snapshot, err
	}
	if present && stored.Index <= applied {
		return stored, nil
	}
	// The durable checkpoint may precede a crash before the applied-index
	// marker. That tail is not skipped and must replay through the handler.
	return snapshot, nil
}

func (f *kvFSM) checkSnapshotCheckpointConsistency() error {
	source, ok := f.store.(interface {
		LoadEncryptionSnapshotCheckpoint() (encryption.SnapshotCheckpoint, bool, error)
	})
	if !ok {
		return nil
	}
	stored, present, err := source.LoadEncryptionSnapshotCheckpoint()
	if err != nil {
		return errors.Wrap(err, "verify snapshot checkpoint before capture")
	}
	if present && stored != f.encryptionCheckpoint {
		return errors.New("encryption checkpoint: durable tail must reconcile before snapshot capture")
	}
	return nil
}
