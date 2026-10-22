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
	f.ApplySnapshotHeader(ceiling, cutover)
	f.encryptionCheckpoint = checkpoint
	return nil
}
