package store

import (
	"encoding/binary"

	"github.com/bootjp/elastickv/internal/encryption"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/v2"
)

var encryptionCheckpointKey = []byte("_meta_encryption_snapshot_checkpoint")

const encryptionCheckpointSize = 8 + 32

// SaveEncryptionSnapshotCheckpoint is an operational metadata write, like the
// writer registry: no user timestamp, nonce, or applied watermark is issued.
// It is durable before the encryption handler returns and later data entries
// can advance the durable applied index past it safely.
func (s *pebbleStore) SaveEncryptionSnapshotCheckpoint(cp encryption.SnapshotCheckpoint) error {
	s.applyMu.Lock()
	defer s.applyMu.Unlock()
	s.dbMu.RLock()
	defer s.dbMu.RUnlock()
	if cp.Index == 0 {
		return errors.New("encryption snapshot checkpoint: zero durable index")
	}
	var raw [encryptionCheckpointSize]byte
	binary.BigEndian.PutUint64(raw[:8], cp.Index)
	copy(raw[8:], cp.State[:])
	return errors.Wrap(s.db.Set(encryptionCheckpointKey, raw[:], pebble.Sync), "persist encryption snapshot checkpoint")
}

func (s *pebbleStore) LoadEncryptionSnapshotCheckpoint() (encryption.SnapshotCheckpoint, bool, error) {
	s.dbMu.RLock()
	defer s.dbMu.RUnlock()
	var cp encryption.SnapshotCheckpoint
	raw, closer, err := s.db.Get(encryptionCheckpointKey)
	if errors.Is(err, pebble.ErrNotFound) {
		return cp, false, nil
	}
	if err != nil {
		return cp, false, errors.Wrap(err, "read durable encryption snapshot checkpoint")
	}
	defer closer.Close()
	if len(raw) != encryptionCheckpointSize {
		return cp, false, errors.New("encryption snapshot checkpoint: corrupt durable record")
	}
	cp.Index = binary.BigEndian.Uint64(raw[:8])
	copy(cp.State[:], raw[8:])
	if cp.Index == 0 {
		return cp, false, errors.New("encryption snapshot checkpoint: zero durable index")
	}
	return cp, true, nil
}
