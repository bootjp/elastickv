package store

import (
	"github.com/bootjp/elastickv/internal/encryption"
	"github.com/cockroachdb/errors"
)

// EncryptionWriteObservation describes the actual installed storage write path.
// It grants no admission and consumes no nonce. Mutable gates require an external
// generation fence covering apply/restore and key/registration changes.
type EncryptionWriteObservation struct {
	KeyID                                 uint32
	NodeID, Epoch                         uint16
	KeyLoaded, EnvelopeActive, Registered bool
}

func (s *pebbleStore) ObserveEncryptionWritePath() (EncryptionWriteObservation, error) {
	s.dbMu.RLock()
	defer s.dbMu.RUnlock()
	var out EncryptionWriteObservation
	if s.db == nil || s.cipher == nil || s.activeStorageKeyID == nil || s.storageEnvelopeActive == nil || s.storageRegistered == nil {
		return out, errors.New("storage encryption observation: incomplete or closed write path")
	}
	nf, ok := s.nonceFactory.(*encryption.DeterministicNonceFactory)
	if !ok || nf == nil {
		return out, errors.New("storage encryption observation: unsupported nonce factory")
	}
	node, epoch, exhausted := nf.NonceIdentity()
	if exhausted {
		return out, errors.WithStack(encryption.ErrWriteCountExhausted)
	}
	id, active := s.activeStorageKeyID()
	if !active {
		id = 0
	}
	return EncryptionWriteObservation{
		KeyID: id, NodeID: node, Epoch: epoch, KeyLoaded: s.cipher.HasKey(id),
		EnvelopeActive: s.storageEnvelopeActive(), Registered: s.storageRegistered(),
	}, nil
}
