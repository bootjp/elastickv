package encryptionwiring

import (
	"github.com/bootjp/elastickv/internal/encryption"
	"github.com/bootjp/elastickv/internal/noderuntime"
	"github.com/cockroachdb/errors"
)

func ValidateBootstrapDomainEncryptionWiring(w WriteWiring, groups []noderuntime.GroupSpec) error {
	if w.Cache == nil || w.Cipher == nil || w.RaftEnvelope == nil || w.RaftRegistration == nil {
		return errors.Wrap(ErrBootstrapEncryptionUnsupported, "protected encryption requires actual complete write wiring")
	}
	r := w.RaftEnvelope
	r.Mu.Lock()
	defer r.Mu.Unlock()
	if r.FullNodeID == 0 || r.Cipher != w.Cipher || r.Registration != w.RaftRegistration || r.CutoverIndex == nil {
		return errors.Wrap(ErrBootstrapEncryptionUnsupported, "raft encryption wiring identity mismatch")
	}
	if err := validateRaftEnvelopeStartupScope(r.CutoverIndex.Load(), groups); err != nil {
		return err
	}
	return validateBootstrapDomainNonceWiring(w, r)
}

func validateBootstrapDomainNonceWiring(w WriteWiring, r *RaftEnvelopeRuntime) error {
	storage, ok := w.NonceFactory.(*encryption.DeterministicNonceFactory)
	if !ok || storage == nil {
		return errors.Wrap(ErrBootstrapEncryptionUnsupported, "missing actual storage nonce factory")
	}
	raft, ok := r.NonceFactory.(*encryption.DeterministicNonceFactory)
	if !ok || raft == nil || r.NonceFactory != w.RaftNonceFactory {
		return errors.Wrap(ErrBootstrapEncryptionUnsupported, "missing actual raft nonce factory")
	}
	snode, sepoch, sexhausted := storage.NonceIdentity()
	rnode, repoch, rexhausted := raft.NonceIdentity()
	if sexhausted || rexhausted {
		return errors.WithStack(encryption.ErrWriteCountExhausted)
	}
	if snode != encryption.NodeID16(r.FullNodeID) || rnode != snode {
		return errors.Wrap(ErrBootstrapEncryptionUnsupported, "pinned encryption node identity mismatch")
	}
	return validateBootstrapDomainNonceEpochs(w, r.RaftEpoch, sepoch, repoch)
}

func validateBootstrapDomainNonceEpochs(w WriteWiring, runtimeEpoch, storageEpoch, raftEpoch uint16) error {
	if storageEpoch != w.Epoch || raftEpoch != w.RaftEpoch || runtimeEpoch != raftEpoch {
		return errors.Wrap(ErrBootstrapEncryptionUnsupported, "pinned encryption nonce identity mismatch")
	}
	return nil
}
