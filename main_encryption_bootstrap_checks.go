package main

import (
	"github.com/bootjp/elastickv/internal/encryption"
	"github.com/bootjp/elastickv/internal/noderuntime"
	"github.com/cockroachdb/errors"
)

func validateBootstrapDomainEncryptionWiring(w encryptionWriteWiring, groups []noderuntime.GroupSpec) error {
	if w.cache == nil || w.cipher == nil || w.raftEnvelope == nil || w.raftRegistration == nil {
		return errors.Wrap(errBootstrapEncryptionUnsupported, "protected encryption requires actual complete write wiring")
	}
	r := w.raftEnvelope
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.fullNodeID == 0 || r.cipher != w.cipher || r.registration != w.raftRegistration || r.cutoverIndex == nil {
		return errors.Wrap(errBootstrapEncryptionUnsupported, "raft encryption wiring identity mismatch")
	}
	if err := validateRaftEnvelopeStartupScope(r.cutoverIndex.Load(), groups); err != nil {
		return err
	}
	return validateBootstrapDomainNonceWiring(w, r)
}

func validateBootstrapDomainNonceWiring(w encryptionWriteWiring, r *raftEnvelopeRuntime) error {
	storage, ok := w.nonceFactory.(*encryption.DeterministicNonceFactory)
	if !ok || storage == nil {
		return errors.Wrap(errBootstrapEncryptionUnsupported, "missing actual storage nonce factory")
	}
	raft, ok := r.nonceFactory.(*encryption.DeterministicNonceFactory)
	if !ok || raft == nil || r.nonceFactory != w.raftNonceFactory {
		return errors.Wrap(errBootstrapEncryptionUnsupported, "missing actual raft nonce factory")
	}
	snode, sepoch, sexhausted := storage.NonceIdentity()
	rnode, repoch, rexhausted := raft.NonceIdentity()
	if sexhausted || rexhausted {
		return errors.WithStack(encryption.ErrWriteCountExhausted)
	}
	if snode != encryption.NodeID16(r.fullNodeID) || rnode != snode {
		return errors.Wrap(errBootstrapEncryptionUnsupported, "pinned encryption node identity mismatch")
	}
	return validateBootstrapDomainNonceEpochs(w, r.raftEpoch, sepoch, repoch)
}

func validateBootstrapDomainNonceEpochs(w encryptionWriteWiring, runtimeEpoch, storageEpoch, raftEpoch uint16) error {
	if storageEpoch != w.epoch || raftEpoch != w.raftEpoch || runtimeEpoch != raftEpoch {
		return errors.Wrap(errBootstrapEncryptionUnsupported, "pinned encryption nonce identity mismatch")
	}
	return nil
}
