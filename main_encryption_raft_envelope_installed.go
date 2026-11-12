package main

import (
	"github.com/bootjp/elastickv/internal/encryption"
	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
)

// Called with r.mu held at the actual publication site, not from observation.
func (r *raftEnvelopeRuntime) installObservedWrap(g *kv.ShardGroup, wrap kv.RaftPayloadWrapper, mutation *kv.RaftPayloadWrapMutation) {
	if r.installedWraps == nil {
		r.installedWraps = make(map[*kv.ShardGroup]kv.RaftPayloadWrapObservation)
	}
	r.installedWraps[g] = g.SetObservedRaftPayloadWrapInMutation(wrap, mutation)
}

type bootstrapEncryptionInstalledProjection struct {
	storage                                      store.EncryptionWriteObservation
	raftKey                                      uint32
	raftFullNodeID                               uint64
	raftWrapperIdentity                          uint64
	raftNode, raftEpoch                          uint16
	raftCutover                                  uint64
	raftKeyLoaded, raftRegistered, raftInstalled bool
}

// newBootstrapEncryptionProjectionInput binds the actual runtime endpoints.
// fence must reject fatal/retired state, and return Pending while reconciliation
// is in progress. The supervisor must also compare its EncryptionTicket before
// publication: the returned generation is only the local observation boundary.
func newBootstrapEncryptionProjectionInput(
	configured, rotationRequested bool, sidecarPath string, group *kv.ShardGroup,
	w encryptionWriteWiring, fullNodeID uint64, fence func() (uint64, error),
) (bootstrapEncryptionProjectionInput, error) {
	in := bootstrapEncryptionProjectionInput{
		configured: configured, rotationRequested: rotationRequested, sidecarPath: sidecarPath,
		fullNodeID: fullNodeID, storageEpoch: w.epoch, raftEpoch: w.raftEpoch,
	}
	if !configured {
		if rotationRequested {
			return in, errors.Wrap(errBootstrapEncryptionUnsupported, "rotation without encryption")
		}
		return in, nil
	}
	if group == nil || !w.encryptionConfigured() {
		return in, errors.Wrap(errBootstrapEncryptionUnsupported, "missing actual group/encryption wiring")
	}
	in.localStore = group.Store
	in = in.bindInstalledObservation(group, w, fence)
	return in, in.validate()
}

func (r *raftEnvelopeRuntime) observeInstalled(g *kv.ShardGroup, keyID uint32) (bootstrapEncryptionInstalledProjection, error) {
	var out bootstrapEncryptionInstalledProjection
	if r == nil {
		return out, errors.Wrap(errBootstrapEncryptionUnsupported, "no raft envelope runtime")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	nf, ok := r.nonceFactory.(*encryption.DeterministicNonceFactory)
	if !ok || nf == nil || r.cutoverIndex == nil {
		return out, errors.Wrap(errBootstrapEncryptionUnsupported, "incomplete raft nonce/cutover wiring")
	}
	node, epoch, exhausted := nf.NonceIdentity()
	if exhausted {
		return out, errors.WithStack(encryption.ErrWriteCountExhausted)
	}
	if r.raftEpoch != epoch {
		return out, errors.New("raft encryption observation: wrapper nonce epoch mismatch")
	}
	return bootstrapEncryptionInstalledProjection{
		raftKey: r.activeRaftDEKID, raftNode: node, raftEpoch: epoch,
		raftFullNodeID:      r.fullNodeID,
		raftWrapperIdentity: r.installedWraps[g].Identity(),
		raftCutover:         r.cutoverIndex.Load(), raftKeyLoaded: r.cipher.HasKey(keyID),
		raftRegistered: r.registration.Registered(r.activeRaftDEKID, epoch),
		raftInstalled:  r.groupWrapInstalled(g),
	}, nil
}

// Called with r.mu held. The token must originate from this runtime's trusted
// installer and still be the actual proposal cell for this attached group.
func (r *raftEnvelopeRuntime) groupWrapInstalled(g *kv.ShardGroup) bool {
	if r.activeWrap == nil || !g.HasRaftPayloadWrap(r.installedWraps[g]) {
		return false
	}
	for _, group := range r.groups {
		if group == g {
			return true
		}
	}
	return false
}

// bindInstalledObservation binds real local endpoints. runtimeFence must return
// a nonzero monotonic generation and fail for fatal/retired dependencies. All
// dependency mutation must invalidate that generation BEFORE becoming visible
// and make the fence fail until a new settled generation can be observed;
// publication must recheck it under the caller's arbiter. No I/O is done in A.
func (in bootstrapEncryptionProjectionInput) bindInstalledObservation(
	g *kv.ShardGroup, w encryptionWriteWiring, runtimeFence func() (uint64, error),
) bootstrapEncryptionProjectionInput {
	in.runtimeFence = runtimeFence
	in.storageWrapperInstalled = nil
	in.raftWrapperInstalled = nil
	in.installed = func(raftKeyID uint32) (bootstrapEncryptionInstalledProjection, error) {
		var out bootstrapEncryptionInstalledProjection
		if g == nil || g.Store != in.localStore {
			return out, errors.Wrap(errBootstrapEncryptionUnsupported, "group/store binding mismatch")
		}
		source, ok := g.Store.(interface {
			ObserveEncryptionWritePath() (store.EncryptionWriteObservation, error)
		})
		if !ok {
			return out, errors.Wrap(errBootstrapEncryptionUnsupported, "no installed storage observation")
		}
		out, err := w.raftEnvelope.observeInstalled(g, raftKeyID)
		if err != nil {
			return bootstrapEncryptionInstalledProjection{}, err
		}
		out.storage, err = source.ObserveEncryptionWritePath()
		return out, errors.Wrap(err, "observe installed storage write path")
	}
	return in
}

func (p *bootstrapEncryptionProjection) validateInstalled(in bootstrapEncryptionProjectionInput) error {
	actual, err := in.installed(p.raft.dekID)
	if err != nil {
		return errors.Wrap(err, "bootstrap encryption projection: installed path")
	}
	if err := p.validateInstalledStorage(actual.storage); err != nil {
		return err
	}
	if err := p.validateInstalledRaft(actual); err != nil {
		return err
	}
	p.storage.wrapperInstalled = p.storage.envelopeActive
	p.raft.wrapperInstalled = p.raft.envelopeActive
	if p.raft.wrapperInstalled {
		p.raft.wrapperIdentity = actual.raftWrapperIdentity
	}
	return nil
}

// Wire before admission starts. The callback must only close memory-owned
// authority; it must not perform store I/O, block, or reenter this runtime.
func (r *raftEnvelopeRuntime) setBeforeEncryptionChange(invalidate func() func()) {
	r.mu.Lock()
	if invalidate == nil {
		r.beforeEncryptionChange.Store(nil)
	} else {
		r.beforeEncryptionChange.Store(&invalidate)
	}
	if r.registration != nil {
		if invalidate == nil {
			r.registration.beforeEncryptionChange.Store(nil)
		} else {
			r.registration.beforeEncryptionChange.Store(&invalidate)
		}
	}
	for _, g := range r.groups {
		g.SetRaftPayloadWrapInvalidator(invalidate)
	}
	r.mu.Unlock()
}

func (r *raftEnvelopeRuntime) beginEncryptionChange() *kv.RaftPayloadWrapMutation {
	var begin func() func()
	if callback := r.beforeEncryptionChange.Load(); callback != nil {
		begin = *callback
	}
	return kv.BeginRaftPayloadWrapMutation(begin)
}

func (p bootstrapEncryptionProjection) validateInstalledStorage(s store.EncryptionWriteObservation) error {
	want := p.storage
	if s.NodeID != encryption.NodeID16(p.fullNodeID) || s.Epoch != want.epoch {
		return errors.New("bootstrap encryption projection: installed storage identity/epoch mismatch")
	}
	if s.KeyID != want.dekID || s.EnvelopeActive != want.envelopeActive {
		return errors.Wrap(errBootstrapEncryptionProjectionPending, "storage key/gate not reconciled")
	}
	if want.dekID != 0 && !s.KeyLoaded {
		return errors.New("bootstrap encryption projection: storage key not loaded")
	}
	if want.envelopeActive && !s.Registered {
		return errors.Wrap(errBootstrapEncryptionProjectionPending, "storage registration gate closed")
	}
	return nil
}

func (p bootstrapEncryptionProjection) validateInstalledRaft(s bootstrapEncryptionInstalledProjection) error {
	want := p.raft
	if err := p.validateInstalledRaftIdentity(s); err != nil {
		return err
	}
	if want.dekID != 0 && !s.raftKeyLoaded {
		return errors.New("bootstrap encryption projection: raft key not loaded")
	}
	// Before cutover the runtime intentionally has no active wrapper/key.
	if !want.envelopeActive {
		return nil
	}
	if s.raftKey != want.dekID || !s.raftInstalled || !s.raftRegistered {
		return errors.Wrap(errBootstrapEncryptionProjectionPending, "raft key/wrapper/registration missing")
	}
	return nil
}

func (p bootstrapEncryptionProjection) validateInstalledRaftIdentity(s bootstrapEncryptionInstalledProjection) error {
	if s.raftFullNodeID != p.fullNodeID {
		return errors.New("bootstrap encryption projection: raft full node identity mismatch")
	}
	if s.raftNode != encryption.NodeID16(p.fullNodeID) || s.raftEpoch != p.raft.epoch {
		return errors.New("bootstrap encryption projection: installed raft identity/epoch mismatch")
	}
	if s.raftCutover != p.raft.cutoverIndex {
		return errors.Wrap(errBootstrapEncryptionProjectionPending, "raft cutover not reconciled")
	}
	return nil
}
