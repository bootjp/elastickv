package encryptionwiring

import (
	"github.com/bootjp/elastickv/internal/encryption"
	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
)

// Called with r.mu held at the actual publication site, not from observation.
func (r *RaftEnvelopeRuntime) installObservedWrap(g *kv.ShardGroup, wrap kv.RaftPayloadWrapper, mutation *kv.RaftPayloadWrapMutation) {
	if r.installedWraps == nil {
		r.installedWraps = make(map[*kv.ShardGroup]kv.RaftPayloadWrapObservation)
	}
	r.installedWraps[g] = g.SetObservedRaftPayloadWrapInMutation(wrap, mutation)
}

type bootstrapEncryptionInstalledProjection struct {
	Storage                                      store.EncryptionWriteObservation
	raftKey                                      uint32
	raftFullNodeID                               uint64
	raftWrapperIdentity                          uint64
	raftNode, raftEpoch                          uint16
	raftCutover                                  uint64
	raftKeyLoaded, raftRegistered, raftInstalled bool
}

// NewBootstrapEncryptionProjectionInput binds the actual runtime endpoints.
// fence must reject fatal/retired state, and return Pending while reconciliation
// is in progress. The supervisor must also compare its EncryptionTicket before
// publication: the returned generation is only the local observation boundary.
func NewBootstrapEncryptionProjectionInput(
	configured, rotationRequested bool, sidecarPath string, group *kv.ShardGroup,
	w WriteWiring, fullNodeID uint64, fence func() (uint64, error),
) (BootstrapEncryptionProjectionInput, error) {
	in := BootstrapEncryptionProjectionInput{
		Configured: configured, RotationRequested: rotationRequested, SidecarPath: sidecarPath,
		FullNodeID: fullNodeID, StorageEpoch: w.Epoch, RaftEpoch: w.RaftEpoch,
	}
	if !configured {
		if rotationRequested {
			return in, errors.Wrap(ErrBootstrapEncryptionUnsupported, "rotation without encryption")
		}
		return in, nil
	}
	if group == nil || !w.EncryptionConfigured() {
		return in, errors.Wrap(ErrBootstrapEncryptionUnsupported, "missing actual group/encryption wiring")
	}
	in.LocalStore = group.Store
	in = in.BindInstalledObservation(group, w, fence)
	return in, in.validate()
}

func (r *RaftEnvelopeRuntime) ObserveInstalled(g *kv.ShardGroup, keyID uint32) (bootstrapEncryptionInstalledProjection, error) {
	var out bootstrapEncryptionInstalledProjection
	if r == nil {
		return out, errors.Wrap(ErrBootstrapEncryptionUnsupported, "no raft envelope runtime")
	}
	r.Mu.Lock()
	defer r.Mu.Unlock()
	nf, ok := r.NonceFactory.(*encryption.DeterministicNonceFactory)
	if !ok || nf == nil || r.CutoverIndex == nil {
		return out, errors.Wrap(ErrBootstrapEncryptionUnsupported, "incomplete raft nonce/cutover wiring")
	}
	node, epoch, exhausted := nf.NonceIdentity()
	if exhausted {
		return out, errors.WithStack(encryption.ErrWriteCountExhausted)
	}
	if r.RaftEpoch != epoch {
		return out, errors.New("raft encryption observation: wrapper nonce epoch mismatch")
	}
	return bootstrapEncryptionInstalledProjection{
		raftKey: r.ActiveRaftDEKID, raftNode: node, raftEpoch: epoch,
		raftFullNodeID:      r.FullNodeID,
		raftWrapperIdentity: r.installedWraps[g].Identity(),
		raftCutover:         r.CutoverIndex.Load(), raftKeyLoaded: r.Cipher.HasKey(keyID),
		raftRegistered: r.Registration.Registered(r.ActiveRaftDEKID, epoch),
		raftInstalled:  r.GroupWrapInstalled(g),
	}, nil
}

// Called with r.mu held. The token must originate from this runtime's trusted
// installer and still be the actual proposal cell for this attached group.
func (r *RaftEnvelopeRuntime) GroupWrapInstalled(g *kv.ShardGroup) bool {
	if r.activeWrap == nil || !g.HasRaftPayloadWrap(r.installedWraps[g]) {
		return false
	}
	for _, group := range r.Groups {
		if group == g {
			return true
		}
	}
	return false
}

// BindInstalledObservation binds real local endpoints. runtimeFence must return
// a nonzero monotonic generation and fail for fatal/retired dependencies. All
// dependency mutation must invalidate that generation BEFORE becoming visible
// and make the fence fail until a new settled generation can be observed;
// publication must recheck it under the caller's arbiter. No I/O is done in A.
func (in BootstrapEncryptionProjectionInput) BindInstalledObservation(
	g *kv.ShardGroup, w WriteWiring, runtimeFence func() (uint64, error),
) BootstrapEncryptionProjectionInput {
	in.RuntimeFence = runtimeFence
	in.StorageWrapperInstalled = nil
	in.RaftWrapperInstalled = nil
	in.installed = func(raftKeyID uint32) (bootstrapEncryptionInstalledProjection, error) {
		var out bootstrapEncryptionInstalledProjection
		if g == nil || g.Store != in.LocalStore {
			return out, errors.Wrap(ErrBootstrapEncryptionUnsupported, "group/store binding mismatch")
		}
		source, ok := g.Store.(interface {
			ObserveEncryptionWritePath() (store.EncryptionWriteObservation, error)
		})
		if !ok {
			return out, errors.Wrap(ErrBootstrapEncryptionUnsupported, "no installed storage observation")
		}
		out, err := w.RaftEnvelope.ObserveInstalled(g, raftKeyID)
		if err != nil {
			return bootstrapEncryptionInstalledProjection{}, err
		}
		out.Storage, err = source.ObserveEncryptionWritePath()
		return out, errors.Wrap(err, "observe installed storage write path")
	}
	return in
}

func (p *BootstrapEncryptionProjection) validateInstalled(in BootstrapEncryptionProjectionInput) error {
	actual, err := in.installed(p.Raft.DekID)
	if err != nil {
		return errors.Wrap(err, "bootstrap encryption projection: installed path")
	}
	if err := p.ValidateInstalledStorage(actual.Storage); err != nil {
		return err
	}
	if err := p.ValidateInstalledRaft(actual); err != nil {
		return err
	}
	p.Storage.WrapperInstalled = p.Storage.EnvelopeActive
	p.Raft.WrapperInstalled = p.Raft.EnvelopeActive
	if p.Raft.WrapperInstalled {
		p.Raft.wrapperIdentity = actual.raftWrapperIdentity
	}
	return nil
}

// Wire before admission starts. The callback must only close memory-owned
// authority; it must not perform store I/O, block, or reenter this runtime.
func (r *RaftEnvelopeRuntime) SetBeforeEncryptionChange(invalidate func() func()) {
	r.Mu.Lock()
	if invalidate == nil {
		r.beforeEncryptionChange.Store(nil)
	} else {
		r.beforeEncryptionChange.Store(&invalidate)
	}
	if r.Registration != nil {
		if invalidate == nil {
			r.Registration.beforeEncryptionChange.Store(nil)
		} else {
			r.Registration.beforeEncryptionChange.Store(&invalidate)
		}
	}
	for _, g := range r.Groups {
		g.SetRaftPayloadWrapInvalidator(invalidate)
	}
	r.Mu.Unlock()
}

func (r *RaftEnvelopeRuntime) beginEncryptionChange() *kv.RaftPayloadWrapMutation {
	var begin func() func()
	if callback := r.beforeEncryptionChange.Load(); callback != nil {
		begin = *callback
	}
	return kv.BeginRaftPayloadWrapMutation(begin)
}

func (p BootstrapEncryptionProjection) ValidateInstalledStorage(s store.EncryptionWriteObservation) error {
	want := p.Storage
	if s.NodeID != encryption.NodeID16(p.FullNodeID) || s.Epoch != want.Epoch {
		return errors.New("bootstrap encryption projection: installed storage identity/epoch mismatch")
	}
	if s.KeyID != want.DekID || s.EnvelopeActive != want.EnvelopeActive {
		return errors.Wrap(ErrBootstrapEncryptionProjectionPending, "storage key/gate not reconciled")
	}
	if want.DekID != 0 && !s.KeyLoaded {
		return errors.New("bootstrap encryption projection: storage key not loaded")
	}
	if want.EnvelopeActive && !s.Registered {
		return errors.Wrap(ErrBootstrapEncryptionProjectionPending, "storage registration gate closed")
	}
	return nil
}

func (p BootstrapEncryptionProjection) ValidateInstalledRaft(s bootstrapEncryptionInstalledProjection) error {
	want := p.Raft
	if err := p.validateInstalledRaftIdentity(s); err != nil {
		return err
	}
	if want.DekID != 0 && !s.raftKeyLoaded {
		return errors.New("bootstrap encryption projection: raft key not loaded")
	}
	// Before cutover the runtime intentionally has no active wrapper/key.
	if !want.EnvelopeActive {
		return nil
	}
	if s.raftKey != want.DekID || !s.raftInstalled || !s.raftRegistered {
		return errors.Wrap(ErrBootstrapEncryptionProjectionPending, "raft key/wrapper/registration missing")
	}
	return nil
}

func (p BootstrapEncryptionProjection) validateInstalledRaftIdentity(s bootstrapEncryptionInstalledProjection) error {
	if s.raftFullNodeID != p.FullNodeID {
		return errors.New("bootstrap encryption projection: raft full node identity mismatch")
	}
	if s.raftNode != encryption.NodeID16(p.FullNodeID) || s.raftEpoch != p.Raft.Epoch {
		return errors.New("bootstrap encryption projection: installed raft identity/epoch mismatch")
	}
	if s.raftCutover != p.Raft.CutoverIndex {
		return errors.Wrap(ErrBootstrapEncryptionProjectionPending, "raft cutover not reconciled")
	}
	return nil
}
