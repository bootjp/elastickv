package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"strconv"

	"github.com/bootjp/elastickv/adapter"
	"github.com/bootjp/elastickv/internal/encryption"
	"github.com/bootjp/elastickv/internal/encryption/kek"
	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
)

type bootstrapEncryptionRuntimeIOConfig struct {
	DefaultRuntime    *raftGroupRuntime
	Group             *kv.ShardGroup
	Wiring            encryptionWriteWiring
	SidecarPath       string
	Wrapper           kek.Wrapper
	Domain            *bootstrapDomain
	NodeID            uint64
	RotationRequested bool
}

type bootstrapEncryptionRuntimeIO struct {
	config   bootstrapEncryptionRuntimeIOConfig
	engine   raftengine.Engine
	registry encryption.WriterRegistryStore
	input    bootstrapEncryptionProjectionInput
	admin    *adapter.EncryptionAdminServer
}

// Construct under startup exclusivity BEFORE registration/watchers are started.
// This binds actual endpoints and memory-only paired hooks, never readiness/F.
func newBootstrapEncryptionRuntimeIO(cfg bootstrapEncryptionRuntimeIOConfig) (*bootstrapEncryptionRuntimeIO, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	engine := cfg.DefaultRuntime.snapshotEngine()
	registry, err := store.WriterRegistryFor(cfg.Group.Store)
	if err != nil {
		return nil, errors.Wrap(err, "encryption runtime IO registry")
	}
	in, err := newBootstrapEncryptionProjectionInput(true, cfg.RotationRequested, cfg.SidecarPath, cfg.Group, cfg.Wiring, cfg.NodeID, cfg.Domain.admission.EncryptionFence)
	if err != nil {
		return nil, err
	}
	admin := adapter.NewEncryptionAdminServer(
		adapter.WithEncryptionAdminFullNodeID(cfg.NodeID), adapter.WithEncryptionAdminSidecarPath(cfg.SidecarPath),
		adapter.WithEncryptionAdminLeaderView(engine), adapter.WithEncryptionAdminRegistryLeaderView(engine),
		adapter.WithEncryptionAdminWriterRegistry(registry), adapter.WithEncryptionAdminProposer(engine),
		adapter.WithEncryptionAdminPostCutoverProposer(cfg.Group.Proposer()))
	if err := admin.Validate(); err != nil {
		return nil, errors.Wrap(err, "encryption runtime IO admin")
	}
	cfg.Wiring.cache.SetBeforeEncryptionChange(cfg.Domain.admission.BeginEncryptionMutation)
	cfg.Wiring.raftEnvelope.setBeforeEncryptionChange(cfg.Domain.admission.BeginEncryptionMutation)
	return &bootstrapEncryptionRuntimeIO{config: cfg, engine: engine, registry: registry, input: in, admin: admin}, nil
}

func (cfg bootstrapEncryptionRuntimeIOConfig) validate() error {
	if cfg.DefaultRuntime == nil || cfg.Group == nil || cfg.Domain == nil || cfg.Domain.admission == nil {
		return errors.Wrap(errBootstrapEncryptionUnsupported, "missing protected runtime/group/domain")
	}
	if cfg.NodeID == 0 || cfg.SidecarPath == "" || cfg.Wrapper == nil || cfg.Wiring.cache == nil || cfg.Wiring.raftEnvelope == nil {
		return errors.Wrap(errBootstrapEncryptionUnsupported, "missing encryption IO dependencies")
	}
	return cfg.validateBinding()
}

func (cfg bootstrapEncryptionRuntimeIOConfig) validateBinding() error {
	rt := cfg.DefaultRuntime
	engine := rt.snapshotEngine()
	if engine == nil || cfg.Group.Engine != engine || cfg.Group.Store != rt.Store || rt.bootstrapDomain != cfg.Domain || rt.Spec.ID == 0 {
		return errors.Wrap(errBootstrapEncryptionUnsupported, "runtime/group/domain binding mismatch")
	}
	if cfg.Group.Proposer() == engine {
		return errors.Wrap(errBootstrapEncryptionUnsupported, "raw proposer fallback is not a protected wrapped path")
	}
	r := cfg.Wiring.raftEnvelope
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.fullNodeID != cfg.NodeID || r.cipher != cfg.Wiring.cipher || r.groups[rt.Spec.ID] != cfg.Group {
		return errors.Wrap(errBootstrapEncryptionUnsupported, "installed raft runtime binding mismatch")
	}
	return nil
}

func (io *bootstrapEncryptionRuntimeIO) Input() bootstrapEncryptionProjectionInput { return io.input }

func (io *bootstrapEncryptionRuntimeIO) check(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return errors.WithStack(err)
	}
	if io.config.DefaultRuntime.snapshotEngine() != io.engine {
		return errors.Wrap(errBootstrapEncryptionUnsupported, "runtime engine retired")
	}
	return nil
}

func (io *bootstrapEncryptionRuntimeIO) Observe(ctx context.Context) (bootstrapEncryptionProjection, error) {
	if err := io.check(ctx); err != nil {
		return bootstrapEncryptionProjection{}, err
	}
	p, err := readBootstrapEncryptionProjection(io.input)
	if err != nil {
		return bootstrapEncryptionProjection{}, err
	}
	sc, err := io.readSidecarAt(p)
	if err != nil {
		return bootstrapEncryptionProjection{}, err
	}
	if err := io.verifyActiveMaterial(ctx, sc); err != nil {
		return bootstrapEncryptionProjection{}, err
	}
	after, err := readBootstrapEncryptionProjection(io.input)
	if err != nil {
		return bootstrapEncryptionProjection{}, err
	}
	if p.generation != after.generation || p.digest() != after.digest() {
		return bootstrapEncryptionProjection{}, errors.Wrap(errBootstrapEncryptionProjectionPending, "encryption changed during material verification")
	}
	if err := io.check(ctx); err != nil {
		return bootstrapEncryptionProjection{}, err
	}
	return after, nil
}

func (io *bootstrapEncryptionRuntimeIO) verifyActiveMaterial(ctx context.Context, sc *encryption.Sidecar) error {
	for _, id := range []uint32{sc.Active.Storage, sc.Active.Raft} {
		if id != 0 {
			if err := io.verifyMaterial(ctx, id, sc.Keys[strconv.FormatUint(uint64(id), 10)]); err != nil {
				return err
			}
		}
	}
	return nil
}

func (io *bootstrapEncryptionRuntimeIO) readSidecarAt(p bootstrapEncryptionProjection) (*encryption.Sidecar, error) {
	sc, err := encryption.ReadSidecar(io.config.SidecarPath)
	if err != nil {
		return nil, errors.Wrap(err, "encryption runtime IO sidecar")
	}
	cp, err := encryption.SidecarCheckpoint(sc)
	if err != nil {
		return nil, errors.Wrap(err, "encryption runtime IO checkpoint")
	}
	if cp != p.checkpoint {
		return nil, errors.Wrap(errBootstrapEncryptionProjectionPending, "sidecar changed after applied observation")
	}
	return sc, nil
}

func (io *bootstrapEncryptionRuntimeIO) RecipeConfig(barrier func(context.Context) (uint64, error), validate func(context.Context) error) bootstrapRotationRecipeConfig {
	return bootstrapRotationRecipeConfig{
		Admin: io.admin, Barrier: barrier, Validate: validate, IsLeader: func() bool {
			return io.config.DefaultRuntime.snapshotEngine() == io.engine && io.engine.State() == raftengine.StateLeader
		},
		ReadSidecar: func() (*encryption.Sidecar, error) {
			sc, err := encryption.ReadSidecar(io.config.SidecarPath)
			return sc, errors.Wrap(err, "rotation runtime sidecar")
		},
		Registry: io.registry, Wrapper: io.config.Wrapper, NodeID: io.config.NodeID, StorageEpoch: io.config.Wiring.epoch, RaftEpoch: io.config.Wiring.raftEpoch,
		InstallAndVerify: io.installAndVerify,
	}
}

// ValidateEpoch validates a locally owned DEK registration, not a peer's epoch.
// Historical IDs are permitted only with exact durable purpose/pinned epoch;
// accepting their registration never makes them current write authority.
func (io *bootstrapEncryptionRuntimeIO) ValidateEpoch(id uint32, epoch uint16) error {
	sc, err := encryption.ReadSidecar(io.config.SidecarPath)
	if err != nil {
		return errors.Wrap(err, "registration epoch sidecar")
	}
	key, ok := sc.Keys[strconv.FormatUint(uint64(id), 10)]
	if !ok || id == 0 || len(key.Wrapped) == 0 {
		return errors.Wrap(errBootstrapEncryptionUnsupported, "registration DEK is not locally owned")
	}
	pinned, err := io.purposeEpoch(key.Purpose)
	if err != nil {
		return err
	}
	if key.LocalEpoch != epoch || pinned != epoch {
		return errors.Wrap(encryption.ErrLocalEpochRollback, "registration epoch differs from durable/pinned epoch")
	}
	return nil
}

func (io *bootstrapEncryptionRuntimeIO) purposeEpoch(purpose string) (uint16, error) {
	switch purpose {
	case encryption.SidecarPurposeStorage:
		return io.config.Wiring.epoch, nil
	case encryption.SidecarPurposeRaft:
		return io.config.Wiring.raftEpoch, nil
	default:
		return 0, errors.Wrap(errBootstrapEncryptionUnsupported, "unknown DEK purpose")
	}
}

// Cryptographically compare the unwrapped material with the installed cipher
// by decrypting a private synthetic challenge. No nonce factory is consumed,
// installed keystore is mutated, ciphertext is emitted, or plaintext/key logged.
func (io *bootstrapEncryptionRuntimeIO) verifyMaterial(ctx context.Context, id uint32, key encryption.SidecarKey) error {
	if err := io.check(ctx); err != nil {
		return err
	}
	dek, err := io.config.Wrapper.Unwrap(bytes.Clone(key.Wrapped))
	if err != nil {
		return errors.New("encryption runtime IO: KEK unwrap failed")
	}
	defer clear(dek)
	keys := encryption.NewKeystore()
	if err := keys.Set(id, dek); err != nil {
		return errors.New("encryption runtime IO: invalid DEK material")
	}
	expected, err := encryption.NewCipher(keys)
	if err != nil {
		return errors.Wrap(err, "encryption material verifier")
	}
	var nonce [encryption.NonceSize]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return errors.Wrap(err, "encryption material challenge nonce")
	}
	challenge := []byte("elastickv/installed-key-proof/v1")
	sealed, err := expected.Encrypt(challenge, nil, id, nonce[:])
	if err != nil {
		return errors.Wrap(err, "encryption material challenge")
	}
	plain, err := io.config.Wiring.cipher.Decrypt(sealed, nil, id, nonce[:])
	if err != nil || !bytes.Equal(plain, challenge) {
		return errors.New("encryption runtime IO: installed cipher material mismatch")
	}
	return io.check(ctx)
}

func (io *bootstrapEncryptionRuntimeIO) installAndVerify(ctx context.Context, id uint32, key encryption.SidecarKey) error {
	if err := io.ValidateEpoch(id, key.LocalEpoch); err != nil {
		return err
	}
	sc, err := io.activeKey(ctx, id, key)
	if err != nil {
		return err
	}
	if err := io.verifyMaterial(ctx, id, key); err != nil {
		return err
	}
	if _, err := readBootstrapWriterRegistration(io.registry, id, io.config.NodeID, key.LocalEpoch); err != nil {
		return err
	}
	if key.Purpose == encryption.SidecarPurposeRaft && sc.RaftEnvelopeCutoverIndex != 0 {
		if err := io.repairRaftWrapper(ctx, id, sc.RaftEnvelopeCutoverIndex); err != nil {
			return err
		}
	}
	if err := io.verifyInstalledPurpose(id, key, sc); err != nil {
		return err
	}
	_, err = io.activeKey(ctx, id, key)
	return err
}

// Reconcile verifies storage before raft. A per-purpose installer must not
// demand the complete projection before the later purpose can be repaired.
func (io *bootstrapEncryptionRuntimeIO) verifyInstalledPurpose(id uint32, key encryption.SidecarKey, sc *encryption.Sidecar) error {
	p := bootstrapEncryptionProjection{fullNodeID: io.config.NodeID}
	if key.Purpose == encryption.SidecarPurposeRaft {
		p.raft = bootstrapEncryptionPurposeProjection{dekID: id, epoch: key.LocalEpoch, envelopeActive: sc.RaftEnvelopeCutoverIndex != 0, cutoverIndex: sc.RaftEnvelopeCutoverIndex}
		actual, err := io.config.Wiring.raftEnvelope.observeInstalled(io.config.Group, id)
		if err != nil {
			return err
		}
		return p.validateInstalledRaft(actual)
	}
	p.storage = bootstrapEncryptionPurposeProjection{dekID: id, epoch: key.LocalEpoch, envelopeActive: sc.StorageEnvelopeActive, cutoverIndex: sc.StorageEnvelopeCutoverIndex}
	source, ok := io.config.Group.Store.(interface {
		ObserveEncryptionWritePath() (store.EncryptionWriteObservation, error)
	})
	if !ok {
		return errors.Wrap(errBootstrapEncryptionUnsupported, "missing storage installed-path observer")
	}
	actual, err := source.ObserveEncryptionWritePath()
	if err != nil {
		return errors.Wrap(err, "rotation installed storage purpose")
	}
	return p.validateInstalledStorage(actual)
}

func (io *bootstrapEncryptionRuntimeIO) activeKey(ctx context.Context, id uint32, key encryption.SidecarKey) (*encryption.Sidecar, error) {
	if err := io.check(ctx); err != nil {
		return nil, err
	}
	source, ok := io.input.localStore.(bootstrapEncryptionProjectionStore)
	if !ok {
		return nil, errors.Wrap(errBootstrapEncryptionUnsupported, "missing checkpoint store")
	}
	applied, cp, err := readBootstrapEncryptionCheckpoint(source)
	if err != nil {
		return nil, err
	}
	sc, err := io.readSidecarAt(bootstrapEncryptionProjection{appliedIndex: applied, checkpoint: cp})
	if err != nil {
		return nil, err
	}
	actual := sc.Keys[strconv.FormatUint(uint64(id), 10)]
	if actual.Purpose != key.Purpose || actual.LocalEpoch != key.LocalEpoch || !bytes.Equal(actual.Wrapped, key.Wrapped) {
		return nil, errors.Wrap(errBootstrapEncryptionProjectionPending, "active key changed before installation")
	}
	active := sc.Active.Storage
	if key.Purpose == encryption.SidecarPurposeRaft {
		active = sc.Active.Raft
	}
	if active != id {
		return nil, errors.Wrap(errBootstrapEncryptionProjectionPending, "key is no longer active")
	}
	return sc, nil
}

func (io *bootstrapEncryptionRuntimeIO) repairRaftWrapper(ctx context.Context, id uint32, cutover uint64) error {
	r := io.config.Wiring.raftEnvelope
	r.mu.Lock()
	installed := r.activeRaftDEKID == id && r.groupWrapInstalled(io.config.Group)
	actualKey := r.activeRaftDEKID
	actualCutover := r.cutoverIndex.Load()
	r.mu.Unlock()
	if actualCutover != cutover {
		return errors.Wrap(errBootstrapEncryptionProjectionPending, "raft cutover not installed by applier")
	}
	if actualKey != id {
		return errors.Wrap(errBootstrapEncryptionProjectionPending, "active raft key awaits applier installation")
	}
	if err := io.check(ctx); err != nil {
		return err
	}
	if !installed {
		// Re-publish the CURRENT installer-owned closure under its mutex. Never
		// overwrite a newer applier-installed key with an old sidecar sample.
		r.reinstallActiveWrap()
	}
	if !r.registration.Registered(id, io.config.Wiring.raftEpoch) {
		r.registration.MarkRegistered(id, io.config.Wiring.raftEpoch)
	}
	return nil
}

// PayloadProjection observes actual wire encoding, including while this load's
// registration is still pending. No sidecar active-ID or cached intent is used.
// The caller still fences enqueue/result against this exact generation under A.
func (io *bootstrapEncryptionRuntimeIO) PayloadProjection(group, generation uint64, grants ...bootstrapRotationPayloadGrant) (bootstrapPayloadProjection, error) {
	var zero bootstrapPayloadProjection
	if err := io.check(context.Background()); err != nil {
		return zero, err
	}
	before, err := io.config.Domain.admission.EncryptionFence()
	if err != nil {
		return zero, errors.WithStack(err)
	}
	if group != io.config.DefaultRuntime.Spec.ID || generation != before {
		return zero, errors.WithStack(errBootstrapPayloadProjection)
	}
	cipher, id, active, err := io.installedPayload()
	if err != nil {
		return zero, err
	}
	after, err := io.config.Domain.admission.EncryptionFence()
	if err != nil {
		return zero, errors.WithStack(err)
	}
	if before != after {
		return zero, errors.Wrap(errBootstrapEncryptionProjectionPending, "payload projection changed")
	}
	return newBootstrapPayloadProjection(group, generation, cipher, id, active, grants...)
}

func (io *bootstrapEncryptionRuntimeIO) installedPayload() (*encryption.Cipher, uint32, bool, error) {
	r := io.config.Wiring.raftEnvelope
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := io.validatePayloadNonce(r); err != nil {
		return nil, 0, false, err
	}
	id, active := r.activeRaftDEKID, r.cutoverIndex.Load() != 0
	if active && (!r.groupWrapInstalled(io.config.Group) || !r.cipher.HasKey(id)) {
		return nil, 0, false, errors.Wrap(errBootstrapEncryptionProjectionPending, "actual raft payload wrapper missing")
	}
	if !active && io.config.Group.RaftPayloadWrap() != nil {
		return nil, 0, false, errors.Wrap(errBootstrapEncryptionProjectionPending, "unexpected raft wrapper before cutover")
	}
	return r.cipher, id, active, nil
}

// Called only while r.mu is held; no admission callback or I/O is performed.
func (io *bootstrapEncryptionRuntimeIO) validatePayloadNonce(r *raftEnvelopeRuntime) error {
	factory, ok := r.nonceFactory.(*encryption.DeterministicNonceFactory)
	if !ok || factory == nil {
		return errors.Wrap(errBootstrapEncryptionUnsupported, "unsupported installed raft nonce factory")
	}
	node, epoch, exhausted := factory.NonceIdentity()
	if exhausted {
		return errors.WithStack(encryption.ErrWriteCountExhausted)
	}
	if node != encryption.NodeID16(io.config.NodeID) || epoch != io.config.Wiring.raftEpoch || r.raftEpoch != epoch || r.fullNodeID != io.config.NodeID {
		return errors.Wrap(errBootstrapEncryptionUnsupported, "installed raft nonce identity mismatch")
	}
	return nil
}
