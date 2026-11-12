package main

import (
	"crypto/sha256"
	"encoding/binary"
	"strconv"

	"github.com/bootjp/elastickv/internal/encryption"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
)

var errBootstrapEncryptionUnsupported = errors.New("bootstrap encryption projection: unsupported configuration")

// Pending means retry with authority closed. All other errors require the
// supervisor to fail closed rather than infer completion or parse error text.
var errBootstrapEncryptionProjectionPending = errors.New("bootstrap encryption projection: pending reconciliation")

type bootstrapEncryptionObservationKind uint8

const (
	bootstrapEncryptionUnobserved bootstrapEncryptionObservationKind = iota
	bootstrapEncryptionNotRequested
	bootstrapEncryptionObserved
)

type bootstrapRotationObservationKind uint8

const (
	bootstrapRotationUnobserved bootstrapRotationObservationKind = iota
	bootstrapRotationNotRequested
	bootstrapRotationRequestedUnproven
)

type bootstrapEncryptionProjectionInput struct {
	configured        bool
	rotationRequested bool
	sidecarPath       string
	localStore        store.MVCCStore
	fullNodeID        uint64
	storageEpoch      uint16
	raftEpoch         uint16
	// These read-only probes must inspect the installed local write path,
	// including its nonce epoch, not merely sidecar/cache intent.
	storageWrapperInstalled func(dekID uint32, epoch uint16) bool
	raftWrapperInstalled    func(dekID uint32, epoch uint16) bool
	// runtimeFence includes fatal/retired evidence; see bindInstalledObservation.
	runtimeFence func() (uint64, error)
	installed    func(raftKeyID uint32) (bootstrapEncryptionInstalledProjection, error)
}

type bootstrapEncryptionPurposeProjection struct {
	dekID            uint32
	epoch            uint16
	registered       bool
	envelopeActive   bool
	cutoverIndex     uint64
	wrapperInstalled bool
	keyDigest        [32]byte
	registry         encryption.RegistryValue
	wrapperIdentity  uint64
}

// This is a local observation, NOT an admission handle, rotation receipt or F.
// No peer TLS, policy, leadership, recipe or responsibility proof is supplied.
type bootstrapEncryptionProjection struct {
	kind              bootstrapEncryptionObservationKind
	rotation          bootstrapRotationObservationKind
	fullNodeID        uint64
	appliedIndex      uint64
	checkpoint        encryption.SnapshotCheckpoint
	storage           bootstrapEncryptionPurposeProjection
	raft              bootstrapEncryptionPurposeProjection
	generation        uint64
	installedEvidence bool
}

type bootstrapEncryptionProjectionStore interface {
	LastAppliedIndex() (uint64, bool, error)
	LoadEncryptionSnapshotCheckpoint() (encryption.SnapshotCheckpoint, bool, error)
}

// readBootstrapEncryptionProjection performs local reads only. Call without the
// admission mutex. Two matching samples reject observed drift, but are not an
// atomic snapshot: the caller must fence restore/apply and validate dependency
// generations before using observations. Success never enables the CLI profile.
func readBootstrapEncryptionProjection(in bootstrapEncryptionProjectionInput) (bootstrapEncryptionProjection, error) {
	if !in.configured {
		if in.rotationRequested {
			return bootstrapEncryptionProjection{}, errors.Wrap(errBootstrapEncryptionUnsupported, "rotation without encryption")
		}
		return bootstrapEncryptionProjection{
			kind: bootstrapEncryptionNotRequested, rotation: bootstrapRotationNotRequested,
		}, nil
	}
	if err := in.validate(); err != nil {
		return bootstrapEncryptionProjection{}, err
	}
	return readFencedBootstrapEncryptionProjection(in)
}

func readFencedBootstrapEncryptionProjection(in bootstrapEncryptionProjectionInput) (bootstrapEncryptionProjection, error) {
	generation, err := in.observeFence()
	if err != nil {
		return bootstrapEncryptionProjection{}, err
	}
	source, ok := in.localStore.(bootstrapEncryptionProjectionStore)
	if !ok {
		return bootstrapEncryptionProjection{}, errors.Wrap(errBootstrapEncryptionUnsupported, "store lacks durable applied/checkpoint reads")
	}
	registry, err := store.WriterRegistryFor(in.localStore)
	if err != nil {
		return bootstrapEncryptionProjection{}, errors.Wrap(err, "bootstrap encryption projection: registry store")
	}
	first, err := sampleBootstrapEncryptionProjection(in, source, registry)
	if err != nil {
		return bootstrapEncryptionProjection{}, err
	}
	second, err := sampleBootstrapEncryptionProjection(in, source, registry)
	if err != nil {
		return bootstrapEncryptionProjection{}, err
	}
	if first != second {
		return bootstrapEncryptionProjection{}, errors.Wrap(errBootstrapEncryptionProjectionPending, "local state changed during observation")
	}
	current, err := in.observeFence()
	if err != nil {
		return bootstrapEncryptionProjection{}, err
	}
	if current != generation {
		return bootstrapEncryptionProjection{}, errors.Wrap(errBootstrapEncryptionProjectionPending, "dependency generation changed")
	}
	second.generation = generation
	return second, nil
}

func (in bootstrapEncryptionProjectionInput) observeFence() (uint64, error) {
	generation, err := in.runtimeFence()
	if err != nil {
		return 0, errors.Wrap(err, "bootstrap encryption projection: fatal/retired runtime fence")
	}
	if generation == 0 {
		return 0, errors.Wrap(errBootstrapEncryptionUnsupported, "zero runtime generation")
	}
	return generation, nil
}

func (in bootstrapEncryptionProjectionInput) validate() error {
	if in.sidecarPath == "" || in.localStore == nil || in.fullNodeID == 0 {
		return errors.Wrap(errBootstrapEncryptionUnsupported, "sidecar, local store and full node ID are required")
	}
	if in.runtimeFence == nil {
		return errors.Wrap(errBootstrapEncryptionUnsupported, "missing runtime generation/fatal fence")
	}
	if in.rotationRequested && in.installed == nil {
		return errors.Wrap(errBootstrapEncryptionUnsupported, "requested rotation requires bound installed observations")
	}
	return nil
}

func readBootstrapEncryptionCheckpoint(source bootstrapEncryptionProjectionStore) (uint64, encryption.SnapshotCheckpoint, error) {
	var empty encryption.SnapshotCheckpoint
	applied, present, err := source.LastAppliedIndex()
	if err != nil {
		return 0, empty, errors.Wrap(err, "bootstrap encryption projection: durable applied index")
	}
	if !present || applied == 0 {
		return 0, empty, errors.Wrap(errBootstrapEncryptionProjectionPending, "missing durable applied index")
	}
	cp, present, err := source.LoadEncryptionSnapshotCheckpoint()
	if err != nil {
		return 0, empty, errors.Wrap(err, "bootstrap encryption projection: applied checkpoint")
	}
	if !present || cp.Index == 0 || cp.Index > applied {
		return 0, empty, errors.Wrap(errBootstrapEncryptionProjectionPending, "checkpoint missing or beyond durable apply")
	}
	return applied, cp, nil
}

func sampleBootstrapEncryptionProjection(
	in bootstrapEncryptionProjectionInput,
	source bootstrapEncryptionProjectionStore,
	registry encryption.WriterRegistryStore,
) (bootstrapEncryptionProjection, error) {
	var result bootstrapEncryptionProjection
	applied, cp, sc, err := readBootstrapAppliedSidecar(in.sidecarPath, source)
	if err != nil {
		return result, err
	}
	storagePurpose := bootstrapEncryptionPurposeProjection{
		dekID: sc.Active.Storage, epoch: in.storageEpoch,
		envelopeActive: sc.StorageEnvelopeActive, cutoverIndex: sc.StorageEnvelopeCutoverIndex,
	}
	storagePurpose, err = observeBootstrapEncryptionPurpose(sc, registry, in.fullNodeID, storagePurpose, in.storageWrapperInstalled)
	if err != nil {
		return result, errors.Wrap(err, "bootstrap encryption projection: storage")
	}
	raftPurpose := bootstrapEncryptionPurposeProjection{
		dekID: sc.Active.Raft, epoch: in.raftEpoch,
		envelopeActive: sc.RaftEnvelopeCutoverIndex != 0, cutoverIndex: sc.RaftEnvelopeCutoverIndex,
	}
	raftPurpose, err = observeBootstrapEncryptionPurpose(sc, registry, in.fullNodeID, raftPurpose, in.raftWrapperInstalled)
	if err != nil {
		return result, errors.Wrap(err, "bootstrap encryption projection: raft")
	}
	result = bootstrapEncryptionProjection{
		kind: bootstrapEncryptionObserved, rotation: bootstrapRotationNotRequested,
		fullNodeID: in.fullNodeID, appliedIndex: applied, checkpoint: cp,
		storage: storagePurpose, raft: raftPurpose,
	}
	if err := result.observeInstalled(in); err != nil {
		return bootstrapEncryptionProjection{}, err
	}
	return result, nil
}

func (result *bootstrapEncryptionProjection) observeInstalled(in bootstrapEncryptionProjectionInput) error {
	if in.rotationRequested {
		result.rotation = bootstrapRotationRequestedUnproven
	}
	if in.installed != nil {
		if err := result.validateInstalled(in); err != nil {
			return err
		}
		result.installedEvidence = true
	} else if (result.storage.envelopeActive && !result.storage.wrapperInstalled) || (result.raft.envelopeActive && !result.raft.wrapperInstalled) {
		return errors.Wrap(errBootstrapEncryptionUnsupported, "missing installed-wrapper probe")
	}
	return nil
}

func readBootstrapAppliedSidecar(path string, source bootstrapEncryptionProjectionStore) (uint64, encryption.SnapshotCheckpoint, *encryption.Sidecar, error) {
	applied, cp, err := readBootstrapEncryptionCheckpoint(source)
	if err != nil {
		return 0, cp, nil, err
	}
	sc, err := encryption.ReadSidecar(path)
	if err != nil {
		return 0, cp, nil, errors.Wrap(err, "bootstrap encryption projection: sidecar")
	}
	want, err := encryption.SidecarCheckpoint(sc)
	if err != nil {
		return 0, cp, nil, errors.Wrap(err, "bootstrap encryption projection: sidecar checkpoint")
	}
	if cp != want {
		return 0, cp, nil, errors.Wrap(errBootstrapEncryptionProjectionPending, "applied checkpoint disagrees with sidecar")
	}
	if sc.Active.Storage == 0 && sc.Active.Raft == 0 {
		return 0, cp, nil, errors.Wrap(errBootstrapEncryptionUnsupported, "configured encryption has no active DEK")
	}
	if err := validateBootstrapEncryptionKeyPurposes(sc); err != nil {
		return 0, cp, nil, err
	}
	return applied, cp, sc, nil
}

func validateBootstrapEncryptionKeyPurposes(sc *encryption.Sidecar) error {
	for _, purpose := range []struct {
		id   uint32
		name string
	}{
		{sc.Active.Storage, encryption.SidecarPurposeStorage}, {sc.Active.Raft, encryption.SidecarPurposeRaft},
	} {
		if purpose.id != 0 && sc.Keys[strconv.FormatUint(uint64(purpose.id), 10)].Purpose != purpose.name {
			return errors.New("bootstrap encryption projection: active key purpose mismatch")
		}
	}
	return nil
}

// digest is domain-separated and contains only immutable values, never local
// function pointers or key plaintext. Applied/checkpoint indices and observation
// generations fence sampling, NOT authority: ordinary data apply must not revoke
// a valid permission. It is NOT a rotation completion receipt.
func (p bootstrapEncryptionProjection) digest() [32]byte {
	b := []byte("elastickv/bootstrap-encryption-observation/v1\x00")
	for _, v := range []uint64{uint64(p.kind), uint64(p.rotation), p.fullNodeID} {
		b = binary.BigEndian.AppendUint64(b, v)
	}
	b = append(b, bootstrapEncryptionBool(p.installedEvidence))
	for _, purpose := range []bootstrapEncryptionPurposeProjection{p.storage, p.raft} {
		for _, v := range []uint64{uint64(purpose.dekID), uint64(purpose.epoch), purpose.cutoverIndex, purpose.registry.FullNodeID, uint64(purpose.registry.FirstSeenLocalEpoch), uint64(purpose.registry.LastSeenLocalEpoch), purpose.wrapperIdentity} {
			b = binary.BigEndian.AppendUint64(b, v)
		}
		b = append(b, purpose.keyDigest[:]...)
		b = append(b, bootstrapEncryptionBool(purpose.registered), bootstrapEncryptionBool(purpose.envelopeActive), bootstrapEncryptionBool(purpose.wrapperInstalled))
	}
	return sha256.Sum256(b)
}

func bootstrapEncryptionBool(v bool) byte {
	if v {
		return 1
	}
	return 0
}

func observeBootstrapEncryptionPurpose(
	sc *encryption.Sidecar,
	registry encryption.WriterRegistryStore,
	fullNodeID uint64,
	p bootstrapEncryptionPurposeProjection,
	installed func(uint32, uint16) bool,
) (bootstrapEncryptionPurposeProjection, error) {
	if err := validateBootstrapEncryptionPurpose(sc, p); err != nil {
		return bootstrapEncryptionPurposeProjection{}, err
	}
	if p.dekID == 0 {
		return p, nil
	}
	registration, err := readBootstrapWriterRegistration(registry, p.dekID, fullNodeID, p.epoch)
	if err != nil {
		return bootstrapEncryptionPurposeProjection{}, err
	}
	p.registered = true
	p.registry = registration
	p.keyDigest = sha256.Sum256(sc.Keys[strconv.FormatUint(uint64(p.dekID), 10)].Wrapped)
	if p.envelopeActive {
		if installed == nil {
			return p, nil
		}
		if !installed(p.dekID, p.epoch) {
			return bootstrapEncryptionPurposeProjection{}, errors.Wrap(errBootstrapEncryptionProjectionPending, "matching local wrapper is not installed")
		}
		p.wrapperInstalled = true
	}
	return p, nil
}

func readBootstrapWriterRegistration(registry encryption.WriterRegistryStore, id uint32, fullNodeID uint64, epoch uint16) (encryption.RegistryValue, error) {
	var empty encryption.RegistryValue
	raw, present, err := registry.GetRegistryRow(encryption.RegistryKey(id, encryption.NodeID16(fullNodeID)))
	if err != nil {
		return empty, errors.Wrap(err, "read registry evidence")
	}
	if !present {
		return empty, errors.Wrap(errBootstrapEncryptionProjectionPending, "writer registry evidence missing")
	}
	registration, err := encryption.DecodeRegistryValue(raw)
	if err != nil {
		return empty, errors.Wrap(err, "decode writer registry evidence")
	}
	if registration.FullNodeID != fullNodeID || registration.FirstSeenLocalEpoch > registration.LastSeenLocalEpoch {
		return empty, errors.New("invalid writer registry evidence")
	}
	if registration.LastSeenLocalEpoch > epoch {
		return empty, errors.Wrap(encryption.ErrLocalEpochRollback, "writer registry exceeds installed local epoch")
	}
	if registration.LastSeenLocalEpoch < epoch {
		return empty, errors.Wrap(errBootstrapEncryptionProjectionPending, "local writer epoch not yet registered")
	}
	return registration, nil
}

func validateBootstrapEncryptionPurpose(sc *encryption.Sidecar, p bootstrapEncryptionPurposeProjection) error {
	if p.envelopeActive != (p.cutoverIndex != 0) || p.cutoverIndex > sc.RaftAppliedIndex {
		return errors.New("inconsistent envelope cutover")
	}
	if p.dekID == 0 {
		if p.envelopeActive {
			return errors.New("active envelope has no DEK")
		}
		return nil
	}
	key := sc.Keys[strconv.FormatUint(uint64(p.dekID), 10)]
	if len(key.Wrapped) == 0 || key.LocalEpoch != p.epoch {
		return errors.New("active DEK material missing or local epoch mismatch")
	}
	return nil
}
