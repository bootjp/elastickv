package main

import (
	"strconv"

	"github.com/bootjp/elastickv/internal/encryption"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
)

var errBootstrapEncryptionUnsupported = errors.New("bootstrap encryption projection: unsupported configuration")

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
}

type bootstrapEncryptionPurposeProjection struct {
	dekID            uint32
	epoch            uint16
	registered       bool
	envelopeActive   bool
	cutoverIndex     uint64
	wrapperInstalled bool
}

// This is a local observation, NOT an admission handle, rotation receipt or F.
// No peer TLS, policy, leadership, recipe or responsibility proof is supplied.
type bootstrapEncryptionProjection struct {
	kind         bootstrapEncryptionObservationKind
	rotation     bootstrapRotationObservationKind
	fullNodeID   uint64
	appliedIndex uint64
	checkpoint   encryption.SnapshotCheckpoint
	storage      bootstrapEncryptionPurposeProjection
	raft         bootstrapEncryptionPurposeProjection
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
	if in.rotationRequested {
		return bootstrapEncryptionProjection{}, errors.Wrap(errBootstrapEncryptionUnsupported, "requested rotation requires receiver-owned reconciliation")
	}
	if !in.configured {
		return bootstrapEncryptionProjection{
			kind: bootstrapEncryptionNotRequested, rotation: bootstrapRotationNotRequested,
		}, nil
	}
	if err := in.validate(); err != nil {
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
		return bootstrapEncryptionProjection{}, errors.New("bootstrap encryption projection: local state changed during observation")
	}
	return second, nil
}

func (in bootstrapEncryptionProjectionInput) validate() error {
	if in.sidecarPath == "" || in.localStore == nil || in.fullNodeID == 0 {
		return errors.Wrap(errBootstrapEncryptionUnsupported, "sidecar, local store and full node ID are required")
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
		return 0, empty, errors.Wrap(errBootstrapEncryptionUnsupported, "missing durable applied index")
	}
	cp, present, err := source.LoadEncryptionSnapshotCheckpoint()
	if err != nil {
		return 0, empty, errors.Wrap(err, "bootstrap encryption projection: applied checkpoint")
	}
	if !present || cp.Index == 0 || cp.Index > applied {
		return 0, empty, errors.New("bootstrap encryption projection: checkpoint missing or beyond durable apply")
	}
	return applied, cp, nil
}

func sampleBootstrapEncryptionProjection(
	in bootstrapEncryptionProjectionInput,
	source bootstrapEncryptionProjectionStore,
	registry encryption.WriterRegistryStore,
) (bootstrapEncryptionProjection, error) {
	var result bootstrapEncryptionProjection
	applied, cp, err := readBootstrapEncryptionCheckpoint(source)
	if err != nil {
		return result, err
	}
	sc, err := encryption.ReadSidecar(in.sidecarPath)
	if err != nil {
		return result, errors.Wrap(err, "bootstrap encryption projection: sidecar")
	}
	want, err := encryption.SidecarCheckpoint(sc)
	if err != nil {
		return result, errors.Wrap(err, "bootstrap encryption projection: sidecar checkpoint")
	}
	if cp != want {
		return result, errors.New("bootstrap encryption projection: applied checkpoint disagrees with sidecar")
	}
	if sc.Active.Storage == 0 && sc.Active.Raft == 0 {
		return result, errors.Wrap(errBootstrapEncryptionUnsupported, "configured encryption has no active DEK")
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
	return bootstrapEncryptionProjection{
		kind: bootstrapEncryptionObserved, rotation: bootstrapRotationNotRequested,
		fullNodeID: in.fullNodeID, appliedIndex: applied, checkpoint: cp,
		storage: storagePurpose, raft: raftPurpose,
	}, nil
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
	registered, err := registrationCommittedAtEpoch(registry, p.dekID, fullNodeID, p.epoch)
	if err != nil {
		return bootstrapEncryptionPurposeProjection{}, errors.Wrap(err, "read local writer registration")
	}
	if !registered {
		return bootstrapEncryptionPurposeProjection{}, errors.New("local writer registration missing or epoch/identity mismatch")
	}
	p.registered = true
	if p.envelopeActive {
		if installed == nil {
			return bootstrapEncryptionPurposeProjection{}, errors.Wrap(errBootstrapEncryptionUnsupported, "missing installed-wrapper probe")
		}
		if !installed(p.dekID, p.epoch) {
			return bootstrapEncryptionPurposeProjection{}, errors.New("matching local wrapper is not installed")
		}
		p.wrapperInstalled = true
	}
	return p, nil
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
