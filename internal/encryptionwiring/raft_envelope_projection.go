package encryptionwiring

import (
	"crypto/sha256"
	"encoding/binary"
	"strconv"

	"github.com/bootjp/elastickv/internal/encryption"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
)

var ErrBootstrapEncryptionUnsupported = errors.New("bootstrap encryption projection: unsupported configuration")

// Pending means retry with authority closed. All other errors require the
// supervisor to fail closed rather than infer completion or parse error text.
var ErrBootstrapEncryptionProjectionPending = errors.New("bootstrap encryption projection: pending reconciliation")

type bootstrapEncryptionObservationKind uint8

const (
	bootstrapEncryptionUnobserved bootstrapEncryptionObservationKind = iota
	bootstrapEncryptionNotRequested
	BootstrapEncryptionObserved
)

type bootstrapRotationObservationKind uint8

const (
	bootstrapRotationUnobserved bootstrapRotationObservationKind = iota
	bootstrapRotationNotRequested
	BootstrapRotationRequestedUnproven
)

type BootstrapEncryptionProjectionInput struct {
	Configured        bool
	RotationRequested bool
	SidecarPath       string
	LocalStore        store.MVCCStore
	FullNodeID        uint64
	StorageEpoch      uint16
	RaftEpoch         uint16
	// These read-only probes must inspect the installed local write path,
	// including its nonce epoch, not merely sidecar/cache intent.
	StorageWrapperInstalled func(dekID uint32, epoch uint16) bool
	RaftWrapperInstalled    func(dekID uint32, epoch uint16) bool
	// RuntimeFence includes fatal/retired evidence; see bindInstalledObservation.
	RuntimeFence func() (uint64, error)
	installed    func(raftKeyID uint32) (bootstrapEncryptionInstalledProjection, error)
}

type BootstrapEncryptionPurposeProjection struct {
	DekID            uint32
	Epoch            uint16
	Registered       bool
	EnvelopeActive   bool
	CutoverIndex     uint64
	WrapperInstalled bool
	KeyDigest        [32]byte
	Registry         encryption.RegistryValue
	wrapperIdentity  uint64
}

// This is a local observation, NOT an admission handle, rotation receipt or F.
// No peer TLS, policy, leadership, recipe or responsibility proof is supplied.
type BootstrapEncryptionProjection struct {
	Kind              bootstrapEncryptionObservationKind
	Rotation          bootstrapRotationObservationKind
	FullNodeID        uint64
	AppliedIndex      uint64
	Checkpoint        encryption.SnapshotCheckpoint
	Storage           BootstrapEncryptionPurposeProjection
	Raft              BootstrapEncryptionPurposeProjection
	Generation        uint64
	InstalledEvidence bool
}

type BootstrapEncryptionProjectionStore interface {
	LastAppliedIndex() (uint64, bool, error)
	LoadEncryptionSnapshotCheckpoint() (encryption.SnapshotCheckpoint, bool, error)
}

// ReadBootstrapEncryptionProjection performs local reads only. Call without the
// admission mutex. Two matching samples reject observed drift, but are not an
// atomic snapshot: the caller must fence restore/apply and validate dependency
// generations before using observations. Success never enables the CLI profile.
func ReadBootstrapEncryptionProjection(in BootstrapEncryptionProjectionInput) (BootstrapEncryptionProjection, error) {
	if !in.Configured {
		if in.RotationRequested {
			return BootstrapEncryptionProjection{}, errors.Wrap(ErrBootstrapEncryptionUnsupported, "rotation without encryption")
		}
		return BootstrapEncryptionProjection{
			Kind: bootstrapEncryptionNotRequested, Rotation: bootstrapRotationNotRequested,
		}, nil
	}
	if err := in.validate(); err != nil {
		return BootstrapEncryptionProjection{}, err
	}
	return readFencedBootstrapEncryptionProjection(in)
}

func readFencedBootstrapEncryptionProjection(in BootstrapEncryptionProjectionInput) (BootstrapEncryptionProjection, error) {
	generation, err := in.observeFence()
	if err != nil {
		return BootstrapEncryptionProjection{}, err
	}
	source, ok := in.LocalStore.(BootstrapEncryptionProjectionStore)
	if !ok {
		return BootstrapEncryptionProjection{}, errors.Wrap(ErrBootstrapEncryptionUnsupported, "store lacks durable applied/checkpoint reads")
	}
	registry, err := store.WriterRegistryFor(in.LocalStore)
	if err != nil {
		return BootstrapEncryptionProjection{}, errors.Wrap(err, "bootstrap encryption projection: registry store")
	}
	first, err := sampleBootstrapEncryptionProjection(in, source, registry)
	if err != nil {
		return BootstrapEncryptionProjection{}, err
	}
	second, err := sampleBootstrapEncryptionProjection(in, source, registry)
	if err != nil {
		return BootstrapEncryptionProjection{}, err
	}
	if first != second {
		return BootstrapEncryptionProjection{}, errors.Wrap(ErrBootstrapEncryptionProjectionPending, "local state changed during observation")
	}
	current, err := in.observeFence()
	if err != nil {
		return BootstrapEncryptionProjection{}, err
	}
	if current != generation {
		return BootstrapEncryptionProjection{}, errors.Wrap(ErrBootstrapEncryptionProjectionPending, "dependency generation changed")
	}
	second.Generation = generation
	return second, nil
}

func (in BootstrapEncryptionProjectionInput) observeFence() (uint64, error) {
	generation, err := in.RuntimeFence()
	if err != nil {
		return 0, errors.Wrap(err, "bootstrap encryption projection: fatal/retired runtime fence")
	}
	if generation == 0 {
		return 0, errors.Wrap(ErrBootstrapEncryptionUnsupported, "zero runtime generation")
	}
	return generation, nil
}

func (in BootstrapEncryptionProjectionInput) validate() error {
	if in.SidecarPath == "" || in.LocalStore == nil || in.FullNodeID == 0 {
		return errors.Wrap(ErrBootstrapEncryptionUnsupported, "sidecar, local store and full node ID are required")
	}
	if in.RuntimeFence == nil {
		return errors.Wrap(ErrBootstrapEncryptionUnsupported, "missing runtime generation/fatal fence")
	}
	if in.RotationRequested && in.installed == nil {
		return errors.Wrap(ErrBootstrapEncryptionUnsupported, "requested rotation requires bound installed observations")
	}
	return nil
}

func ReadBootstrapEncryptionCheckpoint(source BootstrapEncryptionProjectionStore) (uint64, encryption.SnapshotCheckpoint, error) {
	var empty encryption.SnapshotCheckpoint
	applied, present, err := source.LastAppliedIndex()
	if err != nil {
		return 0, empty, errors.Wrap(err, "bootstrap encryption projection: durable applied index")
	}
	if !present || applied == 0 {
		return 0, empty, errors.Wrap(ErrBootstrapEncryptionProjectionPending, "missing durable applied index")
	}
	cp, present, err := source.LoadEncryptionSnapshotCheckpoint()
	if err != nil {
		return 0, empty, errors.Wrap(err, "bootstrap encryption projection: applied checkpoint")
	}
	if !present || cp.Index == 0 || cp.Index > applied {
		return 0, empty, errors.Wrap(ErrBootstrapEncryptionProjectionPending, "checkpoint missing or beyond durable apply")
	}
	return applied, cp, nil
}

func sampleBootstrapEncryptionProjection(
	in BootstrapEncryptionProjectionInput,
	source BootstrapEncryptionProjectionStore,
	registry encryption.WriterRegistryStore,
) (BootstrapEncryptionProjection, error) {
	var result BootstrapEncryptionProjection
	applied, cp, sc, err := readBootstrapAppliedSidecar(in.SidecarPath, source)
	if err != nil {
		return result, err
	}
	storagePurpose := BootstrapEncryptionPurposeProjection{
		DekID: sc.Active.Storage, Epoch: in.StorageEpoch,
		EnvelopeActive: sc.StorageEnvelopeActive, CutoverIndex: sc.StorageEnvelopeCutoverIndex,
	}
	storagePurpose, err = observeBootstrapEncryptionPurpose(sc, registry, in.FullNodeID, storagePurpose, in.StorageWrapperInstalled)
	if err != nil {
		return result, errors.Wrap(err, "bootstrap encryption projection: storage")
	}
	raftPurpose := BootstrapEncryptionPurposeProjection{
		DekID: sc.Active.Raft, Epoch: in.RaftEpoch,
		EnvelopeActive: sc.RaftEnvelopeCutoverIndex != 0, CutoverIndex: sc.RaftEnvelopeCutoverIndex,
	}
	raftPurpose, err = observeBootstrapEncryptionPurpose(sc, registry, in.FullNodeID, raftPurpose, in.RaftWrapperInstalled)
	if err != nil {
		return result, errors.Wrap(err, "bootstrap encryption projection: raft")
	}
	result = BootstrapEncryptionProjection{
		Kind: BootstrapEncryptionObserved, Rotation: bootstrapRotationNotRequested,
		FullNodeID: in.FullNodeID, AppliedIndex: applied, Checkpoint: cp,
		Storage: storagePurpose, Raft: raftPurpose,
	}
	if err := result.ObserveInstalled(in); err != nil {
		return BootstrapEncryptionProjection{}, err
	}
	return result, nil
}

func (result *BootstrapEncryptionProjection) ObserveInstalled(in BootstrapEncryptionProjectionInput) error {
	if in.RotationRequested {
		result.Rotation = BootstrapRotationRequestedUnproven
	}
	if in.installed != nil {
		if err := result.validateInstalled(in); err != nil {
			return err
		}
		result.InstalledEvidence = true
	} else if (result.Storage.EnvelopeActive && !result.Storage.WrapperInstalled) || (result.Raft.EnvelopeActive && !result.Raft.WrapperInstalled) {
		return errors.Wrap(ErrBootstrapEncryptionUnsupported, "missing installed-wrapper probe")
	}
	return nil
}

func readBootstrapAppliedSidecar(path string, source BootstrapEncryptionProjectionStore) (uint64, encryption.SnapshotCheckpoint, *encryption.Sidecar, error) {
	applied, cp, err := ReadBootstrapEncryptionCheckpoint(source)
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
		return 0, cp, nil, errors.Wrap(ErrBootstrapEncryptionProjectionPending, "applied checkpoint disagrees with sidecar")
	}
	if sc.Active.Storage == 0 && sc.Active.Raft == 0 {
		return 0, cp, nil, errors.Wrap(ErrBootstrapEncryptionUnsupported, "configured encryption has no active DEK")
	}
	if err := ValidateBootstrapEncryptionKeyPurposes(sc); err != nil {
		return 0, cp, nil, err
	}
	return applied, cp, sc, nil
}

func ValidateBootstrapEncryptionKeyPurposes(sc *encryption.Sidecar) error {
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

// Digest is domain-separated and contains only immutable values, never local
// function pointers or key plaintext. Applied/checkpoint indices and observation
// generations fence sampling, NOT authority: ordinary data apply must not revoke
// a valid permission. It is NOT a rotation completion receipt.
func (p BootstrapEncryptionProjection) Digest() [32]byte {
	b := []byte("elastickv/bootstrap-encryption-observation/v1\x00")
	for _, v := range []uint64{uint64(p.Kind), uint64(p.Rotation), p.FullNodeID} {
		b = binary.BigEndian.AppendUint64(b, v)
	}
	b = append(b, bootstrapEncryptionBool(p.InstalledEvidence))
	for _, purpose := range []BootstrapEncryptionPurposeProjection{p.Storage, p.Raft} {
		for _, v := range []uint64{uint64(purpose.DekID), uint64(purpose.Epoch), purpose.CutoverIndex, purpose.Registry.FullNodeID, uint64(purpose.Registry.FirstSeenLocalEpoch), uint64(purpose.Registry.LastSeenLocalEpoch), purpose.wrapperIdentity} {
			b = binary.BigEndian.AppendUint64(b, v)
		}
		b = append(b, purpose.KeyDigest[:]...)
		b = append(b, bootstrapEncryptionBool(purpose.Registered), bootstrapEncryptionBool(purpose.EnvelopeActive), bootstrapEncryptionBool(purpose.WrapperInstalled))
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
	p BootstrapEncryptionPurposeProjection,
	installed func(uint32, uint16) bool,
) (BootstrapEncryptionPurposeProjection, error) {
	if err := validateBootstrapEncryptionPurpose(sc, p); err != nil {
		return BootstrapEncryptionPurposeProjection{}, err
	}
	if p.DekID == 0 {
		return p, nil
	}
	registration, err := ReadBootstrapWriterRegistration(registry, p.DekID, fullNodeID, p.Epoch)
	if err != nil {
		return BootstrapEncryptionPurposeProjection{}, err
	}
	p.Registered = true
	p.Registry = registration
	p.KeyDigest = sha256.Sum256(sc.Keys[strconv.FormatUint(uint64(p.DekID), 10)].Wrapped)
	if p.EnvelopeActive {
		if installed == nil {
			return p, nil
		}
		if !installed(p.DekID, p.Epoch) {
			return BootstrapEncryptionPurposeProjection{}, errors.Wrap(ErrBootstrapEncryptionProjectionPending, "matching local wrapper is not installed")
		}
		p.WrapperInstalled = true
	}
	return p, nil
}

func ReadBootstrapWriterRegistration(registry encryption.WriterRegistryStore, id uint32, fullNodeID uint64, epoch uint16) (encryption.RegistryValue, error) {
	var empty encryption.RegistryValue
	raw, present, err := registry.GetRegistryRow(encryption.RegistryKey(id, encryption.NodeID16(fullNodeID)))
	if err != nil {
		return empty, errors.Wrap(err, "read registry evidence")
	}
	if !present {
		return empty, errors.Wrap(ErrBootstrapEncryptionProjectionPending, "writer registry evidence missing")
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
		return empty, errors.Wrap(ErrBootstrapEncryptionProjectionPending, "local writer epoch not yet registered")
	}
	return registration, nil
}

func validateBootstrapEncryptionPurpose(sc *encryption.Sidecar, p BootstrapEncryptionPurposeProjection) error {
	if p.EnvelopeActive != (p.CutoverIndex != 0) || p.CutoverIndex > sc.RaftAppliedIndex {
		return errors.New("inconsistent envelope cutover")
	}
	if p.DekID == 0 {
		if p.EnvelopeActive {
			return errors.New("active envelope has no DEK")
		}
		return nil
	}
	key := sc.Keys[strconv.FormatUint(uint64(p.DekID), 10)]
	if len(key.Wrapped) == 0 || key.LocalEpoch != p.Epoch {
		return errors.New("active DEK material missing or local epoch mismatch")
	}
	return nil
}
