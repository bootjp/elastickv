package kv

import (
	"bytes"
	"context"
	"encoding/binary"
	"slices"

	"github.com/bootjp/elastickv/internal/raftbootstrap"
	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
)

var ErrBootstrapActivationPreparation = errors.New("bootstrap activation preparation unavailable or inconsistent")
var ErrBootstrapActivationNativeProof = errors.New("bootstrap activation native proof unavailable or inconsistent")

const bootstrapPreparationMagic = "BAP1"

// PreparedBootstrapActivation is independently established by the local offline
// preparation owner, never from an incoming activation proposal. Probe includes
// the local member's signed native/data cut; ManifestDigest pins the full agreed
// all-member certificate. Staging this record is a trusted migration operation.
type PreparedBootstrapActivation struct {
	Probe          raftbootstrap.BaselineProbe
	ManifestDigest raftbootstrap.Digest
}

// BootstrapActivationNativeProof must come from the current native owner, not
// from the proposal, preparation record, catalog watcher or policy cache.
type BootstrapActivationNativeProof struct {
	Identity      raftbootstrap.Identity
	Configuration raftbootstrap.Configuration
}

// WithBootstrapActivationNativeProof installs a tests/custom-owner fallback.
// An engine-delivered native image always takes precedence, including failures.
// The getter must work during Open replay, before Open returns an engine, and
// must not perform I/O or wait for the serial apply goroutine.
func WithBootstrapActivationNativeProof(proof func(context.Context) (BootstrapActivationNativeProof, error)) FSMOption {
	return func(f *kvFSM) { f.bootstrapActivationNativeProof = proof }
}

// SetApplyNativeConfiguration receives only the native engine's pre-apply image.
// Copy every slice so neither native mutations nor a retained caller buffer can
// change proof while the indexed transaction is running.
func (f *kvFSM) SetApplyNativeConfiguration(configuration raftengine.NativeConfiguration) {
	configuration.Voters = slices.Clone(configuration.Voters)
	configuration.VotersOutgoing = slices.Clone(configuration.VotersOutgoing)
	configuration.Learners = slices.Clone(configuration.Learners)
	configuration.LearnersNext = slices.Clone(configuration.LearnersNext)
	configuration.Members = slices.Clone(configuration.Members)
	f.pendingNativeConfiguration = &configuration
}

func (f *kvFSM) ClearApplyNativeConfiguration() { f.pendingNativeConfiguration = nil }

// BootstrapActivationProofFromNative combines actual native roles/addresses
// with independently prepared SPKI pins. No field is sourced from the arriving
// proposal. The native image must remain serialized with this apply attempt.
func BootstrapActivationProofFromNative(identity raftbootstrap.Identity, native raftengine.NativeConfiguration, prepared PreparedBootstrapActivation) (BootstrapActivationNativeProof, error) {
	var proof BootstrapActivationNativeProof
	if prepared.Probe.Identity != identity || len(native.Members) != len(prepared.Probe.Configuration.Members) {
		return proof, errors.WithStack(ErrBootstrapActivationNativeProof)
	}
	pins := make(map[uint64]raftbootstrap.Member, len(prepared.Probe.Configuration.Members))
	for _, member := range prepared.Probe.Configuration.Members {
		pins[member.ID] = member
	}
	configuration := raftbootstrap.Configuration{Index: native.Index, State: raftbootstrap.ConfState{
		Voters: slices.Clone(native.Voters), VotersOutgoing: slices.Clone(native.VotersOutgoing),
		Learners: slices.Clone(native.Learners), LearnersNext: slices.Clone(native.LearnersNext), AutoLeave: native.AutoLeave}}
	for _, member := range native.Members {
		pin, ok := pins[member.ID]
		if !ok || pin.Address != member.Address {
			return proof, errors.WithStack(ErrBootstrapActivationNativeProof)
		}
		delete(pins, member.ID)
		configuration.Members = append(configuration.Members, raftbootstrap.Member{ID: member.ID, Address: member.Address, PublicKeyDigest: pin.PublicKeyDigest})
	}
	proof = BootstrapActivationNativeProof{Identity: identity, Configuration: configuration}
	return proof, VerifyBootstrapActivationNativeProof(identity, proof, prepared.Probe.Configuration)
}

func (f *kvFSM) bootstrapActivationProofForApply(ctx context.Context, metadata store.RaftControlMetadataStore) (BootstrapActivationNativeProof, error) {
	if f.pendingNativeConfiguration != nil {
		prepared, err := ReadPreparedBootstrapActivation(ctx, metadata, f.NativeGroupIdentity())
		if err != nil {
			return BootstrapActivationNativeProof{}, err
		}
		return BootstrapActivationProofFromNative(f.NativeGroupIdentity(), *f.pendingNativeConfiguration, prepared)
	}
	if f.bootstrapActivationNativeProof == nil {
		return BootstrapActivationNativeProof{}, errors.WithStack(ErrBootstrapActivationNativeProof)
	}
	proof, err := f.bootstrapActivationNativeProof(ctx)
	if err != nil {
		return proof, errors.Join(ErrBootstrapActivationNativeProof, err)
	}
	return proof, nil
}

func bootstrapActivationMetadataKey(prefix string, identity raftbootstrap.Identity) ([]byte, error) {
	if identity.Cluster == (raftbootstrap.UUID{}) {
		return nil, errors.WithStack(raftbootstrap.ErrInvalid)
	}
	key := append([]byte(prefix), identity.Cluster[:]...)
	return binary.BigEndian.AppendUint64(key, identity.Group), nil
}

// StagePreparedBootstrapActivation syncs local preparation without advancing the
// Raft applied index or data watermark. The caller must hold the offline/native
// exclusive preparation fence and independently verify the current local cut.
// Exact repeats are idempotent; a conflicting prior preparation is not replaced.
func StagePreparedBootstrapActivation(ctx context.Context, metadata store.RaftControlMetadataStore, identity raftbootstrap.Identity, prepared PreparedBootstrapActivation) error {
	key, raw, err := PreparedBootstrapActivationMetadata(identity, prepared)
	if err != nil {
		return err
	}
	if metadata == nil {
		return errors.WithStack(ErrBootstrapActivationPreparation)
	}
	err = metadata.ApplyRaftControlMetadata(ctx, func(tx context.Context) error {
		return stageBootstrapPreparation(tx, metadata, identity, key, raw)
	})
	return errors.Wrap(err, "stage trusted bootstrap activation preparation")
}

// StagePreparedBootstrapActivationInEntry stages into an already-open metadata
// or indexed entry transaction. It never starts a nested transaction.
func StagePreparedBootstrapActivationInEntry(ctx context.Context, metadata store.RaftControlMetadataStore, identity raftbootstrap.Identity, prepared PreparedBootstrapActivation) error {
	if metadata == nil {
		return errors.WithStack(ErrBootstrapActivationPreparation)
	}
	key, raw, err := PreparedBootstrapActivationMetadata(identity, prepared)
	if err != nil {
		return err
	}
	return stageBootstrapPreparation(ctx, metadata, identity, key, raw)
}

// PreparedBootstrapActivationMetadata returns the exact metadata suffix key and
// canonical value staged by StagePreparedBootstrapActivation. Returned slices
// are caller-owned. This pure encoder performs the same validation but grants
// no mutation authority and does not establish trusted local preparation.
func PreparedBootstrapActivationMetadata(identity raftbootstrap.Identity, prepared PreparedBootstrapActivation) (key, value []byte, err error) {
	value, err = encodePreparedBootstrapActivation(identity, prepared)
	if err != nil {
		return nil, nil, err
	}
	key, err = bootstrapActivationMetadataKey("J/activation-prepared/", identity)
	if err != nil {
		return nil, nil, err
	}
	return key, value, nil
}

// DeletePreparedBootstrapActivation is a conditional stage, not a transaction.
// The caller must supply an indexed/metadata callback context and explicit
// expected digest; it is never called as an automatic response to a proposal.
func DeletePreparedBootstrapActivation(ctx context.Context, metadata store.RaftControlMetadataStore, identity raftbootstrap.Identity, expected raftbootstrap.Digest) error {
	prepared, err := ReadPreparedBootstrapActivation(ctx, metadata, identity)
	if err != nil {
		return err
	}
	if expected == (raftbootstrap.Digest{}) || prepared.ManifestDigest != expected {
		return errors.WithStack(ErrBootstrapActivationPreparation)
	}
	key, err := bootstrapActivationMetadataKey("J/activation-prepared/", identity)
	if err != nil {
		return err
	}
	return errors.WithStack(metadata.DeleteRaftControlMetadata(ctx, key))
}

func stageBootstrapPreparation(ctx context.Context, metadata store.RaftControlMetadataStore, identity raftbootstrap.Identity, key, raw []byte) error {
	policyKey, err := BootstrapPolicyMetadataKey(identity)
	if err != nil {
		return err
	}
	if _, err := metadata.ReadRaftControlMetadata(ctx, policyKey); !errors.Is(err, store.ErrKeyNotFound) {
		return errors.Join(ErrBootstrapActivationPreparation, err)
	}
	previous, err := metadata.ReadRaftControlMetadata(ctx, key)
	if err == nil {
		if !bytes.Equal(previous, raw) {
			return errors.WithStack(ErrBootstrapActivationPreparation)
		}
		return nil
	}
	if !errors.Is(err, store.ErrKeyNotFound) {
		return errors.WithStack(err)
	}
	return errors.WithStack(metadata.StageRaftControlMetadata(ctx, key, raw))
}

func encodePreparedBootstrapActivation(identity raftbootstrap.Identity, prepared PreparedBootstrapActivation) ([]byte, error) {
	if prepared.Probe.Identity != identity || prepared.ManifestDigest == (raftbootstrap.Digest{}) {
		return nil, errors.WithStack(ErrBootstrapActivationPreparation)
	}
	probe, err := raftbootstrap.MarshalBaselineProbe(prepared.Probe)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	if len(probe) > raftbootstrap.MaxFileSize-len(bootstrapPreparationMagic)-len(prepared.ManifestDigest) {
		return nil, errors.WithStack(raftbootstrap.ErrInvalid)
	}
	raw := append([]byte(bootstrapPreparationMagic), prepared.ManifestDigest[:]...)
	return append(raw, probe...), nil
}

func ReadPreparedBootstrapActivation(ctx context.Context, metadata store.RaftControlMetadataStore, identity raftbootstrap.Identity) (PreparedBootstrapActivation, error) {
	var prepared PreparedBootstrapActivation
	if metadata == nil {
		return prepared, errors.WithStack(ErrBootstrapActivationPreparation)
	}
	key, err := bootstrapActivationMetadataKey("J/activation-prepared/", identity)
	if err != nil {
		return prepared, err
	}
	raw, err := metadata.ReadRaftControlMetadata(ctx, key)
	if err != nil {
		return prepared, errors.Join(ErrBootstrapActivationPreparation, err)
	}
	const header = len(bootstrapPreparationMagic) + len(raftbootstrap.Digest{})
	if len(raw) <= header || len(raw) > raftbootstrap.MaxFileSize || !bytes.Equal(raw[:len(bootstrapPreparationMagic)], []byte(bootstrapPreparationMagic)) {
		return prepared, errors.WithStack(ErrBootstrapActivationPreparation)
	}
	copy(prepared.ManifestDigest[:], raw[len(bootstrapPreparationMagic):header])
	prepared.Probe, err = raftbootstrap.ParseBaselineProbe(raw[header:])
	if err != nil {
		return prepared, errors.WithStack(err)
	}
	canonical, err := encodePreparedBootstrapActivation(identity, prepared)
	if err != nil || !bytes.Equal(raw, canonical) {
		return prepared, errors.Join(ErrBootstrapActivationPreparation, err)
	}
	return prepared, nil
}

func (f *kvFSM) readBootstrapReceiptVariant(ctx context.Context, identity raftbootstrap.EntryIdentity, payload []byte) (any, bool, error) {
	metadata, ok := f.store.(store.RaftControlMetadataStore)
	if !ok {
		return nil, false, errors.WithStack(ErrGuardedBootstrapApply)
	}
	key, err := raftbootstrap.ReceiptKey(identity)
	if err != nil {
		return nil, false, errors.WithStack(err)
	}
	raw, err := metadata.ReadRaftControlMetadata(ctx, key)
	if errors.Is(err, store.ErrKeyNotFound) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, errors.WithStack(err)
	}
	return matchBootstrapReceiptVariant(raw, identity, payload)
}

func matchBootstrapReceiptVariant(raw []byte, identity raftbootstrap.EntryIdentity, payload []byte) (any, bool, error) {
	guarded, guardErr := raftbootstrap.ParseGuardedReceipt(raw)
	if guardErr == nil {
		if guarded.Entry != identity || activationPayloadKind(payload) {
			return nil, false, errors.WithStack(ErrGuardedBootstrapReceiptConflict)
		}
		return guarded, true, nil
	}
	activation, activationErr := raftbootstrap.ParseActivationReceipt(raw)
	if activationErr == nil {
		if activation.Entry != identity || conflictingActivationPayloadKind(payload) {
			return nil, false, errors.WithStack(ErrGuardedBootstrapReceiptConflict)
		}
		return activation, true, nil
	}
	return nil, false, errors.Join(ErrGuardedBootstrapReceiptConflict, guardErr, activationErr)
}

func activationPayloadKind(payload []byte) bool {
	return len(payload) >= 5 && payload[4] == byte(raftbootstrap.ControlActivate)
}

func conflictingActivationPayloadKind(payload []byte) bool {
	return len(payload) >= 5 && !activationPayloadKind(payload)
}

func (f *kvFSM) applyBootstrapActivation(ctx context.Context, identity raftbootstrap.EntryIdentity, command raftbootstrap.ActivationCommand, payload []byte) (any, error) {
	if command.Witness.Cluster != identity.Cluster || command.Witness.Group != identity.Group {
		return nil, errors.WithStack(ErrGuardedBootstrapReceiptConflict)
	}
	applier, ok := f.store.(store.RaftEntryApplier)
	if !ok {
		return nil, errors.WithStack(ErrGuardedBootstrapApply)
	}
	var receipt any
	err := applier.ApplyRaftEntry(ctx, identity.Index, func(tx context.Context) error {
		var found bool
		var err error
		receipt, found, err = f.readBootstrapReceiptVariant(tx, identity, payload)
		if err != nil || found {
			return err
		}
		if err := f.guardNewBootstrapIndex(identity.Index); err != nil {
			return err
		}
		receipt, err = f.stageBootstrapActivation(tx, identity, command)
		return err
	})
	if err != nil {
		return nil, errors.Wrap(err, "atomic bootstrap activation")
	}
	return receipt, nil
}

func (f *kvFSM) stageBootstrapActivation(ctx context.Context, identity raftbootstrap.EntryIdentity, command raftbootstrap.ActivationCommand) (raftbootstrap.ActivationReceipt, error) {
	metadata, ok := f.store.(store.RaftControlMetadataStore)
	if !ok {
		return raftbootstrap.ActivationReceipt{}, errors.WithStack(ErrGuardedBootstrapApply)
	}
	_, active, err := activationExistingPolicy(ctx, metadata, identity)
	if err != nil {
		return raftbootstrap.ActivationReceipt{}, err
	}
	var proof BootstrapActivationNativeProof
	if !active {
		proof, err = f.bootstrapActivationProofForApply(ctx, metadata)
		if err != nil {
			return raftbootstrap.ActivationReceipt{}, errors.Join(ErrBootstrapActivationNativeProof, err)
		}
	}
	return StageBootstrapActivation(ctx, metadata, identity, command, proof, f.store.LastCommitTS())
}

// StageBootstrapActivation runs inside the caller's indexed entry transaction.
// entry identity and proof must be supplied by the trusted native apply owner.
// watermark is the current KV watermark or TSO allocation floor, not the
// command's claimed value. The caller owns duplicate-first dispatch, commit and
// scalar/mirror publication. Any error must abort the entire transaction.
func StageBootstrapActivation(ctx context.Context, metadata store.RaftControlMetadataStore, identity raftbootstrap.EntryIdentity, command raftbootstrap.ActivationCommand, proof BootstrapActivationNativeProof, watermark uint64) (raftbootstrap.ActivationReceipt, error) {
	receipt, found, err := readActivationReceiptInEntry(ctx, metadata, identity)
	if err != nil || found {
		return receipt, err
	}
	if _, err := raftbootstrap.EncodeActivationCommand(command); err != nil {
		return receipt, errors.WithStack(err)
	}
	return stageNewBootstrapActivation(ctx, metadata, identity, command, proof, watermark)
}

func readActivationReceiptInEntry(ctx context.Context, metadata store.RaftControlMetadataStore, identity raftbootstrap.EntryIdentity) (raftbootstrap.ActivationReceipt, bool, error) {
	var receipt raftbootstrap.ActivationReceipt
	if metadata == nil {
		return receipt, false, errors.WithStack(ErrGuardedBootstrapApply)
	}
	key, err := raftbootstrap.ReceiptKey(identity)
	if err != nil {
		return receipt, false, errors.WithStack(err)
	}
	raw, err := metadata.ReadRaftControlMetadata(ctx, key)
	if errors.Is(err, store.ErrKeyNotFound) {
		return receipt, false, nil
	}
	if err != nil {
		return receipt, false, errors.WithStack(err)
	}
	receipt, err = raftbootstrap.ParseActivationReceipt(raw)
	if err != nil || receipt.Entry != identity {
		return receipt, false, errors.Join(ErrGuardedBootstrapReceiptConflict, err)
	}
	return receipt, true, nil
}

func stageNewBootstrapActivation(ctx context.Context, metadata store.RaftControlMetadataStore, identity raftbootstrap.EntryIdentity, command raftbootstrap.ActivationCommand, proof BootstrapActivationNativeProof, watermark uint64) (raftbootstrap.ActivationReceipt, error) {
	trusted := raftbootstrap.Identity{Cluster: identity.Cluster, Group: identity.Group}
	if command.Manifest.Identity != trusted || command.Witness.Cluster != trusted.Cluster || command.Witness.Group != trusted.Group {
		return raftbootstrap.ActivationReceipt{}, errors.WithStack(ErrGuardedBootstrapReceiptConflict)
	}
	digest, err := raftbootstrap.BaselineManifestDigest(command.Manifest)
	if err != nil {
		return raftbootstrap.ActivationReceipt{}, errors.WithStack(err)
	}
	receipt := raftbootstrap.ActivationReceipt{Entry: identity, Witness: command.Witness,
		OperationID: command.Manifest.OperationID, ManifestDigest: digest, Verdict: raftbootstrap.ActivationReceiptApplied}
	policy, verdict, err := activationPolicyForEntry(ctx, metadata, identity, command, proof, digest, watermark)
	if err != nil {
		return receipt, err
	}
	receipt.Verdict = verdict
	receipt.PolicyDigest, err = raftbootstrap.PolicyDigest(policy)
	if err != nil {
		return receipt, errors.WithStack(err)
	}
	return receipt, stageBootstrapActivationReceipt(ctx, metadata, receipt)
}

func activationPolicyForEntry(ctx context.Context, metadata store.RaftControlMetadataStore, identity raftbootstrap.EntryIdentity, command raftbootstrap.ActivationCommand, proof BootstrapActivationNativeProof, digest raftbootstrap.Digest, watermark uint64) (raftbootstrap.PolicyRecord, raftbootstrap.ActivationReceiptVerdict, error) {
	policy, active, err := activationExistingPolicy(ctx, metadata, identity)
	if err != nil {
		return policy, 0, err
	}
	if active {
		return policy, raftbootstrap.ActivationReceiptAlreadyActive, nil
	}
	trusted := raftbootstrap.Identity{Cluster: identity.Cluster, Group: identity.Group}
	if err := verifyBootstrapActivationPreparation(ctx, metadata, trusted, command, digest, identity.Index, watermark); err != nil {
		return policy, 0, err
	}
	if err := VerifyBootstrapActivationNativeProof(trusted, proof, command.Manifest.Configuration); err != nil {
		return policy, 0, err
	}
	policy, err = NewBootstrapActivationPolicy(trusted, command.Manifest, identity.Index)
	if err != nil {
		return policy, 0, err
	}
	if err := stageBootstrapActivationPolicy(ctx, metadata, command.Manifest, policy); err != nil {
		return policy, 0, err
	}
	return policy, raftbootstrap.ActivationReceiptApplied, nil
}

func activationExistingPolicy(ctx context.Context, metadata store.RaftControlMetadataStore, identity raftbootstrap.EntryIdentity) (raftbootstrap.PolicyRecord, bool, error) {
	trusted := raftbootstrap.Identity{Cluster: identity.Cluster, Group: identity.Group}
	policy, err := readAppliedBootstrapPolicy(ctx, metadata, trusted, identity.Index-1)
	if errors.Is(err, store.ErrKeyNotFound) {
		return policy, false, nil
	}
	return policy, err == nil, err
}

func verifyBootstrapActivationPreparation(ctx context.Context, metadata store.RaftControlMetadataStore, identity raftbootstrap.Identity, command raftbootstrap.ActivationCommand, digest raftbootstrap.Digest, index, watermark uint64) error {
	prepared, err := ReadPreparedBootstrapActivation(ctx, metadata, identity)
	if err != nil {
		return err
	}
	if digest != prepared.ManifestDigest || command.Manifest.CommitIndex >= index || watermark != prepared.Probe.Watermark {
		return errors.WithStack(ErrBootstrapActivationPreparation)
	}
	if err := raftbootstrap.VerifyBaselineManifest(command.Manifest, prepared.Probe.BaselineContext); err != nil {
		return errors.Join(ErrBootstrapActivationPreparation, err)
	}
	if err := matchBootstrapLocalEvidence(prepared.Probe, command.Manifest); err != nil {
		return err
	}
	return nil
}

// NewBootstrapActivationPolicy constructs P only; it does not authorize apply.
// Callers still owe independently stored preparation and live native proof.
// BaselineDigest retains the intent/WAL configuration digest. The complete
// accepted cut is separately bound by preparation identity and entry receipts.
func NewBootstrapActivationPolicy(identity raftbootstrap.Identity, manifest raftbootstrap.BaselineManifest, index uint64) (raftbootstrap.PolicyRecord, error) {
	if identity != manifest.Identity || index <= manifest.CommitIndex {
		return raftbootstrap.PolicyRecord{}, errors.WithStack(ErrBootstrapActivationPreparation)
	}
	if _, err := raftbootstrap.BaselineManifestDigest(manifest); err != nil {
		return raftbootstrap.PolicyRecord{}, errors.WithStack(err)
	}
	digest, err := raftbootstrap.ConfigurationDigest(manifest.Configuration)
	if err != nil {
		return raftbootstrap.PolicyRecord{}, errors.WithStack(err)
	}
	policy := raftbootstrap.PolicyRecord{Version: raftbootstrap.PolicyVersion, Identity: identity,
		BaselineDigest: digest, Minimum: manifest.Minimum, ActivationIndex: index, Generation: 1,
		LastConfigurationIndex: manifest.Configuration.Index, Configuration: manifest.Configuration}
	if _, err := raftbootstrap.MarshalPolicyRecord(policy); err != nil {
		return raftbootstrap.PolicyRecord{}, errors.WithStack(err)
	}
	return policy, nil
}

func matchBootstrapLocalEvidence(probe raftbootstrap.BaselineProbe, manifest raftbootstrap.BaselineManifest) error {
	local, err := raftbootstrap.MarshalBaselineProbe(probe)
	if err != nil {
		return errors.WithStack(err)
	}
	for _, member := range manifest.Members {
		if member.MemberID != probe.Evidence.MemberID {
			continue
		}
		expected, err := raftbootstrap.MarshalBaselineProbe(raftbootstrap.BaselineProbe{BaselineContext: manifest.BaselineContext, Evidence: member})
		if err != nil || !bytes.Equal(local, expected) {
			return errors.Join(ErrBootstrapActivationPreparation, err)
		}
		return nil
	}
	return errors.WithStack(ErrBootstrapActivationPreparation)
}

// VerifyBootstrapActivationNativeProof compares a native-owned current view
// with the independently prepared configuration, including exact ConfState.
func VerifyBootstrapActivationNativeProof(identity raftbootstrap.Identity, proof BootstrapActivationNativeProof, expected raftbootstrap.Configuration) error {
	actualDigest, err := raftbootstrap.ConfigurationDigest(proof.Configuration)
	if err != nil {
		return errors.Join(ErrBootstrapActivationNativeProof, err)
	}
	expectedDigest, err := raftbootstrap.ConfigurationDigest(expected)
	if err != nil {
		return errors.WithStack(err)
	}
	if proof.Identity != identity || proof.Configuration.Index != expected.Index || actualDigest != expectedDigest {
		return errors.WithStack(ErrBootstrapActivationNativeProof)
	}
	return nil
}

func stageBootstrapActivationPolicy(ctx context.Context, metadata store.RaftControlMetadataStore, manifest raftbootstrap.BaselineManifest, policy raftbootstrap.PolicyRecord) error {
	manifestRaw, err := raftbootstrap.MarshalBaselineManifest(manifest)
	if err != nil {
		return errors.WithStack(err)
	}
	baselineKey, err := bootstrapActivationMetadataKey("B/activation/", policy.Identity)
	if err != nil {
		return err
	}
	previous, err := metadata.ReadRaftControlMetadata(ctx, baselineKey)
	if err == nil && !bytes.Equal(previous, manifestRaw) {
		return errors.WithStack(ErrBootstrapActivationPreparation)
	}
	if err != nil && !errors.Is(err, store.ErrKeyNotFound) {
		return errors.WithStack(err)
	}
	if err := metadata.StageRaftControlMetadata(ctx, baselineKey, manifestRaw); err != nil {
		return errors.WithStack(err)
	}
	policyRaw, err := raftbootstrap.MarshalPolicyRecord(policy)
	if err != nil {
		return errors.WithStack(err)
	}
	policyKey, err := BootstrapPolicyMetadataKey(policy.Identity)
	if err != nil {
		return err
	}
	return errors.WithStack(metadata.StageRaftControlMetadata(ctx, policyKey, policyRaw))
}

func stageBootstrapActivationReceipt(ctx context.Context, metadata store.RaftControlMetadataStore, receipt raftbootstrap.ActivationReceipt) error {
	raw, err := raftbootstrap.EncodeActivationReceipt(receipt)
	if err != nil {
		return errors.WithStack(err)
	}
	key, err := raftbootstrap.ReceiptKey(receipt.Entry)
	if err != nil {
		return errors.WithStack(err)
	}
	return errors.WithStack(metadata.StageRaftControlMetadata(ctx, key, raw))
}
