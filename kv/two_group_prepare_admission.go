package kv

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"

	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
	"google.golang.org/protobuf/proto"
)

const TwoGroupHomeAdmissionVersion = 1
const twoGroupManifestVersion = 1
const twoGroupIDSize = 16
const twoGroupObservationHeaderSize = 16

const twoGroupManifestDomain = "elastickv/two-group-prepare/manifest/v1\x00"
const twoGroupProjectionDomain = "elastickv/two-group-prepare/projection/v1\x00"

var ErrTwoGroupAdmission = errors.New("two-group PREPARE admission evidence invalid")

type TwoGroupHomeLookup interface {
	LookupTwoGroupHome(context.Context, [16]byte) ([]byte, uint64, uint64, error)
}

// TwoGroupAdmissionObservation can only be constructed by a successful home lookup.
// It is not accepted on public Forward or inferred from a numeric timestamp.
type TwoGroupAdmissionObservation struct {
	record       []byte
	barrierIndex uint64
	leaderTerm   uint64
}

func ValidTwoGroupPrepareManifest(manifest *pb.TwoGroupPrepareManifest) bool {
	_, _, _, err := canonicalTwoGroupManifest(manifest)
	return err == nil
}

func twoGroupHomeKey(id [16]byte) []byte {
	return append([]byte("timestamp-binding/two-group-home/"), id[:]...)
}

func twoGroupSecondaryKey(id [16]byte) []byte {
	return append([]byte("timestamp-binding/two-group-secondary/"), id[:]...)
}

func canonicalTwoGroupManifest(manifest *pb.TwoGroupPrepareManifest) ([]byte, [32]byte, [2][32]byte, error) {
	var full [32]byte
	var projections [2][32]byte
	if !validTwoGroupManifestHeader(manifest) || !validTwoGroupParticipants(manifest) {
		return nil, full, projections, errors.WithStack(ErrTwoGroupAdmission)
	}
	projections, err := twoGroupManifestProjections(manifest)
	if err != nil {
		return nil, full, projections, err
	}
	encoded, err := (proto.MarshalOptions{Deterministic: true}).Marshal(manifest)
	if err != nil {
		return nil, full, projections, errors.WithStack(err)
	}
	full = sha256.Sum256(append([]byte(twoGroupManifestDomain), encoded...))
	return encoded, full, projections, nil
}

func twoGroupManifestProjections(manifest *pb.TwoGroupPrepareManifest) ([2][32]byte, error) {
	var projections [2][32]byte
	var startTS uint64
	var primaryKey []byte
	primaryKeyWritten := false
	for n, participant := range manifest.GetParticipants() {
		request := participant.GetPrepare()
		if n == 0 {
			startTS = request.GetTs()
		} else if request.GetTs() != startTS {
			return projections, errors.WithStack(ErrTwoGroupAdmission)
		}
		meta, mutations, projection, err := twoGroupParticipantProjection(participant, manifest.GetPersistenceTimestamp())
		if err != nil {
			return projections, err
		}
		if n == 0 {
			primaryKey = meta.PrimaryKey
		} else if !bytes.Equal(primaryKey, meta.PrimaryKey) {
			return projections, errors.WithStack(ErrTwoGroupAdmission)
		}
		if participant.GetGroupId() == manifest.GetPrimaryGroupId() {
			primaryKeyWritten = containsTwoGroupMutationKey(mutations, primaryKey)
		}
		projections[n] = projection
	}
	expectedID := twoGroupOperationID(primaryKey, startTS)
	if !primaryKeyWritten || !bytes.Equal(manifest.GetOperationId(), expectedID[:]) {
		return projections, errors.WithStack(ErrTwoGroupAdmission)
	}
	return projections, nil
}

func containsTwoGroupMutationKey(mutations []*pb.Mutation, key []byte) bool {
	for _, mutation := range mutations {
		if bytes.Equal(mutation.GetKey(), key) {
			return true
		}
	}
	return false
}

func validTwoGroupManifestHeader(manifest *pb.TwoGroupPrepareManifest) bool {
	if manifest == nil {
		return false
	}
	return manifest.GetVersion() == twoGroupManifestVersion && len(manifest.GetOperationId()) == twoGroupIDSize &&
		!bytes.Equal(manifest.GetOperationId(), make([]byte, twoGroupIDSize)) &&
		manifest.GetPersistenceTimestamp() != 0 && manifest.GetPrimaryGroupId() != 0 &&
		len(manifest.GetParticipants()) == 2 && len(manifest.ProtoReflect().GetUnknown()) == 0
}

func validTwoGroupParticipants(manifest *pb.TwoGroupPrepareManifest) bool {
	participants := manifest.GetParticipants()
	first, second := participants[0], participants[1]
	if first == nil || second == nil {
		return false
	}
	return first.GetGroupId() != 0 && first.GetGroupId() < second.GetGroupId() &&
		(manifest.GetPrimaryGroupId() == first.GetGroupId() || manifest.GetPrimaryGroupId() == second.GetGroupId())
}

func validTwoGroupPrepareRequest(request *pb.Request, persistenceTS uint64) bool {
	if request == nil {
		return false
	}
	return request.GetIsTxn() && request.GetPhase() == pb.Phase_PREPARE &&
		request.GetTs() != 0 && request.GetTs() < persistenceTS &&
		len(request.ProtoReflect().GetUnknown()) == 0 && validTwoGroupAdmissionFields(request)
}

func validTwoGroupAdmissionFields(request *pb.Request) bool {
	return len(request.GetTimestampBindingOperationId()) == 0 &&
		request.GetTimestampBindingAdmissionTerm() == 0 && request.GetTimestampAdmissionFloor() == 0
}

func twoGroupParticipantProjection(participant *pb.TwoGroupPrepareParticipant, persistenceTS uint64) (TxnMeta, []*pb.Mutation, [32]byte, error) {
	var digest [32]byte
	request := participant.GetPrepare()
	if len(participant.ProtoReflect().GetUnknown()) != 0 || !validTwoGroupPrepareRequest(request, persistenceTS) {
		return TxnMeta{}, nil, digest, errors.WithStack(ErrTwoGroupAdmission)
	}
	for _, mutation := range request.GetMutations() {
		if mutation == nil || len(mutation.ProtoReflect().GetUnknown()) != 0 {
			return TxnMeta{}, nil, digest, errors.WithStack(ErrTwoGroupAdmission)
		}
	}
	meta, mutations, err := extractTxnMeta(request.GetMutations())
	if err != nil || !validTwoGroupTxnMeta(meta, mutations, persistenceTS) {
		return TxnMeta{}, nil, digest, errors.Join(ErrTwoGroupAdmission, err)
	}
	encoded, err := (proto.MarshalOptions{Deterministic: true}).Marshal(request)
	if err != nil {
		return TxnMeta{}, nil, digest, errors.WithStack(err)
	}
	h := sha256.New()
	_, _ = h.Write([]byte(twoGroupProjectionDomain))
	var group [8]byte
	binary.BigEndian.PutUint64(group[:], participant.GetGroupId())
	_, _ = h.Write(group[:])
	_, _ = h.Write(encoded)
	copy(digest[:], h.Sum(nil))
	return meta, mutations, digest, nil
}

func validTwoGroupTxnMeta(meta TxnMeta, mutations []*pb.Mutation, persistenceTS uint64) bool {
	return meta.CommitTS == persistenceTS && meta.PrepareCommitTS == persistenceTS && meta.PrevCommitTS == 0 &&
		len(meta.PrimaryKey) != 0 && len(mutations) != 0
}

func twoGroupRecord(manifest *pb.TwoGroupPrepareManifest, index, term uint64) ([]byte, TimestampBinding, error) {
	var binding TimestampBinding
	encoded, full, projections, err := canonicalTwoGroupManifest(manifest)
	if err != nil || index == 0 || term == 0 {
		return nil, binding, errors.Join(ErrTwoGroupAdmission, err)
	}
	record := &pb.TwoGroupHomeAdmissionRecord{
		Version:           TwoGroupHomeAdmissionVersion,
		Manifest:          manifest,
		FullDigest:        full[:],
		ProjectionDigests: [][]byte{projections[0][:], projections[1][:]},
		AppliedIndex:      index,
		AppliedTerm:       term,
	}
	raw, err := (proto.MarshalOptions{Deterministic: true}).Marshal(record)
	if err != nil || len(encoded) == 0 {
		return nil, binding, errors.Join(ErrTwoGroupAdmission, err)
	}
	copy(binding.OperationID[:], manifest.GetOperationId())
	binding.Timestamp = manifest.GetPersistenceTimestamp()
	binding.ManifestDigest = full
	binding.IsTxn = true
	binding.Phase = pb.Phase_PREPARE
	return raw, binding, nil
}

func parseTwoGroupRecord(raw []byte) (*pb.TwoGroupHomeAdmissionRecord, TimestampBinding, error) {
	var binding TimestampBinding
	record := new(pb.TwoGroupHomeAdmissionRecord)
	if len(raw) == 0 || proto.Unmarshal(raw, record) != nil || record.GetVersion() != TwoGroupHomeAdmissionVersion ||
		len(record.ProtoReflect().GetUnknown()) != 0 || len(record.GetProjectionDigests()) != 2 {
		return nil, binding, errors.WithStack(ErrTwoGroupAdmission)
	}
	canonical, wantBinding, err := twoGroupRecord(record.GetManifest(), record.GetAppliedIndex(), record.GetAppliedTerm())
	if err != nil || !bytes.Equal(raw, canonical) || !bytes.Equal(record.GetFullDigest(), wantBinding.ManifestDigest[:]) {
		return nil, binding, errors.Join(ErrTwoGroupAdmission, err)
	}
	return record, wantBinding, nil
}

// ApplyTwoGroupHomePrepare is opt-in and must be called from the primary's
// trusted native apply with its actual index/term and PREPARE data callback.
func ApplyTwoGroupHomePrepare(ctx context.Context, st store.MVCCStore, index, term uint64,
	manifest *pb.TwoGroupPrepareManifest, apply func(context.Context) error) (bool, error) {
	metadata, applier, err := twoGroupStores(st)
	if err != nil || apply == nil {
		return false, errors.Join(ErrTwoGroupAdmission, err)
	}
	raw, binding, err := twoGroupRecord(manifest, index, term)
	if err != nil {
		return false, err
	}
	var already bool
	err = applier.ApplyRaftEntry(ctx, index, func(entryCtx context.Context) error {
		var stageErr error
		already, stageErr = stageTwoGroupHomePrepare(entryCtx, metadata, raw, binding, apply)
		return stageErr
	})
	return already, errors.WithStack(err)
}

func stageTwoGroupHomePrepare(ctx context.Context, metadata store.RaftControlMetadataStore, raw []byte,
	binding TimestampBinding, apply func(context.Context) error) (bool, error) {
	old, readErr := metadata.ReadRaftControlMetadata(ctx, twoGroupHomeKey(binding.OperationID))
	if readErr != nil && !errors.Is(readErr, store.ErrKeyNotFound) {
		return false, errors.Wrap(readErr, "read home admission record")
	}
	if readErr == nil {
		return true, replayTwoGroupHome(ctx, metadata, old, binding)
	}
	already, err := StageTimestampBinding(ctx, metadata, binding)
	if err != nil || already {
		return false, errors.Join(ErrTimestampBindingConflict, err)
	}
	if err := metadata.StageRaftControlMetadata(ctx, twoGroupHomeKey(binding.OperationID), raw); err != nil {
		return false, errors.Wrap(err, "stage home admission record")
	}
	return false, apply(ctx)
}

func replayTwoGroupHome(ctx context.Context, metadata store.RaftControlMetadataStore, old []byte, binding TimestampBinding) error {
	_, oldBinding, err := parseTwoGroupRecord(old)
	if err != nil || oldBinding != binding {
		return errors.Join(ErrTimestampBindingConflict, err)
	}
	already, err := StageTimestampBinding(ctx, metadata, binding)
	if err != nil || !already {
		return errors.Join(ErrTimestampBindingConflict, err)
	}
	return nil
}

// LookupTwoGroupHomeRecord is called only after the home leader's applied
// ReadIndex and term/session checks. It cross-checks the immutable two-index
// binding before exposing the record.
func LookupTwoGroupHomeRecord(ctx context.Context, st store.RaftControlMetadataStore, id [16]byte) ([]byte, bool, error) {
	raw, err := st.ReadRaftControlMetadata(ctx, twoGroupHomeKey(id))
	if errors.Is(err, store.ErrKeyNotFound) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, errors.Wrap(err, "read home admission record")
	}
	_, binding, err := parseTwoGroupRecord(raw)
	if err != nil || binding.OperationID != id {
		return nil, false, errors.Join(ErrTwoGroupAdmission, err)
	}
	actual, found, err := LookupTimestampBinding(ctx, st, id)
	if err != nil || !found || actual != binding {
		return nil, false, errors.Join(ErrTwoGroupAdmission, err)
	}
	return raw, true, nil
}

func ObserveTwoGroupHome(ctx context.Context, lookup TwoGroupHomeLookup, manifest *pb.TwoGroupPrepareManifest) (TwoGroupAdmissionObservation, error) {
	var empty TwoGroupAdmissionObservation
	if lookup == nil {
		return empty, errors.WithStack(ErrTwoGroupAdmission)
	}
	_, expected, _, err := canonicalTwoGroupManifest(manifest)
	if err != nil {
		return empty, errors.Wrap(err, "lookup applied home admission")
	}
	var id [16]byte
	copy(id[:], manifest.GetOperationId())
	raw, barrier, term, err := lookup.LookupTwoGroupHome(ctx, id)
	if err != nil {
		return empty, errors.Wrap(err, "lookup applied home admission")
	}
	observed := TwoGroupAdmissionObservation{record: bytes.Clone(raw), barrierIndex: barrier, leaderTerm: term}
	binding, err := validateTwoGroupObservation(observed, manifest, 0)
	if err != nil || binding.OperationID != id || binding.ManifestDigest != expected {
		return empty, errors.Join(ErrTwoGroupAdmission, err)
	}
	return observed, nil
}

func validateTwoGroupObservation(observed TwoGroupAdmissionObservation, manifest *pb.TwoGroupPrepareManifest, group uint64) (TimestampBinding, error) {
	var empty TimestampBinding
	_, expected, projections, err := canonicalTwoGroupManifest(manifest)
	if err != nil {
		return empty, err
	}
	record, binding, err := parseTwoGroupRecord(observed.record)
	if err != nil || binding.ManifestDigest != expected || observed.barrierIndex < record.GetAppliedIndex() || observed.leaderTerm == 0 {
		return empty, errors.Join(ErrTwoGroupAdmission, err)
	}
	if group != 0 && !localTwoGroupProjectionMatches(record, manifest, group, projections) {
		return empty, errors.WithStack(ErrTwoGroupAdmission)
	}
	return binding, nil
}

func localTwoGroupProjectionMatches(record *pb.TwoGroupHomeAdmissionRecord, manifest *pb.TwoGroupPrepareManifest,
	group uint64, projections [2][32]byte) bool {
	if group == manifest.GetPrimaryGroupId() {
		return false
	}
	for n, participant := range record.GetManifest().GetParticipants() {
		if participant.GetGroupId() != group {
			continue
		}
		if n == 0 {
			return bytes.Equal(record.GetProjectionDigests()[0], projections[0][:])
		}
		if n == 1 {
			return bytes.Equal(record.GetProjectionDigests()[1], projections[1][:])
		}
	}
	return false
}

// ApplyTwoGroupSecondaryPrepare revalidates the full manifest and observation
// inside the same indexed entry that stages its local binding and PREPARE.
func ApplyTwoGroupSecondaryPrepare(ctx context.Context, st store.MVCCStore, index uint64, group uint64,
	manifest *pb.TwoGroupPrepareManifest, observed TwoGroupAdmissionObservation,
	apply func(context.Context) error) (bool, error) {
	metadata, applier, err := twoGroupStores(st)
	if err != nil || apply == nil || index == 0 || group == 0 || group == manifest.GetPrimaryGroupId() {
		return false, errors.Join(ErrTwoGroupAdmission, err)
	}
	binding, err := validateTwoGroupObservation(observed, manifest, group)
	if err != nil {
		return false, err
	}
	proof := make([]byte, twoGroupObservationHeaderSize+len(observed.record))
	binary.BigEndian.PutUint64(proof[:8], observed.barrierIndex)
	binary.BigEndian.PutUint64(proof[8:16], observed.leaderTerm)
	copy(proof[16:], observed.record)
	var already bool
	err = applier.ApplyRaftEntry(ctx, index, func(entryCtx context.Context) error {
		var stageErr error
		already, stageErr = stageTwoGroupSecondaryPrepare(entryCtx, metadata, proof, binding, group, apply)
		return stageErr
	})
	return already, errors.WithStack(err)
}

func stageTwoGroupSecondaryPrepare(ctx context.Context, metadata store.RaftControlMetadataStore, proof []byte,
	binding TimestampBinding, group uint64, apply func(context.Context) error) (bool, error) {
	old, readErr := metadata.ReadRaftControlMetadata(ctx, twoGroupSecondaryKey(binding.OperationID))
	if readErr != nil && !errors.Is(readErr, store.ErrKeyNotFound) {
		return false, errors.Wrap(readErr, "read secondary admission observation")
	}
	if readErr == nil {
		return true, replayTwoGroupSecondary(ctx, metadata, old, binding, group)
	}
	already, err := StageTimestampBinding(ctx, metadata, binding)
	if err != nil || already {
		return false, errors.Join(ErrTimestampBindingConflict, err)
	}
	if err := metadata.StageRaftControlMetadata(ctx, twoGroupSecondaryKey(binding.OperationID), proof); err != nil {
		return false, errors.Wrap(err, "stage secondary admission observation")
	}
	return false, apply(ctx)
}

func replayTwoGroupSecondary(ctx context.Context, metadata store.RaftControlMetadataStore, old []byte,
	binding TimestampBinding, group uint64) error {
	oldBinding, err := validateTwoGroupObservationProof(old, group)
	if err != nil || oldBinding != binding {
		return errors.Join(ErrTimestampBindingConflict, err)
	}
	already, err := StageTimestampBinding(ctx, metadata, binding)
	if err != nil || !already {
		return errors.Join(ErrTimestampBindingConflict, err)
	}
	return nil
}

func validateTwoGroupObservationProof(proof []byte, group uint64) (TimestampBinding, error) {
	var binding TimestampBinding
	if len(proof) <= 16 || binary.BigEndian.Uint64(proof[:8]) == 0 || binary.BigEndian.Uint64(proof[8:16]) == 0 {
		return binding, errors.WithStack(ErrTwoGroupAdmission)
	}
	record, bound, err := parseTwoGroupRecord(proof[16:])
	if err != nil || binary.BigEndian.Uint64(proof[:8]) < record.GetAppliedIndex() ||
		group == record.GetManifest().GetPrimaryGroupId() ||
		record.GetManifest().GetParticipants()[0].GetGroupId() != group &&
			record.GetManifest().GetParticipants()[1].GetGroupId() != group {
		return binding, errors.Join(ErrTwoGroupAdmission, err)
	}
	return bound, nil
}

func twoGroupStores(st store.MVCCStore) (store.RaftControlMetadataStore, store.RaftEntryApplier, error) {
	metadata, ok := st.(store.RaftControlMetadataStore)
	if !ok {
		return nil, nil, errors.WithStack(ErrTimestampBindingStoreUnsupported)
	}
	applier, ok := st.(store.RaftEntryApplier)
	if !ok {
		return nil, nil, errors.WithStack(ErrTimestampBindingStoreUnsupported)
	}
	return metadata, applier, nil
}
