package kv

import (
	"bytes"
	"context"

	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
	"google.golang.org/protobuf/proto"
)

const twoGroupTerminalVersion = 1
const TwoGroupHomeDecisionLookupVersion = 2

type TwoGroupDecisionLookup interface {
	LookupTwoGroupDecision(context.Context, [16]byte) ([]byte, uint64, uint64, error)
}

type TwoGroupDecisionObservation struct {
	record       []byte
	barrierIndex uint64
	leaderTerm   uint64
}

func twoGroupTerminalKey(id [16]byte) []byte {
	return append([]byte("timestamp-binding/two-group-terminal/"), id[:]...)
}

func localTwoGroupManifestForLock(ctx context.Context, group *ShardGroup, primaryKey []byte, startTS uint64) (*pb.TwoGroupPrepareManifest, bool, error) {
	if group == nil || group.Store == nil {
		return nil, false, nil
	}
	metadata, ok := group.Store.(store.RaftControlMetadataStore)
	if !ok {
		return nil, false, nil
	}
	id := twoGroupOperationID(primaryKey, startTS)
	raw, err := metadata.ReadRaftControlMetadata(ctx, twoGroupHomeKey(id))
	home := true
	if errors.Is(err, store.ErrKeyNotFound) {
		raw, err = metadata.ReadRaftControlMetadata(ctx, twoGroupSecondaryKey(id))
		home = false
	}
	if errors.Is(err, store.ErrKeyNotFound) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, errors.Wrap(err, "read local two-group PREPARE")
	}
	manifest, err := parseLocalTwoGroupManifest(raw, home, id)
	return manifest, err == nil, err
}

func parseLocalTwoGroupManifest(raw []byte, home bool, id [16]byte) (*pb.TwoGroupPrepareManifest, error) {
	if !home {
		if len(raw) <= twoGroupObservationHeaderSize {
			return nil, errors.WithStack(ErrTwoGroupAdmission)
		}
		raw = raw[twoGroupObservationHeaderSize:]
	}
	record, _, err := parseTwoGroupRecord(raw)
	if err != nil || !bytes.Equal(record.GetManifest().GetOperationId(), id[:]) {
		return nil, errors.Join(ErrTwoGroupAdmission, err)
	}
	return record.GetManifest(), nil
}

func validTwoGroupDecision(decision pb.Phase) bool {
	return decision == pb.Phase_COMMIT || decision == pb.Phase_ABORT
}

func twoGroupTerminalRecord(manifest *pb.TwoGroupPrepareManifest, decision pb.Phase, index, term uint64) ([]byte, error) {
	if _, _, _, err := canonicalTwoGroupManifest(manifest); err != nil || !validTwoGroupDecision(decision) || index == 0 || term == 0 {
		return nil, errors.Join(ErrTwoGroupAdmission, err)
	}
	record := &pb.TwoGroupTerminalRecord{Version: twoGroupTerminalVersion, Manifest: manifest,
		Decision: decision, ResolutionTimestamp: manifest.GetPersistenceTimestamp(), AppliedIndex: index, AppliedTerm: term}
	raw, err := (proto.MarshalOptions{Deterministic: true}).Marshal(record)
	return raw, errors.WithStack(err)
}

func parseTwoGroupTerminalRecord(raw []byte) (*pb.TwoGroupTerminalRecord, error) {
	record := new(pb.TwoGroupTerminalRecord)
	if len(raw) == 0 || proto.Unmarshal(raw, record) != nil || record.GetVersion() != twoGroupTerminalVersion ||
		len(record.ProtoReflect().GetUnknown()) != 0 || record.GetResolutionTimestamp() != record.GetManifest().GetPersistenceTimestamp() {
		return nil, errors.WithStack(ErrTwoGroupAdmission)
	}
	want, err := twoGroupTerminalRecord(record.GetManifest(), record.GetDecision(), record.GetAppliedIndex(), record.GetAppliedTerm())
	if err != nil || !bytes.Equal(raw, want) {
		return nil, errors.Join(ErrTwoGroupAdmission, err)
	}
	return record, nil
}

func sameTwoGroupDecision(record *pb.TwoGroupTerminalRecord, manifest *pb.TwoGroupPrepareManifest, decision pb.Phase) bool {
	_, want, _, err := canonicalTwoGroupManifest(manifest)
	if err != nil || record.GetDecision() != decision || record.GetResolutionTimestamp() != manifest.GetPersistenceTimestamp() {
		return false
	}
	_, got, _, err := canonicalTwoGroupManifest(record.GetManifest())
	return err == nil && got == want
}

func lookupTwoGroupLocalPrepare(ctx context.Context, metadata store.RaftControlMetadataStore,
	manifest *pb.TwoGroupPrepareManifest, group uint64) error {
	var id [16]byte
	copy(id[:], manifest.GetOperationId())
	_, binding, err := twoGroupRecord(manifest, 1, 1)
	if err != nil {
		return err
	}
	actual, found, err := LookupTimestampBinding(ctx, metadata, id)
	if err != nil || !found || actual != binding {
		return errors.Join(ErrTwoGroupAdmission, err)
	}
	return lookupTwoGroupPrepareRecord(ctx, metadata, manifest, group, id, binding)
}

func lookupTwoGroupPrepareRecord(ctx context.Context, metadata store.RaftControlMetadataStore,
	manifest *pb.TwoGroupPrepareManifest, group uint64, id [16]byte, binding TimestampBinding) error {
	if group == manifest.GetPrimaryGroupId() {
		_, found, err := LookupTwoGroupHomeRecord(ctx, metadata, id)
		if err != nil || !found {
			return errors.Join(ErrTwoGroupAdmission, err)
		}
		return nil
	}
	proof, err := metadata.ReadRaftControlMetadata(ctx, twoGroupSecondaryKey(id))
	if err != nil {
		return errors.Join(ErrTwoGroupAdmission, err)
	}
	observedBinding, err := validateTwoGroupObservationProof(proof, group)
	if err != nil || observedBinding != binding {
		return errors.Join(ErrTwoGroupAdmission, err)
	}
	return nil
}

func stageTwoGroupTerminal(ctx context.Context, metadata store.RaftControlMetadataStore, group uint64,
	manifest *pb.TwoGroupPrepareManifest, decision pb.Phase, raw []byte, apply func(context.Context) error) (bool, error) {
	if err := lookupTwoGroupLocalPrepare(ctx, metadata, manifest, group); err != nil {
		return false, err
	}
	var id [16]byte
	copy(id[:], manifest.GetOperationId())
	old, err := metadata.ReadRaftControlMetadata(ctx, twoGroupTerminalKey(id))
	if err == nil {
		record, parseErr := parseTwoGroupTerminalRecord(old)
		if parseErr != nil || !sameTwoGroupDecision(record, manifest, decision) {
			return false, errors.Join(ErrTwoGroupAdmission, parseErr)
		}
		return true, nil
	}
	if !errors.Is(err, store.ErrKeyNotFound) {
		return false, errors.Wrap(err, "read local terminal record")
	}
	if err := metadata.StageRaftControlMetadata(ctx, twoGroupTerminalKey(id), raw); err != nil {
		return false, errors.Wrap(err, "stage local terminal record")
	}
	return false, apply(ctx)
}

func LookupTwoGroupHomeDecision(ctx context.Context, st store.RaftControlMetadataStore, id [16]byte) ([]byte, bool, error) {
	if _, found, err := LookupTwoGroupHomeRecord(ctx, st, id); err != nil || !found {
		return nil, false, errors.Join(ErrTwoGroupAdmission, err)
	}
	raw, err := st.ReadRaftControlMetadata(ctx, twoGroupTerminalKey(id))
	if errors.Is(err, store.ErrKeyNotFound) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, errors.Wrap(err, "read home terminal record")
	}
	record, err := parseTwoGroupTerminalRecord(raw)
	if err != nil || !bytes.Equal(record.GetManifest().GetOperationId(), id[:]) {
		return nil, false, errors.Join(ErrTwoGroupAdmission, err)
	}
	if err := lookupTwoGroupLocalPrepare(ctx, st, record.GetManifest(), record.GetManifest().GetPrimaryGroupId()); err != nil {
		return nil, false, err
	}
	return raw, true, nil
}

func ObserveTwoGroupDecision(ctx context.Context, lookup TwoGroupDecisionLookup,
	manifest *pb.TwoGroupPrepareManifest, decision pb.Phase) (TwoGroupDecisionObservation, error) {
	var empty TwoGroupDecisionObservation
	if lookup == nil || !validTwoGroupDecision(decision) {
		return empty, errors.WithStack(ErrTwoGroupAdmission)
	}
	var id [16]byte
	copy(id[:], manifest.GetOperationId())
	raw, index, term, err := lookup.LookupTwoGroupDecision(ctx, id)
	if err != nil {
		return empty, errors.Wrap(err, "lookup home terminal decision")
	}
	observed := TwoGroupDecisionObservation{record: bytes.Clone(raw), barrierIndex: index, leaderTerm: term}
	if err := validateTwoGroupDecisionObservation(observed, manifest, decision); err != nil {
		return empty, err
	}
	return observed, nil
}

func validateTwoGroupDecisionObservation(observed TwoGroupDecisionObservation,
	manifest *pb.TwoGroupPrepareManifest, decision pb.Phase) error {
	record, err := parseTwoGroupTerminalRecord(observed.record)
	if err != nil || observed.leaderTerm == 0 || observed.barrierIndex < record.GetAppliedIndex() ||
		!sameTwoGroupDecision(record, manifest, decision) || record.GetManifest().GetPrimaryGroupId() == 0 {
		return errors.Join(ErrTwoGroupAdmission, err)
	}
	return nil
}
