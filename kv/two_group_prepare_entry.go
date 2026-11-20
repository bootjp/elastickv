package kv

import (
	"bytes"
	"context"

	pb "github.com/bootjp/elastickv/proto"
	"github.com/cockroachdb/errors"
	"google.golang.org/protobuf/proto"
)

// Wire type 6 is invalid protobuf, so this cannot alias a bare legacy Request.
const raftEncodeTwoGroupPrepare byte = 0x2e
const twoGroupPrepareEntryVersion = 1

type twoGroupPrepareApplyContextKey struct{}

func marshalTwoGroupPrepareEntry(group uint64, manifest *pb.TwoGroupPrepareManifest, observed TwoGroupAdmissionObservation) ([]byte, error) {
	_, _, _, err := canonicalTwoGroupManifest(manifest)
	if err != nil || group == 0 {
		return nil, errors.Join(ErrTwoGroupAdmission, err)
	}
	entry := &pb.TwoGroupPrepareEntry{Version: twoGroupPrepareEntryVersion, LocalGroupId: group, Manifest: manifest}
	if group == manifest.GetPrimaryGroupId() {
		if len(observed.record) != 0 || observed.barrierIndex != 0 || observed.leaderTerm != 0 {
			return nil, errors.WithStack(ErrTwoGroupAdmission)
		}
	} else {
		if _, err := validateTwoGroupObservation(observed, manifest, group); err != nil {
			return nil, err
		}
		entry.HomeObservation = observed.record
		entry.HomeBarrierIndex = observed.barrierIndex
		entry.HomeLeaderTerm = observed.leaderTerm
	}
	raw, err := (proto.MarshalOptions{Deterministic: true}).Marshal(entry)
	if err != nil || len(raw) >= maxMarshaledCommandSize {
		return nil, errors.Join(ErrTwoGroupAdmission, err)
	}
	return prependByte(raftEncodeTwoGroupPrepare, raw), nil
}

func (f *kvFSM) applyTwoGroupPrepareEntry(ctx context.Context, raw []byte) any {
	if !f.twoGroupPrepareEnabled.Load() {
		return haltErr(errors.Wrap(errors.Mark(ErrTwoGroupAdmission, ErrRaftEntryApply), "two-group PREPARE not activated"))
	}
	entry, request, err := f.decodeTwoGroupPrepareEntry(raw)
	if err != nil {
		return err
	}
	return f.applyDecodedTwoGroupPrepare(ctx, entry, request)
}

func (f *kvFSM) decodeTwoGroupPrepareEntry(raw []byte) (*pb.TwoGroupPrepareEntry, *pb.Request, error) {
	if f.pendingApplyIdentity.Index == 0 ||
		f.pendingApplyIdentity.Term == 0 || f.shardGroupID == 0 {
		return nil, nil, errors.WithStack(ErrTwoGroupAdmission)
	}
	entry, err := parseTwoGroupPrepareEntry(raw, f.shardGroupID)
	if err != nil {
		return nil, nil, err
	}
	var request *pb.Request
	for _, participant := range entry.GetManifest().GetParticipants() {
		if participant.GetGroupId() == f.shardGroupID {
			request = participant.GetPrepare()
		}
	}
	if request == nil {
		return nil, nil, errors.WithStack(ErrTwoGroupAdmission)
	}
	if err := validateTwoGroupEntryEvidence(entry, f.shardGroupID); err != nil {
		return nil, nil, err
	}
	return entry, request, nil
}

func parseTwoGroupPrepareEntry(raw []byte, group uint64) (*pb.TwoGroupPrepareEntry, error) {
	entry := new(pb.TwoGroupPrepareEntry)
	if err := proto.Unmarshal(raw, entry); err != nil {
		return nil, errors.Join(ErrTwoGroupAdmission, err)
	}
	canonical, err := (proto.MarshalOptions{Deterministic: true}).Marshal(entry)
	if err != nil || !bytes.Equal(raw, canonical) || entry.GetVersion() != twoGroupPrepareEntryVersion ||
		entry.GetLocalGroupId() != group || len(entry.ProtoReflect().GetUnknown()) != 0 {
		return nil, errors.Join(ErrTwoGroupAdmission, err)
	}
	if _, _, _, err := canonicalTwoGroupManifest(entry.GetManifest()); err != nil {
		return nil, err
	}
	return entry, nil
}

func twoGroupEntryObservation(entry *pb.TwoGroupPrepareEntry) TwoGroupAdmissionObservation {
	return TwoGroupAdmissionObservation{record: entry.GetHomeObservation(),
		barrierIndex: entry.GetHomeBarrierIndex(), leaderTerm: entry.GetHomeLeaderTerm()}
}

func validateTwoGroupEntryEvidence(entry *pb.TwoGroupPrepareEntry, group uint64) error {
	if group == entry.GetManifest().GetPrimaryGroupId() {
		if len(entry.GetHomeObservation()) != 0 || entry.GetHomeBarrierIndex() != 0 || entry.GetHomeLeaderTerm() != 0 {
			return errors.WithStack(ErrTwoGroupAdmission)
		}
		return nil
	}
	_, err := validateTwoGroupObservation(twoGroupEntryObservation(entry), entry.GetManifest(), group)
	return err
}

func (f *kvFSM) applyDecodedTwoGroupPrepare(ctx context.Context, entry *pb.TwoGroupPrepareEntry, request *pb.Request) any {
	manifest := entry.GetManifest()
	f.beginDeferredApplyNotices()
	var commitTS uint64
	apply := func(entryCtx context.Context) error {
		var applyErr error
		commitTS, applyErr = f.applyRequestData(context.WithValue(entryCtx, twoGroupPrepareApplyContextKey{}, true), request)
		return applyErr
	}
	var already bool
	var err error
	if f.shardGroupID == manifest.GetPrimaryGroupId() {
		already, err = ApplyTwoGroupHomePrepare(ctx, f.store, f.pendingApplyIdentity.Index,
			f.pendingApplyIdentity.Term, manifest, apply)
	} else {
		observed := twoGroupEntryObservation(entry)
		already, err = ApplyTwoGroupSecondaryPrepare(ctx, f.store, f.pendingApplyIdentity.Index,
			f.shardGroupID, manifest, observed, apply)
	}
	if err != nil {
		return f.twoGroupApplyError(err)
	}
	if !already {
		f.observeAppliedCommitTS(commitTS)
	}
	f.flushDeferredApplyNotices()
	return nil
}

func (f *kvFSM) twoGroupApplyError(err error) any {
	f.discardDeferredApplyNotices()
	if errors.Is(err, ErrTwoGroupAdmission) || isTimestampBoundCreationVerdict(err) {
		return err
	}
	return haltErr(errors.Wrap(errors.Mark(err, ErrRaftEntryApply), "apply two-group PREPARE entry"))
}
