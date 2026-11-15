package kv

import (
	"bytes"
	"context"

	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
	"google.golang.org/protobuf/proto"
)

// Wire type 6 is invalid protobuf, so a bare legacy Request cannot alias it.
const raftEncodeTwoGroupTerminal byte = 0x36

type twoGroupTerminalApplyContextKey struct{}

func marshalTwoGroupTerminalEntry(group uint64, manifest *pb.TwoGroupPrepareManifest,
	decision pb.Phase, observed TwoGroupDecisionObservation) ([]byte, error) {
	if _, _, _, err := canonicalTwoGroupManifest(manifest); err != nil || group == 0 || !validTwoGroupDecision(decision) {
		return nil, errors.Join(ErrTwoGroupAdmission, err)
	}
	entry := &pb.TwoGroupTerminalEntry{Version: twoGroupTerminalVersion, LocalGroupId: group,
		Manifest: manifest, Decision: decision, ResolutionTimestamp: manifest.GetPersistenceTimestamp()}
	if err := setTwoGroupTerminalObservation(entry, observed); err != nil {
		return nil, err
	}
	raw, err := (proto.MarshalOptions{Deterministic: true}).Marshal(entry)
	if err != nil || len(raw) >= maxMarshaledCommandSize {
		return nil, errors.Join(ErrTwoGroupAdmission, err)
	}
	return prependByte(raftEncodeTwoGroupTerminal, raw), nil
}

func setTwoGroupTerminalObservation(entry *pb.TwoGroupTerminalEntry, observed TwoGroupDecisionObservation) error {
	manifest, decision := entry.GetManifest(), entry.GetDecision()
	group := entry.GetLocalGroupId()
	if group == manifest.GetPrimaryGroupId() {
		if len(observed.record) != 0 || observed.barrierIndex != 0 || observed.leaderTerm != 0 {
			return errors.WithStack(ErrTwoGroupAdmission)
		}
	} else {
		if err := validateTwoGroupDecisionObservation(observed, manifest, decision); err != nil {
			return err
		}
		entry.HomeTerminalRecord = observed.record
		entry.HomeBarrierIndex = observed.barrierIndex
		entry.HomeLeaderTerm = observed.leaderTerm
	}
	return nil
}

func parseTwoGroupTerminalEntry(raw []byte, group uint64) (*pb.TwoGroupTerminalEntry, error) {
	entry := new(pb.TwoGroupTerminalEntry)
	if err := proto.Unmarshal(raw, entry); err != nil {
		return nil, errors.Join(ErrTwoGroupAdmission, err)
	}
	if err := validateTwoGroupTerminalHeader(raw, entry, group); err != nil {
		return nil, err
	}
	if _, _, _, err := canonicalTwoGroupManifest(entry.GetManifest()); err != nil {
		return nil, err
	}
	if err := validateTwoGroupTerminalEntryEvidence(entry, group); err != nil {
		return nil, err
	}
	return entry, nil
}

func validateTwoGroupTerminalHeader(raw []byte, entry *pb.TwoGroupTerminalEntry, group uint64) error {
	canonical, err := (proto.MarshalOptions{Deterministic: true}).Marshal(entry)
	if err != nil || !bytes.Equal(raw, canonical) || entry.GetVersion() != twoGroupTerminalVersion ||
		entry.GetLocalGroupId() != group || !validTwoGroupDecision(entry.GetDecision()) ||
		entry.GetResolutionTimestamp() != entry.GetManifest().GetPersistenceTimestamp() ||
		len(entry.ProtoReflect().GetUnknown()) != 0 {
		return errors.Join(ErrTwoGroupAdmission, err)
	}
	return nil
}

func validateTwoGroupTerminalEntryEvidence(entry *pb.TwoGroupTerminalEntry, group uint64) error {
	if group == entry.GetManifest().GetPrimaryGroupId() {
		if len(entry.GetHomeTerminalRecord()) != 0 || entry.GetHomeBarrierIndex() != 0 || entry.GetHomeLeaderTerm() != 0 {
			return errors.WithStack(ErrTwoGroupAdmission)
		}
	} else {
		observed := twoGroupTerminalEntryObservation(entry)
		if err := validateTwoGroupDecisionObservation(observed, entry.GetManifest(), entry.GetDecision()); err != nil {
			return err
		}
	}
	return nil
}

func twoGroupTerminalEntryObservation(entry *pb.TwoGroupTerminalEntry) TwoGroupDecisionObservation {
	return TwoGroupDecisionObservation{record: entry.GetHomeTerminalRecord(),
		barrierIndex: entry.GetHomeBarrierIndex(), leaderTerm: entry.GetHomeLeaderTerm()}
}

func twoGroupTerminalRequest(entry *pb.TwoGroupTerminalEntry) (*pb.Request, error) {
	var prepare *pb.Request
	for _, participant := range entry.GetManifest().GetParticipants() {
		if participant.GetGroupId() == entry.GetLocalGroupId() {
			prepare = participant.GetPrepare()
			break
		}
	}
	if prepare == nil {
		return nil, errors.WithStack(ErrTwoGroupAdmission)
	}
	meta, mutations, err := extractTxnMeta(prepare.GetMutations())
	if err != nil {
		return nil, err
	}
	return &pb.Request{IsTxn: true, Phase: entry.GetDecision(), Ts: prepare.GetTs(),
		Mutations: append([]*pb.Mutation{txnMetaMutation(meta.PrimaryKey, entry.GetResolutionTimestamp())}, keyMutations(mutations)...),
		ReadKeys:  prepare.GetReadKeys(), ObservedRouteVersion: prepare.GetObservedRouteVersion(),
		WriteFenceBypassKeys: prepare.GetWriteFenceBypassKeys()}, nil
}

func (f *kvFSM) applyTwoGroupTerminalEntry(ctx context.Context, raw []byte) any {
	if !f.twoGroupPrepareEnabled.Load() {
		return haltErr(errors.Wrap(errors.Mark(ErrTwoGroupAdmission, ErrRaftEntryApply), "two-group terminal not activated"))
	}
	if f.pendingApplyIdentity.Index == 0 || f.pendingApplyIdentity.Term == 0 || f.shardGroupID == 0 {
		return errors.WithStack(ErrTwoGroupAdmission)
	}
	entry, request, record, err := f.decodeTwoGroupTerminal(raw)
	if err != nil {
		return err
	}
	metadata, applier, err := twoGroupStores(f.store)
	if err != nil {
		return errors.WithStack(err)
	}
	f.beginDeferredApplyNotices()
	var applied bool
	err = applier.ApplyRaftEntry(ctx, f.pendingApplyIdentity.Index, func(entryCtx context.Context) error {
		var stageErr error
		applied, stageErr = f.stageDecodedTwoGroupTerminal(entryCtx, metadata, entry, request, record)
		return stageErr
	})
	if err != nil {
		f.discardDeferredApplyNotices()
		return twoGroupTerminalApplyError(err)
	}
	if applied {
		f.observeAppliedCommitTS(entry.GetResolutionTimestamp())
	}
	f.flushDeferredApplyNotices()
	return nil
}

func twoGroupTerminalApplyError(err error) any {
	if errors.Is(err, ErrTwoGroupAdmission) || isTimestampBoundCreationVerdict(err) {
		return errors.WithStack(err)
	}
	return haltErr(errors.Wrap(errors.Mark(err, ErrRaftEntryApply), "apply two-group terminal entry"))
}

func (f *kvFSM) decodeTwoGroupTerminal(raw []byte) (*pb.TwoGroupTerminalEntry, *pb.Request, []byte, error) {
	entry, err := parseTwoGroupTerminalEntry(raw, f.shardGroupID)
	if err != nil {
		return nil, nil, nil, err
	}
	request, err := twoGroupTerminalRequest(entry)
	if err != nil {
		return nil, nil, nil, err
	}
	record, err := twoGroupTerminalRecord(entry.GetManifest(), entry.GetDecision(),
		f.pendingApplyIdentity.Index, f.pendingApplyIdentity.Term)
	return entry, request, record, err
}

func (f *kvFSM) stageDecodedTwoGroupTerminal(ctx context.Context, metadata store.RaftControlMetadataStore,
	entry *pb.TwoGroupTerminalEntry, request *pb.Request, record []byte) (bool, error) {
	if entry.GetLocalGroupId() != entry.GetManifest().GetPrimaryGroupId() {
		if err := validateTwoGroupDecisionObservation(twoGroupTerminalEntryObservation(entry),
			entry.GetManifest(), entry.GetDecision()); err != nil {
			return false, err
		}
	}
	already, err := stageTwoGroupTerminal(ctx, metadata, entry.GetLocalGroupId(), entry.GetManifest(),
		entry.GetDecision(), record, func(applyCtx context.Context) error {
			_, err := f.applyRequestData(context.WithValue(applyCtx, twoGroupTerminalApplyContextKey{}, true), request)
			return err
		})
	return !already && err == nil, err
}
