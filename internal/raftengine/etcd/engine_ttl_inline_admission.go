package etcd

import (
	"context"
	"math"
	"slices"

	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/cockroachdb/errors"
	raftpb "go.etcd.io/raft/v3/raftpb"
)

type ttlInlineMarkerClassifier interface {
	IsTTLInlineCapabilityMarkerPayload([]byte) bool
}

type ttlInlineMarkerValidator interface {
	TTLInlineFenceReady() bool
	ValidateTTLInlineCapabilityMarkerPayload([]byte, raftengine.NativeConfiguration) error
}

type ttlInlineProposalCut struct {
	native raftengine.NativeConfiguration
	term   uint64
}

const ttlInlineMarkerScanBatch = 128

var errTTLInlineAdmissionStoragePending = errors.New("etcd raft membership change waits for pending Raft storage append")
var errTTLInlineAdmissionSnapshotPending = errors.New("etcd raft membership change waits for snapshot recovery")
var errTTLInlineMarkerProposalUnauthorized = errors.New("etcd raft TTL-inline capability marker requires verified admission")

func (e *Engine) ttlInlineMembershipAdmission(req adminRequest, changeType raftpb.ConfChangeType, member uint64) error {
	if changeType == raftpb.ConfChangeRemoveNode {
		return nil
	}
	blocked, err := e.ttlInlineMembershipBlocked()
	if err != nil || !blocked {
		return err
	}
	certified, err := e.ttlInlinePromotionCertified(req, changeType, member)
	if err != nil {
		return err
	}
	if !certified {
		return errors.WithStack(errTTLInlineMembershipProofRequired)
	}
	return nil
}

func (e *Engine) ttlInlinePromotionCertified(req adminRequest, changeType raftpb.ConfChangeType, member uint64) (bool, error) {
	if req.action != adminActionPromoteLearner || changeType != raftpb.ConfChangeAddNode || member == 0 {
		return false, nil
	}
	e.mu.RLock()
	learner := e.isLearnerNode[member]
	e.mu.RUnlock()
	if !learner {
		return false, nil
	}
	reader, ok := e.fsm.(interface {
		TTLInlineCertifiedLearner(context.Context, uint64) (bool, error)
	})
	if !ok {
		return false, nil
	}
	certified, err := reader.TTLInlineCertifiedLearner(req.ctx, member)
	return certified, errors.Wrap(err, "verify TTL-inline learner certificate")
}

func (e *Engine) ProposeTTLInlineCapability(ctx context.Context, native raftengine.NativeConfiguration, term uint64, payload []byte) (*raftengine.ProposalResult, error) {
	copy := native
	copy.Voters = slices.Clone(native.Voters)
	copy.VotersOutgoing = slices.Clone(native.VotersOutgoing)
	copy.Learners = slices.Clone(native.Learners)
	copy.LearnersNext = slices.Clone(native.LearnersNext)
	copy.Members = slices.Clone(native.Members)
	return e.proposeWithTTLInlineCut(ctx, payload, &ttlInlineProposalCut{native: copy, term: term})
}

func (e *Engine) authorizeTTLInlineMarkerProposal(req proposalRequest) error {
	if req.ttlInlineCut == nil {
		return e.rejectUnverifiedTTLInlineMarker(req.payload)
	}
	marker, wrapped := e.ttlInlineCleartextMarker(req.payload)
	validator, ok := e.fsm.(ttlInlineMarkerValidator)
	if marker == nil || !ok || !validator.TTLInlineFenceReady() {
		return errors.WithStack(errTTLInlineMarkerProposalUnauthorized)
	}
	current, ok := e.currentTTLInlineProposalCut(req.ttlInlineCut, wrapped)
	if !ok {
		return errors.WithStack(errTTLInlineMarkerProposalUnauthorized)
	}
	if err := validator.ValidateTTLInlineCapabilityMarkerPayload(marker, current); err != nil {
		return errors.Join(errTTLInlineMarkerProposalUnauthorized, err)
	}
	return nil
}

func (e *Engine) currentTTLInlineProposalCut(cut *ttlInlineProposalCut, wrapped bool) (raftengine.NativeConfiguration, bool) {
	if e.rawNode == nil || e.recoveredIdentity == nil || cut.term == 0 {
		return raftengine.NativeConfiguration{}, false
	}
	current := e.captureApplyNativeConfiguration()
	if current == nil || e.rawNode.BasicStatus().GetTerm() != cut.term || !sameTTLInlineProposalCut(*current, cut.native) {
		return raftengine.NativeConfiguration{}, false
	}
	cutover := orInertCutover(e.raftCutoverIndex)()
	if cutover != math.MaxUint64 && e.appliedIndex.Load() >= cutover && !wrapped {
		return raftengine.NativeConfiguration{}, false
	}
	return *current, true
}

func sameTTLInlineProposalCut(left, right raftengine.NativeConfiguration) bool {
	return left.Index == right.Index && left.AutoLeave == right.AutoLeave &&
		slices.Equal(left.Voters, right.Voters) && slices.Equal(left.VotersOutgoing, right.VotersOutgoing) &&
		slices.Equal(left.Learners, right.Learners) && slices.Equal(left.LearnersNext, right.LearnersNext) &&
		slices.Equal(left.Members, right.Members)
}

func (e *Engine) ttlInlineCleartextMarker(payload []byte) ([]byte, bool) {
	classifier, ok := e.fsm.(ttlInlineMarkerClassifier)
	if !ok {
		return nil, false
	}
	if e.raftCipher != nil {
		plain, err := unwrapRaftPayload(e.raftCipher, payload)
		if err == nil {
			if classifier.IsTTLInlineCapabilityMarkerPayload(plain) {
				return plain, true
			}
			return nil, true
		}
	}
	if classifier.IsTTLInlineCapabilityMarkerPayload(payload) {
		return payload, false
	}
	return nil, false
}

func (e *Engine) rejectUnverifiedTTLInlineMarker(payload []byte) error {
	if marker, _ := e.ttlInlineCleartextMarker(payload); marker != nil {
		return errors.WithStack(errTTLInlineMarkerProposalUnauthorized)
	}
	return nil
}

func (e *Engine) ttlInlineMembershipBlocked() (bool, error) {
	classifier, ok := e.fsm.(ttlInlineMarkerClassifier)
	if !ok {
		if recorded, ok := e.fsm.(interface{ TTLInlineCapabilityRecorded() bool }); ok && recorded.TTLInlineCapabilityRecorded() {
			return true, nil
		}
		return false, nil
	}
	if e.ttlInlineSnapshotPending() {
		return false, errors.WithStack(errTTLInlineAdmissionSnapshotPending)
	}
	// The pipeline folds Ready before admitting Admin work. Until its disk
	// actor completes, the storage scan below cannot see every accepted entry.
	if e.storagePipeline != nil && e.storagePipeline.diskOutstanding != 0 {
		return false, errors.WithStack(errTTLInlineAdmissionStoragePending)
	}
	if recorded, ok := e.fsm.(interface{ TTLInlineCapabilityRecorded() bool }); ok && recorded.TTLInlineCapabilityRecorded() {
		return true, nil
	}
	return e.unappliedTTLInlineMarker(classifier)
}

func (e *Engine) ttlInlineSnapshotPending() bool {
	if e.snapshotRestoring.Load() || e.snapshotTransition.Load() {
		return true
	}
	return e.storagePipeline != nil &&
		(e.storagePipeline.receiving != nil || e.storagePipeline.normalizingReceived != nil)
}

func (e *Engine) unappliedTTLInlineMarker(classifier ttlInlineMarkerClassifier) (bool, error) {
	lo, last, err := e.ttlInlineMarkerScanBounds()
	if err != nil {
		return false, err
	}
	cutover := orInertCutover(e.raftCutoverIndex)()
	for lo <= last {
		hi := last + 1
		if hi-lo > ttlInlineMarkerScanBatch {
			hi = lo + ttlInlineMarkerScanBatch
		}
		marker, err := e.ttlInlineMarkerInBatch(lo, hi, classifier, cutover)
		if err != nil || marker {
			return marker, err
		}
		lo = hi
	}
	return false, nil
}

func (e *Engine) ttlInlineMarkerScanBounds() (uint64, uint64, error) {
	if e.storage == nil {
		return 0, 0, errors.Wrap(errTTLInlineMembershipProofRequired, "missing Raft log for marker preflight")
	}
	first, err := e.storage.FirstIndex()
	if err != nil {
		return 0, 0, errors.Wrap(err, "read first Raft log index for marker preflight")
	}
	last, err := e.storage.LastIndex()
	if err != nil {
		return 0, 0, errors.Wrap(err, "read last Raft log index for marker preflight")
	}
	if last == math.MaxUint64 {
		return 0, 0, errors.Wrap(errTTLInlineMembershipProofRequired, "Raft log index overflow")
	}
	lo := e.appliedIndex.Load()
	if lo == math.MaxUint64 {
		return 1, 0, nil
	}
	lo++
	if lo < first {
		lo = first
	}
	return lo, last, nil
}

func (e *Engine) ttlInlineMarkerInBatch(lo, hi uint64, classifier ttlInlineMarkerClassifier, cutover uint64) (bool, error) {
	entries, err := e.storage.Entries(lo, hi, math.MaxUint64)
	if err != nil {
		return false, errors.Wrapf(err, "read pending Raft log entries %d..%d for marker preflight", lo, hi)
	}
	if uint64(len(entries)) != hi-lo {
		return false, errors.Wrap(errTTLInlineMembershipProofRequired, "incomplete Raft log marker preflight")
	}
	for _, entry := range entries {
		marker, err := e.ttlInlineMarkerInEntry(entry, classifier, cutover)
		if err != nil || marker {
			return marker, err
		}
	}
	return false, nil
}

func (e *Engine) ttlInlineMarkerInEntry(entry *raftpb.Entry, classifier ttlInlineMarkerClassifier, cutover uint64) (bool, error) {
	if entry.GetType() != raftpb.EntryNormal {
		return false, nil
	}
	_, payload, decoded := decodeProposalEnvelope(entry.GetData())
	if !decoded {
		return false, nil
	}
	if e.raftCipher != nil {
		plain, err := unwrapRaftPayload(e.raftCipher, payload)
		if err == nil {
			return classifier.IsTTLInlineCapabilityMarkerPayload(plain), nil
		}
		if entry.GetIndex() > cutover {
			return false, errors.Wrap(err, "unwrap Raft log for marker preflight")
		}
	} else if entry.GetIndex() > cutover {
		return false, errors.Wrap(ErrRaftUnwrapFailed, "marker preflight has no Raft cipher past cutover")
	}
	return classifier.IsTTLInlineCapabilityMarkerPayload(payload), nil
}
