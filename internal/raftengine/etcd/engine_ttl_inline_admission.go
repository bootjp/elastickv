package etcd

import (
	"math"

	"github.com/cockroachdb/errors"
	raftpb "go.etcd.io/raft/v3/raftpb"
)

type ttlInlineMarkerClassifier interface {
	IsTTLInlineCapabilityMarkerPayload([]byte) bool
}

const ttlInlineMarkerScanBatch = 128

var errTTLInlineAdmissionStoragePending = errors.New("etcd raft membership change waits for pending Raft storage append")
var errTTLInlineAdmissionSnapshotPending = errors.New("etcd raft membership change waits for snapshot recovery")

func (e *Engine) isTTLInlineCapabilityProposal(payload []byte) bool {
	classifier, ok := e.fsm.(ttlInlineMarkerClassifier)
	if !ok {
		return false
	}
	if e.raftCipher != nil {
		if plain, err := unwrapRaftPayload(e.raftCipher, payload); err == nil {
			return classifier.IsTTLInlineCapabilityMarkerPayload(plain)
		}
	}
	return classifier.IsTTLInlineCapabilityMarkerPayload(payload)
}

func (e *Engine) ttlInlineMembershipBlocked() (bool, error) {
	if e.pendingTTLInlineMarker {
		return true, nil
	}
	if recorded, ok := e.fsm.(interface{ TTLInlineCapabilityRecorded() bool }); ok && recorded.TTLInlineCapabilityRecorded() {
		return true, nil
	}
	classifier, ok := e.fsm.(ttlInlineMarkerClassifier)
	if !ok {
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
