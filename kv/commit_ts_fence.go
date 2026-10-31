package kv

import (
	"bytes"
	"context"

	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
)

// raftEncodeCommitTSFenceActivation turns on the raw and PREPARE apply-order
// fence for the group whose log carries it (docs/design/
// 2026_09_26_partial_serializable_isolation_audit.md, "Raw and 2PC
// apply-order fence"). The entry has no payload. Its apply persists
// commitTSFenceEnabledKey, so the fence state is a function of the applied
// log and every replica of the group reaches the same verdict on every later
// raw entry and PREPARE. A per-node switch could not give that: replicas
// with different settings would accept and reject the same entry.
//
// 0x1f (field 3, wire type 7) is an invalid protobuf prefix, so it cannot be
// mistaken for a legacy bare Request (whose first byte is often 0x18, field 3
// "ts" as a varint) or a RaftCommand.
const raftEncodeCommitTSFenceActivation byte = 0x1f

var (
	commitTSFenceEnabledKey   = []byte(txnMetaPrefix + "commit_ts_fence/enabled")
	commitTSFenceEnabledValue = []byte{1}
)

var ErrInvalidCommitTSFenceActivation = errors.New("invalid commit-ts fence activation")

// EncodeCommitTSFenceActivation returns the Raft payload that turns the raw
// and PREPARE apply-order fence on for a group.
func EncodeCommitTSFenceActivation() []byte {
	return []byte{raftEncodeCommitTSFenceActivation}
}

// CommitTSFenceEnabled reports whether st has applied the fence activation.
func CommitTSFenceEnabled(ctx context.Context, st store.MVCCStore) (bool, error) {
	if st == nil {
		return false, nil
	}
	value, err := st.GetAt(ctx, commitTSFenceEnabledKey, ^uint64(0))
	if errors.Is(err, store.ErrKeyNotFound) {
		return false, nil
	}
	if err != nil {
		return false, errors.WithStack(err)
	}
	if !bytes.Equal(value, commitTSFenceEnabledValue) {
		return false, errors.Wrap(ErrInvalidCommitTSFenceActivation, "invalid marker value")
	}
	return true, nil
}

func (f *kvFSM) reloadCommitTSFence(ctx context.Context) error {
	enabled, err := CommitTSFenceEnabled(ctx, f.store)
	if err != nil {
		return err
	}
	f.commitTSFenceEnabled.Store(enabled)
	return nil
}

// applyCommitTSFenceActivation persists the marker at the current watermark,
// like the prepare evidence activation, so it writes no version above any
// timestamp a client was handed. A replay finds the marker set and is a
// no-op. A failure to persist halts the apply loop: a replica that skipped
// the activation would keep accepting entries its peers fence.
func (f *kvFSM) applyCommitTSFenceActivation(ctx context.Context, body []byte) any {
	if len(body) != 0 {
		return errors.Wrapf(ErrInvalidCommitTSFenceActivation, "unexpected %d-byte payload", len(body))
	}
	if f.commitTSFenceEnabled.Load() {
		return nil
	}
	stamp := max(uint64(1), f.store.LastCommitTS())
	muts := []*store.KVPairMutation{{Op: store.OpTypePut, Key: commitTSFenceEnabledKey, Value: commitTSFenceEnabledValue}}
	if err := f.store.ApplyMutationsRaftAt(ctx, muts, nil, stamp, stamp, f.pendingApplyIdx); err != nil {
		return haltErr(errors.Wrap(errors.Mark(err, ErrRaftEntryApply), "kv/fsm: persist commit-ts fence activation"))
	}
	f.commitTSFenceEnabled.Store(true)
	return nil
}

// checkPrepareCommitTSFence rejects a PREPARE whose settled commit timestamp
// is at or below the group's watermark, once the fence is active. A reader
// that finished before this PREPARE read at a snapshot no higher than that
// watermark; a reader after it meets the PREPARE's locks. Either way the
// COMMIT cannot land below a snapshot that was already read, which is why
// the COMMIT itself is not fenced. A transaction without a settled commit
// timestamp, or a PREPARE that writes nothing, is not checked.
func (f *kvFSM) checkPrepareCommitTSFence(meta TxnMeta, storeMuts []*store.KVPairMutation) error {
	commitTS := prepareRouteFloorTimestamp(meta, 0)
	if !f.commitTSFenceEnabled.Load() || commitTS == 0 {
		return nil
	}
	if commitTS > f.store.LastCommitTS() {
		return nil
	}
	for _, mut := range storeMuts {
		if mut != nil && len(mut.Key) > 0 {
			return errors.WithStack(store.NewWriteConflictError(mut.Key))
		}
	}
	return nil
}

// applyRawMutations applies a raw entry's mutations, fenced once the
// activation has applied. Raw requests commit against the latest state, so
// commitTS is both the validation snapshot and the commit timestamp.
func (f *kvFSM) applyRawMutations(ctx context.Context, muts []*store.KVPairMutation, commitTS uint64) error {
	if f.commitTSFenceEnabled.Load() {
		return errors.WithStack(f.store.ApplyMutationsRaftAtFenced(ctx, muts, nil, commitTS, commitTS, f.pendingApplyIdx))
	}
	return errors.WithStack(f.store.ApplyMutationsRaftAt(ctx, muts, nil, commitTS, commitTS, f.pendingApplyIdx))
}
