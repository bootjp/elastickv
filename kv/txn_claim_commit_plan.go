package kv

import (
	"bytes"
	"context"
	"slices"

	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
	"google.golang.org/protobuf/proto"
)

func (f *kvFSM) buildCommitRequestStoreMutations(ctx context.Context, request *pb.Request, mutations []*pb.Mutation, meta TxnMeta, applyStartTS uint64) ([]*store.KVPairMutation, error) {
	plan, err := f.buildCommitStoreMutations(ctx, mutations, meta, request.Ts, meta.CommitTS, applyStartTS == meta.CommitTS, request.ReadKeys)
	if err != nil || len(request.TimestampClaim) == 0 {
		return plan, err
	}
	return f.omitClaimedAppliedDecision(ctx, request, meta, plan)
}

// Only the exact already-applied decision Put is removed; user writes and
// prepared cleanup retain original-start OCC and exact-version comparison.
// This relies on immutable retained decisions/evidence and engine-owned serial
// FSM apply, not a global store applyMu spanning this read and publication.
func (f *kvFSM) omitClaimedAppliedDecision(ctx context.Context, request *pb.Request, meta TxnMeta, plan []*store.KVPairMutation) ([]*store.KVPairMutation, error) {
	key := txnCommitKey(meta.PrimaryKey, request.Ts)
	if !slices.ContainsFunc(plan, func(mutation *store.KVPairMutation) bool { return bytes.Equal(mutation.Key, key) }) {
		return plan, nil
	}
	version, exists, err := latestMVCCVersionAt(ctx, f.store, key, ^uint64(0))
	if err != nil {
		return nil, err
	}
	if !exists {
		return plan, nil
	}
	value := encodeTxnCommitRecord(meta.CommitTS)
	if !exactClaimDecisionVersion(version, key, value, meta.CommitTS) {
		return nil, errors.WithStack(store.NewWriteConflictError(key))
	}
	if err := f.verifyRetainedClaimDecision(ctx, request, meta); err != nil {
		return nil, err
	}
	return omitExactClaimDecisionPut(plan, key, value)
}

func exactClaimDecisionVersion(version store.MVCCVersion, key, value []byte, timestamp uint64) bool {
	return bytes.Equal(version.Key, key) && version.CommitTS == timestamp && !version.Tombstone &&
		version.ExpireAt == 0 && version.KeyFamily == 0 && bytes.Equal(version.Value, value)
}

func (f *kvFSM) verifyRetainedClaimDecision(ctx context.Context, request *pb.Request, meta TxnMeta) error {
	evidence, _, err := readTxnClaimEvidence(ctx, f.store, meta.PrimaryKey, request.Ts)
	if err != nil {
		return err
	}
	if !proto.Equal(evidence.TimestampIntent, request.TimestampIntent) || !bytes.Equal(evidence.TimestampClaim, request.TimestampClaim) || evidence.TimestampClaimGroupId != request.TimestampClaimGroupId {
		return errors.WithStack(ErrTSOClaimInvalid)
	}
	claim, err := DecodeRequestTimestampClaim(evidence.TimestampClaim)
	if err != nil {
		return err
	}
	if claim.Proof.Record.Timestamp != meta.CommitTS || evidence.TimestampIntent.ReadTimestamp != request.Ts || !bytes.Equal(evidence.TimestampIntent.PrimaryKey, meta.PrimaryKey) {
		return errors.WithStack(ErrTSOClaimInvalid)
	}
	return nil
}

func omitExactClaimDecisionPut(plan []*store.KVPairMutation, key, value []byte) ([]*store.KVPairMutation, error) {
	remaining := make([]*store.KVPairMutation, 0, len(plan))
	for _, mutation := range plan {
		if !bytes.Equal(mutation.Key, key) {
			remaining = append(remaining, mutation)
			continue
		}
		if mutation.Op != store.OpTypePut || mutation.RequireAbsent || mutation.ExpireAt != 0 || !bytes.Equal(mutation.Value, value) {
			return nil, errors.WithStack(ErrTSOClaimInvalid)
		}
	}
	return remaining, nil
}
