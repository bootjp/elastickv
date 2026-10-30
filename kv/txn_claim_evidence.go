package kv

import (
	"bytes"
	"context"
	"crypto/sha256"
	"slices"

	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
	"google.golang.org/protobuf/proto"
)

const txnClaimLockVersion byte = 2
const txnClaimEvidenceHashBytes = 32

func encodeClaimedTxnLock(lock txnLock) []byte {
	hash := lock.ClaimEvidenceHash
	lock.ClaimEvidenceHash = [32]byte{}
	encoded := encodeTxnLock(lock)
	encoded[0] = txnClaimLockVersion
	return append(encoded, hash[:]...)
}

func decodeClaimedTxnLock(raw []byte) (txnLock, error) {
	if len(raw) <= txnClaimEvidenceHashBytes {
		return txnLock{}, errors.WithStack(ErrTxnInvalidMeta)
	}
	plain := bytes.Clone(raw[:len(raw)-txnClaimEvidenceHashBytes])
	plain[0] = txnLockVersion
	lock, err := decodeTxnLock(plain)
	if err != nil {
		return txnLock{}, err
	}
	copy(lock.ClaimEvidenceHash[:], raw[len(raw)-txnClaimEvidenceHashBytes:])
	if lock.ClaimEvidenceHash == ([32]byte{}) || lock.CommitTS == 0 {
		return txnLock{}, errors.WithStack(ErrTxnInvalidMeta)
	}
	return lock, nil
}

func txnClaimEvidenceKey(primary []byte, start uint64) []byte {
	key := []byte(txnMetaPrefix + "slot_claim/")
	return append(key, txnCommitKey(primary, start)...)
}

func encodeTxnClaimEvidence(request *pb.Request) ([]byte, error) {
	evidence := &pb.Request{IsTxn: true, Phase: pb.Phase_PREPARE, Ts: request.Ts,
		TimestampIntent: request.TimestampIntent, TimestampClaim: request.TimestampClaim, TimestampClaimGroupId: request.TimestampClaimGroupId}
	raw, err := (proto.MarshalOptions{Deterministic: true}).Marshal(evidence)
	return raw, errors.Wrap(err, "encode durable transaction claim evidence")
}

func readTxnClaimEvidence(ctx context.Context, st store.MVCCStore, primary []byte, start uint64) (*pb.Request, []byte, error) {
	if st == nil {
		return nil, nil, errors.WithStack(ErrTSOClaimInvalid)
	}
	raw, err := st.GetAt(ctx, txnClaimEvidenceKey(primary, start), ^uint64(0))
	if err != nil {
		return nil, nil, errors.WithStack(err)
	}
	evidence, err := decodeTxnClaimEvidence(raw, start)
	return evidence, raw, err
}

func decodeTxnClaimEvidence(raw []byte, start uint64) (*pb.Request, error) {
	if len(raw) > maxTSOClaimIntentBytes {
		return nil, errors.WithStack(ErrTSOClaimInvalid)
	}
	evidence := &pb.Request{}
	if err := proto.Unmarshal(raw, evidence); err != nil {
		return nil, errors.Wrap(err, "decode durable transaction claim evidence")
	}
	if !evidence.IsTxn || evidence.Phase != pb.Phase_PREPARE || evidence.Ts != start || len(evidence.Mutations) != 0 || evidence.TimestampIntent == nil || len(evidence.ProtoReflect().GetUnknown()) != 0 {
		return nil, errors.WithStack(ErrTSOClaimInvalid)
	}
	return evidence, nil
}

func (f *kvFSM) bindPrepareClaimEvidence(ctx context.Context, request *pb.Request, muts []*store.KVPairMutation) ([]*store.KVPairMutation, error) {
	if len(request.TimestampClaim) == 0 {
		return muts, nil
	}
	if err := ValidateRequestTimestampIntent(request); err != nil {
		return nil, err
	}
	claim, err := DecodeRequestTimestampClaim(request.TimestampClaim)
	if err != nil {
		return nil, err
	}
	meta, _, err := extractTxnMeta(request.Mutations)
	if err != nil {
		return nil, err
	}
	if claim.Proof.Record.Timestamp != meta.PrepareCommitTS {
		return nil, errors.WithStack(ErrTSOClaimInvalid)
	}
	raw, err := encodeTxnClaimEvidence(request)
	if err != nil {
		return nil, err
	}
	hash := sha256.Sum256(raw)
	if err := f.verifyRetainedPrepareClaim(ctx, meta.PrimaryKey, request.Ts, raw); err != nil {
		return nil, err
	}
	if err := bindPrepareClaimLockHashes(muts, hash); err != nil {
		return nil, err
	}
	muts = append(muts, &store.KVPairMutation{Op: store.OpTypePut, Key: txnClaimEvidenceKey(meta.PrimaryKey, request.Ts), Value: raw})
	return muts, nil
}

func bindPrepareClaimLockHashes(muts []*store.KVPairMutation, hash [32]byte) error {
	for _, mut := range muts {
		if !bytes.HasPrefix(mut.Key, []byte(txnLockPrefix)) {
			continue
		}
		lock, err := decodeTxnLock(mut.Value)
		if err != nil {
			return err
		}
		lock.ClaimEvidenceHash = hash
		mut.Value = encodeTxnLock(lock)
	}
	return nil
}

func (f *kvFSM) verifyRetainedPrepareClaim(ctx context.Context, primary []byte, start uint64, raw []byte) error {
	prior, err := f.store.GetAt(ctx, txnClaimEvidenceKey(primary, start), ^uint64(0))
	if errors.Is(err, store.ErrKeyNotFound) {
		return nil
	}
	if err != nil {
		return errors.WithStack(err)
	}
	if !bytes.Equal(prior, raw) {
		return errors.WithStack(ErrTSOClaimConflict)
	}
	return nil
}

func ValidateClaimedTxnResolution(ctx context.Context, allocator TimestampAllocator, st store.MVCCStore, request *pb.Request) error {
	meta, muts, err := parseClaimResolutionRequest(request)
	if err != nil {
		return err
	}
	evidence, _, err := readTxnClaimEvidence(ctx, st, meta.PrimaryKey, request.Ts)
	if err != nil {
		return errors.Wrap(err, "resolution requires durable prepared claim evidence")
	}
	participant, err := retainedResolutionParticipant(request, meta, evidence)
	if err != nil {
		return err
	}
	if err := validateClaimResolutionKeys(muts, participant); err != nil {
		return err
	}
	if !claimResolutionMetadataWithin(request.ReadKeys, participant.ReadKeys) ||
		!claimResolutionMetadataWithin(request.WriteFenceBypassKeys, participant.WriteFenceBypassKeys) {
		return errors.WithStack(ErrTSOClaimInvalid)
	}
	claim, err := validateTimestampIntentClaim(request)
	if err != nil {
		return err
	}
	if meta.CommitTS != claim.Proof.Record.Timestamp {
		return errors.WithStack(ErrTSOClaimInvalid)
	}
	// PREPARE already validated the actual values against this exact immutable
	// capsule. Resolution carries only key placeholders, never invented values
	// reconstructed from digests. Stored intents and lock hashes govern apply.
	return lookupRequestClaimRecord(ctx, allocator, claim)
}

func parseClaimResolutionRequest(request *pb.Request) (TxnMeta, []*pb.Mutation, error) {
	if request == nil || !request.IsTxn || (request.Phase != pb.Phase_COMMIT && request.Phase != pb.Phase_ABORT) || len(request.ProtoReflect().GetUnknown()) != 0 || len(request.TimestampClaimPrefixIndices) != 0 {
		return TxnMeta{}, nil, errors.WithStack(ErrTSOClaimInvalid)
	}
	if err := validateClaimMutationHeaders(request.Mutations); err != nil {
		return TxnMeta{}, nil, err
	}
	return extractTxnMeta(request.Mutations)
}

func retainedResolutionParticipant(request *pb.Request, meta TxnMeta, evidence *pb.Request) (*pb.TimestampClaimParticipant, error) {
	if !proto.Equal(evidence.TimestampIntent, request.TimestampIntent) || !bytes.Equal(evidence.TimestampClaim, request.TimestampClaim) || evidence.TimestampClaimGroupId != request.TimestampClaimGroupId {
		return nil, errors.WithStack(ErrTSOClaimInvalid)
	}
	intent, err := canonicalTimestampIntent(evidence.TimestampIntent)
	if err != nil {
		return nil, err
	}
	if !proto.Equal(intent, evidence.TimestampIntent) || request.Ts != intent.ReadTimestamp || request.ObservedRouteVersion != intent.ObservedRouteVersion || !bytes.Equal(meta.PrimaryKey, intent.PrimaryKey) {
		return nil, errors.WithStack(ErrTSOClaimInvalid)
	}
	return claimIntentParticipant(intent, request.TimestampClaimGroupId)
}

// Canonical participant keys are sorted. Resolvers may omit retained metadata,
// but cannot introduce a new staged-read alias or ownership-fence exception.
func claimResolutionMetadataWithin(actual, retained [][]byte) bool {
	for _, key := range actual {
		if _, found := slices.BinarySearchFunc(retained, key, bytes.Compare); !found {
			return false
		}
	}
	return true
}

func claimIntentParticipant(intent *pb.TimestampClaimIntent, groupID uint64) (*pb.TimestampClaimParticipant, error) {
	for _, part := range intent.Participants {
		if part.GroupId == groupID {
			return part, nil
		}
	}
	return nil, errors.WithStack(ErrTSOClaimInvalid)
}

func validateClaimResolutionKeys(muts []*pb.Mutation, participant *pb.TimestampClaimParticipant) error {
	known := make(map[string]struct{}, len(participant.Mutations))
	for _, mut := range participant.Mutations {
		known[string(mut.Key)] = struct{}{}
	}
	for _, mut := range muts {
		if mut == nil || mut.Op != pb.Op_PUT || len(mut.Value) != 0 || mut.CommitTsValueOffset != 0 || mut.RequireAbsent || len(mut.ProtoReflect().GetUnknown()) != 0 {
			return errors.WithStack(ErrTSOClaimInvalid)
		}
		if _, exists := known[string(mut.Key)]; !exists {
			return errors.WithStack(ErrTSOClaimInvalid)
		}
	}
	return nil
}

func attachRetainedTxnClaim(ctx context.Context, st store.MVCCStore, request *pb.Request, primary []byte) error {
	evidence, _, err := readTxnClaimEvidence(ctx, st, primary, request.Ts)
	if errors.Is(err, store.ErrKeyNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	request.TimestampIntent, request.TimestampClaim, request.TimestampClaimGroupId = evidence.TimestampIntent, evidence.TimestampClaim, evidence.TimestampClaimGroupId
	request.ObservedRouteVersion = evidence.TimestampIntent.ObservedRouteVersion
	return nil
}
