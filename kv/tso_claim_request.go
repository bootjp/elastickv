package kv

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"sort"

	pb "github.com/bootjp/elastickv/proto"
	"github.com/cockroachdb/errors"
	"google.golang.org/protobuf/proto"
)

const maxTSOClaimIntentBytes = 64 << 20

type TSOClaimIdentityProvider interface{ TimestampClaimIdentity() *TSOClaimIdentity }

func TimestampClaimsRequired(allocator TimestampAllocator) bool {
	provider, ok := allocator.(TSOClaimIdentityProvider)
	return ok && provider.TimestampClaimIdentity() != nil
}

func timestampClaimIdentity(allocator any) *TSOClaimIdentity {
	if provider, ok := allocator.(TSOClaimIdentityProvider); ok {
		return provider.TimestampClaimIdentity()
	}
	return nil
}

func EnsureRequestTimestampIntent(request *pb.Request) error {
	if request == nil || !validClaimCreationPhase(request.IsTxn, request.Phase) {
		return errors.WithStack(ErrTSOClaimInvalid)
	}
	if request.TimestampIntent != nil {
		return ValidateRequestTimestampIntent(request)
	}
	operation := make([]byte, len(TSOOperationID{}))
	if _, err := rand.Read(operation); err != nil {
		return errors.Wrap(err, "create timestamp operation ID")
	}
	intent := &pb.TimestampClaimIntent{
		OperationId: operation, IsTxn: request.IsTxn, InitialPhase: request.Phase, ManifestVersion: TSOClaimProtocolVersion,
		ReadKeys: request.ReadKeys, ObservedRouteVersion: request.ObservedRouteVersion,
		WriteFenceBypassKeys: request.WriteFenceBypassKeys,
	}
	muts := request.Mutations
	if request.IsTxn {
		meta, data, err := extractTxnMeta(request.Mutations)
		if err != nil {
			return err
		}
		if err := validateClaimCreationMeta(request.Phase, meta); err != nil {
			return err
		}
		intent.ReadTimestamp, intent.PrimaryKey, intent.PreviousCommitTimestamp = request.Ts, meta.PrimaryKey, meta.PrevCommitTS
		intent.LockTtlMs = meta.LockTTLms
		muts = data
	}
	evidence, err := claimMutationEvidenceList(muts)
	if err != nil {
		return err
	}
	intent.Participants = []*pb.TimestampClaimParticipant{{GroupId: request.TimestampClaimGroupId,
		Mutations: evidence, ReadKeys: request.ReadKeys, WriteFenceBypassKeys: request.WriteFenceBypassKeys}}
	canonical, err := canonicalTimestampIntent(intent)
	if err != nil {
		return err
	}
	request.TimestampIntent = canonical
	return nil
}

func canonicalTimestampIntent(intent *pb.TimestampClaimIntent) (*pb.TimestampClaimIntent, error) {
	if intent == nil || intent.ManifestVersion != TSOClaimProtocolVersion || len(intent.OperationId) != len(TSOOperationID{}) || proto.Size(intent) > maxTSOClaimIntentBytes || len(intent.ProtoReflect().GetUnknown()) != 0 {
		return nil, errors.WithStack(ErrTSOClaimInvalid)
	}
	if err := validateClaimIntentMode(intent); err != nil {
		return nil, err
	}
	return canonicalTimestampIntentContents(intent)
}

func canonicalTimestampIntentContents(intent *pb.TimestampClaimIntent) (*pb.TimestampClaimIntent, error) {
	out := proto.CloneOf(intent)
	if err := canonicalClaimParticipants(out); err != nil {
		return nil, err
	}
	out.ReadKeys = canonicalClaimKeys(out.ReadKeys)
	out.WriteFenceBypassKeys = canonicalClaimKeys(out.WriteFenceBypassKeys)
	return out, nil
}

func canonicalClaimKeys(keys [][]byte) [][]byte {
	unique := make(map[string]struct{}, len(keys))
	for _, key := range keys {
		unique[string(key)] = struct{}{}
	}
	ordered := make([]string, 0, len(unique))
	for key := range unique {
		ordered = append(ordered, key)
	}
	sort.Strings(ordered)
	out := make([][]byte, 0, len(ordered))
	for _, key := range ordered {
		out = append(out, []byte(key))
	}
	return out
}

func timestampIntentDigest(intent *pb.TimestampClaimIntent) (TSOIntentDigest, error) {
	canonical, err := canonicalTimestampIntent(intent)
	if err != nil {
		return TSOIntentDigest{}, err
	}
	raw, err := (proto.MarshalOptions{Deterministic: true}).Marshal(canonical)
	if err != nil {
		return TSOIntentDigest{}, errors.Wrap(err, "encode canonical timestamp intent")
	}
	return TSOIntentDigest(sha256.Sum256(raw)), nil
}

func ValidateRequestTimestampIntent(request *pb.Request) error {
	if request == nil || len(request.ProtoReflect().GetUnknown()) != 0 {
		return errors.WithStack(ErrTSOClaimInvalid)
	}
	if err := validateClaimMutationHeaders(request.Mutations); err != nil {
		return err
	}
	intent, err := canonicalTimestampIntent(request.GetTimestampIntent())
	if err != nil {
		return err
	}
	if !proto.Equal(intent, request.TimestampIntent) {
		return errors.WithStack(ErrTSOClaimInvalid)
	}
	if !claimRequestModeMatches(request, intent) || request.ObservedRouteVersion != intent.ObservedRouteVersion {
		return errors.WithStack(ErrTSOClaimInvalid)
	}
	muts := request.Mutations
	if request.IsTxn {
		data, err := validateClaimTransactionMeta(request, intent)
		if err != nil {
			return err
		}
		muts = data
	}
	return validateTimestampIntentProjection(intent, request, muts)
}

func validateClaimMutationHeaders(mutations []*pb.Mutation) error {
	for _, mutation := range mutations {
		if mutation == nil || len(mutation.ProtoReflect().GetUnknown()) != 0 {
			return errors.WithStack(ErrTSOClaimInvalid)
		}
	}
	return nil
}

func validateClaimTransactionMeta(request *pb.Request, intent *pb.TimestampClaimIntent) ([]*pb.Mutation, error) {
	meta, data, err := extractTxnMeta(request.Mutations)
	if err != nil {
		return nil, err
	}
	if err := validateClaimCreationMeta(request.Phase, meta); err != nil {
		return nil, err
	}
	if request.Ts != intent.ReadTimestamp || !bytes.Equal(meta.PrimaryKey, intent.PrimaryKey) || meta.PrevCommitTS != intent.PreviousCommitTimestamp || meta.LockTTLms != intent.LockTtlMs {
		return nil, errors.WithStack(ErrTSOClaimInvalid)
	}
	return data, nil
}

func validateTimestampIntentProjection(intent *pb.TimestampClaimIntent, request *pb.Request, muts []*pb.Mutation) error {
	if request.Phase == pb.Phase_COMMIT || request.Phase == pb.Phase_ABORT {
		return errors.Wrap(ErrTSOClaimInvalid, "resolution requires durable prepared claim evidence")
	}
	for _, expected := range intent.Participants {
		if expected.GroupId != request.TimestampClaimGroupId {
			continue
		}
		selected, err := selectClaimPrefixPayload(expected, request)
		if err != nil {
			return err
		}
		return validateClaimParticipantRequest(selected, request, muts)
	}
	return errors.WithStack(ErrTSOClaimInvalid)
}

func selectClaimPrefixPayload(expected *pb.TimestampClaimParticipant, request *pb.Request) (*pb.TimestampClaimParticipant, error) {
	indices := request.TimestampClaimPrefixIndices
	if len(indices) == 0 {
		return expected, nil
	}
	if request.IsTxn || request.Phase != pb.Phase_NONE || len(indices) != 1 || uint64(indices[0]) >= uint64(len(expected.Mutations)) {
		return nil, errors.WithStack(ErrTSOClaimInvalid)
	}
	for _, mutation := range expected.Mutations {
		if mutation.Op != pb.Op_DEL_PREFIX {
			return nil, errors.WithStack(ErrTSOClaimInvalid)
		}
	}
	selected := proto.CloneOf(expected)
	selected.Mutations = []*pb.TimestampClaimMutationEvidence{selected.Mutations[indices[0]]}
	return selected, nil
}

func validateClaimParticipantRequest(expected *pb.TimestampClaimParticipant, request *pb.Request, muts []*pb.Mutation) error {
	if len(muts) != len(expected.Mutations) {
		return errors.WithStack(ErrTSOClaimInvalid)
	}
	actual := &pb.TimestampClaimParticipant{GroupId: request.TimestampClaimGroupId, ReadKeys: request.ReadKeys, WriteFenceBypassKeys: request.WriteFenceBypassKeys}
	var claimed TSORequestClaim
	if len(request.TimestampClaim) != 0 {
		if err := decodeClaimWire(request.TimestampClaim, "TSOPRF01", &claimed); err != nil {
			return err
		}
	}
	for index, mut := range muts {
		evidence, err := claimReceivedMutationEvidence(mut, expected.Mutations[index], claimed.Proof.Record.Timestamp)
		if err != nil {
			return err
		}
		actual.Mutations = append(actual.Mutations, evidence)
	}
	canonical, err := canonicalClaimParticipant(actual)
	if err != nil {
		return err
	}
	if !proto.Equal(canonical, expected) {
		return errors.WithStack(ErrTSOClaimInvalid)
	}
	return nil
}

func normalizeClaimedPatch(actual *pb.Mutation, expected *pb.TimestampClaimMutationEvidence, timestamp uint64) error {
	if actual == nil || expected == nil {
		return errors.WithStack(ErrTSOClaimInvalid)
	}
	offset := expected.CommitTsValueOffset
	if offset == 0 {
		if actual.CommitTsValueOffset != 0 {
			return errors.WithStack(ErrTSOClaimInvalid)
		}
		return nil
	}
	if err := validateClaimPatchOffset(actual.CommitTsValueOffset, offset, timestamp); err != nil {
		return err
	}
	if offset > uint64(len(actual.Value)) || uint64(len(actual.Value))-offset < commitTSPatchLen {
		return errors.WithStack(ErrTSOClaimInvalid)
	}
	if timestamp != 0 && binary.BigEndian.Uint64(actual.Value[offset:offset+commitTSPatchLen]) != timestamp {
		return errors.WithStack(ErrTSOClaimInvalid)
	}
	return nil
}

func validateClaimPatchOffset(actual, expected, timestamp uint64) error {
	if actual != 0 && actual != expected {
		return errors.WithStack(ErrTSOClaimInvalid)
	}
	if timestamp == 0 && actual != expected {
		return errors.WithStack(ErrTSOClaimInvalid)
	}
	return nil
}

func IssueRequestTimestampClaim(ctx context.Context, allocator TimestampAllocator, request *pb.Request) error {
	identity := timestampClaimIdentity(allocator)
	issuer, ok := allocator.(TSOClaimIssuer)
	if identity == nil || !ok {
		return errors.WithStack(ErrTSOClaimAuthorityRequired)
	}
	if err := EnsureRequestTimestampIntent(request); err != nil {
		return err
	}
	digest, err := timestampIntentDigest(request.TimestampIntent)
	if err != nil {
		return err
	}
	var operation TSOOperationID
	copy(operation[:], request.TimestampIntent.OperationId)
	binding := TSOClaimBinding{Owner: identity.owner(), Operation: operation, Intent: digest, Role: TSOClaimCommit}
	signed, credential, err := identity.signClaim(binding, request.TimestampIntent.ReadTimestamp, defaultTSOClaimGrantSize)
	if err != nil {
		return err
	}
	record, err := issuer.ClaimTimestamp(ctx, signed)
	if err != nil {
		return errors.Wrap(err, "issue request timestamp claim")
	}
	proof := TSORequestClaim{Signed: signed, Proof: TSOClaimProof{Record: record, Credential: credential}}
	raw, err := EncodeRequestTimestampClaim(proof)
	if err != nil {
		return err
	}
	if err := stampClaimPersistenceTimestamp(request, record.Timestamp); err != nil {
		return err
	}
	request.TimestampClaim = raw
	return nil
}

func stampClaimPersistenceTimestamp(request *pb.Request, timestamp uint64) error {
	if request == nil || !validClaimCreationPhase(request.IsTxn, request.Phase) {
		return errors.WithStack(ErrTSOClaimInvalid)
	}
	if intent := request.TimestampIntent; intent != nil && !claimRequestModeMatches(request, intent) {
		return errors.WithStack(ErrTSOClaimInvalid)
	}
	if !request.IsTxn {
		request.Ts = timestamp
		return StampMutationCommitTS(request.Mutations, timestamp)
	}
	meta, muts, err := extractTxnMeta(request.Mutations)
	if err != nil {
		return err
	}
	if err := validateClaimCreationMeta(request.Phase, meta); err != nil {
		return err
	}
	if request.Phase == pb.Phase_PREPARE {
		meta.PrepareCommitTS = timestamp
	} else {
		meta.CommitTS = timestamp
	}
	if err := StampMutationCommitTS(muts, timestamp); err != nil {
		return err
	}
	request.Mutations[0].Value = EncodeTxnMeta(meta)
	return nil
}

func ValidateRequestTimestampClaim(ctx context.Context, allocator TimestampAllocator, request *pb.Request) error {
	if err := ValidateRequestTimestampIntent(request); err != nil {
		return err
	}
	claim, err := validateTimestampIntentClaim(request)
	if err != nil {
		return err
	}
	return validateRequestClaimRecord(ctx, allocator, request, claim)
}

// Also used for resolution of an already validated, durable PREPARE capsule.
// This verifies the signed manifest, not an arbitrary request's payload.
func validateTimestampIntentClaim(request *pb.Request) (TSORequestClaim, error) {
	claim, err := DecodeRequestTimestampClaim(request.TimestampClaim)
	if err != nil {
		return claim, err
	}
	digest, err := timestampIntentDigest(request.TimestampIntent)
	if err != nil {
		return claim, err
	}
	var operation TSOOperationID
	copy(operation[:], request.TimestampIntent.OperationId)
	expected := TSOClaimBinding{Owner: TSOClaimOwner(sha256.Sum256(claim.Signed.PublicKey[:])), Operation: operation, Intent: digest, Role: TSOClaimCommit}
	if err := validateRequestClaimBinding(claim, expected, request.TimestampIntent.ReadTimestamp); err != nil {
		return claim, err
	}
	return claim, nil
}

func validateRequestClaimBinding(claim TSORequestClaim, expected TSOClaimBinding, minimum uint64) error {
	if !claim.Proof.matches(expected, TSOClaimOnePhase) || claim.Signed.Minimum != minimum {
		return errors.WithStack(ErrTSOClaimInvalid)
	}
	return nil
}

func validateRequestClaimRecord(ctx context.Context, allocator TimestampAllocator, request *pb.Request, claim TSORequestClaim) error {
	timestamp, err := prepareCreationPersistenceTimestamp(request)
	if err != nil {
		return err
	}
	if timestamp != claim.Proof.Record.Timestamp {
		return errors.WithStack(ErrTSOClaimInvalid)
	}
	return lookupRequestClaimRecord(ctx, allocator, claim)
}

func lookupRequestClaimRecord(ctx context.Context, allocator TimestampAllocator, claim TSORequestClaim) error {
	issuer, ok := allocator.(TSOClaimIssuer)
	if !ok {
		return errors.WithStack(ErrTSOProtocolUnsupported)
	}
	record, err := issuer.LookupTimestampClaim(ctx, claim.Signed)
	if err != nil {
		return errors.Wrap(err, "lookup request timestamp claim")
	}
	if record != claim.Proof.Record {
		return errors.WithStack(ErrTSOClaimInvalid)
	}
	return nil
}

const defaultTSOClaimGrantSize = 256
