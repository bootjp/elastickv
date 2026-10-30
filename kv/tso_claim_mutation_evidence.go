package kv

import (
	"bytes"
	"crypto/sha256"

	pb "github.com/bootjp/elastickv/proto"
	"github.com/cockroachdb/errors"
)

func claimMutationEvidenceList(mutations []*pb.Mutation) ([]*pb.TimestampClaimMutationEvidence, error) {
	out := make([]*pb.TimestampClaimMutationEvidence, len(mutations))
	for index, mutation := range mutations {
		if mutation == nil {
			return nil, errors.WithStack(ErrTSOClaimInvalid)
		}
		evidence, err := claimMutationEvidence(mutation, mutation.CommitTsValueOffset)
		if err != nil {
			return nil, err
		}
		out[index] = evidence
	}
	return out, nil
}

func claimMutationEvidence(mutation *pb.Mutation, offset uint64) (*pb.TimestampClaimMutationEvidence, error) {
	if mutation == nil || len(mutation.ProtoReflect().GetUnknown()) != 0 {
		return nil, errors.WithStack(ErrTSOClaimInvalid)
	}
	if _, known := pb.Op_name[int32(mutation.Op)]; !known {
		return nil, errors.WithStack(ErrTSOClaimInvalid)
	}
	evidence := &pb.TimestampClaimMutationEvidence{Op: mutation.Op, Key: bytes.Clone(mutation.Key),
		ValueLength: uint64(len(mutation.Value)), CommitTsValueOffset: offset, RequireAbsent: mutation.RequireAbsent}
	if offset != 0 {
		evidence.CommitTsValueLength = commitTSPatchLen
	}
	// Validate the offset before slicing; subtraction avoids offset overflow.
	if err := validateClaimMutationPatch(evidence); err != nil {
		return nil, err
	}
	digest := normalizedClaimValueDigest(mutation.Value, offset)
	evidence.ValueSha256 = digest[:]
	return evidence, nil
}

func normalizedClaimValueDigest(value []byte, offset uint64) [sha256.Size]byte {
	if offset == 0 {
		return sha256.Sum256(value)
	}
	// Hash the original buffer in segments without cloning a potentially large
	// body. Only the declared fixed-width timestamp is normalized to zero.
	hash := sha256.New()
	_, _ = hash.Write(value[:offset])
	var zero [commitTSPatchLen]byte
	_, _ = hash.Write(zero[:])
	_, _ = hash.Write(value[offset+commitTSPatchLen:])
	var digest [sha256.Size]byte
	hash.Sum(digest[:0])
	return digest
}

func validateClaimMutationPatch(evidence *pb.TimestampClaimMutationEvidence) error {
	offset := evidence.CommitTsValueOffset
	if offset == 0 {
		if evidence.CommitTsValueLength != 0 {
			return errors.WithStack(ErrTSOClaimInvalid)
		}
		return nil
	}
	if evidence.Op != pb.Op_PUT || evidence.CommitTsValueLength != commitTSPatchLen || offset > evidence.ValueLength || evidence.ValueLength-offset < commitTSPatchLen {
		return errors.WithStack(ErrTSOClaimInvalid)
	}
	return nil
}

func validateClaimMutationEvidence(evidence *pb.TimestampClaimMutationEvidence) error {
	if evidence == nil || len(evidence.ValueSha256) != sha256.Size || len(evidence.ProtoReflect().GetUnknown()) != 0 {
		return errors.WithStack(ErrTSOClaimInvalid)
	}
	if _, known := pb.Op_name[int32(evidence.Op)]; !known {
		return errors.WithStack(ErrTSOClaimInvalid)
	}
	return validateClaimMutationPatch(evidence)
}

func claimReceivedMutationEvidence(actual *pb.Mutation, expected *pb.TimestampClaimMutationEvidence, timestamp uint64) (*pb.TimestampClaimMutationEvidence, error) {
	if err := normalizeClaimedPatch(actual, expected, timestamp); err != nil {
		return nil, err
	}
	if actual.Op != expected.Op || !bytes.Equal(actual.Key, expected.Key) || uint64(len(actual.Value)) != expected.ValueLength || actual.RequireAbsent != expected.RequireAbsent {
		return nil, errors.WithStack(ErrTSOClaimInvalid)
	}
	// A stamped request clears its patch offset. The signed manifest restores
	// that normalization boundary only after checking the actual stamped bytes.
	return claimMutationEvidence(actual, expected.CommitTsValueOffset)
}

func claimResolutionKeyMutations(mutations []*pb.TimestampClaimMutationEvidence) []*pb.Mutation {
	out := make([]*pb.Mutation, 0, len(mutations))
	for _, mutation := range mutations {
		out = append(out, &pb.Mutation{Op: pb.Op_PUT, Key: bytes.Clone(mutation.Key)})
	}
	return out
}
