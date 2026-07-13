package kv

import (
	"encoding/binary"

	pb "github.com/bootjp/elastickv/proto"
	"github.com/cockroachdb/errors"
)

const commitTSPatchLen = 8

// StampMutationCommitTS patches every mutation that embeds the resolved
// transaction commit timestamp in its value. Offset zero disables patching;
// non-zero offsets are byte offsets into the mutation value.
func StampMutationCommitTS(muts []*pb.Mutation, commitTS uint64) error {
	for _, mut := range muts {
		if mut == nil || mut.CommitTsValueOffset == 0 {
			continue
		}
		if commitTS == 0 {
			return errors.WithStack(ErrTxnCommitTSRequired)
		}
		if mut.Op != pb.Op_PUT {
			return errors.WithStack(ErrInvalidRequest)
		}
		offset := mut.CommitTsValueOffset
		if offset > uint64(len(mut.Value)) || uint64(len(mut.Value))-offset < commitTSPatchLen {
			return errors.WithStack(ErrInvalidRequest)
		}
		binary.BigEndian.PutUint64(mut.Value[offset:offset+commitTSPatchLen], commitTS)
		mut.CommitTsValueOffset = 0
	}
	return nil
}

func ValidateElemCommitTSPatches(elems []*Elem[OP], commitTS uint64) error {
	if commitTS == 0 {
		return rejectZeroCommitTSPatches(elems)
	}
	for _, elem := range elems {
		if err := validateElemCommitTSPatch(elem); err != nil {
			return err
		}
	}
	return nil
}

func rejectZeroCommitTSPatches(elems []*Elem[OP]) error {
	for _, elem := range elems {
		if elem != nil && elem.CommitTSValueOffset != 0 {
			return errors.WithStack(ErrTxnCommitTSRequired)
		}
	}
	return nil
}

func validateElemCommitTSPatch(elem *Elem[OP]) error {
	if elem == nil || elem.CommitTSValueOffset == 0 {
		return nil
	}
	if elem.Op != Put {
		return errors.WithStack(ErrInvalidRequest)
	}
	offset := elem.CommitTSValueOffset
	if offset > uint64(len(elem.Value)) || uint64(len(elem.Value))-offset < commitTSPatchLen {
		return errors.WithStack(ErrInvalidRequest)
	}
	return nil
}

func StampGroupedMutationCommitTS(grouped map[uint64][]*pb.Mutation, commitTS uint64) error {
	for _, muts := range grouped {
		if err := StampMutationCommitTS(muts, commitTS); err != nil {
			return err
		}
	}
	return nil
}
