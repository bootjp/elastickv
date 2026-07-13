package kv

import (
	"encoding/binary"
	"testing"

	pb "github.com/bootjp/elastickv/proto"
	"github.com/stretchr/testify/require"
)

func TestStampMutationCommitTS(t *testing.T) {
	t.Parallel()

	value := make([]byte, 16)
	muts := []*pb.Mutation{{
		Op:                  pb.Op_PUT,
		Value:               value,
		CommitTsValueOffset: 4,
	}}

	require.NoError(t, StampMutationCommitTS(muts, 42))
	require.Equal(t, uint64(42), binary.BigEndian.Uint64(value[4:12]))
	require.Zero(t, muts[0].CommitTsValueOffset)
}

func TestStampMutationCommitTSRejectsInvalidPatch(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		mutation *pb.Mutation
		commit   uint64
		wantErr  error
	}{
		"zero commit": {
			mutation: &pb.Mutation{Op: pb.Op_PUT, Value: make([]byte, 8), CommitTsValueOffset: 1},
			wantErr:  ErrTxnCommitTSRequired,
		},
		"non put": {
			mutation: &pb.Mutation{Op: pb.Op_DEL, Value: make([]byte, 8), CommitTsValueOffset: 1},
			commit:   42,
			wantErr:  ErrInvalidRequest,
		},
		"out of range": {
			mutation: &pb.Mutation{Op: pb.Op_PUT, Value: make([]byte, 8), CommitTsValueOffset: 2},
			commit:   42,
			wantErr:  ErrInvalidRequest,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			err := StampMutationCommitTS([]*pb.Mutation{tc.mutation}, tc.commit)
			require.ErrorIs(t, err, tc.wantErr)
		})
	}
}
