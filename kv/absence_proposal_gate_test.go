package kv

import (
	"context"
	"testing"

	pb "github.com/bootjp/elastickv/proto"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// Pre-capability binaries decode require_absent as an unknown field and apply
// the PUT, so an entry carrying it must never reach the log before every
// member is fenced onto the capability. Internal.Forward reaches Commit with
// caller-supplied mutations, so the refusal has to happen before proposing.
func TestTransactionManagerRefusesAbsenceGuardBeforeProposal(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		req  *pb.Request
	}{
		{
			name: "one-phase txn",
			req: &pb.Request{IsTxn: true, Phase: pb.Phase_NONE, Ts: 10, Mutations: []*pb.Mutation{
				{Op: pb.Op_PUT, Key: []byte("k"), Value: []byte("v"), RequireAbsent: true},
			}},
		},
		{
			name: "raw",
			req: &pb.Request{Phase: pb.Phase_NONE, Ts: 10, Mutations: []*pb.Mutation{
				{Op: pb.Op_PUT, Key: []byte("k"), Value: []byte("v"), RequireAbsent: true},
			}},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			proposer := &fakeProposer{}
			tm := NewTransactionWithProposer(proposer)
			t.Cleanup(tm.Close)

			_, err := tm.Commit(context.Background(), []*pb.Request{tc.req})
			require.True(t, errors.Is(err, ErrTTLInlineCapabilityRequired), "got %v", err)
			require.Zero(t, proposer.calls.Load())
			require.Zero(t, proposer.adminCalls.Load())
		})
	}
}
