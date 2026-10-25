package kv

import (
	"context"
	"testing"

	pb "github.com/bootjp/elastickv/proto"
	"github.com/stretchr/testify/require"
)

func TestApplyRequestsDoesNotCertifyFailedProposalAsAbort(t *testing.T) {
	t.Parallel()

	proposer := &fakeProposer{err: context.DeadlineExceeded}
	_, _, err := applyRequests(context.Background(), proposer, []*pb.Request{{
		Mutations: []*pb.Mutation{{Key: []byte("k"), Value: []byte("v")}},
	}}, nil)
	require.ErrorIs(t, err, ErrTransactionOutcomeUnknown)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.EqualValues(t, 1, proposer.calls.Load())
}
