package kv

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bootjp/elastickv/internal/raftengine"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/stretchr/testify/require"
)

type ownedProposalTestKey struct{}

type ownedContextProposer struct {
	*fakeProposer
	seen    context.Context
	failure error
	owned   bool
}

func (p *ownedContextProposer) RequiresOwnedProposalContext() bool { return p.owned }
func (p *ownedContextProposer) Propose(ctx context.Context, _ []byte) (*raftengine.ProposalResult, error) {
	p.seen = ctx
	p.calls.Add(1)
	if p.failure != nil {
		return nil, p.failure
	}
	return &raftengine.ProposalResult{CommitIndex: 9}, nil
}

func TestOwnedRawProposalPreservesReceiverContextThroughWrapper(t *testing.T) {
	t.Parallel()
	for _, wrap := range []bool{false, true} {
		p := &ownedContextProposer{fakeProposer: &fakeProposer{}, owned: true}
		var proposer raftengine.Proposer = p
		if wrap {
			var pointer atomic.Pointer[RaftPayloadWrapper]
			proposer = newDynamicWrappedProposer(p, &pointer)
		}
		manager := NewTransactionWithProposer(proposer)
		ctx, cancel := context.WithTimeout(context.WithValue(t.Context(), ownedProposalTestKey{}, "owned"), time.Second)
		response, err := manager.Commit(ctx, []*pb.Request{{Ts: 1, Mutations: []*pb.Mutation{{Key: []byte("key"), Value: []byte("value")}}}})
		cancel()
		require.NoError(t, err)
		require.Equal(t, uint64(9), response.CommitIndex)
		require.Equal(t, "owned", p.seen.Value(ownedProposalTestKey{}))
		_, hasDeadline := p.seen.Deadline()
		require.True(t, hasDeadline)
		manager.Close()
	}
}

func TestOwnedRawProposalPreservesUnknownOutcome(t *testing.T) {
	t.Parallel()
	p := &ownedContextProposer{fakeProposer: &fakeProposer{}, owned: true, failure: context.DeadlineExceeded}
	manager := NewTransactionWithProposer(p)
	defer manager.Close()
	response, err := manager.Commit(t.Context(), []*pb.Request{{Ts: 1, Mutations: []*pb.Mutation{{Key: []byte("key")}}}})
	require.Nil(t, response)
	require.ErrorIs(t, err, ErrTransactionOutcomeUnknown)
	require.ErrorIs(t, err, context.DeadlineExceeded)
}

func TestOwnedRawProposalHonorsClosedManager(t *testing.T) {
	t.Parallel()
	p := &ownedContextProposer{fakeProposer: &fakeProposer{}, owned: true}
	manager := NewTransactionWithProposer(p)
	manager.Close()
	_, err := manager.Commit(t.Context(), []*pb.Request{{Ts: 1, Mutations: []*pb.Mutation{{Key: []byte("key")}}}})
	require.ErrorIs(t, err, errShuttingDown)
	require.Zero(t, p.calls.Load())
}
