package kv

import (
	"context"
	"sync/atomic"
	"testing"

	pb "github.com/bootjp/elastickv/proto"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

type testTTLInlineCapability struct{ active atomic.Bool }

func (c *testTTLInlineCapability) TTLInlineCapabilityActive() bool { return c.active.Load() }

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

func TestTransactionManagerAbsenceGuardNeedsActiveCapabilityAtProposal(t *testing.T) {
	capability := &testTTLInlineCapability{}
	proposer := &fakeProposer{}
	tm := NewTransactionWithProposer(proposer, WithTTLInlineCapabilityAdmission(capability))
	t.Cleanup(tm.Close)
	req := &pb.Request{IsTxn: true, Phase: pb.Phase_NONE, Ts: 10, Mutations: []*pb.Mutation{
		{Op: pb.Op_PUT, Key: []byte("k"), Value: []byte("v"), RequireAbsent: true},
	}}

	_, err := tm.Commit(t.Context(), []*pb.Request{req})
	require.ErrorIs(t, err, ErrTTLInlineCapabilityRequired)
	require.Zero(t, proposer.calls.Load())

	capability.active.Store(true)
	_, err = tm.Commit(t.Context(), []*pb.Request{req})
	require.NoError(t, err)
	require.EqualValues(t, 1, proposer.calls.Load())

	capability.active.Store(false)
	_, _, err = tm.applyRequests(t.Context(), []*pb.Request{req})
	require.ErrorIs(t, err, ErrTTLInlineCapabilityRequired)
	require.EqualValues(t, 1, proposer.calls.Load())
}

func TestTransactionManagerRejectsInvalidAbsenceShapeBeforeProposal(t *testing.T) {
	capability := &testTTLInlineCapability{}
	capability.active.Store(true)
	proposer := &fakeProposer{}
	tm := NewTransactionWithProposer(proposer, WithTTLInlineCapabilityAdmission(capability))
	t.Cleanup(tm.Close)
	for _, req := range []*pb.Request{
		{Phase: pb.Phase_NONE, Ts: 10, Mutations: []*pb.Mutation{{Op: pb.Op_PUT, Key: []byte("k"), RequireAbsent: true}}},
		{IsTxn: true, Phase: pb.Phase_PREPARE, Ts: 10, Mutations: []*pb.Mutation{{Op: pb.Op_PUT, Key: []byte("k"), RequireAbsent: true}}},
		{IsTxn: true, Phase: pb.Phase_NONE, Ts: 10, Mutations: []*pb.Mutation{{Op: pb.Op_DEL, Key: []byte("k"), RequireAbsent: true}}},
	} {
		_, err := tm.Commit(t.Context(), []*pb.Request{req})
		require.ErrorIs(t, err, ErrInvalidRequest)
	}
	require.Zero(t, proposer.calls.Load())
}
