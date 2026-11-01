package kv

import (
	"context"
	"testing"
	"time"

	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

type phaseDControlProposer struct {
	raftengine.Proposer
	admin func(context.Context, []byte) (*raftengine.ProposalResult, error)
}

func (p phaseDControlProposer) ProposeAdmin(ctx context.Context, payload []byte) (*raftengine.ProposalResult, error) {
	return p.admin(ctx, payload)
}

type phaseDControlChildKey struct{}

func TestPhaseDControlAdmissionPreservesDirectCallerLineage(t *testing.T) {
	parent, cancel := context.WithTimeout(context.WithValue(t.Context(), operationHandleKey{}, "original-allocator-owner"), time.Second)
	defer cancel()
	var finished, proposals int
	hook := func(ctx context.Context, group uint64, purpose AdmissionPurpose) (context.Context, func(error) error, error) {
		require.EqualValues(t, 17, group)
		require.Equal(t, AdmissionControl, purpose)
		require.Equal(t, "original-allocator-owner", ctx.Value(operationHandleKey{}))
		return context.WithValue(ctx, phaseDControlChildKey{}, "control-child"), func(err error) error { finished++; return err }, nil
	}
	proposer := phaseDControlProposer{admin: func(ctx context.Context, payload []byte) (*raftengine.ProposalResult, error) {
		proposals++
		require.Equal(t, "original-allocator-owner", ctx.Value(operationHandleKey{}))
		require.Equal(t, "control-child", ctx.Value(phaseDControlChildKey{}))
		deadline, ok := ctx.Deadline()
		require.True(t, ok)
		parentDeadline, _ := parent.Deadline()
		require.Equal(t, parentDeadline, deadline)
		require.Equal(t, EncodePrepareEvidenceSeal(), payload)
		return &raftengine.ProposalResult{CommitIndex: 7}, nil
	}}
	shards := &ShardStore{groups: map[uint64]*ShardGroup{17: {Engine: newStubLeaderEngine(), Store: &operationLockScanStore{}, proposer: proposer}}, operationAdmission: hook}
	require.NoError(t, shards.SealPhaseDCreation(parent))
	require.Equal(t, 1, proposals)
	require.Equal(t, 1, finished)
}

func TestPhaseDControlAdmissionRejectsBeforeProposer(t *testing.T) {
	denied := errors.New("control closed")
	shards := &ShardStore{operationAdmission: func(context.Context, uint64, AdmissionPurpose) (context.Context, func(error) error, error) {
		return nil, nil, denied
	}}
	proposer := phaseDControlProposer{admin: func(context.Context, []byte) (*raftengine.ProposalResult, error) {
		t.Fatal("denied operation reached native proposer")
		return nil, nil
	}}
	result, err := shards.proposePhaseDControl(t.Context(), 17, proposer, EncodePrepareEvidenceSeal())
	require.Nil(t, result)
	require.ErrorIs(t, err, denied)
}

func TestPhaseDControlAdmissionRetainsApplyAndUnknownOutcomes(t *testing.T) {
	revoked := errors.New("control owner revoked")
	applyFailed := errors.New("FSM rejected control")
	for _, tc := range []struct {
		name                   string
		result                 *raftengine.ProposalResult
		proposalErr, errorWant error
	}{
		{"no receipt", nil, nil, ErrTSOProtocolUnsupported},
		{"apply failure", &raftengine.ProposalResult{CommitIndex: 7, Response: applyFailed}, nil, applyFailed},
		{"proposal unknown", nil, ErrTransactionOutcomeUnknown, ErrTransactionOutcomeUnknown},
		{"success then revoked", &raftengine.ProposalResult{CommitIndex: 7}, nil, ErrTransactionOutcomeUnknown},
	} {
		t.Run(tc.name, func(t *testing.T) {
			finished := 0
			shards := &ShardStore{operationAdmission: func(ctx context.Context, _ uint64, purpose AdmissionPurpose) (context.Context, func(error) error, error) {
				require.Equal(t, AdmissionControl, purpose)
				return ctx, func(error) error { finished++; return revoked }, nil
			}}
			proposer := phaseDControlProposer{admin: func(context.Context, []byte) (*raftengine.ProposalResult, error) { return tc.result, tc.proposalErr }}
			result, err := shards.proposePhaseDControl(t.Context(), 17, proposer, EncodePrepareEvidenceSeal())
			require.Nil(t, result)
			require.ErrorIs(t, err, tc.errorWant)
			require.ErrorIs(t, err, revoked)
			require.Equal(t, 1, finished)
		})
	}
}
