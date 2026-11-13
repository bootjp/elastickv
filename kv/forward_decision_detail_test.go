package kv

import (
	"bytes"
	"fmt"
	"testing"

	pb "github.com/bootjp/elastickv/proto"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

func TestForwardDecisionStatusAllowedPairs(t *testing.T) {
	const (
		one      = pb.ForwardFailurePhase_FORWARD_FAILURE_PHASE_ONE_PHASE_APPLY
		prepare  = pb.ForwardFailurePhase_FORWARD_FAILURE_PHASE_PREPARE
		commit   = pb.ForwardFailurePhase_FORWARD_FAILURE_PHASE_COMMIT
		abort    = pb.ForwardFailurePhase_FORWARD_FAILURE_PHASE_ABORT
		conflict = pb.ForwardFailureReason_FORWARD_FAILURE_REASON_CONFLICT
		fence    = pb.ForwardFailureReason_FORWARD_FAILURE_REASON_FENCE
		leader   = pb.ForwardFailureReason_FORWARD_FAILURE_REASON_NO_LEADER
		broken   = pb.ForwardFailureReason_FORWARD_FAILURE_REASON_INTERRUPTED
		aborted  = pb.ForwardFailureReason_FORWARD_FAILURE_REASON_ABORTED
		reject   = pb.ForwardDecisionClass_FORWARD_DECISION_CLASS_REJECTED_BEFORE_SUBMISSION
		settled  = pb.ForwardDecisionClass_FORWARD_DECISION_CLASS_DEFINITIVELY_ABORTED
		unknown  = pb.ForwardDecisionClass_FORWARD_DECISION_CLASS_OUTCOME_UNKNOWN
	)
	allowed := map[pb.ForwardFailurePhase]map[pb.ForwardDecisionClass][]pb.ForwardFailureReason{
		one:     {reject: {leader, fence}, settled: {conflict, fence}, unknown: {conflict, fence, leader, broken}},
		prepare: {settled: {aborted}, unknown: {conflict, fence, leader, broken}},
		commit:  {settled: {aborted}, unknown: {conflict, fence, leader, broken}},
		abort:   {settled: {aborted}, unknown: {conflict, fence, leader, broken}},
	}
	phases := []pb.ForwardFailurePhase{0, one, prepare, commit, abort, 99}
	reasons := []pb.ForwardFailureReason{0, conflict, fence, leader, broken, aborted, 99}
	decisions := []pb.ForwardDecisionClass{0, reject, settled, unknown, 99}
	for _, phase := range phases {
		for _, reason := range reasons {
			for _, decision := range decisions {
				t.Run(fmt.Sprintf("%d_%d_%d", phase, reason, decision), func(t *testing.T) {
					want := false
					for _, allowedReason := range allowed[phase][decision] {
						want = want || reason == allowedReason
					}
					err := ForwardDecisionStatus(phase, reason, decision)
					gotReason, gotDecision, ok := DecodeForwardDecisionStatus(errors.WithStack(err), phase)
					require.Equal(t, want, ok)
					if want {
						require.Equal(t, reason, gotReason)
						require.Equal(t, decision, gotDecision)
						require.Len(t, status.Convert(err).Proto().Details, 1)
					} else {
						require.Equal(t, codes.Internal, status.Code(err))
					}
				})
			}
		}
	}
}

func TestDecodeForwardDecisionStatusRejectsMalformedEvidence(t *testing.T) {
	phase := pb.ForwardFailurePhase_FORWARD_FAILURE_PHASE_ONE_PHASE_APPLY
	reason := pb.ForwardFailureReason_FORWARD_FAILURE_REASON_CONFLICT
	decision := pb.ForwardDecisionClass_FORWARD_DECISION_CLASS_DEFINITIVELY_ABORTED
	base := &pb.ForwardFailureDetail{SchemaVersion: 1, Phase: phase, Reason: reason, Decision: decision}
	makeStatus := func(code codes.Code, details ...*anypb.Any) error {
		p := status.New(code, "safe").Proto()
		p.Details = details
		return status.FromProto(p).Err()
	}
	packed, err := anypb.New(base)
	require.NoError(t, err)
	old, err := anypb.New(&errdetails.ErrorInfo{Domain: forwardErrorDomain, Reason: forwardWriteConflictReason})
	require.NoError(t, err)
	mutate := func(change func(*pb.ForwardFailureDetail)) *anypb.Any {
		copy, ok := proto.Clone(base).(*pb.ForwardFailureDetail)
		require.True(t, ok)
		change(copy)
		raw, err := anypb.New(copy)
		require.NoError(t, err)
		return raw
	}
	malformed, ok := proto.Clone(packed).(*anypb.Any)
	require.True(t, ok)
	malformed.Value = []byte{0xff}
	cases := map[string]error{
		"absent":         status.Error(codes.Aborted, "write conflict"),
		"legacy only":    makeStatus(codes.Aborted, old),
		"duplicate":      makeStatus(codes.Aborted, packed, packed),
		"malformed":      makeStatus(codes.Aborted, malformed),
		"wrong code":     makeStatus(codes.Unavailable, packed),
		"old version":    makeStatus(codes.Aborted, mutate(func(d *pb.ForwardFailureDetail) { d.SchemaVersion = 0 })),
		"new version":    makeStatus(codes.Aborted, mutate(func(d *pb.ForwardFailureDetail) { d.SchemaVersion = 2 })),
		"unknown phase":  makeStatus(codes.Aborted, mutate(func(d *pb.ForwardFailureDetail) { d.Phase = 99 })),
		"unknown reason": makeStatus(codes.Aborted, mutate(func(d *pb.ForwardFailureDetail) { d.Reason = 99 })),
		"unknown class":  makeStatus(codes.Aborted, mutate(func(d *pb.ForwardFailureDetail) { d.Decision = 99 })),
		"invalid pair":   makeStatus(codes.Aborted, mutate(func(d *pb.ForwardFailureDetail) { d.Reason = pb.ForwardFailureReason_FORWARD_FAILURE_REASON_ABORTED })),
		"wrong decision": makeStatus(codes.Aborted, mutate(func(d *pb.ForwardFailureDetail) {
			d.Decision = pb.ForwardDecisionClass_FORWARD_DECISION_CLASS_OUTCOME_UNKNOWN
		})),
	}
	for name, err := range cases {
		t.Run(name, func(t *testing.T) {
			_, _, ok := DecodeForwardDecisionStatus(err, phase)
			require.False(t, ok)
		})
	}
	_, _, ok = DecodeForwardDecisionStatus(makeStatus(codes.Aborted, packed), pb.ForwardFailurePhase_FORWARD_FAILURE_PHASE_PREPARE)
	require.False(t, ok)
	_, _, ok = DecodeForwardDecisionStatus(makeStatus(codes.Aborted, packed), 99)
	require.False(t, ok)
	_, _, ok = DecodeForwardDecisionStatus(nil, phase)
	require.False(t, ok)
	gotReason, gotDecision, ok := DecodeForwardDecisionStatus(makeStatus(codes.Aborted, old, packed), phase)
	require.True(t, ok)
	require.Equal(t, reason, gotReason)
	require.Equal(t, decision, gotDecision)
}

func TestForwardDecisionStatusContainsNoKeyBytes(t *testing.T) {
	key := []byte{0xff, 0x00, 'n', 'o', 't', ' ', 'l', 'e', 'a', 'd', 'e', 'r'}
	err := ForwardDecisionStatus(
		pb.ForwardFailurePhase_FORWARD_FAILURE_PHASE_ONE_PHASE_APPLY,
		pb.ForwardFailureReason_FORWARD_FAILURE_REASON_CONFLICT,
		pb.ForwardDecisionClass_FORWARD_DECISION_CLASS_DEFINITIVELY_ABORTED,
	)
	wire, marshalErr := proto.Marshal(status.Convert(err).Proto())
	require.NoError(t, marshalErr)
	require.NotContains(t, err.Error(), "not leader")
	require.False(t, bytes.Contains(wire, key))
	require.NotContains(t, err.Error(), "write conflict")
}

func TestForwardDecisionStatusCannotLookDefinitiveToLegacyDecoder(t *testing.T) {
	t.Parallel()
	for _, reason := range []pb.ForwardFailureReason{
		pb.ForwardFailureReason_FORWARD_FAILURE_REASON_CONFLICT,
		pb.ForwardFailureReason_FORWARD_FAILURE_REASON_FENCE,
		pb.ForwardFailureReason_FORWARD_FAILURE_REASON_NO_LEADER,
	} {
		err := ForwardDecisionStatus(
			pb.ForwardFailurePhase_FORWARD_FAILURE_PHASE_ONE_PHASE_APPLY,
			reason,
			pb.ForwardDecisionClass_FORWARD_DECISION_CLASS_OUTCOME_UNKNOWN,
		)
		require.False(t, IsForwardWriteConflict(err))
		require.False(t, IsForwardTxnLocked(err))
		require.False(t, IsForwardNotLeader(err))
		require.False(t, IsForwardComposed1Violation(err))
		require.False(t, IsForwardComposed1VersionGCd(err))
		require.False(t, isTransientLeaderError(err))
	}
}
