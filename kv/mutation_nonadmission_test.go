package kv

import (
	"context"
	"io"
	"testing"

	"github.com/bootjp/elastickv/distribution"
	"github.com/bootjp/elastickv/internal/testutil/grpctest"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

func TestNonadmissionProofCannotHideOtherOutcomes(t *testing.T) {
	request := markDataRequestNotSubmitted([]*pb.Request{{}}, ErrRouteCutoverPending)
	logical := &mutationNotAdmitted{cause: request}
	require.True(t, IsDataRequestNotSubmitted(errors.WithStack(request)))
	require.False(t, IsMutationNotAdmitted(request))
	require.True(t, IsMutationNotAdmitted(errors.Wrap(logical, "dispatch")))
	for _, err := range []error{
		errors.Join(logical, io.EOF), errors.Join(logical, ErrTransactionOutcomeUnknown),
		errors.CombineErrors(logical, io.EOF), errors.CombineErrors(logical, ErrTransactionOutcomeUnknown),
		&mutationNotAdmitted{cause: errors.Join(request, io.EOF)},
	} {
		require.False(t, IsMutationNotAdmitted(err))
		require.False(t, IsDataRequestNotSubmitted(err))
	}
}

func TestRawMigrationPreproposalRejectionCarriesRequestEvidence(t *testing.T) {
	f, _, _ := frontierAdmissionSplitFixture(t)
	proposer := &fakeProposer{}
	admission := &transactionMigrationAdmission{store: f.store, groupID: 3}
	index, results, err := admission.apply(t.Context(), proposer, []*pb.Request{{Ts: 200, Mutations: []*pb.Mutation{{Op: pb.Op_PUT, Key: []byte("n"), Value: []byte("v")}}}}, nil)
	require.NoError(t, err)
	require.Zero(t, index)
	require.Zero(t, proposer.calls.Load())
	require.Len(t, results, 1)
	require.ErrorIs(t, results[0], ErrRouteCutoverPending)
	require.True(t, IsDataRequestNotSubmitted(results[0]))
	require.False(t, IsMutationNotAdmitted(results[0]))
}

func TestRawNonadmissionRequiresWholeDispatchScope(t *testing.T) {
	for _, many := range []bool{false, true} {
		t.Run(map[bool]string{false: "single", true: "partial multi group"}[many], func(t *testing.T) {
			engine := distribution.NewEngine()
			engine.UpdateRoute([]byte(""), []byte("m"), 1)
			engine.UpdateRoute([]byte("m"), nil, 2)
			first := &recordingTransactional{}
			rejected := &recordingTransactional{errs: []error{markDataRequestNotSubmitted([]*pb.Request{{}}, ErrRouteCutoverPending)}}
			coord := NewShardedCoordinator(engine, map[uint64]*ShardGroup{1: {Txn: first}, 2: {Txn: rejected}}, 1, NewHLC(), nil)
			elems := []*Elem[OP]{{Op: Put, Key: []byte("z"), Value: []byte("v")}}
			if many {
				elems = append(elems, &Elem[OP]{Op: Put, Key: []byte("a"), Value: []byte("v")})
			}
			_, err := coord.Dispatch(context.Background(), &OperationGroup[OP]{Elems: elems})
			require.Error(t, err)
			require.Equal(t, !many, IsMutationNotAdmitted(err))
			if many {
				require.Len(t, first.requests, 1, "other group actually committed")
			}
		})
	}
}

func TestForwardRawPreSubmissionProofIsPhaseBound(t *testing.T) {
	wire := ForwardDecisionStatus(pb.ForwardFailurePhase_FORWARD_FAILURE_PHASE_ONE_PHASE_APPLY, pb.ForwardFailureReason_FORWARD_FAILURE_REASON_FENCE, pb.ForwardDecisionClass_FORWARD_DECISION_CLASS_REJECTED_BEFORE_SUBMISSION)
	err := classifyForwardRPCError(wire, []*pb.Request{{}})
	require.True(t, IsDataRequestNotSubmitted(err))
	require.False(t, IsMutationNotAdmitted(err))
	require.NotErrorIs(t, err, ErrTransactionOutcomeUnknown)
	for _, reqs := range [][]*pb.Request{{{}, {}}, {{IsTxn: true, Phase: pb.Phase_COMMIT}}, onePhaseForwardTxnRequests(17), onePhaseForwardTxnRequests(0)} {
		err = classifyForwardRPCError(wire, reqs)
		require.ErrorIs(t, err, ErrTransactionOutcomeUnknown)
		require.False(t, IsDataRequestNotSubmitted(err))
	}
}

func TestMixedForwardDecisionCannotProveRejection(t *testing.T) {
	for _, decision := range []pb.ForwardDecisionClass{pb.ForwardDecisionClass_FORWARD_DECISION_CLASS_REJECTED_BEFORE_SUBMISSION, pb.ForwardDecisionClass_FORWARD_DECISION_CLASS_DEFINITIVELY_ABORTED} {
		wire := ForwardDecisionStatus(pb.ForwardFailurePhase_FORWARD_FAILURE_PHASE_ONE_PHASE_APPLY, pb.ForwardFailureReason_FORWARD_FAILURE_REASON_FENCE, decision)
		for _, mixed := range []error{errors.Join(wire, io.EOF), errors.WithSecondaryError(wire, io.EOF), errors.WithStack(errors.Join(wire, io.EOF))} {
			err := classifyForwardRPCError(mixed, []*pb.Request{{}})
			require.ErrorIs(t, err, ErrTransactionOutcomeUnknown)
			require.False(t, IsDataRequestNotSubmitted(err))
			require.False(t, IsCertifiedOnePhaseConflict(err))
		}
	}
}

func TestLaterRawRejectionDoesNotEraseEarlierSubmission(t *testing.T) {
	for _, first := range []error{store.ErrWriteConflict, ErrComposed1VersionGCd} {
		engine := distribution.NewEngine()
		engine.UpdateRoute([]byte(""), nil, 1)
		tx := &recordingTransactional{errs: []error{first, markDataRequestNotSubmitted([]*pb.Request{{}}, ErrRouteCutoverPending)}}
		c := NewShardedCoordinator(engine, map[uint64]*ShardGroup{1: {Txn: tx}}, 1, NewHLC(), nil)
		_, err := c.Dispatch(t.Context(), &OperationGroup[OP]{Elems: []*Elem[OP]{{Op: Put, Key: []byte("z"), Value: []byte("v")}}})
		require.Error(t, err)
		require.Len(t, tx.requests, 2)
		require.False(t, IsMutationNotAdmitted(err), "a later rejection cannot erase an earlier submission")
	}
}

func TestPreSubmissionFenceReplyDoesNotOpenLeaderBreaker(t *testing.T) {
	wire := ForwardDecisionStatus(pb.ForwardFailurePhase_FORWARD_FAILURE_PHASE_ONE_PHASE_APPLY, pb.ForwardFailureReason_FORWARD_FAILURE_REASON_FENCE, pb.ForwardDecisionClass_FORWARD_DECISION_CLASS_REJECTED_BEFORE_SUBMISSION)
	service := &fakeInternal{err: wire, failFor: leaderProxyBreakerFailureThreshold + 2}
	address := grpctest.Serve(t, func(server *grpc.Server) { pb.RegisterInternalServer(server, service) })
	proxy := NewLeaderProxyWithEngine(newStubFollowerEngine(address))
	t.Cleanup(func() { require.NoError(t, proxy.Close()) })
	for i := 0; i < leaderProxyBreakerFailureThreshold+2; i++ {
		_, err := proxy.Commit(t.Context(), []*pb.Request{{}})
		require.True(t, IsDataRequestNotSubmitted(err))
		require.NotErrorIs(t, err, ErrLeaderProxyCircuitOpen)
	}
	require.Equal(t, leaderProxyBreakerFailureThreshold+2, service.callCount())
	require.Zero(t, proxy.forwardBreaker.failures)
	_, err := proxy.Commit(t.Context(), []*pb.Request{{Mutations: []*pb.Mutation{{Op: pb.Op_PUT, Key: []byte("unrelated"), Value: []byte("v")}}}})
	require.NoError(t, err, "an unrelated key on the same healthy leader must still be forwarded")
	require.Equal(t, leaderProxyBreakerFailureThreshold+3, service.callCount())
}

func TestRawMixedFailureNeverRetriesOrBecomesNotAdmitted(t *testing.T) {
	for _, reason := range []error{store.ErrWriteConflict, ErrComposed1VersionGCd} {
		for _, combine := range []func(error, error) error{func(a, b error) error { return errors.Join(a, b) }, errors.CombineErrors} {
			engine := distribution.NewEngine()
			engine.UpdateRoute([]byte(""), nil, 1)
			tx := &recordingTransactional{errs: []error{combine(reason, io.EOF), markDataRequestNotSubmitted([]*pb.Request{{}}, ErrRouteCutoverPending)}}
			c := NewShardedCoordinator(engine, map[uint64]*ShardGroup{1: {Txn: tx}}, 1, NewHLC(), nil)
			_, err := c.Dispatch(t.Context(), &OperationGroup[OP]{Elems: []*Elem[OP]{{Op: Put, Key: []byte("z"), Value: []byte("v")}}})
			require.Error(t, err)
			require.Len(t, tx.requests, 1, "a transport failure must not be erased by a later rejection")
			require.False(t, IsMutationNotAdmitted(err))
		}
	}
}

func TestRequestEvidenceRequiresAnIsolatedMigrationRejection(t *testing.T) {
	for _, err := range []error{io.EOF, errors.Join(ErrRouteWriteFenced, io.EOF), errors.CombineErrors(ErrRouteCutoverPending, io.EOF), errors.Join(ErrRouteCutoverPending, ErrTransactionOutcomeUnknown)} {
		marked := markDataRequestNotSubmitted([]*pb.Request{{}}, err)
		require.False(t, IsDataRequestNotSubmitted(marked))
		require.False(t, IsMigrationAdmissionRejection(err))
	}
}

type legacyAdmissionCause struct{ cause error }

func (e *legacyAdmissionCause) Error() string { return "legacy admission wrapper" }
func (e *legacyAdmissionCause) Cause() error  { return e.cause }
func TestLegacyCauseCannotHideMixedAdmissionEvidence(t *testing.T) {
	proof := &mutationNotAdmitted{cause: ErrRouteWriteFenced}
	wrapped := &legacyAdmissionCause{cause: errors.Join(proof, io.EOF)}
	require.False(t, IsMutationNotAdmitted(wrapped))
	require.False(t, IsMigrationAdmissionRejection(wrapped))
}

func TestRawPreRoutingRecheckKeepsRejectionScope(t *testing.T) {
	engine := distribution.NewEngine()
	require.NoError(t, engine.ApplySnapshot(distribution.CatalogSnapshot{Version: 1, Routes: []distribution.RouteDescriptor{{RouteID: 1, GroupID: 1, State: distribution.RouteStateWriteFenced}}}))
	tx := &recordingTransactional{}
	coord := NewShardedCoordinator(engine, map[uint64]*ShardGroup{1: {Txn: tx}}, 1, NewHLC(), nil)
	request := &OperationGroup[OP]{Elems: []*Elem[OP]{{Op: Put, Key: []byte("z"), Value: []byte("v")}}}
	_, shape, err := coord.dispatchRawWithFenceRetry(t.Context(), request)
	require.Empty(t, tx.requests)
	rejected, proven := classifyFirstRawRejection(err, shape, true)
	require.True(t, proven, "the first inner recheck rejected without dispatch")
	require.True(t, IsMutationNotAdmitted(rejected))
	_, proven = classifyFirstRawRejection(err, shape, false)
	require.False(t, proven, "an outer retry must not erase earlier work")
	shape.retried = true
	_, proven = classifyFirstRawRejection(err, shape, true)
	require.False(t, proven, "an inner retry must not erase earlier work")
}
