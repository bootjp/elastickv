package kv

import (
	"io"
	"testing"

	"github.com/bootjp/elastickv/internal/raftengine"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestRawMigrationGuardRaceKeepsIndexedRejection(t *testing.T) {
	st := store.NewMVCCStore()
	request := deterministicRawRequest(0, "n")
	require.NoError(t, CheckMigrationRequest(t.Context(), st, 1, request), "the advisory check precedes the catalog transition")
	deterministicMigrationOwner(t, st)
	fsm := &kvFSM{store: st, routes: forbiddenApplyWatcher{}, shardGroupID: 1}
	rejectedAtApply := fsm.applyRequestErr(t.Context(), request)
	require.ErrorIs(t, rejectedAtApply, ErrRouteCutoverPending)
	present, err := st.ExistsAt(t.Context(), []byte("n"), 200)
	require.NoError(t, err)
	require.False(t, present, "the apply guard must reject before any data mutation")
	proposer := &fakeProposer{resp: &raftengine.ProposalResult{CommitIndex: 9, Response: rejectedAtApply}}
	transaction := NewTransactionWithProposer(proposer)
	_, err = transaction.Commit(t.Context(), []*pb.Request{request})
	var rejected *AppliedRequestRejection
	require.ErrorAs(t, err, &rejected, "the committed no-effect apply verdict must retain its index")
	require.Equal(t, uint64(9), rejected.CommitIndex())
	require.EqualValues(t, 1, proposer.calls.Load())
	require.ErrorIs(t, err, ErrRouteCutoverPending)
	require.NotErrorIs(t, err, ErrTransactionOutcomeUnknown)
}

func TestIndexedRawMigrationVerdictNeedsOneCompleteRequest(t *testing.T) {
	raw := &pb.Request{}
	guard := markMigrationApplyRejection(raw, ErrRouteCutoverPending)
	for _, tc := range []struct {
		name      string
		reqs      []*pb.Request
		index     uint64
		cause     error
		certified bool
	}{
		{"single indexed", []*pb.Request{raw}, 9, guard, true},
		{"missing index", []*pb.Request{raw}, 0, guard, false},
		{"multiple requests", []*pb.Request{raw, raw}, 9, guard, false},
		{"transaction", []*pb.Request{{IsTxn: true}}, 9, guard, false},
		{"unproven matching sentinel", []*pb.Request{raw}, 9, ErrRouteCutoverPending, false},
		{"lost response", []*pb.Request{raw}, 9, errors.Join(guard, io.EOF), false},
		{"hidden lost response", []*pb.Request{raw}, 9, errors.CombineErrors(guard, io.EOF), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			result := rawIndexedMigrationRejection(tc.reqs, tc.index, tc.cause)
			var indexed *AppliedRequestRejection
			require.Equal(t, tc.certified, errors.As(result, &indexed))
			if tc.certified {
				classified, ok := classifyFirstRawRejection(result, rawDispatchShape{groupCount: 1, requestCount: 1}, true)
				require.True(t, ok)
				require.True(t, IsMutationApplyRejected(classified))
				require.False(t, IsMutationNotAdmitted(classified))
				_, ok = classifyFirstRawRejection(result, rawDispatchShape{groupCount: 1, requestCount: 1, retried: true}, true)
				require.False(t, ok)
				_, ok = classifyFirstRawRejection(result, rawDispatchShape{groupCount: 1, requestCount: 1}, false)
				require.False(t, ok)
			}
		})
	}
	require.False(t, IsCertifiedOnePhaseConflict(errors.Join(&AppliedRequestRejection{commitIndex: 9, cause: guard}, io.EOF)))
	require.False(t, IsCertifiedOnePhaseConflict(errors.CombineErrors(&AppliedRequestRejection{commitIndex: 9, cause: guard}, io.EOF)))
}
