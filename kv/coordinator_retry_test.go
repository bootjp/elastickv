package kv

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bootjp/elastickv/internal/raftengine"
	pb "github.com/bootjp/elastickv/proto"
	cerrors "github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// stubLeaderEngine reports State=Leader and a fixed Leader address. It is
// the minimum surface Coordinate.Dispatch needs to take the local-dispatch
// path (IsLeader() true) without engaging real raft machinery. Methods
// beyond that surface return zero values / nil; Dispatch's retry loop
// never calls them, and refreshLeaseAfterDispatch skips its LeaseProvider
// branch because this stub does not implement that interface.
type stubLeaderEngine struct{}

func (stubLeaderEngine) Propose(context.Context, []byte) (*raftengine.ProposalResult, error) {
	return &raftengine.ProposalResult{}, nil
}
func (stubLeaderEngine) State() raftengine.State { return raftengine.StateLeader }
func (stubLeaderEngine) Leader() raftengine.LeaderInfo {
	return raftengine.LeaderInfo{ID: "self", Address: "127.0.0.1:0"}
}
func (stubLeaderEngine) VerifyLeader(context.Context) error               { return nil }
func (stubLeaderEngine) LinearizableRead(context.Context) (uint64, error) { return 0, nil }
func (stubLeaderEngine) Status() raftengine.Status {
	return raftengine.Status{State: raftengine.StateLeader}
}
func (stubLeaderEngine) Configuration(context.Context) (raftengine.Configuration, error) {
	return raftengine.Configuration{}, nil
}
func (stubLeaderEngine) Close() error { return nil }

// scriptedTransactional returns the first len(errs) entries from errs on
// successive Commit calls, then succeeds. It records every observed
// request so tests can assert on per-attempt StartTS values. Commit is
// called serially from the Dispatch retry loop, so a plain slice index
// (guarded by atomic.Int64 for race-detector friendliness) is enough.
type scriptedTransactional struct {
	errs     []error
	commits  atomic.Int64
	reqs     [][]*pb.Request
	onCommit func(call int64) // optional hook invoked inside Commit
}

func (s *scriptedTransactional) Commit(reqs []*pb.Request) (*TransactionResponse, error) {
	idx := s.commits.Add(1) - 1
	s.reqs = append(s.reqs, reqs)
	if s.onCommit != nil {
		s.onCommit(idx)
	}
	if int(idx) < len(s.errs) && s.errs[idx] != nil {
		return nil, s.errs[idx]
	}
	// idx is strictly non-negative (atomic.Add on an int64 that starts
	// at 0 and only ever increments), so the conversion to uint64 for
	// CommitIndex is safe. gosec G115 can't see that invariant, so
	// silence it inline.
	return &TransactionResponse{CommitIndex: uint64(idx + 1)}, nil //nolint:gosec
}

func (s *scriptedTransactional) Abort([]*pb.Request) (*TransactionResponse, error) {
	return &TransactionResponse{}, nil
}

func TestIsTransientLeaderError_Classification(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"kv.ErrLeaderNotFound", cerrors.WithStack(ErrLeaderNotFound), true},
		{"raftengine.ErrNotLeader", cerrors.WithStack(raftengine.ErrNotLeader), true},
		{"raftengine.ErrLeadershipLost", cerrors.WithStack(raftengine.ErrLeadershipLost), true},
		{"raftengine.ErrLeadershipTransferInProgress",
			cerrors.WithStack(raftengine.ErrLeadershipTransferInProgress), true},
		{"wire not-leader string", errors.New("not leader"), true},
		{"wire leader-not-found string", errors.New("leader not found"), true},
		{"wire leadership-lost string", errors.New("raft engine: leadership lost"), true},
		{"wire leadership-transfer string",
			errors.New("raft engine: leadership transfer in progress"), true},
		{"gRPC status wrapping not leader", fmt.Errorf("rpc error: code = Unknown desc = not leader"), true},
		{"gRPC status wrapping leadership lost",
			fmt.Errorf("rpc error: code = Unknown desc = raft engine: leadership lost"), true},
		{"unrelated error", errors.New("write conflict"), false},
		{"validation error", errors.New("invalid request"), false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tc.want, isTransientLeaderError(tc.err))
		})
	}
}

// newRetryCoordinate wires a Coordinate against stubLeaderEngine and the
// supplied scripted transaction manager. No engine registration happens,
// so the caller does not need to call Close.
func newRetryCoordinate(tx Transactional) *Coordinate {
	return &Coordinate{
		transactionManager: tx,
		engine:             stubLeaderEngine{},
		clock:              NewHLC(),
	}
}

func TestCoordinateDispatch_RetriesTransientLeaderError(t *testing.T) {
	t.Parallel()

	// First two Commit calls fail with transient leader signals (once via
	// the typed raftengine sentinel, once via the wire-level string that
	// gRPC would transport); the third succeeds. Dispatch must absorb
	// both and return success without leaking the transient error.
	tx := &scriptedTransactional{
		errs: []error{
			cerrors.WithStack(raftengine.ErrNotLeader),
			errors.New("leader not found"),
		},
	}
	c := newRetryCoordinate(tx)

	start := time.Now()
	resp, err := c.Dispatch(context.Background(), &OperationGroup[OP]{
		Elems: []*Elem[OP]{{Op: Put, Key: []byte("k"), Value: []byte("v")}},
	})
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.EqualValues(t, 3, tx.commits.Load())
	// Three attempts with a 25ms poll interval gives a realistic lower
	// bound of ~50ms; anything shorter would mean the loop skipped its
	// back-off.
	require.GreaterOrEqual(t, time.Since(start), 2*dispatchLeaderRetryInterval)
}

func TestCoordinateDispatch_NonTransientErrorSurfacesImmediately(t *testing.T) {
	t.Parallel()

	business := errors.New("write conflict")
	tx := &scriptedTransactional{errs: []error{business}}
	c := newRetryCoordinate(tx)

	_, err := c.Dispatch(context.Background(), &OperationGroup[OP]{
		Elems: []*Elem[OP]{{Op: Put, Key: []byte("k"), Value: []byte("v")}},
	})
	require.ErrorIs(t, err, business)
	// Exactly one attempt — the retry loop must not re-drive non-transient
	// errors.
	require.EqualValues(t, 1, tx.commits.Load())
}

func TestCoordinateDispatch_RefreshesStartTSOnRetry(t *testing.T) {
	t.Parallel()

	tx := &scriptedTransactional{
		errs: []error{cerrors.WithStack(raftengine.ErrNotLeader)},
	}
	c := newRetryCoordinate(tx)

	// Caller passes StartTS==0 — the contract is that the coordinator
	// mints the timestamp. Each retry MUST reset StartTS to 0 so
	// dispatchOnce re-mints against the post-churn clock; otherwise the
	// FSM's LatestCommitTS > startTS check could reject the retry
	// against a write that committed during the election window.
	reqs := &OperationGroup[OP]{
		IsTxn: true,
		Elems: []*Elem[OP]{{Op: Put, Key: []byte("k"), Value: []byte("v")}},
	}
	_, err := c.Dispatch(context.Background(), reqs)
	require.NoError(t, err)
	require.EqualValues(t, 2, tx.commits.Load())
	require.Len(t, tx.reqs, 2)
	require.Len(t, tx.reqs[0], 1)
	require.Len(t, tx.reqs[1], 1)

	ts0 := tx.reqs[0][0].Ts
	ts1 := tx.reqs[1][0].Ts
	require.Greater(t, ts0, uint64(0), "first attempt must carry a minted StartTS")
	require.Greater(t, ts1, ts0, "retry must mint a fresh, strictly greater StartTS")
}

func TestCoordinateDispatch_CtxCancelDuringRetrySurfaces(t *testing.T) {
	t.Parallel()

	// An always-transient failure keeps the retry loop alive; cancelling
	// the context during the back-off must surface ctx.Err() to the
	// caller rather than the transient leader error. gRPC clients rely
	// on this to tell "I gave up" from "cluster unavailable".
	ctx, cancel := context.WithCancel(context.Background())
	tx := &scriptedTransactional{
		errs: []error{
			cerrors.WithStack(raftengine.ErrNotLeader),
			cerrors.WithStack(raftengine.ErrNotLeader),
			cerrors.WithStack(raftengine.ErrNotLeader),
		},
		onCommit: func(call int64) {
			// Cancel after the first failed attempt so the next
			// back-off select sees ctx.Done().
			if call == 0 {
				cancel()
			}
		},
	}
	c := &Coordinate{
		transactionManager: tx,
		engine:             stubLeaderEngine{},
		clock:              NewHLC(),
	}

	_, err := c.Dispatch(ctx, &OperationGroup[OP]{
		Elems: []*Elem[OP]{{Op: Put, Key: []byte("k"), Value: []byte("v")}},
	})
	require.ErrorIs(t, err, context.Canceled)
}

func TestCoordinateDispatch_SuccessBeatsConcurrentCancel(t *testing.T) {
	t.Parallel()

	// Inverse race of TestCoordinateDispatch_CtxCancelDuringRetrySurfaces:
	// dispatchOnce returns SUCCESS on the same attempt where the caller
	// cancels ctx. The commit already landed in the FSM, so the loop
	// MUST report success rather than converting it into a
	// context.Canceled error — doing so would make a retrying client
	// re-issue the same write and risk duplicate effects for
	// non-idempotent operations. Pins the fix for the CodeRabbit-major
	// ordering bug in the retry loop (commit + cancel ordering).
	ctx, cancel := context.WithCancel(context.Background())
	tx := &scriptedTransactional{
		onCommit: func(int64) {
			// Cancel inside Commit, BEFORE Commit returns. Dispatch
			// will then observe a successful Commit but a cancelled
			// ctx on its first check.
			cancel()
		},
	}
	c := newRetryCoordinate(tx)

	resp, err := c.Dispatch(ctx, &OperationGroup[OP]{
		Elems: []*Elem[OP]{{Op: Put, Key: []byte("k"), Value: []byte("v")}},
	})
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Greater(t, resp.CommitIndex, uint64(0))
}

// TestIsTransientLeaderError_PinsRealSentinels asserts that the real
// .Error() texts of the upstream sentinels we classify as transient
// still pass through isTransientLeaderError. If a future rename of
// these messages drifts them out of leaderErrorPhrases' closed list,
// this test catches it at CI time rather than during a production
// re-election window. Pinning kv.ErrLeaderNotFound covers the
// wire-level phrase "leader not found"; raftengine.ErrNotLeader
// covers both the errors.Is sentinel path AND the string-fallback
// path ("not leader" substring).
//
// adapter.ErrNotLeader is not pinned here because adapter imports kv,
// which would create a test-time import cycle. A symmetric pin lives
// in the adapter test package alongside that sentinel.
func TestIsTransientLeaderError_PinsRealSentinels(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		err  error
	}{
		{"kv.ErrLeaderNotFound", ErrLeaderNotFound},
		{"raftengine.ErrNotLeader", raftengine.ErrNotLeader},
		{"raftengine.ErrLeadershipLost", raftengine.ErrLeadershipLost},
		{"raftengine.ErrLeadershipTransferInProgress",
			raftengine.ErrLeadershipTransferInProgress},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			require.True(t, isTransientLeaderError(tc.err),
				"sentinel %q (%q) no longer classified as transient — update leaderErrorPhrases or the classifier",
				tc.name, tc.err.Error())
		})
	}
}
