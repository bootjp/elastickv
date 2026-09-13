package kv

import (
	"context"
	"testing"

	"github.com/bootjp/elastickv/distribution"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

// readinessWriteFailureStore fails only the readiness persistence write.
type readinessWriteFailureStore struct {
	store.MVCCStore
	err error
}

func (s *readinessWriteFailureStore) ApplyTargetStagedReadiness(context.Context, store.TargetStagedReadinessState) error {
	return s.err
}

func (s *readinessWriteFailureStore) ApplyTargetStagedReadinessAt(context.Context, store.TargetStagedReadinessState, uint64) error {
	return s.err
}

func (s *readinessWriteFailureStore) MigrationTargetReadinessStates(context.Context) ([]store.TargetStagedReadinessState, error) {
	return nil, nil
}

// Persisting the readiness guard is a local Pebble write. A replica that
// returns an ordinary error advances past the entry without the source fence,
// write tracker or target guard its peers installed, then accepts user writes
// they reject.
func TestApplyTargetStagedReadinessHaltsOnLocalPersistFailure(t *testing.T) {
	t.Parallel()

	local := errors.New("pebble: commit failed")
	fsm := &kvFSM{store: &readinessWriteFailureStore{MVCCStore: store.NewMVCCStore(), err: local}}
	cmd, err := MarshalTargetStagedReadinessCommand(&pb.TargetStagedReadinessRequest{
		JobId:      9,
		RouteStart: []byte("a"),
		RouteEnd:   []byte("z"),
		Armed:      true,
	})
	require.NoError(t, err)

	halt := haltApplyOf(fsm.Apply(cmd))
	require.Error(t, halt, "a local readiness persistence failure must halt apply")
	require.True(t, errors.Is(halt, ErrTargetReadinessApply), "got %v", halt)
	require.ErrorIs(t, halt, local)
}

// The source-read fence returns ErrRouteCutoverPending too, but it is a
// replicated, deterministic verdict: every replica reads the same readiness
// states from its own store and rejects the same transactions. Halting on it
// would stop every source replica during a normal cutover.
//
// Both errors below come from the real verifiers and go through the real
// decision function, so this pins the boundary rather than restating it: an
// earlier version keyed the halt off ErrRouteCutoverPending itself, which
// turned the ordinary rejection into a cluster stop.
func TestApplyErrorResponseHaltsOnlyOnUnprovenLocalReadiness(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	fsm := newTargetReadinessFSM(t, distribution.RouteDescriptor{
		RouteID: 1, Start: []byte("a"), End: []byte("z"), GroupID: 1,
		State: distribution.RouteStateActive,
	})
	writer, ok := fsm.store.(store.MigrationTargetReadinessWriter)
	require.True(t, ok)
	require.NoError(t, writer.ApplyTargetStagedReadiness(ctx, store.TargetStagedReadinessState{
		JobID:                  9,
		RouteStart:             []byte("a"),
		RouteEnd:               []byte("z"),
		ExpectedCutoverVersion: 2,
		MigrationJobID:         9,
		MinWriteTSExclusive:    100,
		Armed:                  true,
		SourceWriteFence:       true,
		SourceReadFence:        true,
		RetentionPinTS:         50,
	}))

	fenceErr := fsm.verifySourceReadFenceForRange(ctx, []byte("b"), nextScanCursor([]byte("b")))
	require.ErrorIs(t, fenceErr, ErrRouteCutoverPending, "the armed fence must reject")
	require.NoError(t, haltApplyOf(applyErrorResponse(fenceErr)),
		"a replicated fence verdict is an ordinary rejection; halting stops every source replica during cutover")

	// The target-readiness proof reads the catalog watcher's current view, so a
	// replica cannot know whether its peers agree. That one must halt.
	//
	// A guard expecting a cutover version this replica's catalog view has not
	// reached is exactly the "peer may be ahead of me" case. It carries no fence
	// flags, because targetReadinessStatesSatisfied skips fenced states.
	require.NoError(t, writer.ApplyTargetStagedReadiness(ctx, store.TargetStagedReadinessState{
		JobID:                  10,
		RouteStart:             []byte("a"),
		RouteEnd:               []byte("z"),
		ExpectedCutoverVersion: 99,
		MigrationJobID:         10,
		MinWriteTSExclusive:    100,
		Armed:                  true,
	}))
	_, readinessErr := fsm.targetReadyRoutesForRouteRange(ctx, []byte("a"), []byte("z"))
	require.ErrorIs(t, readinessErr, ErrRouteCutoverPending)
	halt := haltApplyOf(applyErrorResponse(readinessErr))
	require.Error(t, halt, "an unprovable local readiness verdict must halt")
	require.True(t, errors.Is(halt, ErrTargetReadinessApply), "got %v", halt)
}

// readinessReadFailureStore fails only the readiness states READ.
type readinessReadFailureStore struct {
	store.MVCCStore
	err error
}

func (s *readinessReadFailureStore) MigrationTargetReadinessStates(context.Context) ([]store.TargetStagedReadinessState, error) {
	return nil, s.err
}

// The three sites below are the same rule seen from both directions, which is
// why they are pinned together: only a verdict that depended on process-local
// state halts, and every deterministic verdict stays an ordinary rejection.
// Classifying per call site rather than by that rule is what produced a halt
// that was simultaneously too wide (validation, cursors) and too narrow (local
// read failures).
func TestApplyHaltBoundaryFollowsProcessLocalityNotCallSite(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	local := errors.New("pebble: background error")

	t.Run("local readiness read failure halts", func(t *testing.T) {
		t.Parallel()
		fsm := &kvFSM{store: &readinessReadFailureStore{MVCCStore: store.NewMVCCStore(), err: local}}
		_, err := fsm.targetReadyRoutesForRouteRange(ctx, []byte("a"), []byte("z"))
		require.Error(t, err)
		require.True(t, errors.Is(err, errTargetReadinessUnproven),
			"a read that failed on this replica only must halt, not advance past the entry")
		halt := haltApplyOf(applyErrorResponse(err))
		require.Error(t, halt)
	})

	t.Run("malformed readiness request stays ordinary", func(t *testing.T) {
		t.Parallel()
		// track_writes without a retention pin: the store rejects it
		// deterministically, so every replica reaches the same verdict.
		fsm := &kvFSM{store: store.NewMVCCStore()}
		cmd, err := MarshalTargetStagedReadinessCommand(&pb.TargetStagedReadinessRequest{
			JobId:          9,
			MigrationJobId: 9,
			RouteStart:     []byte("a"),
			RouteEnd:       []byte("z"),
			TrackWrites:    true,
		})
		require.NoError(t, err)

		resp := fsm.Apply(cmd)
		require.NoError(t, haltApplyOf(resp),
			"one malformed internal RPC must not stop the raft group")
		respErr, ok := resp.(error)
		require.True(t, ok, "the request is still rejected, got %T", resp)
		require.True(t, errors.Is(respErr, store.ErrInvalidReadinessState), "got %v", respErr)
	})

	t.Run("invalid cleanup cursor stays ordinary", func(t *testing.T) {
		t.Parallel()
		require.True(t, isMigrationCleanupOrdinaryApplyError(errors.Mark(errors.New("x"), store.ErrInvalidExportCursor)),
			"a cursor verdict is decided from the request alone")
		require.False(t, isMigrationCleanupOrdinaryApplyError(local),
			"a local store failure is not an ordinary verdict")
	})
}

// TestApplyHaltsOnUnprovenReadinessInsideABatchedCommand closes the gap
// between the halt decision and the call site that has to honour it.
//
// The tests above exercise applyErrorResponse directly, so they pass whether or
// not Apply actually routes through it. Apply has two paths: a single request
// goes through applyRequest -> applyErrorResponse, while concurrent raw commits
// coalesced into one multi-request Raft command went through applyRequestErr
// and had every error recorded as a per-request result. An unproven local
// readiness verdict in a coalesced write therefore advanced this voter's
// applied index while its peers applied the write -- the committed-state
// divergence the halt exists to prevent, reachable only through batching.
func TestApplyHaltsOnUnprovenReadinessInsideABatchedCommand(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	fsm := newTargetReadinessFSM(t, distribution.RouteDescriptor{
		RouteID: 1, Start: []byte("a"), End: []byte("z"), GroupID: 1,
		State: distribution.RouteStateActive,
	})
	writer, ok := fsm.store.(store.MigrationTargetReadinessWriter)
	require.True(t, ok)

	// A guard expecting a cutover version this replica's catalog view has not
	// reached: the "a peer may be ahead of me" case, which cannot be proven
	// locally. No fence flags, because targetReadinessStatesSatisfied skips
	// fenced states.
	require.NoError(t, writer.ApplyTargetStagedReadiness(ctx, store.TargetStagedReadinessState{
		JobID:                  10,
		RouteStart:             []byte("a"),
		RouteEnd:               []byte("z"),
		ExpectedCutoverVersion: 99,
		MigrationJobID:         10,
		MinWriteTSExclusive:    100,
		Armed:                  true,
	}))

	// Confirm the precondition through the real verifier, so this test cannot
	// pass by failing for some unrelated reason.
	_, readinessErr := fsm.targetReadyRoutesForRouteRange(ctx, []byte("a"), []byte("z"))
	// cockroachdb's errors.Is, not require.ErrorIs: the sentinel is attached
	// with errors.Mark, which the stdlib traversal does not follow.
	require.True(t, errors.Is(readinessErr, errTargetReadinessUnproven),
		"precondition: the guard must produce an unprovable local verdict, got %v", readinessErr)

	cmd := &pb.RaftCommand{
		Requests: []*pb.Request{
			{Ts: 200, Mutations: []*pb.Mutation{{Op: pb.Op_PUT, Key: []byte("b"), Value: []byte("v1")}}},
			{Ts: 201, Mutations: []*pb.Mutation{{Op: pb.Op_PUT, Key: []byte("c"), Value: []byte("v2")}}},
		},
	}
	data, err := proto.Marshal(cmd)
	require.NoError(t, err)

	resp := fsm.Apply(data)
	_, perRequestResults := resp.(*fsmApplyResponse)
	require.False(t, perRequestResults,
		"a batched apply carrying an unproven readiness verdict must halt, not "+
			"return per-request results and advance the applied index")

	halt := haltApplyOf(resp)
	require.Error(t, halt, "the batch path must produce a halt response")
	require.True(t, errors.Is(halt, ErrTargetReadinessApply), "got %v", halt)
}
