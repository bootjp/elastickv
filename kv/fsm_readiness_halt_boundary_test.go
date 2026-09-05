package kv

import (
	"context"
	"testing"

	"github.com/bootjp/elastickv/distribution"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
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
