package kv

import (
	"context"
	"testing"

	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// cleanupFailureStore fails CleanupVersions and ClearMigrationState with a
// caller-supplied error so apply's halt-vs-ordinary classification can be
// driven directly.
type cleanupFailureStore struct {
	store.MVCCStore
	err error
}

func (s *cleanupFailureStore) CleanupVersions(context.Context, store.CleanupVersionsOptions) (store.CleanupVersionsResult, error) {
	return store.CleanupVersionsResult{}, s.err
}

func (s *cleanupFailureStore) ClearMigrationState(context.Context, uint64, uint64) error {
	return s.err
}

func cleanupCommand(t *testing.T, mode pb.MigrationCleanupMode) []byte {
	t.Helper()
	cmd, err := MarshalMigrationCleanupCommand(&pb.CleanupMigrationRequest{
		JobId:      7,
		Mode:       mode,
		RouteStart: []byte("a"),
		RouteEnd:   []byte("z"),
	})
	require.NoError(t, err)
	return cmd
}

// A replica-local store failure during cleanup must halt apply. Returning an
// ordinary error lets the Raft engine advance the applied index, so this voter
// permanently skips a committed cleanup while healthy voters delete the
// versions -- divergent committed state that no retry repairs. Import, promote
// and retire already halt on their equivalents.
func TestApplyMigrationCleanupHaltsOnLocalStoreFailure(t *testing.T) {
	t.Parallel()

	local := errors.New("pebble: commit failed")
	for _, tc := range []struct {
		name string
		mode pb.MigrationCleanupMode
	}{
		{"versions", pb.MigrationCleanupMode_MIGRATION_CLEANUP_MODE_UNSPECIFIED},
		{"metadata", pb.MigrationCleanupMode_MIGRATION_CLEANUP_MODE_METADATA},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			fsm := &kvFSM{store: &cleanupFailureStore{MVCCStore: store.NewMVCCStore(), err: local}}
			halt := haltApplyOf(fsm.Apply(cleanupCommand(t, tc.mode)))
			require.Error(t, halt, "a replica-local cleanup failure must halt apply, not advance past it")
			// cockroachdb errors.Is traverses Mark(); the stdlib one testify
			// uses does not.
			require.True(t, errors.Is(halt, ErrMigrationCleanupApply), "got %v", halt)
			require.ErrorIs(t, halt, local)
		})
	}
}

// A deterministic verdict on the request itself is reached identically by every
// replica, so it stays an ordinary response rather than halting the cluster.
func TestApplyMigrationCleanupKeepsDeterministicErrorsOrdinary(t *testing.T) {
	t.Parallel()

	fsm := &kvFSM{store: &cleanupFailureStore{MVCCStore: store.NewMVCCStore(), err: store.ErrValueTooLarge}}
	resp := fsm.Apply(cleanupCommand(t, pb.MigrationCleanupMode_MIGRATION_CLEANUP_MODE_UNSPECIFIED))
	require.NoError(t, haltApplyOf(resp), "a deterministic request verdict must not halt apply")
	err, ok := resp.(error)
	require.True(t, ok)
	require.ErrorIs(t, err, store.ErrValueTooLarge)
}
