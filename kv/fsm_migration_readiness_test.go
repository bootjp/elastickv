package kv

import (
	"context"
	"testing"

	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
)

func TestApplyTargetStagedReadinessCommandPersistsGuard(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	st := store.NewMVCCStore()
	fsm := &kvFSM{store: st}

	cmd, err := MarshalTargetStagedReadinessCommand(&pb.TargetStagedReadinessRequest{
		JobId:                  9,
		RouteStart:             []byte("a"),
		RouteEnd:               []byte("z"),
		ExpectedCutoverVersion: 3,
		MigrationJobId:         7,
		MinWriteTsExclusive:    100,
		Armed:                  true,
	})
	require.NoError(t, err)
	require.Nil(t, fsm.Apply(cmd))

	reader, ok := st.(store.MigrationTargetReadinessReader)
	require.True(t, ok)
	states, err := reader.MigrationTargetReadinessStates(ctx)
	require.NoError(t, err)
	require.Equal(t, []store.TargetStagedReadinessState{{
		JobID:                  9,
		RouteStart:             []byte("a"),
		RouteEnd:               []byte("z"),
		ExpectedCutoverVersion: 3,
		MigrationJobID:         7,
		MinWriteTSExclusive:    100,
		Armed:                  true,
	}}, states)
}

func TestApplyTargetStagedReadinessCommandPersistsAppliedIndex(t *testing.T) {
	t.Parallel()

	st, err := store.NewPebbleStore(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, st.Close()) })
	fsm := &kvFSM{store: st, pendingApplyIdx: 77}

	cmd, err := MarshalTargetStagedReadinessCommand(&pb.TargetStagedReadinessRequest{
		JobId:                  9,
		RouteStart:             []byte("a"),
		RouteEnd:               []byte("z"),
		ExpectedCutoverVersion: 3,
		MigrationJobId:         7,
		MinWriteTsExclusive:    100,
		Armed:                  true,
	})
	require.NoError(t, err)
	require.Nil(t, fsm.Apply(cmd))

	appliedReader, ok := st.(interface {
		LastAppliedIndex() (uint64, bool, error)
	})
	require.True(t, ok)
	idx, present, err := appliedReader.LastAppliedIndex()
	require.NoError(t, err)
	require.True(t, present)
	require.Equal(t, uint64(77), idx)
}

func TestRecordMigrationWriteDoesNotAdvanceAppliedIndexBeforeDataWrite(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	st, err := store.NewPebbleStore(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, st.Close()) })
	writer, ok := st.(store.MigrationTargetReadinessWriter)
	require.True(t, ok)
	require.NoError(t, writer.ApplyTargetStagedReadiness(ctx, store.TargetStagedReadinessState{
		JobID:               11,
		RouteStart:          []byte("m"),
		RouteEnd:            []byte("z"),
		MigrationJobID:      11,
		MinWriteTSExclusive: 1,
		Armed:               true,
		TrackWrites:         true,
		RetentionPinTS:      1,
		MinAdmittedTS:       90,
	}))
	fsm := &kvFSM{store: st, pendingApplyIdx: 77}

	err = fsm.recordMigrationWrite(ctx, []*pb.Mutation{{Op: pb.Op_PUT, Key: []byte("n"), Value: []byte("v")}}, 50)
	require.NoError(t, err)
	reader, ok := st.(store.MigrationTargetReadinessReader)
	require.True(t, ok)
	states, err := reader.MigrationTargetReadinessStates(ctx)
	require.NoError(t, err)
	require.Len(t, states, 1)
	require.Equal(t, uint64(50), states[0].MinAdmittedTS)

	appliedReader, ok := st.(interface {
		LastAppliedIndex() (uint64, bool, error)
	})
	require.True(t, ok)
	idx, present, err := appliedReader.LastAppliedIndex()
	require.NoError(t, err)
	require.False(t, present)
	require.Zero(t, idx)

	err = st.ApplyMutationsRaftAt(ctx, []*store.KVPairMutation{{
		Op:    store.OpTypePut,
		Key:   []byte("n"),
		Value: []byte("v"),
	}}, nil, 50, 50, 77)
	require.NoError(t, err)
	idx, present, err = appliedReader.LastAppliedIndex()
	require.NoError(t, err)
	require.True(t, present)
	require.Equal(t, uint64(77), idx)
}

func TestRecordMigrationWriteTracksEmptyDelPrefix(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	st := store.NewMVCCStore()
	writer, ok := st.(store.MigrationTargetReadinessWriter)
	require.True(t, ok)
	reader, ok := st.(store.MigrationTargetReadinessReader)
	require.True(t, ok)

	err := recordMigrationWriteInStates(ctx, writer, []store.TargetStagedReadinessState{{
		JobID:                  11,
		RouteStart:             []byte("m"),
		RouteEnd:               []byte("n"),
		ExpectedCutoverVersion: 2,
		MigrationJobID:         11,
		MinWriteTSExclusive:    100,
		Armed:                  true,
		TrackWrites:            true,
		RetentionPinTS:         40,
		MinAdmittedTS:          90,
	}}, []*pb.Mutation{{Op: pb.Op_DEL_PREFIX}}, 50, 0)
	require.NoError(t, err)

	states, err := reader.MigrationTargetReadinessStates(ctx)
	require.NoError(t, err)
	require.Len(t, states, 1)
	require.Equal(t, uint64(50), states[0].MinAdmittedTS)
}
