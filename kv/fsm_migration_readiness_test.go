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
