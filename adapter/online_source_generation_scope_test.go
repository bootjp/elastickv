package adapter

import (
	"context"
	"testing"

	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
)

func TestOnlineCaptureNonceIsNotPublishedAsActiveOwnership(t *testing.T) {
	st, err := store.NewPebbleStore(t.TempDir())
	require.NoError(t, err)
	defer st.Close()
	generation := "0123456789abcdef0123456789abcdef"
	session := store.OnlineMigrationSession{
		Version:    store.OnlineMigrationVersion,
		Identity:   store.MigrationCaptureIdentity{SourceGroupID: 1, JobID: 1, Generation: 1, Index: 1, Lineage: store.OnlineMigrationLineage(generation)},
		StartIndex: 1, LastIndex: 1, ConfigurationIndex: 1, OwnershipGeneration: generation,
	}
	applier, ok := st.(store.RaftEntryApplier)
	require.True(t, ok)
	require.NoError(t, applier.ApplyRaftEntry(t.Context(), 1, func(ctx context.Context) error { return store.StartOnlineMigrationSession(ctx, st, session) }))
	internal := &Internal{store: st}
	response := &pb.ProbeMigrationStateResponse{Ready: true}
	require.NoError(t, internal.decorateOnlineSourceGeneration(t.Context(), &pb.ProbeMigrationStateRequest{JobId: 1, Kind: pb.MigrationStateProbeKind_MIGRATION_STATE_PROBE_KIND_OWNERSHIP_CAPABILITY}, response))
	require.Equal(t, generation, response.GetOnlineSourceGeneration())
	require.Empty(t, response.GetOwnershipGeneration(), "a saved capture nonce cannot attest an active group ownership namespace")
}
