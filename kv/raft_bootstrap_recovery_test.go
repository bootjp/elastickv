package kv

import (
	"context"
	"testing"

	"github.com/bootjp/elastickv/internal/raftbootstrap"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
)

func TestLoadAppliedBootstrapPolicyFromProtectedFSM(t *testing.T) {
	t.Parallel()
	identity := raftbootstrap.Identity{Cluster: raftbootstrap.UUID{1}, Group: 0}
	policy := raftbootstrap.PolicyRecord{Version: 1, Identity: identity, BaselineDigest: raftbootstrap.Digest{2},
		Minimum: raftbootstrap.Versions{Entry: 1, Snapshot: 1, Session: 1}, ActivationIndex: 2, Generation: 1,
		LastConfigurationIndex: 1, Configuration: raftbootstrap.Configuration{Index: 1,
			State:   raftbootstrap.ConfState{Voters: []uint64{1}},
			Members: []raftbootstrap.Member{{ID: 1, Address: "node1:9000", PublicKeyDigest: raftbootstrap.Digest{3}}}}}
	for _, tc := range []struct {
		name      string
		change    func(*raftbootstrap.PolicyRecord)
		wantError bool
	}{
		{name: "matching"},
		{name: "wrong-identity", change: func(p *raftbootstrap.PolicyRecord) { p.Identity.Group++ }, wantError: true},
		{name: "future-activation", change: func(p *raftbootstrap.PolicyRecord) { p.ActivationIndex = 8 }, wantError: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			st, err := store.NewPebbleStore(t.TempDir())
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, st.Close()) })
			_, err = LoadAppliedBootstrapPolicy(t.Context(), st, identity, 7)
			require.Error(t, err)
			candidate := policy
			if tc.change != nil {
				tc.change(&candidate)
			}
			raw, err := raftbootstrap.MarshalPolicyRecord(candidate)
			require.NoError(t, err)
			key, err := BootstrapPolicyMetadataKey(identity)
			require.NoError(t, err)
			metadata, ok := st.(store.RaftControlMetadataStore)
			require.True(t, ok)
			applier, ok := st.(interface {
				ApplyRaftEntry(context.Context, uint64, func(context.Context) error) error
			})
			require.True(t, ok)
			require.NoError(t, applier.ApplyRaftEntry(t.Context(), 7, func(ctx context.Context) error {
				return metadata.StageRaftControlMetadata(ctx, key, raw)
			}))
			got, err := LoadAppliedBootstrapPolicy(t.Context(), st, identity, 7)
			if tc.wantError {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, policy.Identity, got.Identity)
			_, err = LoadAppliedBootstrapPolicy(t.Context(), st, identity, 6)
			require.Error(t, err, "native/FSM applied index mismatch cannot grant readiness")
		})
	}
}
