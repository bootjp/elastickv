package adapter

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/bootjp/elastickv/internal/raftengine"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
)

type changingNamespaceConfiguration struct {
	raftengine.LeaderView
	first, next uint64
	calls       atomic.Int32
}

func (l *changingNamespaceConfiguration) AppliedConfigurationIndex() (uint64, bool) {
	if l.calls.Add(1) == 1 {
		return l.first, true
	}
	return l.next, true
}

func TestNamespaceProbeDoesNotRelabelOldActivationACK(t *testing.T) {
	for _, required := range []bool{false, true} {
		t.Run(map[bool]string{false: "inventory-discovery", true: "expected-configuration"}[required], func(t *testing.T) {
			internal, _ := namespaceMutationFixture(t, true, true)
			t.Cleanup(func() { require.NoError(t, internal.store.Close()) })
			internal.leader = &changingNamespaceConfiguration{LeaderView: internal.leader, first: 9, next: 10}
			response, err := internal.ProbeMigrationState(t.Context(), &pb.ProbeMigrationStateRequest{JobId: 1, Kind: pb.MigrationStateProbeKind_MIGRATION_STATE_PROBE_KIND_NAMESPACE_ADMISSION, RequireConfigurationIndex: required, ExpectedConfigurationIndex: 10})
			require.NoError(t, err)
			require.False(t, response.Ready, "an ACK proved at index 9 cannot be relabeled as index 10")
			require.Equal(t, uint64(10), response.AppliedConfigurationIndex)
		})
	}
}

type namespaceStateSwappingStore struct {
	store.MVCCStore
	reads int
	swap  func([]byte)
}

func (s *namespaceStateSwappingStore) GetAt(ctx context.Context, key []byte, ts uint64) ([]byte, error) {
	value, err := s.MVCCStore.GetAt(ctx, key, ts)
	s.reads++
	if s.reads == 1 {
		s.swap(key)
	}
	return value, err
}

func TestNamespaceProbeReadsVersionAndConfigurationAtomically(t *testing.T) {
	internal, _ := namespaceMutationFixture(t, true, true)
	base := internal.store
	t.Cleanup(func() { require.NoError(t, base.Close()) })
	wrapped := &namespaceStateSwappingStore{MVCCStore: base, swap: func(key []byte) {
		state, err := store.EncodeMigrationNamespaceConfiguration(store.MigrationNamespaceConfiguration{Index: 10})
		require.NoError(t, err)
		require.NoError(t, base.PutAt(t.Context(), key, state, base.LastCommitTS()+1, 0))
	}}
	internal.store = wrapped
	internal.leader = &changingNamespaceConfiguration{LeaderView: internal.leader, first: 10, next: 10}
	response, err := internal.ProbeMigrationState(t.Context(), &pb.ProbeMigrationStateRequest{JobId: 1, Kind: pb.MigrationStateProbeKind_MIGRATION_STATE_PROBE_KIND_NAMESPACE_ADMISSION, RequireConfigurationIndex: true, ExpectedConfigurationIndex: 10})
	require.NoError(t, err)
	require.False(t, response.Ready, "version from the old record must not combine with the new configuration index")
	require.Equal(t, 1, wrapped.reads, "activation version and configuration must come from one durable read")
}
