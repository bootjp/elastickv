package etcd

import (
	"testing"

	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/stretchr/testify/require"
)

// The etcd backend and engine-neutral callers (adapter, wiring) must
// agree on every member's node ID.
func TestDeriveNodeIDMatchesEngineNeutralDerivation(t *testing.T) {
	t.Parallel()
	for _, id := range []string{"", "n1", "n2", "node-a", "10.0.0.1:50051"} {
		require.Equal(t, raftengine.DeriveNodeID(id), DeriveNodeID(id), "id %q", id)
	}
}
