package hashicorp

import (
	"testing"

	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/cockroachdb/errors"
	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/require"
)

// TestTranslateLeadershipErrMatchesRaftEngineSentinel pins the invariant
// that hashicorp/raft leadership-loss errors are marked against the
// shared raftengine sentinels. The lease-read fast path in package kv
// relies on a single cross-backend errors.Is(err, raftengine.ErrNotLeader)
// check; a future refactor that forgets to mark these errors would
// silently force every read onto the slow LinearizableRead path.
func TestTranslateLeadershipErrMatchesRaftEngineSentinel(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		in   error
		want error
	}{
		{"not leader", raft.ErrNotLeader, raftengine.ErrNotLeader},
		{"leadership lost", raft.ErrLeadershipLost, raftengine.ErrLeadershipLost},
		{"leadership transfer in progress", raft.ErrLeadershipTransferInProgress, raftengine.ErrLeadershipTransferInProgress},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			out := translateLeadershipErr(tc.in)
			require.True(t, errors.Is(out, tc.want),
				"translated error must errors.Is-match the raftengine sentinel")
			require.True(t, errors.Is(out, tc.in),
				"translated error must retain the original raft sentinel for debugging")
		})
	}

	t.Run("unrelated error is passed through", func(t *testing.T) {
		t.Parallel()
		orig := errors.New("write conflict")
		out := translateLeadershipErr(orig)
		require.False(t, errors.Is(out, raftengine.ErrNotLeader))
		require.False(t, errors.Is(out, raftengine.ErrLeadershipLost))
		require.False(t, errors.Is(out, raftengine.ErrLeadershipTransferInProgress))
	})

	t.Run("nil stays nil", func(t *testing.T) {
		t.Parallel()
		require.NoError(t, translateLeadershipErr(nil))
	})
}
