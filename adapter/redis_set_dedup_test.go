package adapter

import (
	"context"
	"testing"

	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/redcon"
)

// recordingConn (defined in redis_retry_test.go) captures handler writes via
// .bulk, .err, .int fields. WriteString and WriteBulk both populate .bulk —
// in this test "OK" lands as bulk=[]byte("OK"), .err stays empty for the
// success path.

// TestStandaloneSetDedup_LandedPriorAttempt_ReturnsOK pins the standalone SET
// dedup path: when the gate is on, SET routes through runTransactionWithDedup
// as a single-mop EXEC body. Attempt 1 lands then errors → reuse probes →
// FSM no-ops → client gets "OK" (the cached result) without re-applying.
//
// Pins that the gate-on path uses the same dedup machinery as MULTI/EXEC.
// Without this routing, a standalone SET under leadership churn would not
// benefit from option-2 dedup (the design's "still open" item before this
// PR).
func TestStandaloneSetDedup_LandedPriorAttempt_ReturnsOK(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	st := store.NewMVCCStore()
	coord := newDedupTestCoordinator(st, 1, true) // attempt 1 lands then errors
	srv := &RedisServer{store: st, coordinator: coord, scriptCache: map[string]string{}, onePhaseTxnDedup: true}

	conn := &recordingConn{}
	cmd := redcon.Command{Args: [][]byte{[]byte(cmdSet), []byte("k"), []byte("v1")}}
	srv.set(conn, cmd)

	require.Equal(t, "OK", string(conn.bulk), "standalone SET must reply with the cached OK from attempt 1")
	require.Empty(t, conn.err, "no error must escape; dedup hid the ambiguous attempt-1 failure")
	require.Equal(t, 2, coord.dispatches, "one ambiguous-land attempt + one reuse")
	require.Equal(t, 1, coord.probeNoOps, "reuse must dedup via the exact-ts probe")

	rawVal, err := st.GetAt(ctx, redisStrKey([]byte("k")), snapshotTS(coord.Clock(), st))
	require.NoError(t, err)
	val, _, err := decodeRedisStr(rawVal)
	require.NoError(t, err)
	require.Equal(t, []byte("v1"), val, "only one apply landed — the value matches attempt 1")
}

// TestStandaloneSetDedup_DisabledKeepsLegacyPath verifies the gate is honored
// for the standalone SET path too: when onePhaseTxnDedup is off, r.set takes
// its legacy fast-path / executeSet shape (no probe, no per-attempt PrevCommitTS).
// Pins that the new routing is strictly opt-in.
func TestStandaloneSetDedup_DisabledKeepsLegacyPath(t *testing.T) {
	t.Parallel()
	st := store.NewMVCCStore()
	coord := newDedupTestCoordinator(st, 1, false) // attempt 1 errors without landing
	srv := &RedisServer{store: st, coordinator: coord, scriptCache: map[string]string{} /* gate left false */}

	conn := &recordingConn{}
	cmd := redcon.Command{Args: [][]byte{[]byte(cmdSet), []byte("k"), []byte("v1")}}
	srv.set(conn, cmd)

	// Legacy path: no probe.
	require.Equal(t, 0, coord.probeNoOps, "gate off — runTransactionWithDedup must not be used")
}
