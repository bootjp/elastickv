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
// benefit from option-2 dedup.
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

// TestStandaloneSetDedup_NXMissReturnsNil pins resultNil routing through
// writeRedisStandaloneResult on the dedup path. SET with NX against an
// existing key returns nil (NX fails because the key exists); the dedup
// loop reuses the cached resultNil and the recording conn observes
// wroteNull. Without correct resultNil arming the client would observe an
// empty bulk reply, breaking NX semantics under dedup.
func TestStandaloneSetDedup_NXMissReturnsNil(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	st := store.NewMVCCStore()

	// Seed the key so the NX condition fails (key already exists).
	require.NoError(t, st.PutAt(ctx, redisStrKey([]byte("k")), encodeRedisStr([]byte("seed"), nil), 5, 0))

	coord := newDedupTestCoordinator(st, 1, true) // attempt 1 lands then errors
	srv := &RedisServer{store: st, coordinator: coord, scriptCache: map[string]string{}, onePhaseTxnDedup: true}

	conn := &recordingConn{}
	// SET k v1 NX -- attempt 1 records resultNil because NX miss.
	cmd := redcon.Command{Args: [][]byte{[]byte(cmdSet), []byte("k"), []byte("v1"), []byte("NX")}}
	srv.set(conn, cmd)

	// Airtight assertion: WriteNull was actually called (not "nothing was
	// written, leaving the zero-value nil"). Without the wroteNull witness
	// flag, a wrong branch that wrote nothing at all would also pass
	// `conn.bulk == nil`.
	require.True(t, conn.wroteNull, "NX miss must call WriteNull, not silently skip the write")
	require.Nil(t, conn.bulk, "WriteNull leaves conn.bulk nil; a stray WriteString/WriteBulk would have populated it")
	require.Empty(t, conn.err, "no error must escape; NX miss is a normal response, not an error")

	// Stored value is still the seed; nothing should have overwritten it.
	rawVal, err := st.GetAt(ctx, redisStrKey([]byte("k")), snapshotTS(coord.Clock(), st))
	require.NoError(t, err)
	val, _, err := decodeRedisStr(rawVal)
	require.NoError(t, err)
	require.Equal(t, []byte("seed"), val, "NX miss must not overwrite the existing value")
}

// TestStandaloneSetDedup_GETOptionReturnsOldBulk pins resultBulk routing
// through writeRedisStandaloneResult on the dedup path. SET ... GET on an
// existing key returns the prior value as a bulk reply; the dedup loop
// reuses the cached resultBulk and the recording conn observes the bytes.
// Without correct resultBulk arming the client would observe an empty or
// nil reply, breaking SET GET semantics under dedup.
func TestStandaloneSetDedup_GETOptionReturnsOldBulk(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	st := store.NewMVCCStore()

	// Seed the prior value -- SET GET returns this as a bulk reply.
	require.NoError(t, st.PutAt(ctx, redisStrKey([]byte("k")), encodeRedisStr([]byte("prior"), nil), 5, 0))

	coord := newDedupTestCoordinator(st, 1, true) // attempt 1 lands then errors
	srv := &RedisServer{store: st, coordinator: coord, scriptCache: map[string]string{}, onePhaseTxnDedup: true}

	conn := &recordingConn{}
	// SET k v1 GET -- attempt 1 records resultBulk("prior").
	cmd := redcon.Command{Args: [][]byte{[]byte(cmdSet), []byte("k"), []byte("v1"), []byte("GET")}}
	srv.set(conn, cmd)

	// recordingConn.WriteBulk copies into .bulk; the prior value must round-trip
	// from the cached attempt-1 result through writeRedisStandaloneResult.
	require.Equal(t, "prior", string(conn.bulk), "GET option must reply with the cached prior value, not a re-read")
	require.Empty(t, conn.err)

	// New value committed via the landed attempt-1 apply.
	rawVal, err := st.GetAt(ctx, redisStrKey([]byte("k")), snapshotTS(coord.Clock(), st))
	require.NoError(t, err)
	val, _, err := decodeRedisStr(rawVal)
	require.NoError(t, err)
	require.Equal(t, []byte("v1"), val, "SET GET still applies the new value; dedup just preserves the GET result")
}

// TestStandaloneSetDedup_OverwritesWideHash verifies that the default-on
// standalone SET dedup path keeps Redis overwrite semantics: a SET without
// GET replaces a collection value with a string and tombstones the old
// wide-column hash rows.
func TestStandaloneSetDedup_OverwritesWideHash(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	st := store.NewMVCCStore()
	key := []byte("k")
	require.NoError(t, st.PutAt(ctx, store.HashFieldKey(key, []byte("f")), []byte("old"), 5, 0))
	require.NoError(t, st.PutAt(ctx, store.HashMetaKey(key), store.MarshalHashMeta(store.HashMeta{Len: 1}), 5, 0))

	coord := newDedupTestCoordinator(st, 1, true)
	srv := &RedisServer{store: st, coordinator: coord, scriptCache: map[string]string{}, onePhaseTxnDedup: true}

	conn := &recordingConn{}
	cmd := redcon.Command{Args: [][]byte{[]byte(cmdSet), key, []byte("v1")}}
	srv.set(conn, cmd)

	require.Equal(t, "OK", string(conn.bulk))
	require.Empty(t, conn.err)
	require.Equal(t, 2, coord.dispatches)
	require.Equal(t, 1, coord.probeNoOps)

	readTS := snapshotTS(coord.Clock(), st)
	rawVal, err := st.GetAt(ctx, redisStrKey(key), readTS)
	require.NoError(t, err)
	val, _, err := decodeRedisStr(rawVal)
	require.NoError(t, err)
	require.Equal(t, []byte("v1"), val)
	_, err = st.GetAt(ctx, store.HashFieldKey(key, []byte("f")), readTS)
	require.ErrorIs(t, err, store.ErrKeyNotFound)
}

func TestStandaloneIncrDedup_LandedPriorAttemptReturnsCachedValue(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	st := store.NewMVCCStore()
	key := []byte("counter")
	require.NoError(t, st.PutAt(ctx, redisStrKey(key), encodeRedisStr([]byte("1"), nil), 5, 0))

	coord := newDedupTestCoordinator(st, 1, true)
	srv := &RedisServer{store: st, coordinator: coord, scriptCache: map[string]string{}, onePhaseTxnDedup: true}

	conn := &recordingConn{}
	cmd := redcon.Command{Args: [][]byte{[]byte(cmdIncr), key}}
	srv.incr(conn, cmd)

	require.Empty(t, conn.err)
	require.Equal(t, int64(2), conn.int)
	require.Equal(t, 2, coord.dispatches)
	require.Equal(t, 1, coord.probeNoOps)

	rawVal, err := st.GetAt(ctx, redisStrKey(key), snapshotTS(coord.Clock(), st))
	require.NoError(t, err)
	val, _, err := decodeRedisStr(rawVal)
	require.NoError(t, err)
	require.Equal(t, []byte("2"), val)
}

func TestStandaloneHSetDedup_LandedPriorAttemptReturnsCachedAdded(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	st := store.NewMVCCStore()
	key := []byte("hash")

	coord := newDedupTestCoordinator(st, 1, true)
	srv := &RedisServer{store: st, coordinator: coord, scriptCache: map[string]string{}, onePhaseTxnDedup: true}

	conn := &recordingConn{}
	cmd := redcon.Command{Args: [][]byte{[]byte(cmdHSet), key, []byte("f"), []byte("v")}}
	srv.hset(conn, cmd)

	require.Empty(t, conn.err)
	require.Equal(t, int64(1), conn.int)
	require.Equal(t, 2, coord.dispatches)
	require.Equal(t, 1, coord.probeNoOps)

	readTS := snapshotTS(coord.Clock(), st)
	raw, err := st.GetAt(ctx, store.HashFieldKey(key, []byte("f")), readTS)
	require.NoError(t, err)
	require.Equal(t, []byte("v"), raw)
	count, exists, err := srv.resolveHashMeta(ctx, key, readTS)
	require.NoError(t, err)
	require.True(t, exists)
	require.Equal(t, int64(1), count)
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
