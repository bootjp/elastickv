package adapter

import (
	"context"
	"testing"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

// TestRedis_SET_OverwritesList_UnderDefaultGate locks in the legacy
// SET-over-collection overwrite semantics under the new default-on dedup
// gate landed in PR #943. The dedup path's applySet returns WRONGTYPE on
// a SET that hits a key already holding a list/hash/set/zset/stream,
// while the legacy setLegacy / executeSet / replaceWithStringTxn path
// deletes the collection's logical elements and writes the string.
// Codex flagged this as a P1 regression on PR #943 round-1: flipping
// onePhaseTxnDedup default-on without a separate gate on the standalone
// SET path would have changed normal Redis overwrite behaviour. The fix
// is the standaloneSetDedup sub-gate, which defaults off — so
// `SET k v` after `RPUSH k x` must still return OK and let the next
// GET observe the string value, not WRONGTYPE.
//
// This is a regression test (CLAUDE.md self-review §5 + the
// "when code review surfaces a defect, first add a failing test"
// convention). If a future change re-enables standaloneSetDedup as
// default-on without bringing applySet to parity with executeSet, this
// test must fail.
func TestRedis_SET_OverwritesList_UnderDefaultGate(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 3)
	defer shutdown(nodes)

	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{Addr: nodes[0].redisAddress})
	defer func() { _ = rdb.Close() }()

	// Seed the key as a list — the encoding that the dedup path's
	// applySet hard-fails against.
	require.NoError(t, rdb.Do(ctx, "RPUSH", "setover:list", "elem-a").Err())

	// Real Redis behaviour: SET unconditionally replaces the value,
	// dropping the previous type. The legacy path implements this; the
	// dedup path returns WRONGTYPE. With the default config
	// (onePhaseTxnDedup on, standaloneSetDedup off) we must take the
	// legacy path and observe OK + string value.
	res, err := rdb.Do(ctx, "SET", "setover:list", "replaced").Result()
	require.NoError(t, err, "SET must overwrite an existing list under default config")
	require.Equal(t, "OK", res)

	got, err := rdb.Get(ctx, "setover:list").Result()
	require.NoError(t, err, "GET after SET-over-list must succeed")
	require.Equal(t, "replaced", got)
}
