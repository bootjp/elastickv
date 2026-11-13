package adapter

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/bootjp/elastickv/store"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

const absentCreatorReproKey = "g7-absent"

// absentKeyCreatorRace is one pair of commands that each create an absent key
// under a different encoding.
type absentKeyCreatorRace struct {
	name string
	// parked runs first and is held right after its first storage read of the
	// key inside its transaction, i.e. after it fixed its snapshot.
	parked func(ctx context.Context, c *redis.Client) (any, error)
	// parkOn selects the read parked is held at; nil means the first read of
	// any key containing the user key.
	parkOn func(op string, readKey []byte) bool
	// winner runs while parked is held and commits.
	winner func(ctx context.Context, c *redis.Client) error
}

func absentKeyCreatorRaces(key string) []absentKeyCreatorRace {
	xadd := func(ctx context.Context, c *redis.Client) (any, error) {
		return c.XAdd(ctx, &redis.XAddArgs{Stream: key, ID: "*", Values: []any{"f", "v"}}).Result()
	}
	hset := func(ctx context.Context, c *redis.Client) (any, error) {
		return c.HSet(ctx, key, "f", "v").Result()
	}
	rpush := func(ctx context.Context, c *redis.Client) (any, error) {
		return c.RPush(ctx, key, "v").Result()
	}
	pfadd := func(ctx context.Context, c *redis.Client) (any, error) {
		return c.PFAdd(ctx, key, "e").Result()
	}
	sadd := func(ctx context.Context, c *redis.Client) (any, error) {
		return c.SAdd(ctx, key, "m").Result()
	}
	evalXAdd := func(ctx context.Context, c *redis.Client) (any, error) {
		return c.Eval(ctx, "return redis.call('XADD', KEYS[1], '1-1', 'f', 'v')", []string{key}).Result()
	}
	commit := func(run func(context.Context, *redis.Client) (any, error)) func(context.Context, *redis.Client) error {
		return func(ctx context.Context, c *redis.Client) error {
			_, err := run(ctx, c)
			return err
		}
	}
	// RPUSH pre-checks the type at a throwaway read timestamp before its
	// transaction starts; hold it at the transaction's base-meta load instead.
	listMeta := store.ListMetaKey([]byte(key))
	rpushTxnRead := func(op string, readKey []byte) bool {
		return op == readSetReproOpGetAt && bytes.Equal(readKey, listMeta)
	}
	return []absentKeyCreatorRace{
		{name: "XADD held, HSET commits", parked: xadd, winner: commit(hset)},
		{name: "HSET held, XADD commits", parked: hset, winner: commit(xadd)},
		{name: "XADD held, RPUSH commits", parked: xadd, winner: commit(rpush)},
		{name: "RPUSH held, XADD commits", parked: rpush, parkOn: rpushTxnRead, winner: commit(xadd)},
		{name: "RPUSH held, EVAL XADD commits", parked: rpush, parkOn: rpushTxnRead, winner: commit(evalXAdd)},
		{name: "EVAL XADD held, RPUSH commits", parked: evalXAdd, winner: commit(rpush)},
		{name: "PFADD held, SADD commits", parked: pfadd, winner: commit(sadd)},
		{name: "SADD held, PFADD commits", parked: sadd, winner: commit(pfadd)},
	}
}

// physicalEncodings lists every encoding that has rows for key at ts. An HLL
// or a legacy bare value reports as "string", matching TYPE.
func (h *readSetReproHarness) physicalEncodings(t *testing.T, key []byte, ts uint64) []string {
	t.Helper()
	var out []string
	for _, typ := range []redisValueType{redisTypeString, redisTypeList, redisTypeHash, redisTypeSet, redisTypeZSet, redisTypeStream} {
		found, err := h.server.probeExpectedType(h.ctx, key, ts, typ)
		require.NoError(t, err)
		if found {
			out = append(out, string(typ))
		}
	}
	return out
}

// TestRedis_AbsentKeyCreatorsDoNotFenceEachOther checks that two commands
// creating the same absent key under different encodings serialize.
//
// Variant: interleaving test over the read-set reproduction harness in
// redis_lua_read_skew_repro_test.go (hold at a read, not at Propose; see the
// harness comment for why).
//
// Client A runs the first creator and is held right after its first storage
// read of the key: its snapshot sees the key absent. Client B runs the second
// creator, which also sees the key absent and commits. A is released and, if
// nothing in its read set was written by B (and B's write set shares no key
// with A's), commits too. XADD writes only the stream meta and entry and PFADD
// only the HLL anchor, while HSET / RPUSH / SADD write their own fence and
// member rows, so today the two write sets are disjoint and the only thing
// that can reject A is a read of the other creator's fence or anchor.
//
// Serial outcomes: exactly one creator succeeds and the other returns
// WRONGTYPE. The test fails if both encodings end up under the key.
func TestRedis_AbsentKeyCreatorsDoNotFenceEachOther(t *testing.T) {
	t.Parallel()

	for _, tc := range absentKeyCreatorRaces(absentCreatorReproKey) {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newReadSetReproHarness(t, "g7c")
			ctx := h.ctx
			key := []byte(absentCreatorReproKey)

			// One committed write so A's startTS is a real HLC timestamp.
			require.NoError(t, h.clientB.Set(ctx, "g7c-warm", "0", 0).Err())

			parkOn := tc.parkOn
			if parkOn == nil {
				parkOn = func(_ string, readKey []byte) bool { return bytes.Contains(readKey, key) }
			}
			park := h.reads.addPark("A: first transactional read of the absent key", parkOn)

			// (1) A starts creating the key and is held after its first read.
			aDone := h.goCommand(func() (any, error) {
				return tc.parked(ctx, h.clientA)
			})
			waitReadSetReproParked(t, park, aDone, "A")

			// (2) B creates the key under another encoding; it commits while A is held.
			bErr := tc.winner(ctx, h.clientB)
			h.log.record("B  create -> %v", bErr)
			require.NoError(t, bErr)

			// (3) Release A.
			park.releaseNow()
			aRes := waitReadSetReproDone(t, aDone, "A")
			h.log.record("A  create returned %s", aRes)

			finalTS := h.st.LastCommitTS()
			encodings := h.physicalEncodings(t, key, finalTS)
			typ, typErr := h.clientB.Type(ctx, absentCreatorReproKey).Result()

			t.Logf("event log:\n%s", h.log)
			t.Logf("final: TYPE=%s (err=%v) encodings with rows at ts=%d: %v", typ, typErr, finalTS, encodings)
			t.Logf("live raw keys containing %q:\n%s", absentCreatorReproKey, h.rawKeysContaining(t, key))

			if len(encodings) > 1 {
				t.Errorf("both creators committed: the key holds %d encodings %v (A returned %s); "+
					"A's absent-key read set did not include what B wrote, and B's did not include what A wrote",
					len(encodings), encodings, aRes)
				return
			}
			require.Len(t, encodings, 1)
			require.Error(t, aRes.err, "the held creator must lose")
			require.True(t, strings.Contains(aRes.err.Error(), "WRONGTYPE"),
				"the held creator's retry must see the other encoding, got %v", aRes.err)
		})
	}
}
