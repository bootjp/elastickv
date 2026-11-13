package adapter

import (
	"bytes"
	"context"
	"errors"
	"strconv"
	"strings"
	"testing"

	"github.com/bootjp/elastickv/internal/fskeys"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

// Reproductions for the G7 existence checks that record nothing: SETNX,
// SET NX / XX and the legacy (dedup-off) string creates decide on the key's
// existence at their read timestamp and then write only the string anchor,
// with no read set and no fence; a MULTI type probe records the type anchors
// but not the collection fences.
//
// Variant: interleaving tests over the read-set reproduction harness in
// redis_lua_read_skew_repro_test.go (hold at a read, not at Propose; see the
// harness comment for why).

const (
	setNXReproKey = "g7-setnx"
	setNXReproOut = "g7-setnx-out"
)

func setNXReproCommit(run func(context.Context, *redis.Client) (any, error)) func(context.Context, *redis.Client) error {
	return func(ctx context.Context, c *redis.Client) error {
		_, err := run(ctx, c)
		return err
	}
}

func TestRedisLegacyBareCleanupPreservesFilesystemInode(t *testing.T) {
	ctx := context.Background()
	server, st := newRedisStorageMigrationTestServer(t)
	inodeKey := fskeys.InodeKey(1)
	require.NoError(t, st.PutAt(ctx, inodeKey, []byte("inode"), 1, 0))
	require.False(t, redisLegacyBareReadFenceAllowed(inodeKey))
	elems, err := server.deleteStringLikeElems(ctx, inodeKey, 2)
	require.NoError(t, err)
	require.False(t, opElemsDeleteKey(elems, inodeKey))
	txn := newRedisTxnTestContext(server)
	txn.startTS = 2
	value, err := txn.load(inodeKey, false)
	require.NoError(t, err)
	require.Nil(t, value.raw)
}

// setNXReproCreators are the commands that create setNXReproKey under a
// non-string encoding.
func setNXReproCreators(key string) map[string]func(context.Context, *redis.Client) (any, error) {
	return map[string]func(context.Context, *redis.Client) (any, error){
		"PFADD": func(ctx context.Context, c *redis.Client) (any, error) {
			return c.PFAdd(ctx, key, "e").Result()
		},
		"XADD": func(ctx context.Context, c *redis.Client) (any, error) {
			return c.XAdd(ctx, &redis.XAddArgs{Stream: key, ID: "*", Values: []any{"f", "v"}}).Result()
		},
		"RPUSH": func(ctx context.Context, c *redis.Client) (any, error) {
			return c.RPush(ctx, key, "x").Result()
		},
		"HSET": func(ctx context.Context, c *redis.Client) (any, error) {
			return c.HSet(ctx, key, "f", "v").Result()
		},
	}
}

// stringCreateRace is one "a string create observed the key absent, then
// another command created it under another encoding" interleaving.
type stringCreateRace struct {
	name string
	// opts configures the server; WithOnePhaseTxnDedup(false) selects the
	// legacy standalone SET / INCR paths.
	opts []RedisServerOption
	// held runs first and is held right after its first storage read of the
	// key, i.e. after it fixed its snapshot and began its existence check.
	held func(ctx context.Context, c *redis.Client) (any, error)
	// winner creates the key under another encoding while held is parked.
	winner string
	// wantHeld checks held's reply once its retry observed the winner.
	wantHeld func(t *testing.T, res readSetReproReply)
	// wantEncoding is the only encoding the key may hold at the end.
	wantEncoding string
}

func stringCreateRaces(key string) []stringCreateRace {
	legacy := []RedisServerOption{WithOnePhaseTxnDedup(false)}
	setnx := func(ctx context.Context, c *redis.Client) (any, error) {
		return c.Do(ctx, "SETNX", key, "s").Result()
	}
	setNX := func(ctx context.Context, c *redis.Client) (any, error) {
		return c.Do(ctx, "SET", key, "s", "NX").Result()
	}
	set := func(ctx context.Context, c *redis.Client) (any, error) {
		return c.Do(ctx, "SET", key, "s").Result()
	}
	incr := func(ctx context.Context, c *redis.Client) (any, error) {
		return c.Do(ctx, "INCR", key).Result()
	}
	notSet := func(t *testing.T, res readSetReproReply) {
		t.Helper()
		require.NoError(t, res.err)
		require.EqualValues(t, 0, res.val, "SETNX must see the key the winner created")
	}
	nilReply := func(t *testing.T, res readSetReproReply) {
		t.Helper()
		require.ErrorIs(t, res.err, redis.Nil, "SET NX must see the key the winner created (got %v)", res.val)
	}
	ok := func(t *testing.T, res readSetReproReply) {
		t.Helper()
		require.NoError(t, res.err)
		require.Equal(t, "OK", res.val)
	}
	wrongType := func(t *testing.T, res readSetReproReply) {
		t.Helper()
		require.Error(t, res.err)
		require.Contains(t, res.err.Error(), "WRONGTYPE")
	}
	return []stringCreateRace{
		// Both PFADD and a string create write the TTL index key, so the
		// write-write check already rejects the second; kept to pin it.
		{name: "SETNX held, PFADD commits", held: setnx, winner: "PFADD", wantHeld: notSet, wantEncoding: "hll"},
		{name: "SETNX held, XADD commits", held: setnx, winner: "XADD", wantHeld: notSet, wantEncoding: "stream"},
		{name: "SETNX held, RPUSH commits", held: setnx, winner: "RPUSH", wantHeld: notSet, wantEncoding: "list"},
		{name: "SETNX held, HSET commits", held: setnx, winner: "HSET", wantHeld: notSet, wantEncoding: "hash"},
		{name: "SET NX held without dedup, PFADD commits", opts: legacy, held: setNX, winner: "PFADD", wantHeld: nilReply, wantEncoding: "hll"},
		{name: "SET NX held without dedup, XADD commits", opts: legacy, held: setNX, winner: "XADD", wantHeld: nilReply, wantEncoding: "stream"},
		// The dedup path already reads the absent-key set; kept as a control.
		{name: "SET NX held with dedup, XADD commits", held: setNX, winner: "XADD", wantHeld: nilReply, wantEncoding: "stream"},
		// A plain SET overwrites whatever it finds, so its retry replaces the
		// stream; only the unvalidated create leaves both.
		{name: "SET held without dedup, XADD commits", opts: legacy, held: set, winner: "XADD", wantHeld: ok, wantEncoding: "string"},
		{name: "INCR held without dedup, XADD commits", opts: legacy, held: incr, winner: "XADD", wantHeld: wrongType, wantEncoding: "stream"},
		{name: "INCR held without dedup, RPUSH commits", opts: legacy, held: incr, winner: "RPUSH", wantHeld: wrongType, wantEncoding: "list"},
	}
}

// TestRedis_StringCreateDoesNotFenceConcurrentCreate checks that a string
// create that observed the key absent serializes with a concurrent create of
// another encoding.
//
// Client A runs the string create (SETNX, SET NX, or a legacy SET / INCR) and
// is held right after its first storage read of the key: its snapshot sees the
// key absent. Client B creates the key as an HLL, stream, list, or hash; the
// create reads the absent-key set or the four txn-wide fences, sees the key
// absent, and commits. A is released and writes the string anchor. Nothing in
// A's write set was written by B, and A reads nothing, so today both commit:
// two encodings under one key, and SETNX / SET NX report success on a key that
// existed below their commit timestamp.
//
// Serial outcomes: B then A leaves only B's encoding (SETNX replies 0, SET NX
// nil, INCR WRONGTYPE) or, for a plain SET, only the string A wrote.
func TestRedis_StringCreateDoesNotFenceConcurrentCreate(t *testing.T) {
	t.Parallel()

	for _, tc := range stringCreateRaces(setNXReproKey) {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newReadSetReproHarness(t, "g7s", tc.opts...)
			ctx := h.ctx
			key := []byte(setNXReproKey)

			// One committed write so A's startTS is a real HLC timestamp.
			require.NoError(t, h.clientB.Set(ctx, "g7s-warm", "0", 0).Err())

			park := h.reads.addPark("A: first read of the key", func(_ string, readKey []byte) bool {
				return bytes.Contains(readKey, key)
			})

			// (1) A starts the string create and is held after its first read.
			aDone := h.goCommand(func() (any, error) {
				return tc.held(ctx, h.clientA)
			})
			waitReadSetReproParked(t, park, aDone, "A")

			// (2) B creates the key under another encoding; it commits while A is held.
			winner := setNXReproCreators(setNXReproKey)[tc.winner]
			require.NotNil(t, winner, "unknown winner %q", tc.winner)
			bErr := setNXReproCommit(winner)(ctx, h.clientB)
			h.log.record("B  %s -> %v", tc.winner, bErr)
			require.NoError(t, bErr)

			// (3) Release A.
			park.releaseNow()
			aRes := waitReadSetReproDone(t, aDone, "A")
			h.log.record("A  returned %s", aRes)

			finalTS := h.st.LastCommitTS()
			encodings := h.physicalEncodings(t, key, finalTS)
			t.Logf("event log:\n%s", h.log)
			t.Logf("final: encodings with rows at ts=%d: %v", finalTS, encodings)
			t.Logf("live raw keys containing %q:\n%s", setNXReproKey, h.rawKeysContaining(t, key))

			if len(encodings) > 1 {
				t.Errorf("both creates committed: the key holds %d encodings %v (A returned %s); "+
					"A's existence check was not in its read set and it Put no fence",
					len(encodings), encodings, aRes)
				return
			}
			require.Equal(t, []string{tc.wantEncoding}, encodings)
			tc.wantHeld(t, aRes)
		})
	}
}

func TestRedis_SetOfExistingStringValidatesTypeChange(t *testing.T) {
	t.Parallel()
	h := newReadSetReproHarness(t, "g7-set-existing", WithOnePhaseTxnDedup(false))
	ctx := h.ctx
	key := []byte("g7-set-existing-key")
	require.NoError(t, h.clientB.Set(ctx, string(key), "old", 0).Err())

	park := h.reads.addPark("A: type probe of existing string", func(_ string, readKey []byte) bool {
		return bytes.Contains(readKey, key)
	})
	aDone := h.goCommand(func() (any, error) {
		return h.clientA.Set(ctx, string(key), "new", 0).Result()
	})
	waitReadSetReproParked(t, park, aDone, "A")

	require.NoError(t, h.clientB.Del(ctx, string(key)).Err())
	require.NoError(t, h.clientB.HSet(ctx, string(key), "f", "v").Err())
	park.releaseNow()
	aRes := waitReadSetReproDone(t, aDone, "A")
	require.NoError(t, aRes.err)
	require.Equal(t, "OK", aRes.val)
	require.Equal(t, []string{"string"}, h.physicalEncodings(t, key, h.st.LastCommitTS()))
}

func TestRedis_SetExInternalShapedUserKeyPreservesOtherString(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		opts []RedisServerOption
	}{
		{name: "default"},
		{name: "legacy", opts: []RedisServerOption{WithOnePhaseTxnDedup(false)}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newReadSetReproHarness(t, "internal-shaped-setex", tc.opts...)
			ctx := h.ctx
			require.NoError(t, h.clientB.Set(ctx, "victim", "value", 0).Err())
			key := string(redisStrKey([]byte("victim")))
			_, err := h.clientB.Get(ctx, key).Result()
			require.ErrorIs(t, err, redis.Nil)
			require.NoError(t, h.clientB.Do(ctx, "SETEX", key, 60, "other").Err())
			require.NoError(t, h.clientB.Do(ctx, "SETEX", key, 60, "again").Err())
			attempts := h.proposer.allWriting(redisStrKey([]byte(key)))
			require.NotEmpty(t, attempts)
			require.False(t, readSetReproContainsKey(attempts[len(attempts)-1].reads, []byte(key)))
			got, err := h.clientB.Get(ctx, "victim").Result()
			require.NoError(t, err)
			require.Equal(t, "value", got)
			require.NoError(t, h.clientB.Del(ctx, key).Err())
			got, err = h.clientB.Get(ctx, "victim").Result()
			require.NoError(t, err)
			require.Equal(t, "value", got)
		})
	}
}

// setCondRace is one "SET NX / XX decided on the key's existence, then another
// command changed it" interleaving on a single string key.
type setCondRace struct {
	name   string
	opts   []RedisServerOption
	seed   func(ctx context.Context, c *redis.Client) error
	held   func(ctx context.Context, c *redis.Client) (any, error)
	winner func(ctx context.Context, c *redis.Client) error
	// check inspects held's reply and the key's final value (nil when absent).
	check func(t *testing.T, res readSetReproReply, final *string)
}

func setCondRaces(key string) []setCondRace {
	legacy := []RedisServerOption{WithOnePhaseTxnDedup(false)}
	do := func(args ...any) func(context.Context, *redis.Client) (any, error) {
		return func(ctx context.Context, c *redis.Client) (any, error) {
			return c.Do(ctx, args...).Result()
		}
	}
	commit := func(args ...any) func(context.Context, *redis.Client) error {
		return setNXReproCommit(do(args...))
	}
	seedString := commit("SET", key, "seed")
	seedHash := commit("HSET", key, "f", "v")
	// NX loses to the winner's create: nil / 0 and the winner's value.
	nxLost := func(t *testing.T, res readSetReproReply, final *string) {
		t.Helper()
		if res.err == nil && (res.val == "OK" || res.val == int64(1)) {
			t.Errorf("both NX creates succeeded (A returned %s, final %v): A's absence check was not validated", res, final)
			return
		}
		if !errors.Is(res.err, redis.Nil) {
			require.NoError(t, res.err)
			require.EqualValues(t, 0, res.val)
		}
		require.NotNil(t, final)
		require.Equal(t, "b", *final)
	}
	// XX loses to the winner's DEL: nil and the key stays absent.
	xxLost := func(t *testing.T, res readSetReproReply, final *string) {
		t.Helper()
		if res.err == nil && res.val == "OK" {
			t.Errorf("SET XX wrote into a key deleted below its commit (A returned %s, final %v)", res, final)
			return
		}
		require.ErrorIs(t, res.err, redis.Nil)
		require.Nil(t, final)
	}
	return []setCondRace{
		{name: "SET NX held without dedup, SET NX commits", opts: legacy, held: do("SET", key, "a", "NX"), winner: commit("SET", key, "b", "NX"), check: nxLost},
		{name: "SETNX held, SET NX commits", held: do("SETNX", key, "a"), winner: commit("SET", key, "b", "NX"), check: nxLost},
		{name: "SETNX held, SETNX commits", held: do("SETNX", key, "a"), winner: commit("SETNX", key, "b"), check: nxLost},
		{name: "SET XX of a string held without dedup, DEL commits", opts: legacy, seed: seedString, held: do("SET", key, "a", "XX"), winner: commit("DEL", key), check: xxLost},
		{name: "SET XX of a hash held without dedup, DEL commits", opts: legacy, seed: seedHash, held: do("SET", key, "a", "XX"), winner: commit("DEL", key), check: xxLost},
	}
}

// TestRedis_SetNXAndXXDecideOnValidatedExistence checks that SET NX / SETNX
// and SET XX serialize with a concurrent create or DEL of the same key.
//
// Client A runs the conditional SET and is held right after its first storage
// read of the key. Client B creates the key (NX) or deletes it (XX) and
// commits. A is released. If A's existence check is validated at commit, its
// first attempt conflicts and the retry replies nil / 0 (NX) or nil with the
// key absent (XX). Serial outcomes are exactly those.
//
// These interleavings are already rejected without a read set: A and B both
// write the string anchor (or, for SET XX over a collection, the four
// txn-wide fences its delete Puts), so the write-write check fails A's first
// attempt. They pin that the standalone paths keep that property; the
// branches with no write of the key are the Lua cases in
// TestLua_CollectionReadOfUnwrittenKeyIsNotValidated.
func TestRedis_SetNXAndXXDecideOnValidatedExistence(t *testing.T) {
	t.Parallel()

	for _, tc := range setCondRaces(setNXReproKey) {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newReadSetReproHarness(t, "g7x", tc.opts...)
			ctx := h.ctx
			key := []byte(setNXReproKey)

			require.NoError(t, h.clientB.Set(ctx, "g7x-warm", "0", 0).Err())
			if tc.seed != nil {
				require.NoError(t, tc.seed(ctx, h.clientB))
			}

			park := h.reads.addPark("A: first read of the key", func(_ string, readKey []byte) bool {
				return bytes.Contains(readKey, key)
			})
			aDone := h.goCommand(func() (any, error) {
				return tc.held(ctx, h.clientA)
			})
			waitReadSetReproParked(t, park, aDone, "A")

			bErr := tc.winner(ctx, h.clientB)
			h.log.record("B  winner -> %v", bErr)
			require.NoError(t, bErr)

			park.releaseNow()
			aRes := waitReadSetReproDone(t, aDone, "A")
			h.log.record("A  returned %s", aRes)

			var final *string
			val, getErr := h.clientB.Get(ctx, setNXReproKey).Result()
			switch {
			case errors.Is(getErr, redis.Nil):
			case getErr != nil && strings.Contains(getErr.Error(), "WRONGTYPE"):
				wrong := "WRONGTYPE"
				final = &wrong
			default:
				require.NoError(t, getErr)
				final = &val
			}
			t.Logf("event log:\n%s", h.log)
			t.Logf("live raw keys containing %q:\n%s", setNXReproKey, h.rawKeysContaining(t, key))
			tc.check(t, aRes, final)
		})
	}
}

// multiProbeRace is one "MULTI { probe k; SET out 1 } held after its probe
// read, while another command changes what the probe observed" interleaving.
type multiProbeRace struct {
	name   string
	seed   func(ctx context.Context, c *redis.Client) error
	probe  func(ctx context.Context, pipe redis.Pipeliner) func() string
	winner func(ctx context.Context, c *redis.Client) error
	stale  string
	fresh  string
}

func multiProbeRaces(key string) []multiProbeRace {
	exists := func(ctx context.Context, pipe redis.Pipeliner) func() string {
		cmd := pipe.Exists(ctx, key)
		return func() string { return strconv.FormatInt(cmd.Val(), 10) }
	}
	get := func(ctx context.Context, pipe redis.Pipeliner) func() string {
		cmd := pipe.Get(ctx, key)
		return func() string {
			switch err := cmd.Err(); {
			case errors.Is(err, redis.Nil):
				return "nil"
			case err != nil:
				return "err"
			default:
				return cmd.Val()
			}
		}
	}
	expire := func(ctx context.Context, pipe redis.Pipeliner) func() string {
		cmd := pipe.Do(ctx, "EXPIRE", key, 100)
		return func() string {
			n, err := cmd.Int64()
			if err != nil {
				return "err"
			}
			return strconv.FormatInt(n, 10)
		}
	}
	hset := func(ctx context.Context, c *redis.Client) error { return c.HSet(ctx, key, "f", "v").Err() }
	sadd := func(ctx context.Context, c *redis.Client) error { return c.SAdd(ctx, key, "m").Err() }
	zadd := func(ctx context.Context, c *redis.Client) error {
		return c.ZAdd(ctx, key, redis.Z{Score: 1, Member: "m"}).Err()
	}
	del := func(ctx context.Context, c *redis.Client) error { return c.Del(ctx, key).Err() }
	return []multiProbeRace{
		{name: "EXISTS of an absent key vs HSET", probe: exists, winner: hset, stale: "0", fresh: "1"},
		{name: "GET of an absent key vs ZADD", probe: get, winner: zadd, stale: "nil", fresh: "err"},
		{name: "EXPIRE of an absent key vs SADD", probe: expire, winner: sadd, stale: "0", fresh: "1"},
		// A present collection is already covered: every DEL and every
		// emptying path that goes through the logical delete tombstones the
		// list meta key, one of the nine anchors the probe records. (An HDEL
		// of the last field leaves the hash's length deltas behind, and the
		// type probe keeps reporting the key as a hash until compaction, so it
		// does not change the probe's answer.) Kept as pins.
		{name: "EXISTS of a hash vs DEL", seed: hset, probe: exists, winner: del, stale: "1", fresh: "0"},
		{name: "GET of a set vs DEL", seed: sadd, probe: get, winner: del, stale: "err", fresh: "nil"},
	}
}

// TestRedis_MultiTypeProbeIsValidated checks that a MULTI whose probe of a key
// (EXISTS, GET, EXPIRE) observed it absent, or holding a collection, is
// validated at commit.
//
// Client A runs MULTI { <probe k>; SET out 1 } and is held right after its
// first storage read of k. Client B creates k as a wide-column collection, or
// deletes the collection k held, and commits. The probe records the nine type
// anchors (string, HLL, bare, list meta, legacy blobs, stream meta), none of
// which a wide-column hash / set / zset create writes; the txn-wide fences,
// which it does write, are not recorded. A is released and, unless its read
// set sees B, commits SET out above B's commit timestamp with a probe reply
// taken before it. A conflict may surface at the FSM or in the adapter's
// pre-dispatch read-set check, so the test asserts only the reply of the
// attempt that committed.
func TestRedis_MultiTypeProbeIsValidated(t *testing.T) {
	t.Parallel()

	for _, tc := range multiProbeRaces(setNXReproKey) {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newReadSetReproHarness(t, "g7m")
			ctx := h.ctx
			key := []byte(setNXReproKey)
			outStrKey := redisStrKey([]byte(setNXReproOut))

			require.NoError(t, h.clientB.Set(ctx, "g7m-warm", "0", 0).Err())
			if tc.seed != nil {
				require.NoError(t, tc.seed(ctx, h.clientB))
			}

			park := h.reads.addPark("A: first read of the probed key", func(_ string, readKey []byte) bool {
				return bytes.Contains(readKey, key) && !bytes.Contains(readKey, []byte(setNXReproOut))
			})
			var probeReply func() string
			aDone := h.goCommand(func() (any, error) {
				pipe := h.clientA.TxPipeline()
				probeReply = tc.probe(ctx, pipe)
				pipe.Set(ctx, setNXReproOut, "1", 0)
				// Exec reports the first failed command's error: WRONGTYPE or
				// redis.Nil from the probe is its reply, not a failure.
				_, err := pipe.Exec(ctx)
				if err != nil && !errors.Is(err, redis.Nil) && !strings.Contains(err.Error(), "WRONGTYPE") {
					return nil, err
				}
				return probeReply(), nil
			})
			waitReadSetReproParked(t, park, aDone, "A (MULTI)")

			bErr := tc.winner(ctx, h.clientB)
			h.log.record("B  winner -> %v", bErr)
			require.NoError(t, bErr)

			park.releaseNow()
			aRes := waitReadSetReproDone(t, aDone, "A (MULTI)")
			h.log.record("A  EXEC returned %s", aRes)

			attempts := h.proposer.allWriting(outStrKey)
			t.Logf("event log:\n%s", h.log)
			for n, attempt := range attempts {
				t.Logf("A attempt %d: startTS=%d commitTS=%d reads=%s outcome=%s",
					n+1, attempt.startTS, attempt.commitTS, readSetReproKeys(attempt.reads), attempt.outcome)
			}
			require.NoError(t, aRes.err)
			require.NotEmpty(t, attempts, "A never proposed a write of %q", setNXReproOut)

			if aRes.val == tc.stale {
				t.Errorf("MULTI committed with probe reply %q taken before a concurrent write committed below its commit "+
					"(first attempt outcome: %s); the probe was not in the read set", tc.stale, attempts[0].outcome)
				return
			}
			require.Equal(t, tc.fresh, aRes.val, "the retried MULTI must observe the concurrent write")
		})
	}
}

// TestRedis_SetExReplacesAnyEncoding checks that SETEX over a key holding a
// collection or an HLL replaces it, as SET EX does.
//
// SETEX writes the string anchor and its TTL index as a blind, non
// transactional write, without probing the key's type: the old encoding
// survives next to the new string, and, since nothing is read or fenced, a
// concurrent create of another encoding cannot conflict with it either.
func TestRedis_SetExReplacesAnyEncoding(t *testing.T) {
	t.Parallel()

	for name, create := range setNXReproCreators(setNXReproKey) {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			h := newReadSetReproHarness(t, "g7e")
			ctx := h.ctx
			key := []byte(setNXReproKey)

			_, err := create(ctx, h.clientB)
			require.NoError(t, err)
			require.NoError(t, h.clientB.Do(ctx, "SETEX", setNXReproKey, 100, "s").Err())

			encodings := h.physicalEncodings(t, key, h.st.LastCommitTS())
			t.Logf("live raw keys containing %q:\n%s", setNXReproKey, h.rawKeysContaining(t, key))
			require.Equal(t, []string{"string"}, encodings, "SETEX must replace the %s it overwrote", name)
			val, err := h.clientB.Get(ctx, setNXReproKey).Result()
			require.NoError(t, err)
			require.Equal(t, "s", val)
		})
	}
}
