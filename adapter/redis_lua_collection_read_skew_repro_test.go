package adapter

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/store"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

const (
	luaCollReadSkewColl = "g3c-coll"
	luaCollReadSkewOut  = "g3c-out"
)

// luaCollectionReadSkewCase is one "script reads a collection, writes an
// unrelated key derived from the read" interleaving.
type luaCollectionReadSkewCase struct {
	name string
	// seed builds the collection before the script runs.
	seed func(ctx context.Context, c *redis.Client) error
	// script reads KEYS[1] and SETs KEYS[2] to a string derived from the read.
	script string
	// concurrent changes what the script read; it commits while the script is
	// held after its first read of KEYS[1].
	concurrent func(ctx context.Context, c *redis.Client) error
	// stale is what KEYS[2] holds when the script committed its first,
	// unvalidated attempt; fresh is what a retry that observes the concurrent
	// write produces.
	stale string
	fresh string
}

func luaCollectionReadSkewCases(coll string) []luaCollectionReadSkewCase {
	return []luaCollectionReadSkewCase{
		{
			// Point reads still record the field key even though HSET now
			// advances the whole-hash fence as well.
			name: "HGET vs update-only HSET",
			seed: func(ctx context.Context, c *redis.Client) error {
				return c.HSet(ctx, coll, "f", "v1").Err()
			},
			script: "local v=redis.call('HGET',KEYS[1],'f'); redis.call('SET',KEYS[2], v or 'none'); return v",
			concurrent: func(ctx context.Context, c *redis.Client) error {
				return c.HSet(ctx, coll, "f", "v2").Err()
			},
			stale: "v1",
			fresh: "v2",
		},
		{
			name: "HGETALL vs HSET of a new field",
			seed: func(ctx context.Context, c *redis.Client) error {
				return c.HSet(ctx, coll, "f", "v1").Err()
			},
			script: "local v=redis.call('HGETALL',KEYS[1]); redis.call('SET',KEYS[2], table.concat(v, ',')); return #v",
			concurrent: func(ctx context.Context, c *redis.Client) error {
				return c.HSet(ctx, coll, "g", "v2").Err()
			},
			stale: "f,v1",
			fresh: "f,v1,g,v2",
		},
		{
			// Update-only HSET advances the whole-hash fence.
			name: "HGETALL vs update-only HSET",
			seed: func(ctx context.Context, c *redis.Client) error {
				return c.HSet(ctx, coll, "f", "v1").Err()
			},
			script: "local v=redis.call('HGETALL',KEYS[1]); redis.call('SET',KEYS[2], table.concat(v, ',')); return #v",
			concurrent: func(ctx context.Context, c *redis.Client) error {
				return c.HSet(ctx, coll, "f", "v2").Err()
			},
			stale: "f,v1",
			fresh: "f,v2",
		},
		{
			// A non-emptying HDEL deletes the field key and writes a unique
			// delta key; HLEN records neither, only the hash fence.
			name: "HLEN vs non-emptying HDEL",
			seed: func(ctx context.Context, c *redis.Client) error {
				return c.HSet(ctx, coll, "f", "v1", "g", "v2").Err()
			},
			script: "local v=redis.call('HLEN',KEYS[1]); redis.call('SET',KEYS[2], tostring(v)); return v",
			concurrent: func(ctx context.Context, c *redis.Client) error {
				return c.HDel(ctx, coll, "g").Err()
			},
			stale: "2",
			fresh: "1",
		},
		{
			name: "HGETALL vs non-emptying HDEL",
			seed: func(ctx context.Context, c *redis.Client) error {
				return c.HSet(ctx, coll, "f", "v1", "g", "v2").Err()
			},
			script: "local v=redis.call('HGETALL',KEYS[1]); redis.call('SET',KEYS[2], table.concat(v, ',')); return #v",
			concurrent: func(ctx context.Context, c *redis.Client) error {
				return c.HDel(ctx, coll, "g").Err()
			},
			stale: "f,v1,g,v2",
			fresh: "f,v1",
		},
		{
			name: "LRANGE vs RPUSH",
			seed: func(ctx context.Context, c *redis.Client) error {
				return c.RPush(ctx, coll, "a").Err()
			},
			script: "local v=redis.call('LRANGE',KEYS[1],0,-1); redis.call('SET',KEYS[2], table.concat(v, ',')); return #v",
			concurrent: func(ctx context.Context, c *redis.Client) error {
				return c.RPush(ctx, coll, "b").Err()
			},
			stale: "a",
			fresh: "a,b",
		},
		{
			name: "SMEMBERS vs SADD",
			seed: func(ctx context.Context, c *redis.Client) error {
				return c.SAdd(ctx, coll, "a").Err()
			},
			script: "local v=redis.call('SMEMBERS',KEYS[1]); table.sort(v); redis.call('SET',KEYS[2], table.concat(v, ',')); return #v",
			concurrent: func(ctx context.Context, c *redis.Client) error {
				return c.SAdd(ctx, coll, "b").Err()
			},
			stale: "a",
			fresh: "a,b",
		},
		{
			name: "SISMEMBER vs SADD",
			seed: func(ctx context.Context, c *redis.Client) error {
				return c.SAdd(ctx, coll, "a").Err()
			},
			script: "local v=redis.call('SISMEMBER',KEYS[1],'b'); redis.call('SET',KEYS[2], tostring(v)); return v",
			concurrent: func(ctx context.Context, c *redis.Client) error {
				return c.SAdd(ctx, coll, "b").Err()
			},
			stale: "0",
			fresh: "1",
		},
		{
			name: "ZSCORE vs ZADD score update",
			seed: func(ctx context.Context, c *redis.Client) error {
				return c.ZAdd(ctx, coll, redis.Z{Score: 1, Member: "m"}).Err()
			},
			script: "local v=redis.call('ZSCORE',KEYS[1],'m'); redis.call('SET',KEYS[2], v or 'none'); return v",
			concurrent: func(ctx context.Context, c *redis.Client) error {
				return c.ZAdd(ctx, coll, redis.Z{Score: 2, Member: "m"}).Err()
			},
			stale: "1",
			fresh: "2",
		},
		// Type probes that resolve to a collection (G7): EXISTS / TYPE / PTTL
		// recorded nothing for a hash, list, set, zset, or stream, so a DEL or
		// type change committing under the script went unseen.
		{
			name: "TYPE of a hash vs DEL and SET",
			seed: func(ctx context.Context, c *redis.Client) error {
				return c.HSet(ctx, coll, "f", "v1").Err()
			},
			script: "local v=redis.call('TYPE',KEYS[1])['ok']; redis.call('SET',KEYS[2], v); return v",
			concurrent: luaReadSkewDelThen(coll, func(ctx context.Context, c *redis.Client) error {
				return c.Set(ctx, coll, "s", 0).Err()
			}),
			stale: "hash",
			fresh: "string",
		},
		{
			name: "TYPE of a zset vs DEL",
			seed: func(ctx context.Context, c *redis.Client) error {
				return c.ZAdd(ctx, coll, redis.Z{Score: 1, Member: "m"}).Err()
			},
			script:     "local v=redis.call('TYPE',KEYS[1])['ok']; redis.call('SET',KEYS[2], v); return v",
			concurrent: luaReadSkewDelThen(coll, nil),
			stale:      "zset",
			fresh:      "none",
		},
		{
			name: "TYPE of a stream vs DEL",
			seed: func(ctx context.Context, c *redis.Client) error {
				return c.XAdd(ctx, &redis.XAddArgs{Stream: coll, ID: "1-1", Values: []any{"f", "v1"}}).Err()
			},
			script:     "local v=redis.call('TYPE',KEYS[1])['ok']; redis.call('SET',KEYS[2], v); return v",
			concurrent: luaReadSkewDelThen(coll, nil),
			stale:      "stream",
			fresh:      "none",
		},
		{
			name: "EXISTS of a list vs DEL",
			seed: func(ctx context.Context, c *redis.Client) error {
				return c.RPush(ctx, coll, "a").Err()
			},
			script:     "local v=redis.call('EXISTS',KEYS[1]); redis.call('SET',KEYS[2], tostring(v)); return v",
			concurrent: luaReadSkewDelThen(coll, nil),
			stale:      "1",
			fresh:      "0",
		},
		{
			name: "PTTL of a set vs DEL",
			seed: func(ctx context.Context, c *redis.Client) error {
				return c.SAdd(ctx, coll, "a").Err()
			},
			script:     "local v=redis.call('PTTL',KEYS[1]); redis.call('SET',KEYS[2], tostring(v)); return v",
			concurrent: luaReadSkewDelThen(coll, nil),
			stale:      "-1",
			fresh:      "-2",
		},
		{
			name: "PTTL of a set vs PEXPIRE",
			seed: func(ctx context.Context, c *redis.Client) error {
				return c.SAdd(ctx, coll, "a").Err()
			},
			script: "local ttl=redis.call('PTTL',KEYS[1]); local v=ttl < 0 and 'no-ttl' or 'ttl'; redis.call('SET',KEYS[2],v); return v",
			concurrent: func(ctx context.Context, c *redis.Client) error {
				return c.PExpire(ctx, coll, time.Hour).Err()
			},
			stale: "no-ttl",
			fresh: "ttl",
		},
		// WRONGTYPE under pcall (G7): the failed read recorded nothing (or, for
		// GET, only the string anchors), so a script that caught the error and
		// wrote committed against a DEL or type change of the key.
		{
			name: "pcall GET of a hash vs DEL",
			seed: func(ctx context.Context, c *redis.Client) error {
				return c.HSet(ctx, coll, "f", "v1").Err()
			},
			script:     luaReadSkewPcallGet,
			concurrent: luaReadSkewDelThen(coll, nil),
			stale:      "err",
			fresh:      "nil",
		},
		{
			// GET records the string key, which the SET rewrites, so this one
			// is already caught; kept to pin it.
			name: "pcall GET of a hash vs DEL and SET",
			seed: func(ctx context.Context, c *redis.Client) error {
				return c.HSet(ctx, coll, "f", "v1").Err()
			},
			script: luaReadSkewPcallGet,
			concurrent: luaReadSkewDelThen(coll, func(ctx context.Context, c *redis.Client) error {
				return c.Set(ctx, coll, "s", 0).Err()
			}),
			stale: "err",
			fresh: "s",
		},
		{
			name: "redis.pcall HGET of a string vs DEL and HSET",
			seed: func(ctx context.Context, c *redis.Client) error {
				return c.Set(ctx, coll, "s", 0).Err()
			},
			script: "local r=redis.pcall('HGET',KEYS[1],'f'); local v; if type(r)=='table' then v='err' else v=r or 'nil' end; " +
				"redis.call('SET',KEYS[2], v); return v",
			concurrent: luaReadSkewDelThen(coll, func(ctx context.Context, c *redis.Client) error {
				return c.HSet(ctx, coll, "f", "v2").Err()
			}),
			stale: "err",
			fresh: "v2",
		},
		{
			name: "pcall LLEN of a set vs DEL",
			seed: func(ctx context.Context, c *redis.Client) error {
				return c.SAdd(ctx, coll, "a").Err()
			},
			script: "local ok, r = pcall(redis.call, 'LLEN', KEYS[1]); local v = ok and tostring(r) or 'err'; " +
				"redis.call('SET',KEYS[2], v); return v",
			concurrent: luaReadSkewDelThen(coll, nil),
			stale:      "err",
			fresh:      "0",
		},
		// Existence checks that skip the write (G7): SET NX / XX, SETNX, DEL,
		// PEXPIRE, and RENAME decide on existence through a probe that
		// recorded nothing, and on the branch that does not write the key the
		// script's commit validated nothing about it.
		{
			name: "SET NX of a string vs DEL",
			seed: func(ctx context.Context, c *redis.Client) error {
				return c.Set(ctx, coll, "s", 0).Err()
			},
			script:     "local r=redis.call('SET',KEYS[1],'a','NX'); local v = r and 'set' or 'skipped'; redis.call('SET',KEYS[2], v); return v",
			concurrent: luaReadSkewDelThen(coll, nil),
			stale:      "skipped",
			fresh:      "set",
		},
		{
			name: "SETNX of a hash vs DEL",
			seed: func(ctx context.Context, c *redis.Client) error {
				return c.HSet(ctx, coll, "f", "v1").Err()
			},
			script:     "local r=redis.call('SETNX',KEYS[1],'a'); redis.call('SET',KEYS[2], tostring(r)); return r",
			concurrent: luaReadSkewDelThen(coll, nil),
			stale:      "0",
			fresh:      "1",
		},
		{
			name:       "SET XX of an absent key vs SET",
			seed:       luaReadSkewWarm,
			script:     "local r=redis.call('SET',KEYS[1],'a','XX'); local v = r and 'set' or 'skipped'; redis.call('SET',KEYS[2], v); return v",
			concurrent: luaReadSkewSetColl(coll),
			stale:      "skipped",
			fresh:      "set",
		},
		{
			name:       "DEL of an absent key vs SET",
			seed:       luaReadSkewWarm,
			script:     "local n=redis.call('DEL',KEYS[1]); redis.call('SET',KEYS[2], tostring(n)); return n",
			concurrent: luaReadSkewSetColl(coll),
			stale:      "0",
			fresh:      "1",
		},
		{
			name:       "PEXPIRE of an absent key vs SET",
			seed:       luaReadSkewWarm,
			script:     "local n=redis.call('PEXPIRE',KEYS[1],100000); redis.call('SET',KEYS[2], tostring(n)); return n",
			concurrent: luaReadSkewSetColl(coll),
			stale:      "0",
			fresh:      "1",
		},
		{
			name: "pcall RENAME of an absent key vs SET",
			seed: luaReadSkewWarm,
			script: "local ok = pcall(redis.call, 'RENAME', KEYS[1], KEYS[1] .. '-dst'); local v = ok and 'renamed' or 'err'; " +
				"redis.call('SET',KEYS[2], v); return v",
			concurrent: luaReadSkewSetColl(coll),
			stale:      "err",
			fresh:      "renamed",
		},
		{
			name: "XLEN vs XADD",
			seed: func(ctx context.Context, c *redis.Client) error {
				return c.XAdd(ctx, &redis.XAddArgs{Stream: coll, ID: "1-1", Values: []any{"f", "v1"}}).Err()
			},
			script: "local v=redis.call('XLEN',KEYS[1]); redis.call('SET',KEYS[2], tostring(v)); return v",
			concurrent: func(ctx context.Context, c *redis.Client) error {
				return c.XAdd(ctx, &redis.XAddArgs{Stream: coll, ID: "1-2", Values: []any{"f", "v2"}}).Err()
			},
			stale: "1",
			fresh: "2",
		},
	}
}

// luaReadSkewPcallGet GETs KEYS[1] under pcall and SETs KEYS[2] to the value,
// "nil", or "err" (WRONGTYPE).
const luaReadSkewPcallGet = "local ok, r = pcall(redis.call, 'GET', KEYS[1]); local v = ok and (r or 'nil') or 'err'; " +
	"redis.call('SET',KEYS[2], v); return v"

// luaReadSkewDelThen DELs coll and then runs next, if any, as separate
// commands that all commit while the script is held.
func luaReadSkewDelThen(coll string, next func(ctx context.Context, c *redis.Client) error) func(ctx context.Context, c *redis.Client) error {
	return func(ctx context.Context, c *redis.Client) error {
		if err := c.Del(ctx, coll).Err(); err != nil {
			return err
		}
		if next == nil {
			return nil
		}
		return next(ctx, c)
	}
}

// luaReadSkewWarm commits an unrelated key so the script's startTS is a real
// HLC timestamp when the case has no collection to seed.
func luaReadSkewWarm(ctx context.Context, c *redis.Client) error {
	return c.Set(ctx, "g3c-warm", "0", 0).Err()
}

// luaReadSkewSetColl creates coll as a string.
func luaReadSkewSetColl(coll string) func(ctx context.Context, c *redis.Client) error {
	return func(ctx context.Context, c *redis.Client) error {
		return c.Set(ctx, coll, "s", 0).Err()
	}
}

// allWriting returns every proposal whose write set contains key, in
// proposal order.
func (p *readSetReproProposer) allWriting(key []byte) []readSetReproProposal {
	p.mu.Lock()
	defer p.mu.Unlock()
	var out []readSetReproProposal
	for _, proposal := range p.proposals {
		if readSetReproContainsKey(proposal.writes, key) {
			out = append(out, proposal)
		}
	}
	return out
}

// TestLua_CollectionReadOfUnwrittenKeyIsNotValidated checks that a Lua
// script's read of a collection it does not write is validated at commit.
//
// Variant: interleaving test over the read-set reproduction harness in
// redis_lua_read_skew_repro_test.go (hold at a read, not at Propose; see the
// harness comment for why).
//
// Client A runs EVAL "read KEYS[1] (a hash / list / set / zset / stream);
// SET KEYS[2] <derived from the read>". A is held right after its first
// storage read of KEYS[1], so it has already fixed its snapshot. Client B then
// changes what A read, and commits. A is released and commits KEYS[2] at a
// commit timestamp above B's. If the read is in A's read set, A's first attempt
// fails with a write conflict and the adapter's retry re-runs the script,
// which observes B's write; KEYS[2] then holds the fresh value. If A instead
// commits the stale value, the snapshot at A's commit timestamp holds B's write
// next to a value derived from the state before it -- a state no serial order
// of A and B produces.
func TestLua_CollectionReadOfUnwrittenKeyIsNotValidated(t *testing.T) {
	t.Parallel()

	for _, tc := range luaCollectionReadSkewCases(luaCollReadSkewColl) {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newReadSetReproHarness(t, "g3c")
			ctx := h.ctx
			const coll = luaCollReadSkewColl
			const out = luaCollReadSkewOut
			collKey := []byte(coll)
			outStrKey := redisStrKey([]byte(out))

			require.NoError(t, tc.seed(ctx, h.clientB))
			h.log.record("B  seed %s -> OK", coll)

			park := h.reads.addPark("A: first read of the collection", func(_ string, key []byte) bool {
				return bytes.Contains(key, collKey)
			})

			// (1) A runs the script and is held after its first read.
			aDone := h.goCommand(func() (any, error) {
				return h.clientA.Eval(ctx, tc.script, []string{coll, out}).Result()
			})
			waitReadSetReproParked(t, park, aDone, "A (EVAL)")

			// (2) B changes what A read; it commits while A is held.
			bErr := tc.concurrent(ctx, h.clientB)
			h.log.record("B  concurrent write -> %v", bErr)
			require.NoError(t, bErr)

			// (3) Release A.
			park.releaseNow()
			aRes := waitReadSetReproDone(t, aDone, "A (EVAL)")
			h.log.record("A  EVAL returned %s", aRes)

			got, getErr := h.clientB.Get(ctx, out).Result()
			attempts := h.proposer.allWriting(outStrKey)

			t.Logf("event log:\n%s", h.log)
			for n, attempt := range attempts {
				t.Logf("A attempt %d: startTS=%d commitTS=%d reads=%s outcome=%s",
					n+1, attempt.startTS, attempt.commitTS, readSetReproKeys(attempt.reads), attempt.outcome)
			}
			require.NoError(t, aRes.err)
			require.NoError(t, getErr)
			require.NotEmpty(t, attempts, "A never proposed a write of %q", out)

			if got == tc.stale {
				t.Errorf("Lua script committed %s=%q derived from a read of %s taken before a concurrent write committed below the script's commit "+
					"(first attempt outcome: %s); the collection read was not in the read set",
					out, got, coll, attempts[0].outcome)
				return
			}
			require.Equal(t, tc.fresh, got, "the retried script must observe the concurrent write")
			require.True(t, strings.Contains(attempts[0].outcome, "FSM error"),
				"A's first attempt must fail validation, got %s", attempts[0].outcome)
		})
	}
}

// TestLua_ReadSetCoversCollectionReads pins the read keys each Lua collection
// read adds to the script's commit, on the fast and the slow paths: an absent
// key records the absent-key set; a list / set records its type fence; a hash
// or zset also records its legacy blob anchor; a stream records its meta key;
// point reads of sets and zsets add the element key; hash reads use the
// whole-hash fence and blob anchor.
// Each script also SETs an unrelated key so it dispatches; that key
// contributes only its own creation fences.
func TestLua_ReadSetCoversCollectionReads(t *testing.T) {
	t.Parallel()

	readKey := []byte("lua:collread:read")
	writeKey := []byte("lua:collread:write")
	absent := redisTxnReadFencePointKeys(readKey)
	with := func(base [][]byte, extra ...[]byte) [][]byte {
		return append(append([][]byte{}, base...), extra...)
	}
	hash := [][]byte{redisTxnWideHashFenceKey(readKey), redisHashKey(readKey)}
	list := [][]byte{redisTxnWideListFenceKey(readKey)}
	set := [][]byte{redisTxnWideSetFenceKey(readKey)}
	zset := [][]byte{redisTxnWideZSetFenceKey(readKey), redisZSetKey(readKey)}
	stream := [][]byte{store.StreamMetaKey(readKey)}

	const (
		seedHash   = `redis.call("HSET", KEYS[1], "f", "v")`
		seedHash2  = `redis.call("HSET", KEYS[1], "f", "v", "g", "w")`
		seedList   = `redis.call("RPUSH", KEYS[1], "a", "b")`
		seedSet    = `redis.call("SADD", KEYS[1], "a")`
		seedZSet   = `redis.call("ZADD", KEYS[1], 1, "m")`
		seedStream = `redis.call("XADD", KEYS[1], "1-1", "f", "v")`
	)
	cases := []struct {
		name string
		seed string
		read string
		want [][]byte
	}{
		{name: "HGET hit (fast path)", seed: seedHash, read: `redis.call("HGET", KEYS[1], "f")`, want: hash},
		{name: "HGET missing field (slow path)", seed: seedHash, read: `redis.call("HGET", KEYS[1], "g")`, want: hash},
		{name: "HGET absent", read: `redis.call("HGET", KEYS[1], "f")`, want: absent},
		{name: "HGET after cached absent", read: `redis.call("GET", KEYS[1]); redis.call("HGET", KEYS[1], "f")`, want: absent},
		{name: "HEXISTS hit (fast path)", seed: seedHash, read: `redis.call("HEXISTS", KEYS[1], "f")`, want: hash},
		{name: "HEXISTS absent", read: `redis.call("HEXISTS", KEYS[1], "f")`, want: absent},
		{name: "HMGET", seed: seedHash, read: `redis.call("HMGET", KEYS[1], "f", "g")`, want: hash},
		{name: "HMGET absent", read: `redis.call("HMGET", KEYS[1], "f", "g")`, want: absent},
		{name: "HGETALL", seed: seedHash, read: `redis.call("HGETALL", KEYS[1])`, want: hash},
		{name: "HGETALL two fields", seed: seedHash2, read: `redis.call("HGETALL", KEYS[1])`, want: hash},
		{name: "HLEN two fields", seed: seedHash2, read: `redis.call("HLEN", KEYS[1])`, want: hash},
		{name: "HGETALL absent", read: `redis.call("HGETALL", KEYS[1])`, want: absent},
		{name: "HLEN", seed: seedHash, read: `redis.call("HLEN", KEYS[1])`, want: hash},
		{name: "LRANGE", seed: seedList, read: `redis.call("LRANGE", KEYS[1], 0, -1)`, want: list},
		{name: "LRANGE absent", read: `redis.call("LRANGE", KEYS[1], 0, -1)`, want: absent},
		{name: "LINDEX", seed: seedList, read: `redis.call("LINDEX", KEYS[1], 0)`, want: list},
		{name: "LLEN", seed: seedList, read: `redis.call("LLEN", KEYS[1])`, want: list},
		{name: "LLEN absent", read: `redis.call("LLEN", KEYS[1])`, want: absent},
		{name: "LPOS", seed: seedList, read: `redis.call("LPOS", KEYS[1], "b")`, want: list},
		{name: "SMEMBERS", seed: seedSet, read: `redis.call("SMEMBERS", KEYS[1])`, want: set},
		{name: "SMEMBERS absent", read: `redis.call("SMEMBERS", KEYS[1])`, want: absent},
		{name: "SCARD", seed: seedSet, read: `redis.call("SCARD", KEYS[1])`, want: set},
		{name: "PTTL set", seed: seedSet, read: `redis.call("PTTL", KEYS[1])`, want: with(set, redisTTLKey(readKey))},
		{name: "PTTL absent", read: `redis.call("PTTL", KEYS[1])`, want: absent},
		{name: "SISMEMBER hit (fast path)", seed: seedSet, read: `redis.call("SISMEMBER", KEYS[1], "a")`, want: set},
		{name: "SISMEMBER miss (slow path)", seed: seedSet, read: `redis.call("SISMEMBER", KEYS[1], "b")`, want: set},
		{name: "SISMEMBER absent", read: `redis.call("SISMEMBER", KEYS[1], "a")`, want: absent},
		{name: "ZSCORE hit (fast path)", seed: seedZSet, read: `redis.call("ZSCORE", KEYS[1], "m")`, want: zset},
		{name: "ZSCORE miss (slow path)", seed: seedZSet, read: `redis.call("ZSCORE", KEYS[1], "x")`, want: zset},
		{name: "ZSCORE absent", read: `redis.call("ZSCORE", KEYS[1], "m")`, want: absent},
		{name: "ZRANGE", seed: seedZSet, read: `redis.call("ZRANGE", KEYS[1], 0, -1)`, want: zset},
		{name: "ZREVRANGE", seed: seedZSet, read: `redis.call("ZREVRANGE", KEYS[1], 0, -1)`, want: zset},
		{name: "ZCARD", seed: seedZSet, read: `redis.call("ZCARD", KEYS[1])`, want: zset},
		{name: "ZCARD absent", read: `redis.call("ZCARD", KEYS[1])`, want: absent},
		{name: "ZCOUNT", seed: seedZSet, read: `redis.call("ZCOUNT", KEYS[1], "-inf", "+inf")`, want: zset},
		{name: "ZRANGEBYSCORE hit (fast path)", seed: seedZSet, read: `redis.call("ZRANGEBYSCORE", KEYS[1], "-inf", "+inf")`, want: zset},
		// An empty fast-path hit cannot tell an empty range from an absent
		// key, so it records the absent-key set, a superset of both.
		{name: "ZRANGEBYSCORE empty range (fast path)", seed: seedZSet, read: `redis.call("ZRANGEBYSCORE", KEYS[1], 5, 10)`, want: absent},
		{name: "ZRANGEBYSCORE absent", read: `redis.call("ZRANGEBYSCORE", KEYS[1], "-inf", "+inf")`, want: absent},
		{name: "ZREVRANGEBYSCORE", seed: seedZSet, read: `redis.call("ZREVRANGEBYSCORE", KEYS[1], "+inf", "-inf")`, want: zset},
		{name: "XLEN", seed: seedStream, read: `redis.call("XLEN", KEYS[1])`, want: stream},
		{name: "XLEN absent", read: `redis.call("XLEN", KEYS[1])`, want: absent},
		{name: "XRANGE", seed: seedStream, read: `redis.call("XRANGE", KEYS[1], "-", "+")`, want: stream},
		{name: "XREVRANGE", seed: seedStream, read: `redis.call("XREVRANGE", KEYS[1], "+", "-")`, want: stream},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			st := store.NewMVCCStore()
			coord := newReadKeyRecordingCoordinator(st)
			server := NewRedisServer(nil, "", st, coord, nil, nil)
			if tc.seed != "" {
				conn := &recordingConn{}
				server.runLuaScript(conn, tc.seed, [][]byte{[]byte("1"), readKey})
				require.Empty(t, conn.err)
				coord.dispatches = 0
				coord.lastReadKeys = nil
			}

			conn := &recordingConn{}
			script := tc.read + `; return redis.call("SET", KEYS[2], "w")`
			server.runLuaScript(conn, script, [][]byte{[]byte("2"), readKey, writeKey})
			require.Empty(t, conn.err)
			require.Equal(t, 1, coord.dispatches)

			want := append(append([][]byte{}, tc.want...), redisTxnWideCollectionFenceKeys(writeKey)...)
			requireReadKeysMatch(t, coord.lastReadKeys, want)
		})
	}
}

func TestLuaHGetAllReadSetBoundedAboveMaxReadKeys(t *testing.T) {
	key := "lua:large-hash"
	values := make(redisHashValue, kv.MaxReadKeys+1)
	for i := 0; i <= kv.MaxReadKeys; i++ {
		values[fmt.Sprintf("f%05d", i)] = "v"
	}
	c := &luaScriptContext{hashes: map[string]*luaHashState{
		key: {loaded: true, exists: true, value: values},
	}}
	reply, err := c.cmdHGetAll([]string{key})
	require.NoError(t, err)
	require.Len(t, reply.array, 2*len(values))
	readKeys, _ := c.sortedTrackedReadKeys(0)
	require.Less(t, len(readKeys), kv.MaxReadKeys)
	requireReadKeysMatch(t, readKeys, [][]byte{
		redisTxnWideHashFenceKey([]byte(key)), redisHashKey([]byte(key)),
	})
}

func TestLuaHMGetReadSetBoundedAboveMaxReadKeys(t *testing.T) {
	key := "lua:many-missing-fields"
	args := make([]string, kv.MaxReadKeys+2)
	args[0] = key
	for i := 1; i < len(args); i++ {
		args[i] = fmt.Sprintf("f%05d", i)
	}
	c := &luaScriptContext{hashes: map[string]*luaHashState{
		key: {loaded: true, exists: true, value: redisHashValue{"seed": "value"}},
	}}
	reply, err := c.cmdHMGet(args)
	require.NoError(t, err)
	require.Len(t, reply.array, kv.MaxReadKeys+1)
	readKeys, _ := c.sortedTrackedReadKeys(0)
	requireReadKeysMatch(t, readKeys, [][]byte{
		redisTxnWideHashFenceKey([]byte(key)), redisHashKey([]byte(key)),
	})
}

func TestLuaSetAndZSetPointReadsStayBounded(t *testing.T) {
	key := "lua:many-missing-members"
	c := &luaScriptContext{
		sets:  map[string]*luaSetState{key: {loaded: true, exists: true, members: map[string]struct{}{"seed": {}}}},
		zsets: map[string]*luaZSetState{key: {loaded: true, exists: true, membersLoaded: true, members: map[string]float64{"seed": 1}}},
	}
	for i := 0; i <= kv.MaxReadKeys; i++ {
		member := fmt.Sprintf("m%05d", i)
		_, err := c.cmdSIsMember([]string{key, member})
		require.NoError(t, err)
		_, err = c.cmdZScore([]string{key, member})
		require.NoError(t, err)
	}
	readKeys, _ := c.sortedTrackedReadKeys(0)
	requireReadKeysMatch(t, readKeys, [][]byte{
		redisTxnWideSetFenceKey([]byte(key)),
		redisTxnWideZSetFenceKey([]byte(key)), redisZSetKey([]byte(key)),
	})
}

func TestHashValueOnlyWritesAdvanceWholeHashFence(t *testing.T) {
	ctx := context.Background()
	key := []byte("hash:value-fence")
	st := store.NewMVCCStore()
	coord := &putRecordingCoordinator{readKeyRecordingCoordinator: newReadKeyRecordingCoordinator(st)}
	server := NewRedisServer(nil, "", st, coord, nil, nil)
	_, err := server.applyHashFieldPairs(key, [][]byte{[]byte("f"), []byte("1")})
	require.NoError(t, err)

	for _, tc := range []struct {
		name  string
		write func() error
	}{
		{name: "HSET", write: func() error {
			_, err := server.applyHashFieldPairs(key, [][]byte{[]byte("f"), []byte("2")})
			return err
		}},
		{name: "HINCRBY", write: func() error {
			_, err := server.hincrbyTxn(ctx, key, []byte("f"), 1)
			return err
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			coord.lastPuts = nil
			require.NoError(t, tc.write())
			require.Contains(t, coord.lastPuts, redisTxnWideHashFenceKey(key))
		})
	}

	state := &hashTxnState{dirty: true, fields: map[string][]byte{"f": []byte("4")},
		origFields: map[string][]byte{"f": []byte("3")}, dirtyFields: map[string]struct{}{"f": {}}}
	txn := &txnContext{}
	elems, emitted := txn.appendHashStateElems(nil, key, state, 1, 0)
	require.False(t, emitted)
	require.True(t, elemKeysContain(elems, redisTxnWideHashFenceKey(key)))
}

// TestHDel_PutsHashFence pins that every HDEL that removes a field Puts the
// hash fence, emptying or not, on the wide-column and the legacy blob paths,
// so a reader of the whole field set (HLEN, HGETALL) that recorded the fence
// sees the removal. An HDEL that removes nothing dispatches nothing.
func TestHDel_PutsHashFence(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	key := []byte("hdel:fence")
	seedWide := func(t *testing.T, server *RedisServer, _ store.MVCCStore) {
		t.Helper()
		_, err := server.applyHashFieldPairs(key, [][]byte{[]byte("f"), []byte("v"), []byte("g"), []byte("w")})
		require.NoError(t, err)
	}
	seedLegacy := func(t *testing.T, _ *RedisServer, st store.MVCCStore) {
		t.Helper()
		raw, err := marshalHashValue(redisHashValue{"f": "v", "g": "w"})
		require.NoError(t, err)
		require.NoError(t, st.PutAt(ctx, redisHashKey(key), raw, 1, 0))
	}

	cases := []struct {
		name      string
		seed      func(*testing.T, *RedisServer, store.MVCCStore)
		fields    []string
		removed   int
		wantFence bool
	}{
		{name: "wide, non-emptying", seed: seedWide, fields: []string{"g"}, removed: 1, wantFence: true},
		{name: "wide, emptying", seed: seedWide, fields: []string{"f", "g"}, removed: 2, wantFence: true},
		{name: "wide, missing field", seed: seedWide, fields: []string{"x"}, removed: 0},
		{name: "legacy, non-emptying", seed: seedLegacy, fields: []string{"g"}, removed: 1, wantFence: true},
		{name: "legacy, emptying", seed: seedLegacy, fields: []string{"f", "g"}, removed: 2, wantFence: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			st := store.NewMVCCStore()
			coord := &putRecordingCoordinator{readKeyRecordingCoordinator: newReadKeyRecordingCoordinator(st)}
			server := NewRedisServer(nil, "", st, coord, nil, nil)
			tc.seed(t, server, st)
			coord.dispatches = 0
			coord.lastPuts = nil

			fields := make([][]byte, 0, len(tc.fields))
			for _, field := range tc.fields {
				fields = append(fields, []byte(field))
			}
			removed, err := server.hdelTxn(ctx, key, fields)
			require.NoError(t, err)
			require.Equal(t, tc.removed, removed)
			if !tc.wantFence {
				require.Zero(t, coord.dispatches)
				return
			}
			require.Equal(t, 1, coord.dispatches)
			var fencePuts [][]byte
			for _, putKey := range coord.lastPuts {
				if redisTxnWideFenceUserKey(putKey) != nil {
					fencePuts = append(fencePuts, putKey)
				}
			}
			require.Contains(t, fencePuts, redisTxnWideHashFenceKey(key))
		})
	}
}

// TestLua_CaughtWrongTypeDoesNotPoisonKeyState checks that a WRONGTYPE a
// script catches leaves no state behind for the key: a later command on the
// same key in the same script sees the key as it is.
//
// Each state loader (string, list, hash, set, zset, stream) put an empty,
// unloaded state into the script's cache before it checked the type, and left
// it there when it returned WRONGTYPE. The next command of that type found the
// cached state and treated the key as an absent value of the wrong type: a
// second read replied nil / 0 instead of WRONGTYPE, and a write (HSET on a
// string, INCR on a hash) succeeded and, at commit, replaced the key's value
// with the new encoding.
func TestLua_CaughtWrongTypeDoesNotPoisonKeyState(t *testing.T) {
	t.Parallel()

	const reply = `if type(r) == "table" then return r["err"] end; return tostring(r)`
	const seedString = `redis.call("SET", KEYS[1], "s")`
	const seedHash = `redis.call("HSET", KEYS[1], "f", "v")`
	const checkString = `return redis.call("GET", KEYS[1])`
	const checkHash = `return redis.call("HGET", KEYS[1], "f")`
	cases := []struct {
		name  string
		seed  string
		probe string
		then  string
		// check reads the key back after the script; it must be unchanged.
		check string
		want  string
	}{
		{name: "HGET then HGET on a string", seed: seedString, probe: `pcall(redis.call, "HGET", KEYS[1], "f")`,
			then: `redis.pcall("HGET", KEYS[1], "f")`, check: checkString, want: "s"},
		{name: "HGET then HSET on a string", seed: seedString, probe: `pcall(redis.call, "HGET", KEYS[1], "f")`,
			then: `redis.pcall("HSET", KEYS[1], "f", "v")`, check: checkString, want: "s"},
		{name: "LLEN then RPUSH on a string", seed: seedString, probe: `pcall(redis.call, "LLEN", KEYS[1])`,
			then: `redis.pcall("RPUSH", KEYS[1], "x")`, check: checkString, want: "s"},
		{name: "SMEMBERS then SADD on a string", seed: seedString, probe: `pcall(redis.call, "SMEMBERS", KEYS[1])`,
			then: `redis.pcall("SADD", KEYS[1], "m")`, check: checkString, want: "s"},
		{name: "ZSCORE then ZADD on a string", seed: seedString, probe: `pcall(redis.call, "ZSCORE", KEYS[1], "m")`,
			then: `redis.pcall("ZADD", KEYS[1], 1, "m")`, check: checkString, want: "s"},
		{name: "XLEN then XADD on a string", seed: seedString, probe: `pcall(redis.call, "XLEN", KEYS[1])`,
			then: `redis.pcall("XADD", KEYS[1], "1-1", "f", "v")`, check: checkString, want: "s"},
		{name: "GET then GET on a hash", seed: seedHash, probe: `pcall(redis.call, "GET", KEYS[1])`,
			then: `redis.pcall("GET", KEYS[1])`, check: checkHash, want: "v"},
		{name: "GET then INCR on a hash", seed: seedHash, probe: `pcall(redis.call, "GET", KEYS[1])`,
			then: `redis.pcall("INCR", KEYS[1])`, check: checkHash, want: "v"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			st := store.NewMVCCStore()
			server := NewRedisServer(nil, "", st, newReadKeyRecordingCoordinator(st), nil, nil)
			keys := [][]byte{[]byte("1"), []byte("lua:wrongtype:key")}

			conn := &recordingConn{}
			server.runLuaScript(conn, tc.seed, keys)
			require.Empty(t, conn.err)

			conn = &recordingConn{}
			server.runLuaScript(conn, tc.probe+"; local r = "+tc.then+"; "+reply, keys)
			require.Empty(t, conn.err)
			require.Contains(t, string(conn.bulk), "WRONGTYPE", "the second command must still see the key's real type")

			conn = &recordingConn{}
			server.runLuaScript(conn, tc.check, keys)
			require.Empty(t, conn.err)
			require.Equal(t, tc.want, string(conn.bulk), "the key's value must survive the caught WRONGTYPE")
		})
	}
}
