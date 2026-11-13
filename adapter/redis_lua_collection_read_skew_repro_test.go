package adapter

import (
	"bytes"
	"context"
	"strings"
	"testing"

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
			// HSET of an existing field writes only the field key (no hash
			// fence), so only the field key in the read set can see it.
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
