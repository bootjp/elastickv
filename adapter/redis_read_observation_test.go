package adapter

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/redcon"
)

type redisReadObservationCoordinator struct {
	stubAdapterCoordinator
	keys      [][]byte
	marks     []uint64
	notLeader map[string]bool
}

func (c *redisReadObservationCoordinator) ObserveReadForKey(_ context.Context, key []byte) {
	c.keys = append(c.keys, bytes.Clone(key))
}

func (c *redisReadObservationCoordinator) MarkUnattributableClientRead(routeID uint64) {
	c.marks = append(c.marks, routeID)
}

func (c *redisReadObservationCoordinator) IsLeaderForKey(key []byte) bool {
	return !c.notLeader[string(key)] && c.stubAdapterCoordinator.IsLeaderForKey(key)
}

func TestRedisReadObservationSpecCoverage(t *testing.T) {
	// Keyless state access marks the entire read window. EVAL and EVALSHA
	// observe their nested commands, and XREAD parses dynamic key positions.
	keyless := map[string]bool{
		cmdDBSize: true, cmdKeys: true, cmdScan: true,
		cmdFlushAll: true, cmdFlushDB: true, cmdFlushLegacy: true,
		cmdEval: true, cmdEvalSHA: true,
	}
	var seenKeyless []string
	for _, spec := range redisCommandSpecs {
		if !hasRedisCommandFlag(spec.Flags, redisCmdFlagReadonly) &&
			!hasRedisCommandFlag(spec.Flags, redisCmdFlagWrite) {
			continue
		}
		if keyless[spec.Constant] {
			require.Zero(t, spec.FirstKey, spec.Constant)
			seenKeyless = append(seenKeyless, spec.Constant)
			if spec.Constant != cmdEval && spec.Constant != cmdEvalSHA {
				require.True(t, redisReadUnattributable(spec.Constant, nil), spec.Constant)
			}
			continue
		}
		if spec.Constant == cmdXRead {
			require.Zero(t, spec.FirstKey)
			continue
		}
		require.Positive(t, spec.FirstKey, spec.Constant)
		require.Positive(t, spec.Step, spec.Constant)
		require.NotEmpty(t, redisReadKeys(spec.Constant, [][]byte{[]byte(spec.Constant), []byte("k"), []byte("x"), []byte("y")}), spec.Constant)
	}
	require.ElementsMatch(t, []string{cmdDBSize, cmdKeys, cmdScan, cmdFlushAll, cmdFlushDB, cmdFlushLegacy, cmdEval, cmdEvalSHA}, seenKeyless)
}

func TestRedisReadObservationExtractsMultipleAndDynamicKeys(t *testing.T) {
	require.Equal(t, [][]byte{[]byte("a"), []byte("b")}, redisReadKeys(cmdExists, [][]byte{[]byte(cmdExists), []byte("a"), []byte("b")}))
	require.Equal(t, [][]byte{[]byte("a"), []byte("b")}, redisReadKeys(cmdDel, [][]byte{[]byte(cmdDel), []byte("a"), []byte("b")}))
	require.Equal(t, [][]byte{[]byte("a"), []byte("b")}, redisReadKeys(cmdXRead, [][]byte{[]byte(cmdXRead), []byte("COUNT"), []byte("2"), []byte("STREAMS"), []byte("a"), []byte("b"), []byte("0"), []byte("0")}))
	require.Nil(t, redisReadKeys(cmdXRead, [][]byte{[]byte(cmdXRead), []byte("STREAMS"), []byte("a")}))
	for _, name := range []string{cmdDBSize, cmdKeys, cmdScan, cmdEval, cmdExec} {
		require.Nil(t, redisReadKeys(name, [][]byte{[]byte(name), []byte("k")}), name)
	}
}

func TestRedisReadObservationCountsNoOpMutations(t *testing.T) {
	for _, tt := range []struct {
		name string
		args [][]byte
	}{
		{cmdGetDel, [][]byte{[]byte(cmdGetDel), []byte("missing")}},
		{cmdSet, [][]byte{[]byte(cmdSet), []byte("existing"), []byte("value"), []byte("NX")}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			coord := &redisReadObservationCoordinator{}
			server := &RedisServer{coordinator: coord}
			cmd := redcon.Command{Args: tt.args}
			server.dispatchCommandDirect(&recordingConn{}, tt.name, func(redcon.Conn, redcon.Command) {
				require.Empty(t, coord.keys)
			}, cmd, time.Now())
			require.Equal(t, [][]byte{redisUserRouteKey(tt.args[1])}, coord.keys)
			require.Empty(t, coord.marks)
		})
	}
}

func TestRedisReadObservationUsesRoutedKeysAndSkipsIngress(t *testing.T) {
	coord := &redisReadObservationCoordinator{}
	server := &RedisServer{coordinator: coord}
	ctx := context.Background()
	server.observeRedisRead(ctx, cmdExists, [][]byte{[]byte(cmdExists), []byte("a"), []byte("b")}, true)
	require.Equal(t, [][]byte{redisUserRouteKey([]byte("a")), redisUserRouteKey([]byte("b"))}, coord.keys)

	coord.keys = nil
	server.observeRedisRead(ctx, cmdGet, [][]byte{[]byte(cmdGet), []byte("a")}, true)
	require.Empty(t, coord.keys)
	server.observeRedisRead(ctx, cmdGet, [][]byte{[]byte(cmdGet), []byte("a")}, false)
	require.Equal(t, [][]byte{redisUserRouteKey([]byte("a"))}, coord.keys)

	coord.keys = nil
	coord.leaderSet = true
	coord.leader = false
	server.observeRedisRead(ctx, cmdHGet, [][]byte{[]byte(cmdHGet), []byte("a"), []byte("field")}, true)
	require.Empty(t, coord.keys)
}

func TestRedisReadObservationXReadAttributesLocalStreamAfterRemote(t *testing.T) {
	remote := redisUserRouteKey([]byte("remote"))
	local := redisUserRouteKey([]byte("local"))
	coord := &redisReadObservationCoordinator{notLeader: map[string]bool{string(remote): true}}
	server := &RedisServer{coordinator: coord}
	server.observeRedisRead(context.Background(), cmdXRead, [][]byte{
		[]byte(cmdXRead), []byte("STREAMS"), []byte("remote"), []byte("local"), []byte("0"), []byte("0"),
	}, true)
	require.Equal(t, [][]byte{local}, coord.keys)
	require.Equal(t, []uint64{0}, coord.marks)
}

func TestRedisReadObservationBlockingXReadRecordsPolls(t *testing.T) {
	st := store.NewMVCCStore()
	defer st.Close()
	coord := &redisReadObservationCoordinator{}
	server := &RedisServer{
		coordinator:       coord,
		store:             st,
		blockWaitFallback: 10 * time.Millisecond,
	}
	req := xreadRequest{
		keys:     [][]byte{[]byte("missing")},
		afterIDs: []string{"0-0"},
	}
	server.xreadBusyPoll(&recordingConn{}, req, time.Now().Add(100*time.Millisecond))
	require.GreaterOrEqual(t, len(coord.keys), 2)
	for _, key := range coord.keys {
		require.Equal(t, redisUserRouteKey([]byte("missing")), key)
	}
	require.Empty(t, coord.marks)
}

func TestRedisReadObservationBlockingBZPopMinRecordsPolls(t *testing.T) {
	st := store.NewMVCCStore()
	defer st.Close()
	coord := &redisReadObservationCoordinator{}
	server := &RedisServer{
		coordinator:       coord,
		store:             st,
		blockWaitFallback: 10 * time.Millisecond,
	}
	server.bzpopminWaitLoop(&recordingConn{}, [][]byte{[]byte("missing")}, time.Now().Add(100*time.Millisecond))
	require.GreaterOrEqual(t, len(coord.keys), 2)
	for _, key := range coord.keys {
		require.Equal(t, redisUserRouteKey([]byte("missing")), key)
	}
	require.Empty(t, coord.marks)
}

func TestRedisReadObservationBZPopMinSingleAttempt(t *testing.T) {
	st := store.NewMVCCStore()
	defer st.Close()
	coord := &redisReadObservationCoordinator{}
	server := &RedisServer{coordinator: coord, store: st}
	cmd := redcon.Command{Args: [][]byte{[]byte(cmdBZPopMin), []byte("missing"), []byte("1")}}
	server.dispatchCommandDirect(&recordingConn{}, cmdBZPopMin, func(conn redcon.Conn, _ redcon.Command) {
		server.bzpopminWaitLoop(conn, [][]byte{[]byte("missing")}, time.Now().Add(-time.Second))
	}, cmd, time.Now())
	require.Equal(t, [][]byte{redisUserRouteKey([]byte("missing"))}, coord.keys)
	require.Empty(t, coord.marks)
}

func TestRedisReadObservationNonblockingXReadCountsActualReads(t *testing.T) {
	for _, tt := range []struct {
		name string
		id   string
		want int
	}{
		{name: "explicit-id", id: "0-0", want: 1},
		{name: "dollar-id", id: "$", want: 2},
	} {
		t.Run(tt.name, func(t *testing.T) {
			st := store.NewMVCCStore()
			defer st.Close()
			coord := &redisReadObservationCoordinator{}
			server := &RedisServer{coordinator: coord, store: st}
			cmd := redcon.Command{Args: [][]byte{[]byte(cmdXRead), []byte("STREAMS"), []byte("missing"), []byte(tt.id)}}
			server.dispatchCommandDirect(&recordingConn{}, cmdXRead, server.xread, cmd, time.Now())
			require.Len(t, coord.keys, tt.want)
			for _, key := range coord.keys {
				require.Equal(t, redisUserRouteKey([]byte("missing")), key)
			}
			require.Empty(t, coord.marks)
		})
	}
}

func TestRedisReadObservationXReadStopsAtWrongType(t *testing.T) {
	for _, block := range []bool{false, true} {
		t.Run(map[bool]string{false: "nonblocking", true: "blocking"}[block], func(t *testing.T) {
			ctx := t.Context()
			st := store.NewMVCCStore()
			defer st.Close()
			require.NoError(t, st.PutAt(ctx, redisStrKey([]byte("wrongtype")), encodeRedisStr([]byte("value"), nil), 1, 0))
			coord := &redisReadObservationCoordinator{}
			server := &RedisServer{coordinator: coord, store: st}
			args := [][]byte{[]byte(cmdXRead)}
			if block {
				args = append(args, []byte("BLOCK"), []byte("1000"))
			}
			args = append(args, []byte("STREAMS"), []byte("wrongtype"), []byte("untouched"), []byte("0"), []byte("0"))
			cmd := redcon.Command{Args: args}
			conn := &recordingConn{}
			server.dispatchCommandDirect(conn, cmdXRead, server.xread, cmd, time.Now())
			require.NotEmpty(t, conn.err)
			require.Equal(t, [][]byte{redisUserRouteKey([]byte("wrongtype"))}, coord.keys)
			require.Empty(t, coord.marks)
		})
	}
}

func TestRedisReadObservationPFCountStopsAtWrongType(t *testing.T) {
	ctx := t.Context()
	st := store.NewMVCCStore()
	defer st.Close()
	require.NoError(t, st.PutAt(ctx, redisStrKey([]byte("wrongtype")), encodeRedisStr([]byte("value"), nil), 1, 0))
	coord := &redisReadObservationCoordinator{}
	server := &RedisServer{coordinator: coord, store: st}
	cmd := redcon.Command{Args: [][]byte{[]byte(cmdPFCount), []byte("wrongtype"), []byte("wrongtype"), []byte("untouched")}}
	conn := &recordingConn{}
	server.dispatchCommandDirect(conn, cmdPFCount, server.pfcountCommand, cmd, time.Now())
	require.NotEmpty(t, conn.err)
	require.Equal(t, [][]byte{redisUserRouteKey([]byte("wrongtype"))}, coord.keys)
	require.Empty(t, coord.marks)
}

func TestRedisReadObservationMultiKeyIngressAttributesLocalAfterRemote(t *testing.T) {
	remote := redisUserRouteKey([]byte("remote"))
	local := redisUserRouteKey([]byte("local"))
	for _, name := range []string{cmdExists, cmdDel} {
		t.Run(name, func(t *testing.T) {
			coord := &redisReadObservationCoordinator{notLeader: map[string]bool{string(remote): true}}
			server := &RedisServer{coordinator: coord}
			server.dispatchCommandDirect(&recordingConn{}, name, func(redcon.Conn, redcon.Command) {}, redcon.Command{
				Args: [][]byte{[]byte(name), []byte("remote"), []byte("local")},
			}, time.Now())
			require.Equal(t, [][]byte{local}, coord.keys)
			require.Equal(t, []uint64{0}, coord.marks)
		})
	}
}

func TestRedisReadObservationExistsHandlerRemoteFirst(t *testing.T) {
	ctx := context.Background()
	st := store.NewMVCCStore()
	defer st.Close()
	for _, key := range []string{"remote", "local"} {
		require.NoError(t, st.PutAt(ctx, redisStrKey([]byte(key)), encodeRedisStr([]byte("value"), nil), 1, 0))
	}
	remote := redisUserRouteKey([]byte("remote"))
	local := redisUserRouteKey([]byte("local"))
	coord := &redisReadObservationCoordinator{notLeader: map[string]bool{string(remote): true}}
	server := &RedisServer{coordinator: coord, store: st}
	conn := &recordingConn{}
	server.dispatchCommandDirect(conn, cmdExists, server.exists, redcon.Command{
		Args: [][]byte{[]byte(cmdExists), []byte("remote"), []byte("local")},
	}, time.Now())
	require.Empty(t, conn.err)
	require.Equal(t, int64(2), conn.int)
	require.Equal(t, [][]byte{local}, coord.keys)
	require.Equal(t, []uint64{0}, coord.marks)
}

func TestRedisReadObservationAfterDirectHandler(t *testing.T) {
	coord := &redisReadObservationCoordinator{}
	server := &RedisServer{coordinator: coord}
	cmd := redcon.Command{Args: [][]byte{[]byte(cmdHGet), []byte("k"), []byte("field")}}
	server.dispatchCommandDirect(&recordingConn{}, cmdHGet, func(redcon.Conn, redcon.Command) {
		require.Empty(t, coord.keys)
	}, cmd, time.Now())
	require.Equal(t, [][]byte{redisUserRouteKey([]byte("k"))}, coord.keys)
}

func TestRedisReadObservationMarksUnattributableOnServingNode(t *testing.T) {
	for _, args := range [][][]byte{
		{[]byte(cmdDBSize)},
		{[]byte(cmdKeys), []byte("*")},
		{[]byte(cmdScan), []byte("0")},
	} {
		t.Run(string(args[0]), func(t *testing.T) {
			coord := &redisReadObservationCoordinator{}
			coord.leaderSet = true
			coord.leader = false
			server := &RedisServer{coordinator: coord}
			server.dispatchCommandDirect(&recordingConn{}, string(args[0]), func(redcon.Conn, redcon.Command) {
				require.Empty(t, coord.marks)
			}, redcon.Command{Args: args}, time.Now())
			require.Equal(t, []uint64{0}, coord.marks)
			require.Empty(t, coord.keys)
		})
	}
}

func TestRedisReadObservationCrossLeaderMultiKeyMarksGlobal(t *testing.T) {
	coord := &redisReadObservationCoordinator{notLeader: map[string]bool{
		string(redisUserRouteKey([]byte("b"))): true,
	}}
	server := &RedisServer{coordinator: coord}
	server.dispatchCommandDirect(&recordingConn{}, cmdExists, func(redcon.Conn, redcon.Command) {}, redcon.Command{
		Args: [][]byte{[]byte(cmdExists), []byte("a"), []byte("b")},
	}, time.Now())
	require.Equal(t, []uint64{0}, coord.marks)
	require.Equal(t, [][]byte{redisUserRouteKey([]byte("a"))}, coord.keys)

	coord.notLeader[string(redisUserRouteKey([]byte("a")))] = true
	coord.marks = nil
	coord.keys = nil
	server.observeRedisRead(context.Background(), cmdExists, [][]byte{[]byte(cmdExists), []byte("a"), []byte("b")}, true)
	require.Equal(t, []uint64{0}, coord.marks, "EXISTS probes both keys at ingress")
	require.Empty(t, coord.keys)
}

func TestRedisReadObservationInsideLua(t *testing.T) {
	coord := &redisReadObservationCoordinator{}
	server := &RedisServer{coordinator: coord, store: store.NewMVCCStore(), scriptCache: map[string]string{}}
	conn := &recordingConn{}
	server.eval(conn, redcon.Command{Args: [][]byte{
		[]byte(cmdEval), []byte("return redis.call('GET', KEYS[1])"), []byte("1"), []byte("lua-key"),
	}})
	require.Empty(t, conn.err)
	require.Equal(t, [][]byte{redisUserRouteKey([]byte("lua-key"))}, coord.keys)
}

func TestRedisReadObservationInsideLuaPCallError(t *testing.T) {
	coord := &redisReadObservationCoordinator{}
	server := &RedisServer{coordinator: coord, store: store.NewMVCCStore(), scriptCache: map[string]string{}}
	conn := &recordingConn{}
	server.eval(conn, redcon.Command{Args: [][]byte{
		[]byte(cmdEval), []byte("redis.call('SET', KEYS[1], 'value'); local result = redis.pcall('HGET', KEYS[1], 'field'); return result.err"), []byte("1"), []byte("wrong-type"),
	}})
	require.Empty(t, conn.err)
	require.Contains(t, string(conn.bulk), "WRONGTYPE")
	require.Equal(t, [][]byte{
		redisUserRouteKey([]byte("wrong-type")), redisUserRouteKey([]byte("wrong-type")),
	}, coord.keys)
}

func TestRedisReadObservationInsideExec(t *testing.T) {
	coord := &redisReadObservationCoordinator{}
	server := &RedisServer{coordinator: coord, store: store.NewMVCCStore()}
	_, err := server.runTransaction([]redcon.Command{{Args: [][]byte{[]byte(cmdGet), []byte("exec-key")}}})
	require.NoError(t, err)
	require.Equal(t, [][]byte{redisUserRouteKey([]byte("exec-key"))}, coord.keys)
}

func TestRedisReadObservationInsideFailedExec(t *testing.T) {
	coord := &redisReadObservationCoordinator{}
	server := &RedisServer{coordinator: coord, store: store.NewMVCCStore()}
	_, err := server.runTransaction([]redcon.Command{
		{Args: [][]byte{[]byte(cmdGet), []byte("exec-key")}},
		{Args: [][]byte{[]byte(cmdHGet), []byte("exec-key"), []byte("field")}},
	})
	require.ErrorContains(t, err, "unsupported")
	require.Contains(t, coord.keys, redisUserRouteKey([]byte("exec-key")))
}
