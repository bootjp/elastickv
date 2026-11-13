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
	// These readonly commands have no attributable user key. XREAD's key
	// positions are dynamic but are parsed separately below. GET is covered
	// by LeaseReadForKeyThrough on the standalone path.
	keyless := map[string]bool{cmdDBSize: true, cmdKeys: true, cmdScan: true}
	var seenKeyless []string
	for _, spec := range redisCommandSpecs {
		if !hasRedisCommandFlag(spec.Flags, redisCmdFlagReadonly) {
			continue
		}
		if keyless[spec.Constant] {
			require.Zero(t, spec.FirstKey, spec.Constant)
			seenKeyless = append(seenKeyless, spec.Constant)
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
	require.ElementsMatch(t, []string{cmdDBSize, cmdKeys, cmdScan}, seenKeyless)
}

func TestRedisReadObservationExtractsMultipleAndDynamicKeys(t *testing.T) {
	require.Equal(t, [][]byte{[]byte("a"), []byte("b")}, redisReadKeys(cmdExists, [][]byte{[]byte(cmdExists), []byte("a"), []byte("b")}))
	require.Equal(t, [][]byte{[]byte("a"), []byte("b")}, redisReadKeys(cmdXRead, [][]byte{[]byte(cmdXRead), []byte("COUNT"), []byte("2"), []byte("STREAMS"), []byte("a"), []byte("b"), []byte("0"), []byte("0")}))
	require.Nil(t, redisReadKeys(cmdXRead, [][]byte{[]byte(cmdXRead), []byte("STREAMS"), []byte("a")}))
	for _, name := range []string{cmdDBSize, cmdKeys, cmdScan, cmdEval, cmdExec} {
		require.Nil(t, redisReadKeys(name, [][]byte{[]byte(name), []byte("k")}), name)
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
		{[]byte(cmdXRead), []byte("STREAMS"), []byte("a")},
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
	require.Empty(t, coord.keys)

	coord.notLeader[string(redisUserRouteKey([]byte("a")))] = true
	coord.marks = nil
	server.observeRedisRead(context.Background(), cmdExists, [][]byte{[]byte(cmdExists), []byte("a"), []byte("b")}, true)
	require.Empty(t, coord.marks, "forwarded ingress must not mark")
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

func TestRedisReadObservationInsideExec(t *testing.T) {
	coord := &redisReadObservationCoordinator{}
	server := &RedisServer{coordinator: coord, store: store.NewMVCCStore()}
	_, err := server.runTransaction([]redcon.Command{{Args: [][]byte{[]byte(cmdGet), []byte("exec-key")}}})
	require.NoError(t, err)
	require.Equal(t, [][]byte{redisUserRouteKey([]byte("exec-key"))}, coord.keys)
}
