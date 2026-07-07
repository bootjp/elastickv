package adapter

import (
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bootjp/elastickv/monitoring"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/redcon"
)

func TestRedisHeavyCommandLimiterRejectsWhenFull(t *testing.T) {
	registry := monitoring.NewRegistry("n1", "10.0.0.1:50051")
	server := NewRedisServer(nil, "", nil, nil, nil, nil,
		WithRedisHeavyCommandSlots(1),
		WithRedisRequestObserver(registry.RedisObserver()))
	require.NotNil(t, server.heavyCommandLimiter)

	require.True(t, server.heavyCommandLimiter.submit(func() {
		var called atomic.Bool
		conn := &commandRecorder{}
		cmd := redcon.Command{Args: [][]byte{[]byte(cmdXRead), []byte("STREAMS"), []byte("s"), []byte("0")}}
		server.dispatchCommand(conn, cmdXRead, func(redcon.Conn, redcon.Command) {
			called.Store(true)
		}, cmd, time.Now())

		require.False(t, called.Load())
		require.Len(t, conn.writes, 1)
		require.Equal(t, "error", conn.writes[0].op)
		require.Equal(t, "BUSY server overloaded", conn.writes[0].s)
	}))

	err := testutil.GatherAndCompare(
		registry.Gatherer(),
		strings.NewReader(`
# HELP elastickv_redis_requests_total Total number of Redis API requests by command and outcome.
# TYPE elastickv_redis_requests_total counter
elastickv_redis_requests_total{command="XREAD",node_address="10.0.0.1:50051",node_id="n1",outcome="error"} 1
# HELP elastickv_redis_errors_total Total number of Redis API errors by command.
# TYPE elastickv_redis_errors_total counter
elastickv_redis_errors_total{command="XREAD",node_address="10.0.0.1:50051",node_id="n1"} 1
`),
		"elastickv_redis_requests_total",
		"elastickv_redis_errors_total",
	)
	require.NoError(t, err)
}

func TestRedisHeavyCommandLimiterReleasesSlot(t *testing.T) {
	server := NewRedisServer(nil, "", nil, nil, nil, nil, WithRedisHeavyCommandSlots(1))
	conn := &commandRecorder{}
	cmd := redcon.Command{Args: [][]byte{[]byte(cmdXRead), []byte("STREAMS"), []byte("s"), []byte("0")}}

	var calls atomic.Int64
	handler := func(c redcon.Conn, _ redcon.Command) {
		calls.Add(1)
		c.WriteString("OK")
	}

	server.dispatchCommand(conn, cmdXRead, handler, cmd, time.Now())
	server.dispatchCommand(conn, cmdXRead, handler, cmd, time.Now())

	require.Equal(t, int64(2), calls.Load())
	require.Len(t, conn.writes, 2)
	require.Equal(t, "string", conn.writes[0].op)
	require.Equal(t, "string", conn.writes[1].op)
}

func TestRedisHeavyCommandLimiterDoesNotGateCheapCommand(t *testing.T) {
	server := NewRedisServer(nil, "", nil, nil, nil, nil, WithRedisHeavyCommandSlots(1))
	require.True(t, server.heavyCommandLimiter.submit(func() {
		conn := &commandRecorder{}
		cmd := redcon.Command{Args: [][]byte{[]byte(cmdGet), []byte("k")}}
		var called atomic.Bool
		server.dispatchCommand(conn, cmdGet, func(c redcon.Conn, _ redcon.Command) {
			called.Store(true)
			c.WriteString("OK")
		}, cmd, time.Now())

		require.True(t, called.Load())
		require.Len(t, conn.writes, 1)
		require.Equal(t, "string", conn.writes[0].op)
	}))
}

func TestRedisHeavyCommandClassification(t *testing.T) {
	for _, cmd := range []string{
		cmdEval, cmdEvalSHA, cmdKeys, cmdScan, cmdHGetAll, cmdLRange,
		cmdSMembers, cmdXRead, cmdXRange, cmdXRevRange, cmdZRange,
		cmdZRangeByScore, cmdZRevRange, cmdZRevRangeByScore, cmdBZPopMin,
	} {
		require.True(t, isRedisHeavyCommand(strings.ToLower(cmd)), cmd)
	}
	for _, cmd := range []string{cmdGet, cmdSet, cmdHGet, cmdLPush, cmdXAdd, cmdTTL} {
		require.False(t, isRedisHeavyCommand(cmd), cmd)
	}
}
