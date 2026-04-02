package adapter

import (
	"net"
	"strings"
	"testing"
	"time"

	"github.com/bootjp/elastickv/monitoring"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/redcon"
)

func TestRedisMetricsObservesSuccessRequest(t *testing.T) {
	registry := monitoring.NewRegistry("n1", "10.0.0.1:50051")
	observer := registry.RedisObserver()

	observer.ObserveRedisRequest(monitoring.RedisRequestReport{
		Command:  "GET",
		IsError:  false,
		Duration: 5 * time.Millisecond,
	})

	err := testutil.GatherAndCompare(
		registry.Gatherer(),
		strings.NewReader(`
# HELP elastickv_redis_requests_total Total number of Redis API requests by command and outcome.
# TYPE elastickv_redis_requests_total counter
elastickv_redis_requests_total{command="GET",node_address="10.0.0.1:50051",node_id="n1",outcome="success"} 1
`),
		"elastickv_redis_requests_total",
	)
	require.NoError(t, err)
}

func TestRedisMetricsObservesErrorRequest(t *testing.T) {
	registry := monitoring.NewRegistry("n1", "10.0.0.1:50051")
	observer := registry.RedisObserver()

	observer.ObserveRedisRequest(monitoring.RedisRequestReport{
		Command:  "SET",
		IsError:  true,
		Duration: 10 * time.Millisecond,
	})

	err := testutil.GatherAndCompare(
		registry.Gatherer(),
		strings.NewReader(`
# HELP elastickv_redis_requests_total Total number of Redis API requests by command and outcome.
# TYPE elastickv_redis_requests_total counter
elastickv_redis_requests_total{command="SET",node_address="10.0.0.1:50051",node_id="n1",outcome="error"} 1
# HELP elastickv_redis_errors_total Total number of Redis API errors by command.
# TYPE elastickv_redis_errors_total counter
elastickv_redis_errors_total{command="SET",node_address="10.0.0.1:50051",node_id="n1"} 1
`),
		"elastickv_redis_requests_total",
		"elastickv_redis_errors_total",
	)
	require.NoError(t, err)
}

func TestRedisMetricsObservesMultipleCommands(t *testing.T) {
	registry := monitoring.NewRegistry("n1", "10.0.0.1:50051")
	observer := registry.RedisObserver()

	observer.ObserveRedisRequest(monitoring.RedisRequestReport{Command: "GET", Duration: time.Millisecond})
	observer.ObserveRedisRequest(monitoring.RedisRequestReport{Command: "GET", Duration: time.Millisecond})
	observer.ObserveRedisRequest(monitoring.RedisRequestReport{Command: "SET", Duration: time.Millisecond})
	observer.ObserveRedisRequest(monitoring.RedisRequestReport{Command: "SET", IsError: true, Duration: time.Millisecond})

	err := testutil.GatherAndCompare(
		registry.Gatherer(),
		strings.NewReader(`
# HELP elastickv_redis_requests_total Total number of Redis API requests by command and outcome.
# TYPE elastickv_redis_requests_total counter
elastickv_redis_requests_total{command="GET",node_address="10.0.0.1:50051",node_id="n1",outcome="success"} 2
elastickv_redis_requests_total{command="SET",node_address="10.0.0.1:50051",node_id="n1",outcome="error"} 1
elastickv_redis_requests_total{command="SET",node_address="10.0.0.1:50051",node_id="n1",outcome="success"} 1
`),
		"elastickv_redis_requests_total",
	)
	require.NoError(t, err)
}

func TestRedisMetricsNormalizesUnknownCommand(t *testing.T) {
	registry := monitoring.NewRegistry("n1", "10.0.0.1:50051")
	observer := registry.RedisObserver()

	observer.ObserveRedisRequest(monitoring.RedisRequestReport{
		Command:  "NOTACOMMAND",
		IsError:  true,
		Duration: time.Millisecond,
	})

	err := testutil.GatherAndCompare(
		registry.Gatherer(),
		strings.NewReader(`
# HELP elastickv_redis_requests_total Total number of Redis API requests by command and outcome.
# TYPE elastickv_redis_requests_total counter
elastickv_redis_requests_total{command="unknown",node_address="10.0.0.1:50051",node_id="n1",outcome="error"} 1
`),
		"elastickv_redis_requests_total",
	)
	require.NoError(t, err)
}

func TestRedisMetricsConnDetectsError(t *testing.T) {
	mc := &redisMetricsConn{Conn: &stubRedisConn{}}
	require.False(t, mc.hadError)

	mc.WriteError("ERR something failed")
	require.True(t, mc.hadError)
}

func TestWithRedisRequestObserverOption(t *testing.T) {
	registry := monitoring.NewRegistry("n1", "10.0.0.1:50051")
	server := NewRedisServer(nil, "", nil, nil, nil, nil, WithRedisRequestObserver(registry.RedisObserver()))
	require.NotNil(t, server.requestObserver)
}

// stubRedisConn is a minimal redcon.Conn implementation for unit tests.
type stubRedisConn struct{}

func (s *stubRedisConn) RemoteAddr() string             { return "127.0.0.1:9999" }
func (s *stubRedisConn) Close() error                   { return nil }
func (s *stubRedisConn) WriteError(msg string)          {}
func (s *stubRedisConn) WriteString(str string)         {}
func (s *stubRedisConn) WriteBulk(bulk []byte)          {}
func (s *stubRedisConn) WriteBulkString(bulk string)    {}
func (s *stubRedisConn) WriteInt(num int)               {}
func (s *stubRedisConn) WriteInt64(num int64)           {}
func (s *stubRedisConn) WriteUint64(num uint64)         {}
func (s *stubRedisConn) WriteArray(count int)           {}
func (s *stubRedisConn) WriteNull()                     {}
func (s *stubRedisConn) WriteRaw(data []byte)           {}
func (s *stubRedisConn) WriteAny(v interface{})         {}
func (s *stubRedisConn) Context() interface{}           { return nil }
func (s *stubRedisConn) SetContext(v interface{})       {}
func (s *stubRedisConn) SetReadBuffer(bytes int)        {}
func (s *stubRedisConn) Detach() redcon.DetachedConn    { return nil }
func (s *stubRedisConn) ReadPipeline() []redcon.Command { return nil }
func (s *stubRedisConn) PeekPipeline() []redcon.Command { return nil }
func (s *stubRedisConn) NetConn() net.Conn              { return nil }
