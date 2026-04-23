package adapter

import (
	"fmt"
	"net"
	"strings"
	"sync"
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
		Command:     "NOTACOMMAND",
		IsError:     true,
		Duration:    time.Millisecond,
		Unsupported: true,
	})

	err := testutil.GatherAndCompare(
		registry.Gatherer(),
		strings.NewReader(`
# HELP elastickv_redis_requests_total Total number of Redis API requests by command and outcome.
# TYPE elastickv_redis_requests_total counter
elastickv_redis_requests_total{command="unknown",node_address="10.0.0.1:50051",node_id="n1",outcome="error"} 1
# HELP elastickv_redis_unsupported_commands_total Count of Redis commands rejected as unsupported, labelled with the actual command name (bounded cardinality).
# TYPE elastickv_redis_unsupported_commands_total counter
elastickv_redis_unsupported_commands_total{command="NOTACOMMAND",node_address="10.0.0.1:50051",node_id="n1"} 1
`),
		"elastickv_redis_requests_total",
		"elastickv_redis_unsupported_commands_total",
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

func TestDispatchCommandObservesSuccessMetrics(t *testing.T) {
	registry := monitoring.NewRegistry("n1", "10.0.0.1:50051")
	server := NewRedisServer(nil, "", nil, nil, nil, nil, WithRedisRequestObserver(registry.RedisObserver()))
	conn := &stubRedisConn{}
	handler := func(c redcon.Conn, cmd redcon.Command) {
		c.WriteString("OK")
	}
	cmd := redcon.Command{Args: [][]byte{[]byte("SET"), []byte("key"), []byte("val")}}

	server.dispatchCommand(conn, "SET", handler, cmd, time.Now())

	err := testutil.GatherAndCompare(
		registry.Gatherer(),
		strings.NewReader(`
# HELP elastickv_redis_requests_total Total number of Redis API requests by command and outcome.
# TYPE elastickv_redis_requests_total counter
elastickv_redis_requests_total{command="SET",node_address="10.0.0.1:50051",node_id="n1",outcome="success"} 1
`),
		"elastickv_redis_requests_total",
	)
	require.NoError(t, err)
}

func TestDispatchCommandObservesErrorMetrics(t *testing.T) {
	registry := monitoring.NewRegistry("n1", "10.0.0.1:50051")
	server := NewRedisServer(nil, "", nil, nil, nil, nil, WithRedisRequestObserver(registry.RedisObserver()))
	conn := &stubRedisConn{}
	handler := func(c redcon.Conn, cmd redcon.Command) {
		c.WriteError("ERR something went wrong")
	}
	cmd := redcon.Command{Args: [][]byte{[]byte("SET"), []byte("key"), []byte("val")}}

	server.dispatchCommand(conn, "SET", handler, cmd, time.Now())

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

func TestObserveRedisErrorRecordsMetrics(t *testing.T) {
	registry := monitoring.NewRegistry("n1", "10.0.0.1:50051")
	server := NewRedisServer(nil, "", nil, nil, nil, nil, WithRedisRequestObserver(registry.RedisObserver()))

	server.observeRedisError("BADCMD", time.Millisecond)

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

func TestObserveRedisSuccessRecordsMetrics(t *testing.T) {
	registry := monitoring.NewRegistry("n1", "10.0.0.1:50051")
	server := NewRedisServer(nil, "", nil, nil, nil, nil, WithRedisRequestObserver(registry.RedisObserver()))

	server.observeRedisSuccess("GET", time.Millisecond)

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

func TestRedisMetricsUnsupportedCommandRecordsRealName(t *testing.T) {
	registry := monitoring.NewRegistry("n1", "10.0.0.1:50051")
	observer := registry.RedisObserver()

	observer.ObserveRedisRequest(monitoring.RedisRequestReport{
		Command:     "proxy",
		IsError:     true,
		Duration:    time.Millisecond,
		Unsupported: true,
	})

	err := testutil.GatherAndCompare(
		registry.Gatherer(),
		strings.NewReader(`
# HELP elastickv_redis_unsupported_commands_total Count of Redis commands rejected as unsupported, labelled with the actual command name (bounded cardinality).
# TYPE elastickv_redis_unsupported_commands_total counter
elastickv_redis_unsupported_commands_total{command="PROXY",node_address="10.0.0.1:50051",node_id="n1"} 1
`),
		"elastickv_redis_unsupported_commands_total",
	)
	require.NoError(t, err)
}

func TestRedisMetricsUnsupportedCommandTwoDistinctNames(t *testing.T) {
	registry := monitoring.NewRegistry("n1", "10.0.0.1:50051")
	observer := registry.RedisObserver()

	observer.ObserveRedisRequest(monitoring.RedisRequestReport{
		Command: "FOO", IsError: true, Duration: time.Millisecond, Unsupported: true,
	})
	observer.ObserveRedisRequest(monitoring.RedisRequestReport{
		Command: "BAR", IsError: true, Duration: time.Millisecond, Unsupported: true,
	})
	observer.ObserveRedisRequest(monitoring.RedisRequestReport{
		Command: "FOO", IsError: true, Duration: time.Millisecond, Unsupported: true,
	})

	err := testutil.GatherAndCompare(
		registry.Gatherer(),
		strings.NewReader(`
# HELP elastickv_redis_unsupported_commands_total Count of Redis commands rejected as unsupported, labelled with the actual command name (bounded cardinality).
# TYPE elastickv_redis_unsupported_commands_total counter
elastickv_redis_unsupported_commands_total{command="BAR",node_address="10.0.0.1:50051",node_id="n1"} 1
elastickv_redis_unsupported_commands_total{command="FOO",node_address="10.0.0.1:50051",node_id="n1"} 2
`),
		"elastickv_redis_unsupported_commands_total",
	)
	require.NoError(t, err)
}

func TestRedisMetricsUnsupportedCommandCapSpillsToOther(t *testing.T) {
	registry := monitoring.NewRegistry("n1", "10.0.0.1:50051")
	observer := registry.RedisObserver()

	// Observe 32 distinct names (the cap).
	for i := 0; i < 32; i++ {
		observer.ObserveRedisRequest(monitoring.RedisRequestReport{
			Command:     fmt.Sprintf("CMD%02d", i),
			IsError:     true,
			Duration:    time.Millisecond,
			Unsupported: true,
		})
	}
	// The 33rd distinct name must fold into "other".
	observer.ObserveRedisRequest(monitoring.RedisRequestReport{
		Command:     "OVERFLOW33",
		IsError:     true,
		Duration:    time.Millisecond,
		Unsupported: true,
	})
	// Same overflow name comes in again: still "other" because it never
	// got admitted into the distinct-name set.
	observer.ObserveRedisRequest(monitoring.RedisRequestReport{
		Command:     "OVERFLOW33",
		IsError:     true,
		Duration:    time.Millisecond,
		Unsupported: true,
	})
	// An already-seen name (under cap) must still record with its real
	// name, not "other".
	observer.ObserveRedisRequest(monitoring.RedisRequestReport{
		Command:     "CMD00",
		IsError:     true,
		Duration:    time.Millisecond,
		Unsupported: true,
	})

	mf, err := registry.Gatherer().Gather()
	require.NoError(t, err)

	var (
		otherCount    float64
		cmd00Count    float64
		distinctNames = map[string]float64{}
	)
	for _, family := range mf {
		if family.GetName() != "elastickv_redis_unsupported_commands_total" {
			continue
		}
		for _, metric := range family.GetMetric() {
			var name string
			for _, lbl := range metric.GetLabel() {
				if lbl.GetName() == "command" {
					name = lbl.GetValue()
					break
				}
			}
			val := metric.GetCounter().GetValue()
			distinctNames[name] = val
			switch name {
			case "other":
				otherCount = val
			case "CMD00":
				cmd00Count = val
			}
		}
	}

	// 32 real names + "other" = 33 distinct label values.
	require.Len(t, distinctNames, 33, "expected exactly 32 real names plus 'other'")
	require.NotContains(t, distinctNames, "OVERFLOW33", "overflow name must not appear as its own label")
	require.Equal(t, float64(2), otherCount, "overflow increments must accumulate in 'other'")
	require.Equal(t, float64(2), cmd00Count, "already-seen name must still record with real label")
}

func TestRedisMetricsUnsupportedCommandTruncatesLongName(t *testing.T) {
	registry := monitoring.NewRegistry("n1", "10.0.0.1:50051")
	observer := registry.RedisObserver()

	long := strings.Repeat("A", 200)
	observer.ObserveRedisRequest(monitoring.RedisRequestReport{
		Command:     long,
		IsError:     true,
		Duration:    time.Millisecond,
		Unsupported: true,
	})

	mf, err := registry.Gatherer().Gather()
	require.NoError(t, err)

	var found bool
	for _, family := range mf {
		if family.GetName() != "elastickv_redis_unsupported_commands_total" {
			continue
		}
		for _, metric := range family.GetMetric() {
			for _, lbl := range metric.GetLabel() {
				if lbl.GetName() != "command" {
					continue
				}
				require.LessOrEqual(t, len(lbl.GetValue()), 64,
					"label value must be length-capped")
				if lbl.GetValue() == strings.Repeat("A", 64) {
					found = true
				}
			}
		}
	}
	require.True(t, found, "expected truncated 64-char label value")
}

func TestRedisMetricsUnsupportedCommandConcurrentObservers(t *testing.T) {
	registry := monitoring.NewRegistry("n1", "10.0.0.1:50051")
	observer := registry.RedisObserver()

	const (
		workers  = 16
		perWorks = 200
	)
	var wg sync.WaitGroup
	wg.Add(workers)
	for w := 0; w < workers; w++ {
		go func() {
			defer wg.Done()
			for i := 0; i < perWorks; i++ {
				observer.ObserveRedisRequest(monitoring.RedisRequestReport{
					Command:     fmt.Sprintf("WCMD%02d", (w+i)%40),
					IsError:     true,
					Duration:    time.Microsecond,
					Unsupported: true,
				})
			}
		}()
	}
	wg.Wait()

	mf, err := registry.Gatherer().Gather()
	require.NoError(t, err)

	var total float64
	for _, family := range mf {
		if family.GetName() != "elastickv_redis_unsupported_commands_total" {
			continue
		}
		for _, metric := range family.GetMetric() {
			total += metric.GetCounter().GetValue()
		}
	}
	require.Equal(t, float64(workers*perWorks), total)
}

// stubRedisConn is a minimal redcon.Conn implementation for unit tests.
type stubRedisConn struct {
	lastError  string
	lastString string
}

func (s *stubRedisConn) RemoteAddr() string             { return "127.0.0.1:9999" }
func (s *stubRedisConn) Close() error                   { return nil }
func (s *stubRedisConn) WriteError(msg string)          { s.lastError = msg }
func (s *stubRedisConn) WriteString(str string)         { s.lastString = str }
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
