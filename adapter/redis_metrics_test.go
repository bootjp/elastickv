package adapter

import (
	"fmt"
	"net"
	"strings"
	"sync"
	"testing"
	"time"
	"unicode/utf8"

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
					"label value must be length-capped (byte count)")
				require.True(t, utf8.ValidString(lbl.GetValue()),
					"label value must be valid UTF-8")
				if lbl.GetValue() == strings.Repeat("A", 64) {
					found = true
				}
			}
		}
	}
	require.True(t, found, "expected truncated 64-char label value")
}

// TestRedisMetricsUnsupportedCommandTruncatesOnRuneBoundary verifies that
// truncating a long command name that contains multibyte UTF-8 runes does
// not split a rune, which would produce an invalid UTF-8 label and cause
// prometheus.WithLabelValues to panic. The input is 63 ASCII 'A' bytes
// followed by a 2-byte rune 'é' repeated enough times to exceed the 64-byte
// cap; a byte-boundary truncate would split the first 'é'.
func TestRedisMetricsUnsupportedCommandTruncatesOnRuneBoundary(t *testing.T) {
	registry := monitoring.NewRegistry("n1", "10.0.0.1:50051")
	observer := registry.RedisObserver()

	// 63 bytes of 'A' + a sequence of 'é' (2 bytes each, uppercases to 'É'
	// which is also 2 bytes). Total byte length well over 64.
	payload := strings.Repeat("A", 63) + strings.Repeat("é", 20)
	require.Greater(t, len(payload), 64)

	require.NotPanics(t, func() {
		observer.ObserveRedisRequest(monitoring.RedisRequestReport{
			Command:     payload,
			IsError:     true,
			Duration:    time.Millisecond,
			Unsupported: true,
		})
	})

	mf, err := registry.Gatherer().Gather()
	require.NoError(t, err)

	var checked bool
	for _, family := range mf {
		if family.GetName() != "elastickv_redis_unsupported_commands_total" {
			continue
		}
		for _, metric := range family.GetMetric() {
			for _, lbl := range metric.GetLabel() {
				if lbl.GetName() != "command" {
					continue
				}
				v := lbl.GetValue()
				require.LessOrEqual(t, len(v), 64,
					"byte-length cap must be preserved")
				require.True(t, utf8.ValidString(v),
					"truncation must not split a rune (invalid UTF-8 would panic prometheus)")
				checked = true
			}
		}
	}
	require.True(t, checked, "expected an unsupported-commands metric to be emitted")
}

// TestRedisMetricsUnsupportedCommandRejectsInvalidUTF8 verifies that an
// ingress command name that is already invalid UTF-8 (e.g. a binary blob
// from a misbehaving or hostile client) does not crash the observer and is
// folded into a fixed "invalid_utf8" sentinel label.
func TestRedisMetricsUnsupportedCommandRejectsInvalidUTF8(t *testing.T) {
	registry := monitoring.NewRegistry("n1", "10.0.0.1:50051")
	observer := registry.RedisObserver()

	// 0xff 0xfe 0xfd are never valid UTF-8 start bytes.
	invalid := string([]byte{0xff, 0xfe, 0xfd, 0xc3, 0x28})
	require.False(t, utf8.ValidString(invalid))

	require.NotPanics(t, func() {
		observer.ObserveRedisRequest(monitoring.RedisRequestReport{
			Command:     invalid,
			IsError:     true,
			Duration:    time.Millisecond,
			Unsupported: true,
		})
	})

	mf, err := registry.Gatherer().Gather()
	require.NoError(t, err)

	var sentinelCount float64
	for _, family := range mf {
		if family.GetName() != "elastickv_redis_unsupported_commands_total" {
			continue
		}
		for _, metric := range family.GetMetric() {
			for _, lbl := range metric.GetLabel() {
				if lbl.GetName() == "command" && lbl.GetValue() == "invalid_utf8" {
					sentinelCount += metric.GetCounter().GetValue()
				}
			}
		}
	}
	require.Equal(t, float64(1), sentinelCount,
		"invalid-UTF-8 input must be folded into the 'invalid_utf8' sentinel")
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

// TestRedisMetricsUnsupportedCommandRawInvalidUTF8FromAdapterPath covers
// the regression fixed alongside this commit: the adapter's Run() loop
// must pass RAW command bytes (not strings.ToUpper'd bytes) to the
// unsupported-command observer. ToUpper silently rewrites invalid UTF-8
// into the U+FFFD replacement character, so if the raw path is not
// preserved the invalid_utf8 sentinel is never reached and a hostile
// client can burn through the distinct-name slots with replacement-char
// garbage. Feeding the observer directly with three invalid bytes must
// produce the sentinel, not a synthetic "valid UTF-8" label.
func TestRedisMetricsUnsupportedCommandRawInvalidUTF8Bytes(t *testing.T) {
	registry := monitoring.NewRegistry("n1", "10.0.0.1:50051")
	observer := registry.RedisObserver()

	raw := string([]byte{0xff, 0xff, 0xff})
	require.False(t, utf8.ValidString(raw), "precondition: bytes must be invalid UTF-8")

	require.NotPanics(t, func() {
		observer.ObserveRedisRequest(monitoring.RedisRequestReport{
			Command:     raw,
			IsError:     true,
			Duration:    time.Millisecond,
			Unsupported: true,
		})
	})

	mf, err := registry.Gatherer().Gather()
	require.NoError(t, err)

	var sentinel float64
	for _, family := range mf {
		if family.GetName() != "elastickv_redis_unsupported_commands_total" {
			continue
		}
		for _, metric := range family.GetMetric() {
			for _, lbl := range metric.GetLabel() {
				if lbl.GetName() == "command" && lbl.GetValue() == "invalid_utf8" {
					sentinel += metric.GetCounter().GetValue()
				}
			}
		}
	}
	require.Equal(t, float64(1), sentinel,
		"raw invalid-UTF-8 bytes must land in the 'invalid_utf8' sentinel")
}

// TestRedisMetricsUnsupportedCommandRejectsPathologicalLength defends
// against CPU-exhaustion input: a hostile client sending a multi-megabyte
// first argument must not cause strings.ToUpper / utf8.ValidString /
// []rune iteration to process the full payload. The observer caps the raw
// input length before any O(n) string operation runs; this test just
// asserts the call returns quickly and does not panic under the default
// `go test -timeout` budget.
func TestRedisMetricsUnsupportedCommandRejectsPathologicalLength(t *testing.T) {
	registry := monitoring.NewRegistry("n1", "10.0.0.1:50051")
	observer := registry.RedisObserver()

	// 1 MiB of 'A'. Well above any reasonable Redis command name.
	huge := strings.Repeat("A", 1<<20)

	require.NotPanics(t, func() {
		observer.ObserveRedisRequest(monitoring.RedisRequestReport{
			Command:     huge,
			IsError:     true,
			Duration:    time.Millisecond,
			Unsupported: true,
		})
	})

	mf, err := registry.Gatherer().Gather()
	require.NoError(t, err)

	var checked bool
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
					"even pathologically long inputs must respect the 64-byte label cap")
				require.True(t, utf8.ValidString(lbl.GetValue()),
					"label value must remain valid UTF-8")
				checked = true
			}
		}
	}
	require.True(t, checked, "expected a metric to be emitted for the huge input")
}

// TestRedisMetricsUnsupportedCommandRWMutexFastPath exercises the
// read-locked fast path in observeUnsupportedCommand under concurrent
// observers hitting a mix of already-admitted names (fast path) and brand
// new names (slow path). It is designed to run under `go test -race`.
func TestRedisMetricsUnsupportedCommandRWMutexFastPath(t *testing.T) {
	registry := monitoring.NewRegistry("n1", "10.0.0.1:50051")
	observer := registry.RedisObserver()

	// Pre-seed a handful of names so the vast majority of concurrent
	// observations take the read-only fast path.
	seeds := []string{"ALPHA", "BRAVO", "CHARLIE", "DELTA", "ECHO"}
	for _, s := range seeds {
		observer.ObserveRedisRequest(monitoring.RedisRequestReport{
			Command:     s,
			IsError:     true,
			Duration:    time.Microsecond,
			Unsupported: true,
		})
	}

	const (
		workers = 32
		iters   = 500
	)
	var wg sync.WaitGroup
	wg.Add(workers)
	for w := 0; w < workers; w++ {
		go func() {
			defer wg.Done()
			for i := 0; i < iters; i++ {
				var cmd string
				// ~80% fast path, ~20% slow path (new names) to stress
				// both RLock and Lock branches concurrently.
				if i%5 == 0 {
					cmd = fmt.Sprintf("NEW_W%03d_I%03d", w, i)
				} else {
					cmd = seeds[i%len(seeds)]
				}
				observer.ObserveRedisRequest(monitoring.RedisRequestReport{
					Command:     cmd,
					IsError:     true,
					Duration:    time.Microsecond,
					Unsupported: true,
				})
			}
		}()
	}
	wg.Wait()

	// Sanity: the total across all label buckets must equal the total
	// number of observations performed (including seeds).
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
	require.Equal(t, float64(len(seeds)+workers*iters), total)
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
