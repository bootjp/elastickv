package monitoring

import (
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/prometheus/client_golang/prometheus"
)

const (
	redisOutcomeSuccess = "success"
	redisOutcomeError   = "error"
	redisCommandUnknown = "unknown"

	// maxUnsupportedCommandLabels caps the cardinality of the
	// elastickv_redis_unsupported_commands_total{command} label set. Once
	// this many distinct names have been seen, further novel names are
	// collapsed into the "other" bucket so a hostile or buggy client
	// cannot explode metric cardinality via protocol abuse.
	maxUnsupportedCommandLabels = 32

	// maxUnsupportedCommandLabelLen defensively caps the length of the
	// actual command-name label value. Redis command names are short; a
	// client sending a pathologically long first argument should not be
	// able to pollute label values with the entire payload.
	maxUnsupportedCommandLabelLen = 64

	// redisUnsupportedCommandOther is the overflow bucket used when the
	// observed name is novel and the distinct-name cap is full.
	redisUnsupportedCommandOther = "other"

	// redisUnsupportedCommandInvalidUTF8 is the sentinel label applied when
	// the raw command name contains bytes that are not valid UTF-8. Using a
	// fixed sentinel avoids ever passing invalid UTF-8 to
	// prometheus.WithLabelValues, which would panic.
	redisUnsupportedCommandInvalidUTF8 = "invalid_utf8"

	// maxUnsupportedCommandRawLen defensively caps the raw input length
	// before any O(n) string operations (strings.ToUpper, utf8.ValidString,
	// rune iteration) run. Redis command names are always short, so a
	// multi-megabyte first argument is abusive input; processing the full
	// payload would let a hostile client burn CPU. The cap is loose enough
	// to cover any plausible command name plus a margin of whitespace; the
	// later rune-aware truncation produces the final label.
	maxUnsupportedCommandRawLen = 256
)

var redisCommandSet = map[string]struct{}{
	"BZPOPMIN":         {},
	"CLIENT":           {},
	"DBSIZE":           {},
	"DEL":              {},
	"DISCARD":          {},
	"EVAL":             {},
	"EVALSHA":          {},
	"EXEC":             {},
	"EXISTS":           {},
	"EXPIRE":           {},
	"FLUSHALL":         {},
	"FLUSHDB":          {},
	"GET":              {},
	"GETDEL":           {},
	"HDEL":             {},
	"HEXISTS":          {},
	"HGET":             {},
	"HGETALL":          {},
	"HINCRBY":          {},
	"HLEN":             {},
	"HMGET":            {},
	"HMSET":            {},
	"HSET":             {},
	"INCR":             {},
	"INFO":             {},
	"KEYS":             {},
	"LINDEX":           {},
	"LLEN":             {},
	"LPOP":             {},
	"LPOS":             {},
	"LPUSH":            {},
	"LRANGE":           {},
	"LREM":             {},
	"LSET":             {},
	"LTRIM":            {},
	"MULTI":            {},
	"PEXPIRE":          {},
	"PFADD":            {},
	"PFCOUNT":          {},
	"PING":             {},
	"PTTL":             {},
	"PUBLISH":          {},
	"PUBSUB":           {},
	"QUIT":             {},
	"RENAME":           {},
	"RPOP":             {},
	"RPOPLPUSH":        {},
	"RPUSH":            {},
	"SADD":             {},
	"SCAN":             {},
	"SCARD":            {},
	"SELECT":           {},
	"SET":              {},
	"SETEX":            {},
	"SETNX":            {},
	"SISMEMBER":        {},
	"SMEMBERS":         {},
	"SREM":             {},
	"SUBSCRIBE":        {},
	"TTL":              {},
	"TYPE":             {},
	"XADD":             {},
	"XLEN":             {},
	"XRANGE":           {},
	"XREAD":            {},
	"XREVRANGE":        {},
	"XTRIM":            {},
	"ZADD":             {},
	"ZCARD":            {},
	"ZCOUNT":           {},
	"ZINCRBY":          {},
	"ZPOPMIN":          {},
	"ZRANGE":           {},
	"ZRANGEBYSCORE":    {},
	"ZREM":             {},
	"ZREMRANGEBYRANK":  {},
	"ZREMRANGEBYSCORE": {},
	"ZREVRANGE":        {},
	"ZREVRANGEBYSCORE": {},
	"ZSCORE":           {},
}

// RedisRequestObserver records per-command Redis API metrics.
type RedisRequestObserver interface {
	ObserveRedisRequest(report RedisRequestReport)
}

// RedisRequestReport is the normalized result of a single Redis command.
type RedisRequestReport struct {
	Command  string
	IsError  bool
	Duration time.Duration
	// Unsupported indicates the command was rejected because the adapter
	// has no route for it. When true, ObserveRedisRequest additionally
	// records the real (bounded) command name in
	// elastickv_redis_unsupported_commands_total alongside the existing
	// "unknown"-bucketed counters, which are preserved unchanged.
	Unsupported bool
}

// RedisMetrics holds all Prometheus metric vectors for the Redis adapter.
type RedisMetrics struct {
	requestsTotal       *prometheus.CounterVec
	requestDuration     *prometheus.HistogramVec
	errorsTotal         *prometheus.CounterVec
	unsupportedCommands *prometheus.CounterVec

	unsupportedMu    sync.RWMutex
	unsupportedNames map[string]struct{}
}

func newRedisMetrics(registerer prometheus.Registerer) *RedisMetrics {
	m := &RedisMetrics{
		requestsTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "elastickv_redis_requests_total",
				Help: "Total number of Redis API requests by command and outcome.",
			},
			[]string{"command", "outcome"},
		),
		requestDuration: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "elastickv_redis_request_duration_seconds",
				Help:    "End-to-end latency of Redis API requests.",
				Buckets: []float64{0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10, 30, 60},
			},
			[]string{"command", "outcome"},
		),
		errorsTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "elastickv_redis_errors_total",
				Help: "Total number of Redis API errors by command.",
			},
			[]string{"command"},
		),
		unsupportedCommands: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "elastickv_redis_unsupported_commands_total",
				Help: "Count of Redis commands rejected as unsupported, labelled with the actual command name (bounded cardinality).",
			},
			[]string{"command"},
		),
		unsupportedNames: make(map[string]struct{}, maxUnsupportedCommandLabels),
	}

	registerer.MustRegister(
		m.requestsTotal,
		m.requestDuration,
		m.errorsTotal,
		m.unsupportedCommands,
	)

	return m
}

// ObserveRedisRequest records the final outcome of a Redis command.
func (m *RedisMetrics) ObserveRedisRequest(report RedisRequestReport) {
	if m == nil {
		return
	}

	command := normalizeRedisCommand(report.Command)
	outcome := redisOutcomeSuccess
	if report.IsError {
		outcome = redisOutcomeError
	}

	m.requestsTotal.WithLabelValues(command, outcome).Inc()
	m.requestDuration.WithLabelValues(command, outcome).Observe(report.Duration.Seconds())
	if report.IsError {
		m.errorsTotal.WithLabelValues(command).Inc()
	}
	if report.Unsupported {
		m.observeUnsupportedCommand(report.Command)
	}
}

// observeUnsupportedCommand increments the bounded-cardinality
// elastickv_redis_unsupported_commands_total counter with the real
// command name, falling back to "other" when the distinct-name cap has
// been reached. The input is uppercased, trimmed, and length-capped to
// avoid pathological label values.
//
// `raw` MUST be the raw bytes received from the client (NOT already
// uppercased). strings.ToUpper silently rewrites invalid UTF-8 bytes
// into the U+FFFD replacement character, which would mask the invalid-
// UTF-8 sentinel path and let a hostile client consume real label slots
// with synthetic "valid" garbage. Keep ToUpper/TrimSpace inside this
// function, AFTER the UTF-8 validity check.
func (m *RedisMetrics) observeUnsupportedCommand(raw string) {
	// Defensive cap on raw input length BEFORE any O(n) string operations
	// (utf8.ValidString, strings.ToUpper, rune iteration). A hostile
	// client sending a multi-megabyte first argument would otherwise burn
	// CPU on every call. Cutting mid-rune is acceptable here because the
	// UTF-8 validity check below rejects the result or the rune-aware
	// truncation further down trims to a clean boundary.
	if len(raw) > maxUnsupportedCommandRawLen {
		raw = raw[:maxUnsupportedCommandRawLen]
	}

	// Guard against raw inputs that are already invalid UTF-8 at ingress
	// (e.g. a binary blob sent as a command name). Passing invalid UTF-8
	// to prometheus.WithLabelValues would panic and crash the calling
	// goroutine. Check the raw string BEFORE strings.ToUpper, which would
	// otherwise silently rewrite invalid bytes to the Unicode replacement
	// character and mask the problem.
	var name string
	if !utf8.ValidString(raw) {
		name = redisUnsupportedCommandInvalidUTF8
	} else {
		name = strings.ToUpper(strings.TrimSpace(raw))
	}
	if name == "" {
		name = redisCommandUnknown
	}
	if len(name) > maxUnsupportedCommandLabelLen {
		// Truncate by UTF-8 rune boundary, not byte boundary, so we never
		// split a multibyte rune and produce invalid UTF-8 (which would
		// make prometheus.WithLabelValues panic). The range-loop index `i`
		// is itself the byte offset of rune `r`; because the upstream
		// UTF-8 validity check guarantees `name` is valid UTF-8 here,
		// utf8.RuneLen(r) is always positive.
		cut := len(name)
		for i, r := range name {
			if i+utf8.RuneLen(r) > maxUnsupportedCommandLabelLen {
				cut = i
				break
			}
		}
		name = name[:cut]
	}

	// Fast path: if the name has already been admitted, only a read lock
	// is needed so concurrent observers do not serialise on the same
	// mutex.
	label := redisUnsupportedCommandOther
	m.unsupportedMu.RLock()
	_, seen := m.unsupportedNames[name]
	m.unsupportedMu.RUnlock()
	if seen {
		label = name
	} else {
		m.unsupportedMu.Lock()
		// Double-check under the write lock: another goroutine may have
		// admitted this name between our RUnlock and Lock.
		if _, seen := m.unsupportedNames[name]; seen {
			label = name
		} else if len(m.unsupportedNames) < maxUnsupportedCommandLabels {
			m.unsupportedNames[name] = struct{}{}
			label = name
		}
		m.unsupportedMu.Unlock()
	}

	m.unsupportedCommands.WithLabelValues(label).Inc()
}

func normalizeRedisCommand(command string) string {
	command = strings.ToUpper(strings.TrimSpace(command))
	if command == "" {
		return redisCommandUnknown
	}
	if _, ok := redisCommandSet[command]; !ok {
		return redisCommandUnknown
	}
	return command
}
