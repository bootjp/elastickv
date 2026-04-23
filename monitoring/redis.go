package monitoring

import (
	"strings"
	"sync"
	"time"

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

	unsupportedMu    sync.Mutex
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
func (m *RedisMetrics) observeUnsupportedCommand(raw string) {
	name := strings.ToUpper(strings.TrimSpace(raw))
	if name == "" {
		name = redisCommandUnknown
	}
	if len(name) > maxUnsupportedCommandLabelLen {
		name = name[:maxUnsupportedCommandLabelLen]
	}

	label := redisUnsupportedCommandOther
	m.unsupportedMu.Lock()
	if _, seen := m.unsupportedNames[name]; seen {
		label = name
	} else if len(m.unsupportedNames) < maxUnsupportedCommandLabels {
		m.unsupportedNames[name] = struct{}{}
		label = name
	}
	m.unsupportedMu.Unlock()

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
