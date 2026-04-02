package monitoring

import (
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

const (
	redisOutcomeSuccess = "success"
	redisOutcomeError   = "error"
	redisCommandUnknown = "unknown"
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
}

// RedisMetrics holds all Prometheus metric vectors for the Redis adapter.
type RedisMetrics struct {
	requestsTotal   *prometheus.CounterVec
	requestDuration *prometheus.HistogramVec
	errorsTotal     *prometheus.CounterVec
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
				Buckets: []float64{0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2, 5},
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
	}

	registerer.MustRegister(
		m.requestsTotal,
		m.requestDuration,
		m.errorsTotal,
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
