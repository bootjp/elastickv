package proxy

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	defaultPoolSize             = 128
	defaultElasticKVPoolSize    = 192
	defaultDialTimeout          = 5 * time.Second
	defaultReadTimeout          = 3 * time.Second
	defaultElasticKVReadTimeout = 35 * time.Second
	defaultWriteTimeout         = 3 * time.Second
	blockingReadGrace           = 10 * time.Second
	respProtocolV2              = 2
)

// Backend abstracts a Redis-protocol endpoint (real Redis or ElasticKV).
type Backend interface {
	// Do sends a single command and returns its result.
	Do(ctx context.Context, args ...any) *redis.Cmd
	// Pipeline sends multiple commands in a pipeline.
	Pipeline(ctx context.Context, cmds [][]any) ([]*redis.Cmd, error)
	// Close releases the underlying connection.
	Close() error
	// Name identifies this backend for logging and metrics.
	Name() string
}

// BackendOptions configures the underlying go-redis connection pool.
type BackendOptions struct {
	DB           int
	Password     string
	PoolSize     int
	DialTimeout  time.Duration
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
}

// BackendPoolStats is a point-in-time snapshot of a backend's current
// go-redis connection pool.
type BackendPoolStats struct {
	Limit           int
	Hits            uint32
	Misses          uint32
	Timeouts        uint32
	WaitCount       uint32
	Unusable        uint32
	WaitDuration    time.Duration
	TotalConns      uint32
	IdleConns       uint32
	StaleConns      uint32
	PendingRequests uint32
}

type backendPoolStatsProvider interface {
	PoolStats() BackendPoolStats
}

// DefaultBackendOptions returns reasonable defaults for a proxy backend.
func DefaultBackendOptions() BackendOptions {
	return BackendOptions{
		PoolSize:     defaultPoolSize,
		DialTimeout:  defaultDialTimeout,
		ReadTimeout:  defaultReadTimeout,
		WriteTimeout: defaultWriteTimeout,
	}
}

// DefaultElasticKVBackendOptions returns defaults for proxy backends that
// connect to ElasticKV's Redis adapter. Production dual-write deployments
// should run the cluster with ELASTICKV_REDIS_PER_PEER_CONNECTIONS sized for
// every proxy replica that may share one client IP, plus dedicated PubSub and
// shadow PubSub connections outside this command pool. Lower the proxy pool
// instead for clusters that keep the server-side per-peer cap below the
// default.
func DefaultElasticKVBackendOptions() BackendOptions {
	opts := DefaultBackendOptions()
	opts.PoolSize = defaultElasticKVPoolSize
	opts.ReadTimeout = defaultElasticKVReadTimeout
	return opts
}

// PubSubBackend is an optional interface for backends that support
// creating dedicated PubSub connections.
type PubSubBackend interface {
	NewPubSub(ctx context.Context) *redis.PubSub
}

// RedisBackend connects to an upstream Redis instance via go-redis.
type RedisBackend struct {
	client *redis.Client
	name   string
}

// NewRedisBackend creates a Backend targeting a Redis server with default pool options.
func NewRedisBackend(addr string, name string) *RedisBackend {
	return NewRedisBackendWithOptions(addr, name, DefaultBackendOptions())
}

// NewRedisBackendWithOptions creates a Backend with explicit pool configuration.
func NewRedisBackendWithOptions(addr string, name string, opts BackendOptions) *RedisBackend {
	return &RedisBackend{
		client: redis.NewClient(&redis.Options{
			Addr:         addr,
			DB:           opts.DB,
			Password:     opts.Password,
			Protocol:     respProtocolV2,
			PoolSize:     opts.PoolSize,
			DialTimeout:  opts.DialTimeout,
			ReadTimeout:  opts.ReadTimeout,
			WriteTimeout: opts.WriteTimeout,
		}),
		name: name,
	}
}

func (b *RedisBackend) Do(ctx context.Context, args ...any) *redis.Cmd {
	return b.client.Do(ctx, args...)
}

// DoWithTimeout executes a command using a per-call socket timeout override.
// This is used for blocking commands whose wait time exceeds the backend's
// default read timeout.
func (b *RedisBackend) DoWithTimeout(ctx context.Context, timeout time.Duration, args ...any) *redis.Cmd {
	return b.client.WithTimeout(effectiveBlockingReadTimeout(timeout)).Do(ctx, args...)
}

func effectiveBlockingReadTimeout(timeout time.Duration) time.Duration {
	if timeout == 0 {
		return 0
	}
	return timeout + blockingReadGrace
}

func (b *RedisBackend) Pipeline(ctx context.Context, cmds [][]any) ([]*redis.Cmd, error) {
	pipe := b.client.Pipeline()
	results := make([]*redis.Cmd, len(cmds))
	for i, args := range cmds {
		results[i] = pipe.Do(ctx, args...)
	}
	_, err := pipe.Exec(ctx)
	if err != nil {
		// go-redis pipelines return redis.Error for Redis reply errors (e.g., EXECABORT).
		// Return results with nil error so callers can read per-command results (especially EXEC).
		// Only propagate true transport/context errors.
		var redisErr redis.Error
		if errors.As(err, &redisErr) || errors.Is(err, redis.Nil) {
			return results, nil
		}
		return results, fmt.Errorf("pipeline exec: %w", err)
	}
	return results, nil
}

func (b *RedisBackend) Close() error {
	if err := b.client.Close(); err != nil {
		return fmt.Errorf("close %s backend: %w", b.name, err)
	}
	return nil
}

func (b *RedisBackend) Name() string {
	return b.name
}

func (b *RedisBackend) PoolStats() BackendPoolStats {
	return redisPoolStatsSnapshot(b.client.PoolStats(), b.client.Options().PoolSize)
}

func redisPoolStatsSnapshot(stats *redis.PoolStats, limit int) BackendPoolStats {
	if stats == nil {
		return BackendPoolStats{Limit: max(limit, 0)}
	}
	return BackendPoolStats{
		Limit:           max(limit, 0),
		Hits:            stats.Hits,
		Misses:          stats.Misses,
		Timeouts:        stats.Timeouts,
		WaitCount:       stats.WaitCount,
		Unusable:        stats.Unusable,
		WaitDuration:    time.Duration(stats.WaitDurationNs),
		TotalConns:      stats.TotalConns,
		IdleConns:       stats.IdleConns,
		StaleConns:      stats.StaleConns,
		PendingRequests: stats.PendingRequests,
	}
}

// NewPubSub creates a dedicated PubSub connection (not from the pool).
func (b *RedisBackend) NewPubSub(ctx context.Context) *redis.PubSub {
	return b.client.Subscribe(ctx)
}
