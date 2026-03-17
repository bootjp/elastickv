package proxy

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	defaultPoolSize     = 128
	defaultDialTimeout  = 5 * time.Second
	defaultReadTimeout  = 3 * time.Second
	defaultWriteTimeout = 3 * time.Second
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
	PoolSize     int
	DialTimeout  time.Duration
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
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

func (b *RedisBackend) Pipeline(ctx context.Context, cmds [][]any) ([]*redis.Cmd, error) {
	pipe := b.client.Pipeline()
	results := make([]*redis.Cmd, len(cmds))
	for i, args := range cmds {
		results[i] = pipe.Do(ctx, args...)
	}
	_, err := pipe.Exec(ctx)
	if err != nil {
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

// NewPubSub creates a dedicated PubSub connection (not from the pool).
func (b *RedisBackend) NewPubSub(ctx context.Context) *redis.PubSub {
	return b.client.Subscribe(ctx)
}
