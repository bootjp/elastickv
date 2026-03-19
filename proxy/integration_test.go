package proxy_test

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"testing"
	"time"

	"github.com/bootjp/elastickv/proxy"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const testPrimaryAddr = "localhost:6379"

// skipWithoutRedis skips the test if the required Redis instances are not available.
func skipWithoutRedis(t *testing.T, addrs ...string) {
	t.Helper()
	for _, addr := range addrs {
		d := net.Dialer{Timeout: 500 * time.Millisecond}
		conn, err := d.DialContext(context.Background(), "tcp", addr)
		if err != nil {
			t.Skipf("skipping integration test: cannot connect to %s: %v", addr, err)
		}
		conn.Close()
	}
}

func freePort(t *testing.T) string {
	t.Helper()
	var lc net.ListenConfig
	ln, err := lc.Listen(context.Background(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := ln.Addr().String()
	ln.Close()
	return addr
}

func setupProxy(t *testing.T, mode proxy.ProxyMode, secondaryAddr string) (*redis.Client, context.CancelFunc) {
	t.Helper()
	listenAddr := freePort(t)
	metricsAddr := freePort(t)

	cfg := proxy.ProxyConfig{
		ListenAddr:       listenAddr,
		PrimaryAddr:      testPrimaryAddr,
		SecondaryAddr:    secondaryAddr,
		Mode:             mode,
		SecondaryTimeout: 5 * time.Second,
		ShadowTimeout:    3 * time.Second,
		MetricsAddr:      metricsAddr,
	}

	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))
	reg := prometheus.NewRegistry()
	metrics := proxy.NewProxyMetrics(reg)
	sentryReporter := proxy.NewSentryReporter("", "", 1.0, logger)

	var primary, secondary proxy.Backend
	switch cfg.Mode {
	case proxy.ModeElasticKVPrimary, proxy.ModeElasticKVOnly:
		primary = proxy.NewRedisBackend(cfg.SecondaryAddr, "elastickv")
		secondary = proxy.NewRedisBackend(cfg.PrimaryAddr, "redis")
	case proxy.ModeRedisOnly, proxy.ModeDualWrite, proxy.ModeDualWriteShadow:
		primary = proxy.NewRedisBackend(cfg.PrimaryAddr, "redis")
		secondary = proxy.NewRedisBackend(cfg.SecondaryAddr, "elastickv")
	}

	dual := proxy.NewDualWriter(primary, secondary, cfg, metrics, sentryReporter, logger)
	srv := proxy.NewProxyServer(cfg, dual, metrics, sentryReporter, logger)

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		_ = srv.ListenAndServe(ctx)
	}()

	// Wait for proxy to be ready
	var client *redis.Client
	for i := range 50 {
		_ = i
		d := net.Dialer{Timeout: 100 * time.Millisecond}
		conn, err := d.DialContext(ctx, "tcp", listenAddr)
		if err == nil {
			conn.Close()
			client = redis.NewClient(&redis.Options{Addr: listenAddr})
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	require.NotNil(t, client, "proxy did not become ready")

	t.Cleanup(func() {
		client.Close()
		primary.Close()
		secondary.Close()
	})

	return client, cancel
}

func TestIntegration_RedisOnly(t *testing.T) {
	skipWithoutRedis(t, testPrimaryAddr)

	client, cancel := setupProxy(t, proxy.ModeRedisOnly, "localhost:16399") // secondary unused
	defer cancel()

	ctx := context.Background()
	key := fmt.Sprintf("proxy-test-%d", time.Now().UnixNano())

	// PING
	pong, err := client.Ping(ctx).Result()
	require.NoError(t, err)
	assert.Equal(t, "PONG", pong)

	// SET / GET
	err = client.Set(ctx, key, "hello", 0).Err()
	require.NoError(t, err)

	val, err := client.Get(ctx, key).Result()
	require.NoError(t, err)
	assert.Equal(t, "hello", val)

	// DEL
	err = client.Del(ctx, key).Err()
	require.NoError(t, err)

	_, err = client.Get(ctx, key).Result()
	assert.Equal(t, redis.Nil, err)
}

func TestIntegration_DualWrite(t *testing.T) {
	secondaryAddr := "localhost:6380"
	skipWithoutRedis(t, testPrimaryAddr, secondaryAddr)

	client, cancel := setupProxy(t, proxy.ModeDualWrite, secondaryAddr)
	defer cancel()

	ctx := context.Background()
	key := fmt.Sprintf("proxy-dual-%d", time.Now().UnixNano())

	// Write through proxy
	err := client.Set(ctx, key, "dual-value", 0).Err()
	require.NoError(t, err)

	// Read through proxy (from primary)
	val, err := client.Get(ctx, key).Result()
	require.NoError(t, err)
	assert.Equal(t, "dual-value", val)

	// Poll secondary until async write propagates (avoids flaky fixed sleeps).
	secondaryClient := redis.NewClient(&redis.Options{Addr: secondaryAddr})
	defer secondaryClient.Close()

	require.Eventually(t, func() bool {
		sVal, sErr := secondaryClient.Get(ctx, key).Result()
		return sErr == nil && sVal == "dual-value"
	}, 5*time.Second, 50*time.Millisecond, "secondary should have the value")

	// Clean up — poll until key is gone on secondary.
	client.Del(ctx, key)
	require.Eventually(t, func() bool {
		_, sErr := secondaryClient.Get(ctx, key).Result()
		return errors.Is(sErr, redis.Nil)
	}, 5*time.Second, 50*time.Millisecond, "secondary should have key deleted")
}

func TestIntegration_HashCommands(t *testing.T) {
	skipWithoutRedis(t, testPrimaryAddr)

	client, cancel := setupProxy(t, proxy.ModeRedisOnly, "localhost:16399")
	defer cancel()

	ctx := context.Background()
	key := fmt.Sprintf("proxy-hash-%d", time.Now().UnixNano())

	// HSET
	err := client.HSet(ctx, key, "field1", "val1", "field2", "val2").Err()
	require.NoError(t, err)

	// HGET
	val, err := client.HGet(ctx, key, "field1").Result()
	require.NoError(t, err)
	assert.Equal(t, "val1", val)

	// HGETALL
	all, err := client.HGetAll(ctx, key).Result()
	require.NoError(t, err)
	assert.Equal(t, "val1", all["field1"])
	assert.Equal(t, "val2", all["field2"])

	// HDEL
	err = client.HDel(ctx, key, "field1").Err()
	require.NoError(t, err)

	_, err = client.HGet(ctx, key, "field1").Result()
	assert.Equal(t, redis.Nil, err)

	// Clean up
	client.Del(ctx, key)
}

func TestIntegration_ListCommands(t *testing.T) {
	skipWithoutRedis(t, testPrimaryAddr)

	client, cancel := setupProxy(t, proxy.ModeRedisOnly, "localhost:16399")
	defer cancel()

	ctx := context.Background()
	key := fmt.Sprintf("proxy-list-%d", time.Now().UnixNano())

	// LPUSH
	err := client.LPush(ctx, key, "a", "b", "c").Err()
	require.NoError(t, err)

	// LLEN
	llen, err := client.LLen(ctx, key).Result()
	require.NoError(t, err)
	assert.Equal(t, int64(3), llen)

	// LRANGE
	vals, err := client.LRange(ctx, key, 0, -1).Result()
	require.NoError(t, err)
	assert.Equal(t, []string{"c", "b", "a"}, vals)

	// Clean up
	client.Del(ctx, key)
}

func TestIntegration_Transaction(t *testing.T) {
	skipWithoutRedis(t, testPrimaryAddr)

	client, cancel := setupProxy(t, proxy.ModeRedisOnly, "localhost:16399")
	defer cancel()

	ctx := context.Background()
	key := fmt.Sprintf("proxy-txn-%d", time.Now().UnixNano())

	// MULTI/EXEC via pipeline (go-redis TxPipelined)
	_, err := client.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
		pipe.Set(ctx, key, "txn-val", 0)
		pipe.Get(ctx, key)
		return nil
	})
	require.NoError(t, err)

	// Verify
	val, err := client.Get(ctx, key).Result()
	require.NoError(t, err)
	assert.Equal(t, "txn-val", val)

	// Clean up
	client.Del(ctx, key)
}
