package main

import (
	"testing"
	"time"

	"github.com/bootjp/elastickv/proxy"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseRuntimeOptionsRejectsNegativeSecondaryConcurrency(t *testing.T) {
	_, err := parseRuntimeOptions("dual-write", 128, 4, -1, 0, 0, 0, 0, 0)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "secondary-write-concurrency")

	_, err = parseRuntimeOptions("dual-write", 128, 4, 0, -1, 0, 0, 0, 0)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "secondary-script-concurrency")

	_, err = parseRuntimeOptions("dual-write", 128, 4, 0, 0, -1, 0, 0, 0)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "secondary-blocking-replay-concurrency")
}

func TestParseRuntimeOptionsRejectsNegativeSecondaryQueueSize(t *testing.T) {
	_, err := parseRuntimeOptions("dual-write", 128, 4, 0, 0, 0, -1, 0, 0)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "secondary-write-queue-size")

	_, err = parseRuntimeOptions("dual-write", 128, 4, 0, 0, 0, 0, -1, 0)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "secondary-script-queue-size")

	_, err = parseRuntimeOptions("dual-write", 128, 4, 0, 0, 0, 0, 0, -1)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "secondary-blocking-replay-queue-size")
}

func TestValidateSecondaryConcurrency(t *testing.T) {
	require.NoError(t, validateSecondaryConcurrency(proxy.ModeDualWrite, 128, 8, 4, 2, 4))
	require.NoError(t, validateSecondaryConcurrency(proxy.ModeDualWrite, 128, 1, 1, 1, 0))
	require.NoError(t, validateSecondaryConcurrency(proxy.ModeRedisOnly, 1, 1, 100, 100, 100))
	require.NoError(t, validateSecondaryConcurrency(proxy.ModeElasticKVOnly, 1, 1, 100, 100, 100))

	err := validateSecondaryConcurrency(proxy.ModeDualWrite, 128, 4, 5, 1, 0)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "pool size")

	err = validateSecondaryConcurrency(proxy.ModeDualWrite, 128, 8, 4, 5, 4)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "secondary-write-concurrency")

	err = validateSecondaryConcurrency(proxy.ModeDualWrite, 128, 8, 5, 2, 4)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "secondary-blocking-replay-concurrency")
}

func TestNewBackendsAllowsRedisOnlyWithoutSecondarySeeds(t *testing.T) {
	cfg := proxy.DefaultConfig()
	cfg.Mode = proxy.ModeRedisOnly
	cfg.PrimaryAddr = "127.0.0.1:6379"
	cfg.SecondaryAddr = ""

	primary, secondary, err := newBackends(cfg, 2, 2, nil)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, primary.Close())
		require.NoError(t, secondary.Close())
	})
	assert.Equal(t, "redis", primary.Name())
	assert.Equal(t, "elastickv", secondary.Name())
}

func TestNewBackendsRejectsDualWriteWithoutSecondarySeeds(t *testing.T) {
	cfg := proxy.DefaultConfig()
	cfg.Mode = proxy.ModeDualWrite
	cfg.PrimaryAddr = "127.0.0.1:6379"
	cfg.SecondaryAddr = ""

	_, _, err := newBackends(cfg, 2, 2, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "secondary address")
}

func TestDeriveSecondaryConcurrency(t *testing.T) {
	tests := []struct {
		name                    string
		mode                    proxy.ProxyMode
		primaryPoolSize         int
		elasticKVPoolSize       int
		writeConcurrency        int
		scriptConcurrency       int
		blockingConcurrency     int
		wantWriteConcurrency    int
		wantScriptConcurrency   int
		wantBlockingConcurrency int
	}{
		{
			name:                    "dual write derives from ElasticKV pool",
			mode:                    proxy.ModeDualWrite,
			primaryPoolSize:         128,
			elasticKVPoolSize:       64,
			wantWriteConcurrency:    32,
			wantScriptConcurrency:   16,
			wantBlockingConcurrency: 20,
		},
		{
			name:                    "shadow mode derives from ElasticKV pool",
			mode:                    proxy.ModeDualWriteShadow,
			primaryPoolSize:         128,
			elasticKVPoolSize:       8,
			wantWriteConcurrency:    4,
			wantScriptConcurrency:   2,
			wantBlockingConcurrency: 4,
		},
		{
			name:                    "ElasticKV primary derives from Redis secondary pool",
			mode:                    proxy.ModeElasticKVPrimary,
			primaryPoolSize:         128,
			elasticKVPoolSize:       4,
			wantWriteConcurrency:    64,
			wantScriptConcurrency:   32,
			wantBlockingConcurrency: 20,
		},
		{
			name:                    "large remaining pool caps blocking replay",
			mode:                    proxy.ModeDualWrite,
			primaryPoolSize:         128,
			elasticKVPoolSize:       144,
			writeConcurrency:        80,
			wantWriteConcurrency:    80,
			wantScriptConcurrency:   40,
			wantBlockingConcurrency: 20,
		},
		{
			name:                    "explicit write keeps derived script",
			mode:                    proxy.ModeDualWrite,
			primaryPoolSize:         128,
			elasticKVPoolSize:       8,
			writeConcurrency:        5,
			wantWriteConcurrency:    5,
			wantScriptConcurrency:   2,
			wantBlockingConcurrency: 3,
		},
		{
			name:                    "explicit values win",
			mode:                    proxy.ModeDualWrite,
			primaryPoolSize:         128,
			elasticKVPoolSize:       16,
			writeConcurrency:        5,
			scriptConcurrency:       3,
			blockingConcurrency:     6,
			wantWriteConcurrency:    5,
			wantScriptConcurrency:   3,
			wantBlockingConcurrency: 6,
		},
		{
			name:                    "small pool disables blocking replay",
			mode:                    proxy.ModeDualWrite,
			primaryPoolSize:         128,
			elasticKVPoolSize:       1,
			wantWriteConcurrency:    1,
			wantScriptConcurrency:   1,
			wantBlockingConcurrency: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			writeConcurrency, scriptConcurrency, blockingConcurrency := deriveSecondaryConcurrency(
				tt.mode,
				tt.primaryPoolSize,
				tt.elasticKVPoolSize,
				tt.writeConcurrency,
				tt.scriptConcurrency,
				tt.blockingConcurrency,
			)
			assert.Equal(t, tt.wantWriteConcurrency, writeConcurrency)
			assert.Equal(t, tt.wantScriptConcurrency, scriptConcurrency)
			assert.Equal(t, tt.wantBlockingConcurrency, blockingConcurrency)
		})
	}
}

func TestAlignElasticKVBackendTimeouts(t *testing.T) {
	t.Run("uses ElasticKV dispatch floor by default", func(t *testing.T) {
		opts := proxy.DefaultElasticKVBackendOptions()

		alignElasticKVBackendTimeouts(&opts, 5*time.Second)

		assert.Equal(t, 11*time.Second, opts.ReadTimeout)
		assert.Equal(t, 11*time.Second, opts.WriteTimeout)
	})

	t.Run("follows longer secondary timeout", func(t *testing.T) {
		opts := proxy.DefaultElasticKVBackendOptions()

		alignElasticKVBackendTimeouts(&opts, 15*time.Second)

		assert.Equal(t, 16*time.Second, opts.ReadTimeout)
		assert.Equal(t, 16*time.Second, opts.WriteTimeout)
	})

	t.Run("keeps explicit larger timeout", func(t *testing.T) {
		opts := proxy.DefaultElasticKVBackendOptions()
		opts.ReadTimeout = 30 * time.Second
		opts.WriteTimeout = 31 * time.Second

		alignElasticKVBackendTimeouts(&opts, 15*time.Second)

		assert.Equal(t, 30*time.Second, opts.ReadTimeout)
		assert.Equal(t, 31*time.Second, opts.WriteTimeout)
	})
}
