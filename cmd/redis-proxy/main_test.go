package main

import (
	"testing"

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
			wantScriptConcurrency:   1,
			wantBlockingConcurrency: 32,
		},
		{
			name:                    "shadow mode derives from ElasticKV pool",
			mode:                    proxy.ModeDualWriteShadow,
			primaryPoolSize:         128,
			elasticKVPoolSize:       8,
			wantWriteConcurrency:    4,
			wantScriptConcurrency:   1,
			wantBlockingConcurrency: 4,
		},
		{
			name:                    "ElasticKV primary derives from Redis secondary pool",
			mode:                    proxy.ModeElasticKVPrimary,
			primaryPoolSize:         128,
			elasticKVPoolSize:       4,
			wantWriteConcurrency:    64,
			wantScriptConcurrency:   2,
			wantBlockingConcurrency: 32,
		},
		{
			name:                    "large remaining pool caps blocking replay",
			mode:                    proxy.ModeDualWrite,
			primaryPoolSize:         128,
			elasticKVPoolSize:       144,
			writeConcurrency:        80,
			wantWriteConcurrency:    80,
			wantScriptConcurrency:   2,
			wantBlockingConcurrency: 32,
		},
		{
			name:                    "explicit write keeps derived script",
			mode:                    proxy.ModeDualWrite,
			primaryPoolSize:         128,
			elasticKVPoolSize:       8,
			writeConcurrency:        5,
			wantWriteConcurrency:    5,
			wantScriptConcurrency:   1,
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

func TestDeriveSecondaryConcurrencyFromDefaultElasticKVPool(t *testing.T) {
	poolSize := proxy.DefaultElasticKVBackendOptions().PoolSize
	writeConcurrency, scriptConcurrency, blockingConcurrency := deriveSecondaryConcurrency(
		proxy.ModeDualWrite,
		proxy.DefaultBackendOptions().PoolSize,
		poolSize,
		0,
		0,
		0,
	)

	require.Equal(t, 192, poolSize)
	require.Equal(t, 96, writeConcurrency)
	require.Equal(t, 3, scriptConcurrency)
	require.Equal(t, 32, blockingConcurrency)
	require.NoError(t, validateSecondaryConcurrency(
		proxy.ModeDualWrite,
		proxy.DefaultBackendOptions().PoolSize,
		poolSize,
		writeConcurrency,
		scriptConcurrency,
		blockingConcurrency,
	))
}
