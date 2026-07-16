package main

import (
	"testing"
	"time"

	"github.com/bootjp/elastickv/proxy"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseRuntimeOptionsRejectsNegativeSecondaryConcurrency(t *testing.T) {
	_, err := parseRuntimeOptions("dual-write", 128, 4, -1, 0)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "secondary-write-concurrency")

	_, err = parseRuntimeOptions("dual-write", 128, 4, 0, -1)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "secondary-script-concurrency")
}

func TestDeriveSecondaryConcurrency(t *testing.T) {
	tests := []struct {
		name                  string
		mode                  proxy.ProxyMode
		primaryPoolSize       int
		elasticKVPoolSize     int
		writeConcurrency      int
		scriptConcurrency     int
		wantWriteConcurrency  int
		wantScriptConcurrency int
	}{
		{
			name:                  "dual write derives from ElasticKV pool",
			mode:                  proxy.ModeDualWrite,
			primaryPoolSize:       128,
			elasticKVPoolSize:     4,
			wantWriteConcurrency:  2,
			wantScriptConcurrency: 1,
		},
		{
			name:                  "shadow mode derives from ElasticKV pool",
			mode:                  proxy.ModeDualWriteShadow,
			primaryPoolSize:       128,
			elasticKVPoolSize:     8,
			wantWriteConcurrency:  4,
			wantScriptConcurrency: 1,
		},
		{
			name:                  "ElasticKV primary derives from Redis secondary pool",
			mode:                  proxy.ModeElasticKVPrimary,
			primaryPoolSize:       128,
			elasticKVPoolSize:     4,
			wantWriteConcurrency:  64,
			wantScriptConcurrency: 32,
		},
		{
			name:                  "explicit write keeps derived script",
			mode:                  proxy.ModeDualWrite,
			primaryPoolSize:       128,
			elasticKVPoolSize:     4,
			writeConcurrency:      5,
			wantWriteConcurrency:  5,
			wantScriptConcurrency: 1,
		},
		{
			name:                  "explicit values win",
			mode:                  proxy.ModeDualWrite,
			primaryPoolSize:       128,
			elasticKVPoolSize:     4,
			writeConcurrency:      5,
			scriptConcurrency:     3,
			wantWriteConcurrency:  5,
			wantScriptConcurrency: 3,
		},
		{
			name:                  "small pool stays usable",
			mode:                  proxy.ModeDualWrite,
			primaryPoolSize:       128,
			elasticKVPoolSize:     1,
			wantWriteConcurrency:  1,
			wantScriptConcurrency: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			writeConcurrency, scriptConcurrency := deriveSecondaryConcurrency(
				tt.mode,
				tt.primaryPoolSize,
				tt.elasticKVPoolSize,
				tt.writeConcurrency,
				tt.scriptConcurrency,
			)
			assert.Equal(t, tt.wantWriteConcurrency, writeConcurrency)
			assert.Equal(t, tt.wantScriptConcurrency, scriptConcurrency)
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
