package server

import (
	"testing"

	"github.com/bootjp/elastickv/adapter"
	"github.com/bootjp/elastickv/monitoring"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

func TestEnabledEnv(t *testing.T) {
	t.Run("unset returns default", func(t *testing.T) {
		t.Setenv("ELASTICKV_TEST_BOOL_ENV", "")
		if !enabledEnv("ELASTICKV_TEST_BOOL_ENV") {
			t.Fatal("enabledEnv() = false, want true")
		}
	})

	t.Run("false disables default true", func(t *testing.T) {
		t.Setenv("ELASTICKV_TEST_BOOL_ENV", "false")
		if enabledEnv("ELASTICKV_TEST_BOOL_ENV") {
			t.Fatal("enabledEnv() = true, want false")
		}
	})

	t.Run("invalid returns default", func(t *testing.T) {
		t.Setenv("ELASTICKV_TEST_BOOL_ENV", "maybe")
		if !enabledEnv("ELASTICKV_TEST_BOOL_ENV") {
			t.Fatal("enabledEnv() = false, want true")
		}
	})
}

func TestRedisDeltaCompactorEnv(t *testing.T) {
	t.Run("default enabled", func(t *testing.T) {
		t.Setenv(redisDeltaCompactorEnabledEnvVar, "")
		if newRedisDeltaCompactorIfEnabled(nil, nil, nil) == nil {
			t.Fatal("newRedisDeltaCompactorIfEnabled() = nil, want compactor")
		}
	})

	t.Run("false disables", func(t *testing.T) {
		t.Setenv(redisDeltaCompactorEnabledEnvVar, "false")
		if newRedisDeltaCompactorIfEnabled(nil, nil, nil) != nil {
			t.Fatal("newRedisDeltaCompactorIfEnabled() returned compactor, want nil")
		}
	})
}

func TestRedisDeltaCompactorRegistersLegacyMetadataMetrics(t *testing.T) {
	t.Setenv(redisDeltaCompactorEnabledEnvVar, "true")
	registry := monitoring.NewRegistry("node-1", "localhost")
	compactor := newRedisDeltaCompactorIfEnabled(nil, nil, registry)
	require.NotNil(t, compactor)
	err := registry.Registerer().Register(adapter.NewLegacyMetadataPassCollector(compactor))
	var already prometheus.AlreadyRegisteredError
	require.ErrorAs(t, err, &already)
}
