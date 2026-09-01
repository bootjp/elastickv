package main

import "testing"

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
		if newRedisDeltaCompactorIfEnabled(nil, nil) == nil {
			t.Fatal("newRedisDeltaCompactorIfEnabled() = nil, want compactor")
		}
	})

	t.Run("false disables", func(t *testing.T) {
		t.Setenv(redisDeltaCompactorEnabledEnvVar, "false")
		if newRedisDeltaCompactorIfEnabled(nil, nil) != nil {
			t.Fatal("newRedisDeltaCompactorIfEnabled() returned compactor, want nil")
		}
	})
}
