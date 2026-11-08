package main

import (
	"context"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type capturedHealthListener struct {
	net.ListenConfig
	address string
}

func (l *capturedHealthListener) Listen(ctx context.Context, network, address string) (net.Listener, error) {
	listener, err := l.ListenConfig.Listen(ctx, network, address)
	if err == nil {
		l.address = listener.Addr().String()
	}
	return listener, err
}

func TestRuntimeHealthListenerIsAliveBeforeReadiness(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	listener := &capturedHealthListener{}
	probe, stop, err := startRuntimeHealth(ctx, listener, cancel, "127.0.0.1:0", "n1")
	require.NoError(t, err)
	defer stop()
	client := &http.Client{Timeout: time.Second}
	for _, test := range []struct {
		path string
		code int
	}{{"/livez", http.StatusOK}, {"/readyz", http.StatusServiceUnavailable}, {"/healthz", http.StatusServiceUnavailable}} {
		response, getErr := client.Get("http://" + listener.address + test.path)
		require.NoError(t, getErr)
		require.Equal(t, test.code, response.StatusCode)
		require.NoError(t, response.Body.Close())
	}
	// Opening API services cannot substitute for initial restore/membership observations.
	probe.MarkServing()
	require.False(t, probe.Evaluate(t.Context()).Ready)
}

func TestRuntimeHealthListenerCanBeDisabled(t *testing.T) {
	t.Parallel()
	listener := &capturedHealthListener{}
	_, stop, err := startRuntimeHealth(t.Context(), listener, func() {}, "", "n1")
	require.NoError(t, err)
	stop()
	require.Empty(t, listener.address)
}
