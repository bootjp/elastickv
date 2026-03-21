package main

import (
	"context"
	"net"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestEffectiveDemoMetricsToken(t *testing.T) {
	require.Equal(t, "custom-token", effectiveDemoMetricsToken(" custom-token "))
	require.Equal(t, "demo-metrics-token", effectiveDemoMetricsToken(""))
}

func TestSetupMetricsHTTPServerAllowsBlankAddress(t *testing.T) {
	listener, server, err := setupMetricsHTTPServer(context.Background(), net.ListenConfig{}, "", "", http.NewServeMux())
	require.NoError(t, err)
	require.Nil(t, listener)
	require.Nil(t, server)
}

func TestSetupMetricsHTTPServerRejectsInvalidAddressBeforeTokenCheck(t *testing.T) {
	listener, server, err := setupMetricsHTTPServer(context.Background(), net.ListenConfig{}, "localhost", "", http.NewServeMux())
	require.ErrorContains(t, err, `invalid metricsAddress "localhost"`)
	require.Nil(t, listener)
	require.Nil(t, server)
}

func TestSetupPprofHTTPServerAllowsBlankAddress(t *testing.T) {
	listener, server, err := setupPprofHTTPServer(context.Background(), net.ListenConfig{}, "", "")
	require.NoError(t, err)
	require.Nil(t, listener)
	require.Nil(t, server)
}

func TestSetupPprofHTTPServerRejectsInvalidAddressBeforeTokenCheck(t *testing.T) {
	listener, server, err := setupPprofHTTPServer(context.Background(), net.ListenConfig{}, "localhost", "")
	require.ErrorContains(t, err, `invalid pprofAddress "localhost"`)
	require.Nil(t, listener)
	require.Nil(t, server)
}

