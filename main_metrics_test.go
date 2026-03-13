package main

import (
	"context"
	"net"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

func TestStartMetricsServerRejectsInvalidAddressBeforeTokenCheck(t *testing.T) {
	eg, ctx := errgroup.WithContext(context.Background())
	err := startMetricsServer(ctx, &net.ListenConfig{}, eg, "localhost", "", http.NewServeMux())
	require.ErrorContains(t, err, `invalid metricsAddress "localhost"`)
}
