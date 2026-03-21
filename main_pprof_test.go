package main

import (
	"context"
	"net"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

func TestStartPprofServerRejectsInvalidAddressBeforeTokenCheck(t *testing.T) {
	eg, ctx := errgroup.WithContext(context.Background())
	err := startPprofServer(ctx, &net.ListenConfig{}, eg, "localhost", "")
	require.ErrorContains(t, err, `invalid pprofAddress "localhost"`)
}

func TestStartPprofServerAllowsEmptyAddress(t *testing.T) {
	eg, ctx := errgroup.WithContext(context.Background())
	err := startPprofServer(ctx, &net.ListenConfig{}, eg, "", "")
	require.NoError(t, err)
}
