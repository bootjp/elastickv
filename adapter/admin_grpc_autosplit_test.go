package adapter

import (
	"context"
	"sync"
	"testing"

	"github.com/bootjp/elastickv/distribution/autosplit"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestAdminSetAutoSplitEnabledControlsRuntimeSwitch(t *testing.T) {
	t.Parallel()
	server := NewAdminServer(NodeIdentity{NodeID: "node-a"}, nil)

	_, err := server.SetAutoSplitEnabled(context.Background(), &pb.SetAutoSplitEnabledRequest{Enabled: false})
	require.Equal(t, codes.Unavailable, status.Code(err))

	runtime := autosplit.NewRuntimeSwitch(true)
	server.RegisterAutoSplitRuntime(runtime)
	disabled, err := server.SetAutoSplitEnabled(context.Background(), &pb.SetAutoSplitEnabledRequest{Enabled: false})
	require.NoError(t, err)
	require.False(t, disabled.GetEnabled())
	require.True(t, runtime.KillSwitch(context.Background()))

	enabled, err := server.SetAutoSplitEnabled(context.Background(), &pb.SetAutoSplitEnabledRequest{Enabled: true})
	require.NoError(t, err)
	require.True(t, enabled.GetEnabled())
	require.False(t, runtime.KillSwitch(context.Background()))
}

func TestAdminSetAutoSplitEnabledIsConcurrentSafe(t *testing.T) {
	t.Parallel()
	server := NewAdminServer(NodeIdentity{NodeID: "node-a"}, nil)
	runtime := autosplit.NewRuntimeSwitch(true)
	server.RegisterAutoSplitRuntime(runtime)

	var wg sync.WaitGroup
	errs := make(chan error, 100)
	for i := range 100 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := server.SetAutoSplitEnabled(context.Background(), &pb.SetAutoSplitEnabledRequest{Enabled: i%2 == 0})
			errs <- err
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}

	_, err := server.SetAutoSplitEnabled(context.Background(), &pb.SetAutoSplitEnabledRequest{Enabled: true})
	require.NoError(t, err)
	require.True(t, runtime.Enabled())
}
