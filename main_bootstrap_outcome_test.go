package main

import (
	"context"
	"testing"

	"github.com/bootjp/elastickv/internal/raftadmission"
	"github.com/bootjp/elastickv/kv"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestBootstrapStartupControlPostSuccessRevocationIsUnknown(t *testing.T) {
	completed := false
	reply, err := runBootstrapStartupControl(t.Context(), nil,
		func(context.Context, any) (any, error) {
			completed = true
			return "committed", nil
		}, func(original error) error {
			require.True(t, completed)
			require.NoError(t, original)
			return raftadmission.ErrStale
		})
	require.Nil(t, reply)
	require.ErrorIs(t, err, raftadmission.ErrStale)
	require.ErrorIs(t, err, kv.ErrTransactionOutcomeUnknown)
	require.Equal(t, codes.Unknown, status.Code(err))
}
