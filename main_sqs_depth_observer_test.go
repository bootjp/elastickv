package main

import (
	"context"
	"testing"

	"github.com/bootjp/elastickv/adapter"
	"github.com/stretchr/testify/require"
)

type fakeSQSDepthBridgeSource struct {
	snaps []adapter.SQSQueueDepth
	ok    bool
}

func (f fakeSQSDepthBridgeSource) SnapshotQueueDepths(context.Context) ([]adapter.SQSQueueDepth, bool) {
	return f.snaps, f.ok
}

func TestSQSDepthSourceAdapterPreservesNilVsEmptySnapshots(t *testing.T) {
	t.Parallel()

	got, ok := (sqsDepthSourceAdapter{inner: fakeSQSDepthBridgeSource{snaps: nil, ok: true}}).
		SnapshotQueueDepths(context.Background())
	require.True(t, ok)
	require.Nil(t, got, "nil successful snapshots must remain nil for follower/step-down cleanup")

	got, ok = (sqsDepthSourceAdapter{inner: fakeSQSDepthBridgeSource{snaps: []adapter.SQSQueueDepth{}, ok: true}}).
		SnapshotQueueDepths(context.Background())
	require.True(t, ok)
	require.NotNil(t, got, "non-nil empty leader snapshots must survive the adapter bridge")
	require.Empty(t, got)
}
