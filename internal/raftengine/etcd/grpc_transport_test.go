package etcd

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	raftpb "go.etcd.io/raft/v3/raftpb"
)

func TestTransportContextAppliesTimeoutWhenUnset(t *testing.T) {
	ctx, cancel := transportContext(context.Background(), 3*time.Second)
	t.Cleanup(cancel)

	deadline, ok := ctx.Deadline()
	require.True(t, ok)
	require.InDelta(t, 3*time.Second, time.Until(deadline), float64(200*time.Millisecond))
}

func TestTransportContextPreservesExistingDeadline(t *testing.T) {
	parent, cancel := context.WithTimeout(context.Background(), time.Second)
	t.Cleanup(cancel)

	ctx, childCancel := transportContext(parent, 3*time.Second)
	t.Cleanup(childCancel)

	parentDeadline, ok := parent.Deadline()
	require.True(t, ok)
	deadline, ok := ctx.Deadline()
	require.True(t, ok)
	require.Equal(t, parentDeadline, deadline)
}

func TestSplitSnapshotMessageReusesClonedPayload(t *testing.T) {
	msg := raftpb.Message{
		Type: raftpb.MsgSnap,
		Snapshot: &raftpb.Snapshot{
			Data: []byte("snapshot"),
			Metadata: raftpb.SnapshotMetadata{
				Index: 9,
				Term:  3,
			},
		},
	}

	header, payload, err := splitSnapshotMessage(msg)
	require.NoError(t, err)
	require.NotEmpty(t, header)
	require.Equal(t, []byte("snapshot"), payload)

	payload[0] = 'S'
	require.Equal(t, byte('S'), msg.Snapshot.Data[0])
}

func TestNewSnapshotSpoolUsesConfiguredDir(t *testing.T) {
	dir := t.TempDir()
	spool, err := newSnapshotSpool(dir)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, spool.Close())
	})

	require.Equal(t, dir, filepath.Dir(spool.path))
	_, err = os.Stat(spool.path)
	require.NoError(t, err)
}
