package kv

import (
	"bytes"
	"context"
	"io"
	"path/filepath"
	"testing"

	store3 "github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
)

func TestKVFSMSnapshotRoundTripsSSTIngestPayload(t *testing.T) {
	srcStore, err := store3.NewPebbleStore(
		filepath.Join(t.TempDir(), "src"),
		store3.WithSSTIngestSnapshots(true),
	)
	require.NoError(t, err)
	defer srcStore.Close()
	require.NoError(t, srcStore.PutAt(context.Background(), []byte("key"), []byte("value"), 41, 0))

	srcFSM := NewKvFSMWithHLC(srcStore, NewHLC())
	snapshot, err := srcFSM.Snapshot()
	require.NoError(t, err)
	var payload bytes.Buffer
	_, err = snapshot.WriteTo(&payload)
	require.NoError(t, err)
	require.NoError(t, snapshot.Close())

	dstStore, err := store3.NewPebbleStore(filepath.Join(t.TempDir(), "dst"))
	require.NoError(t, err)
	defer dstStore.Close()
	dstFSM := NewKvFSMWithHLC(dstStore, NewHLC())
	require.NoError(t, dstFSM.Restore(io.NopCloser(bytes.NewReader(payload.Bytes()))))

	value, err := dstStore.GetAt(context.Background(), []byte("key"), 41)
	require.NoError(t, err)
	require.Equal(t, []byte("value"), value)
}
