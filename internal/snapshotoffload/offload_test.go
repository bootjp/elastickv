package snapshotoffload

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	etcdraftengine "github.com/bootjp/elastickv/internal/raftengine/etcd"
	"github.com/stretchr/testify/require"
)

func TestPublishAndRestorePhysicalSnapshotRoundTrip(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	payload := []byte("EKVTHLC1opaque-physical-snapshot-payload")
	sourceDataDir := seedPhysicalSnapshot(t, root, "source", payload, 42, 7, []etcdraftengine.Peer{
		{NodeID: 1, ID: "n1", Address: "127.0.0.1:12001"},
		{NodeID: 2, ID: "n2", Address: "127.0.0.1:12002"},
	})
	store := newTestLocalStore(t, filepath.Join(root, "objects"))

	manifest, err := PublishPersistedSnapshot(ctx, PublishOptions{
		Store:         store,
		DataDir:       sourceDataDir,
		Prefix:        "cluster-a",
		GroupID:       7,
		SourceCluster: "cluster-a",
		BinaryVersion: "test-version",
		CreatedAt:     time.Unix(100, 0).UTC(),
	})
	require.NoError(t, err)
	require.Equal(t, uint64(42), manifest.SnapshotIndex)
	require.Equal(t, uint64(7), manifest.SnapshotTerm)
	require.Equal(t, int64(len(payload)), manifest.Payload.Bytes)
	require.Equal(t, []uint64{1, 2}, manifest.ConfState.Voters)
	require.Contains(t, manifest.Payload.Key, "/v1/payloads/sha256/")
	require.Contains(t, manifest.ManifestKey, "/v1/groups/7/snapshots/")

	storedManifest := loadTestManifest(t, ctx, store, manifest.ManifestKey)
	require.Equal(t, manifest.Payload, storedManifest.Payload)

	payloadPath, err := store.pathForKey(manifest.Payload.Key)
	require.NoError(t, err)
	storedPayload, err := os.ReadFile(payloadPath)
	require.NoError(t, err)
	require.Equal(t, payload, storedPayload)

	restoreDataDir := filepath.Join(root, "restored")
	result, err := RestorePhysicalSnapshot(ctx, RestoreOptions{
		Store:       store,
		ManifestKey: manifest.ManifestKey,
		DataDir:     restoreDataDir,
		Peers: []etcdraftengine.Peer{
			{NodeID: 9, ID: "n9", Address: "127.0.0.1:19009"},
		},
	})
	require.NoError(t, err)
	require.Equal(t, int64(len(payload)), result.PayloadBytes)
	require.Equal(t, manifest.Payload.SHA256, result.PayloadSHA256)

	export, ok, err := etcdraftengine.OpenPersistedSnapshotExport(restoreDataDir)
	require.NoError(t, err)
	require.True(t, ok)
	defer func() { require.NoError(t, export.Close()) }()
	require.Equal(t, []uint64{9}, export.Metadata().ConfState.GetVoters())
	var restored bytes.Buffer
	n, err := export.WriteTo(&restored)
	require.NoError(t, err)
	require.Equal(t, int64(len(payload)), n)
	require.Equal(t, payload, restored.Bytes())
}

func TestRestoreRejectsCorruptPayloadAndLeavesDestinationAbsent(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	payload := []byte("EKVTHLC1payload-before-corruption")
	sourceDataDir := seedPhysicalSnapshot(t, root, "source", payload, 10, 3, singlePeer())
	store := newTestLocalStore(t, filepath.Join(root, "objects"))
	manifest, err := PublishPersistedSnapshot(ctx, PublishOptions{
		Store:         store,
		DataDir:       sourceDataDir,
		Prefix:        "cluster-a",
		GroupID:       1,
		SourceCluster: "cluster-a",
	})
	require.NoError(t, err)
	payloadPath, err := store.pathForKey(manifest.Payload.Key)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(payloadPath, []byte("corrupt"), 0o600))

	restoreDataDir := filepath.Join(root, "restored")
	_, err = RestorePhysicalSnapshot(ctx, RestoreOptions{
		Store:       store,
		ManifestKey: manifest.ManifestKey,
		DataDir:     restoreDataDir,
		Peers:       singlePeer(),
	})
	require.ErrorIs(t, err, ErrIntegrity)
	_, statErr := os.Stat(restoreDataDir)
	require.True(t, os.IsNotExist(statErr))
}

func TestPublishRejectsExistingPayloadMismatchBeforeManifest(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	payload := []byte("EKVTHLC1payload-with-known-sha")
	sourceDataDir := seedPhysicalSnapshot(t, root, "source", payload, 11, 4, singlePeer())
	store := newTestLocalStore(t, filepath.Join(root, "objects"))
	payloadKey, err := payloadKey("cluster-a", hexSHA256Bytes(payload))
	require.NoError(t, err)
	bad := []byte("different-payload")
	_, err = store.PutObject(ctx, payloadKey, bytes.NewReader(bad), PutOptions{
		Size:   int64(len(bad)),
		SHA256: hexSHA256Bytes(bad),
	})
	require.NoError(t, err)

	_, err = PublishPersistedSnapshot(ctx, PublishOptions{
		Store:         store,
		DataDir:       sourceDataDir,
		Prefix:        "cluster-a",
		GroupID:       1,
		SourceCluster: "cluster-a",
	})
	require.ErrorIs(t, err, ErrIntegrity)
	manifestKey, err := manifestKey("cluster-a", 1, 11, 4)
	require.NoError(t, err)
	_, ok, err := store.HeadObject(ctx, manifestKey)
	require.NoError(t, err)
	require.False(t, ok)
}

func seedPhysicalSnapshot(
	t *testing.T,
	root string,
	name string,
	payload []byte,
	index uint64,
	term uint64,
	peers []etcdraftengine.Peer,
) string {
	t.Helper()
	input := filepath.Join(root, name+".fsm")
	require.NoError(t, os.WriteFile(input, payload, 0o600))
	dataDir := filepath.Join(root, name+"-raft")
	_, err := etcdraftengine.PreparePhysicalSnapshotRestore(etcdraftengine.PhysicalSnapshotRestoreOptions{
		InputFSMPath: input,
		DataDir:      dataDir,
		Index:        index,
		Term:         term,
		Peers:        peers,
	})
	require.NoError(t, err)
	return dataDir
}

func newTestLocalStore(t *testing.T, root string) *LocalStore {
	t.Helper()
	store, err := NewLocalStore(root)
	require.NoError(t, err)
	return store
}

func loadTestManifest(t *testing.T, ctx context.Context, store ObjectStore, key string) Manifest {
	t.Helper()
	body, _, err := store.GetObject(ctx, key)
	require.NoError(t, err)
	defer func() { require.NoError(t, body.Close()) }()
	raw, err := io.ReadAll(body)
	require.NoError(t, err)
	manifest, err := DecodeManifest(raw)
	require.NoError(t, err)
	return manifest
}

func singlePeer() []etcdraftengine.Peer {
	return []etcdraftengine.Peer{{NodeID: 1, ID: "n1", Address: "127.0.0.1:12001"}}
}
