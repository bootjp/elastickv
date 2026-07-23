package snapshotoffload

import (
	"bytes"
	"context"
	"encoding/json"
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
	sourceDataDir := seedPhysicalSnapshot(t, root, payload, 42, 7, []etcdraftengine.Peer{
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
	sourceDataDir := seedPhysicalSnapshot(t, root, payload, 10, 3, singlePeer())
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
	require.NoError(t, os.WriteFile(payloadPath, bytes.Repeat([]byte("x"), len(payload)), 0o600))

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
	sourceDataDir := seedPhysicalSnapshot(t, root, payload, 11, 4, singlePeer())
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

func TestPublishRejectsExistingManifestMismatch(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	payload := []byte("EKVTHLC1payload-with-conflicting-manifest")
	sourceDataDir := seedPhysicalSnapshot(t, root, payload, 15, 8, singlePeer())
	store := newTestLocalStore(t, filepath.Join(root, "objects"))
	key, err := manifestKey("cluster-a", 1, 15, 8)
	require.NoError(t, err)
	bad := []byte(`{"schema_version":1}`)
	_, err = store.PutObject(ctx, key, bytes.NewReader(bad), PutOptions{
		Size:        int64(len(bad)),
		SHA256:      hexSHA256Bytes(bad),
		ContentType: "application/json",
	})
	require.NoError(t, err)

	_, err = PublishPersistedSnapshot(ctx, PublishOptions{
		Store:         store,
		DataDir:       sourceDataDir,
		Prefix:        "cluster-a",
		GroupID:       1,
		SourceCluster: "cluster-a",
		CreatedAt:     time.Unix(200, 0).UTC(),
	})
	require.ErrorIs(t, err, ErrIntegrity)
}

func TestPublishReusesExistingObjectsWithoutHeadChecksum(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	payload := []byte("EKVTHLC1payload-reused-by-retry")
	sourceDataDir := seedPhysicalSnapshot(t, root, payload, 16, 9, singlePeer())
	store := newTestLocalStore(t, filepath.Join(root, "objects"))
	opts := PublishOptions{
		Store:         store,
		DataDir:       sourceDataDir,
		Prefix:        "cluster-a",
		GroupID:       1,
		SourceCluster: "cluster-a",
		CreatedAt:     time.Unix(300, 0).UTC(),
	}

	first, err := PublishPersistedSnapshot(ctx, opts)
	require.NoError(t, err)
	second, err := PublishPersistedSnapshot(ctx, opts)
	require.NoError(t, err)
	require.Equal(t, first.ManifestKey, second.ManifestKey)
	require.Equal(t, first.Payload.Key, second.Payload.Key)
}

func TestPublishReusesExistingManifestWhenCreatedAtOmitted(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	payload := []byte("EKVTHLC1payload-retry-with-implicit-created-at")
	sourceDataDir := seedPhysicalSnapshot(t, root, payload, 17, 10, singlePeer())
	store := newTestLocalStore(t, filepath.Join(root, "objects"))
	opts := PublishOptions{
		Store:         store,
		DataDir:       sourceDataDir,
		Prefix:        "cluster-a",
		GroupID:       1,
		SourceCluster: "cluster-a",
	}

	first, err := PublishPersistedSnapshot(ctx, opts)
	require.NoError(t, err)
	time.Sleep(time.Millisecond)
	second, err := PublishPersistedSnapshot(ctx, opts)
	require.NoError(t, err)
	require.Equal(t, first.ManifestKey, second.ManifestKey)
	require.Equal(t, first.ManifestSHA256, second.ManifestSHA256)
	require.Equal(t, first.CreatedAt, second.CreatedAt)
}

func TestPutManifestReusesExistingManifestAfterCreateConflict(t *testing.T) {
	ctx := context.Background()
	store := newTestLocalStore(t, filepath.Join(t.TempDir(), "objects"))
	key, err := manifestKey("cluster-a", 1, 20, 13)
	require.NoError(t, err)
	payloadSHA := hexSHA256Bytes([]byte("payload"))
	existing := Manifest{
		SchemaVersion: ManifestSchemaVersion,
		CreatedAt:     time.Unix(400, 0).UTC(),
		SourceCluster: "cluster-a",
		GroupID:       1,
		SnapshotIndex: 20,
		SnapshotTerm:  13,
		ConfState: ManifestConfState{
			Voters: []uint64{1},
		},
		Payload: PayloadDescriptor{
			Key:    "cluster-a/v1/payloads/test.fsm",
			Bytes:  int64(len("payload")),
			SHA256: payloadSHA,
		},
		ManifestKey: key,
	}
	data, _, err := existing.MarshalCanonical()
	require.NoError(t, err)
	_, err = store.PutObject(ctx, key, bytes.NewReader(data), PutOptions{
		Size:        int64(len(data)),
		SHA256:      hexSHA256Bytes(data),
		ContentType: "application/json",
	})
	require.NoError(t, err)

	candidate := existing
	candidate.CreatedAt = time.Unix(401, 0).UTC()
	racingStore := &headMissOnceStore{ObjectStore: store, key: key}
	require.NoError(t, putManifest(ctx, racingStore, &candidate, true))
	require.Equal(t, existing.CreatedAt, candidate.CreatedAt)
	require.NotEmpty(t, candidate.ManifestSHA256)
}

func TestDecodeManifestRejectsInvalidConfStateMembership(t *testing.T) {
	base := testManifestForValidation(t)
	testCases := []ManifestConfState{
		{Voters: nil},
		{Voters: []uint64{0}},
		{Voters: []uint64{1}, Learners: []uint64{1}},
		{Voters: []uint64{1}, VotersOutgoing: []uint64{1}, LearnersNext: []uint64{1}},
		{Voters: []uint64{1}, VotersOutgoing: []uint64{1}, LearnersNext: []uint64{2}},
		{Voters: []uint64{1}, AutoLeave: true},
	}
	for _, confState := range testCases {
		manifest := base
		manifest.ConfState = confState
		raw, _, err := manifest.MarshalCanonical()
		require.NoError(t, err)
		_, err = DecodeManifest(raw)
		require.ErrorIs(t, err, ErrInvalidOptions)
	}
}

func TestDecodeManifestAcceptsJointConsensusConfState(t *testing.T) {
	manifest := testManifestForValidation(t)
	manifest.ConfState = ManifestConfState{
		Voters:         []uint64{1, 2},
		VotersOutgoing: []uint64{1, 3},
		LearnersNext:   []uint64{3},
		AutoLeave:      true,
	}
	raw, _, err := manifest.MarshalCanonical()
	require.NoError(t, err)
	decoded, err := DecodeManifest(raw)
	require.NoError(t, err)
	require.Equal(t, manifest.ConfState, decoded.ConfState)
}

func TestPublishRejectsGroupZeroWithoutSourceClusterBeforePayload(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	payload := []byte("EKVTHLC1payload-invalid-group-zero")
	sourceDataDir := seedPhysicalSnapshot(t, root, payload, 18, 11, singlePeer())
	store := newTestLocalStore(t, filepath.Join(root, "objects"))

	_, err := PublishPersistedSnapshot(ctx, PublishOptions{
		Store:   store,
		DataDir: sourceDataDir,
		Prefix:  "cluster-a",
		GroupID: 0,
	})
	require.ErrorIs(t, err, ErrInvalidOptions)

	payloadKey, err := payloadKey("cluster-a", hexSHA256Bytes(payload))
	require.NoError(t, err)
	_, ok, err := store.HeadObject(ctx, payloadKey)
	require.NoError(t, err)
	require.False(t, ok)
}

func TestPublishUsesDataDirLocalSpoolByDefault(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	payload := []byte("EKVTHLC1payload-spooled-near-data-dir")
	sourceDataDir := seedPhysicalSnapshot(t, root, payload, 19, 12, singlePeer())
	store := newTestLocalStore(t, filepath.Join(root, "objects"))

	_, err := PublishPersistedSnapshot(ctx, PublishOptions{
		Store:         store,
		DataDir:       sourceDataDir,
		Prefix:        "cluster-a",
		GroupID:       1,
		SourceCluster: "cluster-a",
	})
	require.NoError(t, err)
	require.DirExists(t, filepath.Join(filepath.Dir(sourceDataDir), ".snapshot-offload-spool"))
}

func TestLoadManifestRejectsStaleSelfHash(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	payload := []byte("EKVTHLC1payload-for-stale-manifest-hash")
	sourceDataDir := seedPhysicalSnapshot(t, root, payload, 12, 5, singlePeer())
	store := newTestLocalStore(t, filepath.Join(root, "objects"))
	manifest, err := PublishPersistedSnapshot(ctx, PublishOptions{
		Store:         store,
		DataDir:       sourceDataDir,
		Prefix:        "cluster-a",
		GroupID:       1,
		SourceCluster: "cluster-a",
	})
	require.NoError(t, err)
	manifestPath, err := store.pathForKey(manifest.ManifestKey)
	require.NoError(t, err)
	raw, err := os.ReadFile(manifestPath)
	require.NoError(t, err)
	var body map[string]any
	require.NoError(t, json.Unmarshal(raw, &body))
	body["snapshot_index"] = float64(99)
	tampered, err := json.MarshalIndent(body, "", "  ")
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(manifestPath, append(tampered, '\n'), 0o600))

	_, err = LoadManifest(ctx, store, manifest.ManifestKey)
	require.ErrorIs(t, err, ErrIntegrity)
}

func TestRestoreInlineManifestRejectsStaleSelfHashBeforePayloadDownload(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	payload := []byte("EKVTHLC1payload-for-inline-stale-manifest-hash")
	sourceDataDir := seedPhysicalSnapshot(t, root, payload, 21, 14, singlePeer())
	store := newTestLocalStore(t, filepath.Join(root, "objects"))
	manifest, err := PublishPersistedSnapshot(ctx, PublishOptions{
		Store:         store,
		DataDir:       sourceDataDir,
		Prefix:        "cluster-a",
		GroupID:       1,
		SourceCluster: "cluster-a",
	})
	require.NoError(t, err)
	tampered := *manifest
	tampered.SnapshotIndex++
	tracked := &countingObjectStore{ObjectStore: store}

	_, err = RestorePhysicalSnapshot(ctx, RestoreOptions{
		Store:    tracked,
		Manifest: &tampered,
		DataDir:  filepath.Join(root, "restored"),
		Peers:    singlePeer(),
	})
	require.ErrorIs(t, err, ErrIntegrity)
	require.Zero(t, tracked.getObjectCalls)
}

func TestLoadManifestRejectsOversizedBody(t *testing.T) {
	ctx := context.Background()
	store := newTestLocalStore(t, filepath.Join(t.TempDir(), "objects"))
	key := "cluster-a/v1/groups/1/snapshots/oversized.json"
	body := bytes.Repeat([]byte("x"), maxManifestBytes+1)
	_, err := store.PutObject(ctx, key, bytes.NewReader(body), PutOptions{
		Size:        int64(len(body)),
		SHA256:      hexSHA256Bytes(body),
		ContentType: "application/json",
	})
	require.NoError(t, err)

	_, err = LoadManifest(ctx, store, key)
	require.ErrorIs(t, err, ErrInvalidOptions)
}

func TestLocalStoreRejectsParentDirectoryKeys(t *testing.T) {
	ctx := context.Background()
	store := newTestLocalStore(t, filepath.Join(t.TempDir(), "objects"))
	emptySHA := hexSHA256Bytes(nil)

	_, err := store.PutObject(ctx, "..", bytes.NewReader(nil), PutOptions{SHA256: emptySHA})
	require.ErrorIs(t, err, ErrInvalidOptions)
	_, err = store.PutObject(ctx, "a/../..", bytes.NewReader(nil), PutOptions{SHA256: emptySHA})
	require.ErrorIs(t, err, ErrInvalidOptions)
}

func TestLocalStoreHeadObjectUsesStatMetadata(t *testing.T) {
	ctx := context.Background()
	store := newTestLocalStore(t, filepath.Join(t.TempDir(), "objects"))
	body := []byte("metadata-only-head")
	key := "cluster-a/v1/payloads/test.fsm"
	putInfo, err := store.PutObject(ctx, key, bytes.NewReader(body), PutOptions{
		Size:        int64(len(body)),
		SHA256:      hexSHA256Bytes(body),
		ContentType: "application/octet-stream",
	})
	require.NoError(t, err)
	require.Equal(t, hexSHA256Bytes(body), putInfo.SHA256)

	headInfo, ok, err := store.HeadObject(ctx, key)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, int64(len(body)), headInfo.Size)
	require.Empty(t, headInfo.SHA256)

	reader, getInfo, err := store.GetObject(ctx, key)
	require.NoError(t, err)
	defer func() { require.NoError(t, reader.Close()) }()
	require.Equal(t, int64(len(body)), getInfo.Size)
	require.Empty(t, getInfo.SHA256)
	got, err := io.ReadAll(reader)
	require.NoError(t, err)
	require.Equal(t, body, got)
}

func TestPublishHonorsCancelledContextWhileSpooling(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	root := t.TempDir()
	payload := []byte("EKVTHLC1payload-cancelled-before-spool")
	sourceDataDir := seedPhysicalSnapshot(t, root, payload, 13, 6, singlePeer())
	store := newTestLocalStore(t, filepath.Join(root, "objects"))

	_, err := PublishPersistedSnapshot(ctx, PublishOptions{
		Store:         store,
		DataDir:       sourceDataDir,
		Prefix:        "cluster-a",
		GroupID:       1,
		SourceCluster: "cluster-a",
	})
	require.ErrorIs(t, err, context.Canceled)
}

func TestRestorePreflightsExistingDestinationBeforePayloadDownload(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	payload := []byte("EKVTHLC1payload-before-existing-destination")
	sourceDataDir := seedPhysicalSnapshot(t, root, payload, 14, 7, singlePeer())
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
	require.NoError(t, os.Mkdir(restoreDataDir, 0o755))

	_, err = RestorePhysicalSnapshot(ctx, RestoreOptions{
		Store:       store,
		ManifestKey: manifest.ManifestKey,
		DataDir:     restoreDataDir,
		Peers:       singlePeer(),
	})
	require.ErrorIs(t, err, etcdraftengine.ErrExternalSnapshotRestoreExists)
}

func TestRestoreRejectsInvalidPeersBeforePayloadDownload(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	payload := []byte("EKVTHLC1payload-invalid-restore-peers")
	sourceDataDir := seedPhysicalSnapshot(t, root, payload, 22, 15, singlePeer())
	store := newTestLocalStore(t, filepath.Join(root, "objects"))
	manifest, err := PublishPersistedSnapshot(ctx, PublishOptions{
		Store:         store,
		DataDir:       sourceDataDir,
		Prefix:        "cluster-a",
		GroupID:       1,
		SourceCluster: "cluster-a",
	})
	require.NoError(t, err)
	tracked := &countingObjectStore{ObjectStore: store}

	_, err = RestorePhysicalSnapshot(ctx, RestoreOptions{
		Store:    tracked,
		Manifest: manifest,
		DataDir:  filepath.Join(root, "restored"),
		Peers: []etcdraftengine.Peer{
			{NodeID: 0, ID: "n0", Address: "127.0.0.1:12000"},
		},
	})
	require.ErrorIs(t, err, ErrInvalidOptions)
	require.Zero(t, tracked.getObjectCalls)
}

func TestRestoreHonorsCancelledContextBeforePayloadDownload(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	root := t.TempDir()
	payload := []byte("EKVTHLC1payload-cancelled-before-restore-download")
	sourceDataDir := seedPhysicalSnapshot(t, root, payload, 23, 16, singlePeer())
	store := newTestLocalStore(t, filepath.Join(root, "objects"))
	manifest, err := PublishPersistedSnapshot(context.Background(), PublishOptions{
		Store:         store,
		DataDir:       sourceDataDir,
		Prefix:        "cluster-a",
		GroupID:       1,
		SourceCluster: "cluster-a",
	})
	require.NoError(t, err)
	tracked := &countingObjectStore{ObjectStore: store}

	_, err = RestorePhysicalSnapshot(ctx, RestoreOptions{
		Store:    tracked,
		Manifest: manifest,
		DataDir:  filepath.Join(root, "restored"),
		Peers:    singlePeer(),
	})
	require.ErrorIs(t, err, context.Canceled)
	require.Zero(t, tracked.getObjectCalls)
}

func TestPrepareRestoreDownloadDirCreatesParentAndCleansOnlyStaleDirs(t *testing.T) {
	root := t.TempDir()
	dataDir := filepath.Join(root, "missing-parent", "restored")

	downloadDir, err := prepareRestoreDownloadDir(dataDir)
	require.NoError(t, err)
	require.DirExists(t, filepath.Dir(dataDir))
	require.Contains(t, downloadDir, filepath.Dir(dataDir))
	require.NoError(t, os.RemoveAll(downloadDir))

	parent := filepath.Dir(dataDir)
	activeDir := filepath.Join(parent, ".snapshot-offload-restore-active")
	require.NoError(t, os.MkdirAll(activeDir, 0o755))
	staleDir := filepath.Join(parent, ".snapshot-offload-restore-stale")
	require.NoError(t, os.MkdirAll(staleDir, 0o755))
	oldTime := time.Now().Add(-restoreTempDirStaleAfter - time.Hour)
	require.NoError(t, os.Chtimes(staleDir, oldTime, oldTime))
	downloadDir, err = prepareRestoreDownloadDir(dataDir)
	require.NoError(t, err)
	defer func() { require.NoError(t, os.RemoveAll(downloadDir)) }()
	require.DirExists(t, activeDir)
	require.NoError(t, os.RemoveAll(activeDir))
	_, err = os.Stat(staleDir)
	require.True(t, os.IsNotExist(err))
}

func testManifestForValidation(t *testing.T) Manifest {
	t.Helper()
	payloadSHA := hexSHA256Bytes([]byte("payload"))
	key, err := manifestKey("cluster-a", 1, 20, 13)
	require.NoError(t, err)
	return Manifest{
		SchemaVersion: ManifestSchemaVersion,
		CreatedAt:     time.Unix(400, 0).UTC(),
		SourceCluster: "cluster-a",
		GroupID:       1,
		SnapshotIndex: 20,
		SnapshotTerm:  13,
		ConfState: ManifestConfState{
			Voters: []uint64{1},
		},
		Payload: PayloadDescriptor{
			Key:    "cluster-a/v1/payloads/test.fsm",
			Bytes:  int64(len("payload")),
			SHA256: payloadSHA,
		},
		ManifestKey: key,
	}
}

func seedPhysicalSnapshot(
	t *testing.T,
	root string,
	payload []byte,
	index uint64,
	term uint64,
	peers []etcdraftengine.Peer,
) string {
	t.Helper()
	input := filepath.Join(root, "source.fsm")
	require.NoError(t, os.WriteFile(input, payload, 0o600))
	dataDir := filepath.Join(root, "source-raft")
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

type countingObjectStore struct {
	ObjectStore
	getObjectCalls int
}

func (s *countingObjectStore) GetObject(ctx context.Context, key string) (io.ReadCloser, ObjectInfo, error) {
	s.getObjectCalls++
	return s.ObjectStore.GetObject(ctx, key)
}

type headMissOnceStore struct {
	ObjectStore
	key  string
	miss bool
}

func (s *headMissOnceStore) HeadObject(ctx context.Context, key string) (ObjectInfo, bool, error) {
	if !s.miss && normalizeObjectKey(key) == normalizeObjectKey(s.key) {
		s.miss = true
		return ObjectInfo{}, false, nil
	}
	return s.ObjectStore.HeadObject(ctx, key)
}

func singlePeer() []etcdraftengine.Peer {
	return []etcdraftengine.Peer{{NodeID: 1, ID: "n1", Address: "127.0.0.1:12001"}}
}
