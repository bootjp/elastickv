package adapter

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bootjp/elastickv/internal/s3keys"
	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
)

func readyS3LegacyMigrationWorkerConfig() S3LegacyBlobMigrationConfig {
	cfg := defaultS3LegacyBlobMigrationConfig()
	cfg.Enabled = true
	cfg.BurnInStart = time.Now().Add(-31 * 24 * time.Hour)
	return cfg
}

func TestS3LegacyMigrationWorkerConfigDefaultsOffAndRequiresBurnIn(t *testing.T) {
	t.Setenv(s3LegacyMigrationEnabledEnv, "")
	cfg, err := S3LegacyBlobMigrationConfigFromEnv()
	require.NoError(t, err)
	require.False(t, cfg.Enabled)

	t.Setenv(s3LegacyMigrationEnabledEnv, "true")
	_, err = S3LegacyBlobMigrationConfigFromEnv()
	require.ErrorContains(t, err, s3LegacyMigrationBurnInStartEnv)
	t.Setenv(s3LegacyMigrationBurnInStartEnv, time.Now().Add(-29*24*time.Hour).Format(time.RFC3339))
	_, err = S3LegacyBlobMigrationConfigFromEnv()
	require.ErrorContains(t, err, "30-day")
	t.Setenv(s3LegacyMigrationBurnInStartEnv, time.Now().Add(-31*24*time.Hour).Format(time.RFC3339))
	cfg, err = S3LegacyBlobMigrationConfigFromEnv()
	require.NoError(t, err)
	require.True(t, cfg.Enabled)
	t.Setenv(s3LegacyMigrationPageSizeEnv, "65")
	_, err = S3LegacyBlobMigrationConfigFromEnv()
	require.ErrorContains(t, err, "bounded scan")
	t.Setenv(s3LegacyMigrationEnabledEnv, "invalid")
	_, err = S3LegacyBlobMigrationConfigFromEnv()
	require.ErrorContains(t, err, s3LegacyMigrationEnabledEnv)
}

func TestS3LegacyMigrationWorkerScansBoundedPagesWithoutStarvation(t *testing.T) {
	st := store.NewMVCCStore()
	s := newS3MigrationTestServer(t, st)
	putS3MigrationTestObject(t, s, []byte("first"))
	for _, object := range []string{"b", "c"} {
		rec := httptest.NewRecorder()
		s.handle(rec, newS3TestRequest(http.MethodPut, "/bucket-a/"+object, bytes.NewReader([]byte(object))))
		require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	}
	cfg := readyS3LegacyMigrationWorkerConfig()
	cfg.PageSize = 1
	cfg.MaxCandidatesPerPass = 1
	w := &s3LegacyBlobMigrationWorker{config: cfg}
	s.blobOffloadEnabled = true
	for i, object := range []string{"b", "c", "object"} {
		require.NoError(t, w.scanCandidates(t.Context(), s))
		manifest, exists, err := s.loadObjectManifestAt(t.Context(), s3keys.ObjectManifestKey("bucket-a", 1, object), s.readTS())
		require.NoError(t, err)
		require.True(t, exists)
		require.True(t, manifest.Parts[0].Offloaded, "candidate %d was not reached", i)
	}
	require.NoError(t, w.scanCandidates(t.Context(), s))
	require.Nil(t, w.cursor, "complete traversal must wrap for a later pass")
}

func TestS3LegacyMigrationWorkerRecoversBeforeAdmittingCandidate(t *testing.T) {
	st := store.NewMVCCStore()
	s := newS3MigrationTestServer(t, st)
	putS3MigrationTestObject(t, s, []byte("recover before admission"))
	s.blobOffloadEnabled = true
	intent, manifest, generation, sourceTS, pin, err := s.beginS3LegacyMigration(t.Context(), "bucket-a", "object")
	require.NoError(t, err)
	part := manifest.Parts[0]
	require.NoError(t, s.stageS3LegacyChunk(t.Context(), "bucket-a", generation, "object", manifest.UploadID, part, 0, part.ChunkSizes[0], sourceTS, *intent))
	pin.Release()
	w := &s3LegacyBlobMigrationWorker{config: readyS3LegacyMigrationWorkerConfig()}
	w.runPass(t.Context(), s)
	_, err = st.GetAt(t.Context(), s3keys.MigrationIntentKey("bucket-a", generation, "object"), ^uint64(0))
	require.ErrorIs(t, err, store.ErrKeyNotFound)
	after, exists, err := s.loadObjectManifestAt(t.Context(), s3keys.ObjectManifestKey("bucket-a", generation, "object"), s.readTS())
	require.NoError(t, err)
	require.True(t, exists)
	require.True(t, after.Parts[0].Offloaded)
}

func TestS3LegacyMigrationWorkerWaitsForGCReadiness(t *testing.T) {
	st := store.NewMVCCStore()
	s := newS3MigrationTestServer(t, st)
	putS3MigrationTestObject(t, s, []byte("wait for recovery"))
	s.blobOffloadEnabled = true
	s.blobOffloadGCReady.Store(false)
	s.blobGC.mu.Lock()
	s.blobGC.started = true
	s.blobGC.mu.Unlock()
	cfg := readyS3LegacyMigrationWorkerConfig()
	cfg.ScanInterval = 5 * time.Millisecond
	s.legacyMigration = &s3LegacyBlobMigrationWorker{config: cfg}
	require.NoError(t, s.StartLegacyBlobMigration(t.Context()))
	time.Sleep(25 * time.Millisecond)
	manifest, _ := migrationTestManifest(t, s)
	require.False(t, manifest.Parts[0].Offloaded)
	s.blobOffloadGCReady.Store(true)
	require.Eventually(t, func() bool {
		manifest, _ := migrationTestManifest(t, s)
		return manifest.Parts[0].Offloaded
	}, time.Second, time.Millisecond)
	s.legacyMigration.stop()
}

type s3MigrationNotLeaderCoordinator struct{ kv.Coordinator }

func (s3MigrationNotLeaderCoordinator) IsLeader() bool { return false }

func TestS3LegacyMigrationWorkerDoesNotScanWithoutLeader(t *testing.T) {
	st := store.NewMVCCStore()
	s := newS3MigrationTestServer(t, st)
	putS3MigrationTestObject(t, s, []byte("remain legacy"))
	s.blobOffloadEnabled = true
	s.coordinator = s3MigrationNotLeaderCoordinator{Coordinator: s.coordinator}
	w := &s3LegacyBlobMigrationWorker{config: readyS3LegacyMigrationWorkerConfig()}
	w.runPass(t.Context(), s)
	require.Nil(t, w.cursor)
	manifest, _ := migrationTestManifest(t, s)
	require.False(t, manifest.Parts[0].Offloaded)
}

type s3MigrationBlockingScanStore struct {
	store.MVCCStore
	started chan struct{}
	stopped chan struct{}
}

func (s *s3MigrationBlockingScanStore) ScanKeysAt(ctx context.Context, start, end []byte, limit int, ts uint64) ([][]byte, error) {
	if bytes.Equal(start, []byte(s3keys.ObjectManifestPrefix)) {
		close(s.started)
		<-ctx.Done()
		close(s.stopped)
		return nil, ctx.Err()
	}
	return s.MVCCStore.ScanKeysAt(ctx, start, end, limit, ts)
}

func TestS3LegacyMigrationWorkerStopJoinsBlockedScan(t *testing.T) {
	base := store.NewMVCCStore()
	blocking := &s3MigrationBlockingScanStore{MVCCStore: base, started: make(chan struct{}), stopped: make(chan struct{})}
	cfg := readyS3LegacyMigrationWorkerConfig()
	cfg.ScanInterval = time.Hour
	s := NewS3Server(nil, "", blocking, newLocalAdapterCoordinator(base), nil,
		WithS3BlobOffloadEnabled(true),
		WithS3BlobCluster(newFakeS3BlobCluster()),
		WithS3BlobLocalStoreResolver(&mutableS3BlobLocalStore{store: base}),
		WithS3LegacyBlobMigrationConfig(cfg),
	)
	s.blobGC.mu.Lock()
	s.blobGC.started = true
	s.blobGC.mu.Unlock()
	s.blobOffloadGCReady.Store(true)
	require.NoError(t, s.StartLegacyBlobMigration(t.Context()))
	select {
	case <-blocking.started:
	case <-time.After(time.Second):
		t.Fatal("candidate scan did not start")
	}
	s.Stop()
	select {
	case <-blocking.stopped:
	default:
		t.Fatal("Stop returned before scan goroutine exited")
	}
}

type s3MigrationBlockingBlobStore struct {
	store.MVCCStore
	block   atomic.Bool
	once    sync.Once
	started chan struct{}
	release chan struct{}
}

func (s *s3MigrationBlockingBlobStore) GetAt(ctx context.Context, key []byte, ts uint64) ([]byte, error) {
	if s.block.Load() && bytes.HasPrefix(key, []byte(s3keys.BlobPrefix)) {
		s.once.Do(func() { close(s.started) })
		<-s.release // Deliberately ignores cancellation to exercise bounded Stop.
	}
	return s.MVCCStore.GetAt(ctx, key, ts)
}

func TestS3LegacyMigrationWorkerStopBoundsBlockedMigration(t *testing.T) {
	base := store.NewMVCCStore()
	blocking := &s3MigrationBlockingBlobStore{
		MVCCStore: base, started: make(chan struct{}), release: make(chan struct{}),
	}
	cfg := readyS3LegacyMigrationWorkerConfig()
	cfg.ScanInterval = time.Hour
	s := NewS3Server(nil, "", blocking, newLocalAdapterCoordinator(base), nil,
		WithS3BlobOffloadEnabled(false),
		WithS3BlobCluster(newFakeS3BlobCluster()),
		WithS3BlobLocalStoreResolver(&mutableS3BlobLocalStore{store: base}),
		WithS3LegacyBlobMigrationConfig(cfg),
	)
	putS3MigrationTestObject(t, s, []byte("blocked migration"))
	s.blobOffloadEnabled = true
	s.blobGC.mu.Lock()
	s.blobGC.started = true
	s.blobGC.mu.Unlock()
	s.blobOffloadGCReady.Store(true)
	s.legacyMigration.joinTimeout = 20 * time.Millisecond
	blocking.block.Store(true)
	require.NoError(t, s.StartLegacyBlobMigration(t.Context()))
	select {
	case <-blocking.started:
	case <-time.After(time.Second):
		t.Fatal("migration did not reach the blocked legacy blob read")
	}
	done := s.legacyMigration.done
	started := time.Now()
	s.Stop()
	require.Less(t, time.Since(started), time.Second)
	select {
	case <-done:
		t.Fatal("blocked migration unexpectedly exited before release")
	default:
	}
	close(blocking.release)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("cancelled migration did not exit after blocked read released")
	}
	require.Error(t, s.StartLegacyBlobMigration(t.Context()), "stopped worker must not restart")
}
