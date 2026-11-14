package adapter

import (
	"bytes"
	"context"
	"crypto/sha256"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/bootjp/elastickv/distribution"
	"github.com/bootjp/elastickv/internal/s3keys"
	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
)

type s3MigrationLostReplyCoordinator struct {
	kv.Coordinator
	failKey    []byte
	failDelete bool
	fired      bool
}

type s3MigrationFollowerCoordinator struct{ kv.Coordinator }

func (s3MigrationFollowerCoordinator) IsLeader() bool { return false }

func (c *s3MigrationLostReplyCoordinator) Dispatch(ctx context.Context, req *kv.OperationGroup[kv.OP]) (*kv.CoordinateResponse, error) {
	resp, err := c.Coordinator.Dispatch(ctx, req)
	if err != nil || c.fired {
		return resp, err
	}
	for _, elem := range req.Elems {
		if bytes.Equal(elem.Key, c.failKey) && ((c.failDelete && elem.Op == kv.Del) || (!c.failDelete && elem.Op == kv.Put)) {
			c.fired = true
			return nil, kv.ErrTransactionOutcomeUnknown
		}
	}
	return resp, nil
}

func newS3MigrationTestServer(t *testing.T, st store.MVCCStore) *S3Server {
	t.Helper()
	return NewS3Server(nil, "", st, newLocalAdapterCoordinator(st), nil,
		WithS3BlobOffloadEnabled(false),
		WithS3BlobCluster(newFakeS3BlobCluster()),
		WithS3BlobLocalStoreResolver(&mutableS3BlobLocalStore{store: st}),
		withS3BlobOffloadGCReadyForTest(),
	)
}

func putS3MigrationTestObject(t *testing.T, s *S3Server, payload []byte) {
	t.Helper()
	rec := httptest.NewRecorder()
	s.handle(rec, newS3TestRequest(http.MethodPut, "/bucket-a", nil))
	if rec.Code != http.StatusOK && rec.Code != http.StatusConflict {
		require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	}
	rec = httptest.NewRecorder()
	s.handle(rec, newS3TestRequest(http.MethodPut, "/bucket-a/object", bytes.NewReader(payload)))
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
}

func getS3MigrationTestObject(t *testing.T, s *S3Server) []byte {
	t.Helper()
	rec := httptest.NewRecorder()
	s.handle(rec, newS3TestRequest(http.MethodGet, "/bucket-a/object", nil))
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	return rec.Body.Bytes()
}

func migrationTestManifest(t *testing.T, s *S3Server) (*s3ObjectManifest, uint64) {
	t.Helper()
	ts := s.readTS()
	manifest, ok, err := s.loadObjectManifestAt(t.Context(), s3keys.ObjectManifestKey("bucket-a", 1, "object"), ts)
	require.NoError(t, err)
	require.True(t, ok)
	return manifest, ts
}

func TestS3LegacyMigrationPreservesMetadataAndSnapshot(t *testing.T) {
	st := store.NewMVCCStore()
	s := newS3MigrationTestServer(t, st)
	payload := bytes.Repeat([]byte("legacy-migration"), s3ChunkSize/len("legacy-migration")+20)
	putS3MigrationTestObject(t, s, payload)
	before, snapshotTS := migrationTestManifest(t, s)
	legacyKey := s3keys.VersionedBlobKey("bucket-a", 1, "object", before.UploadID, 1, 0, before.Parts[0].PartVersion)
	s.blobOffloadEnabled = true
	changed, err := s.migrateS3LegacyObject(t.Context(), "bucket-a", "object")
	require.NoError(t, err)
	require.True(t, changed)
	after, _ := migrationTestManifest(t, s)
	require.Equal(t, before.ETag, after.ETag)
	require.Equal(t, before.LastModifiedHLC, after.LastModifiedHLC)
	require.Equal(t, before.UploadID, after.UploadID)
	require.True(t, after.Parts[0].Offloaded)
	require.NotZero(t, after.Parts[0].ChunkRefVersion)
	require.Equal(t, payload, getS3MigrationTestObject(t, s))
	var snapshotBody []byte
	for _, part := range before.Parts {
		for chunkNo, size := range part.ChunkSizes {
			chunk, err := s.readS3ObjectChunk(t.Context(), "bucket-a", 1, "object", before.UploadID, part, uint64(chunkNo), size, snapshotTS) //nolint:gosec // Bounded manifest index.
			require.NoError(t, err)
			snapshotBody = append(snapshotBody, chunk...)
		}
	}
	require.Equal(t, payload, snapshotBody)
	old, err := st.GetAt(t.Context(), legacyKey, snapshotTS)
	require.NoError(t, err)
	require.Equal(t, payload[:len(old)], old)
	_, err = st.GetAt(t.Context(), legacyKey, ^uint64(0))
	require.ErrorIs(t, err, store.ErrKeyNotFound)
	_, err = st.GetAt(t.Context(), s3keys.MigrationIntentKey("bucket-a", 1, "object"), ^uint64(0))
	require.ErrorIs(t, err, store.ErrKeyNotFound)
}

func TestS3LegacyMigrationRestartAbortsStagedRefsWithRC(t *testing.T) {
	st := store.NewMVCCStore()
	s := newS3MigrationTestServer(t, st)
	payload := []byte("staged migration payload")
	putS3MigrationTestObject(t, s, payload)
	s.blobOffloadEnabled = true
	intent, manifest, generation, sourceTS, pin, err := s.beginS3LegacyMigration(t.Context(), "bucket-a", "object")
	require.NoError(t, err)
	require.NotNil(t, intent)
	part := manifest.Parts[0]
	require.NoError(t, s.stageS3LegacyChunk(t.Context(), "bucket-a", generation, "object", manifest.UploadID, part, 0, part.ChunkSizes[0], sourceTS, *intent))
	pin.Release() // Simulate the process ending before the manifest flip.
	refKey := s3keys.VersionedChunkRefKey("bucket-a", generation, "object", manifest.UploadID, part.PartNo, 0, intent.AttemptVersion)
	digest := sha256.Sum256(payload)
	raw, err := st.GetAt(t.Context(), s3keys.ChunkRefRCKey(digest), ^uint64(0))
	require.NoError(t, err)
	rc, ok := s3keys.DecodeChunkRefRC(raw)
	require.True(t, ok)
	require.Equal(t, uint64(1), rc.Count)
	_, _, _, _, _, err = s.beginS3LegacyMigration(t.Context(), "bucket-a", "object")
	require.ErrorContains(t, err, "intent already exists")
	restarted := newS3MigrationTestServer(t, st)
	require.NoError(t, restarted.recoverPendingS3Migrations(t.Context()))
	_, err = st.GetAt(t.Context(), refKey, ^uint64(0))
	require.ErrorIs(t, err, store.ErrKeyNotFound)
	raw, err = st.GetAt(t.Context(), s3keys.ChunkRefRCKey(digest), ^uint64(0))
	require.NoError(t, err)
	rc, ok = s3keys.DecodeChunkRefRC(raw)
	require.True(t, ok)
	require.Zero(t, rc.Count)
	_, err = st.GetAt(t.Context(), s3keys.MigrationIntentKey("bucket-a", 1, "object"), ^uint64(0))
	require.ErrorIs(t, err, store.ErrKeyNotFound)
	require.Equal(t, payload, getS3MigrationTestObject(t, restarted))
}

func TestS3LegacyMigrationRestartFinishesOldBlobCleanup(t *testing.T) {
	st := store.NewMVCCStore()
	s := newS3MigrationTestServer(t, st)
	payload := []byte("flipped migration payload")
	putS3MigrationTestObject(t, s, payload)
	s.blobOffloadEnabled = true
	intent, manifest, generation, sourceTS, pin, err := s.beginS3LegacyMigration(t.Context(), "bucket-a", "object")
	require.NoError(t, err)
	part := manifest.Parts[0]
	require.NoError(t, s.stageS3LegacyChunk(t.Context(), "bucket-a", generation, "object", manifest.UploadID, part, 0, part.ChunkSizes[0], sourceTS, *intent))
	require.NoError(t, s.flipS3LegacyManifest(t.Context(), "bucket-a", generation, "object", *intent))
	pin.Release()
	legacyKey := s3keys.VersionedBlobKey("bucket-a", generation, "object", manifest.UploadID, part.PartNo, 0, part.PartVersion)
	_, err = st.GetAt(t.Context(), legacyKey, ^uint64(0))
	require.NoError(t, err)
	markerKey := s3keys.MigrationIntentKey("bucket-a", 1, "object")
	markerRaw, err := st.GetAt(t.Context(), markerKey, ^uint64(0))
	require.NoError(t, err)
	marker, err := decodeS3MigrationIntent(markerRaw)
	require.NoError(t, err)
	require.Equal(t, s3MigrationFlipped, marker.Phase)
	restarted := newS3MigrationTestServer(t, st)
	require.NoError(t, restarted.recoverPendingS3Migrations(t.Context()))
	_, err = st.GetAt(t.Context(), legacyKey, ^uint64(0))
	require.ErrorIs(t, err, store.ErrKeyNotFound)
	_, err = st.GetAt(t.Context(), s3keys.MigrationIntentKey("bucket-a", 1, "object"), ^uint64(0))
	require.ErrorIs(t, err, store.ErrKeyNotFound)
	require.Equal(t, payload, getS3MigrationTestObject(t, restarted))
}

func TestS3LegacyMigrationRecoveryAcrossShards(t *testing.T) {
	ctx := t.Context()
	engine := distribution.NewEngine()
	boundary := s3keys.RouteKey("bucket-a", 1, "m")
	engine.UpdateRoute(nil, boundary, 1)
	engine.UpdateRoute(boundary, nil, 2)
	st := kv.NewShardStore(engine, map[uint64]*kv.ShardGroup{
		1: {Store: store.NewMVCCStore()},
		2: {Store: store.NewMVCCStore()},
	})
	blobs := store.NewMVCCStore()
	cluster := newFakeS3BlobCluster()
	newServer := func() *S3Server {
		return NewS3Server(nil, "", st, newLocalAdapterCoordinator(st), nil,
			WithS3BlobOffloadEnabled(false),
			WithS3BlobCluster(cluster),
			WithS3BlobLocalStoreResolver(&mutableS3BlobLocalStore{store: blobs}),
			withS3BlobOffloadGCReadyForTest(),
		)
	}
	s := newServer()
	rec := httptest.NewRecorder()
	s.handle(rec, newS3TestRequest(http.MethodPut, "/bucket-a", nil))
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	for _, object := range []string{"alpha", "zeta"} {
		rec = httptest.NewRecorder()
		s.handle(rec, newS3TestRequest(http.MethodPut, "/bucket-a/"+object, bytes.NewBufferString(object)))
		require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	}
	s.blobOffloadEnabled = true
	for _, object := range []string{"alpha", "zeta"} {
		intent, manifest, generation, sourceTS, pin, err := s.beginS3LegacyMigration(ctx, "bucket-a", object)
		require.NoError(t, err)
		require.NotNil(t, intent)
		part := manifest.Parts[0]
		require.NoError(t, s.stageS3LegacyChunk(ctx, "bucket-a", generation, object, manifest.UploadID,
			part, 0, part.ChunkSizes[0], sourceTS, *intent))
		if object == "zeta" {
			require.NoError(t, s.flipS3LegacyManifest(ctx, "bucket-a", generation, object, *intent))
		}
		pin.Release()
	}

	restarted := newServer()
	require.NoError(t, restarted.recoverPendingS3Migrations(ctx))
	for _, object := range []string{"alpha", "zeta"} {
		_, err := st.GetAt(ctx, s3keys.MigrationIntentKey("bucket-a", 1, object), ^uint64(0))
		require.ErrorIs(t, err, store.ErrKeyNotFound)
		rec = httptest.NewRecorder()
		restarted.handle(rec, newS3TestRequest(http.MethodGet, "/bucket-a/"+object, nil))
		require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
		require.Equal(t, object, rec.Body.String())
	}
}

func TestS3LegacyMigrationRecoveryPreservesActiveAttempt(t *testing.T) {
	st := store.NewMVCCStore()
	cluster := newFakeS3BlobCluster()
	cluster.pushStarted = make(chan struct{}, 2)
	release := make(chan struct{})
	closeRelease := sync.OnceFunc(func() { close(release) })
	t.Cleanup(closeRelease)
	cluster.pushRelease = release
	s := NewS3Server(nil, "", st, newLocalAdapterCoordinator(st), nil,
		WithS3BlobOffloadEnabled(false),
		WithS3BlobCluster(cluster),
		WithS3BlobLocalStoreResolver(&mutableS3BlobLocalStore{store: st}),
		withS3BlobOffloadGCReadyForTest(),
	)
	putS3MigrationTestObject(t, s, []byte("active migration"))
	s.blobOffloadEnabled = true
	done := make(chan error, 1)
	go func() {
		_, err := s.migrateS3LegacyObject(t.Context(), "bucket-a", "object")
		done <- err
	}()
	select {
	case <-cluster.pushStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("migration did not reach chunkblob replication")
	}
	markerKey := s3keys.MigrationIntentKey("bucket-a", 1, "object")
	require.NoError(t, s.recoverPendingS3Migrations(t.Context()))
	follower := NewS3Server(nil, "", st, s3MigrationFollowerCoordinator{newLocalAdapterCoordinator(st)}, nil,
		WithS3BlobCluster(cluster),
		WithS3BlobLocalStoreResolver(&mutableS3BlobLocalStore{store: st}),
	)
	require.NoError(t, follower.recoverPendingS3Migrations(t.Context()))
	raw, err := st.GetAt(t.Context(), markerKey, ^uint64(0))
	require.NoError(t, err)
	intent, err := decodeS3MigrationIntent(raw)
	require.NoError(t, err)
	require.Equal(t, s3MigrationStaging, intent.Phase)
	closeRelease()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("migration did not finish after replication resumed")
	}
	_, err = st.GetAt(t.Context(), markerKey, ^uint64(0))
	require.ErrorIs(t, err, store.ErrKeyNotFound)
}

func TestS3LegacyMigrationConcurrentOverwriteFencesFlip(t *testing.T) {
	st := store.NewMVCCStore()
	s := newS3MigrationTestServer(t, st)
	putS3MigrationTestObject(t, s, []byte("original"))
	s.blobOffloadEnabled = true
	intent, manifest, generation, sourceTS, pin, err := s.beginS3LegacyMigration(t.Context(), "bucket-a", "object")
	require.NoError(t, err)
	part := manifest.Parts[0]
	require.NoError(t, s.stageS3LegacyChunk(t.Context(), "bucket-a", generation, "object", manifest.UploadID, part, 0, part.ChunkSizes[0], sourceTS, *intent))
	s.blobOffloadEnabled = false
	putS3MigrationTestObject(t, s, []byte("replacement"))
	require.Error(t, s.flipS3LegacyManifest(t.Context(), "bucket-a", generation, "object", *intent))
	pin.Release()
	require.NoError(t, s.recoverPendingS3Migrations(t.Context()))
	require.Equal(t, []byte("replacement"), getS3MigrationTestObject(t, s))
}

func TestS3LegacyMigrationConcurrentDeleteFencesFlip(t *testing.T) {
	st := store.NewMVCCStore()
	s := newS3MigrationTestServer(t, st)
	putS3MigrationTestObject(t, s, []byte("deleted during migration"))
	s.blobOffloadEnabled = true
	intent, manifest, generation, sourceTS, pin, err := s.beginS3LegacyMigration(t.Context(), "bucket-a", "object")
	require.NoError(t, err)
	part := manifest.Parts[0]
	require.NoError(t, s.stageS3LegacyChunk(t.Context(), "bucket-a", generation, "object", manifest.UploadID, part, 0, part.ChunkSizes[0], sourceTS, *intent))
	rec := httptest.NewRecorder()
	s.handle(rec, newS3TestRequest(http.MethodDelete, "/bucket-a/object", nil))
	require.Equal(t, http.StatusNoContent, rec.Code)
	require.Error(t, s.flipS3LegacyManifest(t.Context(), "bucket-a", generation, "object", *intent))
	pin.Release()
	require.NoError(t, s.recoverPendingS3Migrations(t.Context()))
	_, err = st.GetAt(t.Context(), s3keys.MigrationIntentKey("bucket-a", generation, "object"), ^uint64(0))
	require.ErrorIs(t, err, store.ErrKeyNotFound)
	_, exists, err := s.loadObjectManifestAt(t.Context(), s3keys.ObjectManifestKey("bucket-a", generation, "object"), s.readTS())
	require.NoError(t, err)
	require.False(t, exists)
}

func TestS3LegacyMigrationOffloadDowngradeFencesFlip(t *testing.T) {
	st := store.NewMVCCStore()
	s := newS3MigrationTestServer(t, st)
	payload := []byte("retain legacy data after downgrade")
	putS3MigrationTestObject(t, s, payload)
	s.blobOffloadEnabled = true
	intent, manifest, generation, sourceTS, pin, err := s.beginS3LegacyMigration(t.Context(), "bucket-a", "object")
	require.NoError(t, err)
	part := manifest.Parts[0]
	require.NoError(t, s.stageS3LegacyChunk(t.Context(), "bucket-a", generation, "object", manifest.UploadID, part, 0, part.ChunkSizes[0], sourceTS, *intent))
	pin.Release()
	s.blobOffloadEnabled = false
	require.ErrorContains(t, s.flipS3LegacyManifest(t.Context(), "bucket-a", generation, "object", *intent), "requires the ready M3 offload path")
	require.NoError(t, s.recoverPendingS3Migrations(t.Context()))
	current, _ := migrationTestManifest(t, s)
	require.False(t, current.Parts[0].Offloaded)
	require.Equal(t, payload, getS3MigrationTestObject(t, s))
}

func TestS3LegacyMigrationLeadershipLossFencesFlip(t *testing.T) {
	st := store.NewMVCCStore()
	s := newS3MigrationTestServer(t, st)
	payload := []byte("retain legacy data after leader loss")
	putS3MigrationTestObject(t, s, payload)
	s.blobOffloadEnabled = true
	intent, manifest, generation, sourceTS, pin, err := s.beginS3LegacyMigration(t.Context(), "bucket-a", "object")
	require.NoError(t, err)
	part := manifest.Parts[0]
	require.NoError(t, s.stageS3LegacyChunk(t.Context(), "bucket-a", generation, "object", manifest.UploadID, part, 0, part.ChunkSizes[0], sourceTS, *intent))
	pin.Release()
	leader := s.coordinator
	s.coordinator = s3MigrationFollowerCoordinator{Coordinator: leader}
	require.ErrorContains(t, s.flipS3LegacyManifest(t.Context(), "bucket-a", generation, "object", *intent), "requires a verified leader")
	s.coordinator = leader
	require.NoError(t, s.recoverPendingS3Migrations(t.Context()))
	current, _ := migrationTestManifest(t, s)
	require.False(t, current.Parts[0].Offloaded)
	require.Equal(t, payload, getS3MigrationTestObject(t, s))
}

func TestS3LegacyMigrationStartupRecoveryPrecedesGCReadiness(t *testing.T) {
	st := store.NewMVCCStore()
	s := newS3MigrationTestServer(t, st)
	putS3MigrationTestObject(t, s, []byte("startup recovery"))
	s.blobOffloadEnabled = true
	intent, manifest, generation, sourceTS, pin, err := s.beginS3LegacyMigration(t.Context(), "bucket-a", "object")
	require.NoError(t, err)
	part := manifest.Parts[0]
	require.NoError(t, s.stageS3LegacyChunk(t.Context(), "bucket-a", generation, "object", manifest.UploadID, part, 0, part.ChunkSizes[0], sourceTS, *intent))
	pin.Release()
	restarted := NewS3Server(nil, "", st, newLocalAdapterCoordinator(st), nil,
		WithS3BlobOffloadEnabled(true),
		WithS3BlobCluster(newFakeS3BlobCluster()),
		WithS3BlobLocalStoreResolver(&mutableS3BlobLocalStore{store: st}),
	)
	require.False(t, restarted.blobOffloadGCReady.Load())
	require.NoError(t, restarted.StartBlobGC(t.Context()))
	require.Eventually(t, restarted.blobOffloadGCReady.Load, time.Second, time.Millisecond)
	_, err = st.GetAt(t.Context(), s3keys.MigrationIntentKey("bucket-a", generation, "object"), ^uint64(0))
	require.ErrorIs(t, err, store.ErrKeyNotFound)
	restarted.Stop()
}

func TestS3LegacyMigrationLostFlipReplyRecoversCommittedSide(t *testing.T) {
	st := store.NewMVCCStore()
	s := newS3MigrationTestServer(t, st)
	putS3MigrationTestObject(t, s, []byte("lost flip reply"))
	s.blobOffloadEnabled = true
	intent, manifest, generation, sourceTS, pin, err := s.beginS3LegacyMigration(t.Context(), "bucket-a", "object")
	require.NoError(t, err)
	part := manifest.Parts[0]
	require.NoError(t, s.stageS3LegacyChunk(t.Context(), "bucket-a", generation, "object", manifest.UploadID, part, 0, part.ChunkSizes[0], sourceTS, *intent))
	legacyKey := s3keys.VersionedBlobKey("bucket-a", generation, "object", manifest.UploadID, part.PartNo, 0, part.PartVersion)
	fault := &s3MigrationLostReplyCoordinator{Coordinator: s.coordinator, failKey: s3keys.ObjectManifestKey("bucket-a", generation, "object")}
	s.coordinator = fault
	require.ErrorIs(t, s.flipS3LegacyManifest(t.Context(), "bucket-a", generation, "object", *intent), kv.ErrTransactionOutcomeUnknown)
	require.True(t, fault.fired)
	pin.Release()
	restarted := newS3MigrationTestServer(t, st)
	require.NoError(t, restarted.recoverPendingS3Migrations(t.Context()))
	_, err = st.GetAt(t.Context(), legacyKey, ^uint64(0))
	require.ErrorIs(t, err, store.ErrKeyNotFound)
	after, _ := migrationTestManifest(t, restarted)
	require.True(t, after.Parts[0].Offloaded)
	require.Equal(t, []byte("lost flip reply"), getS3MigrationTestObject(t, restarted))
}

func TestS3LegacyMigrationLostStageReplyRollsBackRC(t *testing.T) {
	st := store.NewMVCCStore()
	s := newS3MigrationTestServer(t, st)
	payload := []byte("lost stage reply")
	putS3MigrationTestObject(t, s, payload)
	s.blobOffloadEnabled = true
	intent, manifest, generation, sourceTS, pin, err := s.beginS3LegacyMigration(t.Context(), "bucket-a", "object")
	require.NoError(t, err)
	part := manifest.Parts[0]
	refKey := s3keys.VersionedChunkRefKey("bucket-a", generation, "object", manifest.UploadID, part.PartNo, 0, intent.AttemptVersion)
	fault := &s3MigrationLostReplyCoordinator{Coordinator: s.coordinator, failKey: refKey}
	s.coordinator = fault
	require.ErrorIs(t, s.stageS3LegacyChunk(t.Context(), "bucket-a", generation, "object", manifest.UploadID, part, 0, part.ChunkSizes[0], sourceTS, *intent), kv.ErrTransactionOutcomeUnknown)
	require.True(t, fault.fired)
	pin.Release()
	restarted := newS3MigrationTestServer(t, st)
	require.NoError(t, restarted.recoverPendingS3Migrations(t.Context()))
	_, err = st.GetAt(t.Context(), refKey, ^uint64(0))
	require.ErrorIs(t, err, store.ErrKeyNotFound)
	raw, err := st.GetAt(t.Context(), s3keys.ChunkRefRCKey(sha256.Sum256(payload)), ^uint64(0))
	require.NoError(t, err)
	rc, ok := s3keys.DecodeChunkRefRC(raw)
	require.True(t, ok)
	require.Zero(t, rc.Count)
	require.Equal(t, payload, getS3MigrationTestObject(t, restarted))
}

func TestS3LegacyMigrationLostCleanupReplyRetainsObligation(t *testing.T) {
	st := store.NewMVCCStore()
	s := newS3MigrationTestServer(t, st)
	putS3MigrationTestObject(t, s, []byte("lost cleanup reply"))
	s.blobOffloadEnabled = true
	intent, manifest, generation, sourceTS, pin, err := s.beginS3LegacyMigration(t.Context(), "bucket-a", "object")
	require.NoError(t, err)
	part := manifest.Parts[0]
	require.NoError(t, s.stageS3LegacyChunk(t.Context(), "bucket-a", generation, "object", manifest.UploadID, part, 0, part.ChunkSizes[0], sourceTS, *intent))
	require.NoError(t, s.flipS3LegacyManifest(t.Context(), "bucket-a", generation, "object", *intent))
	pin.Release()
	legacyKey := s3keys.VersionedBlobKey("bucket-a", generation, "object", manifest.UploadID, part.PartNo, 0, part.PartVersion)
	markerKey := s3keys.MigrationIntentKey("bucket-a", generation, "object")
	fault := &s3MigrationLostReplyCoordinator{Coordinator: s.coordinator, failKey: legacyKey, failDelete: true}
	s.coordinator = fault
	require.ErrorIs(t, s.recoverS3MigrationIntent(t.Context(), markerKey), kv.ErrTransactionOutcomeUnknown)
	require.True(t, fault.fired)
	_, err = st.GetAt(t.Context(), markerKey, ^uint64(0))
	require.NoError(t, err)
	restarted := newS3MigrationTestServer(t, st)
	require.NoError(t, restarted.recoverPendingS3Migrations(t.Context()))
	_, err = st.GetAt(t.Context(), legacyKey, ^uint64(0))
	require.ErrorIs(t, err, store.ErrKeyNotFound)
	_, err = st.GetAt(t.Context(), markerKey, ^uint64(0))
	require.ErrorIs(t, err, store.ErrKeyNotFound)
	require.Equal(t, []byte("lost cleanup reply"), getS3MigrationTestObject(t, restarted))
}

func TestS3LegacyMigrationLostAbortCleanupReplyRetainsRCObligation(t *testing.T) {
	st := store.NewMVCCStore()
	s := newS3MigrationTestServer(t, st)
	payload := []byte("lost abort cleanup reply")
	putS3MigrationTestObject(t, s, payload)
	s.blobOffloadEnabled = true
	intent, manifest, generation, sourceTS, pin, err := s.beginS3LegacyMigration(t.Context(), "bucket-a", "object")
	require.NoError(t, err)
	part := manifest.Parts[0]
	require.NoError(t, s.stageS3LegacyChunk(t.Context(), "bucket-a", generation, "object", manifest.UploadID, part, 0, part.ChunkSizes[0], sourceTS, *intent))
	pin.Release()
	refKey := s3keys.VersionedChunkRefKey("bucket-a", generation, "object", manifest.UploadID, part.PartNo, 0, intent.AttemptVersion)
	markerKey := s3keys.MigrationIntentKey("bucket-a", generation, "object")
	fault := &s3MigrationLostReplyCoordinator{Coordinator: s.coordinator, failKey: refKey, failDelete: true}
	s.coordinator = fault
	require.ErrorIs(t, s.recoverS3MigrationIntent(t.Context(), markerKey), kv.ErrTransactionOutcomeUnknown)
	require.True(t, fault.fired)
	_, err = st.GetAt(t.Context(), markerKey, ^uint64(0))
	require.NoError(t, err)
	restarted := newS3MigrationTestServer(t, st)
	require.NoError(t, restarted.recoverPendingS3Migrations(t.Context()))
	_, err = st.GetAt(t.Context(), refKey, ^uint64(0))
	require.ErrorIs(t, err, store.ErrKeyNotFound)
	raw, err := st.GetAt(t.Context(), s3keys.ChunkRefRCKey(sha256.Sum256(payload)), ^uint64(0))
	require.NoError(t, err)
	rc, ok := s3keys.DecodeChunkRefRC(raw)
	require.True(t, ok)
	require.Zero(t, rc.Count)
	_, err = st.GetAt(t.Context(), markerKey, ^uint64(0))
	require.ErrorIs(t, err, store.ErrKeyNotFound)
}

func TestS3LegacyMigrationMultipartCleanupBatches(t *testing.T) {
	st := store.NewMVCCStore()
	s := newS3MigrationTestServer(t, st)
	rec := httptest.NewRecorder()
	s.handle(rec, newS3TestRequest(http.MethodPut, "/bucket-a", nil))
	require.Equal(t, http.StatusOK, rec.Code)
	const uploadID = "multipart-fixture"
	parts := []s3ObjectPart{
		{PartNo: 1, ChunkCount: s3MetaBatchOps, ChunkSizes: make([]uint64, s3MetaBatchOps)},
		{PartNo: 2, ChunkCount: 3, ChunkSizes: make([]uint64, 3)},
	}
	var original []byte
	for partIndex := range parts {
		for chunkNo := range parts[partIndex].ChunkSizes {
			chunk := []byte{byte(partIndex + chunkNo + 1)} //nolint:gosec // Small test fixture.
			original = append(original, chunk...)
			parts[partIndex].ChunkSizes[chunkNo] = 1
			key := s3keys.VersionedBlobKey("bucket-a", 1, "object", uploadID, parts[partIndex].PartNo, uint64(chunkNo), 0) //nolint:gosec // Bounded fixture index.
			_, err := s.coordinator.Dispatch(t.Context(), &kv.OperationGroup[kv.OP]{Elems: []*kv.Elem[kv.OP]{{Op: kv.Put, Key: key, Value: chunk}}})
			require.NoError(t, err)
		}
	}
	manifest := &s3ObjectManifest{UploadID: uploadID, SizeBytes: int64(len(original)), Parts: parts}
	value, err := encodeS3ObjectManifest(manifest)
	require.NoError(t, err)
	_, err = s.coordinator.Dispatch(t.Context(), &kv.OperationGroup[kv.OP]{Elems: []*kv.Elem[kv.OP]{{Op: kv.Put, Key: s3keys.ObjectManifestKey("bucket-a", 1, "object"), Value: value}}})
	require.NoError(t, err)
	s.blobOffloadEnabled = true
	changed, err := s.migrateS3LegacyObject(t.Context(), "bucket-a", "object")
	require.NoError(t, err)
	require.True(t, changed)
	require.Equal(t, original, getS3MigrationTestObject(t, s))
	for _, part := range parts {
		for chunkNo := range part.ChunkSizes {
			key := s3keys.VersionedBlobKey("bucket-a", 1, "object", uploadID, part.PartNo, uint64(chunkNo), 0) //nolint:gosec // Bounded fixture index.
			_, err := st.GetAt(t.Context(), key, ^uint64(0))
			require.ErrorIs(t, err, store.ErrKeyNotFound)
		}
	}
}

func TestS3MigrationIntentRejectsUnboundedCleanupCount(t *testing.T) {
	intent := s3MigrationIntent{Phase: s3MigrationAborting, AttemptVersion: 1, UploadID: "upload", LegacyParts: []s3MigrationPart{{PartNo: 1, ChunkCount: s3MigrationMaxChunksPerPart + 1}}}
	raw, err := encodeS3MigrationIntent(intent)
	require.NoError(t, err)
	_, err = decodeS3MigrationIntent(raw)
	require.ErrorContains(t, err, "invalid part")
}
