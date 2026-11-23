package adapter

import (
	"context"
	"crypto/sha256"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/bootjp/elastickv/internal/s3keys"
	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
)

const s3MigrationKillDirEnv = "ELASTICKV_TEST_S3_MIGRATION_KILL_DIR"
const s3MigrationKillAddressesEnv = "ELASTICKV_TEST_S3_MIGRATION_KILL_ADDRESSES"

var s3MigrationKillPayload = []byte("m4 process kill real peer transfer")

func openS3MigrationRealRaftCluster(t *testing.T, dir string, addresses []string) ([]s3MigrationRaftNode, []raftengine.Server) {
	t.Helper()
	require.Len(t, addresses, 3)
	fixtures := make([]s3MigrationRaftNode, len(addresses))
	peers := make([]raftengine.Server, len(addresses))
	members := make([]kv.RaftMember, len(addresses))
	for i, address := range addresses {
		id := strconv.Itoa(i)
		fixtures[i] = s3MigrationRaftNode{
			address:  address,
			dataDir:  filepath.Join(dir, id, "raft"),
			storeDir: filepath.Join(dir, id, "pebble"),
		}
		peers[i] = raftengine.Server{ID: id, Address: address, Suffrage: "voter"}
		members[i] = kv.RaftMember{NodeID: id, Address: address, Suffrage: "voter"}
	}
	t.Cleanup(func() {
		for i := range fixtures {
			if fixtures[i].store != nil {
				closeS3MigrationRaftNode(t, &fixtures[i])
			}
		}
	})
	for i := range fixtures {
		cluster := NewGRPCS3BlobCluster(strconv.Itoa(i), staticS3BlobMembership{members: members}, "read-only-admin", "peer-secret")
		openS3MigrationRaftNode(t, &fixtures[i], i, peers, cluster)
	}
	requireS3MigrationRaftLeader(t, fixtures, peers)
	return fixtures, peers
}

func s3MigrationKillAddresses(t *testing.T) []string {
	t.Helper()
	addresses := make([]string, 3)
	for i := range addresses {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		addresses[i] = listener.Addr().String()
		require.NoError(t, listener.Close())
	}
	return addresses
}

func TestS3LegacyMigrationRealPeersSurviveProcessKillAfterFlip(t *testing.T) {
	if dir := os.Getenv(s3MigrationKillDirEnv); dir != "" {
		addresses := strings.Split(os.Getenv(s3MigrationKillAddressesEnv), ",")
		runS3MigrationKillChild(t, dir, addresses)
		return
	}

	dir := t.TempDir()
	addresses := s3MigrationKillAddresses(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestS3LegacyMigrationRealPeersSurviveProcessKillAfterFlip$", "-test.count=1") //nolint:gosec // Re-executes the test binary with test-controlled arguments.
	cmd.Env = append(os.Environ(),
		s3MigrationKillDirEnv+"="+dir,
		s3MigrationKillAddressesEnv+"="+strings.Join(addresses, ","),
	)
	output, err := cmd.CombinedOutput()
	require.NoError(t, ctx.Err(), string(output))
	var exit *exec.ExitError
	require.ErrorAs(t, err, &exit, string(output))
	status, ok := exit.Sys().(syscall.WaitStatus)
	require.True(t, ok, string(output))
	require.Equal(t, syscall.SIGKILL, status.Signal(), string(output))
	require.FileExists(t, filepath.Join(dir, "flipped"))

	fixtures, _ := openS3MigrationRealRaftCluster(t, dir, addresses)
	assertS3MigrationKilledCluster(t, ctx, fixtures, addresses)
}

func runS3MigrationKillChild(t *testing.T, dir string, addresses []string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	fixtures, _ := openS3MigrationRealRaftCluster(t, dir, addresses)
	leader := fixtures[0].server
	putS3MigrationTestObject(t, leader, s3MigrationKillPayload)
	leader.blobOffloadEnabled = true
	intent, manifest, generation, sourceTS, pin, err := leader.beginS3LegacyMigration(ctx, "bucket-a", "object")
	require.NoError(t, err)
	require.NotNil(t, intent)
	part := manifest.Parts[0]
	require.NoError(t, leader.stageS3LegacyChunk(ctx, "bucket-a", generation, "object", manifest.UploadID,
		part, 0, part.ChunkSizes[0], sourceTS, *intent))
	require.NoError(t, leader.flipS3LegacyManifest(ctx, "bucket-a", generation, "object", *intent))
	pin.Release()
	markerKey := s3keys.MigrationIntentKey("bucket-a", generation, "object")
	for i := range fixtures {
		require.Eventually(t, func() bool {
			_, err := fixtures[i].store.GetAt(ctx, markerKey, ^uint64(0))
			return err == nil
		}, 5*time.Second, 10*time.Millisecond)
	}
	require.NoError(t, os.WriteFile(filepath.Join(dir, "flipped"), []byte("flip committed; cleanup not called"), 0o600))
	require.NoError(t, syscall.Kill(os.Getpid(), syscall.SIGKILL))
	t.Fatal("SIGKILL returned without terminating the child")
}

func assertS3MigrationKilledCluster(t *testing.T, ctx context.Context, fixtures []s3MigrationRaftNode, addresses []string) {
	t.Helper()
	const bucket = "bucket-a"
	const object = "object"
	const generation = uint64(1)
	leader := fixtures[0].server
	manifest, exists, err := leader.loadObjectManifestAt(ctx, s3keys.ObjectManifestKey(bucket, generation, object), ^uint64(0))
	require.NoError(t, err)
	require.True(t, exists)
	require.True(t, manifest.Parts[0].Offloaded)
	part := manifest.Parts[0]
	markerKey := s3keys.MigrationIntentKey(bucket, generation, object)
	legacyKey := s3keys.VersionedBlobKey(bucket, generation, object, manifest.UploadID, part.PartNo, 0, part.PartVersion)
	digest := sha256.Sum256(s3MigrationKillPayload)
	for i := range fixtures {
		_, err := fixtures[i].store.GetAt(ctx, markerKey, ^uint64(0))
		require.NoError(t, err, "node %d lost the cleanup obligation", i)
		_, err = fixtures[i].store.GetAt(ctx, legacyKey, ^uint64(0))
		require.NoError(t, err, "node %d cleaned the legacy blob before recovery", i)
		blob, err := fixtures[i].store.GetAt(ctx, s3keys.ChunkBlobKey(digest), ^uint64(0))
		require.NoError(t, err, "node %d missed the real peer push", i)
		require.Equal(t, s3MigrationKillPayload, blob)
	}
	replica := S3BlobReplica{NodeID: "1", Address: addresses[1], Suffrage: "voter"}
	fetched, err := leader.blobCluster.FetchChunkBlob(ctx, replica, digest)
	require.NoError(t, err)
	require.Equal(t, s3MigrationKillPayload, fetched)

	require.NoError(t, leader.StartBlobGC(ctx))
	require.Eventually(t, leader.blobOffloadGCReady.Load, 5*time.Second, 10*time.Millisecond)
	for i := range fixtures {
		require.Eventually(t, func() bool {
			_, err := fixtures[i].store.GetAt(ctx, markerKey, ^uint64(0))
			return errors.Is(err, store.ErrKeyNotFound)
		}, 5*time.Second, 10*time.Millisecond)
		require.NoError(t, fixtures[i].store.Compact(ctx, fixtures[i].store.LastCommitTS()))
		_, err := fixtures[i].store.GetAt(ctx, legacyKey, ^uint64(0))
		require.ErrorIs(t, err, store.ErrKeyNotFound)
	}
	rec := httptest.NewRecorder()
	leader.handle(rec, newS3TestRequest(http.MethodGet, "/bucket-a/object", nil))
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	require.Equal(t, s3MigrationKillPayload, rec.Body.Bytes())
	raw, err := fixtures[0].store.GetAt(ctx, s3keys.ChunkRefRCKey(digest), ^uint64(0))
	require.NoError(t, err)
	rc, ok := s3keys.DecodeChunkRefRC(raw)
	require.True(t, ok)
	require.Equal(t, uint64(1), rc.Count)
	t.Logf("recovered %x after real SIGKILL with gRPC peer transfer", digest[:4])
}
