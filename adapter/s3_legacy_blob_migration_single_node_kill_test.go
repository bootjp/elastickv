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

const s3MigrationSingleKillDirEnv = "ELASTICKV_TEST_S3_MIGRATION_SINGLE_KILL_DIR"
const s3MigrationSingleKillAddressesEnv = "ELASTICKV_TEST_S3_MIGRATION_SINGLE_KILL_ADDRESSES"

func s3MigrationSplitFixture(t *testing.T, dir string, addresses []string) ([]s3MigrationRaftNode, []raftengine.Server, []kv.RaftMember) {
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
	return fixtures, peers, members
}

func openS3MigrationSplitNode(t *testing.T, fixtures []s3MigrationRaftNode, peers []raftengine.Server, members []kv.RaftMember, id int) {
	t.Helper()
	cluster := NewGRPCS3BlobCluster(strconv.Itoa(id), staticS3BlobMembership{members: members}, "read-only-admin", "peer-secret")
	openS3MigrationRaftNode(t, &fixtures[id], id, peers, cluster)
}

func TestS3LegacyMigrationSingleNodeSIGKILLWithLiveQuorum(t *testing.T) {
	if dir := os.Getenv(s3MigrationSingleKillDirEnv); dir != "" {
		runS3MigrationSingleKillChild(t, dir, strings.Split(os.Getenv(s3MigrationSingleKillAddressesEnv), ","))
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	dir := t.TempDir()
	addresses := s3MigrationKillAddresses(t)
	fixtures, peers, members := s3MigrationSplitFixture(t, dir, addresses)
	openS3MigrationSplitNode(t, fixtures, peers, members, 1)
	openS3MigrationSplitNode(t, fixtures, peers, members, 2)
	require.Eventually(t, func() bool { return s3MigrationLiveLeader(fixtures) != nil }, 10*time.Second, 25*time.Millisecond)

	logPath := filepath.Join(dir, "node-0.log")
	logFile, err := os.Create(logPath)
	require.NoError(t, err)
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestS3LegacyMigrationSingleNodeSIGKILLWithLiveQuorum$", "-test.count=1") //nolint:gosec // Re-executes this test binary.
	cmd.Env = append(os.Environ(),
		s3MigrationSingleKillDirEnv+"="+dir,
		s3MigrationSingleKillAddressesEnv+"="+strings.Join(addresses, ","),
	)
	cmd.Stdout, cmd.Stderr = logFile, logFile
	require.NoError(t, cmd.Start())
	t.Cleanup(func() {
		if cmd.ProcessState == nil {
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
		}
		_ = logFile.Close()
		if t.Failed() {
			output, _ := os.ReadFile(logPath)
			t.Logf("node 0 child output:\n%s", output)
		}
	})
	require.Eventually(t, func() bool {
		if _, err := os.Stat(filepath.Join(dir, "node-0-ready")); err != nil {
			return false
		}
		conn, err := net.DialTimeout("tcp", addresses[0], 100*time.Millisecond)
		if err != nil {
			return false
		}
		_ = conn.Close()
		return true
	}, 10*time.Second, 25*time.Millisecond)
	require.Eventually(t, func() bool {
		leader := s3MigrationLiveLeader(fixtures)
		if leader == nil {
			return false
		}
		transferCtx, transferCancel := context.WithTimeout(ctx, 3*time.Second)
		defer transferCancel()
		admin, ok := leader.(raftengine.Admin)
		return ok && admin.TransferLeadershipToServer(transferCtx, "0", addresses[0]) == nil
	}, 15*time.Second, 100*time.Millisecond)

	err = cmd.Wait()
	require.NoError(t, ctx.Err())
	var exit *exec.ExitError
	require.ErrorAs(t, err, &exit)
	status, ok := exit.Sys().(syscall.WaitStatus)
	require.True(t, ok)
	require.Equal(t, syscall.SIGKILL, status.Signal())
	require.FileExists(t, filepath.Join(dir, "node-0-flipped"))
	requireLiveS3MigrationQuorum(t, fixtures)

	openS3MigrationSplitNode(t, fixtures, peers, members, 0)
	requireS3MigrationRaftLeader(t, fixtures, peers)
	assertS3MigrationSingleKillRecovery(t, ctx, fixtures, addresses)
}

func s3MigrationLiveLeader(fixtures []s3MigrationRaftNode) raftengine.Engine {
	for i := 1; i < len(fixtures); i++ {
		if fixtures[i].engine != nil && fixtures[i].engine.State() == raftengine.StateLeader {
			return fixtures[i].engine
		}
	}
	return nil
}

func requireLiveS3MigrationQuorum(t *testing.T, fixtures []s3MigrationRaftNode) {
	t.Helper()
	require.Eventually(t, func() bool {
		leader := s3MigrationLiveLeader(fixtures)
		return leader != nil && proposeReadinessOnce(leader, 2*time.Second) == nil
	}, 15*time.Second, 100*time.Millisecond, "surviving peers must commit while node 0 is down")
}

func runS3MigrationSingleKillChild(t *testing.T, dir string, addresses []string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	fixtures, peers, members := s3MigrationSplitFixture(t, dir, addresses)
	openS3MigrationSplitNode(t, fixtures, peers, members, 0)
	require.NoError(t, os.WriteFile(filepath.Join(dir, "node-0-ready"), nil, 0o600))
	require.Eventually(t, func() bool { return fixtures[0].engine.State() == raftengine.StateLeader }, 20*time.Second, 25*time.Millisecond)
	require.NoError(t, proposeReadinessOnce(fixtures[0].engine, 2*time.Second))
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
	_, err = fixtures[0].store.GetAt(ctx, s3keys.MigrationIntentKey("bucket-a", generation, "object"), ^uint64(0))
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(dir, "node-0-flipped"), nil, 0o600))
	require.NoError(t, syscall.Kill(os.Getpid(), syscall.SIGKILL))
	t.Fatal("SIGKILL returned without terminating node 0")
}

func assertS3MigrationSingleKillRecovery(t *testing.T, ctx context.Context, fixtures []s3MigrationRaftNode, addresses []string) {
	t.Helper()
	const generation = uint64(1)
	const bucket = "bucket-a"
	const object = "object"
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
		require.Eventually(t, func() bool {
			_, err := fixtures[i].store.GetAt(ctx, markerKey, ^uint64(0))
			return err == nil
		}, 5*time.Second, 10*time.Millisecond)
		_, err := fixtures[i].store.GetAt(ctx, legacyKey, ^uint64(0))
		require.NoError(t, err, "node %d cleaned the legacy blob before recovery", i)
		blob, err := fixtures[i].store.GetAt(ctx, s3keys.ChunkBlobKey(digest), ^uint64(0))
		require.NoError(t, err)
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
	repairStore, err := store.NewPebbleStore(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, repairStore.Close()) })
	resolver, ok := leader.blobLocalStores.(*mutableS3BlobLocalStore)
	require.True(t, ok)
	resolver.set(repairStore)
	rec := httptest.NewRecorder()
	leader.handle(rec, newS3TestRequest(http.MethodGet, "/bucket-a/object", nil))
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	require.Equal(t, s3MigrationKillPayload, rec.Body.Bytes())
	repaired, err := repairStore.GetAt(ctx, s3keys.ChunkBlobKey(digest), ^uint64(0))
	require.NoError(t, err, "GET must repair a missing local blob through a peer fetch")
	require.Equal(t, s3MigrationKillPayload, repaired)
	raw, err := fixtures[0].store.GetAt(ctx, s3keys.ChunkRefRCKey(digest), ^uint64(0))
	require.NoError(t, err)
	rc, ok := s3keys.DecodeChunkRefRC(raw)
	require.True(t, ok)
	require.Equal(t, uint64(1), rc.Count)
}
