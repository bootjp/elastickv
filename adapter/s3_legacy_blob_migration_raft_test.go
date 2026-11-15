package adapter

import (
	"context"
	"crypto/sha256"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/bootjp/elastickv/internal/s3keys"
	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

type s3MigrationRaftNode struct {
	dataDir      string
	storeDir     string
	address      string
	store        store.MVCCStore
	engine       raftengine.Engine
	server       *S3Server
	grpcServer   *grpc.Server
	listener     net.Listener
	closeFactory func() error
}

func openS3MigrationRaftNode(t *testing.T, node *s3MigrationRaftNode, id int, peers []raftengine.Server, blobs *fakeS3BlobCluster) {
	t.Helper()
	st, err := store.NewPebbleStore(node.storeDir)
	require.NoError(t, err)
	node.store = st
	hlc := kv.NewHLC()
	result, err := newTestFactory().Create(raftengine.FactoryConfig{
		LocalID: strconv.Itoa(id), LocalAddress: node.address,
		DataDir: node.dataDir, Peers: peers, Bootstrap: true,
		StateMachine: kv.NewKvFSMWithHLC(st, hlc),
	})
	require.NoError(t, err)
	node.engine, node.closeFactory = result.Engine, result.Close
	node.grpcServer = grpc.NewServer(result.ServerOptions...)
	result.RegisterTransport(node.grpcServer)
	node.listener, err = net.Listen("tcp", node.address)
	require.NoError(t, err)
	go func() { _ = node.grpcServer.Serve(node.listener) }()
	coord := kv.NewCoordinatorWithEngine(kv.NewTransactionWithProposer(node.engine), node.engine, kv.WithHLC(hlc))
	node.server = NewS3Server(nil, "", st, coord, nil,
		WithS3BlobOffloadEnabled(false),
		WithS3BlobCluster(blobs),
		WithS3BlobLocalStoreResolver(&mutableS3BlobLocalStore{store: st}),
		withS3BlobOffloadGCReadyForTest(),
	)
}

func closeS3MigrationRaftNode(t *testing.T, node *s3MigrationRaftNode) {
	t.Helper()
	if node.server != nil {
		node.server.Stop()
	}
	if node.grpcServer != nil {
		node.grpcServer.Stop()
	}
	if node.engine != nil {
		require.NoError(t, node.engine.Close())
	}
	if node.closeFactory != nil {
		require.NoError(t, node.closeFactory())
	}
	if node.store != nil {
		require.NoError(t, node.store.Close())
	}
	node.server, node.grpcServer, node.listener, node.engine, node.closeFactory, node.store = nil, nil, nil, nil, nil, nil
}

func requireS3MigrationRaftLeader(t *testing.T, fixtures []s3MigrationRaftNode, peers []raftengine.Server) {
	t.Helper()
	nodes := make([]Node, len(fixtures))
	for i := range fixtures {
		nodes[i].engine = fixtures[i].engine
	}
	ensureNodeZeroIsLeader(t, nodes, peers, 10*time.Second, 100*time.Millisecond)
	waitForStableLeader(t, nodes, peers, 10*time.Second)
}

func TestS3LegacyMigrationRaftPebbleRestartAfterFlipAndCompact(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	const nodes = 3
	cluster := newFakeS3BlobCluster()
	cluster.self = "0"
	for i := range cluster.replicas {
		cluster.replicas[i].NodeID = strconv.Itoa(i)
	}
	fixtures := make([]s3MigrationRaftNode, nodes)
	peers := make([]raftengine.Server, nodes)
	for i := range fixtures {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		fixtures[i].address = listener.Addr().String()
		require.NoError(t, listener.Close())
		fixtures[i].dataDir = filepath.Join(t.TempDir(), "raft")
		fixtures[i].storeDir = filepath.Join(t.TempDir(), "pebble")
		peers[i] = raftengine.Server{ID: strconv.Itoa(i), Address: fixtures[i].address, Suffrage: "voter"}
	}
	for i := range fixtures {
		openS3MigrationRaftNode(t, &fixtures[i], i, peers, cluster)
	}
	t.Cleanup(func() {
		for i := range fixtures {
			if fixtures[i].store != nil {
				closeS3MigrationRaftNode(t, &fixtures[i])
			}
		}
	})
	requireS3MigrationRaftLeader(t, fixtures, peers)
	leader := fixtures[0].server
	putS3MigrationTestObject(t, leader, []byte("durable migration payload"))
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
	legacyKey := s3keys.VersionedBlobKey("bucket-a", generation, "object", manifest.UploadID, part.PartNo, 0, part.PartVersion)
	_, err = fixtures[0].store.GetAt(ctx, markerKey, ^uint64(0))
	require.NoError(t, err, "flip must leave a durable cleanup obligation")
	_, err = fixtures[0].store.GetAt(ctx, legacyKey, ^uint64(0))
	require.NoError(t, err, "old blob must still exist at the crash boundary")

	closeS3MigrationRaftNode(t, &fixtures[0])
	openS3MigrationRaftNode(t, &fixtures[0], 0, peers, cluster)
	requireS3MigrationRaftLeader(t, fixtures, peers)
	restarted := fixtures[0].server
	require.NoError(t, restarted.StartBlobGC(ctx))
	require.Eventually(t, restarted.blobOffloadGCReady.Load, 5*time.Second, 10*time.Millisecond)
	for i := range fixtures {
		require.Eventually(t, func() bool {
			_, err := fixtures[i].store.GetAt(ctx, markerKey, ^uint64(0))
			return errors.Is(err, store.ErrKeyNotFound)
		}, 5*time.Second, 10*time.Millisecond)
		require.NoError(t, fixtures[i].store.Compact(ctx, fixtures[i].store.LastCommitTS()))
		require.ErrorIs(t, func() error { _, err := fixtures[i].store.GetAt(ctx, legacyKey, ^uint64(0)); return err }(), store.ErrKeyNotFound)
	}
	rec := httptest.NewRecorder()
	restarted.handle(rec, newS3TestRequest(http.MethodGet, "/bucket-a/object", nil))
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	require.Equal(t, "durable migration payload", rec.Body.String())
	for i := 1; i < len(fixtures); i++ {
		peer := fixtures[i].server
		peerManifest, ok, err := peer.loadObjectManifestAt(ctx, s3keys.ObjectManifestKey("bucket-a", generation, "object"), ^uint64(0))
		require.NoError(t, err)
		require.True(t, ok)
		chunk, err := peer.readS3ObjectChunk(ctx, "bucket-a", generation, "object", peerManifest.UploadID,
			peerManifest.Parts[0], 0, peerManifest.Parts[0].ChunkSizes[0], ^uint64(0))
		require.NoError(t, err)
		require.Equal(t, []byte("durable migration payload"), chunk)
	}
	digest := sha256.Sum256([]byte("durable migration payload"))
	raw, err := fixtures[0].store.GetAt(ctx, s3keys.ChunkRefRCKey(digest), ^uint64(0))
	require.NoError(t, err)
	rc, ok := s3keys.DecodeChunkRefRC(raw)
	require.True(t, ok)
	require.Equal(t, uint64(1), rc.Count)
}
