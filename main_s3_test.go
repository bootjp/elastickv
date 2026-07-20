package main

import (
	"context"
	"crypto/sha256"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/bootjp/elastickv/adapter"
	"github.com/bootjp/elastickv/distribution"
	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

func TestStartS3ServerRejectsVirtualHostedStyleConfig(t *testing.T) {
	eg, ctx := errgroup.WithContext(context.Background())
	srv, err := startS3Server(ctx, &net.ListenConfig{}, eg, "localhost:9000", nil, nil, nil, "us-east-1", "", false, nil, nil, nil)
	require.ErrorContains(t, err, "virtual-hosted style S3 requests are not implemented")
	require.Nil(t, srv)
}

func TestStartS3ServerAllowsEmptyAddress(t *testing.T) {
	eg, ctx := errgroup.WithContext(context.Background())
	srv, err := startS3Server(ctx, &net.ListenConfig{}, eg, "", nil, nil, nil, "us-east-1", "", false, nil, nil, nil)
	require.NoError(t, err)
	require.Nil(t, srv)
}

func TestS3BlobNodeEnabledForPeerOnlyNode(t *testing.T) {
	t.Parallel()

	require.True(t, s3BlobNodeEnabled("", "/run/secrets/s3-peer-token"))
	require.True(t, s3BlobNodeEnabled("127.0.0.1:9000", ""))
	require.False(t, s3BlobNodeEnabled("", ""))
}

func TestRunS3BlobBackfillOnlyStartsListenlessServer(t *testing.T) {
	t.Parallel()

	engine := distribution.NewEngine()
	engine.UpdateRoute(nil, nil, 1)
	shardStore := kv.NewShardStore(engine, map[uint64]*kv.ShardGroup{
		1: {Store: store.NewMVCCStore()},
	})
	backfiller := adapter.NewS3BlobBackfiller(adapter.S3BlobBackfillConfig{
		Workers: 1, QueueSize: 1, RatePerPeer: 1, BurstPerPeer: 1,
		ScanInterval: time.Hour, ScanPageSize: 1, MaxAttempts: 1,
		RetryInitial: time.Millisecond, RetryMax: time.Millisecond,
	})
	server, err := newS3Server(
		"", shardStore, &stubStartupCoordinator{clock: kv.NewHLC()}, nil, "", "", false, nil,
		nil, nil, peerOnlyS3BlobCluster{}, nil, backfiller,
	)
	require.NoError(t, err)
	require.NotNil(t, server)

	ctx, cancel := context.WithCancel(context.Background())
	eg, runCtx := errgroup.WithContext(ctx)
	runS3BlobBackfillOnly(runCtx, eg, server)
	cancel()
	require.NoError(t, eg.Wait())
}

type peerOnlyS3BlobCluster struct{}

func (peerOnlyS3BlobCluster) AllPeersSupportS3BlobOffload(context.Context) bool { return true }
func (peerOnlyS3BlobCluster) SelfNodeID() string                                { return "n1" }
func (peerOnlyS3BlobCluster) ReplicasForChunk(context.Context, []byte) ([]adapter.S3BlobReplica, error) {
	return nil, nil
}
func (peerOnlyS3BlobCluster) PushChunkBlob(context.Context, adapter.S3BlobReplica, [sha256.Size]byte, []byte, uint64) error {
	return nil
}
func (peerOnlyS3BlobCluster) FetchChunkBlob(context.Context, adapter.S3BlobReplica, [sha256.Size]byte) ([]byte, error) {
	return nil, nil
}
func (peerOnlyS3BlobCluster) Close() error { return nil }

func TestLoadS3StaticCredentials(t *testing.T) {
	path := filepath.Join(t.TempDir(), "s3creds.json")
	err := os.WriteFile(path, []byte(`{"credentials":[{"access_key_id":"akid","secret_access_key":"secret"}]}`), 0o600)
	require.NoError(t, err)

	creds, err := loadS3StaticCredentials(path)
	require.NoError(t, err)
	require.Equal(t, map[string]string{"akid": "secret"}, creds)
}

func TestLoadS3StaticCredentialsRejectsEmptyValues(t *testing.T) {
	path := filepath.Join(t.TempDir(), "s3creds.json")
	err := os.WriteFile(path, []byte(`{"credentials":[{"access_key_id":" ","secret_access_key":"secret"}]}`), 0o600)
	require.NoError(t, err)

	_, err = loadS3StaticCredentials(path)
	require.ErrorContains(t, err, "s3 credentials file contains an empty access key or secret key")
}

func TestLoadS3StaticCredentialsRejectsDuplicateAccessKeyIDs(t *testing.T) {
	path := filepath.Join(t.TempDir(), "s3creds.json")
	err := os.WriteFile(path, []byte(`{"credentials":[{"access_key_id":"akid","secret_access_key":"secret-1"},{"access_key_id":"akid","secret_access_key":"secret-2"}]}`), 0o600)
	require.NoError(t, err)

	_, err = loadS3StaticCredentials(path)
	require.ErrorContains(t, err, `s3 credentials file contains duplicate access key ID: "akid"`)
}
