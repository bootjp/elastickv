package snapshotoffload

import (
	"bytes"
	"context"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	etcdraftengine "github.com/bootjp/elastickv/internal/raftengine/etcd"
	"github.com/stretchr/testify/require"
)

func TestPublishAndRestorePhysicalSnapshotRoundTripWithS3Store(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	payload := []byte("EKVTHLC1opaque-s3-physical-snapshot-payload")
	sourceDataDir := seedPhysicalSnapshot(t, root, payload, 50, 8, singlePeer())
	store := newTestS3Store(t, newFakeS3Client())

	manifest, err := PublishPersistedSnapshot(ctx, PublishOptions{
		Store:         store,
		DataDir:       sourceDataDir,
		Prefix:        "cluster-s3",
		GroupID:       3,
		SourceCluster: "cluster-s3",
		BinaryVersion: "test-version",
		CreatedAt:     time.Unix(500, 0).UTC(),
	})
	require.NoError(t, err)
	require.Equal(t, uint64(50), manifest.SnapshotIndex)
	require.Equal(t, int64(len(payload)), manifest.Payload.Bytes)

	restoreDataDir := root + "/restored-s3"
	result, err := RestorePhysicalSnapshot(ctx, RestoreOptions{
		Store:       store,
		ManifestKey: manifest.ManifestKey,
		DataDir:     restoreDataDir,
		Peers: []etcdraftengine.Peer{
			{NodeID: 2, ID: "n2", Address: "127.0.0.1:12002"},
		},
	})
	require.NoError(t, err)
	require.Equal(t, manifest.Payload.SHA256, result.PayloadSHA256)
}

func TestS3StorePutHeadGetPreservesIntegrityMetadata(t *testing.T) {
	ctx := context.Background()
	fake := newFakeS3Client()
	store := newTestS3Store(t, fake)
	body := []byte("s3-body")
	sha := hexSHA256Bytes(body)

	info, err := store.PutObject(ctx, "snapshots/body.fsm", bytes.NewReader(body), PutOptions{
		Size:        int64(len(body)),
		SHA256:      sha,
		ContentType: "application/octet-stream",
	})
	require.NoError(t, err)
	require.Equal(t, sha, info.SHA256)
	require.Equal(t, types.ChecksumAlgorithmSha256, fake.lastPutChecksumAlgorithm())

	head, ok, err := store.HeadObject(ctx, "snapshots/body.fsm")
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, int64(len(body)), head.Size)
	require.Equal(t, sha, head.SHA256)

	reader, gotInfo, err := store.GetObject(ctx, "snapshots/body.fsm")
	require.NoError(t, err)
	defer func() { require.NoError(t, reader.Close()) }()
	require.Equal(t, sha, gotInfo.SHA256)
	gotBody, err := io.ReadAll(reader)
	require.NoError(t, err)
	require.Equal(t, body, gotBody)
}

func TestS3StoreReusesIdenticalExistingObject(t *testing.T) {
	ctx := context.Background()
	fake := newFakeS3Client()
	store := newTestS3Store(t, fake)
	body := []byte("same-body")
	sha := hexSHA256Bytes(body)
	_, err := store.PutObject(ctx, "snapshots/body.fsm", bytes.NewReader(body), PutOptions{
		Size:   int64(len(body)),
		SHA256: sha,
	})
	require.NoError(t, err)

	info, err := store.PutObject(ctx, "snapshots/body.fsm", strings.NewReader("not-read-on-conflict"), PutOptions{
		Size:   int64(len(body)),
		SHA256: sha,
	})
	require.NoError(t, err)
	require.Equal(t, sha, info.SHA256)
	require.Equal(t, 2, fake.putAttempts())
}

func TestS3StoreRejectsExistingObjectMismatch(t *testing.T) {
	ctx := context.Background()
	fake := newFakeS3Client()
	store := newTestS3Store(t, fake)
	existing := []byte("existing")
	_, err := store.PutObject(ctx, "snapshots/body.fsm", bytes.NewReader(existing), PutOptions{
		Size:   int64(len(existing)),
		SHA256: hexSHA256Bytes(existing),
	})
	require.NoError(t, err)

	candidate := []byte("candidate")
	_, err = store.PutObject(ctx, "snapshots/body.fsm", bytes.NewReader(candidate), PutOptions{
		Size:   int64(len(candidate)),
		SHA256: hexSHA256Bytes(candidate),
	})
	require.ErrorIs(t, err, ErrIntegrity)
}

func TestS3StoreConflictHashesExistingObjectWithoutIntegrityMetadata(t *testing.T) {
	ctx := context.Background()
	fake := newFakeS3Client()
	store := newTestS3Store(t, fake)
	key := "snapshots/body.fsm"
	body := []byte("same-body")
	sha := hexSHA256Bytes(body)
	fake.putRawObject("backup-bucket", key, body, nil, nil)

	info, err := store.PutObject(ctx, key, strings.NewReader("not-read-on-conflict"), PutOptions{
		Size:   int64(len(body)),
		SHA256: sha,
	})
	require.NoError(t, err)
	require.Equal(t, sha, info.SHA256)
	require.Equal(t, 1, fake.getAttempts())
}

func TestS3StoreConflictRejectsExistingObjectWithoutIntegrityMetadataMismatch(t *testing.T) {
	ctx := context.Background()
	fake := newFakeS3Client()
	store := newTestS3Store(t, fake)
	key := "snapshots/body.fsm"
	existing := []byte("aaaa")
	candidate := []byte("bbbb")
	fake.putRawObject("backup-bucket", key, existing, nil, nil)

	_, err := store.PutObject(ctx, key, strings.NewReader("not-read-on-conflict"), PutOptions{
		Size:   int64(len(candidate)),
		SHA256: hexSHA256Bytes(candidate),
	})
	require.ErrorIs(t, err, ErrIntegrity)
	require.Equal(t, 1, fake.getAttempts())
}

func TestS3StoreRejectsParentDirectoryKeys(t *testing.T) {
	ctx := context.Background()
	store := newTestS3Store(t, newFakeS3Client())
	emptySHA := hexSHA256Bytes(nil)

	_, err := store.PutObject(ctx, "..", bytes.NewReader(nil), PutOptions{SHA256: emptySHA})
	require.ErrorIs(t, err, ErrInvalidOptions)
	_, _, err = store.GetObject(ctx, "a/../..")
	require.ErrorIs(t, err, ErrInvalidOptions)
	_, _, err = store.HeadObject(ctx, "../payload")
	require.ErrorIs(t, err, ErrInvalidOptions)
}

func newTestS3Store(t *testing.T, client *fakeS3Client) *S3Store {
	t.Helper()
	store, err := NewS3Store(context.Background(), S3StoreConfig{
		Client:         client,
		Bucket:         "backup-bucket",
		ForcePathStyle: true,
	})
	require.NoError(t, err)
	return store
}

type fakeS3Client struct {
	mu       sync.Mutex
	objects  map[string]fakeS3Object
	lastPut  types.ChecksumAlgorithm
	attempts int
	gets     int
}

type fakeS3Object struct {
	body     []byte
	metadata map[string]string
	checksum *string
}

func newFakeS3Client() *fakeS3Client {
	return &fakeS3Client{objects: make(map[string]fakeS3Object)}
}

func (c *fakeS3Client) PutObject(_ context.Context, input *s3.PutObjectInput, _ ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	key := fakeS3ClientKey(input.Bucket, input.Key)
	c.mu.Lock()
	defer c.mu.Unlock()
	c.attempts++
	c.lastPut = input.ChecksumAlgorithm
	if _, ok := c.objects[key]; ok && aws.ToString(input.IfNoneMatch) == "*" {
		return nil, &smithy.GenericAPIError{Code: "PreconditionFailed", Message: "exists"}
	}
	body, err := io.ReadAll(input.Body)
	if err != nil {
		return nil, err
	}
	if input.ContentLength != nil && int64(len(body)) != *input.ContentLength {
		return nil, &smithy.GenericAPIError{Code: "BadDigest", Message: "content length mismatch"}
	}
	metadata := make(map[string]string, len(input.Metadata))
	for k, v := range input.Metadata {
		metadata[k] = v
	}
	c.objects[key] = fakeS3Object{
		body:     append([]byte(nil), body...),
		metadata: metadata,
		checksum: input.ChecksumSHA256,
	}
	return &s3.PutObjectOutput{}, nil
}

func (c *fakeS3Client) HeadObject(_ context.Context, input *s3.HeadObjectInput, _ ...func(*s3.Options)) (*s3.HeadObjectOutput, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	obj, ok := c.objects[fakeS3ClientKey(input.Bucket, input.Key)]
	if !ok {
		return nil, &types.NotFound{}
	}
	metadata := make(map[string]string, len(obj.metadata))
	for k, v := range obj.metadata {
		metadata[k] = v
	}
	return &s3.HeadObjectOutput{
		ContentLength:  aws.Int64(int64(len(obj.body))),
		Metadata:       metadata,
		ChecksumSHA256: obj.checksum,
	}, nil
}

func (c *fakeS3Client) GetObject(_ context.Context, input *s3.GetObjectInput, _ ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.gets++
	obj, ok := c.objects[fakeS3ClientKey(input.Bucket, input.Key)]
	if !ok {
		return nil, &types.NotFound{}
	}
	metadata := make(map[string]string, len(obj.metadata))
	for k, v := range obj.metadata {
		metadata[k] = v
	}
	return &s3.GetObjectOutput{
		Body:           io.NopCloser(bytes.NewReader(obj.body)),
		ContentLength:  aws.Int64(int64(len(obj.body))),
		Metadata:       metadata,
		ChecksumSHA256: obj.checksum,
	}, nil
}

func (c *fakeS3Client) lastPutChecksumAlgorithm() types.ChecksumAlgorithm {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.lastPut
}

func (c *fakeS3Client) putAttempts() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.attempts
}

func (c *fakeS3Client) getAttempts() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.gets
}

func (c *fakeS3Client) putRawObject(bucket string, key string, body []byte, metadata map[string]string, checksum *string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	clonedMetadata := make(map[string]string, len(metadata))
	for k, v := range metadata {
		clonedMetadata[k] = v
	}
	c.objects[bucket+"/"+key] = fakeS3Object{
		body:     append([]byte(nil), body...),
		metadata: clonedMetadata,
		checksum: checksum,
	}
}

func fakeS3ClientKey(bucket *string, key *string) string {
	return aws.ToString(bucket) + "/" + aws.ToString(key)
}
