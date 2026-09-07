package snapshotoffload

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	etcdraftengine "github.com/bootjp/elastickv/internal/raftengine/etcd"
	"github.com/cockroachdb/errors"
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
	require.False(t, head.UpdatedAt.IsZero())
	require.Equal(t, types.ChecksumModeEnabled, fake.lastHeadObjectChecksumMode())

	reader, gotInfo, err := store.GetObject(ctx, "snapshots/body.fsm")
	require.NoError(t, err)
	defer func() { require.NoError(t, reader.Close()) }()
	require.Equal(t, sha, gotInfo.SHA256)
	require.Equal(t, head.UpdatedAt, gotInfo.UpdatedAt)
	require.Equal(t, types.ChecksumModeEnabled, fake.lastGetObjectChecksumMode())
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
	fake.putRawObject(key, body, nil, nil)

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
	fake.putRawObject(key, existing, nil, nil)

	_, err := store.PutObject(ctx, key, strings.NewReader("not-read-on-conflict"), PutOptions{
		Size:   int64(len(candidate)),
		SHA256: hexSHA256Bytes(candidate),
	})
	require.ErrorIs(t, err, ErrIntegrity)
	require.Equal(t, 1, fake.getAttempts())
}

func TestS3StoreRetriesConditionalRequestConflict(t *testing.T) {
	ctx := context.Background()
	fake := newFakeS3Client()
	fake.conditionalConflicts = 1
	store := newTestS3Store(t, fake)
	body := []byte("retry-body")
	sha := hexSHA256Bytes(body)

	info, err := store.PutObject(ctx, "snapshots/retry.fsm", bytes.NewReader(body), PutOptions{
		Size:   int64(len(body)),
		SHA256: sha,
	})
	require.NoError(t, err)
	require.Equal(t, sha, info.SHA256)
	require.Equal(t, 2, fake.putAttempts())
}

func TestS3StoreUsesMultipartForLargeObject(t *testing.T) {
	ctx := context.Background()
	fake := newFakeS3Client()
	store := newTestS3Store(t, fake)
	store.multipartThreshold = 4
	store.multipartPartSize = 3
	body := []byte("multipart-body")
	sha := hexSHA256Bytes(body)

	info, err := store.PutObject(ctx, "snapshots/multipart.fsm", bytes.NewReader(body), PutOptions{
		Size:   int64(len(body)),
		SHA256: sha,
	})
	require.NoError(t, err)
	require.Equal(t, sha, info.SHA256)
	require.Equal(t, 1, fake.multipartCompletes)
	require.Greater(t, fake.uploadedParts, 1)
	require.Equal(t, types.ChecksumAlgorithmSha256, fake.lastMultipartCreateChecksumAlgorithm())
	require.Equal(t, types.ChecksumAlgorithmSha256, fake.lastUploadPartChecksumAlgorithm())
	require.NotNil(t, fake.lastUploadPartSHA256())
	require.Equal(t, fake.uploadedParts, fake.completedPartChecksums())
}

func TestS3StoreMultipartStreamsSeekableParts(t *testing.T) {
	ctx := context.Background()
	fake := newFakeS3Client()
	store := newTestS3Store(t, fake)
	store.multipartThreshold = 4
	store.multipartPartSize = 3
	body := []byte("multipart-body")
	source := &spyReadAtSeeker{data: body}
	sha := hexSHA256Bytes(body)

	info, err := store.PutObject(ctx, "snapshots/multipart-streamed.fsm", source, PutOptions{
		Size:   int64(len(body)),
		SHA256: sha,
	})
	require.NoError(t, err)
	require.Equal(t, sha, info.SHA256)
	require.Greater(t, source.readAtCalls, 0)
	require.Zero(t, source.readBytes)
}

func TestS3StoreMultipartHonorsDisabledChecksumHeaders(t *testing.T) {
	ctx := context.Background()
	fake := newFakeS3Client()
	store, err := NewS3Store(ctx, S3StoreConfig{
		Client:                 fake,
		Bucket:                 "backup-bucket",
		ForcePathStyle:         true,
		ServerSideEncryption:   string(types.ServerSideEncryptionAes256),
		DisableChecksumHeaders: true,
	})
	require.NoError(t, err)
	store.multipartThreshold = 4
	store.multipartPartSize = 3
	body := []byte("multipart-body")
	sha := hexSHA256Bytes(body)

	info, err := store.PutObject(ctx, "snapshots/multipart-no-checksum.fsm", bytes.NewReader(body), PutOptions{
		Size:   int64(len(body)),
		SHA256: sha,
	})
	require.NoError(t, err)
	require.Equal(t, sha, info.SHA256)
	require.Equal(t, types.ChecksumAlgorithm(""), fake.lastMultipartCreateChecksumAlgorithm())
	require.Equal(t, types.ChecksumAlgorithm(""), fake.lastUploadPartChecksumAlgorithm())
	require.Nil(t, fake.lastUploadPartSHA256())
	require.Zero(t, fake.completedPartChecksums())
}

func TestS3StoreMultipartRejectsSourceHashMismatch(t *testing.T) {
	ctx := context.Background()
	fake := newFakeS3Client()
	store := newTestS3Store(t, fake)
	store.multipartThreshold = 4
	store.multipartPartSize = 3
	body := []byte("multipart-body")
	expectedSHA := hexSHA256Bytes([]byte("different-body"))

	_, err := store.PutObject(ctx, "snapshots/multipart-mismatch.fsm", bytes.NewReader(body), PutOptions{
		Size:   int64(len(body)),
		SHA256: expectedSHA,
	})
	require.ErrorIs(t, err, ErrIntegrity)
	require.Zero(t, fake.multipartCompletes)
	require.Zero(t, fake.activeMultipartUploads())
}

func TestS3ObjectSHA256PrefersMetadataForCompositeMultipartChecksum(t *testing.T) {
	bodySHA := hexSHA256Bytes([]byte("full-object"))
	compositeSHA, err := sha256HexToBase64(hexSHA256Bytes([]byte("aws-composite-checksum")))
	require.NoError(t, err)

	got, err := s3ObjectSHA256(map[string]string{
		s3MetadataSHA256: bodySHA,
	}, aws.String(compositeSHA), types.ChecksumTypeComposite)
	require.NoError(t, err)
	require.Equal(t, bodySHA, got)
}

func TestS3ObjectSHA256RejectsMetadataFullObjectChecksumMismatch(t *testing.T) {
	metadataSHA := hexSHA256Bytes([]byte("metadata-body"))
	fullObjectChecksum, err := sha256HexToBase64(hexSHA256Bytes([]byte("full-object-body")))
	require.NoError(t, err)

	_, err = s3ObjectSHA256(map[string]string{
		s3MetadataSHA256: metadataSHA,
	}, aws.String(fullObjectChecksum), types.ChecksumTypeFullObject)
	require.ErrorIs(t, err, ErrIntegrity)
}

func TestS3StoreHeadAndGetRequestChecksumModeForFullObjectValidation(t *testing.T) {
	ctx := context.Background()
	fake := newFakeS3Client()
	store := newTestS3Store(t, fake)
	key := "snapshots/checksum-mismatch.fsm"
	metadataSHA := hexSHA256Bytes([]byte("metadata-body"))
	fullObjectChecksum, err := sha256HexToBase64(hexSHA256Bytes([]byte("full-object-body")))
	require.NoError(t, err)
	fake.putRawObject(key, []byte("body"), map[string]string{
		s3MetadataSHA256: metadataSHA,
	}, aws.String(fullObjectChecksum), types.ChecksumTypeFullObject)

	_, ok, err := store.HeadObject(ctx, key)
	require.ErrorIs(t, err, ErrIntegrity)
	require.False(t, ok)
	require.Equal(t, types.ChecksumModeEnabled, fake.lastHeadObjectChecksumMode())

	body, _, err := store.GetObject(ctx, key)
	require.ErrorIs(t, err, ErrIntegrity)
	require.Nil(t, body)
	require.Equal(t, types.ChecksumModeEnabled, fake.lastGetObjectChecksumMode())
}

func TestMultipartPartSizeCapsAtS3ObjectLimit(t *testing.T) {
	partSize, err := multipartPartSize(s3MaxObjectBytes, s3DefaultMultipartPart)
	require.NoError(t, err)
	expectedPartSize := max(s3DefaultMultipartPart, (s3MaxObjectBytes+s3MaxMultipartParts-1)/s3MaxMultipartParts)
	require.Equal(t, expectedPartSize, partSize)
	require.LessOrEqual(t, partSize, s3MaxMultipartPart)

	_, err = multipartPartSize(s3MaxObjectBytes+1, s3DefaultMultipartPart)
	require.ErrorIs(t, err, ErrInvalidOptions)
}

func TestS3StoreRejectsInvalidKMSConfig(t *testing.T) {
	_, err := NewS3Store(context.Background(), S3StoreConfig{
		Client:               newFakeS3Client(),
		Bucket:               "backup-bucket",
		ServerSideEncryption: string(types.ServerSideEncryptionAwsKms),
		SSEKMSKeyID:          "alias/snapshot-key",
	})
	require.ErrorIs(t, err, ErrInvalidOptions)
	require.ErrorContains(t, err, "aliases are not supported")
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
		Client:               client,
		Bucket:               "backup-bucket",
		ForcePathStyle:       true,
		ServerSideEncryption: string(types.ServerSideEncryptionAes256),
	})
	require.NoError(t, err)
	return store
}

type fakeS3Client struct {
	mu                   sync.Mutex
	objects              map[string]fakeS3Object
	multipart            map[string]*fakeMultipartUpload
	lastPut              types.ChecksumAlgorithm
	lastMultipartCreate  types.ChecksumAlgorithm
	lastUploadPart       types.ChecksumAlgorithm
	lastUploadPartSHA    *string
	lastHeadChecksumMode types.ChecksumMode
	lastGetChecksumMode  types.ChecksumMode
	completedChecksums   int
	attempts             int
	gets                 int
	conditionalConflicts int
	nextUploadID         int
	uploadedParts        int
	multipartCompletes   int
	// Retention/GC listing controls.
	listPageSize                int
	listOmitContinuationToken   bool
	listRepeatContinuationToken bool
	listErr                     error
	listModTime                 time.Time
	listPrefixes                []string
	deletes                     []string
}

type fakeS3Object struct {
	body                 []byte
	metadata             map[string]string
	checksum             *string
	checksumType         types.ChecksumType
	serverSideEncryption types.ServerSideEncryption
	kmsKeyID             *string
	modifiedAt           time.Time
}

type fakeMultipartUpload struct {
	bucket               string
	key                  string
	metadata             map[string]string
	checksum             *string
	serverSideEncryption types.ServerSideEncryption
	kmsKeyID             *string
	parts                map[int32][]byte
}

type spyReadAtSeeker struct {
	data        []byte
	offset      int64
	readBytes   int64
	readAtCalls int
}

func (r *spyReadAtSeeker) Read(p []byte) (int, error) {
	if r.offset >= int64(len(r.data)) {
		return 0, io.EOF
	}
	n := copy(p, r.data[r.offset:])
	r.offset += int64(n)
	r.readBytes += int64(n)
	if n < len(p) {
		return n, io.EOF
	}
	return n, nil
}

func (r *spyReadAtSeeker) ReadAt(p []byte, off int64) (int, error) {
	r.readAtCalls++
	if off >= int64(len(r.data)) {
		return 0, io.EOF
	}
	n := copy(p, r.data[off:])
	if n < len(p) {
		return n, io.EOF
	}
	return n, nil
}

func (r *spyReadAtSeeker) Seek(offset int64, whence int) (int64, error) {
	var next int64
	switch whence {
	case io.SeekStart:
		next = offset
	case io.SeekCurrent:
		next = r.offset + offset
	case io.SeekEnd:
		next = int64(len(r.data)) + offset
	default:
		return 0, fmt.Errorf("invalid whence %d", whence)
	}
	if next < 0 {
		return 0, fmt.Errorf("negative offset %d", next)
	}
	r.offset = next
	return next, nil
}

func newFakeS3Client() *fakeS3Client {
	return &fakeS3Client{
		objects:   make(map[string]fakeS3Object),
		multipart: make(map[string]*fakeMultipartUpload),
	}
}

func (c *fakeS3Client) PutObject(_ context.Context, input *s3.PutObjectInput, _ ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	key := fakeS3ClientKey(input.Bucket, input.Key)
	c.mu.Lock()
	defer c.mu.Unlock()
	c.attempts++
	c.lastPut = input.ChecksumAlgorithm
	if c.conditionalConflicts > 0 {
		c.conditionalConflicts--
		return nil, &smithy.GenericAPIError{Code: "ConditionalRequestConflict", Message: "retry"}
	}
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
		body:                 append([]byte(nil), body...),
		metadata:             metadata,
		checksum:             input.ChecksumSHA256,
		checksumType:         s3ChecksumTypeForSHA(input.ChecksumSHA256),
		serverSideEncryption: input.ServerSideEncryption,
		kmsKeyID:             input.SSEKMSKeyId,
		modifiedAt:           time.Now().UTC(),
	}
	return &s3.PutObjectOutput{}, nil
}

func (c *fakeS3Client) HeadObject(_ context.Context, input *s3.HeadObjectInput, _ ...func(*s3.Options)) (*s3.HeadObjectOutput, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.lastHeadChecksumMode = input.ChecksumMode
	obj, ok := c.objects[fakeS3ClientKey(input.Bucket, input.Key)]
	if !ok {
		return nil, &types.NotFound{}
	}
	metadata := make(map[string]string, len(obj.metadata))
	for k, v := range obj.metadata {
		metadata[k] = v
	}
	checksum, checksumType := fakeS3ChecksumForMode(input.ChecksumMode, obj)
	return &s3.HeadObjectOutput{
		ContentLength:        aws.Int64(int64(len(obj.body))),
		Metadata:             metadata,
		ChecksumSHA256:       checksum,
		ChecksumType:         checksumType,
		LastModified:         aws.Time(obj.modifiedAt),
		ServerSideEncryption: obj.serverSideEncryption,
		SSEKMSKeyId:          obj.kmsKeyID,
		ETag:                 aws.String(fakeS3ETag(obj.body)),
	}, nil
}

func (c *fakeS3Client) GetObject(_ context.Context, input *s3.GetObjectInput, _ ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.gets++
	c.lastGetChecksumMode = input.ChecksumMode
	obj, ok := c.objects[fakeS3ClientKey(input.Bucket, input.Key)]
	if !ok {
		return nil, &types.NotFound{}
	}
	metadata := make(map[string]string, len(obj.metadata))
	for k, v := range obj.metadata {
		metadata[k] = v
	}
	checksum, checksumType := fakeS3ChecksumForMode(input.ChecksumMode, obj)
	return &s3.GetObjectOutput{
		Body:                 io.NopCloser(bytes.NewReader(obj.body)),
		ContentLength:        aws.Int64(int64(len(obj.body))),
		Metadata:             metadata,
		ChecksumSHA256:       checksum,
		ChecksumType:         checksumType,
		LastModified:         aws.Time(obj.modifiedAt),
		ServerSideEncryption: obj.serverSideEncryption,
		SSEKMSKeyId:          obj.kmsKeyID,
	}, nil
}

func (c *fakeS3Client) CreateMultipartUpload(
	_ context.Context,
	input *s3.CreateMultipartUploadInput,
	_ ...func(*s3.Options),
) (*s3.CreateMultipartUploadOutput, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.nextUploadID++
	uploadID := fmt.Sprintf("upload-%d", c.nextUploadID)
	c.lastMultipartCreate = input.ChecksumAlgorithm
	metadata := make(map[string]string, len(input.Metadata))
	for k, v := range input.Metadata {
		metadata[k] = v
	}
	c.multipart[uploadID] = &fakeMultipartUpload{
		bucket:               aws.ToString(input.Bucket),
		key:                  aws.ToString(input.Key),
		metadata:             metadata,
		serverSideEncryption: input.ServerSideEncryption,
		kmsKeyID:             input.SSEKMSKeyId,
		parts:                make(map[int32][]byte),
	}
	return &s3.CreateMultipartUploadOutput{UploadId: aws.String(uploadID)}, nil
}

func (c *fakeS3Client) UploadPart(
	_ context.Context,
	input *s3.UploadPartInput,
	_ ...func(*s3.Options),
) (*s3.UploadPartOutput, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	upload := c.multipart[aws.ToString(input.UploadId)]
	if upload == nil {
		return nil, &smithy.GenericAPIError{Code: "NoSuchUpload", Message: "missing upload"}
	}
	body, err := io.ReadAll(input.Body)
	if err != nil {
		return nil, err
	}
	c.lastUploadPart = input.ChecksumAlgorithm
	c.lastUploadPartSHA = cloneStringPtr(input.ChecksumSHA256)
	upload.parts[aws.ToInt32(input.PartNumber)] = append([]byte(nil), body...)
	c.uploadedParts++
	return &s3.UploadPartOutput{ETag: aws.String(fmt.Sprintf("etag-%d", aws.ToInt32(input.PartNumber)))}, nil
}

func (c *fakeS3Client) CompleteMultipartUpload(
	_ context.Context,
	input *s3.CompleteMultipartUploadInput,
	_ ...func(*s3.Options),
) (*s3.CompleteMultipartUploadOutput, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	uploadID := aws.ToString(input.UploadId)
	upload := c.multipart[uploadID]
	if upload == nil {
		return nil, &smithy.GenericAPIError{Code: "NoSuchUpload", Message: "missing upload"}
	}
	key := upload.bucket + "/" + upload.key
	if _, ok := c.objects[key]; ok && aws.ToString(input.IfNoneMatch) == "*" {
		return nil, &smithy.GenericAPIError{Code: "PreconditionFailed", Message: "exists"}
	}
	var body []byte
	for _, part := range input.MultipartUpload.Parts {
		if part.ChecksumSHA256 != nil {
			c.completedChecksums++
		}
		partBody := upload.parts[aws.ToInt32(part.PartNumber)]
		body = append(body, partBody...)
	}
	c.objects[key] = fakeS3Object{
		body:                 body,
		metadata:             upload.metadata,
		checksum:             upload.checksum,
		checksumType:         s3ChecksumTypeForSHA(upload.checksum),
		serverSideEncryption: upload.serverSideEncryption,
		kmsKeyID:             upload.kmsKeyID,
		modifiedAt:           time.Now().UTC(),
	}
	c.multipartCompletes++
	delete(c.multipart, uploadID)
	return &s3.CompleteMultipartUploadOutput{}, nil
}

func (c *fakeS3Client) AbortMultipartUpload(
	_ context.Context,
	input *s3.AbortMultipartUploadInput,
	_ ...func(*s3.Options),
) (*s3.AbortMultipartUploadOutput, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.multipart, aws.ToString(input.UploadId))
	return &s3.AbortMultipartUploadOutput{}, nil
}

func (c *fakeS3Client) lastPutChecksumAlgorithm() types.ChecksumAlgorithm {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.lastPut
}

func (c *fakeS3Client) lastMultipartCreateChecksumAlgorithm() types.ChecksumAlgorithm {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.lastMultipartCreate
}

func (c *fakeS3Client) lastUploadPartChecksumAlgorithm() types.ChecksumAlgorithm {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.lastUploadPart
}

func (c *fakeS3Client) lastUploadPartSHA256() *string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return cloneStringPtr(c.lastUploadPartSHA)
}

func (c *fakeS3Client) lastHeadObjectChecksumMode() types.ChecksumMode {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.lastHeadChecksumMode
}

func (c *fakeS3Client) lastGetObjectChecksumMode() types.ChecksumMode {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.lastGetChecksumMode
}

func (c *fakeS3Client) completedPartChecksums() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.completedChecksums
}

func (c *fakeS3Client) activeMultipartUploads() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.multipart)
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

func (c *fakeS3Client) putRawObject(key string, body []byte, metadata map[string]string, checksum *string, checksumType ...types.ChecksumType) {
	c.mu.Lock()
	defer c.mu.Unlock()
	clonedMetadata := make(map[string]string, len(metadata))
	for k, v := range metadata {
		clonedMetadata[k] = v
	}
	storedChecksumType := s3ChecksumTypeForSHA(checksum)
	if len(checksumType) > 0 {
		storedChecksumType = checksumType[0]
	}
	c.objects["backup-bucket/"+key] = fakeS3Object{
		body:                 append([]byte(nil), body...),
		metadata:             clonedMetadata,
		checksum:             checksum,
		checksumType:         storedChecksumType,
		serverSideEncryption: types.ServerSideEncryptionAes256,
		modifiedAt:           time.Now().UTC(),
	}
}

func s3ChecksumTypeForSHA(checksum *string) types.ChecksumType {
	if checksum == nil {
		return ""
	}
	return types.ChecksumTypeFullObject
}

func fakeS3ChecksumForMode(mode types.ChecksumMode, obj fakeS3Object) (*string, types.ChecksumType) {
	if mode != types.ChecksumModeEnabled {
		return nil, ""
	}
	return obj.checksum, obj.checksumType
}

// listMaxKeys lets a test force pagination without uploading 1000
// objects; zero means "use the request's MaxKeys".
func (c *fakeS3Client) ListObjectsV2(
	_ context.Context, input *s3.ListObjectsV2Input, _ ...func(*s3.Options),
) (*s3.ListObjectsV2Output, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.listErr != nil {
		return nil, c.listErr
	}

	bucketPrefix := aws.ToString(input.Bucket) + "/"
	wantPrefix := aws.ToString(input.Prefix)
	c.listPrefixes = append(c.listPrefixes, wantPrefix)
	keys := c.listObjectKeysLocked(bucketPrefix, wantPrefix)
	start, end := c.listPageBounds(keys, input.ContinuationToken)
	out := &s3.ListObjectsV2Output{
		Contents: c.listPageContentsLocked(bucketPrefix, keys[start:end]),
	}
	c.setListContinuation(out, input, keys, start, end)
	return out, nil
}

func (c *fakeS3Client) listObjectKeysLocked(bucketPrefix, wantPrefix string) []string {
	keys := make([]string, 0, len(c.objects))
	for stored := range c.objects {
		key, ok := strings.CutPrefix(stored, bucketPrefix)
		if !ok || !strings.HasPrefix(key, wantPrefix) {
			continue
		}
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func (c *fakeS3Client) listPageBounds(keys []string, token *string) (int, int) {
	start := 0
	if value := aws.ToString(token); value != "" {
		start = sort.SearchStrings(keys, value)
	}
	pageSize := c.listPageSize
	if pageSize <= 0 {
		pageSize = len(keys)
	}
	return start, min(start+pageSize, len(keys))
}

func (c *fakeS3Client) listPageContentsLocked(bucketPrefix string, keys []string) []types.Object {
	contents := make([]types.Object, 0, len(keys))
	for _, key := range keys {
		obj := c.objects[bucketPrefix+key]
		modifiedAt := obj.modifiedAt
		if !c.listModTime.IsZero() {
			modifiedAt = c.listModTime
		}
		contents = append(contents, types.Object{
			Key:          aws.String(key),
			Size:         aws.Int64(int64(len(obj.body))),
			LastModified: aws.Time(modifiedAt),
			ETag:         aws.String(fakeS3ETag(obj.body)),
		})
	}
	return contents
}

func (c *fakeS3Client) setListContinuation(
	out *s3.ListObjectsV2Output,
	input *s3.ListObjectsV2Input,
	keys []string,
	start int,
	end int,
) {
	if end >= len(keys) {
		return
	}
	out.IsTruncated = aws.Bool(true)
	if c.listOmitContinuationToken {
		return
	}
	out.NextContinuationToken = aws.String(c.nextListContinuationToken(input, keys, start, end))
}

func (c *fakeS3Client) nextListContinuationToken(
	input *s3.ListObjectsV2Input,
	keys []string,
	start int,
	end int,
) string {
	if !c.listRepeatContinuationToken {
		return keys[end]
	}
	if token := aws.ToString(input.ContinuationToken); token != "" {
		return token
	}
	return keys[start]
}

func (c *fakeS3Client) listedPrefixes() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]string(nil), c.listPrefixes...)
}

func (c *fakeS3Client) DeleteObject(
	_ context.Context, input *s3.DeleteObjectInput, _ ...func(*s3.Options),
) (*s3.DeleteObjectOutput, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.deletes = append(c.deletes, aws.ToString(input.Key))
	storeKey := fakeS3ClientKey(input.Bucket, input.Key)
	if want := aws.ToString(input.IfMatch); want != "" {
		obj, ok := c.objects[storeKey]
		if !ok {
			return nil, &types.NotFound{}
		}
		if fakeS3ETag(obj.body) != want {
			return nil, &smithy.GenericAPIError{
				Code:    "PreconditionFailed",
				Message: "At least one of the pre-conditions you specified did not hold",
			}
		}
	}
	delete(c.objects, storeKey)
	return &s3.DeleteObjectOutput{}, nil
}

// fakeS3ETag derives a stable ETag from the object bytes, matching
// S3's single-part semantics closely enough for the If-Match tests.
func fakeS3ETag(body []byte) string {
	return `"` + hexSHA256Bytes(body)[:32] + `"`
}

func fakeS3ClientKey(bucket *string, key *string) string {
	return aws.ToString(bucket) + "/" + aws.ToString(key)
}

func cloneStringPtr(value *string) *string {
	if value == nil {
		return nil
	}
	cloned := *value
	return &cloned
}

// TestS3StoreListObjectsPagesThroughEveryPage proves the lister
// honours its all-or-error contract across pages. Retention's phase-2
// safety depends on a complete listing: a payload missed by a
// truncated page would look unreferenced and be reclaimed while a
// committed manifest still points at it.
func TestS3StoreListObjectsPagesThroughEveryPage(t *testing.T) {
	ctx := context.Background()
	client := newFakeS3Client()
	client.listPageSize = 2
	client.listModTime = time.Unix(1_700_000_000, 0).UTC()
	store := newTestS3Store(t, client)

	want := make([]string, 0, 5)
	for i := range 5 {
		key := fmt.Sprintf("cluster-a/v1/payloads/obj-%d.fsm", i)
		body := []byte(key)
		_, err := store.PutObject(ctx, key, bytes.NewReader(body), PutOptions{
			Size:   int64(len(body)),
			SHA256: hexSHA256Bytes(body),
		})
		require.NoError(t, err)
		want = append(want, key)
	}

	refs, err := store.ListObjects(ctx, "cluster-a/v1/payloads")
	require.NoError(t, err)

	got := make([]string, 0, len(refs))
	for _, ref := range refs {
		got = append(got, ref.Key)
		require.Equal(t, client.listModTime, ref.UpdatedAt)
	}
	sort.Strings(got)
	require.Equal(t, want, got, "every page must be returned")
}

func TestS3StoreListObjectsDelimitsSubtreePrefix(t *testing.T) {
	ctx := context.Background()
	client := newFakeS3Client()
	store := newTestS3Store(t, client)
	realKey := "cluster-a/v1/payloads/obj.fsm"
	siblingKey := "cluster-a/v1/payloads-archive/obj.fsm"
	client.putRawObject(realKey, []byte("real"), nil, nil)
	client.putRawObject(siblingKey, []byte("sibling"), nil, nil)

	refs, err := store.ListObjects(ctx, "cluster-a/v1/payloads")
	require.NoError(t, err)

	require.Len(t, refs, 1)
	require.Equal(t, realKey, refs[0].Key)
	require.Equal(t, int64(len("real")), refs[0].Size)
	require.False(t, refs[0].UpdatedAt.IsZero())
	require.Equal(t, []string{"cluster-a/v1/payloads/"}, client.listedPrefixes())
}

// TestS3StoreListObjectsFailsClosedOnTruncatedPageWithoutToken pins
// the pagination-failure branch: a truncated response with no
// continuation token cannot be continued, so the lister must error
// rather than return the partial set it has.
func TestS3StoreListObjectsFailsClosedOnTruncatedPageWithoutToken(t *testing.T) {
	ctx := context.Background()
	client := newFakeS3Client()
	client.listPageSize = 1
	client.listOmitContinuationToken = true
	store := newTestS3Store(t, client)

	for i := range 3 {
		key := fmt.Sprintf("cluster-a/v1/payloads/obj-%d.fsm", i)
		body := []byte(key)
		_, err := store.PutObject(ctx, key, bytes.NewReader(body), PutOptions{
			Size:   int64(len(body)),
			SHA256: hexSHA256Bytes(body),
		})
		require.NoError(t, err)
	}

	refs, err := store.ListObjects(ctx, "cluster-a/v1/payloads")
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrIntegrity))
	require.Nil(t, refs, "a partial listing must never be returned")
}

func TestS3StoreListObjectsRejectsNonAdvancingContinuationToken(t *testing.T) {
	ctx := context.Background()
	client := newFakeS3Client()
	client.listPageSize = 1
	client.listRepeatContinuationToken = true
	store := newTestS3Store(t, client)

	for i := range 3 {
		key := fmt.Sprintf("cluster-a/v1/payloads/obj-%d.fsm", i)
		client.putRawObject(key, []byte(key), nil, nil)
	}

	refs, err := store.ListObjects(ctx, "cluster-a/v1/payloads")
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrIntegrity))
	require.Nil(t, refs, "a non-advancing token must fail closed")
}

func TestS3StoreListObjectsRejectsNoncanonicalKeys(t *testing.T) {
	ctx := context.Background()
	client := newFakeS3Client()
	store := newTestS3Store(t, client)
	client.putRawObject("cluster-a/v1/payloads//obj.fsm", []byte("body"), nil, nil)

	refs, err := store.ListObjects(ctx, "cluster-a/v1/payloads")
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrIntegrity))
	require.Nil(t, refs, "a canonicalized alias must not be returned with another key's identity")
}

func TestS3StoreDeleteObjectIsIdempotentAndValidatesKeys(t *testing.T) {
	ctx := context.Background()
	client := newFakeS3Client()
	store := newTestS3Store(t, client)

	body := []byte("payload")
	key := "cluster-a/v1/payloads/gone.fsm"
	_, err := store.PutObject(ctx, key, bytes.NewReader(body), PutOptions{
		Size:   int64(len(body)),
		SHA256: hexSHA256Bytes(body),
	})
	require.NoError(t, err)

	require.NoError(t, store.DeleteObject(ctx, key))
	// Deleting an absent key must stay a no-op so GC is retry-safe.
	require.NoError(t, store.DeleteObject(ctx, key))
	require.Equal(t, []string{key, key}, client.deletes)

	require.ErrorIs(t, store.DeleteObject(ctx, "../escape"), ErrInvalidOptions)
}

// TestS3StoreConditionalDeleteUsesIfMatchAndMapsPreconditionFailure
// covers the exact compare-and-delete the retention race fix depends
// on: S3 must be asked to delete only the version GC validated, and a
// 412 must surface as ErrObjectModified so GC treats it as a skip
// rather than a failure.
func TestS3StoreConditionalDeleteUsesIfMatchAndMapsPreconditionFailure(t *testing.T) {
	ctx := context.Background()
	client := newFakeS3Client()
	store := newTestS3Store(t, client)

	key := "cluster-a/v1/payloads/sha256/ab/cond.fsm"
	body := []byte("payload bytes")
	_, err := store.PutObject(ctx, key, bytes.NewReader(body), PutOptions{
		Size:   int64(len(body)),
		SHA256: hexSHA256Bytes(body),
	})
	require.NoError(t, err)

	info, ok, err := store.HeadObject(ctx, key)
	require.NoError(t, err)
	require.True(t, ok)
	require.NotEmpty(t, info.ETag, "the store must carry the version token forward")

	// A stale precondition must be refused, and must not delete.
	err = store.DeleteObjectIfUnmodified(ctx, key, DeletePrecondition{ETag: `"not-the-current-version"`})
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrObjectModified))
	_, stillThere, err := store.HeadObject(ctx, key)
	require.NoError(t, err)
	require.True(t, stillThere, "a failed precondition must leave the object in place")

	// The observed version deletes.
	require.NoError(t, store.DeleteObjectIfUnmodified(ctx, key, DeletePrecondition{ETag: info.ETag}))
	_, gone, err := store.HeadObject(ctx, key)
	require.NoError(t, err)
	require.False(t, gone)
}

// TestS3StoreConditionalDeleteRefusesAnEmptyPrecondition stops a
// silent downgrade to an unconditional delete when neither an etag nor
// a last-modified time is available.
func TestS3StoreConditionalDeleteRefusesAnEmptyPrecondition(t *testing.T) {
	ctx := context.Background()
	client := newFakeS3Client()
	store := newTestS3Store(t, client)

	err := store.DeleteObjectIfUnmodified(ctx, "cluster-a/v1/payloads/sha256/ab/x.fsm", DeletePrecondition{})
	require.ErrorIs(t, err, ErrInvalidOptions)
	require.Empty(t, client.deletes, "an unusable precondition must not reach the store")
}
