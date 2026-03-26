package s3keys

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBucketMetaKey_RoundTripsZeroByteSegments(t *testing.T) {
	t.Parallel()

	bucket := string([]byte{'b', 'u', 0x00, 'c', 'k', 'e', 't'})
	key := BucketMetaKey(bucket)

	parsed, ok := ParseBucketMetaKey(key)
	require.True(t, ok)
	require.Equal(t, bucket, parsed)
}

func TestObjectManifestKey_RoundTripsZeroByteSegments(t *testing.T) {
	t.Parallel()

	bucket := string([]byte{'b', 0x00, 'k'})
	object := string([]byte{'o', 'b', 'j', 0x00, '/', 'x'})
	key := ObjectManifestKey(bucket, 7, object)

	parsedBucket, generation, parsedObject, ok := ParseObjectManifestKey(key)
	require.True(t, ok)
	require.Equal(t, bucket, parsedBucket)
	require.Equal(t, uint64(7), generation)
	require.Equal(t, object, parsedObject)
}

func TestExtractRouteKey_ObjectScopedKeys(t *testing.T) {
	t.Parallel()

	bucket := "bucket-a"
	generation := uint64(3)
	object := "dir/file.txt"
	want := RouteKey(bucket, generation, object)

	keys := [][]byte{
		ObjectManifestKey(bucket, generation, object),
		UploadMetaKey(bucket, generation, object, "upload-1"),
		UploadPartKey(bucket, generation, object, "upload-1", 9),
		BlobKey(bucket, generation, object, "upload-1", 9, 2),
		GCUploadKey(bucket, generation, object, "upload-1"),
	}
	for _, key := range keys {
		require.Equal(t, want, ExtractRouteKey(key))
	}
}

func TestManifestScanRouteBounds(t *testing.T) {
	t.Parallel()

	start := ObjectManifestScanStart("bucket-a", 5, string([]byte{'a', 0x00, 'b'}))
	end := ObjectManifestScanStart("bucket-a", 5, "z")

	routeStart, routeEnd, ok := ManifestScanRouteBounds(start, end)
	require.True(t, ok)
	require.Equal(t, append([]byte(RoutePrefix), start[len(ObjectManifestPrefix):]...), routeStart)
	require.Equal(t, append([]byte(RoutePrefix), end[len(ObjectManifestPrefix):]...), routeEnd)
}

func TestManifestScanRouteBoundsRejectsNonManifestKeys(t *testing.T) {
	t.Parallel()

	routeStart, routeEnd, ok := ManifestScanRouteBounds(BucketMetaKey("bucket-a"), nil)
	require.False(t, ok)
	require.Nil(t, routeStart)
	require.Nil(t, routeEnd)
}

func TestEncodeSegmentPrefix_EscapesZeroBytes(t *testing.T) {
	t.Parallel()

	encoded := EncodeSegmentPrefix([]byte{0x00, 'a', 0x00})
	require.Equal(t, []byte{0x00, 0xFF, 'a', 0x00, 0xFF}, encoded)
	require.False(t, bytes.Contains(encoded, []byte{0x00, 0x00}))
}

func TestParseUploadPartKey_RoundTrip(t *testing.T) {
	t.Parallel()

	bucket := "test-bucket"
	generation := uint64(42)
	object := "dir/photo.jpg"
	uploadID := "abc123"
	partNo := uint64(7)

	key := UploadPartKey(bucket, generation, object, uploadID, partNo)
	parsedBucket, parsedGen, parsedObject, parsedUploadID, parsedPartNo, ok := ParseUploadPartKey(key)
	require.True(t, ok)
	require.Equal(t, bucket, parsedBucket)
	require.Equal(t, generation, parsedGen)
	require.Equal(t, object, parsedObject)
	require.Equal(t, uploadID, parsedUploadID)
	require.Equal(t, partNo, parsedPartNo)
}

func TestParseUploadPartKey_ZeroBytesInSegments(t *testing.T) {
	t.Parallel()

	bucket := string([]byte{'b', 0x00, 'k'})
	object := string([]byte{'o', 0x00, 'j'})
	uploadID := string([]byte{'u', 0x00})

	key := UploadPartKey(bucket, 5, object, uploadID, 3)
	parsedBucket, gen, parsedObject, parsedUploadID, partNo, ok := ParseUploadPartKey(key)
	require.True(t, ok)
	require.Equal(t, bucket, parsedBucket)
	require.Equal(t, uint64(5), gen)
	require.Equal(t, object, parsedObject)
	require.Equal(t, uploadID, parsedUploadID)
	require.Equal(t, uint64(3), partNo)
}

func TestParseUploadPartKey_RejectsNonPartKeys(t *testing.T) {
	t.Parallel()

	_, _, _, _, _, ok := ParseUploadPartKey(BucketMetaKey("bucket"))
	require.False(t, ok)

	_, _, _, _, _, ok = ParseUploadPartKey(ObjectManifestKey("bucket", 1, "obj"))
	require.False(t, ok)

	_, _, _, _, _, ok = ParseUploadPartKey(BlobKey("bucket", 1, "obj", "uid", 1, 0))
	require.False(t, ok)
}

func TestUploadPartPrefixForUpload_IsPrefixOfPartKeys(t *testing.T) {
	t.Parallel()

	bucket := "bucket-a"
	generation := uint64(10)
	object := "file.txt"
	uploadID := "upload-1"

	prefix := UploadPartPrefixForUpload(bucket, generation, object, uploadID)
	for partNo := uint64(1); partNo <= 5; partNo++ {
		key := UploadPartKey(bucket, generation, object, uploadID, partNo)
		require.True(t, bytes.HasPrefix(key, prefix), "part key %d should have the upload prefix", partNo)
	}

	// Different upload should NOT match.
	otherKey := UploadPartKey(bucket, generation, object, "other-upload", 1)
	require.False(t, bytes.HasPrefix(otherKey, prefix))
}

func TestVersionedBlobKey_ZeroVersionMatchesBlobKey(t *testing.T) {
	t.Parallel()

	bucket := "bucket-v"
	generation := uint64(5)
	object := "file.bin"
	uploadID := "upload-v"
	partNo := uint64(2)
	chunkNo := uint64(3)

	// VersionedBlobKey with version=0 must produce the same key as BlobKey.
	require.Equal(t, BlobKey(bucket, generation, object, uploadID, partNo, chunkNo),
		VersionedBlobKey(bucket, generation, object, uploadID, partNo, chunkNo, 0))
}

func TestVersionedBlobKey_NonZeroVersionDiffersFromBlobKey(t *testing.T) {
	t.Parallel()

	bucket := "bucket-v"
	generation := uint64(5)
	object := "file.bin"
	uploadID := "upload-v"
	partNo := uint64(2)
	chunkNo := uint64(3)
	version := uint64(999)

	versionedKey := VersionedBlobKey(bucket, generation, object, uploadID, partNo, chunkNo, version)
	unversionedKey := BlobKey(bucket, generation, object, uploadID, partNo, chunkNo)
	require.NotEqual(t, unversionedKey, versionedKey)

	// Different part versions must produce different keys.
	otherVersionKey := VersionedBlobKey(bucket, generation, object, uploadID, partNo, chunkNo, version+1)
	require.NotEqual(t, versionedKey, otherVersionKey)
}

func TestBlobPrefixForUpload_IsPrefixOfBlobKeys(t *testing.T) {
	t.Parallel()

	bucket := "bucket-b"
	generation := uint64(3)
	object := "data.bin"
	uploadID := "upload-2"

	prefix := BlobPrefixForUpload(bucket, generation, object, uploadID)
	for partNo := uint64(1); partNo <= 3; partNo++ {
		for chunkNo := uint64(0); chunkNo < 4; chunkNo++ {
			key := BlobKey(bucket, generation, object, uploadID, partNo, chunkNo)
			require.True(t, bytes.HasPrefix(key, prefix), "blob key part=%d chunk=%d should have the upload prefix", partNo, chunkNo)
		}
	}

	// Different upload should NOT match.
	otherKey := BlobKey(bucket, generation, object, "other-upload", 1, 0)
	require.False(t, bytes.HasPrefix(otherKey, prefix))
}
