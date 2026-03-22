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
