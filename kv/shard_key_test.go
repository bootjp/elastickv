package kv

import (
	"encoding/base64"
	"testing"

	"github.com/bootjp/elastickv/internal/s3keys"
	"github.com/stretchr/testify/require"
)

func TestRouteKey_NormalizesS3ManifestKey(t *testing.T) {
	t.Parallel()

	key := s3keys.ObjectManifestKey("bucket-a", 7, "path/to/object")
	require.Equal(t, s3keys.RouteKey("bucket-a", 7, "path/to/object"), routeKey(key))
}

func TestRouteKey_NormalizesS3BlobKey(t *testing.T) {
	t.Parallel()

	key := s3keys.BlobKey("bucket-a", 7, "path/to/object", "upload-1", 1, 2)
	require.Equal(t, s3keys.RouteKey("bucket-a", 7, "path/to/object"), routeKey(key))
}

func TestRouteKey_NormalizesTxnWrappedS3Key(t *testing.T) {
	t.Parallel()

	embedded := s3keys.UploadPartKey("bucket-a", 7, "path/to/object", "upload-1", 3)
	require.Equal(t, s3keys.RouteKey("bucket-a", 7, "path/to/object"), routeKey(txnLockKey(embedded)))
}

func TestRouteKey_NormalizesDynamoKeysToTable(t *testing.T) {
	t.Parallel()

	tableSegment := []byte(base64.RawURLEncoding.EncodeToString([]byte("users")))
	indexSegment := base64.RawURLEncoding.EncodeToString([]byte("status-index"))
	want := dynamoRouteTableKey(tableSegment)

	metaKey := append([]byte(dynamoTableMetaPrefix), tableSegment...)
	generationKey := append([]byte(dynamoTableGenerationPrefix), tableSegment...)
	itemKey := append([]byte(dynamoItemPrefix+string(tableSegment)+"|7|"), []byte("pk\x00\x01")...)
	gsiKey := append([]byte(dynamoGSIPrefix+string(tableSegment)+"|7|"+indexSegment+"|"), []byte("idx\x00\x01pk\x00\x01")...)

	require.Equal(t, want, routeKey(metaKey))
	require.Equal(t, want, routeKey(generationKey))
	require.Equal(t, want, routeKey(itemKey))
	require.Equal(t, want, routeKey(gsiKey))
	require.Equal(t, want, routeKey(txnLockKey(itemKey)))
}
