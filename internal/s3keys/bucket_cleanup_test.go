package s3keys_test

import (
	"testing"

	"github.com/bootjp/elastickv/internal/s3keys"
	"github.com/stretchr/testify/require"
)

func TestBucketCleanupKeyRoundTrip(t *testing.T) {
	for _, bucket := range []string{"bucket", "a|b", "a\x00b"} {
		key := s3keys.BucketCleanupKey(bucket, 42)
		got, generation, ok := s3keys.ParseBucketCleanupKey(key)
		require.True(t, ok)
		require.Equal(t, bucket, got)
		require.Equal(t, uint64(42), generation)
	}
	_, _, ok := s3keys.ParseBucketCleanupKey([]byte(s3keys.BucketCleanupPrefix))
	require.False(t, ok)
}
