package distribution

import (
	"testing"

	"github.com/bootjp/elastickv/internal/s3keys"
	"github.com/stretchr/testify/require"
)

// Export and cleanup must agree on which slice owns a bucket's auxiliary rows.
// They resolve it through different call paths -- the adapter's export filter
// and this bracket -- so when the export moved to owner semantics and the
// bracket stayed on intersection, a slice that started inside a bucket's route
// interval would refuse to export that bucket's metadata and then happily
// delete it during CLEANUP. Both now share S3BucketAuxiliaryRouteSelected.
func TestS3BucketAuxiliaryOwnershipIsPointContainmentNotIntersection(t *testing.T) {
	t.Parallel()

	const bucket = "bucket-owner"
	bucketStart := s3keys.RoutePrefixForBucketAnyGeneration(bucket)
	metaKey := s3keys.BucketMetaKey(bucket)

	// A split inside the bucket produces a slice bounded within the bucket's own
	// !s3route| interval: it overlaps the bucket but starts after the bucket's
	// route start, so the preceding slice still owns the auxiliary rows. The
	// bounds also keep the raw !s3| key out of range, which is what isolates the
	// ownership rule from the raw-key match both predicates check first.
	insideStart := s3keys.RouteKey(bucket, 0, "")
	insideEnd := prefixScanEnd(bucketStart)
	require.Greater(t, string(insideStart), string(bucketStart),
		"fixture must start after the bucket route start")
	require.False(t, routeKeyInRange(metaKey, insideStart, insideEnd),
		"fixture must not let the raw-key branch decide")

	require.False(t, S3BucketAuxiliaryRouteSelected(bucket, insideStart, insideEnd),
		"a slice starting inside the bucket does not own its auxiliary rows")

	bracket := MigrationBracket{Family: MigrationFamilyS3BucketMeta, RequiresDecodedS3: true}
	require.False(t, bracket.containsDecodedS3Route(metaKey, insideStart, insideEnd),
		"cleanup must not claim auxiliary rows the export left with the previous owner")

	// The owning slice contains the bucket's route start, so both agree it is
	// exported and may be cleaned up.
	require.True(t, S3BucketAuxiliaryRouteSelected(bucket, bucketStart, insideEnd))
	require.True(t, bracket.containsDecodedS3Route(metaKey, bucketStart, insideEnd))
}
