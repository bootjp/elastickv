package kv

import (
	"encoding/base64"
	"testing"

	"github.com/bootjp/elastickv/internal/s3keys"
	"github.com/bootjp/elastickv/store"
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

func TestRouteKey_NormalizesRedisTxnWideFenceKeys(t *testing.T) {
	t.Parallel()

	userKey := []byte("user:key")
	for _, raw := range [][]byte{
		[]byte("!redis|txn-wide-hash|user:key"),
		[]byte("!redis|txn-wide-set|user:key"),
		[]byte("!redis|txn-wide-list|user:key"),
		[]byte("!redis|txn-wide-zset|user:key"),
	} {
		require.Equal(t, userKey, routeKey(raw))
	}
}

func TestRouteKey_NormalizesDynamoKeysToTable(t *testing.T) {
	t.Parallel()

	tableSegment := []byte(base64.RawURLEncoding.EncodeToString([]byte("users")))
	indexSegment := base64.RawURLEncoding.EncodeToString([]byte("status-index"))
	want := dynamoRouteTableKey(tableSegment)

	metaKey := append([]byte(DynamoTableMetaPrefix), tableSegment...)
	generationKey := append([]byte(DynamoTableGenerationPrefix), tableSegment...)
	itemKey := append([]byte(DynamoItemPrefix+string(tableSegment)+"|7|"), []byte("pk\x00\x01")...)
	gsiKey := append([]byte(DynamoGSIPrefix+string(tableSegment)+"|7|"+indexSegment+"|"), []byte("idx\x00\x01pk\x00\x01")...)

	require.Equal(t, want, routeKey(metaKey))
	require.Equal(t, want, routeKey(generationKey))
	require.Equal(t, want, routeKey(itemKey))
	require.Equal(t, want, routeKey(gsiKey))
	require.Equal(t, want, routeKey(txnLockKey(itemKey)))
}

// TestRouteKey_CollapsesDynamoGenerationsToSameTableRoute proves that two
// DynamoDB item/GSI keys for the SAME table but DIFFERENT generations
// normalize to the identical route key, so they always resolve to the same
// shard group. dynamoRouteFromTablePrefixedKey splits at the first '|' after
// the family prefix — that segment is the table name, and the generation
// (which comes after it) is routing-invisible. This is the invariant that
// makes a per-key lease check on the current generation also fence the
// migration source generation (coderabbit #952 "lease pre-pass ignores
// migration source generations" rebuttal): both generations live on one group.
func TestRouteKey_CollapsesDynamoGenerationsToSameTableRoute(t *testing.T) {
	t.Parallel()

	tableSegment := base64.RawURLEncoding.EncodeToString([]byte("users"))
	indexSegment := base64.RawURLEncoding.EncodeToString([]byte("status-index"))
	want := dynamoRouteTableKey([]byte(tableSegment))

	// Generation 7 is the migrating-to (current) generation; generation 6 is
	// the migration source. The lease pre-pass fences gen 7's key; the read
	// path also reads gen 6's key during migration.
	currentItemKey := append([]byte(DynamoItemPrefix+tableSegment+"|7|"), []byte("pk\x00\x01")...)
	sourceItemKey := append([]byte(DynamoItemPrefix+tableSegment+"|6|"), []byte("pk\x00\x01")...)
	currentGSIKey := append([]byte(DynamoGSIPrefix+tableSegment+"|7|"+indexSegment+"|"), []byte("idx\x00\x01")...)
	sourceGSIKey := append([]byte(DynamoGSIPrefix+tableSegment+"|6|"+indexSegment+"|"), []byte("idx\x00\x01")...)

	require.Equal(t, want, routeKey(currentItemKey))
	require.Equal(t, want, routeKey(sourceItemKey),
		"migration source generation item key must route to the same table group as the current generation")
	require.Equal(t, routeKey(currentItemKey), routeKey(sourceItemKey),
		"current and source generation item keys must collapse to the same route key")
	require.Equal(t, want, routeKey(currentGSIKey))
	require.Equal(t, want, routeKey(sourceGSIKey),
		"migration source generation GSI key must route to the same table group as the current generation")
}

func TestRouteKey_NormalizesCollectionMigrationFamilies(t *testing.T) {
	t.Parallel()

	userKey := []byte("redis:user|with|separators")
	cases := [][]byte{
		store.ListMetaDeltaKey(userKey, 10, 1),
		store.ListClaimKey(userKey, -2),
		store.HashMetaKey(userKey),
		store.HashFieldKey(userKey, []byte("field")),
		store.HashMetaDeltaKey(userKey, 11, 2),
		store.SetMetaKey(userKey),
		store.SetMemberKey(userKey, []byte("member")),
		store.SetMetaDeltaKey(userKey, 12, 3),
		store.ZSetMetaKey(userKey),
		store.ZSetMemberKey(userKey, []byte("member")),
		store.ZSetScoreKey(userKey, 1.25, []byte("member")),
		store.ZSetMetaDeltaKey(userKey, 13, 4),
		store.StreamMetaKey(userKey),
		store.StreamEntryKey(userKey, 14, 5),
	}

	for _, raw := range cases {
		require.Equal(t, userKey, routeKey(raw), "raw key %q must route by its logical user key", raw)
	}
}

func TestRouteKey_NormalizesTxnSuccessMarkerByLockedKey(t *testing.T) {
	t.Parallel()

	lockedKey := []byte("secondary|key\x00with|separators")
	primaryKey := []byte("primary|key\x00with|separators")
	marker := TxnSuccessMarkerKey(lockedKey, 100, 200, primaryKey)

	require.Equal(t, lockedKey, routeKey(marker))

	malformed := append([]byte(nil), marker...)
	malformed[len(txnSuccessPrefixBytes)] = 2
	require.Equal(t, malformed, routeKey(malformed), "malformed success markers must fall back to their raw key")
}

func TestRouteKey_SQSDecoderIsConcreteOnly(t *testing.T) {
	t.Parallel()

	want := []byte(sqsRoutePrefix + "global")
	for _, raw := range [][]byte{
		[]byte(sqsQueueMetaPrefix + "queue"),
		[]byte(sqsQueueGenPrefix + "queue"),
		[]byte(sqsQueueSeqPrefix + "queue"),
		[]byte(sqsQueueTombstonePrefix + "queue"),
		[]byte(sqsMsgDataPrefix + "queue|1|msg"),
		[]byte(sqsMsgVisPrefix + "queue|1|msg"),
		[]byte(sqsMsgDedupPrefix + "queue|1|dedup"),
		[]byte(sqsMsgGroupPrefix + "queue|1|group"),
		[]byte(sqsMsgByAgePrefix + "queue|1|ts"),
		[]byte(sqsMsgDataPrefix + sqsPartitionMarker + "queue|0|1|msg"),
		[]byte(sqsMsgVisPrefix + sqsPartitionMarker + "queue|0|1|msg"),
		[]byte(sqsMsgDedupPrefix + sqsPartitionMarker + "queue|0|1|dedup"),
		[]byte(sqsMsgGroupPrefix + sqsPartitionMarker + "queue|0|1|group"),
		[]byte(sqsMsgByAgePrefix + sqsPartitionMarker + "queue|0|1|ts"),
	} {
		require.Equal(t, want, routeKey(raw), "concrete SQS key %q must use the SQS route", raw)
	}

	rawUser := []byte("!sqs|foo")
	require.Equal(t, rawUser, routeKey(rawUser), "adapter-looking raw user key must stay on its raw route")
}

func TestRouteKey_S3DecoderIsConcreteOnly(t *testing.T) {
	t.Parallel()

	manifest := s3keys.ObjectManifestKey("bucket", 2, "obj")
	require.Equal(t, s3keys.RouteKey("bucket", 2, "obj"), routeKey(manifest))

	rawUser := []byte("!s3|foo")
	require.Equal(t, rawUser, routeKey(rawUser), "adapter-looking raw user key must stay on its raw route")
}

func TestRouteKeyFilterTreatsNilAndEmptyEndAsInfinity(t *testing.T) {
	t.Parallel()

	start := []byte("m")
	for _, tc := range []struct {
		name string
		end  []byte
	}{
		{name: "nil"},
		{name: "empty", end: []byte{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			filter := RouteKeyFilter(start, tc.end)
			require.False(t, filter([]byte("a")))
			require.True(t, filter([]byte("m")))
			require.True(t, filter([]byte("z")))
		})
	}
}
