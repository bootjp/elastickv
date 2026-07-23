package kv

import (
	"bytes"
	"encoding/base64"
	"encoding/binary"
	"testing"

	"github.com/bootjp/elastickv/internal/fskeys"
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

func TestRouteKey_NormalizesFilesystemChunkKey(t *testing.T) {
	t.Parallel()

	want := fskeys.ChunkRouteKey(11, 22)
	require.Equal(t, want, routeKey(fskeys.ChunkKey(11, 22, 1)))
	require.Equal(t, want, routeKey(fskeys.ChunkKey(11, 22, 99)))
	require.Equal(t, want, routeKey(txnLockKey(fskeys.ChunkKey(11, 22, 7))))
	require.Equal(t, fskeys.InodeKey(22), routeKey(fskeys.InodeKey(22)))
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

func TestRouteKey_NormalizesRedisWideColumnKeys(t *testing.T) {
	t.Parallel()

	userKey := []byte("user:key")
	for _, raw := range [][]byte{
		store.HashMetaDeltaKey(userKey, 10, 0),
		store.HashMetaKey(userKey),
		store.HashFieldKey(userKey, []byte("field")),
		store.SetMetaDeltaKey(userKey, 11, 0),
		store.SetMetaKey(userKey),
		store.SetMemberKey(userKey, []byte("member")),
		store.ZSetMetaDeltaKey(userKey, 12, 0),
		store.ZSetMetaKey(userKey),
		store.ZSetMemberKey(userKey, []byte("member")),
		store.ZSetScoreKey(userKey, 1.5, []byte("member")),
	} {
		require.Equal(t, userKey, routeKey(raw))
		require.Equal(t, userKey, routeKey(txnLockKey(raw)))
	}
}

func TestRedisWideColumnScanRouteRangeFansOutBareFamilyAndCursor(t *testing.T) {
	t.Parallel()

	prefix := []byte(store.HashFieldPrefix)
	familyEnd := prefixScanEnd(prefix)
	start := store.HashFieldScanPrefix([]byte("alice"))
	cursor := append(append([]byte(nil), start...), []byte("field\x00")...)

	for _, tc := range []struct {
		name  string
		start []byte
	}{
		{name: "bare family", start: prefix},
		{name: "physical cursor", start: cursor},
	} {
		t.Run(tc.name, func(t *testing.T) {
			routeStart, routeEnd, exact, ok := redisWideColumnScanRouteRange(tc.start, familyEnd)
			require.True(t, ok)
			require.False(t, exact)
			require.Nil(t, routeStart)
			require.Nil(t, routeEnd)
		})
	}

	routeStart, routeEnd, exact, ok := redisWideColumnScanRouteRange(start, prefixScanEnd(start))
	require.True(t, ok)
	require.True(t, exact)
	require.Equal(t, []byte("alice"), routeStart)
	require.Nil(t, routeEnd)
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

func TestRouteKey_ListMetaKeyThatLooksLikeNewDeltaRoutesByRealListKey(t *testing.T) {
	t.Parallel()

	userKey := []byte(store.ListMetaDeltaPrefix + "fake:user")
	baseMeta := store.ListMetaKey(userKey)
	deltaKey := store.ListMetaDeltaKey(userKey, 10, 1)

	require.Equal(t, userKey, routeKey(baseMeta), "base list metadata must not decode as a new delta")
	require.Equal(t, userKey, routeKey(deltaKey), "real list deltas must still route by the logical list key")
}

func TestRouteKey_LegacyListDeltaKeyOnlyUsesBaseMetaRoute(t *testing.T) {
	t.Parallel()

	userKey := []byte("legacy:list")
	raw := legacyListMetaDeltaKey(userKey, 42, 7)
	require.Equal(t, store.ExtractListUserKey(raw), routeKey(raw))
	require.NotEqual(t, userKey, routeKey(raw), "key-only routing must not decode ambiguous legacy deltas")

	collidingUserKey := deltaLookingListMetaUserKey(userKey, 42, 7)
	collidingMeta := store.ListMetaKey(collidingUserKey)
	require.Equal(t, collidingUserKey, routeKey(collidingMeta))
}

func TestRouteKey_MalformedWideColumnKeysFallBackToRaw(t *testing.T) {
	t.Parallel()

	for _, raw := range [][]byte{
		malformedWideColumnKey(store.ListClaimPrefix, 8),
		malformedWideColumnKey(store.HashMetaPrefix, 0),
		malformedWideColumnKey(store.HashFieldPrefix, 0),
		malformedWideColumnKey(store.HashMetaDeltaPrefix, 12),
		malformedWideColumnKey(store.SetMetaPrefix, 0),
		malformedWideColumnKey(store.SetMemberPrefix, 0),
		malformedWideColumnKey(store.SetMetaDeltaPrefix, 12),
		malformedWideColumnKey(store.ZSetMetaPrefix, 0),
		malformedWideColumnKey(store.ZSetMemberPrefix, 0),
		malformedWideColumnKey(store.ZSetScorePrefix, 8),
		malformedWideColumnKey(store.ZSetMetaDeltaPrefix, 12),
	} {
		require.NotPanics(t, func() {
			require.Equal(t, raw, routeKey(raw), "malformed key %q must not decode to a logical route", raw)
		})
	}
}

func malformedWideColumnKey(prefix string, suffixLen int) []byte {
	key := make([]byte, 0, len(prefix)+4+suffixLen)
	key = append(key, prefix...)
	var lenPrefix [4]byte
	binary.BigEndian.PutUint32(lenPrefix[:], ^uint32(0))
	key = append(key, lenPrefix[:]...)
	key = append(key, make([]byte, suffixLen)...)
	return key
}

func legacyListMetaDeltaKey(userKey []byte, commitTS uint64, seqInTxn uint32) []byte {
	key := store.LegacyListMetaDeltaScanPrefix(userKey)
	var ts [8]byte
	binary.BigEndian.PutUint64(ts[:], commitTS)
	key = append(key, ts[:]...)
	var seq [4]byte
	binary.BigEndian.PutUint32(seq[:], seqInTxn)
	return append(key, seq[:]...)
}

func deltaLookingListMetaUserKey(fakeUserKey []byte, commitTS uint64, seqInTxn uint32) []byte {
	key := make([]byte, 0, len("d|")+4+len(fakeUserKey)+8+4)
	key = append(key, "d|"...)
	var lenPrefix [4]byte
	binary.BigEndian.PutUint32(lenPrefix[:], uint32(len(fakeUserKey))) //nolint:gosec // test data is small.
	key = append(key, lenPrefix[:]...)
	key = append(key, fakeUserKey...)
	var ts [8]byte
	binary.BigEndian.PutUint64(ts[:], commitTS)
	key = append(key, ts[:]...)
	var seq [4]byte
	binary.BigEndian.PutUint32(seq[:], seqInTxn)
	return append(key, seq[:]...)
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

func TestRoutePrefixRangeTreatsBroadMappedPrefixesAsFullKeyspace(t *testing.T) {
	t.Parallel()

	tableSegment := base64.RawURLEncoding.EncodeToString([]byte("users"))
	dynamoTableRoute := dynamoRouteTableKey([]byte(tableSegment))

	for _, tc := range []struct {
		name      string
		prefix    []byte
		wantStart []byte
		wantEnd   []byte
	}{
		{
			name:      "raw user prefix",
			prefix:    []byte("ab"),
			wantStart: []byte("ab"),
			wantEnd:   prefixScanEnd([]byte("ab")),
		},
		{
			name:      "concrete redis prefix",
			prefix:    []byte("!redis|string|ab"),
			wantStart: []byte("ab"),
			wantEnd:   prefixScanEnd([]byte("ab")),
		},
		{
			name:      "dynamo table cleanup prefix",
			prefix:    []byte(DynamoItemPrefix + tableSegment + "|7|"),
			wantStart: dynamoTableRoute,
			wantEnd:   routePointRangeEnd(dynamoTableRoute),
		},
		{
			name:      "broad redis namespace",
			prefix:    []byte("!redis|"),
			wantStart: []byte(""),
			wantEnd:   nil,
		},
		{
			name:      "broad wide-column namespace",
			prefix:    []byte("!lst|"),
			wantStart: []byte(""),
			wantEnd:   nil,
		},
		{
			name:      "raw sqs-looking user prefix",
			prefix:    []byte("!sqs|foo"),
			wantStart: []byte("!sqs|foo"),
			wantEnd:   prefixScanEnd([]byte("!sqs|foo")),
		},
		{
			name:      "concrete sqs storage prefix",
			prefix:    []byte(sqsMsgDataPrefix),
			wantStart: sqsGlobalRouteKey,
			wantEnd:   prefixScanEnd(sqsGlobalRouteKey),
		},
		{
			name:      "s3 bucket cleanup prefix",
			prefix:    s3keys.ObjectManifestPrefixForBucket("bucket", 2),
			wantStart: s3keys.RoutePrefixForBucket("bucket", 2),
			wantEnd:   prefixScanEnd(s3keys.RoutePrefixForBucket("bucket", 2)),
		},
		{
			name:      "s3 bucket cleanup route prefix",
			prefix:    s3keys.RoutePrefixForBucket("bucket", 2),
			wantStart: s3keys.RoutePrefixForBucket("bucket", 2),
			wantEnd:   prefixScanEnd(s3keys.RoutePrefixForBucket("bucket", 2)),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			start, end := routePrefixRange(tc.prefix)
			require.Equal(t, tc.wantStart, start)
			require.Equal(t, tc.wantEnd, end)
		})
	}
}

func TestRoutePrefixRangeTreatsDynamoCleanupAsExactRouteKey(t *testing.T) {
	t.Parallel()

	fooSegment := base64.RawURLEncoding.EncodeToString([]byte("foo"))
	foobarSegment := base64.RawURLEncoding.EncodeToString([]byte("foobar"))
	fooRoute := dynamoRouteTableKey([]byte(fooSegment))
	foobarRoute := dynamoRouteTableKey([]byte(foobarSegment))

	start, end := routePrefixRange([]byte(DynamoItemPrefix + fooSegment + "|7|"))

	require.Equal(t, fooRoute, start)
	require.Equal(t, routePointRangeEnd(fooRoute), end)
	require.False(t, rangesIntersectForTest(start, end, foobarRoute, prefixScanEnd(foobarRoute)),
		"cleanup for table foo must not intersect table foobar's route")
}

func rangesIntersectForTest(aStart, aEnd, bStart, bEnd []byte) bool {
	if aEnd != nil && bytes.Compare(aEnd, bStart) <= 0 {
		return false
	}
	if bEnd != nil && bytes.Compare(bEnd, aStart) <= 0 {
		return false
	}
	return true
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

func TestRouteKeyFilterIncludesS3BucketAuxiliaryKeys(t *testing.T) {
	t.Parallel()

	filter := RouteKeyFilter(
		s3keys.RouteKey("bucket-b", 7, "a"),
		s3keys.RouteKey("bucket-b", 7, "z"),
	)

	require.True(t, filter(s3keys.BucketMetaKey("bucket-b")))
	require.True(t, filter(s3keys.BucketGenerationKey("bucket-b")))
	require.True(t, filter(s3keys.ObjectManifestKey("bucket-b", 7, "m")))
	require.False(t, filter(s3keys.BucketMetaKey("bucket-c")))
	require.False(t, filter(s3keys.BucketGenerationKey("bucket-c")))
}

func TestRouteKeyFilterIncludesS3BucketAuxiliaryRawRoute(t *testing.T) {
	t.Parallel()

	filter := RouteKeyFilter([]byte("!s3|"), nil)

	require.True(t, filter(s3keys.BucketMetaKey("bucket-b")))
	require.True(t, filter(s3keys.BucketGenerationKey("bucket-b")))
}

func TestRouteKeyFilterForGroupUsesPartitionResolver(t *testing.T) {
	t.Parallel()

	partitionedKey := []byte(sqsMsgDataPrefix + sqsPartitionMarker + "orders|partition-0|message")
	resolver := &migrationFilterPartitionResolver{
		groups: map[string]uint64{string(partitionedKey): 42},
	}

	require.True(t, RouteKeyFilterForGroup(nil, nil, 42, resolver)(partitionedKey))
	require.False(t, RouteKeyFilterForGroup(nil, nil, 7, resolver)(partitionedKey))
	require.False(t, RouteKeyFilterForGroup(nil, nil, 42, resolver)(
		[]byte(sqsMsgDataPrefix+sqsPartitionMarker+"orders|unknown-partition"),
	))
}

type migrationFilterPartitionResolver struct {
	groups map[string]uint64
}

func (r *migrationFilterPartitionResolver) ResolveGroup(key []byte) (uint64, bool) {
	gid, ok := r.groups[string(key)]
	return gid, ok
}

func (r *migrationFilterPartitionResolver) RecognisesPartitionedKey(key []byte) bool {
	return bytes.HasPrefix(key, []byte(sqsMsgDataPrefix+sqsPartitionMarker))
}
