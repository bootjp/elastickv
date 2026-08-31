package kv

import (
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

func TestRouteOwnershipKey_NormalizesS3BucketAuxiliaryKeys(t *testing.T) {
	t.Parallel()

	want := s3keys.RoutePrefixForBucketAnyGeneration("bucket-a")
	require.Equal(t, want, RouteOwnershipKey(s3keys.BucketMetaKey("bucket-a")))
	require.Equal(t, want, RouteOwnershipKey(s3keys.BucketGenerationKey("bucket-a")))
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

func TestRouteFilterKey_NormalizesRedisAuxiliaryKeys(t *testing.T) {
	t.Parallel()

	userKey := []byte("user:key")
	for _, raw := range [][]byte{
		store.ListMetaDeltaKey(userKey, 10, 0),
		store.ListClaimKey(userKey, 1),
		store.StreamMetaKey(userKey),
		store.StreamEntryKey(userKey, 123, 4),
	} {
		require.Equal(t, userKey, routeFilterKey(raw))
		require.Equal(t, userKey, routeFilterKey(txnLockKey(raw)))
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

func TestListAuxiliaryScanRouteRangeFansOutBareFamilyAndCursor(t *testing.T) {
	t.Parallel()

	prefix := []byte(store.ListMetaDeltaPrefix)
	familyEnd := prefixScanEnd(prefix)
	userPrefix := store.ListMetaDeltaScanPrefix([]byte("alice"))
	cursor := store.ListMetaDeltaKey([]byte("alice"), 10, 0)

	for _, tc := range []struct {
		name  string
		start []byte
	}{
		{name: "bare family", start: prefix},
		{name: "physical cursor", start: cursor},
	} {
		t.Run(tc.name, func(t *testing.T) {
			routeStart, exact, ok := listAuxiliaryScanRouteRange(tc.start, familyEnd)
			require.True(t, ok)
			require.False(t, exact)
			require.Nil(t, routeStart)
		})
	}

	routeStart, exact, ok := listAuxiliaryScanRouteRange(userPrefix, prefixScanEnd(userPrefix))
	require.True(t, ok)
	require.True(t, exact)
	require.Equal(t, []byte("alice"), routeStart)
}

func TestRouteKey_NormalizesRedisInternalEmptyUserKey(t *testing.T) {
	t.Parallel()

	for _, raw := range [][]byte{
		[]byte("!redis|str|"),
		[]byte("!redis|route|"),
	} {
		got := routeKey(raw)
		require.NotNil(t, got)
		require.Empty(t, got)
	}
}

func TestRouteKey_NormalizesRedisListAndStreamAuxiliaryKeys(t *testing.T) {
	t.Parallel()

	for _, userKey := range [][]byte{
		[]byte("!sqs|foo"),
		[]byte("!redis|str|foo"),
	} {
		t.Run(string(userKey), func(t *testing.T) {
			t.Parallel()
			for _, raw := range [][]byte{
				store.ListMetaDeltaKey(userKey, 12, 0),
				store.ListMetaDeltaScanPrefix(userKey),
				store.ListClaimKey(userKey, 3),
				store.ListClaimScanPrefix(userKey),
				store.StreamMetaKey(userKey),
				store.StreamEntryKey(userKey, 123, 4),
			} {
				require.Equal(t, userKey, routeKey(raw))
				require.Equal(t, userKey, routeKey(txnLockKey(raw)))
			}
		})
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

// normalizeRouteKey places stream writes on the logical user-key route. Scans
// must be projected the same way: if XRANGE/XREAD/XTRIM keep resolving through
// the raw !stream|entry| prefix, a split that separates the raw prefix from the
// user key sends the scan to a different group than the XADD that wrote the
// entries, and the reader silently sees an empty stream.
func TestStreamScanRouteRangeResolvesThroughUserKey(t *testing.T) {
	t.Parallel()

	userKey := []byte("alice")
	for _, tc := range []struct {
		name  string
		start []byte
	}{
		{name: "entries", start: store.StreamEntryScanPrefix(userKey)},
		{name: "meta", start: store.StreamMetaKey(userKey)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			routeStart, routeEnd, exact, ok := redisWideColumnScanRouteRange(tc.start, prefixScanEnd(tc.start))
			require.True(t, ok, "stream scans must be recognized as encoded user-key scans")
			require.True(t, exact)
			require.Equal(t, userKey, routeStart)
			require.Nil(t, routeEnd)
		})
	}
}

// A scan over the bare stream family has no single user key to project onto, so
// it must fan out to every route rather than silently resolving to one.
func TestStreamScanRouteRangeFansOutBareFamily(t *testing.T) {
	t.Parallel()

	prefix := []byte(store.StreamEntryPrefix)
	routeStart, routeEnd, exact, ok := redisWideColumnScanRouteRange(prefix, prefixScanEnd(prefix))
	require.True(t, ok)
	require.False(t, exact)
	require.Nil(t, routeStart)
	require.Nil(t, routeEnd)
}

// Stream scans are routed like wide-column scans but must not be dragged
// through the wide-column canonicalization path: they have no legacy physical
// form, so the point reads it performs would be pure overhead.
func TestStreamScansAreNotWideColumnCanonicalizable(t *testing.T) {
	t.Parallel()

	userKey := []byte("alice")
	for _, start := range [][]byte{
		store.StreamEntryScanPrefix(userKey),
		store.StreamMetaKey(userKey),
		[]byte(store.StreamEntryPrefix),
	} {
		require.False(t, redisWideColumnCanonicalizableScan(start))
	}
	for _, start := range [][]byte{
		store.HashFieldScanPrefix(userKey),
		store.ZSetScoreScanPrefix(userKey),
		[]byte(store.HashFieldPrefix),
	} {
		require.True(t, redisWideColumnCanonicalizableScan(start))
	}
}

func legacyListMetaDeltaKey(userKey []byte, commitTS uint64) []byte {
	const seqInTxn = uint32(1)
	key := store.LegacyListMetaDeltaScanPrefix(userKey)
	var ts [8]byte
	binary.BigEndian.PutUint64(ts[:], commitTS)
	key = append(key, ts[:]...)
	var seq [4]byte
	binary.BigEndian.PutUint32(seq[:], seqInTxn)
	return append(key, seq[:]...)
}
