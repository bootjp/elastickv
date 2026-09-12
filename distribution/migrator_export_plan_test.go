package distribution

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/bootjp/elastickv/internal/fskeys"
	"github.com/bootjp/elastickv/internal/s3keys"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestPlanMigrationBracketsIncludesRequiredFamilies(t *testing.T) {
	t.Parallel()

	brackets, err := PlanMigrationBrackets([]byte("m"), []byte("z"))
	require.NoError(t, err)

	byFamily := bracketsByFamily(brackets)
	required := map[uint32]string{
		MigrationFamilyUser:                            "user",
		MigrationFamilyTxnIntent:                       migrationTxnIntentPrefix,
		MigrationFamilyTxnCommit:                       migrationTxnCommitPrefix,
		MigrationFamilyTxnRollback:                     migrationTxnRollbackPrefix,
		MigrationFamilyTxnSuccess:                      migrationTxnSuccessPrefix,
		MigrationFamilyTxnMeta:                         migrationTxnMetaPrefix,
		MigrationFamilyTxnLock:                         migrationTxnLockPrefix,
		MigrationFamilyListMeta:                        store.ListMetaPrefix,
		MigrationFamilyListItem:                        store.ListItemPrefix,
		MigrationFamilyListMetaDelta:                   store.ListMetaDeltaPrefix,
		MigrationFamilyLegacyListMetaDelta:             store.LegacyListMetaDeltaPrefix,
		MigrationFamilyListClaim:                       store.ListClaimPrefix,
		MigrationFamilyRedisLegacy:                     migrationRedisPrefix,
		MigrationFamilyHash:                            migrationHashPrefix,
		MigrationFamilySet:                             migrationSetPrefix,
		MigrationFamilyZSet:                            migrationZSetPrefix,
		MigrationFamilyStreamMeta:                      store.StreamMetaPrefix,
		MigrationFamilyStreamEntry:                     store.StreamEntryPrefix,
		MigrationFamilyDynamoTableMeta:                 migrationDynamoMetaPrefix,
		MigrationFamilyDynamoTableGeneration:           migrationDynamoGenPrefix,
		MigrationFamilyDynamoItem:                      migrationDynamoItemPrefix,
		MigrationFamilyDynamoGSI:                       migrationDynamoGSIPrefix,
		MigrationFamilySQSQueueMeta:                    migrationSQSQueueMetaPrefix,
		MigrationFamilySQSQueueGeneration:              migrationSQSQueueGenPrefix,
		MigrationFamilySQSQueueSequence:                migrationSQSQueueSeqPrefix,
		MigrationFamilySQSQueueTombstone:               migrationSQSQueueTombstonePrefix,
		MigrationFamilySQSMessageData:                  migrationSQSMsgDataPrefix,
		MigrationFamilySQSMessageVisibility:            migrationSQSMsgVisPrefix,
		MigrationFamilySQSMessageDedup:                 migrationSQSMsgDedupPrefix,
		MigrationFamilySQSMessageGroup:                 migrationSQSMsgGroupPrefix,
		MigrationFamilySQSMessageByAge:                 migrationSQSMsgByAgePrefix,
		MigrationFamilySQSPartitionedMessageData:       migrationSQSMsgDataPrefix + migrationSQSPartitionedSuffix,
		MigrationFamilySQSPartitionedMessageVisibility: migrationSQSMsgVisPrefix + migrationSQSPartitionedSuffix,
		MigrationFamilySQSPartitionedMessageDedup:      migrationSQSMsgDedupPrefix + migrationSQSPartitionedSuffix,
		MigrationFamilySQSPartitionedMessageGroup:      migrationSQSMsgGroupPrefix + migrationSQSPartitionedSuffix,
		MigrationFamilySQSPartitionedMessageByAge:      migrationSQSMsgByAgePrefix + migrationSQSPartitionedSuffix,
		MigrationFamilyS3BucketMeta:                    s3keys.BucketMetaPrefix,
		MigrationFamilyS3BucketGeneration:              s3keys.BucketGenerationPrefix,
		MigrationFamilyS3ObjectManifest:                s3keys.ObjectManifestPrefix,
		MigrationFamilyS3UploadMeta:                    s3keys.UploadMetaPrefix,
		MigrationFamilyS3UploadPart:                    s3keys.UploadPartPrefix,
		MigrationFamilyS3Blob:                          s3keys.BlobPrefix,
		MigrationFamilyS3ChunkRef:                      s3keys.ChunkRefPrefix,
		MigrationFamilyS3GCUpload:                      s3keys.GCUploadPrefix,
		MigrationFamilyFilesystemChunk:                 string(fskeys.ChunkAllPrefix()),
		MigrationFamilyFilesystemUsage:                 string(fskeys.UsageRouteAllPrefix()),
	}

	for family, prefix := range required {
		bracket, ok := byFamily[family]
		require.True(t, ok, "missing family %d", family)
		require.Equal(t, uint64(family), bracket.BracketID)
		if family == MigrationFamilyS3BucketMeta || family == MigrationFamilyS3BucketGeneration {
			require.False(t, bracket.RequiresRouteKeyCheck)
			require.True(t, bracket.RequiresDecodedS3)
		} else {
			require.True(t, bracket.RequiresRouteKeyCheck)
			require.False(t, bracket.RequiresDecodedS3)
		}
		if family == MigrationFamilyUser {
			require.Equal(t, []byte("m"), bracket.Start)
			require.Equal(t, []byte("z"), bracket.End)
			require.True(t, bracket.ExcludeKnownInternal)
			continue
		}
		require.Equal(t, []byte(prefix), bracket.Start, "family %d start", family)
		require.Equal(t, prefixScanEnd([]byte(prefix)), bracket.End, "family %d end", family)
	}

	require.True(t, byFamily[MigrationFamilyTxnLock].DrainOnly)
	export, err := PlanExportBrackets([]byte("m"), []byte("z"))
	require.NoError(t, err)
	_, exportedLock := bracketsByFamily(export)[MigrationFamilyTxnLock]
	require.False(t, exportedLock, "txn locks are drain-only and must not be exported as data")
}

func TestPlanMigrationBracketsDisjointPrefixContainment(t *testing.T) {
	t.Parallel()

	brackets, err := PlanMigrationBrackets([]byte("m"), []byte("z"))
	require.NoError(t, err)
	byFamily := bracketsByFamily(brackets)

	listDelta := store.ListMetaDeltaKey([]byte("list"), 1, 0)
	require.True(t, byFamily[MigrationFamilyListMetaDelta].ContainsRawKey(listDelta))
	require.False(t, byFamily[MigrationFamilyListMeta].ContainsRawKey(listDelta))
	require.False(t, byFamily[MigrationFamilyLegacyListMetaDelta].ContainsRawKey(listDelta))

	legacyListDelta := legacyListMetaDeltaKey([]byte("legacy-list"), 2, 0)
	require.True(t, byFamily[MigrationFamilyLegacyListMetaDelta].ContainsRawKey(legacyListDelta))
	require.False(t, byFamily[MigrationFamilyListMeta].ContainsRawKey(legacyListDelta))
	require.False(t, byFamily[MigrationFamilyListMetaDelta].ContainsRawKey(legacyListDelta))

	listMetaWithDeltaLookingUserKey := store.ListMetaKey(deltaLookingListMetaUserKey([]byte("list"), 2, 0))
	require.False(t, byFamily[MigrationFamilyListMeta].ContainsRawKey(listMetaWithDeltaLookingUserKey))
	require.False(t, byFamily[MigrationFamilyListMetaDelta].ContainsRawKey(listMetaWithDeltaLookingUserKey))
	require.True(t, byFamily[MigrationFamilyLegacyListMetaDelta].ContainsRawKey(listMetaWithDeltaLookingUserKey))

	partitionedSQS := []byte(migrationSQSMsgDataPrefix + migrationSQSPartitionedSuffix + "queue|0|1|msg")
	require.True(t, byFamily[MigrationFamilySQSPartitionedMessageData].ContainsRawKey(partitionedSQS))
	require.False(t, byFamily[MigrationFamilySQSMessageData].ContainsRawKey(partitionedSQS))

	user := byFamily[MigrationFamilyUser]
	user.Start = nil
	user.End = nil
	require.False(t, user.ContainsRawKey(s3keys.ChunkRefKey("bucket", 1, "object", "upload", 1, 0)))
	require.False(t, user.ContainsRawKey(fskeys.ChunkKey(1, 2, 3)))
	require.False(t, user.ContainsRawKey(fskeys.UsageRouteKey(fskeys.ChunkRouteKey(1, 2))))
	for _, raw := range [][]byte{
		[]byte("!txn|foo"),
		[]byte("!stream|foo"),
		[]byte("!ddb|foo"),
		[]byte("!sqs|foo"),
		[]byte("!s3|foo"),
		[]byte("ordinary-user-key"),
	} {
		require.True(t, user.ContainsRawKey(raw), "raw user key %q must stay in familyUser", raw)
	}
	for _, raw := range [][]byte{
		[]byte(migrationTxnSuccessPrefix + "x"),
		[]byte(store.StreamMetaPrefix + "x"),
		[]byte(migrationDynamoItemPrefix + "x"),
		[]byte(migrationSQSMsgVisPrefix + "x"),
		[]byte(s3keys.ObjectManifestPrefix + "x"),
		[]byte(migrationRedisPrefix + "string|k"),
		[]byte(migrationHashPrefix + "meta|x"),
		fskeys.UsageRouteKey(fskeys.ChunkRouteKey(1, 2)),
	} {
		require.False(t, user.ContainsRawKey(raw), "concrete internal key %q must be excluded from familyUser", raw)
	}
}

func TestPlanMigrationBracketsNormalizesEmptyRouteEnd(t *testing.T) {
	t.Parallel()

	brackets, err := PlanMigrationBrackets([]byte("m"), []byte{})
	require.NoError(t, err)
	user := bracketsByFamily(brackets)[MigrationFamilyUser]
	require.Nil(t, user.End)
	require.True(t, user.ContainsRawKey([]byte("z")))
}

func TestSplitJobPlanNormalizesEmptySourceRouteEnd(t *testing.T) {
	t.Parallel()

	source := RouteDescriptor{
		RouteID: 9,
		Start:   []byte("a"),
		End:     []byte{},
		GroupID: 3,
		State:   RouteStateActive,
	}
	job := SplitJob{
		JobID:         1,
		SourceRouteID: source.RouteID,
		SplitKey:      []byte("m"),
		TargetGroupID: source.GroupID,
		Phase:         SplitJobPhasePlanned,
	}

	planned, err := InitializeSplitJobPlan(job, source, 1000)
	require.NoError(t, err)
	for _, progress := range planned.BracketProgress {
		if progress.Family != MigrationFamilyUser {
			continue
		}
		require.False(t, progress.Done)
		return
	}
	require.Fail(t, "missing user bracket progress")
}

func TestMigrationBracketContainsRoutedKeyForS3BucketAuxiliaryState(t *testing.T) {
	t.Parallel()

	brackets, err := PlanMigrationBrackets([]byte("m"), []byte("z"))
	require.NoError(t, err)
	byFamily := bracketsByFamily(brackets)

	// A slice strictly inside bucket-b's route interval. It overlaps the bucket
	// but does not contain the bucket's route start, so it is NOT the owner of
	// bucket-b's auxiliary rows -- the preceding slice is.
	//
	// This previously expected true, i.e. any overlapping slice claimed the
	// bucket's metadata. That rule cannot be right for a single row describing
	// the whole bucket: it cannot travel with a partial object range, and every
	// overlapping slice claiming it means the export duplicates it and CLEANUP
	// deletes rows a slice never exported. Ownership is point containment of the
	// bucket route start; see S3BucketAuxiliaryRouteSelected.
	routeStart := s3keys.RouteKey("bucket-b", 7, "a")
	routeEnd := s3keys.RouteKey("bucket-b", 7, "z")
	ownerStart := s3keys.RoutePrefixForBucketAnyGeneration("bucket-b")
	for _, tc := range []struct {
		name       string
		family     uint32
		key        []byte
		start, end []byte
		want       bool
	}{
		{name: "meta inside slice is not the owner", family: MigrationFamilyS3BucketMeta, key: s3keys.BucketMetaKey("bucket-b"), start: routeStart, end: routeEnd, want: false},
		{name: "generation inside slice is not the owner", family: MigrationFamilyS3BucketGeneration, key: s3keys.BucketGenerationKey("bucket-b"), start: routeStart, end: routeEnd, want: false},
		{name: "meta owning slice", family: MigrationFamilyS3BucketMeta, key: s3keys.BucketMetaKey("bucket-b"), start: ownerStart, end: routeEnd, want: true},
		{name: "generation owning slice", family: MigrationFamilyS3BucketGeneration, key: s3keys.BucketGenerationKey("bucket-b"), start: ownerStart, end: routeEnd, want: true},
		{name: "meta different bucket", family: MigrationFamilyS3BucketMeta, key: s3keys.BucketMetaKey("bucket-c"), start: routeStart, end: routeEnd, want: false},
		{name: "generation different bucket", family: MigrationFamilyS3BucketGeneration, key: s3keys.BucketGenerationKey("bucket-c"), start: routeStart, end: routeEnd, want: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := byFamily[tc.family].ContainsRoutedKey(tc.key, tc.start, tc.end, s3keys.ExtractRouteKey)
			require.Equal(t, tc.want, got)
		})
	}
}

func TestMigrationBracketContainsRoutedKeyForS3BucketRawRoute(t *testing.T) {
	t.Parallel()

	brackets, err := PlanMigrationBrackets([]byte("m"), []byte("z"))
	require.NoError(t, err)
	byFamily := bracketsByFamily(brackets)
	routeStart := []byte("!s3|")

	require.True(t, byFamily[MigrationFamilyS3BucketMeta].ContainsRoutedKey(
		s3keys.BucketMetaKey("bucket-b"), routeStart, nil, s3keys.ExtractRouteKey,
	))
	require.True(t, byFamily[MigrationFamilyS3BucketGeneration].ContainsRoutedKey(
		s3keys.BucketGenerationKey("bucket-b"), routeStart, nil, s3keys.ExtractRouteKey,
	))
}

func TestMigrationBracketContainsRoutedKeyUsesObjectRoutes(t *testing.T) {
	t.Parallel()

	brackets, err := PlanMigrationBrackets([]byte("m"), []byte("z"))
	require.NoError(t, err)
	manifest := bracketsByFamily(brackets)[MigrationFamilyS3ObjectManifest]

	key := s3keys.ObjectManifestKey("bucket-b", 7, "m")
	require.True(t, manifest.ContainsRoutedKey(
		key,
		s3keys.RouteKey("bucket-b", 7, "a"),
		s3keys.RouteKey("bucket-b", 7, "z"),
		s3keys.ExtractRouteKey,
	))
	require.False(t, manifest.ContainsRoutedKey(
		key,
		s3keys.RouteKey("bucket-c", 1, "a"),
		nil,
		s3keys.ExtractRouteKey,
	))
}

func TestMigrationBracketContainsRoutedKeyUsesS3ChunkRefRoutes(t *testing.T) {
	t.Parallel()

	brackets, err := PlanMigrationBrackets([]byte("m"), []byte("z"))
	require.NoError(t, err)
	chunkRef := bracketsByFamily(brackets)[MigrationFamilyS3ChunkRef]
	key := s3keys.ChunkRefKey("bucket-b", 7, "m", "upload", 1, 0)

	require.True(t, chunkRef.ContainsRoutedKey(
		key,
		s3keys.RouteKey("bucket-b", 7, "a"),
		s3keys.RouteKey("bucket-b", 7, "z"),
		s3keys.ExtractRouteKey,
	))
	require.False(t, chunkRef.ContainsRoutedKey(
		key,
		s3keys.RouteKey("bucket-c", 1, "a"),
		nil,
		s3keys.ExtractRouteKey,
	))
}

func TestMigrationBracketContainsRoutedKeyUsesFilesystemChunkRoutes(t *testing.T) {
	t.Parallel()

	brackets, err := PlanMigrationBrackets([]byte("m"), []byte("z"))
	require.NoError(t, err)
	chunk := bracketsByFamily(brackets)[MigrationFamilyFilesystemChunk]
	key := fskeys.ChunkKey(10, 20, 3)
	routeKey := fskeys.ChunkRouteKey(10, 20)

	require.True(t, chunk.ContainsRoutedKey(key, routeKey, prefixScanEnd(routeKey), fskeys.ExtractRouteKey))
	require.False(t, chunk.ContainsRoutedKey(
		key,
		fskeys.ChunkRouteKey(11, 20),
		nil,
		fskeys.ExtractRouteKey,
	))
}

func TestMigrationBracketContainsRoutedKeyUsesFilesystemUsageRoutes(t *testing.T) {
	t.Parallel()

	brackets, err := PlanMigrationBrackets([]byte("m"), []byte("z"))
	require.NoError(t, err)
	usage := bracketsByFamily(brackets)[MigrationFamilyFilesystemUsage]
	routeKey := fskeys.ChunkRouteKey(10, 20)
	key := fskeys.UsageRouteKey(routeKey)

	require.True(t, usage.ContainsRoutedKey(key, routeKey, prefixScanEnd(routeKey), fskeys.ExtractRouteKey))
	require.False(t, usage.ContainsRoutedKey(
		key,
		fskeys.ChunkRouteKey(11, 20),
		nil,
		fskeys.ExtractRouteKey,
	))
}

func TestMigrationBracketContainsRoutedKeyAcceptsEmptyLogicalRouteKey(t *testing.T) {
	t.Parallel()

	routeEnd := []byte{0x01}
	brackets, err := PlanMigrationBrackets(nil, routeEnd)
	require.NoError(t, err)
	hash := bracketsByFamily(brackets)[MigrationFamilyHash]
	rawKey := store.HashMetaKey(nil)

	require.True(t, hash.ContainsRoutedKey(
		rawKey,
		nil,
		routeEnd,
		store.ExtractHashUserKeyFromMeta,
	))
	require.False(t, hash.ContainsRoutedKey(
		rawKey,
		nil,
		routeEnd,
		func([]byte) []byte { return nil },
	))
}

func TestMigrationBracketContainsRoutedKeyUsesLegacyListDeltaUserKey(t *testing.T) {
	t.Parallel()

	brackets, err := PlanMigrationBrackets([]byte("a"), []byte("z"))
	require.NoError(t, err)
	legacy := bracketsByFamily(brackets)[MigrationFamilyLegacyListMetaDelta]
	raw := legacyListMetaDeltaKey([]byte("target-list"), 10, 0)
	value := store.MarshalListMetaDelta(store.ListMetaDelta{LenDelta: 1})

	require.True(t, legacy.ContainsRoutedVersion(
		raw,
		value,
		[]byte("target"),
		[]byte("target-list\x00"),
		store.ExtractListUserKey,
	))
	require.False(t, legacy.ContainsRoutedVersion(
		raw,
		value,
		[]byte("d|"),
		[]byte("d}"),
		store.ExtractListUserKey,
	))
	require.False(t, legacy.ContainsRoutedVersion(
		raw,
		value,
		[]byte("zzz"),
		nil,
		store.ExtractListUserKey,
	))
}

func TestMigrationBracketContainsRoutedKeyRoutesAmbiguousLegacyListMetaByBaseKey(t *testing.T) {
	t.Parallel()

	brackets, err := PlanMigrationBrackets([]byte("a"), []byte("z"))
	require.NoError(t, err)
	legacy := bracketsByFamily(brackets)[MigrationFamilyLegacyListMetaDelta]
	userKey := deltaLookingListMetaUserKey([]byte("target-list"), 10, 0)
	raw := store.ListMetaKey(userKey)
	value, err := store.MarshalListMeta(store.ListMeta{Head: 1, Tail: 2, Len: 1})
	require.NoError(t, err)

	require.True(t, legacy.ContainsRoutedVersion(
		raw,
		value,
		[]byte("d|"),
		[]byte("d}"),
		store.ExtractListUserKey,
	))
	require.False(t, legacy.ContainsRoutedVersion(
		raw,
		value,
		[]byte("zzz"),
		nil,
		store.ExtractListUserKey,
	))
}

func TestMigrationBracketContainsRoutedKeyRoutesAmbiguousListMetaTombstoneConservatively(t *testing.T) {
	t.Parallel()

	brackets, err := PlanMigrationBrackets([]byte("a"), []byte("z"))
	require.NoError(t, err)
	legacy := bracketsByFamily(brackets)[MigrationFamilyLegacyListMetaDelta]
	baseUserKey := deltaLookingListMetaUserKey([]byte("target-list"), 10, 0)
	raw := store.ListMetaKey(baseUserKey)

	require.True(t, legacy.ContainsRoutedVersion(
		raw,
		nil,
		[]byte("d|"),
		[]byte("d}"),
		store.ExtractListUserKey,
	))
	require.True(t, legacy.ContainsRoutedVersion(
		raw,
		nil,
		[]byte("target"),
		[]byte("target-list\x00"),
		store.ExtractListUserKey,
	))
	require.False(t, legacy.ContainsRoutedVersion(
		raw,
		nil,
		[]byte("zzz"),
		nil,
		store.ExtractListUserKey,
	))
}

func TestMigrationKnownInternalPrefixesAreConcreteOnly(t *testing.T) {
	t.Parallel()

	for _, raw := range [][]byte{
		[]byte(migrationTxnIntentPrefix + "k"),
		[]byte(migrationTxnSuccessPrefix + "k"),
		[]byte(migrationTxnBackupTimestampFloorKey),
		[]byte(store.ListClaimPrefix + "k"),
		[]byte(store.HashFieldPrefix + "k"),
		[]byte(store.StreamEntryPrefix + "k"),
		[]byte(migrationDynamoMetaPrefix + "t"),
		[]byte(migrationSQSQueueMetaPrefix + "q"),
		[]byte(s3keys.BlobPrefix + "b"),
		fskeys.UsageRouteKey(fskeys.ChunkRouteKey(1, 2)),
	} {
		require.True(t, IsMigrationKnownInternalKey(raw), "concrete internal key %q", raw)
	}

	for _, raw := range [][]byte{
		[]byte("!txn|foo"),
		[]byte("!txn|backup|customer"),
		[]byte("!stream|foo"),
		[]byte("!ddb|foo"),
		[]byte("!sqs|foo"),
		[]byte("!s3|foo"),
	} {
		require.False(t, IsMigrationKnownInternalKey(raw), "umbrella-looking user key %q", raw)
	}

	prefixes := MigrationKnownInternalPrefixes()
	require.NotEmpty(t, prefixes)
	prefixes[0][0] ^= 0xff
	require.False(t, bytes.Equal(prefixes[0], MigrationKnownInternalPrefixes()[0]), "prefix list must be cloned")
}

func TestMigrationStagedDataKeyRoundTrip(t *testing.T) {
	t.Parallel()

	raw := []byte("user|raw")
	key := MigrationStagedDataKey(42, raw)
	require.LessOrEqual(t, len(MigrationStagedDataKey(42, nil)), store.MaxSnapshotInternalKeyEnvelope)
	require.True(t, IsMigrationStagedDataKey(key))
	require.True(t, bytes.HasPrefix(key, MigrationStagedDataKeyPrefix(42)))
	require.False(t, IsMigrationStagedDataKey([]byte("!dist|migstage|short")))

	jobID, original, ok := MigrationStagedDataKeyParts(key)
	require.True(t, ok)
	require.Equal(t, uint64(42), jobID)
	require.Equal(t, []byte("user|raw"), original)

	raw[0] = 'X'
	original[0] = 'Y'
	jobID, original, ok = MigrationStagedDataKeyParts(key)
	require.True(t, ok)
	require.Equal(t, uint64(42), jobID)
	require.Equal(t, []byte("user|raw"), original)
}

func TestValidateMigrationRouteRangeRejectsReservedControlPrefixes(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name  string
		start []byte
		end   []byte
	}{
		{name: "exact dist", start: []byte("!dist|"), end: prefixScanEnd([]byte("!dist|"))},
		{name: "migstage", start: []byte("!migstage|"), end: prefixScanEnd([]byte("!migstage|"))},
		{name: "broad intersection", start: []byte("!"), end: []byte("~")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			err := ValidateMigrationRouteRange(tc.start, tc.end)
			require.True(t, errors.Is(err, ErrMigrationReservedRange), "got %v", err)
		})
	}

	require.NoError(t, ValidateMigrationRouteRange([]byte("m"), []byte("z")))
	err := ValidateMigrationRouteRange([]byte("z"), []byte("m"))
	require.True(t, errors.Is(err, ErrMigrationInvalidRoute), "got %v", err)
}

func TestSplitJobPlannerAndSameGroupNoop(t *testing.T) {
	t.Parallel()

	source := RouteDescriptor{
		RouteID: 9,
		Start:   []byte("a"),
		End:     []byte("z"),
		GroupID: 3,
		State:   RouteStateActive,
	}
	job := SplitJob{
		JobID:         1,
		SourceRouteID: source.RouteID,
		SplitKey:      []byte("m"),
		TargetGroupID: source.GroupID,
		Phase:         SplitJobPhasePlanned,
	}

	planned, err := InitializeSplitJobPlan(job, source, 1000)
	require.NoError(t, err)
	require.Equal(t, SplitJobPhasePlanned, planned.Phase)
	require.NotEmpty(t, planned.BracketProgress)
	require.Equal(t, int64(1000), planned.StartedAtMs)
	require.Equal(t, int64(1000), planned.UpdatedAtMs)
	for _, progress := range planned.BracketProgress {
		require.Equal(t, SplitJobExportPhaseBackfill, progress.ExportPhase)
		require.NotEqual(t, MigrationFamilyTxnLock, progress.Family)
	}

	done, err := AdvanceSameGroupNoop(job, source, 2000)
	require.NoError(t, err)
	require.Equal(t, SplitJobPhaseDone, done.Phase)
	require.True(t, done.TargetPromotionDone)
	require.Equal(t, uint64(2000), done.PromotionCompletedTS)
	require.Equal(t, int64(2000), done.TerminalAtMs)
	for _, progress := range done.BracketProgress {
		require.True(t, progress.Done)
	}

	crossGroup := job
	crossGroup.TargetGroupID = source.GroupID + 1
	_, err = AdvanceSameGroupNoop(crossGroup, source, 3000)
	require.True(t, errors.Is(err, ErrMigrationDataMoveRequired), "got %v", err)
}

func bracketsByFamily(brackets []MigrationBracket) map[uint32]MigrationBracket {
	out := make(map[uint32]MigrationBracket, len(brackets))
	for _, bracket := range brackets {
		out[bracket.Family] = bracket
	}
	return out
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

func legacyListMetaDeltaKey(userKey []byte, commitTS uint64, seqInTxn uint32) []byte {
	key := store.LegacyListMetaDeltaScanPrefix(userKey)
	var ts [8]byte
	binary.BigEndian.PutUint64(ts[:], commitTS)
	key = append(key, ts[:]...)
	var seq [4]byte
	binary.BigEndian.PutUint32(seq[:], seqInTxn)
	return append(key, seq[:]...)
}

// File chunk payloads live under !fs|chk| but route through a virtual
// !fs|route|chk| key. "!fs|chk|" sorts below "!fs|route|chk|", so a
// filesystem-chunk route's user bracket -- whose raw interval IS the virtual
// route range -- never reaches the payloads. Without a dedicated bracket the
// export found nothing, yet the migration completed and was promoted, losing
// every chunk of the moved files on a cross-group split.
func TestPlanMigrationBracketsCoversFilesystemChunkPayloads(t *testing.T) {
	t.Parallel()

	routeStart := fskeys.ChunkRouteKey(0, 1)
	routeEnd := fskeys.ChunkRouteKey(0, 9)
	brackets, err := PlanMigrationBrackets(routeStart, routeEnd)
	require.NoError(t, err)

	var chunk *MigrationBracket
	for i := range brackets {
		if brackets[i].Family == MigrationFamilyFilesystemChunk {
			chunk = &brackets[i]

			break
		}
	}
	require.NotNil(t, chunk, "the plan must carry a filesystem chunk bracket")
	require.Equal(t, fskeys.ChunkAllPrefix(), chunk.Start,
		"the bracket must scan the raw chunk prefix, not the virtual route range")
	require.True(t, chunk.RequiresRouteKeyCheck,
		"raw chunk keys must still be filtered through the logical route")

	// The gap this closes: the raw payload prefix sorts below the virtual route
	// interval, so the user bracket's raw range cannot reach it.
	require.Negative(t, bytes.Compare(fskeys.ChunkAllPrefix(), routeStart),
		"chunk payloads sort below the virtual route interval")
}

// Invariant: every family bracket's scan prefix must also be excluded from the
// user bracket. Both filters accept the same raw row otherwise -- the family
// bracket by prefix and the user bracket by normalized route key -- so the rows
// are exported and proposed through Raft twice under separate bracket IDs.
//
// This is written as an invariant rather than a per-family case because the
// filesystem chunk family was added without its exclusion and nothing caught it.
func TestEveryFamilyBracketPrefixIsExcludedFromUserBracket(t *testing.T) {
	t.Parallel()

	for _, bracket := range migrationFamilyBrackets() {
		if bracket.DrainOnly {
			continue
		}
		require.True(t, IsMigrationKnownInternalKey(bracket.Start),
			"family %d prefix %q must be in migrationInternalFamilyPrefixes, "+
				"otherwise the user bracket exports the same rows a second time",
			bracket.Family, bracket.Start)
	}
}

// Per-route usage counters live at !fs|usage|route|<encoded route> and
// normalize back to the embedded logical route key, so their raw key sits
// outside a user route's interval exactly like chunk payloads. Without a
// bracket the counter stayed on the source: after cutover the usage scan
// filtered that copy out because its logical owner had become the target,
// while target-side updates began from zero, so StatFS undercounted.
func TestPlanMigrationBracketsCoversFilesystemUsageCounters(t *testing.T) {
	t.Parallel()

	routeStart := []byte("a")
	routeEnd := []byte("z")
	brackets, err := PlanMigrationBrackets(routeStart, routeEnd)
	require.NoError(t, err)

	var usage *MigrationBracket
	for i := range brackets {
		if brackets[i].Family == MigrationFamilyFilesystemUsage {
			usage = &brackets[i]

			break
		}
	}
	require.NotNil(t, usage, "the plan must carry a filesystem usage bracket")
	require.Equal(t, fskeys.UsageRouteAllPrefix(), usage.Start,
		"the bracket must scan the raw usage prefix, not the logical route range")
	require.True(t, usage.RequiresRouteKeyCheck,
		"raw usage keys must still be filtered through their embedded route")

	// The gap this closes: the raw counter key sorts outside a user route
	// interval, so the user bracket cannot reach it.
	require.Negative(t, bytes.Compare(fskeys.UsageRouteAllPrefix(), routeStart),
		"usage counters sort below a user route interval")

	// And it must round-trip: a counter for a key inside the interval
	// normalizes back into that interval, so the route filter keeps it.
	counter := fskeys.UsageRouteKey([]byte("customers"))
	require.Equal(t, []byte("customers"), fskeys.ExtractRouteKey(counter))
}

// !s3|chunkblob| rows never travel with a migration. They are written outside
// Raft directly to the receiving node's Pebble and pulled by peers over
// S3BlobFetch, and they are content-addressed, so a single row backs every
// object whose chunk hashes the same -- objects that are not moving included.
// No bracket may claim them: the user bracket would export them by their raw
// digest route, which forges a replicated copy of deliberately unreplicated
// state and lets a later source cleanup delete blobs that unmigrated objects
// still dereference.
func TestPlanExportBracketsExcludesPeerLocalChunkBlobs(t *testing.T) {
	t.Parallel()

	var digest [32]byte
	digest[0] = 0xab
	blobKey := s3keys.ChunkBlobKey(digest)

	brackets, err := PlanExportBrackets([]byte(s3keys.ChunkBlobPrefix), []byte("!s4|"))
	require.NoError(t, err)
	require.NotEmpty(t, brackets)

	for _, bracket := range brackets {
		require.False(t, bracket.ContainsRawKey(blobKey),
			"family %d must not export peer-local chunk blob %q", bracket.Family, blobKey)
	}
	require.True(t, IsMigrationKnownInternalKey(blobKey))

	// The neighbouring replicated S3 families keep their own brackets, so the
	// exclusion is scoped to the blob payloads alone.
	refKey := []byte(s3keys.ChunkRefPrefix + "b|o|u|1|1")
	matched := false
	for _, bracket := range brackets {
		if bracket.Family == MigrationFamilyS3ChunkRef && bracket.ContainsRawKey(refKey) {
			matched = true
		}
	}
	require.True(t, matched, "chunkref stays owned by its own export bracket")
}
