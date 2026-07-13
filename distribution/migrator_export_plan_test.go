package distribution

import (
	"bytes"
	"testing"

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
		MigrationFamilyS3GCUpload:                      s3keys.GCUploadPrefix,
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

	listMetaWithDeltaLookingUserKey := store.ListMetaKey([]byte("d|list"))
	require.True(t, byFamily[MigrationFamilyListMeta].ContainsRawKey(listMetaWithDeltaLookingUserKey))
	require.False(t, byFamily[MigrationFamilyListMetaDelta].ContainsRawKey(listMetaWithDeltaLookingUserKey))

	partitionedSQS := []byte(migrationSQSMsgDataPrefix + migrationSQSPartitionedSuffix + "queue|0|1|msg")
	require.True(t, byFamily[MigrationFamilySQSPartitionedMessageData].ContainsRawKey(partitionedSQS))
	require.False(t, byFamily[MigrationFamilySQSMessageData].ContainsRawKey(partitionedSQS))

	user := byFamily[MigrationFamilyUser]
	user.Start = nil
	user.End = nil
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

func TestMigrationKnownInternalPrefixesAreConcreteOnly(t *testing.T) {
	t.Parallel()

	for _, raw := range [][]byte{
		[]byte(migrationTxnIntentPrefix + "k"),
		[]byte(migrationTxnSuccessPrefix + "k"),
		[]byte(store.ListClaimPrefix + "k"),
		[]byte(store.HashFieldPrefix + "k"),
		[]byte(store.StreamEntryPrefix + "k"),
		[]byte(migrationDynamoMetaPrefix + "t"),
		[]byte(migrationSQSQueueMetaPrefix + "q"),
		[]byte(s3keys.BlobPrefix + "b"),
	} {
		require.True(t, IsMigrationKnownInternalKey(raw), "concrete internal key %q", raw)
	}

	for _, raw := range [][]byte{
		[]byte("!txn|foo"),
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
