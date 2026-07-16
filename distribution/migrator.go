package distribution

import (
	"bytes"

	"github.com/bootjp/elastickv/internal/s3keys"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
)

const (
	MigrationFamilyUser uint32 = iota + 1
	MigrationFamilyTxnIntent
	MigrationFamilyTxnCommit
	MigrationFamilyTxnRollback
	MigrationFamilyTxnSuccess
	MigrationFamilyTxnMeta
	MigrationFamilyTxnLock
	MigrationFamilyListMeta
	MigrationFamilyListItem
	MigrationFamilyListMetaDelta
	MigrationFamilyListClaim
	MigrationFamilyRedisLegacy
	MigrationFamilyHash
	MigrationFamilySet
	MigrationFamilyZSet
	MigrationFamilyStreamMeta
	MigrationFamilyStreamEntry
	MigrationFamilyDynamoTableMeta
	MigrationFamilyDynamoTableGeneration
	MigrationFamilyDynamoItem
	MigrationFamilyDynamoGSI
	MigrationFamilySQSQueueMeta
	MigrationFamilySQSQueueGeneration
	MigrationFamilySQSQueueSequence
	MigrationFamilySQSQueueTombstone
	MigrationFamilySQSMessageData
	MigrationFamilySQSMessageVisibility
	MigrationFamilySQSMessageDedup
	MigrationFamilySQSMessageGroup
	MigrationFamilySQSMessageByAge
	MigrationFamilySQSPartitionedMessageData
	MigrationFamilySQSPartitionedMessageVisibility
	MigrationFamilySQSPartitionedMessageDedup
	MigrationFamilySQSPartitionedMessageGroup
	MigrationFamilySQSPartitionedMessageByAge
	MigrationFamilyS3BucketMeta
	MigrationFamilyS3BucketGeneration
	MigrationFamilyS3ObjectManifest
	MigrationFamilyS3UploadMeta
	MigrationFamilyS3UploadPart
	MigrationFamilyS3Blob
	MigrationFamilyS3GCUpload
	MigrationFamilyLegacyListMetaDelta
)

const (
	migrationTxnIntentPrefix   = "!txn|int|"
	migrationTxnCommitPrefix   = "!txn|cmt|"
	migrationTxnRollbackPrefix = "!txn|rb|"
	migrationTxnSuccessPrefix  = "!txn|ok|"
	migrationTxnMetaPrefix     = "!txn|meta|"
	migrationTxnLockPrefix     = "!txn|lock|"
	migrationRedisPrefix       = "!redis|"
	migrationHashPrefix        = "!hs|"
	migrationSetPrefix         = "!st|"
	migrationZSetPrefix        = "!zs|"
	migrationDynamoMetaPrefix  = "!ddb|meta|table|"
	migrationDynamoGenPrefix   = "!ddb|meta|gen|"
	migrationDynamoItemPrefix  = "!ddb|item|"
	migrationDynamoGSIPrefix   = "!ddb|gsi|"
)

const (
	migrationSQSQueueMetaPrefix      = "!sqs|queue|meta|"
	migrationSQSQueueGenPrefix       = "!sqs|queue|gen|"
	migrationSQSQueueSeqPrefix       = "!sqs|queue|seq|"
	migrationSQSQueueTombstonePrefix = "!sqs|queue|tombstone|"
	migrationSQSMsgDataPrefix        = "!sqs|msg|data|"
	migrationSQSMsgVisPrefix         = "!sqs|msg|vis|"
	migrationSQSMsgDedupPrefix       = "!sqs|msg|dedup|"
	migrationSQSMsgGroupPrefix       = "!sqs|msg|group|"
	migrationSQSMsgByAgePrefix       = "!sqs|msg|byage|"
	migrationSQSPartitionedSuffix    = "p|"
)

var (
	ErrMigrationReservedRange      = errors.New("migration range intersects reserved control prefix")
	ErrMigrationInvalidRoute       = errors.New("migration route is invalid")
	ErrMigrationDataMoveRequired   = errors.New("migration data move is not implemented")
	ErrMigrationSourceRouteChanged = errors.New("migration source route does not match split job")
)

var migrationReservedControlPrefixes = [][]byte{
	[]byte("!dist|"),
	[]byte("!migstage|"),
	[]byte("!migwrite|"),
	[]byte("!migfence|"),
}

var migrationInternalFamilyPrefixes = [][]byte{
	[]byte(migrationTxnLockPrefix),
	[]byte(migrationTxnIntentPrefix),
	[]byte(migrationTxnCommitPrefix),
	[]byte(migrationTxnRollbackPrefix),
	[]byte(migrationTxnSuccessPrefix),
	[]byte(migrationTxnMetaPrefix),
	[]byte(store.ListMetaDeltaPrefix),
	[]byte(store.LegacyListMetaDeltaPrefix),
	[]byte(store.ListClaimPrefix),
	[]byte(store.ListMetaPrefix),
	[]byte(store.ListItemPrefix),
	[]byte(migrationRedisPrefix),
	[]byte(migrationHashPrefix),
	[]byte(migrationSetPrefix),
	[]byte(migrationZSetPrefix),
	[]byte(store.StreamMetaPrefix),
	[]byte(store.StreamEntryPrefix),
	[]byte(migrationDynamoMetaPrefix),
	[]byte(migrationDynamoGenPrefix),
	[]byte(migrationDynamoItemPrefix),
	[]byte(migrationDynamoGSIPrefix),
	[]byte(migrationSQSQueueMetaPrefix),
	[]byte(migrationSQSQueueGenPrefix),
	[]byte(migrationSQSQueueSeqPrefix),
	[]byte(migrationSQSQueueTombstonePrefix),
	[]byte(migrationSQSMsgDataPrefix),
	[]byte(migrationSQSMsgVisPrefix),
	[]byte(migrationSQSMsgDedupPrefix),
	[]byte(migrationSQSMsgGroupPrefix),
	[]byte(migrationSQSMsgByAgePrefix),
	[]byte(s3keys.BucketMetaPrefix),
	[]byte(s3keys.BucketGenerationPrefix),
	[]byte(s3keys.ObjectManifestPrefix),
	[]byte(s3keys.UploadMetaPrefix),
	[]byte(s3keys.UploadPartPrefix),
	[]byte(s3keys.BlobPrefix),
	[]byte(s3keys.GCUploadPrefix),
}

// MigrationBracket is a raw MVCC export or drain slice used by the migrator.
type MigrationBracket struct {
	BracketID             uint64
	Family                uint32
	Start                 []byte
	End                   []byte
	ExcludePrefixes       [][]byte
	ExcludeKnownInternal  bool
	DrainOnly             bool
	RequiresRouteKeyCheck bool
	RequiresDecodedS3     bool
}

// PlanMigrationBrackets returns the full M2 migration plan, including the
// drain-only transaction lock bracket. Data export callers should use
// PlanExportBrackets, which omits drain-only control state.
func PlanMigrationBrackets(routeStart, routeEnd []byte) ([]MigrationBracket, error) {
	routeEnd = normalizeMigrationRouteEnd(routeEnd)
	if err := ValidateMigrationRouteRange(routeStart, routeEnd); err != nil {
		return nil, err
	}

	brackets := []MigrationBracket{
		{
			BracketID:             uint64(MigrationFamilyUser),
			Family:                MigrationFamilyUser,
			Start:                 CloneBytes(routeStart),
			End:                   CloneBytes(routeEnd),
			ExcludeKnownInternal:  true,
			RequiresRouteKeyCheck: true,
		},
	}
	brackets = append(brackets, migrationFamilyBrackets()...)
	return brackets, nil
}

// PlanExportBrackets returns the data-copy bracket plan. Intent locks are
// deliberately absent because the source drains them route-faithfully before
// cutover and the target must not materialize in-flight intents as data.
func PlanExportBrackets(routeStart, routeEnd []byte) ([]MigrationBracket, error) {
	brackets, err := PlanMigrationBrackets(routeStart, routeEnd)
	if err != nil {
		return nil, err
	}
	out := make([]MigrationBracket, 0, len(brackets))
	for _, bracket := range brackets {
		if bracket.DrainOnly {
			continue
		}
		out = append(out, bracket)
	}
	return out, nil
}

// SplitJobBracketProgressForPlan creates durable per-bracket resume state for
// a SplitJob phase.
func SplitJobBracketProgressForPlan(brackets []MigrationBracket, phase SplitJobExportPhase) []SplitJobBracketProgress {
	out := make([]SplitJobBracketProgress, 0, len(brackets))
	for _, bracket := range brackets {
		if bracket.DrainOnly {
			continue
		}
		out = append(out, SplitJobBracketProgress{
			BracketID:   bracket.BracketID,
			Family:      bracket.Family,
			ExportPhase: phase,
		})
	}
	return out
}

// ContainsRawKey reports whether rawKey is inside the bracket's raw scan
// interval after applying bracket-local exclusions. Route ownership still
// requires the caller's RouteKeyFilter for every bracket.
func (b MigrationBracket) ContainsRawKey(rawKey []byte) bool {
	if bytes.Compare(rawKey, b.Start) < 0 {
		return false
	}
	if len(b.End) > 0 && bytes.Compare(rawKey, b.End) >= 0 {
		return false
	}
	if !b.containsFamilyShape(rawKey) {
		return false
	}
	if b.ExcludeKnownInternal && IsMigrationKnownInternalKey(rawKey) {
		return false
	}
	return !hasAnyPrefix(rawKey, b.ExcludePrefixes)
}

// ContainsRoutedKey applies both the bracket's raw family interval and its
// route ownership predicate. S3 bucket-level auxiliary rows do not encode an
// object route key, so they are matched by bucket route-prefix intersection.
func (b MigrationBracket) ContainsRoutedKey(rawKey, routeStart, routeEnd []byte, routeKey func([]byte) []byte) bool {
	return b.ContainsRoutedVersion(rawKey, nil, routeStart, routeEnd, routeKey)
}

// ContainsRoutedVersion is the value-aware variant of ContainsRoutedKey. It is
// needed for legacy list metadata because old delta keys overlap byte-for-byte
// with base metadata keys whose user key begins with "d|".
func (b MigrationBracket) ContainsRoutedVersion(rawKey, value, routeStart, routeEnd []byte, routeKey func([]byte) []byte) bool {
	if !b.ContainsRawKey(rawKey) {
		return false
	}
	routeEnd = normalizeMigrationRouteEnd(routeEnd)
	if b.RequiresDecodedS3 {
		return b.containsDecodedS3Route(rawKey, routeStart, routeEnd)
	}
	if b.Family == MigrationFamilyLegacyListMetaDelta {
		return b.containsLegacyListMetaDeltaRoute(rawKey, value, routeStart, routeEnd)
	}
	if !b.RequiresRouteKeyCheck {
		return true
	}
	if routeKey == nil {
		return false
	}
	return routeKeyInRange(routeKey(rawKey), routeStart, routeEnd)
}

func (b MigrationBracket) containsLegacyListMetaDeltaRoute(rawKey, value, routeStart, routeEnd []byte) bool {
	if value != nil && !store.IsListMetaDeltaValue(value) {
		return routeKeyInRange(store.ExtractListUserKey(rawKey), routeStart, routeEnd)
	}
	return routeKeyInRange(store.ExtractLegacyListUserKeyFromDelta(rawKey), routeStart, routeEnd)
}

func (b MigrationBracket) containsFamilyShape(rawKey []byte) bool {
	switch b.Family {
	case MigrationFamilyListMetaDelta:
		return store.ExtractListUserKeyFromDelta(rawKey) != nil
	case MigrationFamilyLegacyListMetaDelta:
		return store.ExtractLegacyListUserKeyFromDelta(rawKey) != nil
	case MigrationFamilyListMeta:
		return store.ExtractListUserKeyFromDelta(rawKey) == nil &&
			store.ExtractLegacyListUserKeyFromDelta(rawKey) == nil
	default:
		return true
	}
}

func (b MigrationBracket) containsDecodedS3Route(rawKey, routeStart, routeEnd []byte) bool {
	bucket, ok := b.decodedS3Bucket(rawKey)
	if !ok {
		return false
	}
	if routeKeyInRange(rawKey, routeStart, routeEnd) {
		return true
	}
	bucketRouteStart := s3keys.RoutePrefixForBucketAnyGeneration(bucket)
	return rangesIntersect(routeStart, routeEnd, bucketRouteStart, prefixScanEnd(bucketRouteStart))
}

func (b MigrationBracket) decodedS3Bucket(rawKey []byte) (string, bool) {
	switch b.Family {
	case MigrationFamilyS3BucketMeta:
		return s3keys.ParseBucketMetaKey(rawKey)
	case MigrationFamilyS3BucketGeneration:
		return s3keys.ParseBucketGenerationKey(rawKey)
	default:
		return "", false
	}
}

func routeKeyInRange(routeKey, routeStart, routeEnd []byte) bool {
	if routeKey == nil {
		return false
	}
	if bytes.Compare(routeKey, routeStart) < 0 {
		return false
	}
	return len(routeEnd) == 0 || bytes.Compare(routeKey, routeEnd) < 0
}

// InitializeSplitJobPlan validates the source route and seeds the job's
// bracket progress for the moving right child [SplitKey, source.End).
func InitializeSplitJobPlan(job SplitJob, source RouteDescriptor, nowMs int64) (SplitJob, error) {
	if err := validateSplitJobSource(job, source); err != nil {
		return SplitJob{}, err
	}
	routeStart := CloneBytes(job.SplitKey)
	routeEnd := CloneBytes(normalizeMigrationRouteEnd(source.End))
	brackets, err := PlanExportBrackets(routeStart, routeEnd)
	if err != nil {
		return SplitJob{}, err
	}

	out := CloneSplitJob(job)
	if out.Phase == SplitJobPhaseNone {
		out.Phase = SplitJobPhasePlanned
	}
	if len(out.BracketProgress) == 0 {
		out.BracketProgress = SplitJobBracketProgressForPlan(brackets, SplitJobExportPhaseBackfill)
	}
	if out.StartedAtMs == 0 {
		out.StartedAtMs = nowMs
	}
	out.UpdatedAtMs = nowMs
	return out, nil
}

// AdvanceSameGroupNoop completes the PR4 same-group path without attempting
// data movement. Cross-group data copy is added by later M2 PRs.
func AdvanceSameGroupNoop(job SplitJob, source RouteDescriptor, nowMs int64) (SplitJob, error) {
	planned, err := InitializeSplitJobPlan(job, source, nowMs)
	if err != nil {
		return SplitJob{}, err
	}
	if planned.TargetGroupID != source.GroupID {
		return SplitJob{}, errors.WithStack(ErrMigrationDataMoveRequired)
	}

	planned.Phase = SplitJobPhaseDone
	planned.TargetPromotionDone = true
	planned.PromotionCompletedTS = migrationWallMillisToUint64(nowMs)
	planned.UpdatedAtMs = nowMs
	planned.TerminalAtMs = nowMs
	for i := range planned.BracketProgress {
		planned.BracketProgress[i].Done = true
	}
	return planned, nil
}

// MigrationKnownInternalPrefixes returns the concrete internal data/control
// prefixes that the user bracket must exclude. It intentionally does not
// include broad umbrellas such as !txn|, !ddb|, !sqs|, !s3|, or !stream|.
func MigrationKnownInternalPrefixes() [][]byte {
	return cloneByteSlices(migrationInternalFamilyPrefixes)
}

// IsMigrationKnownInternalKey reports whether a raw key belongs to a concrete
// internal family owned by an explicit export bracket or by the txn-lock drain.
func IsMigrationKnownInternalKey(key []byte) bool {
	return hasAnyPrefix(key, migrationInternalFamilyPrefixes)
}

// ValidateMigrationRouteRange rejects route intervals that intersect reserved
// distribution/migration control namespaces.
func ValidateMigrationRouteRange(routeStart, routeEnd []byte) error {
	if len(routeEnd) > 0 && bytes.Compare(routeStart, routeEnd) >= 0 {
		return errors.WithStack(ErrMigrationInvalidRoute)
	}
	for _, prefix := range migrationReservedControlPrefixes {
		if rangesIntersect(routeStart, routeEnd, prefix, prefixScanEnd(prefix)) {
			return errors.WithStack(ErrMigrationReservedRange)
		}
	}
	return nil
}

func migrationFamilyBrackets() []MigrationBracket {
	defs := []struct {
		family          uint32
		prefix          string
		drainOnly       bool
		excludePrefixes []string
	}{
		{family: MigrationFamilyTxnLock, prefix: migrationTxnLockPrefix, drainOnly: true},
		{family: MigrationFamilyTxnIntent, prefix: migrationTxnIntentPrefix},
		{family: MigrationFamilyTxnCommit, prefix: migrationTxnCommitPrefix},
		{family: MigrationFamilyTxnRollback, prefix: migrationTxnRollbackPrefix},
		{family: MigrationFamilyTxnSuccess, prefix: migrationTxnSuccessPrefix},
		{family: MigrationFamilyTxnMeta, prefix: migrationTxnMetaPrefix},
		{family: MigrationFamilyListMetaDelta, prefix: store.ListMetaDeltaPrefix},
		{family: MigrationFamilyLegacyListMetaDelta, prefix: store.LegacyListMetaDeltaPrefix},
		{family: MigrationFamilyListClaim, prefix: store.ListClaimPrefix},
		{family: MigrationFamilyListMeta, prefix: store.ListMetaPrefix},
		{family: MigrationFamilyListItem, prefix: store.ListItemPrefix},
		{family: MigrationFamilyRedisLegacy, prefix: migrationRedisPrefix},
		{family: MigrationFamilyHash, prefix: migrationHashPrefix},
		{family: MigrationFamilySet, prefix: migrationSetPrefix},
		{family: MigrationFamilyZSet, prefix: migrationZSetPrefix},
		{family: MigrationFamilyStreamMeta, prefix: store.StreamMetaPrefix},
		{family: MigrationFamilyStreamEntry, prefix: store.StreamEntryPrefix},
		{family: MigrationFamilyDynamoTableMeta, prefix: migrationDynamoMetaPrefix},
		{family: MigrationFamilyDynamoTableGeneration, prefix: migrationDynamoGenPrefix},
		{family: MigrationFamilyDynamoItem, prefix: migrationDynamoItemPrefix},
		{family: MigrationFamilyDynamoGSI, prefix: migrationDynamoGSIPrefix},
		{family: MigrationFamilySQSQueueMeta, prefix: migrationSQSQueueMetaPrefix},
		{family: MigrationFamilySQSQueueGeneration, prefix: migrationSQSQueueGenPrefix},
		{family: MigrationFamilySQSQueueSequence, prefix: migrationSQSQueueSeqPrefix},
		{family: MigrationFamilySQSQueueTombstone, prefix: migrationSQSQueueTombstonePrefix},
		{family: MigrationFamilySQSMessageData, prefix: migrationSQSMsgDataPrefix, excludePrefixes: []string{migrationSQSMsgDataPrefix + migrationSQSPartitionedSuffix}},
		{family: MigrationFamilySQSMessageVisibility, prefix: migrationSQSMsgVisPrefix, excludePrefixes: []string{migrationSQSMsgVisPrefix + migrationSQSPartitionedSuffix}},
		{family: MigrationFamilySQSMessageDedup, prefix: migrationSQSMsgDedupPrefix, excludePrefixes: []string{migrationSQSMsgDedupPrefix + migrationSQSPartitionedSuffix}},
		{family: MigrationFamilySQSMessageGroup, prefix: migrationSQSMsgGroupPrefix, excludePrefixes: []string{migrationSQSMsgGroupPrefix + migrationSQSPartitionedSuffix}},
		{family: MigrationFamilySQSMessageByAge, prefix: migrationSQSMsgByAgePrefix, excludePrefixes: []string{migrationSQSMsgByAgePrefix + migrationSQSPartitionedSuffix}},
		{family: MigrationFamilySQSPartitionedMessageData, prefix: migrationSQSMsgDataPrefix + migrationSQSPartitionedSuffix},
		{family: MigrationFamilySQSPartitionedMessageVisibility, prefix: migrationSQSMsgVisPrefix + migrationSQSPartitionedSuffix},
		{family: MigrationFamilySQSPartitionedMessageDedup, prefix: migrationSQSMsgDedupPrefix + migrationSQSPartitionedSuffix},
		{family: MigrationFamilySQSPartitionedMessageGroup, prefix: migrationSQSMsgGroupPrefix + migrationSQSPartitionedSuffix},
		{family: MigrationFamilySQSPartitionedMessageByAge, prefix: migrationSQSMsgByAgePrefix + migrationSQSPartitionedSuffix},
		{family: MigrationFamilyS3BucketMeta, prefix: s3keys.BucketMetaPrefix},
		{family: MigrationFamilyS3BucketGeneration, prefix: s3keys.BucketGenerationPrefix},
		{family: MigrationFamilyS3ObjectManifest, prefix: s3keys.ObjectManifestPrefix},
		{family: MigrationFamilyS3UploadMeta, prefix: s3keys.UploadMetaPrefix},
		{family: MigrationFamilyS3UploadPart, prefix: s3keys.UploadPartPrefix},
		{family: MigrationFamilyS3Blob, prefix: s3keys.BlobPrefix},
		{family: MigrationFamilyS3GCUpload, prefix: s3keys.GCUploadPrefix},
	}

	out := make([]MigrationBracket, 0, len(defs))
	for _, def := range defs {
		start := []byte(def.prefix)
		requiresRouteKeyCheck, requiresDecodedS3 := migrationBracketRouteCheck(def.family)
		out = append(out, MigrationBracket{
			BracketID:             uint64(def.family),
			Family:                def.family,
			Start:                 CloneBytes(start),
			End:                   prefixScanEnd(start),
			ExcludePrefixes:       stringsToByteSlices(def.excludePrefixes),
			DrainOnly:             def.drainOnly,
			RequiresRouteKeyCheck: requiresRouteKeyCheck,
			RequiresDecodedS3:     requiresDecodedS3,
		})
	}
	return out
}

func migrationBracketRouteCheck(family uint32) (requiresRouteKeyCheck bool, requiresDecodedS3 bool) {
	switch family {
	case MigrationFamilyS3BucketMeta, MigrationFamilyS3BucketGeneration:
		return false, true
	default:
		return true, false
	}
}

func normalizeMigrationRouteEnd(routeEnd []byte) []byte {
	if len(routeEnd) == 0 {
		return nil
	}
	return routeEnd
}

func stringsToByteSlices(in []string) [][]byte {
	if len(in) == 0 {
		return nil
	}
	out := make([][]byte, len(in))
	for i := range in {
		out[i] = []byte(in[i])
	}
	return out
}

func migrationWallMillisToUint64(ms int64) uint64 {
	if ms <= 0 {
		return 0
	}
	return uint64(ms)
}

func validateSplitJobSource(job SplitJob, source RouteDescriptor) error {
	if job.SourceRouteID != source.RouteID {
		return errors.WithStack(ErrMigrationSourceRouteChanged)
	}
	if len(job.SplitKey) == 0 {
		return errors.WithStack(ErrMigrationInvalidRoute)
	}
	if bytes.Compare(job.SplitKey, source.Start) <= 0 {
		return errors.WithStack(ErrMigrationInvalidRoute)
	}
	if len(source.End) > 0 && bytes.Compare(job.SplitKey, source.End) >= 0 {
		return errors.WithStack(ErrMigrationInvalidRoute)
	}
	return nil
}

func rangesIntersect(aStart, aEnd, bStart, bEnd []byte) bool {
	if len(aEnd) > 0 && bytes.Compare(aEnd, bStart) <= 0 {
		return false
	}
	if len(bEnd) > 0 && bytes.Compare(bEnd, aStart) <= 0 {
		return false
	}
	return true
}

func hasAnyPrefix(key []byte, prefixes [][]byte) bool {
	for _, prefix := range prefixes {
		if bytes.HasPrefix(key, prefix) {
			return true
		}
	}
	return false
}

func cloneByteSlices(in [][]byte) [][]byte {
	out := make([][]byte, len(in))
	for i := range in {
		out[i] = CloneBytes(in[i])
	}
	return out
}
