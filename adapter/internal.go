package adapter

import (
	"bytes"
	"context"
	"os"
	"strings"
	"time"

	"github.com/bootjp/elastickv/distribution"
	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/bootjp/elastickv/internal/s3keys"
	"github.com/bootjp/elastickv/kv"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	// MigrationImportOpcodeEnv enables the internal staged import opcode after
	// every voter is running a compatible build.
	MigrationImportOpcodeEnv = "ELASTICKV_ENABLE_MIGRATION_IMPORT_OPCODE"
	// MigrationPromoteOpcodeEnv enables the internal staged promotion opcode
	// after every voter is running a compatible build.
	MigrationPromoteOpcodeEnv = "ELASTICKV_ENABLE_MIGRATION_PROMOTE_OPCODE"
	// MigrationCleanupOpcodeEnv enables the internal migration cleanup opcode
	// after every voter is running a compatible build.
	MigrationCleanupOpcodeEnv = "ELASTICKV_ENABLE_MIGRATION_CLEANUP_OPCODE"
)

type InternalOption func(*Internal)

func WithInternalTimestampAllocator(alloc kv.TimestampAllocator) InternalOption {
	return func(i *Internal) {
		i.tsAllocator = alloc
	}
}

func WithInternalStore(st store.MVCCStore) InternalOption {
	return func(i *Internal) {
		i.store = st
	}
}

func WithInternalMigrationProposer(proposer raftengine.Proposer) InternalOption {
	return func(i *Internal) {
		i.migrationProposer = proposer
	}
}

func WithInternalMigrationImportGate(gate func(context.Context) error) InternalOption {
	return func(i *Internal) {
		i.migrationImportGate = gate
	}
}

func WithInternalMigrationPromoteGate(gate func(context.Context) error) InternalOption {
	return func(i *Internal) {
		i.migrationPromoteGate = gate
	}
}

func WithInternalMigrationCleanupGate(gate func(context.Context) error) InternalOption {
	return func(i *Internal) {
		i.migrationCleanupGate = gate
	}
}

func WithInternalRouteEngine(engine *distribution.Engine) InternalOption {
	return func(i *Internal) {
		i.routeEngine = engine
	}
}

func WithInternalActiveTimestampTracker(tracker *kv.ActiveTimestampTracker) InternalOption {
	return func(i *Internal) {
		i.readTracker = tracker
	}
}

func WithInternalMigrationExportRouting(groupID uint64, resolver kv.PartitionResolver) InternalOption {
	return func(i *Internal) {
		i.migrationExportGroupID = groupID
		i.migrationExportResolver = resolver
	}
}

func NewInternalWithEngine(txm kv.Transactional, leader raftengine.LeaderView, clock *kv.HLC, relay *RedisPubSubRelay, opts ...InternalOption) *Internal {
	i := &Internal{
		leader:               leader,
		transactionManager:   txm,
		clock:                clock,
		relay:                relay,
		migrationImportGate:  defaultMigrationImportGate,
		migrationPromoteGate: defaultMigrationPromoteGate,
		migrationCleanupGate: defaultMigrationCleanupGate,
	}
	for _, opt := range opts {
		opt(i)
	}
	return i
}

type Internal struct {
	leader                  raftengine.LeaderView
	transactionManager      kv.Transactional
	clock                   *kv.HLC
	tsAllocator             kv.TimestampAllocator
	relay                   *RedisPubSubRelay
	store                   store.MVCCStore
	migrationProposer       raftengine.Proposer
	migrationImportGate     func(context.Context) error
	migrationPromoteGate    func(context.Context) error
	migrationCleanupGate    func(context.Context) error
	routeEngine             *distribution.Engine
	readTracker             *kv.ActiveTimestampTracker
	migrationExportGroupID  uint64
	migrationExportResolver kv.PartitionResolver

	pb.UnimplementedInternalServer
}

var _ pb.InternalServer = (*Internal)(nil)

var ErrNotLeader = errors.New("not leader")
var ErrLeaderNotFound = errors.New("leader not found")
var ErrTxnTimestampOverflow = errors.New("txn timestamp overflow")

const (
	defaultMigrationExportChunkBytes  = 4 << 20
	defaultMigrationExportScanFactor  = 4
	defaultMigrationExportMaxVersions = 1024
)

func (i *Internal) Forward(ctx context.Context, req *pb.ForwardRequest) (*pb.ForwardResponse, error) {
	if i.leader == nil || i.leader.State() != raftengine.StateLeader {
		return nil, errors.WithStack(ErrNotLeader)
	}
	if err := i.leader.VerifyLeader(ctx); err != nil {
		return nil, errors.WithStack(ErrNotLeader)
	}

	commitTS, err := i.stampTimestamps(ctx, req)
	if err != nil {
		return &pb.ForwardResponse{
			Success:     false,
			CommitIndex: 0,
		}, errors.WithStack(err)
	}

	r, err := i.transactionManager.Commit(ctx, req.Requests)
	if err != nil {
		return &pb.ForwardResponse{
			Success:     false,
			CommitIndex: 0,
		}, errors.WithStack(err)
	}

	return &pb.ForwardResponse{
		Success:     true,
		CommitIndex: r.CommitIndex,
		CommitTs:    commitTS,
	}, nil
}

func (i *Internal) ProbeMigrationState(ctx context.Context, req *pb.ProbeMigrationStateRequest) (*pb.ProbeMigrationStateResponse, error) {
	if req == nil || req.GetJobId() == 0 {
		return nil, errors.WithStack(status.Error(codes.InvalidArgument, "migration probe job_id is required"))
	}
	return i.probeMigrationStateKind(ctx, req)
}

func (i *Internal) probeMigrationStateKind(ctx context.Context, req *pb.ProbeMigrationStateRequest) (*pb.ProbeMigrationStateResponse, error) {
	switch req.GetKind() {
	case pb.MigrationStateProbeKind_MIGRATION_STATE_PROBE_KIND_CONTROL_APPLIED:
		return i.probeMigrationControl(ctx, req)
	case pb.MigrationStateProbeKind_MIGRATION_STATE_PROBE_KIND_TARGET_DESCRIPTOR_CLEARED:
		return i.probeMigrationRoute(req, true), nil
	case pb.MigrationStateProbeKind_MIGRATION_STATE_PROBE_KIND_SOURCE_ROUTE_REMOVED:
		return i.probeMigrationRoute(req, false), nil
	case pb.MigrationStateProbeKind_MIGRATION_STATE_PROBE_KIND_SOURCE_READ_DRAINED:
		return &pb.ProbeMigrationStateResponse{
			Ready: time.Now().UnixMilli() >= req.GetReadDrainNotBeforeMs() &&
				(i.readTracker == nil || i.readTracker.Oldest() == 0),
			CatalogVersion: i.migrationCatalogVersion(),
		}, nil
	case pb.MigrationStateProbeKind_MIGRATION_STATE_PROBE_KIND_METADATA_CLEARED:
		return i.probeMigrationMetadataCleared(ctx, req.GetJobId())
	case pb.MigrationStateProbeKind_MIGRATION_STATE_PROBE_KIND_UNSPECIFIED:
		return nil, errors.WithStack(status.Error(codes.InvalidArgument, "migration probe kind is required"))
	default:
		return nil, errors.WithStack(status.Error(codes.InvalidArgument, "unknown migration probe kind"))
	}
}

func (i *Internal) IssueMigrationTimestamp(ctx context.Context, _ *pb.IssueMigrationTimestampRequest) (*pb.IssueMigrationTimestampResponse, error) {
	if err := i.verifyInternalLeaderApplied(ctx); err != nil {
		return nil, err
	}
	if i.store == nil {
		return nil, errors.WithStack(status.Error(codes.FailedPrecondition, "migration timestamp store is not configured"))
	}
	lastCommitTS := i.store.LastCommitTS()
	ts, err := i.nextTimestampAfter(ctx, lastCommitTS, "issue migration timestamp")
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return &pb.IssueMigrationTimestampResponse{Timestamp: ts, LastCommitTs: lastCommitTS}, nil
}

func (i *Internal) probeMigrationControl(ctx context.Context, req *pb.ProbeMigrationStateRequest) (*pb.ProbeMigrationStateResponse, error) {
	reader, ok := i.store.(store.MigrationTargetReadinessReader)
	if !ok {
		return nil, errors.WithStack(status.Error(codes.FailedPrecondition, "migration readiness reader is not configured"))
	}
	states, err := reader.MigrationTargetReadinessStates(ctx)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	for _, state := range states {
		if state.JobID != req.GetJobId() {
			continue
		}
		ready := migrationControlMatches(state, req)
		return &pb.ProbeMigrationStateResponse{Ready: ready, CatalogVersion: i.migrationCatalogVersion(), MinAdmittedTs: state.MinAdmittedTS}, nil
	}
	return &pb.ProbeMigrationStateResponse{CatalogVersion: i.migrationCatalogVersion()}, nil
}

func migrationControlMatches(state store.TargetStagedReadinessState, req *pb.ProbeMigrationStateRequest) bool {
	return state.Armed &&
		bytes.Equal(state.RouteStart, req.GetRouteStart()) &&
		bytes.Equal(state.RouteEnd, normalizeOptionalEnd(req.GetRouteEnd())) &&
		state.ExpectedCutoverVersion == req.GetExpectedCatalogVersion() &&
		state.MigrationJobID == req.GetMigrationJobId() &&
		state.MinWriteTSExclusive == req.GetMinWriteTsExclusive() &&
		state.SourceWriteFence == req.GetSourceWriteFence() &&
		state.SourceReadFence == req.GetSourceReadFence() &&
		state.TrackWrites == req.GetTrackWrites() &&
		state.RetentionPinTS == req.GetRetentionPinTs()
}

func (i *Internal) probeMigrationRoute(req *pb.ProbeMigrationStateRequest, requireCleared bool) *pb.ProbeMigrationStateResponse {
	version := i.migrationCatalogVersion()
	if i.routeEngine == nil || version < req.GetExpectedCatalogVersion() {
		return &pb.ProbeMigrationStateResponse{CatalogVersion: version}
	}
	route, found := i.routeEngine.GetRoute(req.GetRouteStart())
	ready := found && route.GroupID == req.GetExpectedGroupId() &&
		bytes.Equal(route.Start, req.GetRouteStart()) &&
		bytes.Equal(route.End, normalizeOptionalEnd(req.GetRouteEnd()))
	if requireCleared {
		ready = ready && !route.StagedVisibilityActive && route.MigrationJobID == 0 &&
			route.MinWriteTSExclusive == req.GetMinWriteTsExclusive()
	}
	return &pb.ProbeMigrationStateResponse{Ready: ready, CatalogVersion: version}
}

func (i *Internal) probeMigrationMetadataCleared(ctx context.Context, jobID uint64) (*pb.ProbeMigrationStateResponse, error) {
	readiness, ok := i.store.(store.MigrationTargetReadinessReader)
	if !ok {
		return nil, errors.WithStack(status.Error(codes.FailedPrecondition, "migration readiness reader is not configured"))
	}
	states, err := readiness.MigrationTargetReadinessStates(ctx)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	for _, state := range states {
		if state.JobID == jobID {
			return &pb.ProbeMigrationStateResponse{CatalogVersion: i.migrationCatalogVersion()}, nil
		}
	}
	if promotion, ok := i.store.(store.MigrationPromotionStateReader); ok {
		_, found, err := promotion.MigrationPromotionState(ctx, jobID)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		if found {
			return &pb.ProbeMigrationStateResponse{CatalogVersion: i.migrationCatalogVersion()}, nil
		}
	}
	return &pb.ProbeMigrationStateResponse{Ready: true, CatalogVersion: i.migrationCatalogVersion()}, nil
}

func (i *Internal) migrationCatalogVersion() uint64 {
	if i.routeEngine == nil {
		return 0
	}
	return i.routeEngine.Version()
}

func normalizeOptionalEnd(end []byte) []byte {
	if len(end) == 0 {
		return nil
	}
	return end
}

func (i *Internal) RelayPublish(_ context.Context, req *pb.RelayPublishRequest) (*pb.RelayPublishResponse, error) {
	if req == nil || i.relay == nil {
		return &pb.RelayPublishResponse{}, nil
	}
	return &pb.RelayPublishResponse{
		Subscribers: i.relay.Publish(req.Channel, req.Message),
	}, nil
}

func (i *Internal) ExportRangeVersions(req *pb.ExportRangeVersionsRequest, stream pb.Internal_ExportRangeVersionsServer) error {
	if err := i.validateExportRangeVersionsRequest(req); err != nil {
		return err
	}
	if err := i.verifyInternalLeaderApplied(stream.Context()); err != nil {
		return err
	}
	return i.streamExportRangeVersions(req, stream)
}

func (i *Internal) validateExportRangeVersionsRequest(req *pb.ExportRangeVersionsRequest) error {
	if req == nil {
		return errors.WithStack(status.Error(codes.InvalidArgument, "export range versions request is nil"))
	}
	if i.store == nil {
		return errors.WithStack(status.Error(codes.FailedPrecondition, "migration export store is not configured"))
	}
	if req.GetMaxCommitTs() == 0 {
		return errors.WithStack(status.Error(codes.InvalidArgument, "migration export max_commit_ts is required"))
	}
	if req.GetKeyFamily() == 0 {
		return errors.WithStack(status.Error(codes.InvalidArgument, "migration export key_family is required"))
	}
	if exportRangeVersionsRequestFullyUnbounded(req) {
		return errors.WithStack(status.Error(codes.InvalidArgument, "migration export requires a raw or route bound"))
	}
	return nil
}

func exportRangeVersionsRequestFullyUnbounded(req *pb.ExportRangeVersionsRequest) bool {
	return len(req.GetRangeStart()) == 0 &&
		len(req.GetRangeEnd()) == 0 &&
		len(req.GetRouteStart()) == 0 &&
		len(req.GetRouteEnd()) == 0
}

func (i *Internal) streamExportRangeVersions(req *pb.ExportRangeVersionsRequest, stream pb.Internal_ExportRangeVersionsServer) error {
	opts := i.exportRangeVersionsOptions(req)
	for {
		result, err := i.store.ExportVersions(stream.Context(), opts)
		if err != nil {
			return errors.WithStack(err)
		}
		if !result.Done && bytes.Equal(opts.Cursor, result.NextCursor) {
			return errors.WithStack(status.Error(codes.Internal, "migration export cursor did not progress"))
		}
		if err := stream.Send(&pb.ExportRangeVersionsResponse{
			Versions:   protoMVCCVersionsFromStore(result.Versions),
			NextCursor: result.NextCursor,
			Done:       result.Done,
		}); err != nil {
			return errors.WithStack(err)
		}
		if result.Done {
			return nil
		}
		opts.Cursor = result.NextCursor
	}
}

func (i *Internal) ImportRangeVersions(ctx context.Context, req *pb.ImportRangeVersionsRequest) (*pb.ImportRangeVersionsResponse, error) {
	if req == nil {
		return nil, errors.WithStack(status.Error(codes.InvalidArgument, "import range versions request is nil"))
	}
	if err := validateImportRangeVersionsRequest(req); err != nil {
		return nil, err
	}
	if i.migrationProposer == nil {
		return nil, errors.WithStack(status.Error(codes.FailedPrecondition, "migration import proposer is not configured"))
	}
	if err := i.verifyInternalLeader(ctx); err != nil {
		return nil, err
	}
	if err := i.verifyMigrationImportEnabled(ctx); err != nil {
		return nil, err
	}
	result, err := i.proposeMigrationImport(ctx, req)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	if i.clock != nil && result.MaxImportedTS > 0 {
		i.clock.Observe(result.MaxImportedTS)
	}
	return &pb.ImportRangeVersionsResponse{AckedCursor: result.AckedCursor}, nil
}

func validateImportRangeVersionsRequest(req *pb.ImportRangeVersionsRequest) error {
	if req.GetJobId() == 0 {
		return errors.WithStack(status.Error(codes.InvalidArgument, "import range versions job_id is required"))
	}
	if req.GetBracketId() == 0 {
		return errors.WithStack(status.Error(codes.InvalidArgument, "import range versions bracket_id is required"))
	}
	return nil
}

func (i *Internal) verifyMigrationImportEnabled(ctx context.Context) error {
	if i.migrationImportGate == nil {
		return nil
	}
	if err := i.migrationImportGate(ctx); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func (i *Internal) PromoteStagedVersions(ctx context.Context, req *pb.PromoteStagedVersionsRequest) (*pb.PromoteStagedVersionsResponse, error) {
	if req == nil {
		return nil, errors.WithStack(status.Error(codes.InvalidArgument, "promote staged versions request is nil"))
	}
	if req.GetJobId() == 0 {
		return nil, errors.WithStack(status.Error(codes.InvalidArgument, "promote staged versions job_id is required"))
	}
	if i.migrationProposer == nil {
		return nil, errors.WithStack(status.Error(codes.FailedPrecondition, "migration promote proposer is not configured"))
	}
	if err := i.verifyInternalLeader(ctx); err != nil {
		return nil, err
	}
	if err := i.verifyMigrationPromoteEnabled(ctx); err != nil {
		return nil, err
	}
	if err := validatePromoteStagedVersionsRequest(req); err != nil {
		return nil, errors.WithStack(err)
	}
	result, err := i.proposeMigrationPromote(ctx, req)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	if i.clock != nil && result.MaxPromotedTS > 0 {
		i.clock.Observe(result.MaxPromotedTS)
	}
	return &pb.PromoteStagedVersionsResponse{
		NextCursor:    result.NextCursor,
		Done:          result.Done,
		PromotedRows:  result.PromotedRows,
		MaxPromotedTs: result.MaxPromotedTS,
	}, nil
}

func (i *Internal) ApplyTargetStagedReadiness(ctx context.Context, req *pb.TargetStagedReadinessRequest) (*pb.TargetStagedReadinessResponse, error) {
	if err := validateTargetStagedReadinessRequest(req); err != nil {
		return nil, err
	}
	if i.migrationProposer == nil {
		return nil, errors.WithStack(status.Error(codes.FailedPrecondition, "target staged readiness proposer is not configured"))
	}
	if err := i.verifyInternalLeader(ctx); err != nil {
		return nil, err
	}
	if err := i.verifyMigrationPromoteEnabled(ctx); err != nil {
		return nil, err
	}
	if err := i.proposeTargetStagedReadiness(ctx, req); err != nil {
		return nil, errors.WithStack(err)
	}
	return &pb.TargetStagedReadinessResponse{MinAdmittedTs: i.migrationMinAdmittedTS(ctx, req.GetJobId())}, nil
}

func (i *Internal) CleanupMigration(ctx context.Context, req *pb.CleanupMigrationRequest) (*pb.CleanupMigrationResponse, error) {
	if err := validateCleanupMigrationRequest(req); err != nil {
		return nil, err
	}
	if i.migrationProposer == nil {
		return nil, errors.WithStack(status.Error(codes.FailedPrecondition, "migration cleanup proposer is not configured"))
	}
	if err := i.verifyInternalLeader(ctx); err != nil {
		return nil, err
	}
	if err := i.verifyMigrationCleanupEnabled(ctx); err != nil {
		return nil, err
	}
	result, err := i.proposeMigrationCleanup(ctx, req)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return &pb.CleanupMigrationResponse{
		NextCursor:   result.NextCursor,
		Done:         result.Done,
		DeletedRows:  result.DeletedRows,
		DeletedBytes: result.DeletedBytes,
		ScannedBytes: result.ScannedBytes,
	}, nil
}

func validateCleanupMigrationRequest(req *pb.CleanupMigrationRequest) error {
	switch {
	case req == nil:
		return errors.WithStack(status.Error(codes.InvalidArgument, "migration cleanup request is nil"))
	case req.GetJobId() == 0:
		return errors.WithStack(status.Error(codes.InvalidArgument, "migration cleanup job_id is required"))
	case req.GetMode() != pb.MigrationCleanupMode_MIGRATION_CLEANUP_MODE_VERSIONS &&
		req.GetMode() != pb.MigrationCleanupMode_MIGRATION_CLEANUP_MODE_METADATA:
		return errors.WithStack(status.Error(codes.InvalidArgument, "migration cleanup mode is invalid"))
	case req.GetMode() == pb.MigrationCleanupMode_MIGRATION_CLEANUP_MODE_VERSIONS && req.GetKeyFamily() == 0:
		return errors.WithStack(status.Error(codes.InvalidArgument, "migration cleanup key_family is required"))
	}
	return nil
}

func (i *Internal) migrationMinAdmittedTS(ctx context.Context, jobID uint64) uint64 {
	reader, ok := i.store.(store.MigrationTargetReadinessReader)
	if !ok {
		return 0
	}
	states, err := reader.MigrationTargetReadinessStates(ctx)
	if err != nil {
		return 0
	}
	for _, state := range states {
		if state.JobID == jobID {
			return state.MinAdmittedTS
		}
	}
	return 0
}

func validateTargetStagedReadinessRequest(req *pb.TargetStagedReadinessRequest) error {
	switch {
	case req == nil:
		return errors.WithStack(status.Error(codes.InvalidArgument, "target staged readiness request is nil"))
	case req.GetJobId() == 0:
		return errors.WithStack(status.Error(codes.InvalidArgument, "target staged readiness job_id is required"))
	case req.GetMigrationJobId() == 0:
		return errors.WithStack(status.Error(codes.InvalidArgument, "target staged readiness migration_job_id is required"))
	case req.GetArmed() && req.GetMinWriteTsExclusive() == 0:
		return errors.WithStack(status.Error(codes.InvalidArgument, "target staged readiness min_write_ts_exclusive is required when armed"))
	}
	return validateSourceMigrationControlRequest(req)
}

func validateSourceMigrationControlRequest(req *pb.TargetStagedReadinessRequest) error {
	if req.GetSourceReadFence() && !req.GetSourceWriteFence() {
		return errors.WithStack(status.Error(codes.InvalidArgument, "source read fence requires source write fence"))
	}
	if (req.GetSourceWriteFence() || req.GetSourceReadFence()) && req.GetRetentionPinTs() == 0 {
		return errors.WithStack(status.Error(codes.InvalidArgument, "source migration control retention_pin_ts is required"))
	}
	return nil
}

func (i *Internal) ProbeMigrationLocks(ctx context.Context, req *pb.ProbeMigrationLocksRequest) (*pb.ProbeMigrationLocksResponse, error) {
	if req == nil || len(req.GetRouteStart()) == 0 {
		return nil, errors.WithStack(status.Error(codes.InvalidArgument, "migration lock probe route_start is required"))
	}
	if i.store == nil {
		return nil, errors.WithStack(status.Error(codes.FailedPrecondition, "migration lock probe store is not configured"))
	}
	if err := i.verifyInternalLeaderApplied(ctx); err != nil {
		return nil, err
	}
	locks, err := kv.PendingTxnLocksInRoute(ctx, i.store, req.GetRouteStart(), req.GetRouteEnd(), ^uint64(0), int(req.GetLimit()))
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return &pb.ProbeMigrationLocksResponse{PendingCount: uint32(len(locks))}, nil //nolint:gosec // result is bounded by uint32 wire limit
}

func (i *Internal) verifyMigrationPromoteEnabled(ctx context.Context) error {
	if i.migrationPromoteGate == nil {
		return nil
	}
	if err := i.migrationPromoteGate(ctx); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func (i *Internal) verifyMigrationCleanupEnabled(ctx context.Context) error {
	if i.migrationCleanupGate == nil {
		return nil
	}
	if err := i.migrationCleanupGate(ctx); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func validatePromoteStagedVersionsRequest(req *pb.PromoteStagedVersionsRequest) error {
	prefix := distribution.MigrationStagedDataKeyPrefix(req.GetJobId())
	if err := store.ValidatePromotionCursorForRange(req.GetCursor(), prefix, store.PrefixScanEnd(prefix)); err != nil {
		if errors.Is(err, store.ErrInvalidExportCursor) {
			return errors.WithStack(status.Error(codes.InvalidArgument, store.ErrInvalidExportCursor.Error()))
		}
		return errors.WithStack(status.Errorf(codes.Internal, "validate promote cursor: %v", err))
	}
	return nil
}

func defaultMigrationImportGate(context.Context) error {
	if migrationImportOpcodeEnabledFromEnv() {
		return nil
	}
	return errors.WithStack(status.Error(codes.FailedPrecondition, "migration import opcode is disabled; enable after every voter is running a build that supports migration import"))
}

func migrationImportOpcodeEnabledFromEnv() bool {
	return MigrationImportOpcodeEnabledFromEnv()
}

func defaultMigrationPromoteGate(context.Context) error {
	if migrationPromoteOpcodeEnabledFromEnv() {
		return nil
	}
	return errors.WithStack(status.Error(codes.FailedPrecondition, "migration promote opcode is disabled; enable after every voter is running a build that supports migration promotion"))
}

func defaultMigrationCleanupGate(context.Context) error {
	if MigrationCleanupOpcodeEnabledFromEnv() {
		return nil
	}
	return errors.WithStack(status.Error(codes.FailedPrecondition, "migration cleanup opcode is disabled; enable after every voter is running a build that supports migration cleanup"))
}

func migrationPromoteOpcodeEnabledFromEnv() bool {
	return MigrationPromoteOpcodeEnabledFromEnv()
}

// MigrationImportOpcodeEnabledFromEnv reports whether the staged import opcode
// is enabled for this process.
func MigrationImportOpcodeEnabledFromEnv() bool {
	return envFlagEnabled(MigrationImportOpcodeEnv)
}

// MigrationPromoteOpcodeEnabledFromEnv reports whether the staged promotion
// opcode is enabled for this process.
func MigrationPromoteOpcodeEnabledFromEnv() bool {
	return envFlagEnabled(MigrationPromoteOpcodeEnv)
}

// MigrationCleanupOpcodeEnabledFromEnv reports whether the migration cleanup
// opcode is enabled for this process.
func MigrationCleanupOpcodeEnabledFromEnv() bool {
	return envFlagEnabled(MigrationCleanupOpcodeEnv)
}

func envFlagEnabled(name string) bool {
	switch strings.ToLower(strings.TrimSpace(os.Getenv(name))) {
	case "1", "true", "yes", "on":
		return true
	default:
		return false
	}
}

func (i *Internal) verifyInternalLeader(ctx context.Context) error {
	if i.leader == nil {
		return errors.WithStack(ErrNotLeader)
	}
	if i.leader.State() != raftengine.StateLeader {
		return errors.WithStack(ErrNotLeader)
	}
	if err := i.leader.VerifyLeader(ctx); err != nil {
		return errors.WithStack(ErrNotLeader)
	}
	return nil
}

func (i *Internal) verifyInternalLeaderApplied(ctx context.Context) error {
	if i.leader == nil {
		return errors.WithStack(ErrNotLeader)
	}
	if i.leader.State() != raftengine.StateLeader {
		return errors.WithStack(ErrNotLeader)
	}
	_, err := i.leader.LinearizableRead(ctx)
	return errors.WithStack(err)
}

func (i *Internal) proposeMigrationImport(ctx context.Context, req *pb.ImportRangeVersionsRequest) (store.ImportVersionsResult, error) {
	cmd, err := kv.MarshalMigrationImportCommand(req)
	if err != nil {
		return store.ImportVersionsResult{}, errors.WithStack(err)
	}
	resp, err := i.proposeMigrationCommand(ctx, cmd, "migration import")
	if err != nil {
		return store.ImportVersionsResult{}, errors.WithStack(err)
	}
	switch resp := resp.(type) {
	case store.ImportVersionsResult:
		return resp, nil
	case *store.ImportVersionsResult:
		if resp == nil {
			return store.ImportVersionsResult{}, errors.New("migration import apply returned nil result")
		}
		return *resp, nil
	case error:
		return store.ImportVersionsResult{}, errors.WithStack(resp)
	default:
		return store.ImportVersionsResult{}, errors.WithStack(errors.Newf("unexpected migration import apply response type %T", resp))
	}
}

func (i *Internal) proposeMigrationPromote(ctx context.Context, req *pb.PromoteStagedVersionsRequest) (store.PromoteVersionsResult, error) {
	cmd, err := kv.MarshalMigrationPromoteCommand(req)
	if err != nil {
		return store.PromoteVersionsResult{}, errors.WithStack(err)
	}
	resp, err := i.proposeMigrationCommand(ctx, cmd, "migration promote")
	if err != nil {
		return store.PromoteVersionsResult{}, errors.WithStack(err)
	}
	switch resp := resp.(type) {
	case store.PromoteVersionsResult:
		return resp, nil
	case *store.PromoteVersionsResult:
		if resp == nil {
			return store.PromoteVersionsResult{}, errors.New("migration promote apply returned nil result")
		}
		return *resp, nil
	case error:
		return store.PromoteVersionsResult{}, errors.WithStack(resp)
	default:
		return store.PromoteVersionsResult{}, errors.WithStack(errors.Newf("unexpected migration promote apply response type %T", resp))
	}
}

func (i *Internal) proposeMigrationCleanup(ctx context.Context, req *pb.CleanupMigrationRequest) (store.CleanupVersionsResult, error) {
	cmd, err := kv.MarshalMigrationCleanupCommand(req)
	if err != nil {
		return store.CleanupVersionsResult{}, errors.WithStack(err)
	}
	resp, err := i.proposeMigrationCommand(ctx, cmd, "migration cleanup")
	if err != nil {
		return store.CleanupVersionsResult{}, errors.WithStack(err)
	}
	if req.GetMode() == pb.MigrationCleanupMode_MIGRATION_CLEANUP_MODE_METADATA && resp == nil {
		return store.CleanupVersionsResult{Done: true}, nil
	}
	switch resp := resp.(type) {
	case store.CleanupVersionsResult:
		return resp, nil
	case *store.CleanupVersionsResult:
		if resp == nil {
			return store.CleanupVersionsResult{}, errors.New("migration cleanup apply returned nil result")
		}
		return *resp, nil
	case error:
		return store.CleanupVersionsResult{}, errors.WithStack(resp)
	default:
		return store.CleanupVersionsResult{}, errors.WithStack(errors.Newf("unexpected migration cleanup apply response type %T", resp))
	}
}

func (i *Internal) proposeTargetStagedReadiness(ctx context.Context, req *pb.TargetStagedReadinessRequest) error {
	cmd, err := kv.MarshalTargetStagedReadinessCommand(req)
	if err != nil {
		return errors.WithStack(err)
	}
	resp, err := i.proposeMigrationCommand(ctx, cmd, "target staged readiness")
	if err != nil {
		return errors.WithStack(err)
	}
	if resp == nil {
		return nil
	}
	if err, ok := resp.(error); ok {
		return errors.WithStack(err)
	}
	return errors.WithStack(errors.Newf("unexpected target readiness apply response type %T", resp))
}

func (i *Internal) proposeMigrationCommand(ctx context.Context, cmd []byte, label string) (any, error) {
	result, err := i.migrationProposer.Propose(ctx, cmd)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	if result == nil {
		return nil, errors.WithStack(errors.Newf("%s proposal returned nil result", label))
	}
	return result.Response, nil
}

func (i *Internal) exportRangeVersionsOptions(req *pb.ExportRangeVersionsRequest) store.ExportVersionsOptions {
	chunkBytes := uint64(req.GetChunkBytes())
	if chunkBytes == 0 {
		chunkBytes = defaultMigrationExportChunkBytes
	}
	maxScannedBytes := req.GetMaxScannedBytes()
	if maxScannedBytes == 0 {
		maxScannedBytes = chunkBytes * defaultMigrationExportScanFactor
	}
	opts := store.ExportVersionsOptions{
		StartKey:             req.GetRangeStart(),
		EndKey:               req.GetRangeEnd(),
		MinCommitTSExclusive: req.GetMinCommitTs(),
		MaxCommitTSInclusive: req.GetMaxCommitTs(),
		Cursor:               req.GetCursor(),
		MaxVersions:          defaultMigrationExportMaxVersions,
		MaxBytes:             chunkBytes,
		MaxScannedBytes:      maxScannedBytes,
		KeyFamily:            req.GetKeyFamily(),
		AcceptKey:            i.migrationExportFilter(req),
		AcceptVersion:        i.migrationExportVersionFilter(req),
	}
	return opts
}

func (i *Internal) migrationExportFilter(req *pb.ExportRangeVersionsRequest) func([]byte) bool {
	bracket := migrationExportBracket(req)
	if req.GetKeyFamily() == distribution.MigrationFamilyLegacyListMetaDelta {
		return bracket.ContainsRawKey
	}
	routeFilter := i.migrationExportRouteFilter(req)
	if migrationFamilyRequiresDecodedS3(req.GetKeyFamily()) {
		routeFilter = decodedS3BucketRouteFilter(req.GetKeyFamily(), req.GetRouteStart(), req.GetRouteEnd())
	}
	return func(rawKey []byte) bool {
		return bracket.ContainsRawKey(rawKey) && routeFilter(rawKey)
	}
}

func (i *Internal) migrationExportVersionFilter(req *pb.ExportRangeVersionsRequest) func([]byte, []byte) bool {
	if req.GetKeyFamily() != distribution.MigrationFamilyLegacyListMetaDelta {
		return nil
	}
	bracket := migrationExportBracket(req)
	return func(rawKey, value []byte) bool {
		return bracket.ContainsRoutedVersion(rawKey, value, req.GetRouteStart(), req.GetRouteEnd(), nil)
	}
}

func migrationExportBracket(req *pb.ExportRangeVersionsRequest) distribution.MigrationBracket {
	excludeKnownInternal := req.GetExcludeKnownInternal() || req.GetKeyFamily() == distribution.MigrationFamilyUser
	return distribution.MigrationBracket{
		Family:               req.GetKeyFamily(),
		Start:                bytes.Clone(req.GetRangeStart()),
		End:                  bytes.Clone(req.GetRangeEnd()),
		ExcludeKnownInternal: excludeKnownInternal,
		ExcludePrefixes:      cloneByteSlices(req.GetExcludePrefixes()),
	}
}

func (i *Internal) migrationExportRouteFilter(req *pb.ExportRangeVersionsRequest) func([]byte) bool {
	if i != nil && i.migrationExportGroupID != 0 && i.migrationExportResolver != nil {
		return kv.RouteKeyFilterForGroup(req.GetRouteStart(), req.GetRouteEnd(), i.migrationExportGroupID, i.migrationExportResolver)
	}
	return kv.RouteKeyFilter(req.GetRouteStart(), req.GetRouteEnd())
}

func migrationFamilyRequiresDecodedS3(family uint32) bool {
	return family == distribution.MigrationFamilyS3BucketMeta ||
		family == distribution.MigrationFamilyS3BucketGeneration
}

func decodedS3BucketRouteFilter(family uint32, routeStart, routeEnd []byte) func([]byte) bool {
	allowRawRouteMatch := !s3BucketRouteBounds(routeStart, routeEnd)
	return func(rawKey []byte) bool {
		bucket, ok := decodedS3BucketName(family, rawKey)
		if !ok {
			return false
		}
		if allowRawRouteMatch && kv.RouteKeyFilter(routeStart, routeEnd)(rawKey) {
			return true
		}
		return decodedS3BucketRouteIntersects(bucket, routeStart, routeEnd)
	}
}

func s3BucketRouteBounds(routeStart, routeEnd []byte) bool {
	return bytes.HasPrefix(routeStart, []byte(s3keys.RoutePrefix)) ||
		bytes.HasPrefix(routeEnd, []byte(s3keys.RoutePrefix))
}

func decodedS3BucketRouteIntersects(bucket string, routeStart, routeEnd []byte) bool {
	bucketRouteStart := s3keys.RoutePrefixForBucketAnyGeneration(bucket)
	return rangesIntersect(routeStart, routeEnd, bucketRouteStart, prefixScanEnd(bucketRouteStart))
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

func decodedS3BucketName(family uint32, rawKey []byte) (string, bool) {
	switch family {
	case distribution.MigrationFamilyS3BucketMeta:
		return s3keys.ParseBucketMetaKey(rawKey)
	case distribution.MigrationFamilyS3BucketGeneration:
		return s3keys.ParseBucketGenerationKey(rawKey)
	default:
		return "", false
	}
}

func cloneByteSlices(in [][]byte) [][]byte {
	if len(in) == 0 {
		return nil
	}
	out := make([][]byte, len(in))
	for i := range in {
		out[i] = bytes.Clone(in[i])
	}
	return out
}

func protoMVCCVersionsFromStore(in []store.MVCCVersion) []*pb.MVCCVersion {
	out := make([]*pb.MVCCVersion, 0, len(in))
	for _, version := range in {
		out = append(out, &pb.MVCCVersion{
			Key:       bytes.Clone(version.Key),
			CommitTs:  version.CommitTS,
			Tombstone: version.Tombstone,
			Value:     bytes.Clone(version.Value),
			KeyFamily: version.KeyFamily,
			ExpireAt:  version.ExpireAt,
		})
	}
	return out
}

func (i *Internal) stampTimestamps(ctx context.Context, req *pb.ForwardRequest) (uint64, error) {
	if req == nil {
		return 0, nil
	}
	if req.IsTxn {
		return i.stampTxnTimestamps(ctx, req.Requests)
	}

	return 0, i.stampRawTimestamps(ctx, req.Requests)
}

func (i *Internal) nextTimestamp(ctx context.Context, label string) (uint64, error) {
	if i.tsAllocator != nil {
		ts, err := i.tsAllocator.Next(ctx)
		if err != nil {
			return 0, errors.Wrap(err, label)
		}
		return ts, nil
	}
	if i.clock == nil {
		return 1, nil
	}
	ts, err := i.clock.NextFenced()
	if err != nil {
		return 0, errors.Wrap(err, label)
	}
	return ts, nil
}

func (i *Internal) nextTimestampAfter(ctx context.Context, min uint64, label string) (uint64, error) {
	if min == ^uint64(0) {
		return 0, errors.Wrap(kv.ErrTxnCommitTSRequired, label)
	}
	if i.tsAllocator != nil {
		return i.nextTimestampAfterFromAllocator(ctx, min, label)
	}
	if i.clock == nil {
		return min + 1, nil
	}
	if i.clock != nil {
		i.clock.Observe(min)
	}
	ts, err := i.nextTimestamp(ctx, label)
	if err != nil {
		return 0, err
	}
	if ts <= min {
		return 0, errors.Wrap(kv.ErrTxnCommitTSRequired, label)
	}
	return ts, nil
}

func (i *Internal) nextTimestampAfterFromAllocator(ctx context.Context, min uint64, label string) (uint64, error) {
	if after, ok := i.tsAllocator.(kv.TimestampAfterAllocator); ok {
		ts, err := after.NextAfter(ctx, min)
		if err != nil {
			return 0, errors.Wrap(err, label)
		}
		return ts, nil
	}
	ts, err := i.tsAllocator.Next(ctx)
	if err != nil {
		return 0, errors.Wrap(err, label)
	}
	if ts <= min {
		return 0, errors.Wrap(kv.ErrTxnCommitTSRequired, label)
	}
	return ts, nil
}

func (i *Internal) stampRawTimestamps(ctx context.Context, reqs []*pb.Request) error {
	for _, r := range reqs {
		if r == nil {
			continue
		}
		if r.Ts == 0 {
			ts, err := i.nextTimestamp(ctx, "stampRawTimestamps")
			if err != nil {
				return err
			}
			r.Ts = ts
		}
		if err := i.rejectWriteTimestampFloorMutations(r.Mutations, r.Ts); err != nil {
			return err
		}
	}
	return nil
}

func (i *Internal) rejectWriteTimestampFloorMutations(muts []*pb.Mutation, commitTS uint64) error {
	if i == nil || i.routeEngine == nil || commitTS == 0 {
		return nil
	}
	routes := i.routeEngine.Stats()
	for _, mut := range muts {
		if mut == nil || forwardedTxnControlMutation(mut) || (len(mut.Key) == 0 && mut.GetOp() != pb.Op_DEL_PREFIX) {
			continue
		}
		if err := rejectMutationBelowRouteWriteFloor(routes, mut, commitTS); err != nil {
			return err
		}
	}
	return nil
}

func rejectMutationBelowRouteWriteFloor(routes []distribution.Route, mut *pb.Mutation, commitTS uint64) error {
	for _, route := range routes {
		if routeWriteTimestampFloorApplies(route, mut, commitTS) {
			return errors.Wrapf(kv.ErrRouteWriteTimestampTooLow, "key %q commit_ts=%d floor=%d", mut.Key, commitTS, route.MinWriteTSExclusive)
		}
	}
	return nil
}

func forwardedTxnControlMutation(mut *pb.Mutation) bool {
	return mut != nil && bytes.HasPrefix(mut.GetKey(), []byte(kv.TxnKeyPrefix))
}

func routeWriteTimestampFloorApplies(route distribution.Route, mut *pb.Mutation, commitTS uint64) bool {
	if route.MinWriteTSExclusive == 0 || commitTS > route.MinWriteTSExclusive {
		return false
	}
	if mut.GetOp() == pb.Op_DEL_PREFIX {
		start, end := kv.RoutePrefixRange(mut.GetKey())
		return rangesIntersect(route.Start, route.End, start, end)
	}
	if start, end, ok := forwardedS3BucketAuxiliaryRouteRange(mut.GetKey()); ok {
		return rangesIntersect(route.Start, route.End, start, end)
	}
	return kv.RouteKeyFilter(route.Start, route.End)(mut.GetKey())
}

func forwardedS3BucketAuxiliaryRouteRange(key []byte) ([]byte, []byte, bool) {
	bucket, ok := s3keys.ParseBucketMetaKey(key)
	if !ok {
		bucket, ok = s3keys.ParseBucketGenerationKey(key)
	}
	if !ok {
		return nil, nil, false
	}
	start := s3keys.RoutePrefixForBucketAnyGeneration(bucket)
	return start, prefixScanEnd(start), true
}

func (i *Internal) stampTxnTimestamps(ctx context.Context, reqs []*pb.Request) (uint64, error) {
	startTS := forwardedTxnStartTS(reqs)
	if startTS == 0 {
		ts, err := i.nextTimestamp(ctx, "stampTxnTimestamps startTS")
		if err != nil {
			return 0, err
		}
		startTS = ts
	}
	if startTS == ^uint64(0) {
		return 0, errors.WithStack(ErrTxnTimestampOverflow)
	}

	// Assign the unified timestamp to all requests in the transaction.
	for _, r := range reqs {
		if r != nil {
			r.Ts = startTS
		}
	}

	commitTS, err := i.fillForwardedTxnCommitTS(ctx, reqs, startTS)
	if err != nil {
		return 0, err
	}
	if err := i.rejectWriteTimestampFloorTxnRequests(reqs); err != nil {
		return 0, err
	}
	return commitTS, nil
}

func forwardedTxnStartTS(reqs []*pb.Request) uint64 {
	for _, r := range reqs {
		if r != nil && r.Ts != 0 {
			return r.Ts
		}
	}
	return 0
}

func forwardedTxnMetaMutation(r *pb.Request, metaPrefix []byte) (*pb.Mutation, bool) {
	if r == nil || !r.IsTxn {
		return nil, false
	}
	if r.Phase != pb.Phase_COMMIT && r.Phase != pb.Phase_ABORT && r.Phase != pb.Phase_NONE {
		return nil, false
	}
	if len(r.Mutations) == 0 || r.Mutations[0] == nil {
		return nil, false
	}
	if !bytes.HasPrefix(r.Mutations[0].Key, metaPrefix) {
		return nil, false
	}
	return r.Mutations[0], true
}

type forwardedTxnMetaToUpdate struct {
	m    *pb.Mutation
	meta kv.TxnMeta
}

func (i *Internal) fillForwardedTxnCommitTS(ctx context.Context, reqs []*pb.Request, startTS uint64) (uint64, error) {
	metaMutations, commitTS, err := collectForwardedTxnMetas(reqs)
	if err != nil {
		return 0, err
	}

	if commitTS == 0 && len(metaMutations) > 0 {
		commitTS, err = i.forwardedTxnCommitTS(ctx, startTS)
		if err != nil {
			return 0, err
		}
	}

	for _, item := range metaMutations {
		item.meta.CommitTS = commitTS
		item.m.Value = kv.EncodeTxnMeta(item.meta)
	}
	if err := stampRequestMutationCommitTS(reqs, commitTS); err != nil {
		return 0, err
	}
	return commitTS, nil
}

func stampRequestMutationCommitTS(reqs []*pb.Request, commitTS uint64) error {
	for _, r := range reqs {
		if r == nil {
			continue
		}
		if err := kv.StampMutationCommitTS(r.Mutations, commitTS); err != nil {
			return errors.WithStack(err)
		}
	}
	return nil
}

func collectForwardedTxnMetas(reqs []*pb.Request) ([]forwardedTxnMetaToUpdate, uint64, error) {
	metaMutations := make([]forwardedTxnMetaToUpdate, 0, len(reqs))
	var commitTS uint64
	prefix := []byte(kv.TxnMetaPrefix)
	for _, r := range reqs {
		m, ok := forwardedTxnMetaMutation(r, prefix)
		if !ok {
			continue
		}
		meta, err := kv.DecodeTxnMeta(m.Value)
		if err != nil {
			continue
		}
		if meta.CommitTS != 0 {
			if commitTS != 0 && commitTS != meta.CommitTS {
				return nil, 0, errors.WithStack(kv.ErrInvalidRequest)
			}
			commitTS = meta.CommitTS
			continue
		}
		metaMutations = append(metaMutations, forwardedTxnMetaToUpdate{m: m, meta: meta})
	}
	return metaMutations, commitTS, nil
}

func (i *Internal) rejectWriteTimestampFloorTxnRequests(reqs []*pb.Request) error {
	for _, r := range reqs {
		commitTS, ok := forwardedTxnRequestCommitTS(r)
		if !ok {
			continue
		}
		if err := i.rejectWriteTimestampFloorMutations(r.Mutations, commitTS); err != nil {
			return err
		}
	}
	return nil
}

func forwardedTxnRequestCommitTS(r *pb.Request) (uint64, bool) {
	if r == nil || (r.Phase != pb.Phase_COMMIT && r.Phase != pb.Phase_NONE) {
		return 0, false
	}
	m, ok := forwardedTxnMetaMutation(r, []byte(kv.TxnMetaPrefix))
	if !ok {
		return 0, false
	}
	meta, err := kv.DecodeTxnMeta(m.Value)
	if err != nil || meta.CommitTS == 0 {
		return 0, false
	}
	return meta.CommitTS, true
}

// forwardedTxnCommitTS allocates a commit timestamp for a forwarded
// transaction, strictly greater than startTS. Pulled out of
// fillForwardedTxnCommitTS so that function stays under cyclop.
func (i *Internal) forwardedTxnCommitTS(ctx context.Context, startTS uint64) (uint64, error) {
	commitTS := startTS + 1
	if commitTS == 0 {
		// Overflow: can't choose a commit timestamp strictly greater than startTS.
		return 0, errors.WithStack(ErrTxnTimestampOverflow)
	}
	if i.clock != nil {
		i.clock.Observe(startTS)
	}
	if i.tsAllocator != nil || i.clock != nil {
		ts, err := i.nextTimestampAfter(ctx, startTS, "fillForwardedTxnCommitTS")
		if err != nil {
			return 0, err
		}
		commitTS = ts
	}
	if commitTS <= startTS {
		// Defensive: avoid writing an invalid CommitTS.
		return 0, errors.WithStack(ErrTxnTimestampOverflow)
	}
	return commitTS, nil
}
