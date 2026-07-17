package adapter

import (
	"bytes"
	"context"
	"encoding/binary"
	"log/slog"
	"math"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/bootjp/elastickv/distribution"
	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/bootjp/elastickv/kv"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// DistributionServer serves distribution related gRPC APIs.
type DistributionServer struct {
	mu                          sync.Mutex
	engine                      *distribution.Engine
	catalog                     *distribution.CatalogStore
	coordinator                 kv.Coordinator
	readTracker                 *kv.ActiveTimestampTracker
	readBlocked                 func() bool
	migrationCapabilityGate     SplitMigrationCapabilityGate
	splitJobRunnerReady         bool
	splitPromotionClientFactory SplitPromotionClientFactory
	knownRaftGroups             map[uint64]struct{}
	reloadRetry                 struct {
		attempts int
		interval time.Duration
	}
	pb.UnimplementedDistributionServer
}

// DistributionServerOption configures DistributionServer behavior.
type DistributionServerOption func(*DistributionServer)

// SplitMigrationCapabilityGate reports whether this node can safely create
// migration-only side effects. A nil gate keeps StartSplitMigration fail-closed.
type SplitMigrationCapabilityGate func(context.Context) error

// SplitPromotionClient is the subset of the internal gRPC client the split
// job runner needs to promote target-local staged rows.
type SplitPromotionClient interface {
	PromoteStagedVersions(context.Context, *pb.PromoteStagedVersionsRequest, ...grpc.CallOption) (*pb.PromoteStagedVersionsResponse, error)
}

// SplitPromotionClientFactory dials the node currently leading a split job's
// target range and returns the internal client used by the runner.
type SplitPromotionClientFactory func(context.Context, distribution.SplitJob) (SplitPromotionClient, error)

// WithDistributionCoordinator configures the coordinator used for Raft-backed
// catalog mutations in SplitRange.
func WithDistributionCoordinator(coordinator kv.Coordinator) DistributionServerOption {
	return func(s *DistributionServer) {
		s.coordinator = coordinator
	}
}

func WithDistributionActiveTimestampTracker(tracker *kv.ActiveTimestampTracker) DistributionServerOption {
	return func(s *DistributionServer) {
		s.readTracker = tracker
	}
}

func WithDistributionReadGate(blocked func() bool) DistributionServerOption {
	return func(s *DistributionServer) {
		s.readBlocked = blocked
	}
}

func WithSplitMigrationCapabilityGate(gate SplitMigrationCapabilityGate) DistributionServerOption {
	return func(s *DistributionServer) {
		s.migrationCapabilityGate = gate
	}
}

// WithSplitJobRunnerReady opens the split migration capability probe once this
// node can advance planned split jobs.
func WithSplitJobRunnerReady() DistributionServerOption {
	return func(s *DistributionServer) {
		s.splitJobRunnerReady = true
	}
}

// WithSplitPromotionClientFactory configures the client factory used by the
// split job runner to promote target-local staged versions.
func WithSplitPromotionClientFactory(factory SplitPromotionClientFactory) DistributionServerOption {
	return func(s *DistributionServer) {
		s.splitPromotionClientFactory = factory
	}
}

// WithDistributionKnownRaftGroups configures the Raft group IDs this node can
// route migration work to.
func WithDistributionKnownRaftGroups(groupIDs ...uint64) DistributionServerOption {
	return func(s *DistributionServer) {
		groups := make(map[uint64]struct{}, len(groupIDs))
		for _, groupID := range groupIDs {
			if groupID == 0 {
				continue
			}
			groups[groupID] = struct{}{}
		}
		s.knownRaftGroups = groups
	}
}

// WithCatalogReloadRetryPolicy configures the retry policy used after split
// commit when waiting for the local catalog snapshot to become visible.
func WithCatalogReloadRetryPolicy(attempts int, interval time.Duration) DistributionServerOption {
	return func(s *DistributionServer) {
		if attempts > 0 {
			s.reloadRetry.attempts = attempts
		}
		if interval > 0 {
			s.reloadRetry.interval = interval
		}
	}
}

const (
	childRouteCount                = 2
	splitMutationOpCount           = childRouteCount + 3
	listSplitJobsDefaultPageSize   = 200
	splitJobListCursorVersion      = byte(1)
	splitJobListCursorTerminalOff  = 1
	splitJobListCursorJobIDOff     = splitJobListCursorTerminalOff + 8
	splitJobListCursorEncodedBytes = splitJobListCursorJobIDOff + 8
	SplitMigrationCapabilityV2     = "cap_migration_v2"
	splitMigrationCapabilityV2     = SplitMigrationCapabilityV2

	splitPromotionDefaultMaxVersions = 1024
	splitPromotionBytesPerMiB        = 1024 * 1024
	splitPromotionMaxBytesMiB        = 4
	splitPromotionMaxScannedMiB      = 16
	promotionCompleteBaseOpCount     = 2
)

var (
	defaultCatalogReloadRetryAttempts = 20
	defaultCatalogReloadRetryInterval = 10 * time.Millisecond
	defaultSplitJobRunnerInterval     = time.Second
	defaultSplitPromotionMaxVersions  = uint32(splitPromotionDefaultMaxVersions)
	defaultSplitPromotionMaxBytes     = uint64(splitPromotionMaxBytesMiB * splitPromotionBytesPerMiB)
	defaultSplitPromotionMaxScanned   = uint64(splitPromotionMaxScannedMiB * splitPromotionBytesPerMiB)

	errDistributionCatalogNotConfigured   = errors.New("route catalog is not configured")
	errDistributionUnknownRoute           = errors.New("unknown route")
	errDistributionInvalidSplitKey        = errors.New("invalid split key")
	errDistributionSplitKeyAtBoundary     = errors.New("split key at route boundary")
	errDistributionCatalogConflict        = errors.New("catalog version conflict")
	errDistributionRouteIDOverflow        = errors.New("route id overflow")
	errDistributionNotLeader              = errors.New("not leader for distribution catalog")
	errDistributionCoordinatorRequired    = errors.New("distribution coordinator is not configured")
	errDistributionEngineNotConfigured    = errors.New("distribution engine is not configured")
	errDistributionCatalogVersionNotFound = errors.New("route catalog version not found")
	errDistributionClusterNotReady        = errors.New("cluster is not ready for split migration")
	errDistributionRaftGroupsNotKnown     = errors.New("distribution raft groups are not configured")
	errDistributionUnknownTargetGroup     = errors.New("unknown target group")
	errDistributionSourceRouteNotActive   = errors.New("source route is not active")
)

// NewDistributionServer creates a new server.
func NewDistributionServer(e *distribution.Engine, catalog *distribution.CatalogStore, opts ...DistributionServerOption) *DistributionServer {
	s := &DistributionServer{
		engine:  e,
		catalog: catalog,
	}
	s.reloadRetry.attempts = defaultCatalogReloadRetryAttempts
	s.reloadRetry.interval = defaultCatalogReloadRetryInterval
	for _, opt := range opts {
		if opt != nil {
			opt(s)
		}
	}
	return s
}

func (s *DistributionServer) SetReadGate(blocked func() bool) {
	if s != nil {
		s.readBlocked = blocked
	}
}

func (s *DistributionServer) requireReadReady() error {
	if s != nil && s.readBlocked != nil && s.readBlocked() {
		//nolint:wrapcheck // Preserve the gRPC status code for startup readers.
		return status.Error(codes.Unavailable, "distribution startup has not completed")
	}
	return nil
}

func (s *DistributionServer) RunSplitJobRunner(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	ticker := time.NewTicker(defaultSplitJobRunnerInterval)
	defer ticker.Stop()
	for {
		if err := s.RunSplitJobRunnerOnce(ctx); err != nil {
			slog.Warn("split job runner tick failed", "err", err)
		}
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}

func (s *DistributionServer) RunSplitJobRunnerOnce(ctx context.Context) error {
	if !s.splitJobRunnerConfigured() {
		return nil
	}
	leader, err := s.verifySplitJobRunnerLeader(ctx)
	if err != nil || !leader {
		return err
	}
	jobs, err := s.catalog.ListSplitJobs(ctx)
	if err != nil {
		return splitJobCatalogStatusError(err)
	}
	if job, ok := nextCleanupSplitJob(jobs); ok {
		return s.promoteSplitJobTargetAndComplete(ctx, job)
	}
	return nil
}

func (s *DistributionServer) splitJobRunnerConfigured() bool {
	return s != nil && s.catalog != nil && s.coordinator != nil && s.splitPromotionClientFactory != nil
}

func (s *DistributionServer) verifySplitJobRunnerLeader(ctx context.Context) (bool, error) {
	if !s.coordinator.IsLeaderForKey(distribution.CatalogVersionKey()) {
		return false, nil
	}
	if err := s.coordinator.VerifyLeaderForKey(ctx, distribution.CatalogVersionKey()); err != nil {
		return false, errors.WithStack(err)
	}
	return true, nil
}

func nextCleanupSplitJob(jobs []distribution.SplitJob) (distribution.SplitJob, bool) {
	for _, job := range jobs {
		if job.Phase == distribution.SplitJobPhaseCleanup && !job.TargetPromotionDone {
			return job, true
		}
	}
	return distribution.SplitJob{}, false
}

func (s *DistributionServer) promoteSplitJobTargetAndComplete(ctx context.Context, job distribution.SplitJob) error {
	client, err := s.splitPromotionClientFactory(ctx, job)
	if err != nil {
		return errors.WithStack(err)
	}
	if client == nil {
		return errors.New("split promotion client is nil")
	}
	if err := promoteSplitJobTarget(ctx, client, job); err != nil {
		return errors.WithStack(err)
	}
	snapshot, err := s.loadCatalogSnapshot(ctx)
	if err != nil {
		return err
	}
	current, found, err := s.catalog.SplitJobAt(ctx, job.JobID, snapshot.ReadTS)
	if err != nil {
		return splitJobPromotionStatusError(err)
	}
	if !found {
		return splitJobPromotionStatusError(distribution.ErrCatalogSplitJobConflict)
	}
	_, _, err = s.completeSplitJobTargetPromotionViaCoordinator(ctx, snapshot.Version, current, time.Now().UnixMilli())
	return err
}

func promoteSplitJobTarget(ctx context.Context, client SplitPromotionClient, job distribution.SplitJob) error {
	var cursor []byte
	for {
		resp, err := client.PromoteStagedVersions(ctx, &pb.PromoteStagedVersionsRequest{
			JobId:           job.JobID,
			Cursor:          cursor,
			MaxVersions:     defaultSplitPromotionMaxVersions,
			MaxBytes:        defaultSplitPromotionMaxBytes,
			MaxScannedBytes: defaultSplitPromotionMaxScanned,
		})
		if err != nil {
			return errors.WithStack(err)
		}
		if resp.GetDone() {
			return nil
		}
		next := resp.GetNextCursor()
		if len(next) == 0 || bytes.Equal(next, cursor) {
			return errors.New("split promotion made no cursor progress")
		}
		cursor = distribution.CloneBytes(next)
	}
}

// UpdateRoute allows updating route information.
func (s *DistributionServer) UpdateRoute(start, end []byte, group uint64) {
	s.engine.UpdateRoute(start, end, group)
}

// GetRoute returns route for a key.
func (s *DistributionServer) GetRoute(ctx context.Context, req *pb.GetRouteRequest) (*pb.GetRouteResponse, error) {
	if err := s.requireReadReady(); err != nil {
		return nil, err
	}
	r, ok := s.engine.GetRoute(req.Key)
	if !ok {
		return &pb.GetRouteResponse{}, nil
	}
	return &pb.GetRouteResponse{
		Start:       r.Start,
		End:         r.End,
		RaftGroupId: r.GroupID,
	}, nil
}

// GetTimestamp returns monotonically increasing timestamp.
func (s *DistributionServer) GetTimestamp(ctx context.Context, req *pb.GetTimestampRequest) (*pb.GetTimestampResponse, error) {
	ts := s.engine.NextTimestamp()
	return &pb.GetTimestampResponse{Timestamp: ts}, nil
}

// ListRoutes returns all durable routes from catalog storage.
func (s *DistributionServer) ListRoutes(ctx context.Context, req *pb.ListRoutesRequest) (*pb.ListRoutesResponse, error) {
	if err := s.requireReadReady(); err != nil {
		return nil, err
	}
	snapshot, err := s.loadCatalogSnapshot(ctx)
	if err != nil {
		return nil, err
	}

	return &pb.ListRoutesResponse{
		CatalogVersion: snapshot.Version,
		Routes:         toProtoRouteDescriptors(snapshot.Routes),
	}, nil
}

func (s *DistributionServer) GetRouteOwnership(ctx context.Context, req *pb.GetRouteOwnershipRequest) (*pb.GetRouteOwnershipResponse, error) {
	if err := s.requireReadReady(); err != nil {
		return nil, err
	}
	snapshot, err := s.routeSnapshotAt(req.GetCatalogVersion())
	if err != nil {
		return nil, err
	}
	route, ok := snapshot.RouteOf(req.GetKey())
	if !ok {
		return &pb.GetRouteOwnershipResponse{
			CatalogVersion: snapshot.Version(),
			Found:          false,
		}, nil
	}
	return &pb.GetRouteOwnershipResponse{
		Route:          toProtoRoute(route),
		CatalogVersion: snapshot.Version(),
		Found:          true,
	}, nil
}

func (s *DistributionServer) GetIntersectingRoutes(ctx context.Context, req *pb.GetIntersectingRoutesRequest) (*pb.GetIntersectingRoutesResponse, error) {
	if err := s.requireReadReady(); err != nil {
		return nil, err
	}
	snapshot, err := s.routeSnapshotAt(req.GetCatalogVersion())
	if err != nil {
		return nil, err
	}
	end := req.GetEnd()
	if len(end) == 0 {
		end = nil
	}
	routes := snapshot.IntersectingRoutes(req.GetStart(), end)
	out := make([]*pb.RouteDescriptor, 0, len(routes))
	for _, route := range routes {
		out = append(out, toProtoRoute(route))
	}
	return &pb.GetIntersectingRoutesResponse{
		Routes:         out,
		CatalogVersion: snapshot.Version(),
	}, nil
}

func (s *DistributionServer) StartSplitMigration(ctx context.Context, req *pb.StartSplitMigrationRequest) (*pb.StartSplitMigrationResponse, error) {
	if err := s.verifyStartSplitMigrationPreflight(ctx, req); err != nil {
		return nil, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.verifyStartSplitMigrationCatalogReady(ctx); err != nil {
		return nil, err
	}
	snapshot, err := s.loadCatalogSnapshot(ctx)
	if err != nil {
		return nil, err
	}
	defer releaseReadPin(s.pinReadTS(snapshot.ReadTS))

	parent, err := s.startSplitMigrationParent(ctx, snapshot, req)
	if err != nil {
		return nil, err
	}
	job, err := s.newSplitMigrationJob(ctx, snapshot, parent, req)
	if err != nil {
		return nil, err
	}
	if err := s.createSplitJobViaCoordinator(ctx, snapshot.ReadTS, job); err != nil {
		return nil, err
	}
	return &pb.StartSplitMigrationResponse{
		CatalogVersion: snapshot.Version,
		JobId:          job.JobID,
	}, nil
}

func (s *DistributionServer) GetSplitMigrationCapability(context.Context, *pb.GetSplitMigrationCapabilityRequest) (*pb.GetSplitMigrationCapabilityResponse, error) {
	if !s.splitJobRunnerReady {
		return &pb.GetSplitMigrationCapabilityResponse{}, nil
	}
	return &pb.GetSplitMigrationCapabilityResponse{
		MigrationCapable: true,
		Capabilities:     []string{splitMigrationCapabilityV2},
	}, nil
}

func (s *DistributionServer) verifyStartSplitMigrationPreflight(ctx context.Context, req *pb.StartSplitMigrationRequest) error {
	if req.GetTargetGroupId() == 0 {
		return grpcStatusError(codes.InvalidArgument, distribution.ErrCatalogSplitJobTargetGroupRequired.Error())
	}
	if err := s.verifyStartSplitMigrationCatalogReady(ctx); err != nil {
		return err
	}
	return s.verifySplitMigrationCapability(ctx)
}

func (s *DistributionServer) verifyStartSplitMigrationCatalogReady(ctx context.Context) error {
	if err := s.verifyCatalogLeader(ctx); err != nil {
		return err
	}
	if s.catalog == nil {
		return grpcStatusError(codes.FailedPrecondition, errDistributionCatalogNotConfigured.Error())
	}
	return nil
}

func (s *DistributionServer) startSplitMigrationParent(
	ctx context.Context,
	snapshot distribution.CatalogSnapshot,
	req *pb.StartSplitMigrationRequest,
) (distribution.RouteDescriptor, error) {
	if err := validateExpectedCatalogVersion(snapshot.Version, req.GetExpectedCatalogVersion()); err != nil {
		return distribution.RouteDescriptor{}, err
	}
	parent, found := findRouteByID(snapshot.Routes, req.GetRouteId())
	if !found {
		return distribution.RouteDescriptor{}, grpcStatusError(codes.NotFound, errDistributionUnknownRoute.Error())
	}
	if parent.State != distribution.RouteStateActive {
		return distribution.RouteDescriptor{}, grpcStatusError(codes.FailedPrecondition, errDistributionSourceRouteNotActive.Error())
	}
	splitKey := distribution.CloneBytes(req.GetSplitKey())
	if err := validateSplitKey(parent, splitKey); err != nil {
		return distribution.RouteDescriptor{}, err
	}
	if err := distribution.ValidateMigrationRouteRange(splitKey, parent.End); err != nil {
		return distribution.RouteDescriptor{}, splitMigrationRangeStatusError(err)
	}
	if parent.GroupID == req.GetTargetGroupId() {
		return distribution.RouteDescriptor{}, grpcStatusError(codes.InvalidArgument, "target group must differ from source route group")
	}
	if err := s.verifyKnownTargetGroup(req.GetTargetGroupId()); err != nil {
		return distribution.RouteDescriptor{}, err
	}
	if err := s.rejectLiveSplitJob(ctx, snapshot, parent); err != nil {
		return distribution.RouteDescriptor{}, err
	}
	return parent, nil
}

func (s *DistributionServer) newSplitMigrationJob(
	ctx context.Context,
	snapshot distribution.CatalogSnapshot,
	parent distribution.RouteDescriptor,
	req *pb.StartSplitMigrationRequest,
) (distribution.SplitJob, error) {
	jobID, err := s.catalog.NextSplitJobIDAt(ctx, snapshot.ReadTS)
	if err != nil {
		return distribution.SplitJob{}, splitJobCatalogStatusError(err)
	}
	job, err := distribution.InitializeSplitJobPlan(distribution.SplitJob{
		JobID:         jobID,
		SourceRouteID: parent.RouteID,
		SplitKey:      distribution.CloneBytes(req.GetSplitKey()),
		TargetGroupID: req.GetTargetGroupId(),
	}, parent, time.Now().UnixMilli())
	if err != nil {
		return distribution.SplitJob{}, splitJobCatalogStatusError(err)
	}
	return job, nil
}

func (s *DistributionServer) GetSplitJob(ctx context.Context, req *pb.GetSplitJobRequest) (*pb.GetSplitJobResponse, error) {
	if req.GetJobId() == 0 {
		return nil, grpcStatusError(codes.InvalidArgument, distribution.ErrCatalogSplitJobIDRequired.Error())
	}
	if s.catalog == nil {
		return nil, grpcStatusError(codes.FailedPrecondition, errDistributionCatalogNotConfigured.Error())
	}
	job, found, err := s.catalog.SplitJob(ctx, req.GetJobId())
	if err != nil {
		return nil, splitJobCatalogStatusError(err)
	}
	if !found {
		return nil, grpcStatusError(codes.NotFound, distribution.ErrCatalogSplitJobNotFound.Error())
	}
	return &pb.GetSplitJobResponse{Job: distribution.SplitJobToProto(job)}, nil
}

func (s *DistributionServer) ListSplitJobs(ctx context.Context, req *pb.ListSplitJobsRequest) (*pb.ListSplitJobsResponse, error) {
	if s.catalog == nil {
		return nil, grpcStatusError(codes.FailedPrecondition, errDistributionCatalogNotConfigured.Error())
	}
	if req == nil {
		req = &pb.ListSplitJobsRequest{}
	}
	phaseFilter := normalizeSplitJobPhaseFilter(req.GetPhase())
	if phaseFilter != "" && !validSplitJobPhaseFilter(phaseFilter) {
		return nil, grpcStatusErrorf(codes.InvalidArgument, "unknown split job phase %q", req.GetPhase())
	}
	jobs, err := s.catalog.ListSplitJobs(ctx)
	if err != nil {
		return nil, splitJobCatalogStatusError(err)
	}
	resp, err := splitJobListPage(jobs, req, phaseFilter)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (s *DistributionServer) AbandonSplitJob(ctx context.Context, req *pb.AbandonSplitJobRequest) (*pb.AbandonSplitJobResponse, error) {
	if req.GetJobId() == 0 {
		return nil, grpcStatusError(codes.InvalidArgument, distribution.ErrCatalogSplitJobIDRequired.Error())
	}
	if err := s.verifyCatalogLeader(ctx); err != nil {
		return nil, err
	}
	if s.catalog == nil {
		return nil, grpcStatusError(codes.FailedPrecondition, errDistributionCatalogNotConfigured.Error())
	}
	if err := s.updateSplitJobViaCoordinator(ctx, req.GetJobId(), func(job distribution.SplitJob) (distribution.SplitJob, error) {
		return distribution.BeginSplitJobAbandon(job, time.Now().UnixMilli())
	}); err != nil {
		return nil, err
	}
	return &pb.AbandonSplitJobResponse{}, nil
}

func (s *DistributionServer) RetrySplitJob(ctx context.Context, req *pb.RetrySplitJobRequest) (*pb.RetrySplitJobResponse, error) {
	if req.GetJobId() == 0 {
		return nil, grpcStatusError(codes.InvalidArgument, distribution.ErrCatalogSplitJobIDRequired.Error())
	}
	if err := s.verifyCatalogLeader(ctx); err != nil {
		return nil, err
	}
	if s.catalog == nil {
		return nil, grpcStatusError(codes.FailedPrecondition, errDistributionCatalogNotConfigured.Error())
	}
	if err := s.updateSplitJobViaCoordinator(ctx, req.GetJobId(), func(job distribution.SplitJob) (distribution.SplitJob, error) {
		return distribution.RetrySplitJobState(job, time.Now().UnixMilli())
	}); err != nil {
		return nil, err
	}
	return &pb.RetrySplitJobResponse{}, nil
}

func (s *DistributionServer) routeSnapshotAt(version uint64) (distribution.RouteHistorySnapshot, error) {
	if s.engine == nil {
		return distribution.RouteHistorySnapshot{}, grpcStatusError(codes.FailedPrecondition, errDistributionEngineNotConfigured.Error())
	}
	snapshot, ok := s.engine.SnapshotAt(version)
	if !ok {
		return distribution.RouteHistorySnapshot{}, grpcStatusErrorf(codes.NotFound, "%s: %d", errDistributionCatalogVersionNotFound, version)
	}
	return snapshot, nil
}

// SplitRange splits a route into two child routes in the same raft group.
func (s *DistributionServer) SplitRange(ctx context.Context, req *pb.SplitRangeRequest) (*pb.SplitRangeResponse, error) {
	// SplitRange performs a read-modify-write cycle across catalog and engine.
	// Serialize it per server instance to keep these updates consistent.
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.verifyCatalogLeader(ctx); err != nil {
		return nil, err
	}

	snapshot, err := s.loadCatalogSnapshot(ctx)
	if err != nil {
		return nil, err
	}
	defer releaseReadPin(s.pinReadTS(snapshot.ReadTS))
	if err := validateExpectedCatalogVersion(snapshot.Version, req.GetExpectedCatalogVersion()); err != nil {
		return nil, err
	}

	parent, found := findRouteByID(snapshot.Routes, req.GetRouteId())
	if !found {
		return nil, grpcStatusError(codes.NotFound, errDistributionUnknownRoute.Error())
	}

	splitKey := distribution.CloneBytes(req.GetSplitKey())
	if err := validateSplitKey(parent, splitKey); err != nil {
		return nil, err
	}
	splitJobReadKeys, err := s.splitJobOverlapReadKeys(ctx, snapshot, parent)
	if err != nil {
		return nil, err
	}

	leftID, rightID, err := s.allocateChildRouteIDs(ctx, snapshot.ReadTS, snapshot.Routes)
	if err != nil {
		return nil, err
	}
	left, right := splitCatalogRoutes(parent, splitKey, leftID, rightID)

	saved, err := s.saveSplitResultViaCoordinator(ctx, snapshot.ReadTS, req.GetExpectedCatalogVersion(), parent.RouteID, splitJobReadKeys, left, right)
	if err != nil {
		return nil, err
	}
	if err := s.applyEngineSnapshot(saved); err != nil {
		return nil, err
	}

	return &pb.SplitRangeResponse{
		CatalogVersion: saved.Version,
		Left:           toProtoRouteDescriptor(left),
		Right:          toProtoRouteDescriptor(right),
	}, nil
}

func (s *DistributionServer) pinReadTS(ts uint64) *kv.ActiveTimestampToken {
	if s == nil || s.readTracker == nil {
		return &kv.ActiveTimestampToken{}
	}
	return s.readTracker.Pin(ts)
}

func releaseReadPin(token *kv.ActiveTimestampToken) {
	if token == nil {
		return
	}
	token.Release()
}

func (s *DistributionServer) verifyCatalogLeader(ctx context.Context) error {
	if s.coordinator == nil {
		return grpcStatusError(codes.FailedPrecondition, errDistributionCoordinatorRequired.Error())
	}
	key := distribution.CatalogVersionKey()
	if !s.coordinator.IsLeaderForKey(key) {
		return grpcStatusError(codes.FailedPrecondition, errDistributionNotLeader.Error())
	}
	if err := s.coordinator.VerifyLeaderForKey(ctx, key); err != nil {
		return grpcStatusErrorf(codes.FailedPrecondition, "verify catalog leader: %v", err)
	}
	return nil
}

func (s *DistributionServer) saveSplitResultViaCoordinator(
	ctx context.Context,
	readTS uint64,
	expectedVersion uint64,
	parentID uint64,
	readKeys [][]byte,
	left distribution.RouteDescriptor,
	right distribution.RouteDescriptor,
) (distribution.CatalogSnapshot, error) {
	if expectedVersion == math.MaxUint64 {
		return distribution.CatalogSnapshot{}, grpcStatusError(codes.Internal, "catalog version overflow")
	}
	nextVersion := expectedVersion + 1
	if right.RouteID == math.MaxUint64 {
		return distribution.CatalogSnapshot{}, grpcStatusError(codes.Internal, errDistributionRouteIDOverflow.Error())
	}
	nextRouteID := right.RouteID + 1

	ops, err := buildCatalogSplitOps(parentID, left, right, nextVersion, nextRouteID, s.catalog.AllowsRouteDescriptorV2Writes())
	if err != nil {
		return distribution.CatalogSnapshot{}, grpcStatusErrorf(codes.Internal, "build split mutations: %v", err)
	}
	if _, err := s.coordinator.Dispatch(ctx, &kv.OperationGroup[kv.OP]{
		Elems:    ops,
		IsTxn:    true,
		StartTS:  readTS,
		ReadKeys: readKeys,
	}); err != nil {
		if errors.Is(err, store.ErrWriteConflict) {
			return distribution.CatalogSnapshot{}, grpcStatusError(codes.Aborted, errDistributionCatalogConflict.Error())
		}
		return distribution.CatalogSnapshot{}, grpcStatusErrorf(codes.Internal, "commit split mutations: %v", err)
	}
	return s.loadCatalogSnapshotAtLeastVersion(ctx, nextVersion)
}

func buildCatalogSplitOps(
	parentID uint64,
	left distribution.RouteDescriptor,
	right distribution.RouteDescriptor,
	nextVersion uint64,
	nextRouteID uint64,
	allowRouteDescriptorV2Writes bool,
) ([]*kv.Elem[kv.OP], error) {
	// SplitRange mutates the catalog surgically: delete one parent route, add two
	// children, bump the version, and advance the next-route-id counter.
	ops := make([]*kv.Elem[kv.OP], 0, splitMutationOpCount)
	ops = append(ops, &kv.Elem[kv.OP]{
		Op:  kv.Del,
		Key: distribution.CatalogRouteKey(parentID),
	})
	for _, route := range []distribution.RouteDescriptor{left, right} {
		encoded, err := distribution.EncodeRouteDescriptorForCatalogWrite(route, allowRouteDescriptorV2Writes)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		ops = append(ops, &kv.Elem[kv.OP]{
			Op:    kv.Put,
			Key:   distribution.CatalogRouteKey(route.RouteID),
			Value: encoded,
		})
	}
	ops = append(ops, &kv.Elem[kv.OP]{
		Op:    kv.Put,
		Key:   distribution.CatalogVersionKey(),
		Value: distribution.EncodeCatalogVersion(nextVersion),
	})
	ops = append(ops, &kv.Elem[kv.OP]{
		Op:    kv.Put,
		Key:   distribution.CatalogNextRouteIDKey(),
		Value: distribution.EncodeCatalogNextRouteID(nextRouteID),
	})
	return ops, nil
}

func (s *DistributionServer) loadCatalogSnapshot(ctx context.Context) (distribution.CatalogSnapshot, error) {
	if s.catalog == nil {
		return distribution.CatalogSnapshot{}, grpcStatusError(codes.FailedPrecondition, errDistributionCatalogNotConfigured.Error())
	}
	snapshot, err := s.catalog.Snapshot(ctx)
	if err != nil {
		return distribution.CatalogSnapshot{}, grpcStatusErrorf(codes.Internal, "load route catalog: %v", err)
	}
	return snapshot, nil
}

func (s *DistributionServer) loadCatalogSnapshotAtLeastVersion(
	ctx context.Context,
	minVersion uint64,
) (distribution.CatalogSnapshot, error) {
	attempts := s.reloadRetry.attempts
	if attempts <= 0 {
		attempts = defaultCatalogReloadRetryAttempts
	}
	interval := s.reloadRetry.interval
	if interval <= 0 {
		interval = defaultCatalogReloadRetryInterval
	}

	var last distribution.CatalogSnapshot
	for attempt := 0; attempt < attempts; attempt++ {
		snapshot, err := s.catalog.Snapshot(ctx)
		if err != nil {
			return distribution.CatalogSnapshot{}, grpcStatusErrorf(codes.Internal, "reload route catalog: %v", err)
		}
		if snapshot.Version >= minVersion {
			return snapshot, nil
		}
		last = snapshot
		if attempt == attempts-1 {
			break
		}
		if err := waitWithContext(ctx, interval); err != nil {
			return distribution.CatalogSnapshot{}, err
		}
	}
	return distribution.CatalogSnapshot{}, grpcStatusErrorf(
		codes.Internal,
		"catalog split committed but local snapshot is stale: got %d, want at least %d",
		last.Version,
		minVersion,
	)
}

func waitWithContext(ctx context.Context, delay time.Duration) error {
	if ctx == nil {
		ctx = context.Background()
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return errors.WithStack(status.FromContextError(ctx.Err()).Err())
	case <-timer.C:
		return nil
	}
}

func (s *DistributionServer) applyEngineSnapshot(snapshot distribution.CatalogSnapshot) error {
	if s.engine == nil {
		return grpcStatusError(codes.FailedPrecondition, errDistributionEngineNotConfigured.Error())
	}
	if err := s.engine.ApplySnapshot(snapshot); err != nil {
		return grpcStatusErrorf(codes.Internal, "apply engine snapshot: %v", err)
	}
	return nil
}

func (s *DistributionServer) updateSplitJobViaCoordinator(
	ctx context.Context,
	jobID uint64,
	transition func(distribution.SplitJob) (distribution.SplitJob, error),
) error {
	expected, readTS, err := s.catalog.LiveSplitJobForUpdate(ctx, jobID)
	if err != nil {
		return splitJobCatalogStatusError(err)
	}
	next, err := transition(expected)
	if err != nil {
		return splitJobCatalogStatusError(err)
	}
	if distribution.SplitJobsEquivalent(expected, next) {
		return nil
	}
	encoded, err := distribution.EncodeSplitJob(next)
	if err != nil {
		return splitJobCatalogStatusError(err)
	}
	key := distribution.CatalogSplitJobKey(jobID)
	if _, err := s.coordinator.Dispatch(ctx, &kv.OperationGroup[kv.OP]{
		Elems: []*kv.Elem[kv.OP]{{
			Op:    kv.Put,
			Key:   key,
			Value: encoded,
		}},
		IsTxn:    true,
		StartTS:  readTS,
		ReadKeys: [][]byte{key},
	}); err != nil {
		return splitJobCoordinatorStatusError(err)
	}
	return nil
}

func (s *DistributionServer) completeSplitJobTargetPromotionViaCoordinator(
	ctx context.Context,
	expectedVersion uint64,
	expected distribution.SplitJob,
	nowMs int64,
) (distribution.CatalogSnapshot, distribution.SplitJob, error) {
	if err := s.requirePromotionCompletionDeps(); err != nil {
		return distribution.CatalogSnapshot{}, distribution.SplitJob{}, err
	}
	snapshot, current, alreadyDone, err := s.loadPromotionCompletionCandidate(ctx, expectedVersion, expected, nowMs)
	if err != nil {
		return distribution.CatalogSnapshot{}, distribution.SplitJob{}, err
	}
	if alreadyDone {
		return snapshot, current, nil
	}
	completion, err := distribution.CompleteTargetPromotionState(expected, snapshot.Routes, nowMs)
	if err != nil {
		return distribution.CatalogSnapshot{}, distribution.SplitJob{}, splitJobPromotionStatusError(err)
	}
	return s.dispatchPromotionCompletion(ctx, snapshot, expectedVersion, expected.JobID, completion)
}

func (s *DistributionServer) requirePromotionCompletionDeps() error {
	if s.catalog == nil {
		return grpcStatusError(codes.FailedPrecondition, errDistributionCatalogNotConfigured.Error())
	}
	if s.coordinator == nil {
		return grpcStatusError(codes.FailedPrecondition, errDistributionCoordinatorRequired.Error())
	}
	return nil
}

func (s *DistributionServer) loadPromotionCompletionCandidate(
	ctx context.Context,
	expectedVersion uint64,
	expected distribution.SplitJob,
	nowMs int64,
) (distribution.CatalogSnapshot, distribution.SplitJob, bool, error) {
	snapshot, err := s.loadCatalogSnapshot(ctx)
	if err != nil {
		return distribution.CatalogSnapshot{}, distribution.SplitJob{}, false, err
	}
	current, found, err := s.catalog.SplitJobAt(ctx, expected.JobID, snapshot.ReadTS)
	if err != nil {
		return distribution.CatalogSnapshot{}, distribution.SplitJob{}, false, splitJobPromotionStatusError(err)
	}
	if !found {
		return distribution.CatalogSnapshot{}, distribution.SplitJob{}, false, splitJobPromotionStatusError(distribution.ErrCatalogSplitJobConflict)
	}
	if snapshot.Version == expectedVersion && distribution.SplitJobsEquivalent(current, expected) {
		return snapshot, current, false, nil
	}
	job, err := completedPromotionRetryJob(snapshot.Routes, expected, current, nowMs)
	if err != nil {
		return distribution.CatalogSnapshot{}, distribution.SplitJob{}, false, splitJobPromotionStatusError(err)
	}
	if job.TargetPromotionDone {
		return snapshot, job, true, nil
	}
	if snapshot.Version != expectedVersion {
		return distribution.CatalogSnapshot{}, distribution.SplitJob{}, false, splitJobPromotionStatusError(distribution.ErrCatalogVersionMismatch)
	}
	return distribution.CatalogSnapshot{}, distribution.SplitJob{}, false, splitJobPromotionStatusError(distribution.ErrCatalogSplitJobConflict)
}

func completedPromotionRetryJob(
	routes []distribution.RouteDescriptor,
	expected distribution.SplitJob,
	current distribution.SplitJob,
	nowMs int64,
) (distribution.SplitJob, error) {
	matches, err := splitJobPromotionMatchesExpected(expected, current)
	if err != nil {
		return distribution.SplitJob{}, errors.WithStack(err)
	}
	if !matches {
		return distribution.SplitJob{}, nil
	}
	completion, err := distribution.CompleteTargetPromotionState(current, routes, nowMs)
	if err != nil {
		return distribution.SplitJob{}, errors.WithStack(err)
	}
	return completion.Job, nil
}

func (s *DistributionServer) dispatchPromotionCompletion(
	ctx context.Context,
	snapshot distribution.CatalogSnapshot,
	expectedVersion uint64,
	jobID uint64,
	completion distribution.TargetPromotionCompletion,
) (distribution.CatalogSnapshot, distribution.SplitJob, error) {
	if !completion.Changed {
		return snapshot, completion.Job, nil
	}
	commitTS, err := s.nextPromotionCompleteCommitTS(snapshot.ReadTS)
	if err != nil {
		return distribution.CatalogSnapshot{}, distribution.SplitJob{}, err
	}
	completion.Job.PromotionCompletedTS = commitTS
	ops, err := s.buildPromotionCompleteOps(expectedVersion, completion)
	if err != nil {
		return distribution.CatalogSnapshot{}, distribution.SplitJob{}, splitJobPromotionStatusError(err)
	}
	jobKey := distribution.CatalogSplitJobKey(jobID)
	if _, err := s.coordinator.Dispatch(ctx, &kv.OperationGroup[kv.OP]{
		Elems:    ops,
		IsTxn:    true,
		StartTS:  snapshot.ReadTS,
		CommitTS: commitTS,
		ReadKeys: [][]byte{
			distribution.CatalogVersionKey(),
			jobKey,
		},
	}); err != nil {
		return distribution.CatalogSnapshot{}, distribution.SplitJob{}, splitJobCoordinatorStatusError(err)
	}
	loaded, err := s.loadCatalogSnapshotAtLeastVersion(ctx, expectedVersion+1)
	if err != nil {
		return distribution.CatalogSnapshot{}, distribution.SplitJob{}, err
	}
	return loaded, completion.Job, nil
}

func (s *DistributionServer) nextPromotionCompleteCommitTS(readTS uint64) (uint64, error) {
	if readTS == math.MaxUint64 {
		return 0, grpcStatusError(codes.Internal, distribution.ErrCatalogVersionOverflow.Error())
	}
	clock := s.coordinator.Clock()
	if clock == nil {
		return readTS + 1, nil
	}
	clock.Observe(readTS)
	commitTS, err := clock.NextFenced()
	if err != nil {
		return 0, grpcStatusErrorf(codes.FailedPrecondition, "allocate promotion completion timestamp: %v", err)
	}
	if commitTS > readTS {
		return commitTS, nil
	}
	clock.Observe(readTS)
	commitTS, err = clock.NextFenced()
	if err != nil {
		return 0, grpcStatusErrorf(codes.FailedPrecondition, "allocate promotion completion timestamp: %v", err)
	}
	if commitTS <= readTS {
		return 0, grpcStatusError(codes.Internal, "promotion completion timestamp did not advance")
	}
	return commitTS, nil
}

func (s *DistributionServer) buildPromotionCompleteOps(
	expectedVersion uint64,
	completion distribution.TargetPromotionCompletion,
) ([]*kv.Elem[kv.OP], error) {
	if expectedVersion == math.MaxUint64 {
		return nil, errors.WithStack(distribution.ErrCatalogVersionOverflow)
	}
	ops := make([]*kv.Elem[kv.OP], 0, len(completion.ClearedRouteIDs)+promotionCompleteBaseOpCount)
	for _, routeID := range completion.ClearedRouteIDs {
		route, ok := promotionCompleteRouteByID(completion.Routes, routeID)
		if !ok {
			return nil, errors.WithStack(distribution.ErrMigrationPromotionTargetAbsent)
		}
		encoded, err := distribution.EncodeRouteDescriptorForCatalogWrite(route, s.catalog.AllowsRouteDescriptorV2Writes())
		if err != nil {
			return nil, errors.WithStack(err)
		}
		ops = append(ops, &kv.Elem[kv.OP]{
			Op:    kv.Put,
			Key:   distribution.CatalogRouteKey(route.RouteID),
			Value: encoded,
		})
	}
	ops = append(ops, &kv.Elem[kv.OP]{
		Op:    kv.Put,
		Key:   distribution.CatalogVersionKey(),
		Value: distribution.EncodeCatalogVersion(expectedVersion + 1),
	})
	encodedJob, err := distribution.EncodeSplitJob(completion.Job)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	ops = append(ops, &kv.Elem[kv.OP]{
		Op:    kv.Put,
		Key:   distribution.CatalogSplitJobKey(completion.Job.JobID),
		Value: encodedJob,
	})
	return ops, nil
}

func promotionCompleteRouteByID(routes []distribution.RouteDescriptor, routeID uint64) (distribution.RouteDescriptor, bool) {
	for _, route := range routes {
		if route.RouteID == routeID {
			return route, true
		}
	}
	return distribution.RouteDescriptor{}, false
}

func splitJobPromotionMatchesExpected(expected, current distribution.SplitJob) (bool, error) {
	if expected.TargetPromotionDone || !current.TargetPromotionDone || current.PromotionCompletedTS == 0 {
		return false, nil
	}
	normalized := distribution.CloneSplitJob(current)
	normalized.TargetPromotionDone = expected.TargetPromotionDone
	normalized.PromotionCompletedTS = expected.PromotionCompletedTS
	normalized.UpdatedAtMs = expected.UpdatedAtMs
	expectedRaw, err := distribution.EncodeSplitJob(expected)
	if err != nil {
		return false, errors.WithStack(err)
	}
	normalizedRaw, err := distribution.EncodeSplitJob(normalized)
	if err != nil {
		return false, errors.WithStack(err)
	}
	return bytes.Equal(expectedRaw, normalizedRaw), nil
}

func (s *DistributionServer) verifyKnownTargetGroup(groupID uint64) error {
	if len(s.knownRaftGroups) == 0 {
		return grpcStatusError(codes.FailedPrecondition, errDistributionRaftGroupsNotKnown.Error())
	}
	if _, ok := s.knownRaftGroups[groupID]; !ok {
		return grpcStatusError(codes.InvalidArgument, errDistributionUnknownTargetGroup.Error())
	}
	return nil
}

func (s *DistributionServer) createSplitJobViaCoordinator(ctx context.Context, readTS uint64, job distribution.SplitJob) error {
	if job.JobID == math.MaxUint64 {
		return splitJobCatalogStatusError(distribution.ErrCatalogSplitJobIDOverflow)
	}
	encoded, err := distribution.EncodeSplitJob(job)
	if err != nil {
		return splitJobCatalogStatusError(err)
	}
	jobKey := distribution.CatalogSplitJobKey(job.JobID)
	nextIDKey := distribution.CatalogNextSplitJobIDKey()
	if _, err := s.coordinator.Dispatch(ctx, &kv.OperationGroup[kv.OP]{
		Elems: []*kv.Elem[kv.OP]{
			{
				Op:    kv.Put,
				Key:   jobKey,
				Value: encoded,
			},
			{
				Op:    kv.Put,
				Key:   nextIDKey,
				Value: distribution.EncodeCatalogNextSplitJobID(job.JobID + 1),
			},
		},
		IsTxn:   true,
		StartTS: readTS,
		ReadKeys: [][]byte{
			jobKey,
			nextIDKey,
			distribution.CatalogVersionKey(),
			distribution.CatalogRouteKey(job.SourceRouteID),
		},
	}); err != nil {
		return splitJobCoordinatorStatusError(err)
	}
	return nil
}

func (s *DistributionServer) verifySplitMigrationCapability(ctx context.Context) error {
	if s.migrationCapabilityGate == nil {
		return grpcStatusError(codes.FailedPrecondition, errDistributionClusterNotReady.Error())
	}
	if err := s.migrationCapabilityGate(ctx); err != nil {
		return err
	}
	return nil
}

func validateExpectedCatalogVersion(currentVersion, expectedVersion uint64) error {
	if currentVersion != expectedVersion {
		return grpcStatusError(codes.Aborted, errDistributionCatalogConflict.Error())
	}
	return nil
}

func validateSplitKey(parent distribution.RouteDescriptor, splitKey []byte) error {
	startCmp := bytes.Compare(splitKey, parent.Start)
	if startCmp == 0 {
		return grpcStatusError(codes.InvalidArgument, errDistributionSplitKeyAtBoundary.Error())
	}
	if startCmp < 0 {
		return grpcStatusError(codes.InvalidArgument, errDistributionInvalidSplitKey.Error())
	}
	if parent.End == nil {
		return nil
	}

	endCmp := bytes.Compare(splitKey, parent.End)
	if endCmp == 0 {
		return grpcStatusError(codes.InvalidArgument, errDistributionSplitKeyAtBoundary.Error())
	}
	if endCmp > 0 {
		return grpcStatusError(codes.InvalidArgument, errDistributionInvalidSplitKey.Error())
	}
	return nil
}

func (s *DistributionServer) splitJobOverlapReadKeys(ctx context.Context, snapshot distribution.CatalogSnapshot, parent distribution.RouteDescriptor) ([][]byte, error) {
	jobs, err := s.catalog.ListSplitJobsAt(ctx, snapshot.ReadTS)
	if err != nil {
		return nil, grpcStatusErrorf(codes.Internal, "load split jobs: %v", err)
	}
	readKeys := splitJobReadFenceKeys(jobs)
	for _, job := range jobs {
		if !splitJobIsLive(job) {
			continue
		}
		for _, interval := range liveSplitJobIntervals(job, snapshot.Routes) {
			if routeRangeIntersects(parent.Start, parent.End, interval.start, interval.end) {
				return nil, grpcStatusError(codes.Aborted, distribution.ErrSplitJobOverlap.Error())
			}
		}
	}
	return readKeys, nil
}

func splitJobReadFenceKeys(jobs []distribution.SplitJob) [][]byte {
	readKeys := make([][]byte, 0, len(jobs)+1)
	readKeys = append(readKeys, distribution.CatalogNextSplitJobIDKey())
	for _, job := range jobs {
		if splitJobIsLive(job) {
			readKeys = append(readKeys, distribution.CatalogSplitJobKey(job.JobID))
		}
	}
	return readKeys
}

func (s *DistributionServer) rejectLiveSplitJob(ctx context.Context, snapshot distribution.CatalogSnapshot, parent distribution.RouteDescriptor) error {
	jobs, err := s.catalog.ListSplitJobsAt(ctx, snapshot.ReadTS)
	if err != nil {
		return grpcStatusErrorf(codes.Internal, "load split jobs: %v", err)
	}
	liveJobs := 0
	for _, job := range jobs {
		if !splitJobIsLive(job) {
			continue
		}
		liveJobs++
		for _, interval := range liveSplitJobIntervals(job, snapshot.Routes) {
			if routeRangeIntersects(parent.Start, parent.End, interval.start, interval.end) {
				return grpcStatusError(codes.Aborted, distribution.ErrSplitJobOverlap.Error())
			}
		}
	}
	if liveJobs > 0 {
		return grpcStatusError(codes.ResourceExhausted, distribution.ErrTooManyInFlightSplitJobs.Error())
	}
	return nil
}

func splitJobIsLive(job distribution.SplitJob) bool {
	return job.Phase != distribution.SplitJobPhaseDone && job.Phase != distribution.SplitJobPhaseAbandoned
}

type routeInterval struct {
	start []byte
	end   []byte
}

const initialLiveSplitJobIntervalCapacity = 2

func liveSplitJobIntervals(job distribution.SplitJob, routes []distribution.RouteDescriptor) []routeInterval {
	out := make([]routeInterval, 0, initialLiveSplitJobIntervalCapacity)
	for _, route := range routes {
		switch {
		case route.RouteID == job.SourceRouteID:
			out = append(out, routeInterval{
				start: distribution.CloneBytes(job.SplitKey),
				end:   distribution.CloneBytes(route.End),
			})
		case route.ParentRouteID == job.SourceRouteID && routeRangeIntersects(route.Start, route.End, job.SplitKey, nil):
			out = append(out, routeInterval{
				start: distribution.CloneBytes(route.Start),
				end:   distribution.CloneBytes(route.End),
			})
		case job.JobID != 0 && route.MigrationJobID == job.JobID:
			out = append(out, routeInterval{
				start: distribution.CloneBytes(route.Start),
				end:   distribution.CloneBytes(route.End),
			})
		}
	}
	return out
}

func routeRangeIntersects(aStart, aEnd, bStart, bEnd []byte) bool {
	if aEnd != nil && bytes.Compare(aEnd, bStart) <= 0 {
		return false
	}
	if bEnd != nil && bytes.Compare(bEnd, aStart) <= 0 {
		return false
	}
	return true
}

func splitJobPassesListFilter(job distribution.SplitJob, sinceTerminalAtMs uint64, phaseFilter string) bool {
	if sinceTerminalAtMs > 0 && job.TerminalAtMs > 0 && uint64(job.TerminalAtMs) < sinceTerminalAtMs {
		return false
	}
	if phaseFilter == "" {
		return true
	}
	return normalizeSplitJobPhaseFilter(pb.SplitJobPhase(job.Phase).String()) == phaseFilter ||
		normalizeSplitJobPhaseFilter(strings.TrimPrefix(pb.SplitJobPhase(job.Phase).String(), "SPLIT_JOB_PHASE_")) == phaseFilter
}

func normalizeSplitJobPhaseFilter(phase string) string {
	phase = strings.TrimSpace(phase)
	if phase == "" {
		return ""
	}
	phase = strings.ReplaceAll(phase, "-", "_")
	phase = strings.ReplaceAll(phase, " ", "_")
	return strings.ToUpper(phase)
}

func validSplitJobPhaseFilter(phase string) bool {
	for _, candidate := range pb.SplitJobPhase_name {
		full := normalizeSplitJobPhaseFilter(candidate)
		short := normalizeSplitJobPhaseFilter(strings.TrimPrefix(candidate, "SPLIT_JOB_PHASE_"))
		if phase == full || phase == short {
			return true
		}
	}
	return false
}

func splitJobCatalogStatusError(err error) error {
	switch {
	case errors.Is(err, distribution.ErrCatalogSplitJobIDRequired):
		return grpcStatusError(codes.InvalidArgument, distribution.ErrCatalogSplitJobIDRequired.Error())
	case errors.Is(err, distribution.ErrCatalogSplitJobNotFound):
		return grpcStatusError(codes.NotFound, distribution.ErrCatalogSplitJobNotFound.Error())
	case errors.Is(err, distribution.ErrCatalogSplitJobCannotRetry),
		errors.Is(err, distribution.ErrCatalogSplitJobCannotAbandon):
		return grpcStatusError(codes.FailedPrecondition, err.Error())
	case errors.Is(err, distribution.ErrCatalogSplitJobConflict):
		return grpcStatusError(codes.Aborted, distribution.ErrCatalogSplitJobConflict.Error())
	default:
		return grpcStatusErrorf(codes.Internal, "split job catalog: %v", err)
	}
}

func splitMigrationRangeStatusError(err error) error {
	switch {
	case errors.Is(err, distribution.ErrMigrationReservedRange),
		errors.Is(err, distribution.ErrMigrationInvalidRoute):
		return grpcStatusError(codes.InvalidArgument, err.Error())
	default:
		return grpcStatusErrorf(codes.Internal, "validate split migration range: %v", err)
	}
}

func splitJobCoordinatorStatusError(err error) error {
	switch {
	case errors.Is(err, store.ErrWriteConflict):
		return grpcStatusError(codes.Aborted, distribution.ErrCatalogSplitJobConflict.Error())
	case splitJobCoordinatorLeadershipError(err):
		return grpcStatusError(codes.FailedPrecondition, errDistributionNotLeader.Error())
	default:
		return grpcStatusErrorf(codes.Internal, "commit split job mutation: %v", err)
	}
}

func splitJobPromotionStatusError(err error) error {
	switch {
	case errors.Is(err, distribution.ErrCatalogVersionMismatch),
		errors.Is(err, distribution.ErrCatalogSplitJobConflict),
		errors.Is(err, store.ErrWriteConflict):
		return grpcStatusError(codes.Aborted, err.Error())
	case errors.Is(err, distribution.ErrMigrationPromotionNotReady),
		errors.Is(err, distribution.ErrMigrationPromotionTargetAbsent):
		return grpcStatusError(codes.FailedPrecondition, err.Error())
	case errors.Is(err, distribution.ErrMigrationInvalidRoute),
		errors.Is(err, distribution.ErrCatalogRouteV2WriteDisabled):
		return grpcStatusError(codes.InvalidArgument, err.Error())
	default:
		return splitJobCatalogStatusError(err)
	}
}

func splitJobListPage(jobs []distribution.SplitJob, req *pb.ListSplitJobsRequest, phaseFilter string) (*pb.ListSplitJobsResponse, error) {
	filtered := make([]distribution.SplitJob, 0, len(jobs))
	for _, job := range jobs {
		if splitJobPassesListFilter(job, req.GetSinceTerminalAtMs(), phaseFilter) {
			filtered = append(filtered, job)
		}
	}
	sortSplitJobsForList(filtered)

	start, err := splitJobListStartIndex(filtered, req.GetPageCursor())
	if err != nil {
		return nil, err
	}
	end := start + listSplitJobsDefaultPageSize
	if end > len(filtered) {
		end = len(filtered)
	}

	resp := &pb.ListSplitJobsResponse{
		Jobs: make([]*pb.SplitJob, 0, end-start),
	}
	for _, job := range filtered[start:end] {
		resp.Jobs = append(resp.Jobs, distribution.SplitJobToProto(job))
	}
	if end < len(filtered) {
		resp.NextPageCursor = encodeSplitJobListCursor(filtered[end-1])
	}
	return resp, nil
}

func sortSplitJobsForList(jobs []distribution.SplitJob) {
	sort.Slice(jobs, func(i, j int) bool {
		left, right := jobs[i], jobs[j]
		leftLive := left.TerminalAtMs <= 0
		rightLive := right.TerminalAtMs <= 0
		if leftLive != rightLive {
			return leftLive
		}
		if leftLive {
			if left.UpdatedAtMs != right.UpdatedAtMs {
				return left.UpdatedAtMs > right.UpdatedAtMs
			}
			return left.JobID > right.JobID
		}
		if left.TerminalAtMs != right.TerminalAtMs {
			return left.TerminalAtMs > right.TerminalAtMs
		}
		return left.JobID > right.JobID
	})
}

func splitJobListStartIndex(jobs []distribution.SplitJob, cursor []byte) (int, error) {
	if len(cursor) == 0 {
		return 0, nil
	}
	terminalAtMs, jobID, err := decodeSplitJobListCursor(cursor)
	if err != nil {
		return 0, err
	}
	for i, job := range jobs {
		if job.JobID == jobID && splitJobListCursorTerminalAtMs(job) == terminalAtMs {
			return i + 1, nil
		}
	}
	return 0, grpcStatusError(codes.InvalidArgument, "split job page cursor does not match the filtered result set")
}

func encodeSplitJobListCursor(job distribution.SplitJob) []byte {
	cursor := make([]byte, splitJobListCursorEncodedBytes)
	cursor[0] = splitJobListCursorVersion
	binary.BigEndian.PutUint64(
		cursor[splitJobListCursorTerminalOff:splitJobListCursorJobIDOff],
		splitJobListCursorTerminalAtMs(job),
	)
	binary.BigEndian.PutUint64(cursor[splitJobListCursorJobIDOff:], job.JobID)
	return cursor
}

func decodeSplitJobListCursor(cursor []byte) (uint64, uint64, error) {
	if len(cursor) != splitJobListCursorEncodedBytes || cursor[0] != splitJobListCursorVersion {
		return 0, 0, grpcStatusError(codes.InvalidArgument, "invalid split job page cursor")
	}
	terminalAtMs := binary.BigEndian.Uint64(cursor[splitJobListCursorTerminalOff:splitJobListCursorJobIDOff])
	jobID := binary.BigEndian.Uint64(cursor[splitJobListCursorJobIDOff:])
	if jobID == 0 {
		return 0, 0, grpcStatusError(codes.InvalidArgument, "invalid split job page cursor")
	}
	return terminalAtMs, jobID, nil
}

func splitJobListCursorTerminalAtMs(job distribution.SplitJob) uint64 {
	if job.TerminalAtMs <= 0 {
		return 0
	}
	return uint64(job.TerminalAtMs)
}

func splitJobCoordinatorLeadershipError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, kv.ErrLeaderNotFound) ||
		errors.Is(err, raftengine.ErrNotLeader) ||
		errors.Is(err, raftengine.ErrLeadershipLost) ||
		errors.Is(err, raftengine.ErrLeadershipTransferInProgress) {
		return true
	}
	return hasSplitJobLeaderErrorSuffix(err.Error())
}

var splitJobLeaderErrorPhrases = []string{
	"not leader",
	"leader not found",
	"leadership lost",
	"leadership transfer in progress",
}

func hasSplitJobLeaderErrorSuffix(msg string) bool {
	for _, phrase := range splitJobLeaderErrorPhrases {
		if len(msg) >= len(phrase) && strings.EqualFold(msg[len(msg)-len(phrase):], phrase) {
			return true
		}
	}
	return false
}

func splitCatalogRoutes(
	parent distribution.RouteDescriptor,
	splitKey []byte,
	leftID uint64,
	rightID uint64,
) (distribution.RouteDescriptor, distribution.RouteDescriptor) {
	// parent and splitKey are already cloned before this point and are immutable here.
	left := distribution.RouteDescriptor{
		RouteID:                leftID,
		Start:                  parent.Start,
		End:                    splitKey,
		GroupID:                parent.GroupID,
		State:                  parent.State,
		ParentRouteID:          parent.RouteID,
		StagedVisibilityActive: parent.StagedVisibilityActive,
		MigrationJobID:         parent.MigrationJobID,
		MinWriteTSExclusive:    parent.MinWriteTSExclusive,
	}
	right := distribution.RouteDescriptor{
		RouteID:                rightID,
		Start:                  splitKey,
		End:                    parent.End,
		GroupID:                parent.GroupID,
		State:                  parent.State,
		ParentRouteID:          parent.RouteID,
		StagedVisibilityActive: parent.StagedVisibilityActive,
		MigrationJobID:         parent.MigrationJobID,
		MinWriteTSExclusive:    parent.MinWriteTSExclusive,
	}
	return left, right
}

func (s *DistributionServer) allocateChildRouteIDs(ctx context.Context, readTS uint64, routes []distribution.RouteDescriptor) (uint64, uint64, error) {
	nextRouteID, err := s.catalog.NextRouteIDAt(ctx, readTS)
	if err != nil {
		return 0, 0, grpcStatusErrorf(codes.Internal, "load next route id: %v", err)
	}

	// Keep floor computation shared with catalog persistence logic.
	minNextRouteID, err := distribution.NextRouteIDFloor(routes)
	if err != nil {
		if errors.Is(err, distribution.ErrCatalogRouteIDOverflow) {
			return 0, 0, grpcStatusError(codes.Internal, errDistributionRouteIDOverflow.Error())
		}
		return 0, 0, grpcStatusErrorf(codes.Internal, "compute next route id floor: %v", err)
	}
	if nextRouteID < minNextRouteID {
		nextRouteID = minNextRouteID
	}

	if nextRouteID > math.MaxUint64-(childRouteCount-1) {
		return 0, 0, grpcStatusError(codes.Internal, errDistributionRouteIDOverflow.Error())
	}
	leftID := nextRouteID
	rightID := nextRouteID + 1
	return leftID, rightID, nil
}

func findRouteByID(routes []distribution.RouteDescriptor, routeID uint64) (distribution.RouteDescriptor, bool) {
	for _, route := range routes {
		if route.RouteID == routeID {
			return distribution.CloneRouteDescriptor(route), true
		}
	}
	return distribution.RouteDescriptor{}, false
}

func toProtoRouteDescriptors(routes []distribution.RouteDescriptor) []*pb.RouteDescriptor {
	out := make([]*pb.RouteDescriptor, 0, len(routes))
	for _, route := range routes {
		out = append(out, toProtoRouteDescriptor(route))
	}
	return out
}

func toProtoRouteDescriptor(route distribution.RouteDescriptor) *pb.RouteDescriptor {
	return &pb.RouteDescriptor{
		RouteId:                route.RouteID,
		Start:                  distribution.CloneBytes(route.Start),
		End:                    distribution.CloneBytes(route.End),
		RaftGroupId:            route.GroupID,
		State:                  toProtoRouteState(route.State),
		ParentRouteId:          route.ParentRouteID,
		StagedVisibilityActive: route.StagedVisibilityActive,
		MigrationJobId:         route.MigrationJobID,
		MinWriteTsExclusive:    route.MinWriteTSExclusive,
	}
}

func toProtoRoute(route distribution.Route) *pb.RouteDescriptor {
	return &pb.RouteDescriptor{
		RouteId:                route.RouteID,
		Start:                  distribution.CloneBytes(route.Start),
		End:                    distribution.CloneBytes(route.End),
		RaftGroupId:            route.GroupID,
		State:                  toProtoRouteState(route.State),
		StagedVisibilityActive: route.StagedVisibilityActive,
		MigrationJobId:         route.MigrationJobID,
		MinWriteTsExclusive:    route.MinWriteTSExclusive,
	}
}

func toProtoRouteState(state distribution.RouteState) pb.RouteState {
	switch state {
	case distribution.RouteStateActive:
		return pb.RouteState_ROUTE_STATE_ACTIVE
	case distribution.RouteStateWriteFenced:
		return pb.RouteState_ROUTE_STATE_WRITE_FENCED
	case distribution.RouteStateMigratingSource:
		return pb.RouteState_ROUTE_STATE_MIGRATING_SOURCE
	case distribution.RouteStateMigratingTarget:
		return pb.RouteState_ROUTE_STATE_MIGRATING_TARGET
	default:
		return pb.RouteState_ROUTE_STATE_UNSPECIFIED
	}
}

func grpcStatusError(code codes.Code, msg string) error {
	return errors.WithStack(status.Error(code, msg))
}

func grpcStatusErrorf(code codes.Code, format string, args ...any) error {
	return errors.WithStack(status.Errorf(code, format, args...))
}
