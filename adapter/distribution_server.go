package adapter

import (
	"bytes"
	"context"
	"math"
	"sync"
	"time"

	"github.com/bootjp/elastickv/distribution"
	"github.com/bootjp/elastickv/kv"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// DistributionServer serves distribution related gRPC APIs.
type DistributionServer struct {
	mu          sync.Mutex
	engine      *distribution.Engine
	catalog     *distribution.CatalogStore
	coordinator kv.Coordinator
	readTracker *kv.ActiveTimestampTracker
	reloadRetry struct {
		attempts int
		interval time.Duration
	}
	pb.UnimplementedDistributionServer
}

// DistributionServerOption configures DistributionServer behavior.
type DistributionServerOption func(*DistributionServer)

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
	childRouteCount      = 2
	splitMutationOpCount = childRouteCount + 3
)

var (
	defaultCatalogReloadRetryAttempts = 20
	defaultCatalogReloadRetryInterval = 10 * time.Millisecond

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

// UpdateRoute allows updating route information.
func (s *DistributionServer) UpdateRoute(start, end []byte, group uint64) {
	s.engine.UpdateRoute(start, end, group)
}

// GetRoute returns route for a key.
func (s *DistributionServer) GetRoute(ctx context.Context, req *pb.GetRouteRequest) (*pb.GetRouteResponse, error) {
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
	readPin := s.pinReadTS(snapshot.ReadTS)
	defer readPin.Release()
	if err := validateExpectedCatalogVersion(snapshot.Version, req.GetExpectedCatalogVersion()); err != nil {
		return nil, err
	}

	parent, _, found := findRouteByID(snapshot.Routes, req.GetRouteId())
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
		return nil
	}
	return s.readTracker.Pin(ts)
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
		if job.TerminalAtMs > 0 {
			readKeys = append(readKeys, distribution.CatalogSplitJobHistoryKey(job.TerminalAtMs, job.JobID))
			continue
		}
		readKeys = append(readKeys, distribution.CatalogSplitJobKey(job.JobID))
	}
	return readKeys
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

func findRouteByID(routes []distribution.RouteDescriptor, routeID uint64) (distribution.RouteDescriptor, int, bool) {
	for i, route := range routes {
		if route.RouteID == routeID {
			return distribution.CloneRouteDescriptor(route), i, true
		}
	}
	return distribution.RouteDescriptor{}, -1, false
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
