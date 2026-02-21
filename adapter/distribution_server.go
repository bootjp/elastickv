package adapter

import (
	"bytes"
	"context"
	"math"
	"sync"

	"github.com/bootjp/elastickv/distribution"
	"github.com/bootjp/elastickv/kv"
	pb "github.com/bootjp/elastickv/proto"
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

const (
	childRouteCount      = 2
	splitMutationOpCount = childRouteCount + 3
)

var (
	errDistributionCatalogNotConfigured = errors.New("route catalog is not configured")
	errDistributionUnknownRoute         = errors.New("unknown route")
	errDistributionInvalidSplitKey      = errors.New("invalid split key")
	errDistributionSplitKeyAtBoundary   = errors.New("split key at route boundary")
	errDistributionCatalogConflict      = errors.New("catalog version conflict")
	errDistributionRouteIDOverflow      = errors.New("route id overflow")
	errDistributionNotLeader            = errors.New("not leader for distribution catalog")
	errDistributionCoordinatorRequired  = errors.New("distribution coordinator is not configured")
	errDistributionEngineNotConfigured  = errors.New("distribution engine is not configured")
)

// NewDistributionServer creates a new server.
func NewDistributionServer(e *distribution.Engine, catalog *distribution.CatalogStore, opts ...DistributionServerOption) *DistributionServer {
	s := &DistributionServer{
		engine:  e,
		catalog: catalog,
	}
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

// SplitRange splits a route into two child routes in the same raft group.
func (s *DistributionServer) SplitRange(ctx context.Context, req *pb.SplitRangeRequest) (*pb.SplitRangeResponse, error) {
	// SplitRange performs a read-modify-write cycle across catalog and engine.
	// Serialize it per server instance to keep these updates consistent.
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.verifyCatalogLeader(); err != nil {
		return nil, err
	}

	snapshot, err := s.loadCatalogSnapshot(ctx)
	if err != nil {
		return nil, err
	}
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

	leftID, rightID, err := s.allocateChildRouteIDs(ctx, snapshot.ReadTS, snapshot.Routes)
	if err != nil {
		return nil, err
	}
	left, right := splitCatalogRoutes(parent, splitKey, leftID, rightID)

	saved, err := s.saveSplitResult(ctx, snapshot.ReadTS, req.GetExpectedCatalogVersion(), parent.RouteID, left, right)
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

func (s *DistributionServer) verifyCatalogLeader() error {
	if s.coordinator == nil {
		return grpcStatusError(codes.FailedPrecondition, errDistributionCoordinatorRequired.Error())
	}
	key := distribution.CatalogVersionKey()
	if !s.coordinator.IsLeaderForKey(key) {
		return grpcStatusError(codes.FailedPrecondition, errDistributionNotLeader.Error())
	}
	if err := s.coordinator.VerifyLeaderForKey(key); err != nil {
		return grpcStatusErrorf(codes.FailedPrecondition, "verify catalog leader: %v", err)
	}
	return nil
}

func (s *DistributionServer) saveSplitResult(
	ctx context.Context,
	readTS uint64,
	expectedVersion uint64,
	parentID uint64,
	left distribution.RouteDescriptor,
	right distribution.RouteDescriptor,
) (distribution.CatalogSnapshot, error) {
	return s.saveSplitResultViaCoordinator(ctx, readTS, expectedVersion, parentID, left, right)
}

func (s *DistributionServer) saveSplitResultViaCoordinator(
	ctx context.Context,
	readTS uint64,
	expectedVersion uint64,
	parentID uint64,
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

	ops, err := buildCatalogSplitOps(parentID, left, right, nextVersion, nextRouteID)
	if err != nil {
		return distribution.CatalogSnapshot{}, grpcStatusErrorf(codes.Internal, "build split mutations: %v", err)
	}
	if _, err := s.coordinator.Dispatch(ctx, &kv.OperationGroup[kv.OP]{
		Elems:   ops,
		IsTxn:   true,
		StartTS: readTS,
	}); err != nil {
		return distribution.CatalogSnapshot{}, grpcStatusErrorf(codes.Internal, "commit split mutations: %v", err)
	}

	saved, err := s.catalog.Snapshot(ctx)
	if err != nil {
		return distribution.CatalogSnapshot{}, grpcStatusErrorf(codes.Internal, "reload route catalog: %v", err)
	}
	if saved.Version != nextVersion {
		return distribution.CatalogSnapshot{}, grpcStatusErrorf(
			codes.Internal,
			"unexpected catalog version after split: got %d, want %d",
			saved.Version,
			nextVersion,
		)
	}
	return saved, nil
}

func buildCatalogSplitOps(
	parentID uint64,
	left distribution.RouteDescriptor,
	right distribution.RouteDescriptor,
	nextVersion uint64,
	nextRouteID uint64,
) ([]*kv.Elem[kv.OP], error) {
	// SplitRange mutates the catalog surgically: delete one parent route, add two
	// children, bump the version, and advance the next-route-id counter.
	ops := make([]*kv.Elem[kv.OP], 0, splitMutationOpCount)
	ops = append(ops, &kv.Elem[kv.OP]{
		Op:  kv.Del,
		Key: distribution.CatalogRouteKey(parentID),
	})
	for _, route := range []distribution.RouteDescriptor{left, right} {
		encoded, err := distribution.EncodeRouteDescriptor(route)
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

func splitCatalogRoutes(
	parent distribution.RouteDescriptor,
	splitKey []byte,
	leftID uint64,
	rightID uint64,
) (distribution.RouteDescriptor, distribution.RouteDescriptor) {
	left := distribution.RouteDescriptor{
		RouteID:       leftID,
		Start:         distribution.CloneBytes(parent.Start),
		End:           distribution.CloneBytes(splitKey),
		GroupID:       parent.GroupID,
		State:         parent.State,
		ParentRouteID: parent.RouteID,
	}
	right := distribution.RouteDescriptor{
		RouteID:       rightID,
		Start:         distribution.CloneBytes(splitKey),
		End:           distribution.CloneBytes(parent.End),
		GroupID:       parent.GroupID,
		State:         parent.State,
		ParentRouteID: parent.RouteID,
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
			return cloneRouteDescriptor(route), i, true
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
		RouteId:       route.RouteID,
		Start:         distribution.CloneBytes(route.Start),
		End:           distribution.CloneBytes(route.End),
		RaftGroupId:   route.GroupID,
		State:         toProtoRouteState(route.State),
		ParentRouteId: route.ParentRouteID,
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

func cloneRouteDescriptor(route distribution.RouteDescriptor) distribution.RouteDescriptor {
	return distribution.RouteDescriptor{
		RouteID:       route.RouteID,
		Start:         distribution.CloneBytes(route.Start),
		End:           distribution.CloneBytes(route.End),
		GroupID:       route.GroupID,
		State:         route.State,
		ParentRouteID: route.ParentRouteID,
	}
}

func grpcStatusError(code codes.Code, msg string) error {
	return errors.WithStack(status.Error(code, msg))
}

func grpcStatusErrorf(code codes.Code, format string, args ...any) error {
	return errors.WithStack(status.Errorf(code, format, args...))
}
