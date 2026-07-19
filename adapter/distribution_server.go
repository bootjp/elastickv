package adapter

import (
	"bytes"
	"context"
	"log/slog"
	"math"
	"strings"
	"sync"
	"time"

	"github.com/bootjp/elastickv/distribution"
	"github.com/bootjp/elastickv/internal/fskeys"
	"github.com/bootjp/elastickv/kv"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/cockroachdb/errors"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// DistributionServer serves distribution related gRPC APIs.
type DistributionServer struct {
	mu                 sync.Mutex
	engine             *distribution.Engine
	catalog            *distribution.CatalogStore
	coordinator        kv.Coordinator
	timestampAllocator kv.TSOAllocator
	readTracker        *kv.ActiveTimestampTracker
	fsObserver         DistributionFilesystemObserver
	reloadRetry        struct {
		attempts int
		interval time.Duration
	}
	pb.UnimplementedDistributionServer
}

// DistributionServerOption configures DistributionServer behavior.
type DistributionServerOption func(*DistributionServer)

type DistributionFilesystemObserver interface {
	ObserveFilePinnedHotspot(reason string)
}

const DistributionFilePinnedHotspotSplitBoundary = "split_boundary"

// WithDistributionCoordinator configures the coordinator used for Raft-backed
// catalog mutations in SplitRange.
func WithDistributionCoordinator(coordinator kv.Coordinator) DistributionServerOption {
	return func(s *DistributionServer) {
		s.coordinator = coordinator
	}
}

// WithDistributionTimestampAllocator exposes the local dedicated TSO
// allocator through GetTimestamp. The allocator itself rejects followers, so
// clients can re-resolve the group-0 leader without a forwarding loop.
func WithDistributionTimestampAllocator(allocator kv.TSOAllocator) DistributionServerOption {
	return func(s *DistributionServer) {
		s.timestampAllocator = allocator
	}
}

func WithDistributionActiveTimestampTracker(tracker *kv.ActiveTimestampTracker) DistributionServerOption {
	return func(s *DistributionServer) {
		s.readTracker = tracker
	}
}

func WithDistributionFilesystemObserver(observer DistributionFilesystemObserver) DistributionServerOption {
	return func(s *DistributionServer) {
		s.fsObserver = observer
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
	r, ok := s.engine.GetRoute(kv.RouteKey(req.Key))
	if !ok {
		return &pb.GetRouteResponse{}, nil
	}
	return &pb.GetRouteResponse{
		Start:       r.Start,
		End:         r.End,
		RaftGroupId: r.GroupID,
	}, nil
}

// GetTimestamp returns the base of a consecutive timestamp window. When a
// dedicated allocator is configured, only the local group-0 leader can serve
// the request and the returned window is already durable in that Raft group.
func (s *DistributionServer) GetTimestamp(ctx context.Context, req *pb.GetTimestampRequest) (*pb.GetTimestampResponse, error) {
	count, minTimestamp, err := timestampRequestValues(req)
	if err != nil {
		return nil, err
	}
	activateCutover, activatePhaseD, err := timestampActivationValues(req)
	if err != nil {
		return nil, err
	}
	if s.timestampAllocator == nil {
		return s.legacyTimestampResponse(count, minTimestamp, activateCutover, activatePhaseD)
	}

	reservation, err := s.allocateTimestampReservation(ctx, count, minTimestamp, activateCutover, activatePhaseD)
	if err != nil {
		return nil, timestampRPCError(err)
	}
	if err := validateTimestampReservation(reservation, count, minTimestamp); err != nil {
		return nil, errors.WithStack(status.Error(codes.Internal, err.Error()))
	}
	return &pb.GetTimestampResponse{
		Timestamp:               reservation.Base,
		CommittedByDedicatedTso: true,
		Count:                   uint32(count), //nolint:gosec // count is bounded by MaxTSOBatchSize.
		PreviousAllocationFloor: reservation.PreviousAllocationFloor,
		CutoverActive:           reservation.CutoverActive,
		PhaseDActive:            reservation.PhaseDActive,
		PhaseDFloor:             reservation.PhaseDFloor,
	}, nil
}

func timestampActivationValues(req *pb.GetTimestampRequest) (bool, bool, error) {
	activateCutover := req != nil && req.GetActivateCutover()
	activatePhaseD := req != nil && req.GetActivatePhaseD()
	if activatePhaseD && !activateCutover {
		return false, false, errors.WithStack(status.Error(codes.InvalidArgument,
			"phase D activation requires cutover activation"))
	}
	return activateCutover, activatePhaseD, nil
}

func (s *DistributionServer) legacyTimestampResponse(
	count int,
	minTimestamp uint64,
	activateCutover bool,
	activatePhaseD bool,
) (*pb.GetTimestampResponse, error) {
	if count != 1 || minTimestamp != 0 || activateCutover || activatePhaseD {
		return nil, errors.WithStack(status.Error(codes.FailedPrecondition, "dedicated TSO allocator is not configured"))
	}
	if s.engine == nil {
		return nil, errors.WithStack(status.Error(codes.Unavailable, "distribution engine is not configured"))
	}
	return &pb.GetTimestampResponse{Timestamp: s.engine.NextTimestamp()}, nil
}

// ValidateTimestamp verifies a read/start timestamp against the durable M7
// allocation range. The local allocator performs the group-0 leader fence;
// followers reject so clients re-resolve rather than validating stale state.
func (s *DistributionServer) ValidateTimestamp(
	ctx context.Context,
	req *pb.ValidateTimestampRequest,
) (*pb.ValidateTimestampResponse, error) {
	if req == nil || req.GetTimestamp() == 0 {
		return nil, errors.WithStack(status.Error(codes.InvalidArgument, "timestamp is required"))
	}
	validator, ok := s.timestampAllocator.(kv.DurableTimestampValidator)
	if !ok {
		return nil, errors.WithStack(status.Error(codes.FailedPrecondition,
			"dedicated TSO timestamp validation is not configured"))
	}
	if err := validator.ValidateDurableTimestamp(ctx, req.GetTimestamp()); err != nil {
		return nil, timestampRPCError(err)
	}
	resp := &pb.ValidateTimestampResponse{Valid: true, PhaseDActive: true}
	if state, ok := s.timestampAllocator.(interface {
		PhaseDFloor() uint64
		AllocationFloor() uint64
	}); ok {
		resp.PhaseDFloor = state.PhaseDFloor()
		resp.AllocationFloor = state.AllocationFloor()
	}
	return resp, nil
}

func (s *DistributionServer) allocateTimestampReservation(
	ctx context.Context,
	count int,
	minTimestamp uint64,
	activateCutover bool,
	activatePhaseD bool,
) (kv.TSOReservation, error) {
	if allocator, ok := s.timestampAllocator.(kv.TSOReservationAllocator); ok {
		reservation, err := allocator.ReserveBatchAfter(ctx, count, minTimestamp, activateCutover, activatePhaseD)
		return reservation, errors.Wrap(err, "reserve dedicated TSO window")
	}
	reservation := kv.TSOReservation{Count: count}
	switch {
	case activateCutover || activatePhaseD:
		return reservation, errors.WithStack(status.Error(codes.FailedPrecondition,
			"TSO allocator does not support durable cutover"))
	case minTimestamp > 0:
		return reservation, errors.WithStack(status.Error(codes.FailedPrecondition,
			"TSO allocator does not support durable reservation metadata"))
	default:
		base, err := s.timestampAllocator.NextBatch(ctx, count)
		reservation.Base = base
		return reservation, errors.Wrap(err, "reserve TSO window")
	}
}

func validateTimestampWindow(base uint64, count int, minTimestamp uint64) error {
	if base == 0 || base <= minTimestamp {
		return errors.Errorf("dedicated TSO returned base %d at or below minimum %d", base, minTimestamp)
	}
	size := uint64(count) //nolint:gosec // count is positive and bounded by timestampRequestValues.
	if base > math.MaxUint64-(size-1) {
		return errors.Errorf("dedicated TSO returned overflowing window base=%d count=%d", base, count)
	}
	return nil
}

func validateTimestampReservation(reservation kv.TSOReservation, count int, minTimestamp uint64) error {
	if err := validateTimestampWindow(reservation.Base, count, minTimestamp); err != nil {
		return err
	}
	if reservation.Count != count {
		return errors.Errorf("dedicated TSO returned count %d, want %d", reservation.Count, count)
	}
	if reservation.PreviousAllocationFloor >= reservation.Base {
		return errors.Errorf("dedicated TSO returned base %d at or below previous floor %d",
			reservation.Base, reservation.PreviousAllocationFloor)
	}
	return nil
}

func timestampRequestValues(req *pb.GetTimestampRequest) (int, uint64, error) {
	if req == nil {
		return 1, 0, nil
	}
	count := req.GetCount()
	if count == 0 {
		count = 1
	}
	if count > uint32(kv.MaxTSOBatchSize) {
		return 0, 0, status.Errorf(codes.InvalidArgument, "timestamp count %d exceeds maximum %d", count, kv.MaxTSOBatchSize)
	}
	return int(count), req.GetMinTimestamp(), nil
}

func timestampRPCError(err error) error {
	if code := status.Code(err); code != codes.Unknown {
		return err
	}
	switch {
	case errors.Is(err, context.Canceled):
		return errors.WithStack(status.Error(codes.Canceled, err.Error()))
	case errors.Is(err, context.DeadlineExceeded):
		return errors.WithStack(status.Error(codes.DeadlineExceeded, err.Error()))
	case errors.Is(err, kv.ErrInvalidTSOBatchSize), errors.Is(err, kv.ErrTxnCommitTSRequired):
		return errors.WithStack(status.Error(codes.InvalidArgument, err.Error()))
	case errors.Is(err, kv.ErrTSONotLeader), errors.Is(err, kv.ErrLeaderNotFound):
		return errors.WithStack(status.Error(codes.FailedPrecondition, err.Error()))
	case errors.Is(err, kv.ErrTSOTimestampPrePhaseD):
		return errors.WithStack(status.Error(codes.OutOfRange, err.Error()))
	case errors.Is(err, kv.ErrTSOTimestampInvalid):
		return errors.WithStack(status.Error(codes.InvalidArgument, err.Error()))
	case errors.Is(err, kv.ErrTSOPhaseDInactive):
		return errors.WithStack(status.Error(codes.FailedPrecondition, err.Error()))
	default:
		return errors.WithStack(status.Error(codes.Unavailable, err.Error()))
	}
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

	if err := s.verifyCatalogLeader(ctx); err != nil {
		return nil, err
	}

	readTimestamp, err := kv.BeginReadTimestampThrough(
		ctx,
		s.coordinator,
		s.catalog.LatestCommitTS(),
		"distribution split range: begin read timestamp",
	)
	if err != nil {
		return nil, grpcStatusErrorf(codes.Internal, "begin catalog split snapshot: %v", err)
	}
	snapshot, err := s.loadCatalogSnapshotAt(ctx, readTimestamp.Timestamp())
	if err != nil {
		return nil, err
	}
	readPin := s.pinReadTS(snapshot.ReadTS)
	defer readPin.Release()
	if err := validateExpectedCatalogVersion(snapshot.Version, req.GetExpectedCatalogVersion()); err != nil {
		return nil, err
	}

	parent, splitKey, err := s.prepareSplitRange(snapshot.Routes, req)
	if err != nil {
		return nil, err
	}

	leftID, rightID, err := s.allocateChildRouteIDs(ctx, snapshot.ReadTS, snapshot.Routes)
	if err != nil {
		return nil, err
	}
	left, right := splitCatalogRoutes(parent, splitKey, leftID, rightID, 0)

	saved, err := s.saveSplitResultViaCoordinator(ctx, snapshot.ReadTS, req.GetExpectedCatalogVersion(), parent.RouteID, left, right)
	if err != nil {
		return nil, err
	}
	if err := s.applyEngineSnapshot(saved); err != nil {
		return nil, err
	}
	savedLeft, savedRight, err := splitChildrenFromSnapshot(saved, left.RouteID, right.RouteID)
	if err != nil {
		return nil, err
	}

	return &pb.SplitRangeResponse{
		CatalogVersion: saved.Version,
		Left:           toProtoRouteDescriptor(savedLeft),
		Right:          toProtoRouteDescriptor(savedRight),
	}, nil
}

func (s *DistributionServer) prepareSplitRange(routes []distribution.RouteDescriptor, req *pb.SplitRangeRequest) (distribution.RouteDescriptor, []byte, error) {
	parent, found := findRouteByID(routes, req.GetRouteId())
	if !found {
		return distribution.RouteDescriptor{}, nil, grpcStatusError(codes.NotFound, errDistributionUnknownRoute.Error())
	}
	rawSplitKey := req.GetSplitKey()
	splitKey := distribution.CloneBytes(fskeys.NormalizeSplitBoundary(kv.RouteKey(rawSplitKey)))
	if err := validateSplitKey(parent, splitKey); err != nil {
		s.observeFilePinnedHotspotIfNeeded(rawSplitKey, splitKey, err)
		return distribution.RouteDescriptor{}, nil, err
	}
	return parent, splitKey, nil
}

func (s *DistributionServer) pinReadTS(ts uint64) *kv.ActiveTimestampToken {
	if s == nil || s.readTracker == nil {
		return nil
	}
	return s.readTracker.Pin(ts)
}

func (s *DistributionServer) observeFilePinnedHotspotIfNeeded(rawSplitKey []byte, splitKey []byte, err error) {
	if s == nil || s.fsObserver == nil || bytes.Equal(rawSplitKey, splitKey) || !isSplitBoundaryError(err) {
		return
	}
	if !fskeys.IsChunkRouteDomainKey(splitKey) {
		return
	}
	s.fsObserver.ObserveFilePinnedHotspot(DistributionFilePinnedHotspotSplitBoundary)
	if homeSlot, inode, ok := fskeys.ChunkRoutePartsFromKey(splitKey); ok {
		slog.Warn("filesystem file-pinned hotspot rejected split",
			"reason", DistributionFilePinnedHotspotSplitBoundary,
			"home_slot", homeSlot,
			"inode", inode,
		)
	}
}

func isSplitBoundaryError(err error) bool {
	return status.Code(errors.Cause(err)) == codes.InvalidArgument &&
		strings.Contains(err.Error(), errDistributionSplitKeyAtBoundary.Error())
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
	resp, err := s.coordinator.Dispatch(ctx, &kv.OperationGroup[kv.OP]{
		Elems:   ops,
		IsTxn:   true,
		StartTS: readTS,
	})
	if err != nil {
		return distribution.CatalogSnapshot{}, grpcStatusErrorf(codes.Internal, "commit split mutations: %v", err)
	}
	if resp == nil || resp.CommitTS == 0 {
		return distribution.CatalogSnapshot{}, grpcStatusError(codes.Internal, "split commit timestamp missing")
	}
	return s.loadCatalogSnapshotAtVersion(ctx, resp.CommitTS, nextVersion)
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
		patchOffset, err := splitAtHLCPatchOffset(encoded)
		if err != nil {
			return nil, err
		}
		ops = append(ops, &kv.Elem[kv.OP]{
			Op:                  kv.Put,
			Key:                 distribution.CatalogRouteKey(route.RouteID),
			Value:               encoded,
			CommitTSValueOffset: patchOffset,
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

func splitAtHLCPatchOffset(encoded []byte) (uint64, error) {
	const splitAtHLCTailBytes = 8
	if len(encoded) < splitAtHLCTailBytes {
		return 0, errors.WithStack(distribution.ErrCatalogInvalidRouteRecord)
	}
	return uint64(len(encoded) - splitAtHLCTailBytes), nil //nolint:gosec // len was checked to be at least splitAtHLCTailBytes.
}

func splitChildrenFromSnapshot(snapshot distribution.CatalogSnapshot, leftID uint64, rightID uint64) (distribution.RouteDescriptor, distribution.RouteDescriptor, error) {
	left, found := findRouteByID(snapshot.Routes, leftID)
	if !found {
		return distribution.RouteDescriptor{}, distribution.RouteDescriptor{}, grpcStatusError(codes.Internal, "catalog split committed but left child is missing")
	}
	right, found := findRouteByID(snapshot.Routes, rightID)
	if !found {
		return distribution.RouteDescriptor{}, distribution.RouteDescriptor{}, grpcStatusError(codes.Internal, "catalog split committed but right child is missing")
	}
	return left, right, nil
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

func (s *DistributionServer) loadCatalogSnapshotAt(ctx context.Context, readTS uint64) (distribution.CatalogSnapshot, error) {
	if s.catalog == nil {
		return distribution.CatalogSnapshot{}, grpcStatusError(codes.FailedPrecondition, errDistributionCatalogNotConfigured.Error())
	}
	snapshot, err := s.catalog.SnapshotAt(ctx, readTS)
	if err != nil {
		return distribution.CatalogSnapshot{}, grpcStatusErrorf(codes.Internal, "load route catalog: %v", err)
	}
	return snapshot, nil
}

func (s *DistributionServer) loadCatalogSnapshotAtVersion(
	ctx context.Context,
	readTS uint64,
	wantVersion uint64,
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
		snapshot, err := s.catalog.SnapshotAt(ctx, readTS)
		if err != nil {
			return distribution.CatalogSnapshot{}, grpcStatusErrorf(codes.Internal, "reload route catalog: %v", err)
		}
		if snapshot.Version == wantVersion {
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
		"catalog split committed but local snapshot is stale at %d: got %d, want %d",
		readTS,
		last.Version,
		wantVersion,
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

func splitCatalogRoutes(
	parent distribution.RouteDescriptor,
	splitKey []byte,
	leftID uint64,
	rightID uint64,
	splitAtHLC uint64,
) (distribution.RouteDescriptor, distribution.RouteDescriptor) {
	// parent and splitKey are already cloned before this point and are immutable here.
	left := distribution.RouteDescriptor{
		RouteID:       leftID,
		Start:         parent.Start,
		End:           splitKey,
		GroupID:       parent.GroupID,
		State:         parent.State,
		ParentRouteID: parent.RouteID,
		SplitAtHLC:    splitAtHLC,
	}
	right := distribution.RouteDescriptor{
		RouteID:       rightID,
		Start:         splitKey,
		End:           parent.End,
		GroupID:       parent.GroupID,
		State:         parent.State,
		ParentRouteID: parent.RouteID,
		SplitAtHLC:    splitAtHLC,
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
		RouteId:       route.RouteID,
		Start:         distribution.CloneBytes(route.Start),
		End:           distribution.CloneBytes(route.End),
		RaftGroupId:   route.GroupID,
		State:         toProtoRouteState(route.State),
		ParentRouteId: route.ParentRouteID,
		SplitAtHlc:    route.SplitAtHLC,
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
