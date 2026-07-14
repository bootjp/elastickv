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
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// DistributionServer serves distribution related gRPC APIs.
type DistributionServer struct {
	mu            sync.Mutex
	engine        *distribution.Engine
	catalog       *distribution.CatalogStore
	coordinator   kv.Coordinator
	readTracker   *kv.ActiveTimestampTracker
	watchInterval time.Duration
	watchLeader   func() bool
	fsObserver    DistributionFilesystemObserver
	reloadRetry   struct {
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

// WithCatalogWatchInterval sets how often an idle catalog stream checks for a
// newly committed version.
func WithCatalogWatchInterval(interval time.Duration) DistributionServerOption {
	return func(s *DistributionServer) {
		if interval > 0 {
			s.watchInterval = interval
		}
	}
}

// WithCatalogWatchLeaderCheck restricts long-lived catalog streams to the
// current default-group leader. Existing callers without a check retain the
// package-level server behavior used by tests and embedded deployments.
func WithCatalogWatchLeaderCheck(check func() bool) DistributionServerOption {
	return func(s *DistributionServer) {
		s.watchLeader = check
	}
}

const (
	childRouteCount      = 2
	splitMutationOpCount = childRouteCount + 3
)

var (
	defaultCatalogReloadRetryAttempts = 20
	defaultCatalogReloadRetryInterval = 10 * time.Millisecond
	defaultCatalogWatchInterval       = 100 * time.Millisecond

	errDistributionCatalogNotConfigured   = errors.New("route catalog is not configured")
	errDistributionUnknownRoute           = errors.New("unknown route")
	errDistributionInvalidSplitKey        = errors.New("invalid split key")
	errDistributionSplitKeyAtBoundary     = errors.New("split key at route boundary")
	errDistributionCatalogConflict        = errors.New("catalog version conflict")
	errDistributionRouteIDOverflow        = errors.New("route id overflow")
	errDistributionNotLeader              = errors.New("not leader for distribution catalog")
	errDistributionCoordinatorRequired    = errors.New("distribution coordinator is not configured")
	errDistributionEngineNotConfigured    = errors.New("distribution engine is not configured")
	errDistributionCatalogMutationInvalid = errors.New("catalog store mutation is invalid")
)

// NewDistributionServer creates a new server.
func NewDistributionServer(e *distribution.Engine, catalog *distribution.CatalogStore, opts ...DistributionServerOption) *DistributionServer {
	s := &DistributionServer{
		engine:        e,
		catalog:       catalog,
		watchInterval: defaultCatalogWatchInterval,
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

// GetCatalogCapabilities negotiates the durable delta-watch protocol.
func (s *DistributionServer) GetCatalogCapabilities(ctx context.Context, _ *pb.CatalogCapabilitiesRequest) (*pb.CatalogCapabilitiesResponse, error) {
	if s.catalog == nil {
		return nil, grpcStatusError(codes.FailedPrecondition, errDistributionCatalogNotConfigured.Error())
	}
	version, err := s.catalog.Version(ctx)
	if err != nil {
		return nil, grpcStatusErrorf(codes.Internal, "load catalog version: %v", err)
	}
	floor, err := s.catalog.DeltaFloor(ctx)
	if err != nil {
		return nil, grpcStatusErrorf(codes.Internal, "load catalog delta floor: %v", err)
	}
	return &pb.CatalogCapabilitiesResponse{
		SupportedProtocolVersions: []uint32{distribution.CatalogWatchProtocolVersion},
		CurrentVersion:            version,
		OldestDeltaVersion:        floor,
		MaxBatchSize:              distribution.MaxCatalogDeltaBatchSize,
	}, nil
}

// WatchCatalog streams contiguous deltas and emits a snapshot reset when the
// requested reconnect cursor predates retained history.
func (s *DistributionServer) WatchCatalog(req *pb.CatalogWatchRequest, stream pb.Distribution_WatchCatalogServer) error {
	config, err := s.catalogWatchConfig(req)
	if err != nil {
		return err
	}
	for {
		if s.watchLeader != nil && !s.watchLeader() {
			return grpcStatusError(codes.Unavailable, "catalog watch requires the catalog-group leader")
		}
		nextCursor, sent, err := s.sendCatalogChanges(stream, config.cursor, config.batchSize)
		if err != nil {
			return err
		}
		config.cursor = nextCursor
		if sent {
			continue
		}
		stop, err := waitCatalogStream(stream.Context(), config.interval)
		if err != nil {
			return err
		}
		if stop {
			return nil
		}
	}
}

type catalogWatchConfig struct {
	cursor    uint64
	batchSize int
	interval  time.Duration
}

func (s *DistributionServer) catalogWatchConfig(req *pb.CatalogWatchRequest) (catalogWatchConfig, error) {
	if s.catalog == nil {
		return catalogWatchConfig{}, grpcStatusError(codes.FailedPrecondition, errDistributionCatalogNotConfigured.Error())
	}
	if req.GetProtocolVersion() != distribution.CatalogWatchProtocolVersion {
		return catalogWatchConfig{}, grpcStatusErrorf(codes.FailedPrecondition, "unsupported catalog watch protocol %d", req.GetProtocolVersion())
	}
	batchSize := int(req.GetMaxBatchSize())
	if batchSize <= 0 {
		batchSize = distribution.DefaultCatalogDeltaBatchSize
	}
	if batchSize > distribution.MaxCatalogDeltaBatchSize {
		batchSize = distribution.MaxCatalogDeltaBatchSize
	}
	interval := s.watchInterval
	if interval <= 0 {
		interval = defaultCatalogWatchInterval
	}
	return catalogWatchConfig{cursor: req.GetAfterVersion(), batchSize: batchSize, interval: interval}, nil
}

func (s *DistributionServer) sendCatalogChanges(
	stream pb.Distribution_WatchCatalogServer,
	cursor uint64,
	batchSize int,
) (uint64, bool, error) {
	changes, err := s.catalog.ChangesSince(stream.Context(), cursor, batchSize)
	if err != nil {
		if errors.Is(err, distribution.ErrCatalogDeltaVersionFuture) {
			return cursor, false, grpcStatusError(codes.InvalidArgument, err.Error())
		}
		return cursor, false, grpcStatusErrorf(codes.Internal, "read catalog changes: %v", err)
	}
	if changes.Reset != nil {
		return s.sendCatalogReset(stream, changes.Reset)
	}
	return sendCatalogDeltas(stream, cursor, changes.Deltas)
}

func (s *DistributionServer) sendCatalogReset(
	stream pb.Distribution_WatchCatalogServer,
	snapshot *distribution.CatalogSnapshot,
) (uint64, bool, error) {
	event := &pb.CatalogWatchEvent{Payload: &pb.CatalogWatchEvent_Snapshot{
		Snapshot: &pb.CatalogSnapshotReset{
			Version: snapshot.Version,
			Routes:  toProtoRouteDescriptors(snapshot.Routes),
		},
	}}
	if err := stream.Send(event); err != nil {
		return 0, false, errors.WithStack(err)
	}
	return snapshot.Version, true, nil
}

func sendCatalogDeltas(
	stream pb.Distribution_WatchCatalogServer,
	cursor uint64,
	deltas []distribution.CatalogDelta,
) (uint64, bool, error) {
	for _, delta := range deltas {
		event := &pb.CatalogWatchEvent{Payload: &pb.CatalogWatchEvent_Delta{
			Delta: toProtoCatalogDelta(delta),
		}}
		if err := stream.Send(event); err != nil {
			return cursor, false, errors.WithStack(err)
		}
		cursor = delta.Version
	}
	return cursor, len(deltas) > 0, nil
}

func waitCatalogStream(ctx context.Context, interval time.Duration) (bool, error) {
	err := waitWithContext(ctx, interval)
	if errors.Is(err, context.Canceled) || status.Code(errors.Cause(err)) == codes.Canceled {
		return true, nil
	}
	return false, err
}

func toProtoCatalogDelta(delta distribution.CatalogDelta) *pb.CatalogDeltaRecord {
	mutations := make([]*pb.CatalogDeltaMutation, 0, len(delta.Mutations))
	for _, mutation := range delta.Mutations {
		op := pb.CatalogDeltaMutationOp_CATALOG_DELTA_MUTATION_OP_DELETE
		var route *pb.RouteDescriptor
		if mutation.Op == distribution.CatalogMutationUpsert {
			op = pb.CatalogDeltaMutationOp_CATALOG_DELTA_MUTATION_OP_UPSERT
			route = toProtoRouteDescriptor(mutation.Route)
		}
		mutations = append(mutations, &pb.CatalogDeltaMutation{
			Op:      op,
			RouteId: mutation.RouteID,
			Route:   route,
		})
	}
	return &pb.CatalogDeltaRecord{
		PreviousVersion: delta.PreviousVersion,
		Version:         delta.Version,
		Mutations:       mutations,
	}
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

	parent, found := findRouteByID(snapshot.Routes, req.GetRouteId())
	if !found {
		return nil, grpcStatusError(codes.NotFound, errDistributionUnknownRoute.Error())
	}

	rawSplitKey := req.GetSplitKey()
	splitKey := distribution.CloneBytes(fskeys.NormalizeSplitBoundary(kv.RouteKey(rawSplitKey)))
	if err := validateSplitKey(parent, splitKey); err != nil {
		s.observeFilePinnedHotspotIfNeeded(rawSplitKey, splitKey, err)
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
	commitTS, err := kv.NextTimestampAfterThrough(ctx, s.coordinator, readTS, "split range: allocate commitTS")
	if err != nil {
		return distribution.CatalogSnapshot{}, grpcStatusErrorf(codes.Internal, "allocate split commit timestamp: %v", err)
	}
	left.SplitAtHLC = commitTS
	right.SplitAtHLC = commitTS

	ops, err := buildCatalogSplitOps(parentID, left, right, nextVersion, nextRouteID, s.catalog.AllowsRouteDescriptorV2Writes())
	if err != nil {
		return distribution.CatalogSnapshot{}, grpcStatusErrorf(codes.Internal, "build split mutations: %v", err)
	}
	delta := distribution.CatalogDelta{
		PreviousVersion: expectedVersion,
		Version:         nextVersion,
		Mutations: []distribution.CatalogRouteMutation{
			{Op: distribution.CatalogMutationDelete, RouteID: parentID},
			{Op: distribution.CatalogMutationUpsert, RouteID: left.RouteID, Route: left},
			{Op: distribution.CatalogMutationUpsert, RouteID: right.RouteID, Route: right},
		},
	}
	deltaMutations, err := s.catalog.BuildDeltaMutationsAt(ctx, readTS, delta)
	if err != nil {
		return distribution.CatalogSnapshot{}, grpcStatusErrorf(codes.Internal, "build catalog delta mutations: %v", err)
	}
	deltaOps, err := catalogStoreMutationsToOps(deltaMutations)
	if err != nil {
		return distribution.CatalogSnapshot{}, grpcStatusErrorf(codes.Internal, "convert catalog delta mutations: %v", err)
	}
	ops = append(ops, deltaOps...)
	resp, err := s.coordinator.Dispatch(ctx, &kv.OperationGroup[kv.OP]{
		Elems:    ops,
		IsTxn:    true,
		StartTS:  readTS,
		CommitTS: commitTS,
	})
	if err != nil {
		return distribution.CatalogSnapshot{}, grpcStatusErrorf(codes.Internal, "commit split mutations: %v", err)
	}
	if resp == nil || resp.CommitTS == 0 {
		return distribution.CatalogSnapshot{}, grpcStatusError(codes.Internal, "split commit timestamp missing")
	}
	return s.loadCatalogSnapshotAtVersion(ctx, resp.CommitTS, nextVersion)
}

func catalogStoreMutationsToOps(mutations []*store.KVPairMutation) ([]*kv.Elem[kv.OP], error) {
	ops := make([]*kv.Elem[kv.OP], 0, len(mutations))
	for _, mutation := range mutations {
		if mutation == nil {
			return nil, errors.WithStack(errDistributionCatalogMutationInvalid)
		}
		var op kv.OP
		switch mutation.Op {
		case store.OpTypePut:
			op = kv.Put
		case store.OpTypeDelete:
			op = kv.Del
		default:
			return nil, errors.Wrapf(errDistributionCatalogMutationInvalid, "unknown operation %d", mutation.Op)
		}
		ops = append(ops, &kv.Elem[kv.OP]{
			Op:    op,
			Key:   distribution.CloneBytes(mutation.Key),
			Value: distribution.CloneBytes(mutation.Value),
		})
	}
	return ops, nil
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
		RouteID:                leftID,
		Start:                  parent.Start,
		End:                    splitKey,
		GroupID:                parent.GroupID,
		State:                  parent.State,
		ParentRouteID:          parent.RouteID,
		SplitAtHLC:             splitAtHLC,
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
		SplitAtHLC:             splitAtHLC,
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
		SplitAtHlc:             route.SplitAtHLC,
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
