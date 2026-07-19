package distribution

import (
	"context"
	"io"
	"log/slog"
	"time"

	pb "github.com/bootjp/elastickv/proto"
	"github.com/cockroachdb/errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const defaultGRPCCatalogWatcherRetryInterval = 250 * time.Millisecond

var (
	ErrCatalogWatchClientRequired = errors.New("catalog watch client is required")
	ErrCatalogWatchEventInvalid   = errors.New("catalog watch event is invalid")
)

// CatalogWatchClientProvider resolves the Distribution endpoint used for one
// capability negotiation and stream attempt. Production resolves the current
// default-group leader again after every disconnect.
type CatalogWatchClientProvider func(context.Context) (pb.DistributionClient, error)

// GRPCCatalogWatcherOption customizes a remote catalog mirror.
type GRPCCatalogWatcherOption func(*GRPCCatalogWatcher)

// WithGRPCCatalogWatcherRetryInterval sets reconnect and legacy polling delay.
func WithGRPCCatalogWatcherRetryInterval(interval time.Duration) GRPCCatalogWatcherOption {
	return func(w *GRPCCatalogWatcher) {
		if interval > 0 {
			w.retryInterval = interval
		}
	}
}

// WithGRPCCatalogWatcherLogger sets the reconnect logger.
func WithGRPCCatalogWatcherLogger(logger *slog.Logger) GRPCCatalogWatcherOption {
	return func(w *GRPCCatalogWatcher) {
		if logger != nil {
			w.logger = logger
		}
	}
}

// GRPCCatalogWatcher mirrors a remote Distribution catalog into Engine.
type GRPCCatalogWatcher struct {
	clientProvider CatalogWatchClientProvider
	engine         *Engine
	retryInterval  time.Duration
	logger         *slog.Logger
}

// NewGRPCCatalogWatcher creates a capability-negotiated remote mirror.
func NewGRPCCatalogWatcher(client pb.DistributionClient, engine *Engine, opts ...GRPCCatalogWatcherOption) *GRPCCatalogWatcher {
	return NewResolvingGRPCCatalogWatcher(func(context.Context) (pb.DistributionClient, error) {
		if client == nil {
			return nil, errors.WithStack(ErrCatalogWatchClientRequired)
		}
		return client, nil
	}, engine, opts...)
}

// NewResolvingGRPCCatalogWatcher creates a mirror that re-resolves its remote
// endpoint before every reconnect attempt.
func NewResolvingGRPCCatalogWatcher(
	provider CatalogWatchClientProvider,
	engine *Engine,
	opts ...GRPCCatalogWatcherOption,
) *GRPCCatalogWatcher {
	w := &GRPCCatalogWatcher{
		clientProvider: provider,
		engine:         engine,
		retryInterval:  defaultGRPCCatalogWatcherRetryInterval,
		logger:         slog.Default(),
	}
	for _, opt := range opts {
		if opt != nil {
			opt(w)
		}
	}
	return w
}

// Run reconnects from the last atomically published Engine version. Servers
// without capability negotiation use ListRoutes snapshot polling.
func (w *GRPCCatalogWatcher) Run(ctx context.Context) error {
	if err := w.validate(); err != nil {
		return err
	}
	if ctx == nil {
		return errors.WithStack(errCatalogWatcherContextRequired)
	}
	for ctx.Err() == nil {
		if err := w.runAttempt(ctx); err != nil && ctx.Err() == nil {
			w.logger.WarnContext(ctx, "catalog watch attempt failed", "error", err, "version", w.engine.Version())
		}
		if ctx.Err() == nil {
			_ = waitCatalogWatcherRetry(ctx, w.retryInterval)
		}
	}
	return nil
}

func (w *GRPCCatalogWatcher) runAttempt(ctx context.Context) error {
	client, err := w.clientProvider(ctx)
	if err != nil {
		return errors.Wrap(err, "resolve catalog watch endpoint")
	}
	legacy, maxBatch, err := w.negotiate(ctx, client)
	if err != nil {
		return errors.Wrap(err, "negotiate catalog watch capability")
	}
	if legacy {
		return errors.Wrap(w.syncSnapshot(ctx, client), "poll legacy catalog snapshot")
	}
	err = w.consumeStream(ctx, client, maxBatch)
	if status.Code(errors.Cause(err)) != codes.Unimplemented {
		return err
	}
	if syncErr := w.syncSnapshot(ctx, client); syncErr != nil {
		return errors.Join(err, errors.Wrap(syncErr, "poll catalog snapshot after unimplemented watch"))
	}
	return nil
}

func (w *GRPCCatalogWatcher) validate() error {
	if w == nil || w.clientProvider == nil {
		return errors.WithStack(ErrCatalogWatchClientRequired)
	}
	if w.engine == nil {
		return errors.WithStack(ErrEngineRequired)
	}
	if w.retryInterval <= 0 {
		return errors.WithStack(errCatalogWatcherInvalidInterval)
	}
	if w.logger == nil {
		return errors.WithStack(errCatalogWatcherLoggerRequired)
	}
	return nil
}

func (w *GRPCCatalogWatcher) negotiate(ctx context.Context, client pb.DistributionClient) (bool, uint32, error) {
	capabilities, err := client.GetCatalogCapabilities(
		ctx,
		&pb.CatalogCapabilitiesRequest{},
		grpc.WaitForReady(false),
	)
	if err != nil {
		if status.Code(errors.Cause(err)) == codes.Unimplemented {
			return true, 0, nil
		}
		return false, 0, errors.WithStack(err)
	}
	if !supportsCatalogWatchProtocol(capabilities.GetSupportedProtocolVersions(), CatalogWatchProtocolVersion) {
		return true, 0, nil
	}
	maxBatch := capabilities.GetMaxBatchSize()
	if maxBatch == 0 || maxBatch > MaxCatalogDeltaBatchSize {
		maxBatch = MaxCatalogDeltaBatchSize
	}
	return false, maxBatch, nil
}

func supportsCatalogWatchProtocol(supported []uint32, target uint32) bool {
	for _, version := range supported {
		if version == target {
			return true
		}
	}
	return false
}

func (w *GRPCCatalogWatcher) consumeStream(ctx context.Context, client pb.DistributionClient, maxBatch uint32) error {
	streamCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	stream, err := client.WatchCatalog(streamCtx, &pb.CatalogWatchRequest{
		ProtocolVersion: CatalogWatchProtocolVersion,
		AfterVersion:    w.engine.Version(),
		MaxBatchSize:    maxBatch,
	}, grpc.WaitForReady(false))
	if err != nil {
		return errors.WithStack(err)
	}
	for {
		event, err := stream.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return io.EOF
			}
			return errors.WithStack(err)
		}
		if err := w.applyEvent(event); err != nil {
			if errors.Is(err, ErrEngineDeltaVersionGap) || errors.Is(err, ErrCatalogDeltaVersionGap) {
				return w.syncSnapshot(ctx, client)
			}
			return err
		}
	}
}

func (w *GRPCCatalogWatcher) applyEvent(event *pb.CatalogWatchEvent) error {
	if event == nil {
		return errors.WithStack(ErrCatalogWatchEventInvalid)
	}
	if snapshot := event.GetSnapshot(); snapshot != nil {
		routes, err := routeDescriptorsFromProto(snapshot.GetRoutes())
		if err != nil {
			return err
		}
		if err := w.engine.ApplySnapshot(CatalogSnapshot{Version: snapshot.GetVersion(), Routes: routes}); err != nil {
			if errors.Is(err, ErrEngineSnapshotVersionStale) {
				return nil
			}
			return err
		}
		return nil
	}
	if record := event.GetDelta(); record != nil {
		delta, err := catalogDeltaFromProto(record)
		if err != nil {
			return err
		}
		if err := w.engine.ApplyDelta(delta); err != nil {
			if errors.Is(err, ErrEngineSnapshotVersionStale) {
				return nil
			}
			return err
		}
		return nil
	}
	return errors.WithStack(ErrCatalogWatchEventInvalid)
}

func (w *GRPCCatalogWatcher) syncSnapshot(ctx context.Context, client pb.DistributionClient) error {
	response, err := client.ListRoutes(ctx, &pb.ListRoutesRequest{}, grpc.WaitForReady(false))
	if err != nil {
		return errors.WithStack(err)
	}
	routes, err := routeDescriptorsFromProto(response.GetRoutes())
	if err != nil {
		return err
	}
	if err := w.engine.ApplySnapshot(CatalogSnapshot{Version: response.GetCatalogVersion(), Routes: routes}); err != nil {
		if errors.Is(err, ErrEngineSnapshotVersionStale) {
			return nil
		}
		return err
	}
	return nil
}

func waitCatalogWatcherRetry(ctx context.Context, interval time.Duration) error {
	timer := time.NewTimer(interval)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return errors.WithStack(ctx.Err())
	case <-timer.C:
		return nil
	}
}

func catalogDeltaFromProto(record *pb.CatalogDeltaRecord) (CatalogDelta, error) {
	delta := CatalogDelta{
		PreviousVersion: record.GetPreviousVersion(),
		Version:         record.GetVersion(),
		Mutations:       make([]CatalogRouteMutation, 0, len(record.GetMutations())),
	}
	for _, raw := range record.GetMutations() {
		if raw == nil {
			return CatalogDelta{}, errors.WithStack(ErrCatalogWatchEventInvalid)
		}
		mutation := CatalogRouteMutation{RouteID: raw.GetRouteId()}
		switch raw.GetOp() {
		case pb.CatalogDeltaMutationOp_CATALOG_DELTA_MUTATION_OP_UNSPECIFIED:
			return CatalogDelta{}, errors.WithStack(ErrCatalogWatchEventInvalid)
		case pb.CatalogDeltaMutationOp_CATALOG_DELTA_MUTATION_OP_DELETE:
			if raw.Route != nil {
				return CatalogDelta{}, errors.WithStack(ErrCatalogWatchEventInvalid)
			}
			mutation.Op = CatalogMutationDelete
		case pb.CatalogDeltaMutationOp_CATALOG_DELTA_MUTATION_OP_UPSERT:
			mutation.Op = CatalogMutationUpsert
			route, err := routeDescriptorFromProto(raw.GetRoute())
			if err != nil {
				return CatalogDelta{}, err
			}
			mutation.Route = route
		default:
			return CatalogDelta{}, errors.WithStack(ErrCatalogWatchEventInvalid)
		}
		delta.Mutations = append(delta.Mutations, mutation)
	}
	if err := validateCatalogDelta(delta); err != nil {
		return CatalogDelta{}, err
	}
	return delta, nil
}

func routeDescriptorsFromProto(routes []*pb.RouteDescriptor) ([]RouteDescriptor, error) {
	out := make([]RouteDescriptor, 0, len(routes))
	for _, raw := range routes {
		route, err := routeDescriptorFromProto(raw)
		if err != nil {
			return nil, err
		}
		out = append(out, route)
	}
	return out, nil
}

func routeDescriptorFromProto(raw *pb.RouteDescriptor) (RouteDescriptor, error) {
	if raw == nil {
		return RouteDescriptor{}, errors.WithStack(ErrCatalogWatchEventInvalid)
	}
	state, ok := routeStateFromProto(raw.GetState())
	if !ok {
		return RouteDescriptor{}, errors.WithStack(ErrCatalogWatchEventInvalid)
	}
	route := RouteDescriptor{
		RouteID:       raw.GetRouteId(),
		Start:         CloneBytes(raw.GetStart()),
		End:           CloneBytes(raw.GetEnd()),
		GroupID:       raw.GetRaftGroupId(),
		State:         state,
		ParentRouteID: raw.GetParentRouteId(),
	}
	if err := validateRouteDescriptor(route); err != nil {
		return RouteDescriptor{}, err
	}
	return route, nil
}

func routeStateFromProto(state pb.RouteState) (RouteState, bool) {
	switch state {
	case pb.RouteState_ROUTE_STATE_UNSPECIFIED:
		return 0, false
	case pb.RouteState_ROUTE_STATE_ACTIVE:
		return RouteStateActive, true
	case pb.RouteState_ROUTE_STATE_WRITE_FENCED:
		return RouteStateWriteFenced, true
	case pb.RouteState_ROUTE_STATE_MIGRATING_SOURCE:
		return RouteStateMigratingSource, true
	case pb.RouteState_ROUTE_STATE_MIGRATING_TARGET:
		return RouteStateMigratingTarget, true
	default:
		return 0, false
	}
}
