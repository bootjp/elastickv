package adapter

import (
	"context"
	"io"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bootjp/elastickv/distribution"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

func TestDistributionServerCatalogWatchCapabilitiesAndReconnect(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	catalog := distribution.NewCatalogStore(store.NewMVCCStore())
	routes := []distribution.RouteDescriptor{
		{RouteID: 1, Start: []byte(""), End: nil, GroupID: 1, State: distribution.RouteStateActive},
	}
	first, err := catalog.Save(ctx, 0, routes)
	require.NoError(t, err)
	server := NewDistributionServer(distribution.NewEngine(), catalog)

	capabilities, err := server.GetCatalogCapabilities(ctx, &pb.CatalogCapabilitiesRequest{})
	require.NoError(t, err)
	require.Equal(t, []uint32{distribution.CatalogWatchProtocolVersion}, capabilities.GetSupportedProtocolVersions())
	require.Equal(t, first.Version, capabilities.GetCurrentVersion())
	require.EqualValues(t, 1, capabilities.GetOldestDeltaVersion())
	require.EqualValues(t, distribution.MaxCatalogDeltaBatchSize, capabilities.GetMaxBatchSize())

	firstStream := newCaptureCatalogWatchStream()
	require.NoError(t, server.WatchCatalog(&pb.CatalogWatchRequest{
		ProtocolVersion: distribution.CatalogWatchProtocolVersion,
		AfterVersion:    0,
	}, firstStream))
	firstEvent := <-firstStream.events
	require.Equal(t, first.Version, firstEvent.GetDelta().GetVersion())
	require.EqualValues(t, 0, firstEvent.GetDelta().GetPreviousVersion())

	second, err := catalog.Save(ctx, first.Version, routes)
	require.NoError(t, err)
	secondStream := newCaptureCatalogWatchStream()
	require.NoError(t, server.WatchCatalog(&pb.CatalogWatchRequest{
		ProtocolVersion: distribution.CatalogWatchProtocolVersion,
		AfterVersion:    first.Version,
	}, secondStream))
	secondEvent := <-secondStream.events
	require.Equal(t, second.Version, secondEvent.GetDelta().GetVersion())
	require.Equal(t, first.Version, secondEvent.GetDelta().GetPreviousVersion())
}

func TestDistributionServerCatalogWatchRejectsProtocolAndFutureCursor(t *testing.T) {
	t.Parallel()

	catalog := distribution.NewCatalogStore(store.NewMVCCStore())
	server := NewDistributionServer(distribution.NewEngine(), catalog)

	err := server.WatchCatalog(&pb.CatalogWatchRequest{ProtocolVersion: 99}, newCaptureCatalogWatchStream())
	require.Equal(t, codes.FailedPrecondition, status.Code(err))

	err = server.WatchCatalog(&pb.CatalogWatchRequest{
		ProtocolVersion: distribution.CatalogWatchProtocolVersion,
		AfterVersion:    1,
	}, newCaptureCatalogWatchStream())
	require.Equal(t, codes.InvalidArgument, status.Code(err))
}

func TestDistributionServerCatalogWatchRequiresConfiguredLeader(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	catalog := distribution.NewCatalogStore(store.NewMVCCStore())
	_, err := catalog.Save(ctx, 0, []distribution.RouteDescriptor{
		{RouteID: 1, Start: []byte(""), End: nil, GroupID: 1, State: distribution.RouteStateActive},
	})
	require.NoError(t, err)

	var leader atomic.Bool
	server := NewDistributionServer(
		distribution.NewEngine(),
		catalog,
		WithCatalogWatchLeaderCheck(leader.Load),
	)
	req := &pb.CatalogWatchRequest{ProtocolVersion: distribution.CatalogWatchProtocolVersion}
	err = server.WatchCatalog(req, newCaptureCatalogWatchStream())
	require.Equal(t, codes.Unavailable, status.Code(err))

	leader.Store(true)
	require.NoError(t, server.WatchCatalog(req, newCaptureCatalogWatchStream()))
}

func TestDistributionServerCatalogWatchRejectsFollower(t *testing.T) {
	t.Parallel()

	server := NewDistributionServer(
		distribution.NewEngine(),
		distribution.NewCatalogStore(store.NewMVCCStore()),
		WithCatalogWatchLeaderCheck(func() bool { return false }),
	)
	err := server.WatchCatalog(&pb.CatalogWatchRequest{
		ProtocolVersion: distribution.CatalogWatchProtocolVersion,
	}, newCaptureCatalogWatchStream())
	require.Equal(t, codes.Unavailable, status.Code(err))
}

func TestDistributionServerCatalogWatchSendsSnapshotReset(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	st := store.NewMVCCStore()
	catalog := distribution.NewCatalogStore(st)
	routes := []distribution.RouteDescriptor{
		{RouteID: 1, Start: []byte(""), End: nil, GroupID: 1, State: distribution.RouteStateActive},
	}
	first, err := catalog.Save(ctx, 0, routes)
	require.NoError(t, err)
	second, err := catalog.Save(ctx, first.Version, routes)
	require.NoError(t, err)

	startTS := st.LastCommitTS()
	require.NoError(t, st.ApplyMutations(ctx, []*store.KVPairMutation{
		{Op: store.OpTypeDelete, Key: distribution.CatalogDeltaKey(first.Version)},
		{Op: store.OpTypePut, Key: distribution.CatalogDeltaFloorKey(), Value: distribution.EncodeCatalogVersion(second.Version)},
	}, nil, startTS, startTS+1))

	server := NewDistributionServer(distribution.NewEngine(), catalog)
	stream := newCaptureCatalogWatchStream()
	require.NoError(t, server.WatchCatalog(&pb.CatalogWatchRequest{
		ProtocolVersion: distribution.CatalogWatchProtocolVersion,
		AfterVersion:    0,
	}, stream))
	event := <-stream.events
	require.Nil(t, event.GetDelta())
	require.Equal(t, second.Version, event.GetSnapshot().GetVersion())
	require.Len(t, event.GetSnapshot().GetRoutes(), 1)
}

func TestGRPCCatalogWatcherReconnectsFromPublishedVersion(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	catalog := distribution.NewCatalogStore(store.NewMVCCStore())
	routes := []distribution.RouteDescriptor{
		{RouteID: 1, Start: []byte(""), End: nil, GroupID: 1, State: distribution.RouteStateActive},
	}
	first, err := catalog.Save(ctx, 0, routes)
	require.NoError(t, err)
	second, err := catalog.Save(ctx, first.Version, routes)
	require.NoError(t, err)

	base := NewDistributionServer(
		distribution.NewEngine(),
		catalog,
		WithCatalogWatchInterval(time.Millisecond),
	)
	flaky := &flakyCatalogWatchServer{DistributionServer: base}
	client, cleanup := newBufconnDistributionClient(t, flaky)
	defer cleanup()

	mirror := distribution.NewEngineWithDefaultRoute()
	var providerCalls atomic.Int32
	watcher := distribution.NewResolvingGRPCCatalogWatcher(
		func(context.Context) (pb.DistributionClient, error) {
			providerCalls.Add(1)
			return client, nil
		},
		mirror,
		distribution.WithGRPCCatalogWatcherRetryInterval(time.Millisecond),
	)
	watchCtx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() { errCh <- watcher.Run(watchCtx) }()

	require.Eventually(t, func() bool {
		return mirror.Version() == second.Version && flaky.watchCalls.Load() >= 2 && providerCalls.Load() >= 2
	}, 5*time.Second, 5*time.Millisecond)
	cancel()
	require.NoError(t, <-errCh)
}

func TestGRPCCatalogWatcherReResolvesEndpointAfterDisconnect(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	catalog := distribution.NewCatalogStore(store.NewMVCCStore())
	routes := []distribution.RouteDescriptor{
		{RouteID: 1, Start: []byte(""), End: nil, GroupID: 1, State: distribution.RouteStateActive},
	}
	first, err := catalog.Save(ctx, 0, routes)
	require.NoError(t, err)
	second, err := catalog.Save(ctx, first.Version, routes)
	require.NoError(t, err)

	base := NewDistributionServer(
		distribution.NewEngine(),
		catalog,
		WithCatalogWatchInterval(time.Millisecond),
	)
	firstEndpoint := &disconnectAfterOneCatalogDeltaServer{DistributionServer: base}
	firstClient, firstCleanup := newBufconnDistributionClient(t, firstEndpoint)
	defer firstCleanup()
	secondClient, secondCleanup := newBufconnDistributionClient(t, base)
	defer secondCleanup()

	var resolutions atomic.Int32
	mirror := distribution.NewEngineWithDefaultRoute()
	watcher := distribution.NewResolvingGRPCCatalogWatcher(
		func(context.Context) (pb.DistributionClient, error) {
			if resolutions.Add(1) == 1 {
				return firstClient, nil
			}
			return secondClient, nil
		},
		mirror,
		distribution.WithGRPCCatalogWatcherRetryInterval(time.Millisecond),
	)
	watchCtx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() { errCh <- watcher.Run(watchCtx) }()

	require.Eventually(t, func() bool {
		return mirror.Version() == second.Version && resolutions.Load() >= 2
	}, 5*time.Second, 5*time.Millisecond)
	cancel()
	require.NoError(t, <-errCh)
}

func TestGRPCCatalogWatcherFallsBackToLegacyPolling(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	catalog := distribution.NewCatalogStore(store.NewMVCCStore())
	routes := []distribution.RouteDescriptor{
		{RouteID: 1, Start: []byte(""), End: nil, GroupID: 1, State: distribution.RouteStateActive},
	}
	first, err := catalog.Save(ctx, 0, routes)
	require.NoError(t, err)

	legacy := &legacyCatalogServer{catalog: catalog}
	client, cleanup := newBufconnDistributionClient(t, legacy)
	defer cleanup()

	mirror := distribution.NewEngineWithDefaultRoute()
	watcher := distribution.NewGRPCCatalogWatcher(
		client,
		mirror,
		distribution.WithGRPCCatalogWatcherRetryInterval(time.Millisecond),
	)
	watchCtx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() { errCh <- watcher.Run(watchCtx) }()
	require.Eventually(t, func() bool { return mirror.Version() == first.Version }, 5*time.Second, 5*time.Millisecond)

	second, err := catalog.Save(ctx, first.Version, []distribution.RouteDescriptor{
		{RouteID: 2, Start: []byte(""), End: nil, GroupID: 2, State: distribution.RouteStateActive},
	})
	require.NoError(t, err)
	require.Eventually(t, func() bool { return mirror.Version() == second.Version }, 5*time.Second, 5*time.Millisecond)
	cancel()
	require.NoError(t, <-errCh)
}

func TestGRPCCatalogWatcherFallsBackWhenWatchRPCIsUnimplemented(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	catalog := distribution.NewCatalogStore(store.NewMVCCStore())
	routes := []distribution.RouteDescriptor{
		{RouteID: 1, Start: []byte(""), End: nil, GroupID: 1, State: distribution.RouteStateActive},
	}
	first, err := catalog.Save(ctx, 0, routes)
	require.NoError(t, err)

	partial := &capabilitiesWithoutWatchCatalogServer{catalog: catalog}
	client, cleanup := newBufconnDistributionClient(t, partial)
	defer cleanup()

	mirror := distribution.NewEngineWithDefaultRoute()
	watcher := distribution.NewGRPCCatalogWatcher(
		client,
		mirror,
		distribution.WithGRPCCatalogWatcherRetryInterval(time.Millisecond),
	)
	watchCtx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() { errCh <- watcher.Run(watchCtx) }()
	require.Eventually(t, func() bool { return mirror.Version() == first.Version }, 5*time.Second, 5*time.Millisecond)
	cancel()
	require.NoError(t, <-errCh)
}

func TestCatalogStoreMutationsToOpsRejectsInvalidMutation(t *testing.T) {
	t.Parallel()

	_, err := catalogStoreMutationsToOps([]*store.KVPairMutation{nil})
	require.ErrorIs(t, err, errDistributionCatalogMutationInvalid)

	_, err = catalogStoreMutationsToOps([]*store.KVPairMutation{{
		Op:  store.OpType(99),
		Key: []byte("invalid"),
	}})
	require.ErrorIs(t, err, errDistributionCatalogMutationInvalid)
}

type flakyCatalogWatchServer struct {
	*DistributionServer
	watchCalls atomic.Int32
}

type disconnectAfterOneCatalogDeltaServer struct {
	*DistributionServer
}

func (s *disconnectAfterOneCatalogDeltaServer) WatchCatalog(
	req *pb.CatalogWatchRequest,
	stream pb.Distribution_WatchCatalogServer,
) error {
	changes, err := s.catalog.ChangesSince(stream.Context(), req.GetAfterVersion(), 1)
	if err != nil {
		return err
	}
	if len(changes.Deltas) != 1 {
		return status.Error(codes.Internal, "injected disconnect requires one delta")
	}
	if err := stream.Send(&pb.CatalogWatchEvent{Payload: &pb.CatalogWatchEvent_Delta{
		Delta: toProtoCatalogDelta(changes.Deltas[0]),
	}}); err != nil {
		return err
	}
	return status.Error(codes.Unavailable, "injected endpoint replacement")
}

func (s *flakyCatalogWatchServer) WatchCatalog(req *pb.CatalogWatchRequest, stream pb.Distribution_WatchCatalogServer) error {
	if s.watchCalls.Add(1) != 1 {
		return s.DistributionServer.WatchCatalog(req, stream)
	}
	changes, err := s.catalog.ChangesSince(stream.Context(), req.GetAfterVersion(), 1)
	if err != nil {
		return err
	}
	if len(changes.Deltas) != 1 {
		return status.Error(codes.Internal, "injected disconnect requires one delta")
	}
	if err := stream.Send(&pb.CatalogWatchEvent{Payload: &pb.CatalogWatchEvent_Delta{
		Delta: toProtoCatalogDelta(changes.Deltas[0]),
	}}); err != nil {
		return err
	}
	return status.Error(codes.Unavailable, "injected catalog stream disconnect")
}

type legacyCatalogServer struct {
	pb.UnimplementedDistributionServer
	catalog *distribution.CatalogStore
}

type capabilitiesWithoutWatchCatalogServer struct {
	pb.UnimplementedDistributionServer
	catalog *distribution.CatalogStore
}

func (s *capabilitiesWithoutWatchCatalogServer) GetCatalogCapabilities(
	context.Context,
	*pb.CatalogCapabilitiesRequest,
) (*pb.CatalogCapabilitiesResponse, error) {
	return &pb.CatalogCapabilitiesResponse{
		SupportedProtocolVersions: []uint32{distribution.CatalogWatchProtocolVersion},
		MaxBatchSize:              distribution.DefaultCatalogDeltaBatchSize,
	}, nil
}

func (s *capabilitiesWithoutWatchCatalogServer) ListRoutes(
	ctx context.Context,
	_ *pb.ListRoutesRequest,
) (*pb.ListRoutesResponse, error) {
	snapshot, err := s.catalog.Snapshot(ctx)
	if err != nil {
		return nil, err
	}
	return &pb.ListRoutesResponse{
		CatalogVersion: snapshot.Version,
		Routes:         toProtoRouteDescriptors(snapshot.Routes),
	}, nil
}

func (s *legacyCatalogServer) ListRoutes(ctx context.Context, _ *pb.ListRoutesRequest) (*pb.ListRoutesResponse, error) {
	snapshot, err := s.catalog.Snapshot(ctx)
	if err != nil {
		return nil, err
	}
	return &pb.ListRoutesResponse{
		CatalogVersion: snapshot.Version,
		Routes:         toProtoRouteDescriptors(snapshot.Routes),
	}, nil
}

func newBufconnDistributionClient(t *testing.T, server pb.DistributionServer) (pb.DistributionClient, func()) {
	t.Helper()

	listener := bufconn.Listen(1024 * 1024)
	grpcServer := grpc.NewServer()
	pb.RegisterDistributionServer(grpcServer, server)
	serveErr := make(chan error, 1)
	go func() { serveErr <- grpcServer.Serve(listener) }()

	conn, err := grpc.NewClient(
		"passthrough:///bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return listener.Dial()
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)

	cleanup := func() {
		require.NoError(t, conn.Close())
		grpcServer.Stop()
		err := <-serveErr
		if err != nil {
			require.ErrorIs(t, err, grpc.ErrServerStopped)
		}
		require.NoError(t, listener.Close())
	}
	return pb.NewDistributionClient(conn), cleanup
}

type captureCatalogWatchStream struct {
	ctx    context.Context
	cancel context.CancelFunc
	events chan *pb.CatalogWatchEvent
	limit  int
}

func newCaptureCatalogWatchStream() *captureCatalogWatchStream {
	const limit = 1
	ctx, cancel := context.WithCancel(context.Background())
	return &captureCatalogWatchStream{
		ctx:    ctx,
		cancel: cancel,
		events: make(chan *pb.CatalogWatchEvent, limit),
		limit:  limit,
	}
}

func (s *captureCatalogWatchStream) Send(event *pb.CatalogWatchEvent) error {
	s.events <- event
	if len(s.events) >= s.limit {
		s.cancel()
	}
	return nil
}

func (s *captureCatalogWatchStream) SetHeader(metadata.MD) error  { return nil }
func (s *captureCatalogWatchStream) SendHeader(metadata.MD) error { return nil }
func (s *captureCatalogWatchStream) SetTrailer(metadata.MD)       {}
func (s *captureCatalogWatchStream) Context() context.Context     { return s.ctx }
func (s *captureCatalogWatchStream) SendMsg(message any) error {
	event, ok := message.(*pb.CatalogWatchEvent)
	if !ok {
		return io.ErrUnexpectedEOF
	}
	return s.Send(event)
}
func (s *captureCatalogWatchStream) RecvMsg(any) error { return io.EOF }
