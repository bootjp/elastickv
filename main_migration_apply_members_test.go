package main

import (
	"context"
	"net"
	"testing"
	"time"

	internalutil "github.com/bootjp/elastickv/internal"
	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/bootjp/elastickv/kv"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type applyMemberEngine struct {
	capabilityConfigEngine
	index      uint64
	pending    bool
	configured func()
}

type applyMemberAuthServer struct{ pb.UnimplementedInternalServer }

func (applyMemberAuthServer) ProbeMigrationState(ctx context.Context, _ *pb.ProbeMigrationStateRequest) (*pb.ProbeMigrationStateResponse, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	if got := md.Get("authorization"); len(got) != 1 || got[0] != "Bearer migration-secret" {
		return nil, status.Error(codes.Unauthenticated, "missing migration credential")
	}
	if got := md.Get("trace-id"); len(got) != 1 || got[0] != "preserved" {
		return nil, status.Error(codes.InvalidArgument, "missing caller metadata")
	}
	return &pb.ProbeMigrationStateResponse{Ready: true}, nil
}

func TestSplitMigrationApplyMemberSnapshotFactoryPreservesTLSCredentials(t *testing.T) {
	cert, key := writeSelfSignedCert(t)
	require.NoError(t, internalutil.ConfigureGRPCTLS(cert, cert, key))
	t.Cleanup(func() { require.NoError(t, internalutil.ConfigureGRPCTLS("", "", "")) })
	server := grpc.NewServer(internalutil.GRPCServerOptions()...)
	pb.RegisterInternalServer(server, applyMemberAuthServer{})
	engine := newApplyMemberEngine()
	for index := range engine.cfg.Servers {
		var lc net.ListenConfig
		listener, err := lc.Listen(t.Context(), "tcp", "127.0.0.1:0")
		require.NoError(t, err)
		engine.cfg.Servers[index].Address = listener.Addr().String()
		done := make(chan error, 1)
		go func() { done <- server.Serve(listener) }()
		t.Cleanup(func() { server.Stop(); require.NoError(t, <-done) })
	}
	var cache kv.GRPCConnCache
	t.Cleanup(func() { require.NoError(t, cache.Close()) })
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	ctx = metadata.NewOutgoingContext(ctx, metadata.Pairs("authorization", "Bearer stale", "trace-id", "preserved"))
	snapshot, err := splitMigrationApplyMemberSnapshotFactory(map[uint64]*kv.ShardGroup{2: {Engine: engine}}, &cache, "migration-secret")(ctx, 2)
	require.NoError(t, err)
	for _, member := range snapshot.Members {
		_, err := member.Client.ProbeMigrationState(ctx, &pb.ProbeMigrationStateRequest{JobId: 11})
		require.NoError(t, err, member.ID)
		conn, err := cache.ConnFor(member.Address)
		require.NoError(t, err)
		_, err = pb.NewInternalClient(conn).ProbeMigrationState(ctx, &pb.ProbeMigrationStateRequest{JobId: 11})
		require.Equal(t, codes.Unauthenticated, status.Code(err), "shared cache must not retain a runner token")
	}
	md, _ := metadata.FromOutgoingContext(ctx)
	require.Equal(t, []string{"Bearer stale"}, md.Get("authorization"))
}

func (e *applyMemberEngine) AppliedConfigurationIndex() (uint64, bool) { return e.index, true }
func (e *applyMemberEngine) Status() raftengine.Status {
	return raftengine.Status{ConfigurationIndex: e.index, PendingConfChange: e.pending}
}
func (e *applyMemberEngine) Configuration(context.Context) (raftengine.Configuration, error) {
	if e.configured != nil {
		e.configured()
	}
	return e.cfg, nil
}

func newApplyMemberEngine() *applyMemberEngine {
	return &applyMemberEngine{index: 41, capabilityConfigEngine: capabilityConfigEngine{cfg: raftengine.Configuration{Servers: []raftengine.Server{
		{ID: "voter", Address: "127.0.0.1:19001", Suffrage: raftMemberSuffrageVoter},
		{ID: "learner", Address: "127.0.0.1:19002", Suffrage: "Nonvoter"},
	}}}}
}

func TestSplitMigrationApplyMemberSnapshotFactoryIncludesLearners(t *testing.T) {
	engine := newApplyMemberEngine()
	var cache kv.GRPCConnCache
	t.Cleanup(func() { require.NoError(t, cache.Close()) })
	factory := splitMigrationApplyMemberSnapshotFactory(map[uint64]*kv.ShardGroup{2: {Engine: engine}}, &cache, "migration-secret")
	snapshot, err := factory(t.Context(), 2)
	require.NoError(t, err)
	require.Equal(t, uint64(41), snapshot.ConfigurationIndex)
	require.Len(t, snapshot.Members, 2)
	require.Equal(t, "learner", snapshot.Members[1].ID)
	for _, member := range snapshot.Members {
		require.NotNil(t, member.Client)
	}
}

func TestSplitMigrationApplyMemberSnapshotFactoryRejectsUnstableConfiguration(t *testing.T) {
	cases := map[string]func(*applyMemberEngine){
		"zero epoch":        func(e *applyMemberEngine) { e.index = 0 },
		"pending before":    func(e *applyMemberEngine) { e.pending = true },
		"epoch changes":     func(e *applyMemberEngine) { e.configured = func() { e.index++ } },
		"pending after":     func(e *applyMemberEngine) { e.configured = func() { e.pending = true } },
		"empty":             func(e *applyMemberEngine) { e.cfg.Servers = nil },
		"missing id":        func(e *applyMemberEngine) { e.cfg.Servers[1].ID = "" },
		"duplicate id":      func(e *applyMemberEngine) { e.cfg.Servers[1].ID = e.cfg.Servers[0].ID },
		"duplicate address": func(e *applyMemberEngine) { e.cfg.Servers[1].Address = e.cfg.Servers[0].Address },
	}
	for name, change := range cases {
		t.Run(name, func(t *testing.T) {
			engine := newApplyMemberEngine()
			var cache kv.GRPCConnCache
			t.Cleanup(func() { require.NoError(t, cache.Close()) })
			groups := map[uint64]*kv.ShardGroup{2: {Engine: engine}}
			change(engine)
			_, err := splitMigrationApplyMemberSnapshotFactory(groups, &cache, "migration-secret")(t.Context(), 2)
			require.Error(t, err)
		})
	}
}

func TestSplitMigrationApplyMemberSnapshotFactoryRequiresWiring(t *testing.T) {
	var cache kv.GRPCConnCache
	t.Cleanup(func() { require.NoError(t, cache.Close()) })
	groups := map[uint64]*kv.ShardGroup{2: {Engine: newApplyMemberEngine()}}
	_, err := splitMigrationApplyMemberSnapshotFactory(groups, nil, "migration-secret")(t.Context(), 2)
	require.Error(t, err)
	_, err = splitMigrationApplyMemberSnapshotFactory(groups, &cache, "migration-secret")(t.Context(), 3)
	require.Error(t, err)
	groups[2].Engine = capabilityConfigEngine{}
	_, err = splitMigrationApplyMemberSnapshotFactory(groups, &cache, "migration-secret")(t.Context(), 2)
	require.Error(t, err)
}
