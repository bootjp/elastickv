package adapter

import (
	"context"
	"net"
	"slices"
	"testing"
	"time"

	"github.com/bootjp/elastickv/kv"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

type legacyMetadataTestSource struct {
	passes []LegacyMetadataPassSnapshot
	err    error
}

func (s *legacyMetadataTestSource) CompleteLegacyMetadataGroupPasses(context.Context, uint64) ([]LegacyMetadataPassSnapshot, error) {
	return s.passes, s.err
}

func (s *legacyMetadataTestSource) LegacyMetadataPassSnapshots(context.Context) []LegacyMetadataPassSnapshot {
	return s.passes
}

func (s *legacyMetadataTestSource) LegacyMetadataPassGroupIDs() []uint64 {
	groups := make(map[uint64]struct{})
	for _, pass := range s.passes {
		groups[pass.GroupID] = struct{}{}
	}
	ids := make([]uint64, 0, len(groups))
	for id := range groups {
		ids = append(ids, id)
	}
	return ids
}

func completeLegacyMetadataFixture() []LegacyMetadataPassSnapshot {
	start := time.Unix(100, 0)
	passes := make([]LegacyMetadataPassSnapshot, 0, len(legacyMetadataPlanNames))
	for _, plan := range legacyMetadataPlanNames {
		passes = append(passes, LegacyMetadataPassSnapshot{GroupID: 7, Plan: plan,
			CatalogVersion: 11, LeaderTerm: 3, StartedAt: start, CompletionEpoch: start.Add(time.Minute),
			Counts: map[uint64]LegacyMetadataReasonCounts{9: {Served: 2}, 8: {OwnerLedElsewhere: 1}},
		})
	}
	return passes
}

func TestLegacyMetadataAdminRPCRejectsPartialAndMixedPasses(t *testing.T) {
	srv := NewAdminServer(NodeIdentity{}, nil)
	request := &pb.GetRedisLegacyMetadataPassesRequest{GroupId: 7}
	_, err := srv.GetRedisLegacyMetadataPasses(t.Context(), request)
	require.Equal(t, codes.Unavailable, status.Code(err))
	_, err = srv.GetRedisLegacyMetadataPasses(t.Context(), &pb.GetRedisLegacyMetadataPassesRequest{})
	require.Equal(t, codes.InvalidArgument, status.Code(err))
	source := &legacyMetadataTestSource{passes: completeLegacyMetadataFixture()[:3]}
	srv.RegisterLegacyMetadataPassSource(source)
	response, err := srv.GetRedisLegacyMetadataPasses(t.Context(), request)
	require.Nil(t, response)
	require.Equal(t, codes.FailedPrecondition, status.Code(err), "partial passes cannot look like zero backlog")
	source.passes = completeLegacyMetadataFixture()
	source.passes[3].CatalogVersion++
	response, err = srv.GetRedisLegacyMetadataPasses(t.Context(), request)
	require.Nil(t, response)
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
	source.passes = completeLegacyMetadataFixture()
	source.passes[3].LeaderTerm++
	response, err = srv.GetRedisLegacyMetadataPasses(t.Context(), request)
	require.Nil(t, response)
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
	source.passes = completeLegacyMetadataFixture()
	source.passes[3].Plan = source.passes[0].Plan
	response, err = srv.GetRedisLegacyMetadataPasses(t.Context(), request)
	require.Nil(t, response)
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
	source.passes = completeLegacyMetadataFixture()
	source.passes[3].GroupID = 8
	response, err = srv.GetRedisLegacyMetadataPasses(t.Context(), request)
	require.Nil(t, response)
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
	source.passes = completeLegacyMetadataFixture()
	source.err = errors.WithStack(ErrLegacyMetadataPassIncomplete)
	response, err = srv.GetRedisLegacyMetadataPasses(t.Context(), request)
	require.Nil(t, response)
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
}

func TestLegacyMetadataAdminRPCReturnsCompleteDiagnosticSet(t *testing.T) {
	srv := NewAdminServer(NodeIdentity{}, nil)
	srv.RegisterLegacyMetadataPassSource(&legacyMetadataTestSource{passes: completeLegacyMetadataFixture()})
	response, err := srv.GetRedisLegacyMetadataPasses(t.Context(), &pb.GetRedisLegacyMetadataPassesRequest{GroupId: 7})
	require.NoError(t, err)
	require.Equal(t, uint64(7), response.GetGroupId())
	require.Equal(t, uint64(11), response.GetCatalogVersion())
	require.Equal(t, uint64(3), response.GetLeaderTerm())
	require.Len(t, response.GetPasses(), 4)
	for _, pass := range response.GetPasses() {
		require.Equal(t, uint64(7), pass.GetGroupId())
		require.Equal(t, uint64(11), pass.GetCatalogVersion())
		require.Equal(t, uint64(3), pass.GetLeaderTerm())
		require.Equal(t, int64(100_000_000_000), pass.GetStartedAtUnixNanos())
		require.Equal(t, int64(160_000_000_000), pass.GetCompletionEpochUnixNanos())
		require.Equal(t, []uint64{8, 9}, []uint64{pass.GetCounts()[0].GetOwnerGroupId(), pass.GetCounts()[1].GetOwnerGroupId()})
		require.Equal(t, uint64(1), pass.GetCounts()[0].GetOwnerLedElsewhere())
		require.Equal(t, uint64(2), pass.GetCounts()[1].GetServed())
	}
}

func TestLegacyMetadataAdminRPCBearerProtocol(t *testing.T) {
	srv := NewAdminServer(NodeIdentity{}, nil)
	srv.RegisterLegacyMetadataPassSource(&legacyMetadataTestSource{passes: completeLegacyMetadataFixture()})
	unary, _ := AdminTokenAuth("admin-token")
	listener := bufconn.Listen(1 << 20)
	server := grpc.NewServer(grpc.UnaryInterceptor(unary))
	pb.RegisterAdminServer(server, srv)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() { server.Stop(); _ = listener.Close() })
	conn, err := grpc.NewClient("passthrough:///bufnet", grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
		return listener.Dial()
	}), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	client := pb.NewAdminClient(conn)
	request := &pb.GetRedisLegacyMetadataPassesRequest{GroupId: 7}
	_, err = client.GetRedisLegacyMetadataPasses(t.Context(), request)
	require.Equal(t, codes.Unauthenticated, status.Code(err), "%v", err)
	wrong := metadata.AppendToOutgoingContext(t.Context(), "authorization", "Bearer wrong")
	_, err = client.GetRedisLegacyMetadataPasses(wrong, request)
	require.Equal(t, codes.Unauthenticated, status.Code(err))
	correct := metadata.AppendToOutgoingContext(t.Context(), "authorization", "Bearer admin-token")
	response, err := client.GetRedisLegacyMetadataPasses(correct, request)
	require.NoError(t, err)
	require.Len(t, response.GetPasses(), 4)
}

func TestLegacyMetadataMetricsOmitIncompleteGroup(t *testing.T) {
	source := &legacyMetadataTestSource{passes: completeLegacyMetadataFixture()[:3]}
	registry := prometheus.NewRegistry()
	registry.MustRegister(NewLegacyMetadataPassCollector(source))
	metrics, err := registry.Gather()
	require.NoError(t, err)
	require.Empty(t, metrics, "a partial group must not expose a zero-looking metric")
	source.passes = completeLegacyMetadataFixture()
	metrics, err = registry.Gather()
	require.NoError(t, err)
	names := make([]string, 0, len(metrics))
	for _, family := range metrics {
		names = append(names, family.GetName())
		if family.GetName() == "elastickv_redis_legacy_metadata_pass_complete" {
			require.Len(t, family.GetMetric(), 4)
		}
	}
	require.True(t, slices.Contains(names, "elastickv_redis_legacy_metadata_pass_complete"))
	require.True(t, slices.Contains(names, "elastickv_redis_legacy_metadata_pass_rows"))
	source.err = errors.WithStack(ErrLegacyMetadataPassIncomplete)
	metrics, err = registry.Gather()
	require.NoError(t, err)
	require.Empty(t, metrics, "stale published gauges must disappear at scrape time")
}

func TestLegacyMetadataMetricsChecksOnlyEachGroup(t *testing.T) {
	bad := &diagnosticTTLInlineStore{MVCCStore: store.NewMVCCStore(), version: 12, term: 3}
	good := &diagnosticTTLInlineStore{MVCCStore: store.NewMVCCStore(), version: 11, term: 3}
	c := NewDeltaCompactor(good, newLocalAdapterCoordinator(good))
	for _, pass := range completeLegacyMetadataFixture() {
		badPass := pass
		badPass.GroupID = 6
		c.legacyMetadataPasses[ttlInlineMigrationGroupCursorName(pass.Plan, 6)] = legacyMetadataPublishedPass{snapshot: badPass, scanner: bad}
		c.legacyMetadataPasses[ttlInlineMigrationGroupCursorName(pass.Plan, 7)] = legacyMetadataPublishedPass{snapshot: pass, scanner: good}
	}
	passes, err := c.CompleteLegacyMetadataGroupPasses(t.Context(), 7)
	require.NoError(t, err)
	require.Len(t, passes, 4)
	require.Zero(t, bad.identityCalls, "a group lookup must not probe unrelated leaders")
	require.LessOrEqual(t, good.identityCalls, 5)

	registry := prometheus.NewRegistry()
	registry.MustRegister(NewLegacyMetadataPassCollector(c))
	metrics, err := registry.Gather()
	require.NoError(t, err)
	foundComplete := false
	for _, family := range metrics {
		if family.GetName() == "elastickv_redis_legacy_metadata_pass_complete" {
			foundComplete = true
			require.Len(t, family.GetMetric(), 4, "an invalid group must not hide a healthy group")
		}
	}
	require.True(t, foundComplete)
	require.LessOrEqual(t, good.identityCalls, 10, "a scrape should validate each plan once per group")
}

func TestLegacyMetadataCompactorRequiresFourCurrentPlans(t *testing.T) {
	st := &diagnosticTTLInlineStore{MVCCStore: store.NewMVCCStore(), version: 11, term: 3}
	for range legacyMetadataPlanNames {
		st.pages = append(st.pages, kv.GroupOwnedScanPage{CatalogVersion: 11, LeaderTerm: 3})
	}
	c := NewDeltaCompactor(st, newLocalAdapterCoordinator(st))
	srv := NewAdminServer(NodeIdentity{}, nil)
	srv.RegisterLegacyMetadataPassSource(c)
	request := &pb.GetRedisLegacyMetadataPassesRequest{GroupId: 7}
	for index, name := range legacyMetadataPlanNames {
		h := ttlInlineMigrationHandler{typeName: name, prefix: []byte(store.HashMetaPrefix)}
		plan := ttlInlineMigrationScanPlan{cursorName: ttlInlineMigrationGroupCursorName(name, 7), groupID: 7, scanner: st}
		require.NoError(t, c.migrateTTLInlineScanPlan(t.Context(), h, plan, 10, time.Now()))
		passes, err := c.CompleteLegacyMetadataGroupPasses(t.Context(), 7)
		if index < len(legacyMetadataPlanNames)-1 {
			require.Nil(t, passes)
			require.ErrorIs(t, err, ErrLegacyMetadataPassIncomplete)
			response, rpcErr := srv.GetRedisLegacyMetadataPasses(t.Context(), request)
			require.Nil(t, response)
			require.Equal(t, codes.FailedPrecondition, status.Code(rpcErr))
		} else {
			require.NoError(t, err)
			require.Len(t, passes, 4)
			response, rpcErr := srv.GetRedisLegacyMetadataPasses(t.Context(), request)
			require.NoError(t, rpcErr)
			require.Len(t, response.GetPasses(), 4)
		}
	}
	st.term++
	passes, err := c.CompleteLegacyMetadataGroupPasses(t.Context(), 7)
	require.Nil(t, passes)
	require.ErrorIs(t, err, ErrLegacyMetadataPassIncomplete)
	response, rpcErr := srv.GetRedisLegacyMetadataPasses(t.Context(), request)
	require.Nil(t, response)
	require.Equal(t, codes.FailedPrecondition, status.Code(rpcErr))
}
