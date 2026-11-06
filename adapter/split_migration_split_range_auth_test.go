package adapter

import (
	"context"
	"strconv"
	"testing"

	"github.com/bootjp/elastickv/distribution"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

func TestSplitRangeAuthRejectsProofFreeCatalogMutationDuringRollout(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name, configuredToken, credential string
		tls, allowed                      bool
	}{
		{name: "unconfigured", credential: "Bearer migration-secret", tls: true},
		{name: "anonymous", configuredToken: "migration-secret"},
		{name: "token-without-TLS", configuredToken: "migration-secret", credential: "Bearer migration-secret"},
		{name: "TLS-only", configuredToken: "migration-secret", tls: true},
		{name: "read-only-admin-token", configuredToken: "migration-secret", credential: "Bearer admin-secret", tls: true},
		{name: "blob-peer-token", configuredToken: "migration-secret", credential: "Bearer blob-secret", tls: true},
		{name: "migration-token", configuredToken: "migration-secret", credential: "Bearer migration-secret", tls: true, allowed: true},
	}
	for _, enforced := range []bool{false, true} {
		for _, tc := range cases {
			t.Run(tc.name+"/enforced="+strconv.FormatBool(enforced), func(t *testing.T) {
				server, coordinator := proofFreeSplitRangeAuthServer(t)
				ctx := metadata.NewIncomingContext(t.Context(), metadata.Pairs("authorization", tc.credential))
				if tc.tls {
					ctx = peer.NewContext(ctx, &peer.Peer{AuthInfo: migrationTLSInfoFixture(t)})
				}
				unary, _ := SplitMigrationTokenAuth(tc.configuredToken, false, enforced)
				request := &pb.SplitRangeRequest{ExpectedCatalogVersion: 1, RouteId: 1, SplitKey: []byte("m")}
				_, err := unary(ctx, request, &grpc.UnaryServerInfo{FullMethod: pb.Distribution_SplitRange_FullMethodName},
					func(ctx context.Context, _ any) (any, error) { return server.SplitRange(ctx, request) })
				assertSplitRangeAuthResult(t, server, coordinator, tc.allowed, err)
			})
		}
	}
}

func proofFreeSplitRangeAuthServer(t *testing.T) (*DistributionServer, *distributionCoordinatorStub) {
	t.Helper()
	st := store.NewMVCCStore()
	t.Cleanup(func() { require.NoError(t, st.Close()) })
	catalog := distribution.NewCatalogStore(st)
	saved, err := catalog.Save(t.Context(), 0, []distribution.RouteDescriptor{
		{RouteID: 1, Start: []byte("a"), End: []byte("z"), GroupID: 7, State: distribution.RouteStateActive},
	})
	require.NoError(t, err)
	engine := distribution.NewEngine()
	require.NoError(t, engine.ApplySnapshot(saved))
	coordinator := newDistributionCoordinatorStub(st, true)
	return NewDistributionServer(engine, catalog, WithDistributionCoordinator(coordinator)), coordinator
}

func assertSplitRangeAuthResult(t *testing.T, server *DistributionServer, coordinator *distributionCoordinatorStub, allowed bool, err error) {
	t.Helper()
	snapshot, snapshotErr := server.catalog.Snapshot(t.Context())
	require.NoError(t, snapshotErr)
	if allowed {
		require.NoError(t, err)
		require.EqualValues(t, 2, snapshot.Version)
		require.EqualValues(t, 2, server.engine.Version())
		require.Equal(t, 1, coordinator.dispatchCalls)
		return
	}
	require.EqualValues(t, 1, snapshot.Version, "unauthorized SplitRange must not advance the global catalog")
	require.EqualValues(t, 1, server.engine.Version())
	require.Zero(t, coordinator.dispatchCalls)
	require.Equal(t, codes.Unauthenticated, status.Code(err))
}
