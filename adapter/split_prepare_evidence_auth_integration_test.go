package adapter

import (
	"context"
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

func TestSplitAndPrepareEvidenceCredentialsRemainIndependent(t *testing.T) {
	t.Parallel()
	migration, _ := SplitMigrationTokenAuth("migration-secret", false, true)
	admin, _ := AdminTokenAuth("evidence-secret")
	leader := &prepareSafetyRPCLeader{}
	server := NewInternalWithEngine(nil, leader, nil, nil, WithInternalStore(store.NewMVCCStore()), WithInternalPrepareEvidenceToken("evidence-secret"))
	for _, tc := range []struct {
		name, method, token string
		request             any
		allowed             bool
	}{
		{"evidence missing", pb.Internal_GetPrepareEvidenceState_FullMethodName, "", &pb.PrepareEvidenceStateRequest{}, false},
		{"migration cannot inspect evidence", pb.Internal_GetPrepareEvidenceState_FullMethodName, "migration-secret", &pb.PrepareEvidenceStateRequest{}, false},
		{"evidence peer", pb.Internal_GetPrepareEvidenceState_FullMethodName, "evidence-secret", &pb.PrepareEvidenceStateRequest{}, true},
		{"admin cannot inject jobs", pb.Internal_Forward_FullMethodName, "evidence-secret", &pb.ForwardRequest{Requests: []*pb.Request{{Mutations: []*pb.Mutation{{Op: pb.Op_PUT, Key: distribution.CatalogSplitJobKey(7)}}}}}, false},
		{"migration peer", pb.Internal_Forward_FullMethodName, "migration-secret", &pb.ForwardRequest{Requests: []*pb.Request{{Mutations: []*pb.Mutation{{Op: pb.Op_PUT, Key: distribution.CatalogSplitJobKey(7)}}}}}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := metadata.NewIncomingContext(t.Context(), metadata.Pairs("authorization", "Bearer "+tc.token))
			ctx = peer.NewContext(ctx, &peer.Peer{AuthInfo: migrationTLSInfoFixture(t)})
			info := &grpc.UnaryServerInfo{FullMethod: tc.method}
			called := false
			_, err := migration(ctx, tc.request, info, func(ctx context.Context, request any) (any, error) {
				return admin(ctx, request, info, func(ctx context.Context, _ any) (any, error) {
					called = true
					if tc.method == pb.Internal_GetPrepareEvidenceState_FullMethodName {
						return server.GetPrepareEvidenceState(ctx, &pb.PrepareEvidenceStateRequest{})
					}
					return nil, nil
				})
			})
			require.Equal(t, tc.allowed, called)
			if tc.allowed {
				require.NoError(t, err)
			} else {
				require.Equal(t, codes.Unauthenticated, status.Code(err))
			}
		})
	}
	require.Equal(t, int32(1), leader.reads.Load(), "only the evidence credential may trigger quorum inspection")
}
