package adapter

import (
	"context"
	"testing"

	"github.com/bootjp/elastickv/distribution"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

type legacyOnlineMember struct{ SplitMigrationClient }

func (c legacyOnlineMember) ProbeMigrationState(ctx context.Context, request *pb.ProbeMigrationStateRequest, opts ...grpc.CallOption) (*pb.ProbeMigrationStateResponse, error) {
	response, err := c.SplitMigrationClient.ProbeMigrationState(ctx, request, opts...)
	if err != nil {
		return nil, err
	}
	response = proto.CloneOf(response)
	response.OnlineCaptureVersion = 0
	return response, nil
}
func TestNewSplitSelectsLegacyForUnsupportedApplyMember(t *testing.T) {
	for _, group := range []uint64{1, 2} {
		t.Run(map[uint64]string{1: "source member", 2: "target member"}[group], func(t *testing.T) {
			r := newResplitAcceptanceRuntime(t)
			original := r.server.splitApplyMemberFactory
			r.server.splitApplyMemberFactory = func(ctx context.Context, id uint64) (SplitMigrationApplyMemberSnapshot, error) {
				snapshot, err := original(ctx, id)
				if err == nil && id == group {
					snapshot.Members = append(snapshot.Members, SplitMigrationVoter{ID: "legacy-member", Address: "legacy:1", Client: legacyOnlineMember{r.clients[id]}})
				}
				return snapshot, err
			}
			job := distribution.SplitJob{JobID: 99, SourceGroupID: 1, TargetGroupID: 2}
			prior := &pb.ProbeMigrationStateResponse{OnlineCaptureVersion: store.OnlineMigrationVersion}
			control := &pb.TargetStagedReadinessRequest{}
			proof := &pb.MigrationOwnershipProof{Generation: "0123456789abcdef0123456789abcdef"}
			err := r.server.configureOnlineSourceTracker(t.Context(), job, prior, proof, 9, control)
			require.NoError(t, err)
			require.Zero(t, control.OnlineCaptureVersion)
			// Once capture has a durable nonce, an unsupported member cannot downgrade it.
			prior.OnlineSourceGeneration = proof.Generation
			err = r.server.configureOnlineSourceTracker(t.Context(), job, prior, proof, 9, control)
			require.ErrorIs(t, err, store.ErrOnlineMigrationIdentity)
			completed := r.migrate(t, 1, 2, "m")
			require.Equal(t, distribution.SplitJobPhaseDone, completed.Phase)
			require.Zero(t, completed.OnlineCaptureVersion)
		})
	}
}
