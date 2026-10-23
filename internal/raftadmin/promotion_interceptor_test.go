package raftadmin

import (
	"context"
	"testing"

	"github.com/bootjp/elastickv/internal/raftengine"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type promotionInterceptor struct {
	recordingInterceptor
	promote func(context.Context, string, string) error
}

func (p *promotionInterceptor) PrePromoteMember(ctx context.Context, id, address string) error {
	return p.promote(ctx, id, address)
}

func TestPromotionInterceptorRunsBeforeProposal(t *testing.T) {
	t.Parallel()
	for _, reject := range []bool{false, true} {
		t.Run(map[bool]string{false: "accept", true: "reject"}[reject], func(t *testing.T) {
			t.Parallel()
			engine := &fakeEngine{config: raftengine.Configuration{Servers: []raftengine.Server{
				{ID: "candidate", Address: "candidate:50051", Suffrage: "learner"},
			}}}
			calls := 0
			hook := &promotionInterceptor{promote: func(_ context.Context, id, address string) error {
				require.Empty(t, engine.promoteLearnerCalls)
				require.Equal(t, "candidate", id)
				require.Equal(t, "candidate:50051", address)
				calls++
				if reject {
					return status.Error(codes.FailedPrecondition, "migration protocol unsupported")
				}
				return nil
			}}
			server := NewServerWithInterceptor(engine, hook)
			resp, err := server.PromoteLearner(t.Context(), &pb.RaftAdminPromoteLearnerRequest{
				Id: "candidate", PreviousIndex: 7, MinAppliedIndex: 99,
			})
			require.Equal(t, 1, calls)
			if reject {
				require.Equal(t, codes.FailedPrecondition, status.Code(err))
				require.Nil(t, resp)
				require.Empty(t, engine.promoteLearnerCalls)
				return
			}
			require.NoError(t, err)
			require.Equal(t, uint64(44), resp.GetIndex())
			require.Equal(t, []fakePromoteLearnerCall{{id: "candidate", prevIndex: 7, minAppliedIndex: 99}}, engine.promoteLearnerCalls)
			require.Empty(t, hook.calls, "promotion must not rerun encryption registration")
		})
	}
}

func TestPromotionInterceptorRejectsMissingAddressOrPendingConfiguration(t *testing.T) {
	t.Parallel()
	for _, name := range []string{"missing-member", "missing-address", "pending-configuration"} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			engine := &fakeEngine{}
			if name == "missing-address" {
				engine.config.Servers = []raftengine.Server{{ID: "candidate", Suffrage: "learner"}}
			}
			if name == "pending-configuration" {
				engine.status.PendingConfChange = true
			}
			hook := &promotionInterceptor{promote: func(context.Context, string, string) error {
				t.Fatal("invalid configuration must not reach the capability probe")
				return nil
			}}
			server := NewServerWithInterceptor(engine, hook)
			resp, err := server.PromoteLearner(t.Context(), &pb.RaftAdminPromoteLearnerRequest{Id: "candidate", MinAppliedIndex: 99})
			require.Equal(t, codes.FailedPrecondition, status.Code(err))
			require.Nil(t, resp)
			require.Empty(t, engine.promoteLearnerCalls)
		})
	}
}
