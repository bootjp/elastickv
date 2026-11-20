package kv

import (
	"context"
	"testing"

	"github.com/bootjp/elastickv/internal/raftengine"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/stretchr/testify/require"
)

func TestTimestampBindingCapabilityProbesEveryVoterAndLearner(t *testing.T) {
	for _, oldRole := range []string{"voter", "learner"} {
		t.Run(oldRole, func(t *testing.T) {
			shards, engine, _ := newPrepareSafetyGate(t)
			engine.version.Store(7)
			engine.members = append(engine.members, raftengine.Server{ID: "old", Suffrage: oldRole,
				Address: startPrepareSafetyPeer(t, &prepareSafetyPeer{probe: func(_ context.Context, req *pb.PrepareEvidenceStateRequest) (*pb.PrepareEvidenceStateResponse, error) {
					require.True(t, req.GetCapabilityOnly())
					return &pb.PrepareEvidenceStateResponse{Supported: true}, nil
				}})})
			require.ErrorIs(t, shards.CheckTimestampBindingGroupCapability(t.Context(), 1), ErrTimestampBindingCapability)
			require.Empty(t, engine.proposedPayloads())
		})
	}
}

func TestTimestampBindingCapabilityRejectsMembershipAndTermChange(t *testing.T) {
	for _, change := range []string{"none", "configuration", "term", "pending"} {
		t.Run(change, func(t *testing.T) {
			shards, engine, _ := newPrepareSafetyGate(t)
			engine.version.Store(7)
			engine.members = append(engine.members, raftengine.Server{ID: "peer", Suffrage: "learner",
				Address: startPrepareSafetyPeer(t, &prepareSafetyPeer{probe: func(_ context.Context, _ *pb.PrepareEvidenceStateRequest) (*pb.PrepareEvidenceStateResponse, error) {
					switch change {
					case "configuration":
						engine.version.Add(1)
					case "term":
						engine.mu.Lock()
						engine.term = 2
						engine.mu.Unlock()
					case "pending":
						engine.pending.Store(true)
					}
					return &pb.PrepareEvidenceStateResponse{Supported: true, TimestampBindingProtocolVersion: TimestampBindingProtocolVersion}, nil
				}})})
			err := shards.CheckTimestampBindingGroupCapability(t.Context(), 1)
			if change == "none" {
				require.NoError(t, err)
			} else {
				require.ErrorIs(t, err, ErrTimestampBindingCapability)
			}
			require.Empty(t, engine.proposedPayloads())
		})
	}
}

func TestTimestampBindingCapabilityRejectsIncompleteLocalRoster(t *testing.T) {
	shards, engine, _ := newPrepareSafetyGate(t)
	engine.version.Store(7)
	engine.members[0].ID = "other"
	require.ErrorIs(t, shards.CheckTimestampBindingGroupCapability(t.Context(), 1), ErrTimestampBindingCapability)
	engine.members[0].ID = "self"
	engine.members = append(engine.members, engine.members[0])
	require.ErrorIs(t, shards.CheckTimestampBindingGroupCapability(t.Context(), 1), ErrTimestampBindingCapability)
	engine.members = engine.members[:1]
	engine.pending.Store(true)
	require.ErrorIs(t, shards.CheckTimestampBindingGroupCapability(t.Context(), 1), ErrTimestampBindingCapability)
}
