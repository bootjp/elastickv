package main

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/bootjp/elastickv/internal/encryption"
	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/bootjp/elastickv/kv"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func externalRotationTestRequest(f *rotationRecipeFixture) *pb.RotateDEKRequest {
	return &pb.RotateDEKRequest{Purpose: pb.RotateDEKRequest_PURPOSE_STORAGE, NewDekId: 20,
		WrappedNewDek: bytes.Repeat([]byte{9}, encryption.KeySize), ProposerNodeId: f.cfg.NodeID, ProposerLocalEpoch: uint32(f.cfg.StorageEpoch)}
}

func TestBootstrapExternalRotationRecipeRejectsInvalidRequests(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*pb.RotateDEKRequest) *pb.RotateDEKRequest
	}{
		{"nil", func(*pb.RotateDEKRequest) *pb.RotateDEKRequest { return nil }},
		{"unknown", func(r *pb.RotateDEKRequest) *pb.RotateDEKRequest {
			r.ProtoReflect().SetUnknown([]byte{0x38, 1})
			return r
		}},
		{"purpose", func(r *pb.RotateDEKRequest) *pb.RotateDEKRequest {
			r.Purpose = pb.RotateDEKRequest_Purpose(3)
			return r
		}},
		{"zero_id", func(r *pb.RotateDEKRequest) *pb.RotateDEKRequest { r.NewDekId = 0; return r }},
		{"foreign_node", func(r *pb.RotateDEKRequest) *pb.RotateDEKRequest { r.ProposerNodeId++; return r }},
		{"wrong_epoch", func(r *pb.RotateDEKRequest) *pb.RotateDEKRequest { r.ProposerLocalEpoch++; return r }},
		{"epoch_overflow", func(r *pb.RotateDEKRequest) *pb.RotateDEKRequest { r.ProposerLocalEpoch = 65536; return r }},
		{"empty", func(r *pb.RotateDEKRequest) *pb.RotateDEKRequest { r.WrappedNewDek = nil; return r }},
		{"oversized", func(r *pb.RotateDEKRequest) *pb.RotateDEKRequest { r.WrappedNewDek = make([]byte, 4097); return r }},
		{"wrong_key_size", func(r *pb.RotateDEKRequest) *pb.RotateDEKRequest { r.WrappedNewDek = make([]byte, 31); return r }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newRotationRecipeFixture(t)
			_, err := newBootstrapExternalRotationRecipe(t.Context(), f.cfg, tc.mutate(externalRotationTestRequest(f)))
			require.ErrorIs(t, err, errBootstrapExternalRotationRequest)
			require.Zero(t, f.barriers, "malformed requests must not start IO")
			require.Empty(t, f.calls)
		})
	}
}

func TestBootstrapExternalRotationRecipeExactImmutableRequest(t *testing.T) {
	f := newRotationRecipeFixture(t)
	req := externalRotationTestRequest(f)
	expected := cloneBootstrapRotationRequest(req)
	r, err := newBootstrapExternalRotationRecipe(t.Context(), f.cfg, req)
	require.NoError(t, err)
	require.Positive(t, f.barriers)
	require.True(t, proto.Equal(expected, req), "unwrap must not mutate the caller's crypto bytes")
	req.WrappedNewDek[0]++
	req.NewDekId++
	require.True(t, proto.Equal(expected, r.Requests()[0]))
	returned := r.Requests()
	returned[0].WrappedNewDek[0]++
	receipt, err := r.Run(t.Context())
	require.NoError(t, err)
	require.True(t, receipt.Complete)
	require.Len(t, f.calls, 1)
	require.True(t, proto.Equal(expected, f.calls[0]))
	retry, err := newBootstrapExternalRotationRecipe(t.Context(), f.cfg, expected)
	require.NoError(t, err)
	receipt, err = retry.Run(t.Context())
	require.NoError(t, err)
	require.True(t, receipt.Complete)
	require.Len(t, f.calls, 1, "exact active retry must verify, not repropose")
}

func TestBootstrapExternalRotationRecipeCollisionAndSuperseded(t *testing.T) {
	f := newRotationRecipeFixture(t)
	req := externalRotationTestRequest(f)
	f.apply(req)
	for _, mutate := range []func(*pb.RotateDEKRequest){
		func(r *pb.RotateDEKRequest) { r.WrappedNewDek[0]++ },
		func(r *pb.RotateDEKRequest) {
			r.Purpose = pb.RotateDEKRequest_PURPOSE_RAFT
			r.ProposerLocalEpoch = uint32(f.cfg.RaftEpoch)
		},
	} {
		collision := cloneBootstrapRotationRequest(req)
		mutate(collision)
		_, err := newBootstrapExternalRotationRecipe(t.Context(), f.cfg, collision)
		require.ErrorIs(t, err, errBootstrapExternalRotationRequest)
	}
	foreign := cloneBootstrapRotationRequest(req)
	foreign.NewDekId++
	foreign.WrappedNewDek[0]++
	f.apply(foreign)
	r, err := newBootstrapExternalRotationRecipe(t.Context(), f.cfg, req)
	require.NoError(t, err)
	receipt, err := r.Run(t.Context())
	require.NoError(t, err)
	require.Equal(t, bootstrapRotationSuperseded, receipt.Outcome)
	require.False(t, receipt.Complete)
	require.Empty(t, f.calls, "historical exact key must not replace a newer leader plan")
}

func TestBootstrapExternalRotationRecipeLostResultFollowerReconcile(t *testing.T) {
	f := newRotationRecipeFixture(t)
	req := externalRotationTestRequest(f)
	r, err := newBootstrapExternalRotationRecipe(t.Context(), f.cfg, req)
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(t.Context())
	f.admin = func(_ context.Context, request *pb.RotateDEKRequest) (*pb.RotateDEKResponse, error) {
		f.apply(request)
		f.leader = false
		cancel()
		return nil, raftengine.ErrLeadershipLost
	}
	_, err = r.Run(ctx)
	require.ErrorIs(t, err, raftengine.ErrLeadershipLost)
	require.ErrorIs(t, err, context.Canceled)
	receipt, err := r.Reconcile(t.Context())
	require.NoError(t, err)
	require.True(t, receipt.Complete)
	require.Len(t, f.calls, 1, "follower can only reconcile the original exact request")
	require.Equal(t, r.Digest(), receipt.RecipeDigest)
}

func TestBootstrapExternalRotationRecipeCancelledAbsentAndTermChange(t *testing.T) {
	f := newRotationRecipeFixture(t)
	r, err := newBootstrapExternalRotationRecipe(t.Context(), f.cfg, externalRotationTestRequest(f))
	require.NoError(t, err)
	f.admin = func(context.Context, *pb.RotateDEKRequest) (*pb.RotateDEKResponse, error) {
		return nil, context.Canceled
	}
	_, err = r.Run(t.Context())
	require.ErrorIs(t, err, kv.ErrTransactionOutcomeUnknown)
	f.leader = false
	receipt, err := r.Reconcile(t.Context())
	require.NoError(t, err)
	require.False(t, receipt.Complete)
	require.Len(t, f.calls, 1)
	_, err = newBootstrapExternalRotationRecipe(t.Context(), f.cfg, externalRotationTestRequest(f))
	require.ErrorIs(t, err, raftengine.ErrNotLeader)
}

func TestBootstrapExternalRotationRecipeFreshBarrierRequired(t *testing.T) {
	f := newRotationRecipeFixture(t)
	f.barrierErr = errors.New("read index unavailable")
	_, err := newBootstrapExternalRotationRecipe(t.Context(), f.cfg, externalRotationTestRequest(f))
	require.ErrorIs(t, err, f.barrierErr)
	require.Empty(t, f.calls)
}

type externalRotationBoundedWrapper struct{ fakeStartupKEK }

func (externalRotationBoundedWrapper) Unwrap([]byte) ([]byte, error) {
	return bytes.Repeat([]byte{7}, encryption.KeySize), nil
}

func TestBootstrapExternalRotationRecipePurposeEpochAndWrappedLimit(t *testing.T) {
	f := newRotationRecipeFixture(t)
	f.cfg.Wrapper = externalRotationBoundedWrapper{}
	req := externalRotationTestRequest(f)
	req.Purpose, req.ProposerLocalEpoch = pb.RotateDEKRequest_PURPOSE_RAFT, uint32(f.cfg.RaftEpoch)
	req.WrappedNewDek = bytes.Repeat([]byte{8}, 4096)
	r, err := newBootstrapExternalRotationRecipe(t.Context(), f.cfg, req)
	require.NoError(t, err, "the maximum bounded wrapped material is valid")
	receipt, err := r.Run(t.Context())
	require.NoError(t, err)
	require.True(t, receipt.Complete)
	require.Equal(t, req.NewDekId, receipt.Active.Raft)
	require.Equal(t, uint32(1), receipt.Active.Storage, "one request rotates only its own purpose")
}

func TestBootstrapExternalRotationRecipeForeignPlanAfterConstruction(t *testing.T) {
	f := newRotationRecipeFixture(t)
	req := externalRotationTestRequest(f)
	r, err := newBootstrapExternalRotationRecipe(t.Context(), f.cfg, req)
	require.NoError(t, err)
	foreign := cloneBootstrapRotationRequest(req)
	foreign.NewDekId++
	f.apply(foreign)
	receipt, err := r.Run(t.Context())
	require.NoError(t, err)
	require.Equal(t, bootstrapRotationSuperseded, receipt.Outcome)
	require.False(t, receipt.Complete)
	require.Empty(t, f.calls)
}
