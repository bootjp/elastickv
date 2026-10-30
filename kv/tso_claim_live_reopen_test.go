package kv

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/bootjp/elastickv/internal/raftengine/raftenginetest"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

type lostNativeClaimReply struct {
	raftengine.Engine
	lost atomic.Bool
}

func (e *lostNativeClaimReply) Propose(ctx context.Context, payload []byte) (*raftengine.ProposalResult, error) {
	result, err := e.Engine.Propose(ctx, payload)
	if err == nil && bytes.HasPrefix(payload, []byte(tsoClaimIntentEnvelope)) && e.lost.CompareAndSwap(false, true) {
		return nil, context.DeadlineExceeded
	}
	return result, err
}

func TestTSOProductionClaimsNativeReopenUnknownAttempt(t *testing.T) {
	directory := t.TempDir()
	factory := newSingleRaftFactory()
	identity, err := NewTSOClaimIdentity(bytes.Repeat([]byte{31}, ed25519.SeedSize))
	require.NoError(t, err)
	open := func(loseReply bool) (*RaftTSOAllocator, raftengine.Engine) {
		clock := NewHLC()
		state := NewTSOStateMachine(clock)
		result, err := factory.Create(raftengine.FactoryConfig{LocalID: "n1", LocalAddress: "127.0.0.1:0", DataDir: directory,
			Bootstrap: true, StateMachine: state})
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, result.Engine.Close()) })
		raftenginetest.WaitForLeader(t, 5*time.Second, result.Engine)
		engine := result.Engine
		if loseReply {
			engine = &lostNativeClaimReply{Engine: engine}
		}
		allocator, err := NewRaftTSOAllocator(&ShardGroup{Engine: engine, TSOState: state}, clock,
			WithTSOCutoverFloorProvider(&recordingTSOFloorProvider{}), WithTSOPhaseDActivationGate(&testPhaseDGate{}),
			WithRaftTSOClaimIdentity(identity), WithTSOClaimAuthority(pinnedClaimSigner{key: identity.PublicKey()}))
		require.NoError(t, err)
		return allocator, engine
	}
	allocator, engine := open(true)
	for _, payload := range [][]byte{
		marshalHLCLeaseRenew(time.Now().Add(testTSOFutureCeiling).UnixMilli()), marshalTSOCutover(), marshalTSOPhaseD(1),
	} {
		require.NoError(t, proposeTSOClaim(t.Context(), engine, payload))
	}
	request := &pb.Request{TimestampClaimGroupId: 1, Mutations: []*pb.Mutation{{Op: pb.Op_PUT, Key: []byte("reopen"), Value: []byte("value")}}}
	require.NoError(t, EnsureRequestTimestampIntent(request))
	retained := proto.CloneOf(request)
	require.ErrorIs(t, IssueRequestTimestampClaim(t.Context(), allocator, request), ErrTSOClaimUnknown)
	allocator.state.claimMu.RLock()
	require.Len(t, allocator.state.claims.Claims, 1)
	var committed TSOClaimRecord
	for _, record := range allocator.state.claims.Claims {
		committed = record
	}
	allocator.state.claimMu.RUnlock()
	oldFloor := allocator.AllocationFloor()
	require.NoError(t, engine.Close())
	reopened, _ := open(false)
	require.NoError(t, IssueRequestTimestampClaim(t.Context(), reopened, retained))
	proof, err := DecodeRequestTimestampClaim(retained.TimestampClaim)
	require.NoError(t, err)
	require.Equal(t, committed, proof.Proof.Record, "replay cannot replace an applied-but-unreplied concrete slot")
	require.NoError(t, ValidateRequestTimestampClaim(t.Context(), reopened, retained))
	next, err := reopened.Next(t.Context())
	require.NoError(t, err)
	require.Greater(t, next, oldFloor)
	for slot := committed.Timestamp + 1; slot <= oldFloor; slot++ {
		require.ErrorIs(t, reopened.ValidateDurableTimestamp(t.Context(), slot), ErrTSOTimestampInvalid)
	}
}
