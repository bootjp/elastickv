package adapter

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bootjp/elastickv/distribution"
	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/bootjp/elastickv/kv"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type prepareSafetyRPCLeader struct {
	forwardObserverLeader
	follower atomic.Bool
	reads    atomic.Int32
	loseRead bool
}

func (l *prepareSafetyRPCLeader) State() raftengine.State {
	if l.follower.Load() {
		return raftengine.StateFollower
	}
	return raftengine.StateLeader
}

func (l *prepareSafetyRPCLeader) LinearizableRead(context.Context) (uint64, error) {
	l.reads.Add(1)
	if l.loseRead {
		l.follower.Store(true)
	}
	return 23, nil
}

func prepareSafetyInternalClient(t *testing.T, server *Internal) pb.InternalClient {
	t.Helper()
	lis, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	auth, _ := AdminTokenAuth("prepare-safety-token")
	WithInternalPrepareEvidenceToken("prepare-safety-token")(server)
	srv := grpc.NewServer(grpc.UnaryInterceptor(auth))
	pb.RegisterInternalServer(srv, server)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(func() { srv.Stop(); _ = lis.Close() })
	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	return pb.NewInternalClient(conn)
}

func TestPrepareEvidenceSafetyRPCRequiresTokenAndLeaderFence(t *testing.T) {
	st := store.NewMVCCStore()
	fsm := kv.NewKvFSMWithHLC(st, kv.NewHLC())
	require.Nil(t, fsm.Apply(kv.EncodePrepareEvidenceFence(0)))
	leader := &prepareSafetyRPCLeader{}
	leader.follower.Store(true)
	client := prepareSafetyInternalClient(t, NewInternalWithEngine(nil, leader, nil, nil, WithInternalStore(st)))
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	for _, token := range []string{"", "wrong"} {
		unauthorized := metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+token)
		_, err := client.GetPrepareEvidenceState(unauthorized, &pb.PrepareEvidenceStateRequest{CapabilityOnly: true})
		require.Equal(t, codes.Unauthenticated, status.Code(err))
	}
	authorized := metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer prepare-safety-token")
	capability, err := client.GetPrepareEvidenceState(authorized, &pb.PrepareEvidenceStateRequest{CapabilityOnly: true})
	require.NoError(t, err)
	require.True(t, capability.GetSupported(), "a follower must advertise decoder support without a leader read")
	require.Zero(t, leader.reads.Load())
	_, err = client.GetPrepareEvidenceState(authorized, &pb.PrepareEvidenceStateRequest{})
	require.Error(t, err, "a follower must not attest that legacy locks drained")
	require.Zero(t, leader.reads.Load())
	leader.follower.Store(false)
	response, err := client.GetPrepareEvidenceState(authorized, &pb.PrepareEvidenceStateRequest{})
	require.NoError(t, err)
	require.True(t, response.GetProofEnabled())
	require.False(t, response.GetUnprovenLocks())
	require.Equal(t, uint64(23), response.GetAppliedIndex())
	require.Equal(t, int32(1), leader.reads.Load())
}

func TestPrepareEvidenceSafetyRPCDiscardsLeadershipLostDuringRead(t *testing.T) {
	leader := &prepareSafetyRPCLeader{loseRead: true}
	client := prepareSafetyInternalClient(t, NewInternalWithEngine(nil, leader, nil, nil, WithInternalStore(store.NewMVCCStore())))
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	ctx = metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer prepare-safety-token")
	response, err := client.GetPrepareEvidenceState(ctx, &pb.PrepareEvidenceStateRequest{})
	require.Error(t, err)
	require.Nil(t, response)
	require.Equal(t, int32(1), leader.reads.Load())
}

type prepareSafetyCutoverState struct{ active atomic.Bool }

func (s *prepareSafetyCutoverState) CutoverActive() bool { return s.active.Load() }

func TestPrepareEvidenceSafetyCapabilityReportsLocalCutoverOnFollowers(t *testing.T) {
	for _, configured := range []bool{false, true} {
		t.Run(fmt.Sprintf("configured-%t", configured), func(t *testing.T) {
			leader := &prepareSafetyRPCLeader{}
			leader.follower.Store(true)
			state := &prepareSafetyCutoverState{}
			var options []InternalOption
			if configured {
				options = append(options, WithInternalTSOCutoverState(state), WithInternalTimestampAllocator(&phaseDForwardAllocator{}))
			}
			client := prepareSafetyInternalClient(t, NewInternalWithEngine(nil, leader, nil, nil, options...))
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			ctx = metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer prepare-safety-token")
			for _, applied := range []bool{false, true} {
				state.active.Store(applied)
				response, err := client.GetPrepareEvidenceState(ctx, &pb.PrepareEvidenceStateRequest{CapabilityOnly: true})
				require.NoError(t, err)
				require.True(t, response.GetSupported())
				require.Equal(t, configured, response.GetDedicatedTsoConfigured())
				require.Equal(t, configured && applied, response.GetCutoverApplied())
				require.Zero(t, leader.reads.Load(), "capability must expose this follower's applied state without a leader round trip")
			}
		})
	}
}

type prepareSafetyResolutionAllocator struct {
	phaseDForwardAllocator
	bound       uint64
	validations atomic.Int32
}

func (a *prepareSafetyResolutionAllocator) Next(context.Context) (uint64, error) {
	return 0, kv.ErrTSOPhaseDActivationPending
}

func (a *prepareSafetyResolutionAllocator) PhaseDActive() bool { return false }

func (a *prepareSafetyResolutionAllocator) ValidateDurableTimestamp(context.Context, uint64) error {
	return kv.ErrTSOPhaseDInactive
}

func (a *prepareSafetyResolutionAllocator) ValidateLegacyResolutionTimestamps(_ context.Context, start, commit uint64) error {
	a.validations.Add(1)
	if start == 0 || commit <= start || (a.bound != 0 && commit > a.bound) {
		return kv.ErrTSOTimestampInvalid
	}
	return nil
}

func prepareSafetyForward(phase pb.Phase, key string, commit uint64) *pb.ForwardRequest {
	return &pb.ForwardRequest{IsTxn: true, Requests: []*pb.Request{{Ts: 50, IsTxn: true, Phase: phase, Mutations: []*pb.Mutation{
		{Op: pb.Op_PUT, Key: []byte(kv.TxnMetaPrefix), Value: kv.EncodeTxnMeta(kv.TxnMeta{PrimaryKey: []byte("primary"), CommitTS: commit})},
		{Op: pb.Op_PUT, Key: []byte(key)},
	}}}}
}

func TestPrepareEvidenceSafetyExistingResolutionBeforeAndDuringPending(t *testing.T) {
	for _, bound := range []uint64{0, 100} {
		for _, phase := range []pb.Phase{pb.Phase_COMMIT, pb.Phase_ABORT} {
			t.Run(phase.String()+"-"+map[bool]string{true: "before-pending", false: "pending"}[bound == 0], func(t *testing.T) {
				allocator := &prepareSafetyResolutionAllocator{bound: bound}
				txn := &forwardObserverTxn{}
				i := NewInternalWithEngine(txn, forwardObserverLeader{}, nil, nil,
					WithInternalTimestampAllocator(allocator),
					WithInternalPrimaryCommitEvidence(primaryCommitEvidenceFunc(func(context.Context, []byte, uint64) (kv.PrimaryCommitEvidence, error) {
						return kv.PrimaryCommitEvidence{Readable: true, PrimaryPrepared: true}, nil
					})))
				response, err := i.Forward(context.Background(), prepareSafetyForward(phase, "primary", 60))
				require.NoError(t, err)
				require.True(t, response.GetSuccess())
				require.Equal(t, uint64(60), response.GetCommitTs())
				require.NotZero(t, allocator.validations.Load(), "legacy resolution needs the authoritative dedicated validator")
			})
		}
	}
}

func TestPrepareEvidenceSafetyFirstPrimaryUsesActualDurableTimestamp(t *testing.T) {
	for _, commit := range []uint64{60, 61} {
		t.Run(map[bool]string{true: "matching", false: "forged"}[commit == 60], func(t *testing.T) {
			st := store.NewMVCCStore()
			fsm := kv.NewKvFSMWithHLC(st, kv.NewHLC())
			require.Nil(t, fsm.Apply(kv.EncodePrepareEvidenceFence(0)))
			request := prepareSafetyForward(pb.Phase_PREPARE, "primary", 0).Requests[0]
			request.Mutations[0].Value = kv.EncodeTxnMeta(kv.TxnMeta{PrimaryKey: []byte("primary"), PrepareCommitTS: 60})
			request.Mutations[1].Value = []byte("prepared value")
			data, err := proto.Marshal(request)
			require.NoError(t, err)
			require.Nil(t, fsm.Apply(data))
			routes := distribution.NewEngine()
			routes.UpdateRoute(nil, nil, 1)
			shards := kv.NewShardStore(routes, map[uint64]*kv.ShardGroup{1: {Store: st}})
			t.Cleanup(func() { require.NoError(t, shards.Close()) })
			txn := &forwardObserverTxn{}
			i := NewInternalWithEngine(txn, forwardObserverLeader{}, nil, nil,
				WithInternalTimestampAllocator(&phaseDForwardAllocator{floor: 100}), WithInternalPrimaryCommitEvidence(shards))
			response, err := i.Forward(context.Background(), prepareSafetyForward(pb.Phase_COMMIT, "primary", commit))
			if commit != 60 {
				require.ErrorIs(t, err, kv.ErrPrePhaseDCommitUnproven)
				require.Empty(t, txn.reqs)
				return
			}
			require.NoError(t, err)
			require.True(t, response.GetSuccess())
			require.Equal(t, uint64(60), response.GetCommitTs())
		})
	}
}

func TestPrepareEvidenceSafetyPendingRejectsNewTimestampedWork(t *testing.T) {
	allocator := &prepareSafetyResolutionAllocator{bound: 100}
	txn := &forwardObserverTxn{}
	i := NewInternalWithEngine(txn, forwardObserverLeader{}, nil, nil, WithInternalTimestampAllocator(allocator))
	for _, phase := range []pb.Phase{pb.Phase_NONE, pb.Phase_PREPARE} {
		_, err := i.Forward(context.Background(), prepareSafetyForward(phase, "fresh", 60))
		require.True(t, errors.Is(err, kv.ErrTSOPhaseDInactive) || errors.Is(err, kv.ErrTSOPhaseDActivationPending), "%s: %v", phase, err)
	}
	require.Empty(t, txn.reqs)
}

type prepareSafetyReservationAllocator struct {
	phaseDForwardAllocator
	minimum uint64
	fail    error
}

func (*prepareSafetyReservationAllocator) PhaseDActive() bool   { return false }
func (*prepareSafetyReservationAllocator) PhaseDRequired() bool { return false }

func (a *prepareSafetyReservationAllocator) NextAfter(_ context.Context, minimum uint64) (uint64, error) {
	a.minimum = minimum
	if a.fail != nil {
		return 0, a.fail
	}
	return minimum + 1, nil
}

func TestPrepareEvidenceSafetyV3IngressReservesProofWithoutRetimestamp(t *testing.T) {
	reservationFailure := errors.New("durable reservation failed")
	for _, failure := range []error{nil, reservationFailure} {
		allocator := &prepareSafetyReservationAllocator{fail: failure}
		txn := &forwardObserverTxn{}
		i := NewInternalWithEngine(txn, forwardObserverLeader{}, nil, nil, WithInternalTimestampAllocator(allocator))
		request := prepareSafetyForward(pb.Phase_PREPARE, "primary", 0)
		meta := kv.TxnMeta{PrimaryKey: []byte("primary"), PrepareCommitTS: 60}
		request.Requests[0].Mutations[0].Value = kv.EncodeTxnMeta(meta)
		request.Requests[0].Mutations[1].Value = []byte("value already stamped at 60")
		before := bytes.Clone(request.Requests[0].Mutations[1].Value)
		_, err := i.Forward(context.Background(), request)
		require.Equal(t, uint64(60), allocator.minimum, "the chosen proof must be included in the durable reservation range")
		if failure != nil {
			require.ErrorIs(t, err, reservationFailure)
			require.Empty(t, txn.reqs, "reservation failure must precede data proposal")
			continue
		}
		require.NoError(t, err)
		require.Len(t, txn.reqs, 1)
		after, err := kv.DecodeTxnMeta(txn.reqs[0].Mutations[0].Value)
		require.NoError(t, err)
		require.Equal(t, uint64(60), after.PrepareCommitTS, "reserving past the proof must not substitute the newly allocated value")
		require.Equal(t, before, txn.reqs[0].Mutations[1].Value)
	}
}
