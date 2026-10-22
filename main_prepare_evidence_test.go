package main

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/bootjp/elastickv/adapter"
	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/bootjp/elastickv/kv"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

func TestPrepareEvidenceCapabilityRequiresCoordinatorRuntime(t *testing.T) {
	for _, routed := range []bool{false, true} {
		t.Run(map[bool]string{false: "legacy-warmup", true: "wired-cutover"}[routed], func(t *testing.T) {
			setTSOModeFlags(t, routed, false)
			clock := kv.NewHLC()
			clock.SetPhysicalCeiling(time.Now().Add(time.Minute).UnixMilli())
			state := kv.NewTSOStateMachine(clock)
			engine := &mainTSOEngine{state: raftengine.StateLeader, tsoState: state}
			groups := map[uint64]*kv.ShardGroup{0: {Engine: engine, TSOState: state}}
			coord := newMainTSOCoordinator(clock, groups)
			wiring, err := configureCoordinatorTSO(coord, groups, mainTSOFloorProvider{})
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, wiring.Close()) })
			options := append(internalTimestampOptions(newStartupGatedCoordinator(coord, nil)), dedicatedTimestampCutoverOptions(groups)...)
			options = append(options, adapter.WithInternalPrepareEvidenceToken("runtime-token"))
			server := adapter.NewInternalWithEngine(nil, nil, nil, nil, options...)
			ctx := metadata.NewIncomingContext(t.Context(), metadata.Pairs("authorization", "Bearer runtime-token"))
			for _, cutover := range []bool{false, true} {
				if cutover {
					allocator, ok := wiring.serverAllocator.(kv.TSOReservationAllocator)
					require.True(t, ok)
					_, err = allocator.ReserveBatchAfter(t.Context(), 1, 0, true, false)
					require.NoError(t, err)
				}
				response, err := server.GetPrepareEvidenceState(ctx, &pb.PrepareEvidenceStateRequest{CapabilityOnly: true})
				require.NoError(t, err)
				require.Equal(t, routed, response.GetDedicatedTsoConfigured(), "a durable marker must not invent missing coordinator wiring")
				require.Equal(t, cutover, response.GetCutoverApplied())
			}
		})
	}
}

func TestDedicatedTimestampCutoverOptionsRejectsNilState(t *testing.T) {
	require.Empty(t, dedicatedTimestampCutoverOptions(map[uint64]*kv.ShardGroup{0: {}}))
}

type prepareSafetyNextHook func(context.Context, string, string) error

func (f prepareSafetyNextHook) PreAddMember(ctx context.Context, id, address string) error {
	return f(ctx, id, address)
}

func TestPrepareEvidenceSafetyMainMembershipProbePrecedesExistingHook(t *testing.T) {
	probeFailure, nextFailure := errors.New("unsupported member"), errors.New("existing hook rejected")
	for _, probeErr := range []error{nil, probeFailure} {
		var order []string
		gate := &prepareEvidenceMembershipGate{groupID: 7,
			probe: func(_ context.Context, group uint64, address string) error {
				require.Equal(t, uint64(7), group)
				require.Equal(t, "new:50051", address)
				order = append(order, "prepare-protocol")
				return probeErr
			},
			next: prepareSafetyNextHook(func(_ context.Context, id, address string) error {
				require.Equal(t, "new-node", id)
				require.Equal(t, "new:50051", address)
				order = append(order, "existing-hook")
				return nextFailure
			}),
		}
		err := gate.PreAddMember(context.Background(), "new-node", "new:50051")
		if probeErr != nil {
			require.ErrorIs(t, err, probeFailure)
			require.Equal(t, []string{"prepare-protocol"}, order)
		} else {
			require.ErrorIs(t, err, nextFailure)
			require.Equal(t, []string{"prepare-protocol", "existing-hook"}, order)
		}
	}
}

func TestPrepareEvidenceSafetyMainAdmissionStillRejectsOldMemberAfterMarker(t *testing.T) {
	for _, kind := range []string{"old-unsupported", "hlc-only", "fresh-dedicated-learner"} {
		t.Run(kind, func(t *testing.T) {
			lis, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
			require.NoError(t, err)
			auth, _ := adapter.AdminTokenAuth("membership-token")
			srv := grpc.NewServer(grpc.UnaryInterceptor(auth))
			if kind == "old-unsupported" {
				pb.RegisterInternalServer(srv, &pb.UnimplementedInternalServer{})
			} else {
				options := []adapter.InternalOption{adapter.WithInternalPrepareEvidenceToken("membership-token")}
				if kind == "fresh-dedicated-learner" {
					// Configured but not joined to group 0 yet: adoption is false.
					options = append(options, adapter.WithInternalTSOCutoverState(kv.NewTSOStateMachine(kv.NewHLC())),
						adapter.WithInternalTimestampAllocator(&mainTimestampAllocator{}))
				}
				pb.RegisterInternalServer(srv, adapter.NewInternalWithEngine(nil, nil, nil, nil, options...))
			}
			go func() { _ = srv.Serve(lis) }()
			t.Cleanup(func() { srv.Stop(); _ = lis.Close() })
			st := store.NewMVCCStore()
			fsm := kv.NewKvFSMWithHLC(st, kv.NewHLC())
			require.Nil(t, fsm.Apply(kv.EncodePrepareEvidenceFence(0)))
			group := &kv.ShardGroup{Store: st, Engine: &mainTSOEngine{state: raftengine.StateLeader}}
			group.SetLeaderReadToken("membership-token")
			_, _, _, groups := newActiveMainTSOCutover(t)
			groups[7] = group
			shards := kv.NewShardStore(nil, groups)
			t.Cleanup(func() { require.NoError(t, shards.Close()) })
			called := false
			next := prepareSafetyNextHook(func(context.Context, string, string) error { called = true; return nil })
			gate := newPrepareEvidenceMembershipGate(shards, 7, next)
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			err = gate.PreAddMember(ctx, "later-member", lis.Addr().String())
			if kind == "fresh-dedicated-learner" {
				require.NoError(t, err)
				require.True(t, called)
			} else {
				require.ErrorIs(t, err, kv.ErrTSOProtocolUnsupported)
				require.False(t, called, "admission after capability scan/marker cannot bypass the protocol hook")
			}
		})
	}
}
