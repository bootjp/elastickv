package adapter

import (
	"testing"

	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/bootjp/elastickv/kv"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func newTransferGuardInternal(t *testing.T, index uint64, ready func() bool) (*Internal, raftengine.AppliedConfigurationAware) {
	t.Helper()
	st := store.NewMVCCStore()
	t.Cleanup(func() { require.NoError(t, st.Close()) })
	fsm := kv.NewKvFSMWithHLC(st, nil, kv.WithRouteHistory(nil, 1))
	configuration, ok := fsm.(raftengine.AppliedConfigurationAware)
	require.True(t, ok)
	if index != 0 {
		require.NoError(t, configuration.RecordAppliedConfiguration(index))
	}
	i := NewInternalWithEngine(nil, ownershipRuntimeLeader{configuration: configuration}, nil, nil,
		WithInternalStore(st), WithInternalMigrationProposer(&ownershipRuntimeProposer{fsm: fsm, index: 20}),
		WithInternalMigrationOwnershipReady(func() bool { return true }), WithInternalMigrationTransferSizeGuardReady(ready))
	return i, configuration
}

func TestMigrationSizeGuardCapabilitySeparatesSupportFromEpoch(t *testing.T) {
	for _, index := range []uint64{0, 9} {
		for _, ready := range []bool{false, true} {
			i, _ := newTransferGuardInternal(t, index, func() bool { return ready })
			require.Equal(t, ready, i.MigrationTransferSizeGuardSupported())
			require.Equal(t, ready && index != 0, i.MigrationTransferSizeGuardReady())
			resp, err := i.ProbeMigrationState(t.Context(), &pb.ProbeMigrationStateRequest{JobId: 11,
				Kind:                      pb.MigrationStateProbeKind_MIGRATION_STATE_PROBE_KIND_OWNERSHIP_CAPABILITY,
				RequireConfigurationIndex: true})
			require.NoError(t, err)
			if ready {
				require.Equal(t, uint32(1), resp.GetTransferSizeGuardVersion(), "fresh learner can advertise implemented support before joining")
			} else {
				require.Zero(t, resp.GetTransferSizeGuardVersion(), "ownership protocol 2 alone must not advertise the guard")
			}
			if index == 0 {
				require.False(t, resp.GetReady(), "a static capability is never an applied epoch ACK")
			}
		}
	}
}

func TestMigrationSizeGuardStaticSupportSurvivesClosedOperationalGate(t *testing.T) {
	i, _ := newTransferGuardInternal(t, 0, func() bool { return true })
	WithInternalMigrationOwnershipReady(func() bool { return false })(i)
	require.True(t, i.MigrationTransferSizeGuardSupported(), "installed hooks do not disappear when the cluster activation gate closes")
	require.False(t, i.MigrationTransferSizeGuardReady())
	resp, err := i.ProbeMigrationState(t.Context(), &pb.ProbeMigrationStateRequest{JobId: 11,
		Kind: pb.MigrationStateProbeKind_MIGRATION_STATE_PROBE_KIND_OWNERSHIP_CAPABILITY, RequireConfigurationIndex: true})
	require.NoError(t, err)
	require.Equal(t, uint32(1), resp.GetTransferSizeGuardVersion())
	require.False(t, resp.GetReady())
}

func TestMigrationSizeGuardActivationAndAppliedProof(t *testing.T) {
	ready := false
	i, configuration := newTransferGuardInternal(t, 9, func() bool { return ready })
	req := &pb.TargetStagedReadinessRequest{JobId: 11, MigrationJobId: 11, RouteStart: []byte("m"), RouteEnd: []byte("z"),
		Armed: true, TrackWrites: true, MinWriteTsExclusive: 1, RetentionPinTs: 1,
		TransferSizeGuardVersion: 1, RequireConfigurationIndex: true, ExpectedConfigurationIndex: 9}
	_, err := i.ApplyTargetStagedReadiness(t.Context(), req)
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
	ready = true
	probe := &pb.ProbeMigrationStateRequest{JobId: 11, MigrationJobId: 11, RouteStart: []byte("m"), RouteEnd: []byte("z"),
		Kind:        pb.MigrationStateProbeKind_MIGRATION_STATE_PROBE_KIND_CONTROL_APPLIED,
		TrackWrites: true, MinWriteTsExclusive: 1, RetentionPinTs: 1,
		TransferSizeGuardVersion: 1, RequireConfigurationIndex: true, ExpectedConfigurationIndex: 9}
	resp, err := i.ProbeMigrationState(t.Context(), probe)
	require.NoError(t, err)
	require.Equal(t, uint32(1), resp.GetTransferSizeGuardVersion())
	require.False(t, resp.GetReady(), "capability alone cannot certify an unapplied guard")
	_, err = i.ApplyTargetStagedReadiness(t.Context(), req)
	require.NoError(t, err)
	resp, err = i.ProbeMigrationState(t.Context(), probe)
	require.NoError(t, err)
	require.True(t, resp.GetReady())
	WithInternalMigrationOwnershipReady(func() bool { return false })(i)
	resp, err = i.ProbeMigrationState(t.Context(), probe)
	require.NoError(t, err)
	require.Equal(t, uint32(1), resp.GetTransferSizeGuardVersion())
	require.False(t, resp.GetReady(), "a closed operational gate must block active-guard ACKs, not static support")
	WithInternalMigrationOwnershipReady(func() bool { return true })(i)
	require.NoError(t, configuration.RecordAppliedConfiguration(12))
	resp, err = i.ProbeMigrationState(t.Context(), probe)
	require.NoError(t, err)
	require.False(t, resp.GetReady(), "old configuration ACK cannot survive a topology change")
	_, err = i.ApplyTargetStagedReadiness(t.Context(), req)
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
}
