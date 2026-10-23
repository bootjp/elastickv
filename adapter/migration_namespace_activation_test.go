package adapter

import (
	"context"
	"testing"

	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/bootjp/elastickv/internal/raftengine/etcd"
	"github.com/bootjp/elastickv/kv"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

type namespaceRuntimeClient struct {
	splitMigrationClientStub
	internal      *Internal
	version       uint32
	configuration uint64
	proposals     int
	probes        int
}

func (c *namespaceRuntimeClient) ProbeMigrationState(ctx context.Context, req *pb.ProbeMigrationStateRequest, _ ...grpc.CallOption) (*pb.ProbeMigrationStateResponse, error) {
	c.probes++
	response, err := c.internal.probeMigrationNamespace(ctx, req)
	if err != nil {
		return nil, err
	}
	response.NamespaceAdmissionVersion = c.version
	if c.configuration != 0 {
		response.AppliedConfigurationIndex = c.configuration
	}
	return response, nil
}

func (c *namespaceRuntimeClient) CleanupMigration(ctx context.Context, req *pb.CleanupMigrationRequest, _ ...grpc.CallOption) (*pb.CleanupMigrationResponse, error) {
	c.proposals++
	return c.internal.applyMigrationNamespaceAdmission(ctx, req)
}

func namespaceRuntimeFixture(t *testing.T) ([]SplitMigrationVoter, []*namespaceRuntimeClient) {
	t.Helper()
	ids := []uint64{etcd.DeriveNodeID("n1"), etcd.DeriveNodeID("n2"), etcd.DeriveNodeID("learner")}
	var clients []*namespaceRuntimeClient
	var machines []kv.FSM
	for range ids {
		st := store.NewMVCCStore()
		fsm := kv.NewKvFSMWithHLC(st, nil)
		aware, ok := fsm.(raftengine.NamespaceConfigurationAware)
		require.True(t, ok)
		require.NoError(t, aware.RecordAppliedNamespaceConfiguration(9, ids))
		configuration, ok := fsm.(raftengine.AppliedConfigurationAware)
		require.True(t, ok)
		i := NewInternalWithEngine(nil, ownershipRuntimeLeader{configuration: configuration}, nil, nil,
			WithInternalStore(st), WithMigrationNamespaceAdmissionReady(func() bool { return true }))
		clients = append(clients, &namespaceRuntimeClient{internal: i, version: kv.MigrationNamespaceVersion})
		machines = append(machines, fsm)
	}
	proposer := &ownershipRuntimeProposer{fsm: machines[0], followers: machines[1:], index: 10}
	var members []SplitMigrationVoter
	for index, name := range []string{"n1", "n2", "learner"} {
		clients[index].internal.migrationProposer = proposer
		members = append(members, SplitMigrationVoter{ID: name, Address: name + ":7001", Client: clients[index]})
	}
	return members, clients
}

func TestNamespaceAdmissionControllerBootstrapsBeforeOperationalGuards(t *testing.T) {
	members, clients := namespaceRuntimeFixture(t)
	for _, client := range clients {
		probe, err := client.ProbeMigrationState(t.Context(), &pb.ProbeMigrationStateRequest{})
		require.NoError(t, err)
		require.False(t, probe.GetReady())
		require.Equal(t, kv.MigrationNamespaceVersion, probe.GetNamespaceAdmissionVersion())
	}
	require.NoError(t, EnsureMigrationNamespaceAdmission(t.Context(), clients[0], members))
	require.Equal(t, 1, clients[0].proposals)
	for _, client := range clients {
		require.NoError(t, kv.RequireMigrationNamespaceAdmission(t.Context(), client.internal.store))
	}
}

func TestNamespaceAdmissionControllerActiveGroupDoesNotPropose(t *testing.T) {
	members, clients := namespaceRuntimeFixture(t)
	require.NoError(t, EnsureMigrationNamespaceAdmission(t.Context(), clients[0], members))
	for range 3 {
		before := make([]int, len(clients))
		for index, client := range clients {
			before[index] = client.probes
		}
		require.NoError(t, EnsureMigrationNamespaceAdmission(t.Context(), clients[0], members))
		require.Equal(t, 1, clients[0].proposals)
		for index, client := range clients {
			// Three inventory reads, negotiation, and the final durable ACK.
			require.Equal(t, before[index]+5, client.probes)
		}
	}
}

type namespaceProbeFunc func(context.Context, *pb.ProbeMigrationStateRequest) (*pb.ProbeMigrationStateResponse, error)

func (f namespaceProbeFunc) ProbeMigrationState(ctx context.Context, request *pb.ProbeMigrationStateRequest, _ ...grpc.CallOption) (*pb.ProbeMigrationStateResponse, error) {
	return f(ctx, request)
}

func namespaceRuntimeSnapshot(members []SplitMigrationVoter) NamespaceAdmissionSnapshot {
	snapshot := NamespaceAdmissionSnapshot{ConfigurationIndex: 9}
	for _, member := range members {
		snapshot.Members = append(snapshot.Members, NamespaceAdmissionPeer{
			MemberID: etcd.DeriveNodeID(member.ID), Address: member.Address, Client: member.Client,
		})
	}
	return snapshot
}

func TestNamespaceAdmissionNegotiationRequiresEveryMemberReady(t *testing.T) {
	members, clients := namespaceRuntimeFixture(t)
	require.NoError(t, EnsureMigrationNamespaceAdmission(t.Context(), clients[0], members))
	state := namespaceRuntimeSnapshot(members)
	state.Members[2].Client = namespaceProbeFunc(func(ctx context.Context, request *pb.ProbeMigrationStateRequest) (*pb.ProbeMigrationStateResponse, error) {
		response, err := clients[2].ProbeMigrationState(ctx, request)
		if err == nil {
			response.Ready = false
		}
		return response, err
	})
	_, alreadyActive, err := namespaceNegotiation(t.Context(), state, nil)
	require.NoError(t, err)
	require.False(t, alreadyActive)
	require.NoError(t, kv.RequireMigrationNamespaceAdmission(t.Context(), clients[0].internal.store))
}

func TestNamespaceAdmissionActiveGroupJoiningStillRefreshes(t *testing.T) {
	members, clients := namespaceRuntimeFixture(t)
	require.NoError(t, EnsureMigrationNamespaceAdmission(t.Context(), clients[0], members))
	state := namespaceRuntimeSnapshot(members)
	snapshot := func(context.Context) (NamespaceAdmissionSnapshot, error) { return state, nil }
	activate := func(ctx context.Context, command kv.NamespaceAdmissionCommand) error {
		request, err := kv.NewNamespaceAdmissionRequest(1, command)
		if err != nil {
			return err
		}
		_, err = clients[0].CleanupMigration(ctx, request)
		return err
	}
	candidateStore := store.NewMVCCStore()
	candidateFSM := kv.NewKvFSMWithHLC(candidateStore, nil)
	candidate := NewInternalWithEngine(nil, nil, nil, nil, WithInternalStore(candidateStore),
		WithMigrationNamespaceAdmissionReady(func() bool {
			return kv.MigrationNamespaceInstalledVersion(candidateStore, candidateFSM) == kv.MigrationNamespaceVersion
		}))
	candidate.migrationProposer = clients[0].internal.migrationProposer
	joiningID := etcd.DeriveNodeID("joining")
	joining := []NamespaceAdmissionPeer{{MemberID: joiningID, Address: "joining:7001",
		Client: namespaceProbeFunc(candidate.probeMigrationNamespace)}}
	for round := range 2 {
		require.NoError(t, EnsureMigrationNamespaceAdmissionWithSnapshot(t.Context(), snapshot, activate, joining))
		require.Equal(t, 2+round, clients[0].proposals)
		for _, client := range clients {
			require.NoError(t, kv.CheckMigrationNamespacePrejoin(t.Context(), client.internal.store, 9, joiningID, kv.MigrationNamespaceVersion))
		}
	}
}

func TestNamespaceAdmissionActiveGroupStillRequiresFinalACK(t *testing.T) {
	for _, failure := range []string{"ready", "version", "configuration"} {
		t.Run(failure, func(t *testing.T) {
			members, clients := namespaceRuntimeFixture(t)
			require.NoError(t, EnsureMigrationNamespaceAdmission(t.Context(), clients[0], members))
			state := namespaceRuntimeSnapshot(members)
			probes := 0
			state.Members[2].Client = namespaceProbeFunc(func(ctx context.Context, request *pb.ProbeMigrationStateRequest) (*pb.ProbeMigrationStateResponse, error) {
				response, err := clients[2].ProbeMigrationState(ctx, request)
				probes++
				if probes == 2 {
					switch failure {
					case "ready":
						response.Ready = false
					case "version":
						response.NamespaceAdmissionVersion = 0
					case "configuration":
						response.AppliedConfigurationIndex = 8
					}
				}
				return response, err
			})
			snapshot := func(context.Context) (NamespaceAdmissionSnapshot, error) { return state, nil }
			activate := func(context.Context, kv.NamespaceAdmissionCommand) error {
				t.Fatal("active group must not propose")
				return nil
			}
			require.ErrorIs(t, EnsureMigrationNamespaceAdmissionWithSnapshot(t.Context(), snapshot, activate, nil), kv.ErrMigrationNamespaceProtocol)
			require.Equal(t, 2, probes)
			require.Equal(t, 1, clients[0].proposals)
		})
	}
}

func TestNamespaceAdmissionActiveGroupRevalidatesSnapshots(t *testing.T) {
	for _, changeAt := range []int{2, 3} {
		members, clients := namespaceRuntimeFixture(t)
		require.NoError(t, EnsureMigrationNamespaceAdmission(t.Context(), clients[0], members))
		state := namespaceRuntimeSnapshot(members)
		calls := 0
		snapshot := func(context.Context) (NamespaceAdmissionSnapshot, error) {
			calls++
			if calls == changeAt {
				state.ConfigurationIndex++
			}
			return state, nil
		}
		activate := func(context.Context, kv.NamespaceAdmissionCommand) error {
			t.Fatal("active group must not propose")
			return nil
		}
		require.ErrorIs(t, EnsureMigrationNamespaceAdmissionWithSnapshot(t.Context(), snapshot, activate, nil), kv.ErrMigrationNamespaceProtocol)
		require.Equal(t, changeAt, calls)
		require.Equal(t, 1, clients[0].proposals)
	}
}

func TestNamespaceAdmissionControllerDoesNotProposeForOldLearnerOrDifferentEpoch(t *testing.T) {
	for _, old := range []bool{false, true} {
		members, clients := namespaceRuntimeFixture(t)
		if old {
			clients[2].version = 0
		} else {
			clients[2].configuration = 8
		}
		require.Error(t, EnsureMigrationNamespaceAdmission(t.Context(), clients[0], members))
		require.Zero(t, clients[0].proposals)
		for _, client := range clients {
			require.ErrorIs(t, kv.RequireMigrationNamespaceAdmission(t.Context(), client.internal.store), kv.ErrMigrationNamespaceProtocol)
		}
	}
}

func TestNamespaceAdmissionControllerRejectsPartialRosterAtFSM(t *testing.T) {
	members, clients := namespaceRuntimeFixture(t)
	require.Error(t, EnsureMigrationNamespaceAdmission(t.Context(), clients[0], members[:2]))
	for _, client := range clients {
		require.ErrorIs(t, kv.RequireMigrationNamespaceAdmission(t.Context(), client.internal.store), kv.ErrMigrationNamespaceProtocol)
	}
}

func TestNamespaceAdmissionSnapshotChangePreventsControl(t *testing.T) {
	members, clients := namespaceRuntimeFixture(t)
	peer := NamespaceAdmissionPeer{MemberID: etcd.DeriveNodeID("n1"), Address: members[0].Address, Client: clients[0]}
	calls, controls := 0, 0
	snapshot := func(context.Context) (NamespaceAdmissionSnapshot, error) {
		calls++
		return NamespaceAdmissionSnapshot{ConfigurationIndex: uint64(8 + calls), Members: []NamespaceAdmissionPeer{peer}}, nil
	}
	activate := func(context.Context, kv.NamespaceAdmissionCommand) error { controls++; return nil }
	require.Error(t, EnsureMigrationNamespaceAdmissionWithSnapshot(t.Context(), snapshot, activate, nil))
	require.Zero(t, controls)
}
