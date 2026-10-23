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
}

func (c *namespaceRuntimeClient) ProbeMigrationState(ctx context.Context, req *pb.ProbeMigrationStateRequest, _ ...grpc.CallOption) (*pb.ProbeMigrationStateResponse, error) {
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
