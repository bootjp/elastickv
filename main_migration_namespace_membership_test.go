package main

import (
	"context"
	"slices"
	"testing"

	"github.com/bootjp/elastickv/internal/encryption"
	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/bootjp/elastickv/internal/raftengine/etcd"
	"github.com/bootjp/elastickv/kv"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
)

type namespaceGateEngine struct {
	raftengine.Engine
	store         store.MVCCStore
	fsm           raftengine.StateMachine
	configuration raftengine.Configuration
	proposals     int
	index         uint64
}

func (e *namespaceGateEngine) ProposeAdmin(_ context.Context, data []byte) (*raftengine.ProposalResult, error) {
	e.proposals++
	e.index++
	aware, ok := e.fsm.(raftengine.ApplyIndexAware)
	if !ok {
		return nil, kv.ErrMigrationNamespaceProtocol
	}
	aware.SetApplyIndex(e.index)
	return &raftengine.ProposalResult{CommitIndex: e.index, Response: e.fsm.Apply(data)}, nil
}
func (e *namespaceGateEngine) Configuration(context.Context) (raftengine.Configuration, error) {
	return e.configuration, nil
}
func (e *namespaceGateEngine) LinearizableRead(context.Context) (uint64, error) { return e.index, nil }
func (e *namespaceGateEngine) Status() raftengine.Status {
	return raftengine.Status{ConfigurationIndex: 7}
}
func (e *namespaceGateEngine) AppliedConfigurationIndex() (uint64, bool) {
	index, err := kv.MigrationConfigurationIndex(context.Background(), e.store)
	return index, err == nil && index != 0
}

type namespaceGateNext struct {
	check            func()
	adds, promotions int
}

func (n *namespaceGateNext) PreAddMember(context.Context, string, string) error {
	n.check()
	n.adds++
	return nil
}
func (n *namespaceGateNext) PrePromoteMember(context.Context, string, string) error {
	n.check()
	n.promotions++
	return nil
}

func newNamespaceGateFixture(t *testing.T, active bool) (*migrationNamespaceMembershipGate, *namespaceGateEngine, *namespaceGateNext, map[string]uint32, map[string]int) {
	t.Helper()
	st := store.NewMVCCStore()
	fsm := kv.NewKvFSMWithHLC(st, nil)
	ids := []uint64{etcd.DeriveNodeID("n1"), etcd.DeriveNodeID("learner")}
	aware, ok := fsm.(raftengine.NamespaceConfigurationAware)
	require.True(t, ok)
	require.NoError(t, aware.RecordAppliedNamespaceConfiguration(7, ids))
	engine := &namespaceGateEngine{store: st, fsm: fsm, index: 7, configuration: raftengine.Configuration{Servers: []raftengine.Server{{ID: "n1", Address: "n1:7001", Suffrage: "voter"}, {ID: "learner", Address: "learner:7001", Suffrage: "learner"}}}}
	if active {
		slices.Sort(ids)
		command := kv.NamespaceAdmissionCommand{Version: kv.MigrationNamespaceVersion, ConfigurationIndex: 7, ApplyMembers: ids}
		for _, id := range ids {
			command.Members = append(command.Members, kv.NamespaceMemberCapability{MemberID: id, Version: kv.MigrationNamespaceVersion, ConfigurationIndex: 7})
		}
		require.NoError(t, kv.ActivateMigrationNamespace(t.Context(), st, engine, command))
	}
	next := &namespaceGateNext{check: func() {}}
	gate := newMigrationNamespaceMembershipGate(st, engine, engine, "secret", next)
	versions := map[string]uint32{"n1:7001": 1, "learner:7001": 1, "candidate:7001": 1}
	probes := map[string]int{}
	gate.probe = func(ctx context.Context, address string, _ *pb.ProbeMigrationStateRequest) (*pb.ProbeMigrationStateResponse, error) {
		probes[address]++
		version, err := kv.MigrationNamespaceAdmissionVersion(ctx, st)
		if err != nil {
			return nil, err
		}
		index := uint64(7)
		if address == "candidate:7001" {
			index = 0
		}
		return &pb.ProbeMigrationStateResponse{NamespaceAdmissionVersion: versions[address], AppliedConfigurationIndex: index, Ready: version == kv.MigrationNamespaceVersion}, nil
	}
	return gate, engine, next, versions, probes
}

func TestMigrationNamespaceMembershipRegistersBeforeAddAndProbesEveryMember(t *testing.T) {
	gate, engine, next, _, probes := newNamespaceGateFixture(t, true)
	next.check = func() {
		value, err := engine.store.GetAt(t.Context(), store.MigrationConfigurationIndexKey(), ^uint64(0))
		require.NoError(t, err)
		state, err := store.DecodeMigrationNamespaceConfiguration(value)
		require.NoError(t, err)
		require.Contains(t, state.Joining, etcd.DeriveNodeID("candidate"))
	}
	require.NoError(t, gate.PreAddMember(t.Context(), "candidate", "candidate:7001"))
	require.Equal(t, 1, next.adds)
	for _, address := range []string{"n1:7001", "learner:7001", "candidate:7001"} {
		require.Positive(t, probes[address], address)
	}
}

func TestMigrationNamespaceMembershipRejectsOldCandidateAndOldLearnerBeforeProposal(t *testing.T) {
	for _, old := range []string{"candidate:7001", "learner:7001"} {
		gate, engine, next, versions, _ := newNamespaceGateFixture(t, true)
		versions[old] = 0
		before := engine.proposals
		require.Error(t, gate.PreAddMember(t.Context(), "candidate", "candidate:7001"))
		require.Equal(t, before, engine.proposals)
		require.Zero(t, next.adds)
	}
}

func TestMigrationNamespaceMembershipPreservesInactiveAndRechecksPromotion(t *testing.T) {
	gate, _, next, _, probes := newNamespaceGateFixture(t, false)
	require.NoError(t, gate.PreAddMember(t.Context(), "candidate", "candidate:7001"))
	require.Empty(t, probes)
	require.Equal(t, 1, next.adds)
	gate, _, next, versions, _ := newNamespaceGateFixture(t, true)
	require.NoError(t, gate.PrePromoteMember(t.Context(), "learner", "learner:7001"))
	require.Equal(t, 1, next.promotions)
	versions["learner:7001"] = 0
	require.Error(t, gate.PrePromoteMember(t.Context(), "learner", "learner:7001"))
	require.Equal(t, 1, next.promotions)
}

type namespaceHiddenFSM struct{ raftengine.StateMachine }

func TestMigrationNamespaceProductionFSMInstalledWithEncryptionOptions(t *testing.T) {
	for _, encrypted := range []bool{false, true} {
		wiring := encryptionWriteWiring{}
		if encrypted {
			cipher, err := encryption.NewCipher(encryption.NewKeystore())
			require.NoError(t, err)
			wiring = encryptionWriteWiring{cache: encryption.NewStateCache(), cipher: cipher, nonceFactory: encryption.NewDeterministicNonceFactory(1, 1)}
		}
		st, err := store.NewPebbleStore(t.TempDir(), wiring.pebbleOptions()...)
		require.NoError(t, err)
		registry, err := store.WriterRegistryFor(st)
		require.NoError(t, err)
		applier, err := encryption.NewApplier(registry)
		require.NoError(t, err)
		fsm := kv.NewKvFSMWithHLCAndTracker(st, kv.NewHLC(), nil, fsmOptionsForGroup(applier, nil, 1, wiring)...)
		require.Equal(t, kv.MigrationNamespaceVersion, kv.MigrationNamespaceInstalledVersion(st, fsm))
		require.Zero(t, kv.MigrationNamespaceInstalledVersion(st, namespaceHiddenFSM{fsm}))
		require.Zero(t, kv.MigrationNamespaceInstalledVersion(st, kv.NewTSOStateMachine(kv.NewHLC())))
		require.NoError(t, st.Close())
	}
}
