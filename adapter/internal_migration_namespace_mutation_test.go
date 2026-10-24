package adapter

import (
	"bytes"
	"context"
	"testing"

	"github.com/bootjp/elastickv/distribution"
	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/bootjp/elastickv/internal/raftengine/etcd"
	"github.com/bootjp/elastickv/kv"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func activateInternalMigrationTestNamespace(t *testing.T, internal *Internal, proposer *applyingMigrationProposer) {
	t.Helper()
	inventory, ok := proposer.fsm.(raftengine.NamespaceConfigurationAware)
	require.True(t, ok)
	member := etcd.DeriveNodeID("voter")
	require.NoError(t, inventory.RecordAppliedNamespaceConfiguration(9, []uint64{member}))
	proposer.index = max(proposer.index, 9)
	require.NoError(t, kv.ActivateMigrationNamespace(t.Context(), internal.store, proposer, kv.NamespaceAdmissionCommand{
		Version: kv.MigrationNamespaceVersion, ConfigurationIndex: 9, ApplyMembers: []uint64{member},
		Members: []kv.NamespaceMemberCapability{{MemberID: member, Version: kv.MigrationNamespaceVersion, ConfigurationIndex: 9}},
	}))
	WithMigrationNamespaceAdmissionReady(func() bool {
		return kv.MigrationNamespaceInstalledVersion(internal.store, proposer.fsm) == kv.MigrationNamespaceVersion
	})(internal)
}

func namespaceMutationFixture(t *testing.T, active, configured bool) (*Internal, *ownershipRuntimeProposer) {
	t.Helper()
	st := store.NewMVCCStore()
	fsm := kv.NewKvFSMWithHLC(st, nil)
	inventory, ok := fsm.(raftengine.NamespaceConfigurationAware)
	require.True(t, ok)
	member := etcd.DeriveNodeID("voter")
	require.NoError(t, inventory.RecordAppliedNamespaceConfiguration(9, []uint64{member}))
	proposer := &ownershipRuntimeProposer{fsm: fsm, index: 9}
	if active {
		require.NoError(t, kv.ActivateMigrationNamespace(t.Context(), st, proposer, kv.NamespaceAdmissionCommand{
			Version: kv.MigrationNamespaceVersion, ConfigurationIndex: 9, ApplyMembers: []uint64{member},
			Members: []kv.NamespaceMemberCapability{{MemberID: member, Version: kv.MigrationNamespaceVersion, ConfigurationIndex: 9}},
		}))
	}
	i := NewInternalWithEngine(nil, mockInternalLeader{}, nil, nil, WithInternalStore(st), WithInternalMigrationProposer(proposer),
		WithInternalMigrationImportGate(func(context.Context) error { return nil }),
		WithInternalMigrationPromoteGate(func(context.Context) error { return nil }))
	if configured {
		WithMigrationNamespaceAdmissionReady(func() bool { return kv.MigrationNamespaceInstalledVersion(st, fsm) == kv.MigrationNamespaceVersion })(i)
	}
	require.NoError(t, st.PutAt(t.Context(), distribution.MigrationStagedDataKey(7, []byte("n")), []byte("staged"), 20, 0))
	return i, proposer
}

func namespaceMutationRPCs() map[string]func(context.Context, *Internal) error {
	return map[string]func(context.Context, *Internal) error{
		"import-unversioned": func(ctx context.Context, i *Internal) error {
			_, err := i.ImportRangeVersions(ctx, &pb.ImportRangeVersionsRequest{JobId: 7, BracketId: 1, BatchSeq: 1,
				Versions: []*pb.MVCCVersion{{Key: []byte("k"), Value: []byte("value"), CommitTs: 30}}})
			return err
		},
		"promote-unversioned": func(ctx context.Context, i *Internal) error {
			_, err := i.PromoteStagedVersions(ctx, &pb.PromoteStagedVersionsRequest{JobId: 7, MaxVersions: 10})
			return err
		},
		"readiness-unversioned": func(ctx context.Context, i *Internal) error {
			_, err := i.ApplyTargetStagedReadiness(ctx, &pb.TargetStagedReadinessRequest{JobId: 7, MigrationJobId: 7,
				RouteStart: []byte("a"), RouteEnd: []byte("z"), Armed: true, MinWriteTsExclusive: 100, ExpectedCutoverVersion: 1})
			return err
		},
		"readiness-unarmed-unversioned": func(ctx context.Context, i *Internal) error {
			_, err := i.ApplyTargetStagedReadiness(ctx, &pb.TargetStagedReadinessRequest{JobId: 7, MigrationJobId: 7,
				RouteStart: []byte("a"), RouteEnd: []byte("z"), ExpectedCutoverVersion: 1})
			return err
		},
	}
}

func TestInternalMigrationDataOpcodesRequireExplicitGateAfterActivation(t *testing.T) {
	for _, name := range []string{"import-unversioned", "promote-unversioned"} {
		t.Run(name, func(t *testing.T) {
			i, proposer := namespaceMutationFixture(t, true, true)
			i.migrationImportGate, i.migrationPromoteGate = nil, nil
			before, index := namespaceMutationSnapshot(t, i.store), proposer.index
			require.Error(t, namespaceMutationRPCs()[name](t.Context(), i))
			require.Equal(t, index, proposer.index)
			require.Equal(t, before, namespaceMutationSnapshot(t, i.store))
		})
	}
}

func TestInternalMigrationMutationsRetainDedicatedRPCAuthentication(t *testing.T) {
	methods := map[string]string{"import-unversioned": pb.Internal_ImportRangeVersions_FullMethodName,
		"promote-unversioned":   pb.Internal_PromoteStagedVersions_FullMethodName,
		"readiness-unversioned": pb.Internal_ApplyTargetStagedReadiness_FullMethodName}
	for name, method := range methods {
		t.Run(name, func(t *testing.T) {
			for _, authorized := range []bool{false, true} {
				i, proposer := namespaceMutationFixture(t, true, true)
				auth, _ := SplitMigrationTokenAuth(t.Name(), false, false)
				before := proposer.index
				_, err := auth(migrationNamespacePeerContext(t, authorized), nil, &grpc.UnaryServerInfo{FullMethod: method}, func(ctx context.Context, _ any) (any, error) {
					return nil, namespaceMutationRPCs()[name](ctx, i)
				})
				if authorized {
					require.NoError(t, err)
					require.Equal(t, before+1, proposer.index)
				} else {
					require.Equal(t, codes.Unauthenticated, status.Code(err))
					require.Equal(t, before, proposer.index)
				}
			}
		})
	}
}

func TestNamespaceRuntimeFixturesUseActualActivationAndMonotonicProposals(t *testing.T) {
	for _, split := range []bool{false, true} {
		st := store.NewMVCCStore()
		var client *ownershipRuntimeClient
		if split {
			client = newSplitRuntimeClient(t, distribution.NewEngine(), st, 1).ownershipRuntimeClient
		} else {
			client, _ = newOwnershipRuntimeClient(t, st, 1)
		}
		require.True(t, client.internal.MigrationNamespaceAdmissionSupported())
		require.NoError(t, kv.RequireMigrationNamespaceAdmission(t.Context(), st))
		value, err := st.GetAt(t.Context(), store.MigrationConfigurationIndexKey(), ^uint64(0))
		require.NoError(t, err)
		state, err := store.DecodeMigrationNamespaceConfiguration(value)
		require.NoError(t, err)
		require.Equal(t, uint64(9), state.Index)
		require.Equal(t, []uint64{etcd.DeriveNodeID("voter")}, state.Members)
		proposer, ok := client.internal.migrationProposer.(*ownershipRuntimeProposer)
		require.True(t, ok)
		before := proposer.index
		require.Greater(t, before, uint64(9))
		_, err = client.ApplyTargetStagedReadiness(t.Context(), &pb.TargetStagedReadinessRequest{JobId: 7, MigrationJobId: 7,
			RouteStart: []byte("a"), RouteEnd: []byte("z"), Armed: true, MinWriteTsExclusive: 100, ExpectedCutoverVersion: 1})
		require.NoError(t, err)
		require.Equal(t, before+1, proposer.index)
	}
}

func namespaceMutationSnapshot(t *testing.T, st store.MVCCStore) []byte {
	t.Helper()
	snapshot, err := st.Snapshot()
	require.NoError(t, err)
	defer snapshot.Close()
	var raw bytes.Buffer
	_, err = snapshot.WriteTo(&raw)
	require.NoError(t, err)
	return raw.Bytes()
}

func TestInternalMigrationMutationsRequireNamespaceBeforeProposal(t *testing.T) {
	for name, call := range namespaceMutationRPCs() {
		for _, configured := range []bool{false, true} {
			t.Run(name+map[bool]string{false: "/unconfigured", true: "/inactive"}[configured], func(t *testing.T) {
				i, proposer := namespaceMutationFixture(t, false, configured)
				before := namespaceMutationSnapshot(t, i.store)
				index := proposer.index
				err := call(t.Context(), i)
				require.Error(t, err)
				require.Equal(t, codes.FailedPrecondition, status.Code(err))
				require.Equal(t, index, proposer.index, "no unversioned control may be proposed")
				require.Equal(t, before, namespaceMutationSnapshot(t, i.store))
			})
		}
	}
}

func TestInternalMigrationMutationsRequireInstalledHooksAfterActivation(t *testing.T) {
	for name, call := range namespaceMutationRPCs() {
		t.Run(name, func(t *testing.T) {
			i, proposer := namespaceMutationFixture(t, true, false)
			before, index := namespaceMutationSnapshot(t, i.store), proposer.index
			require.Error(t, call(t.Context(), i))
			require.Equal(t, index, proposer.index)
			require.Equal(t, before, namespaceMutationSnapshot(t, i.store))
		})
	}
}

func TestInternalMigrationMutationsAcceptActivatedProtocol(t *testing.T) {
	for name, call := range namespaceMutationRPCs() {
		t.Run(name, func(t *testing.T) {
			i, proposer := namespaceMutationFixture(t, true, true)
			index := proposer.index
			require.NoError(t, call(t.Context(), i))
			require.Equal(t, index+1, proposer.index)
		})
	}
}
