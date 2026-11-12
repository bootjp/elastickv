package main

import (
	"context"
	"testing"

	"github.com/bootjp/elastickv/adapter"
	"github.com/bootjp/elastickv/internal/noderuntime"
	"github.com/bootjp/elastickv/internal/raftadmin"
	"github.com/bootjp/elastickv/kv"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestMigrationMembershipWiringKeepsEnvelopeProposerAndTSOIsolation(t *testing.T) {
	t.Parallel()
	proposer := &capabilityConfigEngine{}
	runtime := &raftGroupRuntime{GroupRuntime: &noderuntime.GroupRuntime{Store: store.NewMVCCStore(), Engine: capabilityConfigEngine{}}}
	runtime.Spec.ID = 7
	gate, ok := migrationMembershipInterceptor(runtime, nil, proposer, t.Name(), nil).(*adapter.MigrationNamespaceMembershipGate)
	require.True(t, ok)
	require.Same(t, proposer, gate.Proposer, "namespace prejoin must use the envelope-aware proposer passed by main")
	_, ok = gate.Next.(*adapter.MigrationMembershipGate)
	require.True(t, ok)
	runtime.Spec.ID = 0
	_, namespace := migrationMembershipInterceptor(runtime, nil, proposer, t.Name(), nil).(*adapter.MigrationNamespaceMembershipGate)
	require.False(t, namespace, "TSO must not require a KV namespace or KV capability")
	_, prepare := migrationMembershipInterceptor(runtime, nil, proposer, t.Name(), nil).(*prepareEvidenceMembershipGate)
	require.True(t, prepare, "existing TSO membership safety hooks must remain installed")
	var next raftadmin.MembershipChangeInterceptor = migrationMembershipNext{}
	require.Equal(t, next, migrationMembershipInterceptor(nil, nil, nil, "", next))
}

func TestMigrationCoverageGateAllowsOnlyTypedNamespaceBootstrap(t *testing.T) {
	t.Parallel()
	var migrationAuth nodeAuthFlags
	request, err := kv.NewNamespaceAdmissionRequest(1, kv.NamespaceAdmissionCommand{Version: kv.MigrationNamespaceVersion,
		ConfigurationIndex: 9, ApplyMembers: []uint64{7},
		Members: []kv.NamespaceMemberCapability{{MemberID: 7, Version: kv.MigrationNamespaceVersion, ConfigurationIndex: 9}}})
	require.NoError(t, err)
	info := &grpc.UnaryServerInfo{FullMethod: pb.Internal_CleanupMigration_FullMethodName}
	called := false
	_, err = adapter.MigrationCoverageUnaryGate(t.Context(), request, info, func(context.Context, any) (any, error) { called = true; return nil, nil }, migrationAuth.splitMigrationLocalReadinessGate)
	require.NoError(t, err)
	require.True(t, called)
	request.NamespaceAdmission = []byte("invalid")
	_, err = adapter.MigrationCoverageUnaryGate(t.Context(), request, info, func(context.Context, any) (any, error) {
		t.Fatal("malformed bootstrap reached the handler")
		return nil, nil
	}, migrationAuth.splitMigrationLocalReadinessGate)
	require.Equal(t, codes.InvalidArgument, status.Code(err))
}
