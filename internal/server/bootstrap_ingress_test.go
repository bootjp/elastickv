package server

import (
	"context"
	"testing"

	"github.com/bootjp/elastickv/internal/clusterbootstrap"
	"github.com/bootjp/elastickv/internal/raftadmission"
	"github.com/bootjp/elastickv/internal/raftbootstrap"
	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/bootjp/elastickv/kv"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func bootstrapIngressFixture(t *testing.T) *clusterbootstrap.StartupPublicKVGate {
	t.Helper()
	cluster := raftbootstrap.UUID{1}
	a, err := raftadmission.New(raftadmission.Config{Cluster: cluster, DefaultGroup: 1, Groups: []uint64{1},
		ValidatePayload: clusterbootstrap.BootstrapPayloadValidator(cluster)})
	require.NoError(t, err)
	t.Cleanup(a.Close)
	bootstrapIngressObserve(t, a, 1)
	c := raftbootstrap.Configuration{Index: 1, State: raftbootstrap.ConfState{Voters: []uint64{1}},
		Members: []raftbootstrap.Member{{ID: 1, Address: "localhost:9000", PublicKeyDigest: raftbootstrap.Digest{1}}}}
	digest, err := raftbootstrap.ConfigurationDigest(c)
	require.NoError(t, err)
	ticket, err := a.Ticket(1)
	require.NoError(t, err)
	require.NoError(t, a.PublishRecovery(ticket, raftbootstrap.PolicyRecord{
		Version: raftbootstrap.PolicyVersion, Identity: raftbootstrap.Identity{Cluster: cluster, Group: 1},
		BaselineDigest: digest, Minimum: raftbootstrap.Versions{Entry: 1, Snapshot: 1, Session: 1},
		ActivationIndex: 1, Generation: 1, LastConfigurationIndex: 1, Configuration: c,
	}, 1, raftadmission.RegistrationNotConfigured))
	gate := &clusterbootstrap.StartupPublicKVGate{Domain: &clusterbootstrap.BootstrapDomain{Admission: a}}
	gate.MarkReady()
	return gate
}

func bootstrapIngressObserve(t *testing.T, a *raftadmission.Arbiter, term uint64) {
	t.Helper()
	require.NoError(t, a.WithNative(t.Context(), 1, func(scope raftengine.NativeScope) error {
		scope.Observe(raftengine.NativeState{Term: term, Role: raftengine.StateLeader, LeaderID: 1, ConfigurationIndex: 1})
		return nil
	}))
}

func bootstrapIngressLoadCatalog(t *testing.T, gate *clusterbootstrap.StartupPublicKVGate) {
	t.Helper()
	ticket, err := gate.Domain.Admission.Ticket(1)
	require.NoError(t, err)
	require.NoError(t, gate.Domain.Admission.MarkCatalogLoaded(ticket))
}

func TestBootstrapIngressRejectsDirectReadBeforeDataReady(t *testing.T) {
	t.Parallel()
	gate := bootstrapIngressFixture(t)
	called := false
	_, err := gate.UnaryInterceptor(t.Context(), nil,
		&grpc.UnaryServerInfo{FullMethod: pb.RawKV_RawGet_FullMethodName},
		func(context.Context, any) (any, error) { called = true; return "value", nil })
	require.Equal(t, codes.Unavailable, status.Code(err))
	require.False(t, called)
}

func TestBootstrapIngressDiscardsReadResultAfterTermChange(t *testing.T) {
	t.Parallel()
	gate := bootstrapIngressFixture(t)
	bootstrapIngressLoadCatalog(t, gate)
	response, err := gate.UnaryInterceptor(t.Context(), nil,
		&grpc.UnaryServerInfo{FullMethod: pb.RawKV_RawGet_FullMethodName},
		func(ctx context.Context, _ any) (any, error) {
			require.NoError(t, gate.Domain.Admission.ValidateResult(ctx))
			bootstrapIngressObserve(t, gate.Domain.Admission, 2)
			return "old value", nil
		})
	require.Equal(t, codes.Unknown, status.Code(err))
	require.ErrorIs(t, err, kv.ErrTransactionOutcomeUnknown)
	require.Nil(t, response)
}

func TestBootstrapIngressCommittedWriteResultRemainsUnknownAfterRevocation(t *testing.T) {
	t.Parallel()
	gate := bootstrapIngressFixture(t)
	bootstrapIngressLoadCatalog(t, gate)
	committed := false
	response, err := gate.UnaryInterceptor(t.Context(), nil,
		&grpc.UnaryServerInfo{FullMethod: pb.RawKV_RawPut_FullMethodName},
		func(ctx context.Context, _ any) (any, error) {
			require.NoError(t, gate.Domain.Admission.ValidateResult(ctx))
			committed = true
			bootstrapIngressObserve(t, gate.Domain.Admission, 2)
			return &pb.RawPutResponse{}, nil
		})
	require.True(t, committed)
	require.Nil(t, response)
	require.Equal(t, codes.Unknown, status.Code(err))
	require.ErrorIs(t, err, kv.ErrTransactionOutcomeUnknown)
	require.ErrorIs(t, err, raftadmission.ErrStale)
}

type bootstrapIngressTestStream struct {
	grpc.ServerStream
	ctx  context.Context
	sent int
}

func (s *bootstrapIngressTestStream) Context() context.Context { return s.ctx }
func (s *bootstrapIngressTestStream) SendMsg(any) error        { s.sent++; return nil }

func TestBootstrapIngressStreamRechecksEveryReply(t *testing.T) {
	t.Parallel()
	gate := bootstrapIngressFixture(t)
	bootstrapIngressLoadCatalog(t, gate)
	stream := &bootstrapIngressTestStream{ctx: t.Context()}
	owned, op, err := gate.Domain.Admission.Begin(t.Context(), raftadmission.Data)
	require.NoError(t, err)
	defer op.CancelAndJoin()
	current := &clusterbootstrap.BootstrapAdmissionStream{ServerStream: stream, Ctx: owned, Admission: gate.Domain.Admission}
	require.NoError(t, current.SendMsg("current"))
	bootstrapIngressObserve(t, gate.Domain.Admission, 2)
	err = current.SendMsg("stale")
	require.Equal(t, codes.Unavailable, status.Code(err))
	require.Equal(t, 1, stream.sent)
}
