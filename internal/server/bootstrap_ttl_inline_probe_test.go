package server

import (
	"bytes"
	"context"
	"testing"

	"github.com/bootjp/elastickv/internal/clusterbootstrap"
	"github.com/bootjp/elastickv/internal/peersession"
	"github.com/bootjp/elastickv/internal/raftengine"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type ttlInlineNetworkProofEngine struct {
	native raftengine.NativeConfiguration
}

func (e ttlInlineNetworkProofEngine) State() raftengine.State { return raftengine.StateLeader }
func (e ttlInlineNetworkProofEngine) NativeConfigurationSnapshot() (raftengine.NativeConfiguration, uint64, bool) {
	return e.native, 1, true
}

type ttlInlineNetworkProofFSM struct{}

func (ttlInlineNetworkProofFSM) TTLInlineFenceReady() bool { return true }

func (s *bootstrapGateService) GetTTLInlineCapability(ctx context.Context, req *pb.GetTTLInlineCapabilityRequest) (*pb.GetTTLInlineCapabilityResponse, error) {
	if err := s.run(ctx); err != nil {
		return nil, err
	}
	return &pb.GetTTLInlineCapabilityResponse{GroupId: req.GetGroupId(), MemberId: 1,
		ConfigurationIndex: req.GetExpectedConfigurationIndex(), Term: req.GetExpectedTerm(),
		Voters: []uint64{1, 2}, Fenced: true, Supported: true, Challenge: bytes.Clone(req.GetChallenge())}, nil
}

func TestBootstrapTTLInlineProbeRequiresPeerSessionBeforeDataReady(t *testing.T) {
	t.Parallel()
	f := newBootstrapNetworkFixture(t, false)
	f.service.action = func(ctx context.Context) error {
		require.NotNil(t, peersession.CapturedReceipt(ctx))
		md, _ := metadata.FromIncomingContext(ctx)
		require.Empty(t, md.Get(clusterbootstrap.BootstrapControlHeader))
		return nil
	}
	req := &pb.GetTTLInlineCapabilityRequest{GroupId: bootstrapNetworkTestGroup,
		Challenge: bytes.Repeat([]byte{0x42}, 32), ExpectedConfigurationIndex: 1, ExpectedTerm: 1}
	response, err := f.network.Control.ProbeTTLInlineCapability(t.Context(), bootstrapNetworkTestGroup, f.address, req)
	require.NoError(t, err)
	require.True(t, response.GetFenced())
	require.True(t, response.GetSupported())
	require.Equal(t, req.GetChallenge(), response.GetChallenge())
	require.False(t, f.network.DataReady(), "proof must work before public DataReady")
	native := raftengine.NativeConfiguration{Index: 1, Voters: []uint64{1, 2},
		Members: []raftengine.NativeConfigurationMember{
			{ID: 1, Address: f.address}, {ID: 2, Address: f.config.Local.Address},
		}}
	certificate, err := clusterbootstrap.CollectTTLInlineCapabilityCertificate(t.Context(), bootstrapNetworkTestGroup,
		f.config, ttlInlineNetworkProofEngine{native: native}, ttlInlineNetworkProofFSM{}, f.network.Control.ProbeTTLInlineCapability)
	require.NoError(t, err)
	require.Equal(t, []uint64{1, 2}, certificate.Members)
	conn, err := f.cache.ConnFor(f.address)
	require.NoError(t, err)
	_, err = pb.NewInternalClient(conn).GetTTLInlineCapability(t.Context(), req)
	require.Equal(t, codes.PermissionDenied, status.Code(err))
	require.EqualValues(t, 2, f.service.calls.Load())
}
