package clusterbootstrap

import (
	"bytes"
	"slices"
	"testing"

	"github.com/bootjp/elastickv/internal/peersession"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func TestTTLInlinePeerMethodBindsChallengeAndNativeCut(t *testing.T) {
	t.Parallel()
	const group uint64 = 7
	method := TTLInlinePeerMethod(group)
	require.False(t, method.AllowPublic)
	require.NotNil(t, method.BoundRequest)
	require.NotNil(t, method.Reply)
	binding := peersession.RequestBinding{Group: group,
		From: peersession.Node{Member: 2}, To: peersession.Node{Member: 1}}
	req := &pb.GetTTLInlineCapabilityRequest{GroupId: group,
		Challenge:                  bytes.Repeat([]byte{0x42}, ttlInlineChallengeSize),
		ExpectedConfigurationIndex: 5, ExpectedTerm: 9}
	message, err := method.BoundRequest(binding, req)
	require.NoError(t, err)
	require.Equal(t, peersession.Probe, message.Traffic)
	require.Equal(t, uint64(2), message.From)
	require.Equal(t, uint64(1), message.To)
	response := &pb.GetTTLInlineCapabilityResponse{GroupId: group, MemberId: 1,
		ConfigurationIndex: 5, Term: 9, Voters: []uint64{1, 2},
		Fenced: true, Supported: true, Challenge: bytes.Clone(req.Challenge)}
	reply, err := method.Reply(message, response)
	require.NoError(t, err)
	require.Equal(t, uint64(1), reply.From)
	require.Equal(t, uint64(2), reply.To)
	require.Equal(t, message.Declaration, reply.Declaration)
	require.False(t, slices.Contains(BootstrapPublicGRPCMethods(), method.Name))

	for _, tc := range []struct {
		name   string
		mutate func(*pb.GetTTLInlineCapabilityResponse)
	}{
		{"wrong member", func(r *pb.GetTTLInlineCapabilityResponse) { r.MemberId++ }},
		{"wrong group", func(r *pb.GetTTLInlineCapabilityResponse) { r.GroupId++ }},
		{"wrong index", func(r *pb.GetTTLInlineCapabilityResponse) { r.ConfigurationIndex++ }},
		{"wrong term", func(r *pb.GetTTLInlineCapabilityResponse) { r.Term++ }},
		{"wrong challenge", func(r *pb.GetTTLInlineCapabilityResponse) { r.Challenge[0]++ }},
		{"unfenced", func(r *pb.GetTTLInlineCapabilityResponse) { r.Fenced = false }},
		{"unsupported", func(r *pb.GetTTLInlineCapabilityResponse) { r.Supported = false }},
		{"invalid roles", func(r *pb.GetTTLInlineCapabilityResponse) { r.Learners = []uint64{1} }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			changed := proto.CloneOf(response)
			tc.mutate(changed)
			_, err := method.Reply(message, changed)
			require.ErrorIs(t, err, peersession.ErrFormat)
		})
	}
}

func TestTTLInlinePeerMethodRejectsUnboundRequests(t *testing.T) {
	t.Parallel()
	method := TTLInlinePeerMethod(7)
	binding := peersession.RequestBinding{Group: 7,
		From: peersession.Node{Member: 2}, To: peersession.Node{Member: 1}}
	request := &pb.GetTTLInlineCapabilityRequest{GroupId: 7,
		Challenge:                  bytes.Repeat([]byte{0x42}, ttlInlineChallengeSize),
		ExpectedConfigurationIndex: 5, ExpectedTerm: 9}
	for _, mutate := range []func(*pb.GetTTLInlineCapabilityRequest){
		func(r *pb.GetTTLInlineCapabilityRequest) { r.GroupId++ },
		func(r *pb.GetTTLInlineCapabilityRequest) { r.Challenge = nil },
		func(r *pb.GetTTLInlineCapabilityRequest) { r.ExpectedConfigurationIndex = 0 },
		func(r *pb.GetTTLInlineCapabilityRequest) { r.ExpectedTerm = 0 },
	} {
		changed := proto.CloneOf(request)
		mutate(changed)
		_, err := method.BoundRequest(binding, changed)
		require.ErrorIs(t, err, peersession.ErrFormat)
	}
	binding.Group++
	_, err := method.BoundRequest(binding, request)
	require.ErrorIs(t, err, peersession.ErrFormat)
}
