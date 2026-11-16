package clusterbootstrap

import (
	"bytes"
	"context"
	"testing"

	"github.com/bootjp/elastickv/internal/peersession"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"
)

func TestBootstrapControlProbeRejectsCallerSessionMetadata(t *testing.T) {
	t.Parallel()
	client := &bootstrapControlClient{Ctx: t.Context(), Done: make(chan struct{}),
		peers: make(map[bootstrapControlPeerKey]*bootstrapControlPeer)}
	req := &pb.GetTTLInlineCapabilityRequest{GroupId: 7,
		Challenge:                  bytes.Repeat([]byte{0x42}, ttlInlineChallengeSize),
		ExpectedConfigurationIndex: 5, ExpectedTerm: 9}
	for _, header := range []string{BootstrapControlHeader, "x-elastickv-peer-session", "x-elastickv-peer-proof"} {
		t.Run(header, func(t *testing.T) {
			ctx := metadata.AppendToOutgoingContext(t.Context(), header, "caller-supplied")
			response, err := client.ProbeTTLInlineCapability(ctx, 7, "unknown", req)
			require.Nil(t, response)
			require.ErrorIs(t, err, peersession.ErrDenied)
			require.Empty(t, client.peers)
		})
	}
}

func TestBootstrapControlProbeRejectsUnboundAndUnknownPeer(t *testing.T) {
	t.Parallel()
	client := &bootstrapControlClient{Ctx: t.Context(), Done: make(chan struct{}),
		peers: make(map[bootstrapControlPeerKey]*bootstrapControlPeer)}
	for _, req := range []*pb.GetTTLInlineCapabilityRequest{
		nil,
		{GroupId: 7, Challenge: []byte("short"), ExpectedConfigurationIndex: 5, ExpectedTerm: 9},
		{GroupId: 7, Challenge: bytes.Repeat([]byte{0x42}, ttlInlineChallengeSize),
			ExpectedConfigurationIndex: 5, ExpectedTerm: 9},
	} {
		response, err := client.ProbeTTLInlineCapability(t.Context(), 7, "unknown", req)
		require.Nil(t, response)
		require.ErrorIs(t, err, peersession.ErrDenied)
		require.Empty(t, client.peers)
	}
	response, err := client.ProbeTTLInlineCapability(context.Background(), 8, "unknown", nil)
	require.Nil(t, response)
	require.ErrorIs(t, err, peersession.ErrDenied)
	require.False(t, bootstrapOutgoingControlTraffic(peersession.Probe))
}
