package clusterbootstrap

import (
	"bytes"
	"context"
	"testing"

	"github.com/bootjp/elastickv/internal/raftengine"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type ttlInlineNativeSnapshotStub struct {
	called bool
}

func (s *ttlInlineNativeSnapshotStub) NativeConfigurationSnapshot() (raftengine.NativeConfiguration, uint64, bool) {
	s.called = true
	return raftengine.NativeConfiguration{Index: 5, Voters: []uint64{1, 2}}, 9, true
}

type ttlInlineFenceStateStub struct{ ready bool }

func (s ttlInlineFenceStateStub) TTLInlineFenceReady() bool { return s.ready }

func TestTTLInlineLiveProbeRejectsRequestsWithoutPeerReceipt(t *testing.T) {
	t.Parallel()
	engine := &ttlInlineNativeSnapshotStub{}
	probe := NewTTLInlineLiveProbe(7, 1, engine, ttlInlineFenceStateStub{ready: true})
	for _, request := range []*pb.GetTTLInlineCapabilityRequest{
		{GroupId: 7, ExpectedConfigurationIndex: 5, ExpectedTerm: 9, Challenge: bytes.Repeat([]byte{1}, ttlInlineChallengeSize)},
		{GroupId: 7, ExpectedConfigurationIndex: 5, ExpectedTerm: 9, Challenge: nil},
		{GroupId: 8, ExpectedConfigurationIndex: 5, ExpectedTerm: 9, Challenge: bytes.Repeat([]byte{1}, ttlInlineChallengeSize)},
	} {
		response, err := probe(context.Background(), request)
		require.Nil(t, response)
		require.Equal(t, codes.PermissionDenied, status.Code(err))
	}
	require.False(t, engine.called)
}
