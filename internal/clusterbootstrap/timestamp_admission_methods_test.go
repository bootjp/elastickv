package clusterbootstrap

import (
	"slices"
	"testing"

	"github.com/bootjp/elastickv/internal/peersession"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/stretchr/testify/require"
)

func TestTimestampAdmissionMethodsArePrivateAndGroupScoped(t *testing.T) {
	public := BootstrapPublicGRPCMethods()
	for _, name := range []string{pb.Internal_IssueBoundTimestamp_FullMethodName, pb.Internal_LookupTimestampBinding_FullMethodName} {
		require.False(t, slices.Contains(public, name), "timestamp admission cannot bypass peer sessions")
	}
	for _, tc := range []struct {
		group uint64
		name  string
		req   any
		reply any
	}{
		{0, pb.Internal_IssueBoundTimestamp_FullMethodName,
			&pb.IssueBoundTimestampRequest{MinTimestamp: 10, PhysicalBandProtocolVersion: 1},
			&pb.IssueBoundTimestampResponse{Timestamp: 11, PhysicalBandProtocolVersion: 1, CommittedBandEnd: 0xffff}},
		{17, pb.Internal_LookupTimestampBinding_FullMethodName, &pb.LookupTimestampBindingRequest{OperationId: append([]byte{1}, make([]byte, 15)...)}, &pb.LookupTimestampBindingResponse{AppliedIndex: 1}},
	} {
		methods := TimestampAdmissionPeerMethods(tc.group)
		require.Len(t, methods, 1)
		method := methods[0]
		require.Equal(t, tc.name, method.Name)
		require.False(t, method.AllowPublic)
		binding := peersession.RequestBinding{Group: tc.group, From: peersession.Node{Member: 2}, To: peersession.Node{Member: 1}}
		message, err := method.BoundRequest(binding, tc.req)
		require.NoError(t, err)
		require.Equal(t, peersession.Control, message.Traffic)
		require.Equal(t, tc.group, message.Group)
		_, err = method.Reply(message, tc.reply)
		require.NoError(t, err)
		if tc.group == 0 {
			_, err = method.BoundRequest(binding, &pb.IssueBoundTimestampRequest{MinTimestamp: 10})
			require.Error(t, err, "old request cannot select physical-band issuance")
			_, err = method.Reply(message, &pb.IssueBoundTimestampResponse{Timestamp: 11})
			require.Error(t, err, "old response cannot satisfy the versioned contract")
		}
		binding.Group++
		_, err = method.BoundRequest(binding, tc.req)
		require.Error(t, err)
	}
}
