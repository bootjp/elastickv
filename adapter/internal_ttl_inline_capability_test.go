package adapter

import (
	"testing"

	pb "github.com/bootjp/elastickv/proto"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestInternalTTLInlineCapabilityUnavailableWithoutProtectedProbe(t *testing.T) {
	t.Parallel()
	server := NewInternalWithEngine(nil, nil, nil, nil)
	response, err := server.GetTTLInlineCapability(t.Context(), &pb.GetTTLInlineCapabilityRequest{})
	require.Nil(t, response)
	require.Equal(t, codes.Unavailable, status.Code(err))
}
