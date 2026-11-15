package adapter

import (
	"testing"

	"github.com/bootjp/elastickv/kv"
	"github.com/stretchr/testify/require"
)

func TestPrepareEvidenceCapabilityAdvertisesTimestampBindingWithoutActivation(t *testing.T) {
	internal := &Internal{}
	reply := internal.prepareEvidenceCapabilities()
	require.Equal(t, kv.TimestampBindingProtocolVersion, reply.GetTimestampBindingProtocolVersion())
	require.False(t, reply.GetCutoverApplied())
	require.False(t, reply.GetProofEnabled())
}
