package server

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPrepareSQSServerRejectsInvalidPublicEndpointBeforeBinding(t *testing.T) {
	t.Parallel()
	server, listener, err := prepareSQSServer(context.Background(), nil, "127.0.0.1:0", nil, nil, nil, "us-east-1", "", "https://sqs.example.test/prefix", nil, nil)
	require.ErrorContains(t, err, "SQS public endpoint")
	require.Nil(t, server)
	require.Nil(t, listener)
}
