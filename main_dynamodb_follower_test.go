package main

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBuildLeaderDynamoWithoutHTTPMap(t *testing.T) {
	t.Parallel()
	groups := []groupSpec{{id: 1, address: "127.0.0.1:50051"}}
	for _, raw := range []string{"", "  "} {
		got, err := buildLeaderDynamo(groups, "127.0.0.1:8000", raw)
		require.NoError(t, err)
		require.Empty(t, got)
	}
	got, err := buildLeaderDynamo(groups, "127.0.0.1:8000", "127.0.0.1:50052=127.0.0.1:8001")
	require.NoError(t, err)
	require.Equal(t, map[string]string{
		"127.0.0.1:50051": "127.0.0.1:8000",
		"127.0.0.1:50052": "127.0.0.1:8001",
	}, got)
	_, err = buildLeaderDynamo(groups, "127.0.0.1:8000", "invalid")
	require.Error(t, err)
}
