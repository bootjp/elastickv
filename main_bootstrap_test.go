package main

import (
	"testing"

	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/require"
)

func TestResolveBootstrapServers(t *testing.T) {
	t.Run("members imply fixed bootstrap servers", func(t *testing.T) {
		servers, err := resolveBootstrapServers("n1", []groupSpec{{id: 1, address: "10.0.0.11:50051"}}, "n1=10.0.0.11:50051")
		require.NoError(t, err)
		require.Equal(t, []raft.Server{
			{Suffrage: raft.Voter, ID: "n1", Address: "10.0.0.11:50051"},
		}, servers)
	})

	t.Run("empty members returns nil", func(t *testing.T) {
		servers, err := resolveBootstrapServers("n1", []groupSpec{{id: 1, address: "10.0.0.11:50051"}}, "")
		require.NoError(t, err)
		require.Nil(t, servers)
	})

	t.Run("single group fixed members", func(t *testing.T) {
		servers, err := resolveBootstrapServers(
			"n1",
			[]groupSpec{{id: 1, address: "10.0.0.11:50051"}},
			"n1=10.0.0.11:50051,n2=10.0.0.12:50051",
		)
		require.NoError(t, err)
		require.Equal(t, []raft.Server{
			{Suffrage: raft.Voter, ID: "n1", Address: "10.0.0.11:50051"},
			{Suffrage: raft.Voter, ID: "n2", Address: "10.0.0.12:50051"},
		}, servers)
	})

	t.Run("multiple groups are rejected", func(t *testing.T) {
		_, err := resolveBootstrapServers(
			"n1",
			[]groupSpec{{id: 1, address: "10.0.0.11:50051"}, {id: 2, address: "10.0.0.11:50052"}},
			"n1=10.0.0.11:50051,n2=10.0.0.12:50051",
		)
		require.ErrorIs(t, err, ErrBootstrapMembersRequireSingleGroup)
	})

	t.Run("missing local member is rejected", func(t *testing.T) {
		_, err := resolveBootstrapServers(
			"n1",
			[]groupSpec{{id: 1, address: "10.0.0.11:50051"}},
			"n2=10.0.0.12:50051",
		)
		require.ErrorIs(t, err, ErrBootstrapMembersMissingLocalNode)
	})

	t.Run("local address mismatch is rejected", func(t *testing.T) {
		_, err := resolveBootstrapServers(
			"n1",
			[]groupSpec{{id: 1, address: "10.0.0.11:50051"}},
			"n1=10.0.0.99:50051,n2=10.0.0.12:50051",
		)
		require.ErrorIs(t, err, ErrBootstrapMembersLocalAddrMismatch)
	})

	t.Run("only separators are rejected", func(t *testing.T) {
		_, err := resolveBootstrapServers("n1", []groupSpec{{id: 1, address: "10.0.0.11:50051"}}, " , , ")
		require.ErrorIs(t, err, ErrNoBootstrapMembersConfigured)
	})
}
