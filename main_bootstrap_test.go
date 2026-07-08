package main

import (
	"testing"
	"time"

	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/stretchr/testify/require"
)

func TestResolveBootstrapServers(t *testing.T) {
	t.Run("members imply fixed bootstrap servers", func(t *testing.T) {
		servers, err := resolveBootstrapServers("n1", []groupSpec{{id: 1, address: "10.0.0.11:50051"}}, "n1=10.0.0.11:50051")
		require.NoError(t, err)
		require.Equal(t, []raftengine.Server{
			{Suffrage: "voter", ID: "n1", Address: "10.0.0.11:50051"},
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
		require.Equal(t, []raftengine.Server{
			{Suffrage: "voter", ID: "n1", Address: "10.0.0.11:50051"},
			{Suffrage: "voter", ID: "n2", Address: "10.0.0.12:50051"},
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

func TestResolveBootstrapConfigGroupPeers(t *testing.T) {
	groups := []groupSpec{
		{id: 1, address: "10.0.0.11:50051"},
		{id: 2, address: "10.0.0.11:50052"},
	}
	raw := "1=n1@10.0.0.11:50051,n2@10.0.0.12:50051;2=n1@10.0.0.11:50052,n2@10.0.0.12:50052"

	t.Run("resolves per group peers", func(t *testing.T) {
		cfg, err := resolveBootstrapConfig("n1", groups, "", raw)
		require.NoError(t, err)
		require.Nil(t, cfg.legacyServers)
		require.Equal(t, []raftengine.Server{
			{Suffrage: "voter", ID: "n1", Address: "10.0.0.11:50051"},
			{Suffrage: "voter", ID: "n2", Address: "10.0.0.12:50051"},
		}, cfg.serversForGroup(1))
		require.Equal(t, []raftengine.Server{
			{Suffrage: "voter", ID: "n1", Address: "10.0.0.11:50052"},
			{Suffrage: "voter", ID: "n2", Address: "10.0.0.12:50052"},
		}, cfg.serversForGroup(2))
		require.Equal(t, cfg.serversForGroup(1), cfg.adminSeed(1))
		require.Equal(t, cfg.serversForGroup(2), cfg.bootstrapSeedForGroup(2))
	})

	t.Run("mutually exclusive with bootstrap members", func(t *testing.T) {
		_, err := resolveBootstrapConfig("n1", groups, "n1=10.0.0.11:50051,n2=10.0.0.12:50051", raw)
		require.ErrorIs(t, err, ErrRaftGroupPeersMutuallyExclusive)
	})

	t.Run("missing group is rejected", func(t *testing.T) {
		_, err := resolveBootstrapConfig("n1", groups, "", "1=n1@10.0.0.11:50051,n2@10.0.0.12:50051")
		require.ErrorIs(t, err, ErrRaftGroupPeersMissingGroup)
	})

	t.Run("local address mismatch is rejected", func(t *testing.T) {
		_, err := resolveBootstrapConfig("n1", groups, "",
			"1=n1@10.0.0.99:50051,n2@10.0.0.12:50051;2=n1@10.0.0.11:50052,n2@10.0.0.12:50052")
		require.ErrorIs(t, err, ErrRaftGroupPeersLocalAddrMismatch)
	})

	t.Run("heterogeneous membership is rejected", func(t *testing.T) {
		_, err := resolveBootstrapConfig("n1", groups, "",
			"1=n1@10.0.0.11:50051,n2@10.0.0.12:50051;2=n1@10.0.0.11:50052,n3@10.0.0.13:50052")
		require.ErrorIs(t, err, ErrRaftGroupPeersHeterogeneous)
	})
}

func TestDurationToTicks(t *testing.T) {
	t.Parallel()

	t.Run("zero tick returns min", func(t *testing.T) {
		require.Equal(t, 5, durationToTicks(200*time.Millisecond, 0, 5))
	})

	t.Run("exact division", func(t *testing.T) {
		require.Equal(t, 20, durationToTicks(200*time.Millisecond, 10*time.Millisecond, 1))
	})

	t.Run("remainder rounds up", func(t *testing.T) {
		require.Equal(t, 21, durationToTicks(205*time.Millisecond, 10*time.Millisecond, 1))
	})

	t.Run("below min returns min", func(t *testing.T) {
		require.Equal(t, 10, durationToTicks(50*time.Millisecond, 10*time.Millisecond, 10))
	})
}

func TestNewRaftFactory_UnsupportedEngine(t *testing.T) {
	t.Parallel()
	_, err := newRaftFactory(raftEngineType("unknown"), nil)
	require.ErrorIs(t, err, ErrUnsupportedRaftEngine)
}
