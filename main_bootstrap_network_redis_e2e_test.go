package main

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/bootjp/elastickv/internal/raftbootstrap"
	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/monitoring"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

func bootstrapNetworkRedisProfile(t *testing.T) ([]*multiEnablementProcess, raftbootstrap.BaselineManifest) {
	t.Helper()
	t.Setenv("ELASTICKV_RAFT_SNAPSHOT_COUNT", "1")
	t.Setenv(redisDeltaCompactorEnabledEnvVar, "false")
	require.Empty(t, *raftBootstrapPeerConfigDir)
	base := t.TempDir()
	identities := multiEnablementIdentities(t, base)
	legacy := multiEnablementStart(t, base, identities, false, false)
	multiEnablementPopulate(t, legacy)
	multiEnablementStop(t, legacy)
	manifest := multiEnablementBaseline(t, identities)
	acks := make([][]byte, 0, len(identities))
	for _, identity := range identities {
		acks = append(acks, multiEnablementPrepareAck(t, identity, manifest))
	}
	payload, err := assembleBootstrapActivation(manifest, manifest.BaselineContext, acks)
	require.NoError(t, err)
	activationPath := filepath.Join(t.TempDir(), "activation.bin")
	require.NoError(t, os.WriteFile(activationPath, payload, 0o600))
	for _, identity := range identities {
		identity.raw.ActivationFile = activationPath
		multiEnablementWriteConfig(t, identity)
	}
	return multiEnablementStart(t, base, identities, true, false), manifest
}

func bootstrapNetworkRedisClients(t *testing.T, processes []*multiEnablementProcess) map[*multiEnablementProcess]*redis.Client {
	t.Helper()
	leaders := make(map[string]string, len(processes))
	clients := make(map[*multiEnablementProcess]*redis.Client, len(processes))
	start := make([]func(), 0, len(processes))
	for _, process := range processes {
		launch, address, client := bootstrapNetworkPrepareRedis(t, process, leaders)
		leaders[process.runtime.spec.address] = address
		clients[process] = client
		start = append(start, launch)
	}
	// No goroutine may read the shared production routing map until complete.
	for _, launch := range start {
		launch()
	}
	return clients
}

func bootstrapNetworkPrepareRedis(t *testing.T, process *multiEnablementProcess, leaders map[string]string) (func(), string, *redis.Client) {
	t.Helper()
	registry := monitoring.NewRegistry("network-redis", process.runtime.spec.address)
	tracker := kv.NewActiveTimestampTracker()
	t.Cleanup(tracker.Close)
	server, compactor, listener, err := prepareRedisServer(process.ctx, &net.ListenConfig{}, "127.0.0.1:0", process.shards,
		newStartupGatedCoordinator(process.coordinate, process.gate), leaders, nil, registry, tracker, nil, 0)
	require.NoError(t, err)
	t.Cleanup(server.Stop)
	client := redis.NewClient(&redis.Options{Addr: listener.Addr().String(), Protocol: 2, DisableIdentity: true, MaxRetries: -1, ContextTimeoutEnabled: true})
	t.Cleanup(func() { require.NoError(t, client.Close()) })
	return func() { runRedisServer(process.ctx, process.eg, server, compactor) }, listener.Addr().String(), client
}

func TestBootstrapNetworkRedisFollowerAfterAcceptedBaseline(t *testing.T) {
	processes, manifest := bootstrapNetworkRedisProfile(t)
	leader := multiEnablementLeader(t, processes)
	var follower *multiEnablementProcess
	for _, process := range processes {
		if process != leader {
			follower = process
			break
		}
	}
	require.NotNil(t, follower)
	clients := bootstrapNetworkRedisClients(t, processes)
	client := clients[follower]
	ctx, cancel := context.WithTimeout(t.Context(), bootstrapE2EWaitTimeout)
	defer cancel()
	require.Error(t, client.Set(ctx, "network-before-ready", "must-not-commit", 0).Err())
	for _, process := range processes {
		require.False(t, process.runtime.publicReady())
	}
	startBootstrapActivationWorkers(leader.ctx, leader.eg, []*raftGroupRuntime{leader.runtime}, map[uint64]*kv.ShardGroup{1: leader.shard})
	for _, process := range processes {
		require.Eventually(t, process.runtime.publicReady, bootstrapE2EWaitTimeout, 10*time.Millisecond)
		require.True(t, process.runtime.bootstrapNetwork.dataReady())
	}
	// Real Redis ingress/forwarding retains Data ownership at each receiver.
	// Redis TCP forwarding is distinct from the pinned gRPC network tests.
	require.NotEqual(t, raftengine.StateLeader, follower.runtime.engine.State())
	require.NoError(t, client.Set(ctx, "network-redis-key", "forwarded-through-TLS", 0).Err())
	value, err := client.Get(ctx, "network-redis-key").Result()
	require.NoError(t, err)
	require.Equal(t, "forwarded-through-TLS", value)
	leaderClient := clients[leader]
	value, err = leaderClient.Get(ctx, "network-redis-key").Result()
	require.NoError(t, err)
	require.Equal(t, "forwarded-through-TLS", value)
	_, err = leaderClient.Get(ctx, "network-before-ready").Result()
	require.ErrorIs(t, err, redis.Nil)
	for _, process := range processes {
		multiEnablementReplicatedRead(t, process, "legacy-key", "replicated-original")
		policy, err := kv.LoadAppliedBootstrapPolicy(ctx, process.runtime.store, manifest.Identity, process.runtime.engine.Status().AppliedIndex)
		require.NoError(t, err)
		require.EqualValues(t, 1, policy.Generation)
	}
}
