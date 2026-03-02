package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/bootjp/elastickv/adapter"
	"github.com/bootjp/elastickv/kv"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type bootstrapE2EEndpoint struct {
	id         string
	raftAddr   string
	redisAddr  string
	dynamoAddr string
}

type bootstrapE2ENode struct {
	id       string
	runtimes []*raftGroupRuntime

	shardStore *kv.ShardStore
	cancel     context.CancelFunc
	eg         *errgroup.Group
}

func (n *bootstrapE2ENode) raft() *raft.Raft {
	if n == nil || len(n.runtimes) == 0 {
		return nil
	}
	return n.runtimes[0].raft
}

func (n *bootstrapE2ENode) close() error {
	if n == nil {
		return nil
	}
	if n.cancel != nil {
		n.cancel()
	}

	var waitErr error
	if n.eg != nil {
		waitErr = n.eg.Wait()
	}

	if n.shardStore != nil {
		_ = n.shardStore.Close()
		n.shardStore = nil
	}
	for _, rt := range n.runtimes {
		if rt != nil {
			rt.Close()
		}
	}
	n.runtimes = nil
	return waitErr
}

func TestRaftBootstrapMembers_E2E_FixedClusterWithoutAddVoter(t *testing.T) {
	const (
		startupAttempts = 5
		nodeCount       = 4
		waitTimeout     = 20 * time.Second
		waitInterval    = 100 * time.Millisecond
		rpcTimeout      = 2 * time.Second
	)

	baseDir := t.TempDir()
	endpoints, nodes := startBootstrapE2ECluster(t, baseDir, nodeCount, startupAttempts)
	t.Cleanup(func() { closeBootstrapE2ENodes(t, nodes) })

	expected := bootstrapExpectedServers(endpoints)
	waitForBootstrapClusterConfig(t, nodes, expected, waitTimeout, waitInterval)
	leaderIdx := waitForSingleLeader(t, nodes, waitTimeout, waitInterval)

	clients, conns := rawKVClients(t, endpoints)
	t.Cleanup(func() {
		for _, conn := range conns {
			_ = conn.Close()
		}
	})

	writerIdx := (leaderIdx + 1) % len(clients)
	key := []byte("bootstrap-members-e2e-key")
	value := []byte("bootstrap-members-e2e-value")

	require.NoError(t, rawPutWithTimeout(clients[writerIdx], key, value, rpcTimeout))

	for i := range clients {
		client := clients[i]
		require.Eventually(t, func() bool {
			resp, getErr := rawGetWithTimeout(client, key, rpcTimeout)
			if getErr != nil {
				return false
			}
			return resp.Exists && bytes.Equal(resp.Value, value)
		}, waitTimeout, waitInterval)
	}
}

func startBootstrapE2ECluster(
	t *testing.T,
	baseDir string,
	nodeCount int,
	startupAttempts int,
) ([]bootstrapE2EEndpoint, []*bootstrapE2ENode) {
	t.Helper()

	var (
		lastErr error
		nodes   []*bootstrapE2ENode
	)

	for attempt := 0; attempt < startupAttempts; attempt++ {
		endpoints := allocateBootstrapE2EEndpoints(t, nodeCount)
		started, err := tryStartBootstrapE2ECluster(baseDir, endpoints)
		if err == nil {
			return endpoints, started
		}
		closeBootstrapE2ENodesIgnoreError(started)
		lastErr = err
		if !isAddressInUseError(err) {
			break
		}
		nodes = nil
	}

	require.NoError(t, lastErr)
	return nil, nodes
}

func allocateBootstrapE2EEndpoints(t *testing.T, nodeCount int) []bootstrapE2EEndpoint {
	t.Helper()

	endpoints := make([]bootstrapE2EEndpoint, 0, nodeCount)
	for i := 0; i < nodeCount; i++ {
		endpoints = append(endpoints, bootstrapE2EEndpoint{
			id:         fmt.Sprintf("n%d", i+1),
			raftAddr:   reserveTCPAddr(t),
			redisAddr:  reserveTCPAddr(t),
			dynamoAddr: reserveTCPAddr(t),
		})
	}
	return endpoints
}

func tryStartBootstrapE2ECluster(baseDir string, endpoints []bootstrapE2EEndpoint) ([]*bootstrapE2ENode, error) {
	bootstrapMembers := bootstrapMembersArg(endpoints)
	nodes := make([]*bootstrapE2ENode, 0, len(endpoints))
	for i, ep := range endpoints {
		node, err := startBootstrapE2ENode(baseDir, ep, i == 0, bootstrapMembers)
		if err != nil {
			return nodes, err
		}
		nodes = append(nodes, node)
	}
	return nodes, nil
}

func closeBootstrapE2ENodesIgnoreError(nodes []*bootstrapE2ENode) {
	for _, n := range nodes {
		if n == nil {
			continue
		}
		_ = n.close()
	}
}

func isAddressInUseError(err error) bool {
	if err == nil {
		return false
	}
	if strings.Contains(strings.ToLower(err.Error()), "address already in use") {
		return true
	}
	var opErr *net.OpError
	if errors.As(err, &opErr) && strings.Contains(strings.ToLower(opErr.Error()), "address already in use") {
		return true
	}
	return false
}

func reserveTCPAddr(t *testing.T) string {
	t.Helper()

	var lc net.ListenConfig
	l, err := lc.Listen(context.Background(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := l.Addr().String()
	require.NoError(t, l.Close())
	return addr
}

func bootstrapMembersArg(endpoints []bootstrapE2EEndpoint) string {
	parts := make([]string, 0, len(endpoints))
	for _, ep := range endpoints {
		parts = append(parts, fmt.Sprintf("%s=%s", ep.id, ep.raftAddr))
	}
	return strings.Join(parts, ",")
}

func bootstrapExpectedServers(endpoints []bootstrapE2EEndpoint) []raft.Server {
	servers := make([]raft.Server, 0, len(endpoints))
	for _, ep := range endpoints {
		servers = append(servers, raft.Server{
			Suffrage: raft.Voter,
			ID:       raft.ServerID(ep.id),
			Address:  raft.ServerAddress(ep.raftAddr),
		})
	}
	return servers
}

func startBootstrapE2ENode(baseDir string, ep bootstrapE2EEndpoint, bootstrap bool, bootstrapMembers string) (*bootstrapE2ENode, error) {
	cfg, err := parseRuntimeConfig(ep.raftAddr, ep.redisAddr, "", "", "")
	if err != nil {
		return nil, err
	}

	bootstrapServers, err := resolveBootstrapServers(ep.id, cfg.groups, bootstrap, bootstrapMembers)
	if err != nil {
		return nil, err
	}

	runtimes, shardGroups, err := buildShardGroups(ep.id, baseDir, cfg.groups, cfg.multi, bootstrap, bootstrapServers)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())
	eg, runCtx := errgroup.WithContext(ctx)

	clock := kv.NewHLC()
	shardStore := kv.NewShardStore(cfg.engine, shardGroups)
	coordinate := kv.NewShardedCoordinator(cfg.engine, shardGroups, cfg.defaultGroup, clock, shardStore)
	distCatalog, err := setupDistributionCatalog(runCtx, runtimes, cfg.engine)
	if err != nil {
		cancel()
		_ = shardStore.Close()
		for _, rt := range runtimes {
			rt.Close()
		}
		return nil, err
	}

	eg.Go(func() error {
		return runDistributionCatalogWatcher(runCtx, distCatalog, cfg.engine)
	})

	distServer := adapter.NewDistributionServer(
		cfg.engine,
		distCatalog,
		adapter.WithDistributionCoordinator(coordinate),
	)

	var lc net.ListenConfig
	err = startRuntimeServers(
		runCtx,
		&lc,
		eg,
		cancel,
		runtimes,
		shardStore,
		coordinate,
		distServer,
		ep.redisAddr,
		cfg.leaderRedis,
		ep.dynamoAddr,
	)
	if err != nil {
		cancel()
		_ = eg.Wait()
		_ = shardStore.Close()
		for _, rt := range runtimes {
			rt.Close()
		}
		return nil, err
	}

	return &bootstrapE2ENode{
		id:         ep.id,
		runtimes:   runtimes,
		shardStore: shardStore,
		cancel:     cancel,
		eg:         eg,
	}, nil
}

func closeBootstrapE2ENodes(t *testing.T, nodes []*bootstrapE2ENode) {
	t.Helper()
	for _, n := range nodes {
		require.NoError(t, n.close())
	}
}

func waitForBootstrapClusterConfig(t *testing.T, nodes []*bootstrapE2ENode, expected []raft.Server, waitTimeout, waitInterval time.Duration) {
	t.Helper()

	require.Eventually(t, func() bool {
		for _, n := range nodes {
			r := n.raft()
			if r == nil {
				return false
			}
			future := r.GetConfiguration()
			if err := future.Error(); err != nil {
				return false
			}
			current := future.Configuration().Servers
			if len(current) != len(expected) {
				return false
			}
			for _, server := range expected {
				if !containsRaftServer(current, server) {
					return false
				}
			}
		}
		return true
	}, waitTimeout, waitInterval)
}

func waitForSingleLeader(t *testing.T, nodes []*bootstrapE2ENode, waitTimeout, waitInterval time.Duration) int {
	t.Helper()

	leaderIdx := -1
	require.Eventually(t, func() bool {
		idx := -1
		leaders := 0
		for i, n := range nodes {
			r := n.raft()
			if r == nil {
				return false
			}
			if r.State() == raft.Leader {
				idx = i
				leaders++
			}
		}
		if leaders != 1 {
			return false
		}
		leaderIdx = idx
		return true
	}, waitTimeout, waitInterval)
	return leaderIdx
}

func rawKVClients(t *testing.T, endpoints []bootstrapE2EEndpoint) ([]pb.RawKVClient, []*grpc.ClientConn) {
	t.Helper()

	clients := make([]pb.RawKVClient, 0, len(endpoints))
	conns := make([]*grpc.ClientConn, 0, len(endpoints))
	for _, ep := range endpoints {
		conn, err := grpc.NewClient(ep.raftAddr,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		)
		require.NoError(t, err)
		conns = append(conns, conn)
		clients = append(clients, pb.NewRawKVClient(conn))
	}
	return clients, conns
}

func rawPutWithTimeout(client pb.RawKVClient, key []byte, value []byte, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	_, err := client.RawPut(ctx, &pb.RawPutRequest{Key: key, Value: value})
	return err
}

func rawGetWithTimeout(client pb.RawKVClient, key []byte, timeout time.Duration) (*pb.RawGetResponse, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return client.RawGet(ctx, &pb.RawGetRequest{Key: key})
}

func containsRaftServer(servers []raft.Server, expected raft.Server) bool {
	for _, s := range servers {
		if s.ID == expected.ID && s.Address == expected.Address && s.Suffrage == expected.Suffrage {
			return true
		}
	}
	return false
}
