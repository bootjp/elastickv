package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/bootjp/elastickv/adapter"
	internalraftadmin "github.com/bootjp/elastickv/internal/raftadmin"
	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/bootjp/elastickv/kv"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/reflection"
)

type bootstrapE2EEndpoint struct {
	id         string
	raftAddr   string
	redisAddr  string
	dynamoAddr string
}

type bootstrapE2EListeners struct {
	grpc   net.Listener
	redis  net.Listener
	dynamo net.Listener
}

type bootstrapE2ENode struct {
	id       string
	runtimes []*raftGroupRuntime

	shardStore *kv.ShardStore
	cancel     context.CancelFunc
	eg         *errgroup.Group
}

func (n *bootstrapE2ENode) engine() raftengine.Engine {
	if n == nil || len(n.runtimes) == 0 {
		return nil
	}
	return n.runtimes[0].engine
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
	for _, engineType := range []raftEngineType{raftEngineHashicorp, raftEngineEtcd} {
		t.Run(string(engineType), func(t *testing.T) {
			const (
				startupAttempts = 5
				nodeCount       = 4
				waitTimeout     = 20 * time.Second
				waitInterval    = 100 * time.Millisecond
				rpcTimeout      = 2 * time.Second
			)

			baseDir := t.TempDir()
			_, endpoints, nodes := startBootstrapE2ECluster(t, baseDir, nodeCount, startupAttempts, engineType)
			t.Cleanup(func() { closeBootstrapE2ENodes(t, nodes) })

			expected := bootstrapExpectedConfiguration(endpoints)
			waitForBootstrapClusterConfig(t, nodes, expected, waitTimeout, waitInterval)
			leaderIdx := waitForSingleLeader(t, nodes, waitTimeout, waitInterval)

			clients, conns := rawKVClients(t, endpoints)
			t.Cleanup(func() { closeGRPCConns(conns) })

			writerIdx := (leaderIdx + 1) % len(clients)
			key := []byte("bootstrap-members-e2e-key")
			value := []byte("bootstrap-members-e2e-value")

			// Retry the first Put: the gRPC connection to a freshly started node
			// may not be ready immediately, causing "context canceled while waiting
			// for connections to become ready".
			require.Eventually(t, func() bool {
				return rawPutWithTimeout(clients[writerIdx], key, value, rpcTimeout) == nil
			}, waitTimeout, waitInterval)

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
		})
	}
}

func TestRaftBootstrapMembers_E2E_EtcdLeaderRestartRecovery(t *testing.T) {
	const (
		startupAttempts = 5
		nodeCount       = 3
		waitTimeout     = 20 * time.Second
		waitInterval    = 100 * time.Millisecond
		rpcTimeout      = 2 * time.Second
	)

	baseDir := t.TempDir()
	clusterDir, endpoints, nodes := startBootstrapE2ECluster(t, baseDir, nodeCount, startupAttempts, raftEngineEtcd)
	t.Cleanup(func() { closeBootstrapE2ENodes(t, nodes) })

	expected := bootstrapExpectedConfiguration(endpoints)
	waitForBootstrapClusterConfig(t, nodes, expected, waitTimeout, waitInterval)
	leaderIdx := waitForSingleLeader(t, nodes, waitTimeout, waitInterval)

	clients, conns := rawKVClients(t, endpoints)
	defer closeGRPCConns(conns)

	keyA := []byte("bootstrap-etcd-restart-key-a")
	valueA := []byte("bootstrap-etcd-restart-value-a")
	// Retry the first Put: the gRPC connection to a freshly started node may not
	// be ready immediately, causing "context canceled while waiting for connections".
	require.Eventually(t, func() bool {
		return rawPutWithTimeout(clients[(leaderIdx+1)%len(clients)], keyA, valueA, rpcTimeout) == nil
	}, waitTimeout, waitInterval)
	waitForValueOnAllClients(t, clients, keyA, valueA, waitTimeout, waitInterval, rpcTimeout)

	require.NoError(t, nodes[leaderIdx].close())
	restartListeners := bindBootstrapE2EEndpointListeners(t, endpoints[leaderIdx])
	restartedNode, err := startBootstrapE2ENode(clusterDir, endpoints[leaderIdx], restartListeners, false, "", raftEngineEtcd)
	require.NoError(t, err)
	nodes[leaderIdx] = restartedNode

	waitForBootstrapClusterConfig(t, nodes, expected, waitTimeout, waitInterval)
	leaderIdx = waitForSingleLeader(t, nodes, waitTimeout, waitInterval)

	restartedClients, restartedConns := rawKVClients(t, endpoints)
	defer closeGRPCConns(restartedConns)
	waitForValueOnAllClients(t, restartedClients, keyA, valueA, waitTimeout, waitInterval, rpcTimeout)

	keyB := []byte("bootstrap-etcd-restart-key-b")
	valueB := []byte("bootstrap-etcd-restart-value-b")
	require.Eventually(t, func() bool {
		return rawPutWithTimeout(restartedClients[(leaderIdx+1)%len(restartedClients)], keyB, valueB, rpcTimeout) == nil
	}, waitTimeout, waitInterval)
	waitForValueOnAllClients(t, restartedClients, keyB, valueB, waitTimeout, waitInterval, rpcTimeout)
}

func startBootstrapE2ECluster(
	t *testing.T,
	baseDir string,
	nodeCount int,
	startupAttempts int,
	engineType raftEngineType,
) (string, []bootstrapE2EEndpoint, []*bootstrapE2ENode) {
	t.Helper()

	var (
		lastErr error
		nodes   []*bootstrapE2ENode
	)

	for attempt := range startupAttempts {
		endpoints, listeners := allocateBootstrapE2EEndpoints(t, nodeCount)
		attemptDir := filepath.Join(baseDir, fmt.Sprintf("attempt-%d", attempt))
		started, err := tryStartBootstrapE2ECluster(attemptDir, endpoints, listeners, engineType)
		if err == nil {
			return attemptDir, endpoints, started
		}
		closeBootstrapE2ENodesIgnoreError(started)
		closeBootstrapE2EListeners(listeners)
		lastErr = err
		if !isAddressInUseError(err) {
			break
		}
		nodes = nil
	}

	require.NoError(t, lastErr)
	return "", nil, nodes
}

func allocateBootstrapE2EEndpoints(t *testing.T, nodeCount int) ([]bootstrapE2EEndpoint, []bootstrapE2EListeners) {
	t.Helper()

	var lc net.ListenConfig
	endpoints := make([]bootstrapE2EEndpoint, 0, nodeCount)
	listeners := make([]bootstrapE2EListeners, 0, nodeCount)
	for i := range nodeCount {
		grpcL, err := lc.Listen(context.Background(), "tcp", "127.0.0.1:0")
		require.NoError(t, err)
		redisL, err := lc.Listen(context.Background(), "tcp", "127.0.0.1:0")
		require.NoError(t, err)
		dynamoL, err := lc.Listen(context.Background(), "tcp", "127.0.0.1:0")
		require.NoError(t, err)

		endpoints = append(endpoints, bootstrapE2EEndpoint{
			id:         fmt.Sprintf("n%d", i+1),
			raftAddr:   grpcL.Addr().String(),
			redisAddr:  redisL.Addr().String(),
			dynamoAddr: dynamoL.Addr().String(),
		})
		listeners = append(listeners, bootstrapE2EListeners{
			grpc:   grpcL,
			redis:  redisL,
			dynamo: dynamoL,
		})
	}
	return endpoints, listeners
}

func bindBootstrapE2EEndpointListeners(t *testing.T, ep bootstrapE2EEndpoint) bootstrapE2EListeners {
	t.Helper()

	var lc net.ListenConfig
	grpcL, err := lc.Listen(context.Background(), "tcp", ep.raftAddr)
	require.NoError(t, err)
	redisL, err := lc.Listen(context.Background(), "tcp", ep.redisAddr)
	require.NoError(t, err)
	dynamoL, err := lc.Listen(context.Background(), "tcp", ep.dynamoAddr)
	require.NoError(t, err)
	return bootstrapE2EListeners{
		grpc:   grpcL,
		redis:  redisL,
		dynamo: dynamoL,
	}
}

func tryStartBootstrapE2ECluster(baseDir string, endpoints []bootstrapE2EEndpoint, listeners []bootstrapE2EListeners, engineType raftEngineType) ([]*bootstrapE2ENode, error) {
	bootstrapMembers := bootstrapMembersArg(endpoints)
	nodes := make([]*bootstrapE2ENode, 0, len(endpoints))
	for i := range endpoints {
		node, err := startBootstrapE2ENode(baseDir, endpoints[i], listeners[i], true, bootstrapMembers, engineType)
		if err != nil {
			return nodes, err
		}
		nodes = append(nodes, node)
	}
	return nodes, nil
}

func closeBootstrapE2EListeners(listeners []bootstrapE2EListeners) {
	for _, lis := range listeners {
		if lis.grpc != nil {
			_ = lis.grpc.Close()
		}
		if lis.redis != nil {
			_ = lis.redis.Close()
		}
		if lis.dynamo != nil {
			_ = lis.dynamo.Close()
		}
	}
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
	if errors.Is(err, syscall.EADDRINUSE) {
		return true
	}
	var opErr *net.OpError
	if errors.As(err, &opErr) && errors.Is(opErr.Err, syscall.EADDRINUSE) {
		return true
	}
	var sysErr *os.SyscallError
	if errors.As(err, &sysErr) && errors.Is(sysErr.Err, syscall.EADDRINUSE) {
		return true
	}
	return false
}

func bootstrapMembersArg(endpoints []bootstrapE2EEndpoint) string {
	parts := make([]string, 0, len(endpoints))
	for _, ep := range endpoints {
		parts = append(parts, fmt.Sprintf("%s=%s", ep.id, ep.raftAddr))
	}
	return strings.Join(parts, ",")
}

func bootstrapExpectedConfiguration(endpoints []bootstrapE2EEndpoint) raftengine.Configuration {
	servers := make([]raftengine.Server, 0, len(endpoints))
	for _, ep := range endpoints {
		servers = append(servers, raftengine.Server{
			Suffrage: "voter",
			ID:       ep.id,
			Address:  ep.raftAddr,
		})
	}
	return raftengine.Configuration{Servers: servers}
}

func startBootstrapE2ENode(
	baseDir string,
	ep bootstrapE2EEndpoint,
	listeners bootstrapE2EListeners,
	bootstrap bool,
	bootstrapMembers string,
	engineType raftEngineType,
) (*bootstrapE2ENode, error) {
	cfg, err := parseRuntimeConfig(ep.raftAddr, ep.redisAddr, "", "", "", "", "", "", "")
	if err != nil {
		return nil, err
	}

	bootstrapServers, err := resolveBootstrapServers(ep.id, cfg.groups, bootstrapMembers)
	if err != nil {
		return nil, err
	}
	bootstrap = bootstrap || len(bootstrapServers) > 0

	factory, err := newRaftFactory(engineType)
	if err != nil {
		return nil, err
	}
	clock := kv.NewHLC()
	runtimes, shardGroups, err := buildShardGroups(ep.id, baseDir, cfg.groups, cfg.multi, bootstrap, bootstrapServers, factory, nil, clock)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())
	eg, runCtx := errgroup.WithContext(ctx)
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

	err = startRuntimeServersWithBoundListeners(
		runCtx,
		eg,
		cancel,
		runtimes,
		shardStore,
		coordinate,
		distServer,
		cfg.leaderRedis,
		listeners,
	)
	if err != nil {
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

func startRuntimeServersWithBoundListeners(
	ctx context.Context,
	eg *errgroup.Group,
	cancel context.CancelFunc,
	runtimes []*raftGroupRuntime,
	shardStore *kv.ShardStore,
	coordinate kv.Coordinator,
	distServer *adapter.DistributionServer,
	leaderRedis map[raft.ServerAddress]string,
	listeners bootstrapE2EListeners,
) error {
	if len(runtimes) != 1 {
		return waitErrgroupAfterStartupFailure(cancel, eg, fmt.Errorf("expected exactly one runtime, got %d", len(runtimes)))
	}
	rt := runtimes[0]
	relay := adapter.NewRedisPubSubRelay()
	redisAddr := leaderRedis[raft.ServerAddress(rt.spec.address)]

	if err := startBoundRedisServer(ctx, eg, listeners.redis, shardStore, coordinate, leaderRedis, redisAddr, relay); err != nil {
		return waitErrgroupAfterStartupFailure(cancel, eg, err)
	}
	if err := startBoundGRPCServer(ctx, eg, rt, shardStore, coordinate, distServer, relay, listeners.grpc); err != nil {
		return waitErrgroupAfterStartupFailure(cancel, eg, err)
	}
	if err := startBoundDynamoDBServer(ctx, eg, listeners.dynamo, shardStore, coordinate); err != nil {
		return waitErrgroupAfterStartupFailure(cancel, eg, err)
	}
	return nil
}

func startBoundGRPCServer(
	ctx context.Context,
	eg *errgroup.Group,
	rt *raftGroupRuntime,
	shardStore *kv.ShardStore,
	coordinate kv.Coordinator,
	distServer *adapter.DistributionServer,
	relay *adapter.RedisPubSubRelay,
	listener net.Listener,
) error {
	if rt == nil || rt.engine == nil {
		return fmt.Errorf("raft runtime is not ready")
	}
	if listener == nil {
		return fmt.Errorf("grpc listener is required")
	}

	gs := grpc.NewServer()
	trx := kv.NewTransactionWithProposer(rt.engine)
	grpcSvc := adapter.NewGRPCServer(shardStore, coordinate)
	pb.RegisterRawKVServer(gs, grpcSvc)
	pb.RegisterTransactionalKVServer(gs, grpcSvc)
	pb.RegisterInternalServer(gs, adapter.NewInternalWithEngine(trx, rt.engine, coordinate.Clock(), relay))
	pb.RegisterDistributionServer(gs, distServer)
	rt.registerGRPC(gs)
	internalraftadmin.RegisterOperationalServices(ctx, gs, rt.engine, []string{"RawKV"})
	reflection.Register(gs)

	srv := gs
	lis := listener
	grpcService := grpcSvc
	eg.Go(func() error {
		var closeOnce sync.Once
		closeService := func() {
			closeOnce.Do(func() { _ = grpcService.Close() })
		}
		stop := make(chan struct{})
		go func() {
			select {
			case <-ctx.Done():
				// Force-stop to avoid cleanup hangs when long-lived raft RPC streams
				// keep GracefulStop waiting indefinitely under -race/CI load.
				srv.Stop()
				_ = lis.Close()
				closeService()
			case <-stop:
			}
		}()
		err := srv.Serve(lis)
		close(stop)
		closeService()
		if errors.Is(err, grpc.ErrServerStopped) || errors.Is(err, net.ErrClosed) {
			return nil
		}
		return err
	})
	return nil
}

func startBoundRedisServer(
	ctx context.Context,
	eg *errgroup.Group,
	listener net.Listener,
	shardStore *kv.ShardStore,
	coordinate kv.Coordinator,
	leaderRedis map[raft.ServerAddress]string,
	redisAddr string,
	relay *adapter.RedisPubSubRelay,
) error {
	if listener == nil {
		return fmt.Errorf("redis listener is required")
	}
	redisServer := adapter.NewRedisServer(listener, redisAddr, shardStore, coordinate, leaderRedis, relay)
	eg.Go(func() error {
		defer redisServer.Stop()
		stop := make(chan struct{})
		go func() {
			select {
			case <-ctx.Done():
				redisServer.Stop()
			case <-stop:
			}
		}()
		err := redisServer.Run()
		close(stop)
		if errors.Is(err, net.ErrClosed) {
			return nil
		}
		return err
	})
	return nil
}

func startBoundDynamoDBServer(
	ctx context.Context,
	eg *errgroup.Group,
	listener net.Listener,
	shardStore *kv.ShardStore,
	coordinate kv.Coordinator,
) error {
	if listener == nil {
		return fmt.Errorf("dynamodb listener is required")
	}
	dynamoServer := adapter.NewDynamoDBServer(listener, shardStore, coordinate)
	eg.Go(func() error {
		defer dynamoServer.Stop()
		stop := make(chan struct{})
		go func() {
			select {
			case <-ctx.Done():
				dynamoServer.Stop()
			case <-stop:
			}
		}()
		err := dynamoServer.Run()
		close(stop)
		if errors.Is(err, net.ErrClosed) {
			return nil
		}
		return err
	})
	return nil
}

func closeBootstrapE2ENodes(t *testing.T, nodes []*bootstrapE2ENode) {
	t.Helper()
	for _, n := range nodes {
		require.NoError(t, n.close())
	}
}

func waitForBootstrapClusterConfig(t *testing.T, nodes []*bootstrapE2ENode, expected raftengine.Configuration, waitTimeout, waitInterval time.Duration) {
	t.Helper()

	require.Eventually(t, func() bool {
		for _, n := range nodes {
			engine := n.engine()
			if engine == nil {
				return false
			}
			current, err := engine.Configuration(context.Background())
			if err != nil {
				return false
			}
			if len(current.Servers) != len(expected.Servers) {
				return false
			}
			for _, server := range expected.Servers {
				if !containsConfigServer(current.Servers, server) {
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
			engine := n.engine()
			if engine == nil {
				return false
			}
			if engine.State() == raftengine.StateLeader {
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

func closeGRPCConns(conns []*grpc.ClientConn) {
	for _, conn := range conns {
		_ = conn.Close()
	}
}

func waitForValueOnAllClients(t *testing.T, clients []pb.RawKVClient, key []byte, value []byte, waitTimeout, waitInterval, rpcTimeout time.Duration) {
	t.Helper()
	for i := range clients {
		client := clients[i]
		require.Eventually(t, func() bool {
			resp, err := rawGetWithTimeout(client, key, rpcTimeout)
			if err != nil {
				return false
			}
			return resp.Exists && bytes.Equal(resp.Value, value)
		}, waitTimeout, waitInterval)
	}
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

func containsConfigServer(servers []raftengine.Server, expected raftengine.Server) bool {
	for _, s := range servers {
		if s.ID == expected.ID && s.Address == expected.Address && s.Suffrage == expected.Suffrage {
			return true
		}
	}
	return false
}
