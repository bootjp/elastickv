package adapter

import (
	"context"
	"log"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	internalutil "github.com/bootjp/elastickv/internal"
	internalraftadmin "github.com/bootjp/elastickv/internal/raftadmin"
	"github.com/bootjp/elastickv/internal/raftengine"
	etcdraftengine "github.com/bootjp/elastickv/internal/raftengine/etcd"
	"github.com/bootjp/elastickv/kv"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
	"google.golang.org/grpc"
)

const (
	testEngineTickInterval  = 10 * time.Millisecond
	testEngineHeartbeatTick = 1
	testEngineElectionTick  = 10
	testEngineMaxSizePerMsg = 1 << 20
	testEngineMaxInflight   = 256

	// leaderChurnRetryTimeout bounds how long doEventually keeps retrying a
	// write that fails with "not leader" right after createNode returns.
	leaderChurnRetryTimeout = 5 * time.Second
	// leaderChurnRetryInterval is the poll interval between retries.
	leaderChurnRetryInterval = 50 * time.Millisecond
)

func newTestFactory() raftengine.Factory {
	return etcdraftengine.NewFactory(etcdraftengine.FactoryConfig{
		TickInterval:   testEngineTickInterval,
		HeartbeatTick:  testEngineHeartbeatTick,
		ElectionTick:   testEngineElectionTick,
		MaxSizePerMsg:  testEngineMaxSizePerMsg,
		MaxInflightMsg: testEngineMaxInflight,
	})
}

func shutdown(nodes []Node) {
	for _, n := range nodes {
		if n.opsCancel != nil {
			n.opsCancel()
		}
		n.grpcServer.Stop()
		if n.grpcService != nil {
			if err := n.grpcService.Close(); err != nil {
				log.Printf("grpc service close: %v", err)
			}
		}
		n.redisServer.Stop()
		if n.dynamoServer != nil {
			n.dynamoServer.Stop()
		}
		if n.engine != nil {
			if err := n.engine.Close(); err != nil {
				log.Printf("engine close: %v", err)
			}
		}
		if n.closeFactory != nil {
			if err := n.closeFactory(); err != nil {
				log.Printf("factory close: %v", err)
			}
		}
	}
}

type portsAdress struct {
	grpc          int
	raft          int
	redis         int
	dynamo        int
	grpcAddress   string
	raftAddress   string
	redisAddress  string
	dynamoAddress string
}

const (
	// raft and the grpc requested by the client use grpc and are received on the same port
	grpcPort   = 50000
	raftPort   = 50000
	redisPort  = 63790
	dynamoPort = 28000
)

var mu sync.Mutex
var portGrpc atomic.Int32
var portRaft atomic.Int32
var portRedis atomic.Int32
var portDynamo atomic.Int32

func init() {
	portGrpc.Store(raftPort)
	portRaft.Store(grpcPort)
	portRedis.Store(redisPort)
	portDynamo.Store(dynamoPort)
}

func portAssigner() portsAdress {
	mu.Lock()
	defer mu.Unlock()
	gp := portGrpc.Add(1)
	rp := portRaft.Add(1)
	rd := portRedis.Add(1)
	dn := portDynamo.Add(1)
	return portsAdress{
		grpc:          int(gp),
		raft:          int(rp),
		redis:         int(rd),
		dynamo:        int(dn),
		grpcAddress:   net.JoinHostPort("localhost", strconv.Itoa(int(gp))),
		raftAddress:   net.JoinHostPort("localhost", strconv.Itoa(int(rp))),
		redisAddress:  net.JoinHostPort("localhost", strconv.Itoa(int(rd))),
		dynamoAddress: net.JoinHostPort("localhost", strconv.Itoa(int(dn))),
	}
}

type Node struct {
	grpcAddress   string
	raftAddress   string
	redisAddress  string
	dynamoAddress string
	grpcServer    *grpc.Server
	grpcService   *GRPCServer
	redisServer   *RedisServer
	dynamoServer  *DynamoDBServer
	opsCancel     context.CancelFunc
	engine        raftengine.Engine
	closeFactory  func() error
}

func newNode(grpcAddress, raftAddress, redisAddress, dynamoAddress string, engine raftengine.Engine, closeFactory func() error, grpcs *grpc.Server, grpcService *GRPCServer, rd *RedisServer, ds *DynamoDBServer, opsCancel context.CancelFunc) Node {
	return Node{
		grpcAddress:   grpcAddress,
		raftAddress:   raftAddress,
		redisAddress:  redisAddress,
		dynamoAddress: dynamoAddress,
		grpcServer:    grpcs,
		grpcService:   grpcService,
		redisServer:   rd,
		dynamoServer:  ds,
		opsCancel:     opsCancel,
		engine:        engine,
		closeFactory:  closeFactory,
	}
}

//nolint:unparam
func createNode(t *testing.T, n int) ([]Node, []string, []string) {
	const (
		waitTimeout  = 5 * time.Second
		waitInterval = 100 * time.Millisecond
	)

	t.Helper()

	ctx := context.Background()

	ports := assignPorts(n)
	nodes, grpcAdders, redisAdders, peers := setupNodes(t, ctx, n, ports)

	waitForNodeListeners(t, ctx, nodes, waitTimeout, waitInterval)
	waitForRaftReadiness(t, nodes, peers, waitTimeout, waitInterval)

	return nodes, grpcAdders, redisAdders
}

type listeners struct {
	grpc   net.Listener
	redis  net.Listener
	dynamo net.Listener
}

func bindListeners(ctx context.Context, lc *net.ListenConfig, port portsAdress) (portsAdress, listeners, bool, error) {
	grpcSock, err := lc.Listen(ctx, "tcp", port.grpcAddress)
	if err != nil {
		if errors.Is(err, unix.EADDRINUSE) {
			return port, listeners{}, true, nil
		}
		return port, listeners{}, false, errors.WithStack(err)
	}

	redisSock, err := lc.Listen(ctx, "tcp", port.redisAddress)
	if err != nil {
		_ = grpcSock.Close()
		if errors.Is(err, unix.EADDRINUSE) {
			return port, listeners{}, true, nil
		}
		return port, listeners{}, false, errors.WithStack(err)
	}

	dynamoSock, err := lc.Listen(ctx, "tcp", port.dynamoAddress)
	if err != nil {
		_ = grpcSock.Close()
		_ = redisSock.Close()
		if errors.Is(err, unix.EADDRINUSE) {
			return port, listeners{}, true, nil
		}
		return port, listeners{}, false, errors.WithStack(err)
	}

	return port, listeners{
		grpc:   grpcSock,
		redis:  redisSock,
		dynamo: dynamoSock,
	}, false, nil
}

func waitForNodeListeners(t *testing.T, ctx context.Context, nodes []Node, waitTimeout, waitInterval time.Duration) {
	t.Helper()
	d := &net.Dialer{Timeout: time.Second}
	for _, n := range nodes {
		require.Eventually(t, func() bool {
			conn, err := d.DialContext(ctx, "tcp", n.grpcAddress)
			if err != nil {
				return false
			}
			_ = conn.Close()
			conn, err = d.DialContext(ctx, "tcp", n.redisAddress)
			if err != nil {
				return false
			}
			_ = conn.Close()
			return true
		}, waitTimeout, waitInterval)
	}
}

func waitForRaftReadiness(t *testing.T, nodes []Node, peers []raftengine.Server, waitTimeout, waitInterval time.Duration) {
	t.Helper()

	// Existing tests assume hosts[0] (node 0) is the cluster leader — the
	// previous hashicorp setup ensured this by giving node 0 an immediate
	// election timeout while others waited 10s. etcd/raft elections are
	// randomised, so whoever wins the first election is effectively random.
	// Nudge leadership onto node 0 if a different node won.
	ensureNodeZeroIsLeader(t, nodes, peers, waitTimeout, waitInterval)

	require.Eventually(t, func() bool {
		var leaderAddr string
		for _, n := range nodes {
			leader := n.engine.Leader().Address
			if leader == "" {
				return false
			}
			if leaderAddr == "" {
				leaderAddr = leader
			} else if leader != leaderAddr {
				return false
			}
		}
		// Confirm the leader address belongs to the configured peers.
		for _, p := range peers {
			if p.Address == leaderAddr {
				return true
			}
		}
		return false
	}, waitTimeout, waitInterval)
}

// ensureNodeZeroIsLeader waits for any node to become leader, then (if
// necessary) triggers a leadership transfer to node 0 so downstream
// tests that assume hosts[0] is authoritative keep working.
func ensureNodeZeroIsLeader(t *testing.T, nodes []Node, peers []raftengine.Server, waitTimeout, waitInterval time.Duration) {
	t.Helper()

	if len(nodes) == 0 || len(peers) == 0 {
		return
	}
	targetAddr := peers[0].Address

	// Step 1: wait until some node is leader so we know the cluster is live.
	require.Eventually(t, func() bool {
		for _, n := range nodes {
			if n.engine.State() == raftengine.StateLeader {
				return true
			}
		}
		return false
	}, waitTimeout, waitInterval, "no node became leader")

	// Step 2: if node 0 isn't already leader, ask the current leader to
	// transfer leadership to it. This is best-effort — a transfer can
	// race with another election, in which case the Eventually below
	// will retry.
	require.Eventually(t, func() bool {
		if nodes[0].engine.State() == raftengine.StateLeader {
			return true
		}
		for _, n := range nodes {
			if n.engine.State() != raftengine.StateLeader {
				continue
			}
			admin, ok := n.engine.(raftengine.Admin)
			if !ok {
				return false
			}
			transferCtx, cancel := context.WithTimeout(context.Background(), waitInterval)
			_ = admin.TransferLeadershipToServer(transferCtx, peers[0].ID, targetAddr)
			cancel()
			break
		}
		return false
	}, waitTimeout, waitInterval, "node 0 did not become leader")
}

func assignPorts(n int) []portsAdress {
	ports := make([]portsAdress, n)
	for i := range n {
		ports[i] = portAssigner()
	}
	return ports
}

func buildTestPeers(n int, ports []portsAdress) []raftengine.Server {
	peers := make([]raftengine.Server, 0, n)
	for i := range n {
		peers = append(peers, raftengine.Server{
			Suffrage: "voter",
			ID:       strconv.Itoa(i),
			Address:  ports[i].raftAddress,
		})
	}
	return peers
}

func setupNodes(t *testing.T, ctx context.Context, n int, ports []portsAdress) ([]Node, []string, []string, []raftengine.Server) {
	t.Helper()
	var grpcAdders []string
	var redisAdders []string
	var nodes []Node
	lc := net.ListenConfig{}
	lis := make([]listeners, n)
	for i := range n {
		var (
			bound portsAdress
			l     listeners
			retry bool
			err   error
		)
		for {
			bound, l, retry, err = bindListeners(ctx, &lc, ports[i])
			require.NoError(t, err)
			if retry {
				ports[i] = portAssigner()
				continue
			}
			ports[i] = bound
			lis[i] = l
			break
		}
	}

	peers := buildTestPeers(n, ports)
	leaderRedisMap := make(map[string]string, len(ports))
	for _, p := range ports {
		leaderRedisMap[p.raftAddress] = p.redisAddress
	}

	factory := newTestFactory()

	for i := range n {
		st := store.NewMVCCStore()
		// Share a single HLC between the FSM and the coordinator so the FSM can
		// advance physicalCeiling (via HLC lease Raft entries) and the coordinator
		// reads it inside Next() to floor timestamps above the previous leader's window.
		hlc := kv.NewHLC()
		fsm := kv.NewKvFSMWithHLC(st, hlc)

		port := ports[i]
		grpcSock := lis[i].grpc
		redisSock := lis[i].redis
		dynamoSock := lis[i].dynamo

		result, err := factory.Create(raftengine.FactoryConfig{
			LocalID:      strconv.Itoa(i),
			LocalAddress: port.raftAddress,
			DataDir:      t.TempDir(),
			Peers:        peers,
			Bootstrap:    true,
			StateMachine: fsm,
		})
		require.NoError(t, err)

		s := grpc.NewServer(internalutil.GRPCServerOptions()...)
		trx := kv.NewTransactionWithProposer(result.Engine)
		coordinator := kv.NewCoordinatorWithEngine(trx, result.Engine, kv.WithHLC(hlc))
		relay := NewRedisPubSubRelay()
		routedStore := kv.NewLeaderRoutedStore(st, coordinator)
		gs := NewGRPCServer(routedStore, coordinator, WithCloseStore())
		opsCtx, opsCancel := context.WithCancel(ctx)
		go coordinator.RunHLCLeaseRenewal(opsCtx)
		if result.RegisterTransport != nil {
			result.RegisterTransport(s)
		}
		pb.RegisterRawKVServer(s, gs)
		pb.RegisterTransactionalKVServer(s, gs)
		pb.RegisterInternalServer(s, NewInternalWithEngine(trx, result.Engine, coordinator.Clock(), relay))
		internalraftadmin.RegisterOperationalServices(opsCtx, s, result.Engine, []string{"Example"})

		grpcAdders = append(grpcAdders, port.grpcAddress)
		redisAdders = append(redisAdders, port.redisAddress)
		go func(srv *grpc.Server, lis net.Listener) {
			assert.NoError(t, srv.Serve(lis))
		}(s, grpcSock)

		dc := NewDeltaCompactor(st, coordinator)
		go func() { _ = dc.Run(opsCtx) }()
		rd := NewRedisServer(redisSock, port.redisAddress, routedStore, coordinator, leaderRedisMap, relay,
			WithRedisCompactor(dc),
		)
		go func(server *RedisServer) {
			assert.NoError(t, server.Run())
		}(rd)

		ds := NewDynamoDBServer(dynamoSock, routedStore, coordinator)
		go func() {
			assert.NoError(t, ds.Run())
		}()

		nodes = append(nodes, newNode(
			port.grpcAddress,
			port.raftAddress,
			port.redisAddress,
			port.dynamoAddress,
			result.Engine,
			result.Close,
			s,
			gs,
			rd,
			ds,
			opsCancel,
		))
	}

	return nodes, grpcAdders, redisAdders, peers
}

// isTransientNotLeaderErr reports whether err is a transient "not leader"
// error that can happen right after createNode returns if the newly elected
// leader briefly steps down due to a missed heartbeat quorum (common on slow
// CI runners under -race). Callers should retry the write in that case.
func isTransientNotLeaderErr(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(err.Error(), "not leader")
}

// doEventually retries do() while it returns a transient "not leader" error,
// giving the cluster a few seconds to re-settle leadership after startup.
// Non-"not leader" errors fail the test immediately.
func doEventually(t *testing.T, do func() error) {
	t.Helper()
	require.Eventually(t, func() bool {
		err := do()
		if err == nil {
			return true
		}
		if isTransientNotLeaderErr(err) {
			return false
		}
		require.NoError(t, err)
		return true
	}, leaderChurnRetryTimeout, leaderChurnRetryInterval)
}

// rpushEventually wraps RPUSH in doEventually so transient leader churn
// immediately after createNode doesn't fail the test.
func rpushEventually(t *testing.T, ctx context.Context, rdb *redis.Client, key string, vals ...any) {
	t.Helper()
	doEventually(t, func() error {
		return rdb.RPush(ctx, key, vals...).Err()
	})
}

// lpushEventually wraps LPUSH in doEventually so transient leader churn
// immediately after createNode doesn't fail the test.
func lpushEventually(t *testing.T, ctx context.Context, rdb *redis.Client, key string, vals ...any) {
	t.Helper()
	doEventually(t, func() error {
		return rdb.LPush(ctx, key, vals...).Err()
	})
}
