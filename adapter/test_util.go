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
	testEngineTickInterval = 10 * time.Millisecond
	// testEngineHeartbeatTick / testEngineElectionTick preserve etcd/raft's
	// recommended 10x ratio (ElectionTick = 10 × HeartbeatTick). Previous
	// values (1 / 10 → 100 ms election timeout) were too tight for `-race`
	// on CI: CheckQuorum only holds ~100 ms of heartbeat-response history,
	// so any scheduler pause on a loaded runner drops the leader's view of
	// quorum, and it steps down with "quorum is not active", bouncing
	// writes to "etcd raft engine is not leader" / "leader not found" in
	// the middle of tests like Test_consistency_satisfy_write_after_read_sequence.
	// 5 / 50 gives a 500 ms election timeout (500-1000 ms randomised) and
	// a 50 ms heartbeat, absorbing goroutine-scheduler jitter while still
	// keeping tests fast. Combined with leaseSafetyMargin = 300 ms, this
	// also yields a non-zero 200 ms LeaseDuration so the lease-read fast
	// path gets exercised instead of always falling through to ReadIndex.
	testEngineHeartbeatTick = 5
	testEngineElectionTick  = 50
	testEngineMaxSizePerMsg = 1 << 20
	testEngineMaxInflight   = 256

	// leaderChurnRetryTimeout bounds how long doEventually keeps retrying a
	// write that fails with a transient leader-unavailable error. It covers
	// both startup churn right after createNode returns and mid-test churn
	// when the leader briefly steps down under CI load.
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
		shutdownNode(n)
	}
}

func shutdownNode(n Node) {
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
	if n.sqsServer != nil {
		n.sqsServer.Stop()
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

type portsAdress struct {
	grpc          int
	raft          int
	redis         int
	dynamo        int
	sqs           int
	grpcAddress   string
	raftAddress   string
	redisAddress  string
	dynamoAddress string
	sqsAddress    string
}

const (
	// raft and the grpc requested by the client use grpc and are received on the same port
	grpcPort   = 50000
	raftPort   = 50000
	redisPort  = 63790
	dynamoPort = 28000
	sqsPort    = 29000
)

var mu sync.Mutex
var portGrpc atomic.Int32
var portRaft atomic.Int32
var portRedis atomic.Int32
var portDynamo atomic.Int32
var portSQS atomic.Int32

func init() {
	portGrpc.Store(raftPort)
	portRaft.Store(grpcPort)
	portRedis.Store(redisPort)
	portDynamo.Store(dynamoPort)
	portSQS.Store(sqsPort)
}

func portAssigner() portsAdress {
	mu.Lock()
	defer mu.Unlock()
	gp := portGrpc.Add(1)
	rp := portRaft.Add(1)
	rd := portRedis.Add(1)
	dn := portDynamo.Add(1)
	sq := portSQS.Add(1)
	return portsAdress{
		grpc:          int(gp),
		raft:          int(rp),
		redis:         int(rd),
		dynamo:        int(dn),
		sqs:           int(sq),
		grpcAddress:   net.JoinHostPort("localhost", strconv.Itoa(int(gp))),
		raftAddress:   net.JoinHostPort("localhost", strconv.Itoa(int(rp))),
		redisAddress:  net.JoinHostPort("localhost", strconv.Itoa(int(rd))),
		dynamoAddress: net.JoinHostPort("localhost", strconv.Itoa(int(dn))),
		sqsAddress:    net.JoinHostPort("localhost", strconv.Itoa(int(sq))),
	}
}

type Node struct {
	grpcAddress   string
	raftAddress   string
	redisAddress  string
	dynamoAddress string
	sqsAddress    string
	grpcServer    *grpc.Server
	grpcService   *GRPCServer
	redisServer   *RedisServer
	dynamoServer  *DynamoDBServer
	sqsServer     *SQSServer
	opsCancel     context.CancelFunc
	engine        raftengine.Engine
	closeFactory  func() error
}

func newNode(grpcAddress, raftAddress, redisAddress, dynamoAddress, sqsAddress string, engine raftengine.Engine, closeFactory func() error, grpcs *grpc.Server, grpcService *GRPCServer, rd *RedisServer, ds *DynamoDBServer, sq *SQSServer, opsCancel context.CancelFunc) Node {
	return Node{
		grpcAddress:   grpcAddress,
		raftAddress:   raftAddress,
		redisAddress:  redisAddress,
		dynamoAddress: dynamoAddress,
		sqsAddress:    sqsAddress,
		grpcServer:    grpcs,
		grpcService:   grpcService,
		redisServer:   rd,
		dynamoServer:  ds,
		sqsServer:     sq,
		opsCancel:     opsCancel,
		engine:        engine,
		closeFactory:  closeFactory,
	}
}

//nolint:unparam
func createNode(t *testing.T, n int) ([]Node, []string, []string) {
	const (
		// listenerWaitTimeout bounds the per-socket Dial loop; a few seconds
		// is plenty since the listeners are already Listen()ing by the time
		// we poll.
		listenerWaitTimeout = 5 * time.Second
		waitInterval        = 100 * time.Millisecond
		// raftReadyTimeout is the overall budget for leader election +
		// 300ms stability window. 10s gives room for re-elections on
		// slow CI runners under -race.
		raftReadyTimeout = 10 * time.Second
	)

	t.Helper()

	ctx := context.Background()

	ports := assignPorts(n)
	nodes, grpcAdders, redisAdders, peers := setupNodes(t, ctx, n, ports)

	waitForNodeListeners(t, ctx, nodes, listenerWaitTimeout, waitInterval)
	waitForRaftReadiness(t, nodes, peers, raftReadyTimeout, waitInterval)

	return nodes, grpcAdders, redisAdders
}

type listeners struct {
	grpc   net.Listener
	redis  net.Listener
	dynamo net.Listener
	sqs    net.Listener
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

	sqsSock, err := lc.Listen(ctx, "tcp", port.sqsAddress)
	if err != nil {
		_ = grpcSock.Close()
		_ = redisSock.Close()
		_ = dynamoSock.Close()
		if errors.Is(err, unix.EADDRINUSE) {
			return port, listeners{}, true, nil
		}
		return port, listeners{}, false, errors.WithStack(err)
	}

	return port, listeners{
		grpc:   grpcSock,
		redis:  redisSock,
		dynamo: dynamoSock,
		sqs:    sqsSock,
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

	// Require the leader to remain stable for a short window before we
	// declare the cluster ready. etcd/raft can briefly publish a leader
	// and then step down if the freshly-elected leader fails its first
	// heartbeat quorum check — callers that issue writes immediately
	// after this helper returns would race into that window and see
	// transient "not leader" errors. A stability window catches the
	// flip and loops until the leader actually holds.
	waitForStableLeader(t, nodes, peers, waitTimeout)
}

// waitForStableLeader polls the cluster until nodes[0] has been the leader
// (and all other nodes agree on it as the leader address) for a continuous
// stability window. If leadership flips during the window, the loop restarts.
//
// Design notes:
//   - The etcd/raft engine exposes no "leader gained" event channel;
//     raftengine.LeaseProvider offers only RegisterLeaderLossCallback
//     (leader -> non-leader), which is the wrong direction for a readiness
//     check. So this helper uses pure polling: a tight inner sample loop
//     checks that every node reports leader == peers[0].Address for 12
//     consecutive samples (25ms × 12 = 300ms). Any miss restarts the
//     outer loop.
//   - The overall deadline is the caller-supplied waitTimeout (10s at the
//     current createNode call site), which bounds how long we re-try
//     re-elections.
func waitForStableLeader(t *testing.T, nodes []Node, peers []raftengine.Server, timeout time.Duration) {
	t.Helper()

	const (
		stabilityWindow = 300 * time.Millisecond
		pollInterval    = 25 * time.Millisecond
	)

	if len(nodes) == 0 || len(peers) == 0 {
		return
	}
	targetAddr := peers[0].Address

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if !leaderLooksReady(nodes, targetAddr) {
			time.Sleep(pollInterval)
			continue
		}
		if leaderStableFor(nodes, targetAddr, stabilityWindow, pollInterval) {
			return
		}
		// Leader flipped during the window — fall through and retry.
	}
	t.Fatalf("leader never stabilised on %s within %s", targetAddr, timeout)
}

// leaderLooksReady returns true when nodes[0] reports itself as leader and
// every node's Leader().Address matches the expected target. This is the
// single-sample precondition used by waitForStableLeader; the stability
// window then requires this to stay true for N consecutive samples.
func leaderLooksReady(nodes []Node, targetAddr string) bool {
	if nodes[0].engine.State() != raftengine.StateLeader {
		return false
	}
	for _, n := range nodes {
		if n.engine.Leader().Address != targetAddr {
			return false
		}
	}
	return true
}

// leaderStableFor samples leaderLooksReady every pollInterval for the full
// window and returns true iff every sample reports ready. A single negative
// sample returns false immediately so the outer loop can re-check.
func leaderStableFor(nodes []Node, targetAddr string, window, pollInterval time.Duration) bool {
	end := time.Now().Add(window)
	for {
		if !leaderLooksReady(nodes, targetAddr) {
			return false
		}
		if !time.Now().Before(end) {
			return true
		}
		time.Sleep(pollInterval)
	}
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
		sqsSock := lis[i].sqs

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

		sq := NewSQSServer(sqsSock, routedStore, coordinator)
		go func() {
			assert.NoError(t, sq.Run())
		}()

		nodes = append(nodes, newNode(
			port.grpcAddress,
			port.raftAddress,
			port.redisAddress,
			port.dynamoAddress,
			port.sqsAddress,
			result.Engine,
			result.Close,
			s,
			gs,
			rd,
			ds,
			sq,
			opsCancel,
		))
	}

	return nodes, grpcAdders, redisAdders, peers
}

// isTransientNotLeaderErr reports whether err is a transient
// leader-unavailable error that can happen right after createNode returns
// (freshly-elected leader briefly steps down due to a missed heartbeat
// quorum) or in the middle of a long-running test when CI load causes the
// leader to miss quorum momentarily.
//
// Both "not leader" (ErrNotLeader, etcd/raft step errors) and
// "leader not found" (ErrLeaderNotFound, emitted while the cluster is
// re-electing) are treated as retryable. The match is case-insensitive
// because Redis protocol error bodies and other layers may capitalise the
// phrase differently (e.g. "ERR Not Leader").
func isTransientNotLeaderErr(err error) bool {
	if err == nil {
		return false
	}
	s := strings.ToLower(err.Error())
	return strings.Contains(s, "not leader") || strings.Contains(s, "leader not found")
}

// doEventually retries do() while it returns a transient "not leader" error,
// giving the cluster a few seconds to re-settle leadership after startup.
// Non-"not leader" errors fail the test immediately.
//
// MUST be called from the main test goroutine only. The final require.NoError
// calls t.FailNow() on failure; invoking FailNow from a worker goroutine is
// a testing.T contract violation. For parallel-worker use, call
// retryNotLeader directly and report errors back via a channel.
func doEventually(t *testing.T, do func() error) {
	t.Helper()
	require.NoError(t, retryNotLeader(context.Background(), do))
}

// retryNotLeader calls do() repeatedly while it returns a transient
// leader-unavailable error, capped at leaderChurnRetryTimeout. It returns
// the final error (or nil on success) without touching testing.T, making
// it safe to call from worker goroutines in parallel tests.
func retryNotLeader(ctx context.Context, do func() error) error {
	deadline := time.Now().Add(leaderChurnRetryTimeout)
	var lastErr error
	for {
		lastErr = do()
		if lastErr == nil || !isTransientNotLeaderErr(lastErr) {
			return lastErr
		}
		if !time.Now().Before(deadline) {
			return lastErr
		}
		select {
		case <-ctx.Done():
			return lastErr
		case <-time.After(leaderChurnRetryInterval):
		}
	}
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

// rawPutEventually wraps RawKV.RawPut in doEventually so transient leader
// churn (either at startup or in the middle of a long-running loop) does
// not fail the test with "not leader" / "leader not found".
func rawPutEventually(t *testing.T, ctx context.Context, c pb.RawKVClient, req *pb.RawPutRequest) {
	t.Helper()
	doEventually(t, func() error {
		_, err := c.RawPut(ctx, req)
		return err
	})
}

// rawGetEventually wraps RawKV.RawGet in doEventually and returns the
// response only after a successful (non-"not leader") call.
func rawGetEventually(t *testing.T, ctx context.Context, c pb.RawKVClient, req *pb.RawGetRequest) *pb.RawGetResponse {
	t.Helper()
	var resp *pb.RawGetResponse
	doEventually(t, func() error {
		r, err := c.RawGet(ctx, req)
		if err != nil {
			return err
		}
		resp = r
		return nil
	})
	return resp
}

// txnPutEventually wraps TransactionalKV.Put in doEventually.
func txnPutEventually(t *testing.T, ctx context.Context, c pb.TransactionalKVClient, req *pb.PutRequest) {
	t.Helper()
	doEventually(t, func() error {
		_, err := c.Put(ctx, req)
		return err
	})
}

// txnGetEventually wraps TransactionalKV.Get in doEventually and returns the
// response only after a successful (non-"not leader") call.
func txnGetEventually(t *testing.T, ctx context.Context, c pb.TransactionalKVClient, req *pb.GetRequest) *pb.GetResponse {
	t.Helper()
	var resp *pb.GetResponse
	doEventually(t, func() error {
		r, err := c.Get(ctx, req)
		if err != nil {
			return err
		}
		resp = r
		return nil
	})
	return resp
}

// txnDeleteEventually wraps TransactionalKV.Delete in doEventually.
func txnDeleteEventually(t *testing.T, ctx context.Context, c pb.TransactionalKVClient, req *pb.DeleteRequest) {
	t.Helper()
	doEventually(t, func() error {
		_, err := c.Delete(ctx, req)
		return err
	})
}
