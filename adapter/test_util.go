package adapter

import (
	"context"
	"log"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	transport "github.com/Jille/raft-grpc-transport"
	internalutil "github.com/bootjp/elastickv/internal"
	internalraftadmin "github.com/bootjp/elastickv/internal/raftadmin"
	hashicorpraftengine "github.com/bootjp/elastickv/internal/raftengine/hashicorp"
	"github.com/bootjp/elastickv/kv"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
	"google.golang.org/grpc"
)

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
		if n.raft != nil {
			n.raft.Shutdown()
		}
		if n.tm != nil {
			if err := n.tm.Close(); err != nil {
				log.Printf("transport close: %v", err)
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

	// followers wait longer before starting elections to give the leader time to bootstrap and share config.
	followerElectionTimeout = 10 * time.Second
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
	raft          *raft.Raft
	tm            *transport.Manager
}

func newNode(grpcAddress, raftAddress, redisAddress, dynamoAddress string, r *raft.Raft, tm *transport.Manager, grpcs *grpc.Server, grpcService *GRPCServer, rd *RedisServer, ds *DynamoDBServer, opsCancel context.CancelFunc) Node {
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
		raft:          r,
		tm:            tm,
	}
}

//nolint:unparam
func createNode(t *testing.T, n int) ([]Node, []string, []string) {
	const (
		waitTimeout           = 5 * time.Second
		waitInterval          = 100 * time.Millisecond
		leaderReadinessWindow = 10 * time.Second
	)

	t.Helper()

	ctx := context.Background()

	ports := assignPorts(n)
	nodes, grpcAdders, redisAdders, cfg := setupNodes(t, ctx, n, ports)

	waitForNodeListeners(t, ctx, nodes, waitTimeout, waitInterval)
	waitForConfigReplication(t, cfg, nodes, waitTimeout, waitInterval)
	waitForRaftReadiness(t, nodes, leaderReadinessWindow, waitInterval)

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
		assert.Eventually(t, func() bool {
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

// leaderStabilityWindow is the duration a node must continuously report itself
// as leader (and peers as followers pointing at it) before we treat the cluster
// as ready. This absorbs the "elected then immediately stepped down" race
// observed on slow CI, where quorum-active-timeout briefly flips the leader
// back to follower moments after it wins an election.
const leaderStabilityWindow = 300 * time.Millisecond

// defaultStabilityPoll is the sampling interval used during the stability
// window when the caller-supplied waitInterval is unsuitable (zero or larger
// than the window itself).
const defaultStabilityPoll = 25 * time.Millisecond

// waitForRaftReadiness blocks until the bootstrap leader (nodes[0]) has been
// elected AND has remained leader for leaderStabilityWindow, with all other
// nodes acknowledging it as leader. It relies on raft.LeaderCh() for the
// initial election event rather than polling, and then verifies stability to
// filter out transient leadership flaps during CI congestion.
//
// waitInterval is the polling interval used during the stability window. The
// parameter is preserved for backwards compatibility with existing call sites.
func waitForRaftReadiness(t *testing.T, nodes []Node, waitTimeout, waitInterval time.Duration) {
	t.Helper()

	if len(nodes) == 0 {
		return
	}

	leader := nodes[0]
	followers := nodes[1:]
	expectedLeader := raft.ServerAddress(leader.raftAddress)

	stabilityPoll := waitInterval
	if stabilityPoll <= 0 || stabilityPoll > leaderStabilityWindow {
		stabilityPoll = defaultStabilityPoll
	}

	deadline := time.Now().Add(waitTimeout)
	leaderCh := leader.raft.LeaderCh()

	// Drain any stale notifications so we only observe this run's transitions.
	drainLeaderCh(leaderCh)

	var lastState raft.RaftState
	for time.Now().Before(deadline) {
		if !awaitLeaderElected(t, leader, leaderCh, deadline, waitTimeout) {
			// Residual `false` on the channel; loop and keep waiting.
			continue
		}
		if leaderHoldsStable(leader, followers, expectedLeader, stabilityPoll, &lastState) {
			return
		}
		// Flipped during the window; go back to waiting for another `true`
		// event from LeaderCh.
	}

	t.Fatalf("leader never stabilised within %s (last state: %s)", waitTimeout, lastState)
}

// awaitLeaderElected returns true once `leader` is in raft.Leader state. If
// the node is not yet leader it blocks on leaderCh until a `true` notification
// arrives or the deadline expires (t.Fatalf on timeout / channel close). It
// returns false when a stale `false` is observed, asking the caller to retry.
func awaitLeaderElected(t *testing.T, leader Node, leaderCh <-chan bool, deadline time.Time, waitTimeout time.Duration) bool {
	t.Helper()
	if leader.raft.State() == raft.Leader {
		return true
	}
	remaining := time.Until(deadline)
	if remaining <= 0 {
		t.Fatalf("no leader elected within %s (last state: %s)", waitTimeout, leader.raft.State())
	}
	select {
	case became, ok := <-leaderCh:
		if !ok {
			t.Fatalf("leader channel closed before leadership acquired")
		}
		return became
	case <-time.After(remaining):
		t.Fatalf("no leader elected within %s (last state: %s)", waitTimeout, leader.raft.State())
	}
	return false
}

// drainLeaderCh consumes any buffered notifications on the leader channel so
// that subsequent receives only observe post-drain events.
func drainLeaderCh(ch <-chan bool) {
	for {
		select {
		case <-ch:
		default:
			return
		}
	}
}

// leaderHoldsStable verifies the leader stays in raft.Leader state, and all
// followers stay in raft.Follower pointing at expectedLeader, for the full
// leaderStabilityWindow. Returns true on success, false if anything flips.
func leaderHoldsStable(leader Node, followers []Node, expectedLeader raft.ServerAddress, poll time.Duration, lastState *raft.RaftState) bool {
	stableUntil := time.Now().Add(leaderStabilityWindow)
	for time.Now().Before(stableUntil) {
		state := leader.raft.State()
		*lastState = state
		if state != raft.Leader {
			return false
		}
		for _, f := range followers {
			if f.raft.State() != raft.Follower {
				return false
			}
			addr, _ := f.raft.LeaderWithID()
			if addr != expectedLeader {
				return false
			}
		}
		time.Sleep(poll)
	}
	return true
}

func waitForConfigReplication(t *testing.T, cfg raft.Configuration, nodes []Node, waitTimeout, waitInterval time.Duration) {
	t.Helper()

	assert.Eventually(t, func() bool {
		for _, n := range nodes {
			future := n.raft.GetConfiguration()
			if future.Error() != nil {
				return false
			}

			current := future.Configuration().Servers
			if len(current) != len(cfg.Servers) {
				return false
			}

			for _, expected := range cfg.Servers {
				if !containsServer(current, expected) {
					return false
				}
			}
		}
		return true
	}, waitTimeout, waitInterval)
}

func containsServer(servers []raft.Server, expected raft.Server) bool {
	for _, s := range servers {
		if s.ID == expected.ID && s.Address == expected.Address && s.Suffrage == expected.Suffrage {
			return true
		}
	}
	return false
}

func assignPorts(n int) []portsAdress {
	ports := make([]portsAdress, n)
	for i := range n {
		ports[i] = portAssigner()
	}
	return ports
}

func buildRaftConfig(n int, ports []portsAdress) raft.Configuration {
	cfg := raft.Configuration{}
	for i := range n {
		suffrage := raft.Nonvoter
		if i == 0 {
			suffrage = raft.Voter
		}

		cfg.Servers = append(cfg.Servers, raft.Server{
			Suffrage: suffrage,
			ID:       raft.ServerID(strconv.Itoa(i)),
			Address:  raft.ServerAddress(ports[i].raftAddress),
		})
	}

	return cfg
}

const leaderElectionTimeout = 0 * time.Second

func setupNodes(t *testing.T, ctx context.Context, n int, ports []portsAdress) ([]Node, []string, []string, raft.Configuration) {
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

	cfg := buildRaftConfig(n, ports)
	leaderRedisMap := make(map[raft.ServerAddress]string, len(ports))
	for _, p := range ports {
		leaderRedisMap[raft.ServerAddress(p.raftAddress)] = p.redisAddress
	}

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

		// リーダーが先に投票を開始させる
		electionTimeout := leaderElectionTimeout
		if i != 0 {
			electionTimeout = followerElectionTimeout
		}

		r, tm, err := newRaft(strconv.Itoa(i), port.raftAddress, fsm, i == 0, cfg, electionTimeout)
		assert.NoError(t, err)

		s := grpc.NewServer(internalutil.GRPCServerOptions()...)
		trx := kv.NewTransaction(r)
		coordinator := kv.NewCoordinator(trx, r, kv.WithHLC(hlc))
		relay := NewRedisPubSubRelay()
		routedStore := kv.NewLeaderRoutedStore(st, coordinator)
		gs := NewGRPCServer(routedStore, coordinator, WithCloseStore())
		opsCtx, opsCancel := context.WithCancel(ctx)
		go coordinator.RunHLCLeaseRenewal(opsCtx)
		tm.Register(s)
		pb.RegisterRawKVServer(s, gs)
		pb.RegisterTransactionalKVServer(s, gs)
		pb.RegisterInternalServer(s, NewInternal(trx, r, coordinator.Clock(), relay))
		internalraftadmin.RegisterOperationalServices(opsCtx, s, hashicorpraftengine.New(r), []string{"Example"})

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
			r,
			tm,
			s,
			gs,
			rd,
			ds,
			opsCancel,
		))
	}

	return nodes, grpcAdders, redisAdders, cfg
}

func newRaft(myID string, myAddress string, fsm raft.FSM, bootstrap bool, cfg raft.Configuration, electionTimeout time.Duration) (*raft.Raft, *transport.Manager, error) {
	c := raft.DefaultConfig()
	c.LocalID = raft.ServerID(myID)
	c.CommitTimeout = 1 * time.Millisecond

	if electionTimeout > 0 {
		c.ElectionTimeout = electionTimeout
	}

	// this config is for development
	ldb := raft.NewInmemStore()
	sdb := raft.NewInmemStore()
	fss := raft.NewInmemSnapshotStore()

	c.Logger = hclog.New(&hclog.LoggerOptions{
		Name:  "raft-" + myID,
		Level: hclog.LevelFromString("WARN"),
	})

	tm := transport.New(raft.ServerAddress(myAddress), internalutil.GRPCDialOptions())

	r, err := raft.NewRaft(c, fsm, ldb, sdb, fss, tm.Transport())
	if err != nil {
		return nil, nil, errors.WithStack(err)
	}

	if bootstrap {
		f := r.BootstrapCluster(cfg)
		if err := f.Error(); err != nil {
			return nil, nil, errors.WithStack(err)
		}
	}

	return r, tm, nil
}
