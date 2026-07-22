package proxy

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseRaftLeaderRedis(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		{
			name: "CRLF-separated INFO reply",
			in:   "# Replication\r\nrole:slave\r\nraft_leader_redis:10.0.0.2:6379\r\n",
			want: "10.0.0.2:6379",
		},
		{
			name: "LF-only INFO reply",
			in:   "# Replication\nrole:master\nraft_leader_redis:127.0.0.1:6379\n",
			want: "127.0.0.1:6379",
		},
		{
			name: "field missing returns empty",
			in:   "# Server\r\nrole:master\r\n",
			want: "",
		},
		{
			name: "trims whitespace around the address",
			in:   "raft_leader_redis: 10.0.0.9:6379 \r\n",
			want: "10.0.0.9:6379",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, parseRaftLeaderRedis(tc.in))
		})
	}
}

func TestNormalizeSeedsDedupesAndTrims(t *testing.T) {
	got := normalizeSeeds([]string{" a:1 ", "b:2", "a:1", "", "c:3 "})
	assert.Equal(t, []string{"a:1", "b:2", "c:3"}, got)
}

// fakeElasticKVNode is a minimal RESP server that serves INFO replication and
// records how many non-INFO commands it received, so tests can assert which
// node handled a proxied command.
type fakeElasticKVNode struct {
	addr       string
	ln         net.Listener
	leaderAddr atomic.Pointer[string]
	commands   atomic.Int64
	infoCalls  atomic.Int64
	scriptErr  atomic.Bool
	infoGateMu sync.RWMutex
	infoGate   <-chan struct{}
}

func newFakeElasticKVNode(t *testing.T) *fakeElasticKVNode {
	t.Helper()
	var lc net.ListenConfig
	ln, err := lc.Listen(context.Background(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	n := &fakeElasticKVNode{ln: ln, addr: ln.Addr().String()}
	go n.serve()
	t.Cleanup(func() { _ = ln.Close() })
	return n
}

func (n *fakeElasticKVNode) SetLeader(addr string) {
	v := addr
	n.leaderAddr.Store(&v)
}

func (n *fakeElasticKVNode) Leader() string {
	if p := n.leaderAddr.Load(); p != nil {
		return *p
	}
	return ""
}

func (n *fakeElasticKVNode) SetScriptNotLeaderError(enabled bool) {
	n.scriptErr.Store(enabled)
}

func (n *fakeElasticKVNode) SetInfoGate(gate <-chan struct{}) {
	n.infoGateMu.Lock()
	n.infoGate = gate
	n.infoGateMu.Unlock()
}

func (n *fakeElasticKVNode) waitInfoGate() {
	n.infoGateMu.RLock()
	gate := n.infoGate
	n.infoGateMu.RUnlock()
	if gate != nil {
		<-gate
	}
}

func (n *fakeElasticKVNode) serve() {
	for {
		conn, err := n.ln.Accept()
		if err != nil {
			return
		}
		go n.handleConn(conn)
	}
}

func (n *fakeElasticKVNode) handleConn(conn net.Conn) {
	defer conn.Close()
	rd := bufio.NewReader(conn)
	for {
		args, err := readRESPArray(rd)
		if err != nil {
			return
		}
		if len(args) == 0 {
			return
		}
		cmd := strings.ToUpper(args[0])
		switch cmd {
		case "HELLO":
			// Force go-redis to fall back to RESP2 without HELLO: return an
			// error the client treats as "HELLO not supported".
			_, _ = conn.Write([]byte("-ERR unknown command 'HELLO'\r\n"))
		case "CLIENT", "AUTH", "SELECT", "PING":
			_, _ = conn.Write([]byte("+OK\r\n"))
		case "INFO":
			n.infoCalls.Add(1)
			n.waitInfoGate()
			body := fmt.Sprintf(
				"# Replication\r\nrole:slave\r\nraft_leader_redis:%s\r\n",
				n.Leader(),
			)
			_, _ = fmt.Fprintf(conn, "$%d\r\n%s\r\n", len(body), body)
		default:
			n.writeCommandResponse(conn, cmd)
		}
	}
}

func (n *fakeElasticKVNode) writeCommandResponse(conn net.Conn, cmd string) {
	n.commands.Add(1)
	if leader := n.Leader(); leader != "" && leader != n.addr {
		_, _ = conn.Write([]byte("-NOTLEADER etcd raft engine is not leader\r\n"))
		return
	}
	if n.scriptErr.Load() && isRedisScriptCommandName(cmd) {
		_, _ = conn.Write([]byte("-NOTLEADER user script\r\n"))
		return
	}
	_, _ = conn.Write([]byte("+OK\r\n"))
}

// readRESPArray reads a single RESP array of bulk strings from rd.
func readRESPArray(rd *bufio.Reader) ([]string, error) {
	header, err := rd.ReadString('\n')
	if err != nil {
		return nil, err
	}
	header = strings.TrimRight(header, "\r\n")
	if !strings.HasPrefix(header, "*") {
		return nil, fmt.Errorf("expected array, got %q", header)
	}
	var n int
	if _, err := fmt.Sscanf(header[1:], "%d", &n); err != nil {
		return nil, err
	}
	out := make([]string, 0, n)
	for i := 0; i < n; i++ {
		lenLine, err := rd.ReadString('\n')
		if err != nil {
			return nil, err
		}
		lenLine = strings.TrimRight(lenLine, "\r\n")
		if !strings.HasPrefix(lenLine, "$") {
			return nil, fmt.Errorf("expected bulk, got %q", lenLine)
		}
		var sz int
		if _, err := fmt.Sscanf(lenLine[1:], "%d", &sz); err != nil {
			return nil, err
		}
		buf := make([]byte, sz+2)
		if _, err := io.ReadFull(rd, buf); err != nil {
			return nil, err
		}
		out = append(out, string(buf[:sz]))
	}
	return out, nil
}

func TestLeaderAwareRedisBackend_FollowsLeaderChange(t *testing.T) {
	nodeA := newFakeElasticKVNode(t)
	nodeB := newFakeElasticKVNode(t)

	// Start: A is leader; both nodes advertise A as the leader.
	nodeA.SetLeader(nodeA.addr)
	nodeB.SetLeader(nodeA.addr)

	backend := NewLeaderAwareRedisBackendWithInterval(
		[]string{nodeA.addr, nodeB.addr},
		"elastickv",
		DefaultBackendOptions(),
		50*time.Millisecond, 500*time.Millisecond,
		testLogger,
	)
	t.Cleanup(func() { _ = backend.Close() })

	require.Eventually(t, func() bool {
		return backend.CurrentLeader() == nodeA.addr
	}, 2*time.Second, 10*time.Millisecond, "initial leader must be A")

	// Issue a command — it must land on A.
	res := backend.Do(context.Background(), "SET", "k", "v")
	require.NoError(t, res.Err())
	require.Equal(t, int64(1), nodeA.commands.Load(), "command must reach leader A")
	require.Equal(t, int64(0), nodeB.commands.Load(), "command must not reach follower B")

	// Leader election flips to B. Both nodes now advertise B as the leader.
	nodeA.SetLeader(nodeB.addr)
	nodeB.SetLeader(nodeB.addr)
	backend.TriggerRefresh()

	require.Eventually(t, func() bool {
		return backend.CurrentLeader() == nodeB.addr
	}, 2*time.Second, 10*time.Millisecond, "backend must follow leader change to B")

	// New commands must land on B.
	beforeA := nodeA.commands.Load()
	beforeB := nodeB.commands.Load()
	res = backend.Do(context.Background(), "SET", "k", "v")
	require.NoError(t, res.Err())
	require.Equal(t, beforeA, nodeA.commands.Load(), "command must not reach former leader A")
	require.Equal(t, beforeB+1, nodeB.commands.Load(), "command must reach new leader B")
}

func TestLeaderAwareRedisBackend_NotLeaderRefreshesWithoutReplay(t *testing.T) {
	nodeA := newFakeElasticKVNode(t)
	nodeB := newFakeElasticKVNode(t)
	nodeA.SetLeader(nodeA.addr)
	nodeB.SetLeader(nodeA.addr)

	backend := NewLeaderAwareRedisBackendWithInterval(
		[]string{nodeA.addr, nodeB.addr},
		"elastickv",
		DefaultBackendOptions(),
		time.Hour, 500*time.Millisecond,
		testLogger,
	)
	t.Cleanup(func() { _ = backend.Close() })
	require.Eventually(t, func() bool {
		return backend.CurrentLeader() == nodeA.addr
	}, 2*time.Second, 10*time.Millisecond)

	nodeA.SetLeader(nodeB.addr)
	nodeB.SetLeader(nodeB.addr)

	res := backend.Do(context.Background(), "SET", "k", "v")
	require.Error(t, res.Err())
	require.Contains(t, res.Err().Error(), "NOTLEADER")
	require.Equal(t, nodeB.addr, backend.CurrentLeader())
	require.Equal(t, int64(1), nodeA.commands.Load(), "first attempt must be rejected by the former leader")
	require.Equal(t, int64(0), nodeB.commands.Load(), "ambiguous writes must not be replayed")
}

func TestLeaderAwareRedisBackend_ScriptNotLeaderRefreshesWithoutReplay(t *testing.T) {
	nodeA := newFakeElasticKVNode(t)
	nodeB := newFakeElasticKVNode(t)
	nodeA.SetLeader(nodeA.addr)
	nodeB.SetLeader(nodeA.addr)

	backend := NewLeaderAwareRedisBackendWithInterval(
		[]string{nodeA.addr, nodeB.addr},
		"elastickv",
		DefaultBackendOptions(),
		time.Hour, 500*time.Millisecond,
		testLogger,
	)
	t.Cleanup(func() { _ = backend.Close() })
	require.Eventually(t, func() bool {
		return backend.CurrentLeader() == nodeA.addr
	}, 2*time.Second, 10*time.Millisecond)

	nodeA.SetLeader(nodeB.addr)
	nodeB.SetLeader(nodeB.addr)

	res := backend.Do(context.Background(), "EVALSHA", "deadbeef", "0")
	require.Error(t, res.Err())
	require.Contains(t, res.Err().Error(), "NOTLEADER")
	require.Equal(t, nodeB.addr, backend.CurrentLeader())
	require.Equal(t, int64(1), nodeA.commands.Load(), "script reaches the stale leader once")
	require.Equal(t, int64(0), nodeB.commands.Load(), "script must not be replayed after refresh")
}

func TestLeaderAwareRedisBackend_DoesNotRetryScriptNotLeaderRedisError(t *testing.T) {
	node := newFakeElasticKVNode(t)
	node.SetLeader(node.addr)
	node.SetScriptNotLeaderError(true)

	backend := NewLeaderAwareRedisBackendWithInterval(
		[]string{node.addr},
		"elastickv",
		DefaultBackendOptions(),
		time.Hour, 500*time.Millisecond,
		testLogger,
	)
	t.Cleanup(func() { _ = backend.Close() })
	require.Eventually(t, func() bool {
		return backend.CurrentLeader() == node.addr
	}, 2*time.Second, 10*time.Millisecond)

	res := backend.Do(context.Background(), "EVAL", "redis.call('SET', KEYS[1], ARGV[1]); return {err='NOTLEADER user script'}", "1", "k", "v")

	require.Error(t, res.Err())
	assert.Contains(t, res.Err().Error(), "NOTLEADER user script")
	assert.Equal(t, int64(1), node.commands.Load(), "script errors must not be retried because the script may already have mutated state")
}

func TestLeaderAwareRedisBackend_CoalescesConcurrentRefreshes(t *testing.T) {
	node := newFakeElasticKVNode(t)
	node.SetLeader(node.addr)

	backend := NewLeaderAwareRedisBackendWithInterval(
		[]string{node.addr},
		"elastickv",
		DefaultBackendOptions(),
		time.Hour, time.Second,
		testLogger,
	)
	t.Cleanup(func() { _ = backend.Close() })
	require.Eventually(t, func() bool {
		return backend.CurrentLeader() == node.addr && node.infoCalls.Load() > 0
	}, 2*time.Second, 10*time.Millisecond)

	gate := make(chan struct{})
	node.SetInfoGate(gate)
	before := node.infoCalls.Load()

	ownerDone := make(chan struct{})
	go func() {
		backend.RefreshLeaderNow(context.Background())
		close(ownerDone)
	}()
	require.Eventually(t, func() bool {
		return node.infoCalls.Load() == before+1
	}, time.Second, 10*time.Millisecond)

	const callers = 32
	var wg sync.WaitGroup
	for range callers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
			defer cancel()
			backend.RefreshLeaderNow(ctx)
		}()
	}
	waitersDone := make(chan struct{})
	go func() {
		wg.Wait()
		close(waitersDone)
	}()
	select {
	case <-waitersDone:
	case <-time.After(time.Second):
		close(gate)
		<-ownerDone
		t.Fatal("refresh waiters did not respect their contexts")
	}

	assert.Equal(t, before+1, node.infoCalls.Load())
	close(gate)
	<-ownerDone
	assert.Equal(t, before+1, node.infoCalls.Load())
}

func TestLeaderAwareRedisBackend_RefreshOutlivesCallerDeadline(t *testing.T) {
	node := newFakeElasticKVNode(t)
	node.SetLeader(node.addr)

	backend := NewLeaderAwareRedisBackendWithInterval(
		[]string{node.addr},
		"elastickv",
		DefaultBackendOptions(),
		time.Hour, time.Second,
		testLogger,
	)
	t.Cleanup(func() { _ = backend.Close() })
	require.Eventually(t, func() bool {
		return backend.CurrentLeader() == node.addr && node.infoCalls.Load() > 0
	}, 2*time.Second, 10*time.Millisecond)

	gate := make(chan struct{})
	node.SetInfoGate(gate)
	before := node.infoCalls.Load()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()

	backend.RefreshLeaderNow(ctx)
	require.ErrorIs(t, ctx.Err(), context.DeadlineExceeded)
	require.Equal(t, before+1, node.infoCalls.Load())

	close(gate)
	require.Eventually(t, func() bool {
		backend.refreshMu.Lock()
		defer backend.refreshMu.Unlock()
		return backend.refreshDone == nil
	}, time.Second, 10*time.Millisecond)
	assert.Equal(t, before+1, node.infoCalls.Load(), "caller cancellation must not start a replacement probe")
}

func TestLeaderRefreshTransportErrorClassification(t *testing.T) {
	require.True(t, isLeaderRefreshTransportError(io.EOF))
	require.True(t, isLeaderRefreshTransportError(&net.OpError{Op: "read", Err: errors.New("connection reset by peer")}))
	require.False(t, isLeaderRefreshTransportError(context.Canceled))
	require.False(t, isLeaderRefreshTransportError(context.DeadlineExceeded))
	require.False(t, isLeaderRefreshTransportError(errors.New("write conflict")))
}

func TestElasticKVNotLeaderErrorClassification(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{name: "nil", err: nil, want: false},
		{name: "canonical redis code", err: redisError("NOTLEADER leader not found"), want: true},
		{name: "bare canonical code", err: errors.New("NOTLEADER"), want: true},
		{name: "internal sentinel text", err: errors.New("raft engine: not leader"), want: true},
		{name: "grpc wrapped text", err: errors.New("rpc error: code = Unknown desc = leader not found"), want: true},
		{name: "unrelated error", err: errors.New("write conflict"), want: false},
		{name: "redis user error containing phrase", err: redisError("ERR script says raft engine: not leader"), want: false},
		{name: "redis user error with bare phrase", err: redisError("leader not found"), want: false},
		{name: "free form suffix", err: errors.New("key value says leader not found"), want: false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, isElasticKVNotLeaderError(tc.err))
		})
	}
}

type redisError string

func (e redisError) Error() string { return string(e) }
func (redisError) RedisError()     {}

func TestLeaderAwareRedisBackend_ConcurrentCloseIsRaceFree(t *testing.T) {
	// Regression guard: Close() must not race concurrent Do() callers —
	// currentClient and ensureClientLocked hold the lock consistently with
	// the closed flag, and Close snapshots the client map before iterating.
	// Run under `go test -race` to exercise the fix.
	leader := newFakeElasticKVNode(t)
	leader.SetLeader(leader.addr)

	backend := NewLeaderAwareRedisBackendWithInterval(
		[]string{leader.addr},
		"elastickv",
		DefaultBackendOptions(),
		20*time.Millisecond, 200*time.Millisecond,
		testLogger,
	)

	require.Eventually(t, func() bool {
		return backend.CurrentLeader() == leader.addr
	}, 2*time.Second, 10*time.Millisecond)

	var wg sync.WaitGroup
	stop := make(chan struct{})
	const workers = 8
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				// Error is expected once the backend closes (ErrNoLeaderBackend
				// or a closed-pool error); we only care that no goroutine panics.
				_ = backend.Do(context.Background(), "PING").Err()
			}
		}()
	}

	// Let the workers hammer the backend briefly, then close while they run.
	time.Sleep(20 * time.Millisecond)
	require.NoError(t, backend.Close())
	close(stop)
	wg.Wait()
}

// clientAddrs returns the set of addresses for which this backend currently
// caches a *redis.Client. Exposed for tests; safe to call at any time.
func (b *LeaderAwareRedisBackend) clientAddrs() []string {
	b.mu.RLock()
	defer b.mu.RUnlock()
	out := make([]string, 0, len(b.clients))
	for addr := range b.clients {
		out = append(out, addr)
	}
	return out
}

func TestLeaderAwareRedisBackend_EvictsOldestNonProtectedClient(t *testing.T) {
	// Simulate leader churn by forcing many leader addresses through setLeader.
	// The cached client map must stay bounded at maxLeaderAwareClients and the
	// seed must never be evicted.
	seed := newFakeElasticKVNode(t)
	seed.SetLeader(seed.addr)

	backend := NewLeaderAwareRedisBackendWithInterval(
		[]string{seed.addr},
		"elastickv",
		DefaultBackendOptions(),
		time.Hour, time.Second, // disable auto-refresh; we drive setLeader manually
		testLogger,
	)
	t.Cleanup(func() { _ = backend.Close() })

	require.Len(t, backend.clientAddrs(), 1, "seed client must be pre-created")

	// Churn through many synthetic addresses. Since auto-refresh is effectively
	// disabled, none of these addresses will ever be probed — we only care
	// that the client cache stays bounded.
	for i := 0; i < maxLeaderAwareClients*2; i++ {
		backend.setLeader(fmt.Sprintf("10.0.0.%d:6379", i+2))
	}

	addrs := backend.clientAddrs()
	assert.LessOrEqual(t, len(addrs), maxLeaderAwareClients,
		"bounded client cache must not exceed maxLeaderAwareClients")
	assert.Contains(t, addrs, seed.addr, "seed must never be evicted")
}

func TestLeaderAwareRedisBackend_FallsBackToSeedOnProbeFailure(t *testing.T) {
	// Seed 1 is unreachable; seed 2 is alive and reports itself as leader.
	aliveLeader := newFakeElasticKVNode(t)
	aliveLeader.SetLeader(aliveLeader.addr)

	backend := NewLeaderAwareRedisBackendWithInterval(
		[]string{"127.0.0.1:1", aliveLeader.addr},
		"elastickv",
		DefaultBackendOptions(),
		50*time.Millisecond, 200*time.Millisecond,
		testLogger,
	)
	t.Cleanup(func() { _ = backend.Close() })

	require.Eventually(t, func() bool {
		return backend.CurrentLeader() == aliveLeader.addr
	}, 3*time.Second, 20*time.Millisecond, "must fall back to the reachable seed and adopt its advertised leader")
}
