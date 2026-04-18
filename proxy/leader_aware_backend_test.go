package proxy

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"strings"
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
}

func newFakeElasticKVNode(t *testing.T, leader string) *fakeElasticKVNode {
	t.Helper()
	var lc net.ListenConfig
	ln, err := lc.Listen(context.Background(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	n := &fakeElasticKVNode{ln: ln, addr: ln.Addr().String()}
	n.SetLeader(leader)
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
			body := fmt.Sprintf(
				"# Replication\r\nrole:slave\r\nraft_leader_redis:%s\r\n",
				n.Leader(),
			)
			_, _ = fmt.Fprintf(conn, "$%d\r\n%s\r\n", len(body), body)
		default:
			n.commands.Add(1)
			_, _ = conn.Write([]byte("+OK\r\n"))
		}
	}
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
	nodeA := newFakeElasticKVNode(t, "")
	nodeB := newFakeElasticKVNode(t, "")

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

func TestLeaderAwareRedisBackend_FallsBackToSeedOnProbeFailure(t *testing.T) {
	// Seed 1 is unreachable; seed 2 is alive and reports itself as leader.
	aliveLeader := newFakeElasticKVNode(t, "")
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
