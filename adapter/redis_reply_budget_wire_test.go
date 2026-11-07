package adapter

import (
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
)

const redisReplyWireValueBytes = 256 << 10

type redisReplyObservedListener struct {
	net.Listener
	writes  chan int
	results chan error
}

type redisReplyObservedConn struct {
	net.Conn
	listener *redisReplyObservedListener
}

func (l *redisReplyObservedListener) Accept() (net.Conn, error) {
	conn, err := l.Listener.Accept()
	if err != nil {
		return nil, fmt.Errorf("accept reply-budget test client: %w", err)
	}
	if tcp, ok := conn.(*net.TCPConn); ok {
		if err := tcp.SetWriteBuffer(1024); err != nil {
			_ = conn.Close()
			return nil, fmt.Errorf("set reply-budget test send buffer: %w", err)
		}
	}
	return &redisReplyObservedConn{Conn: conn, listener: l}, nil
}

func (c *redisReplyObservedConn) Write(p []byte) (int, error) {
	large := len(p) >= redisReplyWireValueBytes
	if large {
		c.listener.writes <- len(p)
	}
	n, err := c.Conn.Write(p)
	if large {
		c.listener.results <- err
	}
	return n, err
}

func startRedisReplyBudgetWire(t *testing.T, g *redisWireTestGate, st store.MVCCStore, limit int, timeout time.Duration, options ...RedisServerOption) (*RedisServer, *redisReplyObservedListener) {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	listener := &redisReplyObservedListener{Listener: l, writes: make(chan int, 32), results: make(chan error, 32)}
	options = append([]RedisServerOption{WithRedisRequestAdmission(g.admit)}, options...)
	s := NewRedisServer(listener, l.Addr().String(), st, newLocalAdapterCoordinator(st), nil, nil, options...)
	s.replyBudget.limit, s.replyWriteTimeout = limit, timeout
	done := make(chan error, 1)
	go func() { done <- s.Run() }()
	t.Cleanup(func() {
		s.Stop()
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			t.Error("reply-budget server did not join")
		}
	})
	return s, listener
}

func seedRedisReplyValue(t *testing.T, st store.MVCCStore) string {
	t.Helper()
	value := strings.Repeat("v", redisReplyWireValueBytes)
	require.NoError(t, st.PutAt(t.Context(), redisStrKey([]byte("large")), encodeRedisStr([]byte(value), nil), 1, 0))
	return fmt.Sprintf("$%d\r\n%s\r\n", len(value), value)
}

func awaitRedisReplyWrite(t *testing.T, listener *redisReplyObservedListener) int {
	t.Helper()
	select {
	case n := <-listener.writes:
		return n
	case <-time.After(3 * time.Second):
		t.Fatal("large reply did not reach the socket")
		return 0
	}
}

func TestRedisReplyBudgetWireConcurrentGETIsReceiverBounded(t *testing.T) {
	t.Parallel()
	st := newRedisWireStore(t)
	want := seedRedisReplyValue(t, st)
	g := newRedisWireTestGate(t)
	charge := redisReplyCapacityCharge(2 * redisReplyWireValueBytes)
	limit := charge + redisReplyCapacityCharge(256)
	s, writes := startRedisReplyBudgetWire(t, g, st, limit, 3*time.Second)
	slow := dialRedisWire(t, s)
	if tcp, ok := slow.Conn.(*net.TCPConn); ok {
		require.NoError(t, tcp.SetReadBuffer(1024))
	}
	slow.send(t, []string{cmdGet, "large"})
	owner := g.next(t)
	require.Equal(t, len(want), awaitRedisReplyWrite(t, writes))
	used, _ := redisReplyUsage(&s.replyBudget)
	require.Equal(t, charge, used, "blocked transport retains its private capacity")
	for range 2 {
		other := dialRedisWire(t, s)
		other.send(t, []string{cmdGet, "large"})
		other.want(t, "-"+errRedisReplyBudget.Error()+"\r\n")
		requireRedisWireJoined(t, g.next(t))
		used, peak := redisReplyUsage(&s.replyBudget)
		require.Equal(t, charge, used)
		require.LessOrEqual(t, peak, limit, "different clients share one receiver budget")
	}
	control := dialRedisWire(t, s)
	control.send(t, []string{cmdPing})
	control.want(t, "+PONG\r\n")
	requireRedisWireJoined(t, g.next(t))
	require.NoError(t, slow.Close())
	requireRedisWireJoined(t, owner)
	requireRedisReplyReleased(t, s)
}

func TestRedisReplyBudgetWirePipelineFlushesEachTerminalReply(t *testing.T) {
	t.Parallel()
	st := newRedisWireStore(t)
	want := seedRedisReplyValue(t, st)
	other := strings.Repeat("w", redisReplyWireValueBytes)
	require.NoError(t, st.PutAt(t.Context(), redisStrKey([]byte("other")), encodeRedisStr([]byte(other), nil), 2, 0))
	wantOther := fmt.Sprintf("$%d\r\n%s\r\n", len(other), other)
	g := newRedisWireTestGate(t)
	limit := redisReplyCapacityCharge(2*redisReplyWireValueBytes) + redisReplyCapacityCharge(256)
	s, writes := startRedisReplyBudgetWire(t, g, st, limit, 3*time.Second)
	c := dialRedisWire(t, s)
	c.send(t, []string{"BUDGET_NO_SUCH"}, []string{cmdGet, "large"}, []string{"BUDGET_NO_SUCH"}, []string{cmdGet, "other"}, []string{cmdQuit})
	for _, reply := range []string{want, wantOther} {
		c.want(t, "-ERR unsupported command 'BUDGET_NO_SUCH'\r\n")
		require.Equal(t, len(reply), awaitRedisReplyWrite(t, writes), "not a redcon aggregate copy of the whole pipeline")
		c.want(t, reply)
		requireRedisWireJoined(t, g.next(t))
	}
	c.want(t, "+OK\r\n")
	_, err := c.r.ReadByte()
	require.ErrorIs(t, err, io.EOF)
	requireRedisWireJoined(t, g.next(t))
	requireRedisReplyReleased(t, s)
	_, peak := redisReplyUsage(&s.replyBudget)
	require.LessOrEqual(t, peak, limit)
}

func TestRedisReplyBudgetWireSlowWriterDeadlineAndShutdownRelease(t *testing.T) {
	t.Parallel()
	for _, shutdown := range []bool{false, true} {
		t.Run(map[bool]string{false: "write deadline", true: "server cancellation"}[shutdown], func(t *testing.T) {
			st := newRedisWireStore(t)
			seedRedisReplyValue(t, st)
			g := newRedisWireTestGate(t)
			timeout := 50 * time.Millisecond
			if shutdown {
				timeout = 3 * time.Second
			}
			s, writes := startRedisReplyBudgetWire(t, g, st, 2*redisReplyWireValueBytes+4096, timeout)
			c := dialRedisWire(t, s)
			if tcp, ok := c.Conn.(*net.TCPConn); ok {
				require.NoError(t, tcp.SetReadBuffer(1024))
			}
			c.send(t, []string{cmdGet, "large"})
			owner := g.next(t)
			awaitRedisReplyWrite(t, writes)
			if shutdown {
				require.NoError(t, s.Close())
			}
			select {
			case err := <-writes.results:
				require.Error(t, err, "non-reading client must not retain budget indefinitely")
				if !shutdown {
					require.ErrorIs(t, err, os.ErrDeadlineExceeded)
				}
			case <-time.After(3 * time.Second):
				t.Fatal("blocked write did not observe deadline/cancellation")
			}
			requireRedisWireJoined(t, owner)
			requireRedisReplyReleased(t, s)
		})
	}
}

func TestRedisReplyBudgetWireDetachedReleasesEveryFlushedMessage(t *testing.T) {
	t.Parallel()
	st := newRedisWireStore(t)
	g := newRedisWireTestGate(t)
	s, _ := startRedisReplyBudgetWire(t, g, st, 2*redisReplyWireValueBytes+4096, 3*time.Second)
	c := dialRedisWire(t, s)
	c.send(t, []string{cmdSubscribe, "events"})
	c.want(t, "*3\r\n$9\r\nsubscribe\r\n$6\r\nevents\r\n:1\r\n")
	owner := g.next(t)
	requireRedisReplyReleased(t, s)
	message := strings.Repeat("m", redisReplyWireValueBytes)
	want := fmt.Sprintf("*3\r\n$7\r\nmessage\r\n$6\r\nevents\r\n$%d\r\n%s\r\n", len(message), message)
	for range 2 {
		done := make(chan int, 1)
		go func() { done <- s.pubsub.Publish("events", message) }()
		c.want(t, want)
		select {
		case delivered := <-done:
			require.Equal(t, 1, delivered)
		case <-time.After(3 * time.Second):
			t.Fatal("pubsub publication did not flush")
		}
		requireRedisReplyReleased(t, s)
		require.Zero(t, owner.finishes.Load(), "root is live, but delivered payload capacity is not pinned")
	}
	require.NoError(t, c.Close())
	requireRedisWireJoined(t, owner)
	requireRedisReplyReleased(t, s)
}

func TestRedisReplyBudgetWireLegacyIsUnchanged(t *testing.T) {
	t.Parallel()
	st := newRedisWireStore(t)
	want := seedRedisReplyValue(t, st)
	g := newRedisWireTestGate(t)
	s, _ := startRedisReplyBudgetWire(t, g, st, 1, time.Millisecond, WithRedisRequestAdmission(nil))
	c := dialRedisWire(t, s)
	c.send(t, []string{cmdGet, "large"})
	c.want(t, want)
	used, peak := redisReplyUsage(&s.replyBudget)
	require.Zero(t, used)
	require.Zero(t, peak)
}

func TestRedisReplyBudgetWireDetachedBlockedWriteRetainsReservation(t *testing.T) {
	t.Parallel()
	st := newRedisWireStore(t)
	g := newRedisWireTestGate(t)
	charge := redisReplyCapacityCharge(2 * redisReplyWireValueBytes)
	s, writes := startRedisReplyBudgetWire(t, g, st, charge+4096, 3*time.Second)
	c := dialRedisWire(t, s)
	if tcp, ok := c.Conn.(*net.TCPConn); ok {
		require.NoError(t, tcp.SetReadBuffer(1024))
	}
	c.send(t, []string{cmdSubscribe, "events"})
	c.want(t, "*3\r\n$9\r\nsubscribe\r\n$6\r\nevents\r\n:1\r\n")
	owner := g.next(t)
	done := make(chan int, 1)
	go func() { done <- s.pubsub.Publish("events", strings.Repeat("m", redisReplyWireValueBytes)) }()
	awaitRedisReplyWrite(t, writes)
	used, peak := redisReplyUsage(&s.replyBudget)
	require.Equal(t, charge, used, "an unflushed detached message cannot release its reservation")
	require.LessOrEqual(t, peak, s.replyBudget.limit)
	select {
	case <-done:
		t.Fatal("non-reading subscriber unexpectedly completed the large write")
	default:
	}
	require.NoError(t, c.Close())
	select {
	case delivered := <-done:
		require.Zero(t, delivered)
	case <-time.After(3 * time.Second):
		t.Fatal("blocked subscriber did not join transport close")
	}
	requireRedisWireJoined(t, owner)
	requireRedisReplyReleased(t, s)
}

func TestRedisReplyBudgetWireTransactionExhaustionPoisonsPipeline(t *testing.T) {
	t.Parallel()
	for _, tiny := range []bool{false, true} {
		t.Run(map[bool]string{false: "EXEC reply", true: "MULTI acknowledgment"}[tiny], func(t *testing.T) {
			st := newRedisWireStore(t)
			seedRedisReplyValue(t, st)
			g := newRedisWireTestGate(t)
			limit := 4096
			if tiny {
				limit = 1
			}
			s, _ := startRedisReplyBudgetWire(t, g, st, limit, 3*time.Second)
			c := dialRedisWire(t, s)
			if !tiny {
				c.send(t, []string{cmdMulti}, []string{cmdGet, "large"})
				c.want(t, "+OK\r\n+QUEUED\r\n")
			}
			commands := [][]string{{cmdExec}, {cmdSet, "after-overflow", "never"}}
			if tiny {
				commands = append([][]string{{cmdMulti}}, commands...)
			}
			c.send(t, commands...)
			if !tiny {
				line, err := c.r.ReadString('\n')
				require.NoError(t, err)
				require.Contains(t, line, errRedisReplyBudget.Error())
				require.Contains(t, line, kv.ErrTransactionOutcomeUnknown.Error())
				require.True(t, strings.HasPrefix(line, "-"), "no partial EXEC success array")
			}
			_, err := c.r.ReadByte()
			require.ErrorIs(t, err, io.EOF)
			requireRedisWireJoined(t, g.next(t))
			requireRedisReplyReleased(t, s)
			requireRedisBudgetReleased(t, s)
			require.Empty(t, g.owners, "buffered commands cannot mint new owners after reply exhaustion")
			_, err = st.GetAt(t.Context(), redisStrKey([]byte("after-overflow")), ^uint64(0))
			require.ErrorIs(t, err, store.ErrKeyNotFound)
			_, peak := redisReplyUsage(&s.replyBudget)
			require.LessOrEqual(t, peak, limit)
		})
	}
}
