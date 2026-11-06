package adapter

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/redcon"
)

type redisReadCopyEvent struct {
	ctx   context.Context
	bytes int
}

// Keep actual backend copies live before any handler reaches WriteBulk. The
// entry count detects work admitted before a payload-size/RESP budget exists.
type redisReadCopyStore struct {
	store.MVCCStore
	key       []byte
	valueSize int
	returned  chan redisReadCopyEvent
	release   <-chan struct{}
	entered   atomic.Int32
	live      atomic.Int64
	peak      atomic.Int64
}

func (s *redisReadCopyStore) GetAt(ctx context.Context, key []byte, ts uint64) ([]byte, error) {
	if !bytes.Equal(key, s.key) {
		value, err := s.MVCCStore.GetAt(ctx, key, ts)
		return value, err
	}
	s.entered.Add(1)
	value, err := s.MVCCStore.GetAt(ctx, key, ts)
	if err != nil {
		return nil, err
	}
	live := s.live.Add(int64(len(value)))
	for previous := s.peak.Load(); live > previous; previous = s.peak.Load() {
		if s.peak.CompareAndSwap(previous, live) {
			break
		}
	}
	defer s.live.Add(-int64(len(value)))
	select {
	case s.returned <- redisReadCopyEvent{ctx: ctx, bytes: len(value)}:
	case <-ctx.Done():
		return nil, fmt.Errorf("backend copy canceled before observation: %w", ctx.Err())
	}
	select {
	case <-s.release:
		runtime.KeepAlive(value)
		return value, nil
	case <-ctx.Done():
		runtime.KeepAlive(value)
		return nil, fmt.Errorf("retained backend copy canceled: %w", ctx.Err())
	}
}

// Current Redis snapshot reads use GetAt after their lease fence. Keep the
// optional spelling on this observer too for alternate snapshot-store callers.
func (s *redisReadCopyStore) GetAtSnapshot(ctx context.Context, key []byte, ts uint64) ([]byte, error) {
	return s.GetAt(ctx, key, ts)
}

type redisReadWireResult struct {
	line     string
	err      error
	closeErr error
}

func redisReadExecutionUsage(s *RedisServer) (used, peak int) {
	s.executionBudget.mu.Lock()
	defer s.executionBudget.mu.Unlock()
	return s.executionBudget.used, s.executionBudget.peak
}

func startRedisReadAdmissionServer(t *testing.T, st store.MVCCStore, g *redisWireTestGate, limit int) (*RedisServer, <-chan struct{}) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	s := NewRedisServer(listener, listener.Addr().String(), st, newLocalAdapterCoordinator(st), nil, nil, WithRedisRequestAdmission(g.admit))
	s.executionBudget.limit = limit
	closed, done := make(chan struct{}, 32), make(chan error, 1)
	go func() {
		done <- redcon.Serve(listener, s.handleRedisCommand, s.acceptConn, func(conn redcon.Conn, _ error) {
			s.closeConn(conn)
			closed <- struct{}{}
		})
	}()
	t.Cleanup(func() {
		s.Stop()
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			t.Error("read admission server did not join")
		}
	})
	return s, closed
}

type redisHeldRead struct {
	client *redisWireClient
	owner  *redisWireTestOwner
}

func newRedisReadSourceBarrier(t *testing.T, clients int) (*redisReadCopyStore, string, func()) {
	t.Helper()
	base, err := store.NewPebbleStore(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, base.Close()) })
	value := strings.Repeat("s", 1<<20)
	key := redisStrKey([]byte("large-source"))
	encoded := encodeRedisStr([]byte(value), nil)
	require.NoError(t, base.PutAt(t.Context(), key, encoded, 1, 0))
	release := make(chan struct{})
	unblock := sync.OnceFunc(func() { close(release) })
	t.Cleanup(unblock)
	observed := &redisReadCopyStore{MVCCStore: base, key: key, valueSize: len(encoded), returned: make(chan redisReadCopyEvent, clients), release: release}
	return observed, fmt.Sprintf("$%d\r\n%s\r\n", len(value), value), unblock
}

func holdRedisReadCopies(t *testing.T, s *RedisServer, g *redisWireTestGate, observed *redisReadCopyStore, count, valueBytes int) []redisHeldRead {
	t.Helper()
	accepted := make([]redisHeldRead, 0, count)
	for range count {
		c := dialRedisWire(t, s)
		c.send(t, []string{cmdGet, "large-source"})
		select {
		case event := <-observed.returned:
			require.GreaterOrEqual(t, event.bytes, valueBytes)
			require.NoError(t, g.a.ValidateResult(event.ctx))
		case <-time.After(3 * time.Second):
			t.Fatal("admitted GET did not make its bounded backend copy")
		}
		accepted = append(accepted, redisHeldRead{client: c, owner: g.next(t)})
	}
	return accepted
}

func refuseRedisReadCopies(t *testing.T, s *RedisServer, g *redisWireTestGate, observed *redisReadCopyStore, closed <-chan struct{}, count int) {
	t.Helper()
	results := make(chan redisReadWireResult, count)
	for range count {
		c := dialRedisWire(t, s)
		c.send(t, []string{cmdGet, "large-source"}, []string{cmdSet, "refused-pipeline", "never"}, []string{cmdGet, "large-source"})
		go func() {
			line, readErr := c.r.ReadString('\n')
			_, closeErr := c.r.ReadByte()
			results <- redisReadWireResult{line: line, err: readErr, closeErr: closeErr}
		}()
	}
	for range count {
		select {
		case event := <-observed.returned:
			t.Fatalf("pre-backend limit bypassed: another %d-byte copy reached the retention barrier (entries=%d)", event.bytes, observed.entered.Load())
		case result := <-results:
			require.NoError(t, result.err)
			require.True(t, strings.HasPrefix(result.line, "-BUSY"), "execution exhaustion has an explicit non-success response")
			require.ErrorIs(t, result.closeErr, io.EOF, "BUSY drains and closes the refused pipeline")
		case <-time.After(3 * time.Second):
			t.Fatal("excess GET was queued behind retained backend copies instead of being refused")
		}
	}
	for range count {
		select {
		case <-closed:
		case <-time.After(3 * time.Second):
			t.Fatal("refused serving pipeline did not retire")
		}
	}
	require.Empty(t, g.owners, "excess GET and refused pipeline cannot capture an owner")
}

func TestRedisReadAdmissionRejectsBeforeBackendCopy(t *testing.T) {
	const limit, clients, valueBytes = 2, 8, 1 << 20
	observed, want, unblock := newRedisReadSourceBarrier(t, clients)
	defer unblock()
	g := newRedisWireTestGate(t)
	s, closed := startRedisReadAdmissionServer(t, observed, g, limit)
	accepted := holdRedisReadCopies(t, s, g, observed, limit, valueBytes)
	used, _ := redisReplyUsage(&s.replyBudget)
	require.Zero(t, used, "source copies exist before WriteBulk can reserve any RESP capacity")
	refuseRedisReadCopies(t, s, g, observed, closed, clients-limit)
	require.EqualValues(t, limit, observed.entered.Load(), "refuse before invoking the allocating backend")
	require.LessOrEqual(t, observed.peak.Load(), int64(limit*observed.valueSize))
	executing, peak := redisReadExecutionUsage(s)
	require.Equal(t, limit, executing)
	require.Equal(t, limit, peak)
	unblock()
	for _, read := range accepted {
		read.client.want(t, want)
		requireRedisWireJoined(t, read.owner)
	}
	require.Eventually(t, func() bool { used, _ := redisReadExecutionUsage(s); return used == 0 }, 3*time.Second, time.Millisecond)
	control := dialRedisWire(t, s)
	control.send(t, []string{cmdGet, "large-source"})
	control.want(t, want)
	requireRedisWireJoined(t, g.next(t))
	require.EqualValues(t, limit+1, observed.entered.Load(), "admission capacity recovers after the original reads finish")
	_, err := observed.MVCCStore.GetAt(t.Context(), redisStrKey([]byte("refused-pipeline")), ^uint64(0))
	require.ErrorIs(t, err, store.ErrKeyNotFound)
	require.Eventually(t, func() bool { used, _ := redisReadExecutionUsage(s); return used == 0 }, 3*time.Second, time.Millisecond)
}

func TestRedisReadAdmissionShutdownJoinsRetainedCopies(t *testing.T) {
	observed, _, unblock := newRedisReadSourceBarrier(t, 2)
	defer unblock()
	g := newRedisWireTestGate(t)
	s, _ := startRedisReadAdmissionServer(t, observed, g, 2)
	accepted := holdRedisReadCopies(t, s, g, observed, 2, 1<<20)
	require.NoError(t, s.Close())
	for _, read := range accepted {
		requireRedisWireJoined(t, read.owner)
		_, err := read.client.r.ReadByte()
		require.ErrorIs(t, err, io.EOF, "cancellation cannot publish a held backend value")
	}
	require.Eventually(t, func() bool { used, _ := redisReadExecutionUsage(s); return used == 0 }, 3*time.Second, time.Millisecond)
	require.Zero(t, observed.live.Load())
	requireRedisReplyReleased(t, s)
}

func TestRedisReadAdmissionBusyEXECRetiresQueuedOwner(t *testing.T) {
	observed, want, unblock := newRedisReadSourceBarrier(t, 2)
	defer unblock()
	g := newRedisWireTestGate(t)
	s, closed := startRedisReadAdmissionServer(t, observed, g, 2)
	txn := dialRedisWire(t, s)
	txn.send(t, []string{cmdMulti}, []string{cmdSet, "queued-before-limit", "never"})
	txn.want(t, "+OK\r\n+QUEUED\r\n")
	transactionOwner := g.next(t)
	require.Eventually(t, func() bool { used, _ := redisReadExecutionUsage(s); return used == 0 }, 3*time.Second, time.Millisecond)
	accepted := holdRedisReadCopies(t, s, g, observed, 2, 1<<20)
	txn.send(t, []string{cmdExec}, []string{cmdSet, "after-busy-exec", "never"}, []string{cmdGet, "large-source"})
	txn.want(t, "-"+errRedisExecutionLimit.Error()+"\r\n")
	requireRedisQuitClosed(t, txn, closed)
	requireRedisWireJoined(t, transactionOwner)
	requireRedisBudgetReleased(t, s)
	require.Empty(t, g.owners, "BUSY EXEC must not refresh the retained MULTI owner")
	require.EqualValues(t, 2, observed.entered.Load())
	unblock()
	for _, read := range accepted {
		read.client.want(t, want)
		requireRedisWireJoined(t, read.owner)
	}
	for _, key := range []string{"queued-before-limit", "after-busy-exec"} {
		_, err := observed.MVCCStore.GetAt(t.Context(), redisStrKey([]byte(key)), ^uint64(0))
		require.ErrorIs(t, err, store.ErrKeyNotFound)
	}
	require.Eventually(t, func() bool { used, _ := redisReadExecutionUsage(s); return used == 0 }, 3*time.Second, time.Millisecond)
}

func TestRedisReadAdmissionFollowerProxyRetainsExecutionSlot(t *testing.T) {
	observed, want, unblock := newRedisReadSourceBarrier(t, 2)
	defer unblock()
	leaderGate := newRedisWireTestGate(t)
	leader, _ := startRedisReadAdmissionServer(t, observed, leaderGate, 1)
	followerStore := newRedisWireStore(t)
	followerGate := newRedisWireTestGate(t)
	followerCoord := newLocalAdapterCoordinator(followerStore)
	followerCoord.leaderSet, followerCoord.leader, followerCoord.raftLeader = true, false, "read-leader"
	follower := startRedisWireServer(t, followerStore, followerCoord, followerGate, func(s *RedisServer) {
		s.executionBudget.limit = 1
		s.leaderRedis = map[string]string{"read-leader": leader.listen.Addr().String()}
	})
	c := dialRedisWire(t, follower)
	c.send(t, []string{cmdGet, "large-source"})
	var source redisReadCopyEvent
	select {
	case source = <-observed.returned:
	case <-time.After(3 * time.Second):
		t.Fatal("proxied GET did not reach the independently admitted leader read")
	}
	owner := followerGate.next(t)
	require.NoError(t, leaderGate.a.ValidateResult(source.ctx))
	used, peak := redisReadExecutionUsage(follower)
	require.Equal(t, 1, used)
	require.Equal(t, 1, peak)
	other := dialRedisWire(t, follower)
	other.send(t, []string{cmdGet, "large-source"})
	other.want(t, "-"+errRedisExecutionLimit.Error()+"\r\n")
	_, err := other.r.ReadByte()
	require.ErrorIs(t, err, io.EOF)
	require.Empty(t, followerGate.owners, "refuse before proxying or minting another sender H")
	require.EqualValues(t, 1, observed.entered.Load())
	unblock()
	c.want(t, want)
	requireRedisWireJoined(t, owner)
	require.Eventually(t, func() bool { used, _ := redisReadExecutionUsage(follower); return used == 0 }, 3*time.Second, time.Millisecond)
	require.Eventually(t, func() bool { used, _ := redisReadExecutionUsage(leader); return used == 0 }, 3*time.Second, time.Millisecond)
}
