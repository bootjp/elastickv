package adapter

import (
	"bytes"
	"context"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/bootjp/elastickv/monitoring"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/redcon"
)

type redisDetachedTestConn struct {
	commandRecorder
	mu       sync.Mutex
	raw      bytes.Buffer
	commands chan redcon.Command
	done     chan struct{}
	once     sync.Once
}
type redisDetachedTestNet struct {
	net.Conn
	conn *redisDetachedTestConn
}

func (c redisDetachedTestNet) Close() error                  { return c.conn.Close() }
func (c *redisDetachedTestConn) NetConn() net.Conn           { return redisDetachedTestNet{conn: c} }
func (c *redisDetachedTestConn) Detach() redcon.DetachedConn { return c }
func (c *redisDetachedTestConn) Flush() error                { return nil }
func (c *redisDetachedTestConn) WriteRaw(p []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()
	_, _ = c.raw.Write(p)
}
func (c *redisDetachedTestConn) response() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.raw.String()
}
func (c *redisDetachedTestConn) Close() error { c.once.Do(func() { close(c.done) }); return nil }
func (c *redisDetachedTestConn) ReadCommand() (redcon.Command, error) {
	select {
	case cmd := <-c.commands:
		return cmd, nil
	case <-c.done:
		return redcon.Command{}, io.EOF
	}
}

func TestRedisOwnedPubSubRetainsAndJoinsOriginalOwner(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	owner := &redisTestOwnership{ctx: ctx}
	s := redisOwnedTestServer(owner)
	c := &redisDetachedTestConn{commands: make(chan redcon.Command, 2), done: make(chan struct{})}
	cmd := redcon.Command{Args: [][]byte{[]byte(cmdSubscribe), []byte("channel")}}
	s.dispatchCommand(c, cmdSubscribe, s.subscribe, cmd, time.Now())
	require.Zero(t, owner.finishes, "SUBSCRIBE must not release its detached root")
	require.Contains(t, c.response(), "subscribe")
	s.pubsub.mu.RLock()
	var sc *pubsubConn
	for _, conn := range s.pubsub.conns {
		sc = conn
	}
	s.pubsub.mu.RUnlock()
	require.NotNil(t, sc)
	require.Equal(t, 1, s.pubsub.Publish("channel", "message"))
	require.Contains(t, c.response(), "message")
	cancel()
	select {
	case <-sc.closeCh:
	case <-time.After(time.Second):
		t.Fatal("revoked detached reader did not close and join")
	}
	require.Equal(t, 1, owner.finishes)
	require.Equal(t, 1, owner.joins)
	before := c.response()
	require.Zero(t, s.pubsub.Publish("channel", "forbidden"))
	require.Equal(t, before, c.response())
}

func TestRedisOwnedPubSubServerCloseCancelsDetachedReader(t *testing.T) {
	owner := &redisTestOwnership{ctx: context.Background()}
	s := redisOwnedTestServer(owner)
	c := &redisDetachedTestConn{commands: make(chan redcon.Command), done: make(chan struct{})}
	s.dispatchCommand(c, cmdSubscribe, s.subscribe, redcon.Command{Args: [][]byte{[]byte(cmdSubscribe), []byte("channel")}}, time.Now())
	s.pubsub.mu.RLock()
	var sc *pubsubConn
	for _, conn := range s.pubsub.conns {
		sc = conn
	}
	s.pubsub.mu.RUnlock()
	require.NoError(t, s.Close())
	select {
	case <-sc.closeCh:
	case <-time.After(time.Second):
		t.Fatal("server close did not join detached subscription")
	}
	require.Equal(t, 1, owner.finishes)
	require.Equal(t, 1, owner.joins)
}

func TestRedisOwnedBlockingCancellationReleasesWaiters(t *testing.T) {
	for _, name := range []string{cmdXRead, cmdBZPopMin} {
		t.Run(name, func(t *testing.T) {
			s, owner := redisPropagationServer(t)
			ctx, cancel := context.WithCancel(owner.ctx)
			owner.ctx = ctx
			args := []string{cmdBZPopMin, "missing", "10"}
			if name == cmdXRead {
				args = []string{cmdXRead, "BLOCK", "10000", "STREAMS", "missing", "0-0"}
			}
			cmd := redcon.Command{}
			for _, arg := range args {
				cmd.Args = append(cmd.Args, []byte(arg))
			}
			c := &redisOwnedTestConn{}
			done := make(chan struct{})
			go func() { defer close(done); s.dispatchCommand(c, name, s.route[name], cmd, time.Now()) }()
			cancel()
			select {
			case <-done:
			case <-time.After(time.Second):
				t.Fatal("blocking request did not cancel and join")
			}
			require.Empty(t, c.raw.String(), "cancellation must not return success-shaped null")
			require.Len(t, c.writes, 1)
			require.Equal(t, 1, owner.finishes)
			require.Equal(t, 1, owner.joins)
		})
	}
}

func TestRedisOwnedPubSubCloseWaitsForRetiringOwner(t *testing.T) {
	finishing, release := make(chan struct{}), make(chan struct{})
	owner := &redisTestOwnership{ctx: context.Background(), onFinish: func() { close(finishing); <-release }}
	s := redisOwnedTestServer(owner)
	c := &redisDetachedTestConn{commands: make(chan redcon.Command), done: make(chan struct{})}
	s.dispatchCommand(c, cmdSubscribe, s.subscribe, redcon.Command{Args: [][]byte{[]byte(cmdSubscribe), []byte("channel")}}, time.Now())
	require.NoError(t, c.Close())
	<-finishing
	closed := make(chan struct{})
	go func() { _ = s.Close(); close(closed) }()
	select {
	case <-closed:
		t.Fatal("server close returned while detached owner was still finishing")
	case <-time.After(10 * time.Millisecond):
	}
	close(release)
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("server close did not join retiring owner")
	}
	require.Equal(t, 1, owner.joins)
}

func TestRedisOwnedPeriodicCompactorCapturesBeforeWork(t *testing.T) {
	owner := &redisTestOwnership{ctx: context.Background()}
	compactor := NewDeltaCompactor(nil, nil)
	mints := 0
	s := NewRedisServer(nil, "", nil, nil, nil, nil, WithRedisCompactor(compactor), WithRedisRequestAdmission(func(context.Context) (RequestOwnership, error) { mints++; return owner, nil }))
	t.Cleanup(func() { require.NoError(t, s.Close()) })
	require.NotNil(t, compactor.requestAdmission)
	require.NoError(t, compactor.SyncOnce(context.Background()))
	require.Equal(t, 1, mints)
	require.Equal(t, 1, owner.finishes)
	require.Equal(t, 1, owner.joins)
	compactor.TriggerUrgentCompaction("hash", []byte("unowned"))
	require.Empty(t, compactor.urgentCh, "ownerless async work may not obtain a new H later")
	compactor.requestAdmission = func(context.Context) (RequestOwnership, error) { return nil, errRedisAdmission }
	require.ErrorIs(t, compactor.SyncOnce(context.Background()), errRedisAdmission)
}

func TestRedisOwnedStopRetainsContextInsensitiveIO(t *testing.T) {
	old := redisShutdownTimeout
	redisShutdownTimeout = time.Millisecond
	defer func() { redisShutdownTimeout = old }()
	s, owner := redisPropagationServer(t)
	started, release, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
	c := &redisOwnedTestConn{}
	s.route[cmdGet] = func(conn redcon.Conn, _ redcon.Command) {
		close(started)
		<-release
		conn.WriteBulkString("must not escape")
	}
	go func() {
		defer close(done)
		s.handleRedisCommand(c, redcon.Command{Args: [][]byte{[]byte(cmdGet), []byte("key")}})
	}()
	<-started
	stopped := make(chan struct{})
	go func() { s.Stop(); close(stopped) }()
	select {
	case <-stopped:
		t.Fatal("protected Stop returned before owned I/O joined")
	case <-time.After(10 * time.Millisecond):
	}
	close(release)
	<-done
	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("Stop did not finish after I/O returned")
	}
	require.Empty(t, c.raw.String())
	require.Equal(t, 1, owner.joins)
}

func TestRedisOwnedPubSubNeverRetainsPooledMetricsConn(t *testing.T) {
	var owners []*redisTestOwnership
	s := NewRedisServer(nil, "", nil, nil, nil, nil,
		WithRedisRequestObserver(monitoring.NewRegistry("metrics-sub", "local").RedisObserver()),
		WithRedisRequestAdmission(func(ctx context.Context) (RequestOwnership, error) {
			owner := &redisTestOwnership{ctx: ctx}
			owners = append(owners, owner)
			return owner, nil
		}))
	t.Cleanup(func() { require.NoError(t, s.Close()) })
	connections := []*redisDetachedTestConn{
		{commands: make(chan redcon.Command), done: make(chan struct{})},
		{commands: make(chan redcon.Command), done: make(chan struct{})},
	}
	channels := []string{"first", "second"}
	for i, c := range connections {
		s.dispatchCommand(c, cmdSubscribe, s.subscribe, redcon.Command{Args: [][]byte{[]byte(cmdSubscribe), []byte(channels[i])}}, time.Now())
		// Force a reset/reuse opportunity after dispatch has returned its wrapper.
		metrics, _ := redisMetricsConnPool.Get().(*redisMetricsConn)
		if metrics != nil {
			metrics.reset(connections[1])
			metrics.Conn = nil
			redisMetricsConnPool.Put(metrics)
		}
	}
	s.pubsub.mu.RLock()
	var first *pubsubConn
	var subscriptions []struct {
		key redcon.Conn
		sub *pubsubConn
	}
	for key, sc := range s.pubsub.conns {
		subscriptions = append(subscriptions, struct {
			key redcon.Conn
			sub *pubsubConn
		}{key, sc})
	}
	s.pubsub.mu.RUnlock()
	for _, entry := range subscriptions {
		key, sc := entry.key, entry.sub
		_, pooledKey := key.(*redisMetricsConn)
		_, pooledConn := sc.conn.(*redisMetricsConn)
		require.False(t, pooledKey)
		require.False(t, pooledConn)
		carrier, ok := sc.conn.(*redisOwnedConn)
		require.True(t, ok)
		if carrier.Conn == connections[0] {
			first = sc
		}
	}
	require.NotNil(t, first)
	require.Equal(t, 1, s.pubsub.Publish("first", "first-only"))
	require.Equal(t, 1, s.pubsub.Publish("second", "second-only"))
	require.NotContains(t, connections[0].response(), "second-only")
	require.NotContains(t, connections[1].response(), "first-only")
	require.NoError(t, connections[0].Close())
	select {
	case <-first.closeCh:
	case <-time.After(time.Second):
		t.Fatal("metrics-on disconnect did not join")
	}
	require.Equal(t, 1, s.pubsub.Publish("second", "still-live"))
	require.NoError(t, s.Close())
	for _, owner := range owners {
		require.Equal(t, 1, owner.finishes)
		require.Equal(t, 1, owner.joins)
	}
}
