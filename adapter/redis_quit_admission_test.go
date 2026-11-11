package adapter

import (
	"context"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/redcon"
)

func TestRedisRequestAdmissionQuitWire(t *testing.T) {
	t.Parallel()
	for _, protected := range []bool{false, true} {
		t.Run(map[bool]string{false: "legacy", true: "protected"}[protected], func(t *testing.T) {
			g := newRedisWireTestGate(t)
			var options []RedisServerOption
			if !protected {
				options = append(options, WithRedisRequestAdmission(nil))
			}
			s := startRedisWireServer(t, nil, nil, g, options...)
			c := dialRedisWire(t, s)
			c.send(t, []string{cmdQuit})
			c.want(t, "+OK\r\n")
			_, err := c.r.ReadByte()
			require.ErrorIs(t, err, io.EOF)
			if protected {
				requireRedisWireJoined(t, g.next(t))
			}
		})
	}
}

// Observe redcon's closed callback after its entire parsed pipeline loop, not
// just client EOF (which alone cannot prove trailing handlers never ran).
func startRedisQuitWireServer(t *testing.T, g *redisWireTestGate, st store.MVCCStore, options ...RedisServerOption) (*RedisServer, <-chan struct{}) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	options = append([]RedisServerOption{WithRedisRequestAdmission(g.admit), WithOnePhaseTxnDedup(false)}, options...)
	s := NewRedisServer(listener, listener.Addr().String(), st, newLocalAdapterCoordinator(st), nil, nil, options...)
	closed, done := make(chan struct{}, 1), make(chan error, 1)
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
			t.Error("QUIT test server did not join")
		}
	})
	return s, closed
}

func requireRedisQuitClosed(t *testing.T, c *redisWireClient, closed <-chan struct{}) {
	t.Helper()
	_, err := c.r.ReadByte()
	require.ErrorIs(t, err, io.EOF)
	select {
	case <-closed:
	case <-time.After(3 * time.Second):
		t.Fatal("QUIT did not retire the serving pipeline")
	}
}

func TestRedisRequestAdmissionQuitDiscardsPipelineAndTransaction(t *testing.T) {
	t.Parallel()
	for _, transaction := range []bool{false, true} {
		t.Run(map[bool]string{false: "standalone", true: "MULTI"}[transaction], func(t *testing.T) {
			st := newRedisWireStore(t)
			g := newRedisWireTestGate(t)
			s, closed := startRedisQuitWireServer(t, g, st)
			var pings atomic.Int32
			s.route[cmdPing] = func(conn redcon.Conn, _ redcon.Command) {
				pings.Add(1)
				conn.WriteString("PONG")
			}
			c := dialRedisWire(t, s)
			if transaction {
				c.send(t, []string{cmdMulti}, []string{cmdSet, "queued", "never"})
				c.want(t, "+OK\r\n+QUEUED\r\n")
			}
			c.send(t, []string{cmdQuit}, []string{cmdSet, "after-quit", "never"}, []string{cmdPing}, []string{cmdExec})
			c.want(t, "+OK\r\n")
			requireRedisQuitClosed(t, c, closed)
			requireRedisWireJoined(t, g.next(t))
			require.Empty(t, g.owners, "QUIT and trailing commands cannot obtain a new transaction owner")
			require.Zero(t, pings.Load())
			requireRedisBudgetReleased(t, s)
			for _, key := range []string{"queued", "after-quit"} {
				_, err := st.GetAt(t.Context(), redisStrKey([]byte(key)), ^uint64(0))
				require.ErrorIs(t, err, store.ErrKeyNotFound, "QUIT must not commit queued or trailing SET")
			}
		})
	}
}

type redisQuitFinishOwnership struct {
	RequestOwnership
	beforeFinish func()
}

func (o redisQuitFinishOwnership) Finish(err error) error {
	o.beforeFinish()
	return o.RequestOwnership.Finish(err)
}

func TestRedisRequestAdmissionQuitTerminalDenied(t *testing.T) {
	t.Parallel()
	for _, transaction := range []bool{false, true} {
		t.Run(map[bool]string{false: "standalone", true: "MULTI"}[transaction], func(t *testing.T) {
			st := newRedisWireStore(t)
			g := newRedisWireTestGate(t)
			s, closed := startRedisQuitWireServer(t, g, st, WithRedisRequestAdmission(func(ctx context.Context) (RequestOwnership, error) {
				owner, err := g.admit(ctx)
				if err != nil {
					return nil, err
				}
				return redisQuitFinishOwnership{RequestOwnership: owner, beforeFinish: func() { g.a.Suspend(1) }}, nil
			}))
			c := dialRedisWire(t, s)
			if transaction {
				c.send(t, []string{cmdMulti}, []string{cmdSet, "queued-denied", "never"})
				c.want(t, "+OK\r\n+QUEUED\r\n")
			}
			c.send(t, []string{cmdQuit}, []string{cmdSet, "denied", "never"})
			c.want(t, "-"+errRedisAdmission.Error()+"\r\n")
			requireRedisQuitClosed(t, c, closed)
			requireRedisWireJoined(t, g.next(t))
			requireRedisBudgetReleased(t, s)
			for _, key := range []string{"queued-denied", "denied"} {
				_, err := st.GetAt(t.Context(), redisStrKey([]byte(key)), ^uint64(0))
				require.ErrorIs(t, err, store.ErrKeyNotFound)
			}
		})
	}
}

func TestRedisRequestAdmissionQuitJoinsTransactionChildBeforePublication(t *testing.T) {
	t.Parallel()
	st := newRedisWireStore(t)
	g := newRedisWireTestGate(t)
	s, closed := startRedisQuitWireServer(t, g, st)
	c := dialRedisWire(t, s)
	c.send(t, []string{cmdMulti}, []string{cmdSet, "queued", "never"})
	c.want(t, "+OK\r\n+QUEUED\r\n")
	owner := g.next(t)
	child, release := make(chan struct{}), make(chan struct{})
	unblock := sync.OnceFunc(func() { close(release) })
	t.Cleanup(unblock)
	require.NoError(t, owner.op.Go(func(context.Context) { close(child); <-release }))
	select {
	case <-child:
	case <-time.After(3 * time.Second):
		t.Fatal("transaction child did not start")
	}
	c.send(t, []string{cmdQuit})
	require.Eventually(t, func() bool { return owner.finishes.Load() == 1 }, 3*time.Second, time.Millisecond)
	require.Positive(t, redisBudgetUsed(s), "queued arguments remain owned through the child join")
	select {
	case <-closed:
		t.Fatal("QUIT closed before its owned child joined")
	default:
	}
	require.NoError(t, c.SetReadDeadline(time.Now().Add(50*time.Millisecond)))
	_, err := c.r.ReadByte()
	var timeout net.Error
	require.ErrorAs(t, err, &timeout)
	require.True(t, timeout.Timeout(), "no success reply may escape before the terminal child join")
	require.NoError(t, c.SetReadDeadline(time.Now().Add(10*time.Second)))
	unblock()
	c.want(t, "+OK\r\n")
	requireRedisQuitClosed(t, c, closed)
	requireRedisWireJoined(t, owner)
	requireRedisBudgetReleased(t, s)
}

type redisQuitOrderConn struct {
	redisOwnedTestConn
	closes   int
	pipeline []redcon.Command
}

func (c *redisQuitOrderConn) Close() error {
	c.closes++
	return nil
}

func (c *redisQuitOrderConn) ReadPipeline() []redcon.Command {
	pipeline := c.pipeline
	c.pipeline = nil
	return pipeline
}

func TestRedisRequestAdmissionQuitPanicNeverPublishesSuccess(t *testing.T) {
	t.Parallel()
	owner := &redisTestOwnership{ctx: t.Context()}
	s := redisOwnedTestServer(owner)
	t.Cleanup(func() { require.NoError(t, s.Close()) })
	c := &redisQuitOrderConn{pipeline: []redcon.Command{{Args: [][]byte{[]byte(cmdSet), []byte("k"), []byte("v")}}}}
	owner.onFinish = func() {
		require.Zero(t, c.closes, "close must follow terminal validation, including panic cleanup")
		require.Empty(t, c.raw.Bytes())
	}
	require.Panics(t, func() {
		s.dispatchCommand(c, cmdQuit, func(conn redcon.Conn, cmd redcon.Command) {
			s.quit(conn, cmd)
			require.Zero(t, c.closes)
			require.Empty(t, c.pipeline, "already parsed trailing commands are removed before close")
			panic("QUIT handler panic")
		}, redcon.Command{Args: [][]byte{[]byte(cmdQuit)}}, time.Now())
	})
	require.Empty(t, c.raw.Bytes())
	require.Equal(t, 1, c.closes)
	require.Equal(t, 1, owner.finishes)
	require.Equal(t, 1, owner.joins)
}
