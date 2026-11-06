package adapter

import (
	"context"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/redcon"
)

func redisBudgetUsed(s *RedisServer) int {
	s.wireTxnMu.Lock()
	defer s.wireTxnMu.Unlock()
	return s.wireTxnBytes
}

func requireRedisBudgetReleased(t *testing.T, s *RedisServer) {
	t.Helper()
	require.Eventually(t, func() bool { return redisBudgetUsed(s) == 0 }, 3*time.Second, time.Millisecond)
}

func redisBudgetFrame(args ...string) []byte {
	out := redcon.AppendArray(nil, len(args))
	for _, arg := range args {
		out = redcon.AppendBulkString(out, arg)
	}
	return out
}

func TestRedisWireBudgetConcurrentReservations(t *testing.T) {
	t.Parallel()
	s := &RedisServer{wireTxnBudgetLimit: 4 * redisWireTxnBaseBytes}
	const clients = 32
	var admitted atomic.Int32
	start, release := make(chan struct{}), make(chan struct{})
	var ready, done sync.WaitGroup
	ready.Add(clients)
	done.Add(clients)
	for range clients {
		go func() {
			defer done.Done()
			<-start
			ok := s.reserveWireTxnBytes(redisWireTxnBaseBytes)
			if ok {
				admitted.Add(1)
			}
			ready.Done()
			<-release
			if ok {
				s.releaseWireTxnBytes(redisWireTxnBaseBytes)
			}
		}()
	}
	close(start)
	ready.Wait()
	require.EqualValues(t, 4, admitted.Load())
	require.Equal(t, s.wireTxnBudgetLimit, redisBudgetUsed(s))
	close(release)
	done.Wait()
	require.Zero(t, redisBudgetUsed(s))
	require.Panics(t, func() { s.releaseWireTxnBytes(1) }, "accounting bugs must not be silently saturated")
}

func TestRedisWireBudgetChargesEveryRetainedCopy(t *testing.T) {
	t.Parallel()
	cmd := redcon.Command{Raw: redisBudgetFrame(cmdSet, "k", "value"), Args: [][]byte{[]byte(cmdSet), []byte("k"), []byte("value")}}
	size := queuedRedisCommandSize(cmd)
	require.Greater(t, size, len(cmd.Raw)+len("SETkvalue")+redisQueuedArgOverhead*len(cmd.Args))
	capacity, growth := redisQueueCapacityGrowth(nil)
	require.Equal(t, 1, capacity)
	require.GreaterOrEqual(t, growth, redisQueuedCommandOverhead)
	queue := make([]redcon.Command, 1, capacity)
	next, more := redisQueueCapacityGrowth(queue)
	require.Equal(t, 2, next)
	require.Positive(t, more)
	queue = make([]redcon.Command, 1, next)
	_, more = redisQueueCapacityGrowth(queue)
	require.Zero(t, more, "unused capacity must not be charged twice")
	s, _ := redisPropagationServer(t)
	s.wireTxnBudgetLimit = redisWireTxnBaseBytes + size + growth
	c := &redisOwnedTestConn{}
	s.handleRedisCommand(c, redcon.Command{Args: [][]byte{[]byte(cmdMulti)}})
	s.handleRedisCommand(c, cmd)
	require.Equal(t, s.wireTxnBudgetLimit, redisBudgetUsed(s), "exact boundary accepts all copies and capacity")
	s.handleRedisCommand(c, cmd)
	require.True(t, getConnState(c).wireTxn.closed)
	require.Empty(t, getConnState(c).queue, "overflow must drop the accepted prefix, not just set EXECABORT")
	requireRedisBudgetReleased(t, s)
}

func TestRedisWireBudgetAdmissionFailureReleasesHolder(t *testing.T) {
	t.Parallel()
	for _, failure := range []string{"nil", "context", "error", "closing"} {
		t.Run(failure, func(t *testing.T) {
			o := &redisTestOwnership{ctx: context.Background()}
			s := redisOwnedTestServer(o)
			t.Cleanup(func() { require.NoError(t, s.Close()) })
			s.requestAdmission = func(context.Context) (RequestOwnership, error) {
				switch failure {
				case "nil":
					return nil, nil
				case "context":
					o.ctx = nil
				case "error":
					return o, errRedisAdmission
				case "closing":
					s.baseCancel()
				}
				return o, nil
			}
			s.handleRedisCommand(&redisOwnedTestConn{}, redcon.Command{Args: [][]byte{[]byte(cmdMulti)}})
			require.Zero(t, redisBudgetUsed(s))
			require.Empty(t, s.wireTxns)
			if failure != "nil" {
				require.Equal(t, 1, o.joins)
			}
		})
	}
}

func TestRedisWireBudgetPerConnectionAndCountOverflowAreTerminal(t *testing.T) {
	t.Parallel()
	for _, boundary := range []string{"bytes", "count", "arguments"} {
		t.Run(boundary, func(t *testing.T) {
			s, owner := redisPropagationServer(t)
			// Let the per-connection guard, not the aggregate cap, reject bytes.
			s.wireTxnBudgetLimit = 2 * redisMaxReplyBytes
			c := &redisOwnedTestConn{}
			s.handleRedisCommand(c, redcon.Command{Args: [][]byte{[]byte(cmdMulti)}})
			switch boundary {
			case "bytes":
				// This input already exists before enqueue. Its conservative clone
				// charge exceeds the cap; no copy of the large argument is allowed.
				cmd := redcon.Command{Args: [][]byte{[]byte(cmdSet), []byte("k"), make([]byte, redisMaxReplyBytes/2)}}
				s.handleRedisCommand(c, cmd)
			case "count":
				cmd := redcon.Command{Args: [][]byte{[]byte(cmdPing)}}
				for range redisMaxQueuedCommands + 1 {
					s.handleRedisCommand(c, cmd)
				}
			case "arguments":
				s.wireTxnBudgetLimit = redisWireTxnBaseBytes + 1024
				cmd := redcon.Command{Args: make([][]byte, 1024)}
				cmd.Args[0], cmd.Args[1], cmd.Args[2] = []byte(cmdSet), []byte("k"), []byte("v")
				s.handleRedisCommand(c, cmd)
			}
			txn := getConnState(c).wireTxn
			require.True(t, txn.closed)
			require.True(t, txn.poisoned)
			require.Empty(t, getConnState(c).queue)
			require.Zero(t, redisBudgetUsed(s))
			require.Equal(t, 1, owner.finishes)
			require.Equal(t, 1, owner.joins)
		})
	}
}

func TestRedisWireBudgetOverflowPoisonsBufferedPipeline(t *testing.T) {
	t.Parallel()
	for _, boundary := range []string{"holder", "queue"} {
		t.Run(boundary, func(t *testing.T) {
			st := newRedisWireStore(t)
			g := newRedisWireTestGate(t)
			limit := redisWireTxnBaseBytes
			if boundary == "queue" {
				limit += 3000
			}
			s := startRedisWireServer(t, st, newLocalAdapterCoordinator(st), g, func(s *RedisServer) { s.wireTxnBudgetLimit = limit })
			holder := dialRedisWire(t, s)
			holder.send(t, []string{cmdMulti})
			holder.want(t, "+OK\r\n")
			o := g.next(t)
			attacker := holder
			if boundary == "holder" {
				attacker = dialRedisWire(t, s)
				attacker.send(t, []string{cmdMulti}, []string{cmdSet, "escaped", "bad"}, []string{cmdExec}, []string{cmdSet, "escaped-again", "bad"})
			} else {
				value := strings.Repeat("x", 512)
				holder.send(t, []string{cmdSet, "accepted-prefix", value})
				holder.want(t, "+QUEUED\r\n")
				attacker.send(t, []string{cmdSet, "overflow", value}, []string{cmdExec}, []string{cmdSet, "escaped", "bad"},
					[]string{cmdDiscard}, []string{cmdSet, "escaped-again", "bad"})
			}
			attacker.want(t, "-"+errRedisTxnQueueLimit.Error()+"\r\n")
			_, err := attacker.r.ReadByte()
			require.ErrorIs(t, err, io.EOF, "overflow must close promptly, not wait for an idle/read timeout")
			require.Empty(t, g.owners, "buffered EXEC/SET after overflow must not mint fresh H")
			if boundary == "holder" {
				require.Equal(t, redisWireTxnBaseBytes, redisBudgetUsed(s))
				holder.send(t, []string{cmdDiscard})
				holder.want(t, "+OK\r\n")
			}
			requireRedisWireJoined(t, o)
			requireRedisBudgetReleased(t, s)
			for _, key := range []string{"accepted-prefix", "overflow", "escaped", "escaped-again"} {
				_, err := st.GetAt(t.Context(), redisStrKey([]byte(key)), ^uint64(0))
				require.ErrorIs(t, err, store.ErrKeyNotFound)
			}
			// Resource exhaustion must not disable healthy clients after release.
			control := dialRedisWire(t, s)
			control.send(t, []string{cmdMulti}, []string{cmdDiscard})
			control.want(t, "+OK\r\n+OK\r\n")
			requireRedisWireJoined(t, g.next(t))
			requireRedisBudgetReleased(t, s)
		})
	}
}

func TestRedisWireBudgetConcurrentMaliciousConnections(t *testing.T) {
	t.Parallel()
	const clients = 8
	st := newRedisWireStore(t)
	g := newRedisWireTestGate(t)
	limit := clients*redisWireTxnBaseBytes + 64<<10
	s := startRedisWireServer(t, st, newLocalAdapterCoordinator(st), g, func(s *RedisServer) {
		s.wireTxnBudgetLimit = limit
		s.executionBudget.limit = clients // Isolate the queue budget from the separate command-execution gate.
	})
	connections := make([]*redisWireClient, clients)
	owners := make([]*redisWireTestOwner, clients)
	for i := range connections {
		connections[i] = dialRedisWire(t, s)
		connections[i].send(t, []string{cmdMulti})
		connections[i].want(t, "+OK\r\n")
		owners[i] = g.next(t)
	}
	start := make(chan struct{})
	var workers sync.WaitGroup
	var accepted, rejected atomic.Int32
	frame := redisBudgetFrame(cmdSet, "never-committed", strings.Repeat("x", 8<<10))
	workers.Add(clients)
	for _, c := range connections {
		go func() {
			defer workers.Done()
			<-start
			if _, err := c.Write(frame); err != nil {
				return
			}
			line, err := c.r.ReadString('\n')
			if err != nil {
				return
			}
			switch line {
			case "+QUEUED\r\n":
				accepted.Add(1)
			case "-" + errRedisTxnQueueLimit.Error() + "\r\n":
				rejected.Add(1)
			}
		}()
	}
	close(start)
	workers.Wait()
	require.EqualValues(t, clients, accepted.Load()+rejected.Load())
	require.Positive(t, accepted.Load())
	require.Positive(t, rejected.Load(), "all per-connection queues must contend for the same cap")
	require.LessOrEqual(t, redisBudgetUsed(s), limit)
	for _, c := range connections {
		_ = c.Close()
	}
	for _, o := range owners {
		requireRedisWireJoined(t, o)
	}
	requireRedisBudgetReleased(t, s)
}

func TestRedisWireBudgetReleaseLifecycle(t *testing.T) {
	t.Parallel()
	for _, exit := range []string{"exec", "discard", "error", "idle", "disconnect", "close", "revoke"} {
		t.Run(exit, func(t *testing.T) {
			st := newRedisWireStore(t)
			g := newRedisWireTestGate(t)
			s := startRedisWireServer(t, st, newLocalAdapterCoordinator(st), g, func(s *RedisServer) {
				if exit == "idle" {
					s.wireTxnIdleTimeout = 250 * time.Millisecond
				}
			})
			c := dialRedisWire(t, s)
			c.send(t, []string{cmdMulti}, []string{cmdSet, "k", "v"})
			c.want(t, "+OK\r\n+QUEUED\r\n")
			o := g.next(t)
			require.Greater(t, redisBudgetUsed(s), redisWireTxnBaseBytes)
			switch exit {
			case "exec":
				c.send(t, []string{cmdExec})
				c.want(t, "*1\r\n+OK\r\n")
			case "discard":
				c.send(t, []string{cmdDiscard})
				c.want(t, "+OK\r\n")
			case "error":
				c.send(t, []string{"UNKNOWN"}, []string{cmdExec})
				line, err := c.r.ReadString('\n')
				require.NoError(t, err)
				require.Contains(t, line, "unsupported command")
				c.want(t, "-"+errRedisExecAbort.Error()+"\r\n")
			case "disconnect":
				require.NoError(t, c.Close())
			case "close":
				require.NoError(t, s.Close())
			case "revoke":
				g.native.Term++
				g.observe(t)
			}
			requireRedisWireJoined(t, o)
			requireRedisBudgetReleased(t, s)
		})
	}
}

func TestRedisWireBudgetRetainedThroughDetachedExec(t *testing.T) {
	t.Parallel()
	st := newRedisWireStore(t)
	g := newRedisWireTestGate(t)
	local := newLocalAdapterCoordinator(st)
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	defer unblock()
	coord := &redisWireDispatchCoordinator{Coordinator: local, dispatch: func(ctx context.Context, ops *kv.OperationGroup[kv.OP]) (*kv.CoordinateResponse, error) {
		close(entered)
		<-release
		return local.Dispatch(ctx, ops)
	}}
	s := startRedisWireServer(t, st, coord, g, func(s *RedisServer) { s.wireTxnBudgetLimit = redisWireTxnBaseBytes + 3000 })
	c := dialRedisWire(t, s)
	c.send(t, []string{cmdMulti}, []string{cmdSet, "k", "v"})
	c.want(t, "+OK\r\n+QUEUED\r\n")
	o := g.next(t)
	reserved := redisBudgetUsed(s)
	c.send(t, []string{cmdExec})
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("EXEC did not enter")
	}
	require.Equal(t, reserved, redisBudgetUsed(s), "state.queue=nil during EXEC must not release live clones")
	other := dialRedisWire(t, s)
	other.send(t, []string{cmdMulti})
	other.want(t, "-"+errRedisTxnQueueLimit.Error()+"\r\n")
	require.Empty(t, g.owners)
	unblock()
	c.want(t, "*1\r\n+OK\r\n")
	requireRedisWireJoined(t, o)
	requireRedisBudgetReleased(t, s)
}

func TestRedisWireBudgetRetainedThroughChildJoin(t *testing.T) {
	t.Parallel()
	st := newRedisWireStore(t)
	g := newRedisWireTestGate(t)
	s := startRedisWireServer(t, st, newLocalAdapterCoordinator(st), g)
	c := dialRedisWire(t, s)
	c.send(t, []string{cmdMulti}, []string{cmdSet, "k", "v"})
	c.want(t, "+OK\r\n+QUEUED\r\n")
	o := g.next(t)
	reserved := redisBudgetUsed(s)
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	defer unblock()
	require.NoError(t, o.op.Go(func(ctx context.Context) {
		<-ctx.Done()
		close(entered)
		<-release
	}))
	closed := make(chan struct{})
	go func() { _ = s.Close(); close(closed) }()
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("child was not canceled")
	}
	require.Equal(t, reserved, redisBudgetUsed(s), "owned child can still retain queued arguments until its join")
	unblock()
	select {
	case <-closed:
	case <-time.After(3 * time.Second):
		t.Fatal("Close did not join")
	}
	requireRedisWireJoined(t, o)
	requireRedisBudgetReleased(t, s)
}

func TestRedisWireBudgetPanicAndLegacyControl(t *testing.T) {
	t.Parallel()
	s, owner := redisPropagationServer(t)
	c := &redisOwnedTestConn{}
	s.handleRedisCommand(c, redcon.Command{Args: [][]byte{[]byte(cmdMulti)}})
	s.handleRedisCommand(c, redcon.Command{Args: [][]byte{[]byte(cmdSet), []byte("k"), []byte("v")}})
	s.route[cmdExec] = func(redcon.Conn, redcon.Command) { panic("test handler panic") }
	require.Panics(t, func() { s.handleRedisCommand(c, redcon.Command{Args: [][]byte{[]byte(cmdExec)}}) })
	requireRedisBudgetReleased(t, s)
	require.Equal(t, 1, owner.joins)
	legacy, st := newRedisStorageMigrationTestServer(t)
	t.Cleanup(func() {
		require.NoError(t, legacy.Close())
		require.NoError(t, st.Close())
	})
	legacy.wireTxnBudgetLimit = 1
	control := &redisOwnedTestConn{}
	for _, args := range [][][]byte{{[]byte(cmdMulti)}, {[]byte(cmdSet), []byte("k"), []byte("v")}, {[]byte(cmdExec)}} {
		legacy.handleRedisCommand(control, redcon.Command{Args: args})
	}
	require.Zero(t, redisBudgetUsed(legacy), "nil-hook legacy must not acquire protected reservations")
	value, err := st.GetAt(t.Context(), redisStrKey([]byte("k")), ^uint64(0))
	require.NoError(t, err)
	decoded, _, err := decodeRedisStr(value)
	require.NoError(t, err)
	require.Equal(t, "v", string(decoded))
}
