package adapter

import (
	"bufio"
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bootjp/elastickv/internal/peersession"
	"github.com/bootjp/elastickv/internal/raftadmission"
	"github.com/bootjp/elastickv/internal/raftbootstrap"
	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/monitoring"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/redcon"
)

type redisWireTestOwner struct {
	a        *raftadmission.Arbiter
	op       *raftadmission.Operation
	ctx      context.Context
	finishes atomic.Int32
	joins    atomic.Int32
	done     chan struct{}
}

func (o *redisWireTestOwner) Context() context.Context { return o.ctx }
func (o *redisWireTestOwner) Validate() error          { return o.a.ValidateResult(o.ctx) }
func (o *redisWireTestOwner) Finish(err error) error {
	o.finishes.Add(1)
	return errors.Join(err, o.op.Finish())
}
func (o *redisWireTestOwner) CancelAndJoin() {
	o.op.CancelAndJoin()
	if o.joins.Add(1) == 1 {
		close(o.done)
	}
}

type redisWireTestGate struct {
	a      *raftadmission.Arbiter
	native raftengine.NativeState
	owners chan *redisWireTestOwner
}

func newRedisWireTestGate(t *testing.T) *redisWireTestGate {
	t.Helper()
	a, err := raftadmission.New(raftadmission.Config{Cluster: raftbootstrap.UUID{1}, DefaultGroup: 1,
		Groups: []uint64{1}, ValidatePayload: func(raftadmission.Kind, uint64, []byte) error { return nil }})
	require.NoError(t, err)
	t.Cleanup(a.Close)
	g := &redisWireTestGate{a: a, owners: make(chan *redisWireTestOwner, 128),
		native: raftengine.NativeState{Term: 1, Role: raftengine.StateLeader, LeaderID: 1, ConfigurationIndex: 1}}
	g.observe(t)
	g.publish(t, 1)
	return g
}

func (g *redisWireTestGate) observe(t *testing.T) {
	t.Helper()
	require.NoError(t, g.a.WithNative(t.Context(), 1, func(s raftengine.NativeScope) error {
		s.Observe(g.native)
		return nil
	}))
}

func (g *redisWireTestGate) publish(t *testing.T, session uint64) {
	t.Helper()
	c := raftbootstrap.Configuration{Index: g.native.ConfigurationIndex, State: raftbootstrap.ConfState{Voters: []uint64{1}},
		Members: []raftbootstrap.Member{{ID: 1, Address: "local:9000", PublicKeyDigest: sha256.Sum256([]byte("local key"))}}}
	digest, err := raftbootstrap.ConfigurationDigest(c)
	require.NoError(t, err)
	policy := raftbootstrap.PolicyRecord{Version: raftbootstrap.PolicyVersion, Identity: raftbootstrap.Identity{Cluster: raftbootstrap.UUID{1}, Group: 1},
		BaselineDigest: digest, Minimum: raftbootstrap.Versions{Entry: 1, Snapshot: 1, Session: 1}, ActivationIndex: 1,
		Generation: 1, LastConfigurationIndex: c.Index, Configuration: c}
	ticket, err := g.a.Ticket(1)
	require.NoError(t, err)
	require.NoError(t, g.a.PublishRecovery(ticket, policy, session, raftadmission.RegistrationNotConfigured))
	ticket, err = g.a.Ticket(1)
	require.NoError(t, err)
	require.NoError(t, g.a.MarkCatalogLoaded(ticket))
}

func (g *redisWireTestGate) admit(ctx context.Context) (RequestOwnership, error) {
	ctx, op, err := g.a.Begin(ctx, raftadmission.Data)
	if err != nil {
		return nil, err
	}
	o := &redisWireTestOwner{a: g.a, op: op, ctx: ctx, done: make(chan struct{})}
	o.ctx = context.WithValue(ctx, redisOwnershipTestKey{}, o)
	g.owners <- o
	return o, nil
}

func (g *redisWireTestGate) next(t *testing.T) *redisWireTestOwner {
	t.Helper()
	select {
	case o := <-g.owners:
		return o
	case <-time.After(3 * time.Second):
		t.Fatal("admission was not captured")
		return nil
	}
}

func requireRedisWireJoined(t *testing.T, o *redisWireTestOwner) {
	t.Helper()
	select {
	case <-o.done:
	case <-time.After(3 * time.Second):
		t.Fatal("retained transaction did not join")
	}
	require.Eventually(t, func() bool { return o.finishes.Load() == 1 }, time.Second, time.Millisecond)
	require.EqualValues(t, 1, o.joins.Load())
}

func startRedisWireServer(t *testing.T, st store.MVCCStore, coord kv.Coordinator, g *redisWireTestGate, options ...RedisServerOption) *RedisServer {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	options = append([]RedisServerOption{WithRedisRequestAdmission(g.admit), WithOnePhaseTxnDedup(false),
		WithRedisRequestObserver(monitoring.NewRegistry("wire-transaction", "local").RedisObserver())}, options...)
	s := NewRedisServer(l, l.Addr().String(), st, coord, nil, nil, options...)
	done := make(chan error, 1)
	go func() { done <- s.Run() }()
	t.Cleanup(func() {
		s.Stop()
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			t.Error("Redis server did not stop")
		}
	})
	return s
}

type redisWireClient struct {
	net.Conn
	r *bufio.Reader
}

func dialRedisWire(t *testing.T, s *RedisServer) *redisWireClient {
	t.Helper()
	c, err := net.Dial("tcp", s.listen.Addr().String())
	require.NoError(t, err)
	require.NoError(t, c.SetDeadline(time.Now().Add(10*time.Second)))
	t.Cleanup(func() { _ = c.Close() })
	return &redisWireClient{Conn: c, r: bufio.NewReader(c)}
}

func (c *redisWireClient) send(t *testing.T, commands ...[]string) {
	t.Helper()
	var b strings.Builder
	for _, args := range commands {
		fmt.Fprintf(&b, "*%d\r\n", len(args))
		for _, arg := range args {
			fmt.Fprintf(&b, "$%d\r\n%s\r\n", len(arg), arg)
		}
	}
	_, err := io.WriteString(c.Conn, b.String())
	require.NoError(t, err)
}

func (c *redisWireClient) want(t *testing.T, expected string) {
	t.Helper()
	got := make([]byte, len(expected))
	_, err := io.ReadFull(c.r, got)
	require.NoError(t, err)
	require.Equal(t, expected, string(got))
}

func newRedisWireStore(t *testing.T) store.MVCCStore {
	t.Helper()
	st := store.NewMVCCStore()
	t.Cleanup(func() { require.NoError(t, st.Close()) })
	return st
}

func TestRedisWireTransactionHealthyPipelineAndDiscard(t *testing.T) {
	t.Parallel()
	st := newRedisWireStore(t)
	g := newRedisWireTestGate(t)
	s := startRedisWireServer(t, st, newLocalAdapterCoordinator(st), g, WithOnePhaseTxnDedup(true))
	c := dialRedisWire(t, s)
	c.send(t, []string{cmdMulti}, []string{cmdSet, "k", "v"}, []string{cmdGet, "k"}, []string{cmdExec})
	c.want(t, "+OK\r\n+QUEUED\r\n+QUEUED\r\n*2\r\n+OK\r\n$1\r\nv\r\n")
	requireRedisWireJoined(t, g.next(t))
	require.Empty(t, g.owners, "queued commands and EXEC must not mint new H")
	c.send(t, []string{cmdMulti}, []string{cmdSet, "k", "lost"}, []string{cmdDiscard}, []string{cmdGet, "k"})
	c.want(t, "+OK\r\n+QUEUED\r\n+OK\r\n$1\r\nv\r\n")
	requireRedisWireJoined(t, g.next(t))
	requireRedisWireJoined(t, g.next(t)) // Subsequent GET is a new request.
	c.send(t, []string{cmdMulti}, []string{cmdIncr, "k"}, []string{cmdGet, "k"}, []string{cmdExec}, []string{cmdPing})
	c.want(t, "+OK\r\n+QUEUED\r\n+QUEUED\r\n*2\r\n-ERR value is not an integer or out of range\r\n$1\r\nv\r\n+PONG\r\n")
	requireRedisWireJoined(t, g.next(t))
	requireRedisWireJoined(t, g.next(t))
}

func TestRedisWireTransactionNativeRevocationBeforeExec(t *testing.T) {
	t.Parallel()
	for _, change := range []string{"term", "configuration"} {
		t.Run(change, func(t *testing.T) {
			st := newRedisWireStore(t)
			g := newRedisWireTestGate(t)
			s := startRedisWireServer(t, st, newLocalAdapterCoordinator(st), g)
			c := dialRedisWire(t, s)
			c.send(t, []string{cmdMulti}, []string{cmdSet, "stale", "never"})
			c.want(t, "+OK\r\n+QUEUED\r\n")
			o := g.next(t)
			if change == "term" {
				g.native.Term++
			} else {
				g.native.ConfigurationIndex++
			}
			g.observe(t)
			requireRedisWireJoined(t, o)
			g.publish(t, 2)
			require.Equal(t, peersession.DataReady, g.a.Stage(1), "idle MULTI must not pin readiness after revocation")
			_, _ = io.WriteString(c.Conn, "EXEC\r\nSET stale newer\r\n")
			_, err := c.r.ReadByte()
			require.Error(t, err, "revoked receiver connection must close, not remint")
			require.Empty(t, g.owners)
			_, err = st.GetAt(t.Context(), redisStrKey([]byte("stale")), ^uint64(0))
			require.ErrorIs(t, err, store.ErrKeyNotFound)
		})
	}
}

func TestRedisWireTransactionLifecycle(t *testing.T) {
	t.Parallel()
	for _, exit := range []string{"disconnect", "close", "idle"} {
		t.Run(exit, func(t *testing.T) {
			st := newRedisWireStore(t)
			g := newRedisWireTestGate(t)
			option := func(s *RedisServer) { s.wireTxnIdleTimeout = 20 * time.Millisecond }
			s := startRedisWireServer(t, st, newLocalAdapterCoordinator(st), g, option)
			c := dialRedisWire(t, s)
			c.send(t, []string{cmdMulti})
			c.want(t, "+OK\r\n")
			o := g.next(t)
			switch exit {
			case "disconnect":
				require.NoError(t, c.Close())
			case "close":
				require.NoError(t, s.Close())
			}
			requireRedisWireJoined(t, o)
			require.Eventually(t, func() bool {
				s.wireTxnMu.Lock()
				defer s.wireTxnMu.Unlock()
				return len(s.wireTxns) == 0
			}, time.Second, time.Millisecond)
		})
	}
}

type redisWireDispatchCoordinator struct {
	kv.Coordinator
	dispatch func(context.Context, *kv.OperationGroup[kv.OP]) (*kv.CoordinateResponse, error)
}

func (c *redisWireDispatchCoordinator) Dispatch(ctx context.Context, ops *kv.OperationGroup[kv.OP]) (*kv.CoordinateResponse, error) {
	return c.dispatch(ctx, ops)
}

func TestRedisWireTransactionRevokedDuringCommit(t *testing.T) {
	t.Parallel()
	st := newRedisWireStore(t)
	g := newRedisWireTestGate(t)
	local := newLocalAdapterCoordinator(st)
	entered, release := make(chan struct{}), make(chan struct{})
	coord := &redisWireDispatchCoordinator{Coordinator: local, dispatch: func(ctx context.Context, ops *kv.OperationGroup[kv.OP]) (*kv.CoordinateResponse, error) {
		require.NoError(t, g.a.ValidateResult(ctx))
		result, err := local.Dispatch(ctx, ops)
		close(entered)
		<-release // Simulate a lost commit reply after the actual mutation.
		return result, err
	}}
	s := startRedisWireServer(t, st, coord, g)
	c := dialRedisWire(t, s)
	c.send(t, []string{cmdMulti}, []string{cmdSet, "committed", "v"})
	c.want(t, "+OK\r\n+QUEUED\r\n")
	o := g.next(t)
	c.send(t, []string{cmdExec})
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("commit did not start")
	}
	g.native.Term++
	g.observe(t)
	close(release)
	line, err := c.r.ReadString('\n')
	require.NoError(t, err)
	require.Contains(t, line, kv.ErrTransactionOutcomeUnknown.Error())
	require.NotContains(t, line, "+OK")
	requireRedisWireJoined(t, o)
	value, err := st.GetAt(t.Context(), redisStrKey([]byte("committed")), ^uint64(0))
	require.NoError(t, err)
	decoded, _, err := decodeRedisStr(value)
	require.NoError(t, err)
	require.Equal(t, "v", string(decoded))
}

func TestRedisWireTransactionQueueBoundsAndErrors(t *testing.T) {
	t.Parallel()
	s, owner := redisPropagationServer(t)
	c := &redisOwnedTestConn{}
	command := func(args ...string) {
		cmd := redcon.Command{}
		for _, arg := range args {
			cmd.Args = append(cmd.Args, []byte(arg))
		}
		s.handleRedisCommand(c, cmd)
	}
	command(cmdMulti)
	s.wireTxnBudgetLimit = redisWireTxnBaseBytes
	command(cmdSet, "k", "never")
	command(cmdExec)
	require.Len(t, c.writes, 2)
	require.Contains(t, c.writes[0].s, "queue limit exceeded")
	require.Contains(t, c.writes[1].s, errRedisAdmission.Error())
	require.True(t, getConnState(c).wireTxn.closed)
	require.Equal(t, 1, owner.finishes)
	require.Equal(t, 1, owner.joins)
}

func TestRedisWireTransactionDisconnectDuringCommitRetainsOwnerUntilJoin(t *testing.T) {
	t.Parallel()
	st := newRedisWireStore(t)
	g := newRedisWireTestGate(t)
	local := newLocalAdapterCoordinator(st)
	entered, release := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	defer unblock()
	coord := &redisWireDispatchCoordinator{Coordinator: local, dispatch: func(ctx context.Context, ops *kv.OperationGroup[kv.OP]) (*kv.CoordinateResponse, error) {
		close(entered)
		<-release
		return local.Dispatch(ctx, ops)
	}}
	s := startRedisWireServer(t, st, coord, g)
	c := dialRedisWire(t, s)
	c.send(t, []string{cmdMulti}, []string{cmdSet, "k", "v"})
	c.want(t, "+OK\r\n+QUEUED\r\n")
	o := g.next(t)
	c.send(t, []string{cmdExec})
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("commit did not enter")
	}
	require.NoError(t, c.Close())
	select {
	case <-o.done:
		t.Error("disconnect abandoned executing work before its join")
	default:
	}
	unblock()
	requireRedisWireJoined(t, o)
	require.Empty(t, g.owners)
}

func TestRedisWireTransactionCloseJoinsOwnedChildren(t *testing.T) {
	t.Parallel()
	st := newRedisWireStore(t)
	g := newRedisWireTestGate(t)
	s := startRedisWireServer(t, st, newLocalAdapterCoordinator(st), g)
	c := dialRedisWire(t, s)
	c.send(t, []string{cmdMulti})
	c.want(t, "+OK\r\n")
	o := g.next(t)
	entered, release := make(chan struct{}), make(chan struct{})
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
		t.Fatal("owned child was not canceled")
	}
	select {
	case <-closed:
		t.Error("Close returned before owned child joined")
	default:
	}
	close(release)
	select {
	case <-closed:
	case <-time.After(3 * time.Second):
		t.Fatal("Close did not join")
	}
	requireRedisWireJoined(t, o)
}

func TestRedisWireTransactionIdleCancelsOwnerChildren(t *testing.T) {
	t.Parallel()
	st := newRedisWireStore(t)
	g := newRedisWireTestGate(t)
	s := startRedisWireServer(t, st, newLocalAdapterCoordinator(st), g, func(s *RedisServer) {
		s.wireTxnIdleTimeout = 250 * time.Millisecond
	})
	c := dialRedisWire(t, s)
	c.send(t, []string{cmdMulti})
	c.want(t, "+OK\r\n")
	o := g.next(t)
	childDone := make(chan struct{})
	require.NoError(t, o.op.Go(func(ctx context.Context) {
		<-ctx.Done()
		close(childDone)
	}))
	requireRedisWireJoined(t, o)
	select {
	case <-childDone:
	default:
		t.Fatal("idle timeout released ownership before canceling its child")
	}
}

func TestRedisWireTransactionHeavyPoolFullFinishesOwner(t *testing.T) {
	t.Parallel()
	st := newRedisWireStore(t)
	g := newRedisWireTestGate(t)
	limiter := newRedisHeavyCommandLimiter(1)
	limiter.slots <- struct{}{}
	s := startRedisWireServer(t, st, newLocalAdapterCoordinator(st), g, func(s *RedisServer) {
		s.heavyCommandLimiter = limiter
	})
	c := dialRedisWire(t, s)
	c.send(t, []string{cmdMulti}, []string{cmdHGetAll, "hash"}, []string{cmdExec})
	c.want(t, "+OK\r\n+QUEUED\r\n-"+errRedisHeavyCommandPoolFull.Error()+"\r\n")
	requireRedisWireJoined(t, g.next(t))
	require.Empty(t, g.owners)
}

func TestRedisWireTransactionPreservesTypedUnknown(t *testing.T) {
	t.Parallel()
	s, owner := redisPropagationServer(t)
	calls := 0
	s.coordinator = &redisWireDispatchCoordinator{Coordinator: s.coordinator, dispatch: func(context.Context, *kv.OperationGroup[kv.OP]) (*kv.CoordinateResponse, error) {
		calls++
		return nil, kv.ErrTransactionOutcomeUnknown
	}}
	c := &redisOwnedTestConn{}
	for _, args := range [][][]byte{{[]byte(cmdMulti)}, {[]byte(cmdSet), []byte("k"), []byte("v")}, {[]byte(cmdExec)}} {
		s.handleRedisCommand(c, redcon.Command{Args: args})
	}
	require.Equal(t, 1, calls)
	require.ErrorIs(t, owner.original, kv.ErrTransactionOutcomeUnknown)
	require.Len(t, c.writes, 1)
	require.Equal(t, "ERR operation outcome unknown", c.writes[0].s)
	require.Equal(t, 1, owner.finishes)
	require.Equal(t, 1, owner.joins)
}

func TestRedisWireTransactionProxyReplyTypes(t *testing.T) {
	t.Parallel()
	queue := []redcon.Command{
		{Args: [][]byte{[]byte(cmdSet), []byte("k"), []byte("v")}},
		{Args: [][]byte{[]byte(cmdHMSet), []byte("h"), []byte("f"), []byte("v")}},
		{Args: [][]byte{[]byte(cmdGet), []byte("literal-ok")}},
		{Args: [][]byte{[]byte(cmdSet), []byte("k"), []byte("v"), []byte("GET")}},
		{Args: [][]byte{[]byte(cmdIncr), []byte("bad")}},
		{Args: [][]byte{[]byte(cmdSet), []byte("k"), []byte("v"), []byte("NX")}},
	}
	cmds := make([]*redis.Cmd, len(queue))
	for i := range cmds {
		cmds[i] = redis.NewCmd(t.Context())
		cmds[i].SetVal("OK")
	}
	cmds[4].SetErr(wrongTypeError())
	cmds[5].SetErr(redis.Nil)
	c := &redisOwnedConn{writer: &redisReplyBuffer{}}
	writeOwnedProxyTxnResults(c, queue, cmds)
	require.Equal(t, "*6\r\n+OK\r\n+OK\r\n$2\r\nOK\r\n$2\r\nOK\r\n-"+wrongTypeError().Error()+"\r\n$-1\r\n", string(c.writer.Buffer()))
	require.NoError(t, c.resultErr, "per-element errors must not become a top-level failure")
}

func TestRedisOwnedProxyOverflowUnknownClassification(t *testing.T) {
	t.Parallel()
	for _, failure := range []error{errRedisReplyTooLarge, errRedisReplyBudget} {
		recorder := &redisOwnedTestConn{}
		target := &redisOwnedConn{Conn: recorder, writer: &redisReplyBuffer{err: failure},
			mayWrite: true, workStarted: true}
		target.writeTerminalFailure()
		require.Len(t, recorder.writes, 1)
		message := recorder.writes[0].s
		for _, wireMessage := range []string{message, "ERR " + message} {
			remote := errors.New(wireMessage)
			require.True(t, isProxyTransactionOutcomeUnknown(remote))
			require.ErrorIs(t, ownedProxyTransactionError(remote, nil), kv.ErrTransactionOutcomeUnknown)
			cmd := redis.NewCmd(t.Context())
			cmd.SetErr(remote)
			require.ErrorIs(t, ownedProxyTransactionError(nil, []*redis.Cmd{cmd}), kv.ErrTransactionOutcomeUnknown)
		}
	}
	require.True(t, isProxyTransactionOutcomeUnknown(errors.New("ERR operation outcome unknown")))
	require.ErrorIs(t, ownedProxyTransactionError(errors.New("ERR operation outcome unknown"), nil), kv.ErrTransactionOutcomeUnknown)
	cmd := redis.NewCmd(t.Context())
	cmd.SetErr(errors.New("ERR operation outcome unknown"))
	require.ErrorIs(t, ownedProxyTransactionError(nil, []*redis.Cmd{cmd}), kv.ErrTransactionOutcomeUnknown)
	for _, other := range []string{"prefix " + kv.ErrTransactionOutcomeUnknown.Error(), kv.ErrTransactionOutcomeUnknown.Error() + "; unrelated", kv.ErrTransactionOutcomeUnknown.Error() + "ish", "ERR operation outcome unknownish"} {
		require.False(t, isProxyTransactionOutcomeUnknown(errors.New(other)))
	}
}

func TestRedisOwnedProxyMutatingReplyOverflowIsTerminal(t *testing.T) {
	t.Parallel()
	st := newRedisWireStore(t)
	leaderGate := newRedisWireTestGate(t)
	leader := startRedisWireServer(t, st, newLocalAdapterCoordinator(st), leaderGate)
	seed := dialRedisWire(t, leader)
	const valueSize = 1 << 20
	seed.send(t, []string{cmdSet, "big", strings.Repeat("x", valueSize)})
	seed.want(t, "+OK\r\n")
	requireRedisWireJoined(t, leaderGate.next(t))
	followerStore := newRedisWireStore(t)
	followerGate := newRedisWireTestGate(t)
	followerCoord := newLocalAdapterCoordinator(followerStore)
	followerCoord.leaderSet, followerCoord.leader = true, false
	followerCoord.raftLeader = "leader-raft"
	follower := startRedisWireServer(t, followerStore, followerCoord, followerGate, func(s *RedisServer) {
		s.leaderRedis = map[string]string{"leader-raft": leader.listen.Addr().String()}
	})
	c := dialRedisWire(t, follower)
	commands := [][]string{{cmdMulti}, {cmdSet, "committed", "v"}}
	for range redisMaxReplyBytes/valueSize + 1 {
		commands = append(commands, []string{cmdGet, "big"})
	}
	commands = append(commands, []string{cmdExec})
	c.send(t, commands...)
	c.want(t, "+OK\r\n"+strings.Repeat("+QUEUED\r\n", len(commands)-2))
	line, err := c.r.ReadString('\n')
	require.NoError(t, err)
	require.True(t, strings.HasPrefix(line, "-"), "overflow must be a top-level EXEC error, not an array: %q", line)
	require.Equal(t, "-ERR operation outcome unknown\r\n", line)
	requireRedisWireJoined(t, followerGate.next(t))
	require.Empty(t, followerGate.owners, "overflow must not retry or remint receiver H")
	value, err := st.GetAt(t.Context(), redisStrKey([]byte("committed")), ^uint64(0))
	require.NoError(t, err)
	decoded, _, err := decodeRedisStr(value)
	require.NoError(t, err)
	require.Equal(t, "v", string(decoded), "the mutation committed before reply overflow")
	c.send(t, []string{cmdPing})
	c.want(t, "+PONG\r\n")
}

func TestRedisWireTransactionFollowerProxy(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"healthy", "revoked", "lost-reply", "target-unknown"} {
		t.Run(mode, func(t *testing.T) {
			runRedisWireTransactionFollowerProxy(t, mode)
		})
	}
}

func runRedisWireTransactionFollowerProxy(t *testing.T, mode string) {
	t.Helper()
	st := newRedisWireStore(t)
	leaderGate := newRedisWireTestGate(t)
	local := newLocalAdapterCoordinator(st)
	entered, release := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	defer unblock()
	if mode != "revoked" {
		unblock()
	}
	dispatched := make(chan *redisWireTestOwner, 1)
	coord := &redisWireDispatchCoordinator{Coordinator: local, dispatch: func(ctx context.Context, ops *kv.OperationGroup[kv.OP]) (*kv.CoordinateResponse, error) {
		require.NoError(t, leaderGate.a.ValidateResult(ctx))
		owner, ok := ctx.Value(redisOwnershipTestKey{}).(*redisWireTestOwner)
		require.True(t, ok)
		dispatched <- owner
		result, err := local.Dispatch(ctx, ops)
		close(entered)
		<-release
		if mode == "target-unknown" {
			return nil, kv.ErrTransactionOutcomeUnknown
		}
		return result, err
	}}
	leader := startRedisWireServer(t, st, coord, leaderGate, func(s *RedisServer) {
		if mode == "lost-reply" {
			s.route[cmdExec] = func(conn redcon.Conn, cmd redcon.Command) {
				s.exec(conn, cmd)
				_ = conn.NetConn().Close()
			}
		}
	})
	followerStore := newRedisWireStore(t)
	followerGate := newRedisWireTestGate(t)
	followerCoord := newLocalAdapterCoordinator(followerStore)
	followerCoord.leaderSet, followerCoord.leader = true, false
	followerCoord.raftLeader = "leader-raft"
	follower := startRedisWireServer(t, followerStore, followerCoord, followerGate, func(s *RedisServer) {
		s.leaderRedis = map[string]string{"leader-raft": leader.listen.Addr().String()}
	})
	c := dialRedisWire(t, follower)
	c.send(t, []string{cmdMulti}, []string{cmdSet, "bad-counter", "v"}, []string{cmdIncr, "bad-counter"}, []string{cmdIncr, "counter"})
	c.want(t, "+OK\r\n+QUEUED\r\n+QUEUED\r\n+QUEUED\r\n")
	o := followerGate.next(t)
	c.send(t, []string{cmdExec})
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("proxied transaction did not commit")
	}
	if mode == "revoked" {
		followerGate.native.Term++
		followerGate.observe(t)
	}
	if mode == "healthy" {
		c.want(t, "*3\r\n+OK\r\n-ERR value is not an integer or out of range\r\n:1\r\n")
	} else {
		line, err := c.r.ReadString('\n')
		require.NoError(t, err)
		require.Equal(t, "-ERR operation outcome unknown\r\n", line)
	}
	requireRedisWireJoined(t, o)
	require.Empty(t, followerGate.owners, "proxy must retain MULTI's receiver H")
	requireRedisBudgetReleased(t, follower)
	if mode == "revoked" {
		require.Greater(t, redisBudgetUsed(leader), redisWireTxnBaseBytes, "remote EXEC still retains its own queued copies")
	}
	unblock()
	requireRedisWireJoined(t, <-dispatched)
	requireRedisBudgetReleased(t, leader)
	require.Empty(t, dispatched, "a lost EXEC reply must not be retried")
	value, err := st.GetAt(t.Context(), redisStrKey([]byte("counter")), ^uint64(0))
	require.NoError(t, err)
	decoded, _, err := decodeRedisStr(value)
	require.NoError(t, err)
	require.Equal(t, "1", string(decoded))
}
