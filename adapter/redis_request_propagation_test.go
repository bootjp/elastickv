package adapter

import (
	"context"
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/redcon"
)

type redisContextCheckingCoordinator struct {
	kv.Coordinator
	check func(context.Context)
}

func (c *redisContextCheckingCoordinator) Dispatch(ctx context.Context, g *kv.OperationGroup[kv.OP]) (*kv.CoordinateResponse, error) {
	c.check(ctx)
	return c.Coordinator.Dispatch(ctx, g)
}

func redisPropagationServer(t *testing.T) (*RedisServer, *redisTestOwnership) {
	t.Helper()
	owner := &redisTestOwnership{ctx: context.WithValue(context.Background(), redisOwnershipTestKey{}, "propagated")}
	base := store.NewMVCCStore()
	t.Cleanup(func() { require.NoError(t, base.Close()) })
	check := func(ctx context.Context) {
		require.Equal(t, "propagated", ctx.Value(redisOwnershipTestKey{}))
		require.NoError(t, ctx.Err())
	}
	st := &redisOwnershipReadStore{MVCCStore: base, onRead: check}
	coord := &redisContextCheckingCoordinator{Coordinator: newLocalAdapterCoordinator(base), check: check}
	s := NewRedisServer(nil, "", st, coord, nil, nil, WithRedisRequestAdmission(func(context.Context) (RequestOwnership, error) { return owner, nil }), WithOnePhaseTxnDedup(false))
	t.Cleanup(func() { require.NoError(t, s.Close()) })
	return s, owner
}

func redisPropagationCommand(t *testing.T, s *RedisServer, args ...string) *redisOwnedTestConn {
	t.Helper()
	cmd := redcon.Command{}
	for _, arg := range args {
		cmd.Args = append(cmd.Args, []byte(arg))
	}
	c := &redisOwnedTestConn{}
	s.dispatchCommand(c, args[0], s.route[args[0]], cmd, time.Now())
	require.Empty(t, c.writes, "command %s rejected: %+v", args[0], c.writes)
	return c
}

func TestRedisOwnedStandardCommandPropagation(t *testing.T) {
	t.Parallel()
	s, _ := redisPropagationServer(t)
	cases := []struct {
		args []string
		want string
	}{
		{[]string{cmdSet, "string", "value"}, "+OK\r\n"},
		{[]string{cmdGet, "string"}, "$5\r\nvalue\r\n"},
		{[]string{cmdIncr, "counter"}, ":1\r\n"},
		{[]string{cmdRPush, "list", "a", "b"}, ":2\r\n"},
		{[]string{cmdLRange, "list", "0", "-1"}, "*2\r\n$1\r\na\r\n$1\r\nb\r\n"},
		{[]string{cmdSAdd, "set", "a", "b"}, ":2\r\n"},
		{[]string{cmdSMembers, "set"}, "*2\r\n$1\r\na\r\n$1\r\nb\r\n"},
		{[]string{cmdZAdd, "zset", "1", "a"}, ":1\r\n"},
		{[]string{cmdZRange, "zset", "0", "-1"}, "*1\r\n$1\r\na\r\n"},
		{[]string{cmdElasticKVZRemFast, "zset", "a"}, ":1\r\n"},
		{[]string{cmdHSet, "hash", "f", "v"}, ":1\r\n"},
		{[]string{cmdHGetAll, "hash"}, "*2\r\n$1\r\nf\r\n$1\r\nv\r\n"},
		{[]string{cmdExpire, "string", "20"}, ":1\r\n"},
		{[]string{cmdEval, "redis.call('SET', KEYS[1], ARGV[1]); return redis.call('GET', KEYS[1])", "1", "lua", "result"}, "$6\r\nresult\r\n"},
		{[]string{cmdFlushDB}, "+OK\r\n"},
		{[]string{cmdFlushAll}, "+OK\r\n"},
		{[]string{cmdFlushLegacy}, ":0\r\n"},
	}
	for _, tc := range cases {
		t.Run(tc.args[0], func(t *testing.T) {
			c := redisPropagationCommand(t, s, tc.args...)
			require.Equal(t, tc.want, c.raw.String())
		})
	}
}

func TestRedisOwnedMissingCarrierFailsClosed(t *testing.T) {
	t.Parallel()
	s, _ := redisPropagationServer(t)
	for _, args := range [][]string{{cmdGet, "key"}, {cmdSet, "key", "value"}, {cmdHGet, "key", "field"}, {cmdEval, "return 1", "0"}} {
		c := &redisOwnedTestConn{}
		cmd := redcon.Command{}
		for _, arg := range args {
			cmd.Args = append(cmd.Args, []byte(arg))
		}
		s.route[args[0]](c, cmd)
		require.Empty(t, c.raw.String())
		require.Len(t, c.writes, 1)
		require.Equal(t, errRedisAdmission.Error(), c.writes[0].s)
	}
	ctx := s.handlerContext()
	require.Error(t, ctx.Err())
	require.Error(t, (&txnContext{server: s}).ctxOrBackground().Err())
	require.Error(t, (&luaScriptContext{server: s}).scriptCtx().Err())
}

func TestRedisReplyBufferLimitAndOriginalError(t *testing.T) {
	t.Parallel()
	b := &redisReplyBuffer{}
	b.WriteBulkString(strings.Repeat("x", redisMaxReplyBytes))
	require.ErrorIs(t, b.err, errRedisReplyTooLarge)
	require.Empty(t, b.Buffer())
	b = &redisReplyBuffer{}
	b.WriteArray(2)
	b.WriteBulkFrom(int64(redisMaxReplyBytes), strings.NewReader("not read"))
	require.ErrorIs(t, b.err, errRedisReplyTooLarge)
	require.Empty(t, b.Buffer())
	owner := &redisTestOwnership{ctx: context.Background()}
	s := redisOwnedTestServer(owner)
	c := &redisOwnedTestConn{}
	s.dispatchCommand(c, cmdGet, func(conn redcon.Conn, _ redcon.Command) { writeRedisError(conn, kv.ErrTransactionOutcomeUnknown) }, redcon.Command{}, time.Now())
	require.ErrorIs(t, owner.original, kv.ErrTransactionOutcomeUnknown)
	require.Empty(t, c.raw.String())
	require.Len(t, c.writes, 1)
	require.NotContains(t, c.writes[0].s, "NOTLEADER")
	require.Contains(t, c.writes[0].s, kv.ErrTransactionOutcomeUnknown.Error())
}

func TestRedisOwnedInternalTransactionUsesCapturedOwner(t *testing.T) {
	t.Parallel()
	for _, dedup := range []bool{false, true} {
		t.Run(map[bool]string{false: "legacy", true: "dedup"}[dedup], func(t *testing.T) {
			s, owner := redisPropagationServer(t)
			s.onePhaseTxnDedup = dedup
			c := &redisOwnedTestConn{}
			queued := redcon.Command{Args: [][]byte{[]byte(cmdSet), []byte("queued"), []byte("original")}}
			s.dispatchCommand(c, cmdSet, func(conn redcon.Conn, _ redcon.Command) {
				results, err := s.runTransaction([]redcon.Command{queued}, s.handlerContext(conn))
				if err != nil {
					writeRedisError(conn, err)
					return
				}
				s.writeResults(conn, results)
			}, queued, time.Now())
			require.Empty(t, c.writes)
			require.Equal(t, "*1\r\n+OK\r\n", c.raw.String())
			require.Equal(t, 1, owner.finishes)
			read := redisPropagationCommand(t, s, cmdGet, "queued")
			require.Equal(t, "$8\r\noriginal\r\n", read.raw.String())
		})
	}
}

func TestRedisOwnedCancelledWriteDoesNotClaimSuccess(t *testing.T) {
	t.Parallel()
	owner := &redisTestOwnership{ctx: context.Background()}
	c := &redisOwnedTestConn{}
	root := &redisOwnedConn{Conn: c, owner: owner, ctx: owner.ctx, writer: &redisReplyBuffer{}, mayWrite: true, workStarted: true}
	root.WriteString("OK")
	owner.revoked.Store(true)
	root.publish()
	require.Empty(t, c.raw.String())
	require.Len(t, c.writes, 1)
	require.ErrorIs(t, owner.original, kv.ErrTransactionOutcomeUnknown)
	require.Contains(t, c.writes[0].s, kv.ErrTransactionOutcomeUnknown.Error())
}

func TestRedisOwnedNestedErrorKeepsRESPShape(t *testing.T) {
	t.Parallel()
	owner := &redisTestOwnership{ctx: context.Background()}
	s := redisOwnedTestServer(owner)
	c := &redisOwnedTestConn{}
	s.dispatchCommand(c, cmdGet, func(conn redcon.Conn, _ redcon.Command) {
		conn.WriteArray(2)
		writeRedisError(conn, errors.New("ERR nested"))
		conn.WriteInt(7)
	}, redcon.Command{}, time.Now())
	require.Equal(t, "*2\r\n-ERR nested\r\n:7\r\n", c.raw.String())
	require.Empty(t, c.writes)
}

func TestRedisOwnedNoDetachedBackgroundContextInCommandPaths(t *testing.T) {
	t.Parallel()
	paths, err := filepath.Glob("redis*.go")
	require.NoError(t, err)
	fs := token.NewFileSet()
	for _, path := range paths {
		if strings.HasSuffix(path, "_test.go") {
			continue
		}
		f, err := parser.ParseFile(fs, path, nil, 0)
		require.NoError(t, err)
		for _, decl := range f.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil || fn.Name.Name == "NewRedisServer" || fn.Name.Name == "baseRequestContext" {
				continue
			}
			assertRedisCommandContextAST(t, fs, fn)
		}
	}
}

func assertRedisCommandContextAST(t *testing.T, fs *token.FileSet, fn *ast.FuncDecl) {
	t.Helper()
	ast.Inspect(fn.Body, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		selector, ok := call.Fun.(*ast.SelectorExpr)
		if !ok {
			return true
		}
		pkg, ok := selector.X.(*ast.Ident)
		if !ok {
			return true
		}
		if pkg.Name == "context" {
			require.NotContains(t, []string{"Background", "TODO"}, selector.Sel.Name, "unowned context at %s", fs.Position(call.Pos()))
		}
		return true
	})
}

func TestRedisOwnedUrgentCompactionUsesOriginalContext(t *testing.T) {
	t.Parallel()
	s, owner := redisPropagationServer(t)
	key := []byte("urgent")
	require.NoError(t, s.store.PutAt(owner.ctx, store.HashMetaKey(key), store.MarshalHashMeta(store.HashMeta{Len: 1}), 1, 0))
	require.NoError(t, s.store.PutAt(owner.ctx, store.HashMetaDeltaKey(key, 2, 0), store.MarshalHashMetaDelta(store.HashMetaDelta{LenDelta: 1}), 2, 0))
	dispatched := 0
	s.coordinator = &redisContextCheckingCoordinator{Coordinator: s.coordinator, check: func(ctx context.Context) {
		require.Equal(t, "propagated", ctx.Value(redisOwnershipTestKey{}))
		require.NotNil(t, ctx.Value(redisOwnedRequestKey{}))
		dispatched++
	}}
	s.compactor = NewDeltaCompactor(s.store, s.coordinator)
	ctx := context.WithValue(owner.ctx, redisOwnedRequestKey{}, true)
	s.triggerUrgentCompactionContext(ctx, "hash", key)
	require.Empty(t, s.compactor.urgentCh, "protected maintenance must not escape onto the ownerless queue")
	require.Positive(t, dispatched)
}

func TestRedisOwnedRegisteredAndUnsupportedCommands(t *testing.T) {
	t.Parallel()
	for _, spec := range redisCommandSpecs {
		require.True(t, redisOwnedCommandSupported(spec.Constant), "registered command %s needs an ownership audit", spec.Constant)
	}
	s, owner := redisPropagationServer(t)
	for _, name := range []string{"PSUBSCRIBE", "WATCH", "UNIMPLEMENTED"} {
		c := &redisOwnedTestConn{}
		s.handleRedisCommand(c, redcon.Command{Args: [][]byte{[]byte(name), []byte("key")}})
		require.Len(t, c.writes, 1)
		require.Equal(t, "error", c.writes[0].op)
		require.Contains(t, c.writes[0].s, "unsupported command")
		require.Empty(t, c.raw.String())
	}
	require.Zero(t, owner.finishes, "unsupported protocol commands must perform no data work")
}

func TestRedisOwnedWireTransactionCannotRemintQueuedGeneration(t *testing.T) {
	t.Parallel()
	s, owner := redisPropagationServer(t)
	mints := 0
	s.requestAdmission = func(context.Context) (RequestOwnership, error) {
		mints++
		return &redisTestOwnership{ctx: owner.ctx}, nil
	}
	c := &redisOwnedTestConn{}
	state := getConnState(c)
	state.inTxn = true
	state.queue = []redcon.Command{{Args: [][]byte{[]byte(cmdSet), []byte("stale"), []byte("must-not-write")}}}
	owner.revoked.Store(true)
	s.handleRedisCommand(c, redcon.Command{Args: [][]byte{[]byte(cmdSet), []byte("stale"), []byte("must-not-write")}})
	s.handleRedisCommand(c, redcon.Command{Args: [][]byte{[]byte(cmdExec)}})
	require.Zero(t, mints, "a queue missing its carrier must not obtain fresh H")
	require.Empty(t, c.raw.String())
	for _, write := range c.writes {
		require.Equal(t, "error", write.op)
		require.Equal(t, errRedisAdmission.Error(), write.s)
	}
	require.Empty(t, state.queue)
	require.False(t, state.inTxn)
	require.False(t, state.transactionRefused)
	_, err := s.store.GetAt(owner.ctx, redisStrKey([]byte("stale")), s.readTS())
	require.ErrorIs(t, err, store.ErrKeyNotFound)
}

func TestRedisOwnedExecStylePerElementErrors(t *testing.T) {
	t.Parallel()
	for _, revoke := range []bool{false, true} {
		t.Run(map[bool]string{false: "live", true: "revoked"}[revoke], func(t *testing.T) {
			owner := &redisTestOwnership{ctx: context.Background()}
			s := redisOwnedTestServer(owner)
			c := &redisOwnedTestConn{}
			s.dispatchCommand(c, cmdSet, func(conn redcon.Conn, _ redcon.Command) {
				s.writeResults(conn, []redisResult{{typ: resultBulk, bulk: []byte("a")}, {typ: resultError, err: kv.ErrTransactionOutcomeUnknown}, {typ: resultInt, integer: 7}})
				if revoke {
					owner.revoked.Store(true)
				}
			}, redcon.Command{}, time.Now())
			if revoke {
				require.Empty(t, c.raw.String())
				require.Len(t, c.writes, 1)
				require.Contains(t, c.writes[0].s, kv.ErrTransactionOutcomeUnknown.Error())
			} else {
				require.Equal(t, "*3\r\n$1\r\na\r\n-"+kv.ErrTransactionOutcomeUnknown.Error()+"\r\n:7\r\n", c.raw.String())
				require.Empty(t, c.writes)
			}
		})
	}
}

func TestRedisOwnedPreservesTypedErrorAndProtocolPrefix(t *testing.T) {
	t.Parallel()
	owner := &redisTestOwnership{ctx: context.Background()}
	s := redisOwnedTestServer(owner)
	c := &redisOwnedTestConn{}
	s.dispatchCommand(c, cmdGet, func(conn redcon.Conn, _ redcon.Command) { writeRedisError(conn, kv.ErrLeaderNotFound) }, redcon.Command{}, time.Now())
	require.ErrorIs(t, owner.original, kv.ErrLeaderNotFound)
	require.Len(t, c.writes, 1)
	require.Equal(t, "NOTLEADER "+kv.ErrLeaderNotFound.Error(), c.writes[0].s)
}
