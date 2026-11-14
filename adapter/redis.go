package adapter

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"log/slog"
	"net"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/bootjp/elastickv/keyviz"
	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/monitoring"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/redis/go-redis/v9"
	"github.com/tidwall/redcon"
	"google.golang.org/grpc/status"
)

const (
	cmdBZPopMin         = "BZPOPMIN"
	cmdClient           = "CLIENT"
	cmdCommand          = "COMMAND"
	cmdDBSize           = "DBSIZE"
	cmdDel              = "DEL"
	cmdDiscard          = "DISCARD"
	cmdEval             = "EVAL"
	cmdEvalSHA          = "EVALSHA"
	cmdExec             = "EXEC"
	cmdExists           = "EXISTS"
	cmdExpire           = "EXPIRE"
	cmdFlushAll         = "FLUSHALL"
	cmdFlushDB          = "FLUSHDB"
	cmdFlushLegacy      = "FLUSHLEGACY"
	cmdGet              = "GET"
	cmdGetDel           = "GETDEL"
	cmdHDel             = "HDEL"
	cmdHExists          = "HEXISTS"
	cmdHGet             = "HGET"
	cmdHGetAll          = "HGETALL"
	cmdHIncrBy          = "HINCRBY"
	cmdHLen             = "HLEN"
	cmdHMGet            = "HMGET"
	cmdHMSet            = "HMSET"
	cmdHSet             = "HSET"
	cmdHello            = "HELLO"
	cmdInfo             = "INFO"
	cmdIncr             = "INCR"
	cmdKeys             = "KEYS"
	cmdLIndex           = "LINDEX"
	cmdLLen             = "LLEN"
	cmdLPop             = "LPOP"
	cmdLPos             = "LPOS"
	cmdLPush            = "LPUSH"
	cmdLRange           = "LRANGE"
	cmdLRem             = "LREM"
	cmdLSet             = "LSET"
	cmdLTrim            = "LTRIM"
	cmdMulti            = "MULTI"
	cmdPExpire          = "PEXPIRE"
	cmdPFAdd            = "PFADD"
	cmdPFCount          = "PFCOUNT"
	cmdPing             = "PING"
	cmdPTTL             = "PTTL"
	cmdPublish          = "PUBLISH"
	cmdPubSub           = "PUBSUB"
	cmdQuit             = "QUIT"
	cmdRename           = "RENAME"
	cmdRPop             = "RPOP"
	cmdRPopLPush        = "RPOPLPUSH"
	cmdRPush            = "RPUSH"
	cmdSAdd             = "SADD"
	cmdSCard            = "SCARD"
	cmdScan             = "SCAN"
	cmdSelect           = "SELECT"
	cmdSet              = "SET"
	cmdSetEx            = "SETEX"
	cmdSetNX            = "SETNX"
	cmdSIsMember        = "SISMEMBER"
	cmdSMembers         = "SMEMBERS"
	cmdSRem             = "SREM"
	cmdSubscribe        = "SUBSCRIBE"
	cmdType             = "TYPE"
	cmdTTL              = "TTL"
	cmdXAdd             = "XADD"
	cmdXLen             = "XLEN"
	cmdXRead            = "XREAD"
	cmdXRange           = "XRANGE"
	cmdXRevRange        = "XREVRANGE"
	cmdXTrim            = "XTRIM"
	cmdZAdd             = "ZADD"
	cmdZCard            = "ZCARD"
	cmdZCount           = "ZCOUNT"
	cmdZIncrBy          = "ZINCRBY"
	cmdZRange           = "ZRANGE"
	cmdZRangeByScore    = "ZRANGEBYSCORE"
	cmdZRem             = "ZREM"
	cmdZRemRangeByScore = "ZREMRANGEBYSCORE"
	cmdZRemRangeByRank  = "ZREMRANGEBYRANK"
	cmdZPopMin          = "ZPOPMIN"
	cmdZRevRange        = "ZREVRANGE"
	cmdZRevRangeByScore = "ZREVRANGEBYSCORE"
	cmdZScore           = "ZSCORE"
	minKeyedArgs        = 2
)

const cmdElasticKVZRemFast = "ELASTICKV.ZREMFAST"

const (
	redisDispatchTimeout = 30 * time.Second
	// redisLuaDispatchTimeout gives EVAL/EVALSHA enough room for migration
	// scripts that expand into thousands of Redis calls. Regular commands stay
	// on redisDispatchTimeout so non-script heavy paths cannot hold worker slots
	// for the full script replay budget.
	redisLuaDispatchTimeout = 5 * time.Minute
	// defaultRedisBlockWaitFallback is the safety-net poll interval for
	// blocking-command wait loops when no in-process write signal arrives.
	// Signals cover normal XADD / ZADD / ZINCRBY wakeups immediately; this
	// interval only bounds missed-signal, wrong-type, and BLOCK-deadline checks.
	// Keep normal producer wakeups event-driven while reducing idle fallback scans
	// from blocked consumers waiting on empty collections.
	defaultRedisBlockWaitFallback = time.Second
	redisFinalTypeCheckTimeout    = 100 * time.Millisecond
	redisFlushLegacyTimeout       = 10 * time.Minute
	redisRelayPublishTimeout      = 2 * time.Second
	redisTraceArgLimit            = 6
	redisTraceArgMaxLen           = 96
	redisTraceArgEllipsis         = "..."
	redisTraceArgTrimLen          = redisTraceArgMaxLen - len(redisTraceArgEllipsis)
	redisTraceRedactAfter         = 1 // redact arguments after key (command name already stripped by caller)

	// listPopDeltaOverhead is the number of extra elements reserved in a list
	// pop elem slice beyond the per-position claim keys and per-item del keys:
	// one slot for the list meta delta key appended by the caller.
	listPopDeltaOverhead = 1
)

type RedisServer struct {
	listen      net.Listener
	store       store.MVCCStore
	coordinator kv.Coordinator
	readTracker *kv.ActiveTimestampTracker
	pubsub      *redisPubSub
	scriptMu    sync.RWMutex
	scriptCache map[string]string
	luaPool     *luaStatePool
	luaPoolOnce sync.Once
	// luaPoolMaxIdle is the configured cap on idle pooled *lua.LStates.
	// Set via WithLuaPoolMaxIdle before NewRedisServer materializes the
	// pool; getLuaPool falls back to DefaultLuaPoolMaxIdle when the
	// value is non-positive (covers test fixtures that bypass
	// NewRedisServer).
	luaPoolMaxIdle      int
	traceCommands       bool
	traceSeq            atomic.Uint64
	redisAddr           string
	relay               *RedisPubSubRelay
	relayConnCache      kv.GRPCConnCache
	requestObserver     monitoring.RedisRequestObserver
	luaObserver         monitoring.LuaScriptObserver
	luaFastPathObserver monitoring.LuaFastPathObserver
	// luaFastPathZRange is the pre-resolved counter bundle for the
	// ZRANGEBYSCORE / ZREVRANGEBYSCORE Lua fast path. Resolved once in
	// WithLuaFastPathObserver so the hot path does not pay for
	// CounterVec.WithLabelValues on every redis.call().
	luaFastPathZRange monitoring.LuaFastPathCmd
	// heavyCommandLimiter bounds CPU-heavy Redis command families so one
	// expensive path cannot occupy every scheduler P and starve Raft.
	// nil preserves the historical unbounded path for narrow test fixtures.
	heavyCommandLimiter     *redisHeavyCommandLimiter
	requestAdmission        RequestAdmission
	replyBudget             redisReplyBudget
	replyWriteTimeout       time.Duration
	executionBudget         redisProtectedExecutionBudget
	blockingExecutionBudget redisProtectedExecutionBudget
	wireTxnMu               sync.Mutex
	wireTxns                map[*redisWireTxn]struct{}
	wireTxnIdleTimeout      time.Duration
	wireTxnBudgetLimit      int
	wireTxnBytes            int
	// peerLimiter bounds concurrent Redis connections per remote peer IP.
	peerLimiter *redisPeerLimiter
	// blockWaitFallback is the safety-net polling interval used by blocking
	// Redis commands when no waiter signal arrives.
	blockWaitFallback time.Duration
	// baseCtx is the parent context for per-request handlers.
	// NewRedisServer creates a cancelable context here; Stop() cancels
	// it so in-flight handlers abort promptly instead of running
	// unbounded on context.Background(). Test stubs that construct
	// RedisServer literals directly (bypassing NewRedisServer) may
	// leave baseCtx nil; handlerContext() falls back to
	// context.Background() in that case.
	baseCtx    context.Context
	baseCancel context.CancelFunc
	// TODO manage membership from raft log
	leaderRedis map[string]string

	// leaderClients caches go-redis clients per leader address to avoid
	// creating a new connection pool for every proxied request.
	leaderClientsMu        sync.RWMutex
	leaderClients          map[string]*redis.Client
	blockingLeaderClients  map[string]*redis.Client
	leaderClientsCloseOnce sync.Once

	// compactor is the background DeltaCompactor for this node. When set,
	// urgent compaction is triggered on ErrDeltaScanTruncated to unblock
	// reads on hot keys faster than the regular compaction interval.
	compactor *DeltaCompactor

	// disableLegacyTTLReadFallback removes !redis|ttl| from the authoritative
	// read path after the inline TTL migrator has rewritten all anchors. The
	// default keeps the fallback enabled for rolling upgrades.
	disableLegacyTTLReadFallback bool

	// connIDSeq hands out monotonically increasing per-connection
	// identifiers. The zero value is never returned (atomic.AddUint64
	// returns 1 on first call) so clients can treat 0 as "unset".
	// Exposed via HELLO / CLIENT ID.
	connIDSeq atomic.Uint64

	// streamWaiters lets XADD wake an XREAD BLOCK waiter on the same
	// node, replacing what was a 10 ms time.Sleep busy-poll. See
	// redis_key_waiters.go.
	streamWaiters *keyWaiterRegistry

	// zsetWaiters lets ZADD / ZINCRBY wake a BZPOPMIN waiter on the
	// same node, replacing what was a 10 ms time.Sleep busy-poll. See
	// redis_key_waiters.go.
	zsetWaiters *keyWaiterRegistry

	// applyObserver is the shared FSM observer when this RedisServer is wired
	// through WithRedisApplyObserver. Local zset writes use it to keep their
	// FSM-originated wake fast-safe.
	applyObserver *RedisApplyObserver

	// onePhaseTxnDedup enables option-2 one-phase idempotency: on a
	// retryable write error, list-push retries reuse the failed attempt's
	// write set and carry prev_commit_ts so the FSM can dedup a commit that
	// landed under leadership churn (see
	// docs/design/2026_05_21_implemented_txn_secondary_idempotency.md). The
	// FSM probe ships on every node in production, satisfying R5 (FSM
	// determinism across a rolling upgrade), so the gate now defaults on
	// per docs/design/2026_06_10_implemented_redis_onephase_dedup_default_on.md.
	// Set ELASTICKV_REDIS_ONEPHASE_DEDUP=0 (or WithOnePhaseTxnDedup(false))
	// to opt out — kept as a one-env-var operator rollback.
	onePhaseTxnDedup bool

	route map[string]func(conn redcon.Conn, cmd redcon.Command)

	// inflight counts the commands being executed so Stop can wait for them,
	// bounded, after closing the listener; see inflightGate.
	inflight *inflightGate
}

type RedisServerOption func(*RedisServer)

// WithOnePhaseTxnDedup enables (or disables) the option-2 one-phase
// idempotency dedup on list-push and MULTI/EXEC retries
// (see RedisServer.onePhaseTxnDedup). On by default since the rollout
// recorded in docs/design/2026_06_10_implemented_redis_onephase_dedup_default_on.md;
// pass false to opt out from code, or set ELASTICKV_REDIS_ONEPHASE_DEDUP=0
// to opt out from the environment. The constructor option trumps the env var.
func WithOnePhaseTxnDedup(enabled bool) RedisServerOption {
	return func(r *RedisServer) {
		r.onePhaseTxnDedup = enabled
	}
}

func WithRedisActiveTimestampTracker(tracker *kv.ActiveTimestampTracker) RedisServerOption {
	return func(r *RedisServer) {
		r.readTracker = tracker
	}
}

// WithRedisCompactor wires a DeltaCompactor to the RedisServer so that urgent
// single-key compaction can be triggered when ErrDeltaScanTruncated is hit.
func WithRedisCompactor(c *DeltaCompactor) RedisServerOption {
	return func(r *RedisServer) {
		r.compactor = c
	}
}

// WithRedisLegacyTTLReadFallback controls whether pre-inline !redis|ttl|
// entries are still consulted after the anchor-specific TTL probes miss.
// Leave this enabled during rolling upgrades; disable it only after the
// background inline TTL migrator has completed for the cluster and legacy bare
// string keys have been separately ruled out or rewritten.
func WithRedisLegacyTTLReadFallback(enabled bool) RedisServerOption {
	return func(r *RedisServer) {
		r.disableLegacyTTLReadFallback = !enabled
	}
}

// WithRedisRequestObserver enables Prometheus-compatible request metrics.
func WithRedisRequestObserver(observer monitoring.RedisRequestObserver) RedisServerOption {
	return func(r *RedisServer) {
		r.requestObserver = observer
	}
}

// WithLuaObserver enables per-phase Lua script metrics (VM exec, Raft commit, retries).
func WithLuaObserver(observer monitoring.LuaScriptObserver) RedisServerOption {
	return func(r *RedisServer) {
		r.luaObserver = observer
	}
}

// WithLuaFastPathObserver enables per-redis.call() fast-path outcome
// metrics inside Lua scripts. Used to diagnose fast-path hit ratios
// for commands like ZRANGEBYSCORE / ZSCORE / HGET.
//
// Resolves per-command counter handles up front so the hot path
// avoids CounterVec.WithLabelValues on every redis.call().
func WithLuaFastPathObserver(observer monitoring.LuaFastPathObserver) RedisServerOption {
	return func(r *RedisServer) {
		r.luaFastPathObserver = observer
		r.luaFastPathZRange = observer.ForCommand(luaFastPathCmdZRangeByScore)
	}
}

// WithLuaPoolMaxIdle caps the number of idle *lua.LState instances
// the Lua VM pool retains between EVALs. The cap controls the steady-
// state memory floor of the pool (maxIdle * per-state footprint —
// empirically ~200 KiB) without bounding throughput: get() falls
// through to a fresh allocation when the pool is empty, and put()
// drops a state to the GC when the pool is full. n <= 0 is clamped
// to DefaultLuaPoolMaxIdle, matching newLuaStatePoolWithMaxIdle.
//
// Passing this option overrides the default. The option records the
// requested cap on the RedisServer; the pool itself is constructed
// after all options are applied so the recorded cap takes effect.
func WithLuaPoolMaxIdle(n int) RedisServerOption {
	return func(r *RedisServer) {
		r.luaPoolMaxIdle = n
	}
}

// WithRedisHeavyCommandSlots overrides the heavy-command limiter size.
// n <= 0 disables the limiter. Omit the option to use the environment/default
// selected by newDefaultRedisHeavyCommandLimiter.
func WithRedisHeavyCommandSlots(n int) RedisServerOption {
	return func(r *RedisServer) {
		r.heavyCommandLimiter = newRedisHeavyCommandLimiter(n)
	}
}

// WithRedisPerPeerConnectionLimit overrides the per-peer Redis connection cap.
// n <= 0 disables the cap.
func WithRedisPerPeerConnectionLimit(n int) RedisServerOption {
	return func(r *RedisServer) {
		r.peerLimiter = newRedisPeerLimiter(n)
	}
}

// WithRedisBlockWaitFallback overrides the safety-net polling interval for
// blocking Redis commands. Production should normally use the default and rely
// on waiter signals for immediate wakeups; tests use a shorter interval to keep
// package runtime bounded.
func WithRedisBlockWaitFallback(d time.Duration) RedisServerOption {
	return func(r *RedisServer) {
		if d > 0 {
			r.blockWaitFallback = d
		}
	}
}

// luaFastPathCmdZRangeByScore is the shared label for ZRANGEBYSCORE
// and ZREVRANGEBYSCORE fast-path outcomes. Both directions take the
// same branch through zsetRangeByScoreFast so sharing one label
// keeps the counter cardinality bounded.
const luaFastPathCmdZRangeByScore = "zrangebyscore"

// redisMetricsConn wraps a redcon.Conn to detect whether WriteError was called.
type redisMetricsConn struct {
	redcon.Conn
	hadError bool
}

var redisMetricsConnPool = sync.Pool{
	New: func() any { return &redisMetricsConn{} },
}

func (c *redisMetricsConn) WriteError(msg string) {
	c.hadError = true
	c.Conn.WriteError(msg)
}

const redisOutcomeUnknownReply = "ERR operation outcome unknown"
const redisNotLeaderReply = "NOTLEADER leader unavailable"

// writeRedisError writes a RESP error reply for err, prepending the
// Redis-protocol semantic code NOTLEADER when err is a transient
// leadership-loss signal. Real Redis clients (Carmine, and through it
// jepsen-io/redis) classify error replies by their first whitespace
// token converted to a keyword (e.g. ERR, MOVED, NOTLEADER). A bare
// "leader not found" reply ends up as `:prefix :leader`, which the
// upstream `with-exceptions` macro does not catch, so a Jepsen worker
// crashes instead of recording a clean `:fail` op.
//
// Only local typed leadership errors qualify for NOTLEADER. An unclassified
// gRPC status cannot prove a remote operation was rejected before submission.
func writeRedisError(conn redcon.Conn, err error) {
	if recorder, ok := conn.(interface{ recordRequestError(error) }); ok {
		recorder.recordRequestError(err)
	}
	if isRedisIndeterminate(err) {
		conn.WriteError(redisOutcomeUnknownReply)
		return
	}
	if isRedisNotLeaderReply(err) {
		conn.WriteError(redisNotLeaderReply)
		return
	}
	msg := err.Error()
	if isTransientLeaderRedisError(err) {
		if strings.HasPrefix(strings.ToUpper(msg), "NOTLEADER ") {
			conn.WriteError(msg)
			return
		}
		conn.WriteError("NOTLEADER " + msg)
		return
	}
	conn.WriteError(msg)
}

func isRedisTransportUncertain(err error) bool {
	if err == nil {
		return false
	}
	if _, ok := status.FromError(err); ok {
		return true
	}
	var netErr net.Error
	return errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) ||
		errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) || errors.As(err, &netErr)
}

func isRedisIndeterminate(err error) bool {
	return errors.Is(err, kv.ErrTransactionOutcomeUnknown) ||
		errors.Is(err, raftengine.ErrLeadershipLost) || isRedisTransportUncertain(err)
}

func isRedisNotLeaderReply(err error) bool {
	var reply redis.Error
	if !errors.As(err, &reply) {
		return false
	}
	code, _, ok := strings.Cut(reply.Error(), " ")
	return ok && code == "NOTLEADER"
}

// isTransientLeaderRedisError reports whether err is a transient
// leader-unavailable signal that the Redis adapter should rewrite to a
// NOTLEADER reply. Uses cockroachdb/errors.Is so Mark-based equivalence
// (used by raftengine sentinels) and %w-chain wrapping (cockroachdb
// WithStack, fmt.Errorf %w, etc.) both resolve correctly.
func isTransientLeaderRedisError(err error) bool {
	if err == nil {
		return false
	}
	// A proposal may have committed before leadership was lost. An unknown
	// transaction outcome must not become a NOTLEADER retry instruction.
	if isRedisIndeterminate(err) {
		return false
	}
	if errors.Is(err, ErrLeaderNotFound) ||
		errors.Is(err, ErrNotLeader) ||
		errors.Is(err, kv.ErrLeaderNotFound) ||
		errors.Is(err, kv.ErrLeaderProxyCircuitOpen) ||
		errors.Is(err, raftengine.ErrNotLeader) ||
		errors.Is(err, raftengine.ErrLeadershipTransferInProgress) {
		return true
	}
	return false
}

func (c *redisMetricsConn) reset(conn redcon.Conn) {
	c.Conn = conn
	c.hadError = false
}

type connState struct {
	mu                  sync.Mutex
	inTxn               bool
	transactionRefused  bool
	transactionPoisoned bool
	wireTxn             *redisWireTxn
	queue               []redcon.Command
	// connID is a monotonically increasing per-server connection
	// identifier assigned on first access via getConnState. Exposed via
	// HELLO's `id` field and CLIENT ID / CLIENT INFO for parity with
	// real Redis so that clients that rely on a stable numeric ID
	// (e.g. go-redis connection pool tagging) do not break.
	connID uint64
	// clientName is the name set via HELLO SETNAME or CLIENT SETNAME,
	// returned by CLIENT GETNAME. Empty string means no name set, which
	// CLIENT GETNAME must report as a null bulk string.
	clientName     string
	peerKey        string
	peerCounted    bool
	pubsubDetached bool
}

type resultType int

const (
	resultNil resultType = iota
	resultError
	resultBulk
	resultString
	resultArray
	resultInt
)

type redisResult struct {
	typ     resultType
	bulk    []byte
	str     string
	arr     []string
	integer int64
	err     error
}

func NewRedisServer(listen net.Listener, redisAddr string, store store.MVCCStore, coordinate kv.Coordinator, leaderRedis map[string]string, relay *RedisPubSubRelay, opts ...RedisServerOption) *RedisServer {
	if relay == nil {
		relay = NewRedisPubSubRelay()
	}
	baseCtx, baseCancel := context.WithCancel(context.Background())
	r := &RedisServer{
		listen:                listen,
		store:                 store,
		coordinator:           kv.WithKeyVizLabel(coordinate, keyviz.LabelRedis),
		redisAddr:             redisAddr,
		relay:                 relay,
		leaderRedis:           leaderRedis,
		leaderClients:         make(map[string]*redis.Client),
		blockingLeaderClients: make(map[string]*redis.Client),
		pubsub:                newRedisPubSub(),
		scriptCache:           map[string]string{},
		// luaPool is materialized after the option loop so
		// WithLuaPoolMaxIdle can influence its sizing. Test fixtures
		// that bypass NewRedisServer construct the pool lazily via
		// getLuaPool, which honors luaPoolMaxIdle the same way.
		luaPool:             nil,
		traceCommands:       os.Getenv("ELASTICKV_REDIS_TRACE") == "1",
		heavyCommandLimiter: newDefaultRedisHeavyCommandLimiter(),
		peerLimiter:         newDefaultRedisPeerLimiter(),
		blockWaitFallback:   defaultRedisBlockWaitFallback,
		// onePhaseTxnDedup defaults on — the parent design's R5 rolling-upgrade
		// constraint is discharged (FSM probe shipped on every node months ago,
		// 12 consecutive green dedup-mode Jepsen runs 2026-05-31 → 2026-06-10).
		// See docs/design/2026_06_10_implemented_redis_onephase_dedup_default_on.md.
		// ELASTICKV_REDIS_ONEPHASE_DEDUP=0 opts out; the WithOnePhaseTxnDedup
		// constructor option still trumps the env var.
		onePhaseTxnDedup: os.Getenv("ELASTICKV_REDIS_ONEPHASE_DEDUP") != "0",
		baseCtx:          baseCtx,
		baseCancel:       baseCancel,
		inflight:         newInflightGate(),
		streamWaiters:    newKeyWaiterRegistry(),
		zsetWaiters:      newKeyWaiterRegistry(),
	}
	r.relay.Bind(r.publishLocal)

	// route, argsLen, and redisCommandTable all derive from the single
	// redisCommandSpecs slice (adapter/redis_command_specs.go) so adding
	// a command is a one-row diff there and the three views can never
	// drift. See buildRouteMap for the per-server bind.
	r.route = r.buildRouteMap()
	for _, opt := range opts {
		if opt != nil {
			opt(r)
		}
	}
	if r.compactor != nil && r.requestAdmission != nil {
		// Startup-only injection before the compactor's Run goroutine starts.
		r.compactor.requestAdmission = r.requestAdmission
	}
	r.pubsub.detach = r.detachPubSubConn
	r.pubsub.onClose = r.releaseDetachedPubSubConn
	// Materialize the Lua VM pool after option processing so
	// WithLuaPoolMaxIdle can choose the cap. newLuaStatePoolWithMaxIdle
	// clamps non-positive values to DefaultLuaPoolMaxIdle, so callers
	// that omit the option still get a sensible default. The
	// luaPoolOnce barrier in getLuaPool keeps test fixtures that build
	// a bare &RedisServer{} literal (and never call NewRedisServer)
	// from racing on the same field.
	r.luaPool = newLuaStatePoolWithMaxIdle(r.luaPoolMaxIdle)

	return r
}

func getConnState(conn redcon.Conn) *connState {
	if ctx := conn.Context(); ctx != nil {
		if st, ok := ctx.(*connState); ok {
			return st
		}
	}
	st := &connState{}
	conn.SetContext(st)
	return st
}

func (r *RedisServer) acceptConn(conn redcon.Conn) bool {
	if r == nil || r.peerLimiter == nil {
		return true
	}
	peer, ok := r.peerLimiter.accept(conn.RemoteAddr())
	st := getConnState(conn)
	st.mu.Lock()
	st.peerKey = peer
	st.peerCounted = ok
	st.pubsubDetached = false
	st.mu.Unlock()
	if !ok {
		conn.WriteError(redisPeerLimitError)
	}
	return ok
}

func (r *RedisServer) closeConn(conn redcon.Conn) {
	r.closeWireTransaction(conn)
	if r == nil || r.peerLimiter == nil {
		return
	}
	st, ok := conn.Context().(*connState)
	if !ok {
		return
	}
	st.mu.Lock()
	defer st.mu.Unlock()
	r.releaseConnStateLocked(st)
}

func (r *RedisServer) detachPubSubConn(conn redcon.Conn) redcon.DetachedConn {
	st := getConnState(conn)
	st.mu.Lock()
	st.pubsubDetached = true
	st.mu.Unlock()
	return conn.Detach()
}

func (r *RedisServer) releaseDetachedPubSubConn(conn redcon.Conn) {
	if r == nil || r.peerLimiter == nil {
		return
	}
	st := getConnState(conn)
	st.mu.Lock()
	defer st.mu.Unlock()
	st.pubsubDetached = false
	r.releaseConnStateLocked(st)
}

func (r *RedisServer) releaseConnStateLocked(st *connState) {
	if r == nil || r.peerLimiter == nil || !st.peerCounted || st.pubsubDetached {
		return
	}
	r.peerLimiter.release(st.peerKey)
	st.peerCounted = false
}

// ensureConnID assigns and returns a per-connection numeric ID for the
// given state, allocating one lazily on first access. The ID comes from
// r.connIDSeq; atomic.AddUint64 returns 1 on first call so zero is
// reserved as "no id assigned yet" for external observers. IDs are not
// reused when a connection closes — this matches real Redis semantics
// and keeps the identifier usable as a debugging breadcrumb.
func (r *RedisServer) ensureConnID(st *connState) uint64 {
	if st == nil {
		return 0
	}
	if st.connID != 0 {
		return st.connID
	}
	st.connID = r.connIDSeq.Add(1)
	return st.connID
}

func (r *RedisServer) nextCommitTSAfter(ctx context.Context, startTS uint64, label string) (uint64, error) {
	if r == nil || r.coordinator == nil {
		return 0, errors.Wrap(kv.ErrTSOCoordinatorNil, label)
	}
	ts, err := kv.NextTimestampAfterThrough(ctx, r.coordinator, startTS, label)
	if err != nil {
		return 0, errors.WithStack(err)
	}
	return ts, nil
}

func (r *RedisServer) pinReadTS(ts uint64) *kv.ActiveTimestampToken {
	if r == nil || r.readTracker == nil {
		return nil
	}
	return r.readTracker.Pin(ts)
}

// triggerUrgentCompaction signals the DeltaCompactor to immediately compact
// the given key, bypassing the regular interval. No-op when no compactor is wired.
func (r *RedisServer) triggerUrgentCompaction(typeName string, key []byte) {
	if r.compactor != nil {
		r.compactor.TriggerUrgentCompaction(typeName, key)
	}
}

func (r *RedisServer) dispatchCommand(conn redcon.Conn, name string, handler func(redcon.Conn, redcon.Command), cmd redcon.Command, start time.Time) {
	if r.dispatchProtectedTransaction(conn, name, cmd, start) {
		return
	}
	if r.requestAdmission != nil {
		r.dispatchOwnedCommand(conn, name, handler, cmd, start)
		return
	}
	r.dispatchCommandUnowned(conn, name, handler, cmd, start)
}

func (r *RedisServer) dispatchCommandUnowned(conn redcon.Conn, name string, handler func(redcon.Conn, redcon.Command), cmd redcon.Command, start time.Time) {
	if r.shouldLimitHeavyCommand(name) {
		if ok := r.heavyCommandLimiter.submit(func() {
			r.dispatchCommandDirect(conn, name, handler, cmd, start)
		}); !ok {
			r.rejectHeavyCommand(conn, name, cmd, start)
		}
		return
	}
	r.dispatchCommandDirect(conn, name, handler, cmd, start)
}

func (r *RedisServer) dispatchCommandDirect(conn redcon.Conn, name string, handler func(redcon.Conn, redcon.Command), cmd redcon.Command, start time.Time) {
	defer func() {
		if owned, ok := conn.(*redisOwnedConn); ok && !owned.workStarted {
			return
		}
		if redisReadObservedAtProbe(name) {
			return
		}
		r.observeRedisRead(r.handlerContext(conn), name, cmd.Args, true)
	}()
	switch {
	case r.requestObserver != nil:
		metricsConn, _ := redisMetricsConnPool.Get().(*redisMetricsConn)
		if metricsConn == nil {
			metricsConn = &redisMetricsConn{}
		}
		metricsConn.reset(conn)
		if r.traceCommands {
			traceID, traceStart := r.traceCommandStart(conn, name, cmd.Args[1:])
			handler(metricsConn, cmd)
			r.traceCommandFinish(traceID, conn, name, time.Since(traceStart))
		} else {
			handler(metricsConn, cmd)
		}
		r.requestObserver.ObserveRedisRequest(monitoring.RedisRequestReport{
			Command:  name,
			IsError:  metricsConn.hadError,
			Duration: time.Since(start),
		})
		metricsConn.Conn = nil
		redisMetricsConnPool.Put(metricsConn)
	case r.traceCommands:
		traceID, traceStart := r.traceCommandStart(conn, name, cmd.Args[1:])
		handler(conn, cmd)
		r.traceCommandFinish(traceID, conn, name, time.Since(traceStart))
	default:
		handler(conn, cmd)
	}
}

func (r *RedisServer) shouldLimitHeavyCommand(name string) bool {
	return r != nil && r.heavyCommandLimiter != nil && isRedisHeavyCommand(name) && !isRedisIdleWaitCommand(name)
}

func (r *RedisServer) runWithHeavyCommandSlot(fn func()) bool {
	if r == nil || r.heavyCommandLimiter == nil {
		fn()
		return true
	}
	return r.heavyCommandLimiter.submit(fn)
}

func isRedisIdleWaitCommand(name string) bool {
	switch name {
	case cmdXRead, cmdBZPopMin:
		return true
	default:
		return false
	}
}

func (r *RedisServer) rejectHeavyCommand(conn redcon.Conn, name string, cmd redcon.Command, start time.Time) {
	if !r.requireRequestContext(conn) {
		return
	}

	r.traceCommandError(conn, name, cmd.Args[1:], "heavy command pool full")
	conn.WriteError(errRedisHeavyCommandPoolFull.Error())
	r.observeRedisError(name, time.Since(start))
}

// handlerContext extracts a command carrier without touching connection state.
// Missing carriers fail closed in protected mode. Legacy fixtures fall back to
// the server context; handlers may add a deadline with context.WithTimeout.
func (r *RedisServer) handlerContext(conns ...redcon.Conn) context.Context {
	if len(conns) != 0 {
		if carrier, ok := conns[0].(interface{ requestContext() context.Context }); ok {
			if ctx := carrier.requestContext(); ctx != nil {
				return ctx
			}
		}
	}
	return r.redisContext()
}

// Close cancels the base context, signalling all in-flight handlers to
// abort. Idempotent. The underlying redcon listener is still owned by
// the caller; Close does NOT touch it so shutdown orchestration can
// remain with the server owner.
func (r *RedisServer) Close() error {
	if r == nil {
		return nil
	}
	if r.baseCancel != nil {
		r.baseCancel()
	}
	r.closeWireTransactions()
	if r.pubsub != nil {
		r.pubsub.Close()
	}
	return nil
}

// RegisterLuaPoolMetrics wires this server's bounded Lua VM pool
// into the supplied Prometheus registerer, exposing five metrics
// (hits / misses / drops / idle / max_idle). See
// monitoring.RegisterLuaPool for the per-metric definitions.
//
// Returns nil if r, the pool, or registerer is nil — callers can
// invoke this unconditionally from main.go without guarding for
// test fixtures. The registration uses prometheus.NewCounterFunc /
// NewGaugeFunc, so the values are read from the pool's atomic
// counters at scrape time; no observability load is added to the
// EVAL hot path.
func (r *RedisServer) RegisterLuaPoolMetrics(registerer prometheus.Registerer) error {
	if r == nil || registerer == nil {
		return nil
	}
	pool := r.getLuaPool()
	if pool == nil {
		return nil
	}
	if err := monitoring.RegisterLuaPool(registerer, pool); err != nil {
		return errors.Wrap(err, "register lua pool metrics")
	}
	return nil
}

func (r *RedisServer) Run() error {
	err := redcon.Serve(r.listen,
		r.handleRedisCommand,
		func(conn redcon.Conn) bool {
			return r.acceptConn(conn)
		},
		func(conn redcon.Conn, err error) {
			// This is called when the connection has been closed.
			// PubSub connections clean up their own subscriptions via bgrunner.
			r.closeConn(conn)
		})

	return errors.WithStack(err)
}

func (r *RedisServer) handleRedisCommand(conn redcon.Conn, cmd redcon.Command) {
	conn = r.replyConn(conn)
	if !r.inflight.enter() {
		conn.WriteError("ERR server is shutting down")
		return
	}
	defer r.inflight.exit()
	execution, admitted := r.acquireProtectedExecution(conn, cmd)
	if !admitted {
		return
	}
	defer releaseRedisExecution(execution)
	if len(cmd.Args) == 0 {
		conn.WriteError("ERR empty command")
		return
	}
	var start time.Time
	if r.requestObserver != nil || r.traceCommands {
		start = time.Now()
	}
	name := strings.ToUpper(string(cmd.Args[0]))
	if r.dispatchProtectedTransaction(conn, name, cmd, start) {
		return
	}
	handler, ok := r.route[name]
	if !ok {
		r.traceCommandError(conn, name, cmd.Args[1:], "unsupported")
		conn.WriteError("ERR unsupported command '" + string(cmd.Args[0]) + "'")
		// Keep raw bytes for the observer's invalid UTF-8 classification.
		r.observeRedisUnsupported(string(cmd.Args[0]), time.Since(start))
		return
	}
	if err := r.validateCmd(cmd); err != nil {
		r.traceCommandError(conn, name, cmd.Args[1:], err.Error())
		writeRedisError(conn, err)
		r.observeRedisError(name, time.Since(start))
		return
	}
	state := getConnState(conn)
	if redisQueueOnlyCommand(state, name) {
		// Protected queues are handled above with their retained MULTI owner.
		state.queue = append(state.queue, cloneCommand(cmd))
		r.traceCommandDone(conn, name, cmd.Args[1:], 0, true)
		conn.WriteString("QUEUED")
		r.observeRedisSuccess(name, time.Since(start))
		return
	}
	r.dispatchCommand(conn, name, handler, cmd, start)
}

func redisQueueOnlyCommand(state *connState, name string) bool {
	return state.inTxn && name != cmdExec && name != cmdDiscard && name != cmdMulti
}

func cloneCommand(cmd redcon.Command) redcon.Command {
	out := redcon.Command{
		Raw:  bytes.Clone(cmd.Raw),
		Args: make([][]byte, len(cmd.Args)),
	}
	for i := range cmd.Args {
		out.Args[i] = bytes.Clone(cmd.Args[i])
	}
	return out
}

func (r *RedisServer) traceCommandStart(conn redcon.Conn, name string, args [][]byte) (uint64, time.Time) {
	if !r.traceCommands {
		return 0, time.Time{}
	}
	id := r.traceSeq.Add(1)
	log.Printf("redis trace start id=%d remote=%s cmd=%s args=%s", id, conn.RemoteAddr(), name, formatTraceArgs(args))
	return id, time.Now()
}

func (r *RedisServer) traceCommandFinish(id uint64, conn redcon.Conn, name string, dur time.Duration) {
	if !r.traceCommands {
		return
	}
	log.Printf("redis trace done id=%d remote=%s cmd=%s dur=%s", id, conn.RemoteAddr(), name, dur)
}

func (r *RedisServer) traceCommandDone(conn redcon.Conn, name string, args [][]byte, dur time.Duration, queued bool) {
	if !r.traceCommands {
		return
	}
	status := "done"
	if queued {
		status = "queued"
	}
	log.Printf("redis trace %s remote=%s cmd=%s args=%s dur=%s", status, conn.RemoteAddr(), name, formatTraceArgs(args), dur)
}

func (r *RedisServer) traceCommandError(conn redcon.Conn, name string, args [][]byte, err string) {
	if !r.traceCommands {
		return
	}
	log.Printf("redis trace error remote=%s cmd=%s args=%s err=%q", conn.RemoteAddr(), name, formatTraceArgs(args), err)
}

func formatTraceArgs(args [][]byte) string {
	if len(args) == 0 {
		return "[]"
	}
	parts := make([]string, 0, min(len(args), redisTraceArgLimit))
	for i, arg := range args {
		if i >= redisTraceArgLimit {
			parts = append(parts, redisTraceArgEllipsis)
			break
		}
		if i >= redisTraceRedactAfter {
			parts = append(parts, fmt.Sprintf("<%d bytes>", len(arg)))
			continue
		}
		s := strconv.QuoteToASCII(string(arg))
		if len(s) > redisTraceArgMaxLen {
			s = s[:redisTraceArgTrimLen] + redisTraceArgEllipsis
		}
		parts = append(parts, s)
	}
	return "[" + strings.Join(parts, " ") + "]"
}

func (r *RedisServer) observeRedisError(command string, dur time.Duration) {
	if r.requestObserver == nil {
		return
	}
	r.requestObserver.ObserveRedisRequest(monitoring.RedisRequestReport{
		Command:  command,
		IsError:  true,
		Duration: dur,
	})
}

// observeRedisUnsupported records a command that was rejected because
// the adapter has no route for it. In addition to the usual error
// counters (which bucket the name into "unknown"), this flags the
// report so the monitoring layer can record the real command name in
// its bounded-cardinality unsupported-commands counter.
//
// IMPORTANT: `command` must be the RAW bytes the client sent (not an
// already-uppercased value). The monitoring layer relies on seeing the
// raw bytes to detect invalid UTF-8 before strings.ToUpper silently
// replaces invalid bytes with the Unicode replacement character.
func (r *RedisServer) observeRedisUnsupported(command string, dur time.Duration) {
	if r.requestObserver == nil {
		return
	}
	r.requestObserver.ObserveRedisRequest(monitoring.RedisRequestReport{
		Command:     command,
		IsError:     true,
		Duration:    dur,
		Unsupported: true,
	})
}

func (r *RedisServer) observeRedisSuccess(command string, dur time.Duration) {
	if r.requestObserver == nil {
		return
	}
	r.requestObserver.ObserveRedisRequest(monitoring.RedisRequestReport{
		Command:  command,
		IsError:  false,
		Duration: dur,
	})
}

func (r *RedisServer) Stop() {
	// Cancel baseCtx first so in-flight handlers observe a cancelled
	// context before their network connections are torn down.
	_ = r.Close()
	if err := r.relayConnCache.Close(); err != nil {
		slog.Warn("redis server: relay conn cache close",
			slog.String("addr", r.redisAddr),
			slog.Any("err", err),
		)
	}
	if r.listen != nil {
		if err := r.listen.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			slog.Warn("redis server: listener close",
				slog.String("addr", r.redisAddr),
				slog.Any("err", err),
			)
		}
	}
	// redcon closes the connections when Serve returns but does not join
	// the handlers; give the commands still running their bounded window
	// before the caller tears down the stores they use.
	if !r.inflight.drain(redisShutdownTimeout) {
		slog.Warn("redis server: commands still running after the shutdown bound, continuing",
			slog.String("addr", r.redisAddr),
			slog.Int64("in_flight", r.inflight.remaining()),
			slog.Duration("timeout", redisShutdownTimeout),
		)
		if r.requestAdmission != nil {
			// A protected runtime must retain ownership instead of closing
			// storage underneath a context-insensitive I/O still in flight.
			<-r.inflight.idle
		}
	}
	r.closeLeaderClients()
}

func (r *RedisServer) closeLeaderClients() {
	r.leaderClientsCloseOnce.Do(func() {
		r.leaderClientsMu.Lock()
		defer r.leaderClientsMu.Unlock()
		for _, clients := range []map[string]*redis.Client{r.leaderClients, r.blockingLeaderClients} {
			for _, client := range clients {
				_ = client.Close()
			}
		}
	})
}

func (r *RedisServer) publishLocal(channel, message []byte) int64 {
	return int64(r.pubsub.Publish(string(channel), string(message)))
}

func (r *RedisServer) relayPeers() []string {
	if len(r.leaderRedis) == 0 {
		return nil
	}

	byRedis := make(map[string]string, len(r.leaderRedis))
	for addr, redisAddr := range r.leaderRedis {
		if redisAddr == "" || redisAddr == r.redisAddr {
			continue
		}
		prev, ok := byRedis[redisAddr]
		if !ok || addr < prev {
			byRedis[redisAddr] = addr
		}
	}

	peers := make([]string, 0, len(byRedis))
	for _, addr := range byRedis {
		peers = append(peers, addr)
	}
	sort.Slice(peers, func(i, j int) bool {
		return peers[i] < peers[j]
	})
	return peers
}

func (r *RedisServer) publishCluster(ctx context.Context, channel, message []byte) int64 {
	delivered := r.publishLocal(channel, message)
	peers := r.relayPeers()
	if len(peers) == 0 {
		return delivered
	}

	type peerResult struct {
		subscribers int64
		err         error
	}
	results := make(chan peerResult, len(peers))
	overallCtx, overallCancel := context.WithTimeout(ctx, redisRelayPublishTimeout)
	defer overallCancel()

	for _, peer := range peers {
		go func(peer string) { //nolint:dupl
			conn, err := r.relayConnCache.ConnFor(peer)
			if err != nil {
				log.Printf("redis relay publish dial peer=%s err=%v", peer, err)
				results <- peerResult{err: err}
				return
			}
			resp, err := pb.NewInternalClient(conn).RelayPublish(overallCtx, &pb.RelayPublishRequest{
				Channel: bytes.Clone(channel),
				Message: bytes.Clone(message),
			})
			if err != nil {
				log.Printf("redis relay publish peer=%s err=%v", peer, err)
				results <- peerResult{err: err}
				return
			}
			results <- peerResult{subscribers: resp.GetSubscribers()}
		}(peer)
	}

	for range peers {
		if res := <-results; res.err == nil {
			delivered += res.subscribers
		}
	}
	return delivered
}

func (r *RedisServer) validateCmd(cmd redcon.Command) error {
	name := strings.ToUpper(string(cmd.Args[0]))
	expected, ok := argsLen[name]
	if !ok {
		return nil
	}

	switch {
	case expected > 0 && len(cmd.Args) != expected:
		//nolint:wrapcheck
		return errors.WithStack(errors.Newf("ERR wrong number of arguments for '%s' command", string(cmd.Args[0])))
	case expected < 0 && len(cmd.Args) < -expected:
		return errors.WithStack(errors.Newf("ERR wrong number of arguments for '%s' command", string(cmd.Args[0])))
	}
	return nil
}

const pingMessageArgCount = 2

func (r *RedisServer) ping(conn redcon.Conn, cmd redcon.Command) {
	if !r.requireRequestContext(conn) {
		return
	}

	switch len(cmd.Args) {
	case 1:
		conn.WriteString("PONG")
	case pingMessageArgCount:
		conn.WriteBulk(cmd.Args[1])
	default:
		conn.WriteError("ERR wrong number of arguments for 'ping' command")
	}
}
