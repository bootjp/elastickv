package adapter

import (
	"bytes"
	"context"
	"fmt"
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
	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/monitoring"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/redis/go-redis/v9"
	"github.com/tidwall/redcon"
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

const (
	redisDispatchTimeout     = 10 * time.Second
	redisFlushLegacyTimeout  = 10 * time.Minute
	redisRelayPublishTimeout = 2 * time.Second
	redisTraceArgLimit       = 6
	redisTraceArgMaxLen      = 96
	redisTraceArgEllipsis    = "..."
	redisTraceArgTrimLen     = redisTraceArgMaxLen - len(redisTraceArgEllipsis)
	redisTraceRedactAfter    = 1 // redact arguments after key (command name already stripped by caller)

	// listPopDeltaOverhead is the number of extra elements reserved in a list
	// pop elem slice beyond the per-position claim keys and per-item del keys:
	// one slot for the list meta delta key appended by the caller.
	listPopDeltaOverhead = 1
)

type RedisServer struct {
	listen          net.Listener
	store           store.MVCCStore
	coordinator     kv.Coordinator
	readTracker     *kv.ActiveTimestampTracker
	redisTranscoder *redisTranscoder
	pubsub          *redisPubSub
	scriptMu        sync.RWMutex
	scriptCache     map[string]string
	luaPool         *luaStatePool
	luaPoolOnce     sync.Once
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
	leaderClientsMu sync.RWMutex
	leaderClients   map[string]*redis.Client

	// compactor is the background DeltaCompactor for this node. When set,
	// urgent compaction is triggered on ErrDeltaScanTruncated to unblock
	// reads on hot keys faster than the regular compaction interval.
	compactor *DeltaCompactor

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

	// onePhaseTxnDedup enables option-2 one-phase idempotency: on a
	// retryable write error, list-push retries reuse the failed attempt's
	// write set and carry prev_commit_ts so the FSM can dedup a commit that
	// landed under leadership churn (see
	// docs/design/2026_05_21_proposed_txn_secondary_idempotency.md). The
	// FSM probe ships on every node in production, satisfying R5 (FSM
	// determinism across a rolling upgrade), so the gate now defaults on
	// per docs/design/2026_06_10_proposed_redis_onephase_dedup_default_on.md.
	// Set ELASTICKV_REDIS_ONEPHASE_DEDUP=0 (or WithOnePhaseTxnDedup(false))
	// to opt out — kept as a one-env-var operator rollback.
	onePhaseTxnDedup bool

	// standaloneSetDedup gates whether the *standalone* SET command (not SET
	// inside MULTI/EXEC) routes through runTransactionWithDedup. Default off
	// because the dedup path's applySet does not match the legacy
	// executeSet/replaceWithStringTxn semantics for SET-over-collection: a
	// `SET k v` after `RPUSH k x` returns WRONGTYPE on the dedup path but
	// overwrites correctly on the legacy path (PR #943 round-1 codex P1).
	// Bringing applySet to parity (collection-deletion + string write inside
	// the dedup txn) is tracked as a follow-up; until that lands, the
	// standalone SET path stays on the legacy default-on-flip code path
	// regardless of onePhaseTxnDedup's value. Enable explicitly via
	// WithStandaloneSetDedup(true) or ELASTICKV_REDIS_ONEPHASE_DEDUP_SET=1
	// only when applySet's parity is verified for the workload at hand.
	standaloneSetDedup bool

	route map[string]func(conn redcon.Conn, cmd redcon.Command)
}

type RedisServerOption func(*RedisServer)

// WithOnePhaseTxnDedup enables (or disables) the option-2 one-phase
// idempotency dedup on list-push and MULTI/EXEC retries
// (see RedisServer.onePhaseTxnDedup). On by default since the rollout
// recorded in docs/design/2026_06_10_proposed_redis_onephase_dedup_default_on.md;
// pass false to opt out from code, or set ELASTICKV_REDIS_ONEPHASE_DEDUP=0
// to opt out from the environment. The constructor option trumps the env var.
// Standalone SET requires the separate WithStandaloneSetDedup gate; see
// RedisServer.standaloneSetDedup.
func WithOnePhaseTxnDedup(enabled bool) RedisServerOption {
	return func(r *RedisServer) {
		r.onePhaseTxnDedup = enabled
	}
}

// WithStandaloneSetDedup enables the option-2 dedup path on the *standalone*
// SET command (not SET inside MULTI/EXEC). Off by default because the dedup
// path's applySet does not yet match the legacy executeSet semantics for
// SET-over-collection — see RedisServer.standaloneSetDedup. Enable only
// after verifying applySet parity for the workload (no SET-over-list /
// SET-over-hash / SET-over-set / SET-over-zset / SET-over-stream issued).
func WithStandaloneSetDedup(enabled bool) RedisServerOption {
	return func(r *RedisServer) {
		r.standaloneSetDedup = enabled
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

// writeRedisError writes a RESP error reply for err, prepending the
// Redis-protocol semantic code NOTLEADER when err is a transient
// leadership-loss signal. Real Redis clients (Carmine, and through it
// jepsen-io/redis) classify error replies by their first whitespace
// token converted to a keyword (e.g. ERR, MOVED, NOTLEADER). A bare
// "leader not found" reply ends up as `:prefix :leader`, which the
// upstream `with-exceptions` macro does not catch, so a Jepsen worker
// crashes instead of recording a clean `:fail` op.
//
// Classification is purely typed: cockroachdb/errors.Is traverses both
// the standard %w chain and Mark-based equivalence, so wrappers
// (errors.WithStack, errors.Wrapf, raftengine's marked sentinels) do
// not defeat the check. Sentinels mirror kv.isTransientLeaderError +
// kv.isLeadershipLossError so any sentinel those classifiers already
// recognize as transient also flips a Redis reply to NOTLEADER.
func writeRedisError(conn redcon.Conn, err error) {
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

// isTransientLeaderRedisError reports whether err is a transient
// leader-unavailable signal that the Redis adapter should rewrite to a
// NOTLEADER reply. Uses cockroachdb/errors.Is so Mark-based equivalence
// (used by raftengine sentinels) and %w-chain wrapping (cockroachdb
// WithStack, fmt.Errorf %w, etc.) both resolve correctly.
func isTransientLeaderRedisError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, ErrLeaderNotFound) ||
		errors.Is(err, ErrNotLeader) ||
		errors.Is(err, kv.ErrLeaderNotFound) ||
		errors.Is(err, raftengine.ErrNotLeader) ||
		errors.Is(err, raftengine.ErrLeadershipLost) ||
		errors.Is(err, raftengine.ErrLeadershipTransferInProgress) {
		return true
	}
	// Suffix fallback for gRPC-wrapped sentinels. When the coordinator
	// forwards a request to a remote leader via the operational gRPC
	// service and that leader returns ErrLeaderNotFound, the status
	// interceptor flattens the error to "rpc error: code = Unknown
	// desc = leader not found"; the typed sentinel chain is stripped
	// at the wire boundary, so errors.Is misses it. The Jepsen Redis
	// workload (scheduled run 26035515694) saw workers crash with
	// `:prefix :rpc` because the un-prefixed `"rpc error: …"` string
	// reached Carmine. Match the same closed phrase set
	// kv.hasTransientLeaderPhrase uses, with the same HasSuffix
	// guard: free-form Contains would misclassify a user-controlled
	// key like "key: not leader: write conflict" as transient.
	return hasTransientLeaderSuffix(err.Error())
}

// redisLeaderErrorPhrases mirrors kv.leaderErrorPhrases (the kv
// package keeps it unexported). Any new transient-leader phrase the
// kv layer treats as retryable should also flip a NOTLEADER prefix
// on the Redis wire so Carmine's with-exceptions catches it; keep
// in lockstep.
var redisLeaderErrorPhrases = []string{
	"not leader",
	"leader not found",
	"leadership lost",
	"leadership transfer in progress",
}

// hasTransientLeaderSuffix is the suffix-match fallback for
// isTransientLeaderRedisError. Suffix — not free-form Contains —
// because cockroachdb/errors %w-prefix and gRPC status.Errorf's
// "rpc error: code = X desc = <orig>" both leave the original
// sentinel text at the END of the composed string; a Contains
// match would tag a user-controlled key like "key: not leader:
// conflict" as transient.
//
// strings.EqualFold on the trailing slice — rather than
// strings.ToLower + HasSuffix — avoids allocating a copy of the
// full message. cockroachdb/errors messages can be multi-KB when
// they carry a serialized stack trace; this matters on the error
// path under leader-loss storms.
func hasTransientLeaderSuffix(msg string) bool {
	for _, phrase := range redisLeaderErrorPhrases {
		if len(msg) >= len(phrase) &&
			strings.EqualFold(msg[len(msg)-len(phrase):], phrase) {
			return true
		}
	}
	return false
}

func (c *redisMetricsConn) reset(conn redcon.Conn) {
	c.Conn = conn
	c.hadError = false
}

type connState struct {
	inTxn bool
	queue []redcon.Command
	// connID is a monotonically increasing per-server connection
	// identifier assigned on first access via getConnState. Exposed via
	// HELLO's `id` field and CLIENT ID / CLIENT INFO for parity with
	// real Redis so that clients that rely on a stable numeric ID
	// (e.g. go-redis connection pool tagging) do not break.
	connID uint64
	// clientName is the name set via HELLO SETNAME or CLIENT SETNAME,
	// returned by CLIENT GETNAME. Empty string means no name set, which
	// CLIENT GETNAME must report as a null bulk string.
	clientName string
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
		listen:          listen,
		store:           store,
		coordinator:     coordinate,
		redisTranscoder: newRedisTranscoder(),
		redisAddr:       redisAddr,
		relay:           relay,
		leaderRedis:     leaderRedis,
		leaderClients:   make(map[string]*redis.Client),
		pubsub:          newRedisPubSub(),
		scriptCache:     map[string]string{},
		// luaPool is materialized after the option loop so
		// WithLuaPoolMaxIdle can influence its sizing. Test fixtures
		// that bypass NewRedisServer construct the pool lazily via
		// getLuaPool, which honors luaPoolMaxIdle the same way.
		luaPool:       nil,
		traceCommands: os.Getenv("ELASTICKV_REDIS_TRACE") == "1",
		// onePhaseTxnDedup defaults on — the parent design's R5 rolling-upgrade
		// constraint is discharged (FSM probe shipped on every node months ago,
		// 12 consecutive green dedup-mode Jepsen runs 2026-05-31 → 2026-06-10).
		// See docs/design/2026_06_10_proposed_redis_onephase_dedup_default_on.md.
		// ELASTICKV_REDIS_ONEPHASE_DEDUP=0 opts out; the WithOnePhaseTxnDedup
		// constructor option still trumps the env var.
		onePhaseTxnDedup: os.Getenv("ELASTICKV_REDIS_ONEPHASE_DEDUP") != "0",
		// standaloneSetDedup defaults off; see field comment for the
		// applySet-vs-executeSet parity gap that gates this separately.
		standaloneSetDedup: os.Getenv("ELASTICKV_REDIS_ONEPHASE_DEDUP_SET") == "1",
		baseCtx:            baseCtx,
		baseCancel:         baseCancel,
		streamWaiters:      newKeyWaiterRegistry(),
		zsetWaiters:        newKeyWaiterRegistry(),
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

func (r *RedisServer) readTS() uint64 {
	return snapshotTS(r.coordinator.Clock(), r.store)
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

// handlerContext returns the base context for a request handler.
// Falls back to context.Background() when the server was constructed
// by a test stub that bypassed NewRedisServer. Handlers that need a
// deadline should wrap this via context.WithTimeout.
func (r *RedisServer) handlerContext() context.Context {
	if r == nil || r.baseCtx == nil {
		return context.Background()
	}
	return r.baseCtx
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
		func(conn redcon.Conn, cmd redcon.Command) {
			needsTiming := r.requestObserver != nil || r.traceCommands
			var start time.Time
			if needsTiming {
				start = time.Now()
			}
			state := getConnState(conn)
			name := strings.ToUpper(string(cmd.Args[0]))
			handler, ok := r.route[name]
			if !ok {
				r.traceCommandError(conn, name, cmd.Args[1:], "unsupported")
				conn.WriteError("ERR unsupported command '" + string(cmd.Args[0]) + "'")
				// Pass the RAW command bytes (not the already-uppercased `name`)
				// so that the unsupported-command observer can detect invalid
				// UTF-8 before strings.ToUpper silently rewrites the bytes to
				// the U+FFFD replacement character. See observeUnsupportedCommand
				// in monitoring/redis.go.
				r.observeRedisUnsupported(string(cmd.Args[0]), time.Since(start))
				return
			}

			if err := r.validateCmd(cmd); err != nil {
				r.traceCommandError(conn, name, cmd.Args[1:], err.Error())
				writeRedisError(conn, err)
				r.observeRedisError(name, time.Since(start))
				return
			}

			if state.inTxn && name != cmdExec && name != cmdDiscard && name != cmdMulti {
				// redcon reuses the underlying argument buffers; copy queued commands
				// so MULTI/EXEC works reliably under concurrency and with -race.
				state.queue = append(state.queue, cloneCommand(cmd))
				r.traceCommandDone(conn, name, cmd.Args[1:], 0, true)
				conn.WriteString("QUEUED")
				r.observeRedisSuccess(name, time.Since(start))
				return
			}

			r.dispatchCommand(conn, name, handler, cmd, start)
		},
		func(conn redcon.Conn) bool {
			// Use this function to accept or deny the connection.
			// log.Printf("accept: %s", conn.RemoteAddr())
			return true
		},
		func(conn redcon.Conn, err error) {
			// This is called when the connection has been closed.
			// PubSub connections clean up their own subscriptions via bgrunner.
		})

	return errors.WithStack(err)
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

func (r *RedisServer) ping(conn redcon.Conn, _ redcon.Command) {
	conn.WriteString("PONG")
}
