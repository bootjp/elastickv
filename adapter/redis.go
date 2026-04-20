package adapter

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"log/slog"
	"maps"
	"math"
	"net"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/monitoring"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
	"github.com/hashicorp/raft"
	"github.com/redis/go-redis/v9"
	"github.com/tidwall/redcon"
)

const (
	cmdBZPopMin         = "BZPOPMIN"
	cmdClient           = "CLIENT"
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

var redisTxnKeyPrefix = []byte("!txn|")

type redisSetOptions struct {
	existsCond  bool
	missingCond bool
	returnOld   bool
	ttl         *time.Time
}

type redisSetState struct {
	rawTyp   redisValueType // TTL-unaware type, for internal-key cleanup
	typ      redisValueType // TTL-aware type, for NX/XX/GET semantics
	oldValue []byte
}

type redisSetExecution struct {
	state        redisSetState
	wroteNull    bool
	wroteOldBulk bool
}

type txnCommandHandler func(*txnContext, redcon.Command) (redisResult, error)

var txnApplyHandlers = map[string]txnCommandHandler{
	cmdSet:     (*txnContext).applySet,
	cmdDel:     (*txnContext).applyDel,
	cmdGet:     (*txnContext).applyGet,
	cmdExists:  (*txnContext).applyExists,
	cmdRPush:   (*txnContext).applyRPush,
	cmdLRange:  (*txnContext).applyLRange,
	cmdZIncrBy: (*txnContext).applyZIncrBy,
	cmdExpire:  (*txnContext).applyExpireSeconds,
	cmdPExpire: (*txnContext).applyExpireMilliseconds,
}

//nolint:mnd
var argsLen = map[string]int{
	cmdBZPopMin:         -3,
	cmdClient:           -2,
	cmdDBSize:           1,
	cmdDel:              -2,
	cmdDiscard:          1,
	cmdEval:             -3,
	cmdEvalSHA:          -3,
	cmdExec:             1,
	cmdExists:           -2,
	cmdExpire:           -3,
	cmdFlushAll:         1,
	cmdFlushDB:          1,
	cmdFlushLegacy:      1,
	cmdGet:              2,
	cmdGetDel:           2,
	cmdHDel:             -3,
	cmdHExists:          3,
	cmdHGet:             3,
	cmdHGetAll:          2,
	cmdHIncrBy:          4,
	cmdHLen:             2,
	cmdHMGet:            -3,
	cmdHMSet:            -4,
	cmdHSet:             -4,
	cmdInfo:             -1,
	cmdIncr:             2,
	cmdKeys:             2,
	cmdLIndex:           3,
	cmdLLen:             2,
	cmdLPop:             2,
	cmdLPush:            -3,
	cmdLPos:             -3,
	cmdLRange:           4,
	cmdLRem:             4,
	cmdLSet:             4,
	cmdLTrim:            4,
	cmdMulti:            1,
	cmdPExpire:          -3,
	cmdPFAdd:            -3,
	cmdPFCount:          -2,
	cmdPing:             -1,
	cmdPTTL:             2,
	cmdPublish:          3,
	cmdPubSub:           -2,
	cmdQuit:             1,
	cmdRename:           3,
	cmdRPop:             2,
	cmdRPopLPush:        3,
	cmdRPush:            -3,
	cmdSAdd:             -3,
	cmdSCard:            2,
	cmdScan:             -2,
	cmdSelect:           2,
	cmdSet:              -3,
	cmdSetEx:            4,
	cmdSetNX:            3,
	cmdSIsMember:        3,
	cmdSMembers:         2,
	cmdSRem:             -3,
	cmdSubscribe:        -2,
	cmdTTL:              2,
	cmdType:             2,
	cmdXAdd:             -5,
	cmdXLen:             2,
	cmdXRead:            -4,
	cmdXRange:           -4,
	cmdXRevRange:        -4,
	cmdXTrim:            -4,
	cmdZAdd:             -4,
	cmdZCard:            2,
	cmdZCount:           4,
	cmdZIncrBy:          4,
	cmdZRange:           -4,
	cmdZRangeByScore:    -4,
	cmdZRem:             -3,
	cmdZRemRangeByScore: 4,
	cmdZRemRangeByRank:  4,
	cmdZPopMin:          -2,
	cmdZRevRange:        -4,
	cmdZRevRangeByScore: -4,
	cmdZScore:           3,
}

type RedisServer struct {
	listen          net.Listener
	store           store.MVCCStore
	coordinator     kv.Coordinator
	readTracker     *kv.ActiveTimestampTracker
	redisTranscoder *redisTranscoder
	pubsub          *redisPubSub
	scriptMu        sync.RWMutex
	scriptCache     map[string]string
	traceCommands   bool
	traceSeq        atomic.Uint64
	redisAddr       string
	relay           *RedisPubSubRelay
	relayConnCache  kv.GRPCConnCache
	requestObserver monitoring.RedisRequestObserver
	luaObserver     monitoring.LuaScriptObserver
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
	leaderRedis map[raft.ServerAddress]string

	// leaderClients caches go-redis clients per leader address to avoid
	// creating a new connection pool for every proxied request.
	leaderClientsMu sync.RWMutex
	leaderClients   map[string]*redis.Client

	// compactor is the background DeltaCompactor for this node. When set,
	// urgent compaction is triggered on ErrDeltaScanTruncated to unblock
	// reads on hot keys faster than the regular compaction interval.
	compactor *DeltaCompactor

	route map[string]func(conn redcon.Conn, cmd redcon.Command)
}

type RedisServerOption func(*RedisServer)

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

func (c *redisMetricsConn) reset(conn redcon.Conn) {
	c.Conn = conn
	c.hadError = false
}

type connState struct {
	inTxn bool
	queue []redcon.Command
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

func NewRedisServer(listen net.Listener, redisAddr string, store store.MVCCStore, coordinate kv.Coordinator, leaderRedis map[raft.ServerAddress]string, relay *RedisPubSubRelay, opts ...RedisServerOption) *RedisServer {
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
		traceCommands:   os.Getenv("ELASTICKV_REDIS_TRACE") == "1",
		baseCtx:         baseCtx,
		baseCancel:      baseCancel,
	}
	r.relay.Bind(r.publishLocal)

	r.route = map[string]func(conn redcon.Conn, cmd redcon.Command){
		cmdBZPopMin:         r.bzpopmin,
		cmdClient:           r.client,
		cmdDBSize:           r.dbsize,
		cmdDel:              r.del,
		cmdDiscard:          r.discard,
		cmdEval:             r.eval,
		cmdEvalSHA:          r.evalsha,
		cmdExec:             r.exec,
		cmdExists:           r.exists,
		cmdExpire:           r.expire,
		cmdFlushAll:         r.flushall,
		cmdFlushDB:          r.flushdb,
		cmdFlushLegacy:      r.flushlegacy,
		cmdGet:              r.get,
		cmdGetDel:           r.getdel,
		cmdHDel:             r.hdel,
		cmdHExists:          r.hexists,
		cmdHGet:             r.hget,
		cmdHGetAll:          r.hgetall,
		cmdHIncrBy:          r.hincrby,
		cmdHLen:             r.hlen,
		cmdHMGet:            r.hmget,
		cmdHMSet:            r.hmset,
		cmdHSet:             r.hset,
		cmdInfo:             r.info,
		cmdIncr:             r.incr,
		cmdKeys:             r.keys,
		cmdLIndex:           r.lindex,
		cmdLLen:             r.llen,
		cmdLPop:             r.lpop,
		cmdLPos:             r.lpos,
		cmdLPush:            r.lpush,
		cmdLRange:           r.lrange,
		cmdLRem:             r.lrem,
		cmdLSet:             r.lset,
		cmdLTrim:            r.ltrim,
		cmdMulti:            r.multi,
		cmdPExpire:          r.pexpire,
		cmdPFAdd:            r.pfadd,
		cmdPFCount:          r.pfcount,
		cmdPing:             r.ping,
		cmdPTTL:             r.pttl,
		cmdPublish:          r.publish,
		cmdPubSub:           r.pubsubCmd,
		cmdQuit:             r.quit,
		cmdRename:           r.rename,
		cmdRPop:             r.rpop,
		cmdRPopLPush:        r.rpoplpush,
		cmdRPush:            r.rpush,
		cmdSAdd:             r.sadd,
		cmdSCard:            r.scard,
		cmdScan:             r.scan,
		cmdSelect:           r.selectDB,
		cmdSet:              r.set,
		cmdSetEx:            r.setex,
		cmdSetNX:            r.setnx,
		cmdSIsMember:        r.sismember,
		cmdSMembers:         r.smembers,
		cmdSRem:             r.srem,
		cmdSubscribe:        r.subscribe,
		cmdTTL:              r.ttl,
		cmdType:             r.typeCmd,
		cmdXAdd:             r.xadd,
		cmdXLen:             r.xlen,
		cmdXRead:            r.xread,
		cmdXRange:           r.xrange,
		cmdXRevRange:        r.xrevrange,
		cmdXTrim:            r.xtrim,
		cmdZAdd:             r.zadd,
		cmdZCard:            r.zcard,
		cmdZCount:           r.zcount,
		cmdZIncrBy:          r.zincrby,
		cmdZRange:           r.zrange,
		cmdZRangeByScore:    r.zrangebyscore,
		cmdZRem:             r.zrem,
		cmdZRemRangeByScore: r.zremrangebyscore,
		cmdZRemRangeByRank:  r.zremrangebyrank,
		cmdZPopMin:          r.zpopmin,
		cmdZRevRange:        r.zrevrange,
		cmdZRevRangeByScore: r.zrevrangebyscore,
		cmdZScore:           r.zscore,
	}
	for _, opt := range opts {
		if opt != nil {
			opt(r)
		}
	}

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
				r.observeRedisError(name, time.Since(start))
				return
			}

			if err := r.validateCmd(cmd); err != nil {
				r.traceCommandError(conn, name, cmd.Args[1:], err.Error())
				conn.WriteError(err.Error())
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

func parseRedisSetOptions(args [][]byte, now time.Time) (redisSetOptions, error) {
	opts := redisSetOptions{}
	for i := 0; i < len(args); i++ {
		opt := strings.ToUpper(string(args[i]))
		switch opt {
		case "EX", "PX":
			ttl, nextIndex, err := parseRedisSetTTL(args, i, opt, now)
			if err != nil {
				return redisSetOptions{}, err
			}
			opts.ttl = ttl
			i = nextIndex
		case "NX":
			opts.missingCond = true
		case "XX":
			opts.existsCond = true
		case "GET":
			opts.returnOld = true
		default:
			return redisSetOptions{}, errors.New("ERR syntax error")
		}
	}
	if opts.existsCond && opts.missingCond {
		return redisSetOptions{}, errors.New("ERR syntax error")
	}
	return opts, nil
}

func parseRedisSetTTL(args [][]byte, index int, opt string, now time.Time) (*time.Time, int, error) {
	if index+1 >= len(args) {
		return nil, index, errors.New("ERR syntax error")
	}
	n, err := strconv.ParseInt(string(args[index+1]), 10, 64)
	if err != nil {
		// Match Redis behavior: invalid numeric TTL value should not expose
		// internal parsing errors, but return a stable protocol error.
		return nil, index, errors.New("ERR value is not an integer or out of range")
	}
	if n <= 0 {
		return nil, index, errors.New("ERR invalid expire time in 'set' command")
	}

	unit := time.Millisecond
	if opt == "EX" {
		unit = time.Second
	}
	if n > math.MaxInt64/int64(unit) {
		return nil, index, errors.New("ERR invalid expire time in 'set' command")
	}

	expireAt := now.Add(time.Duration(n) * unit)
	return &expireAt, index + 1, nil
}

func (o redisSetOptions) isFastPath() bool {
	return !o.returnOld && !o.existsCond && !o.missingCond
}

func (o redisSetOptions) allows(exists bool) bool {
	if o.existsCond && !exists {
		return false
	}
	if o.missingCond && exists {
		return false
	}
	return true
}

func (r *RedisServer) loadRedisSetState(ctx context.Context, key []byte, readTS uint64, returnOld bool) (redisSetState, error) {
	// Probe type ONCE (rawKeyTypeAt issues up to ~17 pebble seeks),
	// then derive both the raw and TTL-filtered views from it. The
	// previous implementation called rawKeyTypeAt + keyTypeAt, which
	// called rawKeyTypeAt again inside -- doubling every SET to ~34
	// seeks for purely redundant work.
	rawTyp, err := r.rawKeyTypeAt(ctx, key, readTS)
	if err != nil {
		return redisSetState{}, err
	}
	// typ (TTL-aware) drives NX/XX/GET Redis semantics: expired keys are "gone".
	typ, err := r.applyTTLFilter(ctx, key, readTS, rawTyp)
	if err != nil {
		return redisSetState{}, err
	}

	state := redisSetState{rawTyp: rawTyp, typ: typ}
	if !returnOld || typ != redisTypeString {
		return state, nil
	}

	oldValue, _, err := r.readRedisStringAt(key, readTS)
	if err != nil && !errors.Is(err, store.ErrKeyNotFound) {
		return redisSetState{}, err
	}
	state.oldValue = oldValue
	return state, nil
}

func (r *RedisServer) replaceWithStringTxn(ctx context.Context, key, value []byte, ttl *time.Time, typ redisValueType, readTS uint64) error {
	var elems []*kv.Elem[kv.OP]
	if isNonStringCollectionType(typ) {
		delElems, _, err := r.deleteLogicalKeyElems(ctx, key, readTS)
		if err != nil {
			return err
		}
		elems = append(elems, delElems...)
	}
	// Embed TTL in the string value; write !redis|ttl| as a secondary scan index.
	encoded := encodeRedisStr(bytes.Clone(value), ttl)
	elems = append(elems, &kv.Elem[kv.OP]{Op: kv.Put, Key: redisStrKey(key), Value: encoded})
	if ttl != nil {
		elems = append(elems, &kv.Elem[kv.OP]{Op: kv.Put, Key: redisTTLKey(key), Value: encodeRedisTTL(*ttl)})
	} else {
		// Clear any prior scan index so a persistent string is not later expired.
		elems = append(elems, &kv.Elem[kv.OP]{Op: kv.Del, Key: redisTTLKey(key)})
	}
	return r.dispatchElems(ctx, true, readTS, elems)
}

func (r *RedisServer) executeSet(ctx context.Context, key, value []byte, opts redisSetOptions) (redisSetExecution, error) {
	var result redisSetExecution
	err := r.retryRedisWrite(ctx, func() error {
		readTS := r.readTS()
		state, err := r.loadRedisSetState(ctx, key, readTS, opts.returnOld)
		if err != nil {
			return err
		}

		exists := state.typ != redisTypeNone
		if !opts.allows(exists) {
			result = redisSetExecution{wroteNull: true}
			return nil
		}
		if opts.returnOld && exists && state.typ != redisTypeString {
			return wrongTypeError()
		}
		// Use rawTyp for cleanup so expired-but-lingering internal keys are deleted.
		if err := r.replaceWithStringTxn(ctx, key, value, opts.ttl, state.rawTyp, readTS); err != nil {
			return err
		}
		result = redisSetExecution{state: state, wroteOldBulk: opts.returnOld}
		return nil
	})
	return result, err
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

func (r *RedisServer) relayPeers() []raft.ServerAddress {
	if len(r.leaderRedis) == 0 {
		return nil
	}

	byRedis := make(map[string]raft.ServerAddress, len(r.leaderRedis))
	for addr, redisAddr := range r.leaderRedis {
		if redisAddr == "" || redisAddr == r.redisAddr {
			continue
		}
		prev, ok := byRedis[redisAddr]
		if !ok || string(addr) < string(prev) {
			byRedis[redisAddr] = addr
		}
	}

	peers := make([]raft.ServerAddress, 0, len(byRedis))
	for _, addr := range byRedis {
		peers = append(peers, addr)
	}
	sort.Slice(peers, func(i, j int) bool {
		return string(peers[i]) < string(peers[j])
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
		go func(peer raft.ServerAddress) {
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

// trySetFastPath attempts the fast-path for SET (no NX/XX/GET flags) when the
// key is a string or absent. Returns true if the fast-path handled the command.
// When the key holds a non-string type, returns false so the caller can fall
// through to executeSet which cleans up internal keys before overwriting.
func (r *RedisServer) trySetFastPath(conn redcon.Conn, ctx context.Context, key, value []byte, ttl *time.Time) bool {
	// Only use the fast path when we are the leader for this key so the local
	// type check is authoritative. On followers, stale MVCC state could miss a
	// non-string type, leaving orphaned internal keys after overwrite.
	if !r.coordinator.IsLeaderForKey(key) {
		return false
	}
	readTS := r.readTS()
	// Use rawKeyTypeAt (TTL-unaware) so that expired keys whose internal data
	// still exists are detected and routed through the full cleanup path.
	typ, err := r.rawKeyTypeAt(context.Background(), key, readTS)
	if err != nil {
		conn.WriteError(err.Error())
		return true
	}
	if isNonStringCollectionType(typ) {
		return false
	}
	if err := r.saveString(ctx, key, value, ttl); err != nil {
		conn.WriteError(err.Error())
		return true
	}
	conn.WriteString("OK")
	return true
}

func (r *RedisServer) set(conn redcon.Conn, cmd redcon.Command) {
	if r.proxyToLeader(conn, cmd, cmd.Args[1]) {
		return
	}
	opts, err := parseRedisSetOptions(cmd.Args[3:], time.Now())
	if err != nil {
		conn.WriteError(err.Error())
		return
	}

	ctx, cancel := context.WithTimeout(r.handlerContext(), redisDispatchTimeout)
	defer cancel()

	if opts.isFastPath() && r.trySetFastPath(conn, ctx, cmd.Args[1], cmd.Args[2], opts.ttl) {
		return
	}

	result, err := r.executeSet(ctx, cmd.Args[1], cmd.Args[2], opts)
	if err != nil {
		conn.WriteError(err.Error())
		return
	}
	if result.wroteNull {
		conn.WriteNull()
		return
	}
	if result.wroteOldBulk {
		if result.state.oldValue == nil {
			conn.WriteNull()
			return
		}
		conn.WriteBulk(result.state.oldValue)
		return
	}
	conn.WriteString("OK")
}

func (r *RedisServer) get(conn redcon.Conn, cmd redcon.Command) {
	key := cmd.Args[1]
	if r.proxyToLeader(conn, cmd, key) {
		return
	}

	// Single bounded context for the slow paths in this handler,
	// derived from the server's base context so Close() cancels any
	// in-flight handler instead of leaving it running on a detached
	// context.Background(). Only LeaseReadForKey and keyTypeAt accept
	// a context; readRedisStringAt is a local-store read that does
	// not take one. The shared deadline bounds the only branches
	// that can actually block on quorum / I/O.
	ctx, cancel := context.WithTimeout(r.handlerContext(), redisDispatchTimeout)
	defer cancel()
	if _, err := kv.LeaseReadForKeyThrough(r.coordinator, ctx, key); err != nil {
		conn.WriteError(err.Error())
		return
	}
	readTS := r.readTS()

	// Fast path: attempt the string read directly instead of probing
	// every possible Redis encoding first. rawKeyTypeAt issues up to
	// ~17 pebble seeks (list meta + list delta + 3×wide-column probes
	// each doing 3 seeks + hash/set/zset/stream/HLL/str/bare); that
	// overhead dominated every GET on a hot cluster (see
	// docs/lease_read_design.md). A live string key resolves in 1-2
	// seeks here, and we only fall back to keyTypeAt when the string
	// path returns ErrKeyNotFound (meaning either missing, expired,
	// or a non-string type is present under this user-key).
	//
	// Use the snapshot variant: LeaseReadForKeyThrough above already
	// established the ReadIndex fence, so a per-call VerifyLeaderForKey
	// (inside leaderAwareGetAt) would duplicate the quorum work.
	v, _, err := r.readRedisStringAtSnapshot(key, readTS)
	if err == nil {
		conn.WriteBulk(v)
		return
	}
	if !errors.Is(err, store.ErrKeyNotFound) {
		conn.WriteError(err.Error())
		return
	}

	// Slow path: disambiguate "missing / expired" from WRONGTYPE.
	// keyTypeAt applies the TTL filter, so an expired string reports
	// as redisTypeNone here and we return nil -- matching the
	// pre-optimisation behaviour.
	typ, err := r.keyTypeAt(ctx, key, readTS)
	if err != nil {
		conn.WriteError(err.Error())
		return
	}
	if typ == redisTypeNone {
		conn.WriteNull()
		return
	}
	// If keyTypeAt disagrees with the fast path and classifies the key
	// as a live string (e.g. a rare TTL-filter discrepancy between
	// decodePrefixedStringWith/readBareLegacyStringWith and
	// hasExpiredTTLAt), match the pre-optimisation behaviour and
	// return nil rather than WRONGTYPE.
	if typ == redisTypeString {
		conn.WriteNull()
		return
	}
	conn.WriteError(wrongTypeMessage)
}

// leaderEmbeddedTTLExpired looks at !redis|str|<key> on the leader and, if the
// payload is in new format, returns the embedded-TTL expiry verdict. The bool
// indicates whether the caller should use this verdict (true) or fall through
// to the legacy !redis|ttl| index (false).
func (r *RedisServer) leaderEmbeddedTTLExpired(key []byte) (bool, bool) {
	raw, err := r.tryLeaderGetAt(redisStrKey(key), 0)
	if err != nil || !isNewRedisStrFormat(raw) {
		return false, false
	}
	_, expireAt, decErr := decodeRedisStr(raw)
	if decErr != nil {
		// Malformed new-format payload: treat as expired rather than silently alive.
		return true, true
	}
	if expireAt == nil {
		return false, true
	}
	return !expireAt.After(time.Now()), true
}

// isLeaderKeyExpired checks whether the key has an expired TTL on the leader.
func (r *RedisServer) isLeaderKeyExpired(key []byte) bool {
	// For string keys with new encoding: check embedded TTL.
	if expired, ok := r.leaderEmbeddedTTLExpired(key); ok {
		return expired
	}
	raw, err := r.tryLeaderGetAt(redisTTLKey(key), 0)
	if err != nil {
		return false
	}
	ttl, err := decodeRedisTTL(raw)
	if err != nil {
		return false
	}
	return !ttl.After(time.Now())
}

// tryLeaderNonStringExists checks whether the key exists as a non-string type
// (hash, set, zset, stream, HLL, or list) on the leader. Returns false if the
// key has an expired TTL.
func (r *RedisServer) tryLeaderNonStringExists(key []byte) bool {
	// Check TTL first: if expired, the key is logically gone.
	if raw, err := r.tryLeaderGetAt(redisTTLKey(key), 0); err == nil {
		if ttl, decErr := decodeRedisTTL(raw); decErr == nil && !ttl.After(time.Now()) {
			return false
		}
	}
	for _, internalKey := range [][]byte{
		redisHashKey(key),
		redisSetKey(key),
		redisHLLKey(key),
		redisZSetKey(key),
		redisStreamKey(key),
	} {
		if _, err := r.tryLeaderGetAt(internalKey, 0); err == nil {
			return true
		}
	}
	if _, err := r.tryLeaderGetAt(listMetaKey(key), 0); err == nil {
		return true
	}
	return false
}

// tryLeaderLogicalExists checks whether the key exists as any type on the leader.
func (r *RedisServer) tryLeaderLogicalExists(key []byte) bool {
	// Prefer asking the leader's Redis command path directly: it evaluates
	// existence with ttlAt() semantics (including the in-memory TTL buffer).
	// If this path is unavailable we fall back to raw-KV probing, which is
	// best-effort and may lag unflushed buffer-only TTL updates.
	if cli, err := r.leaderClientForKey(key); err == nil {
		ctx, cancel := context.WithTimeout(r.handlerContext(), redisDispatchTimeout)
		defer cancel()
		if count, existsErr := cli.Exists(ctx, string(key)).Result(); existsErr == nil {
			return count > 0
		}
	}

	// Fallback to raw KV probing if Redis command proxying is unavailable.
	if r.isLeaderKeyExpired(key) {
		return false
	}
	// String type (raw user key).
	if _, err := r.tryLeaderGetAt(key, 0); err == nil {
		return true
	}
	return r.tryLeaderNonStringExists(key)
}

func (r *RedisServer) del(conn redcon.Conn, cmd redcon.Command) {
	// DEL discovers internal keys via local MVCC state. On followers this state
	// may lag, producing incomplete deletes. Check per-key leadership and proxy
	// non-local keys to the correct leader for accurate internal-key discovery.
	localKeys := make([][]byte, 0, len(cmd.Args)-1)
	proxyKeys := make([][]byte, 0)
	for _, key := range cmd.Args[1:] {
		if r.coordinator.IsLeaderForKey(key) {
			localKeys = append(localKeys, key)
		} else {
			proxyKeys = append(proxyKeys, key)
		}
	}

	var removed int64

	// Proxy non-local keys to the appropriate leader.
	if len(proxyKeys) > 0 {
		proxied, err := r.proxyDel(proxyKeys)
		if err != nil {
			conn.WriteError(err.Error())
			return
		}
		removed += proxied
	}

	// Delete local keys directly.
	if len(localKeys) > 0 {
		localRemoved, err := r.delLocal(localKeys)
		if err != nil {
			conn.WriteError(err.Error())
			return
		}
		removed += int64(localRemoved)
	}

	conn.WriteInt64(removed)
}

func (r *RedisServer) delLocal(keys [][]byte) (int, error) {
	ctx, cancel := context.WithTimeout(r.handlerContext(), redisDispatchTimeout)
	defer cancel()
	var removed int
	err := r.retryRedisWrite(ctx, func() error {
		elems := []*kv.Elem[kv.OP]{}
		nextRemoved := 0
		readTS := r.readTS()
		for _, key := range keys {
			keyElems, existed, err := r.deleteLogicalKeyElems(ctx, key, readTS)
			if err != nil {
				return err
			}
			if existed {
				nextRemoved++
			}
			elems = append(elems, keyElems...)
		}
		if err := r.dispatchElems(ctx, true, readTS, elems); err != nil {
			return err
		}
		removed = nextRemoved
		return nil
	})
	return removed, err
}

func (r *RedisServer) exists(conn redcon.Conn, cmd redcon.Command) {
	readTS := r.readTS()
	// Derive ctx from the server's base context so proxy hops (which
	// DO honor ctx) are bounded by this deadline and cancel on
	// shutdown. Local Pebble reads (store.GetAt / ExistsAt / ScanAt)
	// currently ignore the context parameter, so cancellation does
	// not actually interrupt an in-flight local probe -- the deadline
	// bounds the per-key loop iteration and any leader-proxy RPC
	// taken by the negative-result fallback, not the local probes
	// themselves.
	ctx, cancel := context.WithTimeout(r.handlerContext(), redisDispatchTimeout)
	defer cancel()
	count := 0
	for _, key := range cmd.Args[1:] {
		ok, err := r.existsAtFast(ctx, key, readTS)
		if err != nil {
			conn.WriteError(err.Error())
			return
		}
		if ok {
			count++
		} else if !r.coordinator.IsLeaderForKey(key) {
			// Local MVCC may be stale on a follower; proxy to the leader.
			if r.tryLeaderLogicalExists(key) {
				count++
			}
		}
	}
	conn.WriteInt(count)
}

// existsAtFast is a string-first fast path for EXISTS-style liveness
// checks. Strings dominate real workloads, and a live string key
// resolves here in 1-2 seeks against redisStrKey (with TTL filtering
// applied inline) versus the ~17 seeks of a full logicalExistsAt
// probe. When the redisStrKey probe misses we fall back to the full
// type-probe.
//
// The probe goes directly to the local store. EXISTS tolerates stale-
// positive reads on followers by design -- the pre-optimisation flow
// (logicalExistsAt → keyTypeAt → local store.ExistsAt) never proxied
// to the leader for the probe itself; proxying is reserved for the
// negative-result fallback (tryLeaderLogicalExists in the caller).
// Routing through readRedisStringAt here would instead issue a Raft
// round-trip per key on every follower, regressing EXISTS latency on
// workloads that were previously all-local.
func (r *RedisServer) existsAtFast(ctx context.Context, key []byte, readTS uint64) (bool, error) {
	raw, err := r.store.GetAt(ctx, redisStrKey(key), readTS)
	if err == nil {
		alive, decErr := r.stringPayloadIsLive(ctx, key, raw, readTS)
		if decErr != nil {
			return false, errors.WithStack(decErr)
		}
		if alive {
			return true, nil
		}
		// Expired: fall through so other encodings still get their
		// chance. Undecodable payloads are already propagated as an
		// error by stringPayloadIsLive above -- they're a corruption
		// signal, not a "try something else" case.
	} else if !errors.Is(err, store.ErrKeyNotFound) {
		return false, errors.WithStack(err)
	}
	return r.logicalExistsAt(ctx, key, readTS)
}

// stringPayloadIsLive reports whether a redisStrKey payload is still
// TTL-alive. New-format payloads carry their expiry inline; legacy-
// format payloads need the !redis|ttl| index consulted for the TTL.
// Both paths use the LOCAL store, matching existsAtFast's no-proxy
// contract.
func (r *RedisServer) stringPayloadIsLive(ctx context.Context, key, raw []byte, readTS uint64) (bool, error) {
	if isNewRedisStrFormat(raw) {
		_, expireAt, err := decodeRedisStr(raw)
		if err != nil {
			return false, err
		}
		return expireAt == nil || expireAt.After(time.Now()), nil
	}
	ttl, err := r.legacyIndexTTLAt(ctx, key, readTS)
	if err != nil {
		return false, err
	}
	return ttl == nil || ttl.After(time.Now()), nil
}

func (r *RedisServer) keys(conn redcon.Conn, cmd redcon.Command) {
	pattern := cmd.Args[1]

	if r.coordinator.IsLeader() {
		if err := r.coordinator.VerifyLeader(); err != nil {
			conn.WriteError(err.Error())
			return
		}
		keys, err := r.visibleKeys(pattern)
		if err != nil {
			conn.WriteError(err.Error())
			return
		}
		conn.WriteArray(len(keys))
		for _, k := range keys {
			conn.WriteBulk(k)
		}
		return
	}

	keys, err := r.proxyKeys(pattern)
	if err != nil {
		conn.WriteError(err.Error())
		return
	}

	conn.WriteArray(len(keys))
	for _, k := range keys {
		conn.WriteBulkString(k)
	}
}

func (r *RedisServer) localKeys(pattern []byte) ([][]byte, error) {
	if !bytes.Contains(pattern, []byte("*")) {
		return r.localKeysExact(pattern)
	}
	return r.localKeysPattern(pattern)
}

func (r *RedisServer) localKeysExact(pattern []byte) ([][]byte, error) {
	typ, err := r.keyTypeAt(context.Background(), pattern, r.readTS())
	if err != nil {
		return nil, err
	}
	if typ != redisTypeNone {
		return [][]byte{bytes.Clone(pattern)}, nil
	}
	return [][]byte{}, nil
}

// mergeInternalNamespaces scans all internal key namespaces (list, hash, set,
// zset, and other internal prefixes) for keys that match pattern and merges
// them into the caller's keyset via mergeScannedKeys. Called only when the
// pattern is bounded (start != nil) because unbounded scans already cover the
// full keyspace.
func (r *RedisServer) mergeInternalNamespaces(start []byte, pattern []byte, mergeScannedKeys func([]byte, []byte) error) error {
	metaStart, metaEnd := listPatternScanBounds(store.ListMetaPrefix, pattern)
	if err := mergeScannedKeys(metaStart, metaEnd); err != nil {
		return err
	}
	itemStart, itemEnd := listPatternScanBounds(store.ListItemPrefix, pattern)
	if err := mergeScannedKeys(itemStart, itemEnd); err != nil {
		return err
	}
	for _, prefix := range redisInternalPrefixes {
		internalStart, internalEnd := listPatternScanBounds(prefix, pattern)
		if err := mergeScannedKeys(internalStart, internalEnd); err != nil {
			return err
		}
	}
	// Wide-column hash/set/zset keys embed the user-key as
	// <prefix><4-byte-len><userKey><field|member>, so the binary length
	// prefix makes straightforward bounds-based scanning non-trivial.
	// Use the user-key prefix as the lower bound and scan to the end of each
	// namespace; collectUserKeys filters false positives by pattern.
	hashFieldStart := store.HashFieldScanPrefix(start)
	hashFieldEnd := prefixScanEnd([]byte(store.HashFieldPrefix))
	if err := mergeScannedKeys(hashFieldStart, hashFieldEnd); err != nil {
		return err
	}
	setMemberStart := store.SetMemberScanPrefix(start)
	setMemberEnd := prefixScanEnd([]byte(store.SetMemberPrefix))
	if err := mergeScannedKeys(setMemberStart, setMemberEnd); err != nil {
		return err
	}
	zsetMemberStart := store.ZSetMemberScanPrefix(start)
	zsetMemberEnd := prefixScanEnd([]byte(store.ZSetMemberPrefix))
	return mergeScannedKeys(zsetMemberStart, zsetMemberEnd)
}

func (r *RedisServer) localKeysPattern(pattern []byte) ([][]byte, error) {
	start, end := patternScanBounds(pattern)
	keyset := map[string][]byte{}
	readTS := r.readTS()

	mergeScannedKeys := func(scanStart, scanEnd []byte) error {
		keys, err := r.store.ScanAt(context.Background(), scanStart, scanEnd, math.MaxInt, readTS)
		if err != nil {
			return errors.WithStack(err)
		}
		maps.Copy(keyset, r.collectUserKeys(keys, pattern))
		return nil
	}

	if err := mergeScannedKeys(start, end); err != nil {
		return nil, err
	}

	// When the pattern is bounded (start != nil), user-key scans do not
	// naturally include internal data namespaces, so scan those separately
	// and map them back to logical user keys.  For unbounded patterns
	// (e.g. "*"), the full-keyspace scan already covers everything.
	if start != nil {
		if err := r.mergeInternalNamespaces(start, pattern, mergeScannedKeys); err != nil {
			return nil, err
		}
	}

	out := make([][]byte, 0, len(keyset))
	for _, v := range keyset {
		out = append(out, v)
	}
	return out, nil
}

func patternScanBounds(pattern []byte) ([]byte, []byte) {
	if bytes.Equal(pattern, []byte("*")) {
		return nil, nil
	}

	i := bytes.IndexByte(pattern, '*')
	if i <= 0 {
		return nil, nil
	}

	start := bytes.Clone(pattern[:i])
	return start, prefixScanEnd(start)
}

func listPatternScanBounds(prefix string, pattern []byte) ([]byte, []byte) {
	userStart, userEnd := patternScanBounds(pattern)
	prefixBytes := []byte(prefix)

	if userStart == nil && userEnd == nil {
		return prefixBytes, prefixScanEnd(prefixBytes)
	}

	start := append(bytes.Clone(prefixBytes), userStart...)
	if userEnd == nil {
		return start, prefixScanEnd(prefixBytes)
	}
	end := append(bytes.Clone(prefixBytes), userEnd...)
	return start, end
}

func matchesAsteriskPattern(pattern, key []byte) bool {
	parts := bytes.Split(pattern, []byte("*"))
	if len(parts) == 1 {
		return bytes.Equal(pattern, key)
	}

	pos := 0
	if len(parts[0]) > 0 {
		if !bytes.HasPrefix(key, parts[0]) {
			return false
		}
		pos = len(parts[0])
	}

	for i := 1; i < len(parts)-1; i++ {
		part := parts[i]
		if len(part) == 0 {
			continue
		}
		idx := bytes.Index(key[pos:], part)
		if idx < 0 {
			return false
		}
		pos += idx + len(part)
	}

	last := parts[len(parts)-1]
	if len(last) > 0 && !bytes.HasSuffix(key, last) {
		return false
	}

	return true
}

func (r *RedisServer) collectUserKeys(kvs []*store.KVPair, pattern []byte) map[string][]byte {
	keyset := map[string][]byte{}
	for _, kvPair := range kvs {
		userKey := redisVisibleUserKey(kvPair.Key)
		if userKey == nil || !matchesAsteriskPattern(pattern, userKey) {
			continue
		}
		keyset[string(userKey)] = userKey
	}
	return keyset
}

// zsetWideColumnVisibleUserKey handles the ZSet-specific part of wide-column key mapping.
// Returns (nil, true) for internal-only keys and (userKey, true) for visible keys.
func zsetWideColumnVisibleUserKey(key []byte) (userKey []byte, isWide bool) {
	if store.IsZSetMetaDeltaKey(key) || store.IsZSetMetaKey(key) {
		return nil, true
	}
	if store.IsZSetMemberKey(key) {
		return store.ExtractZSetUserKeyFromMember(key), true
	}
	if store.IsZSetScoreKey(key) {
		return store.ExtractZSetUserKeyFromScore(key), true
	}
	return nil, false
}

// wideColumnVisibleUserKey maps a wide-column internal key to its visible user
// key, or returns (nil, true) for internal-only keys (meta/delta), and
// (nil, false) if the key is not a wide-column key at all.
func wideColumnVisibleUserKey(key []byte) (userKey []byte, isWide bool) {
	// Check delta prefixes before meta prefixes (delta starts with meta prefix).
	if store.IsHashMetaDeltaKey(key) || store.IsHashMetaKey(key) {
		return nil, true
	}
	if store.IsHashFieldKey(key) {
		return store.ExtractHashUserKeyFromField(key), true
	}
	if store.IsSetMetaDeltaKey(key) || store.IsSetMetaKey(key) {
		return nil, true
	}
	if store.IsSetMemberKey(key) {
		return store.ExtractSetUserKeyFromMember(key), true
	}
	return zsetWideColumnVisibleUserKey(key)
}

func redisVisibleUserKey(key []byte) []byte {
	if bytes.HasPrefix(key, redisTxnKeyPrefix) || isRedisTTLKey(key) {
		return nil
	}
	// List item keys are visible; meta, delta, and claim keys are internal-only.
	if store.IsListItemKey(key) {
		return store.ExtractListUserKey(key)
	}
	if store.IsListMetaKey(key) || store.IsListMetaDeltaKey(key) || store.IsListClaimKey(key) {
		return nil
	}
	if userKey, isWide := wideColumnVisibleUserKey(key); isWide {
		return userKey
	}
	if userKey := extractRedisInternalUserKey(key); userKey != nil {
		return userKey
	}
	return key
}

func (r *RedisServer) proxyKeys(pattern []byte) ([]string, error) {
	leader := r.coordinator.RaftLeader()
	if leader == "" {
		return nil, ErrLeaderNotFound
	}

	leaderAddr, ok := r.leaderRedis[leader]
	if !ok || leaderAddr == "" {
		return nil, errors.WithStack(errors.Newf("leader redis address unknown for %s", leader))
	}

	cli := r.getOrCreateLeaderClient(leaderAddr)

	ctx, cancel := context.WithTimeout(r.handlerContext(), redisDispatchTimeout)
	defer cancel()

	keys, err := cli.Keys(ctx, string(pattern)).Result()
	return keys, errors.WithStack(err)
}

// MULTI/EXEC/DISCARD handling
func (r *RedisServer) multi(conn redcon.Conn, _ redcon.Command) {
	state := getConnState(conn)
	if state.inTxn {
		conn.WriteError("ERR MULTI calls can not be nested")
		return
	}
	state.inTxn = true
	state.queue = nil
	conn.WriteString("OK")
}

func (r *RedisServer) discard(conn redcon.Conn, _ redcon.Command) {
	state := getConnState(conn)
	if !state.inTxn {
		conn.WriteError("ERR DISCARD without MULTI")
		return
	}
	state.inTxn = false
	state.queue = nil
	conn.WriteString("OK")
}

func (r *RedisServer) exec(conn redcon.Conn, _ redcon.Command) {
	state := getConnState(conn)
	if !state.inTxn {
		conn.WriteError("ERR EXEC without MULTI")
		return
	}

	queue := state.queue
	state.inTxn = false
	state.queue = nil

	// Always execute MULTI/EXEC on the leader so that reads and writes within
	// the transaction see consistent, up-to-date data. Serving transactions
	// on followers risks reading stale MVCC state and producing write cycles.
	if !r.coordinator.IsLeader() {
		r.proxyTransactionToLeader(conn, queue)
		return
	}

	results, err := r.runTransaction(queue)
	if err != nil {
		conn.WriteError(err.Error())
		return
	}

	r.writeResults(conn, results)
}

// proxyTransactionToLeader forwards a MULTI/EXEC transaction to the leader
// node and writes the EXEC response array back to conn.
//
//nolint:cyclop // inherent complexity of MULTI/EXEC proxy; refactoring would obscure the protocol flow
func (r *RedisServer) proxyTransactionToLeader(conn redcon.Conn, queue []redcon.Command) {
	leaderAddr, ok := r.resolveLeaderRedisAddr(conn)
	if !ok {
		return
	}
	cli := r.getOrCreateLeaderClient(leaderAddr)

	ctx, cancel := context.WithTimeout(r.handlerContext(), redisDispatchTimeout)
	defer cancel()

	cmds, err := r.execTxPipeline(ctx, cli, queue)
	if handleProxyTxnError(conn, err) {
		return
	}
	writeProxyCmdsResult(conn, cmds)
}

// resolveLeaderRedisAddr looks up the Redis address of the current Raft leader,
// writes an error reply to conn on failure and returns ("", false).
func (r *RedisServer) resolveLeaderRedisAddr(conn redcon.Conn) (string, bool) {
	leader := r.coordinator.RaftLeader()
	if leader == "" {
		conn.WriteError(ErrLeaderNotFound.Error())
		return "", false
	}
	leaderAddr, ok := r.leaderRedis[leader]
	if !ok || leaderAddr == "" {
		conn.WriteError(fmt.Sprintf("leader redis address unknown for raft address %s", leader))
		return "", false
	}
	return leaderAddr, true
}

// execTxPipeline sends queue as a single TxPipelined batch and returns the
// per-command result handles together with any pipeline-level error.
func (r *RedisServer) execTxPipeline(ctx context.Context, cli *redis.Client, queue []redcon.Command) ([]*redis.Cmd, error) {
	cmds := make([]*redis.Cmd, 0, len(queue))
	_, err := cli.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
		for _, cmd := range queue {
			args := make([]interface{}, len(cmd.Args))
			for i, a := range cmd.Args {
				args[i] = a
			}
			cmds = append(cmds, pipe.Do(ctx, args...))
		}
		return nil
	})
	return cmds, errors.WithStack(err)
}

// handleProxyTxnError writes the appropriate reply for terminal pipeline errors
// and returns true when the caller should return early without writing results.
func handleProxyTxnError(conn redcon.Conn, err error) bool {
	// Transaction aborted (WATCH conflict): Redis protocol requires a Null
	// array reply (*-1\r\n), not a null bulk string or an error.
	// redis.Nil is a per-command nil response and must NOT be treated as an
	// EXEC abort — only redis.TxFailedErr signals that.
	if errors.Is(err, redis.TxFailedErr) {
		conn.WriteArray(-1)
		return true
	}
	// Fatal transport / context error: per-command results are unreliable.
	if err != nil {
		var netErr net.Error
		if errors.Is(err, context.DeadlineExceeded) ||
			errors.Is(err, context.Canceled) ||
			errors.Is(err, io.EOF) ||
			errors.Is(err, io.ErrUnexpectedEOF) ||
			errors.As(err, &netErr) {
			conn.WriteError(err.Error())
			return true
		}
	}
	return false
}

// writeProxyCmdsResult writes an EXEC-style array reply for the given pipeline
// command handles. For any other non-nil per-command errors, each cmd carries
// its own result, which is the correct Redis EXEC semantics.
func writeProxyCmdsResult(conn redcon.Conn, cmds []*redis.Cmd) {
	conn.WriteArray(len(cmds))
	for _, cmd := range cmds {
		writeGoRedisResult(conn, cmd)
	}
}

type txnValue struct {
	raw     []byte
	ttl     *time.Time
	deleted bool
	dirty   bool
	loaded  bool
}

type txnContext struct {
	server     *RedisServer
	working    map[string]*txnValue
	listStates map[string]*listTxnState
	zsetStates map[string]*zsetTxnState
	ttlStates  map[string]*ttlTxnState
	readKeys   map[string][]byte
	startTS    uint64
}

type listTxnState struct {
	meta           store.ListMeta
	metaExists     bool
	appends        [][]byte
	deleted        bool
	purge          bool
	purgeMeta      store.ListMeta
	existingDeltas [][]byte // delta key bytes present at load time; deleted on purge/delete
}

type zsetTxnState struct {
	members     map[string]float64 // current (potentially modified) state
	origMembers map[string]float64 // original state at load time (for wide-column diff)
	isWide      bool               // true if loaded from wide-column !zs|mem| storage
	exists      bool
	dirty       bool
}

type ttlTxnState struct {
	value *time.Time
	dirty bool
}

func stageListDelete(st *listTxnState) {
	if st == nil {
		return
	}
	if st.metaExists {
		st.purge = true
		st.purgeMeta = st.meta
	}
	st.deleted = true
	st.appends = nil
}

func (t *txnContext) trackReadKey(key []byte) {
	if len(key) == 0 {
		return
	}
	k := string(key)
	if _, ok := t.readKeys[k]; ok {
		return
	}
	t.readKeys[k] = bytes.Clone(key)
}

func (t *txnContext) trackTypeReadKeys(key []byte) {
	for _, readKey := range [][]byte{
		listMetaKey(key),
		redisHashKey(key),
		redisSetKey(key),
		redisZSetKey(key),
		redisStreamKey(key),
		redisHLLKey(key),
		redisStrKey(key),
		key, // legacy bare key for fallback reads
	} {
		t.trackReadKey(readKey)
	}
}

func (t *txnContext) load(key []byte) (*txnValue, error) {
	// If the key is already an internal key (e.g., !redis|hash|...,
	// !lst|..., !txn|..., !ddb|..., !s3|..., !dist|...), use it as-is.
	// Otherwise, it's a bare user key for a string value — prefix it.
	storageKey := key
	if !isKnownInternalKey(key) {
		storageKey = redisStrKey(key)
	}
	k := string(storageKey)
	if tv, ok := t.working[k]; ok {
		return tv, nil
	}
	t.trackReadKey(storageKey)
	if !isKnownInternalKey(key) {
		// Track the bare key too for conflict detection on legacy fallback reads.
		t.trackReadKey(key)
	}
	tv := &txnValue{}
	var val []byte
	if !isKnownInternalKey(key) {
		// For bare user string keys, use the fallback-aware reader.
		var (
			err error
			ttl *time.Time
		)
		val, ttl, err = t.server.readRedisStringAt(key, t.startTS)
		if err != nil && !errors.Is(err, store.ErrKeyNotFound) {
			return nil, errors.WithStack(err)
		}
		tv.ttl = ttl
	} else {
		var err error
		val, err = t.server.readValueAt(storageKey, t.startTS)
		if err != nil && !errors.Is(err, store.ErrKeyNotFound) {
			return nil, errors.WithStack(err)
		}
	}
	tv.raw = val
	tv.loaded = true
	t.working[k] = tv
	return tv, nil
}

func (t *txnContext) loadListState(key []byte) (*listTxnState, error) {
	k := string(key)
	if st, ok := t.listStates[k]; ok {
		return st, nil
	}
	ctx := context.Background()
	meta, exists, err := t.server.resolveListMeta(ctx, key, t.startTS)
	if err != nil {
		return nil, err
	}

	// Capture existing delta keys so they can be deleted if the list is later
	// purged or deleted within this transaction. Scan one extra item to detect
	// truncation: if >MaxDeltaScanLimit deltas exist the transaction cannot
	// safely enumerate all of them for deletion, so we return ErrDeltaScanTruncated
	// and let the caller retry after the background compactor has caught up.
	deltaPrefix := store.ListMetaDeltaScanPrefix(key)
	deltaEnd := store.PrefixScanEnd(deltaPrefix)
	deltaKVs, err := t.server.store.ScanAt(ctx, deltaPrefix, deltaEnd, store.MaxDeltaScanLimit+1, t.startTS)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	if len(deltaKVs) > store.MaxDeltaScanLimit {
		return nil, ErrDeltaScanTruncated
	}
	existingDeltas := make([][]byte, 0, len(deltaKVs))
	for _, kv := range deltaKVs {
		existingDeltas = append(existingDeltas, kv.Key)
	}

	st := &listTxnState{
		meta:           meta,
		metaExists:     exists,
		appends:        [][]byte{},
		existingDeltas: existingDeltas,
	}
	t.listStates[k] = st

	// Track the list-item key at the current tail (and the position before the
	// head) so that concurrent RPUSH/LPUSH operations—which write to exactly
	// these positions—trigger a read-write conflict and force a retry.
	// Without this, a MULTI transaction that reads a list via LRANGE can commit
	// with a stale snapshot while a concurrent RPUSH commits a new item,
	// forming an anti-dependency (G2-item) cycle.
	// The base meta key (listMetaKey) is intentionally NOT tracked here: the
	// Delta scheme allows the DeltaCompactor to rewrite it without conflicting
	// with ongoing push/read transactions (see TestRedisTxnValidateReadSet_ListMetaUpdateNoConflict).
	t.trackReadKey(listItemKey(key, meta.Head+meta.Len)) // next RPUSH target
	if meta.Head > math.MinInt64 {
		t.trackReadKey(listItemKey(key, meta.Head-1)) // next LPUSH target
	}

	return st, nil
}

func (t *txnContext) listLength(st *listTxnState) int64 {
	return st.meta.Len + int64(len(st.appends))
}

func (t *txnContext) loadZSetState(key []byte) (*zsetTxnState, error) {
	k := string(key)
	if st, ok := t.zsetStates[k]; ok {
		return st, nil
	}
	t.trackReadKey(redisZSetKey(key))
	// Check TTL: treat expired keys as non-existent.
	ttlSt, err := t.loadTTLState(key)
	if err != nil {
		return nil, err
	}
	if ttlSt.value != nil && !ttlSt.value.After(time.Now()) {
		st := &zsetTxnState{
			members:     map[string]float64{},
			origMembers: map[string]float64{},
			exists:      false,
		}
		t.zsetStates[k] = st
		return st, nil
	}

	// Detect wide-column storage by probing the !zs|mem| prefix.
	memberPrefix := store.ZSetMemberScanPrefix(key)
	memberEnd := store.PrefixScanEnd(memberPrefix)
	probeKVs, probeErr := t.server.store.ScanAt(context.Background(), memberPrefix, memberEnd, 1, t.startTS)
	if probeErr != nil {
		return nil, errors.WithStack(probeErr)
	}
	isWide := len(probeKVs) > 0

	value, exists, err := t.server.loadZSetAt(context.Background(), key, t.startTS)
	if err != nil {
		return nil, err
	}
	members := zsetEntriesToMap(value.Entries)
	// Snapshot the original members for wide-column diff at commit time.
	origMembers := make(map[string]float64, len(members))
	for m, s := range members {
		origMembers[m] = s
	}
	st := &zsetTxnState{
		members:     members,
		origMembers: origMembers,
		isWide:      isWide,
		exists:      exists,
	}
	t.zsetStates[k] = st
	return st, nil
}

func (t *txnContext) loadTTLState(key []byte) (*ttlTxnState, error) {
	k := string(key)
	if st, ok := t.ttlStates[k]; ok {
		return st, nil
	}
	value, err := t.server.ttlAt(context.Background(), key, t.startTS)
	if err != nil {
		return nil, err
	}
	st := &ttlTxnState{value: value}
	t.ttlStates[k] = st
	return st, nil
}

func (t *txnContext) stagedKeyType(key []byte) (redisValueType, error) {
	k := string(key)
	if typ, ok := t.stagedZSetType(k); ok {
		return typ, nil
	}
	if typ, ok := t.stagedListType(k); ok {
		return typ, nil
	}
	if typ, ok := t.stagedStringType(k); ok {
		return typ, nil
	}
	t.trackTypeReadKeys(key)
	return t.server.keyTypeAt(context.Background(), key, t.startTS)
}

func (t *txnContext) stagedZSetType(key string) (redisValueType, bool) {
	st, ok := t.zsetStates[key]
	if !ok || (!st.dirty && !st.exists) {
		return redisTypeNone, false
	}
	if len(st.members) == 0 {
		return redisTypeNone, true
	}
	return redisTypeZSet, true
}

func (t *txnContext) stagedListType(key string) (redisValueType, bool) {
	st, ok := t.listStates[key]
	if !ok {
		return redisTypeNone, false
	}
	if st.deleted {
		return redisTypeNone, true
	}
	if st.metaExists || len(st.appends) > 0 {
		return redisTypeList, true
	}
	return redisTypeNone, false
}

func (t *txnContext) stagedStringType(key string) (redisValueType, bool) {
	tv, ok := t.working[string(redisStrKey([]byte(key)))]
	if !ok {
		return redisTypeNone, false
	}
	if tv.deleted || tv.raw == nil {
		return redisTypeNone, true
	}
	return redisTypeString, true
}

func (t *txnContext) apply(cmd redcon.Command) (redisResult, error) {
	handler, ok := txnApplyHandlers[strings.ToUpper(string(cmd.Args[0]))]
	if !ok {
		return redisResult{}, errors.WithStack(errors.Newf("ERR unsupported command '%s'", cmd.Args[0]))
	}
	return handler(t, cmd)
}

func (t *txnContext) applyExpireSeconds(cmd redcon.Command) (redisResult, error) {
	return t.applyExpire(cmd, time.Second)
}

func (t *txnContext) applyExpireMilliseconds(cmd redcon.Command) (redisResult, error) {
	return t.applyExpire(cmd, time.Millisecond)
}

func (t *txnContext) applySet(cmd redcon.Command) (redisResult, error) {
	if isList, err := t.server.isListKeyAt(context.Background(), cmd.Args[1], t.startTS); err != nil {
		return redisResult{}, err
	} else if isList {
		return redisResult{typ: resultError, err: errors.New("WRONGTYPE Operation against a key holding the wrong kind of value")}, nil
	}

	opts, err := parseRedisSetOptions(cmd.Args[3:], time.Now())
	if err != nil {
		return redisResult{}, err
	}

	// NX/XX: skip the write if the key-existence condition is not met.
	blocked, res, err := t.applySetCondition(cmd.Args[1], opts)
	if err != nil {
		return redisResult{}, err
	}
	if blocked {
		return res, nil
	}

	tv, err := t.load(cmd.Args[1])
	if err != nil {
		return redisResult{}, err
	}

	var oldValue []byte
	if opts.returnOld && !tv.deleted {
		oldValue = tv.raw
	}

	tv.raw = cmd.Args[2]
	tv.deleted = false
	tv.dirty = true

	// Always update TTL state: EX/PX sets a new expiry; a plain SET clears it
	// (opts.ttl == nil → nil stored → PERSIST semantics, matching Redis behaviour).
	if err := t.applySetTTL(cmd.Args[1], opts.ttl); err != nil {
		return redisResult{}, err
	}

	return applySetResult(opts, oldValue), nil
}

// applySetCondition checks NX/XX conditions.  Returns (blocked, result, err).
// blocked=true means the condition prevented the write; callers should return result.
// Returns (false, _, nil) immediately when no condition is set.
func (t *txnContext) applySetCondition(key []byte, opts redisSetOptions) (bool, redisResult, error) {
	if !opts.existsCond && !opts.missingCond {
		return false, redisResult{}, nil
	}
	typ, err := t.stagedKeyType(key)
	if err != nil {
		return false, redisResult{}, err
	}
	exists := typ != redisTypeNone
	if (opts.missingCond && exists) || (opts.existsCond && !exists) {
		return true, redisResult{typ: resultNil}, nil
	}
	return false, redisResult{}, nil
}

// applySetTTL stores the expiry in ttlStates so flushTTLToBuffer sends it to
// the TTLBuffer after a successful commit.
func (t *txnContext) applySetTTL(key []byte, expireAt *time.Time) error {
	ttlSt, err := t.loadTTLState(key)
	if err != nil {
		return err
	}
	ttlSt.value = expireAt
	ttlSt.dirty = true
	return nil
}

// applySetResult returns the appropriate redisResult for a completed SET.
func applySetResult(opts redisSetOptions, oldValue []byte) redisResult {
	if !opts.returnOld {
		return redisResult{typ: resultString, str: "OK"}
	}
	if oldValue == nil {
		return redisResult{typ: resultNil}
	}
	return redisResult{typ: resultBulk, bulk: oldValue}
}

func (t *txnContext) applyDel(cmd redcon.Command) (redisResult, error) {
	var deleted int64
	for _, key := range cmd.Args[1:] {
		typ, err := t.stagedKeyType(key)
		if err != nil {
			return redisResult{}, err
		}
		if typ == redisTypeNone {
			continue
		}
		if _, err := t.stageKeyDeletion(key); err != nil {
			return redisResult{}, err
		}
		deleted++
	}
	return redisResult{typ: resultInt, integer: deleted}, nil
}

func (t *txnContext) applyGet(cmd redcon.Command) (redisResult, error) {
	typ, err := t.stagedKeyType(cmd.Args[1])
	if err != nil {
		return redisResult{}, err
	}
	if isNonStringCollectionType(typ) {
		return redisResult{typ: resultError, err: wrongTypeError()}, nil
	}

	tv, err := t.load(cmd.Args[1])
	if err != nil {
		return redisResult{}, err
	}
	if tv.deleted || tv.raw == nil {
		return redisResult{typ: resultNil}, nil
	}
	return redisResult{typ: resultBulk, bulk: tv.raw}, nil
}

func (t *txnContext) applyExists(cmd redcon.Command) (redisResult, error) {
	var count int64
	for _, key := range cmd.Args[1:] {
		typ, err := t.stagedKeyType(key)
		if err != nil {
			return redisResult{}, err
		}
		if typ != redisTypeNone {
			count++
		}
	}
	return redisResult{typ: resultInt, integer: count}, nil
}

func (t *txnContext) applyRPush(cmd redcon.Command) (redisResult, error) {
	st, err := t.loadListState(cmd.Args[1])
	if err != nil {
		return redisResult{}, err
	}
	if st.deleted {
		if st.metaExists {
			st.purge = true
			st.purgeMeta = st.meta
		}
		// DEL followed by RPUSH in the same transaction recreates the list.
		st.deleted = false
		st.metaExists = false
		st.meta = store.ListMeta{}
		st.appends = nil
	}

	for _, v := range cmd.Args[2:] {
		st.appends = append(st.appends, bytes.Clone(v))
	}

	return redisResult{typ: resultInt, integer: t.listLength(st)}, nil
}

func (t *txnContext) applyLRange(cmd redcon.Command) (redisResult, error) {
	st, err := t.loadListState(cmd.Args[1])
	if err != nil {
		return redisResult{}, err
	}

	s, e, err := parseRangeBounds(cmd.Args[2], cmd.Args[3], int(t.listLength(st)))
	if err != nil {
		return redisResult{}, err
	}
	if e < s {
		return redisResult{typ: resultArray, arr: []string{}}, nil
	}

	out, err := t.listRangeValues(cmd.Args[1], st, s, e)
	if err != nil {
		return redisResult{}, err
	}

	return redisResult{typ: resultArray, arr: out}, nil
}

func (t *txnContext) applyZIncrBy(cmd redcon.Command) (redisResult, error) {
	typ, err := t.stagedKeyType(cmd.Args[1])
	if err != nil {
		return redisResult{}, err
	}
	if typ != redisTypeNone && typ != redisTypeZSet {
		return redisResult{typ: resultError, err: wrongTypeError()}, nil
	}

	inc, err := strconv.ParseFloat(string(cmd.Args[2]), 64)
	if err != nil {
		return redisResult{}, errors.WithStack(err)
	}
	st, err := t.loadZSetState(cmd.Args[1])
	if err != nil {
		return redisResult{}, err
	}
	member := string(cmd.Args[3])
	st.members[member] += inc
	st.dirty = true
	return redisResult{typ: resultBulk, bulk: []byte(formatRedisFloat(st.members[member]))}, nil
}

func (t *txnContext) applyExpire(cmd redcon.Command, unit time.Duration) (redisResult, error) {
	typ, err := t.stagedKeyType(cmd.Args[1])
	if err != nil {
		return redisResult{}, err
	}
	if typ == redisTypeNone {
		return redisResult{typ: resultInt, integer: 0}, nil
	}

	ttl, err := strconv.ParseInt(string(cmd.Args[2]), 10, 64)
	if err != nil {
		return redisResult{}, errors.WithStack(err)
	}
	nxOnly, err := parseExpireNXOnly(cmd.Args[3:])
	if err != nil {
		return redisResult{}, err
	}

	state, err := t.loadTTLState(cmd.Args[1])
	if err != nil {
		return redisResult{}, err
	}
	if nxOnly && hasActiveTTL(state.value, time.Now()) {
		return redisResult{typ: resultInt, integer: 0}, nil
	}

	if ttl <= 0 {
		return t.stageKeyDeletion(cmd.Args[1])
	}
	return t.applyPositiveExpire(cmd.Args[1], ttl, unit, typ, state)
}

func (t *txnContext) applyPositiveExpire(key []byte, ttl int64, unit time.Duration, typ redisValueType, state *ttlTxnState) (redisResult, error) {
	if ttl > math.MaxInt64/int64(unit) {
		return redisResult{}, errors.New("ERR invalid expire time in command")
	}
	expireAt := time.Now().Add(time.Duration(ttl) * unit)
	state.value = &expireAt
	state.dirty = true
	if typ == redisTypeString {
		plain, err := t.server.isPlainRedisString(context.Background(), key, t.startTS)
		if err != nil {
			return redisResult{}, err
		}
		if plain {
			return t.markStringDirty(key)
		}
		// HLL is reported as redisTypeString but stores its payload under
		// !redis|hll|<key>; keep TTL in the legacy scan index via buildTTLElems.
	}
	return redisResult{typ: resultInt, integer: 1}, nil
}

// markStringDirty loads the string value into the working set so that
// buildKeyElems will re-encode it with the updated embedded TTL.
func (t *txnContext) markStringDirty(key []byte) (redisResult, error) {
	tv, err := t.load(key)
	if err != nil {
		return redisResult{}, err
	}
	tv.dirty = true
	return redisResult{typ: resultInt, integer: 1}, nil
}

func (t *txnContext) stageKeyDeletion(key []byte) (redisResult, error) {
	// Mark the list for deletion.
	st, err := t.loadListState(key)
	if err != nil {
		return redisResult{}, err
	}
	stageListDelete(st)
	// Mark the string/main value for deletion.
	tv, err := t.load(key)
	if err != nil {
		return redisResult{}, err
	}
	tv.deleted = true
	tv.dirty = true
	// Mark TTL for deletion.
	ttlState, err := t.loadTTLState(key)
	if err != nil {
		return redisResult{}, err
	}
	ttlState.value = nil
	ttlState.dirty = true
	// Mark zset for deletion. Use empty map (not nil) so that subsequent
	// writes (e.g. ZINCRBY) in the same transaction can safely insert.
	zs, err := t.loadZSetState(key)
	if err != nil {
		return redisResult{}, err
	}
	zs.members = map[string]float64{}
	zs.exists = false
	zs.dirty = true
	// Mark hash, set, stream, and HLL internal keys for deletion.
	for _, internalKey := range [][]byte{
		redisHashKey(key),
		redisSetKey(key),
		redisStreamKey(key),
		redisHLLKey(key),
	} {
		iv, err := t.load(internalKey)
		if err != nil {
			return redisResult{}, err
		}
		iv.deleted = true
		iv.dirty = true
	}
	// Mark legacy bare string key for deletion. We bypass load() here
	// because load() auto-prefixes bare keys to !redis|str|.
	// Track the bare key in the read set for conflict detection.
	t.trackReadKey(key)
	bareK := string(key)
	if _, ok := t.working[bareK]; !ok {
		t.working[bareK] = &txnValue{}
	}
	t.working[bareK].deleted = true
	t.working[bareK].dirty = true
	return redisResult{typ: resultInt, integer: 1}, nil
}

func parseRangeBounds(startRaw, endRaw []byte, total int) (int, int, error) {
	start, err := parseInt(startRaw)
	if err != nil {
		return 0, 0, err
	}
	end, err := parseInt(endRaw)
	if err != nil {
		return 0, 0, err
	}
	s, e := clampRange(start, end, total)
	return s, e, nil
}

func (t *txnContext) listRangeValues(key []byte, st *listTxnState, s, e int) ([]string, error) {
	persistedLen := int(st.meta.Len)

	switch {
	case e < persistedLen:
		return t.server.fetchListRange(context.Background(), key, st.meta, int64(s), int64(e), t.startTS)
	case s >= persistedLen:
		return appendValues(st.appends, s-persistedLen, e-persistedLen), nil
	default:
		head, err := t.server.fetchListRange(context.Background(), key, st.meta, int64(s), int64(persistedLen-1), t.startTS)
		if err != nil {
			return nil, err
		}
		tail := appendValues(st.appends, 0, e-persistedLen)
		return append(head, tail...), nil
	}
}

func appendValues(buf [][]byte, start, end int) []string {
	out := make([]string, 0, end-start+1)
	for i := start; i <= end; i++ {
		out = append(out, string(buf[i]))
	}
	return out
}

func (t *txnContext) validateReadSet(ctx context.Context) error {
	for _, key := range t.readKeys {
		latestTS, exists, err := t.server.store.LatestCommitTS(ctx, key)
		if err != nil {
			return errors.WithStack(err)
		}
		if exists && latestTS > t.startTS {
			return errors.WithStack(store.NewWriteConflictError(key))
		}
	}
	return nil
}

func (t *txnContext) commit() error {
	elems := t.buildKeyElems()

	// Pre-allocate commitTS so Delta keys can embed it in their bytes before
	// the coordinator assigns it during Dispatch.
	commitTS := t.server.coordinator.Clock().Next()
	listElems := t.buildListElems(commitTS)
	zsetElems, err := t.buildZSetElems(commitTS)
	if err != nil {
		return err
	}
	// TTL elements: string keys have TTL embedded in value (buildKeyElems handles that),
	// non-string keys get a !redis|ttl| element written in the same transaction.
	ttlElems := t.buildTTLElems()

	elems = append(elems, listElems...)
	elems = append(elems, zsetElems...)
	elems = append(elems, ttlElems...)
	if len(elems) == 0 {
		return nil
	}

	readKeys := make([][]byte, 0, len(t.readKeys))
	for _, k := range t.readKeys {
		readKeys = append(readKeys, k)
	}
	group := &kv.OperationGroup[kv.OP]{
		IsTxn:    true,
		Elems:    elems,
		StartTS:  t.startTS,
		CommitTS: commitTS,
		ReadKeys: readKeys,
	}
	ctx, cancel := context.WithTimeout(t.server.handlerContext(), redisDispatchTimeout)
	defer cancel()
	if _, err := t.server.coordinator.Dispatch(ctx, group); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

// stringValueAndTTLElem returns the encoded string value and an optional
// !redis|ttl| scan-index mutation for a string write. Dirty EXPIRE/PERSIST
// state takes priority; otherwise the TTL loaded with the value is preserved
// so commands like INCR or SETBIT inside MULTI/EXEC don't clear it. A dirty
// PERSIST emits a Del so the sweeper cannot later expire a persistent key.
func (t *txnContext) stringValueAndTTLElem(userKey []byte, tv *txnValue) ([]byte, *kv.Elem[kv.OP]) {
	ttl := tv.ttl
	ttlSt := t.ttlStates[string(userKey)]
	if ttlSt != nil && ttlSt.dirty {
		ttl = ttlSt.value
	}
	value := encodeRedisStr(tv.raw, ttl)
	if ttl != nil {
		return value, &kv.Elem[kv.OP]{Op: kv.Put, Key: redisTTLKey(userKey), Value: encodeRedisTTL(*ttl)}
	}
	// ttl is nil: emit Del when there was a prior TTL (loaded or dirty-cleared)
	// so the sweeper cannot later expire a now-persistent key or hit a stale index.
	if tv.ttl != nil || (ttlSt != nil && ttlSt.dirty) {
		return value, &kv.Elem[kv.OP]{Op: kv.Del, Key: redisTTLKey(userKey)}
	}
	return value, nil
}

func (t *txnContext) buildKeyElems() []*kv.Elem[kv.OP] {
	keys := make([]string, 0, len(t.working))
	for k := range t.working {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var elems []*kv.Elem[kv.OP]
	for _, k := range keys {
		tv := t.working[k]
		if !tv.dirty {
			continue
		}
		storageKey := []byte(k)
		if tv.deleted {
			elems = append(elems, &kv.Elem[kv.OP]{Op: kv.Del, Key: storageKey})
			// Deleting a string anchor must also drop any stale !redis|ttl|
			// scan-index entry; buildTTLElems skips strings because it assumes
			// the inline-TTL path owns them.
			if bytes.HasPrefix(storageKey, []byte(redisStrPrefix)) {
				userKey := storageKey[len(redisStrPrefix):]
				elems = append(elems, &kv.Elem[kv.OP]{Op: kv.Del, Key: redisTTLKey(userKey)})
			}
			continue
		}
		value := tv.raw
		if bytes.HasPrefix(storageKey, []byte(redisStrPrefix)) {
			userKey := storageKey[len(redisStrPrefix):]
			var extra *kv.Elem[kv.OP]
			value, extra = t.stringValueAndTTLElem(userKey, tv)
			if extra != nil {
				elems = append(elems, extra)
			}
		}
		elems = append(elems, &kv.Elem[kv.OP]{Op: kv.Put, Key: storageKey, Value: value})
	}
	return elems
}

func listDeleteMeta(st *listTxnState) (store.ListMeta, bool) {
	switch {
	case st.metaExists:
		return st.meta, true
	case st.purge:
		return st.purgeMeta, true
	default:
		return store.ListMeta{}, false
	}
}

func appendListDeleteOps(elems []*kv.Elem[kv.OP], userKey []byte, meta store.ListMeta) []*kv.Elem[kv.OP] {
	for seq := meta.Head; seq < meta.Tail; seq++ {
		elems = append(elems, &kv.Elem[kv.OP]{Op: kv.Del, Key: listItemKey(userKey, seq)})
	}
	return append(elems, &kv.Elem[kv.OP]{Op: kv.Del, Key: listMetaKey(userKey)})
}

func (t *txnContext) buildListElems(commitTS uint64) []*kv.Elem[kv.OP] {
	listKeys := make([]string, 0, len(t.listStates))
	for k := range t.listStates {
		listKeys = append(listKeys, k)
	}
	sort.Strings(listKeys)

	var elems []*kv.Elem[kv.OP]
	var seqInTxn uint32
	for _, k := range listKeys {
		st := t.listStates[k]
		userKey := []byte(k)

		if st.deleted {
			if meta, ok := listDeleteMeta(st); ok {
				elems = appendListDeleteOps(elems, userKey, meta)
			}
			// Delete existing delta keys so they don't survive the logical delete.
			for _, dk := range st.existingDeltas {
				elems = append(elems, &kv.Elem[kv.OP]{Op: kv.Del, Key: dk})
			}
			continue
		}
		if len(st.appends) == 0 {
			continue
		}
		if st.purge {
			elems = appendListDeleteOps(elems, userKey, st.purgeMeta)
			// Delete existing delta keys so they don't accumulate after DEL+RPUSH.
			for _, dk := range st.existingDeltas {
				elems = append(elems, &kv.Elem[kv.OP]{Op: kv.Del, Key: dk})
			}
		}

		startSeq := st.meta.Head + st.meta.Len
		for i, v := range st.appends {
			elems = append(elems, &kv.Elem[kv.OP]{
				Op:    kv.Put,
				Key:   listItemKey(userKey, startSeq+int64(i)),
				Value: v,
			})
		}

		// Emit a Delta key instead of updating the base metadata key.
		// Each list key in this transaction gets a unique seqInTxn.
		n := int64(len(st.appends))
		deltaVal := store.MarshalListMetaDelta(store.ListMetaDelta{HeadDelta: 0, LenDelta: n})
		elems = append(elems, &kv.Elem[kv.OP]{
			Op:    kv.Put,
			Key:   store.ListMetaDeltaKey(userKey, commitTS, seqInTxn),
			Value: deltaVal,
		})
		seqInTxn++
	}
	return elems
}

func (t *txnContext) buildZSetElems(commitTS uint64) ([]*kv.Elem[kv.OP], error) {
	keys := make([]string, 0, len(t.zsetStates))
	for k := range t.zsetStates {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	elems := make([]*kv.Elem[kv.OP], 0, len(keys))
	seqInTxn := uint32(0)
	for _, k := range keys {
		st := t.zsetStates[k]
		if !st.dirty {
			continue
		}
		key := []byte(k)
		if st.isWide {
			wideElems, lenDelta := buildZSetWideElems(key, st)
			elems = append(elems, wideElems...)
			if lenDelta != 0 {
				deltaVal := store.MarshalZSetMetaDelta(store.ZSetMetaDelta{LenDelta: lenDelta})
				elems = append(elems, &kv.Elem[kv.OP]{
					Op:    kv.Put,
					Key:   store.ZSetMetaDeltaKey(key, commitTS, seqInTxn),
					Value: deltaVal,
				})
				seqInTxn++
			}
			continue
		}
		// Legacy blob path.
		if len(st.members) == 0 {
			elems = append(elems, &kv.Elem[kv.OP]{Op: kv.Del, Key: redisZSetKey(key)})
			continue
		}
		payload, err := marshalZSetValue(redisZSetValue{Entries: zsetMapToEntries(st.members)})
		if err != nil {
			return nil, err
		}
		elems = append(elems, &kv.Elem[kv.OP]{Op: kv.Put, Key: redisZSetKey(key), Value: payload})
	}
	return elems, nil
}

// buildZSetWideElems computes the minimal set of ops to transition from st.origMembers to
// st.members in wide-column format. Returns the ops and the net length delta.
func buildZSetWideElems(key []byte, st *zsetTxnState) ([]*kv.Elem[kv.OP], int64) {
	elems := make([]*kv.Elem[kv.OP], 0, len(st.members)+len(st.origMembers))
	var lenDelta int64

	// Deletions: members removed or score changed (old score index must be removed).
	for member, oldScore := range st.origMembers {
		newScore, inNew := st.members[member]
		if !inNew {
			// Fully removed.
			elems = append(elems,
				&kv.Elem[kv.OP]{Op: kv.Del, Key: store.ZSetMemberKey(key, []byte(member))},
				&kv.Elem[kv.OP]{Op: kv.Del, Key: store.ZSetScoreKey(key, oldScore, []byte(member))},
			)
			lenDelta--
		} else if newScore != oldScore {
			// Score updated: delete old score index.
			elems = append(elems, &kv.Elem[kv.OP]{Op: kv.Del, Key: store.ZSetScoreKey(key, oldScore, []byte(member))})
		}
	}

	// Insertions / updates.
	for member, newScore := range st.members {
		_, wasOrig := st.origMembers[member]
		elems = append(elems,
			&kv.Elem[kv.OP]{Op: kv.Put, Key: store.ZSetMemberKey(key, []byte(member)), Value: store.MarshalZSetScore(newScore)},
			&kv.Elem[kv.OP]{Op: kv.Put, Key: store.ZSetScoreKey(key, newScore, []byte(member)), Value: []byte{}},
		)
		if !wasOrig {
			lenDelta++
		}
	}
	return elems, lenDelta
}

// buildTTLElems returns !redis|ttl| Raft elements for non-string keys with dirty TTL state.
// String keys have TTL embedded in the value; they are handled by buildKeyElems.
func (t *txnContext) buildTTLElems() []*kv.Elem[kv.OP] {
	var elems []*kv.Elem[kv.OP]
	for k, st := range t.ttlStates {
		if !st.dirty {
			continue
		}
		// String keys encode TTL inside the value in buildKeyElems; skip them here.
		if _, isString := t.working[string(redisStrKey([]byte(k)))]; isString {
			continue
		}
		if st.value == nil {
			elems = append(elems, &kv.Elem[kv.OP]{Op: kv.Del, Key: redisTTLKey([]byte(k))})
		} else {
			elems = append(elems, &kv.Elem[kv.OP]{Op: kv.Put, Key: redisTTLKey([]byte(k)), Value: encodeRedisTTL(*st.value)})
		}
	}
	return elems
}

func (r *RedisServer) runTransaction(queue []redcon.Command) ([]redisResult, error) {
	dispatchCtx, cancel := context.WithTimeout(r.handlerContext(), redisDispatchTimeout)
	defer cancel()

	var results []redisResult
	err := r.retryRedisWrite(dispatchCtx, func() error {
		startTS := r.txnStartTS()
		readPin := r.pinReadTS(startTS)
		defer readPin.Release()

		txn := &txnContext{
			server:     r,
			working:    map[string]*txnValue{},
			listStates: map[string]*listTxnState{},
			zsetStates: map[string]*zsetTxnState{},
			ttlStates:  map[string]*ttlTxnState{},
			readKeys:   map[string][]byte{},
			startTS:    startTS,
		}

		nextResults := make([]redisResult, 0, len(queue))
		for _, cmd := range queue {
			res, err := txn.apply(cmd)
			if err != nil {
				return err
			}
			nextResults = append(nextResults, res)
		}

		if err := txn.validateReadSet(dispatchCtx); err != nil {
			return err
		}
		if err := txn.commit(); err != nil {
			return err
		}
		results = nextResults
		return nil
	})
	if err != nil {
		return nil, err
	}

	return results, nil
}

func (r *RedisServer) txnStartTS() uint64 {
	// store.LastCommitTS() is the authoritative safe-snapshot watermark: it is
	// updated atomically only AFTER the corresponding Pebble batch commit, so
	// every version with commitTS ≤ store.LastCommitTS() is guaranteed visible
	// in the store when we read.
	//
	// We must NOT return clock.Next() here.  clock.Next() can be AHEAD of
	// store.LastCommitTS() because concurrent dispatchTxn calls advance the HLC
	// before their Raft entry is applied.  If startTS = clock.Next() = T, a
	// concurrent transaction that already called clock.Next() to obtain
	// commitTS = T-1 and is still in the Raft pipeline will satisfy
	//   latestTS(key) = T-1  ≤  T = startTS
	// causing the FSM conflict check (latestTS > startTS) to silently pass even
	// though we read stale data.  This allows two concurrent RPUSHes to pick the
	// same sequence number, with the second overwriting the first — a lost write.
	//
	// Using store.LastCommitTS() directly closes this gap: any concurrent commit
	// at > maxTS triggers a WriteConflict and a retry via retryRedisWrite.
	//
	// The Observe call still advances the HLC so that dispatchTxn's clock.Next()
	// produces a commitTS strictly greater than maxTS (leader-election safety).
	//
	// When maxTS is 0 (empty store) we return 1 so the coordinator treats this
	// as a valid startTS and does not override it with clock.Next() — which
	// could be ahead of unapplied Raft entries and reintroduce the anomaly.
	var maxTS uint64
	if r.store != nil {
		maxTS = r.store.LastCommitTS()
	}
	if r.coordinator != nil && r.coordinator.Clock() != nil && maxTS > 0 {
		r.coordinator.Clock().Observe(maxTS)
	}
	if maxTS == 0 {
		return 1
	}
	return maxTS
}

func (r *RedisServer) writeResults(conn redcon.Conn, results []redisResult) {
	conn.WriteArray(len(results))
	for _, res := range results {
		switch res.typ {
		case resultNil:
			conn.WriteNull()
		case resultError:
			conn.WriteError(res.err.Error())
		case resultBulk:
			conn.WriteBulk(res.bulk)
		case resultString:
			conn.WriteString(res.str)
		case resultArray:
			conn.WriteArray(len(res.arr))
			for _, s := range res.arr {
				conn.WriteBulkString(s)
			}
		case resultInt:
			conn.WriteInt64(res.integer)
		default:
			conn.WriteNull()
		}
	}
}

func listMetaKey(userKey []byte) []byte {
	return store.ListMetaKey(userKey)
}

func listItemKey(userKey []byte, seq int64) []byte {
	return store.ListItemKey(userKey, seq)
}

func clampRange(start, end, length int) (int, int) {
	if start < 0 {
		start = length + start
	}
	if end < 0 {
		end = length + end
	}
	if start < 0 {
		start = 0
	}
	if end >= length {
		end = length - 1
	}
	if end < start {
		return 0, -1
	}
	return start, end
}

func (r *RedisServer) loadListMetaAt(ctx context.Context, key []byte, readTS uint64) (store.ListMeta, bool, error) {
	val, err := r.store.GetAt(ctx, store.ListMetaKey(key), readTS)
	if err != nil {
		if errors.Is(err, store.ErrKeyNotFound) {
			return store.ListMeta{}, false, nil
		}
		return store.ListMeta{}, false, errors.WithStack(err)
	}
	meta, err := store.UnmarshalListMeta(val)
	if err != nil {
		return store.ListMeta{}, false, errors.WithStack(err)
	}
	return meta, true, nil
}

func (r *RedisServer) isListKeyAt(ctx context.Context, key []byte, readTS uint64) (bool, error) {
	_, exists, err := r.loadListMetaAt(ctx, key, readTS)
	return exists, err
}

// buildRPushOps creates operations to append values to the tail of a list using
// the Delta pattern. Instead of writing to the base metadata key (causing OCC
// conflicts), it emits a single ListMetaDelta key with LenDelta = len(values).
// commitTS must be pre-allocated via dispatchElemsWithCommitTS; seqInTxn
// disambiguates multiple push operations in the same transaction.
func (r *RedisServer) buildRPushOps(meta store.ListMeta, key []byte, values [][]byte, commitTS uint64, seqInTxn uint32) ([]*kv.Elem[kv.OP], store.ListMeta, error) {
	if len(values) == 0 {
		return nil, meta, nil
	}

	elems := make([]*kv.Elem[kv.OP], 0, len(values)+1)
	seq := meta.Head + meta.Len
	for _, v := range values {
		vCopy := bytes.Clone(v)
		elems = append(elems, &kv.Elem[kv.OP]{Op: kv.Put, Key: listItemKey(key, seq), Value: vCopy})
		seq++
	}

	// Emit a Delta key instead of writing the base meta key.
	delta := store.MarshalListMetaDelta(store.ListMetaDelta{HeadDelta: 0, LenDelta: int64(len(values))})
	elems = append(elems, &kv.Elem[kv.OP]{Op: kv.Put, Key: store.ListMetaDeltaKey(key, commitTS, seqInTxn), Value: delta})

	meta.Len += int64(len(values))
	meta.Tail = meta.Head + meta.Len
	return elems, meta, nil
}

// listPushBuildFn is the type for functions that build list push operations.
type listPushBuildFn func(meta store.ListMeta, key []byte, values [][]byte, commitTS uint64, seqInTxn uint32) ([]*kv.Elem[kv.OP], store.ListMeta, error)

// listPushCore is the shared retry loop for RPUSH and LPUSH. The caller supplies
// a buildFn that assembles the specific operations (RPUSH appends to tail, LPUSH
// prepends to head).
func (r *RedisServer) listPushCore(ctx context.Context, key []byte, values [][]byte, buildFn listPushBuildFn) (int64, error) {
	var newLen int64
	err := r.retryRedisWrite(ctx, func() error {
		readTS := r.readTS()
		meta, _, err := r.resolveListMeta(ctx, key, readTS)
		if err != nil {
			return err
		}

		// Pre-allocate commitTS so we can embed it in the Delta key.
		commitTS := r.coordinator.Clock().Next()
		ops, updatedMeta, err := buildFn(meta, key, values, commitTS, 0)
		if err != nil {
			return err
		}
		if len(ops) == 0 {
			newLen = updatedMeta.Len
			return nil
		}

		// Dispatch with the pre-allocated commitTS.
		_, dispErr := r.coordinator.Dispatch(ctx, &kv.OperationGroup[kv.OP]{
			IsTxn:    true,
			StartTS:  normalizeStartTS(readTS),
			CommitTS: commitTS,
			Elems:    ops,
		})
		if dispErr != nil {
			return errors.WithStack(dispErr)
		}
		newLen = updatedMeta.Len
		return nil
	})
	return newLen, err
}

func (r *RedisServer) listRPush(ctx context.Context, key []byte, values [][]byte) (int64, error) {
	return r.listPushCore(ctx, key, values, r.buildRPushOps)
}

// buildLPushOps creates operations to prepend values to the head of a list using
// the Delta pattern. LPUSH reverses the order of arguments:
// LPUSH key a b c → [c, b, a, ...existing].
func (r *RedisServer) buildLPushOps(meta store.ListMeta, key []byte, values [][]byte, commitTS uint64, seqInTxn uint32) ([]*kv.Elem[kv.OP], store.ListMeta, error) {
	if len(values) == 0 {
		return nil, meta, nil
	}

	n := int64(len(values))
	if meta.Head < math.MinInt64+n {
		return nil, meta, errors.WithStack(errors.New("LPUSH would underflow list Head sequence number"))
	}
	elems := make([]*kv.Elem[kv.OP], 0, len(values)+1)
	// LPUSH reverses args, so last arg gets the lowest sequence number.
	newHead := meta.Head - n
	for i, v := range values {
		// values[0]=a, values[1]=b, values[2]=c → seq ordering: c(newHead), b(newHead+1), a(newHead+2)
		seq := newHead + n - 1 - int64(i)
		vCopy := bytes.Clone(v)
		elems = append(elems, &kv.Elem[kv.OP]{Op: kv.Put, Key: listItemKey(key, seq), Value: vCopy})
	}

	// Emit a Delta key instead of writing the base meta key.
	delta := store.MarshalListMetaDelta(store.ListMetaDelta{HeadDelta: -n, LenDelta: n})
	elems = append(elems, &kv.Elem[kv.OP]{Op: kv.Put, Key: store.ListMetaDeltaKey(key, commitTS, seqInTxn), Value: delta})

	meta.Head = newHead
	meta.Len += n
	return elems, meta, nil
}

func (r *RedisServer) listLPush(ctx context.Context, key []byte, values [][]byte) (int64, error) {
	return r.listPushCore(ctx, key, values, r.buildLPushOps)
}

// clampPopCount clamps count to [1, min(listLen, maxWideColumnItems)].
// An error is returned when the effective count would exceed maxWideColumnItems,
// which guards against OOM from enormous claim-key allocations.
func clampPopCount(count int, listLen int64) (int64, error) {
	n := int64(count)
	if n > listLen {
		n = listLen
	}
	if n > int64(maxWideColumnItems) {
		return 0, errors.Wrapf(ErrCollectionTooLarge, "LPOP/RPOP count %d exceeds maximum %d", n, maxWideColumnItems)
	}
	return n, nil
}

// listPopClaim implements LPOP (left=true) or RPOP (left=false) using the
// Claim pattern to avoid write-write conflicts on the list metadata key.
// For each item popped it emits:
//   - Del(listItemKey) — removes the item value
//   - Put(listClaimKey, empty) — uniqueness guard; conflicts if another txn
//     claims the same sequence number concurrently
//
// A single ListMetaDelta with {HeadDelta, LenDelta} is emitted for the whole batch.
//
// Returns the popped values (len ≤ count) or nil if the list does not exist.
func (r *RedisServer) buildListPopElems(ctx context.Context, key []byte, meta store.ListMeta, n int64, left bool, readTS uint64) ([]string, []*kv.Elem[kv.OP], error) {
	// Build the [start, end) scan range covering exactly the n items to pop.
	// n is already clamped to meta.Len by the caller, so no overflow is possible.
	var startKey, endKey []byte
	if left {
		startKey = listItemKey(key, meta.Head)
		endKey = listItemKey(key, meta.Head+n)
	} else {
		startKey = listItemKey(key, meta.Tail-n)
		endKey = listItemKey(key, meta.Tail)
	}

	var kvps []*store.KVPair
	var scanErr error
	if left {
		kvps, scanErr = r.store.ScanAt(ctx, startKey, endKey, int(n), readTS)
	} else {
		kvps, scanErr = r.store.ReverseScanAt(ctx, startKey, endKey, int(n), readTS)
	}
	if scanErr != nil {
		return nil, nil, errors.WithStack(scanErr)
	}

	// Emit claim keys for every sequence position in the claimed range, including
	// holes. This ensures that two concurrent pops over the same hole produce a
	// write conflict rather than both silently advancing HeadDelta over the same
	// empty position, which would otherwise orphan later items.
	var claimStart, claimEnd int64
	if left {
		claimStart = meta.Head
		claimEnd = meta.Head + n
	} else {
		claimStart = meta.Tail - n
		claimEnd = meta.Tail
	}
	// Capacity: n claim keys + n Del(item) for found items + 1 for the delta key appended by caller.
	// n is bounded by maxWideColumnItems (100_000) so the int conversion is safe.
	elems := make([]*kv.Elem[kv.OP], 0, int(n)+len(kvps)+listPopDeltaOverhead)
	for seq := claimStart; seq < claimEnd; seq++ {
		elems = append(elems, &kv.Elem[kv.OP]{Op: kv.Put, Key: store.ListClaimKey(key, seq), Value: []byte{}})
	}

	values := make([]string, 0, len(kvps))
	for _, pair := range kvps {
		_, ok := store.ExtractListItemSeq(pair.Key, key)
		if !ok {
			continue
		}
		values = append(values, string(pair.Value))
		elems = append(elems, &kv.Elem[kv.OP]{Op: kv.Del, Key: bytes.Clone(pair.Key)})
	}
	return values, elems, nil
}

// checkListKeyType verifies the key is a list. Returns (keyFound, error).
// Writes wrongTypeError if the key exists but is not a list.
func (r *RedisServer) checkListKeyType(ctx context.Context, key []byte, readTS uint64) (found bool, err error) {
	typ, typErr := r.keyTypeAt(ctx, key, readTS)
	if typErr != nil {
		return false, typErr
	}
	if typ == redisTypeNone {
		return false, nil
	}
	if typ != redisTypeList {
		return false, wrongTypeError()
	}
	return true, nil
}

// listPopClaimOnce executes one attempt of a pop-with-claim transaction.
// Returns (nil, nil) for a missing key or an empty list, and the popped
// values otherwise.
func (r *RedisServer) listPopClaimOnce(ctx context.Context, key []byte, count int, left bool, readTS uint64) ([]string, error) {
	found, typeErr := r.checkListKeyType(ctx, key, readTS)
	if typeErr != nil || !found {
		return nil, typeErr
	}

	meta, exists, metaErr := r.resolveListMeta(ctx, key, readTS)
	if metaErr != nil {
		return nil, metaErr
	}
	if !exists || meta.Len == 0 {
		// count >= 1 on an empty list: Redis returns nil (same as missing key).
		return nil, nil
	}

	n, err := clampPopCount(count, meta.Len)
	if err != nil {
		return nil, err
	}

	values, elems, buildErr := r.buildListPopElems(ctx, key, meta, n, left, readTS)
	if buildErr != nil {
		return nil, buildErr
	}

	// n is the number of sequence positions claimed (including any holes).
	// HeadDelta and LenDelta must use n, not len(values), so that Head
	// advances past holes and the metadata stays consistent with Tail.
	commitTS := r.coordinator.Clock().Next()
	var headDelta int64
	if left {
		headDelta = n // head advances by n positions for LPOP
	}
	delta := store.MarshalListMetaDelta(store.ListMetaDelta{HeadDelta: headDelta, LenDelta: -n})
	elems = append(elems, &kv.Elem[kv.OP]{
		Op:    kv.Put,
		Key:   store.ListMetaDeltaKey(key, commitTS, 0),
		Value: delta,
	})

	_, dispErr := r.coordinator.Dispatch(ctx, &kv.OperationGroup[kv.OP]{
		IsTxn:    true,
		StartTS:  normalizeStartTS(readTS),
		CommitTS: commitTS,
		Elems:    elems,
	})
	if dispErr != nil {
		return nil, errors.WithStack(dispErr)
	}
	return values, nil
}

func (r *RedisServer) listPopClaim(ctx context.Context, key []byte, count int, left bool) ([]string, error) {
	// count=0: Redis returns an empty array if the key exists as a list, nil otherwise.
	if count <= 0 {
		readTS := r.readTS()
		found, err := r.checkListKeyType(ctx, key, readTS)
		if err != nil || !found {
			return nil, err
		}
		return []string{}, nil
	}

	var popped []string
	err := r.retryRedisWrite(ctx, func() error {
		result, popErr := r.listPopClaimOnce(ctx, key, count, left, r.readTS())
		if popErr != nil {
			return popErr
		}
		popped = result
		return nil
	})
	return popped, err
}

func (r *RedisServer) fetchListRange(ctx context.Context, key []byte, meta store.ListMeta, startIdx, endIdx int64, readTS uint64) ([]string, error) {
	if endIdx < startIdx {
		return []string{}, nil
	}

	startSeq := meta.Head + startIdx
	endSeq := meta.Head + endIdx

	startKey := listItemKey(key, startSeq)
	endKey := listItemKey(key, endSeq+1) // exclusive

	kvs, err := r.store.ScanAt(ctx, startKey, endKey, int(endIdx-startIdx+1), readTS)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	out := make([]string, 0, len(kvs))
	for _, kvp := range kvs {
		out = append(out, string(kvp.Value))
	}
	return out, nil
}

func (r *RedisServer) rangeList(key []byte, startRaw, endRaw []byte) ([]string, error) {
	if !r.coordinator.IsLeaderForKey(key) {
		return r.proxyLRange(key, startRaw, endRaw)
	}

	readTS := r.readTS()
	typ, err := r.keyTypeAt(context.Background(), key, readTS)
	if err != nil {
		return nil, err
	}
	if typ == redisTypeNone {
		return []string{}, nil
	}
	if typ != redisTypeList {
		return nil, wrongTypeError()
	}

	if err := r.coordinator.VerifyLeaderForKey(key); err != nil {
		return nil, errors.WithStack(err)
	}

	meta, exists, err := r.resolveListMeta(context.Background(), key, readTS)
	if err != nil {
		return nil, err
	}
	if !exists || meta.Len == 0 {
		return []string{}, nil
	}

	s, e, err := parseRangeBounds(startRaw, endRaw, int(meta.Len))
	if err != nil {
		return nil, err
	}

	return r.fetchListRange(context.Background(), key, meta, int64(s), int64(e), readTS)
}

func (r *RedisServer) proxyLRange(key []byte, startRaw, endRaw []byte) ([]string, error) {
	leader := r.coordinator.RaftLeaderForKey(key)
	if leader == "" {
		return nil, ErrLeaderNotFound
	}
	leaderAddr, ok := r.leaderRedis[leader]
	if !ok || leaderAddr == "" {
		return nil, errors.WithStack(errors.Newf("leader redis address unknown for %s", leader))
	}

	cli := r.getOrCreateLeaderClient(leaderAddr)

	start, err := parseInt(startRaw)
	if err != nil {
		return nil, err
	}
	end, err := parseInt(endRaw)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(r.handlerContext(), redisDispatchTimeout)
	defer cancel()

	res, err := cli.LRange(ctx, string(key), int64(start), int64(end)).Result()
	return res, errors.WithStack(err)
}

func (r *RedisServer) proxyRPush(key []byte, values [][]byte) (int64, error) {
	leader := r.coordinator.RaftLeaderForKey(key)
	if leader == "" {
		return 0, ErrLeaderNotFound
	}
	leaderAddr, ok := r.leaderRedis[leader]
	if !ok || leaderAddr == "" {
		return 0, errors.WithStack(errors.Newf("leader redis address unknown for %s", leader))
	}

	cli := r.getOrCreateLeaderClient(leaderAddr)

	args := make([]any, 0, len(values))
	for _, v := range values {
		args = append(args, string(v))
	}

	ctx, cancel := context.WithTimeout(r.handlerContext(), redisDispatchTimeout)
	defer cancel()

	res, err := cli.RPush(ctx, string(key), args...).Result()
	return res, errors.WithStack(err)
}

func (r *RedisServer) proxyLPush(key []byte, values [][]byte) (int64, error) {
	leader := r.coordinator.RaftLeaderForKey(key)
	if leader == "" {
		return 0, ErrLeaderNotFound
	}
	leaderAddr, ok := r.leaderRedis[leader]
	if !ok || leaderAddr == "" {
		return 0, errors.WithStack(errors.Newf("leader redis address unknown for %s", leader))
	}

	cli := r.getOrCreateLeaderClient(leaderAddr)

	args := make([]any, 0, len(values))
	for _, v := range values {
		args = append(args, string(v))
	}

	ctx, cancel := context.WithTimeout(r.handlerContext(), redisDispatchTimeout)
	defer cancel()

	res, err := cli.LPush(ctx, string(key), args...).Result()
	return res, errors.WithStack(err)
}

// getOrCreateLeaderClient returns a cached go-redis client for the given address,
// creating one if it doesn't exist.
func (r *RedisServer) getOrCreateLeaderClient(addr string) *redis.Client {
	r.leaderClientsMu.RLock()
	cli, ok := r.leaderClients[addr]
	r.leaderClientsMu.RUnlock()
	if ok {
		return cli
	}

	r.leaderClientsMu.Lock()
	defer r.leaderClientsMu.Unlock()
	// Double-check after acquiring write lock.
	if cli, ok = r.leaderClients[addr]; ok {
		return cli
	}
	cli = redis.NewClient(&redis.Options{Addr: addr})
	r.leaderClients[addr] = cli
	return cli
}

// leaderClientForKey returns a cached go-redis client connected to the leader
// for the given key.
func (r *RedisServer) leaderClientForKey(key []byte) (*redis.Client, error) {
	leader := r.coordinator.RaftLeaderForKey(key)
	if leader == "" {
		return nil, ErrLeaderNotFound
	}
	leaderAddr, ok := r.leaderRedis[leader]
	if !ok || leaderAddr == "" {
		return nil, errors.WithStack(errors.Newf("leader redis address unknown for %s", leader))
	}
	return r.getOrCreateLeaderClient(leaderAddr), nil
}

// proxyToLeader forwards a Redis command to the leader and writes the
// response to conn. Returns true if the command was proxied (caller should
// return immediately), false if this node is the leader.
func (r *RedisServer) proxyToLeader(conn redcon.Conn, cmd redcon.Command, key []byte) bool {
	if r.coordinator.IsLeaderForKey(key) {
		return false
	}
	cli, err := r.leaderClientForKey(key)
	if err != nil {
		conn.WriteError(err.Error())
		return true
	}

	ctx, cancel := context.WithTimeout(r.handlerContext(), redisDispatchTimeout)
	defer cancel()

	args := make([]interface{}, len(cmd.Args))
	for i, a := range cmd.Args {
		args[i] = a
	}
	writeGoRedisResult(conn, cli.Do(ctx, args...))
	return true
}

func writeGoRedisResult(conn redcon.Conn, cmd *redis.Cmd) {
	val, err := cmd.Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			conn.WriteNull()
		} else {
			conn.WriteError(err.Error())
		}
		return
	}
	writeGoRedisValue(conn, val)
}

func writeGoRedisValue(conn redcon.Conn, val interface{}) {
	switch v := val.(type) {
	case string:
		conn.WriteBulkString(v)
	case []byte:
		conn.WriteBulk(v)
	case int64:
		conn.WriteInt64(v)
	case bool:
		conn.WriteInt(boolToInt(v))
	case float64:
		conn.WriteBulkString(strconv.FormatFloat(v, 'f', -1, 64))
	case []interface{}:
		writeGoRedisArray(conn, v)
	case nil:
		conn.WriteNull()
	default:
		conn.WriteBulkString(fmt.Sprint(v))
	}
}

func writeGoRedisArray(conn redcon.Conn, arr []interface{}) {
	conn.WriteArray(len(arr))
	for _, item := range arr {
		writeGoRedisValue(conn, item)
	}
}

func boolToInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

func parseInt(b []byte) (int, error) {
	i, err := strconv.Atoi(string(b))
	return i, errors.WithStack(err)
}

// tryLeaderGet proxies a GET to the current Raft leader, returning the value and
// whether the proxy succeeded.
func (r *RedisServer) tryLeaderGetAt(key []byte, ts uint64) ([]byte, error) {
	addr := r.coordinator.RaftLeaderForKey(key)
	if addr == "" {
		return nil, ErrLeaderNotFound
	}

	conn, err := r.relayConnCache.ConnFor(addr)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	ctx, cancel := context.WithTimeout(r.handlerContext(), redisRelayPublishTimeout)
	defer cancel()

	cli := pb.NewRawKVClient(conn)
	resp, err := cli.RawGet(ctx, &pb.RawGetRequest{Key: key, Ts: ts})
	if err != nil {
		return nil, errors.WithStack(err)
	}
	// Compatibility with older nodes that don't set RawGetResponse.exists:
	// treat any non-nil payload as found even when exists=false.
	if !resp.GetExists() && resp.GetValue() == nil {
		return nil, errors.WithStack(store.ErrKeyNotFound)
	}
	return resp.Value, nil
}

func (r *RedisServer) readValueAt(key []byte, readTS uint64) ([]byte, error) {
	ttlKey := key
	nonStringInternal := false
	if userKey := extractRedisInternalUserKey(key); userKey != nil {
		ttlKey = userKey
		// Non-string internal keys (!redis|hash|, !redis|set|, …) can never
		// carry an embedded-TTL payload, so we can skip the !redis|str| probe
		// that ttlAt would otherwise make.
		nonStringInternal = !bytes.HasPrefix(key, []byte(redisStrPrefix))
	}
	expired, err := r.hasExpired(context.Background(), ttlKey, readTS, nonStringInternal)
	if err != nil {
		return nil, err
	}
	if expired {
		return nil, errors.WithStack(store.ErrKeyNotFound)
	}

	if r.coordinator.IsLeaderForKey(key) {
		if err := r.coordinator.VerifyLeaderForKey(key); err != nil {
			return nil, errors.WithStack(err)
		}
		v, err := r.store.GetAt(context.Background(), key, readTS)
		return v, errors.WithStack(err)
	}
	return r.tryLeaderGetAt(key, readTS)
}

type listPushFunc func(ctx context.Context, key []byte, values [][]byte) (int64, error)
type listProxyFunc func(key []byte, values [][]byte) (int64, error)

func (r *RedisServer) listPushCmd(conn redcon.Conn, cmd redcon.Command, pushFn listPushFunc, proxyFn listProxyFunc) {
	key := cmd.Args[1]
	if !r.coordinator.IsLeaderForKey(key) {
		length, err := proxyFn(key, cmd.Args[2:])
		if err != nil {
			conn.WriteError(err.Error())
			return
		}
		conn.WriteInt64(length)
		return
	}

	readTS := r.readTS()
	typ, err := r.keyTypeAt(context.Background(), key, readTS)
	if err != nil {
		conn.WriteError(err.Error())
		return
	}
	if typ != redisTypeNone && typ != redisTypeList {
		conn.WriteError(wrongTypeMessage)
		return
	}

	ctx := context.Background()
	length, err := pushFn(ctx, key, cmd.Args[2:])

	if err != nil {
		conn.WriteError(err.Error())
		return
	}
	conn.WriteInt64(length)
}

func (r *RedisServer) rpush(conn redcon.Conn, cmd redcon.Command) {
	r.listPushCmd(conn, cmd, r.listRPush, r.proxyRPush)
}

func (r *RedisServer) lrange(conn redcon.Conn, cmd redcon.Command) {
	items, err := r.rangeList(cmd.Args[1], cmd.Args[2], cmd.Args[3])
	if err != nil {
		conn.WriteError(err.Error())
		return
	}
	conn.WriteArray(len(items))
	for _, it := range items {
		conn.WriteBulkString(it)
	}
}
