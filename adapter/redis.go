package adapter

import (
	"bytes"
	"context"
	"log"
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
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
	"github.com/hashicorp/raft"
	"github.com/redis/go-redis/v9"
	"github.com/tidwall/redcon"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
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
	redisLatestCommitTimeout = 5 * time.Second
	redisDispatchTimeout     = 10 * time.Second
	redisRelayPublishTimeout = 2 * time.Second
	redisTraceArgLimit       = 6
	redisTraceArgMaxLen      = 96
	redisTraceArgEllipsis    = "..."
	redisTraceArgTrimLen     = redisTraceArgMaxLen - len(redisTraceArgEllipsis)
)

var redisTxnKeyPrefix = []byte("!txn|")

type redisSetOptions struct {
	existsCond  bool
	missingCond bool
	returnOld   bool
	ttl         *time.Time
}

type redisSetState struct {
	typ      redisValueType
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
	redisTranscoder *redisTranscoder
	pubsub          *redisPubSub
	scriptMu        sync.RWMutex
	scriptCache     map[string]string
	traceCommands   bool
	traceSeq        atomic.Uint64
	redisAddr       string
	relay           *RedisPubSubRelay
	relayConnCache  kv.GRPCConnCache
	// TODO manage membership from raft log
	leaderRedis map[raft.ServerAddress]string

	route map[string]func(conn redcon.Conn, cmd redcon.Command)
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

func NewRedisServer(listen net.Listener, redisAddr string, store store.MVCCStore, coordinate kv.Coordinator, leaderRedis map[raft.ServerAddress]string, relay *RedisPubSubRelay) *RedisServer {
	if relay == nil {
		relay = NewRedisPubSubRelay()
	}
	r := &RedisServer{
		listen:          listen,
		store:           store,
		coordinator:     coordinate,
		redisTranscoder: newRedisTranscoder(),
		redisAddr:       redisAddr,
		relay:           relay,
		leaderRedis:     leaderRedis,
		pubsub:          newRedisPubSub(),
		scriptCache:     map[string]string{},
		traceCommands:   os.Getenv("ELASTICKV_REDIS_TRACE") == "1",
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

func (r *RedisServer) Run() error {
	err := redcon.Serve(r.listen,
		func(conn redcon.Conn, cmd redcon.Command) {
			state := getConnState(conn)
			name := strings.ToUpper(string(cmd.Args[0]))
			f, ok := r.route[name]
			if !ok {
				r.traceCommandError(conn, name, cmd.Args[1:], "unsupported")
				conn.WriteError("ERR unsupported command '" + string(cmd.Args[0]) + "'")
				return
			}

			if err := r.validateCmd(cmd); err != nil {
				r.traceCommandError(conn, name, cmd.Args[1:], err.Error())
				conn.WriteError(err.Error())
				return
			}

			if state.inTxn && name != cmdExec && name != cmdDiscard && name != cmdMulti {
				// redcon reuses the underlying argument buffers; copy queued commands
				// so MULTI/EXEC works reliably under concurrency and with -race.
				state.queue = append(state.queue, cloneCommand(cmd))
				r.traceCommandDone(conn, name, cmd.Args[1:], 0, true)
				conn.WriteString("QUEUED")
				return
			}

			if r.traceCommands {
				traceID, traceStart := r.traceCommandStart(conn, name, cmd.Args[1:])
				f(conn, cmd)
				r.traceCommandFinish(traceID, conn, name, time.Since(traceStart))
			} else {
				f(conn, cmd)
			}
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
		return nil, index, errors.WithStack(err)
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

func (r *RedisServer) loadRedisSetState(key []byte, readTS uint64, returnOld bool) (redisSetState, error) {
	typ, err := r.keyTypeAt(context.Background(), key, readTS)
	if err != nil {
		return redisSetState{}, err
	}

	state := redisSetState{typ: typ}
	if !returnOld || typ != redisTypeString {
		return state, nil
	}

	oldValue, err := r.readValueAt(key, readTS)
	if err != nil && !errors.Is(err, store.ErrKeyNotFound) {
		return redisSetState{}, err
	}
	state.oldValue = oldValue
	return state, nil
}

func (r *RedisServer) replaceWithStringTxn(ctx context.Context, key, value []byte, ttl *time.Time, typ redisValueType, readTS uint64) error {
	var elems []*kv.Elem[kv.OP]
	if typ != redisTypeNone && typ != redisTypeString {
		delElems, _, err := r.deleteLogicalKeyElems(ctx, key, readTS)
		if err != nil {
			return err
		}
		elems = append(elems, delElems...)
	}
	elems = append(elems, &kv.Elem[kv.OP]{Op: kv.Put, Key: key, Value: bytes.Clone(value)})
	if ttl != nil {
		elems = append(elems, &kv.Elem[kv.OP]{Op: kv.Put, Key: redisTTLKey(key), Value: encodeRedisTTL(*ttl)})
	} else {
		elems = append(elems, &kv.Elem[kv.OP]{Op: kv.Del, Key: redisTTLKey(key)})
	}
	return r.dispatchElems(ctx, true, readTS, elems)
}

func (r *RedisServer) executeSet(ctx context.Context, key, value []byte, opts redisSetOptions) (redisSetExecution, error) {
	var result redisSetExecution
	err := r.retryRedisWrite(ctx, func() error {
		readTS := r.readTS()
		state, err := r.loadRedisSetState(key, readTS, opts.returnOld)
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
		if err := r.replaceWithStringTxn(ctx, key, value, opts.ttl, state.typ, readTS); err != nil {
			return err
		}
		result = redisSetExecution{state: state, wroteOldBulk: opts.returnOld}
		return nil
	})
	return result, err
}

func (r *RedisServer) Stop() {
	_ = r.relayConnCache.Close()
	_ = r.listen.Close()
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
	for _, peer := range r.relayPeers() {
		conn, err := r.relayConnCache.ConnFor(peer)
		if err != nil {
			log.Printf("redis relay publish dial peer=%s err=%v", peer, err)
			continue
		}
		callCtx, cancel := context.WithTimeout(ctx, redisRelayPublishTimeout)
		resp, err := pb.NewInternalClient(conn).RelayPublish(callCtx, &pb.RelayPublishRequest{
			Channel: bytes.Clone(channel),
			Message: bytes.Clone(message),
		})
		cancel()
		if err != nil {
			log.Printf("redis relay publish peer=%s err=%v", peer, err)
			continue
		}
		delivered += resp.GetSubscribers()
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

func (r *RedisServer) set(conn redcon.Conn, cmd redcon.Command) {
	opts, err := parseRedisSetOptions(cmd.Args[3:], time.Now())
	if err != nil {
		conn.WriteError(err.Error())
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), redisDispatchTimeout)
	defer cancel()

	if opts.isFastPath() {
		if err := r.saveString(ctx, cmd.Args[1], cmd.Args[2], opts.ttl); err != nil {
			conn.WriteError(err.Error())
			return
		}
		conn.WriteString("OK")
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
	readTS := r.readTS()
	isLeader := r.coordinator.IsLeaderForKey(key)

	typ, err := r.keyTypeAt(context.Background(), key, readTS)
	if err != nil {
		conn.WriteError(err.Error())
		return
	}
	if typ == redisTypeNone {
		r.getHandleNone(conn, key, isLeader)
		return
	}
	if typ != redisTypeString {
		conn.WriteError(wrongTypeMessage)
		return
	}

	// When proxying reads to the leader, let the leader choose a safe snapshot.
	// Our local store watermark may lag behind a just-committed transaction.
	valueTS := readTS
	if !isLeader {
		valueTS = 0
	}
	v, err := r.readValueAt(key, valueTS)
	if err != nil {
		if errors.Is(err, store.ErrKeyNotFound) {
			conn.WriteNull()
		} else {
			conn.WriteError(err.Error())
		}
		return
	}

	conn.WriteBulk(v)
}

// getHandleNone handles GET when the local type check returns none.
// On followers the local MVCC may lag, so we proxy to the leader.
func (r *RedisServer) getHandleNone(conn redcon.Conn, key []byte, isLeader bool) {
	if isLeader {
		conn.WriteNull()
		return
	}
	v, err := r.tryLeaderGetAt(key, 0)
	if err != nil {
		if errors.Is(err, store.ErrKeyNotFound) {
			conn.WriteNull()
		} else {
			conn.WriteError(err.Error())
		}
		return
	}
	conn.WriteBulk(v)
}

func (r *RedisServer) del(conn redcon.Conn, cmd redcon.Command) {
	ctx, cancel := context.WithTimeout(context.Background(), redisDispatchTimeout)
	defer cancel()
	removed := 0
	err := r.retryRedisWrite(ctx, func() error {
		elems := []*kv.Elem[kv.OP]{}
		nextRemoved := 0
		readTS := r.readTS()
		for _, key := range cmd.Args[1:] {
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
	if err != nil {
		conn.WriteError(err.Error())
		return
	}
	conn.WriteInt(removed)
}

func (r *RedisServer) exists(conn redcon.Conn, cmd redcon.Command) {
	readTS := r.readTS()
	count := 0
	for _, key := range cmd.Args[1:] {
		ok, err := r.logicalExistsAt(context.Background(), key, readTS)
		if err != nil {
			conn.WriteError(err.Error())
			return
		}
		if ok {
			count++
		} else if !r.coordinator.IsLeaderForKey(key) {
			// Local MVCC may be stale on a follower; proxy to the leader.
			_, err := r.tryLeaderGetAt(key, 0)
			if err == nil {
				count++
			} else if !errors.Is(err, store.ErrKeyNotFound) {
				conn.WriteError(err.Error())
				return
			}
		}
	}
	conn.WriteInt(count)
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
		metaStart, metaEnd := listPatternScanBounds(store.ListMetaPrefix, pattern)
		if err := mergeScannedKeys(metaStart, metaEnd); err != nil {
			return nil, err
		}

		itemStart, itemEnd := listPatternScanBounds(store.ListItemPrefix, pattern)
		if err := mergeScannedKeys(itemStart, itemEnd); err != nil {
			return nil, err
		}

		for _, prefix := range []string{
			redisHashPrefix,
			redisSetPrefix,
			redisHLLPrefix,
			redisZSetPrefix,
			redisStreamPrefix,
		} {
			internalStart, internalEnd := listPatternScanBounds(prefix, pattern)
			if err := mergeScannedKeys(internalStart, internalEnd); err != nil {
				return nil, err
			}
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

func redisVisibleUserKey(key []byte) []byte {
	if bytes.HasPrefix(key, redisTxnKeyPrefix) || isRedisTTLKey(key) {
		return nil
	}
	if store.IsListMetaKey(key) || store.IsListItemKey(key) {
		return store.ExtractListUserKey(key)
	}
	if userKey := extractRedisInternalUserKey(key); userKey != nil {
		return userKey
	}
	if bytes.HasPrefix(key, []byte("!")) {
		return nil
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

	cli := redis.NewClient(&redis.Options{
		Addr: leaderAddr,
	})
	defer func() {
		_ = cli.Close()
	}()

	keys, err := cli.Keys(context.Background(), string(pattern)).Result()
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

	results, err := r.runTransaction(state.queue)
	state.inTxn = false
	state.queue = nil
	if err != nil {
		conn.WriteError(err.Error())
		return
	}

	r.writeResults(conn, results)
}

type txnValue struct {
	raw     []byte
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
	startTS    uint64
}

type listTxnState struct {
	meta       store.ListMeta
	metaExists bool
	appends    [][]byte
	deleted    bool
	purge      bool
	purgeMeta  store.ListMeta
}

type zsetTxnState struct {
	members map[string]float64
	exists  bool
	dirty   bool
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

func (t *txnContext) load(key []byte) (*txnValue, error) {
	k := string(key)
	if tv, ok := t.working[k]; ok {
		return tv, nil
	}
	tv := &txnValue{}
	val, err := t.server.readValueAt(key, t.startTS)
	if err != nil && !errors.Is(err, store.ErrKeyNotFound) {
		return nil, errors.WithStack(err)
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

	meta, exists, err := t.server.loadListMetaAt(context.Background(), key, t.startTS)
	if err != nil {
		return nil, err
	}

	st := &listTxnState{
		meta:       meta,
		metaExists: exists,
		appends:    [][]byte{},
	}
	t.listStates[k] = st
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
	value, exists, err := t.server.loadZSetAt(context.Background(), key, t.startTS)
	if err != nil {
		return nil, err
	}
	st := &zsetTxnState{
		members: zsetEntriesToMap(value.Entries),
		exists:  exists,
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
	tv, ok := t.working[key]
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

	tv, err := t.load(cmd.Args[1])
	if err != nil {
		return redisResult{}, err
	}
	tv.raw = cmd.Args[2]
	tv.deleted = false
	tv.dirty = true
	return redisResult{typ: resultString, str: "OK"}, nil
}

func (t *txnContext) applyDel(cmd redcon.Command) (redisResult, error) {
	return t.stageKeyDeletion(cmd.Args[1])
}

func (t *txnContext) applyGet(cmd redcon.Command) (redisResult, error) {
	if isList, err := t.server.isListKeyAt(context.Background(), cmd.Args[1], t.startTS); err != nil {
		return redisResult{}, err
	} else if isList {
		return redisResult{typ: resultError, err: errors.New("WRONGTYPE Operation against a key holding the wrong kind of value")}, nil
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
	if isList, err := t.server.isListKeyAt(context.Background(), cmd.Args[1], t.startTS); err != nil {
		return redisResult{}, err
	} else if isList {
		return redisResult{typ: resultInt, integer: 1}, nil
	}

	tv, err := t.load(cmd.Args[1])
	if err != nil {
		return redisResult{}, err
	}
	if tv.deleted || tv.raw == nil {
		return redisResult{typ: resultInt, integer: 0}, nil
	}
	return redisResult{typ: resultInt, integer: 1}, nil
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

	expireAt := time.Now().Add(time.Duration(ttl) * unit)
	state.value = &expireAt
	state.dirty = true
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
	// Mark zset for deletion.
	zs, err := t.loadZSetState(key)
	if err != nil {
		return redisResult{}, err
	}
	zs.members = nil
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

func (t *txnContext) commit() error {
	elems := t.buildKeyElems()

	listElems, err := t.buildListElems()
	if err != nil {
		return err
	}
	zsetElems, err := t.buildZSetElems()
	if err != nil {
		return err
	}
	ttlElems := t.buildTTLElems()

	elems = append(elems, listElems...)
	elems = append(elems, zsetElems...)
	elems = append(elems, ttlElems...)
	if len(elems) == 0 {
		return nil
	}

	group := &kv.OperationGroup[kv.OP]{IsTxn: true, Elems: elems, StartTS: t.startTS}
	ctx, cancel := context.WithTimeout(context.Background(), redisDispatchTimeout)
	defer cancel()
	if _, err := t.server.coordinator.Dispatch(ctx, group); err != nil {
		return errors.WithStack(err)
	}
	return nil
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
		key := []byte(k)
		if tv.deleted {
			elems = append(elems, &kv.Elem[kv.OP]{Op: kv.Del, Key: key})
			continue
		}
		elems = append(elems, &kv.Elem[kv.OP]{Op: kv.Put, Key: key, Value: tv.raw})
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

func (t *txnContext) buildListElems() ([]*kv.Elem[kv.OP], error) {
	listKeys := make([]string, 0, len(t.listStates))
	for k := range t.listStates {
		listKeys = append(listKeys, k)
	}
	sort.Strings(listKeys)

	var elems []*kv.Elem[kv.OP]
	for _, k := range listKeys {
		st := t.listStates[k]
		userKey := []byte(k)

		if st.deleted {
			if meta, ok := listDeleteMeta(st); ok {
				elems = appendListDeleteOps(elems, userKey, meta)
			}
			continue
		}
		if len(st.appends) == 0 {
			continue
		}
		if st.purge {
			elems = appendListDeleteOps(elems, userKey, st.purgeMeta)
		}

		startSeq := st.meta.Head + st.meta.Len
		for i, v := range st.appends {
			elems = append(elems, &kv.Elem[kv.OP]{
				Op:    kv.Put,
				Key:   listItemKey(userKey, startSeq+int64(i)),
				Value: v,
			})
		}

		st.meta.Len += int64(len(st.appends))
		st.meta.Tail = st.meta.Head + st.meta.Len
		metaBytes, err := store.MarshalListMeta(st.meta)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		elems = append(elems, &kv.Elem[kv.OP]{Op: kv.Put, Key: listMetaKey(userKey), Value: metaBytes})
	}
	return elems, nil
}

func (t *txnContext) buildZSetElems() ([]*kv.Elem[kv.OP], error) {
	keys := make([]string, 0, len(t.zsetStates))
	for k := range t.zsetStates {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	elems := make([]*kv.Elem[kv.OP], 0, len(keys))
	for _, k := range keys {
		st := t.zsetStates[k]
		if !st.dirty {
			continue
		}
		if len(st.members) == 0 {
			elems = append(elems, &kv.Elem[kv.OP]{Op: kv.Del, Key: redisZSetKey([]byte(k))})
			continue
		}
		payload, err := marshalZSetValue(redisZSetValue{Entries: zsetMapToEntries(st.members)})
		if err != nil {
			return nil, err
		}
		elems = append(elems, &kv.Elem[kv.OP]{Op: kv.Put, Key: redisZSetKey([]byte(k)), Value: payload})
	}
	return elems, nil
}

func (t *txnContext) buildTTLElems() []*kv.Elem[kv.OP] {
	keys := make([]string, 0, len(t.ttlStates))
	for k := range t.ttlStates {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	elems := make([]*kv.Elem[kv.OP], 0, len(keys))
	for _, k := range keys {
		st := t.ttlStates[k]
		if !st.dirty {
			continue
		}
		if st.value == nil {
			elems = append(elems, &kv.Elem[kv.OP]{Op: kv.Del, Key: redisTTLKey([]byte(k))})
			continue
		}
		elems = append(elems, &kv.Elem[kv.OP]{Op: kv.Put, Key: redisTTLKey([]byte(k)), Value: encodeRedisTTL(*st.value)})
	}
	return elems
}

func (r *RedisServer) runTransaction(queue []redcon.Command) ([]redisResult, error) {
	startTS, err := r.txnStartTS(queue)
	if err != nil {
		return nil, err
	}

	ctx := &txnContext{
		server:     r,
		working:    map[string]*txnValue{},
		listStates: map[string]*listTxnState{},
		zsetStates: map[string]*zsetTxnState{},
		ttlStates:  map[string]*ttlTxnState{},
		startTS:    startTS,
	}

	results := make([]redisResult, 0, len(queue))
	for _, cmd := range queue {
		res, err := ctx.apply(cmd)
		if err != nil {
			return nil, err
		}
		results = append(results, res)
	}

	if err := ctx.commit(); err != nil {
		return nil, err
	}

	return results, nil
}

func (r *RedisServer) txnStartTS(queue []redcon.Command) (uint64, error) {
	ctx, cancel := context.WithTimeout(context.Background(), redisLatestCommitTimeout)
	defer cancel()

	maxTS, err := r.maxLatestCommitTS(ctx, queue)
	if err != nil {
		return 0, err
	}
	if r.coordinator != nil && r.coordinator.Clock() != nil && maxTS > 0 {
		r.coordinator.Clock().Observe(maxTS)
	}
	if r.coordinator == nil || r.coordinator.Clock() == nil {
		return maxTS + 1, nil
	}
	return r.coordinator.Clock().Next(), nil
}

func (r *RedisServer) maxLatestCommitTS(ctx context.Context, queue []redcon.Command) (uint64, error) {
	if r.store == nil {
		return 0, nil
	}

	// NOTE: This currently calls LatestCommitTS for each (unique) key involved in
	// the transaction. kv.MaxLatestCommitTS deduplicates keys and performs the
	// lookups in parallel, but very large transactions can still make this a
	// latency hot path. If needed, add batching/caching at the storage layer.
	const txnLatestCommitKeysPerCmd = 4
	keys := make([][]byte, 0, len(queue)*txnLatestCommitKeysPerCmd)
	for _, cmd := range queue {
		if len(cmd.Args) < minKeyedArgs {
			continue
		}
		name := strings.ToUpper(string(cmd.Args[0]))
		switch name {
		case cmdSet, cmdGet, cmdDel, cmdExists, cmdRPush, cmdLRange, cmdExpire, cmdPExpire, cmdZIncrBy:
			key := cmd.Args[1]
			keys = append(keys, key)
			// Also account for list metadata keys to avoid stale typing decisions.
			keys = append(keys, listMetaKey(key))
			keys = append(keys, redisZSetKey(key))
			keys = append(keys, redisTTLKey(key))
		}
	}
	ts, err := kv.MaxLatestCommitTS(ctx, r.store, keys)
	if err != nil {
		return 0, errors.WithStack(err)
	}
	return ts, nil
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

func (r *RedisServer) buildRPushOps(meta store.ListMeta, key []byte, values [][]byte) ([]*kv.Elem[kv.OP], store.ListMeta, error) {
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

	meta.Len += int64(len(values))
	meta.Tail = meta.Head + meta.Len

	b, err := store.MarshalListMeta(meta)
	if err != nil {
		return nil, meta, errors.WithStack(err)
	}

	elems = append(elems, &kv.Elem[kv.OP]{Op: kv.Put, Key: listMetaKey(key), Value: b})
	return elems, meta, nil
}

func (r *RedisServer) listRPush(ctx context.Context, key []byte, values [][]byte) (int64, error) {
	readTS := r.readTS()
	meta, _, err := r.loadListMetaAt(ctx, key, readTS)
	if err != nil {
		return 0, err
	}

	ops, newMeta, err := r.buildRPushOps(meta, key, values)
	if err != nil {
		return 0, err
	}
	if len(ops) == 0 {
		return newMeta.Len, nil
	}

	return newMeta.Len, r.dispatchElems(ctx, true, readTS, ops)
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
	if !r.coordinator.IsLeaderForKey(key) {
		return r.proxyLRange(key, startRaw, endRaw)
	}

	if err := r.coordinator.VerifyLeaderForKey(key); err != nil {
		return nil, errors.WithStack(err)
	}

	meta, exists, err := r.loadListMetaAt(context.Background(), key, readTS)
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

	cli := redis.NewClient(&redis.Options{Addr: leaderAddr})
	defer func() { _ = cli.Close() }()

	start, err := parseInt(startRaw)
	if err != nil {
		return nil, err
	}
	end, err := parseInt(endRaw)
	if err != nil {
		return nil, err
	}

	res, err := cli.LRange(context.Background(), string(key), int64(start), int64(end)).Result()
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

	cli := redis.NewClient(&redis.Options{Addr: leaderAddr})
	defer func() { _ = cli.Close() }()

	args := make([]any, 0, len(values))
	for _, v := range values {
		args = append(args, string(v))
	}

	res, err := cli.RPush(context.Background(), string(key), args...).Result()
	return res, errors.WithStack(err)
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

	conn, err := grpc.NewClient(string(addr),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.WaitForReady(true)),
	)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	defer conn.Close()

	cli := pb.NewRawKVClient(conn)
	resp, err := cli.RawGet(context.Background(), &pb.RawGetRequest{Key: key, Ts: ts})
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
	expired, err := r.hasExpiredTTLAt(context.Background(), key, readTS)
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

func (r *RedisServer) rpush(conn redcon.Conn, cmd redcon.Command) {
	readTS := r.readTS()
	typ, err := r.keyTypeAt(context.Background(), cmd.Args[1], readTS)
	if err != nil {
		conn.WriteError(err.Error())
		return
	}
	if typ != redisTypeNone && typ != redisTypeList {
		conn.WriteError(wrongTypeMessage)
		return
	}

	ctx := context.Background()

	var length int64
	if r.coordinator.IsLeaderForKey(cmd.Args[1]) {
		length, err = r.listRPush(ctx, cmd.Args[1], cmd.Args[2:])
	} else {
		length, err = r.proxyRPush(cmd.Args[1], cmd.Args[2:])
	}

	if err != nil {
		conn.WriteError(err.Error())
		return
	}
	conn.WriteInt64(length)
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
