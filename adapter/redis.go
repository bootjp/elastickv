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
)

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
	pubsub          redcon.PubSub
	pubsubMu        sync.Mutex
	pubsubChannels  map[string]int
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
	inTxn         bool
	queue         []redcon.Command
	subscriptions map[string]struct{}
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
		pubsubChannels:  map[string]int{},
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

			traceID, traceStart := r.traceCommandStart(conn, name, cmd.Args[1:])
			f(conn, cmd)
			r.traceCommandFinish(traceID, conn, name, time.Since(traceStart))
		},
		func(conn redcon.Conn) bool {
			// Use this function to accept or deny the connection.
			// log.Printf("accept: %s", conn.RemoteAddr())
			return true
		},
		func(conn redcon.Conn, err error) {
			// This is called when the connection has been closed
			// log.Printf("closed: %s, err: %v", conn.RemoteAddr(), err)
			r.cleanupConnSubscriptions(conn)
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
	parts := make([]string, 0, min(len(args), 6))
	for i, arg := range args {
		if i >= 6 {
			parts = append(parts, "...")
			break
		}
		s := strconv.QuoteToASCII(string(arg))
		if len(s) > 96 {
			s = s[:93] + "..."
		}
		parts = append(parts, s)
	}
	return "[" + strings.Join(parts, " ") + "]"
}

func (r *RedisServer) trackSubscription(conn redcon.Conn, channel string) {
	state := getConnState(conn)
	if state.subscriptions == nil {
		state.subscriptions = map[string]struct{}{}
	}
	if _, ok := state.subscriptions[channel]; ok {
		return
	}
	state.subscriptions[channel] = struct{}{}

	r.pubsubMu.Lock()
	defer r.pubsubMu.Unlock()
	r.pubsubChannels[channel]++
}

func (r *RedisServer) cleanupConnSubscriptions(conn redcon.Conn) {
	ctx := conn.Context()
	state, ok := ctx.(*connState)
	if !ok || len(state.subscriptions) == 0 {
		return
	}

	r.pubsubMu.Lock()
	defer r.pubsubMu.Unlock()
	for channel := range state.subscriptions {
		if n := r.pubsubChannels[channel]; n <= 1 {
			delete(r.pubsubChannels, channel)
		} else {
			r.pubsubChannels[channel] = n - 1
		}
	}
	state.subscriptions = nil
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
	var (
		existsCond  bool
		missingCond bool
		returnOld   bool
		ttl         *time.Time
	)

	for i := 3; i < len(cmd.Args); i++ {
		opt := strings.ToUpper(string(cmd.Args[i]))
		switch opt {
		case "EX", "PX":
			if i+1 >= len(cmd.Args) {
				conn.WriteError("ERR syntax error")
				return
			}
			n, err := strconv.ParseInt(string(cmd.Args[i+1]), 10, 64)
			if err != nil {
				conn.WriteError(err.Error())
				return
			}
			if n <= 0 {
				conn.WriteError("ERR invalid expire time in 'set' command")
				return
			}
			if opt == "EX" {
				t := time.Now().Add(time.Duration(n) * time.Second)
				ttl = &t
			} else {
				t := time.Now().Add(time.Duration(n) * time.Millisecond)
				ttl = &t
			}
			i++
		case "NX":
			missingCond = true
		case "XX":
			existsCond = true
		case "GET":
			returnOld = true
		default:
			conn.WriteError("ERR syntax error")
			return
		}
	}
	if existsCond && missingCond {
		conn.WriteError("ERR syntax error")
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), redisDispatchTimeout)
	defer cancel()

	if !returnOld && !existsCond && !missingCond {
		if err := r.saveString(ctx, cmd.Args[1], cmd.Args[2], ttl); err != nil {
			conn.WriteError(err.Error())
			return
		}
		conn.WriteString("OK")
		return
	}

	readTS := r.readTS()
	typ, err := r.keyTypeAt(context.Background(), cmd.Args[1], readTS)
	if err != nil {
		conn.WriteError(err.Error())
		return
	}
	exists := typ != redisTypeNone
	if existsCond && !exists {
		conn.WriteNull()
		return
	}
	if missingCond && exists {
		conn.WriteNull()
		return
	}
	if returnOld && exists && typ != redisTypeString {
		conn.WriteError(wrongTypeMessage)
		return
	}

	var oldValue []byte
	if returnOld && typ == redisTypeString {
		oldValue, err = r.readValueAt(cmd.Args[1], readTS)
		if err != nil && !errors.Is(err, store.ErrKeyNotFound) {
			conn.WriteError(err.Error())
			return
		}
	}

	if !returnOld && !existsCond && !missingCond && (typ == redisTypeNone || typ == redisTypeString) {
		if err := r.saveString(ctx, cmd.Args[1], cmd.Args[2], ttl); err != nil {
			conn.WriteError(err.Error())
			return
		}
		conn.WriteString("OK")
		return
	}

	elems, _, err := r.deleteLogicalKeyElems(ctx, cmd.Args[1], readTS)
	if err != nil {
		conn.WriteError(err.Error())
		return
	}
	elems = append(elems, &kv.Elem[kv.OP]{Op: kv.Put, Key: cmd.Args[1], Value: bytes.Clone(cmd.Args[2])})
	if ttl != nil {
		elems = append(elems, &kv.Elem[kv.OP]{Op: kv.Put, Key: redisTTLKey(cmd.Args[1]), Value: encodeRedisTTL(*ttl)})
	} else {
		elems = append(elems, &kv.Elem[kv.OP]{Op: kv.Del, Key: redisTTLKey(cmd.Args[1])})
	}
	if err := r.dispatchElems(ctx, true, elems); err != nil {
		conn.WriteError(err.Error())
		return
	}

	if returnOld {
		if oldValue == nil {
			conn.WriteNull()
			return
		}
		conn.WriteBulk(oldValue)
		return
	}
	conn.WriteString("OK")
}

func (r *RedisServer) get(conn redcon.Conn, cmd redcon.Command) {
	typ, err := r.keyType(context.Background(), cmd.Args[1])
	if err != nil {
		conn.WriteError(err.Error())
		return
	}
	if typ == redisTypeNone {
		conn.WriteNull()
		return
	}
	if typ != redisTypeString {
		conn.WriteError(wrongTypeMessage)
		return
	}

	key := cmd.Args[1]
	readTS := r.readTS()
	// When proxying reads to the leader, let the leader choose a safe snapshot.
	// Our local store watermark may lag behind a just-committed transaction.
	if !r.coordinator.IsLeaderForKey(key) {
		readTS = 0
	}
	v, err := r.readValueAt(key, readTS)
	if err != nil {
		switch {
		case errors.Is(err, store.ErrKeyNotFound):
			conn.WriteNull()
		default:
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
		if err := r.dispatchElems(ctx, true, elems); err != nil {
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

	// User-key scans do not naturally include internal data namespaces, so scan
	// those separately and map them back to logical user keys.
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
		if bytes.HasPrefix(kvPair.Key, []byte("!txn|")) {
			continue
		}
		if store.IsListMetaKey(kvPair.Key) || store.IsListItemKey(kvPair.Key) {
			if userKey := store.ExtractListUserKey(kvPair.Key); userKey != nil {
				if !matchesAsteriskPattern(pattern, userKey) {
					continue
				}
				keyset[string(userKey)] = userKey
			}
			continue
		}
		if userKey := extractRedisInternalUserKey(kvPair.Key); userKey != nil {
			if !matchesAsteriskPattern(pattern, userKey) {
				continue
			}
			keyset[string(userKey)] = userKey
			continue
		}
		if isRedisTTLKey(kvPair.Key) {
			continue
		}
		if bytes.HasPrefix(kvPair.Key, []byte("!")) {
			continue
		}
		if !matchesAsteriskPattern(pattern, kvPair.Key) {
			continue
		}
		keyset[string(kvPair.Key)] = kvPair.Key
	}
	return keyset
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
	if st, ok := t.zsetStates[k]; ok && (st.dirty || st.exists) {
		if len(st.members) == 0 {
			return redisTypeNone, nil
		}
		return redisTypeZSet, nil
	}
	if st, ok := t.listStates[k]; ok {
		if st.deleted {
			return redisTypeNone, nil
		}
		if st.metaExists || len(st.appends) > 0 {
			return redisTypeList, nil
		}
	}
	if tv, ok := t.working[k]; ok {
		if tv.deleted || tv.raw == nil {
			return redisTypeNone, nil
		}
		return redisTypeString, nil
	}
	return t.server.keyTypeAt(context.Background(), key, t.startTS)
}

func (t *txnContext) apply(cmd redcon.Command) (redisResult, error) {
	switch strings.ToUpper(string(cmd.Args[0])) {
	case cmdSet:
		return t.applySet(cmd)
	case cmdDel:
		return t.applyDel(cmd)
	case cmdGet:
		return t.applyGet(cmd)
	case cmdExists:
		return t.applyExists(cmd)
	case cmdRPush:
		return t.applyRPush(cmd)
	case cmdLRange:
		return t.applyLRange(cmd)
	case cmdZIncrBy:
		return t.applyZIncrBy(cmd)
	case cmdExpire:
		return t.applyExpire(cmd, time.Second)
	case cmdPExpire:
		return t.applyExpire(cmd, time.Millisecond)
	default:
		return redisResult{}, errors.WithStack(errors.Newf("ERR unsupported command '%s'", cmd.Args[0]))
	}
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
	// Always stage list deletion so DEL followed by RPUSH in the same MULTI
	// reliably recreates the list from an empty state.
	st, err := t.loadListState(cmd.Args[1])
	if err != nil {
		return redisResult{}, err
	}
	stageListDelete(st)

	tv, err := t.load(cmd.Args[1])
	if err != nil {
		return redisResult{}, err
	}
	tv.deleted = true
	tv.dirty = true
	return redisResult{typ: resultInt, integer: 1}, nil
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
	nxOnly := false
	for _, arg := range cmd.Args[3:] {
		switch strings.ToUpper(string(arg)) {
		case "NX":
			nxOnly = true
		default:
			return redisResult{}, errors.New("ERR syntax error")
		}
	}

	state, err := t.loadTTLState(cmd.Args[1])
	if err != nil {
		return redisResult{}, err
	}
	if nxOnly && state.value != nil && state.value.After(time.Now()) {
		return redisResult{typ: resultInt, integer: 0}, nil
	}

	expireAt := time.Now().Add(time.Duration(ttl) * unit)
	state.value = &expireAt
	state.dirty = true
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
	ttlElems, err := t.buildTTLElems()
	if err != nil {
		return err
	}

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

func (t *txnContext) buildTTLElems() ([]*kv.Elem[kv.OP], error) {
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
	return elems, nil
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

func (r *RedisServer) loadListMeta(ctx context.Context, key []byte) (store.ListMeta, bool, error) {
	return r.loadListMetaAt(ctx, key, r.readTS())
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

func (r *RedisServer) isListKey(ctx context.Context, key []byte) (bool, error) {
	_, exists, err := r.loadListMetaAt(ctx, key, r.readTS())
	return exists, err
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
	meta, _, err := r.loadListMeta(ctx, key)
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

	group := &kv.OperationGroup[kv.OP]{IsTxn: true, Elems: ops}
	if _, err := r.coordinator.Dispatch(ctx, group); err != nil {
		return 0, errors.WithStack(err)
	}
	return newMeta.Len, nil
}

func (r *RedisServer) deleteList(ctx context.Context, key []byte) error {
	_, exists, err := r.loadListMeta(ctx, key)
	if err != nil {
		return err
	}
	if !exists {
		return nil
	}

	start := listItemKey(key, math.MinInt64)
	end := listItemKey(key, math.MaxInt64)

	startTS := r.readTS()
	if startTS == ^uint64(0) && r.coordinator != nil && r.coordinator.Clock() != nil {
		startTS = r.coordinator.Clock().Next()
	}

	// Keep DEL atomic by deleting all persisted list entries and metadata in one
	// transaction at a single snapshot timestamp. This can allocate large slices
	// for very large lists; if the storage layer grows range-delete support, this
	// path should move to a streaming/range tombstone strategy.
	kvs, err := r.store.ScanAt(ctx, start, end, math.MaxInt, startTS)
	if err != nil {
		return errors.WithStack(err)
	}

	ops := make([]*kv.Elem[kv.OP], 0, len(kvs)+1)
	for _, kvp := range kvs {
		ops = append(ops, &kv.Elem[kv.OP]{Op: kv.Del, Key: kvp.Key})
	}
	// delete meta last
	ops = append(ops, &kv.Elem[kv.OP]{Op: kv.Del, Key: listMetaKey(key)})

	group := &kv.OperationGroup[kv.OP]{IsTxn: true, StartTS: startTS, Elems: ops}
	_, err = r.coordinator.Dispatch(ctx, group)
	return errors.WithStack(err)
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
	switch typ {
	case redisTypeNone:
		return []string{}, nil
	case redisTypeList:
	default:
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

	start, err := strconv.Atoi(string(startRaw))
	if err != nil {
		return nil, errors.WithStack(err)
	}
	end, err := strconv.Atoi(string(endRaw))
	if err != nil {
		return nil, errors.WithStack(err)
	}

	s, e := clampRange(start, end, int(meta.Len))
	if e < s {
		return []string{}, nil
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
