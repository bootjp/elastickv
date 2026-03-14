package adapter

import (
	"context"
	"errors"
	"log"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
	"unsafe"

	"github.com/bootjp/elastickv/kv"
	"github.com/tidwall/btree"
	"github.com/tidwall/redcon"
)

func (r *RedisServer) info(conn redcon.Conn, _ redcon.Command) {
	conn.WriteBulkString(strings.Join([]string{
		"# Server",
		"redis_version:7.2.0",
		"loading:0",
		"role:master",
		"",
	}, "\r\n"))
}

func (r *RedisServer) client(conn redcon.Conn, cmd redcon.Command) {
	sub := strings.ToUpper(string(cmd.Args[1]))
	switch sub {
	case "SETINFO", "SETNAME":
		conn.WriteString("OK")
	case "GETNAME":
		conn.WriteNull()
	case "INFO":
		conn.WriteBulkString("id=1 addr=" + conn.RemoteAddr())
	default:
		conn.WriteError("ERR unsupported CLIENT subcommand '" + sub + "'")
	}
}

func (r *RedisServer) selectDB(conn redcon.Conn, cmd redcon.Command) {
	if _, err := strconv.Atoi(string(cmd.Args[1])); err != nil {
		conn.WriteError(err.Error())
		return
	}
	conn.WriteString("OK")
}

func (r *RedisServer) quit(conn redcon.Conn, _ redcon.Command) {
	conn.WriteString("OK")
	_ = conn.Close()
}

func (r *RedisServer) typeCmd(conn redcon.Conn, cmd redcon.Command) {
	typ, err := r.keyType(context.Background(), cmd.Args[1])
	if err != nil {
		conn.WriteError(err.Error())
		return
	}
	conn.WriteString(string(typ))
}

func (r *RedisServer) ttl(conn redcon.Conn, cmd redcon.Command) {
	r.writeTTL(conn, cmd.Args[1], false)
}

func (r *RedisServer) pttl(conn redcon.Conn, cmd redcon.Command) {
	r.writeTTL(conn, cmd.Args[1], true)
}

func (r *RedisServer) writeTTL(conn redcon.Conn, key []byte, milliseconds bool) {
	readTS := r.readTS()
	exists, err := r.logicalExistsAt(context.Background(), key, readTS)
	if err != nil {
		conn.WriteError(err.Error())
		return
	}
	if !exists {
		conn.WriteInt64(-2)
		return
	}
	ttl, err := r.ttlAt(context.Background(), key, readTS)
	if err != nil {
		conn.WriteError(err.Error())
		return
	}
	ms := ttlMilliseconds(ttl)
	if ms == -1 {
		conn.WriteInt64(-1)
		return
	}
	if !milliseconds && ms >= 0 {
		ms /= 1000
	}
	conn.WriteInt64(ms)
}

func (r *RedisServer) expire(conn redcon.Conn, cmd redcon.Command) {
	r.setExpire(conn, cmd, time.Second)
}

func (r *RedisServer) pexpire(conn redcon.Conn, cmd redcon.Command) {
	r.setExpire(conn, cmd, time.Millisecond)
}

func (r *RedisServer) setExpire(conn redcon.Conn, cmd redcon.Command, unit time.Duration) {
	ttl, err := strconv.ParseInt(string(cmd.Args[2]), 10, 64)
	if err != nil {
		conn.WriteError(err.Error())
		return
	}

	nxOnly := false
	for _, arg := range cmd.Args[3:] {
		switch strings.ToUpper(string(arg)) {
		case "NX":
			nxOnly = true
		default:
			conn.WriteError("ERR syntax error")
			return
		}
	}

	readTS := r.readTS()
	exists, err := r.logicalExistsAt(context.Background(), cmd.Args[1], readTS)
	if err != nil {
		conn.WriteError(err.Error())
		return
	}
	if !exists {
		conn.WriteInt(0)
		return
	}

	currentTTL, err := r.ttlAt(context.Background(), cmd.Args[1], readTS)
	if err != nil {
		conn.WriteError(err.Error())
		return
	}
	if nxOnly && currentTTL != nil && currentTTL.After(time.Now()) {
		conn.WriteInt(0)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), redisDispatchTimeout)
	defer cancel()

	if ttl <= 0 {
		elems, existed, err := r.deleteLogicalKeyElems(ctx, cmd.Args[1], readTS)
		if err != nil {
			conn.WriteError(err.Error())
			return
		}
		if err := r.dispatchElems(ctx, true, elems); err != nil {
			conn.WriteError(err.Error())
			return
		}
		if existed {
			conn.WriteInt(1)
		} else {
			conn.WriteInt(0)
		}
		return
	}

	expireAt := time.Now().Add(time.Duration(ttl) * unit)
	if err := r.dispatchElems(ctx, false, []*kv.Elem[kv.OP]{
		{Op: kv.Put, Key: redisTTLKey(cmd.Args[1]), Value: encodeRedisTTL(expireAt)},
	}); err != nil {
		conn.WriteError(err.Error())
		return
	}
	conn.WriteInt(1)
}

func (r *RedisServer) scan(conn redcon.Conn, cmd redcon.Command) {
	cursor, err := strconv.Atoi(string(cmd.Args[1]))
	if err != nil || cursor < 0 {
		conn.WriteError("ERR invalid cursor")
		return
	}

	pattern := []byte("*")
	count := 10
	for i := 2; i < len(cmd.Args); i += 2 {
		if i+1 >= len(cmd.Args) {
			conn.WriteError("ERR syntax error")
			return
		}
		switch strings.ToUpper(string(cmd.Args[i])) {
		case "MATCH":
			pattern = cmd.Args[i+1]
		case "COUNT":
			count, err = strconv.Atoi(string(cmd.Args[i+1]))
			if err != nil || count <= 0 {
				conn.WriteError("ERR syntax error")
				return
			}
		default:
			conn.WriteError("ERR syntax error")
			return
		}
	}

	keys, err := r.visibleKeys(pattern)
	if err != nil {
		conn.WriteError(err.Error())
		return
	}
	if cursor >= len(keys) {
		conn.WriteArray(2)
		conn.WriteBulkString("0")
		conn.WriteArray(0)
		return
	}

	end := minRedisInt(cursor+count, len(keys))
	next := 0
	if end < len(keys) {
		next = end
	}

	conn.WriteArray(2)
	conn.WriteBulkString(strconv.Itoa(next))
	conn.WriteArray(end - cursor)
	for _, key := range keys[cursor:end] {
		conn.WriteBulk(key)
	}
}

func (r *RedisServer) publish(conn redcon.Conn, cmd redcon.Command) {
	count := r.publishCluster(context.Background(), cmd.Args[1], cmd.Args[2])
	if r.traceCommands {
		log.Printf("redis trace publish remote=%s channel=%q subscribers=%d", conn.RemoteAddr(), string(cmd.Args[1]), count)
	}
	conn.WriteInt64(count)
}

func (r *RedisServer) subscribe(conn redcon.Conn, cmd redcon.Command) {
	for _, channel := range cmd.Args[1:] {
		r.trackSubscription(conn, string(channel))
		r.pubsub.Subscribe(conn, string(channel))
		if r.traceCommands {
			log.Printf("redis trace subscribe remote=%s channel=%q snapshot=%v", conn.RemoteAddr(), string(channel), r.pubsubChannelCounts())
		}
	}
}

func (r *RedisServer) dbsize(conn redcon.Conn, _ redcon.Command) {
	if !r.coordinator.IsLeader() {
		size, err := r.proxyDBSize()
		if err != nil {
			conn.WriteError(err.Error())
			return
		}
		conn.WriteInt(size)
		return
	}
	if err := r.coordinator.VerifyLeader(); err != nil {
		conn.WriteError(err.Error())
		return
	}

	keys, err := r.visibleKeys([]byte("*"))
	if err != nil {
		conn.WriteError(err.Error())
		return
	}
	conn.WriteInt(len(keys))
}

func (r *RedisServer) flushdb(conn redcon.Conn, _ redcon.Command) {
	r.flushDatabase(conn, false)
}

func (r *RedisServer) flushall(conn redcon.Conn, _ redcon.Command) {
	r.flushDatabase(conn, true)
}

func (r *RedisServer) flushDatabase(conn redcon.Conn, all bool) {
	if !r.coordinator.IsLeader() {
		if err := r.proxyFlushDatabase(all); err != nil {
			conn.WriteError(err.Error())
			return
		}
		conn.WriteString("OK")
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), redisDispatchTimeout)
	defer cancel()

	if err := r.retryRedisWrite(ctx, func() error {
		if err := r.coordinator.VerifyLeader(); err != nil {
			return err
		}

		keys, err := r.visibleKeys([]byte("*"))
		if err != nil {
			return err
		}
		if len(keys) == 0 {
			return nil
		}

		readTS := r.readTS()
		elems := make([]*kv.Elem[kv.OP], 0, len(keys))
		for _, key := range keys {
			keyElems, _, err := r.deleteLogicalKeyElems(ctx, key, readTS)
			if err != nil {
				return err
			}
			elems = append(elems, keyElems...)
		}
		if len(elems) == 0 {
			return nil
		}
		return r.dispatchElems(ctx, true, elems)
	}); err != nil {
		conn.WriteError(err.Error())
		return
	}

	conn.WriteString("OK")
}

func (r *RedisServer) pubsubCmd(conn redcon.Conn, cmd redcon.Command) {
	sub := strings.ToUpper(string(cmd.Args[1]))
	switch sub {
	case "CHANNELS":
		pattern := []byte("*")
		if len(cmd.Args) >= 3 {
			pattern = cmd.Args[2]
		}

		counts := r.pubsubChannelCounts()
		channels := make([]string, 0, len(counts))
		for channel, count := range counts {
			if count <= 0 || !matchesAsteriskPattern(pattern, []byte(channel)) {
				continue
			}
			channels = append(channels, channel)
		}

		sort.Strings(channels)
		conn.WriteArray(len(channels))
		for _, channel := range channels {
			conn.WriteBulkString(channel)
		}
	case "NUMSUB":
		snapshot := r.pubsubChannelCounts()
		counts := make([]int, 0, len(cmd.Args)-2)
		for _, channel := range cmd.Args[2:] {
			counts = append(counts, snapshot[string(channel)])
		}

		conn.WriteArray((len(cmd.Args) - 2) * 2)
		for i, channel := range cmd.Args[2:] {
			conn.WriteBulk(channel)
			conn.WriteInt(counts[i])
		}
	case "NUMPAT":
		conn.WriteInt(0)
	default:
		conn.WriteError("ERR unsupported PUBSUB subcommand '" + sub + "'")
	}
}

func (r *RedisServer) pubsubChannelCounts() map[string]int {
	ps := reflect.ValueOf(&r.pubsub).Elem()
	muField := ps.FieldByName("mu")
	mu := (*sync.RWMutex)(unsafe.Pointer(muField.UnsafeAddr()))
	mu.RLock()
	defer mu.RUnlock()

	chansField := ps.FieldByName("chans")
	if chansField.IsNil() {
		return map[string]int{}
	}

	tree := reflect.NewAt(chansField.Type(), unsafe.Pointer(chansField.UnsafeAddr())).Elem().Interface().(*btree.BTree)
	counts := map[string]int{}
	tree.Walk(func(items []interface{}) {
		for _, item := range items {
			entry := reflect.ValueOf(item).Elem()
			if entry.FieldByName("pattern").Bool() {
				continue
			}
			channel := entry.FieldByName("channel").String()
			counts[channel]++
		}
	})
	return counts
}

func (r *RedisServer) sadd(conn redcon.Conn, cmd redcon.Command) {
	r.mutateExactSet(conn, "set", cmd.Args[1], cmd.Args[2:], true)
}

func (r *RedisServer) srem(conn redcon.Conn, cmd redcon.Command) {
	r.mutateExactSet(conn, "set", cmd.Args[1], cmd.Args[2:], false)
}

func (r *RedisServer) mutateExactSet(conn redcon.Conn, kind string, key []byte, members [][]byte, add bool) {
	readTS := r.readTS()
	typ, err := r.keyTypeAt(context.Background(), key, readTS)
	if err != nil {
		conn.WriteError(err.Error())
		return
	}
	switch kind {
	case "set":
		if typ != redisTypeNone && typ != redisTypeSet {
			conn.WriteError(wrongTypeMessage)
			return
		}
		if typ == redisTypeNone {
			ok, err := r.store.ExistsAt(context.Background(), redisHLLKey(key), readTS)
			if err != nil {
				conn.WriteError(err.Error())
				return
			}
			if ok {
				conn.WriteError(wrongTypeMessage)
				return
			}
		}
	case "hll":
		if typ != redisTypeNone {
			ok, err := r.store.ExistsAt(context.Background(), redisHLLKey(key), readTS)
			if err != nil {
				conn.WriteError(err.Error())
				return
			}
			if !ok {
				conn.WriteError(wrongTypeMessage)
				return
			}
		}
	default:
		conn.WriteError("ERR unsupported exact set kind")
		return
	}
	value, _, err := r.loadSetAt(context.Background(), kind, key, readTS)
	if err != nil {
		conn.WriteError(err.Error())
		return
	}

	existing := make(map[string]struct{}, len(value.Members))
	for _, member := range value.Members {
		existing[member] = struct{}{}
	}

	var changed int
	for _, member := range members {
		ms := string(member)
		_, ok := existing[ms]
		if add {
			if ok {
				continue
			}
			existing[ms] = struct{}{}
			changed++
			continue
		}
		if ok {
			delete(existing, ms)
			changed++
		}
	}

	if changed > 0 {
		value.Members = value.Members[:0]
		for member := range existing {
			value.Members = append(value.Members, member)
		}
		sort.Strings(value.Members)

		ctx, cancel := context.WithTimeout(context.Background(), redisDispatchTimeout)
		defer cancel()

		if len(value.Members) == 0 {
			elems, _, err := r.deleteLogicalKeyElems(ctx, key, readTS)
			if err != nil {
				conn.WriteError(err.Error())
				return
			}
			err = r.dispatchElems(ctx, true, filterElemsForKey(elems, redisExactSetStorageKey(kind, key), redisTTLKey(key)))
		} else {
			err = r.saveSet(ctx, kind, key, value)
		}
		if err != nil {
			conn.WriteError(err.Error())
			return
		}
	}

	conn.WriteInt(changed)
}

func filterElemsForKey(elems []*kv.Elem[kv.OP], keys ...[]byte) []*kv.Elem[kv.OP] {
	allowed := make(map[string]struct{}, len(keys))
	for _, key := range keys {
		allowed[string(key)] = struct{}{}
	}
	out := make([]*kv.Elem[kv.OP], 0, len(elems))
	for _, elem := range elems {
		if _, ok := allowed[string(elem.Key)]; ok {
			out = append(out, elem)
		}
	}
	return out
}

func (r *RedisServer) sismember(conn redcon.Conn, cmd redcon.Command) {
	readTS := r.readTS()
	typ, err := r.keyTypeAt(context.Background(), cmd.Args[1], readTS)
	if err != nil {
		conn.WriteError(err.Error())
		return
	}
	switch typ {
	case redisTypeNone:
		conn.WriteInt(0)
		return
	case redisTypeSet:
	default:
		conn.WriteError(wrongTypeMessage)
		return
	}

	value, _, err := r.loadSetAt(context.Background(), "set", cmd.Args[1], readTS)
	if err != nil {
		conn.WriteError(err.Error())
		return
	}
	for _, member := range value.Members {
		if member == string(cmd.Args[2]) {
			conn.WriteInt(1)
			return
		}
	}
	conn.WriteInt(0)
}

func (r *RedisServer) smembers(conn redcon.Conn, cmd redcon.Command) {
	readTS := r.readTS()
	typ, err := r.keyTypeAt(context.Background(), cmd.Args[1], readTS)
	if err != nil {
		conn.WriteError(err.Error())
		return
	}
	switch typ {
	case redisTypeNone:
		conn.WriteArray(0)
		return
	case redisTypeSet:
	default:
		conn.WriteError(wrongTypeMessage)
		return
	}

	value, _, err := r.loadSetAt(context.Background(), "set", cmd.Args[1], readTS)
	if err != nil {
		conn.WriteError(err.Error())
		return
	}
	conn.WriteArray(len(value.Members))
	for _, member := range value.Members {
		conn.WriteBulkString(member)
	}
}

func (r *RedisServer) pfadd(conn redcon.Conn, cmd redcon.Command) {
	readTS := r.readTS()
	typ, err := r.keyTypeAt(context.Background(), cmd.Args[1], readTS)
	if err != nil {
		conn.WriteError(err.Error())
		return
	}
	if typ != redisTypeNone {
		ok, err := r.store.ExistsAt(context.Background(), redisHLLKey(cmd.Args[1]), readTS)
		if err != nil {
			conn.WriteError(err.Error())
			return
		}
		if !ok {
			conn.WriteError(wrongTypeMessage)
			return
		}
	}

	value, _, err := r.loadSetAt(context.Background(), "hll", cmd.Args[1], readTS)
	if err != nil {
		conn.WriteError(err.Error())
		return
	}
	existing := make(map[string]struct{}, len(value.Members))
	for _, member := range value.Members {
		existing[member] = struct{}{}
	}
	changed := false
	for _, member := range cmd.Args[2:] {
		ms := string(member)
		if _, ok := existing[ms]; ok {
			continue
		}
		existing[ms] = struct{}{}
		changed = true
	}
	if changed {
		value.Members = value.Members[:0]
		for member := range existing {
			value.Members = append(value.Members, member)
		}
		sort.Strings(value.Members)
		ctx, cancel := context.WithTimeout(context.Background(), redisDispatchTimeout)
		defer cancel()
		if err := r.saveSet(ctx, "hll", cmd.Args[1], value); err != nil {
			conn.WriteError(err.Error())
			return
		}
		conn.WriteInt(1)
		return
	}
	conn.WriteInt(0)
}

func (r *RedisServer) pfcount(conn redcon.Conn, cmd redcon.Command) {
	readTS := r.readTS()
	union := map[string]struct{}{}
	for _, key := range cmd.Args[1:] {
		typ, err := r.keyTypeAt(context.Background(), key, readTS)
		if err != nil {
			conn.WriteError(err.Error())
			return
		}
		if typ != redisTypeNone {
			hllExists, err := r.store.ExistsAt(context.Background(), redisHLLKey(key), readTS)
			if err != nil {
				conn.WriteError(err.Error())
				return
			}
			if !hllExists {
				conn.WriteError(wrongTypeMessage)
				return
			}
		}
		value, _, err := r.loadSetAt(context.Background(), "hll", key, readTS)
		if err != nil {
			conn.WriteError(err.Error())
			return
		}
		for _, member := range value.Members {
			union[member] = struct{}{}
		}
	}
	conn.WriteInt(len(union))
}

func (r *RedisServer) hset(conn redcon.Conn, cmd redcon.Command) {
	added, err := r.applyHashFieldPairs(cmd.Args[1], cmd.Args[2:], false)
	if err != nil {
		conn.WriteError(err.Error())
		return
	}
	conn.WriteInt(added)
}

func (r *RedisServer) hmset(conn redcon.Conn, cmd redcon.Command) {
	if _, err := r.applyHashFieldPairs(cmd.Args[1], cmd.Args[2:], true); err != nil {
		conn.WriteError(err.Error())
		return
	}
	conn.WriteString("OK")
}

func (r *RedisServer) applyHashFieldPairs(key []byte, args [][]byte, hmset bool) (int, error) {
	if len(args) == 0 || len(args)%2 != 0 {
		return 0, errors.New("ERR wrong number of arguments for hash command")
	}

	readTS := r.readTS()
	typ, err := r.keyTypeAt(context.Background(), key, readTS)
	if err != nil {
		return 0, err
	}
	if typ != redisTypeNone && typ != redisTypeHash {
		return 0, wrongTypeError()
	}

	value, _, err := r.loadHashAt(context.Background(), key, readTS)
	if err != nil {
		return 0, err
	}

	added := 0
	for i := 0; i < len(args); i += 2 {
		field := string(args[i])
		if _, ok := value[field]; !ok {
			added++
		}
		value[field] = string(args[i+1])
	}

	ctx, cancel := context.WithTimeout(context.Background(), redisDispatchTimeout)
	defer cancel()
	if err := r.saveHash(ctx, key, value); err != nil {
		return 0, err
	}
	if hmset {
		return 0, nil
	}
	return added, nil
}

func (r *RedisServer) hget(conn redcon.Conn, cmd redcon.Command) {
	readTS := r.readTS()
	typ, err := r.keyTypeAt(context.Background(), cmd.Args[1], readTS)
	if err != nil {
		conn.WriteError(err.Error())
		return
	}
	switch typ {
	case redisTypeNone:
		conn.WriteNull()
		return
	case redisTypeHash:
	default:
		conn.WriteError(wrongTypeMessage)
		return
	}

	value, _, err := r.loadHashAt(context.Background(), cmd.Args[1], readTS)
	if err != nil {
		conn.WriteError(err.Error())
		return
	}
	fieldValue, ok := value[string(cmd.Args[2])]
	if !ok {
		conn.WriteNull()
		return
	}
	conn.WriteBulkString(fieldValue)
}

func (r *RedisServer) hmget(conn redcon.Conn, cmd redcon.Command) {
	readTS := r.readTS()
	typ, err := r.keyTypeAt(context.Background(), cmd.Args[1], readTS)
	if err != nil {
		conn.WriteError(err.Error())
		return
	}
	switch typ {
	case redisTypeNone:
		conn.WriteArray(len(cmd.Args) - 2)
		for range cmd.Args[2:] {
			conn.WriteNull()
		}
		return
	case redisTypeHash:
	default:
		conn.WriteError(wrongTypeMessage)
		return
	}

	value, _, err := r.loadHashAt(context.Background(), cmd.Args[1], readTS)
	if err != nil {
		conn.WriteError(err.Error())
		return
	}
	conn.WriteArray(len(cmd.Args) - 2)
	for _, field := range cmd.Args[2:] {
		fieldValue, ok := value[string(field)]
		if !ok {
			conn.WriteNull()
			continue
		}
		conn.WriteBulkString(fieldValue)
	}
}

func (r *RedisServer) hdel(conn redcon.Conn, cmd redcon.Command) {
	readTS := r.readTS()
	typ, err := r.keyTypeAt(context.Background(), cmd.Args[1], readTS)
	if err != nil {
		conn.WriteError(err.Error())
		return
	}
	switch typ {
	case redisTypeNone:
		conn.WriteInt(0)
		return
	case redisTypeHash:
	default:
		conn.WriteError(wrongTypeMessage)
		return
	}

	value, _, err := r.loadHashAt(context.Background(), cmd.Args[1], readTS)
	if err != nil {
		conn.WriteError(err.Error())
		return
	}

	removed := 0
	for _, field := range cmd.Args[2:] {
		if _, ok := value[string(field)]; ok {
			delete(value, string(field))
			removed++
		}
	}
	if removed == 0 {
		conn.WriteInt(0)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), redisDispatchTimeout)
	defer cancel()
	if len(value) == 0 {
		err = r.dispatchElems(ctx, true, []*kv.Elem[kv.OP]{
			{Op: kv.Del, Key: redisHashKey(cmd.Args[1])},
			{Op: kv.Del, Key: redisTTLKey(cmd.Args[1])},
		})
	} else {
		err = r.saveHash(ctx, cmd.Args[1], value)
	}
	if err != nil {
		conn.WriteError(err.Error())
		return
	}
	conn.WriteInt(removed)
}

func (r *RedisServer) hexists(conn redcon.Conn, cmd redcon.Command) {
	readTS := r.readTS()
	typ, err := r.keyTypeAt(context.Background(), cmd.Args[1], readTS)
	if err != nil {
		conn.WriteError(err.Error())
		return
	}
	switch typ {
	case redisTypeNone:
		conn.WriteInt(0)
		return
	case redisTypeHash:
	default:
		conn.WriteError(wrongTypeMessage)
		return
	}

	value, _, err := r.loadHashAt(context.Background(), cmd.Args[1], readTS)
	if err != nil {
		conn.WriteError(err.Error())
		return
	}
	if _, ok := value[string(cmd.Args[2])]; ok {
		conn.WriteInt(1)
		return
	}
	conn.WriteInt(0)
}

func (r *RedisServer) hlen(conn redcon.Conn, cmd redcon.Command) {
	readTS := r.readTS()
	typ, err := r.keyTypeAt(context.Background(), cmd.Args[1], readTS)
	if err != nil {
		conn.WriteError(err.Error())
		return
	}
	switch typ {
	case redisTypeNone:
		conn.WriteInt(0)
		return
	case redisTypeHash:
	default:
		conn.WriteError(wrongTypeMessage)
		return
	}

	value, _, err := r.loadHashAt(context.Background(), cmd.Args[1], readTS)
	if err != nil {
		conn.WriteError(err.Error())
		return
	}
	conn.WriteInt(len(value))
}

func (r *RedisServer) hincrby(conn redcon.Conn, cmd redcon.Command) {
	increment, err := strconv.ParseInt(string(cmd.Args[3]), 10, 64)
	if err != nil {
		conn.WriteError(err.Error())
		return
	}
	readTS := r.readTS()
	typ, err := r.keyTypeAt(context.Background(), cmd.Args[1], readTS)
	if err != nil {
		conn.WriteError(err.Error())
		return
	}
	if typ != redisTypeNone && typ != redisTypeHash {
		conn.WriteError(wrongTypeMessage)
		return
	}

	value, _, err := r.loadHashAt(context.Background(), cmd.Args[1], readTS)
	if err != nil {
		conn.WriteError(err.Error())
		return
	}

	current := int64(0)
	if raw, ok := value[string(cmd.Args[2])]; ok {
		current, err = strconv.ParseInt(raw, 10, 64)
		if err != nil {
			conn.WriteError("ERR hash value is not an integer")
			return
		}
	}
	current += increment
	value[string(cmd.Args[2])] = strconv.FormatInt(current, 10)

	ctx, cancel := context.WithTimeout(context.Background(), redisDispatchTimeout)
	defer cancel()
	if err := r.saveHash(ctx, cmd.Args[1], value); err != nil {
		conn.WriteError(err.Error())
		return
	}
	conn.WriteInt64(current)
}

func (r *RedisServer) incr(conn redcon.Conn, cmd redcon.Command) {
	readTS := r.readTS()
	typ, err := r.keyTypeAt(context.Background(), cmd.Args[1], readTS)
	if err != nil {
		conn.WriteError(err.Error())
		return
	}
	if typ != redisTypeNone && typ != redisTypeString {
		conn.WriteError(wrongTypeMessage)
		return
	}

	current := int64(0)
	if typ == redisTypeString {
		raw, err := r.readValueAt(cmd.Args[1], readTS)
		if err != nil {
			conn.WriteError(err.Error())
			return
		}
		current, err = strconv.ParseInt(string(raw), 10, 64)
		if err != nil {
			conn.WriteError("ERR value is not an integer or out of range")
			return
		}
	}
	current++

	ctx, cancel := context.WithTimeout(context.Background(), redisDispatchTimeout)
	defer cancel()
	if err := r.dispatchElems(ctx, false, []*kv.Elem[kv.OP]{
		{Op: kv.Put, Key: cmd.Args[1], Value: []byte(strconv.FormatInt(current, 10))},
		{Op: kv.Del, Key: redisTTLKey(cmd.Args[1])},
	}); err != nil {
		conn.WriteError(err.Error())
		return
	}
	conn.WriteInt64(current)
}

func (r *RedisServer) hgetall(conn redcon.Conn, cmd redcon.Command) {
	readTS := r.readTS()
	typ, err := r.keyTypeAt(context.Background(), cmd.Args[1], readTS)
	if err != nil {
		conn.WriteError(err.Error())
		return
	}
	switch typ {
	case redisTypeNone:
		conn.WriteArray(0)
		return
	case redisTypeHash:
	default:
		conn.WriteError(wrongTypeMessage)
		return
	}

	value, _, err := r.loadHashAt(context.Background(), cmd.Args[1], readTS)
	if err != nil {
		conn.WriteError(err.Error())
		return
	}
	fields := make([]string, 0, len(value))
	for field := range value {
		fields = append(fields, field)
	}
	sort.Strings(fields)
	conn.WriteArray(len(fields) * 2)
	for _, field := range fields {
		conn.WriteBulkString(field)
		conn.WriteBulkString(value[field])
	}
}

func (r *RedisServer) zadd(conn redcon.Conn, cmd redcon.Command) {
	if (len(cmd.Args)-2)%2 != 0 {
		conn.WriteError("ERR syntax error")
		return
	}

	readTS := r.readTS()
	typ, err := r.keyTypeAt(context.Background(), cmd.Args[1], readTS)
	if err != nil {
		conn.WriteError(err.Error())
		return
	}
	if typ != redisTypeNone && typ != redisTypeZSet {
		conn.WriteError(wrongTypeMessage)
		return
	}

	value, _, err := r.loadZSetAt(context.Background(), cmd.Args[1], readTS)
	if err != nil {
		conn.WriteError(err.Error())
		return
	}
	members := zsetEntriesToMap(value.Entries)

	var added int
	for i := 2; i < len(cmd.Args); i += 2 {
		score, err := strconv.ParseFloat(string(cmd.Args[i]), 64)
		if err != nil {
			conn.WriteError(err.Error())
			return
		}
		member := string(cmd.Args[i+1])
		if _, ok := members[member]; !ok {
			added++
		}
		members[member] = score
	}
	value.Entries = zsetMapToEntries(members)

	ctx, cancel := context.WithTimeout(context.Background(), redisDispatchTimeout)
	defer cancel()
	if err := r.saveZSet(ctx, cmd.Args[1], value); err != nil {
		conn.WriteError(err.Error())
		return
	}
	conn.WriteInt(added)
}

func (r *RedisServer) zincrby(conn redcon.Conn, cmd redcon.Command) {
	increment, err := strconv.ParseFloat(string(cmd.Args[2]), 64)
	if err != nil {
		conn.WriteError(err.Error())
		return
	}

	readTS := r.readTS()
	typ, err := r.keyTypeAt(context.Background(), cmd.Args[1], readTS)
	if err != nil {
		conn.WriteError(err.Error())
		return
	}
	if typ != redisTypeNone && typ != redisTypeZSet {
		conn.WriteError(wrongTypeMessage)
		return
	}

	value, _, err := r.loadZSetAt(context.Background(), cmd.Args[1], readTS)
	if err != nil {
		conn.WriteError(err.Error())
		return
	}
	members := zsetEntriesToMap(value.Entries)
	member := string(cmd.Args[3])
	members[member] += increment
	newScore := members[member]
	value.Entries = zsetMapToEntries(members)

	ctx, cancel := context.WithTimeout(context.Background(), redisDispatchTimeout)
	defer cancel()
	if err := r.saveZSet(ctx, cmd.Args[1], value); err != nil {
		conn.WriteError(err.Error())
		return
	}
	conn.WriteBulkString(formatRedisFloat(newScore))
}

func (r *RedisServer) zrange(conn redcon.Conn, cmd redcon.Command) {
	start, err := parseInt(cmd.Args[2])
	if err != nil {
		conn.WriteError(err.Error())
		return
	}
	stop, err := parseInt(cmd.Args[3])
	if err != nil {
		conn.WriteError(err.Error())
		return
	}

	withScores := false
	reverse := false
	for _, arg := range cmd.Args[4:] {
		switch strings.ToUpper(string(arg)) {
		case "WITHSCORES":
			withScores = true
		case "REV":
			reverse = true
		default:
			conn.WriteError("ERR syntax error")
			return
		}
	}

	readTS := r.readTS()
	typ, err := r.keyTypeAt(context.Background(), cmd.Args[1], readTS)
	if err != nil {
		conn.WriteError(err.Error())
		return
	}
	switch typ {
	case redisTypeNone:
		conn.WriteArray(0)
		return
	case redisTypeZSet:
	default:
		conn.WriteError(wrongTypeMessage)
		return
	}

	value, _, err := r.loadZSetAt(context.Background(), cmd.Args[1], readTS)
	if err != nil {
		conn.WriteError(err.Error())
		return
	}
	entries := append([]redisZSetEntry(nil), value.Entries...)
	if reverse {
		for i, j := 0, len(entries)-1; i < j; i, j = i+1, j-1 {
			entries[i], entries[j] = entries[j], entries[i]
		}
	}
	s, e := normalizeRankRange(start, stop, len(entries))
	if e < s {
		conn.WriteArray(0)
		return
	}
	selected := entries[s : e+1]

	if withScores {
		conn.WriteArray(len(selected) * 2)
		for _, entry := range selected {
			conn.WriteBulkString(entry.Member)
			conn.WriteBulkString(formatRedisFloat(entry.Score))
		}
		return
	}

	conn.WriteArray(len(selected))
	for _, entry := range selected {
		conn.WriteBulkString(entry.Member)
	}
}

func (r *RedisServer) zrem(conn redcon.Conn, cmd redcon.Command) {
	readTS := r.readTS()
	typ, err := r.keyTypeAt(context.Background(), cmd.Args[1], readTS)
	if err != nil {
		conn.WriteError(err.Error())
		return
	}
	switch typ {
	case redisTypeNone:
		conn.WriteInt(0)
		return
	case redisTypeZSet:
	default:
		conn.WriteError(wrongTypeMessage)
		return
	}

	value, _, err := r.loadZSetAt(context.Background(), cmd.Args[1], readTS)
	if err != nil {
		conn.WriteError(err.Error())
		return
	}
	members := zsetEntriesToMap(value.Entries)

	removed := 0
	for _, member := range cmd.Args[2:] {
		if _, ok := members[string(member)]; ok {
			delete(members, string(member))
			removed++
		}
	}

	if removed > 0 {
		ctx, cancel := context.WithTimeout(context.Background(), redisDispatchTimeout)
		defer cancel()
		if len(members) == 0 {
			elems, _, err := r.deleteLogicalKeyElems(ctx, cmd.Args[1], readTS)
			if err != nil {
				conn.WriteError(err.Error())
				return
			}
			err = r.dispatchElems(ctx, true, filterElemsForKey(elems, redisZSetKey(cmd.Args[1]), redisTTLKey(cmd.Args[1])))
			if err != nil {
				conn.WriteError(err.Error())
				return
			}
		} else {
			value.Entries = zsetMapToEntries(members)
			if err := r.saveZSet(ctx, cmd.Args[1], value); err != nil {
				conn.WriteError(err.Error())
				return
			}
		}
	}
	conn.WriteInt(removed)
}

func (r *RedisServer) zremrangebyrank(conn redcon.Conn, cmd redcon.Command) {
	start, err := parseInt(cmd.Args[2])
	if err != nil {
		conn.WriteError(err.Error())
		return
	}
	stop, err := parseInt(cmd.Args[3])
	if err != nil {
		conn.WriteError(err.Error())
		return
	}

	readTS := r.readTS()
	typ, err := r.keyTypeAt(context.Background(), cmd.Args[1], readTS)
	if err != nil {
		conn.WriteError(err.Error())
		return
	}
	switch typ {
	case redisTypeNone:
		conn.WriteInt(0)
		return
	case redisTypeZSet:
	default:
		conn.WriteError(wrongTypeMessage)
		return
	}

	value, _, err := r.loadZSetAt(context.Background(), cmd.Args[1], readTS)
	if err != nil {
		conn.WriteError(err.Error())
		return
	}
	s, e := normalizeRankRange(start, stop, len(value.Entries))
	if e < s {
		conn.WriteInt(0)
		return
	}

	remaining := append([]redisZSetEntry{}, value.Entries[:s]...)
	remaining = append(remaining, value.Entries[e+1:]...)
	removed := e - s + 1

	ctx, cancel := context.WithTimeout(context.Background(), redisDispatchTimeout)
	defer cancel()
	if len(remaining) == 0 {
		elems, _, err := r.deleteLogicalKeyElems(ctx, cmd.Args[1], readTS)
		if err != nil {
			conn.WriteError(err.Error())
			return
		}
		err = r.dispatchElems(ctx, true, filterElemsForKey(elems, redisZSetKey(cmd.Args[1]), redisTTLKey(cmd.Args[1])))
		if err != nil {
			conn.WriteError(err.Error())
			return
		}
	} else {
		value.Entries = remaining
		if err := r.saveZSet(ctx, cmd.Args[1], value); err != nil {
			conn.WriteError(err.Error())
			return
		}
	}
	conn.WriteInt(removed)
}

func (r *RedisServer) bzpopmin(conn redcon.Conn, cmd redcon.Command) {
	timeoutSeconds, err := strconv.ParseFloat(string(cmd.Args[len(cmd.Args)-1]), 64)
	if err != nil || timeoutSeconds < 0 {
		conn.WriteError("ERR timeout is not a float or out of range")
		return
	}

	var deadline time.Time
	if timeoutSeconds > 0 {
		deadline = time.Now().Add(time.Duration(timeoutSeconds * float64(time.Second)))
	}

	for {
		for _, key := range cmd.Args[1 : len(cmd.Args)-1] {
			readTS := r.readTS()
			typ, err := r.keyTypeAt(context.Background(), key, readTS)
			if err != nil {
				conn.WriteError(err.Error())
				return
			}
			switch typ {
			case redisTypeNone:
				continue
			case redisTypeZSet:
			default:
				conn.WriteError(wrongTypeMessage)
				return
			}

			value, _, err := r.loadZSetAt(context.Background(), key, readTS)
			if err != nil {
				conn.WriteError(err.Error())
				return
			}
			if len(value.Entries) == 0 {
				continue
			}

			entry := value.Entries[0]
			remaining := append([]redisZSetEntry(nil), value.Entries[1:]...)

			ctx, cancel := context.WithTimeout(context.Background(), redisDispatchTimeout)
			if len(remaining) == 0 {
				err = r.dispatchElems(ctx, true, []*kv.Elem[kv.OP]{
					{Op: kv.Del, Key: redisZSetKey(key)},
					{Op: kv.Del, Key: redisTTLKey(key)},
				})
			} else {
				err = r.saveZSet(ctx, key, redisZSetValue{Entries: remaining})
			}
			cancel()
			if err != nil {
				conn.WriteError(err.Error())
				return
			}

			conn.WriteArray(3)
			conn.WriteBulk(key)
			conn.WriteBulkString(entry.Member)
			conn.WriteBulkString(formatRedisFloat(entry.Score))
			return
		}

		if !deadline.IsZero() && !time.Now().Before(deadline) {
			conn.WriteNull()
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func (r *RedisServer) lpush(conn redcon.Conn, cmd redcon.Command) {
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

	current, err := r.listValuesAt(context.Background(), cmd.Args[1], readTS)
	if err != nil {
		conn.WriteError(err.Error())
		return
	}

	prefix := make([]string, 0, len(cmd.Args)-2)
	for i := len(cmd.Args) - 1; i >= 2; i-- {
		prefix = append(prefix, string(cmd.Args[i]))
	}
	next := append(prefix, current...)

	ctx, cancel := context.WithTimeout(context.Background(), redisDispatchTimeout)
	defer cancel()
	if err := r.rewriteList(ctx, cmd.Args[1], next); err != nil {
		conn.WriteError(err.Error())
		return
	}
	conn.WriteInt(len(next))
}

func (r *RedisServer) ltrim(conn redcon.Conn, cmd redcon.Command) {
	start, err := parseInt(cmd.Args[2])
	if err != nil {
		conn.WriteError(err.Error())
		return
	}
	stop, err := parseInt(cmd.Args[3])
	if err != nil {
		conn.WriteError(err.Error())
		return
	}
	readTS := r.readTS()
	typ, err := r.keyTypeAt(context.Background(), cmd.Args[1], readTS)
	if err != nil {
		conn.WriteError(err.Error())
		return
	}
	if typ == redisTypeNone {
		conn.WriteString("OK")
		return
	}
	if typ != redisTypeList {
		conn.WriteError(wrongTypeMessage)
		return
	}
	current, err := r.listValuesAt(context.Background(), cmd.Args[1], readTS)
	if err != nil {
		conn.WriteError(err.Error())
		return
	}
	s, e := normalizeRankRange(start, stop, len(current))
	trimmed := []string{}
	if e >= s {
		trimmed = append(trimmed, current[s:e+1]...)
	}
	ctx, cancel := context.WithTimeout(context.Background(), redisDispatchTimeout)
	defer cancel()
	if err := r.rewriteList(ctx, cmd.Args[1], trimmed); err != nil {
		conn.WriteError(err.Error())
		return
	}
	conn.WriteString("OK")
}

func (r *RedisServer) lindex(conn redcon.Conn, cmd redcon.Command) {
	index, err := parseInt(cmd.Args[2])
	if err != nil {
		conn.WriteError(err.Error())
		return
	}
	readTS := r.readTS()
	typ, err := r.keyTypeAt(context.Background(), cmd.Args[1], readTS)
	if err != nil {
		conn.WriteError(err.Error())
		return
	}
	if typ == redisTypeNone {
		conn.WriteNull()
		return
	}
	if typ != redisTypeList {
		conn.WriteError(wrongTypeMessage)
		return
	}
	values, err := r.listValuesAt(context.Background(), cmd.Args[1], readTS)
	if err != nil {
		conn.WriteError(err.Error())
		return
	}
	idx := normalizeIndex(index, len(values))
	if idx < 0 {
		conn.WriteNull()
		return
	}
	conn.WriteBulkString(values[idx])
}

func (r *RedisServer) xadd(conn redcon.Conn, cmd redcon.Command) {
	readTS := r.readTS()
	typ, err := r.keyTypeAt(context.Background(), cmd.Args[1], readTS)
	if err != nil {
		conn.WriteError(err.Error())
		return
	}
	if typ != redisTypeNone && typ != redisTypeStream {
		conn.WriteError(wrongTypeMessage)
		return
	}

	argIndex := 2
	maxLen := 0
	if len(cmd.Args) >= 5 && strings.ToUpper(string(cmd.Args[argIndex])) == "MAXLEN" {
		argIndex++
		if argIndex < len(cmd.Args) && string(cmd.Args[argIndex]) == "~" {
			argIndex++
		}
		if argIndex >= len(cmd.Args) {
			conn.WriteError("ERR syntax error")
			return
		}
		maxLen, err = strconv.Atoi(string(cmd.Args[argIndex]))
		if err != nil || maxLen < 0 {
			conn.WriteError("ERR syntax error")
			return
		}
		argIndex++
	}
	if argIndex >= len(cmd.Args) {
		conn.WriteError("ERR syntax error")
		return
	}
	id := string(cmd.Args[argIndex])
	argIndex++
	if (len(cmd.Args)-argIndex)%2 != 0 {
		conn.WriteError("ERR wrong number of arguments for 'XADD' command")
		return
	}

	stream, _, err := r.loadStreamAt(context.Background(), cmd.Args[1], readTS)
	if err != nil {
		conn.WriteError(err.Error())
		return
	}

	if id == "*" {
		nowID := strconv.FormatInt(time.Now().UnixMilli(), 10) + "-0"
		if len(stream.Entries) > 0 && compareRedisStreamID(nowID, stream.Entries[len(stream.Entries)-1].ID) <= 0 {
			last, _ := parseRedisStreamID(stream.Entries[len(stream.Entries)-1].ID)
			nowID = strconv.FormatUint(last.ms, 10) + "-" + strconv.FormatUint(last.seq+1, 10)
		}
		id = nowID
	} else if len(stream.Entries) > 0 && compareRedisStreamID(id, stream.Entries[len(stream.Entries)-1].ID) <= 0 {
		conn.WriteError("ERR The ID specified in XADD is equal or smaller than the target stream top item")
		return
	}

	fields := make([]string, 0, len(cmd.Args)-argIndex)
	for _, arg := range cmd.Args[argIndex:] {
		fields = append(fields, string(arg))
	}
	stream.Entries = append(stream.Entries, redisStreamEntry{ID: id, Fields: fields})
	if maxLen > 0 && len(stream.Entries) > maxLen {
		stream.Entries = append([]redisStreamEntry(nil), stream.Entries[len(stream.Entries)-maxLen:]...)
	}

	ctx, cancel := context.WithTimeout(context.Background(), redisDispatchTimeout)
	defer cancel()
	if err := r.saveStream(ctx, cmd.Args[1], stream); err != nil {
		conn.WriteError(err.Error())
		return
	}
	conn.WriteBulkString(id)
}

func (r *RedisServer) xtrim(conn redcon.Conn, cmd redcon.Command) {
	if strings.ToUpper(string(cmd.Args[2])) != "MAXLEN" {
		conn.WriteError("ERR syntax error")
		return
	}

	argIndex := 3
	if argIndex < len(cmd.Args) && (string(cmd.Args[argIndex]) == "~" || string(cmd.Args[argIndex]) == "=") {
		argIndex++
	}
	if argIndex != len(cmd.Args)-1 {
		conn.WriteError("ERR syntax error")
		return
	}

	maxLen, err := strconv.Atoi(string(cmd.Args[argIndex]))
	if err != nil || maxLen < 0 {
		conn.WriteError("ERR syntax error")
		return
	}

	readTS := r.readTS()
	typ, err := r.keyTypeAt(context.Background(), cmd.Args[1], readTS)
	if err != nil {
		conn.WriteError(err.Error())
		return
	}
	if typ == redisTypeNone {
		conn.WriteInt(0)
		return
	}
	if typ != redisTypeStream {
		conn.WriteError(wrongTypeMessage)
		return
	}

	stream, _, err := r.loadStreamAt(context.Background(), cmd.Args[1], readTS)
	if err != nil {
		conn.WriteError(err.Error())
		return
	}
	if len(stream.Entries) <= maxLen {
		conn.WriteInt(0)
		return
	}

	removed := len(stream.Entries) - maxLen
	stream.Entries = append([]redisStreamEntry(nil), stream.Entries[removed:]...)

	ctx, cancel := context.WithTimeout(context.Background(), redisDispatchTimeout)
	defer cancel()
	if maxLen == 0 {
		if err := r.dispatchElems(ctx, true, []*kv.Elem[kv.OP]{
			{Op: kv.Del, Key: redisStreamKey(cmd.Args[1])},
			{Op: kv.Del, Key: redisTTLKey(cmd.Args[1])},
		}); err != nil {
			conn.WriteError(err.Error())
			return
		}
		conn.WriteInt(removed)
		return
	}
	if err := r.saveStream(ctx, cmd.Args[1], stream); err != nil {
		conn.WriteError(err.Error())
		return
	}
	conn.WriteInt(removed)
}

func (r *RedisServer) xrange(conn redcon.Conn, cmd redcon.Command) {
	r.rangeStream(conn, cmd, false)
}

func (r *RedisServer) xrevrange(conn redcon.Conn, cmd redcon.Command) {
	r.rangeStream(conn, cmd, true)
}

func (r *RedisServer) xread(conn redcon.Conn, cmd redcon.Command) {
	block := time.Duration(0)
	count := -1
	streamsIndex := -1

	for i := 1; i < len(cmd.Args); {
		switch strings.ToUpper(string(cmd.Args[i])) {
		case "COUNT":
			if i+1 >= len(cmd.Args) {
				conn.WriteError("ERR syntax error")
				return
			}
			var err error
			count, err = strconv.Atoi(string(cmd.Args[i+1]))
			if err != nil || count <= 0 {
				conn.WriteError("ERR syntax error")
				return
			}
			i += 2
		case "BLOCK":
			if i+1 >= len(cmd.Args) {
				conn.WriteError("ERR syntax error")
				return
			}
			ms, err := strconv.Atoi(string(cmd.Args[i+1]))
			if err != nil || ms < 0 {
				conn.WriteError("ERR syntax error")
				return
			}
			block = time.Duration(ms) * time.Millisecond
			i += 2
		case "STREAMS":
			streamsIndex = i + 1
			i = len(cmd.Args)
		default:
			conn.WriteError("ERR syntax error")
			return
		}
	}

	if streamsIndex < 0 || streamsIndex >= len(cmd.Args) {
		conn.WriteError("ERR syntax error")
		return
	}
	remaining := len(cmd.Args) - streamsIndex
	if remaining%2 != 0 {
		conn.WriteError("ERR syntax error")
		return
	}

	streamCount := remaining / 2
	keys := make([][]byte, streamCount)
	afterIDs := make([]string, streamCount)
	for i := 0; i < streamCount; i++ {
		keys[i] = cmd.Args[streamsIndex+i]
		afterIDs[i] = string(cmd.Args[streamsIndex+streamCount+i])
	}

	for i, afterID := range afterIDs {
		if afterID != "$" {
			continue
		}
		readTS := r.readTS()
		typ, err := r.keyTypeAt(context.Background(), keys[i], readTS)
		if err != nil {
			conn.WriteError(err.Error())
			return
		}
		switch typ {
		case redisTypeNone:
			afterIDs[i] = "0-0"
		case redisTypeStream:
			stream, _, err := r.loadStreamAt(context.Background(), keys[i], readTS)
			if err != nil {
				conn.WriteError(err.Error())
				return
			}
			if len(stream.Entries) == 0 {
				afterIDs[i] = "0-0"
			} else {
				afterIDs[i] = stream.Entries[len(stream.Entries)-1].ID
			}
		default:
			conn.WriteError(wrongTypeMessage)
			return
		}
	}

	var deadline time.Time
	if block > 0 {
		deadline = time.Now().Add(block)
	}

	for {
		type streamRead struct {
			key     []byte
			entries []redisStreamEntry
		}
		results := make([]streamRead, 0, len(keys))

		for i, key := range keys {
			readTS := r.readTS()
			typ, err := r.keyTypeAt(context.Background(), key, readTS)
			if err != nil {
				conn.WriteError(err.Error())
				return
			}
			switch typ {
			case redisTypeNone:
				continue
			case redisTypeStream:
			default:
				conn.WriteError(wrongTypeMessage)
				return
			}

			stream, _, err := r.loadStreamAt(context.Background(), key, readTS)
			if err != nil {
				conn.WriteError(err.Error())
				return
			}
			selected := make([]redisStreamEntry, 0, len(stream.Entries))
			for _, entry := range stream.Entries {
				if compareRedisStreamID(entry.ID, afterIDs[i]) <= 0 {
					continue
				}
				selected = append(selected, entry)
				if count > 0 && len(selected) >= count {
					break
				}
			}
			if len(selected) > 0 {
				results = append(results, streamRead{key: key, entries: selected})
			}
		}

		if len(results) > 0 {
			conn.WriteArray(len(results))
			for _, result := range results {
				conn.WriteArray(2)
				conn.WriteBulk(result.key)
				conn.WriteArray(len(result.entries))
				for _, entry := range result.entries {
					conn.WriteArray(2)
					conn.WriteBulkString(entry.ID)
					conn.WriteArray(len(entry.Fields))
					for _, field := range entry.Fields {
						conn.WriteBulkString(field)
					}
				}
			}
			return
		}

		if block == 0 || (!deadline.IsZero() && !time.Now().Before(deadline)) {
			conn.WriteNull()
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func (r *RedisServer) xlen(conn redcon.Conn, cmd redcon.Command) {
	readTS := r.readTS()
	typ, err := r.keyTypeAt(context.Background(), cmd.Args[1], readTS)
	if err != nil {
		conn.WriteError(err.Error())
		return
	}
	if typ == redisTypeNone {
		conn.WriteInt(0)
		return
	}
	if typ != redisTypeStream {
		conn.WriteError(wrongTypeMessage)
		return
	}
	stream, _, err := r.loadStreamAt(context.Background(), cmd.Args[1], readTS)
	if err != nil {
		conn.WriteError(err.Error())
		return
	}
	conn.WriteInt(len(stream.Entries))
}

func (r *RedisServer) rangeStream(conn redcon.Conn, cmd redcon.Command, reverse bool) {
	count := -1
	for i := 4; i < len(cmd.Args); i += 2 {
		if i+1 >= len(cmd.Args) || strings.ToUpper(string(cmd.Args[i])) != "COUNT" {
			conn.WriteError("ERR syntax error")
			return
		}
		var err error
		count, err = strconv.Atoi(string(cmd.Args[i+1]))
		if err != nil || count < 0 {
			conn.WriteError("ERR syntax error")
			return
		}
	}

	readTS := r.readTS()
	typ, err := r.keyTypeAt(context.Background(), cmd.Args[1], readTS)
	if err != nil {
		conn.WriteError(err.Error())
		return
	}
	if typ == redisTypeNone {
		conn.WriteArray(0)
		return
	}
	if typ != redisTypeStream {
		conn.WriteError(wrongTypeMessage)
		return
	}

	stream, _, err := r.loadStreamAt(context.Background(), cmd.Args[1], readTS)
	if err != nil {
		conn.WriteError(err.Error())
		return
	}

	selected := make([]redisStreamEntry, 0, len(stream.Entries))
	startRaw := string(cmd.Args[2])
	endRaw := string(cmd.Args[3])

	appendIfMatch := func(entry redisStreamEntry) bool {
		if reverse {
			if !streamWithinUpper(entry.ID, startRaw) || !streamWithinLower(entry.ID, endRaw) {
				return true
			}
		} else {
			if !streamWithinLower(entry.ID, startRaw) || !streamWithinUpper(entry.ID, endRaw) {
				return true
			}
		}
		selected = append(selected, entry)
		return count < 0 || len(selected) < count
	}

	if reverse {
		for i := len(stream.Entries) - 1; i >= 0; i-- {
			if !appendIfMatch(stream.Entries[i]) {
				break
			}
		}
	} else {
		for _, entry := range stream.Entries {
			if !appendIfMatch(entry) {
				break
			}
		}
	}

	conn.WriteArray(len(selected))
	for _, entry := range selected {
		conn.WriteArray(2)
		conn.WriteBulkString(entry.ID)
		conn.WriteArray(len(entry.Fields))
		for _, field := range entry.Fields {
			conn.WriteBulkString(field)
		}
	}
}

func streamWithinLower(entryID, raw string) bool {
	if raw == "-" {
		return true
	}
	exclusive := strings.HasPrefix(raw, "(")
	if exclusive {
		raw = raw[1:]
	}
	cmp := compareRedisStreamID(entryID, raw)
	if exclusive {
		return cmp > 0
	}
	return cmp >= 0
}

func streamWithinUpper(entryID, raw string) bool {
	if raw == "+" {
		return true
	}
	exclusive := strings.HasPrefix(raw, "(")
	if exclusive {
		raw = raw[1:]
	}
	cmp := compareRedisStreamID(entryID, raw)
	if exclusive {
		return cmp < 0
	}
	return cmp <= 0
}
