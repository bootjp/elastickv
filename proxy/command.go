package proxy

import "strings"

// CommandCategory classifies a Redis command for routing purposes.
type CommandCategory int

const (
	CmdRead     CommandCategory = iota // GET, HGET, LRANGE, ZRANGE, etc.
	CmdWrite                           // SET, DEL, HSET, LPUSH, ZADD, etc.
	CmdBlocking                        // BLPOP, BRPOP, BZPOPMIN, XREAD (with BLOCK)
	CmdPubSub                          // SUBSCRIBE, UNSUBSCRIBE, PSUBSCRIBE, PUNSUBSCRIBE, PUBSUB (note: PUBLISH is CmdWrite)
	CmdAdmin                           // PING, INFO, CLIENT, SELECT, QUIT, DBSIZE, SCAN, AUTH
	CmdTxn                             // MULTI, EXEC, DISCARD
	CmdScript                          // EVAL, EVALSHA
)

const (
	cmdNameAUTH   = "AUTH"
	cmdNameSELECT = "SELECT"
)

var commandTable = map[string]CommandCategory{
	// ---- Read commands ----
	"GET":                  CmdRead,
	"GETRANGE":             CmdRead,
	"MGET":                 CmdRead,
	"STRLEN":               CmdRead,
	"HGET":                 CmdRead,
	"HGETALL":              CmdRead,
	"HEXISTS":              CmdRead,
	"HLEN":                 CmdRead,
	"HMGET":                CmdRead,
	"HKEYS":                CmdRead,
	"HVALS":                CmdRead,
	"HRANDFIELD":           CmdRead,
	"HSCAN":                CmdRead,
	"EXISTS":               CmdRead,
	"KEYS":                 CmdRead,
	"RANDOMKEY":            CmdRead,
	"LINDEX":               CmdRead,
	"LLEN":                 CmdRead,
	"LPOS":                 CmdRead,
	"LRANGE":               CmdRead,
	"PTTL":                 CmdRead,
	"TTL":                  CmdRead,
	"TYPE":                 CmdRead,
	"SCARD":                CmdRead,
	"SISMEMBER":            CmdRead,
	"SMISMEMBER":           CmdRead,
	"SMEMBERS":             CmdRead,
	"SRANDMEMBER":          CmdRead,
	"SDIFF":                CmdRead,
	"SINTER":               CmdRead,
	"SINTERCARD":           CmdRead,
	"SUNION":               CmdRead,
	"SSCAN":                CmdRead,
	"XLEN":                 CmdRead,
	"XRANGE":               CmdRead,
	"XREVRANGE":            CmdRead,
	"XINFO":                CmdRead,
	"XPENDING":             CmdRead,
	"ZCARD":                CmdRead,
	"ZCOUNT":               CmdRead,
	"ZLEXCOUNT":            CmdRead,
	"ZMSCORE":              CmdRead,
	"ZRANGE":               CmdRead,
	"ZRANGEBYSCORE":        CmdRead,
	"ZRANGEBYLEX":          CmdRead,
	"ZREVRANGE":            CmdRead,
	"ZREVRANGEBYSCORE":     CmdRead,
	"ZREVRANGEBYLEX":       CmdRead,
	"ZRANK":                CmdRead,
	"ZREVRANK":             CmdRead,
	"ZSCORE":               CmdRead,
	"ZSCAN":                CmdRead,
	"ZDIFF":                CmdRead,
	"PFCOUNT":              CmdRead,
	"TOUCH":                CmdRead,
	"DUMP":                 CmdRead,
	"GEODIST":              CmdRead,
	"GEOHASH":              CmdRead,
	"GEOPOS":               CmdRead,
	"GEOSEARCH":            CmdRead,
	"GEORADIUS_RO":         CmdRead,
	"GEORADIUSBYMEMBER_RO": CmdRead,
	"OBJECT":               CmdRead,
	"SORT_RO":              CmdRead,
	"SUBSTR":               CmdRead,

	// ---- Write commands ----
	"SET":               CmdWrite,
	"SETEX":             CmdWrite,
	"PSETEX":            CmdWrite,
	"SETNX":             CmdWrite,
	"SETRANGE":          CmdWrite,
	"MSET":              CmdWrite,
	"MSETNX":            CmdWrite,
	"APPEND":            CmdWrite,
	"GETSET":            CmdWrite,
	"GETEX":             CmdWrite,
	"GETDEL":            CmdWrite,
	"DEL":               CmdWrite,
	"UNLINK":            CmdWrite,
	"COPY":              CmdWrite,
	"RENAME":            CmdWrite,
	"RENAMENX":          CmdWrite,
	"RESTORE":           CmdWrite,
	"INCR":              CmdWrite,
	"INCRBY":            CmdWrite,
	"INCRBYFLOAT":       CmdWrite,
	"DECR":              CmdWrite,
	"DECRBY":            CmdWrite,
	"HSET":              CmdWrite,
	"HMSET":             CmdWrite,
	"HDEL":              CmdWrite,
	"HINCRBY":           CmdWrite,
	"HINCRBYFLOAT":      CmdWrite,
	"HSETNX":            CmdWrite,
	"LPUSH":             CmdWrite,
	"LPUSHX":            CmdWrite,
	"LPOP":              CmdWrite,
	"RPUSH":             CmdWrite,
	"RPUSHX":            CmdWrite,
	"RPOP":              CmdWrite,
	"RPOPLPUSH":         CmdWrite,
	"LMOVE":             CmdWrite,
	"LREM":              CmdWrite,
	"LSET":              CmdWrite,
	"LTRIM":             CmdWrite,
	"LINSERT":           CmdWrite,
	"SADD":              CmdWrite,
	"SREM":              CmdWrite,
	"SPOP":              CmdWrite,
	"SMOVE":             CmdWrite,
	"SDIFFSTORE":        CmdWrite,
	"SINTERSTORE":       CmdWrite,
	"SUNIONSTORE":       CmdWrite,
	"EXPIRE":            CmdWrite,
	"PEXPIRE":           CmdWrite,
	"EXPIREAT":          CmdWrite,
	"PEXPIREAT":         CmdWrite,
	"PERSIST":           CmdWrite,
	"SORT":              CmdWrite,
	"XADD":              CmdWrite,
	"XTRIM":             CmdWrite,
	"XACK":              CmdWrite,
	"XDEL":              CmdWrite,
	"XGROUP":            CmdWrite,
	"XCLAIM":            CmdWrite,
	"XAUTOCLAIM":        CmdWrite,
	"ZADD":              CmdWrite,
	"ZINCRBY":           CmdWrite,
	"ZREM":              CmdWrite,
	"ZREMRANGEBYSCORE":  CmdWrite,
	"ZREMRANGEBYRANK":   CmdWrite,
	"ZREMRANGEBYLEX":    CmdWrite,
	"ZPOPMIN":           CmdWrite,
	"ZPOPMAX":           CmdWrite,
	"ZRANGESTORE":       CmdWrite,
	"ZUNIONSTORE":       CmdWrite,
	"ZINTERSTORE":       CmdWrite,
	"ZDIFFSTORE":        CmdWrite,
	"PFADD":             CmdWrite,
	"PFMERGE":           CmdWrite,
	"GEOADD":            CmdWrite,
	"GEORADIUS":         CmdWrite,
	"GEORADIUSBYMEMBER": CmdWrite,
	"GEOSEARCHSTORE":    CmdWrite,
	"FLUSHALL":          CmdWrite,
	"FLUSHDB":           CmdWrite,
	"PUBLISH":           CmdWrite, // write to both backends

	// ---- Blocking commands ----
	"BLPOP":      CmdBlocking,
	"BRPOP":      CmdBlocking,
	"BRPOPLPUSH": CmdBlocking,
	"BLMOVE":     CmdBlocking,
	"BLMPOP":     CmdBlocking,
	"BZPOPMIN":   CmdBlocking,
	"BZPOPMAX":   CmdBlocking,
	// XREAD/XREADGROUP handled specially in ClassifyCommand (BLOCK arg check)

	// ---- PubSub commands ----
	"SUBSCRIBE":    CmdPubSub,
	"UNSUBSCRIBE":  CmdPubSub,
	"PSUBSCRIBE":   CmdPubSub,
	"PUNSUBSCRIBE": CmdPubSub,
	"SSUBSCRIBE":   CmdPubSub,
	"SUNSUBSCRIBE": CmdPubSub,
	"PUBSUB":       CmdPubSub,

	// ---- Admin commands — forwarded to primary only ----
	"PING":    CmdAdmin,
	"ECHO":    CmdAdmin,
	"INFO":    CmdAdmin,
	"CLIENT":  CmdAdmin,
	"SELECT":  CmdAdmin,
	"QUIT":    CmdAdmin,
	"RESET":   CmdAdmin,
	"DBSIZE":  CmdAdmin,
	"SCAN":    CmdAdmin,
	"AUTH":    CmdAdmin,
	"HELLO":   CmdAdmin,
	"WAIT":    CmdAdmin,
	"CONFIG":  CmdAdmin,
	"DEBUG":   CmdAdmin,
	"CLUSTER": CmdAdmin,
	"COMMAND": CmdAdmin,
	"TIME":    CmdAdmin,
	"SLOWLOG": CmdAdmin,
	"MEMORY":  CmdAdmin,
	"LATENCY": CmdAdmin,
	"MODULE":  CmdAdmin,
	"ACL":     CmdAdmin,
	"SWAPDB":  CmdAdmin,
	// WATCH/UNWATCH: connection-scoped optimistic locking.
	// Forwarded to primary only since the proxy uses a shared connection pool
	// and per-connection WATCH state cannot be maintained.
	"WATCH":   CmdAdmin,
	"UNWATCH": CmdAdmin,

	// ---- Transaction commands ----
	"MULTI":   CmdTxn,
	"EXEC":    CmdTxn,
	"DISCARD": CmdTxn,

	// ---- Script commands ----
	"EVAL":       CmdScript,
	"EVALSHA":    CmdScript,
	"EVAL_RO":    CmdScript,
	"EVALSHA_RO": CmdScript,
	"SCRIPT":     CmdScript,
	"FUNCTION":   CmdScript,
	"FCALL":      CmdScript,
	"FCALL_RO":   CmdScript,
}

// ClassifyCommand returns the category for a Redis command name.
// XREAD/XREADGROUP is classified as CmdBlocking if args contain BLOCK, otherwise CmdRead.
// Unknown commands default to CmdWrite (sent to both backends).
func ClassifyCommand(name string, args [][]byte) CommandCategory {
	upper := strings.ToUpper(name)

	// Special case: XREAD/XREADGROUP with BLOCK
	if upper == "XREAD" || upper == "XREADGROUP" {
		for _, arg := range args {
			if strings.ToUpper(string(arg)) == "BLOCK" {
				return CmdBlocking
			}
		}
		return CmdRead
	}

	if cat, ok := commandTable[upper]; ok {
		return cat
	}
	// Unknown commands → treat as write (safe default, sent to both backends)
	return CmdWrite
}
