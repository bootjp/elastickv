package proxy

import "strings"

// CommandCategory classifies a Redis command for routing purposes.
type CommandCategory int

const (
	CmdRead     CommandCategory = iota // GET, HGET, LRANGE, ZRANGE, etc.
	CmdWrite                           // SET, DEL, HSET, LPUSH, ZADD, etc.
	CmdBlocking                        // BZPOPMIN, XREAD (with BLOCK)
	CmdPubSub                          // SUBSCRIBE, PUBSUB
	CmdAdmin                           // PING, INFO, CLIENT, SELECT, QUIT, DBSIZE, SCAN, AUTH
	CmdTxn                             // MULTI, EXEC, DISCARD
	CmdScript                          // EVAL, EVALSHA
)

var commandTable = map[string]CommandCategory{
	// Read commands
	"GET":              CmdRead,
	"GETDEL":           CmdWrite, // read+write → write
	"HGET":             CmdRead,
	"HGETALL":          CmdRead,
	"HEXISTS":          CmdRead,
	"HLEN":             CmdRead,
	"HMGET":            CmdRead,
	"EXISTS":           CmdRead,
	"KEYS":             CmdRead,
	"LINDEX":           CmdRead,
	"LLEN":             CmdRead,
	"LPOS":             CmdRead,
	"LRANGE":           CmdRead,
	"PTTL":             CmdRead,
	"TTL":              CmdRead,
	"TYPE":             CmdRead,
	"SCARD":            CmdRead,
	"SISMEMBER":        CmdRead,
	"SMEMBERS":         CmdRead,
	"XLEN":             CmdRead,
	"XRANGE":           CmdRead,
	"XREVRANGE":        CmdRead,
	"ZCARD":            CmdRead,
	"ZCOUNT":           CmdRead,
	"ZRANGE":           CmdRead,
	"ZRANGEBYSCORE":    CmdRead,
	"ZREVRANGE":        CmdRead,
	"ZREVRANGEBYSCORE": CmdRead,
	"ZSCORE":           CmdRead,
	"PFCOUNT":          CmdRead,

	// Write commands
	"SET":              CmdWrite,
	"SETEX":            CmdWrite,
	"SETNX":            CmdWrite,
	"DEL":              CmdWrite,
	"HSET":             CmdWrite,
	"HMSET":            CmdWrite,
	"HDEL":             CmdWrite,
	"HINCRBY":          CmdWrite,
	"INCR":             CmdWrite,
	"LPUSH":            CmdWrite,
	"LPOP":             CmdWrite,
	"RPUSH":            CmdWrite,
	"RPOP":             CmdWrite,
	"RPOPLPUSH":        CmdWrite,
	"LREM":             CmdWrite,
	"LSET":             CmdWrite,
	"LTRIM":            CmdWrite,
	"SADD":             CmdWrite,
	"SREM":             CmdWrite,
	"EXPIRE":           CmdWrite,
	"PEXPIRE":          CmdWrite,
	"RENAME":           CmdWrite,
	"XADD":             CmdWrite,
	"XTRIM":            CmdWrite,
	"ZADD":             CmdWrite,
	"ZINCRBY":          CmdWrite,
	"ZREM":             CmdWrite,
	"ZREMRANGEBYSCORE": CmdWrite,
	"ZREMRANGEBYRANK":  CmdWrite,
	"ZPOPMIN":          CmdWrite,
	"PFADD":            CmdWrite,
	"FLUSHALL":         CmdWrite,
	"FLUSHDB":          CmdWrite,
	"PUBLISH":          CmdWrite, // write to both backends

	// Blocking commands
	"BZPOPMIN": CmdBlocking,
	// XREAD is handled specially in ClassifyCommand (BLOCK arg check)

	// PubSub commands
	"SUBSCRIBE":    CmdPubSub,
	"UNSUBSCRIBE":  CmdPubSub,
	"PSUBSCRIBE":   CmdPubSub,
	"PUNSUBSCRIBE": CmdPubSub,
	"PUBSUB":       CmdPubSub,

	// Admin commands — forwarded to primary only
	"PING":    CmdAdmin,
	"INFO":    CmdAdmin,
	"CLIENT":  CmdAdmin,
	"SELECT":  CmdAdmin,
	"QUIT":    CmdAdmin,
	"DBSIZE":  CmdAdmin,
	"SCAN":    CmdAdmin,
	"AUTH":    CmdAdmin,
	"HELLO":   CmdAdmin,
	"WAIT":    CmdAdmin,
	"CONFIG":  CmdAdmin,
	"OBJECT":  CmdAdmin,
	"DEBUG":   CmdAdmin,
	"CLUSTER": CmdAdmin,
	"COMMAND": CmdAdmin,

	// Transaction commands
	"MULTI":   CmdTxn,
	"EXEC":    CmdTxn,
	"DISCARD": CmdTxn,

	// Script commands
	"EVAL":    CmdScript,
	"EVALSHA": CmdScript,
}

// ClassifyCommand returns the category for a Redis command name.
// XREAD is classified as CmdBlocking if args contain BLOCK, otherwise CmdRead.
// Unknown commands default to CmdWrite (sent to both backends).
func ClassifyCommand(name string, args [][]byte) CommandCategory {
	upper := strings.ToUpper(name)

	// Special case: XREAD with BLOCK
	if upper == "XREAD" {
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
