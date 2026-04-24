package adapter

// redis_command_info.go holds the static metadata table consumed by the
// Redis `COMMAND` handler. It is intentionally a single, grep-able file so
// that adding a new command is a one-liner:
//
//	1) Register the new handler in RedisServer.route (redis.go).
//	2) Add an argsLen entry (redis.go).
//	3) Add a row below. Forgetting step 3 is NOT fatal — the COMMAND
//	   handler falls back to a zero-metadata entry and emits one warning
//	   log per command name so the omission is discoverable — but you
//	   should do step 3 anyway.
//
// The table is the source of truth for `COMMAND`, `COMMAND INFO`,
// `COMMAND COUNT`, `COMMAND LIST`, `COMMAND DOCS`, and `COMMAND GETKEYS`.
//
// Shape notes (Redis reference):
//   - arity: exact positive arity, or negative meaning "at least |arity|"
//   - flags: one of "readonly" | "write" | "admin". We do NOT currently
//     emit "denyoom" / "pubsub" / "loading" / "stale" / "fast" etc. —
//     real Redis clients only consume this field for coarse routing.
//   - first_key / last_key / step describe the key positions inside the
//     argv. first_key=0 means the command operates on zero keys (pure
//     connection / server commands). last_key=-1 means "all remaining
//     args are keys" (MSET-shaped). step=1 means keys are consecutive;
//     step=2 is used by MSET-like key/value pairs.

import (
	"log"
	"sort"
	"strings"
	"sync"
)

// redisCommandFlag values are string constants so the raw strings are not
// duplicated across the table.
const (
	redisCmdFlagReadonly = "readonly"
	redisCmdFlagWrite    = "write"
	redisCmdFlagAdmin    = "admin"
)

// redisCommandMeta is a single row in the COMMAND table.
type redisCommandMeta struct {
	// Name is the lowercase command name as reported by Redis. Keyed in the
	// table by uppercase for dispatch-time lookup; the lowercase form is
	// what goes onto the wire in COMMAND INFO.
	Name     string
	Arity    int
	Flags    []string
	FirstKey int
	LastKey  int
	Step     int
}

// redisCommandTable maps UPPERCASE command name -> metadata. Every entry
// routed by RedisServer.route should appear here. Entries are listed in
// alphabetical order to keep diffs small when adding new commands.
//
//nolint:mnd // magic numbers here are literal Redis metadata (arity, key positions)
var redisCommandTable = map[string]redisCommandMeta{
	"BZPOPMIN": {Name: "bzpopmin", Arity: -3, Flags: []string{redisCmdFlagWrite}, FirstKey: 1, LastKey: -2, Step: 1},
	"CLIENT":   {Name: "client", Arity: -2, Flags: []string{redisCmdFlagAdmin}, FirstKey: 0, LastKey: 0, Step: 0},
	"COMMAND":  {Name: "command", Arity: -1, Flags: []string{redisCmdFlagAdmin}, FirstKey: 0, LastKey: 0, Step: 0},
	"DBSIZE":   {Name: "dbsize", Arity: 1, Flags: []string{redisCmdFlagReadonly}, FirstKey: 0, LastKey: 0, Step: 0},
	"DEL":      {Name: "del", Arity: -2, Flags: []string{redisCmdFlagWrite}, FirstKey: 1, LastKey: -1, Step: 1},
	"DISCARD":  {Name: "discard", Arity: 1, Flags: []string{redisCmdFlagAdmin}, FirstKey: 0, LastKey: 0, Step: 0},
	"EVAL":     {Name: "eval", Arity: -3, Flags: []string{redisCmdFlagWrite}, FirstKey: 0, LastKey: 0, Step: 0},
	"EVALSHA":  {Name: "evalsha", Arity: -3, Flags: []string{redisCmdFlagWrite}, FirstKey: 0, LastKey: 0, Step: 0},
	"EXEC":     {Name: "exec", Arity: 1, Flags: []string{redisCmdFlagAdmin}, FirstKey: 0, LastKey: 0, Step: 0},
	"EXISTS":   {Name: "exists", Arity: -2, Flags: []string{redisCmdFlagReadonly}, FirstKey: 1, LastKey: -1, Step: 1},
	"EXPIRE":   {Name: "expire", Arity: -3, Flags: []string{redisCmdFlagWrite}, FirstKey: 1, LastKey: 1, Step: 1},
	"FLUSHALL": {Name: "flushall", Arity: 1, Flags: []string{redisCmdFlagWrite}, FirstKey: 0, LastKey: 0, Step: 0},
	"FLUSHDB":  {Name: "flushdb", Arity: 1, Flags: []string{redisCmdFlagWrite}, FirstKey: 0, LastKey: 0, Step: 0},
	// FLUSHLEGACY is an elastickv-internal alias; mirror FLUSHDB metadata.
	"FLUSHLEGACY":      {Name: "flushlegacy", Arity: 1, Flags: []string{redisCmdFlagWrite}, FirstKey: 0, LastKey: 0, Step: 0},
	"GET":              {Name: "get", Arity: 2, Flags: []string{redisCmdFlagReadonly}, FirstKey: 1, LastKey: 1, Step: 1},
	"GETDEL":           {Name: "getdel", Arity: 2, Flags: []string{redisCmdFlagWrite}, FirstKey: 1, LastKey: 1, Step: 1},
	"HDEL":             {Name: "hdel", Arity: -3, Flags: []string{redisCmdFlagWrite}, FirstKey: 1, LastKey: 1, Step: 1},
	"HEXISTS":          {Name: "hexists", Arity: 3, Flags: []string{redisCmdFlagReadonly}, FirstKey: 1, LastKey: 1, Step: 1},
	"HGET":             {Name: "hget", Arity: 3, Flags: []string{redisCmdFlagReadonly}, FirstKey: 1, LastKey: 1, Step: 1},
	"HGETALL":          {Name: "hgetall", Arity: 2, Flags: []string{redisCmdFlagReadonly}, FirstKey: 1, LastKey: 1, Step: 1},
	"HINCRBY":          {Name: "hincrby", Arity: 4, Flags: []string{redisCmdFlagWrite}, FirstKey: 1, LastKey: 1, Step: 1},
	"HLEN":             {Name: "hlen", Arity: 2, Flags: []string{redisCmdFlagReadonly}, FirstKey: 1, LastKey: 1, Step: 1},
	"HMGET":            {Name: "hmget", Arity: -3, Flags: []string{redisCmdFlagReadonly}, FirstKey: 1, LastKey: 1, Step: 1},
	"HMSET":            {Name: "hmset", Arity: -4, Flags: []string{redisCmdFlagWrite}, FirstKey: 1, LastKey: 1, Step: 1},
	"HSET":             {Name: "hset", Arity: -4, Flags: []string{redisCmdFlagWrite}, FirstKey: 1, LastKey: 1, Step: 1},
	"INCR":             {Name: "incr", Arity: 2, Flags: []string{redisCmdFlagWrite}, FirstKey: 1, LastKey: 1, Step: 1},
	"INFO":             {Name: "info", Arity: -1, Flags: []string{redisCmdFlagAdmin}, FirstKey: 0, LastKey: 0, Step: 0},
	"KEYS":             {Name: "keys", Arity: 2, Flags: []string{redisCmdFlagReadonly}, FirstKey: 0, LastKey: 0, Step: 0},
	"LINDEX":           {Name: "lindex", Arity: 3, Flags: []string{redisCmdFlagReadonly}, FirstKey: 1, LastKey: 1, Step: 1},
	"LLEN":             {Name: "llen", Arity: 2, Flags: []string{redisCmdFlagReadonly}, FirstKey: 1, LastKey: 1, Step: 1},
	"LPOP":             {Name: "lpop", Arity: 2, Flags: []string{redisCmdFlagWrite}, FirstKey: 1, LastKey: 1, Step: 1},
	"LPOS":             {Name: "lpos", Arity: -3, Flags: []string{redisCmdFlagReadonly}, FirstKey: 1, LastKey: 1, Step: 1},
	"LPUSH":            {Name: "lpush", Arity: -3, Flags: []string{redisCmdFlagWrite}, FirstKey: 1, LastKey: 1, Step: 1},
	"LRANGE":           {Name: "lrange", Arity: 4, Flags: []string{redisCmdFlagReadonly}, FirstKey: 1, LastKey: 1, Step: 1},
	"LREM":             {Name: "lrem", Arity: 4, Flags: []string{redisCmdFlagWrite}, FirstKey: 1, LastKey: 1, Step: 1},
	"LSET":             {Name: "lset", Arity: 4, Flags: []string{redisCmdFlagWrite}, FirstKey: 1, LastKey: 1, Step: 1},
	"LTRIM":            {Name: "ltrim", Arity: 4, Flags: []string{redisCmdFlagWrite}, FirstKey: 1, LastKey: 1, Step: 1},
	"MULTI":            {Name: "multi", Arity: 1, Flags: []string{redisCmdFlagAdmin}, FirstKey: 0, LastKey: 0, Step: 0},
	"PEXPIRE":          {Name: "pexpire", Arity: -3, Flags: []string{redisCmdFlagWrite}, FirstKey: 1, LastKey: 1, Step: 1},
	"PFADD":            {Name: "pfadd", Arity: -3, Flags: []string{redisCmdFlagWrite}, FirstKey: 1, LastKey: 1, Step: 1},
	"PFCOUNT":          {Name: "pfcount", Arity: -2, Flags: []string{redisCmdFlagReadonly}, FirstKey: 1, LastKey: -1, Step: 1},
	"PING":             {Name: "ping", Arity: -1, Flags: []string{redisCmdFlagAdmin}, FirstKey: 0, LastKey: 0, Step: 0},
	"PTTL":             {Name: "pttl", Arity: 2, Flags: []string{redisCmdFlagReadonly}, FirstKey: 1, LastKey: 1, Step: 1},
	"PUBLISH":          {Name: "publish", Arity: 3, Flags: []string{redisCmdFlagAdmin}, FirstKey: 0, LastKey: 0, Step: 0},
	"PUBSUB":           {Name: "pubsub", Arity: -2, Flags: []string{redisCmdFlagAdmin}, FirstKey: 0, LastKey: 0, Step: 0},
	"QUIT":             {Name: "quit", Arity: 1, Flags: []string{redisCmdFlagAdmin}, FirstKey: 0, LastKey: 0, Step: 0},
	"RENAME":           {Name: "rename", Arity: 3, Flags: []string{redisCmdFlagWrite}, FirstKey: 1, LastKey: 2, Step: 1},
	"RPOP":             {Name: "rpop", Arity: 2, Flags: []string{redisCmdFlagWrite}, FirstKey: 1, LastKey: 1, Step: 1},
	"RPOPLPUSH":        {Name: "rpoplpush", Arity: 3, Flags: []string{redisCmdFlagWrite}, FirstKey: 1, LastKey: 2, Step: 1},
	"RPUSH":            {Name: "rpush", Arity: -3, Flags: []string{redisCmdFlagWrite}, FirstKey: 1, LastKey: 1, Step: 1},
	"SADD":             {Name: "sadd", Arity: -3, Flags: []string{redisCmdFlagWrite}, FirstKey: 1, LastKey: 1, Step: 1},
	"SCAN":             {Name: "scan", Arity: -2, Flags: []string{redisCmdFlagReadonly}, FirstKey: 0, LastKey: 0, Step: 0},
	"SCARD":            {Name: "scard", Arity: 2, Flags: []string{redisCmdFlagReadonly}, FirstKey: 1, LastKey: 1, Step: 1},
	"SELECT":           {Name: "select", Arity: 2, Flags: []string{redisCmdFlagAdmin}, FirstKey: 0, LastKey: 0, Step: 0},
	"SET":              {Name: "set", Arity: -3, Flags: []string{redisCmdFlagWrite}, FirstKey: 1, LastKey: 1, Step: 1},
	"SETEX":            {Name: "setex", Arity: 4, Flags: []string{redisCmdFlagWrite}, FirstKey: 1, LastKey: 1, Step: 1},
	"SETNX":            {Name: "setnx", Arity: 3, Flags: []string{redisCmdFlagWrite}, FirstKey: 1, LastKey: 1, Step: 1},
	"SISMEMBER":        {Name: "sismember", Arity: 3, Flags: []string{redisCmdFlagReadonly}, FirstKey: 1, LastKey: 1, Step: 1},
	"SMEMBERS":         {Name: "smembers", Arity: 2, Flags: []string{redisCmdFlagReadonly}, FirstKey: 1, LastKey: 1, Step: 1},
	"SREM":             {Name: "srem", Arity: -3, Flags: []string{redisCmdFlagWrite}, FirstKey: 1, LastKey: 1, Step: 1},
	"SUBSCRIBE":        {Name: "subscribe", Arity: -2, Flags: []string{redisCmdFlagAdmin}, FirstKey: 0, LastKey: 0, Step: 0},
	"TTL":              {Name: "ttl", Arity: 2, Flags: []string{redisCmdFlagReadonly}, FirstKey: 1, LastKey: 1, Step: 1},
	"TYPE":             {Name: "type", Arity: 2, Flags: []string{redisCmdFlagReadonly}, FirstKey: 1, LastKey: 1, Step: 1},
	"XADD":             {Name: "xadd", Arity: -5, Flags: []string{redisCmdFlagWrite}, FirstKey: 1, LastKey: 1, Step: 1},
	"XLEN":             {Name: "xlen", Arity: 2, Flags: []string{redisCmdFlagReadonly}, FirstKey: 1, LastKey: 1, Step: 1},
	"XRANGE":           {Name: "xrange", Arity: -4, Flags: []string{redisCmdFlagReadonly}, FirstKey: 1, LastKey: 1, Step: 1},
	"XREAD":            {Name: "xread", Arity: -4, Flags: []string{redisCmdFlagReadonly}, FirstKey: 0, LastKey: 0, Step: 0},
	"XREVRANGE":        {Name: "xrevrange", Arity: -4, Flags: []string{redisCmdFlagReadonly}, FirstKey: 1, LastKey: 1, Step: 1},
	"XTRIM":            {Name: "xtrim", Arity: -4, Flags: []string{redisCmdFlagWrite}, FirstKey: 1, LastKey: 1, Step: 1},
	"ZADD":             {Name: "zadd", Arity: -4, Flags: []string{redisCmdFlagWrite}, FirstKey: 1, LastKey: 1, Step: 1},
	"ZCARD":            {Name: "zcard", Arity: 2, Flags: []string{redisCmdFlagReadonly}, FirstKey: 1, LastKey: 1, Step: 1},
	"ZCOUNT":           {Name: "zcount", Arity: 4, Flags: []string{redisCmdFlagReadonly}, FirstKey: 1, LastKey: 1, Step: 1},
	"ZINCRBY":          {Name: "zincrby", Arity: 4, Flags: []string{redisCmdFlagWrite}, FirstKey: 1, LastKey: 1, Step: 1},
	"ZPOPMIN":          {Name: "zpopmin", Arity: -2, Flags: []string{redisCmdFlagWrite}, FirstKey: 1, LastKey: 1, Step: 1},
	"ZRANGE":           {Name: "zrange", Arity: -4, Flags: []string{redisCmdFlagReadonly}, FirstKey: 1, LastKey: 1, Step: 1},
	"ZRANGEBYSCORE":    {Name: "zrangebyscore", Arity: -4, Flags: []string{redisCmdFlagReadonly}, FirstKey: 1, LastKey: 1, Step: 1},
	"ZREM":             {Name: "zrem", Arity: -3, Flags: []string{redisCmdFlagWrite}, FirstKey: 1, LastKey: 1, Step: 1},
	"ZREMRANGEBYRANK":  {Name: "zremrangebyrank", Arity: 4, Flags: []string{redisCmdFlagWrite}, FirstKey: 1, LastKey: 1, Step: 1},
	"ZREMRANGEBYSCORE": {Name: "zremrangebyscore", Arity: 4, Flags: []string{redisCmdFlagWrite}, FirstKey: 1, LastKey: 1, Step: 1},
	"ZREVRANGE":        {Name: "zrevrange", Arity: -4, Flags: []string{redisCmdFlagReadonly}, FirstKey: 1, LastKey: 1, Step: 1},
	"ZREVRANGEBYSCORE": {Name: "zrevrangebyscore", Arity: -4, Flags: []string{redisCmdFlagReadonly}, FirstKey: 1, LastKey: 1, Step: 1},
	"ZSCORE":           {Name: "zscore", Arity: 3, Flags: []string{redisCmdFlagReadonly}, FirstKey: 1, LastKey: 1, Step: 1},
}

// redisCommandFallbackWarnedOnce deduplicates the "missing metadata" log so
// that a hostile or buggy client probing the same unknown-but-routed name
// cannot generate unbounded log spam. The fallback is a safety net for
// commands that get added to the route but where the table row is
// forgotten; the unit test `TestCommand_RouteMatchesTable` is the hard
// gate, but in production we prefer a degraded reply + one log line over
// a silently-missing command.
var (
	redisCommandFallbackWarnedOnceMu sync.Mutex
	redisCommandFallbackWarnedOnce   = map[string]struct{}{}
)

// routedRedisCommandMetas returns the metadata rows for every command
// currently routed (keyed via argsLen, which is populated 1:1 with the
// route map — see redis.go). Rows are returned in sorted UPPER-case order
// so wire output is deterministic. Names present in redisCommandTable
// produce their real row; names absent from the table but routed produce
// a zero-metadata row and a one-shot log warning. This is the source of
// truth for `COMMAND` (no args) and `COMMAND LIST`; `COMMAND INFO <name>`
// goes through redisCommandTable directly so unknowns produce the nil
// reply required by Redis semantics.
func routedRedisCommandMetas() []redisCommandMeta {
	names := make([]string, 0, len(argsLen))
	for name := range argsLen {
		names = append(names, strings.ToUpper(name))
	}
	sort.Strings(names)
	metas := make([]redisCommandMeta, 0, len(names))
	for _, name := range names {
		if meta, ok := redisCommandTable[name]; ok {
			metas = append(metas, meta)
			continue
		}
		warnMissingRedisCommandMeta(name)
		metas = append(metas, redisCommandMeta{
			Name:     strings.ToLower(name),
			Arity:    -1,
			Flags:    nil,
			FirstKey: 0,
			LastKey:  0,
			Step:     0,
		})
	}
	return metas
}

// warnMissingRedisCommandMeta emits a one-shot warning when a routed
// command has no entry in redisCommandTable. Subsequent calls for the
// same name are silent so a hot dispatch path does not produce log spam.
func warnMissingRedisCommandMeta(upper string) {
	redisCommandFallbackWarnedOnceMu.Lock()
	_, warned := redisCommandFallbackWarnedOnce[upper]
	if !warned {
		redisCommandFallbackWarnedOnce[upper] = struct{}{}
	}
	redisCommandFallbackWarnedOnceMu.Unlock()
	if !warned {
		log.Printf("redis-command: routed command %q has no entry in redisCommandTable; emitting zero-metadata fallback. Add a row to adapter/redis_command_info.go.", upper)
	}
}

// redisCommandGetKeys extracts the key positions from a full command-form
// argv (argv[0] is the command name, argv[1:] are its arguments).
// Returns an error when the command is unknown; returns an empty slice when
// the command is routed but has no keys.
//
// Semantics mirror Redis's own COMMAND GETKEYS:
//   - first_key=0: no keys (empty slice).
//   - last_key=-1: "all args after first_key are keys". step controls spacing
//     (step=1 → every arg; step=2 → every other arg, as in MSET).
//   - last_key=-N (N>1): last key index is len(argv)-N. Commands like
//     BZPOPMIN use -2 to exclude a trailing timeout arg that is NOT a key;
//     treating every negative as "to end" would wrongly expose the timeout
//     via COMMAND GETKEYS and break client key-routing decisions.
//   - otherwise: args in [first_key .. last_key] at `step` stride.
//
// This is *positional*. It does not understand option prefixes (e.g. the
// `EX`/`PX` flags of SET); clients that need option-aware parsing would
// look at Redis 7's key-specs shape, which we explicitly do not emit. For
// the commands elastickv supports the naive positional scheme is correct.
func redisCommandGetKeys(meta redisCommandMeta, argv [][]byte) [][]byte {
	if meta.FirstKey <= 0 {
		return nil
	}
	if meta.Step <= 0 {
		return nil
	}
	if meta.FirstKey >= len(argv) {
		return nil
	}
	last := meta.LastKey
	if last < 0 {
		// Negative last_key is an offset from the end: -1 means the
		// final arg, -2 means the second-to-last, and so on. Use
		// len(argv)+last so BZPOPMIN (-2) excludes its trailing
		// timeout argument instead of claiming the timeout as a key.
		last = len(argv) + last
	}
	if last >= len(argv) {
		last = len(argv) - 1
	}
	if last < meta.FirstKey {
		return nil
	}
	keys := make([][]byte, 0, (last-meta.FirstKey)/meta.Step+1)
	for i := meta.FirstKey; i <= last; i += meta.Step {
		keys = append(keys, argv[i])
	}
	return keys
}
