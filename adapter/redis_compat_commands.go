package adapter

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log"
	"math"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/monitoring"
	"github.com/bootjp/elastickv/store"
	cockerrors "github.com/cockroachdb/errors"
	"github.com/tidwall/redcon"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	redisPairWidth      = 2
	redisTripletWidth   = 3
	pubsubPatternArgMin = 3
	pubsubFirstChannel  = 2
	// redisBlockWaitFallback is the safety-net poll interval that fires
	// in blocking-command wait loops (XREAD BLOCK, BZPOPMIN — and the
	// future BLPOP / BRPOP / BLMOVE) when no in-process write signal
	// arrives. The signal path covers all in-process XADD / ZADD /
	// ZINCRBY on the same node; the fallback covers paths that bypass
	// Signal (Lua flush, follower-applied entries — both addressed by
	// the FSM ApplyObserver follow-up tracked in
	// docs/design/2026_04_26_proposed_fsm_apply_observer.md).
	// 100 ms keeps the fallback CPU at roughly 1/10th of the prior
	// busy-poll, while bounding stale-poll latency to a value clients
	// already tolerate from network round-trips.
	redisBlockWaitFallback = 100 * time.Millisecond
	redisKeywordCount      = "COUNT"

	// setWideColOverhead is the number of extra elements reserved in a set
	// wide-column mutation slice beyond the per-member elements: one for the
	// metadata key and one for the legacy-blob deletion tombstone.
	setWideColOverhead = 2

	// wideColumnBulkScanThreshold is the minimum batch size at which a full
	// prefix scan is used to check field/member existence instead of one
	// ExistsAt per field. Below this threshold the per-key lookups are cheaper
	// because they avoid scanning an arbitrarily large collection.
	wideColumnBulkScanThreshold = 16
)

type xreadRequest struct {
	block    time.Duration
	count    int
	keys     [][]byte
	afterIDs []string
}

type xreadOptions struct {
	block        time.Duration
	count        int
	streamsIndex int
}

type xreadResult struct {
	key     []byte
	entries []redisStreamEntry
}

type xaddRequest struct {
	// maxLen is -1 when no MAXLEN clause was given, 0 for explicit MAXLEN 0,
	// or a positive value for MAXLEN <n>.
	maxLen int
	id     string
	fields []string
}

type zrangeOptions struct {
	withScores bool
	reverse    bool
}

type bzpopminResult struct {
	key   []byte
	entry redisZSetEntry
}

func (r *RedisServer) info(conn redcon.Conn, _ redcon.Command) {
	role := "slave"
	if r.coordinator != nil && r.coordinator.IsLeader() {
		role = "master"
	}

	leaderRedis := r.raftLeaderRedisAddr()

	conn.WriteBulkString(strings.Join([]string{
		"# Server",
		"redis_version:7.2.0",
		"loading:0",
		"role:" + role,
		"",
		"# Replication",
		"role:" + role,
		"raft_leader_redis:" + leaderRedis,
		"",
	}, "\r\n"))
}

// raftLeaderRedisAddr returns the Redis-protocol address of the current Raft
// leader as known by this node. When this node is itself the leader the
// server's own listen address is returned. An empty string is returned when
// the leader is not yet known or when the leader's Redis address is not
// configured in the leaderRedis map.
func (r *RedisServer) raftLeaderRedisAddr() string {
	if r.coordinator == nil {
		return ""
	}
	if r.coordinator.IsLeader() {
		return r.redisAddr
	}
	leader := r.coordinator.RaftLeader()
	if leader == "" {
		return ""
	}
	return r.leaderRedis[leader]
}

// SETEX key seconds value — equivalent to SET key value EX seconds
func (r *RedisServer) setex(conn redcon.Conn, cmd redcon.Command) {
	if r.proxyToLeader(conn, cmd, cmd.Args[1]) {
		return
	}
	seconds, err := strconv.ParseInt(string(cmd.Args[2]), 10, 64)
	if err != nil || seconds <= 0 {
		conn.WriteError("ERR invalid expire time in 'setex' command")
		return
	}
	ttl := time.Now().Add(time.Duration(seconds) * time.Second)

	ctx, cancel := context.WithTimeout(context.Background(), redisDispatchTimeout)
	defer cancel()
	if err := r.saveString(ctx, cmd.Args[1], cmd.Args[3], &ttl); err != nil {
		conn.WriteError(err.Error())
		return
	}
	conn.WriteString("OK")
}

// GETDEL key — get the value and delete the key atomically
func (r *RedisServer) getdel(conn redcon.Conn, cmd redcon.Command) {
	if r.proxyToLeader(conn, cmd, cmd.Args[1]) {
		return
	}
	key := cmd.Args[1]

	ctx, cancel := context.WithTimeout(context.Background(), redisDispatchTimeout)
	defer cancel()
	var v []byte
	err := r.retryRedisWrite(ctx, func() error {
		readTS := r.readTS()
		typ, err := r.keyTypeAt(context.Background(), key, readTS)
		if err != nil {
			return err
		}
		if typ == redisTypeNone {
			v = nil
			return nil
		}
		if typ != redisTypeString {
			return wrongTypeError()
		}
		raw, _, err := r.readRedisStringAt(key, readTS)
		if err != nil {
			// Key may have expired or been deleted between type check and read.
			v = nil
			return nil //nolint:nilerr // treat not-found/expired as nil value
		}
		elems, _, err := r.deleteLogicalKeyElems(ctx, key, readTS)
		if err != nil {
			return err
		}
		if err := r.dispatchElems(ctx, true, readTS, elems); err != nil {
			return err
		}
		v = raw
		return nil
	})
	if err != nil {
		conn.WriteError(err.Error())
		return
	}
	if v == nil {
		conn.WriteNull()
		return
	}
	conn.WriteBulk(v)
}

// SETNX key value — set if not exists, returns 1 on success, 0 on failure
func (r *RedisServer) setnx(conn redcon.Conn, cmd redcon.Command) {
	if r.proxyToLeader(conn, cmd, cmd.Args[1]) {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), redisDispatchTimeout)
	defer cancel()

	opts := redisSetOptions{missingCond: true}
	result, err := r.executeSet(ctx, cmd.Args[1], cmd.Args[2], opts)
	if err != nil {
		conn.WriteError(err.Error())
		return
	}
	if result.wroteNull {
		conn.WriteInt(0)
		return
	}
	conn.WriteInt(1)
}

// clientSubcommandArgCount is the total cmd.Args length (including
// CLIENT + subcommand) required by no-operand CLIENT subcommands
// like GETNAME / ID / INFO.
const clientSubcommandArgCount = 2

// checkClientArity verifies cmd.Args has exactly want elements and
// writes the standard Redis wrong-arity error otherwise. Returns
// true when the caller should stop handling (bad arity).
func checkClientArity(conn redcon.Conn, cmd redcon.Command, sub string, want int) bool {
	if len(cmd.Args) == want {
		return false
	}
	conn.WriteError("ERR wrong number of arguments for 'client|" + strings.ToLower(sub) + "' command")
	return true
}

// clientSetName handles CLIENT SETNAME. SETNAME is shared with
// HELLO's SETNAME clause; both write into the same connState.clientName
// slot so a client that uses HELLO SETNAME once and then queries
// CLIENT GETNAME gets the right answer without having to re-issue
// CLIENT SETNAME.
func clientSetName(conn redcon.Conn, cmd redcon.Command, state *connState) {
	if checkClientArity(conn, cmd, "SETNAME", clientSetNameArgCount) {
		return
	}
	state.clientName = string(cmd.Args[2])
	conn.WriteString("OK")
}

func clientGetName(conn redcon.Conn, cmd redcon.Command, state *connState) {
	if checkClientArity(conn, cmd, "GETNAME", clientSubcommandArgCount) {
		return
	}
	if state.clientName == "" {
		conn.WriteNull()
		return
	}
	conn.WriteBulkString(state.clientName)
}

func (r *RedisServer) clientID(conn redcon.Conn, cmd redcon.Command, state *connState) {
	if checkClientArity(conn, cmd, "ID", clientSubcommandArgCount) {
		return
	}
	conn.WriteInt64(int64(r.ensureConnID(state))) //nolint:gosec // connID monotonic counter, guaranteed <= math.MaxInt64 in practice
}

func (r *RedisServer) clientInfo(conn redcon.Conn, cmd redcon.Command, state *connState) {
	if checkClientArity(conn, cmd, "INFO", clientSubcommandArgCount) {
		return
	}
	id := r.ensureConnID(state)
	conn.WriteBulkString(fmt.Sprintf("id=%d addr=%s name=%s", id, conn.RemoteAddr(), state.clientName))
}

// clientSetInfo handles CLIENT SETINFO <attr> <value>. elastickv does
// not persist the advertised attributes (lib-name / lib-ver, etc.), but
// it MUST still enforce exact arity — otherwise `CLIENT SETINFO` with
// no operands returns OK and masks a client bug that real Redis would
// have surfaced as a wrong-arity error.
func clientSetInfo(conn redcon.Conn, cmd redcon.Command) {
	if checkClientArity(conn, cmd, "SETINFO", clientSetInfoArgCount) {
		return
	}
	conn.WriteString("OK")
}

func (r *RedisServer) client(conn redcon.Conn, cmd redcon.Command) {
	sub := strings.ToUpper(string(cmd.Args[1]))
	state := getConnState(conn)
	switch sub {
	case "SETINFO":
		clientSetInfo(conn, cmd)
	case "SETNAME":
		clientSetName(conn, cmd, state)
	case "GETNAME":
		clientGetName(conn, cmd, state)
	case "ID":
		r.clientID(conn, cmd, state)
	case "INFO":
		r.clientInfo(conn, cmd, state)
	default:
		conn.WriteError("ERR unsupported CLIENT subcommand '" + sub + "'")
	}
}

// command implements the Redis `COMMAND` family used by clients for
// capability probing at connect time (go-redis, redis-py, ioredis, …).
// Subcommand matrix:
//
//	COMMAND                    -> array of per-command info
//	COMMAND COUNT              -> integer
//	COMMAND LIST               -> array of names (FILTERBY rejected)
//	COMMAND INFO [name ...]    -> array of per-command info (nil per unknown)
//	COMMAND DOCS [name ...]    -> minimal map-shaped doc entries
//	COMMAND GETKEYS cmd args   -> array of extracted keys
//	COMMAND GETKEYSANDFLAGS    -> ERR unsupported
func (r *RedisServer) command(conn redcon.Conn, cmd redcon.Command) {
	if len(cmd.Args) == 1 {
		r.writeCommandInfoAll(conn)
		return
	}
	sub := strings.ToUpper(string(cmd.Args[1]))
	switch sub {
	case "COUNT":
		// COUNT must match the cardinality of COMMAND / COMMAND LIST —
		// which iterate argsLen (= routed set). The table has the same
		// size by invariant, but driving COUNT off argsLen keeps the
		// three subcommands wire-consistent even during the brief
		// window when a new route has been added but the table row is
		// still pending.
		conn.WriteInt(len(argsLen))
	case "LIST":
		// `COMMAND LIST` takes no args (bare list) or `FILTERBY …` which we
		// reject below. Anything past the subcommand slot is a filter.
		const commandListArgFixed = 2
		if len(cmd.Args) > commandListArgFixed {
			// We explicitly do not support FILTERBY MODULE|ACLCAT|PATTERN
			// — elastickv has no modules and no ACL categories. Rejecting
			// here is consistent with how real Redis would behave when a
			// filter resolves to an empty universe; clients that see this
			// fall back to COMMAND (no args), which we support.
			conn.WriteError("ERR unsupported COMMAND LIST filter")
			return
		}
		r.writeCommandList(conn)
	case "INFO":
		r.writeCommandInfo(conn, cmd.Args[2:])
	case "DOCS":
		r.writeCommandDocs(conn, cmd.Args[2:])
	case "GETKEYS":
		r.writeCommandGetKeys(conn, cmd.Args[2:])
	case "GETKEYSANDFLAGS":
		conn.WriteError("ERR unsupported COMMAND subcommand 'GETKEYSANDFLAGS'")
	default:
		conn.WriteError("ERR Unknown COMMAND subcommand '" + sub + "'")
	}
}

// writeCommandInfoEntry emits the 6-element per-command info array for a
// single command. Redis 7 extends this to 10 elements; we deliberately
// stop at 6 because every client we care about parses the first 6 fields
// and ignores trailing elements.
func writeCommandInfoEntry(conn redcon.Conn, meta redisCommandMeta) {
	const infoArity = 6
	conn.WriteArray(infoArity)
	conn.WriteBulkString(meta.Name)
	conn.WriteInt(meta.Arity)
	conn.WriteArray(len(meta.Flags))
	for _, f := range meta.Flags {
		conn.WriteBulkString(f)
	}
	conn.WriteInt(meta.FirstKey)
	conn.WriteInt(meta.LastKey)
	conn.WriteInt(meta.Step)
}

func (r *RedisServer) writeCommandInfoAll(conn redcon.Conn) {
	metas := routedRedisCommandMetas()
	conn.WriteArray(len(metas))
	for _, meta := range metas {
		writeCommandInfoEntry(conn, meta)
	}
}

func (r *RedisServer) writeCommandList(conn redcon.Conn) {
	metas := routedRedisCommandMetas()
	conn.WriteArray(len(metas))
	for _, meta := range metas {
		conn.WriteBulkString(meta.Name)
	}
}

func (r *RedisServer) writeCommandInfo(conn redcon.Conn, requested [][]byte) {
	// `COMMAND INFO` with no names is equivalent to `COMMAND` (no args):
	// return info for every known command. This is what real Redis does
	// and what go-redis relies on when it issues bare `COMMAND INFO`.
	if len(requested) == 0 {
		r.writeCommandInfoAll(conn)
		return
	}
	conn.WriteArray(len(requested))
	for _, raw := range requested {
		meta, ok := redisCommandTable[strings.ToUpper(string(raw))]
		if !ok {
			conn.WriteNull()
			continue
		}
		writeCommandInfoEntry(conn, meta)
	}
}

// writeCommandDocs emits the RESP2 flat-map form of COMMAND DOCS:
// alternating command-name keys and 4-element doc-maps with "summary"
// and "arguments" fields. Two compliance-critical behaviours:
//
//  1. Bare `COMMAND DOCS` (no names) returns docs for ALL routed
//     commands, identical to how `COMMAND INFO` and bare `COMMAND`
//     behave. Clients/tools like redis-cli --docs rely on this.
//  2. Every requested entry writes BOTH the command-name key AND the
//     doc map value. Clients decode the top-level array as a map of
//     name -> docs, so skipping the name key makes the reply
//     unparseable. Unknown commands emit the requested name followed
//     by nil (Redis semantics).
//
// We do not maintain per-command docs, so summary is "" and arguments
// is empty. The wire-shape is what clients care about at connect time.
func (r *RedisServer) writeCommandDocs(conn redcon.Conn, requested [][]byte) {
	const docEntryLen = 4
	// Bare DOCS (no command names): iterate the routed set so the
	// reply mirrors `COMMAND` / `COMMAND INFO` / `COMMAND LIST`.
	if len(requested) == 0 {
		metas := routedRedisCommandMetas()
		// Two wire slots per command (name + doc map).
		conn.WriteArray(len(metas) * 2) //nolint:mnd // 2 = (name, docs) pair
		for _, meta := range metas {
			conn.WriteBulkString(meta.Name)
			conn.WriteArray(docEntryLen)
			conn.WriteBulkString("summary")
			conn.WriteBulkString("")
			conn.WriteBulkString("arguments")
			conn.WriteArray(0)
		}
		return
	}
	// Explicit names: preserve the caller-supplied order so a client
	// that expects its own request ordering back (e.g. for building a
	// lookup table) is not surprised. Each pair is (name, docs) or
	// (name, nil) for unknowns.
	conn.WriteArray(len(requested) * 2) //nolint:mnd // 2 = (name, docs) pair
	for _, raw := range requested {
		name := string(raw)
		meta, ok := redisCommandTable[strings.ToUpper(name)]
		if !ok {
			conn.WriteBulkString(name)
			conn.WriteNull()
			continue
		}
		conn.WriteBulkString(meta.Name)
		conn.WriteArray(docEntryLen)
		conn.WriteBulkString("summary")
		conn.WriteBulkString("")
		conn.WriteBulkString("arguments")
		conn.WriteArray(0)
	}
}

// writeCommandGetKeys dispatches COMMAND GETKEYS for a given subcommand
// plus its arguments. Real Redis requires at least one arg after GETKEYS
// (the command name itself); we enforce that here rather than lean on
// argsLen which only validates the outer COMMAND call.
func (r *RedisServer) writeCommandGetKeys(conn redcon.Conn, argv [][]byte) {
	if len(argv) == 0 {
		conn.WriteError("ERR wrong number of arguments for 'command|getkeys' command")
		return
	}
	meta, ok := redisCommandTable[strings.ToUpper(string(argv[0]))]
	if !ok {
		conn.WriteError("ERR Invalid command specified")
		return
	}
	// validate arity of the nested command so we match Redis behaviour of
	// refusing to compute keys for obviously malformed commands (a common
	// source of confusion in client test suites).
	switch {
	case meta.Arity > 0 && len(argv) != meta.Arity:
		conn.WriteError("ERR Invalid arguments specified for populating the array of keys")
		return
	case meta.Arity < 0 && len(argv) < -meta.Arity:
		conn.WriteError("ERR Invalid arguments specified for populating the array of keys")
		return
	}
	keys := redisCommandGetKeys(meta, argv)
	if len(keys) == 0 {
		// `The command has no key arguments` — real Redis returns an error
		// in this case rather than an empty array, and go-redis's test
		// suite expects the error form.
		conn.WriteError("ERR The command has no key arguments")
		return
	}
	conn.WriteArray(len(keys))
	for _, k := range keys {
		conn.WriteBulk(k)
	}
}

// HELLO reply and protocol constants. Kept as named constants so the
// linter's "no magic numbers" rule accepts the wire-format values.
const (
	// helloReplyVersion is the version string returned by HELLO's
	// `version` field. Clients like ioredis and jedis perform loose
	// version checks against this field; returning a recent Redis
	// version string keeps maximum compatibility. Intentionally not
	// elastickv's own version — clients that fail to parse a
	// non-Redis version string would treat elastickv as an
	// unsupported backend.
	helloReplyVersion = "7.0.0"
	// helloReplyProto is the RESP protocol version advertised by the
	// server. elastickv is RESP2-only because redcon exposes no RESP3
	// map-reply API.
	helloReplyProto = 2
	// helloReplyArrayLen is the number of elements in the flat
	// alternating key/value reply: 7 pairs = 14 elements.
	helloReplyArrayLen = 14
	// clientSetNameArgCount is the EXACT cmd.Args length for CLIENT
	// SETNAME (CLIENT + SETNAME + name = 3). Kept as an exact-arity
	// constant — not a minimum — because checkClientArity compares
	// `len(cmd.Args) == want`; renaming it to *MinArgs would invite a
	// future refactor that "just" swaps the helper for a >= check and
	// silently re-introduces the wrong-arity silent-OK bug.
	clientSetNameArgCount = 3
	// clientSetInfoArgCount is the EXACT cmd.Args length for CLIENT
	// SETINFO (CLIENT + SETINFO + attr + value = 4). Real Redis
	// rejects any other arity for `client|setinfo`; without this
	// check we would keep returning OK for `CLIENT SETINFO` with no
	// operands, matching exactly the silent-success behaviour this
	// PR is supposed to eliminate for every CLIENT subcommand.
	clientSetInfoArgCount = 4
)

// helloParseError is the internal signal used by parseHelloArgs to
// surface a client-facing error without forcing the top-level hello
// handler to pay for additional branches. The caller writes err to
// the wire verbatim.
type helloParseError struct{ msg string }

func (e *helloParseError) Error() string { return e.msg }

// parseHelloArgs walks the optional HELLO argument list and mutates
// connState for any recognized options. Returns a non-nil error
// containing the exact wire-format string to emit via WriteError.
// Split out of hello() so the handler's cyclomatic complexity stays
// within the linter's budget.
// parsedHelloOption is the pure-function result of a single option
// token. advance is the number of input args consumed. Exactly one
// of (advance > 0) or (err != nil) is non-zero.
type parsedHelloOption struct {
	name    string
	hasName bool
	advance int
}

const (
	// helloAuthOptionArity is the total token count a HELLO AUTH
	// clause consumes: keyword + username + password.
	helloAuthOptionArity = 3
	// helloSetNameOptionArity is keyword + name.
	helloSetNameOptionArity = 2
	// streamZeroID is the canonical "empty stream" / "smallest possible ID"
	// sentinel used by XREAD '$' on an empty or missing stream.
	streamZeroID = "0-0"
)

// parseHelloOption decodes one HELLO option starting at args[0] (the
// option keyword). Returns how many input tokens the option consumed
// and any client-side staging it wants applied.
func parseHelloOption(args [][]byte) (parsedHelloOption, error) {
	opt := strings.ToUpper(string(args[0]))
	switch opt {
	case "AUTH":
		if len(args) < helloAuthOptionArity {
			return parsedHelloOption{}, &helloParseError{msg: "ERR Syntax error in HELLO AUTH"}
		}
		// elastickv's Redis adapter has no AUTH layer. Rejecting rather
		// than silently accepting keeps operators honest.
		return parsedHelloOption{}, &helloParseError{msg: "NOPERM HELLO AUTH is not supported"}
	case "SETNAME":
		if len(args) < helloSetNameOptionArity {
			return parsedHelloOption{}, &helloParseError{msg: "ERR Syntax error in HELLO SETNAME"}
		}
		return parsedHelloOption{
			name:    string(args[1]),
			hasName: true,
			advance: helloSetNameOptionArity,
		}, nil
	default:
		return parsedHelloOption{}, &helloParseError{msg: "ERR Syntax error in HELLO option '" + opt + "'"}
	}
}

func parseHelloArgs(state *connState, args [][]byte) error {
	if len(args) == 0 {
		return nil
	}
	protover, err := strconv.Atoi(string(args[0]))
	if err != nil || protover != helloReplyProto {
		// Non-numeric, RESP3 (3), or any other requested version:
		// reject with NOPROTO so well-behaved clients fall back to
		// RESP2.
		return &helloParseError{msg: "NOPROTO unsupported protocol version"}
	}
	// Buffer side effects locally so a partial parse (e.g. SETNAME
	// followed by a bad option or AUTH) leaves connState untouched —
	// the command must be all-or-nothing, matching real Redis.
	var (
		pendingName    string
		pendingNameSet bool
	)
	for i := 1; i < len(args); {
		opt, err := parseHelloOption(args[i:])
		if err != nil {
			return err
		}
		if opt.hasName {
			pendingName = opt.name
			pendingNameSet = true
		}
		i += opt.advance
	}
	if pendingNameSet {
		state.clientName = pendingName
	}
	return nil
}

// hello implements the Redis HELLO command. Syntax:
//
//	HELLO [protover [AUTH username password] [SETNAME clientname]]
//
// elastickv speaks RESP2 only (redcon is RESP2-only and exposes no
// RESP3 map-reply API), so:
//
//   - No protover, or protover == 2: succeed and return the server-info
//     array.
//   - protover == 3 or any other non-2 value: reply with the
//     NOPROTO error the real Redis server uses when a client requests
//     an unsupported protocol version. go-redis and friends fall back
//     to RESP2 when they see this.
//   - AUTH is rejected because elastickv has no auth layer wired into
//     the Redis adapter; silently accepting any credentials would be a
//     security footgun for operators who assume AUTH means something.
//     We return a NOPERM-style error so clients surface a clear error
//     rather than assuming auth succeeded.
//   - SETNAME is wired into the existing connState.clientName slot, so
//     a subsequent CLIENT GETNAME observes the name set here.
func (r *RedisServer) hello(conn redcon.Conn, cmd redcon.Command) {
	state := getConnState(conn)
	if err := parseHelloArgs(state, cmd.Args[1:]); err != nil {
		conn.WriteError(err.Error())
		return
	}

	role := "slave"
	if r.coordinator != nil && r.coordinator.IsLeader() {
		role = "master"
	}
	id := r.ensureConnID(state)

	// Reply as a flat RESP2 array of alternating key/value pairs, the
	// same wire shape Redis uses when a client negotiates RESP2 via
	// HELLO. Order matches real Redis so clients that parse
	// positionally (jedis has done this historically) still work.
	conn.WriteArray(helloReplyArrayLen)
	conn.WriteBulkString("server")
	conn.WriteBulkString("redis")
	conn.WriteBulkString("version")
	conn.WriteBulkString(helloReplyVersion)
	conn.WriteBulkString("proto")
	conn.WriteInt(helloReplyProto)
	conn.WriteBulkString("id")
	conn.WriteInt64(int64(id)) //nolint:gosec // connID monotonic counter, fits in int64 in practice.
	conn.WriteBulkString("mode")
	conn.WriteBulkString("standalone")
	conn.WriteBulkString("role")
	conn.WriteBulkString(role)
	conn.WriteBulkString("modules")
	conn.WriteArray(0)
}

func (r *RedisServer) selectDB(conn redcon.Conn, cmd redcon.Command) {
	if _, err := strconv.Atoi(string(cmd.Args[1])); err != nil {
		conn.WriteError("ERR invalid DB index")
		return
	}
	conn.WriteString("OK")
}

func (r *RedisServer) quit(conn redcon.Conn, _ redcon.Command) {
	conn.WriteString("OK")
	_ = conn.Close()
}

func (r *RedisServer) typeCmd(conn redcon.Conn, cmd redcon.Command) {
	if r.proxyToLeader(conn, cmd, cmd.Args[1]) {
		return
	}
	typ, err := r.keyType(context.Background(), cmd.Args[1])
	if err != nil {
		conn.WriteError(err.Error())
		return
	}
	conn.WriteString(string(typ))
}

func (r *RedisServer) ttl(conn redcon.Conn, cmd redcon.Command) {
	if r.proxyToLeader(conn, cmd, cmd.Args[1]) {
		return
	}
	r.writeTTL(conn, cmd.Args[1], false)
}

func (r *RedisServer) pttl(conn redcon.Conn, cmd redcon.Command) {
	if r.proxyToLeader(conn, cmd, cmd.Args[1]) {
		return
	}
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
	if r.proxyToLeader(conn, cmd, cmd.Args[1]) {
		return
	}
	r.setExpire(conn, cmd, time.Second)
}

func (r *RedisServer) pexpire(conn redcon.Conn, cmd redcon.Command) {
	if r.proxyToLeader(conn, cmd, cmd.Args[1]) {
		return
	}
	r.setExpire(conn, cmd, time.Millisecond)
}

func parseExpireNXOnly(args [][]byte) (bool, error) {
	nxOnly := false
	for _, arg := range args {
		if !strings.EqualFold(string(arg), "NX") {
			return false, errors.New("ERR syntax error")
		}
		nxOnly = true
	}
	return nxOnly, nil
}

func hasActiveTTL(ttl *time.Time, now time.Time) bool {
	return ttl != nil && ttl.After(now)
}

func parseExpireTTL(raw []byte) (int64, error) {
	ttl, err := strconv.ParseInt(string(raw), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parse expire ttl: %w", err)
	}
	return ttl, nil
}

func (r *RedisServer) prepareExpire(key []byte, nxOnly bool) (uint64, bool, error) {
	readTS := r.readTS()
	exists, err := r.logicalExistsAt(context.Background(), key, readTS)
	if err != nil {
		return 0, false, err
	}
	if !exists {
		return readTS, false, nil
	}

	if !nxOnly {
		return readTS, true, nil
	}

	currentTTL, err := r.ttlAt(context.Background(), key, readTS)
	if err != nil {
		return 0, false, err
	}
	return readTS, !hasActiveTTL(currentTTL, time.Now()), nil
}

func (r *RedisServer) setExpire(conn redcon.Conn, cmd redcon.Command, unit time.Duration) {
	ttl, err := parseExpireTTL(cmd.Args[2])
	if err != nil {
		conn.WriteError(err.Error())
		return
	}

	nxOnly, err := parseExpireNXOnly(cmd.Args[3:])
	if err != nil {
		conn.WriteError(err.Error())
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), redisDispatchTimeout)
	defer cancel()

	// Pin expireAt once before the retry loop so successive attempts all write
	// the same wall-clock deadline (OCC retries must not push expiry forward).
	var expireAt time.Time
	if ttl > 0 {
		if ttl > math.MaxInt64/int64(unit) {
			conn.WriteError("ERR invalid expire time in command")
			return
		}
		expireAt = time.Now().Add(time.Duration(ttl) * unit)
	}

	var result int
	if err := r.retryRedisWrite(ctx, func() error {
		var retErr error
		result, retErr = r.doSetExpire(ctx, cmd.Args[1], ttl, expireAt, nxOnly)
		return retErr
	}); err != nil {
		conn.WriteError(err.Error())
		return
	}
	conn.WriteInt(result)
}

// doSetExpire is the inner body of setExpire's retryRedisWrite loop.
// All reads (existence, type, value) use the same readTS snapshot so they form
// a consistent view. The subsequent dispatchElems calls use IsTxn=true with
// StartTS=readTS, which causes coordinator.Dispatch to reject the write with
// ErrWriteConflict if any touched key was modified after readTS. retryRedisWrite
// then re-invokes doSetExpire with a fresh readTS, providing OCC safety without
// an explicit mutex. Leadership is verified by coordinator.Dispatch itself.
func (r *RedisServer) doSetExpire(ctx context.Context, key []byte, ttl int64, expireAt time.Time, nxOnly bool) (int, error) {
	readTS, eligible, err := r.prepareExpire(key, nxOnly)
	if err != nil {
		return 0, err
	}
	if !eligible {
		return 0, nil
	}
	if ttl <= 0 {
		return r.expireDeleteKey(ctx, key, readTS)
	}
	typ, err := r.rawKeyTypeAt(ctx, key, readTS)
	if err != nil {
		return 0, err
	}
	if typ == redisTypeString {
		// rawKeyTypeAt also reports HLL as redisTypeString; HLL payloads live
		// under !redis|hll|<key> and don't carry an inline TTL, so fall back
		// to the legacy scan-index path for them.
		plain, err := r.isPlainRedisString(ctx, key, readTS)
		if err != nil {
			return 0, err
		}
		if plain {
			applied, err := r.dispatchStringExpire(ctx, key, readTS, expireAt)
			if err != nil || !applied {
				return 0, err
			}
			return 1, nil
		}
	}
	elems := []*kv.Elem[kv.OP]{{Op: kv.Put, Key: redisTTLKey(key), Value: encodeRedisTTL(expireAt)}}
	return 1, r.dispatchElems(ctx, true, readTS, elems)
}

// isPlainRedisString distinguishes a plain Redis string (stored under
// !redis|str|<key> or, for legacy data, the bare key) from a HyperLogLog
// (stored under !redis|hll|<key>), both of which rawKeyTypeAt reports as
// redisTypeString.
func (r *RedisServer) isPlainRedisString(ctx context.Context, key []byte, readTS uint64) (bool, error) {
	exists, err := r.store.ExistsAt(ctx, redisStrKey(key), readTS)
	if err != nil {
		return false, cockerrors.WithStack(err)
	}
	if exists {
		return true, nil
	}
	// Fall back to the bare legacy layout.
	legacy, err := r.store.ExistsAt(ctx, key, readTS)
	if err != nil {
		return false, cockerrors.WithStack(err)
	}
	return legacy, nil
}

func (r *RedisServer) expireDeleteKey(ctx context.Context, key []byte, readTS uint64) (int, error) {
	elems, existed, err := r.deleteLogicalKeyElems(ctx, key, readTS)
	if err != nil {
		return 0, err
	}
	if err := r.dispatchElems(ctx, true, readTS, elems); err != nil {
		return 0, err
	}
	if existed {
		return 1, nil
	}
	return 0, nil
}

// dispatchStringExpire performs a read-modify-write on the string anchor key:
// it reads the current value at readTS, re-encodes it with the new expiry, and
// writes both the updated value and the !redis|ttl| scan index in a single Raft
// entry (IsTxn=true, StartTS=readTS). The coordinator rejects the write with
// ErrWriteConflict if any key was modified after readTS, so stale-data safety is
// guaranteed by OCC — no explicit mutex is required.
func (r *RedisServer) dispatchStringExpire(ctx context.Context, key []byte, readTS uint64, expireAt time.Time) (bool, error) {
	userValue, _, readErr := r.readRedisStringAt(key, readTS)
	if readErr != nil {
		if cockerrors.Is(readErr, store.ErrKeyNotFound) {
			// Raced with a delete/expiry between prepareExpire and this read;
			// do not resurrect the key with an empty anchor.
			return false, nil
		}
		return false, cockerrors.WithStack(readErr)
	}
	encoded := encodeRedisStr(userValue, &expireAt)
	elems := []*kv.Elem[kv.OP]{
		{Op: kv.Put, Key: redisStrKey(key), Value: encoded},
		{Op: kv.Put, Key: redisTTLKey(key), Value: encodeRedisTTL(expireAt)},
	}
	return true, r.dispatchElems(ctx, true, readTS, elems)
}

func parseScanArgs(args [][]byte) (int, []byte, int, error) {
	cursor, err := strconv.Atoi(string(args[1]))
	if err != nil || cursor < 0 {
		return 0, nil, 0, errors.New("ERR invalid cursor")
	}

	pattern := []byte("*")
	count := 10
	for i := redisPairWidth; i < len(args); i += redisPairWidth {
		if i+1 >= len(args) {
			return 0, nil, 0, errors.New("ERR syntax error")
		}
		switch strings.ToUpper(string(args[i])) {
		case "MATCH":
			pattern = args[i+1]
		case redisKeywordCount:
			count, err = strconv.Atoi(string(args[i+1]))
			if err != nil || count <= 0 {
				return 0, nil, 0, errors.New("ERR syntax error")
			}
		default:
			return 0, nil, 0, errors.New("ERR syntax error")
		}
	}
	return cursor, pattern, count, nil
}

func writeScanReply(conn redcon.Conn, next int, keys [][]byte) {
	conn.WriteArray(redisPairWidth)
	conn.WriteBulkString(strconv.Itoa(next))
	conn.WriteArray(len(keys))
	for _, key := range keys {
		conn.WriteBulk(key)
	}
}

func (r *RedisServer) scan(conn redcon.Conn, cmd redcon.Command) {
	cursor, pattern, count, err := parseScanArgs(cmd.Args)
	if err != nil {
		conn.WriteError(err.Error())
		return
	}

	keys, err := r.visibleKeys(pattern)
	if err != nil {
		conn.WriteError(err.Error())
		return
	}
	if cursor >= len(keys) {
		writeScanReply(conn, 0, nil)
		return
	}

	end := minRedisInt(cursor+count, len(keys))
	next := 0
	if end < len(keys) {
		next = end
	}

	writeScanReply(conn, next, keys[cursor:end])
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
		r.pubsub.Subscribe(conn, string(channel))
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

// deleteLegacyKeys scans the full keyspace and deletes keys that do not belong
// to any known internal prefix. Returns the number of user-visible legacy keys
// deleted. TTL keys are intentionally NOT deleted because the !redis|ttl|
// namespace is shared across all Redis types — deleting them could strip
// expiration from already-migrated or newly-created keys.
func (r *RedisServer) deleteLegacyKeys(ctx context.Context, readTS uint64) (int, error) {
	const batchSize = 1000
	var totalDeleted int
	cursor := make([]byte, 0, batchSize)
	for {
		kvs, err := r.store.ScanAt(ctx, cursor, nil, batchSize, readTS)
		if err != nil {
			return totalDeleted, fmt.Errorf("scan: %w", err)
		}
		if len(kvs) == 0 {
			break
		}

		elems := make([]*kv.Elem[kv.OP], 0, len(kvs))
		legacyCount := 0
		for _, pair := range kvs {
			if !isKnownInternalKey(pair.Key) {
				legacyCount++
				elems = append(elems, &kv.Elem[kv.OP]{Op: kv.Del, Key: pair.Key})
			}
		}

		if len(elems) > 0 {
			if err := r.dispatchElems(ctx, false, readTS, elems); err != nil {
				return totalDeleted, err
			}
			totalDeleted += legacyCount
		}

		// Advance cursor past the last key in this batch.
		lastKey := kvs[len(kvs)-1].Key
		cursor = make([]byte, len(lastKey)+1)
		copy(cursor, lastKey)

		// Yield briefly between batches to avoid saturating the Raft log.
		time.Sleep(time.Millisecond)
	}
	return totalDeleted, nil
}

// flushlegacy deletes old unprefixed Redis string keys that were written before
// the !redis|str| prefix migration. It scans all keys and deletes those that
// do not match any known internal prefix. This is a one-time migration operation.
func (r *RedisServer) flushlegacy(conn redcon.Conn, _ redcon.Command) {
	if !r.coordinator.IsLeader() {
		n, err := r.proxyFlushLegacy()
		if err != nil {
			conn.WriteError(err.Error())
			return
		}
		conn.WriteInt(n)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), redisFlushLegacyTimeout)
	defer cancel()

	totalDeleted, err := r.deleteLegacyKeys(ctx, r.readTS())
	if err != nil {
		conn.WriteError(err.Error())
		return
	}
	conn.WriteInt(totalDeleted)
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
			return fmt.Errorf("verify leader: %w", err)
		}

		// Delete only Redis-related keys. Each DEL_PREFIX operation must be
		// dispatched separately because the FSM processes only one DEL_PREFIX
		// per request (the first mutation).
		//
		// Namespaces covered:
		//   "!redis|" — str, legacy hash/set/zset/hll/stream, ttl
		//   "!lst|"   — list meta + items
		//   "!zs|"    — zset wide-column
		//   "!hs|"    — hash wide-column meta/field/delta
		//   "!st|"    — set wide-column meta/member/delta
		//
		// Legacy bare keys are NOT deleted here to avoid a full keyspace
		// scan. Run FLUSHLEGACY first to clean up legacy data.
		//
		// All prefixes are attempted even if one dispatch fails so that we
		// delete as many namespaces as possible before reporting errors.
		var combined error
		for _, prefix := range [][]byte{
			[]byte("!redis|"),
			[]byte("!lst|"),
			[]byte("!zs|"),
			[]byte("!hs|"),
			[]byte("!st|"),
		} {
			if _, err := r.coordinator.Dispatch(ctx, &kv.OperationGroup[kv.OP]{
				Elems: []*kv.Elem[kv.OP]{
					{Op: kv.DelPrefix, Key: prefix},
				},
			}); err != nil {
				combined = cockerrors.CombineErrors(combined, fmt.Errorf("dispatch del_prefix %q: %w", prefix, err))
			}
		}
		return cockerrors.WithStack(combined)
	}); err != nil {
		conn.WriteError(err.Error())
		return
	}

	conn.WriteString("OK")
}

func (r *RedisServer) pubsubCmd(conn redcon.Conn, cmd redcon.Command) {
	switch strings.ToUpper(string(cmd.Args[1])) {
	case "CHANNELS":
		r.writePubSubChannels(conn, cmd.Args)
	case "NUMSUB":
		r.writePubSubNumSub(conn, cmd.Args)
	case "NUMPAT":
		conn.WriteInt(0)
	default:
		conn.WriteError("ERR unsupported PUBSUB subcommand '" + string(cmd.Args[1]) + "'")
	}
}

func (r *RedisServer) writePubSubChannels(conn redcon.Conn, args [][]byte) {
	pattern := []byte("*")
	if len(args) >= pubsubPatternArgMin {
		pattern = args[pubsubFirstChannel]
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
}

func (r *RedisServer) writePubSubNumSub(conn redcon.Conn, args [][]byte) {
	channels := args[pubsubFirstChannel:]
	snapshot := r.pubsubChannelCounts()

	conn.WriteArray(len(channels) * redisPairWidth)
	for _, channel := range channels {
		conn.WriteBulk(channel)
		conn.WriteInt(snapshot[string(channel)])
	}
}

func (r *RedisServer) pubsubChannelCounts() map[string]int {
	return r.pubsub.ChannelCounts()
}

func (r *RedisServer) sadd(conn redcon.Conn, cmd redcon.Command) {
	if r.proxyToLeader(conn, cmd, cmd.Args[1]) {
		return
	}
	r.mutateExactSet(conn, setKind, cmd.Args[1], cmd.Args[2:], true)
}

func (r *RedisServer) srem(conn redcon.Conn, cmd redcon.Command) {
	if r.proxyToLeader(conn, cmd, cmd.Args[1]) {
		return
	}
	r.mutateExactSet(conn, setKind, cmd.Args[1], cmd.Args[2:], false)
}

func (r *RedisServer) validateExactSetKind(kind string, key []byte, readTS uint64) error {
	typ, err := r.keyTypeAt(context.Background(), key, readTS)
	if err != nil {
		return err
	}

	switch kind {
	case setKind:
		return r.validateExactSetType(typ, key, readTS)
	case hllKind:
		return r.validateExactHLLType(typ, key, readTS)
	default:
		return errors.New("ERR unsupported exact set kind")
	}
}

func (r *RedisServer) hllExistsAt(key []byte, readTS uint64) (bool, error) {
	exists, err := r.store.ExistsAt(context.Background(), redisHLLKey(key), readTS)
	if err != nil {
		return false, fmt.Errorf("exists hll: %w", err)
	}
	return exists, nil
}

func (r *RedisServer) validateExactSetType(typ redisValueType, key []byte, readTS uint64) error {
	if typ == redisTypeSet {
		return nil
	}
	if typ != redisTypeNone {
		return wrongTypeError()
	}

	hllExists, err := r.hllExistsAt(key, readTS)
	if err != nil {
		return err
	}
	if hllExists {
		return wrongTypeError()
	}
	return nil
}

func (r *RedisServer) validateExactHLLType(typ redisValueType, key []byte, readTS uint64) error {
	if typ == redisTypeNone {
		return nil
	}

	hllExists, err := r.hllExistsAt(key, readTS)
	if err != nil {
		return err
	}
	if !hllExists {
		return wrongTypeError()
	}
	return nil
}

func exactSetMembers(value redisSetValue) map[string]struct{} {
	members := make(map[string]struct{}, len(value.Members))
	for _, member := range value.Members {
		members[member] = struct{}{}
	}
	return members
}

func applyExactSetMutation(existing map[string]struct{}, members [][]byte, add bool) int {
	changed := 0
	for _, member := range members {
		memberKey := string(member)
		_, ok := existing[memberKey]
		if add {
			if ok {
				continue
			}
			existing[memberKey] = struct{}{}
			changed++
			continue
		}
		if ok {
			delete(existing, memberKey)
			changed++
		}
	}
	return changed
}

func sortedExactSetMembers(existing map[string]struct{}) []string {
	out := make([]string, 0, len(existing))
	for member := range existing {
		out = append(out, member)
	}
	sort.Strings(out)
	return out
}

func (r *RedisServer) persistExactSetMembersTxn(ctx context.Context, kind string, key []byte, readTS uint64, members map[string]struct{}) error {
	if kind != setKind {
		// HLL and other non-set kinds keep using the legacy blob format.
		if len(members) == 0 {
			elems, _, err := r.deleteLogicalKeyElems(ctx, key, readTS)
			if err != nil {
				return err
			}
			return r.dispatchElems(ctx, true, readTS, elems)
		}
		payload, err := marshalSetValue(redisSetValue{Members: sortedExactSetMembers(members)})
		if err != nil {
			return err
		}
		return r.dispatchElems(ctx, true, readTS, []*kv.Elem[kv.OP]{
			{Op: kv.Put, Key: redisExactSetStorageKey(kind, key), Value: payload},
		})
	}
	// Wide-column set: full rewrite (used when the whole state is available).
	if len(members) == 0 {
		elems, _, err := r.deleteLogicalKeyElems(ctx, key, readTS)
		if err != nil {
			return err
		}
		return r.dispatchElems(ctx, true, readTS, elems)
	}
	elems := make([]*kv.Elem[kv.OP], 0, len(members)+setWideColOverhead)
	for member := range members {
		elems = append(elems, &kv.Elem[kv.OP]{
			Op:    kv.Put,
			Key:   store.SetMemberKey(key, []byte(member)),
			Value: []byte{},
		})
	}
	elems = append(elems, &kv.Elem[kv.OP]{
		Op:    kv.Put,
		Key:   store.SetMetaKey(key),
		Value: store.MarshalSetMeta(store.SetMeta{Len: int64(len(members))}),
	})
	// Remove legacy blob if present.
	elems = append(elems, &kv.Elem[kv.OP]{Op: kv.Del, Key: redisSetKey(key)})
	return r.dispatchElems(ctx, true, readTS, elems)
}

// applySetMemberMutation emits a Put or Del for one set member and returns the
// change count (1) and the signed length delta (+1 or -1), or (0, 0) if no change.
func applySetMemberMutation(elems []*kv.Elem[kv.OP], memberKey []byte, exists, add bool) ([]*kv.Elem[kv.OP], int, int64) {
	if add && !exists {
		return append(elems, &kv.Elem[kv.OP]{Op: kv.Put, Key: memberKey, Value: []byte{}}), 1, 1
	}
	if !add && exists {
		return append(elems, &kv.Elem[kv.OP]{Op: kv.Del, Key: memberKey}), 1, -1
	}
	return elems, 0, 0
}

// mutateExactSetLegacy handles SADD/SREM for non-set kinds (e.g. HLL) via the legacy blob path.
func (r *RedisServer) mutateExactSetLegacy(conn redcon.Conn, ctx context.Context, kind string, key []byte, members [][]byte, add bool) {
	var changed int
	if err := r.retryRedisWrite(ctx, func() error {
		readTS := r.readTS()
		if err := r.validateExactSetKind(kind, key, readTS); err != nil {
			return err
		}
		value, err := r.loadSetAt(context.Background(), kind, key, readTS)
		if err != nil {
			return err
		}
		existing := exactSetMembers(value)
		changed = applyExactSetMutation(existing, members, add)
		if changed == 0 {
			return nil
		}
		return r.persistExactSetMembersTxn(ctx, kind, key, readTS, existing)
	}); err != nil {
		conn.WriteError(err.Error())
		return
	}
	conn.WriteInt(changed)
}

// mutateExactSetWide handles SADD/SREM for the wide-column set path.
func (r *RedisServer) mutateExactSetWide(conn redcon.Conn, ctx context.Context, key []byte, members [][]byte, add bool) {
	var changed int
	if err := r.retryRedisWrite(ctx, func() error {
		readTS := r.readTS()
		if err := r.validateExactSetKind(setKind, key, readTS); err != nil {
			return err
		}

		commitTS := r.coordinator.Clock().Next()

		migrationElems, migErr := r.buildSetLegacyMigrationElems(ctx, key, readTS)
		if migErr != nil {
			return migErr
		}
		elems := make([]*kv.Elem[kv.OP], 0, len(migrationElems)+len(members)+setWideColOverhead)
		elems = append(elems, migrationElems...)

		// Extract legacy member names from migration ops so that applySetMemberMutations
		// can treat them as already-existing (they are not yet visible at readTS).
		legacyMemberBase := buildLegacySetMemberBase(migrationElems, key)

		var lenDelta int64
		var mutErr error
		elems, changed, lenDelta, mutErr = r.applySetMemberMutations(ctx, key, members, add, readTS, elems, legacyMemberBase)
		if mutErr != nil {
			return mutErr
		}

		if changed == 0 && len(migrationElems) == 0 {
			return nil
		}

		if lenDelta != 0 {
			deltaVal := store.MarshalSetMetaDelta(store.SetMetaDelta{LenDelta: lenDelta})
			elems = append(elems, &kv.Elem[kv.OP]{
				Op:    kv.Put,
				Key:   store.SetMetaDeltaKey(key, commitTS, 0),
				Value: deltaVal,
			})
		}

		if len(elems) == 0 {
			return nil
		}

		_, dispatchErr := r.coordinator.Dispatch(ctx, &kv.OperationGroup[kv.OP]{
			IsTxn:    true,
			StartTS:  normalizeStartTS(readTS),
			CommitTS: commitTS,
			Elems:    elems,
		})
		return cockerrors.WithStack(dispatchErr)
	}); err != nil {
		conn.WriteError(err.Error())
		return
	}
	conn.WriteInt(changed)
}

// scanSetMemberExistsMap does a paginated prefix scan of all member keys for
// the given set and returns a map from member name to struct{}{}.
// Using a single prefix scan eliminates the per-member ExistsAt round-trip.
func (r *RedisServer) scanSetMemberExistsMap(ctx context.Context, key []byte, readTS uint64) (map[string]struct{}, error) {
	return r.scanKeyExistsMap(ctx, store.SetMemberScanPrefix(key), readTS,
		func(k []byte) []byte { return store.ExtractSetMemberName(k, key) })
}

// scanHashFieldExistsMap does a paginated prefix scan of all field keys for
// the given hash and returns a map from field name to struct{}{}.
// Using a single prefix scan eliminates per-field ExistsAt round-trips.
func (r *RedisServer) scanHashFieldExistsMap(ctx context.Context, key []byte, readTS uint64) (map[string]struct{}, error) {
	return r.scanKeyExistsMap(ctx, store.HashFieldScanPrefix(key), readTS,
		func(k []byte) []byte { return store.ExtractHashFieldName(k, key) })
}

// mergeZSetBulkScores performs a single prefix scan of ZSet member keys and
// merges the store scores into inTxnView when pairCount >= wideColumnBulkScanThreshold.
// This avoids O(pairCount) individual GetAt round-trips inside applyZAddPair.
// Members already in inTxnView (migration elems or earlier pairs) take precedence.
// Returns inTxnView unchanged when the batch is below the threshold.
func (r *RedisServer) mergeZSetBulkScores(ctx context.Context, key []byte, readTS uint64, pairCount int, inTxnView map[string]float64) (map[string]float64, error) {
	if pairCount < wideColumnBulkScanThreshold {
		return inTxnView, nil
	}
	bulkScores, err := r.scanZSetMemberScoreMap(ctx, key, readTS)
	if err != nil {
		return nil, err
	}
	if inTxnView == nil {
		return bulkScores, nil
	}
	for m, s := range bulkScores {
		if _, alreadySeen := inTxnView[m]; !alreadySeen {
			inTxnView[m] = s
		}
	}
	return inTxnView, nil
}

// scanZSetMemberScoreMap does a paginated prefix scan of all member keys for
// the given ZSet and returns a map from member name to its current score.
// Using a single prefix scan eliminates O(N) GetAt round-trips in ZADD for
// large batches (>= wideColumnBulkScanThreshold pairs).
func (r *RedisServer) scanZSetMemberScoreMap(ctx context.Context, key []byte, readTS uint64) (map[string]float64, error) {
	scanPrefix := store.ZSetMemberScanPrefix(key)
	scanEnd := store.PrefixScanEnd(scanPrefix)
	scores := make(map[string]float64)
	cursor := scanPrefix
	for {
		scanKVs, err := r.store.ScanAt(ctx, cursor, scanEnd, store.MaxDeltaScanLimit, readTS)
		if err != nil {
			return nil, cockerrors.WithStack(err)
		}
		for _, pair := range scanKVs {
			m := store.ExtractZSetMemberName(pair.Key, key)
			if m == nil {
				continue
			}
			if s, decodeErr := store.UnmarshalZSetScore(pair.Value); decodeErr == nil {
				scores[string(m)] = s
			}
		}
		if len(scanKVs) < store.MaxDeltaScanLimit {
			break
		}
		lastKey := scanKVs[len(scanKVs)-1].Key
		next := make([]byte, len(lastKey)+1)
		copy(next, lastKey)
		cursor = next
	}
	return scores, nil
}

// scanKeyExistsMap paginates through all keys under scanPrefix, extracts a
// name from each key using extractName, and builds a set of existing names.
// It is used by scanSetMemberExistsMap and scanHashFieldExistsMap to eliminate
// per-key ExistsAt round-trips during SADD/SREM/HDEL operations.
func (r *RedisServer) scanKeyExistsMap(ctx context.Context, scanPrefix []byte, readTS uint64, extractName func([]byte) []byte) (map[string]struct{}, error) {
	scanEnd := store.PrefixScanEnd(scanPrefix)
	existsMap := make(map[string]struct{})
	cursor := scanPrefix
	for {
		scanKVs, err := r.store.ScanAt(ctx, cursor, scanEnd, store.MaxDeltaScanLimit, readTS)
		if err != nil {
			return nil, cockerrors.WithStack(err)
		}
		for _, pair := range scanKVs {
			if name := extractName(pair.Key); name != nil {
				existsMap[string(name)] = struct{}{}
			}
		}
		if len(scanKVs) < store.MaxDeltaScanLimit {
			break
		}
		lastKey := scanKVs[len(scanKVs)-1].Key
		next := make([]byte, len(lastKey)+1)
		copy(next, lastKey)
		cursor = next
	}
	return existsMap, nil
}

// initSetExistsMap builds the initial existence map for a set mutation batch.
// For large batches or when legacy members are present it does a bulk prefix
// scan; otherwise it returns an empty (non-nil) map for per-member ExistsAt
// fallback. Legacy members from migration elems are merged in so that members
// already in-flight in the same transaction are treated as existing.
func (r *RedisServer) initSetExistsMap(ctx context.Context, key []byte, members [][]byte, readTS uint64, legacyBase map[string]struct{}) (map[string]struct{}, error) {
	existsMap := make(map[string]struct{})
	if len(members) >= wideColumnBulkScanThreshold || len(legacyBase) > 0 {
		var err error
		existsMap, err = r.scanSetMemberExistsMap(ctx, key, readTS)
		if err != nil {
			return nil, cockerrors.WithStack(err)
		}
	}
	for m := range legacyBase {
		existsMap[m] = struct{}{}
	}
	return existsMap, nil
}

// lookupSetMemberExists reports whether memberStr is present, updating
// existsMap as a cache. For small clean batches (no bulk scan, no legacy
// migration) it falls back to an ExistsAt store read; otherwise it relies
// solely on the pre-built map.
func (r *RedisServer) lookupSetMemberExists(ctx context.Context, memberStr string, memberKey []byte, readTS uint64, existsMap map[string]struct{}, isSmallClean bool) (bool, error) {
	if _, ok := existsMap[memberStr]; ok {
		return true, nil
	}
	if !isSmallClean {
		return false, nil
	}
	exists, err := r.store.ExistsAt(ctx, memberKey, readTS)
	if err != nil {
		return false, cockerrors.WithStack(err)
	}
	if exists {
		existsMap[memberStr] = struct{}{}
	}
	return exists, nil
}

// applySetMemberMutations resolves existence for each member using either a
// pre-built bulk scan (for large batches) or individual ExistsAt calls (for
// small batches), then applies the mutation to elems.
// The bulk scan threshold is wideColumnBulkScanThreshold.
// legacyBase contains members from a legacy blob being migrated in the same
// transaction; they are not visible at readTS and must be treated as existing.
func (r *RedisServer) applySetMemberMutations(ctx context.Context, key []byte, members [][]byte, add bool, readTS uint64, elems []*kv.Elem[kv.OP], legacyBase map[string]struct{}) ([]*kv.Elem[kv.OP], int, int64, error) {
	existsMap, err := r.initSetExistsMap(ctx, key, members, readTS, legacyBase)
	if err != nil {
		return nil, 0, 0, err
	}
	isSmallClean := len(members) < wideColumnBulkScanThreshold && len(legacyBase) == 0
	changed := 0
	lenDelta := int64(0)
	for _, member := range members {
		memberStr := string(member)
		memberKey := store.SetMemberKey(key, member)
		exists, lookupErr := r.lookupSetMemberExists(ctx, memberStr, memberKey, readTS, existsMap, isSmallClean)
		if lookupErr != nil {
			return nil, 0, 0, lookupErr
		}
		var cnt int
		var d int64
		elems, cnt, d = applySetMemberMutation(elems, memberKey, exists, add)
		changed += cnt
		lenDelta += d
		// Update existsMap to reflect this mutation so that subsequent
		// duplicate members in this call observe the correct in-txn state.
		if add {
			existsMap[memberStr] = struct{}{}
		} else {
			delete(existsMap, memberStr)
		}
	}
	return elems, changed, lenDelta, nil
}

func (r *RedisServer) mutateExactSet(conn redcon.Conn, kind string, key []byte, members [][]byte, add bool) {
	ctx, cancel := context.WithTimeout(context.Background(), redisDispatchTimeout)
	defer cancel()

	if kind != setKind {
		r.mutateExactSetLegacy(conn, ctx, kind, key, members, add)
		return
	}
	r.mutateExactSetWide(conn, ctx, key, members, add)
}

func (r *RedisServer) sismember(conn redcon.Conn, cmd redcon.Command) {
	if r.proxyToLeader(conn, cmd, cmd.Args[1]) {
		return
	}
	key := cmd.Args[1]
	member := cmd.Args[2]
	readTS := r.readTS()
	ctx := context.Background()

	hit, alive, err := r.setMemberFastExists(ctx, key, member, readTS)
	if err != nil {
		conn.WriteError(err.Error())
		return
	}
	if hit {
		if alive {
			conn.WriteInt(1)
		} else {
			conn.WriteInt(0)
		}
		return
	}
	r.sismemberSlow(conn, ctx, key, member, readTS)
}

func (r *RedisServer) setMemberFastExists(ctx context.Context, key, member []byte, readTS uint64) (hit, alive bool, err error) {
	// Probe FIRST; guard only on hit. See hashFieldFastLookup for the
	// regression rationale.
	exists, err := r.store.ExistsAt(ctx, store.SetMemberKey(key, member), readTS)
	if err != nil {
		return false, false, cockerrors.WithStack(err)
	}
	if !exists {
		return false, false, nil
	}
	if higher, hErr := r.hasHigherPriorityStringEncoding(ctx, key, readTS); hErr != nil {
		return false, false, hErr
	} else if higher {
		return false, false, nil
	}
	expired, expErr := r.hasExpired(ctx, key, readTS, true)
	if expErr != nil {
		return false, false, cockerrors.WithStack(expErr)
	}
	return true, !expired, nil
}

func (r *RedisServer) sismemberSlow(conn redcon.Conn, ctx context.Context, key, member []byte, readTS uint64) {
	typ, err := r.keyTypeAt(ctx, key, readTS)
	if err != nil {
		conn.WriteError(err.Error())
		return
	}
	if typ == redisTypeNone {
		conn.WriteInt(0)
		return
	}
	if typ != redisTypeSet {
		conn.WriteError(wrongTypeMessage)
		return
	}
	value, err := r.loadSetAt(ctx, setKind, key, readTS)
	if err != nil {
		conn.WriteError(err.Error())
		return
	}
	if slices.Contains(value.Members, string(member)) {
		conn.WriteInt(1)
		return
	}
	conn.WriteInt(0)
}

func (r *RedisServer) smembers(conn redcon.Conn, cmd redcon.Command) {
	if r.proxyToLeader(conn, cmd, cmd.Args[1]) {
		return
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
	if typ != redisTypeSet {
		conn.WriteError(wrongTypeMessage)
		return
	}

	value, err := r.loadSetAt(context.Background(), setKind, cmd.Args[1], readTS)
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
	if r.proxyToLeader(conn, cmd, cmd.Args[1]) {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), redisDispatchTimeout)
	defer cancel()
	var changed int
	if err := r.retryRedisWrite(ctx, func() error {
		readTS := r.readTS()
		if err := r.validateExactSetKind(hllKind, cmd.Args[1], readTS); err != nil {
			return err
		}

		value, err := r.loadSetAt(context.Background(), hllKind, cmd.Args[1], readTS)
		if err != nil {
			return err
		}
		existing := exactSetMembers(value)
		changed = applyExactSetMutation(existing, cmd.Args[2:], true)
		if changed == 0 {
			return nil
		}

		return r.persistExactSetMembersTxn(ctx, hllKind, cmd.Args[1], readTS, existing)
	}); err != nil {
		conn.WriteError(err.Error())
		return
	}
	if changed == 0 {
		conn.WriteInt(0)
	} else {
		conn.WriteInt(1)
	}
}

func (r *RedisServer) pfcount(conn redcon.Conn, cmd redcon.Command) {
	if r.proxyToLeader(conn, cmd, cmd.Args[1]) {
		return
	}
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
		value, err := r.loadSetAt(context.Background(), hllKind, key, readTS)
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
	if r.proxyToLeader(conn, cmd, cmd.Args[1]) {
		return
	}
	added, err := r.applyHashFieldPairs(cmd.Args[1], cmd.Args[2:])
	if err != nil {
		conn.WriteError(err.Error())
		return
	}
	conn.WriteInt(added)
}

func (r *RedisServer) hmset(conn redcon.Conn, cmd redcon.Command) {
	if r.proxyToLeader(conn, cmd, cmd.Args[1]) {
		return
	}
	if _, err := r.applyHashFieldPairs(cmd.Args[1], cmd.Args[2:]); err != nil {
		conn.WriteError(err.Error())
		return
	}
	conn.WriteString("OK")
}

// buildHashLegacyMigrationElems returns ops that atomically migrate a legacy
// !redis|hash| blob to wide-column !hs|fld| keys.  Returns nil if no legacy
// blob exists.  The base meta key is also written with the migrated count so
// that resolveHashMeta works correctly after migration.
func (r *RedisServer) buildHashLegacyMigrationElems(ctx context.Context, key []byte, readTS uint64) ([]*kv.Elem[kv.OP], error) {
	raw, err := r.store.GetAt(ctx, redisHashKey(key), readTS)
	if cockerrors.Is(err, store.ErrKeyNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, cockerrors.WithStack(err)
	}
	value, err := unmarshalHashValue(raw)
	if err != nil {
		return nil, err
	}
	elems := make([]*kv.Elem[kv.OP], 0, len(value)+setWideColOverhead)
	for field, val := range value {
		elems = append(elems, &kv.Elem[kv.OP]{
			Op:    kv.Put,
			Key:   store.HashFieldKey(key, []byte(field)),
			Value: []byte(val),
		})
	}
	// Delete the legacy blob.
	elems = append(elems, &kv.Elem[kv.OP]{Op: kv.Del, Key: redisHashKey(key)})
	// Write a base meta so that resolveHashMeta starts from an accurate count.
	elems = append(elems, &kv.Elem[kv.OP]{
		Op:    kv.Put,
		Key:   store.HashMetaKey(key),
		Value: store.MarshalHashMeta(store.HashMeta{Len: int64(len(value))}),
	})
	return elems, nil
}

// buildSetLegacyMigrationElems returns ops that atomically migrate a legacy
// !redis|set| blob to wide-column !st|mem| keys.  Returns nil if no legacy
// blob exists.
func (r *RedisServer) buildSetLegacyMigrationElems(ctx context.Context, key []byte, readTS uint64) ([]*kv.Elem[kv.OP], error) {
	raw, err := r.store.GetAt(ctx, redisSetKey(key), readTS)
	if cockerrors.Is(err, store.ErrKeyNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, cockerrors.WithStack(err)
	}
	value, err := unmarshalSetValue(raw)
	if err != nil {
		return nil, err
	}
	elems := make([]*kv.Elem[kv.OP], 0, len(value.Members)+setWideColOverhead)
	for _, member := range value.Members {
		elems = append(elems, &kv.Elem[kv.OP]{
			Op:    kv.Put,
			Key:   store.SetMemberKey(key, []byte(member)),
			Value: []byte{},
		})
	}
	// Delete the legacy blob.
	elems = append(elems, &kv.Elem[kv.OP]{Op: kv.Del, Key: redisSetKey(key)})
	// Write a base meta so that resolveSetMeta starts from an accurate count.
	elems = append(elems, &kv.Elem[kv.OP]{
		Op:    kv.Put,
		Key:   store.SetMetaKey(key),
		Value: store.MarshalSetMeta(store.SetMeta{Len: int64(len(value.Members))}),
	})
	return elems, nil
}

// buildZSetLegacyMigrationElems returns ops that atomically migrate a legacy
// !redis|zset| blob to wide-column !zs|mem| + !zs|scr| keys. Returns nil if no legacy
// blob exists.  The base meta key is also written with the migrated count so
// that resolveZSetMeta works correctly after migration.
func (r *RedisServer) buildZSetLegacyMigrationElems(ctx context.Context, key []byte, readTS uint64) ([]*kv.Elem[kv.OP], error) {
	raw, err := r.store.GetAt(ctx, redisZSetKey(key), readTS)
	if cockerrors.Is(err, store.ErrKeyNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, cockerrors.WithStack(err)
	}
	value, err := unmarshalZSetValue(raw)
	if err != nil {
		return nil, err
	}
	// Each entry → member key + score index key; plus legacy blob deletion + base meta.
	elems := make([]*kv.Elem[kv.OP], 0, len(value.Entries)*2+setWideColOverhead) //nolint:mnd // 2 ops per entry (member + score index)
	for _, entry := range value.Entries {
		elems = append(elems,
			&kv.Elem[kv.OP]{
				Op:    kv.Put,
				Key:   store.ZSetMemberKey(key, []byte(entry.Member)),
				Value: store.MarshalZSetScore(entry.Score),
			},
			&kv.Elem[kv.OP]{
				Op:    kv.Put,
				Key:   store.ZSetScoreKey(key, entry.Score, []byte(entry.Member)),
				Value: []byte{},
			},
		)
	}
	// Delete the legacy blob.
	elems = append(elems, &kv.Elem[kv.OP]{Op: kv.Del, Key: redisZSetKey(key)})
	// Write a base meta so that resolveZSetMeta starts from an accurate count.
	elems = append(elems, &kv.Elem[kv.OP]{
		Op:    kv.Put,
		Key:   store.ZSetMetaKey(key),
		Value: store.MarshalZSetMeta(store.ZSetMeta{Len: int64(len(value.Entries))}),
	})
	return elems, nil
}

// addLegacyHashFieldsToMap adds field names from migration Put elems (fields
// being migrated in the current transaction, not yet visible at readTS) into
// existsMap so that buildHashFieldElems does not count them as new fields.
func addLegacyHashFieldsToMap(migrationElems []*kv.Elem[kv.OP], key []byte, existsMap map[string]struct{}) {
	for _, elem := range migrationElems {
		if elem.Op == kv.Put {
			if f := store.ExtractHashFieldName(elem.Key, key); f != nil {
				existsMap[string(f)] = struct{}{}
			}
		}
	}
}

// buildLegacySetMemberBase extracts member names from migration Put elems
// (members being migrated in the current transaction, invisible at readTS)
// and returns them as a set. Returns nil when no migration is happening.
func buildLegacySetMemberBase(migrationElems []*kv.Elem[kv.OP], key []byte) map[string]struct{} {
	var base map[string]struct{}
	for _, elem := range migrationElems {
		if elem.Op == kv.Put {
			if m := store.ExtractSetMemberName(elem.Key, key); m != nil {
				if base == nil {
					base = make(map[string]struct{})
				}
				base[string(m)] = struct{}{}
			}
		}
	}
	return base
}

// buildHashFieldElems iterates over field-value pairs in args, checks each
// field against existsMap to determine if it is new, appends Put operations
// to elems, and returns the updated elems and new-field count.
// existsMap is built by scanHashFieldExistsMap before this call so that
// existence checks are a single bulk scan rather than N ExistsAt round-trips.
func (r *RedisServer) buildHashFieldElems(key []byte, args [][]byte, existsMap map[string]struct{}, elems []*kv.Elem[kv.OP]) ([]*kv.Elem[kv.OP], int) {
	newFields := 0
	for i := 0; i < len(args); i += redisPairWidth {
		field := args[i]
		value := args[i+1]
		fieldStr := string(field)
		fieldKey := store.HashFieldKey(key, field)
		if _, exists := existsMap[fieldStr]; !exists {
			newFields++
			// Mark as seen so duplicate field names in one HSET call are not
			// counted as additional new fields (Redis deduplication semantics).
			existsMap[fieldStr] = struct{}{}
		}
		elems = append(elems, &kv.Elem[kv.OP]{Op: kv.Put, Key: fieldKey, Value: value})
	}
	return elems, newFields
}

func (r *RedisServer) applyHashFieldPairs(key []byte, args [][]byte) (int, error) {
	if len(args) == 0 || len(args)%redisPairWidth != 0 {
		return 0, errors.New("ERR wrong number of arguments for hash command")
	}

	ctx, cancel := context.WithTimeout(context.Background(), redisDispatchTimeout)
	defer cancel()
	var added int
	err := r.retryRedisWrite(ctx, func() error {
		readTS := r.readTS()
		typ, err := r.keyTypeAtExpect(ctx, key, readTS, redisTypeHash)
		if err != nil {
			return err
		}
		if typ != redisTypeNone && typ != redisTypeHash {
			return wrongTypeError()
		}

		commitTS := r.coordinator.Clock().Next()

		// Atomically migrate any legacy blob on first wide-column write.
		// Fetch migration elems before allocating the main elems slice so that
		// the initial capacity accounts for both migration and field Put ops,
		// avoiding a reallocation when a legacy blob is present.
		migrationElems, err := r.buildHashLegacyMigrationElems(ctx, key, readTS)
		if err != nil {
			return err
		}
		elems := make([]*kv.Elem[kv.OP], 0, len(migrationElems)+len(args)/redisPairWidth+setWideColOverhead)
		elems = append(elems, migrationElems...)

		// Bulk-scan existing fields once so buildHashFieldElems can check
		// existence via a map lookup instead of per-field ExistsAt.
		existsMap, err := r.scanHashFieldExistsMap(ctx, key, readTS)
		if err != nil {
			return err
		}
		// Fields from the legacy blob are being migrated in this same transaction,
		// so they are not yet visible at readTS. Add them to existsMap so that
		// buildHashFieldElems does not count already-existing fields as new.
		addLegacyHashFieldsToMap(migrationElems, key, existsMap)

		var newFields int
		elems, newFields = r.buildHashFieldElems(key, args, existsMap, elems)
		added = newFields

		// Emit a single delta key for all newly-added fields.
		if newFields != 0 {
			deltaVal := store.MarshalHashMetaDelta(store.HashMetaDelta{LenDelta: int64(newFields)})
			elems = append(elems, &kv.Elem[kv.OP]{
				Op:    kv.Put,
				Key:   store.HashMetaDeltaKey(key, commitTS, 0),
				Value: deltaVal,
			})
		}

		if len(elems) == 0 {
			return nil
		}

		_, dispatchErr := r.coordinator.Dispatch(ctx, &kv.OperationGroup[kv.OP]{
			IsTxn:    true,
			StartTS:  normalizeStartTS(readTS),
			CommitTS: commitTS,
			Elems:    elems,
		})
		return cockerrors.WithStack(dispatchErr)
	})
	return added, err
}

func (r *RedisServer) hget(conn redcon.Conn, cmd redcon.Command) {
	if r.proxyToLeader(conn, cmd, cmd.Args[1]) {
		return
	}
	key := cmd.Args[1]
	field := cmd.Args[2]
	readTS := r.readTS()
	ctx := context.Background()

	// Fast path: look the wide-column field up directly. Live
	// wide-column hashes resolve here in 1 seek + TTL probe versus
	// the ~17 seeks rawKeyTypeAt issues through keyTypeAt. Legacy-
	// blob hashes miss the wide-column key and fall through.
	raw, hit, alive, err := r.hashFieldFastLookup(ctx, key, field, readTS)
	if err != nil {
		conn.WriteError(err.Error())
		return
	}
	if hit {
		if !alive {
			conn.WriteNull()
			return
		}
		// WriteBulk sends the payload directly from the []byte backing
		// store; WriteBulkString(string(raw)) would force a []byte →
		// string copy on every fast-path hit.
		conn.WriteBulk(raw)
		return
	}
	r.hgetSlow(conn, ctx, key, field, readTS)
}

// hashFieldFastLookup probes the wide-column field entry directly and
// reports whether it is present and TTL-alive. Returns hit=false when
// the wide-column key is absent, or when the narrow string-encoding
// guard in hasHigherPriorityStringEncoding fires, so the caller
// falls through to hgetSlow.
//
// Priority-alignment scope: this fast path does NOT fully mirror
// rawKeyTypeAt / keyTypeAt's priority checks. The guard only probes
// redisStrKey (the common SET-over-previous-hash corruption case);
// rarer dual-encoding corruption involving HLL, legacy bare keys, or
// list meta / delta entries is NOT caught here and will surface the
// wide-column hash answer instead of the WRONGTYPE / nil response
// keyTypeAt would produce. In normal operation at most one encoding
// exists per user key, so the guard is a guaranteed miss and the
// priority-alignment gap is invisible; pre-existing writers already
// clean up the old encoding before switching types. A full check
// would cost ~3-5 extra seeks per fast-path hit, which would negate
// most of the gain over the ~17-seek keyTypeAt slow path.
func (r *RedisServer) hashFieldFastLookup(ctx context.Context, key, field []byte, readTS uint64) (raw []byte, hit, alive bool, err error) {
	// Probe the wide-column field FIRST so the priority guard only
	// runs on a hit. Placing the guard before the probe made every
	// miss (nonexistent key, legacy-blob hash, or wrong-type) pay an
	// unnecessary ExistsAt on redisStrKey -- pure overhead for the
	// common negative-lookup case and for any workload that still
	// carries legacy-blob encodings. See the PR #565 independent
	// review for the Medium-severity regression this addresses.
	raw, err = r.store.GetAt(ctx, store.HashFieldKey(key, field), readTS)
	if err != nil {
		if cockerrors.Is(err, store.ErrKeyNotFound) {
			return nil, false, false, nil
		}
		return nil, false, false, cockerrors.WithStack(err)
	}
	// Only pay the guard seek when we actually have a hit to defer.
	if higher, hErr := r.hasHigherPriorityStringEncoding(ctx, key, readTS); hErr != nil {
		return nil, false, false, hErr
	} else if higher {
		return nil, false, false, nil
	}
	expired, expErr := r.hasExpired(ctx, key, readTS, true)
	if expErr != nil {
		return nil, false, false, cockerrors.WithStack(expErr)
	}
	return raw, true, !expired, nil
}

// hasHigherPriorityStringEncoding returns true iff the new-format
// string encoding (redisStrKey) exists for key. This is NARROWER
// than rawKeyTypeAt's full string-wins tiebreaker, which also covers
// HyperLogLog (redisHLLKey) and the legacy bare key: those rarer
// dual-encoding corruption cases still reach the wide-column fast
// path and may return the collection-specific answer instead of
// WRONGTYPE / nil.
//
// The narrow scope is deliberate -- expanding the guard to every
// string-priority candidate (3 ExistsAt calls + the list-meta probe)
// would cost ~4-5 extra seeks per fast-path hit, regressing the
// negative case further than the ordering tweak in
// hashFieldFastLookup / setMemberFastExists / hashFieldFastExists
// already saved. Callers that require complete priority alignment
// must take the keyTypeAt slow path explicitly.
func (r *RedisServer) hasHigherPriorityStringEncoding(ctx context.Context, key []byte, readTS uint64) (bool, error) {
	exists, err := r.store.ExistsAt(ctx, redisStrKey(key), readTS)
	if err != nil {
		return false, cockerrors.WithStack(err)
	}
	return exists, nil
}

// zsetMemberFastScore probes the wide-column score entry for (key,
// member) directly and reports whether it is present and TTL-alive.
// Priority-alignment scope mirrors hashFieldFastLookup: only the
// redisStrKey dual-encoding case is guarded (see
// hasHigherPriorityStringEncoding's narrow-scope caveats). Callers
// must fall back to the full zsetState loader on hit=false to cover
// legacy-blob zsets and nil / WRONGTYPE disambiguation.
//
// Probe ORDER matches hashFieldFastLookup / setMemberFastExists /
// hashFieldFastExists post-PR #565: hit the wide-column score key
// first so the negative case (missing, legacy-blob, wrong-type) does
// not pay the priority-guard seek.
func (r *RedisServer) zsetMemberFastScore(ctx context.Context, key, member []byte, readTS uint64) (score float64, hit, alive bool, err error) {
	raw, err := r.store.GetAt(ctx, store.ZSetMemberKey(key, member), readTS)
	if err != nil {
		if cockerrors.Is(err, store.ErrKeyNotFound) {
			return 0, false, false, nil
		}
		return 0, false, false, cockerrors.WithStack(err)
	}
	score, err = store.UnmarshalZSetScore(raw)
	if err != nil {
		return 0, false, false, cockerrors.WithStack(err)
	}
	if higher, hErr := r.hasHigherPriorityStringEncoding(ctx, key, readTS); hErr != nil {
		return 0, false, false, hErr
	} else if higher {
		return 0, false, false, nil
	}
	expired, expErr := r.hasExpired(ctx, key, readTS, true)
	if expErr != nil {
		return 0, false, false, cockerrors.WithStack(expErr)
	}
	return score, true, !expired, nil
}

// zsetRangeByScoreFast streams the score index for key over the
// caller-supplied [startKey, endKey) byte range, returning the
// decoded entries up to offset+limit. This replaces the
// load-the-whole-zset path used by cmdZRangeByScore / cmdZRevRangeByScore
// when the caller has no script-local mutations and the zset is in
// wide-column form. For a delay-queue poll ("next 10 jobs due by
// now") the cost goes from O(N) member GetAts to O(range_width +
// offset + limit) score-index entries.
//
// hit=false means the fast path cannot safely answer (legacy-blob
// zset present, string-encoding corruption, or empty-result case
// where we cannot distinguish "zset is empty in this range" from
// "key exists as another type / is missing"). Callers MUST take
// the slow path on hit=false so keyTypeAt disambiguation fires.
// reason carries the specific hit=false branch so observers can
// subdivide fallback rates for dashboarding; "" when hit=true.
//
// scoreInRange filter is applied post-scan for exclusive bound
// edge cases; the caller supplies precomputed scan bounds that
// over-approximate toward INclusive and lets this helper filter.
func (r *RedisServer) zsetRangeByScoreFast(
	ctx context.Context,
	key, startKey, endKey []byte,
	reverse bool,
	offset, limit int,
	scoreFilter func(float64) bool,
	readTS uint64,
) ([]redisZSetEntry, bool, monitoring.LuaFastPathFallbackReason, error) {
	if eligible, err := r.zsetFastPathEligible(ctx, key, readTS); err != nil || !eligible {
		return nil, false, monitoring.LuaFastPathFallbackIneligible, err
	}
	// Large-offset short-circuit: once offset >= maxWideScanLimit,
	// the fast path can only scan maxWideScanLimit rows then skip all
	// of them -- guaranteed wasted I/O. Defer to the slow path
	// immediately so it can answer from the full member load without
	// the redundant score-index scan.
	if offset >= maxWideScanLimit {
		return nil, false, monitoring.LuaFastPathFallbackLargeOffset, nil
	}
	scanLimit := zsetFastScanLimit(offset, limit)
	if scanLimit <= 0 || bytes.Compare(startKey, endKey) >= 0 {
		hit, reason, err := r.zsetRangeEmptyFastResult(ctx, key, readTS)
		return nil, hit, reason, err
	}
	kvs, err := r.zsetScoreScan(ctx, startKey, endKey, scanLimit, reverse, readTS)
	if err != nil {
		return nil, false, monitoring.LuaFastPathFallbackOther, err
	}
	return r.finalizeZSetFastRange(ctx, key, kvs, offset, limit, scanLimit, scoreFilter, readTS)
}

// finalizeZSetFastRange runs the post-scan priority guard, decodes
// the candidate score rows into redisZSetEntry, and applies the TTL
// filter. Factored out so zsetRangeByScoreFast stays under the
// cyclomatic-complexity cap.
//
// Takes scanLimit so we can detect a saturated scan: if the scanner
// returned exactly scanLimit rows AND the caller's request is not
// satisfied (unbounded limit, or collected fewer entries than limit),
// there MAY be more entries beyond the scan window. In that case we
// return hit=false so the slow path can produce the authoritative
// answer -- the fast path MUST NOT silently truncate.
func (r *RedisServer) finalizeZSetFastRange(
	ctx context.Context, key []byte, kvs []*store.KVPair,
	offset, limit, scanLimit int, scoreFilter func(float64) bool, readTS uint64,
) ([]redisZSetEntry, bool, monitoring.LuaFastPathFallbackReason, error) {
	// Priority guard runs after a candidate hit (mirrors post-PR #565
	// ordering). Skip it on empty result -- the empty-result tail
	// handles disambiguation.
	if len(kvs) > 0 {
		if higher, hErr := r.hasHigherPriorityStringEncoding(ctx, key, readTS); hErr != nil {
			return nil, false, monitoring.LuaFastPathFallbackOther, hErr
		} else if higher {
			return nil, false, monitoring.LuaFastPathFallbackWrongType, nil
		}
	}
	entries := decodeZSetScoreRange(key, kvs, offset, limit, scoreFilter)
	// Truncation guard: the raw scanner hit its cap AND the caller did
	// not get a satisfied result. Entries beyond the window may
	// exist; defer to the slow path for correctness.
	if zsetFastPathTruncated(len(kvs), scanLimit, len(entries), limit) {
		return nil, false, monitoring.LuaFastPathFallbackTruncated, nil
	}
	if len(entries) == 0 {
		hit, reason, err := r.zsetRangeEmptyFastResult(ctx, key, readTS)
		return nil, hit, reason, err
	}
	expired, expErr := r.hasExpired(ctx, key, readTS, true)
	if expErr != nil {
		return nil, false, monitoring.LuaFastPathFallbackOther, cockerrors.WithStack(expErr)
	}
	if expired {
		return nil, true, "", nil
	}
	return entries, true, "", nil
}

// zsetFastPathTruncated reports whether the bounded score-index scan
// may have dropped entries that the caller's request would otherwise
// include. Returns true when the scanner returned the full quota
// (scannedRows == scanLimit) AND the caller's request is still
// unsatisfied (unbounded limit or collectedEntries < limit). In that
// case the caller must fall back to the slow full-load path to get
// the authoritative result.
func zsetFastPathTruncated(scannedRows, scanLimit, collectedEntries, limit int) bool {
	if scannedRows < scanLimit {
		return false
	}
	if limit < 0 {
		return true
	}
	return collectedEntries < limit
}

// zsetFastPathEligible returns false (without error) when a legacy-
// blob zset is present; the caller must take the slow path so
// ensureZSetLoaded / blob decoding runs.
func (r *RedisServer) zsetFastPathEligible(ctx context.Context, key []byte, readTS uint64) (bool, error) {
	legacyExists, err := r.store.ExistsAt(ctx, redisZSetKey(key), readTS)
	if err != nil {
		return false, cockerrors.WithStack(err)
	}
	return !legacyExists, nil
}

// zsetFastScanLimit clamps offset+limit to maxWideScanLimit so an
// unbounded or malicious LIMIT cannot force an O(N) scan of a large
// zset. A negative limit means "unbounded" at the Redis level; cap it
// at the collection OOM limit.
//
// Check bounds BEFORE adding to avoid signed-integer overflow on
// hostile input (e.g. a Lua script passing offset=limit=math.MaxInt).
// A wrap would produce a negative scanLimit and cause the caller's
// `scanLimit <= 0` branch to misroute a live zset into the
// empty-result tail.
func zsetFastScanLimit(offset, limit int) int {
	// limit == 0: the caller wants zero entries regardless of offset.
	// Return 0 so the caller's `scanLimit <= 0` branch routes to the
	// empty-result tail (which still runs resolveZSetMeta for proper
	// WRONGTYPE / existence disambiguation) instead of a pointless
	// full-quota scan.
	if limit == 0 {
		return 0
	}
	if limit < 0 {
		return maxWideScanLimit
	}
	if offset >= maxWideScanLimit {
		return maxWideScanLimit
	}
	if limit > maxWideScanLimit-offset {
		return maxWideScanLimit
	}
	return offset + limit
}

// zsetScoreScan picks Forward / Reverse ScanAt based on direction.
func (r *RedisServer) zsetScoreScan(
	ctx context.Context, startKey, endKey []byte, scanLimit int, reverse bool, readTS uint64,
) ([]*store.KVPair, error) {
	if reverse {
		kvs, err := r.store.ReverseScanAt(ctx, startKey, endKey, scanLimit, readTS)
		return kvs, cockerrors.WithStack(err)
	}
	kvs, err := r.store.ScanAt(ctx, startKey, endKey, scanLimit, readTS)
	return kvs, cockerrors.WithStack(err)
}

// zsetDecodeAllocSize returns a tight upper bound on the collected
// entry count for decodeZSetScoreRange: (kvLen - offset) capped by
// limit, never negative. Avoiding a make([]...len(kvs)) saves up to
// maxWideScanLimit entries of wasted slice capacity when the caller
// asked for a small window at a large offset.
func zsetDecodeAllocSize(kvLen, offset, limit int) int {
	allocSize := kvLen - offset
	if allocSize < 0 {
		return 0
	}
	if limit >= 0 && limit < allocSize {
		return limit
	}
	return allocSize
}

// decodeZSetScoreRange decodes score-index scan results into
// redisZSetEntry, applying the post-scan score filter (exclusive
// bound edges) and the offset / limit pagination. Entries that fail
// to decode are silently dropped -- they can only appear under data
// corruption.
func decodeZSetScoreRange(
	key []byte, kvs []*store.KVPair, offset, limit int, scoreFilter func(float64) bool,
) []redisZSetEntry {
	entries := make([]redisZSetEntry, 0, zsetDecodeAllocSize(len(kvs), offset, limit))
	skipped := 0
	for _, kv := range kvs {
		score, member, ok := store.ExtractZSetScoreAndMember(kv.Key, key)
		if !ok {
			continue
		}
		if scoreFilter != nil && !scoreFilter(score) {
			continue
		}
		// Check limit saturation BEFORE the offset skip so a small
		// limit with a large offset exits immediately instead of
		// burning offset iterations on the skip branch. Correct for
		// any (offset, limit): once len(entries) >= limit we are done
		// regardless of remaining skip budget.
		if limit >= 0 && len(entries) >= limit {
			break
		}
		if skipped < offset {
			skipped++
			continue
		}
		entries = append(entries, redisZSetEntry{Member: string(member), Score: score})
	}
	return entries
}

// zsetRangeEmptyFastResult is the empty-result tail: either the
// score range is genuinely empty on a live zset (return empty +
// hit=true) or the zset does not exist in wide-column form (return
// hit=false so the caller takes the slow path for WRONGTYPE / missing
// disambiguation).
//
// Uses resolveZSetMeta so delta-only wide zsets (a fresh zset whose
// base meta has not been persisted yet, only delta rows) are detected
// as "exists". Using a plain ExistsAt on ZSetMetaKey would miss those
// and force the slow path unnecessarily. Also runs the string-priority
// guard so a corrupted redisStrKey + zset meta surfaces WRONGTYPE via
// the slow path rather than an empty array.
// zsetRangeEmptyFastResult returns (hit, reason, err) for the empty-
// result tail. hit=true means the key is a live zset whose score
// range is simply empty (callers return an empty array and no
// fallback); hit=false carries a specific fallback reason so the
// caller can route its slow-path observation accordingly.
func (r *RedisServer) zsetRangeEmptyFastResult(ctx context.Context, key []byte, readTS uint64) (bool, monitoring.LuaFastPathFallbackReason, error) {
	_, zsetExists, err := r.resolveZSetMeta(ctx, key, readTS)
	if err != nil {
		return false, monitoring.LuaFastPathFallbackOther, cockerrors.WithStack(err)
	}
	if !zsetExists {
		// The key has no ZSet encoding at readTS. Redis semantics:
		//   - key truly absent  → ZRANGEBYSCORE returns empty
		//   - key is another type → ZRANGEBYSCORE returns WRONGTYPE
		// Production metric (PR #572) showed this branch is the
		// hot-path dominant outcome (~96% of ZRANGEBYSCORE calls on
		// BullMQ-style workloads that poll an empty delayed queue).
		// Punting every such call to the slow path repeats the same
		// 3-probe member/meta/delta scan we just did and then
		// re-probes all other types anyway -- pure duplicate I/O.
		//
		// Short-circuit: use keyTypeAt (logical type after TTL check)
		// to distinguish "truly absent" from "wrong type". If None,
		// return hit=true with an empty result -- that is the correct
		// Redis answer and saves the slow-path round-trip. Otherwise
		// fall back so the slow path can produce WRONGTYPE.
		typ, typErr := r.keyTypeAtExpect(ctx, key, readTS, redisTypeZSet)
		if typErr != nil {
			return false, monitoring.LuaFastPathFallbackOther, cockerrors.WithStack(typErr)
		}
		if typ == redisTypeNone {
			return true, "", nil
		}
		return false, monitoring.LuaFastPathFallbackWrongType, nil
	}
	if higher, hErr := r.hasHigherPriorityStringEncoding(ctx, key, readTS); hErr != nil {
		return false, monitoring.LuaFastPathFallbackOther, hErr
	} else if higher {
		return false, monitoring.LuaFastPathFallbackWrongType, nil
	}
	// hasExpired is called for its error-surfacing side effect only:
	// whether the zset is expired or not, a live zset with no members
	// in range returns an empty hit=true result. Keep the call so
	// storage errors during TTL resolution still propagate.
	if _, expErr := r.hasExpired(ctx, key, readTS, true); expErr != nil {
		return false, monitoring.LuaFastPathFallbackOther, cockerrors.WithStack(expErr)
	}
	return true, "", nil
}

// hgetSlow falls back to the type-probing path when hashFieldFastLookup
// misses. Handles legacy-blob hashes and nil / WRONGTYPE disambiguation.
func (r *RedisServer) hgetSlow(conn redcon.Conn, ctx context.Context, key, field []byte, readTS uint64) {
	typ, err := r.keyTypeAtExpect(ctx, key, readTS, redisTypeHash)
	if err != nil {
		conn.WriteError(err.Error())
		return
	}
	if typ == redisTypeNone {
		conn.WriteNull()
		return
	}
	if typ != redisTypeHash {
		conn.WriteError(wrongTypeMessage)
		return
	}
	value, err := r.loadHashAt(ctx, key, readTS)
	if err != nil {
		conn.WriteError(err.Error())
		return
	}
	fieldValue, ok := value[string(field)]
	if !ok {
		conn.WriteNull()
		return
	}
	conn.WriteBulkString(fieldValue)
}

func (r *RedisServer) hmget(conn redcon.Conn, cmd redcon.Command) {
	if r.proxyToLeader(conn, cmd, cmd.Args[1]) {
		return
	}
	readTS := r.readTS()
	typ, err := r.keyTypeAtExpect(context.Background(), cmd.Args[1], readTS, redisTypeHash)
	if err != nil {
		conn.WriteError(err.Error())
		return
	}
	fields := cmd.Args[redisPairWidth:]
	if typ == redisTypeNone {
		conn.WriteArray(len(fields))
		for range cmd.Args[2:] {
			conn.WriteNull()
		}
		return
	}
	if typ != redisTypeHash {
		conn.WriteError(wrongTypeMessage)
		return
	}

	value, err := r.loadHashAt(context.Background(), cmd.Args[1], readTS)
	if err != nil {
		conn.WriteError(err.Error())
		return
	}
	conn.WriteArray(len(fields))
	for _, field := range fields {
		fieldValue, ok := value[string(field)]
		if !ok {
			conn.WriteNull()
			continue
		}
		conn.WriteBulkString(fieldValue)
	}
}

func (r *RedisServer) hdel(conn redcon.Conn, cmd redcon.Command) {
	if r.proxyToLeader(conn, cmd, cmd.Args[1]) {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), redisDispatchTimeout)
	defer cancel()
	var removed int
	if err := r.retryRedisWrite(ctx, func() error {
		var err error
		removed, err = r.hdelTxn(ctx, cmd.Args[1], cmd.Args[2:])
		return err
	}); err != nil {
		conn.WriteError(err.Error())
		return
	}
	conn.WriteInt(removed)
}

// hdelWideColumn deletes the given fields from the wide-column hash and emits a negative delta.
func (r *RedisServer) hdelWideColumn(ctx context.Context, key []byte, fields [][]byte, readTS uint64) (int, error) {
	delElems, removed, err := r.resolveHashFieldDelElems(ctx, key, fields, readTS)
	if err != nil {
		return 0, err
	}
	if removed == 0 {
		return 0, nil
	}
	commitTS := r.coordinator.Clock().Next()
	elems := delElems
	deltaVal := store.MarshalHashMetaDelta(store.HashMetaDelta{LenDelta: int64(-removed)})
	elems = append(elems, &kv.Elem[kv.OP]{
		Op:    kv.Put,
		Key:   store.HashMetaDeltaKey(key, commitTS, 0),
		Value: deltaVal,
	})
	_, dispatchErr := r.coordinator.Dispatch(ctx, &kv.OperationGroup[kv.OP]{
		IsTxn:    true,
		StartTS:  normalizeStartTS(readTS),
		CommitTS: commitTS,
		Elems:    elems,
	})
	return removed, cockerrors.WithStack(dispatchErr)
}

// resolveHashFieldDelElems checks which fields exist using either a bulk scan
// (for large batches) or individual ExistsAt calls (for small batches), then
// returns Del elems for every field that exists and the count of deletions.
func (r *RedisServer) resolveHashFieldDelElems(ctx context.Context, key []byte, fields [][]byte, readTS uint64) ([]*kv.Elem[kv.OP], int, error) {
	var existsMap map[string]struct{}
	if len(fields) >= wideColumnBulkScanThreshold {
		var err error
		existsMap, err = r.scanHashFieldExistsMap(ctx, key, readTS)
		if err != nil {
			return nil, 0, err
		}
	}
	elems := make([]*kv.Elem[kv.OP], 0, len(fields)+1)
	removed := 0
	for _, field := range fields {
		fieldKey := store.HashFieldKey(key, field)
		var exists bool
		if existsMap != nil {
			_, exists = existsMap[string(field)]
		} else {
			var err error
			exists, err = r.store.ExistsAt(ctx, fieldKey, readTS)
			if err != nil {
				return nil, 0, cockerrors.WithStack(err)
			}
		}
		if exists {
			elems = append(elems, &kv.Elem[kv.OP]{Op: kv.Del, Key: fieldKey})
			removed++
		}
	}
	return elems, removed, nil
}

func (r *RedisServer) hdelTxn(ctx context.Context, key []byte, fields [][]byte) (int, error) {
	readTS := r.readTS()
	typ, err := r.keyTypeAtExpect(ctx, key, readTS, redisTypeHash)
	if err != nil {
		return 0, err
	}
	if typ == redisTypeNone {
		return 0, nil
	}
	if typ != redisTypeHash {
		return 0, wrongTypeError()
	}

	// Wide-column path: check if any !hs|fld| keys exist for this key.
	hashFieldPrefix := store.HashFieldScanPrefix(key)
	hashFieldEnd := store.PrefixScanEnd(hashFieldPrefix)
	wideKVs, err := r.store.ScanAt(context.Background(), hashFieldPrefix, hashFieldEnd, 1, readTS)
	if err != nil {
		return 0, cockerrors.WithStack(err)
	}
	if len(wideKVs) > 0 {
		return r.hdelWideColumn(ctx, key, fields, readTS)
	}

	// Legacy blob path.
	value, err := r.loadHashAt(context.Background(), key, readTS)
	if err != nil {
		return 0, err
	}
	removed := removeHashFields(value, fields)
	if removed == 0 {
		return 0, nil
	}
	return removed, r.persistHashTxn(ctx, key, readTS, value)
}

func removeHashFields(value redisHashValue, fields [][]byte) int {
	removed := 0
	for _, field := range fields {
		if _, ok := value[string(field)]; ok {
			delete(value, string(field))
			removed++
		}
	}
	return removed
}

func (r *RedisServer) persistHashTxn(ctx context.Context, key []byte, readTS uint64, value redisHashValue) error {
	if len(value) == 0 {
		elems, _, err := r.deleteLogicalKeyElems(ctx, key, readTS)
		if err != nil {
			return err
		}
		return r.dispatchElems(ctx, true, readTS, elems)
	}
	// Wide-column rewrite: write per-field keys and a new base meta.
	// deleteLogicalKeyElems (called by the caller when needed) clears old keys.
	elems := make([]*kv.Elem[kv.OP], 0, len(value)+1)
	for field, val := range value {
		elems = append(elems, &kv.Elem[kv.OP]{
			Op:    kv.Put,
			Key:   store.HashFieldKey(key, []byte(field)),
			Value: []byte(val),
		})
	}
	elems = append(elems, &kv.Elem[kv.OP]{
		Op:    kv.Put,
		Key:   store.HashMetaKey(key),
		Value: store.MarshalHashMeta(store.HashMeta{Len: int64(len(value))}),
	})
	// Also remove the legacy blob if it was present.
	elems = append(elems, &kv.Elem[kv.OP]{Op: kv.Del, Key: redisHashKey(key)})
	return r.dispatchElems(ctx, true, readTS, elems)
}

func (r *RedisServer) hexists(conn redcon.Conn, cmd redcon.Command) {
	if r.proxyToLeader(conn, cmd, cmd.Args[1]) {
		return
	}
	key := cmd.Args[1]
	field := cmd.Args[2]
	readTS := r.readTS()
	ctx := context.Background()

	// Fast path: direct wide-column field existence check. ExistsAt
	// is cheaper than GetAt since we don't need the value payload.
	hit, alive, err := r.hashFieldFastExists(ctx, key, field, readTS)
	if err != nil {
		conn.WriteError(err.Error())
		return
	}
	if hit {
		if alive {
			conn.WriteInt(1)
		} else {
			conn.WriteInt(0)
		}
		return
	}
	r.hexistsSlow(conn, ctx, key, field, readTS)
}

func (r *RedisServer) hashFieldFastExists(ctx context.Context, key, field []byte, readTS uint64) (hit, alive bool, err error) {
	// Probe FIRST; guard only on hit. See hashFieldFastLookup for the
	// regression rationale.
	exists, err := r.store.ExistsAt(ctx, store.HashFieldKey(key, field), readTS)
	if err != nil {
		return false, false, cockerrors.WithStack(err)
	}
	if !exists {
		return false, false, nil
	}
	if higher, hErr := r.hasHigherPriorityStringEncoding(ctx, key, readTS); hErr != nil {
		return false, false, hErr
	} else if higher {
		return false, false, nil
	}
	expired, expErr := r.hasExpired(ctx, key, readTS, true)
	if expErr != nil {
		return false, false, cockerrors.WithStack(expErr)
	}
	return true, !expired, nil
}

func (r *RedisServer) hexistsSlow(conn redcon.Conn, ctx context.Context, key, field []byte, readTS uint64) {
	typ, err := r.keyTypeAtExpect(ctx, key, readTS, redisTypeHash)
	if err != nil {
		conn.WriteError(err.Error())
		return
	}
	if typ == redisTypeNone {
		conn.WriteInt(0)
		return
	}
	if typ != redisTypeHash {
		conn.WriteError(wrongTypeMessage)
		return
	}
	value, err := r.loadHashAt(ctx, key, readTS)
	if err != nil {
		conn.WriteError(err.Error())
		return
	}
	if _, ok := value[string(field)]; ok {
		conn.WriteInt(1)
		return
	}
	conn.WriteInt(0)
}

func (r *RedisServer) hlen(conn redcon.Conn, cmd redcon.Command) {
	if r.proxyToLeader(conn, cmd, cmd.Args[1]) {
		return
	}
	readTS := r.readTS()
	typ, err := r.keyTypeAtExpect(context.Background(), cmd.Args[1], readTS, redisTypeHash)
	if err != nil {
		conn.WriteError(err.Error())
		return
	}
	if typ == redisTypeNone {
		conn.WriteInt(0)
		return
	}
	if typ != redisTypeHash {
		conn.WriteError(wrongTypeMessage)
		return
	}

	// Wide-column path: use delta-aggregated metadata for O(1) count.
	count, exists, err := r.resolveHashMeta(context.Background(), cmd.Args[1], readTS)
	if err != nil {
		conn.WriteError(err.Error())
		return
	}
	if exists {
		conn.WriteInt64(count)
		return
	}
	// Legacy blob fallback: load all fields and count.
	value, err := r.loadHashAt(context.Background(), cmd.Args[1], readTS)
	if err != nil {
		conn.WriteError(err.Error())
		return
	}
	conn.WriteInt(len(value))
}

func (r *RedisServer) hincrby(conn redcon.Conn, cmd redcon.Command) {
	if r.proxyToLeader(conn, cmd, cmd.Args[1]) {
		return
	}
	increment, err := strconv.ParseInt(string(cmd.Args[3]), 10, 64)
	if err != nil {
		conn.WriteError(err.Error())
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), redisDispatchTimeout)
	defer cancel()
	var current int64
	if err := r.retryRedisWrite(ctx, func() error {
		var txnErr error
		current, txnErr = r.hincrbyTxn(ctx, cmd.Args[1], cmd.Args[2], increment)
		return txnErr
	}); err != nil {
		conn.WriteError(err.Error())
		return
	}
	conn.WriteInt64(current)
}

// readHashFieldInt reads the current integer value of a hash field from wide-column or legacy storage.
// Returns (current, isNewField, legacyHashValue, error). legacyHashValue is non-nil only when
// the value came from a legacy JSON blob that needs to be migrated on the next write.
func (r *RedisServer) readHashFieldInt(ctx context.Context, key, field []byte, readTS uint64) (int64, bool, redisHashValue, error) {
	fieldKey := store.HashFieldKey(key, field)
	raw, readErr := r.store.GetAt(ctx, fieldKey, readTS)
	if readErr != nil && !cockerrors.Is(readErr, store.ErrKeyNotFound) {
		return 0, true, nil, cockerrors.WithStack(readErr)
	}
	if readErr == nil {
		current, parseErr := strconv.ParseInt(string(raw), 10, 64)
		if parseErr != nil {
			return 0, false, nil, errors.New("ERR hash value is not an integer")
		}
		return current, false, nil, nil
	}
	// Not in wide-column – check legacy blob.
	legacyValue, legacyErr := r.loadHashAt(ctx, key, readTS)
	if legacyErr != nil {
		return 0, true, nil, legacyErr
	}
	if rawLegacy, ok := legacyValue[string(field)]; ok {
		current, parseErr := strconv.ParseInt(rawLegacy, 10, 64)
		if parseErr != nil {
			return 0, false, nil, errors.New("ERR hash value is not an integer")
		}
		return current, false, legacyValue, nil
	}
	return 0, true, legacyValue, nil
}

// hincrbyWithMigration handles the HINCRBY case where a legacy JSON blob must be migrated
// atomically with the increment operation.
func (r *RedisServer) hincrbyWithMigration(ctx context.Context, key, fieldKey []byte, readTS, commitTS uint64, current int64, isNewField bool, increment int64) (int64, error) {
	migrationElems, migErr := r.buildHashLegacyMigrationElems(ctx, key, readTS)
	if migErr != nil {
		return 0, migErr
	}
	current += increment
	newVal := strconv.FormatInt(current, 10)
	elems := make([]*kv.Elem[kv.OP], 0, len(migrationElems)+setWideColOverhead)
	elems = append(elems, migrationElems...)
	elems = append(elems, &kv.Elem[kv.OP]{Op: kv.Put, Key: fieldKey, Value: []byte(newVal)})
	if isNewField {
		deltaVal := store.MarshalHashMetaDelta(store.HashMetaDelta{LenDelta: 1})
		elems = append(elems, &kv.Elem[kv.OP]{
			Op:    kv.Put,
			Key:   store.HashMetaDeltaKey(key, commitTS, 0),
			Value: deltaVal,
		})
	}
	_, dispatchErr := r.coordinator.Dispatch(ctx, &kv.OperationGroup[kv.OP]{
		IsTxn:    true,
		StartTS:  normalizeStartTS(readTS),
		CommitTS: commitTS,
		Elems:    elems,
	})
	return current, cockerrors.WithStack(dispatchErr)
}

func (r *RedisServer) hincrbyTxn(ctx context.Context, key, field []byte, increment int64) (int64, error) {
	readTS := r.readTS()
	typ, err := r.keyTypeAtExpect(ctx, key, readTS, redisTypeHash)
	if err != nil {
		return 0, err
	}
	if typ != redisTypeNone && typ != redisTypeHash {
		return 0, wrongTypeError()
	}

	commitTS := r.coordinator.Clock().Next()
	fieldKey := store.HashFieldKey(key, field)

	current, isNewField, legacyValue, err := r.readHashFieldInt(ctx, key, field, readTS)
	if err != nil {
		return 0, err
	}

	// If a legacy blob exists, migrate it atomically with the increment.
	if len(legacyValue) > 0 {
		return r.hincrbyWithMigration(ctx, key, fieldKey, readTS, commitTS, current, isNewField, increment)
	}

	current += increment
	newVal := strconv.FormatInt(current, 10)
	elems := make([]*kv.Elem[kv.OP], 0, setWideColOverhead)
	elems = append(elems, &kv.Elem[kv.OP]{Op: kv.Put, Key: fieldKey, Value: []byte(newVal)})
	if isNewField {
		deltaVal := store.MarshalHashMetaDelta(store.HashMetaDelta{LenDelta: 1})
		elems = append(elems, &kv.Elem[kv.OP]{
			Op:    kv.Put,
			Key:   store.HashMetaDeltaKey(key, commitTS, 0),
			Value: deltaVal,
		})
	}
	_, dispatchErr := r.coordinator.Dispatch(ctx, &kv.OperationGroup[kv.OP]{
		IsTxn:    true,
		StartTS:  normalizeStartTS(readTS),
		CommitTS: commitTS,
		Elems:    elems,
	})
	return current, cockerrors.WithStack(dispatchErr)
}

func (r *RedisServer) incr(conn redcon.Conn, cmd redcon.Command) {
	if r.proxyToLeader(conn, cmd, cmd.Args[1]) {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), redisDispatchTimeout)
	defer cancel()
	var current int64
	if err := r.retryRedisWrite(ctx, func() error {
		readTS := r.readTS()
		typ, err := r.keyTypeAt(context.Background(), cmd.Args[1], readTS)
		if err != nil {
			return err
		}
		if typ != redisTypeNone && typ != redisTypeString {
			return wrongTypeError()
		}

		current = 0
		var existingTTL *time.Time
		if typ == redisTypeString {
			raw, ttl, err := r.readRedisStringAt(cmd.Args[1], readTS)
			if err != nil {
				return err
			}
			existingTTL = ttl
			current, err = strconv.ParseInt(string(raw), 10, 64)
			if err != nil {
				return fmt.Errorf("ERR value is not an integer or out of range")
			}
		}
		current++

		// INCR preserves any existing TTL (Redis semantics).
		encoded := encodeRedisStr([]byte(strconv.FormatInt(current, 10)), existingTTL)
		elems := []*kv.Elem[kv.OP]{
			{Op: kv.Put, Key: redisStrKey(cmd.Args[1]), Value: encoded},
		}
		if existingTTL != nil {
			elems = append(elems, &kv.Elem[kv.OP]{Op: kv.Put, Key: redisTTLKey(cmd.Args[1]), Value: encodeRedisTTL(*existingTTL)})
		} else {
			// Defensively clear any stale/legacy scan index entry so the sweeper
			// cannot later expire a now-persistent key.
			elems = append(elems, &kv.Elem[kv.OP]{Op: kv.Del, Key: redisTTLKey(cmd.Args[1])})
		}
		return r.dispatchElems(ctx, true, readTS, elems)
	}); err != nil {
		conn.WriteError(err.Error())
		return
	}
	conn.WriteInt64(current)
}

func (r *RedisServer) hgetall(conn redcon.Conn, cmd redcon.Command) {
	if r.proxyToLeader(conn, cmd, cmd.Args[1]) {
		return
	}
	readTS := r.readTS()
	typ, err := r.keyTypeAtExpect(context.Background(), cmd.Args[1], readTS, redisTypeHash)
	if err != nil {
		conn.WriteError(err.Error())
		return
	}
	if typ == redisTypeNone {
		conn.WriteArray(0)
		return
	}
	if typ != redisTypeHash {
		conn.WriteError(wrongTypeMessage)
		return
	}

	value, err := r.loadHashAt(context.Background(), cmd.Args[1], readTS)
	if err != nil {
		conn.WriteError(err.Error())
		return
	}
	fields := make([]string, 0, len(value))
	for field := range value {
		fields = append(fields, field)
	}
	sort.Strings(fields)
	conn.WriteArray(len(fields) * redisPairWidth)
	for _, field := range fields {
		conn.WriteBulkString(field)
		conn.WriteBulkString(value[field])
	}
}

type zaddFlags struct {
	nx bool // only add new elements
	xx bool // only update existing elements
	gt bool // only update when new score > current score
	lt bool // only update when new score < current score
}

func parseZAddFlags(args [][]byte) (zaddFlags, int, error) {
	var flags zaddFlags
	i := 2
	for i < len(args) {
		if !flags.applyFlag(strings.ToUpper(string(args[i]))) {
			break
		}
		i++
	}
	if err := flags.validate(); err != nil {
		return zaddFlags{}, 0, err
	}
	return flags, i, nil
}

func (f *zaddFlags) applyFlag(name string) bool {
	switch name {
	case "NX":
		f.nx = true
	case "XX":
		f.xx = true
	case "GT":
		f.gt = true
	case "LT":
		f.lt = true
	default:
		return false
	}
	return true
}

func (f zaddFlags) allows(exists bool, oldScore, newScore float64) bool {
	if (f.nx && exists) || (f.xx && !exists) {
		return false
	}
	return !exists || f.scoreAllowed(oldScore, newScore)
}

func (f zaddFlags) scoreAllowed(oldScore, newScore float64) bool {
	if f.gt && newScore <= oldScore {
		return false
	}
	if f.lt && newScore >= oldScore {
		return false
	}
	return true
}

func (f zaddFlags) validate() error {
	if f.nx && f.xx {
		return fmt.Errorf("ERR XX and NX options at the same time are not compatible")
	}
	if f.nx && (f.gt || f.lt) {
		return fmt.Errorf("ERR GT, LT, and NX options at the same time are not compatible")
	}
	return nil
}

type zaddPair struct {
	score  float64
	member string
}

func parseZAddPairs(remaining [][]byte) ([]zaddPair, error) {
	pairs := make([]zaddPair, 0, len(remaining)/redisPairWidth)
	for i := 0; i < len(remaining); i += redisPairWidth {
		score, err := strconv.ParseFloat(string(remaining[i]), 64)
		if err != nil {
			return nil, fmt.Errorf("parse zadd score: %w", err)
		}
		pairs = append(pairs, zaddPair{score: score, member: string(remaining[i+1])})
	}
	return pairs, nil
}

func (r *RedisServer) zadd(conn redcon.Conn, cmd redcon.Command) {
	if r.proxyToLeader(conn, cmd, cmd.Args[1]) {
		return
	}
	flags, pairStart, err := parseZAddFlags(cmd.Args)
	if err != nil {
		conn.WriteError(err.Error())
		return
	}
	remaining := cmd.Args[pairStart:]
	if len(remaining) == 0 || len(remaining)%redisPairWidth != 0 {
		conn.WriteError("ERR syntax error")
		return
	}
	pairs, err := parseZAddPairs(remaining)
	if err != nil {
		conn.WriteError(err.Error())
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), redisDispatchTimeout)
	defer cancel()
	var added int
	if err := r.retryRedisWrite(ctx, func() error {
		var err error
		added, err = r.zaddTxn(ctx, cmd.Args[1], flags, pairs)
		return err
	}); err != nil {
		conn.WriteError(err.Error())
		return
	}
	conn.WriteInt(added)
}

// buildZSetMigrationView extracts member→score from ZSet migration Put elems
// so that applyZAddPair can see migrated members without a store round-trip.
// Returns a map from member name to score; absent members were not migrated.
func buildZSetMigrationView(migrationElems []*kv.Elem[kv.OP], key []byte) map[string]float64 {
	view := make(map[string]float64)
	for _, elem := range migrationElems {
		if elem.Op != kv.Put {
			continue
		}
		m := store.ExtractZSetMemberName(elem.Key, key)
		if m == nil {
			continue
		}
		score, err := store.UnmarshalZSetScore(elem.Value)
		if err == nil {
			view[string(m)] = score
		}
	}
	return view
}

// resolveZSetMemberScore returns the current score and existence for a ZSet
// member. It checks inTxnView first (covers migration elems and earlier pairs
// in the same ZADD call), then falls back to a store GetAt.
func (r *RedisServer) resolveZSetMemberScore(ctx context.Context, memberKey []byte, member string, readTS uint64, inTxnView map[string]float64) (score float64, exists bool, err error) {
	if s, ok := inTxnView[member]; ok {
		return s, true, nil
	}
	raw, getErr := r.store.GetAt(ctx, memberKey, readTS)
	if getErr == nil {
		s, unmarshalErr := store.UnmarshalZSetScore(raw)
		if unmarshalErr != nil {
			return 0, false, cockerrors.WithStack(unmarshalErr)
		}
		return s, true, nil
	}
	if !cockerrors.Is(getErr, store.ErrKeyNotFound) {
		return 0, false, cockerrors.WithStack(getErr)
	}
	return 0, false, nil
}

// applyZAddPair processes one ZADD pair against the wide-column store: reads the
// existing member score (if any), checks the ZADD flags, emits del-old-score /
// put-member / put-score-index ops, and returns the updated elems, the add count
// (0 or 1), and the length delta (0 or +1).
// inTxnView provides an in-transaction view of member→score for members written
// in the same transaction (migration or earlier pairs); checked before GetAt so
// migrated and duplicate members are handled correctly.
func (r *RedisServer) applyZAddPair(ctx context.Context, key []byte, p zaddPair, flags zaddFlags, readTS uint64, elems []*kv.Elem[kv.OP], inTxnView map[string]float64) ([]*kv.Elem[kv.OP], int, int64, error) {
	memberKey := store.ZSetMemberKey(key, []byte(p.member))
	oldScore, memberExists, err := r.resolveZSetMemberScore(ctx, memberKey, p.member, readTS, inTxnView)
	if err != nil {
		return nil, 0, 0, err
	}
	if !flags.allows(memberExists, oldScore, p.score) {
		return elems, 0, 0, nil
	}
	if memberExists {
		elems = append(elems, &kv.Elem[kv.OP]{Op: kv.Del, Key: store.ZSetScoreKey(key, oldScore, []byte(p.member))})
	}
	elems = append(elems,
		&kv.Elem[kv.OP]{Op: kv.Put, Key: memberKey, Value: store.MarshalZSetScore(p.score)},
		&kv.Elem[kv.OP]{Op: kv.Put, Key: store.ZSetScoreKey(key, p.score, []byte(p.member)), Value: []byte{}},
	)
	// Update inTxnView so subsequent pairs (duplicates) see this write.
	inTxnView[p.member] = p.score
	if memberExists {
		return elems, 0, 0, nil
	}
	return elems, 1, 1, nil
}

func (r *RedisServer) zaddTxn(ctx context.Context, key []byte, flags zaddFlags, pairs []zaddPair) (int, error) {
	readTS := r.readTS()
	typ, err := r.keyTypeAtExpect(ctx, key, readTS, redisTypeZSet)
	if err != nil {
		return 0, err
	}
	if typ != redisTypeNone && typ != redisTypeZSet {
		return 0, wrongTypeError()
	}

	commitTS := r.coordinator.Clock().Next()

	migrationElems, err := r.buildZSetLegacyMigrationElems(ctx, key, readTS)
	if err != nil {
		return 0, err
	}
	// Capacity: each pair may produce 3 ops (del old score + put member + put score index),
	// plus migration elems and a delta key.
	elems := make([]*kv.Elem[kv.OP], 0, len(migrationElems)+len(pairs)*3+setWideColOverhead) //nolint:mnd // 3 ops per pair
	elems = append(elems, migrationElems...)

	// Seed the in-transaction view from migration elems so that migrated
	// members are not incorrectly counted as new by applyZAddPair.
	inTxnView := buildZSetMigrationView(migrationElems, key)

	// For large batches, mergeZSetBulkScores performs one prefix scan that
	// eliminates O(N) GetAt calls inside applyZAddPair; it is a no-op for
	// batches below wideColumnBulkScanThreshold.
	inTxnView, err = r.mergeZSetBulkScores(ctx, key, readTS, len(pairs), inTxnView)
	if err != nil {
		return 0, err
	}

	added := 0
	lenDelta := int64(0)
	for _, p := range pairs {
		var c int
		var d int64
		elems, c, d, err = r.applyZAddPair(ctx, key, p, flags, readTS, elems, inTxnView)
		if err != nil {
			return 0, err
		}
		added += c
		lenDelta += d
	}

	if len(elems) == 0 {
		return 0, nil
	}

	if lenDelta != 0 {
		deltaVal := store.MarshalZSetMetaDelta(store.ZSetMetaDelta{LenDelta: lenDelta})
		elems = append(elems, &kv.Elem[kv.OP]{
			Op:    kv.Put,
			Key:   store.ZSetMetaDeltaKey(key, commitTS, 0),
			Value: deltaVal,
		})
	}

	return added, r.dispatchAndSignalZSet(ctx, readTS, commitTS, elems, key)
}

// dispatchAndSignalZSet dispatches the elems through the coordinator
// and, on success, wakes any BZPOPMIN waiter on the same node.
// coordinator.Dispatch blocks until the FSM applies locally, so by
// the time Signal fires the new members are visible at the readTS
// the woken waiter will pick on its next iteration. Pulled out of
// zaddTxn / zincrbyTxn so the parents stay under the cyclop budget
// — the signal step would otherwise add an extra branch on the
// dispatch error path.
func (r *RedisServer) dispatchAndSignalZSet(
	ctx context.Context,
	readTS, commitTS uint64,
	elems []*kv.Elem[kv.OP],
	zsetKey []byte,
) error {
	_, err := r.coordinator.Dispatch(ctx, &kv.OperationGroup[kv.OP]{
		IsTxn:    true,
		StartTS:  normalizeStartTS(readTS),
		CommitTS: commitTS,
		Elems:    elems,
	})
	if err != nil {
		return cockerrors.WithStack(err)
	}
	r.zsetWaiters.Signal(zsetKey)
	return nil
}

// zincrbyTxn performs one attempt of ZINCRBY in wide-column format.
// Returns the new score after applying increment.
func (r *RedisServer) zincrbyTxn(ctx context.Context, key []byte, member string, increment float64) (float64, error) {
	readTS := r.readTS()
	typ, err := r.keyTypeAtExpect(ctx, key, readTS, redisTypeZSet)
	if err != nil {
		return 0, err
	}
	if typ != redisTypeNone && typ != redisTypeZSet {
		return 0, wrongTypeError()
	}

	memberKey := store.ZSetMemberKey(key, []byte(member))
	commitTS := r.coordinator.Clock().Next()

	migrationElems, migErr := r.buildZSetLegacyMigrationElems(ctx, key, readTS)
	if migErr != nil {
		return 0, migErr
	}

	// Check in-txn migration view before falling back to the store
	// (migrated keys are not yet visible at readTS).
	inTxnView := buildZSetMigrationView(migrationElems, key)
	oldScore, memberExists, err := r.resolveZSetMemberScore(ctx, memberKey, member, readTS, inTxnView)
	if err != nil {
		return 0, err
	}

	newScore := oldScore + increment
	if math.IsNaN(newScore) {
		return 0, errors.New("ERR resulting score is not a number (NaN)")
	}
	elems := make([]*kv.Elem[kv.OP], 0, len(migrationElems)+3) //nolint:mnd // del old score + put member + put score index
	elems = append(elems, migrationElems...)
	if memberExists {
		elems = append(elems, &kv.Elem[kv.OP]{Op: kv.Del, Key: store.ZSetScoreKey(key, oldScore, []byte(member))})
	}
	elems = append(elems,
		&kv.Elem[kv.OP]{Op: kv.Put, Key: memberKey, Value: store.MarshalZSetScore(newScore)},
		&kv.Elem[kv.OP]{Op: kv.Put, Key: store.ZSetScoreKey(key, newScore, []byte(member)), Value: []byte{}},
	)
	if !memberExists {
		deltaVal := store.MarshalZSetMetaDelta(store.ZSetMetaDelta{LenDelta: 1})
		elems = append(elems, &kv.Elem[kv.OP]{
			Op:    kv.Put,
			Key:   store.ZSetMetaDeltaKey(key, commitTS, 0),
			Value: deltaVal,
		})
	}
	if err := r.dispatchAndSignalZSet(ctx, readTS, commitTS, elems, key); err != nil {
		return 0, err
	}
	return newScore, nil
}

func (r *RedisServer) zincrby(conn redcon.Conn, cmd redcon.Command) {
	if r.proxyToLeader(conn, cmd, cmd.Args[1]) {
		return
	}
	increment, err := strconv.ParseFloat(string(cmd.Args[2]), 64)
	if err != nil {
		conn.WriteError(err.Error())
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), redisDispatchTimeout)
	defer cancel()
	var newScore float64
	if err := r.retryRedisWrite(ctx, func() error {
		var txnErr error
		newScore, txnErr = r.zincrbyTxn(ctx, cmd.Args[1], string(cmd.Args[3]), increment)
		return txnErr
	}); err != nil {
		conn.WriteError(err.Error())
		return
	}
	conn.WriteBulkString(formatRedisFloat(newScore))
}

func parseZRangeOptions(args [][]byte) (zrangeOptions, error) {
	opts := zrangeOptions{}
	for _, arg := range args {
		switch strings.ToUpper(string(arg)) {
		case "WITHSCORES":
			opts.withScores = true
		case "REV":
			opts.reverse = true
		default:
			return zrangeOptions{}, errors.New("ERR syntax error")
		}
	}
	return opts, nil
}

func reverseZSetEntries(entries []redisZSetEntry) {
	for i, j := 0, len(entries)-1; i < j; i, j = i+1, j-1 {
		entries[i], entries[j] = entries[j], entries[i]
	}
}

func writeZRangeReply(conn redcon.Conn, entries []redisZSetEntry, withScores bool) {
	if withScores {
		conn.WriteArray(len(entries) * redisPairWidth)
		for _, entry := range entries {
			conn.WriteBulkString(entry.Member)
			conn.WriteBulkString(formatRedisFloat(entry.Score))
		}
		return
	}

	conn.WriteArray(len(entries))
	for _, entry := range entries {
		conn.WriteBulkString(entry.Member)
	}
}

func removeZSetMembers(members map[string]float64, rawMembers [][]byte) int {
	removed := 0
	for _, member := range rawMembers {
		memberKey := string(member)
		if _, ok := members[memberKey]; ok {
			delete(members, memberKey)
			removed++
		}
	}
	return removed
}

func (r *RedisServer) persistZSetEntriesTxn(ctx context.Context, key []byte, readTS uint64, entries []redisZSetEntry) error {
	if len(entries) == 0 {
		elems, _, err := r.deleteLogicalKeyElems(ctx, key, readTS)
		if err != nil {
			return err
		}
		return r.dispatchElems(ctx, true, readTS, elems)
	}
	payload, err := marshalZSetValue(redisZSetValue{Entries: entries})
	if err != nil {
		return err
	}
	return r.dispatchElems(ctx, true, readTS, []*kv.Elem[kv.OP]{
		{Op: kv.Put, Key: redisZSetKey(key), Value: payload},
	})
}

func (r *RedisServer) zrange(conn redcon.Conn, cmd redcon.Command) {
	if r.proxyToLeader(conn, cmd, cmd.Args[1]) {
		return
	}
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

	opts, err := parseZRangeOptions(cmd.Args[4:])
	if err != nil {
		conn.WriteError(err.Error())
		return
	}

	r.zrangeRead(conn, cmd.Args[1], start, stop, opts)
}

func (r *RedisServer) zrangeRead(conn redcon.Conn, key []byte, start, stop int, opts zrangeOptions) {
	readTS := r.readTS()
	typ, err := r.keyTypeAtExpect(context.Background(), key, readTS, redisTypeZSet)
	if err != nil {
		conn.WriteError(err.Error())
		return
	}
	if typ == redisTypeNone {
		conn.WriteArray(0)
		return
	}
	if typ != redisTypeZSet {
		conn.WriteError(wrongTypeMessage)
		return
	}

	value, _, err := r.loadZSetAt(context.Background(), key, readTS)
	if err != nil {
		conn.WriteError(err.Error())
		return
	}
	entries := append([]redisZSetEntry(nil), value.Entries...)
	if opts.reverse {
		reverseZSetEntries(entries)
	}
	s, e := normalizeRankRange(start, stop, len(entries))
	if e < s {
		conn.WriteArray(0)
		return
	}
	writeZRangeReply(conn, entries[s:e+1], opts.withScores)
}

func (r *RedisServer) zrem(conn redcon.Conn, cmd redcon.Command) {
	if r.proxyToLeader(conn, cmd, cmd.Args[1]) {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), redisDispatchTimeout)
	defer cancel()
	var removed int
	if err := r.retryRedisWrite(ctx, func() error {
		readTS := r.readTS()
		typ, err := r.keyTypeAtExpect(ctx, cmd.Args[1], readTS, redisTypeZSet)
		if err != nil {
			return err
		}
		if typ == redisTypeNone {
			removed = 0
			return nil
		}
		if typ != redisTypeZSet {
			return wrongTypeError()
		}
		value, _, err := r.loadZSetAt(context.Background(), cmd.Args[1], readTS)
		if err != nil {
			return err
		}
		members := zsetEntriesToMap(value.Entries)
		removed = removeZSetMembers(members, cmd.Args[2:])
		if removed == 0 {
			return nil
		}
		return r.persistZSetEntriesTxn(ctx, cmd.Args[1], readTS, zsetMapToEntries(members))
	}); err != nil {
		conn.WriteError(err.Error())
		return
	}
	conn.WriteInt(removed)
}

func (r *RedisServer) zremrangebyrank(conn redcon.Conn, cmd redcon.Command) {
	if r.proxyToLeader(conn, cmd, cmd.Args[1]) {
		return
	}
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

	ctx, cancel := context.WithTimeout(context.Background(), redisDispatchTimeout)
	defer cancel()
	var removed int
	if err := r.retryRedisWrite(ctx, func() error {
		readTS := r.readTS()
		typ, err := r.keyTypeAtExpect(ctx, cmd.Args[1], readTS, redisTypeZSet)
		if err != nil {
			return err
		}
		if typ == redisTypeNone {
			removed = 0
			return nil
		}
		if typ != redisTypeZSet {
			return wrongTypeError()
		}
		value, _, err := r.loadZSetAt(context.Background(), cmd.Args[1], readTS)
		if err != nil {
			return err
		}
		s, e := normalizeRankRange(start, stop, len(value.Entries))
		if e < s {
			removed = 0
			return nil
		}
		remaining := append([]redisZSetEntry{}, value.Entries[:s]...)
		remaining = append(remaining, value.Entries[e+1:]...)
		removed = e - s + 1
		return r.persistZSetEntriesTxn(ctx, cmd.Args[1], readTS, remaining)
	}); err != nil {
		conn.WriteError(err.Error())
		return
	}
	conn.WriteInt(removed)
}

// tryBZPopMinWithMode runs one BZPOPMIN attempt against key. The
// fast flag selects keyTypeAtExpectFast (no slow-path fallback, no
// wrongType detection) when true; the caller MUST guarantee that the
// only mutations since the previous full check are signalling writes
// (ZADD/ZINCRBY for zsetWaiters). bzpopminWaitLoop enforces this by
// running fast=false on the first iteration and after every
// fallback-timer wake or wall-time-bounded re-arm.
func (r *RedisServer) tryBZPopMinWithMode(key []byte, fast bool) (*bzpopminResult, error) {
	ctx, cancel := context.WithTimeout(context.Background(), redisDispatchTimeout)
	defer cancel()
	var result *bzpopminResult
	err := r.retryRedisWrite(ctx, func() error {
		readTS := r.readTS()
		var typ redisValueType
		var err error
		if fast {
			typ, err = r.keyTypeAtExpectFast(ctx, key, readTS, redisTypeZSet)
		} else {
			typ, err = r.keyTypeAtExpect(ctx, key, readTS, redisTypeZSet)
		}
		if err != nil {
			return err
		}
		if typ == redisTypeNone {
			result = nil
			return nil
		}
		if typ != redisTypeZSet {
			return wrongTypeError()
		}
		value, _, err := r.loadZSetAt(context.Background(), key, readTS)
		if err != nil {
			return err
		}
		if len(value.Entries) == 0 {
			result = nil
			return nil
		}
		popped := value.Entries[0]
		remaining := append([]redisZSetEntry(nil), value.Entries[1:]...)

		// Detect wide-column storage.
		memberPrefix := store.ZSetMemberScanPrefix(key)
		memberEnd := store.PrefixScanEnd(memberPrefix)
		probeKVs, probeErr := r.store.ScanAt(ctx, memberPrefix, memberEnd, 1, readTS)
		if probeErr != nil {
			return cockerrors.WithStack(probeErr)
		}
		isWide := len(probeKVs) > 0

		if err := r.persistBZPopMinResult(ctx, key, readTS, popped, remaining, isWide); err != nil {
			return err
		}
		result = &bzpopminResult{key: key, entry: popped}
		return nil
	})
	return result, err
}

func (r *RedisServer) persistBZPopMinResult(ctx context.Context, key []byte, readTS uint64, popped redisZSetEntry, remaining []redisZSetEntry, isWide bool) error {
	if len(remaining) == 0 {
		elems, _, err := r.deleteLogicalKeyElems(ctx, key, readTS)
		if err != nil {
			return err
		}
		return r.dispatchElems(ctx, true, readTS, elems)
	}
	if isWide {
		// Wide-column: delete the popped member key + score index, emit delta -1.
		commitTS := r.coordinator.Clock().Next()
		deltaVal := store.MarshalZSetMetaDelta(store.ZSetMetaDelta{LenDelta: -1})
		elems := []*kv.Elem[kv.OP]{
			{Op: kv.Del, Key: store.ZSetMemberKey(key, []byte(popped.Member))},
			{Op: kv.Del, Key: store.ZSetScoreKey(key, popped.Score, []byte(popped.Member))},
			{Op: kv.Put, Key: store.ZSetMetaDeltaKey(key, commitTS, 0), Value: deltaVal},
		}
		_, dispatchErr := r.coordinator.Dispatch(ctx, &kv.OperationGroup[kv.OP]{
			IsTxn:    true,
			StartTS:  normalizeStartTS(readTS),
			CommitTS: commitTS,
			Elems:    elems,
		})
		return cockerrors.WithStack(dispatchErr)
	}
	// Legacy blob: write back all remaining entries.
	payload, err := marshalZSetValue(redisZSetValue{Entries: remaining})
	if err != nil {
		return err
	}
	return r.dispatchElems(ctx, true, readTS, []*kv.Elem[kv.OP]{
		{Op: kv.Put, Key: redisZSetKey(key), Value: payload},
	})
}

func (r *RedisServer) bzpopmin(conn redcon.Conn, cmd redcon.Command) {
	if r.proxyToLeader(conn, cmd, cmd.Args[1]) {
		return
	}
	timeoutSeconds, err := strconv.ParseFloat(string(cmd.Args[len(cmd.Args)-1]), 64)
	if err != nil || timeoutSeconds < 0 {
		conn.WriteError("ERR timeout is not a float or out of range")
		return
	}

	// timeout=0 means infinite wait in Redis; cap at redisDispatchTimeout to prevent goroutine leak.
	if timeoutSeconds == 0 {
		timeoutSeconds = redisDispatchTimeout.Seconds()
	}
	deadline := time.Now().Add(time.Duration(timeoutSeconds * float64(time.Second)))

	keys := cmd.Args[1 : len(cmd.Args)-1]
	r.bzpopminWaitLoop(conn, keys, deadline)
}

// bzpopminWaitLoop runs the BLOCK-window wait loop. Extracted from
// bzpopmin so the parent function stays under the cyclop budget.
// Uses an event-driven signal from the in-process ZADD / ZINCRBY
// path with a fallback timer for paths that bypass the signal.
//
// Registration happens BEFORE the first tryBZPopMin so a signal that
// fires between the check and the wait cannot be lost: the buffered
// channel holds it, and the next select wakes immediately.
func (r *RedisServer) bzpopminWaitLoop(conn redcon.Conn, keys [][]byte, deadline time.Time) {
	handlerCtx := r.handlerContext()
	w, release := r.zsetWaiters.Register(keys)
	defer release()
	// fast tracks whether the next iteration may skip the wrongType
	// slow probe. The first iteration is always full so an existing
	// wrongType key surfaces an immediate WRONGTYPE; subsequent
	// iterations after a signal-driven wake skip the wrongType
	// detection because zsetWaiters.Signal only fires for ZADD /
	// ZINCRBY (neither of which can introduce a wrongType).
	//
	// lastFullCheck wall-time-bounds how long the fast mode can stay
	// active under sustained signal pressure. Without this gate, a
	// hot key whose zsetWaiters.Signal fires faster than each
	// bzpopminTryAllKeys round finishes can keep waiterC perpetually
	// full, starving the fallback timer and letting a wrongType
	// write on a co-registered key (multi-key BZPOPMIN) go
	// undetected for the entire BLOCK window. Demoting `fast` back
	// to false after redisBlockWaitFallback elapses since the last
	// full check restores the #666 ceiling: WRONGTYPE on any
	// registered key surfaces within ~one fallback interval (100 ms)
	// regardless of signal rate. See
	// TestRedis_BZPopMinDetectsWrongTypeUnderSignalLoad for the
	// regression scenario.
	fast := false
	lastFullCheck := time.Now()
	for {
		if handlerCtx.Err() != nil {
			conn.WriteNull()
			return
		}
		if r.bzpopminTryAllKeys(conn, keys, fast) {
			return
		}
		if !fast {
			lastFullCheck = time.Now()
		}
		if !time.Now().Before(deadline) {
			conn.WriteNull()
			return
		}
		signaled := waitForBlockedCommandUpdate(handlerCtx, w.C, deadline)
		fast = signaled && time.Since(lastFullCheck) < redisBlockWaitFallback
	}
}

// bzpopminTryAllKeys runs one tryBZPopMinWithMode pass across keys.
// Returns true when a result was written (success or terminal error)
// and the caller should stop the loop, false to continue waiting.
// The fast flag is forwarded to tryBZPopMinWithMode: true selects
// the signal-driven-wake path (skips wrongType detection); false
// selects the full check.
func (r *RedisServer) bzpopminTryAllKeys(conn redcon.Conn, keys [][]byte, fast bool) bool {
	for _, key := range keys {
		result, err := r.tryBZPopMinWithMode(key, fast)
		if err != nil {
			conn.WriteError(err.Error())
			return true
		}
		if result == nil {
			continue
		}
		conn.WriteArray(redisTripletWidth)
		conn.WriteBulk(result.key)
		conn.WriteBulkString(result.entry.Member)
		conn.WriteBulkString(formatRedisFloat(result.entry.Score))
		return true
	}
	return false
}

// waitForBlockedCommandUpdate blocks until one of: a write signal
// arrives, the fallback poll tick fires, the parent handlerCtx is
// cancelled, or the BLOCK deadline elapses — whichever happens first.
// The fallback bounds latency for write paths that do not signal (Lua
// flush, follower-applied entries); it cannot exceed the remaining
// BLOCK window so the deadline branch in the caller's loop top always
// gets a chance to fire when the BLOCK expires. Shared by every
// blocking-command wait loop (XREAD BLOCK, BZPOPMIN today; BLPOP /
// BRPOP / BLMOVE in follow-ups) — the keyWaiterRegistry that produces
// waiterC is per-domain (streamWaiters vs zsetWaiters), but the
// timer-and-select shape is identical.
//
// Returns true iff the wake came from waiterC (i.e., a producer
// Signal). False on fallback-timer fire or handlerCtx cancellation.
// Callers that have a signal-implied invariant (e.g., "only ZADD /
// ZINCRBY fires zsetWaiters.Signal") can use the return value to
// pick a faster re-check on the next iteration; fallback wakes
// always need the full check because writes that bypass Signal
// (Lua flush, follower-applied entries, wrongType-introducing
// commands) only become observable through the timer branch.
func waitForBlockedCommandUpdate(handlerCtx context.Context, waiterC <-chan struct{}, deadline time.Time) bool {
	fallback := redisBlockWaitFallback
	if remaining := time.Until(deadline); remaining < fallback {
		fallback = remaining
	}
	timer := time.NewTimer(fallback)
	defer func() {
		if !timer.Stop() {
			// The timer either fired (its case won and the channel
			// was drained inline by select) or is still buffering
			// the tick (waiter / handlerCtx won the race); drain
			// the channel non-blocking so timer GC is clean.
			select {
			case <-timer.C:
			default:
			}
		}
	}()
	select {
	case <-waiterC:
		return true
	case <-timer.C:
		return false
	case <-handlerCtx.Done():
		return false
	}
}

func (r *RedisServer) lpush(conn redcon.Conn, cmd redcon.Command) {
	r.listPushCmd(conn, cmd, r.listLPush, r.proxyLPush)
}

func (r *RedisServer) ltrim(conn redcon.Conn, cmd redcon.Command) {
	if r.proxyToLeader(conn, cmd, cmd.Args[1]) {
		return
	}
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
	ctx, cancel := context.WithTimeout(context.Background(), redisDispatchTimeout)
	defer cancel()
	if err := r.retryRedisWrite(ctx, func() error {
		readTS := r.readTS()
		typ, err := r.keyTypeAt(context.Background(), cmd.Args[1], readTS)
		if err != nil {
			return err
		}
		if typ == redisTypeNone {
			return nil
		}
		if typ != redisTypeList {
			return wrongTypeError()
		}
		current, err := r.listValuesAt(context.Background(), cmd.Args[1], readTS)
		if err != nil {
			return err
		}
		s, e := normalizeRankRange(start, stop, len(current))
		trimmed := []string{}
		if e >= s {
			trimmed = append(trimmed, current[s:e+1]...)
		}
		return r.rewriteListTxn(ctx, cmd.Args[1], readTS, trimmed)
	}); err != nil {
		conn.WriteError(err.Error())
		return
	}
	conn.WriteString("OK")
}

func (r *RedisServer) lindex(conn redcon.Conn, cmd redcon.Command) {
	if r.proxyToLeader(conn, cmd, cmd.Args[1]) {
		return
	}
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

func parseXAddMaxLen(args [][]byte) (int, int, error) {
	argIndex := redisPairWidth
	if len(args) < 5 || !strings.EqualFold(string(args[argIndex]), "MAXLEN") {
		return -1, argIndex, nil
	}

	argIndex++
	if argIndex < len(args) && string(args[argIndex]) == "~" {
		argIndex++
	}
	if argIndex >= len(args) {
		return 0, 0, errors.New("ERR syntax error")
	}

	maxLen, err := strconv.Atoi(string(args[argIndex]))
	if err != nil || maxLen < 0 {
		return 0, 0, errors.New("ERR syntax error")
	}
	return maxLen, argIndex + 1, nil
}

func parseXAddFields(args [][]byte, argIndex int) ([]string, error) {
	if argIndex >= len(args) {
		return nil, errors.New("ERR syntax error")
	}
	if (len(args)-argIndex)%redisPairWidth != 0 {
		return nil, errors.New("ERR wrong number of arguments for 'XADD' command")
	}

	fields := make([]string, 0, len(args)-argIndex)
	for _, arg := range args[argIndex:] {
		fields = append(fields, string(arg))
	}
	return fields, nil
}

func parseXAddRequest(args [][]byte) (xaddRequest, error) {
	maxLen, argIndex, err := parseXAddMaxLen(args)
	if err != nil {
		return xaddRequest{}, err
	}
	if argIndex >= len(args) {
		return xaddRequest{}, errors.New("ERR syntax error")
	}
	fields, err := parseXAddFields(args, argIndex+1)
	if err != nil {
		return xaddRequest{}, err
	}
	return xaddRequest{maxLen: maxLen, id: string(args[argIndex]), fields: fields}, nil
}

// nextXAddID computes the ID the next XADD should assign.
//
// hasLast reports whether the stream currently tracks a "last" ID (i.e. at
// least one XADD has ever succeeded). last{Ms,Seq} must be the highest ID
// the stream has ever seen — not merely the current tail — so that XADD '*'
// stays strictly monotonic even after XTRIM removes the current tail.
func nextXAddID(hasLast bool, lastMs, lastSeq uint64, requested string) (string, error) {
	if requested != "*" {
		requestedID, requestedValid := tryParseRedisStreamID(requested)
		if !requestedValid {
			return "", errors.New("ERR Invalid stream ID specified as stream command argument")
		}
		// Redis rejects IDs <= 0-0 unconditionally; a stream entry with
		// ID "0-0" is unreachable via XREAD ... 0 (which means "after 0-0").
		if requestedID.ms == 0 && requestedID.seq == 0 {
			return "", errors.New("ERR The ID specified in XADD must be greater than 0-0")
		}
		if hasLast && compareStreamIDs(requestedID.ms, requestedID.seq, lastMs, lastSeq) <= 0 {
			return "", errors.New("ERR The ID specified in XADD is equal or smaller than the target stream top item")
		}
		return requested, nil
	}
	return autoXAddID(safeUnixMilliToUint64(time.Now().UnixMilli()), hasLast, lastMs, lastSeq)
}

// autoXAddID resolves XADD '*' to a concrete stream ID given a wall-clock
// nowMs. Pulled out of nextXAddID so the auto-ID branch is testable
// without depending on time.Now() — the only un-injectable dependency is
// already isolated in the caller.
//
// Two corner cases the caller cannot rely on the wall clock to avoid:
//
//   - nowMs == 0 on a fresh stream (!hasLast). A naive "<nowMs>-0" reply
//     yields "0-0", which Redis explicitly rejects as a stream ID and
//     which XREAD ... 0 would treat as the empty after-marker. Bump the
//     seq to 1 so the first auto-generated entry is "0-1" — strictly
//     greater than 0-0 and reachable via XREAD ... 0. (This case fires
//     only when safeUnixMilliToUint64 clamped a pre-epoch clock to 0;
//     under any sane clock, nowMs is well above 0.)
//
//   - nowMs <= lastMs. Advance past lastMs/lastSeq via bumpStreamID so
//     the stream stays strictly monotonic even across a backwards clock
//     step or a corrupted meta where lastMs is far in the future.
func autoXAddID(nowMs uint64, hasLast bool, lastMs, lastSeq uint64) (string, error) {
	if !hasLast || nowMs > lastMs {
		seq := uint64(0)
		if nowMs == 0 {
			seq = 1
		}
		return strconv.FormatUint(nowMs, 10) + "-" + strconv.FormatUint(seq, 10), nil
	}
	// Either nowMs == lastMs (same millisecond), or lastMs is in the future
	// (monotonic guarantee across a backwards clock step or a corrupted
	// meta). Advance past lastMs-lastSeq via bumpStreamID; if the ID space
	// is exhausted, surface an error rather than wrap to 0.
	ms, seq, err := bumpStreamID(lastMs, lastSeq)
	if err != nil {
		return "", err
	}
	return strconv.FormatUint(ms, 10) + "-" + strconv.FormatUint(seq, 10), nil
}

// safeUnixMilliToUint64 returns ms as uint64, clamping any negative value
// (caused by a system clock set before the Unix epoch) to 0. Without this
// clamp, a direct uint64 cast of a negative int64 would yield a value
// near math.MaxUint64, which would then make nextXAddID's "future-ms"
// branch chase that pathological value forever — effectively wedging
// every subsequent XADD '*' on the stream until the clock recovers.
// The lastMs/lastSeq monotonic guarantee carries the stream forward
// from there via bumpStreamID.
func safeUnixMilliToUint64(ms int64) uint64 {
	if ms < 0 {
		return 0
	}
	return uint64(ms) //nolint:gosec // negative values handled above
}

// bumpStreamID returns the strictly-greater successor of (ms, seq) within
// the uint64-uint64 stream ID space. Bumps seq; on seq overflow carries
// to ms+1, seq=0; on ms overflow returns an error (no representable
// successor) instead of wrapping to 0-0, which would produce a duplicate
// or non-monotonic ID.
func bumpStreamID(ms, seq uint64) (uint64, uint64, error) {
	switch {
	case seq < ^uint64(0):
		return ms, seq + 1, nil
	case ms < ^uint64(0):
		return ms + 1, 0, nil
	default:
		return 0, 0, errors.New("ERR The stream has exhausted the ID space")
	}
}

func compareStreamIDs(lms, lseq, rms, rseq uint64) int {
	switch {
	case lms < rms:
		return -1
	case lms > rms:
		return 1
	case lseq < rseq:
		return -1
	case lseq > rseq:
		return 1
	default:
		return 0
	}
}

func (r *RedisServer) xadd(conn redcon.Conn, cmd redcon.Command) {
	if r.proxyToLeader(conn, cmd, cmd.Args[1]) {
		return
	}
	req, err := parseXAddRequest(cmd.Args)
	if err != nil {
		conn.WriteError(err.Error())
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), redisDispatchTimeout)
	defer cancel()
	var id string
	if err := r.retryRedisWrite(ctx, func() error {
		id, err = r.xaddTxn(ctx, cmd.Args[1], req)
		return err
	}); err != nil {
		conn.WriteError(err.Error())
		return
	}
	conn.WriteBulkString(id)
}

func (r *RedisServer) xaddTxn(ctx context.Context, key []byte, req xaddRequest) (string, error) {
	readTS := r.readTS()
	typ, err := r.keyTypeAtExpect(ctx, key, readTS, redisTypeStream)
	if err != nil {
		return "", err
	}
	if typ != redisTypeNone && typ != redisTypeStream {
		return "", wrongTypeError()
	}

	legacyCleanup, meta, metaFound, err := r.streamWriteBase(ctx, key, readTS)
	if err != nil {
		return "", err
	}

	id, parsedID, err := resolveXAddID(meta, metaFound, req.id)
	if err != nil {
		return "", err
	}

	if err := xaddEnforceMaxWideColumn(key, meta.Length, req.maxLen); err != nil {
		return "", err
	}

	entryValue, err := marshalStreamEntry(newRedisStreamEntry(id, req.fields))
	if err != nil {
		return "", err
	}

	// Capacity hint covers: optional legacy-cleanup Del + one entry Put +
	// one meta Put + the trim Dels. legacyCleanup is at most one element,
	// and only non-empty on the very first write against a stream whose
	// pre-migration blob is still on disk.
	const xaddFixedElemCount = 2
	elems := make([]*kv.Elem[kv.OP], 0,
		len(legacyCleanup)+xaddFixedElemCount+estimateXAddTrimCount(req.maxLen, meta.Length))
	elems = append(elems, legacyCleanup...)
	elems = append(elems, &kv.Elem[kv.OP]{
		Op:    kv.Put,
		Key:   store.StreamEntryKey(key, parsedID.ms, parsedID.seq),
		Value: entryValue,
	})

	nextLen, trim, err := r.xaddTrimIfNeeded(ctx, key, readTS, req.maxLen, meta.Length+1)
	if err != nil {
		return "", err
	}
	elems = append(elems, trim...)
	elems = appendMaxLenZeroSelfDel(elems, req.maxLen, key, parsedID)

	metaBytes, err := store.MarshalStreamMeta(store.StreamMeta{
		Length:  nextLen,
		LastMs:  parsedID.ms,
		LastSeq: parsedID.seq,
	})
	if err != nil {
		return "", cockerrors.WithStack(err)
	}
	elems = append(elems, &kv.Elem[kv.OP]{Op: kv.Put, Key: store.StreamMetaKey(key), Value: metaBytes})

	return id, r.dispatchAndSignalStream(ctx, true, readTS, elems, key)
}

// dispatchAndSignalStream dispatches the elems through the coordinator
// and, on success, wakes any XREAD BLOCK waiter on the same node.
// dispatchElems blocks until the FSM applies locally, so by the time
// Signal fires the new entries are visible at the readTS the woken
// waiter will pick on its next iteration. Pulled out of xaddTxn so the
// parent function stays under the cyclop budget — the signal step
// would otherwise add an extra branch on the dispatch error path.
func (r *RedisServer) dispatchAndSignalStream(
	ctx context.Context,
	isTxn bool,
	startTS uint64,
	elems []*kv.Elem[kv.OP],
	streamKey []byte,
) error {
	if err := r.dispatchElems(ctx, isTxn, startTS, elems); err != nil {
		return err
	}
	r.streamWaiters.Signal(streamKey)
	return nil
}

// appendMaxLenZeroSelfDel handles the MAXLEN 0 edge case. The trim loop
// runs scans at readTS and therefore cannot see the entry we just queued,
// so without this follow-up Del the freshly-added entry would survive
// while meta.Length said 0. The coordinator applies elems in order at a
// single commitTS, so appending Del after the Put tombstones it cleanly.
func appendMaxLenZeroSelfDel(elems []*kv.Elem[kv.OP], maxLen int, key []byte, parsedID redisStreamID) []*kv.Elem[kv.OP] {
	if maxLen != 0 {
		return elems
	}
	return append(elems, &kv.Elem[kv.OP]{
		Op:  kv.Del,
		Key: store.StreamEntryKey(key, parsedID.ms, parsedID.seq),
	})
}

// xaddEnforceMaxWideColumn rejects an XADD that would push the stream past
// maxWideColumnItems when no MAXLEN clause could rescue it. A MAXLEN >= 0
// and <= the cap keeps the committed length bounded even when meta.Length is
// already at the ceiling, so we only reject on the ungated path.
func xaddEnforceMaxWideColumn(key []byte, currentLength int64, maxLen int) error {
	if maxLen >= 0 && maxLen <= maxWideColumnItems {
		return nil
	}
	if currentLength < int64(maxWideColumnItems) {
		return nil
	}
	return cockerrors.Wrapf(ErrCollectionTooLarge,
		"stream %q would exceed %d entries", key, maxWideColumnItems)
}

// xaddTrimIfNeeded returns (finalLength, trimElems, err) for an XADD.
// estimateXAddTrimCount returns how many entries the XADD's MAXLEN trim
// will remove, or 0 when maxLen is unset or the current length fits under
// it. Used only as a capacity hint for the elems slice; the actual trim
// list is computed by xaddTrimIfNeeded.
func estimateXAddTrimCount(maxLen int, currentLength int64) int {
	if maxLen < 0 {
		return 0
	}
	nextLen := currentLength + 1
	if nextLen <= int64(maxLen) {
		return 0
	}
	// Compute in int64 and clamp at maxWideColumnItems. A capacity hint
	// of math.MaxInt would let make() try to allocate ~16 EiB on 64-bit
	// targets and either panic or OOM; capping at the wide-column ceiling
	// keeps the hint useful (saves slice growth in the common case) while
	// preventing pathological allocation when meta.Length is corrupted.
	// xaddTrimIfNeeded enforces the same cap on the actual trim count;
	// this hint just sizes the elems slice.
	diff := nextLen - int64(maxLen)
	if diff <= 0 {
		return 0
	}
	if diff > int64(maxWideColumnItems) {
		return maxWideColumnItems
	}
	return int(diff)
}

// When maxLen < 0 (unset) or the new length fits under it, no trim is
// emitted and trimElems is nil; otherwise Del operations for the oldest
// entries are returned and finalLength equals maxLen. All scans use the
// caller's ctx and readTS so the trim happens at the same MVCC snapshot
// as the write.
func (r *RedisServer) xaddTrimIfNeeded(
	ctx context.Context,
	key []byte,
	readTS uint64,
	maxLen int,
	candidateLen int64,
) (int64, []*kv.Elem[kv.OP], error) {
	if maxLen < 0 || candidateLen <= int64(maxLen) {
		return candidateLen, nil, nil
	}
	// int64 arithmetic + clamp at maxWideColumnItems. A single XADD must
	// not emit more than maxWideColumnItems Del operations: it would risk
	// exceeding the Raft message-size limit and would force a single
	// commit to materialise an unbounded list of keys. The cap is loose
	// enough that it never bites in normal operation (xaddEnforceMaxWideColumn
	// rejects streams whose committed length is already at the ceiling),
	// but defends against a corrupted meta.Length feeding the trim path.
	diff := candidateLen - int64(maxLen)
	if diff <= 0 {
		return candidateLen, nil, nil
	}
	count := maxWideColumnItems
	if diff <= int64(maxWideColumnItems) {
		count = int(diff)
	}
	trim, err := r.buildXTrimHeadElems(ctx, key, readTS, count)
	if err != nil {
		return 0, nil, err
	}
	// Final length must reflect the trim that actually committed, not
	// the requested maxLen, so that meta.Length stays consistent with
	// the entries on disk when the cap kicks in or the scan returns
	// fewer rows than requested. MAXLEN 0 is a special case: the
	// freshly-added entry is removed by appendMaxLenZeroSelfDel in the
	// caller, so the post-commit length is 0 regardless of what trim
	// did to the pre-existing rows.
	if maxLen == 0 {
		return 0, trim, nil
	}
	return candidateLen - int64(len(trim)), trim, nil
}

// streamWriteBase prepares a write to a stream. Returns the loaded meta
// (zero value when the stream has never been written) and, when a legacy
// single-blob key is still present on disk, a Del elem that the caller
// must include in the write transaction. No migration is performed:
// legacy entries are discarded, not re-materialised into the new layout.
// This matches the PR #620 operator directive that pre-migration data is
// expendable and is cleared explicitly rather than saved.
func (r *RedisServer) streamWriteBase(ctx context.Context, key []byte, readTS uint64) ([]*kv.Elem[kv.OP], store.StreamMeta, bool, error) {
	meta, metaFound, err := r.loadStreamMetaAt(ctx, key, readTS)
	if err != nil {
		return nil, store.StreamMeta{}, false, err
	}
	if metaFound {
		return nil, meta, true, nil
	}
	legacyCleanup, err := r.legacyStreamCleanupElems(ctx, key, readTS)
	if err != nil {
		return nil, store.StreamMeta{}, false, err
	}
	return legacyCleanup, store.StreamMeta{}, false, nil
}

// legacyStreamCleanupElems returns a Del elem for the legacy single-blob
// key if one is still present on disk, or nil otherwise. Called by
// streamWriteBase and deleteStreamWideColumnElems so every write or delete
// that touches a stream also evicts any stale legacy data.
func (r *RedisServer) legacyStreamCleanupElems(ctx context.Context, key []byte, readTS uint64) ([]*kv.Elem[kv.OP], error) {
	legacyKey := redisStreamKey(key)
	exists, err := r.store.ExistsAt(ctx, legacyKey, readTS)
	if err != nil {
		return nil, cockerrors.WithStack(err)
	}
	if !exists {
		return nil, nil
	}
	return []*kv.Elem[kv.OP]{{Op: kv.Del, Key: legacyKey}}, nil
}

// resolveXAddID resolves the requested ID (possibly '*') against the current
// stream meta and returns the assigned string ID plus its parsed form.
func resolveXAddID(meta store.StreamMeta, hasMeta bool, requested string) (string, redisStreamID, error) {
	var (
		hasLast         bool
		lastMs, lastSeq uint64
	)
	if hasMeta {
		// LastMs/LastSeq carry the highest ID ever assigned even when the
		// stream was trimmed to empty, so auto-ID generation stays
		// monotonic across MAXLEN=0 / XDEL-all cycles.
		hasLast = meta.Length > 0 || meta.LastMs != 0 || meta.LastSeq != 0
		lastMs, lastSeq = meta.LastMs, meta.LastSeq
	}
	id, err := nextXAddID(hasLast, lastMs, lastSeq, requested)
	if err != nil {
		return "", redisStreamID{}, err
	}
	parsed, ok := tryParseRedisStreamID(id)
	if !ok {
		return "", redisStreamID{}, errors.New("ERR Invalid stream ID specified as stream command argument")
	}
	return id, parsed, nil
}

// buildXTrimHeadElems emits Del operations for the oldest `count` entries
// in the entry-per-key layout via a bounded range scan at the caller's
// MVCC snapshot (ctx, readTS). Mixing a later timestamp here would let us
// tombstone keys the caller's view never saw.
func (r *RedisServer) buildXTrimHeadElems(
	ctx context.Context,
	key []byte,
	readTS uint64,
	count int,
) ([]*kv.Elem[kv.OP], error) {
	if count <= 0 {
		return nil, nil
	}
	// Defense-in-depth cap on the per-trim scan so a caller that asked
	// for math.MaxInt (corrupted meta upstream) cannot try to materialise
	// an unbounded list of Del elems in a single transaction. Callers
	// (xaddTrimIfNeeded, xtrimTxn) already cap; this is a belt-and-braces
	// guard on the boundary that actually allocates.
	if count > maxWideColumnItems {
		count = maxWideColumnItems
	}
	prefix := store.StreamEntryScanPrefix(key)
	end := store.PrefixScanEnd(prefix)
	kvs, err := r.store.ScanAt(ctx, prefix, end, count, readTS)
	if err != nil {
		return nil, cockerrors.WithStack(err)
	}
	elems := make([]*kv.Elem[kv.OP], 0, len(kvs))
	for _, pair := range kvs {
		elems = append(elems, &kv.Elem[kv.OP]{Op: kv.Del, Key: append([]byte(nil), pair.Key...)})
	}
	return elems, nil
}

func parseXTrimMaxLen(args [][]byte) (int, error) {
	if !strings.EqualFold(string(args[2]), "MAXLEN") {
		return 0, errors.New("ERR syntax error")
	}

	argIndex := 3
	if argIndex < len(args) && (string(args[argIndex]) == "~" || string(args[argIndex]) == "=") {
		argIndex++
	}
	if argIndex != len(args)-1 {
		return 0, errors.New("ERR syntax error")
	}

	maxLen, err := strconv.Atoi(string(args[argIndex]))
	if err != nil || maxLen < 0 {
		return 0, errors.New("ERR syntax error")
	}
	return maxLen, nil
}

func (r *RedisServer) xtrim(conn redcon.Conn, cmd redcon.Command) {
	if r.proxyToLeader(conn, cmd, cmd.Args[1]) {
		return
	}
	maxLen, err := parseXTrimMaxLen(cmd.Args)
	if err != nil {
		conn.WriteError("ERR syntax error")
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), redisDispatchTimeout)
	defer cancel()
	var removed int
	if err := r.retryRedisWrite(ctx, func() error {
		var err error
		removed, err = r.xtrimTxn(ctx, cmd.Args[1], maxLen)
		return err
	}); err != nil {
		conn.WriteError(err.Error())
		return
	}
	conn.WriteInt(removed)
}

// streamTypeForWrite returns (true, nil) when the key is either absent
// (no-op write) or already a stream, (false, nil) when the caller should
// short-circuit with "no stream here", and (_, err) for wrong-type or
// store errors. Extracted from xtrimTxn so the outer function stays
// within the cyclop budget.
func (r *RedisServer) streamTypeForWrite(ctx context.Context, key []byte, readTS uint64) (bool, error) {
	typ, err := r.keyTypeAtExpect(ctx, key, readTS, redisTypeStream)
	if err != nil {
		return false, err
	}
	switch typ {
	case redisTypeNone:
		return false, nil
	case redisTypeStream:
		return true, nil
	case redisTypeString, redisTypeList, redisTypeHash, redisTypeSet, redisTypeZSet:
		return false, wrongTypeError()
	default:
		return false, wrongTypeError()
	}
}

// flushLegacyCleanupOnTrimNoOp commits the legacy-blob Del + meta Put
// for an XTRIM whose length is already under maxLen. Without this
// flush a subsequent read would still find the stale legacy blob.
// Returns 0 removed entries; callers use that directly.
func (r *RedisServer) flushLegacyCleanupOnTrimNoOp(
	ctx context.Context, readTS uint64, key []byte,
	meta store.StreamMeta, legacyCleanup []*kv.Elem[kv.OP],
) (int, error) {
	if len(legacyCleanup) == 0 {
		return 0, nil
	}
	metaBytes, err := store.MarshalStreamMeta(meta)
	if err != nil {
		return 0, cockerrors.WithStack(err)
	}
	elems := make([]*kv.Elem[kv.OP], 0, len(legacyCleanup)+1)
	elems = append(elems, legacyCleanup...)
	elems = append(elems, &kv.Elem[kv.OP]{Op: kv.Put, Key: store.StreamMetaKey(key), Value: metaBytes})
	return 0, r.dispatchElems(ctx, true, readTS, elems)
}

func (r *RedisServer) xtrimTxn(ctx context.Context, key []byte, maxLen int) (int, error) {
	readTS := r.readTS()
	proceed, err := r.streamTypeForWrite(ctx, key, readTS)
	if err != nil || !proceed {
		return 0, err
	}

	legacyCleanup, meta, _, err := r.streamWriteBase(ctx, key, readTS)
	if err != nil {
		return 0, err
	}

	if meta.Length <= int64(maxLen) {
		return r.flushLegacyCleanupOnTrimNoOp(ctx, readTS, key, meta, legacyCleanup)
	}

	// Cap the trim request at maxWideColumnItems so a single XTRIM cannot
	// emit an unbounded list of Del operations in one Raft commit. int64
	// arithmetic upfront also keeps a corrupted meta.Length (>MaxInt)
	// from wrapping into a negative scan count.
	diff := meta.Length - int64(maxLen)
	requestedRemoved := maxWideColumnItems
	if diff <= int64(maxWideColumnItems) {
		requestedRemoved = int(diff)
	}
	trim, err := r.buildXTrimHeadElems(ctx, key, readTS, requestedRemoved)
	if err != nil {
		return 0, err
	}

	// Use len(trim) — the actual entries we are about to delete — for
	// both the meta.Length update and the XTRIM return value. The
	// requested count and the actual count can diverge when the trim
	// hits the per-txn cap or the underlying scan returns fewer rows
	// than requested (concurrent writes / partial consistency); using
	// the actual count keeps meta.Length consistent with on-disk state
	// and reports the truth back to the client.
	actualRemoved := len(trim)
	elems := make([]*kv.Elem[kv.OP], 0, len(legacyCleanup)+actualRemoved+1)
	elems = append(elems, legacyCleanup...)
	elems = append(elems, trim...)
	meta.Length -= int64(actualRemoved)
	metaBytes, err := store.MarshalStreamMeta(meta)
	if err != nil {
		return 0, cockerrors.WithStack(err)
	}
	elems = append(elems, &kv.Elem[kv.OP]{Op: kv.Put, Key: store.StreamMetaKey(key), Value: metaBytes})
	return actualRemoved, r.dispatchElems(ctx, true, readTS, elems)
}

func (r *RedisServer) xrange(conn redcon.Conn, cmd redcon.Command) {
	if r.proxyToLeader(conn, cmd, cmd.Args[1]) {
		return
	}
	r.rangeStream(conn, cmd, false)
}

func (r *RedisServer) xrevrange(conn redcon.Conn, cmd redcon.Command) {
	if r.proxyToLeader(conn, cmd, cmd.Args[1]) {
		return
	}
	r.rangeStream(conn, cmd, true)
}

func parseXReadCountArg(args [][]byte, index int) (int, error) {
	if index+1 >= len(args) {
		return 0, errors.New("ERR syntax error")
	}
	count, err := strconv.Atoi(string(args[index+1]))
	if err != nil || count <= 0 {
		return 0, errors.New("ERR syntax error")
	}
	// Clamp client-supplied COUNT to the wide-column ceiling so a single
	// XREAD cannot pre-allocate a maxInt-sized []redisStreamEntry slice or
	// pull more entries than the store will return for the equivalent
	// uncapped scan. Cap is silent (Redis-compatible): the client always
	// sees at most maxWideColumnItems entries per stream per call.
	if count > maxWideColumnItems {
		count = maxWideColumnItems
	}
	return count, nil
}

func parseXReadBlockArg(args [][]byte, index int) (time.Duration, error) {
	if index+1 >= len(args) {
		return 0, errors.New("ERR syntax error")
	}
	ms, err := strconv.Atoi(string(args[index+1]))
	if err != nil || ms < 0 {
		return 0, errors.New("ERR syntax error")
	}
	return time.Duration(ms) * time.Millisecond, nil
}

func parseXReadOptions(args [][]byte) (xreadOptions, error) {
	opts := xreadOptions{count: -1, streamsIndex: -1}
	for i := 1; i < len(args); {
		next, done, err := parseXReadOption(&opts, args, i)
		if err != nil {
			return xreadOptions{}, err
		}
		if done {
			return opts, nil
		}
		i = next
	}
	return opts, nil
}

func parseXReadOption(opts *xreadOptions, args [][]byte, i int) (int, bool, error) {
	switch strings.ToUpper(string(args[i])) {
	case redisKeywordCount:
		count, err := parseXReadCountArg(args, i)
		if err != nil {
			return 0, false, err
		}
		opts.count = count
		return i + redisPairWidth, false, nil
	case "BLOCK":
		block, err := parseXReadBlockArg(args, i)
		if err != nil {
			return 0, false, err
		}
		opts.block = block
		return i + redisPairWidth, false, nil
	case "STREAMS":
		opts.streamsIndex = i + 1
		return len(args), true, nil
	default:
		return 0, false, errors.New("ERR syntax error")
	}
}

func splitXReadStreams(args [][]byte, streamsIndex int) ([][]byte, []string, error) {
	if streamsIndex < 0 || streamsIndex >= len(args) {
		return nil, nil, errors.New("ERR syntax error")
	}
	remaining := len(args) - streamsIndex
	if remaining%redisPairWidth != 0 {
		return nil, nil, errors.New("ERR syntax error")
	}

	streamCount := remaining / redisPairWidth
	keys := make([][]byte, streamCount)
	afterIDs := make([]string, streamCount)
	for i := range streamCount {
		keys[i] = args[streamsIndex+i]
		afterIDs[i] = string(args[streamsIndex+streamCount+i])
	}
	return keys, afterIDs, nil
}

func parseXReadRequest(args [][]byte) (xreadRequest, error) {
	opts, err := parseXReadOptions(args)
	if err != nil {
		return xreadRequest{}, err
	}
	keys, afterIDs, err := splitXReadStreams(args, opts.streamsIndex)
	if err != nil {
		return xreadRequest{}, err
	}
	return xreadRequest{block: opts.block, count: opts.count, keys: keys, afterIDs: afterIDs}, nil
}

func (r *RedisServer) resolveXReadAfterIDs(ctx context.Context, req *xreadRequest) error {
	for i, afterID := range req.afterIDs {
		if afterID != "$" {
			continue
		}
		resolved, err := r.resolveXReadDollarID(ctx, req.keys[i])
		if err != nil {
			return err
		}
		req.afterIDs[i] = resolved
	}
	return nil
}

// resolveXReadDollarID resolves the "$" after-ID for a single stream by
// asking the store for the highest ID ever assigned. The new-layout meta
// answers in one read; when meta is absent the stream is treated as
// empty — legacy single-blob data is intentionally ignored under the
// "discard-on-read, delete-on-write" contract documented on
// dollarIDFromState (and matching loadStreamAt). Returns streamZeroID
// for non-existent and empty-never-written streams. ctx threads through
// the caller's cancellation/deadline so the resolve step doesn't survive
// past a BLOCK-window cancel.
func (r *RedisServer) resolveXReadDollarID(ctx context.Context, key []byte) (string, error) {
	readTS := r.readTS()
	typ, err := r.keyTypeAtExpect(ctx, key, readTS, redisTypeStream)
	if err != nil {
		return "", err
	}
	if typ == redisTypeNone {
		return streamZeroID, nil
	}
	if typ != redisTypeStream {
		return "", wrongTypeError()
	}
	return r.dollarIDFromState(ctx, key, readTS)
}

// dollarIDFromState returns the highest-ever-assigned stream ID as a string.
// Reads the new-layout meta record (O(1)); when meta is absent the stream
// is treated as empty — legacy single-blob data is intentionally ignored
// under the "discard-on-read, delete-on-write" contract (see loadStreamAt
// and the PR #620 writeup), so $ resolves to streamZeroID for any stream
// that has never been written in the new layout.
func (r *RedisServer) dollarIDFromState(ctx context.Context, key []byte, readTS uint64) (string, error) {
	meta, found, err := r.loadStreamMetaAt(ctx, key, readTS)
	if err != nil {
		return "", err
	}
	if !found {
		return streamZeroID, nil
	}
	if meta.Length == 0 && meta.LastMs == 0 && meta.LastSeq == 0 {
		return streamZeroID, nil
	}
	return strconv.FormatUint(meta.LastMs, 10) + "-" + strconv.FormatUint(meta.LastSeq, 10), nil
}

func selectXReadEntries(entries []redisStreamEntry, afterID string, count int) []redisStreamEntry {
	afterParsedID, afterParsedValid := tryParseRedisStreamID(afterID)
	start := sort.Search(len(entries), func(i int) bool {
		return entries[i].compareID(afterID, afterParsedID, afterParsedValid) > 0
	})
	if start >= len(entries) {
		return nil
	}
	end := len(entries)
	if count > 0 && start+count < end {
		end = start + count
	}
	return entries[start:end]
}

func (r *RedisServer) xreadOnce(ctx context.Context, req xreadRequest) ([]xreadResult, error) {
	results := make([]xreadResult, 0, len(req.keys))
	for i, key := range req.keys {
		readTS := r.readTS()
		typ, err := r.keyTypeAtExpect(ctx, key, readTS, redisTypeStream)
		if err != nil {
			return nil, err
		}
		if typ == redisTypeNone {
			continue
		}
		if typ != redisTypeStream {
			return nil, wrongTypeError()
		}

		entries, err := r.readStreamAfter(ctx, key, readTS, req.afterIDs[i], req.count)
		if err != nil {
			return nil, err
		}
		if len(entries) > 0 {
			results = append(results, xreadResult{key: key, entries: entries})
		}
	}
	return results, nil
}

// readStreamAfter returns up to `count` entries with ID strictly greater
// than afterID via the entry-per-key range scan. When the meta key is
// absent the stream is treated as empty; legacy single-blob data is
// intentionally ignored under the "discard-on-read, delete-on-write"
// contract documented on loadStreamAt. A subsequent XADD or XTRIM will
// delete any lingering legacy blob in the same transaction, so a stream
// whose meta is still missing here cannot have live legacy data from the
// caller's perspective.
func (r *RedisServer) readStreamAfter(ctx context.Context, key []byte, readTS uint64, afterID string, count int) ([]redisStreamEntry, error) {
	_, found, err := r.loadStreamMetaAt(ctx, key, readTS)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, nil
	}
	return r.scanStreamEntriesAfter(ctx, key, readTS, afterID, count)
}

// scanStreamEntriesAfter runs a [strictly-after(afterID), ∞) range scan over
// entry keys, capped by count (when positive) or maxWideScanLimit otherwise.
// When count is non-positive, we mirror scanStreamEntriesAt's guard: request
// maxWideScanLimit (which is maxWideColumnItems+1) and reject if the scan
// filled, so an XREAD without COUNT cannot OOM the server on a pathological
// stream.
//
// afterID must be a parseable stream ID in either the strict "ms-seq" form or
// the shorthand "ms" form (no dash), which Redis normalises to "ms-0".
// Genuinely malformed IDs are rejected immediately so the caller never
// receives a full-stream result set for invalid input.
func (r *RedisServer) scanStreamEntriesAfter(ctx context.Context, key []byte, readTS uint64, afterID string, count int) ([]redisStreamEntry, error) {
	afterID, ok := normalizeStreamAfterID(afterID)
	if !ok {
		return nil, errors.New("ERR Invalid stream ID specified as stream command argument")
	}
	prefix := store.StreamEntryScanPrefix(key)
	end := store.PrefixScanEnd(prefix)
	start := streamScanStartForAfter(prefix, afterID)
	limit := count
	unbounded := limit <= 0
	if unbounded {
		limit = maxWideScanLimit
	}
	kvs, err := r.store.ScanAt(ctx, start, end, limit, readTS)
	if err != nil {
		return nil, cockerrors.WithStack(err)
	}
	if unbounded && len(kvs) > maxWideColumnItems {
		return nil, cockerrors.Wrapf(ErrCollectionTooLarge, "stream %q exceeds %d entries", key, maxWideColumnItems)
	}
	entries := make([]redisStreamEntry, 0, len(kvs))
	for _, pair := range kvs {
		entry, err := unmarshalStreamEntry(pair.Value)
		if err != nil {
			return nil, err
		}
		entries = append(entries, entry)
	}
	return entries, nil
}

// streamScanStartForAfter returns the inclusive start key to use for an
// XREAD-style "after afterID" range scan. If afterID parses cleanly we
// start at ID+1 so the scan is exclusive of afterID. Callers must validate
// afterID before calling this function; if afterID is unparseable, the
// returned prefix is the entry-prefix start, which gives a full scan.
//
// Edge case: if afterID is (math.MaxUint64-math.MaxUint64), there is no
// successor ID inside the entry-prefix keyspace, so the correct start is
// one past the prefix (empty scan). Returning the afterID key itself
// would make the inclusive scan include it, which is the opposite of
// "strictly after."
func streamScanStartForAfter(prefix []byte, afterID string) []byte {
	parsed, ok := tryParseRedisStreamID(afterID)
	if !ok {
		return prefix
	}
	ms, seq := parsed.ms, parsed.seq
	switch {
	case seq < ^uint64(0):
		seq++
	case ms < ^uint64(0):
		ms++
		seq = 0
	default:
		// afterID is the largest representable stream ID. No entry can be
		// strictly after it; return the scan-end sentinel so the scan is
		// empty instead of silently inclusive.
		return store.PrefixScanEnd(prefix)
	}
	start := make([]byte, 0, len(prefix)+store.StreamIDBytes)
	start = append(start, prefix...)
	start = append(start, store.EncodeStreamID(ms, seq)...)
	return start
}

// normalizeStreamAfterID normalises an XREAD afterID to the strict "ms-seq"
// form used by tryParseRedisStreamID. Redis accepts a shorthand "ms" form
// (no dash) as meaning "ms-0". Truly invalid IDs — those that are neither
// valid "ms-seq" strings nor parseable as a bare uint64 — return ("", false).
func normalizeStreamAfterID(id string) (string, bool) {
	if strings.IndexByte(id, '-') >= 0 {
		_, ok := tryParseRedisStreamID(id)
		return id, ok
	}
	// Shorthand: bare millisecond component only. Redis treats "ms" as "ms-0"
	// for XREAD after-IDs (entries strictly after ms-0).
	if _, err := strconv.ParseUint(id, 10, 64); err != nil {
		return "", false
	}
	return id + "-0", true
}

func writeStreamEntry(conn redcon.Conn, entry redisStreamEntry) {
	conn.WriteArray(redisPairWidth)
	conn.WriteBulkString(entry.ID)
	conn.WriteArray(len(entry.Fields))
	for _, field := range entry.Fields {
		conn.WriteBulkString(field)
	}
}

func writeStreamEntries(conn redcon.Conn, entries []redisStreamEntry) {
	conn.WriteArray(len(entries))
	for _, entry := range entries {
		writeStreamEntry(conn, entry)
	}
}

func writeXReadResults(conn redcon.Conn, results []xreadResult) {
	conn.WriteArray(len(results))
	for _, result := range results {
		conn.WriteArray(redisPairWidth)
		conn.WriteBulk(result.key)
		writeStreamEntries(conn, result.entries)
	}
}

// isXReadIterCtxError reports whether err originates from the per-iteration
// XREAD context firing (BLOCK budget consumed mid-call). The check covers
// the bare context sentinels, cockroachdb/errors-wrapped variants, and
// gRPC's status.Error(codes.DeadlineExceeded / codes.Canceled, ...) which
// is what bubbles up through coordinator.Dispatch when the iter ctx fires
// during a Raft-mediated read. Hits on this path must be silently
// translated to "empty iteration" so the BLOCK-window null contract holds.
func isXReadIterCtxError(err error) bool {
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return true
	}
	if cockerrors.Is(err, context.DeadlineExceeded) || cockerrors.Is(err, context.Canceled) {
		return true
	}
	switch status.Code(err) { //nolint:exhaustive // only the two ctx-related codes matter; the rest must propagate as real errors
	case codes.DeadlineExceeded, codes.Canceled:
		return true
	default:
		return false
	}
}

func (r *RedisServer) xread(conn redcon.Conn, cmd redcon.Command) {
	req, err := parseXReadRequest(cmd.Args)
	if err != nil {
		conn.WriteError(err.Error())
		return
	}

	blockDuration := req.block
	// block=0 means infinite wait in Redis; cap at redisDispatchTimeout to prevent goroutine leak.
	if blockDuration == 0 {
		blockDuration = redisDispatchTimeout
	}
	deadline := time.Now().Add(blockDuration)

	// $ resolution uses a short fixed timeout rather than the BLOCK
	// window: it's a single bounded read per key, not a wait. A tight
	// BLOCK (e.g. `BLOCK 1`) used to turn any slow $-resolve into a
	// protocol-level error on this path; use redisDispatchTimeout so
	// the resolve either succeeds quickly or fails cleanly, leaving
	// the BLOCK-window timeout semantics (null on expiry) to the
	// busy-poll below.
	//
	// Parent on r.handlerContext() (not context.Background()) so an
	// in-flight resolve aborts promptly when the server is shutting
	// down — otherwise the per-resolve ScanAt could survive past
	// graceful-shutdown's drain window.
	resolveCtx, resolveCancel := context.WithTimeout(r.handlerContext(), redisDispatchTimeout)
	err = r.resolveXReadAfterIDs(resolveCtx, &req)
	resolveCancel()
	if err != nil {
		conn.WriteError(err.Error())
		return
	}

	r.xreadBusyPoll(conn, req, deadline)
}

// xreadBusyPoll runs the BLOCK-window wait loop. Extracted from xread so
// the parent function stays under the cyclop budget. Uses an event-driven
// signal from the in-process XADD path with a fallback timer for paths
// that bypass the signal (Lua flush, follower-side FSM apply).
//
// Registration happens BEFORE the first xreadOnce so a signal that fires
// between the check and the wait cannot be lost: the buffered channel
// holds it, and the next select wakes immediately.
func (r *RedisServer) xreadBusyPoll(conn redcon.Conn, req xreadRequest, deadline time.Time) {
	handlerCtx := r.handlerContext()
	w, release := r.streamWaiters.Register(req.keys)
	defer release()
	for {
		// Server-shutdown short-circuit: if the parent handlerContext
		// has been cancelled, abandon the wait loop immediately rather
		// than block until the BLOCK deadline. iterCtx below is rooted
		// in handlerCtx, so it would cancel-on-call too — but routing
		// through isXReadIterCtxError silently translates that into an
		// empty iteration and the loop would otherwise wait at
		// redisBlockWaitFallback cadence until the deadline.
		if handlerCtx.Err() != nil {
			conn.WriteNull()
			return
		}
		// BLOCK-expired before the loop body: respect the Redis contract
		// that a BLOCK timeout returns null, not an error. If we fell
		// through here without remaining time (very small BLOCK, or
		// $-resolution consumed the budget) creating an
		// already-expired context.WithTimeout would make xreadOnce
		// return DeadlineExceeded, which we'd then surface as an error.
		iterTimeout := time.Until(deadline)
		if iterTimeout <= 0 {
			conn.WriteNull()
			return
		}
		// Cap each iteration at redisDispatchTimeout to avoid holding
		// storage resources longer than a single dispatch.
		if iterTimeout > redisDispatchTimeout {
			iterTimeout = redisDispatchTimeout
		}
		// iterCtx is rooted in handlerCtx so its underlying storage
		// scans abort promptly on server shutdown rather than running
		// until iterTimeout fires. The handlerCtx.Err() guard at the
		// top of each iteration prevents the loop from spinning once
		// the parent ctx is cancelled.
		iterCtx, iterCancel := context.WithTimeout(handlerCtx, iterTimeout)
		results, err := r.xreadOnce(iterCtx, req)
		iterCancel()
		// Per-iteration ctx hitting its deadline (or being cancelled by
		// the upstream BLOCK timeout) is not a client-facing error — it
		// just means this poll round did not see any new entries. Treat
		// it as an empty iteration so the loop continues to the next
		// round (or falls through to the null-on-deadline branch below).
		// Without this, a tight BLOCK (e.g. BLOCK 10 against a busy /
		// slow node) leaks the iteration ctx-deadline into a -ERR reply,
		// which violates the Redis BLOCK-timeout contract (null on
		// timeout). xreadOnce returns nil results on any error, so
		// suppressing iter-ctx errors here is sound.
		if err != nil && !isXReadIterCtxError(err) {
			conn.WriteError(err.Error())
			return
		}
		if len(results) > 0 {
			writeXReadResults(conn, results)
			return
		}

		if !time.Now().Before(deadline) {
			conn.WriteNull()
			return
		}
		waitForBlockedCommandUpdate(handlerCtx, w.C, deadline)
	}
}

func (r *RedisServer) xlen(conn redcon.Conn, cmd redcon.Command) {
	if r.proxyToLeader(conn, cmd, cmd.Args[1]) {
		return
	}
	readTS := r.readTS()
	typ, err := r.keyTypeAtExpect(context.Background(), cmd.Args[1], readTS, redisTypeStream)
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
	meta, found, err := r.loadStreamMetaAt(context.Background(), cmd.Args[1], readTS)
	if err != nil {
		conn.WriteError(err.Error())
		return
	}
	if found {
		conn.WriteInt64(meta.Length)
		return
	}
	stream, err := r.loadStreamAt(context.Background(), cmd.Args[1], readTS)
	if err != nil {
		conn.WriteError(err.Error())
		return
	}
	conn.WriteInt64(int64(len(stream.Entries)))
}

func parseRangeStreamCount(args [][]byte) (int, error) {
	count := -1
	for i := 4; i < len(args); i += redisPairWidth {
		// args[i] is safe: the for-loop guard `i < len(args)` ensures it.
		// gosec G602 false-positives here under flow analysis.
		if i+1 >= len(args) || !strings.EqualFold(string(args[i]), redisKeywordCount) { //nolint:gosec
			return 0, errors.New("ERR syntax error")
		}
		nextCount, err := strconv.Atoi(string(args[i+1]))
		if err != nil || nextCount < 0 {
			return 0, errors.New("ERR syntax error")
		}
		count = nextCount
	}
	// Clamp client-supplied COUNT for XRANGE / XREVRANGE the same way XREAD
	// clamps it (parseXReadCountArg). The negative sentinel -1 (no COUNT)
	// is preserved unchanged so the unbounded path still trips
	// maxWideColumnItems guard inside rangeStreamNewLayout.
	if count > maxWideColumnItems {
		count = maxWideColumnItems
	}
	return count, nil
}

func streamEntryMatchesRange(entryID, startRaw, endRaw string, reverse bool) bool {
	if reverse {
		return streamWithinUpper(entryID, startRaw) && streamWithinLower(entryID, endRaw)
	}
	return streamWithinLower(entryID, startRaw) && streamWithinUpper(entryID, endRaw)
}

func selectForwardStreamRangeEntries(entries []redisStreamEntry, startRaw, endRaw string, count int) []redisStreamEntry {
	selected := make([]redisStreamEntry, 0, len(entries))
	for _, entry := range entries {
		if !streamEntryMatchesRange(entry.ID, startRaw, endRaw, false) {
			continue
		}
		selected = append(selected, entry)
		if count >= 0 && len(selected) >= count {
			break
		}
	}
	return selected
}

func selectReverseStreamRangeEntries(entries []redisStreamEntry, startRaw, endRaw string, count int) []redisStreamEntry {
	selected := make([]redisStreamEntry, 0, len(entries))
	for i := len(entries) - 1; i >= 0; i-- {
		if !streamEntryMatchesRange(entries[i].ID, startRaw, endRaw, true) {
			continue
		}
		selected = append(selected, entries[i])
		if count >= 0 && len(selected) >= count {
			break
		}
	}
	return selected
}

func selectStreamRangeEntries(entries []redisStreamEntry, startRaw, endRaw string, reverse bool, count int) []redisStreamEntry {
	if reverse {
		return selectReverseStreamRangeEntries(entries, startRaw, endRaw, count)
	}
	return selectForwardStreamRangeEntries(entries, startRaw, endRaw, count)
}

func (r *RedisServer) rangeStream(conn redcon.Conn, cmd redcon.Command, reverse bool) {
	count, err := parseRangeStreamCount(cmd.Args)
	if err != nil {
		conn.WriteError(err.Error())
		return
	}

	readTS := r.readTS()
	typ, err := r.keyTypeAtExpect(context.Background(), cmd.Args[1], readTS, redisTypeStream)
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

	startRaw, endRaw := string(cmd.Args[2]), string(cmd.Args[3])

	_, metaFound, err := r.loadStreamMetaAt(context.Background(), cmd.Args[1], readTS)
	if err != nil {
		conn.WriteError(err.Error())
		return
	}
	if metaFound {
		selected, err := r.rangeStreamNewLayout(context.Background(), cmd.Args[1], readTS, startRaw, endRaw, reverse, count)
		if err != nil {
			conn.WriteError(err.Error())
			return
		}
		writeStreamEntries(conn, selected)
		return
	}

	stream, err := r.loadStreamAt(context.Background(), cmd.Args[1], readTS)
	if err != nil {
		conn.WriteError(err.Error())
		return
	}
	selected := selectStreamRangeEntries(stream.Entries, startRaw, endRaw, reverse, count)
	writeStreamEntries(conn, selected)
}

// rangeStreamNewLayout serves XRANGE / XREVRANGE from the entry-per-key
// layout via a bounded range scan. The (start, end) inputs are the raw
// command bounds — "-", "+", "(1000-0", or "1000-0" — and are converted to
// binary scan bounds so only the selected entries are unmarshaled.
func (r *RedisServer) rangeStreamNewLayout(
	ctx context.Context, key []byte, readTS uint64,
	startRaw, endRaw string, reverse bool, count int,
) ([]redisStreamEntry, error) {
	prefix := store.StreamEntryScanPrefix(key)
	scanStart, scanEnd, ok, err := streamScanBounds(prefix, startRaw, endRaw, reverse)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, nil
	}
	limit := count
	unbounded := limit <= 0
	if unbounded {
		limit = maxWideScanLimit
	}
	var kvs []*store.KVPair
	if reverse {
		kvs, err = r.store.ReverseScanAt(ctx, scanStart, scanEnd, limit, readTS)
	} else {
		kvs, err = r.store.ScanAt(ctx, scanStart, scanEnd, limit, readTS)
	}
	if err != nil {
		return nil, cockerrors.WithStack(err)
	}
	// An XRANGE/XREVRANGE without COUNT on a pathological stream must
	// not be able to pull maxWideScanLimit entries into a single reply.
	// Mirror scanStreamEntriesAt's guard.
	if unbounded && len(kvs) > maxWideColumnItems {
		return nil, cockerrors.Wrapf(ErrCollectionTooLarge, "stream %q exceeds %d entries", key, maxWideColumnItems)
	}
	entries := make([]redisStreamEntry, 0, len(kvs))
	for _, pair := range kvs {
		entry, err := unmarshalStreamEntry(pair.Value)
		if err != nil {
			return nil, err
		}
		entries = append(entries, entry)
	}
	return entries, nil
}

// streamScanBounds maps the raw XRANGE / XREVRANGE bounds to half-open
// [start, end) scan bounds over the entry prefix. For reverse scans,
// the ReverseScanAt convention is still [start, end) with results in
// descending order starting from just-before(end).
//
// Returns ok=false when the bounds define an empty range (e.g. start > end),
// in which case the caller should emit an empty array.
func streamScanBounds(prefix []byte, startRaw, endRaw string, reverse bool) ([]byte, []byte, bool, error) {
	var lowRaw, highRaw string
	if reverse {
		// XREVRANGE takes (high, low).
		highRaw, lowRaw = startRaw, endRaw
	} else {
		lowRaw, highRaw = startRaw, endRaw
	}

	start, err := streamBoundLow(prefix, lowRaw)
	if err != nil {
		return nil, nil, false, err
	}
	end, err := streamBoundHigh(prefix, highRaw)
	if err != nil {
		return nil, nil, false, err
	}
	if bytes.Compare(start, end) >= 0 {
		return nil, nil, false, nil
	}
	return start, end, true, nil
}

// streamBoundLow returns the inclusive lower bound of the scan in binary form.
// When the bound is "(ID" (exclusive) and ID is the largest representable
// stream ID, the scan-end sentinel is returned so streamScanBounds'
// start >= end check collapses the range to empty; otherwise the scan
// would silently include the exclusive bound entry.
func streamBoundLow(prefix []byte, raw string) ([]byte, error) {
	if raw == "-" {
		return prefix, nil
	}
	exclusive := strings.HasPrefix(raw, "(")
	if exclusive {
		raw = raw[1:]
	}
	ms, seq, ok := parseStreamBoundID(raw, false, exclusive)
	if !ok {
		return nil, errors.New("ERR Invalid stream ID specified as stream command argument")
	}
	if exclusive {
		switch {
		case seq < ^uint64(0):
			seq++
		case ms < ^uint64(0):
			ms++
			seq = 0
		default:
			return store.PrefixScanEnd(prefix), nil
		}
	}
	return appendStreamKey(prefix, ms, seq), nil
}

// streamBoundHigh returns the exclusive upper bound of the scan in binary form.
func streamBoundHigh(prefix []byte, raw string) ([]byte, error) {
	if raw == "+" {
		return store.PrefixScanEnd(prefix), nil
	}
	exclusive := strings.HasPrefix(raw, "(")
	if exclusive {
		raw = raw[1:]
	}
	ms, seq, ok := parseStreamBoundID(raw, true, exclusive)
	if !ok {
		return nil, errors.New("ERR Invalid stream ID specified as stream command argument")
	}
	if !exclusive {
		switch {
		case seq < ^uint64(0):
			seq++
		case ms < ^uint64(0):
			ms++
			seq = 0
		default:
			return store.PrefixScanEnd(prefix), nil
		}
	}
	return appendStreamKey(prefix, ms, seq), nil
}

// parseStreamBoundID accepts both the strict ms-seq form and the shorthand
// "ms" form that Redis XRANGE/XREVRANGE allow. Redis interprets a shorthand
// ID differently depending on position and exclusivity:
//
//   - Lower bound inclusive ("5"): expand to 5-0; scan starts at 5-0.
//   - Lower bound exclusive ("(5"): expand to 5-0; caller shifts +1 → 5-1.
//   - Upper bound inclusive ("5"): expand to 5-MaxUint64; caller shifts +1 → 6-0 (exclusive upper).
//   - Upper bound exclusive ("(5"): expand to 5-0; scan stops at 5-0 (excludes all ms=5 entries).
//
// The rule is: seq = MaxUint64 when upper && !exclusive (need to include the
// full ms row before the caller's inclusive→exclusive shift), seq = 0
// otherwise. Full ms-seq IDs pass through unchanged.
func parseStreamBoundID(raw string, upper, exclusive bool) (uint64, uint64, bool) {
	if strings.IndexByte(raw, '-') >= 0 {
		parsed, ok := tryParseRedisStreamID(raw)
		if !ok {
			return 0, 0, false
		}
		return parsed.ms, parsed.seq, true
	}
	ms, err := strconv.ParseUint(raw, 10, 64)
	if err != nil {
		return 0, 0, false
	}
	// Upper inclusive bounds need seq=MaxUint64 so the caller's +1 shift
	// produces (ms+1)-0, covering the entire ms row. All other
	// combinations use seq=0: lower inclusive starts at ms-0, lower
	// exclusive starts at ms-0 then the caller shifts to ms-1, and upper
	// exclusive stops before ms-0 (excluding the whole ms).
	if upper && !exclusive {
		return ms, ^uint64(0), true
	}
	return ms, 0, true
}

func appendStreamKey(prefix []byte, ms, seq uint64) []byte {
	out := make([]byte, 0, len(prefix)+store.StreamIDBytes)
	out = append(out, prefix...)
	out = append(out, store.EncodeStreamID(ms, seq)...)
	return out
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
