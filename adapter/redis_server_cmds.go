package adapter

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/bootjp/elastickv/kv"
	cockerrors "github.com/cockroachdb/errors"
	"github.com/tidwall/redcon"
)

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
		writeRedisError(conn, err)
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
	if err := conn.Close(); err != nil {
		log.Printf("redis quit close failed: %v", err)
	}
}

func (r *RedisServer) typeCmd(conn redcon.Conn, cmd redcon.Command) {
	if r.proxyToLeader(conn, cmd, cmd.Args[1]) {
		return
	}
	typ, err := r.keyType(context.Background(), cmd.Args[1])
	if err != nil {
		writeRedisError(conn, err)
		return
	}
	conn.WriteString(string(typ))
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
		writeRedisError(conn, err)
		return
	}

	keys, err := r.visibleKeys(pattern)
	if err != nil {
		writeRedisError(conn, err)
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
			writeRedisError(conn, err)
			return
		}
		conn.WriteInt(size)
		return
	}
	if err := r.coordinator.VerifyLeader(r.handlerContext()); err != nil {
		writeRedisError(conn, err)
		return
	}

	keys, err := r.visibleKeys([]byte("*"))
	if err != nil {
		writeRedisError(conn, err)
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
			writeRedisError(conn, err)
			return
		}
		conn.WriteInt(n)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), redisFlushLegacyTimeout)
	defer cancel()

	totalDeleted, err := r.deleteLegacyKeys(ctx, r.readTS())
	if err != nil {
		writeRedisError(conn, err)
		return
	}
	conn.WriteInt(totalDeleted)
}

func (r *RedisServer) flushDatabase(conn redcon.Conn, all bool) {
	if !r.coordinator.IsLeader() {
		if err := r.proxyFlushDatabase(all); err != nil {
			writeRedisError(conn, err)
			return
		}
		conn.WriteString("OK")
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), redisDispatchTimeout)
	defer cancel()

	if err := r.retryRedisWrite(ctx, func() error {
		// Use the per-call ctx with redisDispatchTimeout, NOT
		// handlerContext (the long-lived server baseCtx). FLUSHDB's
		// retry budget already lives in ctx; routing it to
		// VerifyLeader keeps the whole command bounded.
		if err := r.coordinator.VerifyLeader(ctx); err != nil {
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
		writeRedisError(conn, err)
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
