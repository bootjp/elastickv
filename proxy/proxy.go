package proxy

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"strings"

	"github.com/redis/go-redis/v9"
	"github.com/tidwall/redcon"
)

// txnCommandsOverhead is the number of extra commands (MULTI + EXEC) wrapping queued commands.
const txnCommandsOverhead = 2

// respWriter is the subset of redcon.Conn and redcon.DetachedConn used for writing RESP responses.
// Both connection types satisfy this interface, enabling shared response-writing logic
// between the main event loop and detached pub/sub sessions.
type respWriter interface {
	WriteError(msg string)
	WriteString(msg string)
	WriteBulk(b []byte)
	WriteBulkString(msg string)
	WriteInt64(num int64)
	WriteArray(count int)
	WriteNull()
}

// proxyConnState tracks per-connection state (transactions, PubSub).
type proxyConnState struct {
	inTxn    bool
	txnQueue [][][]byte // buffered commands between MULTI and EXEC
}

// ProxyServer is a Redis-protocol proxy that dual-writes to two backends.
type ProxyServer struct {
	cfg     ProxyConfig
	dual    *DualWriter
	metrics *ProxyMetrics
	sentry  *SentryReporter
	logger  *slog.Logger

	// shutdownCtx is cancelled on graceful shutdown, used for blocking commands.
	shutdownCtx context.Context
}

// NewProxyServer creates a proxy server with the given configuration and backends.
func NewProxyServer(cfg ProxyConfig, dual *DualWriter, metrics *ProxyMetrics, sentryReporter *SentryReporter, logger *slog.Logger) *ProxyServer {
	return &ProxyServer{
		cfg:     cfg,
		dual:    dual,
		metrics: metrics,
		sentry:  sentryReporter,
		logger:  logger,
	}
}

// ListenAndServe starts the redcon proxy server.
func (p *ProxyServer) ListenAndServe(ctx context.Context) error {
	if p.cfg.RedisOnlyRaw && p.cfg.Mode == ModeRedisOnly {
		return p.listenAndServeRawRedis(ctx)
	}

	p.shutdownCtx = ctx

	var lc net.ListenConfig
	ln, err := lc.Listen(ctx, "tcp", p.cfg.ListenAddr)
	if err != nil {
		return fmt.Errorf("proxy listen: %w", err)
	}

	srv := redcon.NewServer(p.cfg.ListenAddr,
		p.handleCommand,
		p.handleAccept,
		p.handleClose,
	)

	// Graceful shutdown on context cancel.
	go func() {
		<-ctx.Done()
		p.logger.Info("shutting down proxy server")
		srv.Close()
	}()

	p.logger.Info("proxy server starting",
		"addr", p.cfg.ListenAddr,
		"mode", p.cfg.Mode.String(),
		"primary", p.cfg.PrimaryAddr,
		"secondary", p.cfg.SecondaryAddr,
	)

	if err = srv.Serve(ln); err != nil {
		// During graceful shutdown, srv.Close() causes Serve to return
		// a listener-closed error. Treat this as a normal exit.
		if ctx.Err() != nil {
			return nil //nolint:nilerr // intentional: suppress expected listener-closed error during graceful shutdown
		}
		return fmt.Errorf("proxy serve: %w", err)
	}
	return nil
}

func (p *ProxyServer) handleAccept(conn redcon.Conn) bool {
	p.metrics.ActiveConnections.Inc()
	conn.SetContext(&proxyConnState{})
	return true
}

func (p *ProxyServer) handleClose(conn redcon.Conn, _ error) {
	p.metrics.ActiveConnections.Dec()
}

func getConnState(conn redcon.Conn) *proxyConnState {
	if ctx := conn.Context(); ctx != nil {
		if st, ok := ctx.(*proxyConnState); ok {
			return st
		}
	}
	st := &proxyConnState{}
	conn.SetContext(st)
	return st
}

func (p *ProxyServer) handleCommand(conn redcon.Conn, cmd redcon.Command) {
	if len(cmd.Args) == 0 {
		conn.WriteError("ERR empty command")
		return
	}

	name := strings.ToUpper(string(cmd.Args[0]))
	args := cloneArgs(cmd.Args)
	state := getConnState(conn)

	// Transaction handling
	if state.inTxn {
		p.handleQueuedCommand(conn, state, name, args)
		return
	}

	p.dispatchCommand(conn, state, name, args)
}

func (p *ProxyServer) dispatchCommand(conn redcon.Conn, state *proxyConnState, name string, args [][]byte) {
	cat := ClassifyCommand(name, args[1:])

	switch cat {
	case CmdTxn:
		p.handleTxnCommand(conn, state, name)
	case CmdWrite:
		p.handleWrite(conn, name, args)
	case CmdRead:
		p.handleRead(conn, name, args)
	case CmdBlocking:
		p.handleBlocking(conn, name, args)
	case CmdPubSub:
		p.handlePubSub(conn, name, args)
	case CmdAdmin:
		p.handleAdmin(conn, name, args)
	case CmdScript:
		p.handleScript(conn, name, args)
	}
}

func (p *ProxyServer) handleQueuedCommand(conn redcon.Conn, state *proxyConnState, name string, args [][]byte) {
	switch name {
	case cmdExec:
		p.execTxn(conn, state)
	case cmdDiscard:
		p.discardTxn(conn, state)
	case cmdMulti:
		conn.WriteError("ERR MULTI calls can not be nested")
	default:
		// NOTE: Commands are queued locally and always return QUEUED without
		// upstream validation. Real Redis validates queued commands immediately
		// (e.g., wrong arity returns an error). Full compatibility would require
		// pinning a dedicated upstream connection for the MULTI..EXEC lifetime.
		state.txnQueue = append(state.txnQueue, args)
		conn.WriteString("QUEUED")
	}
}

func (p *ProxyServer) handleWrite(conn redcon.Conn, cmd string, args [][]byte) {
	resp, err := p.dual.Write(context.Background(), cmd, args)
	writeCommandResponse(conn, cmd, args, resp, err)
}

func (p *ProxyServer) handleRead(conn redcon.Conn, cmd string, args [][]byte) {
	resp, err := p.dual.Read(context.Background(), cmd, args)
	writeCommandResponse(conn, cmd, args, resp, err)
}

func (p *ProxyServer) handleBlocking(conn redcon.Conn, cmd string, args [][]byte) {
	// Use shutdownCtx so blocking commands are interrupted on graceful shutdown.
	resp, err := p.dual.Blocking(p.shutdownCtx, cmd, args)
	writeCommandResponse(conn, cmd, args, resp, err)
}

func (p *ProxyServer) handlePubSub(conn redcon.Conn, name string, args [][]byte) {

	switch name {
	case cmdSubscribe, cmdPSubscribe:
		p.startPubSubSession(conn, name, args)
	case cmdUnsubscribe, cmdPUnsubscribe:
		// No active session; emit one reply per argument, or a single null reply if none.
		kind := strings.ToLower(name)
		if len(args) > 1 {
			for _, ch := range args[1:] {
				conn.WriteArray(pubsubArrayReply)
				conn.WriteBulkString(kind)
				conn.WriteBulkString(string(ch))
				conn.WriteInt64(0)
			}
		} else {
			conn.WriteArray(pubsubArrayReply)
			conn.WriteBulkString(kind)
			conn.WriteNull()
			conn.WriteInt64(0)
		}
	default:
		// PUBSUB CHANNELS / NUMSUB etc.
		resp, err := p.dual.Admin(context.Background(), name, args)
		writeResponse(conn, resp, err)
	}
}

func (p *ProxyServer) startPubSubSession(conn redcon.Conn, cmdName string, args [][]byte) {
	psBackend := p.dual.PubSubBackend()
	if psBackend == nil {
		conn.WriteError("ERR PubSub not supported by backend")
		return
	}

	if len(args) < pubsubMinArgs {
		conn.WriteError(fmt.Sprintf("ERR wrong number of arguments for '%s' command", strings.ToLower(cmdName)))
		return
	}

	// Create dedicated upstream PubSub connection.
	upstream := psBackend.NewPubSub(context.Background())

	channels := byteSlicesToStrings(args[1:])
	var err error
	if cmdName == cmdSubscribe {
		err = upstream.Subscribe(context.Background(), channels...)
	} else {
		err = upstream.PSubscribe(context.Background(), channels...)
	}
	if err != nil {
		upstream.Close()
		writeRedisError(conn, err)
		return
	}

	// Detach the connection from redcon's event loop.
	dconn := conn.Detach()

	session := &pubsubSession{
		dconn:      dconn,
		upstream:   upstream,
		proxy:      p,
		logger:     p.logger,
		channelSet: make(map[string]struct{}),
		patternSet: make(map[string]struct{}),
	}

	session.shadow = p.createShadowPubSub(cmdName, channels)

	// Write initial subscription confirmations.
	kind := strings.ToLower(cmdName)
	for _, ch := range channels {
		dconn.WriteArray(pubsubArrayReply)
		dconn.WriteBulkString(kind)
		dconn.WriteBulkString(ch)
		if cmdName == cmdSubscribe {
			session.channelSet[ch] = struct{}{}
		} else {
			session.patternSet[ch] = struct{}{}
		}
		dconn.WriteInt64(int64(session.subCount()))
	}
	if err := dconn.Flush(); err != nil {
		dconn.Close()
		upstream.Close()
		if session.shadow != nil {
			session.shadow.Close()
		}
		return
	}

	go session.run()
}

// createShadowPubSub creates a shadow pub/sub for secondary comparison if in shadow mode.
// Returns nil if shadow mode is not active or the shadow subscribe fails.
func (p *ProxyServer) createShadowPubSub(cmdName string, channels []string) *shadowPubSub {
	shadowBackend := p.dual.ShadowPubSubBackend()
	if shadowBackend == nil {
		return nil
	}
	shadow := newShadowPubSub(shadowBackend, p.metrics, p.sentry, p.logger, p.cfg.PubSubCompareWindow)
	var err error
	if cmdName == cmdSubscribe {
		err = shadow.Subscribe(context.Background(), channels...)
	} else {
		err = shadow.PSubscribe(context.Background(), channels...)
	}
	if err != nil {
		p.logger.Warn("shadow pubsub subscribe failed", "err", err)
		p.metrics.PubSubShadowErrors.Inc()
		shadow.Close()
		return nil
	}
	shadow.Start()
	return shadow
}

const (
	pingMessageArgCount = 2
	helloProtoArgCount  = 2
	subcommandArgCount  = 2
	statusThirdArgCount = 3
	setMinimumArgs      = 3
)

func (p *ProxyServer) handleAdmin(conn redcon.Conn, name string, args [][]byte) {
	// Handle PING locally for speed.
	if name == cmdPing {
		writePingResponse(conn, args)
		return
	}

	// Handle QUIT locally.
	if name == cmdQuit {
		conn.WriteString("OK")
		conn.Close()
		return
	}

	// SELECT: accept only the configured DB; reject others since the proxy
	// uses a shared connection pool and cannot maintain per-client DB state.
	if name == cmdNameSELECT {
		p.writeSelectResponse(conn, args)
		return
	}

	// AUTH is handled at the connection-pool level via config.
	// Silently accept so clients don't break.
	if name == cmdNameAUTH {
		conn.WriteString("OK")
		return
	}

	// HELLO 2 is safe to forward: both the client and pooled upstream
	// connections stay on RESP2. Options change per-connection state and
	// cannot be forwarded through the shared upstream pool.
	if name == "HELLO" {
		p.writeHelloResponse(conn, args)
		return
	}

	resp, err := p.dual.Admin(context.Background(), name, args)
	writeAdminResponse(conn, name, args, resp, err)
}

func writePingResponse(conn redcon.Conn, args [][]byte) {
	switch len(args) {
	case 1:
		conn.WriteString("PONG")
	case pingMessageArgCount:
		conn.WriteBulk(args[1])
	default:
		conn.WriteError("ERR wrong number of arguments for 'ping' command")
	}
}

func (p *ProxyServer) writeSelectResponse(conn redcon.Conn, args [][]byte) {
	if len(args) != selectArgCount {
		conn.WriteError("ERR wrong number of arguments for 'select' command")
		return
	}
	requestedDB := string(args[1])
	if requestedDB != fmt.Sprintf("%d", p.cfg.PrimaryDB) {
		conn.WriteError(fmt.Sprintf("ERR proxy does not support SELECT %s (configured DB: %d)", requestedDB, p.cfg.PrimaryDB))
		return
	}
	conn.WriteString("OK")
}

func (p *ProxyServer) writeHelloResponse(conn redcon.Conn, args [][]byte) {
	if len(args) > helloProtoArgCount || (len(args) == helloProtoArgCount && string(args[1]) != "2") {
		conn.WriteError("ERR proxy supports HELLO 2 without options only")
		return
	}
	resp, err := p.dual.Admin(context.Background(), "HELLO", args)
	writeCommandResponse(conn, "HELLO", args, resp, err)
}

func writeAdminResponse(w respWriter, name string, args [][]byte, resp any, err error) {
	if name == "COMMAND" {
		if len(args) == 1 {
			writeCommandInfoResponse(w, resp, err)
			return
		}
		switch strings.ToUpper(string(args[1])) {
		case "DOCS":
			writeCommandDocsResponse(w, resp, err)
			return
		case "INFO":
			writeCommandInfoResponse(w, resp, err)
			return
		case "GETKEYSANDFLAGS":
			writeCommandGetKeysAndFlagsResponse(w, resp, err)
			return
		}
	}
	writeCommandResponse(w, name, args, resp, err)
}

func (p *ProxyServer) handleScript(conn redcon.Conn, name string, args [][]byte) {
	resp, err := p.dual.Script(context.Background(), name, args)
	writeCommandResponse(conn, name, args, resp, err)
}

// Transaction handling

func (p *ProxyServer) handleTxnCommand(conn redcon.Conn, state *proxyConnState, name string) {
	switch name {
	case cmdMulti:
		if state.inTxn {
			conn.WriteError("ERR MULTI calls can not be nested")
			return
		}
		state.inTxn = true
		state.txnQueue = nil
		conn.WriteString("OK")
	case cmdExec:
		if !state.inTxn {
			conn.WriteError("ERR EXEC without MULTI")
			return
		}
		p.execTxn(conn, state)
	case cmdDiscard:
		if !state.inTxn {
			conn.WriteError("ERR DISCARD without MULTI")
			return
		}
		p.discardTxn(conn, state)
	}
}

func (p *ProxyServer) execTxn(conn redcon.Conn, state *proxyConnState) {
	queue := state.txnQueue
	state.inTxn = false
	state.txnQueue = nil

	ctx := context.Background()

	// Build pipeline: MULTI + queued commands + EXEC
	cmds := make([][]any, 0, len(queue)+txnCommandsOverhead)
	cmds = append(cmds, []any{cmdMulti})
	for _, args := range queue {
		cmds = append(cmds, bytesArgsToInterfaces(args))
	}
	cmds = append(cmds, []any{cmdExec})

	results, err := p.dual.Primary().Pipeline(ctx, cmds)
	switch {
	case err != nil:
		// Pipeline-level error (connection/transport failure) takes precedence.
		writeRedisError(conn, err)
	case len(results) > 0:
		// Write the EXEC result (last command in the pipeline).
		lastResult := results[len(results)-1]
		resp, rErr := lastResult.Result()
		writeTxnResponse(conn, queue, resp, rErr)
	default:
		conn.WriteError("ERR empty transaction response")
	}

	// Async replay to secondary (bounded)
	if p.dual.hasSecondaryWrite() {
		p.dual.goTxnReplay(cmds, func(ctx context.Context) {
			p.dual.writeSecondaryPipeline(ctx, cmds)
		})
	}
}

func (p *ProxyServer) discardTxn(conn redcon.Conn, state *proxyConnState) {
	state.inTxn = false
	state.txnQueue = nil
	conn.WriteString("OK")
}

// writeResponse handles the common pattern of writing a go-redis response,
// correctly handling redis.Nil and upstream errors.
func writeResponse(w respWriter, resp any, err error) {
	if err != nil {
		if errors.Is(err, redis.Nil) {
			w.WriteNull()
			return
		}
		writeRedisError(w, err)
		return
	}
	writeRedisValue(w, resp)
}

// writeCommandResponse restores top-level RESP2 types that go-redis's generic
// Cmd decoder represents as plain Go strings or redis.Nil.
func writeCommandResponse(w respWriter, name string, args [][]byte, resp any, err error) {
	if errors.Is(err, redis.Nil) && isNullArrayResponse(name) {
		w.WriteArray(-1)
		return
	}
	if err != nil {
		writeResponse(w, nil, err)
		return
	}
	writeCommandValue(w, name, args, resp)
}

func writeCommandValue(w respWriter, name string, args [][]byte, resp any) {
	if name == cmdNameDEBUG && writeDebugDigestValues(w, args, resp) {
		return
	}
	if isScriptReturnCommand(name) {
		writeScriptValue(w, resp)
		return
	}
	if value, ok := resp.(string); ok {
		if isSimpleStringCommand(name, args) || isACLDRYRUNSuccess(name, args, value) || isDefaultRoutedStatus(name, args, value) {
			w.WriteString(value)
			return
		}
	}
	writeRedisValue(w, resp)
}

func writeScriptValue(w respWriter, resp any) {
	switch value := resp.(type) {
	case string:
		if isStatusResponse(value) {
			w.WriteString(value)
		} else {
			w.WriteBulkString(value)
		}
	case []any:
		w.WriteArray(len(value))
		for _, item := range value {
			writeScriptValue(w, item)
		}
	default:
		writeRedisValue(w, resp)
	}
}

func writeDebugDigestValues(w respWriter, args [][]byte, resp any) bool {
	if !hasSubcommand(args, "DIGEST-VALUE") {
		return false
	}
	values, ok := resp.([]any)
	if !ok {
		return false
	}
	w.WriteArray(len(values))
	for _, value := range values {
		if digest, ok := value.(string); ok {
			w.WriteString(digest)
		} else {
			writeRedisValue(w, value)
		}
	}
	return true
}

// go-redis loses the RESP type of string values, including strings nested in
// script result arrays. Preserve the prior status-token handling until the
// primary transport can expose the upstream RESP type directly.
func isScriptReturnCommand(name string) bool {
	switch name {
	case cmdEval, cmdEvalSHA, cmdEvalRO, cmdEvalSHARO, cmdNameFCALL, cmdNameFCALLRO:
		return true
	default:
		return false
	}
}

func isStatusResponse(s string) bool {
	switch s {
	case "OK", "QUEUED", "PONG":
		return true
	default:
		return false
	}
}

// Unlisted commands are forwarded as writes. Preserve the status-token
// behavior they had before the RESP2 bulk-string fix; their reply schema is
// unavailable to the parsed proxy after go-redis decodes the upstream frame.
func isDefaultRoutedStatus(name string, args [][]byte, value string) bool {
	if _, known := commandTable[name]; known || len(args) == 0 {
		return false
	}
	return ClassifyCommand(name, args[1:]) == CmdWrite && isStatusResponse(value)
}

func isACLDRYRUNSuccess(name string, args [][]byte, value string) bool {
	return name == cmdNameACL && hasSubcommand(args, "DRYRUN") && value == "OK"
}

func isNullArrayResponse(name string) bool {
	switch name {
	case cmdNameBLPOP, cmdNameBRPOP, cmdNameBZPOPMIN, cmdNameBZPOPMAX, cmdNameXREAD, cmdNameXREADGROUP, cmdExec:
		return true
	default:
		return false
	}
}

func isSimpleStringCommand(name string, args [][]byte) bool {
	if name == "PING" {
		return len(args) == 1
	}
	if isFixedSimpleStringCommand(name) {
		return true
	}
	if name == "SET" {
		return setReturnsStatus(args)
	}
	return isSimpleStringSubcommand(name, args)
}

func isSimpleStringSubcommand(name string, args [][]byte) bool {
	switch name {
	case "CLIENT":
		return isSimpleClientSubcommand(args)
	case cmdScript:
		return hasSubcommand(args, "FLUSH", "KILL", cmdNameDEBUG)
	case cmdNameFUNCTION:
		return hasSubcommand(args, "FLUSH", "KILL", "DELETE", "RESTORE")
	case "XGROUP":
		return hasSubcommand(args, "CREATE", "SETID")
	default:
		return isSimpleAdminSubcommand(name, args)
	}
}

func isSimpleClientSubcommand(args [][]byte) bool {
	if hasSubcommand(args, "SETNAME", "SETINFO", "PAUSE", "UNPAUSE", "TRACKING", "CACHING", "NO-EVICT", "NO-TOUCH") {
		return true
	}
	return len(args) == statusThirdArgCount && (hasSubcommand(args, "KILL") ||
		(hasSubcommand(args, "REPLY") && strings.EqualFold(string(args[2]), "ON")))
}

func isSimpleAdminSubcommand(name string, args [][]byte) bool {
	switch name {
	case "CONFIG":
		return hasSubcommand(args, "SET", "RESETSTAT", "REWRITE")
	case cmdNameACL:
		return isSimpleACLSubcommand(args)
	case "MODULE":
		return hasSubcommand(args, "LOAD", "LOADEX", "UNLOAD")
	case "SLOWLOG":
		return hasSubcommand(args, "RESET")
	case "MEMORY":
		return hasSubcommand(args, "PURGE")
	case "CLUSTER":
		return hasSubcommand(args, "ADDSLOTS", "ADDSLOTSRANGE", "DELSLOTS", "DELSLOTSRANGE", "FAILOVER", "FLUSHSLOTS", "FORGET", "MEET", "REPLICATE", "RESET", "SAVECONFIG", "SET-CONFIG-EPOCH", "SETSLOT")
	case cmdNameDEBUG:
		return isSimpleDebugSubcommand(args)
	default:
		return false
	}
}

// DEBUG has both simple-string diagnostics and bulk-string data responses.
// These subcommands use addReplyStatus or shared.ok in Redis 8.10.1.
func isSimpleDebugSubcommand(args [][]byte) bool {
	if len(args) < subcommandArgCount {
		return false
	}
	switch strings.ToUpper(string(args[1])) {
	case "OOM", "KEYSIZES-HIST-ASSERT", "ALLOCSIZE-SLOTS-ASSERT", "LOG", "LEAK", "RELOAD", "LOADAOF",
		"DROP-CLUSTER-PACKET-FILTER", "OBJECT", "SDSLEN", "LISTPACK", "QUICKLIST", "POPULATE", "DIGEST",
		"SLEEP", "SET-ACTIVE-EXPIRE", "SET-ALLOW-ACCESS-EXPIRED", "QUICKLIST-PACKED-THRESHOLD",
		"SET-SKIP-CHECKSUM-VALIDATION", "ENABLE-KEYMETA-RUNTIME-REGISTRATION", "AOF-FLUSH-SLEEP",
		"REPLICATE", "CHANGE-REPL-ID", "STRINGMATCH-TEST", "SET-DISABLE-DENY-SCRIPTS",
		"CONFIG-REWRITE-FORCE-ALL", "PAUSE-CRON", "REPLYBUFFER", "REPLY-COPY-AVOIDANCE",
		"REPL-PAUSE", "DICT-RESIZING", cmdScript, "MARK-INTERNAL-CLIENT", "ASM-FAILPOINT", "ASM-TRIM-METHOD":
		return true
	default:
		return false
	}
}

func isSimpleACLSubcommand(args [][]byte) bool {
	return hasSubcommand(args, "SETUSER", "SAVE", "LOAD") ||
		(len(args) == statusThirdArgCount && hasSubcommand(args, "LOG") && strings.EqualFold(string(args[2]), "RESET"))
}

func setReturnsStatus(args [][]byte) bool {
	if len(args) < setMinimumArgs {
		return false
	}
	for i := setMinimumArgs; i < len(args); i++ {
		option := strings.ToUpper(string(args[i]))
		if option == "GET" {
			return false
		}
		if setOptionHasValue(option) {
			i++
		}
	}
	return true
}

func setOptionHasValue(option string) bool {
	switch option {
	case "EX", "PX", "EXAT", "PXAT", "IFEQ", "IFNE", "IFDEQ", "IFDNE":
		return true
	default:
		return false
	}
}

func hasSubcommand(args [][]byte, names ...string) bool {
	if len(args) < subcommandArgCount {
		return false
	}
	for _, name := range names {
		if strings.EqualFold(string(args[1]), name) {
			return true
		}
	}
	return false
}

func isFixedSimpleStringCommand(name string) bool {
	switch name {
	case "TYPE", "SETEX", "PSETEX", "MSET", "HMSET", "LSET", "LTRIM", "RENAME", "RESTORE", "PFMERGE", "FLUSHALL", "FLUSHDB", "SWAPDB", "WATCH", "UNWATCH", "RESET", "SAVE", "BGSAVE", "BGREWRITEAOF", "REPLICAOF", "SLAVEOF", "MIGRATE", "READONLY", "READWRITE", "ASKING", "REPLCONF", "RESTORE-ASKING", "FAILOVER":
		return true
	default:
		return false
	}
}

func writeTxnResponse(w respWriter, queue [][][]byte, resp any, err error) {
	if err != nil {
		writeCommandResponse(w, cmdExec, nil, nil, err)
		return
	}
	values, ok := resp.([]any)
	if !ok || len(values) != len(queue) {
		writeRedisValue(w, resp)
		return
	}
	w.WriteArray(len(values))
	for i, value := range values {
		if len(queue[i]) == 0 {
			writeRedisValue(w, value)
			continue
		}
		writeCommandValue(w, strings.ToUpper(string(queue[i][0])), queue[i], value)
	}
}

// writeRedisError writes an upstream error to the client.
// go-redis redis.Error values already carry the Redis prefix (e.g. "ERR ...", "WRONGTYPE ...").
// Other errors (timeouts, dial failures) are normalized to "ERR ..." to produce valid RESP.
func writeRedisError(w respWriter, err error) {
	var redisErr redis.Error
	if errors.As(err, &redisErr) {
		w.WriteError(redisErr.Error())
		return
	}
	w.WriteError("ERR " + err.Error())
}

// writeRedisValue writes a go-redis response value to a respWriter.
func writeRedisValue(w respWriter, val any) {
	if val == nil {
		w.WriteNull()
		return
	}
	switch v := val.(type) {
	case string:
		w.WriteBulkString(v)
	case int64:
		w.WriteInt64(v)
	case []any:
		w.WriteArray(len(v))
		for _, item := range v {
			writeRedisValue(w, item)
		}
	case []byte:
		w.WriteBulk(v)
	case redis.Error:
		w.WriteError(v.Error())
	default:
		w.WriteBulkString(fmt.Sprintf("%v", v))
	}
}

func cloneArgs(args [][]byte) [][]byte {
	out := make([][]byte, len(args))
	for i, arg := range args {
		cp := make([]byte, len(arg))
		copy(cp, arg)
		out[i] = cp
	}
	return out
}
