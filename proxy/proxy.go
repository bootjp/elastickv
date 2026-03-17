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
		p.handleWrite(conn, args)
	case CmdRead:
		p.handleRead(conn, args)
	case CmdBlocking:
		p.handleBlocking(conn, args)
	case CmdPubSub:
		p.handlePubSub(conn, args)
	case CmdAdmin:
		p.handleAdmin(conn, args)
	case CmdScript:
		p.handleScript(conn, args)
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

func (p *ProxyServer) handleWrite(conn redcon.Conn, args [][]byte) {
	resp, err := p.dual.Write(context.Background(), args)
	writeResponse(conn, resp, err)
}

func (p *ProxyServer) handleRead(conn redcon.Conn, args [][]byte) {
	resp, err := p.dual.Read(context.Background(), args)
	writeResponse(conn, resp, err)
}

func (p *ProxyServer) handleBlocking(conn redcon.Conn, args [][]byte) {
	// Use shutdownCtx so blocking commands are interrupted on graceful shutdown.
	resp, err := p.dual.Blocking(p.shutdownCtx, args)
	writeResponse(conn, resp, err)
}

func (p *ProxyServer) handlePubSub(conn redcon.Conn, args [][]byte) {
	name := strings.ToUpper(string(args[0]))

	switch name {
	case cmdSubscribe, cmdPSubscribe:
		p.startPubSubSession(conn, name, args)
	case cmdUnsubscribe, cmdPUnsubscribe:
		// No active session; return empty confirmation.
		conn.WriteArray(pubsubArrayReply)
		conn.WriteBulkString(strings.ToLower(name))
		conn.WriteNull()
		conn.WriteInt64(0)
	default:
		// PUBSUB CHANNELS / NUMSUB etc.
		resp, err := p.dual.Admin(context.Background(), args)
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
		return
	}

	go session.run()
}

func (p *ProxyServer) handleAdmin(conn redcon.Conn, args [][]byte) {
	name := strings.ToUpper(string(args[0]))

	// Handle PING locally for speed.
	if name == cmdPing {
		if len(args) > 1 {
			conn.WriteBulk(args[1])
		} else {
			conn.WriteString("PONG")
		}
		return
	}

	// Handle QUIT locally.
	if name == cmdQuit {
		conn.WriteString("OK")
		conn.Close()
		return
	}

	// SELECT and AUTH are handled at the connection-pool level via config.
	// Silently accept them so clients don't break.
	if name == "SELECT" || name == "AUTH" {
		conn.WriteString("OK")
		return
	}

	resp, err := p.dual.Admin(context.Background(), args)
	writeResponse(conn, resp, err)
}

func (p *ProxyServer) handleScript(conn redcon.Conn, args [][]byte) {
	resp, err := p.dual.Script(context.Background(), args)
	writeResponse(conn, resp, err)
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
	cmds = append(cmds, []any{"MULTI"})
	for _, args := range queue {
		cmds = append(cmds, bytesArgsToInterfaces(args))
	}
	cmds = append(cmds, []any{"EXEC"})

	results, err := p.dual.Primary().Pipeline(ctx, cmds)
	if err != nil {
		// Pipeline-level error (connection/transport failure) takes precedence.
		writeRedisError(conn, err)
	} else if len(results) > 0 {
		// Write the EXEC result (last command in the pipeline).
		lastResult := results[len(results)-1]
		resp, rErr := lastResult.Result()
		writeResponse(conn, resp, rErr)
	}

	// Async replay to secondary (bounded)
	if p.dual.hasSecondaryWrite() {
		p.dual.goAsync(func() {
			sCtx, cancel := context.WithTimeout(context.Background(), p.cfg.SecondaryTimeout)
			defer cancel()
			_, pErr := p.dual.Secondary().Pipeline(sCtx, cmds)
			if pErr != nil {
				p.logger.Warn("secondary txn replay failed", "err", pErr)
				p.metrics.SecondaryWriteErrors.Inc()
			}
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
		if isStatusResponse(v) {
			w.WriteString(v)
		} else {
			w.WriteBulkString(v)
		}
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

// isStatusResponse detects known Redis status reply strings that should be
// sent as simple strings (+OK) rather than bulk strings ($2\r\nOK).
func isStatusResponse(s string) bool {
	switch s {
	case "OK", "QUEUED", "PONG":
		return true
	default:
		return false
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
