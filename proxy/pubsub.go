package proxy

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/tidwall/redcon"
)

const (
	pubsubArrayMessage  = 3 // ["message", channel, payload]
	pubsubArrayPMessage = 4 // ["pmessage", pattern, channel, payload]
	pubsubArrayReply    = 3 // ["subscribe"/"unsubscribe", channel, count]
	pubsubArrayPong     = 2 // ["pong", data]
	pubsubMinArgs       = 2 // command + at least one channel
	selectArgCount      = 2 // command name + db index argument

	cmdSubscribe    = "SUBSCRIBE"
	cmdUnsubscribe  = "UNSUBSCRIBE"
	cmdPSubscribe   = "PSUBSCRIBE"
	cmdPUnsubscribe = "PUNSUBSCRIBE"
	cmdMulti        = "MULTI"
	cmdExec         = "EXEC"
	cmdDiscard      = "DISCARD"
	cmdPing         = "PING"
	cmdQuit         = "QUIT"

	// cleanupFwdTimeout bounds the wait for forwardMessages to exit during cleanup.
	// If the client socket is stuck, we don't want to block indefinitely.
	cleanupFwdTimeout = 5 * time.Second
)

// pubsubSession manages a single client's detached connection.
// It bridges a detached redcon connection to an upstream go-redis PubSub connection.
// When all subscriptions are removed, the session transitions to normal command mode,
// enabling the client to execute regular Redis commands without reconnecting.
type pubsubSession struct {
	mu       sync.Mutex // protects upstream, closed, and shadow; channelSet/patternSet/txn are goroutine-confined to commandLoop
	writeMu  sync.Mutex // serializes writes to dconn; may be held while acquiring mu (lock ordering: writeMu then mu)
	dconn    redcon.DetachedConn
	upstream *redis.PubSub // nil when not in pub/sub mode
	proxy    *ProxyServer
	logger   *slog.Logger
	closed   bool

	// Track subscribed channels/patterns in sets for idempotent subscribe/unsubscribe
	// and correct subscription count tracking (matching Redis behavior).
	channelSet map[string]struct{}
	patternSet map[string]struct{}

	// fwdDone is closed when the current forwardMessages goroutine exits.
	fwdDone chan struct{}

	// Shadow pub/sub for secondary comparison (nil when not in shadow mode).
	shadow *shadowPubSub

	// Transaction state for normal command mode.
	inTxn    bool
	txnQueue [][][]byte
}

// subCount returns the total number of active subscriptions (channels + patterns).
func (s *pubsubSession) subCount() int {
	return len(s.channelSet) + len(s.patternSet)
}

// run starts the session. It blocks until the client disconnects or sends QUIT.
func (s *pubsubSession) run() {
	defer s.cleanup()
	s.startForwarding()
	s.commandLoop()
}

func (s *pubsubSession) cleanup() {
	s.mu.Lock()
	s.closed = true
	if s.upstream != nil {
		s.upstream.Close()
		s.upstream = nil
	}
	s.mu.Unlock()
	if s.fwdDone != nil {
		// Bounded wait: if forwardMessages is stuck on a slow/dead client socket,
		// close dconn to unblock it, then wait with a second bounded timeout.
		if !s.waitFwdDone() {
			s.logger.Warn("forwardMessages did not exit within timeout, closing dconn to unblock")
			s.dconn.Close()
			if !s.waitFwdDone() {
				s.logger.Error("forwardMessages still stuck after dconn.Close, abandoning")
			}
			s.closeShadow()
			return // dconn already closed
		}
	}
	// Close shadow after forwardMessages exits (it calls RecordPrimary).
	s.closeShadow()
	s.dconn.Close()
}

// waitFwdDone waits for fwdDone with a bounded timeout, returning true if it completed.
func (s *pubsubSession) waitFwdDone() bool {
	select {
	case <-s.fwdDone:
		return true
	case <-time.After(cleanupFwdTimeout):
		return false
	}
}

func (s *pubsubSession) startForwarding() {
	// Capture upstream under lock to avoid race with exitPubSubMode.
	s.mu.Lock()
	upstream := s.upstream
	s.mu.Unlock()
	if upstream == nil {
		return
	}
	ch := upstream.Channel()
	s.fwdDone = make(chan struct{})
	go func() {
		defer close(s.fwdDone)
		s.forwardMessages(ch)
	}()
}

// forwardMessages reads from the upstream go-redis PubSub channel and writes
// messages to the detached client connection.
func (s *pubsubSession) forwardMessages(ch <-chan *redis.Message) {
	for msg := range ch {
		s.mu.Lock()
		closed := s.closed
		s.mu.Unlock()
		if closed {
			return
		}
		s.writeMu.Lock()
		if msg.Pattern != "" {
			s.dconn.WriteArray(pubsubArrayPMessage)
			s.dconn.WriteBulkString("pmessage")
			s.dconn.WriteBulkString(msg.Pattern)
			s.dconn.WriteBulkString(msg.Channel)
			s.dconn.WriteBulkString(msg.Payload)
		} else {
			s.dconn.WriteArray(pubsubArrayMessage)
			s.dconn.WriteBulkString("message")
			s.dconn.WriteBulkString(msg.Channel)
			s.dconn.WriteBulkString(msg.Payload)
		}
		err := s.dconn.Flush()
		s.writeMu.Unlock()
		if err != nil {
			// Mark closed and close dconn to unblock commandLoop, preventing
			// goroutine/resource leaks (aligned with adapter/redis_pubsub.go).
			s.mu.Lock()
			s.closed = true
			s.mu.Unlock()
			_ = s.dconn.Close()
			return
		}
		// Record for shadow comparison (outside writeMu to avoid nested locking).
		// Capture shadow under mu since cleanup/exitPubSubMode can nil it concurrently.
		s.mu.Lock()
		shadow := s.shadow
		s.mu.Unlock()
		if shadow != nil {
			shadow.RecordPrimary(msg)
		}
	}
}

// commandLoop reads commands from the detached client and dispatches them.
// In pub/sub mode, only subscription commands are allowed.
// When all subscriptions are removed, it transitions to normal command mode.
func (s *pubsubSession) commandLoop() {
	for {
		cmd, err := s.dconn.ReadCommand()
		if err != nil {
			return
		}
		if len(cmd.Args) == 0 {
			continue
		}
		args := cloneArgs(cmd.Args)
		name := strings.ToUpper(string(args[0]))

		// subCount() reads channelSet/patternSet which are only modified
		// from this goroutine (commandLoop), so no lock is needed.
		if s.subCount() > 0 {
			if !s.dispatchPubSubCommand(args) {
				return
			}
			if s.shouldExitPubSub() {
				s.exitPubSubMode()
			}
		} else if !s.dispatchNormalCommand(name, args) {
			return
		}
	}
}

func (s *pubsubSession) shouldExitPubSub() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.upstream != nil && s.subCount() == 0
}

func (s *pubsubSession) exitPubSubMode() {
	s.mu.Lock()
	if s.upstream != nil {
		s.upstream.Close()
		s.upstream = nil
	}
	s.mu.Unlock()
	if s.fwdDone != nil {
		if !s.waitFwdDone() {
			s.logger.Warn("forwardMessages did not exit within timeout, closing dconn to unblock")
			s.dconn.Close()
			if !s.waitFwdDone() {
				s.logger.Error("forwardMessages still stuck after dconn.Close, abandoning")
			}
		}
		s.fwdDone = nil
	}
	// Close shadow after forwardMessages exits (it calls RecordPrimary).
	s.closeShadow()
}

// dispatchPubSubCommand handles a single command in pub/sub mode.
// Returns false if the session should end (QUIT).
func (s *pubsubSession) dispatchPubSubCommand(args [][]byte) bool {
	switch strings.ToUpper(string(args[0])) {
	case cmdSubscribe:
		s.handleSubscribe(args)
	case cmdUnsubscribe:
		s.handleUnsub(args, false)
	case cmdPSubscribe:
		s.handlePSubscribe(args)
	case cmdPUnsubscribe:
		s.handleUnsub(args, true)
	case cmdPing:
		s.handlePubSubPing(args)
	case cmdQuit:
		s.writeString("OK")
		return false
	default:
		s.writeError("ERR only (P)SUBSCRIBE / (P)UNSUBSCRIBE / PING / QUIT allowed in this context")
	}
	return true
}

// dispatchNormalCommand handles a command in normal (non-pub/sub) mode.
// Returns false if the session should end (QUIT).
func (s *pubsubSession) dispatchNormalCommand(name string, args [][]byte) bool {
	if name == cmdQuit {
		s.writeString("OK")
		return false
	}
	if name == cmdPing {
		s.handleNormalPing(args)
		return true
	}
	// Let the transaction handler process or queue commands first, so that
	// behavior during MULTI is consistent with the main ProxyServer handler.
	if s.handleTxnInSession(name, args) {
		return true
	}
	if name == cmdSubscribe || name == cmdPSubscribe {
		s.reenterPubSub(name, args)
		return true
	}
	if name == cmdUnsubscribe || name == cmdPUnsubscribe {
		s.handleUnsubNoSession(name)
		return true
	}
	s.dispatchRegularCommand(name, args)
	return true
}

// handleTxnInSession handles transaction commands in normal mode.
// Returns true if the command was handled as a transaction command.
func (s *pubsubSession) handleTxnInSession(name string, args [][]byte) bool {
	switch name {
	case cmdMulti:
		if s.inTxn {
			s.writeError("ERR MULTI calls can not be nested")
		} else {
			s.inTxn = true
			s.txnQueue = nil
			s.writeString("OK")
		}
		return true
	case cmdExec:
		if !s.inTxn {
			s.writeError("ERR EXEC without MULTI")
		} else {
			s.execTxn()
		}
		return true
	case cmdDiscard:
		if !s.inTxn {
			s.writeError("ERR DISCARD without MULTI")
		} else {
			s.inTxn = false
			s.txnQueue = nil
			s.writeString("OK")
		}
		return true
	}
	if s.inTxn {
		s.txnQueue = append(s.txnQueue, args)
		s.writeString("QUEUED")
		return true
	}
	return false
}

// handleProxySpecialCommand handles AUTH and SELECT at the proxy level
// (not forwarded to a backend). Returns true if the command was handled.
func (s *pubsubSession) handleProxySpecialCommand(name string, args [][]byte) bool {
	// AUTH is handled at the connection-pool level; accept silently.
	if name == "AUTH" {
		s.writeString("OK")
		return true
	}
	// Mirror ProxyServer's SELECT handling: accept only the configured DB to
	// avoid per-connection DB state with pooled connections.
	if name != "SELECT" {
		return false
	}
	// Enforce Redis arity: SELECT requires exactly one DB index argument.
	if len(args) != selectArgCount {
		s.writeError("ERR wrong number of arguments for 'select' command")
		return true
	}
	dbStr := string(args[1])
	if dbStr != fmt.Sprintf("%d", s.proxy.cfg.PrimaryDB) {
		s.writeError(fmt.Sprintf("ERR proxy configured for DB %d, but SELECT %s requested", s.proxy.cfg.PrimaryDB, dbStr))
		return true
	}
	s.writeString("OK")
	return true
}

// dispatchRegularCommand sends a non-transaction, non-special command to the backend.
func (s *pubsubSession) dispatchRegularCommand(name string, args [][]byte) {
	if s.handleProxySpecialCommand(name, args) {
		return
	}

	cat := ClassifyCommand(name, args[1:])
	ctx := context.Background()

	var resp any
	var err error

	switch cat {
	case CmdWrite:
		resp, err = s.proxy.dual.Write(ctx, name, args)
	case CmdRead:
		resp, err = s.proxy.dual.Read(ctx, name, args)
	case CmdBlocking:
		resp, err = s.proxy.dual.Blocking(s.proxy.shutdownCtx, name, args)
	case CmdPubSub:
		resp, err = s.proxy.dual.Admin(ctx, name, args)
	case CmdAdmin:
		resp, err = s.proxy.dual.Admin(ctx, name, args)
	case CmdScript:
		resp, err = s.proxy.dual.Script(ctx, name, args)
	case CmdTxn:
		// Handled by handleTxnInSession; should not reach here.
		return
	}

	s.writeMu.Lock()
	writeResponse(s.dconn, resp, err)
	s.flushOrClose()
	s.writeMu.Unlock()
}

func (s *pubsubSession) reenterPubSub(cmdName string, args [][]byte) {
	if len(args) < pubsubMinArgs {
		s.writeError(fmt.Sprintf("ERR wrong number of arguments for '%s' command", strings.ToLower(cmdName)))
		return
	}
	psBackend := s.proxy.dual.PubSubBackend()
	if psBackend == nil {
		s.writeError("ERR PubSub not supported by backend")
		return
	}

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
		s.writeRedisError(err)
		return
	}

	shadow := s.proxy.createShadowPubSub(cmdName, channels)

	s.mu.Lock()
	s.upstream = upstream
	s.shadow = shadow
	s.mu.Unlock()

	// Update state and write confirmations together so the per-reply count
	// increments as each channel is added (matching Redis behaviour).
	kind := strings.ToLower(cmdName)
	s.writeMu.Lock()
	for _, ch := range channels {
		if cmdName == cmdSubscribe {
			s.channelSet[ch] = struct{}{}
		} else {
			s.patternSet[ch] = struct{}{}
		}
		s.dconn.WriteArray(pubsubArrayReply)
		s.dconn.WriteBulkString(kind)
		s.dconn.WriteBulkString(ch)
		s.dconn.WriteInt64(int64(s.subCount()))
	}
	s.flushOrClose()
	s.writeMu.Unlock()

	s.startForwarding()
}

func (s *pubsubSession) execTxn() {
	queue := s.txnQueue
	s.inTxn = false
	s.txnQueue = nil

	ctx := context.Background()
	cmds := make([][]any, 0, len(queue)+txnCommandsOverhead)
	cmds = append(cmds, []any{"MULTI"})
	for _, args := range queue {
		cmds = append(cmds, bytesArgsToInterfaces(args))
	}
	cmds = append(cmds, []any{"EXEC"})

	results, err := s.proxy.dual.Primary().Pipeline(ctx, cmds)

	s.writeMu.Lock()
	switch {
	case err != nil:
		// Pipeline-level error (connection/transport failure) takes precedence.
		writeRedisError(s.dconn, err)
	case len(results) > 0:
		lastResult := results[len(results)-1]
		resp, rErr := lastResult.Result()
		writeResponse(s.dconn, resp, rErr)
	default:
		s.dconn.WriteError("ERR empty transaction response")
	}
	s.flushOrClose()
	s.writeMu.Unlock()

	if s.proxy.dual.hasSecondaryWrite() {
		s.proxy.dual.goAsync(func() {
			sCtx, cancel := context.WithTimeout(context.Background(), s.proxy.cfg.SecondaryTimeout)
			defer cancel()
			_, pErr := s.proxy.dual.Secondary().Pipeline(sCtx, cmds)
			if pErr != nil {
				s.proxy.logger.Warn("secondary txn replay failed", "err", pErr)
				s.proxy.metrics.SecondaryWriteErrors.Inc()
			}
		})
	}
}

// --- Subscription handlers ---

func (s *pubsubSession) handleSubscribe(args [][]byte) {
	s.handleSub(args, false)
}

func (s *pubsubSession) handlePSubscribe(args [][]byte) {
	s.handleSub(args, true)
}

// handleSub is the shared implementation for SUBSCRIBE and PSUBSCRIBE.
func (s *pubsubSession) handleSub(args [][]byte, isPattern bool) {
	kind := "subscribe"
	if isPattern {
		kind = "psubscribe"
	}
	if len(args) < pubsubMinArgs {
		s.writeError(fmt.Sprintf("ERR wrong number of arguments for '%s' command", kind))
		return
	}
	names := byteSlicesToStrings(args[1:])
	var err error
	if isPattern {
		err = s.upstream.PSubscribe(context.Background(), names...)
	} else {
		err = s.upstream.Subscribe(context.Background(), names...)
	}
	if err != nil {
		s.logger.Warn("upstream "+kind+" failed", "err", err)
		s.writeRedisError(err)
		return
	}
	s.mirrorSub(names, isPattern)
	// Update state and write confirmations together so the per-reply count
	// increments as each name is added (matching Redis behaviour).
	s.writeMu.Lock()
	for _, n := range names {
		if isPattern {
			s.patternSet[n] = struct{}{}
		} else {
			s.channelSet[n] = struct{}{}
		}
		s.dconn.WriteArray(pubsubArrayReply)
		s.dconn.WriteBulkString(kind)
		s.dconn.WriteBulkString(n)
		s.dconn.WriteInt64(int64(s.subCount()))
	}
	s.flushOrClose()
	s.writeMu.Unlock()
}

// handleUnsub handles both UNSUBSCRIBE and PUNSUBSCRIBE.
// When isPattern is true, it operates on pattern subscriptions.
func (s *pubsubSession) handleUnsub(args [][]byte, isPattern bool) {
	kind := "unsubscribe"
	unsubFn := s.upstream.Unsubscribe
	if isPattern {
		kind = "punsubscribe"
		unsubFn = s.upstream.PUnsubscribe
	}

	if len(args) < pubsubMinArgs {
		// Unsubscribe all: emit per-channel reply (matching Redis behavior).
		if err := unsubFn(context.Background()); err != nil {
			s.logger.Warn("upstream "+kind+" failed", "err", err)
			s.writeRedisError(err)
			return
		}
		if s.shadow != nil {
			s.mirrorUnsubAll(isPattern)
		}
		s.writeUnsubAll(kind, isPattern)
		return
	}

	names := byteSlicesToStrings(args[1:])
	if err := unsubFn(context.Background(), names...); err != nil {
		s.logger.Warn("upstream "+kind+" failed", "err", err)
		s.writeRedisError(err)
		return
	}
	if s.shadow != nil {
		s.mirrorUnsub(names, isPattern)
	}
	// Update state (goroutine-confined) and pre-compute counts before taking writeMu.
	counts := make([]int, len(names))
	for i, n := range names {
		if isPattern {
			delete(s.patternSet, n)
		} else {
			delete(s.channelSet, n)
		}
		counts[i] = s.subCount()
	}
	s.writeMu.Lock()
	for i, n := range names {
		s.dconn.WriteArray(pubsubArrayReply)
		s.dconn.WriteBulkString(kind)
		s.dconn.WriteBulkString(n)
		s.dconn.WriteInt64(int64(counts[i]))
	}
	s.flushOrClose()
	s.writeMu.Unlock()
}

// writeUnsubAll emits per-channel/pattern unsubscribe replies, removing entries
// one-by-one so the subscription count decrements per reply (matching Redis).
func (s *pubsubSession) writeUnsubAll(kind string, isPattern bool) {
	set := s.channelSet
	if isPattern {
		set = s.patternSet
	}

	if len(set) == 0 {
		// No subscriptions: single reply with null channel (matching Redis).
		s.writeMu.Lock()
		s.dconn.WriteArray(pubsubArrayReply)
		s.dconn.WriteBulkString(kind)
		s.dconn.WriteNull()
		s.dconn.WriteInt64(int64(s.subCount()))
		s.flushOrClose()
		s.writeMu.Unlock()
		return
	}

	// Collect names, sort for deterministic reply ordering, and pre-compute
	// decreasing counts (state is goroutine-confined).
	names := make([]string, 0, len(set))
	for n := range set {
		names = append(names, n)
	}
	sort.Strings(names)
	counts := make([]int, len(names))
	for i, n := range names {
		if isPattern {
			delete(s.patternSet, n)
		} else {
			delete(s.channelSet, n)
		}
		counts[i] = s.subCount()
	}

	s.writeMu.Lock()
	for i, n := range names {
		s.dconn.WriteArray(pubsubArrayReply)
		s.dconn.WriteBulkString(kind)
		s.dconn.WriteBulkString(n)
		s.dconn.WriteInt64(int64(counts[i]))
	}
	s.flushOrClose()
	s.writeMu.Unlock()
}

// --- Ping handlers ---

func (s *pubsubSession) handlePubSubPing(args [][]byte) {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	s.dconn.WriteArray(pubsubArrayPong)
	s.dconn.WriteBulkString("pong")
	if len(args) > 1 {
		s.dconn.WriteBulk(args[1])
	} else {
		s.dconn.WriteBulkString("")
	}
	s.flushOrClose()
}

func (s *pubsubSession) handleNormalPing(args [][]byte) {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	if len(args) > 1 {
		s.dconn.WriteBulk(args[1])
	} else {
		s.dconn.WriteString("PONG")
	}
	s.flushOrClose()
}

func (s *pubsubSession) handleUnsubNoSession(cmdName string) {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	s.dconn.WriteArray(pubsubArrayReply)
	s.dconn.WriteBulkString(strings.ToLower(cmdName))
	s.dconn.WriteNull()
	s.dconn.WriteInt64(0)
	s.flushOrClose()
}

func (s *pubsubSession) closeShadow() {
	s.mu.Lock()
	shadow := s.shadow
	s.shadow = nil
	s.mu.Unlock()
	if shadow != nil {
		shadow.Close()
	}
}

// --- Shadow mirror helpers ---

func (s *pubsubSession) mirrorSub(names []string, isPattern bool) {
	if s.shadow == nil {
		return
	}
	var err error
	if isPattern {
		err = s.shadow.PSubscribe(context.Background(), names...)
	} else {
		err = s.shadow.Subscribe(context.Background(), names...)
	}
	if err != nil {
		kind := "subscribe"
		if isPattern {
			kind = "psubscribe"
		}
		s.logger.Warn("shadow "+kind+" failed", "err", err)
		s.proxy.metrics.PubSubShadowErrors.Inc()
	}
}

func (s *pubsubSession) mirrorUnsubAll(isPattern bool) {
	var err error
	if isPattern {
		err = s.shadow.PUnsubscribe(context.Background())
	} else {
		err = s.shadow.Unsubscribe(context.Background())
	}
	if err != nil {
		s.logger.Warn("shadow unsubscribe-all failed", "err", err)
		s.proxy.metrics.PubSubShadowErrors.Inc()
	}
}

func (s *pubsubSession) mirrorUnsub(names []string, isPattern bool) {
	var err error
	if isPattern {
		err = s.shadow.PUnsubscribe(context.Background(), names...)
	} else {
		err = s.shadow.Unsubscribe(context.Background(), names...)
	}
	if err != nil {
		s.logger.Warn("shadow unsubscribe failed", "err", err)
		s.proxy.metrics.PubSubShadowErrors.Inc()
	}
}

// --- Helpers ---

// flushOrClose flushes the detached connection. On error it closes the
// connection and marks the session as closed so that background forwarding
// loops exit promptly. Caller must hold s.writeMu.
func (s *pubsubSession) flushOrClose() {
	if err := s.dconn.Flush(); err != nil {
		s.logger.Warn("failed to flush to client; closing connection", "err", err)
		_ = s.dconn.Close()

		// Mark the session as closed so background forwarding loops can exit promptly.
		s.mu.Lock()
		s.closed = true
		s.mu.Unlock()
	}
}

func (s *pubsubSession) writeError(msg string) {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	s.dconn.WriteError(msg)
	s.flushOrClose()
}

// writeRedisError writes an upstream error, preserving redis.Error prefixes verbatim
// and normalizing other errors to "ERR ..." (matching writeRedisError in proxy.go).
func (s *pubsubSession) writeRedisError(err error) {
	var redisErr redis.Error
	if errors.As(err, &redisErr) {
		s.writeError(redisErr.Error())
		return
	}
	s.writeError("ERR " + err.Error())
}

func (s *pubsubSession) writeString(msg string) {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	s.dconn.WriteString(msg)
	s.flushOrClose()
}

func byteSlicesToStrings(bs [][]byte) []string {
	out := make([]string, len(bs))
	for i, b := range bs {
		out[i] = string(b)
	}
	return out
}
