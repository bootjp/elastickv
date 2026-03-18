package proxy

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
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
	mu       sync.Mutex // protects upstream and closed (channelSet, patternSet, txn are goroutine-confined to commandLoop)
	writeMu  sync.Mutex // serializes writes to dconn; never held across state operations
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
		// close dconn to unblock it, then wait for completion.
		select {
		case <-s.fwdDone:
		case <-time.After(cleanupFwdTimeout):
			s.logger.Warn("forwardMessages did not exit within timeout, closing dconn to unblock")
			s.dconn.Close()
			<-s.fwdDone
			return // dconn already closed
		}
	}
	s.dconn.Close()
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
			return
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
		select {
		case <-s.fwdDone:
		case <-time.After(cleanupFwdTimeout):
			s.logger.Warn("forwardMessages did not exit within timeout, closing dconn to unblock")
			s.dconn.Close()
			<-s.fwdDone
		}
		s.fwdDone = nil
	}
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

// dispatchRegularCommand sends a non-transaction, non-special command to the backend.
func (s *pubsubSession) dispatchRegularCommand(name string, args [][]byte) {
	// SELECT and AUTH are handled at the connection-pool level; accept silently.
	if name == "SELECT" || name == "AUTH" {
		s.writeString("OK")
		return
	}

	cat := ClassifyCommand(name, args[1:])
	ctx := context.Background()

	var resp any
	var err error

	switch cat {
	case CmdWrite:
		resp, err = s.proxy.dual.Write(ctx, args)
	case CmdRead:
		resp, err = s.proxy.dual.Read(ctx, args)
	case CmdBlocking:
		resp, err = s.proxy.dual.Blocking(s.proxy.shutdownCtx, args)
	case CmdPubSub:
		resp, err = s.proxy.dual.Admin(ctx, args)
	case CmdAdmin:
		resp, err = s.proxy.dual.Admin(ctx, args)
	case CmdScript:
		resp, err = s.proxy.dual.Script(ctx, args)
	case CmdTxn:
		// Handled by handleTxnInSession; should not reach here.
		return
	}

	s.writeMu.Lock()
	writeResponse(s.dconn, resp, err)
	_ = s.dconn.Flush()
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

	s.mu.Lock()
	s.upstream = upstream
	s.mu.Unlock()
	s.startForwarding()

	// Update state (sets only accessed from commandLoop goroutine).
	kind := strings.ToLower(cmdName)
	for _, ch := range channels {
		if cmdName == cmdSubscribe {
			s.channelSet[ch] = struct{}{}
		} else {
			s.patternSet[ch] = struct{}{}
		}
	}
	s.writeMu.Lock()
	for _, ch := range channels {
		s.dconn.WriteArray(pubsubArrayReply)
		s.dconn.WriteBulkString(kind)
		s.dconn.WriteBulkString(ch)
		s.dconn.WriteInt64(int64(s.subCount()))
	}
	_ = s.dconn.Flush()
	s.writeMu.Unlock()
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
	if err != nil {
		// Pipeline-level error (connection/transport failure) takes precedence.
		writeRedisError(s.dconn, err)
	} else if len(results) > 0 {
		lastResult := results[len(results)-1]
		resp, rErr := lastResult.Result()
		writeResponse(s.dconn, resp, rErr)
	}
	_ = s.dconn.Flush()
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
	if len(args) < pubsubMinArgs {
		s.writeError("ERR wrong number of arguments for 'subscribe'")
		return
	}
	channels := byteSlicesToStrings(args[1:])
	if err := s.upstream.Subscribe(context.Background(), channels...); err != nil {
		s.logger.Warn("upstream subscribe failed", "err", err)
		s.writeRedisError(err)
		return
	}
	// Update state (channelSet is only accessed from commandLoop goroutine).
	for _, ch := range channels {
		s.channelSet[ch] = struct{}{}
	}
	s.writeMu.Lock()
	for _, ch := range channels {
		s.dconn.WriteArray(pubsubArrayReply)
		s.dconn.WriteBulkString("subscribe")
		s.dconn.WriteBulkString(ch)
		s.dconn.WriteInt64(int64(s.subCount()))
	}
	_ = s.dconn.Flush()
	s.writeMu.Unlock()
}

func (s *pubsubSession) handlePSubscribe(args [][]byte) {
	if len(args) < pubsubMinArgs {
		s.writeError("ERR wrong number of arguments for 'psubscribe'")
		return
	}
	pats := byteSlicesToStrings(args[1:])
	if err := s.upstream.PSubscribe(context.Background(), pats...); err != nil {
		s.logger.Warn("upstream psubscribe failed", "err", err)
		s.writeRedisError(err)
		return
	}
	// Update state (patternSet is only accessed from commandLoop goroutine).
	for _, p := range pats {
		s.patternSet[p] = struct{}{}
	}
	s.writeMu.Lock()
	for _, p := range pats {
		s.dconn.WriteArray(pubsubArrayReply)
		s.dconn.WriteBulkString("psubscribe")
		s.dconn.WriteBulkString(p)
		s.dconn.WriteInt64(int64(s.subCount()))
	}
	_ = s.dconn.Flush()
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
			s.logger.Warn("upstream "+kind+" failed, closing session", "err", err)
			s.writeRedisError(err)
			return
		}
		s.writeUnsubAll(kind, isPattern)
		return
	}

	names := byteSlicesToStrings(args[1:])
	if err := unsubFn(context.Background(), names...); err != nil {
		s.logger.Warn("upstream "+kind+" failed, closing session", "err", err)
		s.writeRedisError(err)
		return
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
	_ = s.dconn.Flush()
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
		_ = s.dconn.Flush()
		s.writeMu.Unlock()
		return
	}

	// Collect names and pre-compute decreasing counts (state is goroutine-confined).
	names := make([]string, 0, len(set))
	for n := range set {
		names = append(names, n)
	}
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
	_ = s.dconn.Flush()
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
	_ = s.dconn.Flush()
}

func (s *pubsubSession) handleNormalPing(args [][]byte) {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	if len(args) > 1 {
		s.dconn.WriteBulk(args[1])
	} else {
		s.dconn.WriteString("PONG")
	}
	_ = s.dconn.Flush()
}

func (s *pubsubSession) handleUnsubNoSession(cmdName string) {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	s.dconn.WriteArray(pubsubArrayReply)
	s.dconn.WriteBulkString(strings.ToLower(cmdName))
	s.dconn.WriteNull()
	s.dconn.WriteInt64(0)
	_ = s.dconn.Flush()
}

// --- Helpers ---

func (s *pubsubSession) writeError(msg string) {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	s.dconn.WriteError(msg)
	_ = s.dconn.Flush()
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
	_ = s.dconn.Flush()
}

func byteSlicesToStrings(bs [][]byte) []string {
	out := make([]string, len(bs))
	for i, b := range bs {
		out[i] = string(b)
	}
	return out
}
