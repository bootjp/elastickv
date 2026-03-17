package proxy

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync"

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
)

// pubsubSession manages a single client's detached connection.
// It bridges a detached redcon connection to an upstream go-redis PubSub connection.
// When all subscriptions are removed, the session transitions to normal command mode,
// enabling the client to execute regular Redis commands without reconnecting.
type pubsubSession struct {
	mu       sync.Mutex
	dconn    redcon.DetachedConn
	upstream *redis.PubSub // nil when not in pub/sub mode
	proxy    *ProxyServer
	logger   *slog.Logger
	closed   bool

	// Track subscription counts for RESP replies.
	channels int
	patterns int

	// fwdDone is closed when the current forwardMessages goroutine exits.
	fwdDone chan struct{}

	// Transaction state for normal command mode.
	inTxn    bool
	txnQueue [][][]byte
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
		<-s.fwdDone
	}
	s.dconn.Close()
}

func (s *pubsubSession) startForwarding() {
	s.fwdDone = make(chan struct{})
	go func() {
		defer close(s.fwdDone)
		s.forwardMessages()
	}()
}

// forwardMessages reads from the upstream go-redis PubSub channel and writes
// messages to the detached client connection.
func (s *pubsubSession) forwardMessages() {
	ch := s.upstream.Channel()
	for msg := range ch {
		s.mu.Lock()
		if s.closed {
			s.mu.Unlock()
			return
		}
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
		s.mu.Unlock()
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

		s.mu.Lock()
		inPubSub := s.channels > 0 || s.patterns > 0
		s.mu.Unlock()

		if inPubSub {
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
	return s.upstream != nil && s.channels == 0 && s.patterns == 0
}

func (s *pubsubSession) exitPubSubMode() {
	s.mu.Lock()
	if s.upstream != nil {
		s.upstream.Close()
		s.upstream = nil
	}
	s.mu.Unlock()
	if s.fwdDone != nil {
		<-s.fwdDone
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
	if name == cmdSubscribe || name == cmdPSubscribe {
		s.reenterPubSub(name, args)
		return true
	}
	if name == cmdUnsubscribe || name == cmdPUnsubscribe {
		s.handleUnsubNoSession(name)
		return true
	}
	if s.handleTxnInSession(name, args) {
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
	cat := ClassifyCommand(name, args[1:])
	ctx := context.Background()

	var resp interface{}
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

	s.mu.Lock()
	writeResponse(s.dconn, resp, err)
	_ = s.dconn.Flush()
	s.mu.Unlock()
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
		s.writeError("ERR " + err.Error())
		return
	}

	s.mu.Lock()
	s.upstream = upstream
	s.mu.Unlock()
	s.startForwarding()

	kind := strings.ToLower(cmdName)
	s.mu.Lock()
	for _, ch := range channels {
		if cmdName == cmdSubscribe {
			s.channels++
		} else {
			s.patterns++
		}
		s.dconn.WriteArray(pubsubArrayReply)
		s.dconn.WriteBulkString(kind)
		s.dconn.WriteBulkString(ch)
		s.dconn.WriteInt(s.channels + s.patterns)
	}
	_ = s.dconn.Flush()
	s.mu.Unlock()
}

func (s *pubsubSession) execTxn() {
	queue := s.txnQueue
	s.inTxn = false
	s.txnQueue = nil

	ctx := context.Background()
	cmds := make([][]interface{}, 0, len(queue)+txnCommandsOverhead)
	cmds = append(cmds, []interface{}{"MULTI"})
	for _, args := range queue {
		cmds = append(cmds, bytesArgsToInterfaces(args))
	}
	cmds = append(cmds, []interface{}{"EXEC"})

	results, err := s.proxy.dual.Primary().Pipeline(ctx, cmds)

	s.mu.Lock()
	if len(results) > 0 {
		lastResult := results[len(results)-1]
		resp, rErr := lastResult.Result()
		writeResponse(s.dconn, resp, rErr)
	} else if err != nil {
		writeRedisError(s.dconn, err)
	}
	_ = s.dconn.Flush()
	s.mu.Unlock()

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
		return
	}
	s.mu.Lock()
	for _, ch := range channels {
		s.channels++
		s.dconn.WriteArray(pubsubArrayReply)
		s.dconn.WriteBulkString("subscribe")
		s.dconn.WriteBulkString(ch)
		s.dconn.WriteInt(s.channels + s.patterns)
	}
	_ = s.dconn.Flush()
	s.mu.Unlock()
}

func (s *pubsubSession) handlePSubscribe(args [][]byte) {
	if len(args) < pubsubMinArgs {
		s.writeError("ERR wrong number of arguments for 'psubscribe'")
		return
	}
	pats := byteSlicesToStrings(args[1:])
	if err := s.upstream.PSubscribe(context.Background(), pats...); err != nil {
		s.logger.Warn("upstream psubscribe failed", "err", err)
		return
	}
	s.mu.Lock()
	for _, p := range pats {
		s.patterns++
		s.dconn.WriteArray(pubsubArrayReply)
		s.dconn.WriteBulkString("psubscribe")
		s.dconn.WriteBulkString(p)
		s.dconn.WriteInt(s.channels + s.patterns)
	}
	_ = s.dconn.Flush()
	s.mu.Unlock()
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
		// Unsubscribe all
		if err := unsubFn(context.Background()); err != nil {
			s.logger.Warn("upstream "+kind+" failed", "err", err)
		}
		s.mu.Lock()
		if isPattern {
			s.patterns = 0
		} else {
			s.channels = 0
		}
		s.dconn.WriteArray(pubsubArrayReply)
		s.dconn.WriteBulkString(kind)
		s.dconn.WriteNull()
		s.dconn.WriteInt(s.channels + s.patterns)
		_ = s.dconn.Flush()
		s.mu.Unlock()
		return
	}

	names := byteSlicesToStrings(args[1:])
	if err := unsubFn(context.Background(), names...); err != nil {
		s.logger.Warn("upstream "+kind+" failed", "err", err)
	}
	s.mu.Lock()
	for _, n := range names {
		if isPattern {
			if s.patterns > 0 {
				s.patterns--
			}
		} else {
			if s.channels > 0 {
				s.channels--
			}
		}
		s.dconn.WriteArray(pubsubArrayReply)
		s.dconn.WriteBulkString(kind)
		s.dconn.WriteBulkString(n)
		s.dconn.WriteInt(s.channels + s.patterns)
	}
	_ = s.dconn.Flush()
	s.mu.Unlock()
}

// --- Ping handlers ---

func (s *pubsubSession) handlePubSubPing(args [][]byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
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
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(args) > 1 {
		s.dconn.WriteBulk(args[1])
	} else {
		s.dconn.WriteString("PONG")
	}
	_ = s.dconn.Flush()
}

func (s *pubsubSession) handleUnsubNoSession(cmdName string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.dconn.WriteArray(pubsubArrayReply)
	s.dconn.WriteBulkString(strings.ToLower(cmdName))
	s.dconn.WriteNull()
	s.dconn.WriteInt64(0)
	_ = s.dconn.Flush()
}

// --- Helpers ---

func (s *pubsubSession) writeError(msg string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.dconn.WriteError(msg)
	_ = s.dconn.Flush()
}

func (s *pubsubSession) writeString(msg string) {
	s.mu.Lock()
	defer s.mu.Unlock()
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
