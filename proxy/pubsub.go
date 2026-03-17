package proxy

import (
	"context"
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
)

// pubsubSession manages a single client's pub/sub session.
// It bridges a detached redcon connection to an upstream go-redis PubSub connection.
type pubsubSession struct {
	mu       sync.Mutex
	dconn    redcon.DetachedConn
	upstream *redis.PubSub
	logger   *slog.Logger
	closed   bool

	// Track subscription counts for RESP replies.
	channels int
	patterns int
}

// run starts the forwarding session. It blocks until the client disconnects
// or the upstream closes.
func (s *pubsubSession) run() {
	defer func() {
		s.upstream.Close()
		s.mu.Lock()
		s.closed = true
		s.mu.Unlock()
		s.dconn.Close()
	}()

	go s.forwardMessages()
	s.readClientCommands()
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

// readClientCommands reads commands from the detached client connection.
// In pub/sub mode, only SUBSCRIBE, UNSUBSCRIBE, PSUBSCRIBE, PUNSUBSCRIBE,
// PING, and QUIT are valid.
func (s *pubsubSession) readClientCommands() {
	for {
		cmd, err := s.dconn.ReadCommand()
		if err != nil {
			return
		}
		if len(cmd.Args) == 0 {
			continue
		}
		if !s.dispatchPubSubCommand(cmd.Args) {
			return
		}
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
	case "PING":
		s.handlePing(args)
	case "QUIT":
		return false
	default:
		s.writeError("ERR only (P)SUBSCRIBE / (P)UNSUBSCRIBE / PING / QUIT allowed in this context")
	}
	return true
}

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

func (s *pubsubSession) handlePing(args [][]byte) {
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

func (s *pubsubSession) writeError(msg string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.dconn.WriteError(msg)
	_ = s.dconn.Flush()
}

func byteSlicesToStrings(bs [][]byte) []string {
	out := make([]string, len(bs))
	for i, b := range bs {
		out[i] = string(b)
	}
	return out
}
