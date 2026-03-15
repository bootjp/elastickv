package adapter

import (
	"fmt"
	"strings"
	"sync"

	"github.com/tidwall/redcon"
)

const (
	respArrayMessage = 3 // RESP array length for pub/sub message/subscribe/unsubscribe
	respArrayPong    = 2 // RESP array length for PING reply
	pubsubMinCmdArgs = 2 // minimum args for SUBSCRIBE/UNSUBSCRIBE (command + channel)
)

// redisPubSub is a self-contained PubSub implementation that replaces
// redcon.PubSub. It manages subscriptions and detached connections directly,
// so channel counts are always available without relying on reflect/unsafe
// to introspect redcon internals.
type redisPubSub struct {
	mu    sync.RWMutex
	conns map[redcon.Conn]*pubsubConn
	// subs maps channel → set of *pubsubConn subscribed to it
	subs map[string]map[*pubsubConn]struct{}
}

type pubsubConn struct {
	mu      sync.Mutex
	dconn   redcon.DetachedConn
	chans   map[string]struct{}
	closed  bool
	closeCh chan struct{} // closed when bgrunner exits
}

func newRedisPubSub() *redisPubSub {
	return &redisPubSub{
		conns: make(map[redcon.Conn]*pubsubConn),
		subs:  make(map[string]map[*pubsubConn]struct{}),
	}
}

// Subscribe adds a connection to a channel. On first call for a given
// connection, the connection is detached from the main redcon loop and a
// background goroutine is started to handle subsequent subscribe/unsubscribe
// commands on the detached connection.
func (ps *redisPubSub) Subscribe(conn redcon.Conn, channel string) {
	ps.mu.Lock()
	sc, isNew := ps.getOrCreate(conn)
	ps.addEntry(sc, channel)
	ps.mu.Unlock()

	ps.writeSubscribeReply(sc, channel)

	if isNew {
		go sc.bgrunner(ps)
	}
}

// Publish sends a message to all subscribers of the given channel and
// returns the number of clients that received it.
func (ps *redisPubSub) Publish(channel, message string) int {
	ps.mu.RLock()
	set := ps.subs[channel]
	// snapshot subscribers under read lock
	targets := make([]*pubsubConn, 0, len(set))
	for sc := range set {
		targets = append(targets, sc)
	}
	ps.mu.RUnlock()

	sent := 0
	for _, sc := range targets {
		sc.mu.Lock()
		if !sc.closed {
			sc.dconn.WriteArray(respArrayMessage)
			sc.dconn.WriteBulkString("message")
			sc.dconn.WriteBulkString(channel)
			sc.dconn.WriteBulkString(message)
			_ = sc.dconn.Flush()
			sent++
		}
		sc.mu.Unlock()
	}
	return sent
}

// ChannelCounts returns a snapshot of channel → subscriber count.
func (ps *redisPubSub) ChannelCounts() map[string]int {
	ps.mu.RLock()
	defer ps.mu.RUnlock()
	counts := make(map[string]int, len(ps.subs))
	for ch, set := range ps.subs {
		if n := len(set); n > 0 {
			counts[ch] = n
		}
	}
	return counts
}

func (ps *redisPubSub) getOrCreate(conn redcon.Conn) (*pubsubConn, bool) {
	sc, ok := ps.conns[conn]
	if ok {
		return sc, false
	}
	dconn := conn.Detach()
	sc = &pubsubConn{
		dconn:   dconn,
		chans:   make(map[string]struct{}),
		closeCh: make(chan struct{}),
	}
	ps.conns[conn] = sc
	return sc, true
}

func (ps *redisPubSub) addEntry(sc *pubsubConn, channel string) {
	sc.chans[channel] = struct{}{}
	if ps.subs[channel] == nil {
		ps.subs[channel] = make(map[*pubsubConn]struct{})
	}
	ps.subs[channel][sc] = struct{}{}
}

func (ps *redisPubSub) removeEntry(sc *pubsubConn, channel string) {
	delete(sc.chans, channel)
	if set, ok := ps.subs[channel]; ok {
		delete(set, sc)
		if len(set) == 0 {
			delete(ps.subs, channel)
		}
	}
}

func (ps *redisPubSub) removeAll(sc *pubsubConn) {
	for ch := range sc.chans {
		if set, ok := ps.subs[ch]; ok {
			delete(set, sc)
			if len(set) == 0 {
				delete(ps.subs, ch)
			}
		}
	}
	sc.chans = nil
}

func (ps *redisPubSub) writeSubscribeReply(sc *pubsubConn, channel string) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	sc.dconn.WriteArray(respArrayMessage)
	sc.dconn.WriteBulkString("subscribe")
	sc.dconn.WriteBulkString(channel)
	sc.dconn.WriteInt(len(sc.chans))
	_ = sc.dconn.Flush()
}

func (ps *redisPubSub) writeUnsubscribeReply(sc *pubsubConn, channel string, remaining int) {
	// caller must hold sc.mu
	sc.dconn.WriteArray(respArrayMessage)
	sc.dconn.WriteBulkString("unsubscribe")
	if channel == "" {
		sc.dconn.WriteNull()
	} else {
		sc.dconn.WriteBulkString(channel)
	}
	sc.dconn.WriteInt(remaining)
	_ = sc.dconn.Flush()
}

func (sc *pubsubConn) handleSubscribe(ps *redisPubSub, args [][]byte) {
	if len(args) < pubsubMinCmdArgs {
		sc.mu.Lock()
		sc.dconn.WriteError(fmt.Sprintf("ERR wrong number of arguments for '%s'", args[0]))
		_ = sc.dconn.Flush()
		sc.mu.Unlock()
		return
	}
	channels := make([]string, 0, len(args)-1)
	ps.mu.Lock()
	for i := 1; i < len(args); i++ {
		ch := string(args[i])
		ps.addEntry(sc, ch)
		channels = append(channels, ch)
	}
	ps.mu.Unlock()

	for _, ch := range channels {
		ps.writeSubscribeReply(sc, ch)
	}
}

func (sc *pubsubConn) handleUnsubscribe(ps *redisPubSub, args [][]byte) {
	if len(args) < pubsubMinCmdArgs {
		// Unsubscribe all: collect channels, remove entries, capture remaining
		// counts — all under ps.mu — then release ps.mu and write replies.
		type unsubInfo struct {
			channel   string
			remaining int
		}
		ps.mu.Lock()
		channels := make([]string, 0, len(sc.chans))
		for ch := range sc.chans {
			channels = append(channels, ch)
		}
		replies := make([]unsubInfo, 0, len(channels))
		for _, ch := range channels {
			ps.removeEntry(sc, ch)
			replies = append(replies, unsubInfo{channel: ch, remaining: len(sc.chans)})
		}
		ps.mu.Unlock()

		sc.mu.Lock()
		if len(replies) == 0 {
			ps.writeUnsubscribeReply(sc, "", 0)
		} else {
			for _, r := range replies {
				ps.writeUnsubscribeReply(sc, r.channel, r.remaining)
			}
		}
		sc.mu.Unlock()
		return
	}

	// Unsubscribe specific channels: remove entries and capture remaining
	// counts under ps.mu, then release ps.mu and write replies.
	type unsubInfo struct {
		channel   string
		remaining int
	}
	replies := make([]unsubInfo, 0, len(args)-1)
	ps.mu.Lock()
	for i := 1; i < len(args); i++ {
		ch := string(args[i])
		ps.removeEntry(sc, ch)
		replies = append(replies, unsubInfo{channel: ch, remaining: len(sc.chans)})
	}
	ps.mu.Unlock()

	sc.mu.Lock()
	for _, r := range replies {
		ps.writeUnsubscribeReply(sc, r.channel, r.remaining)
	}
	sc.mu.Unlock()
}

func (sc *pubsubConn) handlePing(args [][]byte) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	sc.dconn.WriteArray(respArrayPong)
	sc.dconn.WriteBulkString("pong")
	if len(args) > 1 {
		sc.dconn.WriteBulk(args[1])
	} else {
		sc.dconn.WriteBulkString("")
	}
	_ = sc.dconn.Flush()
}

// bgrunner reads commands from the detached connection (subscribe, unsubscribe).
func (sc *pubsubConn) bgrunner(ps *redisPubSub) {
	defer func() {
		ps.mu.Lock()
		ps.removeAll(sc)
		for conn, s := range ps.conns {
			if s == sc {
				delete(ps.conns, conn)
				break
			}
		}
		ps.mu.Unlock()

		sc.mu.Lock()
		sc.closed = true
		sc.mu.Unlock()
		sc.dconn.Close()
		close(sc.closeCh)
	}()

	for {
		cmd, err := sc.dconn.ReadCommand()
		if err != nil {
			return
		}
		if len(cmd.Args) == 0 {
			continue
		}
		switch strings.ToLower(string(cmd.Args[0])) {
		case "subscribe":
			sc.handleSubscribe(ps, cmd.Args)
		case "unsubscribe":
			sc.handleUnsubscribe(ps, cmd.Args)
		case "ping":
			sc.handlePing(cmd.Args)
		case "quit":
			return
		}
	}
}
