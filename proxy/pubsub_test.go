package proxy

import (
	"errors"
	"net"
	"sync"
	"testing"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/tidwall/redcon"
)

// Tagged types to distinguish RESP wire types in mock writes.
type respInt struct{ V int }     // WriteInt
type respInt64 struct{ V int64 } // WriteInt64
type respArray struct{ N int }   // WriteArray

// mockDetachedConn implements redcon.DetachedConn for unit testing.
type mockDetachedConn struct {
	mu       sync.Mutex
	commands []redcon.Command // queued commands to return from ReadCommand
	cmdIdx   int
	writes   []any // recorded writes: string for WriteString/WriteError/WriteBulkString, respInt/respInt64/respArray for typed ints, nil for WriteNull
	closed   bool
	readErr  error // error to return from ReadCommand when commands exhausted
}

func newMockDetachedConn() *mockDetachedConn {
	return &mockDetachedConn{
		readErr: errors.New("EOF"),
	}
}

func (m *mockDetachedConn) queueCommand(args ...string) {
	bArgs := make([][]byte, len(args))
	for i, a := range args {
		bArgs[i] = []byte(a)
	}
	m.commands = append(m.commands, redcon.Command{Args: bArgs})
}

func (m *mockDetachedConn) ReadCommand() (redcon.Command, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.cmdIdx >= len(m.commands) {
		return redcon.Command{}, m.readErr
	}
	cmd := m.commands[m.cmdIdx]
	m.cmdIdx++
	return cmd, nil
}

func (m *mockDetachedConn) Flush() error { return nil }
func (m *mockDetachedConn) Close() error { m.closed = true; return nil }

func (m *mockDetachedConn) RemoteAddr() string { return "127.0.0.1:9999" }

func (m *mockDetachedConn) WriteError(msg string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.writes = append(m.writes, "ERR:"+msg)
}
func (m *mockDetachedConn) WriteString(str string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.writes = append(m.writes, "STR:"+str)
}
func (m *mockDetachedConn) WriteBulk(bulk []byte) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.writes = append(m.writes, "BULK:"+string(bulk))
}
func (m *mockDetachedConn) WriteBulkString(bulk string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.writes = append(m.writes, "BULKSTR:"+bulk)
}
func (m *mockDetachedConn) WriteInt(num int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.writes = append(m.writes, respInt{num})
}
func (m *mockDetachedConn) WriteInt64(num int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.writes = append(m.writes, respInt64{num})
}
func (m *mockDetachedConn) WriteUint64(_ uint64) {
	// Not used in pubsub tests.
}
func (m *mockDetachedConn) WriteArray(count int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.writes = append(m.writes, respArray{count})
}
func (m *mockDetachedConn) WriteNull() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.writes = append(m.writes, nil)
}
func (m *mockDetachedConn) WriteRaw(data []byte) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.writes = append(m.writes, "RAW:"+string(data))
}
func (m *mockDetachedConn) WriteAny(v any) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.writes = append(m.writes, v)
}
func (m *mockDetachedConn) Context() any                   { return nil }
func (m *mockDetachedConn) SetContext(v any)               {}
func (m *mockDetachedConn) SetReadBuffer(n int)            {}
func (m *mockDetachedConn) Detach() redcon.DetachedConn    { return m }
func (m *mockDetachedConn) ReadPipeline() []redcon.Command { return nil }
func (m *mockDetachedConn) PeekPipeline() []redcon.Command { return nil }
func (m *mockDetachedConn) NetConn() net.Conn              { return nil }

func (m *mockDetachedConn) getWrites() []any {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]any, len(m.writes))
	copy(out, m.writes)
	return out
}

// newTestSession creates a pubsubSession with a mock connection for testing.
func newTestSession(dconn *mockDetachedConn) *pubsubSession {
	return &pubsubSession{
		dconn:      dconn,
		logger:     testLogger,
		channelSet: make(map[string]struct{}),
		patternSet: make(map[string]struct{}),
	}
}

func TestPubSub_SubscribeDuplicate_IdempotentCount(t *testing.T) {
	dconn := newMockDetachedConn()
	s := newTestSession(dconn)

	// Simulate an upstream that's already connected.
	s.channelSet["ch1"] = struct{}{}

	// Subscribe to ch1 again and ch2 (should count ch1 only once).
	// We can't call handleSubscribe directly because it needs s.upstream,
	// so we test the set logic directly.
	s.channelSet["ch1"] = struct{}{} // re-add same key
	s.channelSet["ch2"] = struct{}{}

	assert.Equal(t, 2, len(s.channelSet), "duplicate subscribe should not increase count")
	assert.Equal(t, 2, s.subCount())
}

func TestPubSub_UnsubscribeNonSubscribed_NoEffect(t *testing.T) {
	dconn := newMockDetachedConn()
	s := newTestSession(dconn)

	s.channelSet["ch1"] = struct{}{}
	s.channelSet["ch2"] = struct{}{}

	// Unsubscribe from "ch3" which was never subscribed.
	delete(s.channelSet, "ch3")

	assert.Equal(t, 2, len(s.channelSet), "unsubscribe non-subscribed channel should not affect count")
}

func TestPubSub_UnsubscribeSpecific_RemovesFromSet(t *testing.T) {
	dconn := newMockDetachedConn()
	s := newTestSession(dconn)

	s.channelSet["ch1"] = struct{}{}
	s.channelSet["ch2"] = struct{}{}
	s.channelSet["ch3"] = struct{}{}

	delete(s.channelSet, "ch2")

	assert.Equal(t, 2, len(s.channelSet))
	_, hasCh1 := s.channelSet["ch1"]
	_, hasCh2 := s.channelSet["ch2"]
	_, hasCh3 := s.channelSet["ch3"]
	assert.True(t, hasCh1)
	assert.False(t, hasCh2)
	assert.True(t, hasCh3)
}

func TestPubSub_WriteUnsubAll_PerChannelReplies(t *testing.T) {
	dconn := newMockDetachedConn()
	s := newTestSession(dconn)

	s.channelSet["ch1"] = struct{}{}
	s.channelSet["ch2"] = struct{}{}
	s.patternSet["pat1"] = struct{}{}

	s.mu.Lock()
	s.writeUnsubAll("unsubscribe", false)
	s.mu.Unlock()

	writes := dconn.getWrites()

	// Should have emitted 2 unsubscribe replies (one per channel).
	// Each reply is: WriteArray(3), WriteBulkString("unsubscribe"), WriteBulkString(name), WriteInt(remaining)
	assert.Equal(t, 0, len(s.channelSet), "channelSet should be cleared")
	assert.Equal(t, 1, len(s.patternSet), "patternSet should not be affected")

	// Count array headers (each reply starts with WriteArray(3))
	arrCount := 0
	for _, w := range writes {
		if a, ok := w.(respArray); ok && a.N == 3 {
			arrCount++
		}
	}
	assert.Equal(t, 2, arrCount, "should emit one reply per unsubscribed channel")
}

func TestPubSub_WriteUnsubAll_EmptySet_SingleNullReply(t *testing.T) {
	dconn := newMockDetachedConn()
	s := newTestSession(dconn)

	// No subscriptions
	s.mu.Lock()
	s.writeUnsubAll("unsubscribe", false)
	s.mu.Unlock()

	writes := dconn.getWrites()

	// Should emit single reply with null channel.
	// WriteArray(3), WriteBulkString("unsubscribe"), WriteNull(), WriteInt(0)
	assert.Len(t, writes, 4, "single null-channel reply expected")

	hasNull := false
	for _, w := range writes {
		if w == nil {
			hasNull = true
		}
	}
	assert.True(t, hasNull, "should contain null for empty unsubscribe-all")
}

func TestPubSub_WriteUnsubAll_Patterns(t *testing.T) {
	dconn := newMockDetachedConn()
	s := newTestSession(dconn)

	s.patternSet["h*"] = struct{}{}
	s.channelSet["ch1"] = struct{}{}

	s.mu.Lock()
	s.writeUnsubAll("punsubscribe", true)
	s.mu.Unlock()

	assert.Equal(t, 0, len(s.patternSet), "patternSet should be cleared")
	assert.Equal(t, 1, len(s.channelSet), "channelSet should not be affected")

	writes := dconn.getWrites()
	// Should have 1 reply (one pattern)
	arrCount := 0
	for _, w := range writes {
		if a, ok := w.(respArray); ok && a.N == 3 {
			arrCount++
		}
	}
	assert.Equal(t, 1, arrCount)
}

func TestPubSub_SubCount(t *testing.T) {
	s := newTestSession(newMockDetachedConn())

	assert.Equal(t, 0, s.subCount())

	s.channelSet["a"] = struct{}{}
	assert.Equal(t, 1, s.subCount())

	s.patternSet["b*"] = struct{}{}
	assert.Equal(t, 2, s.subCount())

	s.channelSet["a"] = struct{}{} // duplicate
	assert.Equal(t, 2, s.subCount())

	delete(s.channelSet, "a")
	assert.Equal(t, 1, s.subCount())
}

func TestPubSub_DispatchPubSubCommand_Quit(t *testing.T) {
	dconn := newMockDetachedConn()
	s := newTestSession(dconn)

	cont := s.dispatchPubSubCommand([][]byte{[]byte("QUIT")})
	assert.False(t, cont, "QUIT should return false")

	writes := dconn.getWrites()
	assert.Contains(t, writes, "STR:OK")
}

func TestPubSub_DispatchPubSubCommand_InvalidCommand(t *testing.T) {
	dconn := newMockDetachedConn()
	s := newTestSession(dconn)

	cont := s.dispatchPubSubCommand([][]byte{[]byte("GET"), []byte("key")})
	assert.True(t, cont, "invalid command should not end session")

	writes := dconn.getWrites()
	found := false
	for _, w := range writes {
		if str, ok := w.(string); ok && str == "ERR:ERR only (P)SUBSCRIBE / (P)UNSUBSCRIBE / PING / QUIT allowed in this context" {
			found = true
		}
	}
	assert.True(t, found, "should write error for invalid pub/sub command")
}

func TestPubSub_DispatchNormalCommand_Quit(t *testing.T) {
	dconn := newMockDetachedConn()
	s := newTestSession(dconn)

	cont := s.dispatchNormalCommand("QUIT", [][]byte{[]byte("QUIT")})
	assert.False(t, cont, "QUIT should return false")
}

func TestPubSub_DispatchNormalCommand_Ping(t *testing.T) {
	dconn := newMockDetachedConn()
	s := newTestSession(dconn)

	cont := s.dispatchNormalCommand("PING", [][]byte{[]byte("PING")})
	assert.True(t, cont)

	writes := dconn.getWrites()
	assert.Contains(t, writes, "STR:PONG")
}

func TestPubSub_DispatchNormalCommand_PingWithMessage(t *testing.T) {
	dconn := newMockDetachedConn()
	s := newTestSession(dconn)

	cont := s.dispatchNormalCommand("PING", [][]byte{[]byte("PING"), []byte("hello")})
	assert.True(t, cont)

	writes := dconn.getWrites()
	assert.Contains(t, writes, "BULK:hello")
}

func TestPubSub_HandlePubSubPing(t *testing.T) {
	dconn := newMockDetachedConn()
	s := newTestSession(dconn)

	s.handlePubSubPing([][]byte{[]byte("PING")})

	writes := dconn.getWrites()
	// ["pong", ""]
	assert.Contains(t, writes, respArray{2}) // WriteArray(2)
	assert.Contains(t, writes, "BULKSTR:pong")
	assert.Contains(t, writes, "BULKSTR:") // empty string
}

func TestPubSub_HandlePubSubPingWithData(t *testing.T) {
	dconn := newMockDetachedConn()
	s := newTestSession(dconn)

	s.handlePubSubPing([][]byte{[]byte("PING"), []byte("hello")})

	writes := dconn.getWrites()
	assert.Contains(t, writes, "BULK:hello")
}

func TestPubSub_HandleUnsubNoSession(t *testing.T) {
	dconn := newMockDetachedConn()
	s := newTestSession(dconn)

	s.handleUnsubNoSession("UNSUBSCRIBE")

	writes := dconn.getWrites()
	// ["unsubscribe", null, 0]
	assert.Contains(t, writes, "BULKSTR:unsubscribe")
	hasNull := false
	for _, w := range writes {
		if w == nil {
			hasNull = true
		}
	}
	assert.True(t, hasNull)
	assert.Contains(t, writes, respInt64{0}) // WriteInt64(0)
}

func TestPubSub_SubscribeInTxnRejected(t *testing.T) {
	dconn := newMockDetachedConn()
	s := newTestSession(dconn)
	s.inTxn = true

	cont := s.dispatchNormalCommand("SUBSCRIBE", [][]byte{[]byte("SUBSCRIBE"), []byte("ch1")})
	assert.True(t, cont)

	writes := dconn.getWrites()
	found := false
	for _, w := range writes {
		if str, ok := w.(string); ok && str == "ERR:ERR Command not allowed inside a transaction" {
			found = true
		}
	}
	assert.True(t, found, "SUBSCRIBE during MULTI should be rejected")
}

func TestPubSub_HandleTxnInSession_MultiExecDiscard(t *testing.T) {
	dconn := newMockDetachedConn()
	s := newTestSession(dconn)

	// MULTI
	handled := s.handleTxnInSession("MULTI", nil)
	assert.True(t, handled)
	assert.True(t, s.inTxn)

	// Nested MULTI
	dconn2 := newMockDetachedConn()
	s2 := newTestSession(dconn2)
	s2.inTxn = true
	handled = s2.handleTxnInSession("MULTI", nil)
	assert.True(t, handled)
	writes := dconn2.getWrites()
	found := false
	for _, w := range writes {
		if str, ok := w.(string); ok && str == "ERR:ERR MULTI calls can not be nested" {
			found = true
		}
	}
	assert.True(t, found)

	// Queue a command
	handled = s.handleTxnInSession("SET", [][]byte{[]byte("SET"), []byte("k"), []byte("v")})
	assert.True(t, handled)
	assert.Len(t, s.txnQueue, 1)

	// DISCARD
	handled = s.handleTxnInSession("DISCARD", nil)
	assert.True(t, handled)
	assert.False(t, s.inTxn)
	assert.Nil(t, s.txnQueue)

	// EXEC without MULTI
	handled = s.handleTxnInSession("EXEC", nil)
	assert.True(t, handled)
	writes = dconn.getWrites()
	found = false
	for _, w := range writes {
		if str, ok := w.(string); ok && str == "ERR:ERR EXEC without MULTI" {
			found = true
		}
	}
	assert.True(t, found)

	// Non-txn command when not in txn
	handled = s.handleTxnInSession("GET", [][]byte{[]byte("GET"), []byte("k")})
	assert.False(t, handled, "non-txn command outside txn should not be handled")
}

func TestPubSub_ShouldExitPubSub(t *testing.T) {
	s := newTestSession(newMockDetachedConn())

	// No upstream, no subs → false (no upstream to close)
	assert.False(t, s.shouldExitPubSub())

	// With upstream but has subs → false
	s.upstream = &redis.PubSub{}
	s.channelSet["ch1"] = struct{}{}
	assert.False(t, s.shouldExitPubSub())

	// With upstream and no subs → true
	delete(s.channelSet, "ch1")
	assert.True(t, s.shouldExitPubSub())
}

func TestPubSub_ByteSlicesToStrings(t *testing.T) {
	input := [][]byte{[]byte("hello"), []byte("world")}
	result := byteSlicesToStrings(input)
	assert.Equal(t, []string{"hello", "world"}, result)

	// Empty
	assert.Equal(t, []string{}, byteSlicesToStrings([][]byte{}))
}

func TestPubSub_SelectAuthSilentlyAccepted(t *testing.T) {
	for _, cmd := range []string{"SELECT", "AUTH"} {
		t.Run(cmd, func(t *testing.T) {
			dconn := newMockDetachedConn()
			s := newTestSession(dconn)

			s.dispatchRegularCommand(cmd, [][]byte{[]byte(cmd), []byte("0")})

			writes := dconn.getWrites()
			assert.Contains(t, writes, "STR:OK")
		})
	}
}

// TestPubSub_CleanupClosesUpstream verifies that cleanup closes upstream and dconn.
func TestPubSub_CleanupClosesUpstream(t *testing.T) {
	dconn := newMockDetachedConn()
	s := newTestSession(dconn)

	s.cleanup()
	assert.True(t, s.closed)
	assert.True(t, dconn.closed)
	assert.Nil(t, s.upstream)
}

// TestPubSub_CommandLoop_EmptyArgs verifies that empty command args are skipped.
func TestPubSub_CommandLoop_EmptyArgs(t *testing.T) {
	dconn := newMockDetachedConn()
	// Queue empty args then QUIT
	dconn.commands = append(dconn.commands, redcon.Command{Args: nil})
	dconn.queueCommand("QUIT")

	s := newTestSession(dconn)
	// Not in pub/sub mode, so should dispatch as normal commands
	s.commandLoop()

	writes := dconn.getWrites()
	assert.Contains(t, writes, "STR:OK", "QUIT should be handled")
}

func TestPubSub_CommandLoop_EOF(t *testing.T) {
	dconn := newMockDetachedConn()
	dconn.readErr = errors.New("connection closed")

	s := newTestSession(dconn)
	s.commandLoop() // should return without panic
}
