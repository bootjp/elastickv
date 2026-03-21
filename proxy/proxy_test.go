package proxy

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
)

var testLogger = slog.New(slog.NewTextHandler(io.Discard, nil))

// --- Mock Backend ---

type mockBackend struct {
	name   string
	doFunc func(ctx context.Context, args ...any) *redis.Cmd
	mu     sync.Mutex
	calls  [][]any
}

func newMockBackend(name string) *mockBackend {
	return &mockBackend{name: name}
}

func (b *mockBackend) Do(ctx context.Context, args ...any) *redis.Cmd {
	b.mu.Lock()
	b.calls = append(b.calls, args)
	b.mu.Unlock()
	if b.doFunc != nil {
		return b.doFunc(ctx, args...)
	}
	cmd := redis.NewCmd(ctx, args...)
	cmd.SetVal("OK")
	return cmd
}

func (b *mockBackend) Pipeline(ctx context.Context, cmds [][]any) ([]*redis.Cmd, error) {
	results := make([]*redis.Cmd, len(cmds))
	for i, args := range cmds {
		results[i] = b.Do(ctx, args...)
	}
	return results, nil
}

func (b *mockBackend) Close() error { return nil }
func (b *mockBackend) Name() string { return b.name }

func (b *mockBackend) CallCount() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.calls)
}

// Helper to create a doFunc that returns a specific value.
func makeCmd(val any, err error) func(ctx context.Context, args ...any) *redis.Cmd {
	return func(ctx context.Context, args ...any) *redis.Cmd {
		cmd := redis.NewCmd(ctx, args...)
		if err != nil {
			cmd.SetErr(err)
		} else {
			cmd.SetVal(val)
		}
		return cmd
	}
}

func newTestMetrics() *ProxyMetrics {
	reg := prometheus.NewRegistry()
	return NewProxyMetrics(reg)
}

func newTestSentry() *SentryReporter {
	return NewSentryReporter("", "", 1.0, testLogger)
}

// ========== command.go tests ==========

func TestClassifyCommand(t *testing.T) {
	tests := []struct {
		name     string
		cmd      string
		args     [][]byte
		expected CommandCategory
	}{
		{"GET is read", "GET", nil, CmdRead},
		{"SET is write", "SET", nil, CmdWrite},
		{"DEL is write", "DEL", nil, CmdWrite},
		{"HGET is read", "HGET", nil, CmdRead},
		{"HSET is write", "HSET", nil, CmdWrite},
		{"ZADD is write", "ZADD", nil, CmdWrite},
		{"ZRANGE is read", "ZRANGE", nil, CmdRead},
		{"PING is admin", "PING", nil, CmdAdmin},
		{"INFO is admin", "INFO", nil, CmdAdmin},
		{"SELECT is admin", "SELECT", nil, CmdAdmin},
		{"QUIT is admin", "QUIT", nil, CmdAdmin},
		{"AUTH is admin", "AUTH", nil, CmdAdmin},
		{"HELLO is admin", "HELLO", nil, CmdAdmin},
		{"CONFIG is admin", "CONFIG", nil, CmdAdmin},
		{"WAIT is admin", "WAIT", nil, CmdAdmin},
		{"COMMAND is admin", "COMMAND", nil, CmdAdmin},
		{"MULTI is txn", "MULTI", nil, CmdTxn},
		{"EXEC is txn", "EXEC", nil, CmdTxn},
		{"DISCARD is txn", "DISCARD", nil, CmdTxn},
		{"SUBSCRIBE is pubsub", "SUBSCRIBE", nil, CmdPubSub},
		{"UNSUBSCRIBE is pubsub", "UNSUBSCRIBE", nil, CmdPubSub},
		{"PSUBSCRIBE is pubsub", "PSUBSCRIBE", nil, CmdPubSub},
		{"PUNSUBSCRIBE is pubsub", "PUNSUBSCRIBE", nil, CmdPubSub},
		{"BZPOPMIN is blocking", "BZPOPMIN", nil, CmdBlocking},
		{"EVAL is script", "EVAL", nil, CmdScript},
		{"lowercase get is read", "get", nil, CmdRead},
		{"GETDEL is write", "GETDEL", nil, CmdWrite},
		{"PUBLISH is write", "PUBLISH", nil, CmdWrite},
		{"unknown cmd is write", "UNKNOWNCMD", nil, CmdWrite},

		// XREAD special handling
		{"XREAD without BLOCK is read", "XREAD", [][]byte{[]byte("COUNT"), []byte("10"), []byte("STREAMS"), []byte("s1"), []byte("0")}, CmdRead},
		{"XREAD with BLOCK is blocking", "XREAD", [][]byte{[]byte("BLOCK"), []byte("0"), []byte("STREAMS"), []byte("s1"), []byte("0")}, CmdBlocking},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ClassifyCommand(tt.cmd, tt.args)
			assert.Equal(t, tt.expected, got)
		})
	}
}

// ========== config.go tests ==========

func TestParseProxyMode(t *testing.T) {
	tests := []struct {
		input    string
		expected ProxyMode
		ok       bool
	}{
		{"redis-only", ModeRedisOnly, true},
		{"dual-write", ModeDualWrite, true},
		{"dual-write-shadow", ModeDualWriteShadow, true},
		{"elastickv-primary", ModeElasticKVPrimary, true},
		{"elastickv-only", ModeElasticKVOnly, true},
		{"invalid", 0, false},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got, ok := ParseProxyMode(tt.input)
			assert.Equal(t, tt.ok, ok)
			if ok {
				assert.Equal(t, tt.expected, got)
			}
		})
	}
}

func TestProxyModeString(t *testing.T) {
	assert.Equal(t, "redis-only", ModeRedisOnly.String())
	assert.Equal(t, "dual-write", ModeDualWrite.String())
	assert.Equal(t, "dual-write-shadow", ModeDualWriteShadow.String())
	assert.Equal(t, "elastickv-primary", ModeElasticKVPrimary.String())
	assert.Equal(t, "elastickv-only", ModeElasticKVOnly.String())
	assert.Equal(t, "unknown", ProxyMode(99).String())
}

func TestDefaultConfig(t *testing.T) {
	cfg := DefaultConfig()
	assert.Equal(t, ":6479", cfg.ListenAddr)
	assert.Equal(t, "localhost:6379", cfg.PrimaryAddr)
	assert.Equal(t, "localhost:6380", cfg.SecondaryAddr)
	assert.Equal(t, ModeDualWrite, cfg.Mode)
}

// ========== compare.go tests ==========

func TestDivergenceKindString(t *testing.T) {
	assert.Equal(t, "migration_gap", DivMigrationGap.String())
	assert.Equal(t, "data_mismatch", DivDataMismatch.String())
	assert.Equal(t, "extra_data", DivExtraData.String())
	assert.Equal(t, "unknown", DivergenceKind(99).String())
}

func TestExtractKey(t *testing.T) {
	assert.Equal(t, "mykey", extractKey([][]byte{[]byte("GET"), []byte("mykey")}))
	assert.Equal(t, "", extractKey([][]byte{[]byte("PING")}))
	assert.Equal(t, "", extractKey(nil))
}

func TestBytesArgsToInterfaces(t *testing.T) {
	args := [][]byte{[]byte("SET"), []byte("key"), []byte("val")}
	result := bytesArgsToInterfaces(args)
	assert.Len(t, result, 3)
	assert.Equal(t, []byte("SET"), result[0])
	assert.Equal(t, []byte("key"), result[1])
	assert.Equal(t, []byte("val"), result[2])
}

func TestResponseEqual(t *testing.T) {
	tests := []struct {
		name string
		a, b any
		want bool
	}{
		{"nil nil", nil, nil, true},
		{"nil vs value", nil, "hello", false},
		{"value vs nil", "hello", nil, false},
		{"same string", "hello", "hello", true},
		{"diff string", "hello", "world", false},
		{"empty string equals empty string", "", "", true},
		{"same int64", int64(42), int64(42), true},
		{"diff int64", int64(42), int64(43), false},
		{"same array", []any{"a", "b"}, []any{"a", "b"}, true},
		{"diff array values", []any{"a", "b"}, []any{"a", "c"}, false},
		{"diff array length", []any{"a"}, []any{"a", "b"}, false},
		{"empty arrays", []any{}, []any{}, true},
		{"same bytes", []byte("data"), []byte("data"), true},
		{"diff bytes", []byte("data"), []byte("other"), false},
		{"nested array", []any{[]any{"x"}}, []any{[]any{"x"}}, true},
		{"type mismatch string vs int", "42", int64(42), false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, responseEqual(tt.a, tt.b))
		})
	}
}

func TestClassifyDivergence(t *testing.T) {
	tests := []struct {
		name                       string
		primaryResp, secondaryResp any
		primaryErr, secondaryErr   error
		want                       DivergenceKind
	}{
		{"primary has data, secondary nil", "val", nil, nil, redis.Nil, DivMigrationGap},
		{"primary nil, secondary has data", nil, "val", redis.Nil, nil, DivExtraData},
		{"both have different data", "val1", "val2", nil, nil, DivDataMismatch},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := classifyDivergence(tt.primaryResp, tt.primaryErr, tt.secondaryResp, tt.secondaryErr)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestIsNilResp(t *testing.T) {
	assert.True(t, isNilResp(nil, redis.Nil))
	assert.True(t, isNilResp(nil, nil))
	assert.False(t, isNilResp("", nil), "empty string is NOT nil")
	assert.False(t, isNilResp("hello", nil))
	assert.False(t, isNilResp(int64(0), nil))
}

// ========== proxy.go tests ==========

func TestCloneArgs(t *testing.T) {
	original := [][]byte{[]byte("GET"), []byte("key")}
	cloned := cloneArgs(original)

	assert.Equal(t, original, cloned)

	// Mutating cloned should not affect original.
	cloned[0][0] = 'X'
	assert.Equal(t, byte('G'), original[0][0])
}

func TestIsStatusResponse(t *testing.T) {
	assert.True(t, isStatusResponse("OK"))
	assert.True(t, isStatusResponse("QUEUED"))
	assert.True(t, isStatusResponse("PONG"))
	assert.False(t, isStatusResponse("hello"))
	assert.False(t, isStatusResponse(""))
	assert.False(t, isStatusResponse("ok")) // case-sensitive
}

// ========== sentry.go tests ==========

func TestSentryReporterDisabled(t *testing.T) {
	r := NewSentryReporter("", "", 1.0, nil)
	assert.False(t, r.enabled)
	// Should not panic
	r.CaptureException(nil, "test", nil)
	r.CaptureDivergence(Divergence{})
	r.Flush(0)
	// ShouldReport always returns false when disabled (no side-effects on lastReport).
	assert.False(t, r.ShouldReport("any-fingerprint"))
	assert.Empty(t, r.lastReport, "disabled reporter must not modify lastReport")
}

func TestShouldReportCooldown(t *testing.T) {
	now := time.Now()
	r := &SentryReporter{
		enabled:    true,
		lastReport: make(map[string]time.Time),
		cooldown:   50 * time.Millisecond,
		nowFunc:    func() time.Time { return now },
	}

	assert.True(t, r.ShouldReport("fp1"))
	assert.False(t, r.ShouldReport("fp1")) // within cooldown

	now = now.Add(60 * time.Millisecond)  // advance past cooldown
	assert.True(t, r.ShouldReport("fp1")) // cooldown elapsed
}

func TestShouldReportEvictsExpired(t *testing.T) {
	now := time.Now()
	r := &SentryReporter{
		enabled:    true,
		lastReport: make(map[string]time.Time),
		cooldown:   1 * time.Millisecond,
		nowFunc:    func() time.Time { return now },
	}
	// Fill to maxReportEntries — all entries already expired
	for i := range maxReportEntries {
		r.lastReport[string(rune(i))] = now.Add(-time.Hour)
	}
	assert.True(t, r.ShouldReport("new-fp"))
	assert.Less(t, len(r.lastReport), maxReportEntries)
}

// ========== dualwrite.go tests ==========

func TestHasSecondaryWrite(t *testing.T) {
	for _, tc := range []struct {
		mode     ProxyMode
		expected bool
	}{
		{ModeRedisOnly, false},
		{ModeDualWrite, true},
		{ModeDualWriteShadow, true},
		{ModeElasticKVPrimary, true},
		{ModeElasticKVOnly, false},
	} {
		d := &DualWriter{cfg: ProxyConfig{Mode: tc.mode}, writeSem: make(chan struct{}, 1), shadowSem: make(chan struct{}, 1)}
		assert.Equal(t, tc.expected, d.hasSecondaryWrite(), "mode=%s", tc.mode)
	}
}

func TestDualWriter_Write_PrimarySuccess(t *testing.T) {
	primary := newMockBackend("primary")
	primary.doFunc = makeCmd("OK", nil)
	secondary := newMockBackend("secondary")
	secondary.doFunc = makeCmd("OK", nil)

	metrics := newTestMetrics()
	d := NewDualWriter(primary, secondary, ProxyConfig{Mode: ModeDualWrite, SecondaryTimeout: time.Second}, metrics, newTestSentry(), testLogger)

	resp, err := d.Write(context.Background(), "SET", [][]byte{[]byte("SET"), []byte("k"), []byte("v")})
	assert.NoError(t, err)
	assert.Equal(t, "OK", resp)
	assert.Equal(t, 1, primary.CallCount())

	// Wait for async secondary write deterministically.
	assert.Eventually(t, func() bool { return secondary.CallCount() == 1 },
		time.Second, time.Millisecond, "secondary should be called once")
}

func TestDualWriter_Write_PrimaryFail(t *testing.T) {
	primary := newMockBackend("primary")
	primary.doFunc = makeCmd(nil, errors.New("connection refused"))
	secondary := newMockBackend("secondary")

	metrics := newTestMetrics()
	d := NewDualWriter(primary, secondary, ProxyConfig{Mode: ModeDualWrite, SecondaryTimeout: time.Second}, metrics, newTestSentry(), testLogger)

	_, err := d.Write(context.Background(), "SET", [][]byte{[]byte("SET"), []byte("k"), []byte("v")})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "connection refused")
	// Secondary should NOT be called when primary fails; drain async work.
	d.Close()
	assert.Equal(t, 0, secondary.CallCount())
}

func TestDualWriter_Write_SecondaryFail_ClientSucceeds(t *testing.T) {
	primary := newMockBackend("primary")
	primary.doFunc = makeCmd("OK", nil)
	secondary := newMockBackend("secondary")
	secondary.doFunc = makeCmd(nil, errors.New("secondary down"))

	metrics := newTestMetrics()
	d := NewDualWriter(primary, secondary, ProxyConfig{Mode: ModeDualWrite, SecondaryTimeout: time.Second}, metrics, newTestSentry(), testLogger)

	resp, err := d.Write(context.Background(), "SET", [][]byte{[]byte("SET"), []byte("k"), []byte("v")})
	assert.NoError(t, err)
	assert.Equal(t, "OK", resp)

	d.Close()
	assert.InDelta(t, 1, testutil.ToFloat64(metrics.SecondaryWriteErrors), 0.001)
}

func TestDualWriter_Write_RedisNil(t *testing.T) {
	// SET with NX returns redis.Nil when key exists
	primary := newMockBackend("primary")
	primary.doFunc = makeCmd(nil, redis.Nil)
	secondary := newMockBackend("secondary")
	secondary.doFunc = makeCmd(nil, redis.Nil)

	metrics := newTestMetrics()
	d := NewDualWriter(primary, secondary, ProxyConfig{Mode: ModeDualWrite, SecondaryTimeout: time.Second}, metrics, newTestSentry(), testLogger)

	resp, err := d.Write(context.Background(), "SET", [][]byte{[]byte("SET"), []byte("k"), []byte("v"), []byte("NX")})
	assert.ErrorIs(t, err, redis.Nil)
	assert.Nil(t, resp)
	// Should still send to secondary
	assert.Eventually(t, func() bool { return secondary.CallCount() == 1 },
		time.Second, time.Millisecond, "secondary should be called for redis.Nil")
}

func TestDualWriter_Write_RedisOnlyMode(t *testing.T) {
	primary := newMockBackend("primary")
	primary.doFunc = makeCmd("OK", nil)
	secondary := newMockBackend("secondary")

	metrics := newTestMetrics()
	d := NewDualWriter(primary, secondary, ProxyConfig{Mode: ModeRedisOnly}, metrics, newTestSentry(), testLogger)

	_, err := d.Write(context.Background(), "SET", [][]byte{[]byte("SET"), []byte("k"), []byte("v")})
	assert.NoError(t, err)
	d.Close()
	assert.Equal(t, 0, secondary.CallCount(), "secondary should not be called in redis-only mode")
}

func TestDualWriter_Write_ElasticKVOnlyMode(t *testing.T) {
	primary := newMockBackend("elastickv")
	primary.doFunc = makeCmd("OK", nil)
	secondary := newMockBackend("redis")

	metrics := newTestMetrics()
	d := NewDualWriter(primary, secondary, ProxyConfig{Mode: ModeElasticKVOnly}, metrics, newTestSentry(), testLogger)

	_, err := d.Write(context.Background(), "SET", [][]byte{[]byte("SET"), []byte("k"), []byte("v")})
	assert.NoError(t, err)
	d.Close()
	assert.Equal(t, 0, secondary.CallCount(), "secondary should not be called in elastickv-only mode")
}

func TestDualWriter_Read_WithShadow(t *testing.T) {
	primary := newMockBackend("primary")
	primary.doFunc = makeCmd("hello", nil)
	secondary := newMockBackend("secondary")
	secondary.doFunc = makeCmd("hello", nil)

	metrics := newTestMetrics()
	cfg := ProxyConfig{Mode: ModeDualWriteShadow, ShadowTimeout: time.Second, SecondaryTimeout: time.Second}
	d := NewDualWriter(primary, secondary, cfg, metrics, newTestSentry(), testLogger)

	resp, err := d.Read(context.Background(), "GET", [][]byte{[]byte("GET"), []byte("k")})
	assert.NoError(t, err)
	assert.Equal(t, "hello", resp)

	// Wait for shadow read deterministically.
	assert.Eventually(t, func() bool { return secondary.CallCount() == 1 },
		time.Second, time.Millisecond, "shadow read should be issued")
}

func TestDualWriter_Read_NoShadowInDualWrite(t *testing.T) {
	primary := newMockBackend("primary")
	primary.doFunc = makeCmd("hello", nil)
	secondary := newMockBackend("secondary")

	metrics := newTestMetrics()
	cfg := ProxyConfig{Mode: ModeDualWrite, ShadowTimeout: time.Second}
	d := NewDualWriter(primary, secondary, cfg, metrics, newTestSentry(), testLogger)

	_, err := d.Read(context.Background(), "GET", [][]byte{[]byte("GET"), []byte("k")})
	assert.NoError(t, err)
	d.Close()
	assert.Equal(t, 0, secondary.CallCount(), "no shadow in dual-write mode")
}

type timeoutCapturingBackend struct {
	name        string
	timeout     time.Duration
	args        []any
	doCalls     int
	doWithCalls int
	returnValue any
	returnErr   error
}

func (b *timeoutCapturingBackend) Do(ctx context.Context, args ...any) *redis.Cmd {
	b.doCalls++
	b.args = append([]any(nil), args...)
	cmd := redis.NewCmd(ctx, args...)
	if b.returnErr != nil {
		cmd.SetErr(b.returnErr)
		return cmd
	}
	cmd.SetVal(b.returnValue)
	return cmd
}

func (b *timeoutCapturingBackend) DoWithTimeout(ctx context.Context, timeout time.Duration, args ...any) *redis.Cmd {
	b.doWithCalls++
	b.timeout = timeout
	b.args = append([]any(nil), args...)
	cmd := redis.NewCmd(ctx, args...)
	if b.returnErr != nil {
		cmd.SetErr(b.returnErr)
		return cmd
	}
	cmd.SetVal(b.returnValue)
	return cmd
}

func (b *timeoutCapturingBackend) Pipeline(ctx context.Context, cmds [][]any) ([]*redis.Cmd, error) {
	results := make([]*redis.Cmd, len(cmds))
	for i, args := range cmds {
		results[i] = b.Do(ctx, args...)
	}
	return results, nil
}

func (b *timeoutCapturingBackend) Close() error { return nil }
func (b *timeoutCapturingBackend) Name() string { return b.name }

func TestBlockingCommandTimeout(t *testing.T) {
	tests := []struct {
		name     string
		cmd      string
		args     [][]byte
		expected time.Duration
	}{
		{
			name:     "BZPOPMIN seconds",
			cmd:      "BZPOPMIN",
			args:     [][]byte{[]byte("BZPOPMIN"), []byte("queue"), []byte("5")},
			expected: 5 * time.Second,
		},
		{
			name:     "BLMOVE float seconds",
			cmd:      "BLMOVE",
			args:     [][]byte{[]byte("BLMOVE"), []byte("src"), []byte("dst"), []byte("LEFT"), []byte("RIGHT"), []byte("2.5")},
			expected: 2500 * time.Millisecond,
		},
		{
			name:     "XREAD block milliseconds",
			cmd:      "XREAD",
			args:     [][]byte{[]byte("XREAD"), []byte("BLOCK"), []byte("1500"), []byte("STREAMS"), []byte("jobs"), []byte("0")},
			expected: 1500 * time.Millisecond,
		},
		{
			name:     "XREADGROUP block zero",
			cmd:      "XREADGROUP",
			args:     [][]byte{[]byte("XREADGROUP"), []byte("GROUP"), []byte("g"), []byte("c"), []byte("BLOCK"), []byte("0"), []byte("STREAMS"), []byte("jobs"), []byte(">")},
			expected: 0,
		},
		{
			name:     "missing block falls back to zero",
			cmd:      "XREAD",
			args:     [][]byte{[]byte("XREAD"), []byte("STREAMS"), []byte("jobs"), []byte("0")},
			expected: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, blockingCommandTimeout(tt.cmd, tt.args))
		})
	}
}

func TestDualWriter_Blocking_UsesTimeoutAwareBackend(t *testing.T) {
	primary := &timeoutCapturingBackend{name: "primary", returnValue: "OK"}
	secondary := newMockBackend("secondary")

	metrics := newTestMetrics()
	d := NewDualWriter(primary, secondary, ProxyConfig{Mode: ModeRedisOnly}, metrics, newTestSentry(), testLogger)

	resp, err := d.Blocking(context.Background(), "BZPOPMIN", [][]byte{[]byte("BZPOPMIN"), []byte("queue"), []byte("5")})
	assert.NoError(t, err)
	assert.Equal(t, "OK", resp)
	assert.Equal(t, 0, primary.doCalls)
	assert.Equal(t, 1, primary.doWithCalls)
	assert.Equal(t, 5*time.Second, primary.timeout)
	assert.Equal(t, []any{[]byte("BZPOPMIN"), []byte("queue"), []byte("5")}, primary.args)
}

func TestDualWriter_GoAsync_Bounded(t *testing.T) {
	primary := newMockBackend("primary")
	primary.doFunc = makeCmd("OK", nil)
	secondary := newMockBackend("secondary")

	metrics := newTestMetrics()
	cfg := ProxyConfig{Mode: ModeDualWrite, SecondaryTimeout: 10 * time.Second}
	d := NewDualWriter(primary, secondary, cfg, metrics, newTestSentry(), testLogger)

	// Fill the write semaphore with blocking goroutines
	blocker := make(chan struct{})
	for range maxWriteGoroutines {
		d.goAsync(func() {
			<-blocker
		})
	}

	// Next one should be dropped, not block
	done := make(chan struct{})
	go func() {
		d.goAsync(func() { t.Error("should not run") })
		close(done)
	}()

	select {
	case <-done:
		// good — goAsync returned immediately
	case <-time.After(time.Second):
		t.Fatal("goAsync blocked when semaphore was full")
	}

	// Verify drop metric was incremented
	assert.InDelta(t, 1, testutil.ToFloat64(metrics.AsyncDrops), 0.001)

	close(blocker) // unblock all
	d.Close()      // wait for all goroutines to finish
}

// TestDualWriter_Close_NoWaitGroupRace verifies that concurrent calls to
// goAsync and Close do not trigger a "WaitGroup misuse: Add called
// concurrently with Wait" panic under the race detector.
func TestDualWriter_Close_NoWaitGroupRace(t *testing.T) {
	primary := newMockBackend("primary")
	primary.doFunc = makeCmd("OK", nil)
	secondary := newMockBackend("secondary")
	secondary.doFunc = makeCmd("OK", nil)

	metrics := newTestMetrics()
	cfg := ProxyConfig{Mode: ModeDualWrite, SecondaryTimeout: time.Second}
	d := NewDualWriter(primary, secondary, cfg, metrics, newTestSentry(), testLogger)

	// Hammer goAsync from multiple goroutines while Close races against them.
	var wg sync.WaitGroup
	for range 50 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			d.goAsync(func() {})
		}()
	}
	// Close concurrently — must not panic with "Add called concurrently with Wait".
	d.Close()
	wg.Wait()
}

// ========== ShadowReader tests ==========

func TestShadowReader_Compare_Equal(t *testing.T) {
	secondary := newMockBackend("secondary")
	secondary.doFunc = makeCmd("hello", nil)

	metrics := newTestMetrics()
	sr := NewShadowReader(secondary, metrics, newTestSentry(), testLogger, time.Second)

	sr.Compare(context.Background(), "GET", [][]byte{[]byte("GET"), []byte("k")}, "hello", nil)

	// No divergence should be reported
	assert.InDelta(t, 0, testutil.ToFloat64(metrics.Divergences.WithLabelValues("GET", "data_mismatch")), 0.001)
	assert.InDelta(t, 0, testutil.ToFloat64(metrics.MigrationGaps.WithLabelValues("GET")), 0.001)
}

func TestShadowReader_Compare_BothNil(t *testing.T) {
	secondary := newMockBackend("secondary")
	secondary.doFunc = makeCmd(nil, redis.Nil)

	metrics := newTestMetrics()
	sr := NewShadowReader(secondary, metrics, newTestSentry(), testLogger, time.Second)

	sr.Compare(context.Background(), "GET", [][]byte{[]byte("GET"), []byte("k")}, nil, redis.Nil)

	assert.InDelta(t, 0, testutil.ToFloat64(metrics.Divergences.WithLabelValues("GET", "data_mismatch")), 0.001)
	assert.InDelta(t, 0, testutil.ToFloat64(metrics.MigrationGaps.WithLabelValues("GET")), 0.001)
}

func TestShadowReader_Compare_MigrationGap(t *testing.T) {
	secondary := newMockBackend("secondary")
	secondary.doFunc = makeCmd(nil, redis.Nil) // secondary has no data

	metrics := newTestMetrics()
	sr := NewShadowReader(secondary, metrics, newTestSentry(), testLogger, time.Second)

	sr.Compare(context.Background(), "GET", [][]byte{[]byte("GET"), []byte("k")}, "hello", nil)

	assert.InDelta(t, 1, testutil.ToFloat64(metrics.MigrationGaps.WithLabelValues("GET")), 0.001)
	assert.InDelta(t, 0, testutil.ToFloat64(metrics.Divergences.WithLabelValues("GET", "data_mismatch")), 0.001)
}

func TestShadowReader_Compare_DataMismatch(t *testing.T) {
	secondary := newMockBackend("secondary")
	secondary.doFunc = makeCmd("world", nil)

	metrics := newTestMetrics()
	sr := NewShadowReader(secondary, metrics, newTestSentry(), testLogger, time.Second)

	sr.Compare(context.Background(), "GET", [][]byte{[]byte("GET"), []byte("k")}, "hello", nil)

	assert.InDelta(t, 1, testutil.ToFloat64(metrics.Divergences.WithLabelValues("GET", "data_mismatch")), 0.001)
}

func TestShadowReader_Compare_ExtraData(t *testing.T) {
	secondary := newMockBackend("secondary")
	secondary.doFunc = makeCmd("surprise", nil)

	metrics := newTestMetrics()
	sr := NewShadowReader(secondary, metrics, newTestSentry(), testLogger, time.Second)

	sr.Compare(context.Background(), "GET", [][]byte{[]byte("GET"), []byte("k")}, nil, redis.Nil)

	assert.InDelta(t, 1, testutil.ToFloat64(metrics.Divergences.WithLabelValues("GET", "extra_data")), 0.001)
}

func TestShadowReader_Compare_EmptyStringIsNotNil(t *testing.T) {
	// Primary returns "", secondary returns "" → equal, no divergence
	secondary := newMockBackend("secondary")
	secondary.doFunc = makeCmd("", nil)

	metrics := newTestMetrics()
	sr := NewShadowReader(secondary, metrics, newTestSentry(), testLogger, time.Second)

	sr.Compare(context.Background(), "GET", [][]byte{[]byte("GET"), []byte("k")}, "", nil)

	assert.InDelta(t, 0, testutil.ToFloat64(metrics.Divergences.WithLabelValues("GET", "data_mismatch")), 0.001)
	assert.InDelta(t, 0, testutil.ToFloat64(metrics.MigrationGaps.WithLabelValues("GET")), 0.001)
}

func TestShadowReader_Compare_EmptyStringVsNil(t *testing.T) {
	// Primary returns "", secondary returns nil → MigrationGap
	secondary := newMockBackend("secondary")
	secondary.doFunc = makeCmd(nil, redis.Nil)

	metrics := newTestMetrics()
	sr := NewShadowReader(secondary, metrics, newTestSentry(), testLogger, time.Second)

	sr.Compare(context.Background(), "GET", [][]byte{[]byte("GET"), []byte("k")}, "", nil)

	assert.InDelta(t, 1, testutil.ToFloat64(metrics.MigrationGaps.WithLabelValues("GET")), 0.001)
}

func TestShadowReader_Compare_MigrationGapSampling(t *testing.T) {
	secondary := newMockBackend("secondary")
	secondary.doFunc = makeCmd(nil, redis.Nil)

	metrics := newTestMetrics()
	sr := NewShadowReader(secondary, metrics, newTestSentry(), testLogger, time.Second)
	sr.gapLogSampleRate = 10

	for range 25 {
		sr.Compare(context.Background(), "GET", [][]byte{[]byte("GET"), []byte("k")}, "val", nil)
	}

	assert.InDelta(t, 25, testutil.ToFloat64(metrics.MigrationGaps.WithLabelValues("GET")), 0.001)
	assert.Equal(t, int64(25), sr.gapCount.Load())
}

// ========== Backend tests ==========

func TestDefaultBackendOptions(t *testing.T) {
	opts := DefaultBackendOptions()
	assert.Equal(t, 128, opts.PoolSize)
	assert.Equal(t, 5*time.Second, opts.DialTimeout)
}

func TestNewRedisBackend_UsesRESP2(t *testing.T) {
	backend := NewRedisBackend("127.0.0.1:6379", "test")
	t.Cleanup(func() {
		assert.NoError(t, backend.Close())
	})

	assert.Equal(t, respProtocolV2, backend.client.Options().Protocol)
}

func TestEffectiveBlockingReadTimeout(t *testing.T) {
	assert.Equal(t, time.Duration(0), effectiveBlockingReadTimeout(0))
	assert.Equal(t, 20*time.Second, effectiveBlockingReadTimeout(10*time.Second))
	assert.Equal(t, 11*time.Second, effectiveBlockingReadTimeout(time.Second))
}

// ========== Pipeline error handling tests ==========

func TestPipeline_TransportError(t *testing.T) {
	b := newMockBackend("test")
	b.doFunc = makeCmd(nil, errors.New("connection refused"))

	// mockBackend.Pipeline doesn't simulate pipe.Exec; test RedisBackend via unit behaviour.
	// Here we verify the mock-based pipeline returns results.
	results, err := b.Pipeline(context.Background(), [][]any{{"MULTI"}, {"SET", "k", "v"}, {"EXEC"}})
	assert.NoError(t, err) // mock always returns nil error
	assert.Len(t, results, 3)
}

func TestDualWriter_Script_CachesEvalForEvalSHAFallback(t *testing.T) {
	primary := newMockBackend("primary")
	primary.doFunc = makeCmd("OK", nil)

	secondary := newMockBackend("secondary")
	script := "return ARGV[1]"
	sha := scriptSHA(script)
	var calls int
	secondary.doFunc = func(ctx context.Context, args ...any) *redis.Cmd {
		calls++
		cmd := redis.NewCmd(ctx, args...)
		switch calls {
		case 1:
			assert.Equal(t, []byte("EVALSHA"), args[0])
			assert.Equal(t, []byte(sha), args[1])
			cmd.SetErr(testRedisErr("NOSCRIPT No matching script. Please use EVAL."))
		case 2:
			assert.Equal(t, []byte("EVAL"), args[0])
			assert.Equal(t, []byte(script), args[1])
			cmd.SetVal("OK")
		default:
			t.Fatalf("unexpected secondary call %d", calls)
		}
		return cmd
	}

	metrics := newTestMetrics()
	d := NewDualWriter(primary, secondary, ProxyConfig{Mode: ModeRedisOnly, SecondaryTimeout: time.Second}, metrics, newTestSentry(), testLogger)

	_, err := d.Script(context.Background(), "EVAL", [][]byte{[]byte("EVAL"), []byte(script), []byte("0"), []byte("value")})
	assert.NoError(t, err)

	d.cfg.Mode = ModeDualWrite
	d.writeSecondary("EVALSHA", []any{[]byte("EVALSHA"), []byte(sha), []byte("0"), []byte("value")})

	assert.Equal(t, 2, calls)
	assert.InDelta(t, 0, testutil.ToFloat64(metrics.SecondaryWriteErrors), 0.001)
}

func TestDualWriter_Script_EvalSHARO_FallsBackToEvalRO(t *testing.T) {
	primary := newMockBackend("primary")
	primary.doFunc = makeCmd("OK", nil)

	secondary := newMockBackend("secondary")
	script := "return KEYS[1]"
	sha := scriptSHA(script)
	var calls int
	secondary.doFunc = func(ctx context.Context, args ...any) *redis.Cmd {
		calls++
		cmd := redis.NewCmd(ctx, args...)
		switch calls {
		case 1:
			assert.Equal(t, []byte("EVALSHA_RO"), args[0])
			cmd.SetErr(testRedisErr("NOSCRIPT No matching script. Please use EVAL."))
		case 2:
			// Must fall back to EVAL_RO, not EVAL, to preserve read-only semantics.
			assert.Equal(t, []byte("EVAL_RO"), args[0])
			assert.Equal(t, []byte(script), args[1])
			cmd.SetVal("mykey")
		default:
			t.Fatalf("unexpected secondary call %d", calls)
		}
		return cmd
	}

	metrics := newTestMetrics()
	d := NewDualWriter(primary, secondary, ProxyConfig{Mode: ModeRedisOnly, SecondaryTimeout: time.Second}, metrics, newTestSentry(), testLogger)

	// Register the script body via EVAL_RO so the proxy can fall back.
	_, err := d.Script(context.Background(), "EVAL_RO", [][]byte{[]byte("EVAL_RO"), []byte(script), []byte("1"), []byte("mykey")})
	assert.NoError(t, err)

	d.cfg.Mode = ModeDualWrite
	d.writeSecondary("EVALSHA_RO", []any{[]byte("EVALSHA_RO"), []byte(sha), []byte("1"), []byte("mykey")})

	assert.Equal(t, 2, calls)
	assert.InDelta(t, 0, testutil.ToFloat64(metrics.SecondaryWriteErrors), 0.001)
}

func TestDualWriter_Script_NoRememberOnPrimaryError(t *testing.T) {
	// Verify that a failed SCRIPT FLUSH on the primary does NOT clear the proxy
	// script cache, so that subsequent EVALSHA → EVAL fallbacks still work.
	primary := newMockBackend("primary")
	primary.doFunc = makeCmd(nil, testRedisErr("ERR flush failed"))

	secondary := newMockBackend("secondary")
	secondary.doFunc = makeCmd("OK", nil)

	metrics := newTestMetrics()
	d := NewDualWriter(primary, secondary, ProxyConfig{Mode: ModeRedisOnly, SecondaryTimeout: time.Second}, metrics, newTestSentry(), testLogger)

	// Seed the script cache directly.
	script := "return 1"
	d.storeScript(script)
	sha := scriptSHA(script)
	_, cached := d.lookupScript(sha)
	assert.True(t, cached, "script should be cached before flush attempt")

	// Attempt SCRIPT FLUSH — primary returns an error so the cache must be untouched.
	_, err := d.Script(context.Background(), "SCRIPT", [][]byte{[]byte("SCRIPT"), []byte("FLUSH")})
	assert.Error(t, err)

	_, stillCached := d.lookupScript(sha)
	assert.True(t, stillCached, "cache must not be cleared when primary SCRIPT FLUSH fails")
}

func TestDualWriter_ScriptCache_BoundedEviction(t *testing.T) {
	// Fill the cache beyond maxScriptCacheSize and verify it stays bounded.
	primary := newMockBackend("primary")
	primary.doFunc = makeCmd("OK", nil)

	metrics := newTestMetrics()
	d := NewDualWriter(primary, nil, ProxyConfig{Mode: ModeRedisOnly, SecondaryTimeout: time.Second}, metrics, newTestSentry(), testLogger)

	// Insert maxScriptCacheSize+10 unique scripts.
	total := maxScriptCacheSize + 10
	for i := range total {
		d.storeScript(fmt.Sprintf("return %d", i))
	}

	d.scriptMu.RLock()
	size := len(d.scripts)
	d.scriptMu.RUnlock()

	assert.Equal(t, maxScriptCacheSize, size, "cache must not exceed maxScriptCacheSize")

	// The first 10 scripts (insertion order) must have been evicted.
	for i := range 10 {
		sha := scriptSHA(fmt.Sprintf("return %d", i))
		_, ok := d.lookupScript(sha)
		assert.False(t, ok, "script %d should have been evicted", i)
	}
	// The last maxScriptCacheSize scripts must still be present.
	for i := 10; i < total; i++ {
		sha := scriptSHA(fmt.Sprintf("return %d", i))
		_, ok := d.lookupScript(sha)
		assert.True(t, ok, "script %d should still be cached", i)
	}
}

// ========== writeRedisValue tests ==========

// testRedisErr satisfies the redis.Error interface for testing.
type testRedisErr string

func (e testRedisErr) Error() string { return string(e) }
func (e testRedisErr) RedisError()   {}

type mockRespWriter struct {
	writes []any
}

func (m *mockRespWriter) WriteError(msg string)      { m.writes = append(m.writes, "ERR:"+msg) }
func (m *mockRespWriter) WriteString(msg string)     { m.writes = append(m.writes, "STR:"+msg) }
func (m *mockRespWriter) WriteBulk(b []byte)         { m.writes = append(m.writes, "BULK:"+string(b)) }
func (m *mockRespWriter) WriteBulkString(msg string) { m.writes = append(m.writes, "BULKSTR:"+msg) }
func (m *mockRespWriter) WriteInt64(num int64)       { m.writes = append(m.writes, num) }
func (m *mockRespWriter) WriteArray(count int)       { m.writes = append(m.writes, count) }
func (m *mockRespWriter) WriteNull()                 { m.writes = append(m.writes, nil) }

func TestWriteRedisValue(t *testing.T) {
	tests := []struct {
		name   string
		val    any
		expect []any
	}{
		{"nil", nil, []any{nil}},
		{"status OK", "OK", []any{"STR:OK"}},
		{"status QUEUED", "QUEUED", []any{"STR:QUEUED"}},
		{"status PONG", "PONG", []any{"STR:PONG"}},
		{"bulk string", "hello", []any{"BULKSTR:hello"}},
		{"int64", int64(42), []any{int64(42)}},
		{"bytes", []byte("data"), []any{"BULK:data"}},
		{"array", []any{"a", int64(1)}, []any{2, "BULKSTR:a", int64(1)}},
		{"nested array", []any{[]any{"x"}}, []any{1, 1, "BULKSTR:x"}},
		{"redis error", testRedisErr("WRONGTYPE bad"), []any{"ERR:WRONGTYPE bad"}},
		{"default type", float64(3.14), []any{"BULKSTR:3.14"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := &mockRespWriter{}
			writeRedisValue(w, tt.val)
			assert.Equal(t, tt.expect, w.writes)
		})
	}
}

func TestWriteRedisError(t *testing.T) {
	t.Run("redis.Error passthrough", func(t *testing.T) {
		w := &mockRespWriter{}
		writeRedisError(w, testRedisErr("WRONGTYPE Operation against a key"))
		assert.Equal(t, []any{"ERR:WRONGTYPE Operation against a key"}, w.writes)
	})

	t.Run("generic error gets ERR prefix", func(t *testing.T) {
		w := &mockRespWriter{}
		writeRedisError(w, errors.New("connection refused"))
		assert.Equal(t, []any{"ERR:ERR connection refused"}, w.writes)
	})
}

func TestWriteResponse(t *testing.T) {
	t.Run("nil error with value", func(t *testing.T) {
		w := &mockRespWriter{}
		writeResponse(w, "hello", nil)
		assert.Equal(t, []any{"BULKSTR:hello"}, w.writes)
	})

	t.Run("redis.Nil", func(t *testing.T) {
		w := &mockRespWriter{}
		writeResponse(w, nil, redis.Nil)
		assert.Equal(t, []any{nil}, w.writes)
	})

	t.Run("real error", func(t *testing.T) {
		w := &mockRespWriter{}
		writeResponse(w, nil, errors.New("timeout"))
		assert.Equal(t, []any{"ERR:ERR timeout"}, w.writes)
	})
}

// ========== truncateValue tests ==========

type testStringer struct{ s string }

func (ts testStringer) String() string { return ts.s }

func TestTruncateValue(t *testing.T) {
	tests := []struct {
		name   string
		input  any
		expect string
	}{
		{"nil", nil, "<nil>"},
		{"short string", "hello", "hello"},
		{"short bytes", []byte("abc"), "abc"},
		{"fmt.Stringer", testStringer{"ok"}, "ok"},
		{"int", 42, "42"},
		{"slice", []int{1, 2, 3}, "[1, 2, 3]"},
		{"map", map[string]int{"a": 1}, "{a: 1}"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := truncateValue(tt.input)
			assert.Equal(t, tt.expect, result)
		})
	}

	// Long string truncation
	long := make([]byte, 300)
	for i := range long {
		long[i] = 'a'
	}
	result := truncateValue(string(long))
	assert.Contains(t, result, "...(truncated)")
	assert.LessOrEqual(t, len(result), 300)

	// Long []byte truncation
	result = truncateValue(long)
	assert.Contains(t, result, "...(truncated)")
}

// ========== Additional command table tests ==========

func TestClassifyCommand_NewCommands(t *testing.T) {
	reads := []string{"MGET", "GETRANGE", "STRLEN", "SRANDMEMBER", "SDIFF", "SINTER", "SUNION", "XINFO", "GEODIST", "GEOSEARCH", "OBJECT", "RANDOMKEY", "TOUCH"}
	for _, cmd := range reads {
		assert.Equal(t, CmdRead, ClassifyCommand(cmd, nil), "expected %s to be CmdRead", cmd)
	}

	writes := []string{"MSET", "MSETNX", "APPEND", "INCRBY", "DECR", "SETRANGE", "SPOP", "SMOVE", "PERSIST", "EXPIREAT", "GEOADD", "UNLINK", "COPY", "ZPOPMAX", "RESTORE"}
	for _, cmd := range writes {
		assert.Equal(t, CmdWrite, ClassifyCommand(cmd, nil), "expected %s to be CmdWrite", cmd)
	}

	blocking := []string{"BLPOP", "BRPOP", "BZPOPMAX", "BLMOVE"}
	for _, cmd := range blocking {
		assert.Equal(t, CmdBlocking, ClassifyCommand(cmd, nil), "expected %s to be CmdBlocking", cmd)
	}

	// XREADGROUP with BLOCK
	assert.Equal(t, CmdBlocking, ClassifyCommand("XREADGROUP", [][]byte{[]byte("GROUP"), []byte("g"), []byte("c"), []byte("BLOCK"), []byte("0"), []byte("STREAMS"), []byte("s1"), []byte(">")}))
	// XREADGROUP without BLOCK
	assert.Equal(t, CmdRead, ClassifyCommand("XREADGROUP", [][]byte{[]byte("GROUP"), []byte("g"), []byte("c"), []byte("STREAMS"), []byte("s1"), []byte(">")}))

	admins := []string{"WATCH", "UNWATCH", "HELLO", "TIME", "SLOWLOG", "ACL"}
	for _, cmd := range admins {
		assert.Equal(t, CmdAdmin, ClassifyCommand(cmd, nil), "expected %s to be CmdAdmin", cmd)
	}

	pubsubs := []string{"SSUBSCRIBE", "SUNSUBSCRIBE"}
	for _, cmd := range pubsubs {
		assert.Equal(t, CmdPubSub, ClassifyCommand(cmd, nil), "expected %s to be CmdPubSub", cmd)
	}
}
