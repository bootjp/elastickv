package proxy

import (
	"context"
	"errors"
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
	doFunc func(ctx context.Context, args ...interface{}) *redis.Cmd
	mu     sync.Mutex
	calls  [][]interface{}
}

func newMockBackend(name string) *mockBackend {
	return &mockBackend{name: name}
}

func (b *mockBackend) Do(ctx context.Context, args ...interface{}) *redis.Cmd {
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

func (b *mockBackend) Pipeline(ctx context.Context, cmds [][]interface{}) ([]*redis.Cmd, error) {
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
func makeCmd(val interface{}, err error) func(ctx context.Context, args ...interface{}) *redis.Cmd {
	return func(ctx context.Context, args ...interface{}) *redis.Cmd {
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
		a, b interface{}
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
		{"same array", []interface{}{"a", "b"}, []interface{}{"a", "b"}, true},
		{"diff array values", []interface{}{"a", "b"}, []interface{}{"a", "c"}, false},
		{"diff array length", []interface{}{"a"}, []interface{}{"a", "b"}, false},
		{"empty arrays", []interface{}{}, []interface{}{}, true},
		{"same bytes", []byte("data"), []byte("data"), true},
		{"diff bytes", []byte("data"), []byte("other"), false},
		{"nested array", []interface{}{[]interface{}{"x"}}, []interface{}{[]interface{}{"x"}}, true},
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
		primaryResp, secondaryResp interface{}
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
}

func TestShouldReportCooldown(t *testing.T) {
	r := &SentryReporter{
		lastReport: make(map[string]time.Time),
		cooldown:   50 * time.Millisecond,
	}

	assert.True(t, r.ShouldReport("fp1"))
	assert.False(t, r.ShouldReport("fp1")) // within cooldown

	time.Sleep(60 * time.Millisecond)
	assert.True(t, r.ShouldReport("fp1")) // cooldown elapsed
}

func TestShouldReportEvictsExpired(t *testing.T) {
	r := &SentryReporter{
		lastReport: make(map[string]time.Time),
		cooldown:   1 * time.Millisecond,
	}
	// Fill to maxReportEntries
	for i := range maxReportEntries {
		r.lastReport[string(rune(i))] = time.Now().Add(-time.Hour)
	}
	time.Sleep(2 * time.Millisecond)
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
		d := &DualWriter{cfg: ProxyConfig{Mode: tc.mode}, asyncSem: make(chan struct{}, 1)}
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

	resp, err := d.Write(context.Background(), [][]byte{[]byte("SET"), []byte("k"), []byte("v")})
	assert.NoError(t, err)
	assert.Equal(t, "OK", resp)
	assert.Equal(t, 1, primary.CallCount())

	// Wait for async secondary write
	time.Sleep(50 * time.Millisecond)
	assert.Equal(t, 1, secondary.CallCount())
}

func TestDualWriter_Write_PrimaryFail(t *testing.T) {
	primary := newMockBackend("primary")
	primary.doFunc = makeCmd(nil, errors.New("connection refused"))
	secondary := newMockBackend("secondary")

	metrics := newTestMetrics()
	d := NewDualWriter(primary, secondary, ProxyConfig{Mode: ModeDualWrite, SecondaryTimeout: time.Second}, metrics, newTestSentry(), testLogger)

	_, err := d.Write(context.Background(), [][]byte{[]byte("SET"), []byte("k"), []byte("v")})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "connection refused")
	// Secondary should NOT be called when primary fails
	time.Sleep(50 * time.Millisecond)
	assert.Equal(t, 0, secondary.CallCount())
}

func TestDualWriter_Write_SecondaryFail_ClientSucceeds(t *testing.T) {
	primary := newMockBackend("primary")
	primary.doFunc = makeCmd("OK", nil)
	secondary := newMockBackend("secondary")
	secondary.doFunc = makeCmd(nil, errors.New("secondary down"))

	metrics := newTestMetrics()
	d := NewDualWriter(primary, secondary, ProxyConfig{Mode: ModeDualWrite, SecondaryTimeout: time.Second}, metrics, newTestSentry(), testLogger)

	resp, err := d.Write(context.Background(), [][]byte{[]byte("SET"), []byte("k"), []byte("v")})
	assert.NoError(t, err)
	assert.Equal(t, "OK", resp)

	time.Sleep(50 * time.Millisecond)
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

	resp, err := d.Write(context.Background(), [][]byte{[]byte("SET"), []byte("k"), []byte("v"), []byte("NX")})
	assert.ErrorIs(t, err, redis.Nil)
	assert.Nil(t, resp)
	// Should still send to secondary
	time.Sleep(50 * time.Millisecond)
	assert.Equal(t, 1, secondary.CallCount())
}

func TestDualWriter_Write_RedisOnlyMode(t *testing.T) {
	primary := newMockBackend("primary")
	primary.doFunc = makeCmd("OK", nil)
	secondary := newMockBackend("secondary")

	metrics := newTestMetrics()
	d := NewDualWriter(primary, secondary, ProxyConfig{Mode: ModeRedisOnly}, metrics, newTestSentry(), testLogger)

	_, err := d.Write(context.Background(), [][]byte{[]byte("SET"), []byte("k"), []byte("v")})
	assert.NoError(t, err)
	time.Sleep(50 * time.Millisecond)
	assert.Equal(t, 0, secondary.CallCount(), "secondary should not be called in redis-only mode")
}

func TestDualWriter_Write_ElasticKVOnlyMode(t *testing.T) {
	primary := newMockBackend("elastickv")
	primary.doFunc = makeCmd("OK", nil)
	secondary := newMockBackend("redis")

	metrics := newTestMetrics()
	d := NewDualWriter(primary, secondary, ProxyConfig{Mode: ModeElasticKVOnly}, metrics, newTestSentry(), testLogger)

	_, err := d.Write(context.Background(), [][]byte{[]byte("SET"), []byte("k"), []byte("v")})
	assert.NoError(t, err)
	time.Sleep(50 * time.Millisecond)
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

	resp, err := d.Read(context.Background(), [][]byte{[]byte("GET"), []byte("k")})
	assert.NoError(t, err)
	assert.Equal(t, "hello", resp)

	// Wait for shadow read
	time.Sleep(50 * time.Millisecond)
	assert.Equal(t, 1, secondary.CallCount(), "shadow read should be issued")
}

func TestDualWriter_Read_NoShadowInDualWrite(t *testing.T) {
	primary := newMockBackend("primary")
	primary.doFunc = makeCmd("hello", nil)
	secondary := newMockBackend("secondary")

	metrics := newTestMetrics()
	cfg := ProxyConfig{Mode: ModeDualWrite, ShadowTimeout: time.Second}
	d := NewDualWriter(primary, secondary, cfg, metrics, newTestSentry(), testLogger)

	_, err := d.Read(context.Background(), [][]byte{[]byte("GET"), []byte("k")})
	assert.NoError(t, err)
	time.Sleep(50 * time.Millisecond)
	assert.Equal(t, 0, secondary.CallCount(), "no shadow in dual-write mode")
}

func TestDualWriter_GoAsync_Bounded(t *testing.T) {
	primary := newMockBackend("primary")
	primary.doFunc = makeCmd("OK", nil)
	secondary := newMockBackend("secondary")

	metrics := newTestMetrics()
	cfg := ProxyConfig{Mode: ModeDualWrite, SecondaryTimeout: 10 * time.Second}
	d := NewDualWriter(primary, secondary, cfg, metrics, newTestSentry(), testLogger)

	// Fill the semaphore with blocking goroutines
	blocker := make(chan struct{})
	for range maxAsyncGoroutines {
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

	close(blocker) // unblock all
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
