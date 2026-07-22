package proxy

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
)

type poolStatsMockBackend struct {
	*mockBackend
	stats BackendPoolStats
}

func (b *poolStatsMockBackend) PoolStats() BackendPoolStats { return b.stats }

func TestClassifySecondaryWriteError(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want string
	}{
		{"nil error falls back to other", nil, "other"},
		{"plain write conflict", errors.New("write conflict on key foo"), "write_conflict"},
		{
			"retry-limit message wins over embedded write-conflict substring",
			errors.New("redis txn retry limit exceeded: write conflict"),
			"retry_limit",
		},
		{"heavy command pool busy", errors.New(serverOverloadedMarker), "busy"},
		{"not leader", errors.New("not leader"), "not_leader"},
		{"linearizable read not leader", errors.New("linearizable read: not leader"), "not_leader"},
		{"leader not found (ErrLeaderNotFound message)", errors.New("leader not found"), "not_leader"},
		{"context deadline exceeded via errors.Is", context.DeadlineExceeded, "deadline_exceeded"},
		{"wrapped deadline exceeded", fmt.Errorf("dispatch failed: %w", context.DeadlineExceeded), "deadline_exceeded"},
		{"deadline exceeded substring only", errors.New("rpc: deadline exceeded"), "deadline_exceeded"},
		{"txn already committed", errors.New("txn already committed"), "txn_already_finalized"},
		{"txn already aborted", errors.New("txn already aborted"), "txn_already_finalized"},
		{"txn locked", errors.New("key: foo: txn locked"), "txn_locked"},
		{"unknown", errors.New("some random failure"), "other"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := classifySecondaryWriteError(tc.err)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestRecordSecondaryWriteFailureEmitsBothCounters(t *testing.T) {
	metrics := newTestMetrics()
	primary := newMockBackend("primary")
	secondary := newMockBackend("secondary")
	d := NewDualWriter(
		primary, secondary,
		ProxyConfig{Mode: ModeDualWrite, SecondaryTimeout: time.Second},
		metrics, newTestSentry(), testLogger,
	)
	defer d.Close()

	err := errors.New("write conflict on !txn|rb|foo")
	d.recordSecondaryWriteFailure("SET", []any{"SET", "k", "v"}, 5*time.Millisecond, 1, false, err)

	assert.InDelta(t, 1, testutil.ToFloat64(metrics.SecondaryWriteErrors), 0.001,
		"unlabelled counter should still tick for dashboard backwards compatibility")
	assert.InDelta(t, 1, testutil.ToFloat64(
		metrics.SecondaryWriteErrorsByReason.WithLabelValues("SET", "write_conflict"),
	), 0.001, "labelled counter should record the write_conflict reason")

	// Second failure: different reason + command should populate a distinct label pair.
	d.recordSecondaryWriteFailure("EVALSHA", []any{"EVALSHA", "deadbeef"}, time.Millisecond, 3, false,
		errors.New("redis txn retry limit exceeded: write conflict"))

	assert.InDelta(t, 2, testutil.ToFloat64(metrics.SecondaryWriteErrors), 0.001)
	assert.InDelta(t, 1, testutil.ToFloat64(
		metrics.SecondaryWriteErrorsByReason.WithLabelValues("EVALSHA", "retry_limit"),
	), 0.001)
	assert.InDelta(t, 1, testutil.ToFloat64(
		metrics.SecondaryWriteErrorsByReason.WithLabelValues("SET", "write_conflict"),
	), 0.001, "previous label pair should be unchanged")
}

func TestRedisPoolStatsSnapshot(t *testing.T) {
	stats := &redis.PoolStats{
		Hits:            11,
		Misses:          3,
		Timeouts:        2,
		WaitCount:       5,
		Unusable:        1,
		WaitDurationNs:  int64(250 * time.Millisecond),
		TotalConns:      8,
		IdleConns:       3,
		StaleConns:      4,
		PendingRequests: 2,
	}

	snapshot := redisPoolStatsSnapshot(stats, 8)
	assert.Equal(t, 8, snapshot.Limit)
	assert.Equal(t, uint32(11), snapshot.Hits)
	assert.Equal(t, uint32(5), snapshot.WaitCount)
	assert.Equal(t, 250*time.Millisecond, snapshot.WaitDuration)
	assert.Equal(t, uint32(8), snapshot.TotalConns)
	assert.Equal(t, uint32(2), snapshot.PendingRequests)
}

func TestObserveBackendPoolMetrics(t *testing.T) {
	metrics := newTestMetrics()
	backend := &poolStatsMockBackend{
		mockBackend: newMockBackend("elastickv"),
		stats: BackendPoolStats{
			Limit:           8,
			TotalConns:      7,
			IdleConns:       2,
			PendingRequests: 3,
			WaitCount:       9,
			Timeouts:        4,
			WaitDuration:    150 * time.Millisecond,
		},
	}

	metrics.observeBackendPool(backend)

	assert.InDelta(t, 8, testutil.ToFloat64(metrics.BackendPoolLimit.WithLabelValues("elastickv")), 0.001)
	assert.InDelta(t, 7, testutil.ToFloat64(
		metrics.BackendPoolConnections.WithLabelValues("elastickv", "total")), 0.001)
	assert.InDelta(t, 3, testutil.ToFloat64(metrics.BackendPoolPending.WithLabelValues("elastickv")), 0.001)
	assert.InDelta(t, 9, testutil.ToFloat64(
		metrics.BackendPoolEvents.WithLabelValues("elastickv", "waits")), 0.001)
	assert.InDelta(t, 0.15, testutil.ToFloat64(
		metrics.BackendPoolWaitDuration.WithLabelValues("elastickv")), 0.001)
}

func TestDualWriterSamplesBackendPoolsOnStart(t *testing.T) {
	metrics := newTestMetrics()
	primary := &poolStatsMockBackend{
		mockBackend: newMockBackend("redis"),
		stats: BackendPoolStats{
			Limit:           16,
			TotalConns:      12,
			PendingRequests: 5,
		},
	}
	secondary := &poolStatsMockBackend{
		mockBackend: newMockBackend("elastickv"),
		stats: BackendPoolStats{
			Limit:           32,
			TotalConns:      20,
			PendingRequests: 7,
		},
	}

	d := NewDualWriter(primary, secondary, ProxyConfig{Mode: ModeDualWrite, SecondaryTimeout: time.Second}, metrics, newTestSentry(), testLogger)
	defer d.Close()

	assert.InDelta(t, 16, testutil.ToFloat64(metrics.BackendPoolLimit.WithLabelValues("redis")), 0.001)
	assert.InDelta(t, 12, testutil.ToFloat64(
		metrics.BackendPoolConnections.WithLabelValues("redis", "total")), 0.001)
	assert.InDelta(t, 5, testutil.ToFloat64(metrics.BackendPoolPending.WithLabelValues("redis")), 0.001)
	assert.InDelta(t, 32, testutil.ToFloat64(metrics.BackendPoolLimit.WithLabelValues("elastickv")), 0.001)
	assert.InDelta(t, 20, testutil.ToFloat64(
		metrics.BackendPoolConnections.WithLabelValues("elastickv", "total")), 0.001)
	assert.InDelta(t, 7, testutil.ToFloat64(metrics.BackendPoolPending.WithLabelValues("elastickv")), 0.001)
}
