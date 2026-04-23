package proxy

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
)

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
