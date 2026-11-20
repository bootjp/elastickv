package server

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bootjp/elastickv/distribution"
	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/stretchr/testify/require"
)

// Pin placement before seeding so paired runs include the same forwarding
// paths. This is diagnostic setup, not a production leader-balancing policy.
func pinPerformanceSplitLeaders(t *testing.T, nodes []*bootstrapE2ENode, endpoints []bootstrapE2EMultiGroupEndpoint) {
	t.Helper()
	for i, group := range []uint64{1, 2} {
		current := waitForGroupLeader(t, nodes, group, bootstrapE2EWaitTimeout, bootstrapE2EWaitInterval)
		if current == i {
			continue
		}
		admin, ok := nodes[current].engineForGroup(group).(raftengine.Admin)
		require.True(t, ok)
		ctx, cancel := context.WithTimeout(t.Context(), bootstrapE2ERPCRequestTimeout)
		err := admin.TransferLeadershipToServer(ctx, endpoints[i].id, endpoints[i].raftAddrs[group])
		cancel()
		require.NoError(t, err)
		waitForGroupLeaderOnNode(t, nodes, group, i, bootstrapE2EWaitTimeout, bootstrapE2EWaitInterval)
	}
}

const performancePreflightPhase = uint32(100)

// The waiter owns timing fields; traffic workers read only current. Samples
// are attributed to the phase observed at RPC start, with 100 ms resolution.
type splitPhaseRecorder struct {
	current   atomic.Uint32
	last      time.Time
	durations map[uint32]time.Duration
}

func newSplitPhaseRecorder() *splitPhaseRecorder {
	return &splitPhaseRecorder{last: time.Now(), durations: make(map[uint32]time.Duration)}
}

func (r *splitPhaseRecorder) observe(job distribution.SplitJob) {
	phase := uint32(job.Phase)
	if job.Phase == distribution.SplitJobPhaseFence && !job.TransferPreflightDone {
		phase = performancePreflightPhase
	}
	now := time.Now()
	r.durations[r.current.Load()] += now.Sub(r.last)
	r.last = now
	r.current.Store(phase)
}

func reportSplitPhaseTraffic(t *testing.T, results []splitTrafficResult, recorder *splitPhaseRecorder) {
	t.Helper()
	for _, phase := range []uint32{0, 1, 2, performancePreflightPhase, 3, 4, 5, 6} {
		elapsed := recorder.durations[phase].Seconds()
		if elapsed <= 0 {
			continue
		}
		samples := make([]splitTrafficResult, len(results))
		for i, r := range results {
			if sample := r.phases[phase]; sample != nil {
				samples[i] = *sample
			}
		}
		name := fmt.Sprintf("migration-%d", phase)
		if phase == performancePreflightPhase {
			name = "migration-preflight"
		}
		for _, write := range []bool{false, true} {
			reportSplitTraffic(t, samples, name, write, elapsed, false)
		}
	}
}

func (r *splitTrafficResult) recordOperation(err error, latency time.Duration) {
	if err != nil {
		r.errors++
		if r.firstErr == nil {
			r.firstErr = err
		}
		return
	}
	r.latencies = append(r.latencies, latency)
}

func (r *splitTrafficResult) recordPhaseOperation(phase uint32, err error, latency time.Duration) {
	sample := r.phases[phase]
	if sample == nil {
		sample = &splitTrafficResult{write: r.write}
		r.phases[phase] = sample
	}
	sample.recordOperation(err, latency)
}
