package kv

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/stretchr/testify/require"
)

// The invariants below are the decision-independent half of
// docs/design/2026_08_29_proposed_tso_batch_slot_claims.md §6. They hold under
// every option that document weighs -- forcing batchSize==1, durable per-slot
// claims, or neither -- so they are the fixed point any of those changes has to
// preserve, and they are written before the mechanism is chosen rather than
// after.
//
// What they do NOT assert is the hole itself: a timestamp inside a committed
// window that nobody claimed still validates today. That is the open question
// the design doc exists to settle, and asserting current behaviour there would
// lock in the bug.

func newSlotClaimTSOFixture(t *testing.T) (*RaftTSOAllocator, *TSOStateMachine) {
	t.Helper()

	clock := NewHLC()
	clock.SetPhysicalCeiling(time.Now().Add(testTSOFutureCeiling).UnixMilli())
	fsm := NewTSOStateMachine(clock)
	engine := &recordingTSOEngine{
		state:  raftengine.StateLeader,
		leader: raftengine.LeaderInfo{Address: "self"},
		term:   1,
		apply:  applyTSOTestFSM(fsm),
	}
	alloc, err := newTestRaftTSOAllocator(&ShardGroup{Engine: engine, TSOState: fsm}, clock)
	require.NoError(t, err)
	return alloc, fsm
}

// TestTSOIssuesEachTimestampAtMostOnceUnderConcurrency is §6's uniqueness
// property.
//
// OCC's conflict check is `latestTS(key) > startTS`, so two commits that share a
// timestamp can each read the other as not-newer. Uniqueness of issued
// timestamps is therefore load-bearing for correctness, not merely tidy, and it
// has to survive whatever the slot-claim work changes about validation.
func TestTSOIssuesEachTimestampAtMostOnceUnderConcurrency(t *testing.T) {
	t.Parallel()

	alloc, _ := newSlotClaimTSOFixture(t)

	const (
		goroutines = 8
		perWorker  = 64
	)
	issued := make([][]uint64, goroutines)
	var wg sync.WaitGroup
	for g := range goroutines {
		wg.Add(1)
		go func() {
			defer wg.Done()
			mine := make([]uint64, 0, perWorker)
			for range perWorker {
				ts, err := alloc.Next(context.Background())
				if err != nil {
					continue
				}
				mine = append(mine, ts)
			}
			issued[g] = mine
		}()
	}
	wg.Wait()

	seen := make(map[uint64]int, goroutines*perWorker)
	total := 0
	for _, mine := range issued {
		for _, ts := range mine {
			seen[ts]++
			total++
		}
	}
	require.Positive(t, total, "the fixture must actually issue timestamps")
	for ts, count := range seen {
		require.Equal(t, 1, count,
			"timestamp %d was issued %d times; two writes sharing one commit_ts can "+
				"each read the other as not-newer under OCC", ts, count)
	}
}

// The same property through a BatchAllocator, which is the Phase-D path the
// design doc is about: windows are reserved in bulk and handed out locally, so
// a bug in the local hand-out would duplicate without any Raft round trip to
// catch it.
func TestBatchAllocatorIssuesEachTimestampAtMostOnce(t *testing.T) {
	t.Parallel()

	alloc, _ := newSlotClaimTSOFixture(t)
	batch, err := NewBatchAllocator(alloc, 16)
	require.NoError(t, err)

	const (
		goroutines = 8
		perWorker  = 64
	)
	var (
		mu    sync.Mutex
		seen  = make(map[uint64]int, goroutines*perWorker)
		total int
		wg    sync.WaitGroup
	)
	for range goroutines {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range perWorker {
				ts, err := batch.Next(context.Background())
				if err != nil {
					continue
				}
				mu.Lock()
				seen[ts]++
				total++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()

	require.Positive(t, total)
	for ts, count := range seen {
		require.Equal(t, 1, count,
			"batched issuance handed timestamp %d out %d times", ts, count)
	}
}

// TestTSOIssuanceIsStrictlyMonotonic pins the other half of the ordering
// contract. Duplicate detection alone would pass a sequence that went
// backwards, and a commit_ts below one already read breaks snapshot
// repeatability just as a duplicate breaks OCC.
func TestTSOIssuanceIsStrictlyMonotonic(t *testing.T) {
	t.Parallel()

	alloc, _ := newSlotClaimTSOFixture(t)

	prev := uint64(0)
	for range 256 {
		ts, err := alloc.Next(context.Background())
		require.NoError(t, err)
		require.Greater(t, ts, prev, "issuance must be strictly increasing")
		prev = ts
	}
}

// TestValidateDurableTimestampRefusesBeyondTheAllocationFloor pins the boundary
// the slot-claim work will TIGHTEN but must not loosen: whatever a claim record
// ends up proving, a timestamp past the highest committed window end has
// certainly never been issued.
func TestValidateDurableTimestampRefusesBeyondTheAllocationFloor(t *testing.T) {
	t.Parallel()

	alloc, fsm := newSlotClaimTSOFixture(t)
	ctx := context.Background()

	// Issue one so a window is committed.
	issued, err := alloc.Next(ctx)
	require.NoError(t, err)

	end := fsm.AllocationFloor()
	require.GreaterOrEqual(t, end, issued)

	require.Error(t, alloc.ValidateDurableTimestamp(ctx, end+1),
		"a timestamp past the highest committed window end was never issued")
	require.Error(t, alloc.ValidateDurableTimestamp(ctx, 0),
		"zero is not a timestamp")
}
