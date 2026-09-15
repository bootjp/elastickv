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
	failures := make([][]error, goroutines)
	var wg sync.WaitGroup
	for g := range goroutines {
		wg.Add(1)
		go func() {
			defer wg.Done()
			mine := make([]uint64, 0, perWorker)
			for range perWorker {
				ts, err := alloc.Next(context.Background())
				if err != nil {
					// Collected rather than skipped: uniqueness over whatever
					// happened to succeed is satisfied by a single timestamp,
					// so a fixture that failed almost every call would still
					// look like a passing uniqueness property.
					failures[g] = append(failures[g], err)
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
	for g, mine := range issued {
		require.Empty(t, failures[g], "issuance must not fail under concurrency")
		for _, ts := range mine {
			seen[ts]++
			total++
		}
	}
	require.Equal(t, goroutines*perWorker, total,
		"every request must have produced a timestamp")
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

	const (
		allocators = 8
		perWorker  = 64
	)
	// One BatchAllocator per goroutine, all backed by the same TSO state. A
	// single shared allocator serialises every reservation behind its own
	// refill mutex, which makes the interesting failure -- two node-local
	// allocators handed overlapping windows -- unreachable. Separate
	// allocators are what production has: one per node, one shared service.
	var (
		mu    sync.Mutex
		seen  = make(map[uint64]int, allocators*perWorker)
		total int
		wg    sync.WaitGroup
	)
	failures := make([][]error, allocators)
	for a := range allocators {
		batch, err := NewBatchAllocator(alloc, 16)
		require.NoError(t, err)
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range perWorker {
				ts, err := batch.Next(context.Background())
				if err != nil {
					failures[a] = append(failures[a], err)
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

	for a := range allocators {
		require.Empty(t, failures[a], "batched issuance must not fail under concurrency")
	}
	require.Equal(t, allocators*perWorker, total,
		"every request must have produced a timestamp")
	for ts, count := range seen {
		require.Equal(t, 1, count,
			"batched issuance handed timestamp %d out %d times", ts, count)
	}
}

// Uniqueness alone would accept a window handed out backwards, so the batched
// path needs the ordering half too. Sequential on purpose: interleaving two
// allocators has no total order to assert, while a single allocator handing out
// its own window must never go backwards.
func TestBatchAllocatorIssuanceIsStrictlyMonotonic(t *testing.T) {
	t.Parallel()

	alloc, _ := newSlotClaimTSOFixture(t)
	batch, err := NewBatchAllocator(alloc, 16)
	require.NoError(t, err)

	prev := uint64(0)
	for range 256 {
		ts, err := batch.Next(context.Background())
		require.NoError(t, err)
		require.Greater(t, ts, prev,
			"batched issuance must be strictly increasing, including across refills")
		prev = ts
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

	// Phase D has to be active before the bound is even consulted:
	// ValidateDurableTimestamp refuses an inactive state first, so on an
	// unactivated fixture every timestamp is refused for the wrong reason and
	// the assertion below would hold with the bound deleted.
	require.Nil(t, fsm.Apply(marshalTSOCutover()))
	require.Nil(t, fsm.Apply(marshalTSOPhaseD(1)))
	require.True(t, fsm.PhaseDActive())

	// Issue one so a window is committed.
	issued, err := alloc.Next(ctx)
	require.NoError(t, err)

	end := fsm.AllocationFloor()
	require.GreaterOrEqual(t, end, issued)

	// A timestamp inside the committed window validates, which is what makes
	// the two refusals below about the bound rather than about the state.
	require.NoError(t, alloc.ValidateDurableTimestamp(ctx, issued))

	err = alloc.ValidateDurableTimestamp(ctx, end+1)
	require.ErrorIs(t, err, ErrTSOTimestampInvalid,
		"a timestamp past the highest committed window end was never issued")
	require.NotErrorIs(t, err, ErrTSOPhaseDInactive)

	require.ErrorIs(t, alloc.ValidateDurableTimestamp(ctx, 0), ErrTSOTimestampInvalid,
		"zero is not a timestamp")
}
