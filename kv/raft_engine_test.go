package kv

import (
	"context"
	"testing"
	"time"

	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/cockroachdb/errors"
)

// blockingLeaderView is a LeaderView whose VerifyLeader blocks until ctx is
// cancelled, modelling the production pathology where ReadIndex stalls
// because heartbeat acks fail to land. LinearizableRead is similarly
// well-behaved on cancel; State / Leader are stamped enough to satisfy the
// callers under test.
type blockingLeaderView struct{}

func (blockingLeaderView) State() raftengine.State       { return raftengine.StateLeader }
func (blockingLeaderView) Leader() raftengine.LeaderInfo { return raftengine.LeaderInfo{ID: "self"} }
func (blockingLeaderView) VerifyLeader(ctx context.Context) error {
	<-ctx.Done()
	return ctx.Err()
}
func (blockingLeaderView) LinearizableRead(ctx context.Context) (uint64, error) {
	<-ctx.Done()
	return 0, ctx.Err()
}

// TestVerifyLeaderEngine_BoundsBlockingReadIndex pins the regression: if a
// stalled ReadIndex used to return only when the underlying ctx fired, but
// callers passed context.Background(), the goroutine pinned forever. After
// the 2026-05-08 incident this must complete within roughly
// verifyLeaderTimeout, surfacing context.DeadlineExceeded.
//
// Skipped under -short because the whole point is to wait for the deadline
// to fire; the no-skip path adds verifyLeaderTimeout (5s) to every default
// `make test` run.
func TestVerifyLeaderEngine_BoundsBlockingReadIndex(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("skipping: blocks for verifyLeaderTimeout (5s)")
	}

	start := time.Now()
	err := verifyLeaderEngine(blockingLeaderView{})
	elapsed := time.Since(start)

	if err == nil {
		t.Fatalf("verifyLeaderEngine(blocking) returned nil; expected DeadlineExceeded")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("verifyLeaderEngine(blocking) err = %v; want DeadlineExceeded", err)
	}
	// Lower bound: confirm the engine actually held the call until the
	// deadline fired, not that some other error path returned
	// immediately. Without this, a future regression that returned
	// DeadlineExceeded before doing any work (e.g. a misplaced ctx
	// check before the engine call) would silently pass.
	//
	// Tolerate a 200ms early-return slack so a slow CI scheduler that
	// trips ctx.Done() a hair before the wall clock catches up does
	// not flake.
	const slack = 200 * time.Millisecond
	if elapsed+slack < verifyLeaderTimeout {
		t.Fatalf("verifyLeaderEngine(blocking) returned too early after %s; want >= %s (-%s slack)", elapsed, verifyLeaderTimeout, slack)
	}
	// Upper bound: prove the call returned at all. Generous so a slow
	// CI host does not flake.
	if elapsed > 2*verifyLeaderTimeout {
		t.Fatalf("verifyLeaderEngine(blocking) returned after %s; want <= 2x verifyLeaderTimeout (%s)", elapsed, verifyLeaderTimeout)
	}
}
