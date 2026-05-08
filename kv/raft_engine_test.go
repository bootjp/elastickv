package kv

import (
	"context"
	stderrors "errors"
	"testing"
	"time"

	"github.com/bootjp/elastickv/internal/raftengine"
)

// blockingLeaderView is a LeaderView whose VerifyLeader blocks until ctx is
// cancelled, modelling the production pathology where ReadIndex stalls
// because heartbeat acks fail to land. LinearizableRead is similarly
// well-behaved on cancel; State / Leader are stamped enough to satisfy the
// callers under test.
type blockingLeaderView struct{}

func (blockingLeaderView) State() raftengine.State        { return raftengine.StateLeader }
func (blockingLeaderView) Leader() raftengine.LeaderInfo  { return raftengine.LeaderInfo{ID: "self"} }
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
// 2026-05-08-style stalls in production this must complete within roughly
// verifyLeaderTimeout, surfacing context.DeadlineExceeded.
func TestVerifyLeaderEngine_BoundsBlockingReadIndex(t *testing.T) {
	t.Parallel()

	start := time.Now()
	err := verifyLeaderEngine(blockingLeaderView{})
	elapsed := time.Since(start)

	if err == nil {
		t.Fatalf("verifyLeaderEngine(blocking) returned nil; expected DeadlineExceeded")
	}
	if !stderrors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("verifyLeaderEngine(blocking) err = %v; want DeadlineExceeded", err)
	}
	// Allow generous slack so a slow CI host does not flake; the point is
	// not to assert a tight bound but to prove the call returns at all.
	if elapsed > 2*verifyLeaderTimeout {
		t.Fatalf("verifyLeaderEngine(blocking) returned after %s; want <= 2x verifyLeaderTimeout (%s)", elapsed, verifyLeaderTimeout)
	}
}

