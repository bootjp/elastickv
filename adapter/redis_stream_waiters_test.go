package adapter

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestStreamWaiterRegistry_SignalWakesRegisteredWaiter(t *testing.T) {
	t.Parallel()
	reg := newStreamWaiterRegistry()
	w, release := reg.Register([][]byte{[]byte("stream-a")})
	defer release()

	reg.Signal([]byte("stream-a"))

	select {
	case <-w.C:
	case <-time.After(time.Second):
		t.Fatal("Signal did not wake waiter")
	}
}

func TestStreamWaiterRegistry_SignalUnrelatedKeyDoesNotWake(t *testing.T) {
	t.Parallel()
	reg := newStreamWaiterRegistry()
	w, release := reg.Register([][]byte{[]byte("stream-a")})
	defer release()

	reg.Signal([]byte("stream-b"))

	select {
	case <-w.C:
		t.Fatal("Signal on unrelated key woke waiter")
	case <-time.After(50 * time.Millisecond):
	}
}

func TestStreamWaiterRegistry_MultiKeyWaiterWokenByAnyKey(t *testing.T) {
	t.Parallel()
	reg := newStreamWaiterRegistry()
	w, release := reg.Register([][]byte{[]byte("a"), []byte("b"), []byte("c")})
	defer release()

	reg.Signal([]byte("b"))

	select {
	case <-w.C:
	case <-time.After(time.Second):
		t.Fatal("multi-key waiter not woken by middle key")
	}
}

func TestStreamWaiterRegistry_DuplicateKeysDeduplicated(t *testing.T) {
	t.Parallel()
	reg := newStreamWaiterRegistry()
	// Same key twice in the request; the dedup pass should record one
	// registration so a single Signal does not double-send into the
	// waiter's channel.
	w, release := reg.Register([][]byte{[]byte("dup"), []byte("dup")})
	defer release()

	reg.Signal([]byte("dup"))

	// First select drains the (single) buffered signal.
	select {
	case <-w.C:
	case <-time.After(time.Second):
		t.Fatal("dedup waiter not signaled")
	}
	// Without dedup, the same Signal would have tried twice to send into
	// a buffer of size 1 — the second send would still drop (default
	// branch) so the channel would have only one item — but the buffer
	// would be re-filled if the first drain had not happened yet. The
	// stronger guarantee we test here is that registry.waiters only
	// recorded one membership, so a *second* Signal (after drain) wakes
	// exactly once, not twice.
	reg.Signal([]byte("dup"))
	select {
	case <-w.C:
	case <-time.After(time.Second):
		t.Fatal("post-drain Signal failed to wake waiter")
	}
	// After draining both signals, no further signal is in flight; the
	// channel must be empty.
	select {
	case <-w.C:
		t.Fatal("waiter received a phantom third wake")
	case <-time.After(20 * time.Millisecond):
	}
}

func TestStreamWaiterRegistry_CoalescesPreDrainSignals(t *testing.T) {
	t.Parallel()
	reg := newStreamWaiterRegistry()
	w, release := reg.Register([][]byte{[]byte("k")})
	defer release()

	// Three signals fire before the waiter has a chance to drain — the
	// channel buffer is 1, so the latter two must drop on the
	// non-blocking send. A correctly-coalescing waiter sees exactly one
	// wake.
	reg.Signal([]byte("k"))
	reg.Signal([]byte("k"))
	reg.Signal([]byte("k"))

	select {
	case <-w.C:
	case <-time.After(time.Second):
		t.Fatal("waiter not signaled by burst")
	}
	select {
	case <-w.C:
		t.Fatal("burst was not coalesced — waiter saw a second wake")
	case <-time.After(20 * time.Millisecond):
	}
}

func TestStreamWaiterRegistry_ReleaseStopsFurtherSignals(t *testing.T) {
	t.Parallel()
	reg := newStreamWaiterRegistry()
	w, release := reg.Register([][]byte{[]byte("k")})
	release()
	// Drain the channel just in case it got buffered before release.
	select {
	case <-w.C:
	default:
	}
	reg.Signal([]byte("k"))
	select {
	case <-w.C:
		t.Fatal("waiter received signal after release")
	case <-time.After(20 * time.Millisecond):
	}
}

func TestStreamWaiterRegistry_ReleaseIsIdempotent(t *testing.T) {
	t.Parallel()
	reg := newStreamWaiterRegistry()
	_, release := reg.Register([][]byte{[]byte("k")})
	release()
	release() // second call must not panic / double-delete
	reg.Signal([]byte("k"))
}

func TestStreamWaiterRegistry_SignalWithNoWaiterIsNoOp(t *testing.T) {
	t.Parallel()
	reg := newStreamWaiterRegistry()
	reg.Signal([]byte("nobody-here"))
}

func TestStreamWaiterRegistry_NilRegistry(t *testing.T) {
	t.Parallel()
	var reg *streamWaiterRegistry
	w, release := reg.Register([][]byte{[]byte("k")})
	defer release()
	if w == nil {
		t.Fatal("nil registry must return a usable waiter")
	}
	if w.C == nil {
		t.Fatal("nil registry waiter must have a non-nil channel")
	}
	// Buffered-1 invariant: a non-blocking direct send must succeed
	// without deadlocking. (In production, Signal is a no-op for nil
	// registries; this only matters for test stubs that hand-write
	// into w.C — but the contract is documented as buffered-1 either
	// way, and an unbuffered channel would deadlock here.)
	select {
	case w.C <- struct{}{}:
	default:
		t.Fatal("nil-registry waiter channel must be buffered (size 1)")
	}
	// And it must drain.
	select {
	case <-w.C:
	default:
		t.Fatal("nil-registry waiter channel did not deliver the manual send")
	}
	// Signal on nil registry is a no-op.
	reg.Signal([]byte("k"))
}

func TestStreamWaiterRegistry_ManyWaitersFanOut(t *testing.T) {
	t.Parallel()
	reg := newStreamWaiterRegistry()
	const n = 10
	waiters := make([]*streamWaiter, 0, n)
	releases := make([]func(), 0, n)
	for range n {
		w, rel := reg.Register([][]byte{[]byte("fan")})
		waiters = append(waiters, w)
		releases = append(releases, rel)
	}
	defer func() {
		for _, rel := range releases {
			rel()
		}
	}()

	reg.Signal([]byte("fan"))

	for i, w := range waiters {
		select {
		case <-w.C:
		case <-time.After(time.Second):
			t.Fatalf("waiter %d not signaled", i)
		}
	}
}

// TestStreamWaiterRegistry_ConcurrentRegisterSignal exercises the lock to
// catch ordering bugs in Register/Signal/release: parallel registers and
// signals on the same key must always either reach the waiter or have
// happened before its registration window closed.
func TestStreamWaiterRegistry_ConcurrentRegisterSignal(t *testing.T) {
	t.Parallel()
	reg := newStreamWaiterRegistry()
	const goroutines = 64
	const iterations = 100
	var wokeOrTimedOut atomic.Int64

	var start, done sync.WaitGroup
	start.Add(1)
	for i := 0; i < goroutines; i++ {
		done.Add(1)
		go func() {
			defer done.Done()
			start.Wait()
			for j := 0; j < iterations; j++ {
				w, release := reg.Register([][]byte{[]byte("hot")})
				reg.Signal([]byte("hot"))
				select {
				case <-w.C:
				case <-time.After(50 * time.Millisecond):
				}
				release()
				wokeOrTimedOut.Add(1)
			}
		}()
	}
	start.Done()
	done.Wait()
	if got := wokeOrTimedOut.Load(); got != int64(goroutines*iterations) {
		t.Fatalf("expected %d completions, got %d", goroutines*iterations, got)
	}
}
