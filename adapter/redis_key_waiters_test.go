package adapter

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestKeyWaiterRegistry_SignalWakesRegisteredWaiter(t *testing.T) {
	t.Parallel()
	reg := newKeyWaiterRegistry()
	w, release := reg.Register([][]byte{[]byte("z-a")})
	defer release()

	reg.Signal([]byte("z-a"))

	select {
	case <-w.C:
	case <-time.After(time.Second):
		t.Fatal("Signal did not wake waiter")
	}
}

func TestKeyWaiterRegistry_SignalUnrelatedKeyDoesNotWake(t *testing.T) {
	t.Parallel()
	reg := newKeyWaiterRegistry()
	w, release := reg.Register([][]byte{[]byte("z-a")})
	defer release()

	reg.Signal([]byte("z-b"))

	select {
	case <-w.C:
		t.Fatal("Signal on unrelated key woke waiter")
	case <-time.After(50 * time.Millisecond):
	}
}

func TestKeyWaiterRegistry_MultiKeyWaiterWokenByAnyKey(t *testing.T) {
	t.Parallel()
	reg := newKeyWaiterRegistry()
	w, release := reg.Register([][]byte{[]byte("a"), []byte("b"), []byte("c")})
	defer release()

	reg.Signal([]byte("b"))

	select {
	case <-w.C:
	case <-time.After(time.Second):
		t.Fatal("multi-key waiter not woken by middle key")
	}
}

func TestKeyWaiterRegistry_DuplicateKeysDeduplicated(t *testing.T) {
	t.Parallel()
	reg := newKeyWaiterRegistry()
	w, release := reg.Register([][]byte{[]byte("dup"), []byte("dup")})
	defer release()

	reg.Signal([]byte("dup"))

	select {
	case <-w.C:
	case <-time.After(time.Second):
		t.Fatal("dedup waiter not signaled")
	}
	reg.Signal([]byte("dup"))
	select {
	case <-w.C:
	case <-time.After(time.Second):
		t.Fatal("post-drain Signal failed to wake waiter")
	}
	select {
	case <-w.C:
		t.Fatal("waiter received a phantom third wake")
	case <-time.After(20 * time.Millisecond):
	}
}

func TestKeyWaiterRegistry_CoalescesPreDrainSignals(t *testing.T) {
	t.Parallel()
	reg := newKeyWaiterRegistry()
	w, release := reg.Register([][]byte{[]byte("k")})
	defer release()

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

func TestKeyWaiterRegistry_ReleaseStopsFurtherSignals(t *testing.T) {
	t.Parallel()
	reg := newKeyWaiterRegistry()
	w, release := reg.Register([][]byte{[]byte("k")})
	release()
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

func TestKeyWaiterRegistry_ReleaseIsIdempotent(t *testing.T) {
	t.Parallel()
	reg := newKeyWaiterRegistry()
	_, release := reg.Register([][]byte{[]byte("k")})
	release()
	release()
	reg.Signal([]byte("k"))
}

func TestKeyWaiterRegistry_SignalWithNoWaiterIsNoOp(t *testing.T) {
	t.Parallel()
	reg := newKeyWaiterRegistry()
	reg.Signal([]byte("nobody-here"))
}

func TestKeyWaiterRegistry_NilRegistryIsNoOp(t *testing.T) {
	t.Parallel()
	var reg *keyWaiterRegistry
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
	// registries; this only matters for test stubs that hand-send
	// into w.C — but the contract is documented as buffered-1 either
	// way, and an unbuffered channel would deadlock here.)
	select {
	case w.C <- struct{}{}:
	default:
		t.Fatal("nil-registry waiter channel must be buffered (size 1)")
	}
	select {
	case <-w.C:
	default:
		t.Fatal("nil-registry waiter channel did not deliver the manual send")
	}
	reg.Signal([]byte("k"))
}

func TestKeyWaiterRegistry_ManyWaitersFanOut(t *testing.T) {
	t.Parallel()
	reg := newKeyWaiterRegistry()
	const n = 10
	waiters := make([]*keyWaiter, 0, n)
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

func TestKeyWaiterRegistry_ConcurrentRegisterSignal(t *testing.T) {
	t.Parallel()
	reg := newKeyWaiterRegistry()
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
