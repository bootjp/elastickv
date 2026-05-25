package encryption_test

import (
	"encoding/binary"
	"sync"
	"testing"

	"github.com/bootjp/elastickv/internal/encryption"
)

func TestDeterministicNonceFactory_Layout(t *testing.T) {
	t.Parallel()
	const nodeID uint16 = 0xABCD
	const epoch uint16 = 0x0007
	f := encryption.NewDeterministicNonceFactory(nodeID, epoch)
	n, err := f.Next()
	if err != nil {
		t.Fatalf("Next: %v", err)
	}
	if got := binary.BigEndian.Uint16(n[0:2]); got != nodeID {
		t.Errorf("node_id field = 0x%04X, want 0x%04X", got, nodeID)
	}
	if got := binary.BigEndian.Uint16(n[2:4]); got != epoch {
		t.Errorf("local_epoch field = 0x%04X, want 0x%04X", got, epoch)
	}
	// First nonce carries write_count=1 (pre-increment), never 0.
	if got := binary.BigEndian.Uint64(n[4:12]); got != 1 {
		t.Errorf("write_count field = %d, want 1", got)
	}
}

func TestDeterministicNonceFactory_MonotonicAndUnique(t *testing.T) {
	t.Parallel()
	f := encryption.NewDeterministicNonceFactory(0x0001, 0x0002)
	const n = 1000
	seen := make(map[[encryption.NonceSize]byte]struct{}, n)
	var prev uint64
	for i := 0; i < n; i++ {
		nonce, err := f.Next()
		if err != nil {
			t.Fatalf("Next[%d]: %v", i, err)
		}
		if _, dup := seen[nonce]; dup {
			t.Fatalf("duplicate nonce at i=%d: %x", i, nonce)
		}
		seen[nonce] = struct{}{}
		wc := binary.BigEndian.Uint64(nonce[4:12])
		if wc <= prev {
			t.Fatalf("write_count not strictly increasing: %d after %d", wc, prev)
		}
		prev = wc
	}
}

// TestDeterministicNonceFactory_DistinctEpochsDisjoint pins the
// restart-safety property: two factories with the same node_id but
// different local_epoch (as a bump produces across restarts) never
// collide even though both reset write_count to 0.
func TestDeterministicNonceFactory_DistinctEpochsDisjoint(t *testing.T) {
	t.Parallel()
	f1 := encryption.NewDeterministicNonceFactory(0x00AA, 0x0001)
	f2 := encryption.NewDeterministicNonceFactory(0x00AA, 0x0002)
	seen := make(map[[encryption.NonceSize]byte]struct{})
	for i := 0; i < 100; i++ {
		a, err := f1.Next()
		if err != nil {
			t.Fatalf("f1.Next[%d]: %v", i, err)
		}
		b, err := f2.Next()
		if err != nil {
			t.Fatalf("f2.Next[%d]: %v", i, err)
		}
		if a == b {
			t.Fatalf("epoch-disjoint factories collided at i=%d", i)
		}
		seen[a] = struct{}{}
		if _, dup := seen[b]; dup {
			t.Fatalf("f2 nonce collided with an f1 nonce at i=%d", i)
		}
		seen[b] = struct{}{}
	}
}

func TestDeterministicNonceFactory_ConcurrentNextUnique(t *testing.T) {
	t.Parallel()
	f := encryption.NewDeterministicNonceFactory(0xCAFE, 0x0003)
	const goroutines = 8
	const perG = 500
	var wg sync.WaitGroup
	results := make([][][encryption.NonceSize]byte, goroutines)
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			local := make([][encryption.NonceSize]byte, perG)
			for i := 0; i < perG; i++ {
				n, err := f.Next()
				if err != nil {
					t.Errorf("Next: %v", err)
					return
				}
				local[i] = n
			}
			results[g] = local
		}(g)
	}
	wg.Wait()
	seen := make(map[[encryption.NonceSize]byte]struct{}, goroutines*perG)
	for _, batch := range results {
		for _, n := range batch {
			if _, dup := seen[n]; dup {
				t.Fatalf("concurrent duplicate nonce: %x", n)
			}
			seen[n] = struct{}{}
		}
	}
	if len(seen) != goroutines*perG {
		t.Errorf("unique nonces = %d, want %d", len(seen), goroutines*perG)
	}
}
