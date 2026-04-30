package encryption_test

import (
	"bytes"
	"crypto/rand"
	"sync"
	"testing"

	"github.com/bootjp/elastickv/internal/encryption"
	"github.com/cockroachdb/errors"
)

func TestKeystore_SetGet(t *testing.T) {
	ks := encryption.NewKeystore()
	dek := make([]byte, encryption.KeySize)
	if _, err := rand.Read(dek); err != nil {
		t.Fatalf("rand.Read: %v", err)
	}
	if err := ks.Set(42, dek); err != nil {
		t.Fatalf("Set: %v", err)
	}
	got, ok := ks.Get(42)
	if !ok {
		t.Fatal("Get(42) reported not found")
	}
	if !bytes.Equal(got, dek) {
		t.Fatal("Get(42) returned different bytes")
	}
}

func TestKeystore_Set_RejectsReservedKeyID(t *testing.T) {
	ks := encryption.NewKeystore()
	dek := make([]byte, encryption.KeySize)
	err := ks.Set(encryption.ReservedKeyID, dek)
	if !errors.Is(err, encryption.ErrReservedKeyID) {
		t.Fatalf("expected ErrReservedKeyID, got %v", err)
	}
	if ks.Has(encryption.ReservedKeyID) {
		t.Fatal("ReservedKeyID was inserted despite the rejection")
	}
}

func TestKeystore_Set_RejectsBadKeySize(t *testing.T) {
	ks := encryption.NewKeystore()
	for _, n := range []int{0, 1, 16, 24, 31, 33, 64} {
		err := ks.Set(7, make([]byte, n))
		if !errors.Is(err, encryption.ErrBadKeySize) {
			t.Fatalf("len=%d: expected ErrBadKeySize, got %v", n, err)
		}
	}
}

func TestKeystore_Set_CopiesInput(t *testing.T) {
	ks := encryption.NewKeystore()
	dek := make([]byte, encryption.KeySize)
	dek[0] = 0xAA
	if err := ks.Set(1, dek); err != nil {
		t.Fatalf("Set: %v", err)
	}
	// Mutate caller's slice; the keystore copy must be unaffected.
	dek[0] = 0xBB
	got, ok := ks.Get(1)
	if !ok {
		t.Fatal("Get(1) reported not found")
	}
	if got[0] != 0xAA {
		t.Fatalf("keystore aliased input: got[0]=0x%02x, want 0xAA", got[0])
	}
}

func TestKeystore_Delete(t *testing.T) {
	ks := encryption.NewKeystore()
	dek := make([]byte, encryption.KeySize)
	_ = ks.Set(1, dek)
	_ = ks.Set(2, dek)
	ks.Delete(1)
	if ks.Has(1) {
		t.Fatal("Delete(1) left it present")
	}
	if !ks.Has(2) {
		t.Fatal("Delete(1) removed unrelated keys")
	}
	// Delete on an absent key must be a no-op (no panic, no error).
	ks.Delete(999)
}

func TestKeystore_IDs_Sorted(t *testing.T) {
	ks := encryption.NewKeystore()
	dek := make([]byte, encryption.KeySize)
	for _, id := range []uint32{5, 1, 9, 3, 7} {
		if err := ks.Set(id, dek); err != nil {
			t.Fatalf("Set(%d): %v", id, err)
		}
	}
	got := ks.IDs()
	want := []uint32{1, 3, 5, 7, 9}
	if len(got) != len(want) {
		t.Fatalf("IDs() length: got %d, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("IDs()[%d] = %d, want %d", i, got[i], want[i])
		}
	}
}

func TestKeystore_Len(t *testing.T) {
	ks := encryption.NewKeystore()
	if ks.Len() != 0 {
		t.Fatalf("empty Len() = %d, want 0", ks.Len())
	}
	dek := make([]byte, encryption.KeySize)
	_ = ks.Set(1, dek)
	_ = ks.Set(2, dek)
	if ks.Len() != 2 {
		t.Fatalf("Len() after 2 Sets = %d, want 2", ks.Len())
	}
	ks.Delete(1)
	if ks.Len() != 1 {
		t.Fatalf("Len() after Delete = %d, want 1", ks.Len())
	}
}

// TestKeystore_Concurrent stresses the copy-on-write semantics: many
// readers should see consistent snapshots even while writers churn the
// map. Run under -race.
func TestKeystore_Concurrent(t *testing.T) {
	ks := encryption.NewKeystore()
	dek := make([]byte, encryption.KeySize)
	dek[0] = 0xAA

	const (
		writers    uint32 = 4
		readers    uint32 = 16
		iterations uint32 = 5_000
	)

	var wg sync.WaitGroup
	wg.Add(int(writers + readers))
	for w := uint32(0); w < writers; w++ {
		go func() {
			defer wg.Done()
			for i := uint32(0); i < iterations; i++ {
				id := (w << 16) | (i & 0xffff)
				if id == encryption.ReservedKeyID {
					continue
				}
				_ = ks.Set(id, dek)
				if i%4 == 0 {
					ks.Delete(id)
				}
			}
		}()
	}
	for r := uint32(0); r < readers; r++ {
		_ = r
		go func() {
			defer wg.Done()
			for i := uint32(0); i < iterations; i++ {
				_ = ks.IDs()
				_, _ = ks.Get(1)
			}
		}()
	}
	wg.Wait()
}
