package encryption

import (
	"sort"
	"sync/atomic"

	"github.com/cockroachdb/errors"
)

// Keystore is a copy-on-write map from key_id to DEK bytes.
//
// Reads on the hot path (Cipher.Encrypt / Cipher.Decrypt) take a single
// atomic load and observe an immutable snapshot of the DEK map. Writes
// (rotation, bootstrap, retire) allocate a new map and CAS it in via
// atomic.Pointer.Store.
//
// Per §10 self-review lens 2: this avoids contending a mutex on the hot
// path while keeping rotation atomic with respect to readers.
type Keystore struct {
	snap atomic.Pointer[map[uint32][]byte]
}

// NewKeystore returns an empty Keystore.
func NewKeystore() *Keystore {
	ks := &Keystore{}
	empty := map[uint32][]byte{}
	ks.snap.Store(&empty)
	return ks
}

// Get returns the DEK bytes for keyID and whether it was found.
//
// The returned slice MUST NOT be modified by the caller; the keystore
// treats DEK bytes as immutable so an atomic.Pointer load is sufficient.
// Callers that need to mutate must Clone first.
func (k *Keystore) Get(keyID uint32) ([]byte, bool) {
	m := *k.snap.Load()
	dek, ok := m[keyID]
	return dek, ok
}

// Has reports whether keyID is loaded. Equivalent to the second return of
// Get without exposing the bytes.
func (k *Keystore) Has(keyID uint32) bool {
	m := *k.snap.Load()
	_, ok := m[keyID]
	return ok
}

// Set installs a DEK under keyID. dek must be exactly KeySize bytes; the
// reserved key_id 0 is rejected with ErrReservedKeyID. The DEK bytes are
// copied so the caller is free to zero or reuse the source slice.
func (k *Keystore) Set(keyID uint32, dek []byte) error {
	if keyID == ReservedKeyID {
		return errors.WithStack(ErrReservedKeyID)
	}
	if len(dek) != KeySize {
		return errors.Wrapf(ErrBadKeySize, "got %d bytes, want %d", len(dek), KeySize)
	}
	cp := make([]byte, KeySize)
	copy(cp, dek)

	for {
		cur := k.snap.Load()
		m := make(map[uint32][]byte, len(*cur)+1)
		for id, v := range *cur {
			m[id] = v
		}
		m[keyID] = cp
		if k.snap.CompareAndSwap(cur, &m) {
			return nil
		}
	}
}

// Delete removes the DEK for keyID. No-op if absent.
func (k *Keystore) Delete(keyID uint32) {
	for {
		cur := k.snap.Load()
		if _, ok := (*cur)[keyID]; !ok {
			return
		}
		m := make(map[uint32][]byte, len(*cur))
		for id, v := range *cur {
			if id != keyID {
				m[id] = v
			}
		}
		if k.snap.CompareAndSwap(cur, &m) {
			return
		}
	}
}

// IDs returns a sorted snapshot of all currently-loaded key_ids.
func (k *Keystore) IDs() []uint32 {
	m := *k.snap.Load()
	ids := make([]uint32, 0, len(m))
	for id := range m {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids
}

// Len reports the number of currently-loaded keys.
func (k *Keystore) Len() int {
	return len(*k.snap.Load())
}
