package encryption

import (
	"crypto/aes"
	"crypto/cipher"
	"slices"
	"sync/atomic"

	"github.com/cockroachdb/errors"
)

// keyEntry holds a single DEK plus its pre-initialized AEAD instance. The
// AEAD is built once at Set time so the hot path (Cipher.Encrypt /
// Cipher.Decrypt) never pays for aes.NewCipher / cipher.NewGCM —
// per-call key expansion would be a measurable regression in any
// production-realistic write rate (gemini-code-assist review on PR #719).
//
// dek is stored as a fixed-size array rather than a slice so the type
// system enforces immutability: callers of DEK() get a value copy, not a
// pointer into the live map. This closes the "Get returns mutable slice"
// hazard flagged by claude[bot] review.
type keyEntry struct {
	dek  [KeySize]byte
	aead cipher.AEAD
}

// Keystore is a copy-on-write map from key_id to (DEK, pre-init AEAD).
//
// Reads on the hot path take a single atomic load and observe an
// immutable snapshot of the map. Writes (rotation, bootstrap, retire)
// allocate a new map and CAS it in via atomic.Pointer.Store.
//
// Per §10 self-review lens 2: this avoids contending a mutex on the hot
// path while keeping rotation atomic with respect to readers.
type Keystore struct {
	snap atomic.Pointer[map[uint32]*keyEntry]
}

// NewKeystore returns an empty Keystore.
func NewKeystore() *Keystore {
	ks := &Keystore{}
	empty := map[uint32]*keyEntry{}
	ks.snap.Store(&empty)
	return ks
}

// AEAD returns the pre-initialized cipher.AEAD for keyID, ready for
// Seal/Open. The returned value is safe for concurrent use by multiple
// goroutines (Go stdlib AEAD implementations are stateless after
// initialization).
//
// Used by Cipher.Encrypt / Cipher.Decrypt on the hot path. Returns
// (nil, false) if keyID is not loaded.
func (k *Keystore) AEAD(keyID uint32) (cipher.AEAD, bool) {
	m := *k.snap.Load()
	e, ok := m[keyID]
	if !ok {
		return nil, false
	}
	return e.aead, true
}

// DEK returns the raw 32-byte DEK for keyID. The returned array is a
// value copy — callers are free to mutate it without affecting the
// keystore. The bool reports whether keyID is loaded.
//
// Most call sites should use AEAD instead; DEK is provided for the
// rotation / rewrap path that needs the raw key material to wrap it
// under a new KEK.
func (k *Keystore) DEK(keyID uint32) ([KeySize]byte, bool) {
	m := *k.snap.Load()
	e, ok := m[keyID]
	if !ok {
		return [KeySize]byte{}, false
	}
	return e.dek, true
}

// Has reports whether keyID is loaded.
func (k *Keystore) Has(keyID uint32) bool {
	m := *k.snap.Load()
	_, ok := m[keyID]
	return ok
}

// Set installs a DEK under keyID and pre-initializes the cipher.AEAD.
// dek must be exactly KeySize bytes; the reserved key_id 0 is rejected
// with ErrReservedKeyID. The DEK bytes are copied into the keystore so
// the caller is free to zero or reuse the source slice.
func (k *Keystore) Set(keyID uint32, dek []byte) error {
	if keyID == ReservedKeyID {
		return errors.WithStack(ErrReservedKeyID)
	}
	if len(dek) != KeySize {
		return errors.Wrapf(ErrBadKeySize, "got %d bytes, want %d", len(dek), KeySize)
	}
	entry := &keyEntry{}
	copy(entry.dek[:], dek)
	block, err := aes.NewCipher(entry.dek[:])
	if err != nil {
		return errors.Wrap(err, "encryption: aes.NewCipher")
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return errors.Wrap(err, "encryption: cipher.NewGCM")
	}
	entry.aead = aead

	for {
		cur := k.snap.Load()
		m := make(map[uint32]*keyEntry, len(*cur)+1)
		for id, v := range *cur {
			m[id] = v
		}
		m[keyID] = entry
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
		m := make(map[uint32]*keyEntry, len(*cur))
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
	slices.Sort(ids)
	return ids
}

// Len reports the number of currently-loaded keys.
func (k *Keystore) Len() int {
	return len(*k.snap.Load())
}
