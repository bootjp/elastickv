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
// Cipher.Decrypt) never pays for aes.NewCipher / cipher.NewGCM on every
// call.
//
// dek is stored as a fixed-size array rather than a slice so the type
// system enforces immutability: callers of DEK() receive a value copy,
// not a pointer into the live map.
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
//
// Zero-value safety: a `var ks Keystore` (or a nil *Keystore) is
// degraded but does not panic — read methods (AEAD, DEK, Has, IDs,
// Len) treat it as the empty keystore, Delete is a no-op, and Set
// returns ErrNilKeystore for a nil receiver. Always prefer NewKeystore
// so an unwrap path that needs to install keys reports the wiring
// mistake immediately.
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

// loadMap returns the current snapshot map, or nil for a nil receiver
// or zero-value Keystore (where snap.Load() returns nil because nothing
// was Stored). Read methods treat a nil map as the empty keystore so a
// caller that bypasses NewKeystore observes consistent (zero/false)
// results instead of nil-deref panicking. Mirrors the defensive pattern
// Cipher uses for ErrNilKeystore.
func (k *Keystore) loadMap() map[uint32]*keyEntry {
	if k == nil {
		return nil
	}
	p := k.snap.Load()
	if p == nil {
		return nil
	}
	return *p
}

// AEAD returns the pre-initialized cipher.AEAD for keyID, ready for
// Seal/Open. The returned value is safe for concurrent use by multiple
// goroutines (Go stdlib AEAD implementations are stateless after
// initialization).
//
// Used by Cipher.Encrypt / Cipher.Decrypt on the hot path. Returns
// (nil, false) if keyID is not loaded, the receiver is nil, or the
// Keystore is zero-valued.
func (k *Keystore) AEAD(keyID uint32) (cipher.AEAD, bool) {
	e, ok := k.loadMap()[keyID]
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
	e, ok := k.loadMap()[keyID]
	if !ok {
		return [KeySize]byte{}, false
	}
	return e.dek, true
}

// Has reports whether keyID is loaded.
func (k *Keystore) Has(keyID uint32) bool {
	_, ok := k.loadMap()[keyID]
	return ok
}

// Set installs a DEK under keyID and pre-initializes the cipher.AEAD.
// dek must be exactly KeySize bytes; the reserved key_id 0 is rejected
// with ErrReservedKeyID. The DEK bytes are copied into the keystore so
// the caller is free to zero or reuse the source slice.
//
// Set is set-once with idempotent-same semantics: re-Set under an
// existing keyID with byte-identical DEK is a no-op (returns nil), but
// Set with DIFFERENT bytes for an already-loaded keyID returns
// ErrKeyConflict. Replacing live key bytes for a keyID would render
// every envelope already persisted under that id undecryptable.
//
// A nil receiver returns ErrNilKeystore; zero-value Keystores are
// rejected at the same boundary as Cipher.
func (k *Keystore) Set(keyID uint32, dek []byte) error {
	if k == nil {
		return errors.WithStack(ErrNilKeystore)
	}
	entry, err := buildKeyEntry(keyID, dek)
	if err != nil {
		return err
	}
	for {
		cur := k.snap.Load()
		var src map[uint32]*keyEntry
		if cur != nil {
			src = *cur
		}
		if done, err := checkExistingEntry(src, keyID, entry); done || err != nil {
			return err
		}
		m := make(map[uint32]*keyEntry, len(src)+1)
		for id, v := range src {
			m[id] = v
		}
		m[keyID] = entry
		if k.snap.CompareAndSwap(cur, &m) {
			return nil
		}
	}
}

// buildKeyEntry validates keyID/dek and pre-initializes the AEAD.
// Hoisted out of Set so the CAS retry loop stays cyclomatically simple.
func buildKeyEntry(keyID uint32, dek []byte) (*keyEntry, error) {
	if keyID == ReservedKeyID {
		return nil, errors.WithStack(ErrReservedKeyID)
	}
	if len(dek) != KeySize {
		return nil, errors.Wrapf(ErrBadKeySize, "got %d bytes, want %d", len(dek), KeySize)
	}
	entry := &keyEntry{}
	copy(entry.dek[:], dek)
	block, err := aes.NewCipher(entry.dek[:])
	if err != nil {
		return nil, errors.Wrap(err, "encryption: aes.NewCipher")
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, errors.Wrap(err, "encryption: cipher.NewGCM")
	}
	entry.aead = aead
	return entry, nil
}

// checkExistingEntry implements Set's set-once-with-idempotent-same
// rule. Returns (done=true, nil) if keyID is already present with the
// same DEK bytes (Set is a no-op), (true, ErrKeyConflict) if it is
// present with different bytes, or (false, nil) to indicate the
// caller should proceed with the CAS insert.
func checkExistingEntry(src map[uint32]*keyEntry, keyID uint32, entry *keyEntry) (bool, error) {
	existing, ok := src[keyID]
	if !ok {
		return false, nil
	}
	if existing.dek == entry.dek {
		return true, nil
	}
	return true, errors.Wrapf(ErrKeyConflict, "key_id=%d", keyID)
}

// Delete removes the DEK for keyID. No-op if absent, the receiver is
// nil, or the Keystore is zero-valued (no map ever Stored).
func (k *Keystore) Delete(keyID uint32) {
	if k == nil {
		return
	}
	for {
		cur := k.snap.Load()
		if cur == nil {
			return
		}
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
// Returns nil for a nil receiver or zero-value Keystore.
func (k *Keystore) IDs() []uint32 {
	m := k.loadMap()
	if len(m) == 0 {
		return nil
	}
	ids := make([]uint32, 0, len(m))
	for id := range m {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	return ids
}

// Len reports the number of currently-loaded keys. Returns 0 for a
// nil receiver or zero-value Keystore.
func (k *Keystore) Len() int {
	return len(k.loadMap())
}
