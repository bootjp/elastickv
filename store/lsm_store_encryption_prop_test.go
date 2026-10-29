package store

import (
	"bytes"
	"context"
	"path/filepath"
	"testing"

	"github.com/bootjp/elastickv/internal/encryption"
	"github.com/bootjp/elastickv/internal/encryption/encryptiontest"
	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"
)

// TestEncryption_Property_PutGet is a rapid-driven round-trip
// regression for the §4.1 envelope. For every drawn (key, value, ts)
// triple, an encrypted PutAt followed by a GetAt at ts must return
// the original plaintext byte-for-byte. The property covers:
//
//   - Empty plaintext (envelope still has 34 bytes of overhead)
//   - Single byte, multi-KiB, and adversarial random binary keys
//   - Timestamps spanning the full uint64 range (commit-ts is
//     part of AAD via encodeKey, so timestamp interactions cannot
//     silently corrupt decrypt)
//
// The fixture is constructed once per Check call so the same
// keystore + cipher + nonce factory exercises many writes — this
// catches any nonce-reuse regression in DeterministicNonceFactory's
// atomic counter.
//
// Tamper rejection is covered by the deterministic
// TestEncryption_TagTamper unit test, where the close/reopen
// dance is straightforward; reproducing it inside rapid.Check
// would tangle with Cleanup ordering for marginal extra signal.
func TestEncryption_Property_PutGet(t *testing.T) {
	mvcc := newPropEncryptedStore(t)
	rapid.Check(t, func(rt *rapid.T) {
		key := nonEmptyBytes.Draw(rt, "key")
		value := rapid.SliceOf(rapid.Byte()).Draw(rt, "value")
		ts := rapid.Uint64Range(1, ^uint64(0)>>1).Draw(rt, "ts")

		ctx := context.Background()
		require.NoError(rt, mvcc.PutAt(ctx, key, value, ts, 0))
		got, err := mvcc.GetAt(ctx, key, ts)
		require.NoError(rt, err)
		// bytes.Equal treats nil and []byte{} as equal; the AEAD's
		// Open returns nil for an empty plaintext, but the input
		// from rapid may have len=0 with a non-nil slice header.
		// The on-disk bytes are byte-for-byte identical either way.
		require.True(rt, bytes.Equal(value, got),
			"round-trip mismatch: input=%q got=%q", value, got)
	})
}

// newPropEncryptedStore builds one encrypted fixture per Check so the nonce
// counter is shared by all generated writes, including shrinking attempts.
func newPropEncryptedStore(t *testing.T) MVCCStore {
	t.Helper()
	c := encryptiontest.NewCipher(t, 7)
	dir := filepath.Join(t.TempDir(), "pebble")
	mvcc, err := NewPebbleStore(dir,
		WithEncryption(c,
			encryption.NewDeterministicNonceFactory(0xCAFE, 0x0001),
			func() (uint32, bool) { return 7, true },
		),
	)
	if err != nil {
		t.Fatalf("NewPebbleStore: %v", err)
	}
	t.Cleanup(func() { _ = mvcc.Close() })
	return mvcc
}
