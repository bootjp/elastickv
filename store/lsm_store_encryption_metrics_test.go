package store

import (
	"context"
	"crypto/rand"
	"path/filepath"
	"sync"
	"testing"

	"github.com/bootjp/elastickv/internal/encryption"
	"github.com/cockroachdb/pebble/v2"
	"github.com/stretchr/testify/require"
)

// recordingEncryptionObserver captures the §9.2 observations the
// storage envelope path emits. It is concurrency-safe because the
// production observer is called with the store's write locks held in
// some paths and without them in others.
type recordingEncryptionObserver struct {
	mu       sync.Mutex
	failures []string
	writes   []encryptionWriteObservation
}

type encryptionWriteObservation struct {
	keyID          uint32
	plaintextBytes int
	payloadBytes   int
}

func (o *recordingEncryptionObserver) ObserveEncryptionDecryptFailure(reason string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.failures = append(o.failures, reason)
}

func (o *recordingEncryptionObserver) ObserveEncryptionWrite(keyID uint32, plaintextBytes, payloadBytes int) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.writes = append(o.writes, encryptionWriteObservation{
		keyID:          keyID,
		plaintextBytes: plaintextBytes,
		payloadBytes:   payloadBytes,
	})
}

func (o *recordingEncryptionObserver) snapshot() ([]string, []encryptionWriteObservation) {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]string(nil), o.failures...), append([]encryptionWriteObservation(nil), o.writes...)
}

// observedEncryptedFixture is newEncryptedStoreFixture plus a
// recording observer. It reopens through the same option set so a
// tamper-and-reread cycle keeps observing.
type observedEncryptedFixture struct {
	dir    string
	mvcc   MVCCStore
	cipher *encryption.Cipher
	keyID  uint32
	obs    *recordingEncryptionObserver
	closed bool
}

func (f *observedEncryptedFixture) closeIfOpen(tb testing.TB) {
	tb.Helper()
	if f.closed {
		return
	}
	f.closed = true
	require.NoError(tb, f.mvcc.Close())
}

func (f *observedEncryptedFixture) options() []PebbleStoreOption {
	return []PebbleStoreOption{
		WithEncryption(f.cipher,
			NewCounterNonceFactory(0xBEEF, 0x0001),
			func() (uint32, bool) { return f.keyID, true },
		),
		WithEncryptionObserver(f.obs),
	}
}

func (f *observedEncryptedFixture) reopen(tb testing.TB) {
	tb.Helper()
	mvcc, err := NewPebbleStore(f.dir, f.options()...)
	require.NoError(tb, err)
	f.mvcc = mvcc
	f.closed = false
}

func newObservedEncryptedFixture(t *testing.T, keyID uint32) *observedEncryptedFixture {
	t.Helper()

	ks := encryption.NewKeystore()
	dek := make([]byte, encryption.KeySize)
	_, err := rand.Read(dek)
	require.NoError(t, err)
	require.NoError(t, ks.Set(keyID, dek))

	cipher, err := encryption.NewCipher(ks)
	require.NoError(t, err)

	f := &observedEncryptedFixture{
		dir:    filepath.Join(t.TempDir(), "pebble"),
		cipher: cipher,
		keyID:  keyID,
		obs:    &recordingEncryptionObserver{},
	}
	mvcc, err := NewPebbleStore(f.dir, f.options()...)
	require.NoError(t, err)
	f.mvcc = mvcc
	t.Cleanup(func() {
		if !f.closed {
			_ = f.mvcc.Close()
		}
	})
	return f
}

// tamper closes the store, mutates the raw on-disk value, and reopens
// through the observing option set.
func (f *observedEncryptedFixture) tamper(t *testing.T, key []byte, ts uint64, mutate func([]byte) []byte) {
	t.Helper()
	f.closeIfOpen(t)

	pdb, err := pebble.Open(f.dir, &pebble.Options{})
	require.NoError(t, err)
	pebbleKey := encodeKey(key, ts)
	raw, closer, err := pdb.Get(pebbleKey)
	if err != nil {
		_ = pdb.Close()
		t.Fatalf("read raw value: %v", err)
	}
	tampered := mutate(append([]byte(nil), raw...))
	require.NoError(t, closer.Close())
	if err := pdb.Set(pebbleKey, tampered, pebble.Sync); err != nil {
		_ = pdb.Close()
		t.Fatalf("write tampered value: %v", err)
	}
	require.NoError(t, pdb.Close())

	f.reopen(t)
}

// TestEncryptionObserverCountsEveryEmittedEnvelope pins the write-side
// wiring at the real storage seam: the observation must carry the
// active DEK's key_id, the caller's plaintext length, and the length of
// the bytes actually handed to Pebble.
func TestEncryptionObserverCountsEveryEmittedEnvelope(t *testing.T) {
	t.Parallel()

	const keyID = uint32(11)
	f := newObservedEncryptedFixture(t, keyID)
	ctx := context.Background()

	writes := []struct {
		key   []byte
		value []byte
		ts    uint64
	}{
		{key: []byte("k1"), value: []byte("hello"), ts: 100},
		{key: []byte("k2"), value: []byte(""), ts: 101},
		{key: []byte("k3"), value: []byte("a-somewhat-longer-value"), ts: 102},
	}
	for _, w := range writes {
		require.NoError(t, f.mvcc.PutAt(ctx, w.key, w.value, w.ts, 0))
	}

	failures, observed := f.obs.snapshot()
	require.Empty(t, failures, "healthy writes must not touch the paging-grade counter")
	require.Len(t, observed, len(writes))

	for i, got := range observed {
		require.Equal(t, keyID, got.keyID)
		require.Equal(t, len(writes[i].value), got.plaintextBytes)
		require.Greater(t, got.payloadBytes, got.plaintextBytes,
			"an uncompressed envelope always adds header+tag over the plaintext")
	}
}

// TestEncryptionObserverCountsTagMismatchOnTamperedEnvelope is the
// paging-grade path: a disk attacker flips a tag bit and the read must
// both fail closed AND be counted as tag_mismatch.
func TestEncryptionObserverCountsTagMismatchOnTamperedEnvelope(t *testing.T) {
	t.Parallel()

	f := newObservedEncryptedFixture(t, 12)
	ctx := context.Background()
	key := []byte("tampered")

	require.NoError(t, f.mvcc.PutAt(ctx, key, []byte("secret"), 100, 0))
	f.tamper(t, key, 100, func(raw []byte) []byte {
		raw[len(raw)-1] ^= 0xff
		return raw
	})

	_, err := f.mvcc.GetAt(ctx, key, 100)
	require.Error(t, err, "a tampered envelope must fail closed")

	failures, _ := f.obs.snapshot()
	require.Equal(t, []string{encryption.DecryptFailureReasonTagMismatch}, failures)
}

// TestEncryptionObserverStaysSilentOnHealthyReads guards against a
// wiring mistake that would make the counter fire on normal traffic
// and destroy its value as an alert.
func TestEncryptionObserverStaysSilentOnHealthyReads(t *testing.T) {
	t.Parallel()

	f := newObservedEncryptedFixture(t, 13)
	ctx := context.Background()
	key := []byte("healthy")

	require.NoError(t, f.mvcc.PutAt(ctx, key, []byte("value"), 100, 0))
	got, err := f.mvcc.GetAt(ctx, key, 100)
	require.NoError(t, err)
	require.Equal(t, []byte("value"), got)

	failures, _ := f.obs.snapshot()
	require.Empty(t, failures)
}

// TestEncryptionObserverIgnoresRebadgeGuardTrialDecrypts is the
// cardinality/noise guard for the rebadge guard in
// rejectRebadgedEnvelope. That guard trial-decrypts a cleartext body
// against every loaded DEK, so a tag mismatch there is the EXPECTED,
// healthy outcome. Counting it would increment the paging-grade
// counter on essentially every cleartext read.
func TestEncryptionObserverIgnoresRebadgeGuardTrialDecrypts(t *testing.T) {
	t.Parallel()

	const keyID = uint32(14)

	ks := encryption.NewKeystore()
	dek := make([]byte, encryption.KeySize)
	_, err := rand.Read(dek)
	require.NoError(t, err)
	require.NoError(t, ks.Set(keyID, dek))
	cipher, err := encryption.NewCipher(ks)
	require.NoError(t, err)

	obs := &recordingEncryptionObserver{}
	// Cipher wired but the cutover gate is OFF: writes stay
	// cleartext, and every read runs the rebadge trial-decrypt.
	mvcc, err := NewPebbleStore(filepath.Join(t.TempDir(), "pebble"),
		WithEncryption(cipher,
			NewCounterNonceFactory(0xCAFE, 0x0001),
			func() (uint32, bool) { return keyID, true },
		),
		WithStorageEnvelopeGate(func() bool { return false }),
		WithEncryptionObserver(obs),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = mvcc.Close() })

	ctx := context.Background()
	// A long value so the body clears encryption.EnvelopeOverhead and
	// the guard actually runs its trial decrypts rather than
	// short-circuiting on length.
	value := make([]byte, 256)
	for i := range value {
		value[i] = byte(i)
	}
	key := []byte("cleartext")
	require.NoError(t, mvcc.PutAt(ctx, key, value, 100, 0))

	got, err := mvcc.GetAt(ctx, key, 100)
	require.NoError(t, err)
	require.Equal(t, value, got)

	failures, writes := obs.snapshot()
	require.Empty(t, failures,
		"rebadge-guard trial decrypts are expected to fail and must never be counted")
	require.Empty(t, writes,
		"a cleartext write emits no envelope, so there is nothing to count")
}
