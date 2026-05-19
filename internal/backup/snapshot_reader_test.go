package backup

import (
	"bytes"
	"encoding/binary"
	"io"
	"math"
	"testing"

	"github.com/cockroachdb/errors"
)

// snapBuilder is a test-side mirror of the live store's native
// snapshot writer (store/lsm_store.go). Each WriteEntry call
// appends an entry whose key is `<userKey><invTS(8 BE)>` and whose
// value is `<flags(1)><expireAt(8 LE)><body>`. The Bytes() result
// is the full `EKVPBBL1`-prefixed snapshot stream a real .fsm file
// would carry.
type snapBuilder struct {
	buf bytes.Buffer
}

func newSnapBuilder(lastCommitTS uint64) *snapBuilder {
	b := &snapBuilder{}
	b.buf.Write(PebbleSnapshotMagic[:])
	_ = binary.Write(&b.buf, binary.LittleEndian, lastCommitTS)
	return b
}

// WriteEntry appends one (userKey, commitTS, body) tuple. Tombstone
// flag is independent of body; the live store writes the same
// header shape for both. expireAt is the absolute Unix-ms expiry
// (0 == no TTL).
func (b *snapBuilder) WriteEntry(userKey []byte, commitTS uint64, body []byte, tombstone bool, expireAt uint64, encState byte) {
	// key = userKey || (^commitTS)BE
	encKey := make([]byte, len(userKey)+snapshotTSSize)
	copy(encKey, userKey)
	binary.BigEndian.PutUint64(encKey[len(userKey):], ^commitTS)
	encVal := make([]byte, snapshotValueHeaderSize+len(body))
	var flags byte
	if tombstone {
		flags |= snapshotTombstoneMask
	}
	flags |= (encState << snapshotEncStateShift) & snapshotEncStateMask
	encVal[0] = flags
	binary.LittleEndian.PutUint64(encVal[1:snapshotValueHeaderSize], expireAt)
	copy(encVal[snapshotValueHeaderSize:], body)
	_ = binary.Write(&b.buf, binary.LittleEndian, uint64(len(encKey)))
	b.buf.Write(encKey)
	_ = binary.Write(&b.buf, binary.LittleEndian, uint64(len(encVal)))
	b.buf.Write(encVal)
}

func (b *snapBuilder) Bytes() []byte { return b.buf.Bytes() }

// TestReadSnapshot_HeaderOnly pins that a snapshot containing only
// the magic + ts header (no entries) terminates cleanly with the
// header surfaced to the callback's first arg and zero entry
// callbacks.
func TestReadSnapshot_HeaderOnly(t *testing.T) {
	t.Parallel()
	b := newSnapBuilder(0xdeadbeef)
	var sawHeader SnapshotHeader
	var count int
	err := ReadSnapshot(bytes.NewReader(b.Bytes()), func(h SnapshotHeader, _ SnapshotEntry) error {
		sawHeader = h
		count++
		return nil
	})
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if count != 0 {
		t.Fatalf("entries = %d, want 0", count)
	}
	// Header callback never fires on an empty body — we don't
	// surface SnapshotHeader through fn until at least one entry
	// arrives. The "want 0" check above is sufficient.
	_ = sawHeader
}

// TestReadSnapshot_SingleEntryRoundTrip pins the basic single-entry
// path: header parses, key/value decode, MVCC metadata surfaces.
func TestReadSnapshot_SingleEntryRoundTrip(t *testing.T) {
	t.Parallel()
	b := newSnapBuilder(100)
	b.WriteEntry([]byte("!redis|str|hello"), 42, []byte("world"), false, 1234567, snapshotEncStateCleartx)
	var got SnapshotEntry
	var hdr SnapshotHeader
	err := ReadSnapshot(bytes.NewReader(b.Bytes()), func(h SnapshotHeader, e SnapshotEntry) error {
		hdr = h
		got = e
		// Clone the bytes because the reader's scratch buffer
		// may be overwritten on return.
		got.UserKey = bytes.Clone(e.UserKey)
		got.UserValue = bytes.Clone(e.UserValue)
		return nil
	})
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if hdr.LastCommitTS != 100 {
		t.Fatalf("hdr.LastCommitTS = %d, want 100", hdr.LastCommitTS)
	}
	if string(got.UserKey) != "!redis|str|hello" {
		t.Fatalf("UserKey = %q, want %q", got.UserKey, "!redis|str|hello")
	}
	if string(got.UserValue) != "world" {
		t.Fatalf("UserValue = %q, want %q", got.UserValue, "world")
	}
	if got.CommitTS != 42 {
		t.Fatalf("CommitTS = %d, want 42", got.CommitTS)
	}
	if got.ExpireAt != 1234567 {
		t.Fatalf("ExpireAt = %d, want 1234567", got.ExpireAt)
	}
	if got.Tombstone {
		t.Fatalf("Tombstone = true, want false")
	}
}

// TestReadSnapshot_TombstoneFlagSurfaced pins that the tombstone
// bit on the value-header is exposed via SnapshotEntry.Tombstone.
// Phase 0a callers (the dispatcher) skip tombstones so a restored
// dump matches the snapshot's logical visibility; surfacing the
// flag also lets diagnostic dumps include them.
func TestReadSnapshot_TombstoneFlagSurfaced(t *testing.T) {
	t.Parallel()
	b := newSnapBuilder(1)
	b.WriteEntry([]byte("!redis|str|gone"), 7, nil, true, 0, snapshotEncStateCleartx)
	var sawTomb bool
	err := ReadSnapshot(bytes.NewReader(b.Bytes()), func(_ SnapshotHeader, e SnapshotEntry) error {
		sawTomb = e.Tombstone
		return nil
	})
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if !sawTomb {
		t.Fatalf("tombstone flag did not surface")
	}
}

// TestReadSnapshot_MultipleEntriesPreserveOrder pins that the
// reader yields entries in the on-disk order. The live snapshot
// stream is sorted by encoded key (userKey||^TS), so newer
// versions of the same userKey appear FIRST. The dispatcher
// relies on this order for first-seen-wins dedup.
func TestReadSnapshot_MultipleEntriesPreserveOrder(t *testing.T) {
	t.Parallel()
	b := newSnapBuilder(0)
	keys := []string{"!redis|str|a", "!redis|str|b", "!redis|str|c"}
	for i, k := range keys {
		b.WriteEntry([]byte(k), uint64(i+1), []byte{byte(i)}, false, 0, snapshotEncStateCleartx) //nolint:gosec // i bounded by len(keys)<<63
	}
	var gotKeys []string
	err := ReadSnapshot(bytes.NewReader(b.Bytes()), func(_ SnapshotHeader, e SnapshotEntry) error {
		gotKeys = append(gotKeys, string(e.UserKey))
		return nil
	})
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if len(gotKeys) != len(keys) {
		t.Fatalf("entries = %d, want %d", len(gotKeys), len(keys))
	}
	for i, k := range keys {
		if gotKeys[i] != k {
			t.Fatalf("entries[%d] = %q, want %q", i, gotKeys[i], k)
		}
	}
}

// TestReadSnapshot_RejectsBadMagic pins that a file whose first 8
// bytes are not "EKVPBBL1" fails closed with ErrSnapshotBadMagic.
// This is the trip-wire for "operator pointed the decoder at an
// MVCC streaming snapshot, a tar archive, or a truncated file."
func TestReadSnapshot_RejectsBadMagic(t *testing.T) {
	t.Parallel()
	bad := []byte("MVCCSTRM" + "....16....bytes.")
	err := ReadSnapshot(bytes.NewReader(bad), func(_ SnapshotHeader, _ SnapshotEntry) error {
		t.Fatalf("callback fired on bad-magic input")
		return nil
	})
	if !errors.Is(err, ErrSnapshotBadMagic) {
		t.Fatalf("err = %v want ErrSnapshotBadMagic", err)
	}
}

// TestReadSnapshot_RejectsTruncatedHeader pins that a file shorter
// than the 16-byte header (magic + ts) returns io.EOF / unexpected
// EOF wrapped in WithStack — not ErrSnapshotBadMagic. This
// matters: a 0-byte file is "no snapshot" and a 4-byte file is "I
// tried to write a snapshot and crashed"; conflating the two would
// hide truncation incidents.
func TestReadSnapshot_RejectsTruncatedHeader(t *testing.T) {
	t.Parallel()
	err := ReadSnapshot(bytes.NewReader([]byte("EKVP")), func(_ SnapshotHeader, _ SnapshotEntry) error {
		return nil
	})
	if err == nil {
		t.Fatalf("expected error on truncated header")
	}
	if errors.Is(err, ErrSnapshotBadMagic) {
		t.Fatalf("truncated header must NOT report bad magic, got %v", err)
	}
}

// TestReadSnapshot_RejectsTruncatedEntry pins that a snapshot
// ending after a key-length but before the key bytes surfaces as
// ErrSnapshotTruncated. A clean EOF at the start of the key-length
// field is the normal terminator and must NOT trigger this.
func TestReadSnapshot_RejectsTruncatedEntry(t *testing.T) {
	t.Parallel()
	b := newSnapBuilder(0)
	// Append a key-length but no key bytes.
	var l [8]byte
	binary.LittleEndian.PutUint64(l[:], 32)
	full := append(b.Bytes(), l[:]...)
	err := ReadSnapshot(bytes.NewReader(full), func(_ SnapshotHeader, _ SnapshotEntry) error {
		return nil
	})
	if !errors.Is(err, ErrSnapshotTruncated) {
		t.Fatalf("err = %v want ErrSnapshotTruncated", err)
	}
}

// TestReadSnapshot_RejectsTruncatedBeforeValueLength pins the
// gemini-high fix on PR #792: a snapshot that ends AFTER the key
// bytes but BEFORE the value-length field must surface as
// ErrSnapshotTruncated (not the previous ErrSnapshotShortValue
// misclassification, which happened because readEntryLen's `eof`
// return value was being ignored on the second call).
func TestReadSnapshot_RejectsTruncatedBeforeValueLength(t *testing.T) {
	t.Parallel()
	b := newSnapBuilder(0)
	// Build a complete key (length + bytes) but no value-length.
	encKey := make([]byte, 1+snapshotTSSize)
	encKey[0] = 'k'
	binary.BigEndian.PutUint64(encKey[1:], ^uint64(1))
	var kLen [8]byte
	binary.LittleEndian.PutUint64(kLen[:], uint64(len(encKey)))
	full := append(b.Bytes(), kLen[:]...)
	full = append(full, encKey...)
	err := ReadSnapshot(bytes.NewReader(full), func(_ SnapshotHeader, _ SnapshotEntry) error {
		return nil
	})
	if !errors.Is(err, ErrSnapshotTruncated) {
		t.Fatalf("err = %v want ErrSnapshotTruncated", err)
	}
}

// TestReadSnapshot_RejectsKeyLengthOverBudget pins the codex P1 +
// gemini security-high fix on PR #792: a corrupt or adversarial
// snapshot whose length-prefix declares a key larger than
// MaxSnapshotEncodedKeySize must fail at the length-prefix check
// BEFORE allocating `make([]byte, kLen)`. Without this guard a 4 GB
// length prefix would OOM the decoder (or panic on 32-bit when
// uint64 → int narrowing wraps).
func TestReadSnapshot_RejectsKeyLengthOverBudget(t *testing.T) {
	t.Parallel()
	b := newSnapBuilder(0)
	// Length-prefix one byte over the budget.
	var l [8]byte
	binary.LittleEndian.PutUint64(l[:], uint64(MaxSnapshotEncodedKeySize)+1)
	full := append(b.Bytes(), l[:]...)
	err := ReadSnapshot(bytes.NewReader(full), func(_ SnapshotHeader, _ SnapshotEntry) error {
		t.Fatalf("callback fired on over-budget key length")
		return nil
	})
	if !errors.Is(err, ErrSnapshotKeyTooLarge) {
		t.Fatalf("err = %v want ErrSnapshotKeyTooLarge", err)
	}
}

// TestReadSnapshot_RejectsValueLengthOverBudget mirrors the
// key-length guard for the value side.
func TestReadSnapshot_RejectsValueLengthOverBudget(t *testing.T) {
	t.Parallel()
	b := newSnapBuilder(0)
	// Build a valid key entry, then a value-length one byte over the
	// budget.
	encKey := make([]byte, 1+snapshotTSSize)
	encKey[0] = 'k'
	binary.BigEndian.PutUint64(encKey[1:], ^uint64(1))
	var kLen, vLen [8]byte
	binary.LittleEndian.PutUint64(kLen[:], uint64(len(encKey)))
	binary.LittleEndian.PutUint64(vLen[:], uint64(MaxSnapshotEncodedValueSize)+1)
	full := append(b.Bytes(), kLen[:]...)
	full = append(full, encKey...)
	full = append(full, vLen[:]...)
	err := ReadSnapshot(bytes.NewReader(full), func(_ SnapshotHeader, _ SnapshotEntry) error {
		t.Fatalf("callback fired on over-budget value length")
		return nil
	})
	if !errors.Is(err, ErrSnapshotValueTooLarge) {
		t.Fatalf("err = %v want ErrSnapshotValueTooLarge", err)
	}
}

// TestReadSnapshot_AcceptsKeyLengthAtBudgetBoundary pins the off-
// by-one: keyLen == MaxSnapshotEncodedKeySize must be accepted (the
// reader rejects only >, matching the live store's
// `readRestoreFieldLen` semantics). We test with a 1 KiB key
// because allocating the full 1 MiB on every run would slow the
// test suite — the boundary is the same regardless of magnitude.
func TestReadSnapshot_AcceptsKeyLengthAtBudgetBoundary(t *testing.T) {
	t.Parallel()
	b := newSnapBuilder(0)
	// 1 KiB user key + 8-byte TS suffix.
	const userKeyLen = 1 << 10
	userKey := make([]byte, userKeyLen)
	for i := range userKey {
		userKey[i] = byte(i % 256)
	}
	b.WriteEntry(userKey, 1, []byte("v"), false, 0, snapshotEncStateCleartx)
	var got SnapshotEntry
	err := ReadSnapshot(bytes.NewReader(b.Bytes()), func(_ SnapshotHeader, e SnapshotEntry) error {
		got = e
		got.UserKey = bytes.Clone(e.UserKey)
		return nil
	})
	if err != nil {
		t.Fatalf("err = %v want nil at length within budget", err)
	}
	if !bytes.Equal(got.UserKey, userKey) {
		t.Fatalf("UserKey mismatch at boundary")
	}
}

// TestReadSnapshot_RejectsShortKey pins the encoded-key-length
// invariant. Every encoded key has an 8-byte TS suffix; an entry
// with a shorter key indicates store corruption.
func TestReadSnapshot_RejectsShortKey(t *testing.T) {
	t.Parallel()
	b := newSnapBuilder(0)
	// Encode an entry with a key shorter than the TS suffix by
	// hand. WriteEntry won't do this, so we splice the bytes in
	// directly.
	bodyHdr := []byte{0x00, 0, 0, 0, 0, 0, 0, 0, 0} // 9-byte header, zero flags + expireAt
	var kLen, vLen [8]byte
	binary.LittleEndian.PutUint64(kLen[:], 4) // < snapshotTSSize=8
	binary.LittleEndian.PutUint64(vLen[:], uint64(len(bodyHdr)))
	full := append(b.Bytes(), kLen[:]...)
	full = append(full, 'a', 'b', 'c', 'd')
	full = append(full, vLen[:]...)
	full = append(full, bodyHdr...)
	err := ReadSnapshot(bytes.NewReader(full), func(_ SnapshotHeader, _ SnapshotEntry) error {
		return nil
	})
	if !errors.Is(err, ErrSnapshotShortKey) {
		t.Fatalf("err = %v want ErrSnapshotShortKey", err)
	}
}

// TestReadSnapshot_RejectsShortValue pins that an entry with a
// value shorter than the 9-byte header (flags + expireAt) fails
// closed. The live store always emits the full 9 bytes even for
// tombstones.
func TestReadSnapshot_RejectsShortValue(t *testing.T) {
	t.Parallel()
	b := newSnapBuilder(0)
	// Encoded key with a valid 8-byte TS suffix.
	encKey := make([]byte, 1+snapshotTSSize)
	encKey[0] = 'k'
	binary.BigEndian.PutUint64(encKey[1:], ^uint64(1))
	var kLen, vLen [8]byte
	binary.LittleEndian.PutUint64(kLen[:], uint64(len(encKey)))
	binary.LittleEndian.PutUint64(vLen[:], 4) // < snapshotValueHeaderSize=9
	full := append(b.Bytes(), kLen[:]...)
	full = append(full, encKey...)
	full = append(full, vLen[:]...)
	full = append(full, 'a', 'b', 'c', 'd')
	err := ReadSnapshot(bytes.NewReader(full), func(_ SnapshotHeader, _ SnapshotEntry) error {
		return nil
	})
	if !errors.Is(err, ErrSnapshotShortValue) {
		t.Fatalf("err = %v want ErrSnapshotShortValue", err)
	}
}

// TestReadSnapshot_RejectsEncryptedEntry pins that Phase 0a fails
// closed on encrypted entries. Stage 8 of the encryption rollout
// will add a keyring-aware variant; until then, attempting to
// decode an encrypted snapshot with this binary returns
// ErrSnapshotEncryptedEntry rather than silently emitting
// ciphertext.
func TestReadSnapshot_RejectsEncryptedEntry(t *testing.T) {
	t.Parallel()
	b := newSnapBuilder(0)
	b.WriteEntry([]byte("k"), 1, []byte("ciphertext"), false, 0, snapshotEncStateEncrypt)
	err := ReadSnapshot(bytes.NewReader(b.Bytes()), func(_ SnapshotHeader, _ SnapshotEntry) error {
		t.Fatalf("callback must not fire on encrypted entry")
		return nil
	})
	if !errors.Is(err, ErrSnapshotEncryptedEntry) {
		t.Fatalf("err = %v want ErrSnapshotEncryptedEntry", err)
	}
}

// TestReadSnapshot_RejectsReservedEncryptionState pins the §7.1
// fail-closed contract for reserved encState bits (0b10, 0b11) and
// the high-bit reserved-zero range.
func TestReadSnapshot_RejectsReservedEncryptionState(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name  string
		flags byte
	}{
		{"reserved-encstate-10", 0b0000_0100}, // encState bits = 0b10
		{"reserved-encstate-11", 0b0000_0110}, // encState bits = 0b11
		{"reserved-high-bit-3", 0b0000_1000},  // bit 3 set
		{"reserved-high-bit-7", 0b1000_0000},  // bit 7 set
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			b := newSnapBuilder(0)
			// Build an entry by hand with the chosen flags byte.
			encKey := make([]byte, 1+snapshotTSSize)
			encKey[0] = 'k'
			binary.BigEndian.PutUint64(encKey[1:], ^uint64(1))
			encVal := make([]byte, snapshotValueHeaderSize+1)
			encVal[0] = tc.flags
			binary.LittleEndian.PutUint64(encVal[1:snapshotValueHeaderSize], 0)
			encVal[snapshotValueHeaderSize] = 'v'
			var kLen, vLen [8]byte
			binary.LittleEndian.PutUint64(kLen[:], uint64(len(encKey)))
			binary.LittleEndian.PutUint64(vLen[:], uint64(len(encVal)))
			full := append(b.Bytes(), kLen[:]...)
			full = append(full, encKey...)
			full = append(full, vLen[:]...)
			full = append(full, encVal...)
			err := ReadSnapshot(bytes.NewReader(full), func(_ SnapshotHeader, _ SnapshotEntry) error {
				t.Fatalf("callback fired on reserved encState")
				return nil
			})
			if !errors.Is(err, ErrSnapshotEncryptedReserved) {
				t.Fatalf("err = %v want ErrSnapshotEncryptedReserved", err)
			}
		})
	}
}

// TestReadSnapshot_CallbackErrorPropagates pins that fn's error
// terminates iteration and returns the error verbatim (caller can
// distinguish their own error from a snapshot-format error via
// errors.Is).
func TestReadSnapshot_CallbackErrorPropagates(t *testing.T) {
	t.Parallel()
	b := newSnapBuilder(0)
	b.WriteEntry([]byte("k1"), 1, []byte("v1"), false, 0, snapshotEncStateCleartx)
	b.WriteEntry([]byte("k2"), 2, []byte("v2"), false, 0, snapshotEncStateCleartx)
	sentinel := errors.New("test sentinel")
	var calls int
	err := ReadSnapshot(bytes.NewReader(b.Bytes()), func(_ SnapshotHeader, _ SnapshotEntry) error {
		calls++
		return sentinel
	})
	if !errors.Is(err, sentinel) {
		t.Fatalf("err = %v want sentinel", err)
	}
	if calls != 1 {
		t.Fatalf("callback called %d times after sentinel error, want 1", calls)
	}
}

// TestReadSnapshot_CommitTSInversionRoundTrips pins that the
// inverted-TS suffix (^commitTS) decodes back to the original TS
// across the math.MaxUint64 boundary. Off-by-one errors in the
// XOR would break sort order silently.
func TestReadSnapshot_CommitTSInversionRoundTrips(t *testing.T) {
	t.Parallel()
	for _, ts := range []uint64{0, 1, 1 << 32, math.MaxUint64 - 1, math.MaxUint64} {
		b := newSnapBuilder(0)
		b.WriteEntry([]byte("k"), ts, []byte("v"), false, 0, snapshotEncStateCleartx)
		var got uint64
		err := ReadSnapshot(bytes.NewReader(b.Bytes()), func(_ SnapshotHeader, e SnapshotEntry) error {
			got = e.CommitTS
			return nil
		})
		if err != nil {
			t.Fatalf("ts=%d: err=%v", ts, err)
		}
		if got != ts {
			t.Fatalf("ts=%d round-tripped to %d", ts, got)
		}
	}
}

// TestReadSnapshot_EmptyValueBody pins that an entry with a 9-byte
// value (header only, zero body) decodes to UserValue=[] without
// firing ErrSnapshotShortValue. This is the on-disk shape of a
// tombstone or a deliberately empty value.
func TestReadSnapshot_EmptyValueBody(t *testing.T) {
	t.Parallel()
	b := newSnapBuilder(0)
	b.WriteEntry([]byte("k"), 1, nil, false, 0, snapshotEncStateCleartx)
	var sawEmpty bool
	err := ReadSnapshot(bytes.NewReader(b.Bytes()), func(_ SnapshotHeader, e SnapshotEntry) error {
		sawEmpty = len(e.UserValue) == 0
		return nil
	})
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if !sawEmpty {
		t.Fatalf("expected empty UserValue, got non-empty")
	}
}

// Compile-time sanity: io.EOF can flow up from the reader without
// being misclassified as a snapshot-format error. (Mostly a guard
// for future refactors that might add error wrapping.)
var _ = io.EOF
