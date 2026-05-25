package backup

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/cockroachdb/errors"
)

// encodeFixtureTS / encodeFixtureTS2 are representative 64-bit HLC
// commit timestamps (non-zero physical half, non-zero logical half)
// used across the encoder-core tests so the invTS suffix is visibly
// distinct from both 0 and all-ones. Two distinct values are used so
// the builder's commitTS parameter is exercised with more than one
// argument.
const (
	encodeFixtureTS  uint64 = 0x0001_8F1A_2B3C_0007
	encodeFixtureTS2 uint64 = 0x0001_8F1A_2B3C_0099
)

// TestEncodeMVCCKeyLayout pins that encodeMVCCKey produces
// <userKey><^commitTS BE> — the layout snapshot_reader.go strips off.
func TestEncodeMVCCKeyLayout(t *testing.T) {
	t.Parallel()
	userKey := []byte("!redis|str|session:abc")
	encKey := encodeMVCCKey(userKey, encodeFixtureTS)
	if !bytes.Equal(encKey[:len(userKey)], userKey) {
		t.Fatalf("encKey prefix = %q, want %q", encKey[:len(userKey)], userKey)
	}
	gotInv := binary.BigEndian.Uint64(encKey[len(userKey):])
	if gotInv != ^encodeFixtureTS {
		t.Fatalf("invTS = %#x, want %#x", gotInv, ^encodeFixtureTS)
	}
}

// TestEncodeMVCCValueLayout pins that encodeMVCCValue produces
// <flags=0><expireAt LE><body>.
func TestEncodeMVCCValueLayout(t *testing.T) {
	t.Parallel()
	userValue := []byte("hello-world")
	const expireAt uint64 = 1_735_689_600_000
	encVal := encodeMVCCValue(userValue, expireAt)
	if encVal[0] != 0 {
		t.Fatalf("flags = %#x, want 0", encVal[0])
	}
	gotExp := binary.LittleEndian.Uint64(encVal[1:snapshotValueHeaderSize])
	if gotExp != expireAt {
		t.Fatalf("expireAt = %d, want %d", gotExp, expireAt)
	}
	if !bytes.Equal(encVal[snapshotValueHeaderSize:], userValue) {
		t.Fatalf("body = %q, want %q", encVal[snapshotValueHeaderSize:], userValue)
	}
}

// TestEncodeMVCCDecodeRoundTrip is the strongest guarantee the encode
// and decode directions agree: the decode primitive recovers exactly
// the tuple that was MVCC-framed.
func TestEncodeMVCCDecodeRoundTrip(t *testing.T) {
	t.Parallel()
	userKey := []byte("!redis|str|session:abc")
	userValue := []byte("hello-world")
	const expireAt uint64 = 1_735_689_600_000

	encKey := encodeMVCCKey(userKey, encodeFixtureTS)
	encVal := encodeMVCCValue(userValue, expireAt)
	entry, err := decodeSnapshotEntry(encKey, encVal)
	if err != nil {
		t.Fatalf("decodeSnapshotEntry: %v", err)
	}
	switch {
	case !bytes.Equal(entry.UserKey, userKey):
		t.Fatalf("decoded UserKey = %q, want %q", entry.UserKey, userKey)
	case !bytes.Equal(entry.UserValue, userValue):
		t.Fatalf("decoded UserValue = %q, want %q", entry.UserValue, userValue)
	case entry.CommitTS != encodeFixtureTS:
		t.Fatalf("decoded CommitTS = %#x, want %#x", entry.CommitTS, encodeFixtureTS)
	case entry.ExpireAt != expireAt:
		t.Fatalf("decoded ExpireAt = %d, want %d", entry.ExpireAt, expireAt)
	case entry.Tombstone:
		t.Fatal("decoded Tombstone = true, want false")
	}
}

// TestSnapshotBuilderRoundTrip is the M1 single-Redis-string fixture:
// a builder with one !redis|str| record writes an EKVPBBL1 stream that
// DecodeLiveEntries reads back to the identical (userKey, userValue,
// expireAt) tuple, and the header carries the builder's commitTS.
func TestSnapshotBuilderRoundTrip(t *testing.T) {
	t.Parallel()
	userKey := []byte("!redis|str|mykey")
	userValue := []byte("myvalue")
	const expireAt uint64 = 0

	b := newSnapshotBuilder(encodeFixtureTS)
	if err := b.Add(userKey, userValue, expireAt); err != nil {
		t.Fatalf("Add: %v", err)
	}
	if b.Len() != 1 {
		t.Fatalf("Len = %d, want 1", b.Len())
	}

	var buf bytes.Buffer
	n, err := b.WriteTo(&buf)
	if err != nil {
		t.Fatalf("WriteTo: %v", err)
	}
	// Byte count is deterministic: magic + lastCommitTS + per-entry
	// (8-byte len prefix + bytes) for both key and value.
	wantN := int64(len(PebbleSnapshotMagic) + 8 +
		(8 + len(userKey) + snapshotTSSize) +
		(8 + snapshotValueHeaderSize + len(userValue)))
	assertWriteByteCount(t, n, &buf, wantN)

	entries, hdr, err := DecodeLiveEntries(&buf)
	if err != nil {
		t.Fatalf("DecodeLiveEntries: %v", err)
	}
	if hdr.LastCommitTS != encodeFixtureTS {
		t.Fatalf("header LastCommitTS = %#x, want %#x", hdr.LastCommitTS, encodeFixtureTS)
	}
	assertSingleEntry(t, entries, userKey, userValue, expireAt)
}

// assertWriteByteCount checks WriteTo's returned count matches the
// expected value and the actual buffer length.
func assertWriteByteCount(t *testing.T, n int64, buf *bytes.Buffer, want int64) {
	t.Helper()
	if n != want {
		t.Fatalf("WriteTo n = %d, want %d", n, want)
	}
	if n != int64(buf.Len()) {
		t.Fatalf("WriteTo n = %d, but buf has %d bytes", n, buf.Len())
	}
}

// assertSingleEntry checks the decoded set has exactly one entry
// matching the given key/value/expiry.
func assertSingleEntry(t *testing.T, entries []RoundTripEntry, key, val []byte, exp uint64) {
	t.Helper()
	if len(entries) != 1 {
		t.Fatalf("got %d entries, want 1", len(entries))
	}
	switch {
	case !bytes.Equal(entries[0].UserKey, key):
		t.Fatalf("UserKey = %q, want %q", entries[0].UserKey, key)
	case !bytes.Equal(entries[0].UserValue, val):
		t.Fatalf("UserValue = %q, want %q", entries[0].UserValue, val)
	case entries[0].ExpireAt != exp:
		t.Fatalf("ExpireAt = %d, want %d", entries[0].ExpireAt, exp)
	}
}

// TestSnapshotBuilderSortedOutput verifies the stream is emitted in
// ascending encoded-key order regardless of Add order — the
// determinism the design's §"Target .fsm format" requires and the
// order the live Pebble-snapshot writer produces.
func TestSnapshotBuilderSortedOutput(t *testing.T) {
	t.Parallel()
	// Add three keys out of order; identical commitTS means the
	// invTS suffix is constant, so sort order is governed by userKey.
	keys := [][]byte{
		[]byte("!redis|str|charlie"),
		[]byte("!redis|str|alpha"),
		[]byte("!redis|str|bravo"),
	}
	// Use a distinct commitTS here so the builder's parameter is
	// exercised with more than the one fixture value.
	b := newSnapshotBuilder(encodeFixtureTS2)
	for _, k := range keys {
		if err := b.Add(k, []byte("v"), 0); err != nil {
			t.Fatalf("Add %q: %v", k, err)
		}
	}
	var buf bytes.Buffer
	if _, err := b.WriteTo(&buf); err != nil {
		t.Fatalf("WriteTo: %v", err)
	}
	entries, _, err := DecodeLiveEntries(&buf)
	if err != nil {
		t.Fatalf("DecodeLiveEntries: %v", err)
	}
	want := [][]byte{
		[]byte("!redis|str|alpha"),
		[]byte("!redis|str|bravo"),
		[]byte("!redis|str|charlie"),
	}
	if len(entries) != len(want) {
		t.Fatalf("got %d entries, want %d", len(entries), len(want))
	}
	for i := range want {
		if !bytes.Equal(entries[i].UserKey, want[i]) {
			t.Fatalf("entry[%d] UserKey = %q, want %q", i, entries[i].UserKey, want[i])
		}
	}
}

// TestSnapshotBuilderRejectsDuplicate pins that two records encoding
// to the same key fail closed with ErrEncodeDuplicateKey rather than
// producing an order-dependent Pebble image.
func TestSnapshotBuilderRejectsDuplicate(t *testing.T) {
	t.Parallel()
	b := newSnapshotBuilder(encodeFixtureTS)
	if err := b.Add([]byte("!redis|str|dup"), []byte("first"), 0); err != nil {
		t.Fatalf("first Add: %v", err)
	}
	err := b.Add([]byte("!redis|str|dup"), []byte("second"), 0)
	if !errors.Is(err, ErrEncodeDuplicateKey) {
		t.Fatalf("err = %v, want ErrEncodeDuplicateKey", err)
	}
}

// TestSnapshotBuilderRejectsOversizeKey pins the per-entry key cap:
// a userKey that pushes the encoded key past MaxSnapshotEncodedKeySize
// fails closed with ErrEncodeKeyTooLarge before any byte is written.
// (The value cap is the identical code path against
// MaxSnapshotEncodedValueSize; it is not exercised here because
// allocating a 256 MiB value in a unit test is wasteful.)
func TestSnapshotBuilderRejectsOversizeKey(t *testing.T) {
	t.Parallel()
	// encKey = userKey + 8-byte suffix; exceed the cap by one.
	oversize := make([]byte, maxSnapshotUserKeySize+1)
	b := newSnapshotBuilder(encodeFixtureTS)
	err := b.Add(oversize, []byte("v"), 0)
	if !errors.Is(err, ErrEncodeKeyTooLarge) {
		t.Fatalf("err = %v, want ErrEncodeKeyTooLarge", err)
	}
}

// TestSnapshotBuilderEmptyRoundTrip pins the empty-builder path (the
// "fresh cluster / empty shard" restore case): WriteTo emits a valid
// header-only stream that DecodeLiveEntries reads back as zero entries
// with the header timestamp intact.
func TestSnapshotBuilderEmptyRoundTrip(t *testing.T) {
	t.Parallel()
	b := newSnapshotBuilder(encodeFixtureTS)
	var buf bytes.Buffer
	if _, err := b.WriteTo(&buf); err != nil {
		t.Fatalf("WriteTo: %v", err)
	}
	entries, hdr, err := DecodeLiveEntries(&buf)
	if err != nil {
		t.Fatalf("DecodeLiveEntries: %v", err)
	}
	if hdr.LastCommitTS != encodeFixtureTS {
		t.Fatalf("hdr.LastCommitTS = %#x, want %#x", hdr.LastCommitTS, encodeFixtureTS)
	}
	if len(entries) != 0 {
		t.Fatalf("got %d entries, want 0", len(entries))
	}
}

// TestSnapshotBuilderRejectsReuse pins that a builder is single-use:
// a second WriteTo fails closed with ErrSnapshotBuilderReused rather
// than silently re-emitting the already-written entries.
func TestSnapshotBuilderRejectsReuse(t *testing.T) {
	t.Parallel()
	b := newSnapshotBuilder(encodeFixtureTS)
	if err := b.Add([]byte("!redis|str|k"), []byte("v"), 0); err != nil {
		t.Fatalf("Add: %v", err)
	}
	var buf bytes.Buffer
	if _, err := b.WriteTo(&buf); err != nil {
		t.Fatalf("first WriteTo: %v", err)
	}
	_, err := b.WriteTo(&buf)
	if !errors.Is(err, ErrSnapshotBuilderReused) {
		t.Fatalf("second WriteTo err = %v, want ErrSnapshotBuilderReused", err)
	}
}

// TestResolveCommitTS pins the --last-commit-ts override semantics
// (design §"MVCC re-encoding"): no override yields the manifest value;
// an override >= manifest is accepted; an override < manifest fails
// closed with ErrLastCommitTSRegression.
func TestResolveCommitTS(t *testing.T) {
	t.Parallel()
	const manifestTS uint64 = 1000
	ptr := func(v uint64) *uint64 { return &v }
	cases := []struct {
		name     string
		override *uint64
		want     uint64
		wantErr  bool
	}{
		{"no override", nil, manifestTS, false},
		{"override equal", ptr(manifestTS), manifestTS, false},
		{"override higher", ptr(manifestTS + 5), manifestTS + 5, false},
		{"override lower rejected", ptr(manifestTS - 1), 0, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := ResolveCommitTS(manifestTS, tc.override)
			if tc.wantErr {
				if !errors.Is(err, ErrLastCommitTSRegression) {
					t.Fatalf("err = %v, want ErrLastCommitTSRegression", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected err: %v", err)
			}
			if got != tc.want {
				t.Fatalf("got %d, want %d", got, tc.want)
			}
		})
	}
}
