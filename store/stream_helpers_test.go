package store

import (
	"bytes"
	"encoding/binary"
	"math"
	"testing"
)

// TestExtractStreamUserKeyFromMeta_BoundsOverflow guards against a corrupted
// ukLen near math.MaxUint32 wrapping the (wideColKeyLenSize + ukLen) sum
// when the comparison was previously done in uint32. A wrapped sum that
// becomes smaller than len(trimmed) would skip the early return and panic
// on the trimmed[lo:hi] slice. The fix moves the comparison into uint64.
func TestExtractStreamUserKeyFromMeta_BoundsOverflow(t *testing.T) {
	t.Parallel()

	// Craft a meta key with the prefix + a length-prefix word claiming
	// math.MaxUint32 user-key bytes follow, but with no actual user key
	// data. uint32(wideColKeyLenSize) + math.MaxUint32 wraps to
	// (wideColKeyLenSize - 1) under uint32 arithmetic — a value far less
	// than len(trimmed)=4, which would cause the old guard to fall through.
	key := make([]byte, 0, len(StreamMetaPrefix)+wideColKeyLenSize)
	key = append(key, StreamMetaPrefix...)
	var lenPrefix [wideColKeyLenSize]byte
	binary.BigEndian.PutUint32(lenPrefix[:], math.MaxUint32)
	key = append(key, lenPrefix[:]...)

	got := ExtractStreamUserKeyFromMeta(key)
	if got != nil {
		t.Fatalf("ExtractStreamUserKeyFromMeta on overflow ukLen: want nil, got %q", got)
	}
}

// TestExtractStreamUserKeyFromEntry_BoundsOverflow is the entry-key analogue
// of the meta-key test, with the additional StreamIDBytes (16) suffix in the
// bounds check that must also be done in uint64.
func TestExtractStreamUserKeyFromEntry_BoundsOverflow(t *testing.T) {
	t.Parallel()

	// Length-prefix says math.MaxUint32 user-key bytes, then we append
	// exactly StreamIDBytes worth of bytes so that
	// len(trimmed) = wideColKeyLenSize + StreamIDBytes passes the very
	// first early-return ("< wideColKeyLenSize+StreamIDBytes") and exposes
	// the second bounds check to the overflow.
	key := make([]byte, 0, len(StreamEntryPrefix)+wideColKeyLenSize+StreamIDBytes)
	key = append(key, StreamEntryPrefix...)
	var lenPrefix [wideColKeyLenSize]byte
	binary.BigEndian.PutUint32(lenPrefix[:], math.MaxUint32)
	key = append(key, lenPrefix[:]...)
	key = append(key, make([]byte, StreamIDBytes)...)

	got := ExtractStreamUserKeyFromEntry(key)
	if got != nil {
		t.Fatalf("ExtractStreamUserKeyFromEntry on overflow ukLen: want nil, got %q", got)
	}
}

// TestExtractStreamUserKeyFromMeta_RoundTrip is a happy-path regression test
// proving the fix does not break the common case.
func TestExtractStreamUserKeyFromMeta_RoundTrip(t *testing.T) {
	t.Parallel()
	want := []byte("user-key-42")
	if got := ExtractStreamUserKeyFromMeta(StreamMetaKey(want)); !bytes.Equal(got, want) {
		t.Fatalf("round trip: want %q, got %q", want, got)
	}
}

// TestExtractStreamUserKeyFromEntry_RoundTrip is the happy-path round-trip
// for the entry-key extractor.
func TestExtractStreamUserKeyFromEntry_RoundTrip(t *testing.T) {
	t.Parallel()
	want := []byte("entry-user-key")
	if got := ExtractStreamUserKeyFromEntry(StreamEntryKey(want, 1234, 5)); !bytes.Equal(got, want) {
		t.Fatalf("round trip: want %q, got %q", want, got)
	}
}

func TestStreamMetaMarshalRoundTripTrimCursor(t *testing.T) {
	t.Parallel()

	want := StreamMeta{
		Length:     42,
		LastMs:     1000,
		LastSeq:    7,
		ExpireAt:   123456,
		TrimmedMs:  999,
		TrimmedSeq: 6,
	}
	raw, err := MarshalStreamMeta(want)
	if err != nil {
		t.Fatalf("MarshalStreamMeta: %v", err)
	}
	if len(raw) != streamMetaTrimBinarySize {
		t.Fatalf("encoded size: want %d, got %d", streamMetaTrimBinarySize, len(raw))
	}
	got, err := UnmarshalStreamMeta(raw)
	if err != nil {
		t.Fatalf("UnmarshalStreamMeta: %v", err)
	}
	if got != want {
		t.Fatalf("round trip: want %+v, got %+v", want, got)
	}
}

func TestStreamMetaUnmarshalLegacySizes(t *testing.T) {
	t.Parallel()

	legacy := make([]byte, streamMetaLegacyBinarySize)
	binary.BigEndian.PutUint64(legacy[0:8], 3)
	binary.BigEndian.PutUint64(legacy[8:16], 10)
	binary.BigEndian.PutUint64(legacy[16:24], 2)
	got, err := UnmarshalStreamMeta(legacy)
	if err != nil {
		t.Fatalf("legacy UnmarshalStreamMeta: %v", err)
	}
	if got != (StreamMeta{Length: 3, LastMs: 10, LastSeq: 2}) {
		t.Fatalf("legacy meta: got %+v", got)
	}

	current := make([]byte, streamMetaBinarySize)
	copy(current, legacy)
	binary.BigEndian.PutUint64(current[24:32], 99)
	got, err = UnmarshalStreamMeta(current)
	if err != nil {
		t.Fatalf("current UnmarshalStreamMeta: %v", err)
	}
	if got != (StreamMeta{Length: 3, LastMs: 10, LastSeq: 2, ExpireAt: 99}) {
		t.Fatalf("current meta: got %+v", got)
	}
}

func TestStreamEntryScanStartUsesTrimCursor(t *testing.T) {
	t.Parallel()

	key := []byte("trim-cursor")
	if got := StreamEntryScanStart(key, StreamMeta{}); !bytes.Equal(got, StreamEntryScanPrefix(key)) {
		t.Fatalf("no trim cursor: got %q", got)
	}
	if got := StreamEntryScanStart(key, StreamMeta{TrimmedMs: 9, TrimmedSeq: 4}); !bytes.Equal(got, StreamEntryKey(key, 9, 5)) {
		t.Fatalf("trim cursor: want start after 9-4, got %q", got)
	}
	if got := StreamEntryScanStart(key, StreamMeta{TrimmedMs: 9, TrimmedSeq: ^uint64(0)}); !bytes.Equal(got, StreamEntryKey(key, 10, 0)) {
		t.Fatalf("seq overflow cursor: want start at 10-0, got %q", got)
	}
}
