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
