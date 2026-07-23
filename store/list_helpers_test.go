package store

import (
	"bytes"
	"encoding/binary"
	"math"
	"testing"
)

func TestExtractListUserKeyFromScanKeyBoundsOverflow(t *testing.T) {
	t.Parallel()

	var lenPrefix [wideColKeyLenSize]byte
	binary.BigEndian.PutUint32(lenPrefix[:], math.MaxUint32)

	for _, tc := range []struct {
		name    string
		key     []byte
		extract func([]byte) []byte
	}{
		{
			name:    "delta scan",
			key:     append(append([]byte(nil), []byte(ListMetaDeltaPrefix)...), lenPrefix[:]...),
			extract: ExtractListUserKeyFromDeltaScanKey,
		},
		{
			name:    "claim scan",
			key:     append(append([]byte(nil), []byte(ListClaimPrefix)...), lenPrefix[:]...),
			extract: ExtractListUserKeyFromClaimScanKey,
		},
		{
			name:    "full delta",
			key:     append(append(append([]byte(nil), []byte(ListMetaDeltaPrefix)...), lenPrefix[:]...), make([]byte, deltaKeyTSSize+deltaKeySeqSize)...),
			extract: ExtractListUserKeyFromDelta,
		},
		{
			name:    "full claim",
			key:     append(append(append([]byte(nil), []byte(ListClaimPrefix)...), lenPrefix[:]...), make([]byte, sortableInt64Bytes)...),
			extract: ExtractListUserKeyFromClaim,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := tc.extract(tc.key); got != nil {
				t.Fatalf("overflow user-key length: want nil, got %q", got)
			}
		})
	}
}

func TestExtractListUserKeyFromScanKeyRoundTrip(t *testing.T) {
	t.Parallel()

	userKey := []byte("list-user")
	if got := ExtractListUserKeyFromDeltaScanKey(ListMetaDeltaScanPrefix(userKey)); !bytes.Equal(got, userKey) {
		t.Fatalf("delta scan round trip: want %q, got %q", userKey, got)
	}
	if got := ExtractListUserKeyFromClaimScanKey(ListClaimScanPrefix(userKey)); !bytes.Equal(got, userKey) {
		t.Fatalf("claim scan round trip: want %q, got %q", userKey, got)
	}
}
