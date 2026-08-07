package encryption_test

import (
	"bytes"
	"testing"

	"github.com/bootjp/elastickv/internal/encryption"
	"github.com/cockroachdb/errors"
)

func TestRegistryKey_RoundTrip(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name     string
		dekID    uint32
		nodeID16 uint16
	}{
		{"low ids", 1, 1},
		{"mid ids", 0xCAFE, 0xBABE},
		{"max ids", 0xFFFFFFFF, 0xFFFF},
		{"reserved keyID", encryption.ReservedKeyID, 0x1234},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			key := encryption.RegistryKey(tc.dekID, tc.nodeID16)
			if !bytes.HasPrefix(key, encryption.WriterRegistryPrefix) {
				t.Fatalf("key missing prefix: %q", key)
			}
			gotDEK, gotNode, err := encryption.DecodeRegistryKey(key)
			if err != nil {
				t.Fatalf("DecodeRegistryKey: %v", err)
			}
			if gotDEK != tc.dekID || gotNode != tc.nodeID16 {
				t.Fatalf("round-trip mismatch: got (%d, %d) want (%d, %d)",
					gotDEK, gotNode, tc.dekID, tc.nodeID16)
			}
		})
	}
}

// TestRegistryKey_ByteLayoutPin pins the on-disk byte layout so a
// future refactor cannot silently change the wire format every
// replica's Pebble store agrees on.
func TestRegistryKey_ByteLayoutPin(t *testing.T) {
	t.Parallel()
	got := encryption.RegistryKey(0xAABBCCDD, 0xEEFF)
	want := append([]byte("!encryption|writers|"),
		0xAA, 0xBB, 0xCC, 0xDD,
		'|',
		0xEE, 0xFF,
	)
	if !bytes.Equal(got, want) {
		t.Fatalf("layout drift:\n got  %x\n want %x", got, want)
	}
}

func TestRegistryDEKPrefix(t *testing.T) {
	t.Parallel()
	pfx := encryption.RegistryDEKPrefix(0x42)
	want := append([]byte("!encryption|writers|"), 0x00, 0x00, 0x00, 0x42, '|')
	if !bytes.Equal(pfx, want) {
		t.Fatalf("DEK prefix drift:\n got  %x\n want %x", pfx, want)
	}
	// Every key for the same dek_id must start with this prefix.
	for _, nodeID16 := range []uint16{0, 1, 0xFFFF} {
		full := encryption.RegistryKey(0x42, nodeID16)
		if !bytes.HasPrefix(full, pfx) {
			t.Fatalf("nodeID16=%d key not under DEK prefix", nodeID16)
		}
	}
	// A different dek_id must NOT start with this prefix.
	other := encryption.RegistryKey(0x43, 0)
	if bytes.HasPrefix(other, pfx) {
		t.Fatal("different dek_id key starts with retired-DEK prefix")
	}
}

func TestIsRegistryKey(t *testing.T) {
	t.Parallel()
	valid := encryption.RegistryKey(0xfeedbeef, 0xcafe)
	wrongPrefix := append([]byte{}, valid...)
	wrongPrefix[1] = 'x'
	missingSep := append([]byte{}, valid...)
	missingSep[len(encryption.WriterRegistryPrefix)+4] = 'X'
	prefixUserKey := append([]byte{}, encryption.WriterRegistryPrefix...)
	prefixUserKey = append(prefixUserKey, []byte("tenant-visible-key")...)
	cases := []struct {
		name string
		key  []byte
		want bool
	}{
		{"valid", valid, true},
		{"empty", nil, false},
		{"wrong length", append([]byte{}, valid[:len(valid)-1]...), false},
		{"wrong prefix same length", wrongPrefix, false},
		{"missing separator", missingSep, false},
		{"user key with registry prefix remains data", prefixUserKey, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := encryption.IsRegistryKey(tc.key); got != tc.want {
				t.Fatalf("IsRegistryKey() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestDecodeRegistryKey_RejectsMalformed(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		key  []byte
	}{
		{"empty", nil},
		{"short", []byte("x")},
		{"wrong prefix", append([]byte("!admin|writers|"), 0x01, 0x02, 0x03, 0x04, '|', 0x00, 0x01)},
		{"missing separator", append([]byte("!encryption|writers|"), 0x01, 0x02, 0x03, 0x04, 'X', 0x00, 0x01)},
		{"truncated suffix", append([]byte("!encryption|writers|"), 0x01, 0x02, 0x03, 0x04, '|', 0x00)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, _, err := encryption.DecodeRegistryKey(tc.key)
			if !errors.Is(err, encryption.ErrRegistryKeyMalformed) {
				t.Fatalf("expected ErrRegistryKeyMalformed, got %v", err)
			}
		})
	}
}

func TestRegistryValue_RoundTrip(t *testing.T) {
	t.Parallel()
	cases := []encryption.RegistryValue{
		{},
		{FullNodeID: 1, FirstSeenLocalEpoch: 1, LastSeenLocalEpoch: 1},
		{FullNodeID: 0xDEADBEEFCAFEBABE, FirstSeenLocalEpoch: 0xABCD, LastSeenLocalEpoch: 0xEF01},
		{FullNodeID: ^uint64(0), FirstSeenLocalEpoch: 0xFFFF, LastSeenLocalEpoch: 0xFFFF},
	}
	for _, want := range cases {
		raw := encryption.EncodeRegistryValue(want)
		got, err := encryption.DecodeRegistryValue(raw)
		if err != nil {
			t.Fatalf("decode %+v: %v", want, err)
		}
		if got != want {
			t.Fatalf("round-trip mismatch: got %+v want %+v", got, want)
		}
	}
}

// TestRegistryValue_ByteLayoutPin pins the on-disk value layout so
// the registry's stored bytes survive a refactor without forcing a
// migration of every replica's Pebble database.
func TestRegistryValue_ByteLayoutPin(t *testing.T) {
	t.Parallel()
	got := encryption.EncodeRegistryValue(encryption.RegistryValue{
		FullNodeID:          0x0102030405060708,
		FirstSeenLocalEpoch: 0x090A,
		LastSeenLocalEpoch:  0x0B0C,
	})
	want := []byte{
		0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, // FullNodeID
		0x09, 0x0A, // FirstSeenLocalEpoch
		0x0B, 0x0C, // LastSeenLocalEpoch
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("layout drift:\n got  %x\n want %x", got, want)
	}
}

func TestDecodeRegistryValue_RejectsMalformed(t *testing.T) {
	t.Parallel()
	for _, l := range []int{0, 1, 11, 13, 100} {
		_, err := encryption.DecodeRegistryValue(make([]byte, l))
		if !errors.Is(err, encryption.ErrRegistryValueMalformed) {
			t.Fatalf("len=%d: expected ErrRegistryValueMalformed, got %v", l, err)
		}
	}
}
