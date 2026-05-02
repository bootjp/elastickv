package encryption_test

import (
	"bytes"
	"testing"

	"github.com/bootjp/elastickv/internal/encryption"
	"github.com/cockroachdb/errors"
)

func TestEnvelope_RoundTrip(t *testing.T) {
	cases := []struct {
		name string
		env  encryption.Envelope
	}{
		{
			name: "empty body",
			env: encryption.Envelope{
				Version: encryption.EnvelopeVersionV1,
				Flag:    0,
				KeyID:   1,
				Body:    bytes.Repeat([]byte{0xAA}, encryption.TagSize),
			},
		},
		{
			name: "compressed flag set",
			env: encryption.Envelope{
				Version: encryption.EnvelopeVersionV1,
				Flag:    encryption.FlagCompressed,
				KeyID:   0xCAFE_F00D,
				Body:    bytes.Repeat([]byte{0x55}, 64+encryption.TagSize),
			},
		},
		{
			name: "max key_id",
			env: encryption.Envelope{
				Version: encryption.EnvelopeVersionV1,
				Flag:    0xff,
				KeyID:   0xFFFF_FFFF,
				Body:    bytes.Repeat([]byte{0x77}, 1024+encryption.TagSize),
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			checkEnvelopeRoundTrip(t, tc.env)
		})
	}
}

// checkEnvelopeRoundTrip encodes the envelope, validates header layout, then
// decodes and asserts every field is preserved bit-for-bit. Split out of
// TestEnvelope_RoundTrip to keep the table-driven loop body simple enough
// for the project's gocognit / cyclop thresholds.
func checkEnvelopeRoundTrip(t *testing.T, env encryption.Envelope) {
	t.Helper()
	// Set a deterministic nonce to make round-trip checks trivially
	// comparable.
	for i := range env.Nonce {
		env.Nonce[i] = byte(i + 1)
	}
	encoded, err := env.Encode()
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	checkEncodedHeader(t, encoded, env)

	decoded, err := encryption.DecodeEnvelope(encoded)
	if err != nil {
		t.Fatalf("DecodeEnvelope: %v", err)
	}
	checkDecodedFields(t, decoded, env)
}

func checkEncodedHeader(t *testing.T, encoded []byte, env encryption.Envelope) {
	t.Helper()
	if got, want := len(encoded), encryption.HeaderSize+len(env.Body); got != want {
		t.Fatalf("encoded length = %d, want %d", got, want)
	}
	if encoded[0] != env.Version {
		t.Fatalf("version byte mismatch: got 0x%02x, want 0x%02x", encoded[0], env.Version)
	}
	if encoded[1] != env.Flag {
		t.Fatalf("flag byte mismatch: got 0x%02x, want 0x%02x", encoded[1], env.Flag)
	}
}

func checkDecodedFields(t *testing.T, decoded *encryption.Envelope, want encryption.Envelope) {
	t.Helper()
	if decoded.Version != want.Version {
		t.Fatal("Version mismatch")
	}
	if decoded.Flag != want.Flag {
		t.Fatal("Flag mismatch")
	}
	if decoded.KeyID != want.KeyID {
		t.Fatalf("KeyID mismatch: got %d, want %d", decoded.KeyID, want.KeyID)
	}
	if decoded.Nonce != want.Nonce {
		t.Fatal("Nonce mismatch")
	}
	if !bytes.Equal(decoded.Body, want.Body) {
		t.Fatal("Body mismatch")
	}
}

func TestEnvelope_Decode_TooShort(t *testing.T) {
	// Anything less than HeaderSize + TagSize is invalid.
	for _, n := range []int{0, 1, encryption.HeaderSize - 1, encryption.HeaderSize, encryption.HeaderSize + encryption.TagSize - 1} {
		buf := make([]byte, n)
		if n > 0 {
			buf[0] = encryption.EnvelopeVersionV1
		}
		_, err := encryption.DecodeEnvelope(buf)
		if !errors.Is(err, encryption.ErrEnvelopeShort) {
			t.Fatalf("len=%d: expected ErrEnvelopeShort, got %v", n, err)
		}
	}
}

func TestEnvelope_Decode_RejectsUnknownVersion(t *testing.T) {
	for _, version := range []byte{0x00, 0x02, 0x10, 0xFE, 0xFF} {
		buf := make([]byte, encryption.HeaderSize+encryption.TagSize)
		buf[0] = version
		_, err := encryption.DecodeEnvelope(buf)
		if !errors.Is(err, encryption.ErrEnvelopeVersion) {
			t.Fatalf("version=0x%02x: expected ErrEnvelopeVersion, got %v", version, err)
		}
	}
}

func TestEnvelope_Decode_DoesNotAliasInput(t *testing.T) {
	src := make([]byte, encryption.HeaderSize+encryption.TagSize+8)
	src[0] = encryption.EnvelopeVersionV1
	for i := encryption.HeaderSize; i < len(src); i++ {
		src[i] = 0x42
	}
	decoded, err := encryption.DecodeEnvelope(src)
	if err != nil {
		t.Fatalf("DecodeEnvelope: %v", err)
	}
	// Mutate src; decoded.Body must not change.
	src[encryption.HeaderSize] = 0xff
	if decoded.Body[0] == 0xff {
		t.Fatal("Decode aliased input: src mutation leaked into decoded.Body")
	}
}

func TestHeaderAADBytes_Layout(t *testing.T) {
	got := encryption.HeaderAADBytes(encryption.EnvelopeVersionV1, encryption.FlagCompressed, 0x12345678)
	want := []byte{0x01, 0x01, 0x12, 0x34, 0x56, 0x78}
	if !bytes.Equal(got, want) {
		t.Fatalf("HeaderAADBytes layout mismatch:\n  got  %x\n  want %x", got, want)
	}
}

func TestAppendHeaderAADBytes_AppendsAndAllocFree(t *testing.T) {
	// Append onto an existing buffer. The result should be the original
	// bytes followed by the 6-byte header.
	prefix := []byte{0xCA, 0xFE}
	got := encryption.AppendHeaderAADBytes(prefix, encryption.EnvelopeVersionV1, encryption.FlagCompressed, 0x12345678)
	want := []byte{0xCA, 0xFE, 0x01, 0x01, 0x12, 0x34, 0x56, 0x78}
	if !bytes.Equal(got, want) {
		t.Fatalf("AppendHeaderAADBytes mismatch:\n  got  %x\n  want %x", got, want)
	}

	// With cap >= len(prefix)+HeaderAADSize, append should reuse the
	// backing array (no allocation). This is the storage-layer hot-path
	// expectation.
	buf := make([]byte, 0, 32)
	buf = append(buf, 0xAA, 0xBB)
	out := encryption.AppendHeaderAADBytes(buf, 0x01, 0x00, 0xDEADBEEF)
	if &out[0] != &buf[0] {
		t.Fatal("AppendHeaderAADBytes allocated when input had spare capacity")
	}
}

func TestEnvelope_Encode_RejectsBadVersion(t *testing.T) {
	env := encryption.Envelope{
		Version: 0x02, // not EnvelopeVersionV1
		Body:    bytes.Repeat([]byte{0xAA}, encryption.TagSize),
	}
	_, err := env.Encode()
	if !errors.Is(err, encryption.ErrEnvelopeVersion) {
		t.Fatalf("expected ErrEnvelopeVersion, got %v", err)
	}
}

func TestEnvelope_Encode_RejectsShortBody(t *testing.T) {
	for _, n := range []int{0, 1, encryption.TagSize - 1} {
		env := encryption.Envelope{
			Version: encryption.EnvelopeVersionV1,
			Body:    make([]byte, n),
		}
		_, err := env.Encode()
		if !errors.Is(err, encryption.ErrEnvelopeShort) {
			t.Fatalf("body=%d: expected ErrEnvelopeShort, got %v", n, err)
		}
	}
}
