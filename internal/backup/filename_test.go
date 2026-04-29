package backup

import (
	"crypto/rand"
	"strings"
	"testing"

	"github.com/cockroachdb/errors"
	"pgregory.net/rapid"
)

func TestEncodeSegment_PassthroughForUnreserved(t *testing.T) {
	t.Parallel()
	// Every unreserved byte must round-trip without escaping.
	cases := []string{
		"",
		"a",
		"A",
		"0",
		"-",
		".",
		"_",
		"abc",
		"ABCdef-123_test.json",
		"customer-7421",
		"2026-04-29T12-00-00Z",
	}
	for _, c := range cases {
		t.Run(c, func(t *testing.T) {
			t.Parallel()
			enc := EncodeSegment([]byte(c))
			if enc != c {
				t.Fatalf("EncodeSegment(%q) = %q, want %q", c, enc, c)
			}
			dec, err := DecodeSegment(enc)
			if err != nil {
				t.Fatalf("DecodeSegment(%q) error: %v", enc, err)
			}
			if string(dec) != c {
				t.Fatalf("round-trip: got %q, want %q", dec, c)
			}
		})
	}
}

func TestEncodeSegment_PercentEscapesReservedBytes(t *testing.T) {
	t.Parallel()
	cases := map[string]string{
		"hello world":   "hello%20world",
		"a/b":           "a%2Fb",
		"a:b":           "a%3Ab",
		"key|with|pipe": "key%7Cwith%7Cpipe",
		"\x00":          "%00",
		"\xff":          "%FF",
		"colon:.":       "colon%3A.",
		"plus+":         "plus%2B",
	}
	for raw, want := range cases {
		t.Run(want, func(t *testing.T) {
			t.Parallel()
			got := EncodeSegment([]byte(raw))
			if got != want {
				t.Fatalf("EncodeSegment(%q) = %q, want %q", raw, got, want)
			}
			dec, err := DecodeSegment(got)
			if err != nil {
				t.Fatalf("DecodeSegment(%q) error: %v", got, err)
			}
			if string(dec) != raw {
				t.Fatalf("round-trip: got %q, want %q", dec, raw)
			}
		})
	}
}

func TestEncodeSegment_HexIsUppercase(t *testing.T) {
	t.Parallel()
	enc := EncodeSegment([]byte{0xab, 0xcd})
	if enc != "%AB%CD" {
		t.Fatalf("EncodeSegment hex case: got %q want %q", enc, "%AB%CD")
	}
}

func TestEncodeSegment_LongInputTakesShaFallback(t *testing.T) {
	t.Parallel()
	// 250 bytes of unreserved chars: percent-encoded length == 250, which
	// exceeds the 240-byte ceiling, so the SHA fallback fires.
	raw := strings.Repeat("a", 250)
	enc := EncodeSegment([]byte(raw))
	if !IsShaFallback(enc) {
		t.Fatalf("EncodeSegment(250 unreserved bytes) did not take SHA fallback: %q", enc)
	}
	if len(enc) > maxSegmentBytes {
		t.Fatalf("SHA-fallback output exceeds max: len=%d > %d", len(enc), maxSegmentBytes)
	}
	// Decoder reports the fallback and refuses to fabricate the original.
	if _, err := DecodeSegment(enc); !errors.Is(err, ErrShaFallbackNeedsKeymap) {
		t.Fatalf("DecodeSegment of SHA-fallback: err=%v want ErrShaFallbackNeedsKeymap", err)
	}
}

func TestEncodeSegment_ShortBytesThatExpandPastCeilingTakeShaFallback(t *testing.T) {
	t.Parallel()
	// Each byte percent-encodes to 3 chars, so 81 reserved bytes -> 243 chars.
	raw := strings.Repeat("\x01", 81)
	enc := EncodeSegment([]byte(raw))
	if !IsShaFallback(enc) {
		t.Fatalf("expected SHA fallback for 81 reserved bytes (243 expanded), got %q", enc)
	}
}

func TestEncodeSegment_Deterministic(t *testing.T) {
	t.Parallel()
	// Same input must encode to the same output across calls.
	raw := []byte("session:abc:123/4")
	a := EncodeSegment(raw)
	b := EncodeSegment(raw)
	if a != b {
		t.Fatalf("non-deterministic: %q != %q", a, b)
	}
	// Note: EncodeSegment is intentionally NOT idempotent — `%` is a reserved
	// byte and a second pass percent-encodes it again. Decode-then-encode is
	// the round-trip that holds, and is covered by other tests.
}

func TestEncodeBinarySegment_BasicRoundTrip(t *testing.T) {
	t.Parallel()
	cases := [][]byte{
		nil,
		{},
		{0x00},
		{0x01, 0x02, 0x03},
		[]byte("not-binary-but-still-a-byte-string"),
		{0xff, 0xfe, 0xfd, 0xfc},
	}
	for _, raw := range cases {
		enc := EncodeBinarySegment(raw)
		if !strings.HasPrefix(enc, binaryPrefix) {
			t.Fatalf("EncodeBinarySegment(%x) = %q, missing binary prefix", raw, enc)
		}
		if !IsBinarySegment(enc) {
			t.Fatalf("IsBinarySegment(%q) = false", enc)
		}
		dec, err := DecodeSegment(enc)
		if err != nil {
			t.Fatalf("DecodeSegment(%q) error: %v", enc, err)
		}
		if string(dec) != string(raw) {
			t.Fatalf("binary round-trip: got %x want %x", dec, raw)
		}
	}
}

func TestEncodeBinarySegment_LongInputTakesShaFallback(t *testing.T) {
	t.Parallel()
	// base64 ~= 4/3 the raw length; raw=200 -> ~268 chars after b64 prefix.
	raw := make([]byte, 200)
	if _, err := rand.Read(raw); err != nil {
		t.Fatalf("rand: %v", err)
	}
	enc := EncodeBinarySegment(raw)
	if !IsShaFallback(enc) {
		t.Fatalf("expected SHA fallback for 200-byte binary, got %q (len %d)", enc, len(enc))
	}
}

func TestEncodeSegment_ShaFallbackPrefixCannotCollideWithEncodedHex(t *testing.T) {
	t.Parallel()
	// A user key consisting solely of unreserved hex chars and underscores
	// must NOT be detected as a SHA fallback: the SHA fallback requires the
	// 32-hex prefix to be followed by exactly "__" — the user input
	// "abcdef...__rest" with shorter prefix or wrong separator length must
	// fall through.
	cases := []string{
		// 31 hex + double-underscore: too short by one
		"0123456789abcdef0123456789abcde__",
		// 32 hex + single underscore
		"0123456789abcdef0123456789abcdef_",
		// Correct prefix length but trailing single underscore: 33-char prefix,
		// the "__" check at offset 32 is "_X", not "__".
		"0123456789abcdef0123456789abcdef_x",
	}
	for _, c := range cases {
		if IsShaFallback(c) {
			t.Fatalf("false positive: IsShaFallback(%q) = true", c)
		}
	}
}

func TestDecodeSegment_RejectsTruncatedPercentEscape(t *testing.T) {
	t.Parallel()
	cases := []string{
		"%",
		"%1",
		"abc%",
		"abc%2",
	}
	for _, c := range cases {
		if _, err := DecodeSegment(c); !errors.Is(err, ErrInvalidEncodedSegment) {
			t.Fatalf("DecodeSegment(%q): err=%v want ErrInvalidEncodedSegment", c, err)
		}
	}
}

func TestDecodeSegment_RejectsNonHexInPercentEscape(t *testing.T) {
	t.Parallel()
	cases := []string{
		"%GG",
		"%1G",
		"%G1",
		"foo%XYbar",
	}
	for _, c := range cases {
		if _, err := DecodeSegment(c); !errors.Is(err, ErrInvalidEncodedSegment) {
			t.Fatalf("DecodeSegment(%q): err=%v want ErrInvalidEncodedSegment", c, err)
		}
	}
}

func TestDecodeSegment_RejectsRawReservedBytes(t *testing.T) {
	t.Parallel()
	// A literal `/` or other reserved byte in an encoded segment is invalid;
	// percent-encoded segments must contain only [unreserved-set | "%HH"].
	cases := []string{
		"a/b",
		"a:b",
		"hello world",
	}
	for _, c := range cases {
		if _, err := DecodeSegment(c); !errors.Is(err, ErrInvalidEncodedSegment) {
			t.Fatalf("DecodeSegment(%q): err=%v want ErrInvalidEncodedSegment", c, err)
		}
	}
}

func TestDecodeSegment_RejectsMalformedBinary(t *testing.T) {
	t.Parallel()
	cases := []string{
		"b64.!!!",      // "!" is not a base64url alphabet character
		"b64.padding=", // RawURLEncoding does not accept padding
	}
	for _, c := range cases {
		if _, err := DecodeSegment(c); !errors.Is(err, ErrInvalidEncodedSegment) {
			t.Fatalf("DecodeSegment(%q): err=%v want ErrInvalidEncodedSegment", c, err)
		}
	}
}

func TestEncodeSegment_OutputLengthBoundedByMax(t *testing.T) {
	t.Parallel()
	// For any input — including pathological ones that expand 3x under
	// percent-encoding — the encoded output never exceeds maxSegmentBytes.
	for _, n := range []int{0, 1, 240, 241, 1000, 65536} {
		raw := make([]byte, n)
		if _, err := rand.Read(raw); err != nil {
			t.Fatalf("rand: %v", err)
		}
		enc := EncodeSegment(raw)
		if len(enc) > maxSegmentBytes {
			t.Fatalf("EncodeSegment(len=%d): output len=%d > max=%d", n, len(enc), maxSegmentBytes)
		}
	}
}

func TestEncodeSegment_FuzzRoundTripIfNotShaFallback(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(t *rapid.T) {
		raw := rapid.SliceOfN(rapid.Byte(), 0, 80).Draw(t, "raw")
		enc := EncodeSegment(raw)
		if IsShaFallback(enc) {
			// Only assert the documented post-condition for fallback inputs:
			// decode must refuse rather than fabricate.
			if _, err := DecodeSegment(enc); !errors.Is(err, ErrShaFallbackNeedsKeymap) {
				t.Fatalf("SHA-fallback decode did not return ErrShaFallbackNeedsKeymap: %v", err)
			}
			return
		}
		dec, err := DecodeSegment(enc)
		if err != nil {
			t.Fatalf("DecodeSegment(%q) error: %v (raw=%x)", enc, err, raw)
		}
		if string(dec) != string(raw) {
			t.Fatalf("round-trip mismatch: raw=%x dec=%x enc=%q", raw, dec, enc)
		}
	})
}

func TestEncodeBinarySegment_FuzzRoundTripIfNotShaFallback(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(t *rapid.T) {
		raw := rapid.SliceOfN(rapid.Byte(), 0, 150).Draw(t, "raw")
		enc := EncodeBinarySegment(raw)
		if IsShaFallback(enc) {
			if _, err := DecodeSegment(enc); !errors.Is(err, ErrShaFallbackNeedsKeymap) {
				t.Fatalf("SHA-fallback decode did not return ErrShaFallbackNeedsKeymap: %v", err)
			}
			return
		}
		if !IsBinarySegment(enc) {
			t.Fatalf("non-fallback binary segment missing prefix: %q", enc)
		}
		dec, err := DecodeSegment(enc)
		if err != nil {
			t.Fatalf("DecodeSegment(%q) error: %v (raw=%x)", enc, err, raw)
		}
		if string(dec) != string(raw) {
			t.Fatalf("binary round-trip mismatch: raw=%x dec=%x enc=%q", raw, dec, enc)
		}
	})
}

func TestEncodeSegment_ShaFallbackEmbedsRecognisableSuffix(t *testing.T) {
	t.Parallel()
	// The truncated suffix in the SHA-fallback rendering must be derivable
	// from the original key, so an operator can grep the file tree for a
	// known-prefix key. Use an all-letter prefix so percent-encoding leaves
	// it intact (otherwise the suffix is itself percent-encoded).
	prefix := "human-recognisable-prefix"
	raw := []byte(prefix + strings.Repeat("a", 300))
	enc := EncodeSegment(raw)
	if !IsShaFallback(enc) {
		t.Fatalf("expected SHA fallback for 325-byte input")
	}
	if !strings.Contains(enc, prefix) {
		t.Fatalf("SHA fallback %q does not contain %q", enc, prefix)
	}
}
