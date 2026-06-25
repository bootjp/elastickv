package adapter

import (
	"encoding/base64"
	"encoding/hex"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestEncodeReceiptHandleV2_RoundTrip pins the v2 codec contract:
// every (partition, queue_gen, message_id, receipt_token) tuple
// round-trips through encodeReceiptHandleV2 → decodeReceiptHandle
// without loss, and the decoded Version is sqsReceiptHandleVersion2
// while Partition matches the encoder's input.
func TestEncodeReceiptHandleV2_RoundTrip(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name      string
		partition uint32
		gen       uint64
		id        string
	}{
		{"partition 0 / gen 1", 0, 1, "00000000000000000000000000000000"},
		{"partition 7 / large gen", 7, 0xFFFFFFFFFFFF, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},
		{"partition 31 / typical id", 31, 42, "deadbeefdeadbeefdeadbeefdeadbeef"},
		// PartitionCount cap is 32 per §3.1, so the largest valid
		// partition index is 31. The codec itself is uint32 wide
		// so an out-of-cap value still encodes — caller validation
		// is responsible for cap enforcement.
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			token := make([]byte, sqsReceiptTokenBytes)
			for i := range token {
				token[i] = byte(i + int(tc.partition))
			}
			h, err := encodeReceiptHandleV2(tc.partition, tc.gen, tc.id, token)
			require.NoError(t, err)

			back, err := decodeReceiptHandle(h)
			require.NoError(t, err)
			require.Equal(t, sqsReceiptHandleVersion2, back.Version,
				"Version must report v2 so callers can dispatch on it")
			require.Equal(t, tc.partition, back.Partition)
			require.Equal(t, tc.gen, back.QueueGeneration)
			require.Equal(t, tc.id, back.MessageIDHex)
			require.Equal(t, hex.EncodeToString(token),
				hex.EncodeToString(back.ReceiptToken))
		})
	}
}

// TestEncodeReceiptHandleV1_StillReportsV1 pins the v1 round-trip:
// a v1-encoded handle decodes with Version=v1, Partition=0. This
// guards against a refactor that accidentally upgrades v1
// encoders to v2 (or vice versa) — the wire-format compatibility
// is the operator's contract.
func TestEncodeReceiptHandleV1_StillReportsV1(t *testing.T) {
	t.Parallel()
	token := make([]byte, sqsReceiptTokenBytes)
	for i := range token {
		token[i] = byte(0xAA)
	}
	h, err := encodeReceiptHandle(99, "deadbeefdeadbeefdeadbeefdeadbeef", token)
	require.NoError(t, err)

	back, err := decodeReceiptHandle(h)
	require.NoError(t, err)
	require.Equal(t, sqsReceiptHandleVersion1, back.Version,
		"v1 encoder must produce v1-decodable handle")
	require.Equal(t, uint32(0), back.Partition,
		"v1 handle has no partition field; Partition must be 0")
	require.Equal(t, uint64(99), back.QueueGeneration)
	require.Equal(t, "deadbeefdeadbeefdeadbeefdeadbeef", back.MessageIDHex)
}

// TestDecodeReceiptHandle_VersionDispatch pins that decode picks
// the right layout based on the version byte. Critical because
// v1 and v2 have different sizes (41 vs 45 bytes); reading a v2
// blob with v1 offsets would silently misinterpret the queue_gen
// field.
func TestDecodeReceiptHandle_VersionDispatch(t *testing.T) {
	t.Parallel()
	token := make([]byte, sqsReceiptTokenBytes)
	id := "0123456789abcdef0123456789abcdef"

	v1, err := encodeReceiptHandle(7, id, token)
	require.NoError(t, err)
	v2, err := encodeReceiptHandleV2(3, 7, id, token)
	require.NoError(t, err)

	// v1 and v2 must produce distinct on-wire bytes — version
	// byte differs AND v2 carries 4 extra bytes for the partition.
	v1Raw, err := base64.RawURLEncoding.DecodeString(v1)
	require.NoError(t, err)
	v2Raw, err := base64.RawURLEncoding.DecodeString(v2)
	require.NoError(t, err)
	require.Equal(t, sqsReceiptHandleV1Size, len(v1Raw))
	require.Equal(t, sqsReceiptHandleV2Size, len(v2Raw))
	require.Equal(t, sqsReceiptHandleVersion1, v1Raw[0])
	require.Equal(t, sqsReceiptHandleVersion2, v2Raw[0])

	// Decode dispatches by version: same queue_gen=7, same id,
	// same token, distinct Version + Partition.
	v1Back, err := decodeReceiptHandle(v1)
	require.NoError(t, err)
	v2Back, err := decodeReceiptHandle(v2)
	require.NoError(t, err)
	require.Equal(t, sqsReceiptHandleVersion1, v1Back.Version)
	require.Equal(t, uint32(0), v1Back.Partition)
	require.Equal(t, sqsReceiptHandleVersion2, v2Back.Version)
	require.Equal(t, uint32(3), v2Back.Partition)
	require.Equal(t, v1Back.QueueGeneration, v2Back.QueueGeneration)
	require.Equal(t, v1Back.MessageIDHex, v2Back.MessageIDHex)
}

// TestDecodeReceiptHandle_RejectsLengthMismatch pins that a blob
// with a known version byte but wrong length fails decode. The
// failure mode the test would catch: a refactor that adds /
// removes a field but forgets to bump the version byte.
func TestDecodeReceiptHandle_RejectsLengthMismatch(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		make func() []byte
	}{
		{
			name: "v1 byte but v2 length",
			make: func() []byte {
				out := make([]byte, sqsReceiptHandleV2Size)
				out[0] = sqsReceiptHandleVersion1
				return out
			},
		},
		{
			name: "v2 byte but v1 length",
			make: func() []byte {
				out := make([]byte, sqsReceiptHandleV1Size)
				out[0] = sqsReceiptHandleVersion2
				return out
			},
		},
		{
			name: "v1 truncated",
			make: func() []byte {
				out := make([]byte, sqsReceiptHandleV1Size-1)
				out[0] = sqsReceiptHandleVersion1
				return out
			},
		},
		{
			name: "v2 truncated",
			make: func() []byte {
				out := make([]byte, sqsReceiptHandleV2Size-1)
				out[0] = sqsReceiptHandleVersion2
				return out
			},
		},
		{
			name: "v2 oversized",
			make: func() []byte {
				out := make([]byte, sqsReceiptHandleV2Size+1)
				out[0] = sqsReceiptHandleVersion2
				return out
			},
		},
		{
			name: "v1 oversized",
			make: func() []byte {
				out := make([]byte, sqsReceiptHandleV1Size+1)
				out[0] = sqsReceiptHandleVersion1
				return out
			},
		},
		{
			name: "empty",
			make: func() []byte { return nil },
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := base64.RawURLEncoding.EncodeToString(tc.make())
			_, err := decodeReceiptHandle(h)
			require.Error(t, err)
			require.Contains(t, err.Error(), "length or version mismatch")
		})
	}
}

// TestDecodeReceiptHandle_RejectsUnknownVersion pins that an
// unknown version byte fails decode rather than falling through
// to one of the known layouts.
func TestDecodeReceiptHandle_RejectsUnknownVersion(t *testing.T) {
	t.Parallel()
	for _, version := range []byte{0x00, 0x03, 0x42, 0xFF} {
		t.Run("version 0x"+hex.EncodeToString([]byte{version}), func(t *testing.T) {
			t.Parallel()
			out := make([]byte, sqsReceiptHandleV1Size)
			out[0] = version
			h := base64.RawURLEncoding.EncodeToString(out)
			_, err := decodeReceiptHandle(h)
			require.Error(t, err,
				"unknown version 0x%02x must fail decode", version)
		})
	}
}

// TestEncodeReceiptHandleV2_RejectsBadInputs pins the encoder's
// input validation: a token of the wrong length, or a hex id
// that doesn't decode to 16 bytes, surfaces as an error rather
// than producing a malformed handle.
func TestEncodeReceiptHandleV2_RejectsBadInputs(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name  string
		token []byte
		id    string
	}{
		{"short token", make([]byte, sqsReceiptTokenBytes-1), "deadbeefdeadbeefdeadbeefdeadbeef"},
		{"long token", make([]byte, sqsReceiptTokenBytes+1), "deadbeefdeadbeefdeadbeefdeadbeef"},
		{"non-hex id", make([]byte, sqsReceiptTokenBytes), "not-hex-not-hex-not-hex-not-hex0"},
		{"short id", make([]byte, sqsReceiptTokenBytes), "deadbeef"},
		{"empty id", make([]byte, sqsReceiptTokenBytes), ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, err := encodeReceiptHandleV2(0, 1, tc.id, tc.token)
			require.Error(t, err)
		})
	}
}

// TestReceiptHandleVersionConstants_Distinct pins the assertion
// that v1 and v2 version bytes differ. A refactor that
// accidentally collapses them (e.g. both → 0x01) would cause v2
// handles to decode as v1 with garbled data.
func TestReceiptHandleVersionConstants_Distinct(t *testing.T) {
	t.Parallel()
	require.NotEqual(t, sqsReceiptHandleVersion1, sqsReceiptHandleVersion2,
		"v1 and v2 version bytes must differ for the dispatch to work")
	require.Equal(t, byte(0x01), sqsReceiptHandleVersion1)
	require.Equal(t, byte(0x02), sqsReceiptHandleVersion2)
	// On-wire size constants must equal what the encoders write —
	// pinning them keeps a future struct change from silently
	// changing the wire format.
	require.Equal(t, 1+8+sqsMessageIDBytes+sqsReceiptTokenBytes,
		sqsReceiptHandleV1Size)
	require.Equal(t, 1+4+8+sqsMessageIDBytes+sqsReceiptTokenBytes,
		sqsReceiptHandleV2Size)
}

// TestDecodeReceiptHandle_RejectsBase64Garbage pins that
// non-base64 input fails decode at the base64 step rather than
// the version-byte step. The error wrap chain matters for
// operators triaging client-side encoding bugs.
func TestDecodeReceiptHandle_RejectsBase64Garbage(t *testing.T) {
	t.Parallel()
	_, err := decodeReceiptHandle("!!!" + strings.Repeat("?", 50))
	require.Error(t, err,
		"non-base64 input must fail at the base64 decode step")
}

// TestDecodeClientReceiptHandle_AcceptsV1 pins the public-API
// wrapper's happy path — v1 handles flow through unchanged.
func TestDecodeClientReceiptHandle_AcceptsV1(t *testing.T) {
	t.Parallel()
	token := make([]byte, sqsReceiptTokenBytes)
	h, err := encodeReceiptHandle(7, "deadbeefdeadbeefdeadbeefdeadbeef", token)
	require.NoError(t, err)
	back, err := decodeClientReceiptHandle(h)
	require.NoError(t, err)
	require.Equal(t, sqsReceiptHandleVersion1, back.Version)
	require.Equal(t, uint64(7), back.QueueGeneration)
}

// TestDecodeClientReceiptHandle_AcceptsV2 pins the PR 5b-2 contract
// shift: the public API wrapper no longer enforces the PR 5a
// blanket v2 rejection. Version validation moved into
// validateReceiptHandleVersion, which the meta-loading callers
// (loadMessageForDelete / loadAndVerifyMessage) invoke once they
// have the queue's PartitionCount in scope. The dormancy promise
// is preserved by the queue-aware check downstream — v2 handles
// against a non-partitioned queue still surface as
// ReceiptHandleIsInvalid (see
// TestValidateReceiptHandleVersion_RejectsV2OnNonPartitioned),
// and the §11 PR 2 dormancy gate still rejects PartitionCount > 1
// at CreateQueue, so no production queue can be in the
// partitioned branch until PR 5b-3 lifts the gate atomically.
func TestDecodeClientReceiptHandle_AcceptsV2(t *testing.T) {
	t.Parallel()
	token := make([]byte, sqsReceiptTokenBytes)
	h, err := encodeReceiptHandleV2(3, 7, "deadbeefdeadbeefdeadbeefdeadbeef", token)
	require.NoError(t, err)

	back, err := decodeClientReceiptHandle(h)
	require.NoError(t, err,
		"v2 handle must decode at the public API; version "+
			"validation moved to validateReceiptHandleVersion")
	require.Equal(t, sqsReceiptHandleVersion2, back.Version)
	require.Equal(t, uint32(3), back.Partition)
	require.Equal(t, uint64(7), back.QueueGeneration)
}

// TestDecodeClientReceiptHandle_PassesThroughDecodeErrors pins
// that decode-error propagation is unchanged — a malformed blob
// still surfaces the underlying base64 / length error.
func TestDecodeClientReceiptHandle_PassesThroughDecodeErrors(t *testing.T) {
	t.Parallel()
	_, err := decodeClientReceiptHandle("!!!" + strings.Repeat("?", 50))
	require.Error(t, err)
}
