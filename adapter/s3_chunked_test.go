package adapter

import (
	"bytes"
	"encoding/base64"
	"fmt"
	"hash/crc32"
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// encodeAwsChunked returns an aws-chunked framed body that carries the given
// decoded payload split into chunks of size chunkSize. If trailer is
// non-empty it is appended as a single chunk-extension trailer header.
func encodeAwsChunked(t *testing.T, payload []byte, chunkSize int, trailerName, trailerValue string) []byte {
	t.Helper()
	require.Positive(t, chunkSize)
	var buf bytes.Buffer
	for i := 0; i < len(payload); i += chunkSize {
		end := i + chunkSize
		if end > len(payload) {
			end = len(payload)
		}
		fmt.Fprintf(&buf, "%x\r\n", end-i)
		buf.Write(payload[i:end])
		buf.WriteString("\r\n")
	}
	buf.WriteString("0\r\n")
	if trailerName != "" {
		fmt.Fprintf(&buf, "%s: %s\r\n", trailerName, trailerValue)
	}
	buf.WriteString("\r\n")
	return buf.Bytes()
}

// encodeSignedAwsChunked produces a signed-chunk body. The fake chunk
// signature is never validated by the decoder (see security note in
// s3_chunked.go).
func encodeSignedAwsChunked(t *testing.T, payload []byte, chunkSize int) []byte {
	t.Helper()
	var buf bytes.Buffer
	for i := 0; i < len(payload); i += chunkSize {
		end := i + chunkSize
		if end > len(payload) {
			end = len(payload)
		}
		fmt.Fprintf(&buf, "%x;chunk-signature=deadbeef\r\n", end-i)
		buf.Write(payload[i:end])
		buf.WriteString("\r\n")
	}
	buf.WriteString("0;chunk-signature=cafed00d\r\n\r\n")
	return buf.Bytes()
}

func TestAwsChunkedReader_SingleChunk(t *testing.T) {
	payload := []byte("hello aws chunked")
	body := encodeAwsChunked(t, payload, len(payload), "", "")

	r := newAwsChunkedReader(bytes.NewReader(body), int64(len(payload)), 1<<20)
	got, err := io.ReadAll(r)
	require.NoError(t, err)
	require.Equal(t, payload, got)
}

func TestAwsChunkedReader_MultipleChunks(t *testing.T) {
	payload := bytes.Repeat([]byte("abcde"), 100) // 500 bytes
	body := encodeAwsChunked(t, payload, 37, "", "")

	r := newAwsChunkedReader(bytes.NewReader(body), int64(len(payload)), 1<<20)
	got, err := io.ReadAll(r)
	require.NoError(t, err)
	require.Equal(t, payload, got)
}

func TestAwsChunkedReader_SignedChunkExtensionsIgnored(t *testing.T) {
	payload := []byte("signed chunked body")
	body := encodeSignedAwsChunked(t, payload, 8)

	r := newAwsChunkedReader(bytes.NewReader(body), int64(len(payload)), 1<<20)
	got, err := io.ReadAll(r)
	require.NoError(t, err)
	require.Equal(t, payload, got)
}

func TestAwsChunkedReader_TrailerExposed(t *testing.T) {
	payload := []byte("with trailer")
	body := encodeAwsChunked(t, payload, 5, "x-amz-checksum-crc32", "Zm9vYmFyMA==")

	r := newAwsChunkedReader(bytes.NewReader(body), int64(len(payload)), 1<<20)
	_, err := io.ReadAll(r)
	require.NoError(t, err)
	require.Equal(t, "Zm9vYmFyMA==", r.Trailer()["X-Amz-Checksum-Crc32"])
}

func TestAwsChunkedReader_DecodedLengthMismatch(t *testing.T) {
	payload := []byte("exact length matters")
	body := encodeAwsChunked(t, payload, 7, "", "")
	// Declare a smaller length than the actual decoded body to ensure the
	// reader enforces equality and does not silently truncate.
	r := newAwsChunkedReader(bytes.NewReader(body), int64(len(payload)-1), 1<<20)
	_, err := io.ReadAll(r)
	require.Error(t, err)
	require.Contains(t, err.Error(), "X-Amz-Decoded-Content-Length")
}

func TestAwsChunkedReader_DecodedLengthShort(t *testing.T) {
	payload := []byte("shorter than declared")
	body := encodeAwsChunked(t, payload, 6, "", "")
	r := newAwsChunkedReader(bytes.NewReader(body), int64(len(payload)+5), 1<<20)
	_, err := io.ReadAll(r)
	require.Error(t, err)
	require.Contains(t, err.Error(), "X-Amz-Decoded-Content-Length")
}

func TestAwsChunkedReader_MaxDecodedEnforced(t *testing.T) {
	payload := bytes.Repeat([]byte("x"), 100)
	body := encodeAwsChunked(t, payload, 16, "", "")
	r := newAwsChunkedReader(bytes.NewReader(body), -1, 40)
	_, err := io.ReadAll(r)
	require.Error(t, err)
	require.Contains(t, err.Error(), "maximum allowed size")
}

func TestAwsChunkedReader_RejectsOversizedChunkHeader(t *testing.T) {
	oversized := fmt.Sprintf("%x\r\n", s3ChunkSizeMax+1)
	r := newAwsChunkedReader(strings.NewReader(oversized), -1, 1<<30)
	_, err := io.ReadAll(r)
	require.Error(t, err)
	require.Contains(t, err.Error(), "exceeds maximum")
}

func TestAwsChunkedReader_RejectsMalformedSizeLine(t *testing.T) {
	body := "zz\r\npayload\r\n0\r\n\r\n"
	r := newAwsChunkedReader(strings.NewReader(body), -1, 1<<20)
	_, err := io.ReadAll(r)
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid chunk size")
}

func TestAwsChunkedReader_RejectsMissingTrailingCRLF(t *testing.T) {
	// Chunk data without the terminating \r\n.
	body := "5\r\nhellomore"
	r := newAwsChunkedReader(strings.NewReader(body), -1, 1<<20)
	_, err := io.ReadAll(r)
	require.Error(t, err)
}

func TestAwsChunkedReader_TruncatedInput(t *testing.T) {
	// Declares 10 byte chunk but only provides 3 bytes before EOF.
	body := "a\r\nabc"
	r := newAwsChunkedReader(strings.NewReader(body), -1, 1<<20)
	_, err := io.ReadAll(r)
	require.Error(t, err)
}

// TestAwsChunkedReader_TruncatedAtChunkBoundary covers the scenario the
// gemini review flagged: the underlying reader hits EOF exactly at the end
// of a data chunk (before the terminating 0-chunk). Without the
// !r.finished guard the reader would happily return io.EOF and a caller
// like io.ReadAll would treat the upload as complete.
func TestAwsChunkedReader_TruncatedAtChunkBoundary(t *testing.T) {
	// Valid first chunk followed by its CRLF, but the terminator chunk
	// is missing.
	body := "5\r\nhello\r\n"
	r := newAwsChunkedReader(strings.NewReader(body), -1, 1<<20)
	_, err := io.ReadAll(r)
	require.Error(t, err, "must not silently accept truncation at a chunk boundary")
}

func TestAwsChunkedReader_OversizedLineRejectedWithoutUnboundedAlloc(t *testing.T) {
	// Single line that never terminates with \n. ReadSlice must surface
	// ErrBufferFull once the bufio buffer is saturated (8 KiB) instead of
	// growing to accommodate the attacker-controlled input.
	raw := strings.Repeat("A", s3ChunkedMaxLineLength*4)
	r := newAwsChunkedReader(strings.NewReader(raw), -1, 1<<20)
	_, err := io.ReadAll(r)
	require.Error(t, err)
	require.Contains(t, err.Error(), "exceeds")
}

func TestS3StreamingBody_VerifyTrailer_CRC32(t *testing.T) {
	payload := []byte("Mary had a little lamb")
	sum := crc32.NewIEEE()
	_, err := sum.Write(payload)
	require.NoError(t, err)
	expected := base64.StdEncoding.EncodeToString(sum.Sum(nil))

	body := encodeAwsChunked(t, payload, 5, "x-amz-checksum-crc32", expected)
	reader := newAwsChunkedReader(bytes.NewReader(body), int64(len(payload)), 1<<20)

	hasher := newS3TrailerChecksumHasher("x-amz-checksum-crc32")
	require.NotNil(t, hasher)
	sb := &s3StreamingBody{
		reader:      reader,
		trailerName: canonicalTrailerName("x-amz-checksum-crc32"),
		trailerHash: hasher,
	}
	got, err := io.ReadAll(reader)
	require.NoError(t, err)
	require.Equal(t, payload, got)
	sb.writeDecoded(got)
	require.NoError(t, sb.verifyTrailer())
}

func TestS3StreamingBody_VerifyTrailer_Mismatch(t *testing.T) {
	payload := []byte("Mary had a little lamb")
	wrong := base64.StdEncoding.EncodeToString([]byte{0xDE, 0xAD, 0xBE, 0xEF})
	body := encodeAwsChunked(t, payload, 5, "x-amz-checksum-crc32", wrong)
	reader := newAwsChunkedReader(bytes.NewReader(body), int64(len(payload)), 1<<20)
	hasher := newS3TrailerChecksumHasher("x-amz-checksum-crc32")
	sb := &s3StreamingBody{
		reader:      reader,
		trailerName: canonicalTrailerName("x-amz-checksum-crc32"),
		trailerHash: hasher,
	}
	got, err := io.ReadAll(reader)
	require.NoError(t, err)
	sb.writeDecoded(got)
	err = sb.verifyTrailer()
	require.Error(t, err)
	require.Contains(t, err.Error(), "checksum mismatch")
}

func TestS3StreamingBody_ZeroValueIsNoOp(t *testing.T) {
	var sb s3StreamingBody
	sb.writeDecoded([]byte("anything"))
	require.NoError(t, sb.verifyTrailer())
}

func TestIsS3PayloadMarker(t *testing.T) {
	cases := map[string]bool{
		"UNSIGNED-PAYLOAD":                               true,
		"unsigned-payload":                               true,
		"STREAMING-UNSIGNED-PAYLOAD-TRAILER":             true,
		"STREAMING-AWS4-HMAC-SHA256-PAYLOAD":             true,
		"STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER":     true,
		"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b93": false,
		"":                       false,
		"SOMETHING-ELSE-PAYLOAD": false,
	}
	for in, want := range cases {
		got := isS3PayloadMarker(in)
		require.Equal(t, want, got, "isS3PayloadMarker(%q)", in)
	}
}

func TestIsS3StreamingPayloadMarker(t *testing.T) {
	require.False(t, isS3StreamingPayloadMarker("UNSIGNED-PAYLOAD"))
	require.True(t, isS3StreamingPayloadMarker("STREAMING-UNSIGNED-PAYLOAD-TRAILER"))
	require.True(t, isS3StreamingPayloadMarker("streaming-aws4-hmac-sha256-payload"))
	require.False(t, isS3StreamingPayloadMarker(""))
}

func TestCleanStoredContentEncoding(t *testing.T) {
	cases := map[string]string{
		"":                      "",
		"aws-chunked":           "",
		"AWS-CHUNKED":           "",
		"gzip":                  "gzip",
		"aws-chunked, gzip":     "gzip",
		"gzip, aws-chunked":     "gzip",
		"gzip,  aws-chunked ":   "gzip",
		"identity":              "identity",
		"aws-chunked, gzip, br": "gzip, br",
	}
	for in, want := range cases {
		require.Equal(t, want, cleanStoredContentEncoding(in), "input %q", in)
	}
}
