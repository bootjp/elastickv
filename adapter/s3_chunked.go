package adapter

import (
	"bufio"
	"encoding/base64"
	"hash"
	"hash/crc32"
	"io"
	"net/textproto"
	"strconv"
	"strings"

	"github.com/cockroachdb/errors"
)

// Limits applied to aws-chunked framed request bodies. A single chunk must
// fit within s3ChunkSizeMax so a malicious client cannot force the server to
// allocate an unbounded buffer based on the attacker-supplied hex size line.
// The cap is generous relative to AWS SDK defaults (128 KiB - 1 MiB) so it
// only triggers on clearly pathological inputs.
const (
	s3ChunkSizeMax     = 16 * 1024 * 1024 // 16 MiB
	s3MaxTrailerLength = 4 * 1024         // 4 KiB total across trailer headers
)

// awsChunkedError tags failures that originate from aws-chunked framing or
// from enforcing the declared decoded length. Handlers unwrap to this type
// to distinguish malformed-input failures (400 InvalidRequest) from generic
// I/O errors (500 InternalError).
type awsChunkedError struct{ err error }

func (e *awsChunkedError) Error() string { return e.err.Error() }
func (e *awsChunkedError) Unwrap() error { return e.err }

func newAwsChunkedError(err error) error {
	if err == nil {
		return nil
	}
	return &awsChunkedError{err: err}
}

// awsChunkedReader decodes an aws-chunked framed body into the underlying
// decoded bytes. The wire format is:
//
//	<hex-chunk-size>[;chunk-signature=<hex>]\r\n
//	<chunk-data>\r\n
//	... (repeat) ...
//	0\r\n
//	<trailer-header>: <value>\r\n   (optional, zero or more)
//	\r\n
//
// The decoder tolerates both the unsigned and signed chunk forms because the
// signature parameter is parsed and discarded (see isS3StreamingPayloadMarker
// and the security note in s3_chunked_test.go for the rationale). It also
// records trailer headers the caller may verify (currently used for the
// x-amz-checksum-* trailers so a CRC mismatch surfaces as 400 to the client
// instead of silent corruption).
type awsChunkedReader struct {
	br              *bufio.Reader
	cur             int64 // bytes remaining in the current chunk
	finished        bool
	totalDecoded    int64
	maxDecoded      int64 // decoded-byte ceiling; 0 means unlimited
	declaredDecoded int64 // X-Amz-Decoded-Content-Length; -1 means unknown
	trailers        map[string]string
}

func newAwsChunkedReader(r io.Reader, declaredDecoded, maxDecoded int64) *awsChunkedReader {
	return &awsChunkedReader{
		br:              bufio.NewReader(r),
		cur:             0,
		declaredDecoded: declaredDecoded,
		maxDecoded:      maxDecoded,
		trailers:        map[string]string{},
	}
}

// Trailer returns the trailer headers collected after the terminating
// 0-length chunk. The map key is canonical MIME form (e.g.
// "X-Amz-Checksum-Crc32"). Only valid after Read has returned io.EOF.
func (r *awsChunkedReader) Trailer() map[string]string {
	out := make(map[string]string, len(r.trailers))
	for k, v := range r.trailers {
		out[k] = v
	}
	return out
}

//nolint:cyclop // aws-chunked decoder branches on chunk boundaries, framing errors, and decoded-length limits; splitting further hurts readability.
func (r *awsChunkedReader) Read(p []byte) (int, error) {
	if r.finished {
		return 0, io.EOF
	}
	if len(p) == 0 {
		return 0, nil
	}

	if r.cur == 0 {
		if err := r.readChunkHeader(); err != nil {
			return 0, newAwsChunkedError(err)
		}
		if r.finished {
			return 0, io.EOF
		}
	}

	want := int64(len(p))
	if want > r.cur {
		want = r.cur
	}
	n, err := r.br.Read(p[:want])
	if n > 0 {
		if sizeErr := r.accountDecoded(int64(n)); sizeErr != nil {
			return n, newAwsChunkedError(sizeErr)
		}
	}
	// The underlying reader ran out of bytes before the chunk completed.
	// Treat that as a malformed stream rather than a clean EOF so the caller
	// does not silently store a truncated object.
	if errors.Is(err, io.EOF) && r.cur > 0 {
		return n, newAwsChunkedError(io.ErrUnexpectedEOF)
	}
	if r.cur == 0 && err == nil {
		// Drain the trailing CRLF that follows chunk data.
		if err = r.consumeCRLF(); err != nil {
			return n, newAwsChunkedError(err)
		}
	}
	return n, err //nolint:wrapcheck // propagate underlying reader error verbatim
}

func (r *awsChunkedReader) accountDecoded(n int64) error {
	r.cur -= n
	r.totalDecoded += n
	if r.maxDecoded > 0 && r.totalDecoded > r.maxDecoded {
		return errors.New("aws-chunked: decoded body exceeded maximum allowed size")
	}
	if r.declaredDecoded >= 0 && r.totalDecoded > r.declaredDecoded {
		return errors.New("aws-chunked: decoded bytes exceed X-Amz-Decoded-Content-Length")
	}
	return nil
}

func (r *awsChunkedReader) readChunkHeader() error {
	line, err := r.readLine()
	if err != nil {
		return errors.Wrap(err, "aws-chunked: read chunk header")
	}
	sizeStr := line
	if idx := strings.IndexByte(line, ';'); idx >= 0 {
		// Discard `;chunk-signature=<hex>` (and any other chunk extensions).
		// TODO: optionally verify chunk-signature when we implement
		// signed-chunk support.
		sizeStr = line[:idx]
	}
	sizeStr = strings.TrimSpace(sizeStr)
	size, err := strconv.ParseInt(sizeStr, 16, 64)
	if err != nil {
		return errors.Wrapf(err, "aws-chunked: invalid chunk size %q", sizeStr)
	}
	if size < 0 {
		return errors.Newf("aws-chunked: negative chunk size %d", size) //nolint:wrapcheck // creating new error, nothing to wrap
	}
	if size > s3ChunkSizeMax {
		return errors.Newf("aws-chunked: chunk size %d exceeds maximum %d", size, s3ChunkSizeMax) //nolint:wrapcheck // creating new error, nothing to wrap
	}
	if size == 0 {
		if err := r.readTrailer(); err != nil {
			return err
		}
		r.finished = true
		if r.declaredDecoded >= 0 && r.totalDecoded != r.declaredDecoded {
			return errors.Newf( //nolint:wrapcheck // creating new error, nothing to wrap
				"aws-chunked: decoded %d bytes but X-Amz-Decoded-Content-Length was %d",
				r.totalDecoded, r.declaredDecoded,
			)
		}
		return nil
	}
	r.cur = size
	return nil
}

func (r *awsChunkedReader) readTrailer() error {
	total := 0
	for {
		line, err := r.readLine()
		if err != nil {
			return errors.Wrap(err, "aws-chunked: read trailer")
		}
		if line == "" {
			return nil
		}
		total += len(line)
		if total > s3MaxTrailerLength {
			return errors.New("aws-chunked: trailer headers exceed maximum length")
		}
		key, value, ok := strings.Cut(line, ":")
		if !ok {
			return errors.Newf("aws-chunked: malformed trailer line %q", line) //nolint:wrapcheck // creating new error, nothing to wrap
		}
		r.trailers[textproto.CanonicalMIMEHeaderKey(strings.TrimSpace(key))] = strings.TrimSpace(value)
	}
}

// readLine reads one CRLF-terminated line (without the CRLF) while bounding
// the line length so a malicious peer cannot force us to read an unbounded
// buffer.
func (r *awsChunkedReader) readLine() (string, error) {
	const maxLine = 8 * 1024
	line, err := r.br.ReadString('\n')
	if err != nil {
		return "", errors.Wrap(err, "aws-chunked: read line")
	}
	if len(line) > maxLine {
		return "", errors.Newf("aws-chunked: header line exceeds %d bytes", maxLine) //nolint:wrapcheck // creating new error, nothing to wrap
	}
	return strings.TrimRight(line, "\r\n"), nil
}

func (r *awsChunkedReader) consumeCRLF() error {
	var buf [2]byte
	if _, err := io.ReadFull(r.br, buf[:]); err != nil {
		return errors.Wrap(err, "aws-chunked: read chunk trailing CRLF")
	}
	if buf != [2]byte{'\r', '\n'} {
		return errors.Newf("aws-chunked: chunk not terminated by CRLF (got %q)", string(buf[:])) //nolint:wrapcheck // creating new error, nothing to wrap
	}
	return nil
}

// newS3TrailerChecksumHasher returns a hasher matching the given trailer
// name (e.g. "x-amz-checksum-crc32"). It returns nil when the algorithm is
// unsupported; callers then skip validation. Trailer values are base64.
func newS3TrailerChecksumHasher(name string) hash.Hash {
	switch textproto.CanonicalMIMEHeaderKey(strings.TrimSpace(name)) {
	case "X-Amz-Checksum-Crc32":
		return crc32.NewIEEE()
	case "X-Amz-Checksum-Crc32c":
		return crc32.New(crc32.MakeTable(crc32.Castagnoli))
	}
	return nil
}

func decodeS3TrailerChecksum(value string) ([]byte, error) {
	decoded, err := base64.StdEncoding.DecodeString(strings.TrimSpace(value))
	if err != nil {
		return nil, errors.Wrap(err, "aws-chunked: decode trailer base64")
	}
	return decoded, nil
}

// s3StreamingBody is the mutable state a streaming PUT needs to keep around
// while it drains the request body: the wrapped reader (for trailer
// extraction after EOF), an optional running checksum of the decoded bytes
// for the trailer the client promised, and the canonical trailer header
// name. When the request is not streaming, all fields are zero-valued and
// the helpers below are no-ops.
type s3StreamingBody struct {
	reader      *awsChunkedReader
	trailerName string // canonical MIME form, e.g. "X-Amz-Checksum-Crc32"
	trailerHash hash.Hash
}

// writeDecoded feeds each decoded chunk through the optional trailer hasher.
// Safe to call when s is the zero value.
func (s *s3StreamingBody) writeDecoded(p []byte) {
	if s == nil || s.trailerHash == nil {
		return
	}
	_, _ = s.trailerHash.Write(p)
}

// verifyTrailer must be called after the body has been read to EOF. It
// returns a non-nil error when the client advertised a trailer checksum via
// X-Amz-Trailer and the value that arrived in the chunked trailer does not
// match the bytes we received. Returns nil for non-streaming requests or
// when the client did not advertise a supported checksum.
func (s *s3StreamingBody) verifyTrailer() error {
	if s == nil || s.trailerName == "" || s.trailerHash == nil || s.reader == nil {
		return nil
	}
	received, ok := s.reader.Trailer()[s.trailerName]
	if !ok {
		return errors.Newf("aws-chunked: advertised trailer %q missing", s.trailerName) //nolint:wrapcheck // creating new error, nothing to wrap
	}
	expected, err := decodeS3TrailerChecksum(received)
	if err != nil {
		return errors.Wrapf(err, "aws-chunked: decode trailer %q", s.trailerName)
	}
	computed := s.trailerHash.Sum(nil)
	if !hashesEqual(expected, computed) {
		return errors.Newf("aws-chunked: trailer %q checksum mismatch", s.trailerName) //nolint:wrapcheck // creating new error, nothing to wrap
	}
	return nil
}

func hashesEqual(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
