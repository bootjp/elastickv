package backup

import (
	"bufio"
	"encoding/base64"
	"encoding/json"
	"io"

	"github.com/cockroachdb/errors"
)

// KEYMAP.jsonl shape (one record per line):
//
//	{"encoded":"<encoded-segment>","original":"<base64url-no-padding>","kind":"sha-fallback"}
//
// Records are written in encounter order (the order the encoder produced
// them) and never modified after write. The file is append-only; if the same
// encoded segment is written twice the reader keeps the last entry, but the
// encoder is expected not to emit duplicates within a single dump.
//
// Records exist only for entries whose original bytes are NOT recoverable
// from the encoded filename alone:
//
//   - KindSHAFallback — segment is `<sha-prefix-32>__<truncated-original>`
//     (filename length exceeded EncodeSegment's 240-byte ceiling).
//   - KindS3LeafData  — S3 object renamed to `<obj>.elastickv-leaf-data`
//     because both `<obj>` and `<obj>/...` existed in the same bucket.
//   - KindMetaCollision — user S3 object key happened to end in
//     `.elastickv-meta.json`; renamed under --rename-collisions.
//
// A consumer that does not care about reversing these to original bytes can
// ignore KEYMAP.jsonl entirely.
const (
	KindSHAFallback   = "sha-fallback"
	KindS3LeafData    = "s3-leaf-data"
	KindMetaCollision = "meta-suffix-rename"
)

// keymapBufSizeWriter is the bufio.Writer buffer size for the JSONL writer.
// 64 KiB amortises the per-syscall cost across hundreds of small records
// without holding pathological amounts of memory.
const keymapBufSizeWriter = 64 << 10

// keymapBufSizeReader bounds bufio.Scanner's per-line buffer. KEYMAP records
// carry a ~240-byte encoded segment plus a base64'd original key (which can
// itself be arbitrarily large but is bounded by the practical maximum key
// size on the source store). 1 MiB per line is generous; if a record
// genuinely exceeds it the reader returns a typed error rather than
// silently truncating.
const keymapBufSizeReader = 1 << 20

// ErrInvalidKeymapRecord is returned by Reader.Next when a line does not
// parse as a KeymapRecord (malformed JSON, missing field, malformed
// base64, etc.).
var ErrInvalidKeymapRecord = errors.New("backup: invalid KEYMAP.jsonl record")

// KeymapRecord is a single mapping from encoded filename component back to
// the original key bytes. Original bytes are arbitrary (binary safe), so
// they are encoded as base64url-no-padding for transport in JSON.
type KeymapRecord struct {
	// Encoded is the filename segment as it appears in the dump tree.
	Encoded string `json:"encoded"`
	// OriginalB64 is base64url-no-padding of the original key bytes.
	OriginalB64 string `json:"original"`
	// Kind classifies why this record exists; see Kind* constants.
	Kind string `json:"kind"`
}

// Original returns the decoded original key bytes from r.OriginalB64.
func (r KeymapRecord) Original() ([]byte, error) {
	out, err := base64.RawURLEncoding.DecodeString(r.OriginalB64)
	if err != nil {
		return nil, errors.Wrap(ErrInvalidKeymapRecord, err.Error())
	}
	return out, nil
}

// KeymapWriter appends records to a KEYMAP.jsonl stream. Concurrent calls to
// Write are serialised through the underlying bufio.Writer; the caller is
// expected to use a single writer per scope.
type KeymapWriter struct {
	bw  *bufio.Writer
	enc *json.Encoder
	// count tracks how many records have been written; exposed so the caller
	// can decide to omit an empty KEYMAP.jsonl file (per the spec, the file
	// is omitted when no entries exist).
	count int
}

// NewKeymapWriter returns a writer that appends JSONL records to w. Close
// must be called to flush.
func NewKeymapWriter(w io.Writer) *KeymapWriter {
	bw := bufio.NewWriterSize(w, keymapBufSizeWriter)
	enc := json.NewEncoder(bw)
	enc.SetEscapeHTML(false) // we never embed user keys in HTML; preserve `<>&`
	return &KeymapWriter{bw: bw, enc: enc}
}

// Write appends one KeymapRecord. The record is JSON-serialised with a
// trailing newline (json.Encoder behavior), giving the JSONL contract.
func (w *KeymapWriter) Write(rec KeymapRecord) error {
	if rec.Encoded == "" {
		return errors.WithStack(errors.New("backup: KEYMAP record encoded must be non-empty"))
	}
	if rec.Kind == "" {
		return errors.WithStack(errors.New("backup: KEYMAP record kind must be non-empty"))
	}
	if err := w.enc.Encode(rec); err != nil {
		return errors.WithStack(err)
	}
	w.count++
	return nil
}

// WriteOriginal is a convenience wrapper that base64-encodes raw original
// bytes for the caller.
func (w *KeymapWriter) WriteOriginal(encoded string, original []byte, kind string) error {
	return w.Write(KeymapRecord{
		Encoded:     encoded,
		OriginalB64: base64.RawURLEncoding.EncodeToString(original),
		Kind:        kind,
	})
}

// Count returns the number of records written so far. Useful for the
// "omit empty KEYMAP file" decision after the dump completes.
func (w *KeymapWriter) Count() int { return w.count }

// Close flushes any buffered records to the underlying writer.
func (w *KeymapWriter) Close() error {
	if w.bw == nil {
		return nil
	}
	if err := w.bw.Flush(); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

// KeymapReader iterates JSONL records line-by-line. Memory footprint is
// bounded by keymapBufSizeReader regardless of file size.
type KeymapReader struct {
	sc  *bufio.Scanner
	err error
}

// NewKeymapReader wraps r so the caller can iterate records via Next.
func NewKeymapReader(r io.Reader) *KeymapReader {
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 0, keymapBufSizeReader), keymapBufSizeReader)
	return &KeymapReader{sc: sc}
}

// Next decodes the next record. It returns (rec, true, nil) on success,
// (zero, false, nil) at end of stream, and (zero, false, err) on parse
// failure or I/O error. Once an error is returned the reader is sticky:
// subsequent calls return the same error.
//
// The base64-encoded `original` field is validated at parse time rather
// than lazily: a malformed dump must surface on the first read of the
// affected line, not propagate silently until a much later
// rec.Original() call. Same error class either way.
func (r *KeymapReader) Next() (KeymapRecord, bool, error) {
	if r.err != nil {
		return KeymapRecord{}, false, r.err
	}
	if !r.sc.Scan() {
		if err := r.sc.Err(); err != nil {
			r.err = errors.WithStack(err)
			return KeymapRecord{}, false, r.err
		}
		return KeymapRecord{}, false, nil
	}
	line := r.sc.Bytes()
	var rec KeymapRecord
	if err := json.Unmarshal(line, &rec); err != nil {
		r.err = errors.Wrap(ErrInvalidKeymapRecord, err.Error())
		return KeymapRecord{}, false, r.err
	}
	if rec.Encoded == "" || rec.Kind == "" {
		r.err = errors.Wrap(ErrInvalidKeymapRecord, "missing encoded or kind")
		return KeymapRecord{}, false, r.err
	}
	if _, err := base64.RawURLEncoding.DecodeString(rec.OriginalB64); err != nil {
		r.err = errors.Wrap(ErrInvalidKeymapRecord, err.Error())
		return KeymapRecord{}, false, r.err
	}
	return rec, true, nil
}

// LoadKeymap reads every record from r into an in-memory map keyed by
// encoded segment. The last record wins on duplicates. Suitable for
// scopes where the keymap fits comfortably in memory; for large scopes
// callers should use KeymapReader directly.
func LoadKeymap(r io.Reader) (map[string]KeymapRecord, error) {
	out := make(map[string]KeymapRecord)
	rd := NewKeymapReader(r)
	for {
		rec, ok, err := rd.Next()
		if err != nil {
			return nil, err
		}
		if !ok {
			return out, nil
		}
		out[rec.Encoded] = rec
	}
}
