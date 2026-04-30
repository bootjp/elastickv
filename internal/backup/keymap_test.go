package backup

import (
	"bytes"
	"strings"
	"testing"

	"github.com/cockroachdb/errors"
)

type keymapCase struct {
	encoded  string
	original []byte
	kind     string
}

func keymapRoundTripCases() []keymapCase {
	return []keymapCase{
		{"abcdef0123456789abcdef0123456789__hello", []byte("hello-but-much-longer-than-fits"), KindSHAFallback},
		{"path%2Fto.elastickv-leaf-data", []byte("path/to"), KindS3LeafData},
		{"foo.elastickv-meta.json.user-data", []byte("foo.elastickv-meta.json"), KindMetaCollision},
		{"binary-key", []byte{0x00, 0xff, 0x01, 0xfe}, KindSHAFallback},
		{"empty-original", []byte{}, KindSHAFallback},
	}
}

func writeKeymapCases(t *testing.T, w *KeymapWriter, cases []keymapCase) {
	t.Helper()
	for _, c := range cases {
		if err := w.WriteOriginal(c.encoded, c.original, c.kind); err != nil {
			t.Fatalf("Write(%q): %v", c.encoded, err)
		}
	}
}

func assertKeymapRecord(t *testing.T, got map[string]KeymapRecord, c keymapCase) {
	t.Helper()
	rec, ok := got[c.encoded]
	if !ok {
		t.Fatalf("missing record for %q", c.encoded)
	}
	if rec.Kind != c.kind {
		t.Fatalf("%q kind = %q, want %q", c.encoded, rec.Kind, c.kind)
	}
	orig, err := rec.Original()
	if err != nil {
		t.Fatalf("%q Original: %v", c.encoded, err)
	}
	if !bytes.Equal(orig, c.original) {
		t.Fatalf("%q original = %x, want %x", c.encoded, orig, c.original)
	}
}

func TestKeymapWriter_RoundTrip(t *testing.T) {
	t.Parallel()
	cases := keymapRoundTripCases()
	var buf bytes.Buffer
	w := NewKeymapWriter(&buf)
	writeKeymapCases(t, w, cases)
	if w.Count() != len(cases) {
		t.Fatalf("Count = %d, want %d", w.Count(), len(cases))
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	got, err := LoadKeymap(&buf)
	if err != nil {
		t.Fatalf("LoadKeymap: %v", err)
	}
	if len(got) != len(cases) {
		t.Fatalf("loaded len = %d, want %d", len(got), len(cases))
	}
	for _, c := range cases {
		assertKeymapRecord(t, got, c)
	}
}

func TestKeymapWriter_RejectsEmptyEncoded(t *testing.T) {
	t.Parallel()
	w := NewKeymapWriter(&bytes.Buffer{})
	if err := w.Write(KeymapRecord{Encoded: "", Kind: KindSHAFallback}); err == nil {
		t.Fatalf("expected error for empty encoded, got nil")
	}
	if err := w.Write(KeymapRecord{Encoded: "x", Kind: ""}); err == nil {
		t.Fatalf("expected error for empty kind, got nil")
	}
}

func TestKeymapWriter_DoesNotEscapeHTML(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	w := NewKeymapWriter(&buf)
	// json.Encoder escapes `<`, `>`, `&` by default; we disable that so
	// keys containing these bytes encode/decode without surprise.
	if err := w.WriteOriginal("a%3Cb%3Ec", []byte("a<b>c&d"), KindSHAFallback); err != nil {
		t.Fatalf("WriteOriginal: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	out := buf.String()
	if strings.Contains(out, `<`) || strings.Contains(out, `>`) || strings.Contains(out, `&`) {
		t.Fatalf("unwanted HTML escape in output: %q", out)
	}
	// And the base64 of "a<b>c&d" appears intact:
	if !strings.Contains(out, "YTxiPmMmZA") {
		t.Fatalf("missing base64 of original in output: %q", out)
	}
}

func TestKeymapWriter_OmitEmpty(t *testing.T) {
	t.Parallel()
	// The "omit when empty" decision is the caller's; the writer just
	// reports whether any records were written.
	var buf bytes.Buffer
	w := NewKeymapWriter(&buf)
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if w.Count() != 0 {
		t.Fatalf("Count = %d, want 0", w.Count())
	}
	if buf.Len() != 0 {
		t.Fatalf("empty writer produced output: %q", buf.String())
	}
}

func TestKeymapReader_RejectsMalformedJSON(t *testing.T) {
	t.Parallel()
	r := NewKeymapReader(strings.NewReader("not-json\n"))
	_, _, err := r.Next()
	if !errors.Is(err, ErrInvalidKeymapRecord) {
		t.Fatalf("err = %v, want ErrInvalidKeymapRecord", err)
	}
	// Sticky: subsequent calls return the same wrapped error class.
	_, _, err2 := r.Next()
	if !errors.Is(err2, ErrInvalidKeymapRecord) {
		t.Fatalf("non-sticky error: %v", err2)
	}
}

func TestKeymapReader_RejectsRecordWithoutEncodedOrKind(t *testing.T) {
	t.Parallel()
	cases := []string{
		`{"original":"AA"}`,
		`{"encoded":"","kind":"sha-fallback"}`,
		`{"encoded":"x"}`,
		`{"encoded":"x","kind":""}`,
	}
	for _, line := range cases {
		r := NewKeymapReader(strings.NewReader(line + "\n"))
		_, _, err := r.Next()
		if !errors.Is(err, ErrInvalidKeymapRecord) {
			t.Fatalf("input %q: err = %v, want ErrInvalidKeymapRecord", line, err)
		}
	}
}

func TestKeymapReader_RejectsBlankLines(t *testing.T) {
	t.Parallel()
	// bufio.Scanner skips trailing newline but emits an empty line when one
	// is in the middle of the stream. We require strict JSONL — every
	// non-empty line must be a record. An empty line in the middle must
	// surface as ErrInvalidKeymapRecord rather than be silently skipped,
	// so truncated dumps are recognised.
	input := `{"encoded":"x","original":"AA","kind":"sha-fallback"}` + "\n\n" +
		`{"encoded":"y","original":"AA","kind":"sha-fallback"}` + "\n"
	r := NewKeymapReader(strings.NewReader(input))
	if _, ok, err := r.Next(); !ok || err != nil {
		t.Fatalf("first record: ok=%v err=%v", ok, err)
	}
	if _, _, err := r.Next(); !errors.Is(err, ErrInvalidKeymapRecord) {
		t.Fatalf("blank line: err=%v want ErrInvalidKeymapRecord", err)
	}
}

func TestLoadKeymap_LastRecordWins(t *testing.T) {
	t.Parallel()
	input := `{"encoded":"x","original":"YQ","kind":"sha-fallback"}` + "\n" +
		`{"encoded":"x","original":"Yg","kind":"sha-fallback"}` + "\n"
	got, err := LoadKeymap(strings.NewReader(input))
	if err != nil {
		t.Fatalf("LoadKeymap: %v", err)
	}
	rec, ok := got["x"]
	if !ok {
		t.Fatalf("missing record")
	}
	orig, err := rec.Original()
	if err != nil {
		t.Fatalf("Original: %v", err)
	}
	if string(orig) != "b" {
		t.Fatalf("last-wins broken: got %q want %q", orig, "b")
	}
}

func TestKeymapRecord_OriginalRejectsBadBase64(t *testing.T) {
	t.Parallel()
	rec := KeymapRecord{Encoded: "x", OriginalB64: "!!!", Kind: KindSHAFallback}
	if _, err := rec.Original(); !errors.Is(err, ErrInvalidKeymapRecord) {
		t.Fatalf("err = %v, want ErrInvalidKeymapRecord", err)
	}
}

func TestKeymapReader_RejectsMalformedBase64AtParseTime(t *testing.T) {
	t.Parallel()
	// JSON parses fine; the structural fields are present; only the
	// `original` base64 is malformed. The reader must catch this on
	// the first Next() rather than defer it to a later Original()
	// call — Codex P1 #179.
	input := `{"encoded":"x","original":"!!!","kind":"sha-fallback"}` + "\n"
	r := NewKeymapReader(strings.NewReader(input))
	_, _, err := r.Next()
	if !errors.Is(err, ErrInvalidKeymapRecord) {
		t.Fatalf("err=%v want ErrInvalidKeymapRecord on parse-time base64 validation", err)
	}
}
