package backup

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

// redisEncTS is the commit timestamp the Redis encoder tests stamp.
const redisEncTS uint64 = 0x0001_8F1A_2B3C_0042

// writeRedisFile writes one file under <root>/redis/db_0/<rel>,
// creating parent dirs.
func writeRedisFile(t *testing.T, root, rel string, data []byte) {
	t.Helper()
	path := filepath.Join(root, "redis", "db_0", rel)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatalf("WriteFile %s: %v", rel, err)
	}
}

// encodeRedisTree runs the Redis encoder over inRoot and returns the
// resulting EKVPBBL1 bytes.
func encodeRedisTree(t *testing.T, inRoot string) []byte {
	t.Helper()
	b := newSnapshotBuilder(redisEncTS)
	if err := NewRedisEncoder(inRoot, 0).Encode(b); err != nil {
		t.Fatalf("RedisEncoder.Encode: %v", err)
	}
	var buf bytes.Buffer
	if _, err := b.WriteTo(&buf); err != nil {
		t.Fatalf("WriteTo: %v", err)
	}
	return buf.Bytes()
}

// decodeRedisTree decodes fsm bytes through the real decode path into a
// fresh output tree and returns its root.
func decodeRedisTree(t *testing.T, fsm []byte) string {
	t.Helper()
	out := t.TempDir()
	_, err := DecodeSnapshot(bytes.NewReader(fsm), DecodeOptions{
		OutRoot:  out,
		Adapters: AdapterSet{Redis: true},
	})
	if err != nil {
		t.Fatalf("DecodeSnapshot: %v", err)
	}
	return out
}

// TestRedisEncodeStringRoundTripViaDecode runs the gold-standard
// directory round-trip for a no-TTL string: dir -> encode -> .fsm ->
// (real) decode -> dir', asserting the body survives byte-for-byte.
func TestRedisEncodeStringRoundTripViaDecode(t *testing.T) {
	t.Parallel()
	in := t.TempDir()
	enc := EncodeSegment([]byte("mykey"))
	writeRedisFile(t, in, filepath.Join("strings", enc+".bin"), []byte("hello"))

	out := decodeRedisTree(t, encodeRedisTree(t, in))

	got, err := os.ReadFile(filepath.Join(out, "redis", "db_0", "strings", enc+".bin"))
	if err != nil {
		t.Fatalf("read decoded string: %v", err)
	}
	if !bytes.Equal(got, []byte("hello")) {
		t.Fatalf("decoded string = %q, want %q", got, "hello")
	}
}

// TestRedisEncodeStringTTLRoundTripViaDecode pins that a string's TTL
// (held in strings_ttl.jsonl on input) is folded into the inline value
// header by the encoder and recovered on decode — back into
// strings_ttl.jsonl — without an !redis|ttl| row leaking in.
func TestRedisEncodeStringTTLRoundTripViaDecode(t *testing.T) {
	t.Parallel()
	in := t.TempDir()
	enc := EncodeSegment([]byte("sess"))
	const expireMs uint64 = 1_735_689_600_000
	writeRedisFile(t, in, filepath.Join("strings", enc+".bin"), []byte("v"))
	writeTTLSidecar(t, in, "strings_ttl.jsonl", enc, expireMs)

	out := decodeRedisTree(t, encodeRedisTree(t, in))

	got, err := os.ReadFile(filepath.Join(out, "redis", "db_0", "strings", enc+".bin"))
	if err != nil {
		t.Fatalf("read decoded string: %v", err)
	}
	if !bytes.Equal(got, []byte("v")) {
		t.Fatalf("decoded string = %q, want %q", got, "v")
	}
	if gotMs := readTTLSidecar(t, out, "strings_ttl.jsonl")[enc]; gotMs != expireMs {
		t.Fatalf("decoded strings_ttl[%s] = %d, want %d", enc, gotMs, expireMs)
	}
}

// TestRedisEncodeHLLTTLRoundTripViaDecode pins that an HLL sketch is
// emitted raw under !redis|hll| and its TTL via an !redis|ttl| row,
// both recovered on decode (hll/<k>.bin + hll_ttl.jsonl).
func TestRedisEncodeHLLTTLRoundTripViaDecode(t *testing.T) {
	t.Parallel()
	in := t.TempDir()
	enc := EncodeSegment([]byte("counter"))
	const expireMs uint64 = 1_700_000_000_000
	sketch := []byte{0x01, 0x02, 0x03, 0xFF, 0x00}
	writeRedisFile(t, in, filepath.Join("hll", enc+".bin"), sketch)
	writeTTLSidecar(t, in, "hll_ttl.jsonl", enc, expireMs)

	out := decodeRedisTree(t, encodeRedisTree(t, in))

	got, err := os.ReadFile(filepath.Join(out, "redis", "db_0", "hll", enc+".bin"))
	if err != nil {
		t.Fatalf("read decoded hll: %v", err)
	}
	if !bytes.Equal(got, sketch) {
		t.Fatalf("decoded hll = %x, want %x", got, sketch)
	}
	if gotMs := readTTLSidecar(t, out, "hll_ttl.jsonl")[enc]; gotMs != expireMs {
		t.Fatalf("decoded hll_ttl[%s] = %d, want %d", enc, gotMs, expireMs)
	}
}

// TestRedisEncodeStringInlineValueLayout pins the inline value bytes
// directly (non-circular against the round-trip): no-TTL strings get a
// 3-byte header, TTL strings an 11-byte header with BE millis.
func TestRedisEncodeStringInlineValueLayout(t *testing.T) {
	t.Parallel()
	noTTL := encodeRedisStrInlineValue([]byte("ab"), 0)
	want := []byte{redisStrMagic, redisStrVersion, 0x00, 'a', 'b'}
	if !bytes.Equal(noTTL, want) {
		t.Fatalf("no-TTL value = %x, want %x", noTTL, want)
	}
	withTTL := encodeRedisStrInlineValue([]byte("ab"), 0x0102030405060708)
	wantHdr := []byte{redisStrMagic, redisStrVersion, redisStrHasTTL,
		0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 'a', 'b'}
	if !bytes.Equal(withTTL, wantHdr) {
		t.Fatalf("TTL value = %x, want %x", withTTL, wantHdr)
	}
}

// TestRedisEncodeMissingDBIsNoop pins that a missing redis/db_0/ dir
// yields zero entries and no error (nothing to encode).
func TestRedisEncodeMissingDBIsNoop(t *testing.T) {
	t.Parallel()
	b := newSnapshotBuilder(redisEncTS)
	if err := NewRedisEncoder(t.TempDir(), 0).Encode(b); err != nil {
		t.Fatalf("Encode on empty tree: %v", err)
	}
	if b.Len() != 0 {
		t.Fatalf("got %d entries, want 0", b.Len())
	}
}

// writeTTLSidecar appends one {"key","expire_at_ms"} record to a TTL
// sidecar JSONL under <root>/redis/db_0/.
func writeTTLSidecar(t *testing.T, root, name, encodedKey string, expireMs uint64) {
	t.Helper()
	rec := ttlSidecarRecord{Key: encodedKey, ExpireAtMs: expireMs}
	line, err := json.Marshal(rec)
	if err != nil {
		t.Fatalf("marshal ttl rec: %v", err)
	}
	writeRedisFile(t, root, name, append(line, '\n'))
}

// readTTLSidecar parses a decoded TTL sidecar JSONL into
// encoded-key -> expire_at_ms.
func readTTLSidecar(t *testing.T, root, name string) map[string]uint64 {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(root, "redis", "db_0", name))
	if err != nil {
		t.Fatalf("read ttl sidecar %s: %v", name, err)
	}
	out := map[string]uint64{}
	dec := json.NewDecoder(bytes.NewReader(data))
	for dec.More() {
		var rec ttlSidecarRecord
		if err := dec.Decode(&rec); err != nil {
			t.Fatalf("decode ttl sidecar: %v", err)
		}
		out[rec.Key] = rec.ExpireAtMs
	}
	return out
}
