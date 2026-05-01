package backup

import (
	"encoding/binary"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/cockroachdb/errors"
)

// encodeHashMetaValue builds the 8-byte BE field-count value used by
// the live store/hash_helpers.go.
func encodeHashMetaValue(fieldCount int64) []byte {
	v := make([]byte, 8)
	binary.BigEndian.PutUint64(v, uint64(fieldCount)) //nolint:gosec
	return v
}

// hashMetaKey is the test-side key constructor for !hs|meta|<len><userKey>.
// Mirrors store.HashMetaKey.
func hashMetaKey(userKey string) []byte {
	out := []byte(RedisHashMetaPrefix)
	var l [4]byte
	binary.BigEndian.PutUint32(l[:], uint32(len(userKey))) //nolint:gosec
	out = append(out, l[:]...)
	return append(out, userKey...)
}

// hashFieldKey mirrors store.HashFieldKey:
// !hs|fld|<len(4)><userKey><fieldName>.
func hashFieldKey(userKey, fieldName string) []byte {
	out := []byte(RedisHashFieldPrefix)
	var l [4]byte
	binary.BigEndian.PutUint32(l[:], uint32(len(userKey))) //nolint:gosec
	out = append(out, l[:]...)
	out = append(out, userKey...)
	return append(out, fieldName...)
}

func readHashJSON(t *testing.T, path string) map[string]any {
	t.Helper()
	b, err := os.ReadFile(path) //nolint:gosec // test path
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	var m map[string]any
	if err := json.Unmarshal(b, &m); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	return m
}

// hashFloat / hashSubMap are local type-assertion-checking helpers
// that satisfy the forcetypeassert linter on JSON-decoded test
// fixtures. JSON unmarshals all numbers to float64 and all objects
// to map[string]any; these wrappers fail loudly with the field name
// rather than panicking on a bare assertion.
func hashFloat(t *testing.T, m map[string]any, key string) float64 {
	t.Helper()
	v, ok := m[key]
	if !ok {
		t.Fatalf("field %q missing in %+v", key, m)
	}
	f, ok := v.(float64)
	if !ok {
		t.Fatalf("field %q = %T(%v), want float64", key, v, v)
	}
	return f
}

func hashSubMap(t *testing.T, m map[string]any, key string) map[string]any {
	t.Helper()
	v, ok := m[key]
	if !ok {
		t.Fatalf("field %q missing in %+v", key, m)
	}
	sub, ok := v.(map[string]any)
	if !ok {
		t.Fatalf("field %q = %T(%v), want map[string]any", key, v, v)
	}
	return sub
}

func TestRedisDB_HashRoundTripBasic(t *testing.T) {
	t.Parallel()
	db, root := newRedisDB(t)
	if err := db.HandleHashMeta(hashMetaKey("user:42"), encodeHashMetaValue(2)); err != nil {
		t.Fatalf("HandleHashMeta: %v", err)
	}
	if err := db.HandleHashField(hashFieldKey("user:42", "name"), []byte("alice")); err != nil {
		t.Fatalf("HandleHashField: %v", err)
	}
	if err := db.HandleHashField(hashFieldKey("user:42", "age"), []byte("30")); err != nil {
		t.Fatalf("HandleHashField: %v", err)
	}
	if err := db.Finalize(); err != nil {
		t.Fatalf("Finalize: %v", err)
	}
	got := readHashJSON(t, filepath.Join(root, "redis", "db_0", "hashes", "user%3A42.json"))
	if hashFloat(t, got, "format_version") != 1 {
		t.Fatalf("format_version = %v", got["format_version"])
	}
	if got["expire_at_ms"] != nil {
		t.Fatalf("expire_at_ms must be nil without TTL, got %v", got["expire_at_ms"])
	}
	fields := hashSubMap(t, got, "fields")
	if fields["name"] != "alice" {
		t.Fatalf("name = %v want alice", fields["name"])
	}
	if fields["age"] != "30" {
		t.Fatalf("age = %v want 30", fields["age"])
	}
}

func TestRedisDB_HashEmptyHashStillEmitsFile(t *testing.T) {
	t.Parallel()
	db, root := newRedisDB(t)
	// HLEN==0 hashes are observable to clients; the dump must
	// preserve their existence.
	if err := db.HandleHashMeta(hashMetaKey("empty"), encodeHashMetaValue(0)); err != nil {
		t.Fatal(err)
	}
	if err := db.Finalize(); err != nil {
		t.Fatal(err)
	}
	got := readHashJSON(t, filepath.Join(root, "redis", "db_0", "hashes", "empty.json"))
	fields := hashSubMap(t, got, "fields")
	if len(fields) != 0 {
		t.Fatalf("empty hash should emit empty fields object, got %v", fields)
	}
}

func TestRedisDB_HashTTLInlinedFromScanIndex(t *testing.T) {
	t.Parallel()
	db, root := newRedisDB(t)
	if err := db.HandleHashMeta(hashMetaKey("k"), encodeHashMetaValue(1)); err != nil {
		t.Fatal(err)
	}
	if err := db.HandleHashField(hashFieldKey("k", "f"), []byte("v")); err != nil {
		t.Fatal(err)
	}
	// TTL records arrive on the !redis|ttl| scan index. The hash
	// encoder must fold this into the JSON file's expire_at_ms
	// field, NOT route it to a separate sidecar (that's the
	// strings/HLL pattern only).
	if err := db.HandleTTL([]byte("k"), encodeTTLValue(fixedExpireMs)); err != nil {
		t.Fatal(err)
	}
	if err := db.Finalize(); err != nil {
		t.Fatal(err)
	}
	got := readHashJSON(t, filepath.Join(root, "redis", "db_0", "hashes", "k.json"))
	if hashFloat(t, got, "expire_at_ms") != float64(fixedExpireMs) {
		t.Fatalf("expire_at_ms = %v want %d", got["expire_at_ms"], fixedExpireMs)
	}
	// And no hashes_ttl.jsonl should ever be emitted.
	if _, err := os.Stat(filepath.Join(root, "redis", "db_0", "hashes_ttl.jsonl")); !os.IsNotExist(err) {
		t.Fatalf("unexpected hash TTL sidecar: stat err=%v", err)
	}
}

func TestRedisDB_HashLengthMismatchWarns(t *testing.T) {
	t.Parallel()
	db, _ := newRedisDB(t)
	var events []string
	db.WithWarnSink(func(event string, _ ...any) { events = append(events, event) })
	// Meta declares 5 fields but only 1 field key arrives — surface
	// as a structured warning so operators can correlate dump shape
	// with mid-write snapshot timing.
	if err := db.HandleHashMeta(hashMetaKey("h"), encodeHashMetaValue(5)); err != nil {
		t.Fatal(err)
	}
	if err := db.HandleHashField(hashFieldKey("h", "only"), []byte("x")); err != nil {
		t.Fatal(err)
	}
	if err := db.Finalize(); err != nil {
		t.Fatal(err)
	}
	wantEvent := "redis_hash_length_mismatch"
	found := false
	for _, e := range events {
		if e == wantEvent {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected %q warning, got %v", wantEvent, events)
	}
}

func TestRedisDB_HashBinaryFieldValueUsesBase64Envelope(t *testing.T) {
	t.Parallel()
	db, root := newRedisDB(t)
	if err := db.HandleHashMeta(hashMetaKey("k"), encodeHashMetaValue(1)); err != nil {
		t.Fatal(err)
	}
	// 0x80 0xff is invalid UTF-8 — emit must use the typed
	// `{"base64":"..."}` envelope (Codex pattern from PR #714 SQS body).
	if err := db.HandleHashField(hashFieldKey("k", "blob"), []byte{0x80, 0xff, 0x01}); err != nil {
		t.Fatal(err)
	}
	if err := db.Finalize(); err != nil {
		t.Fatal(err)
	}
	got := readHashJSON(t, filepath.Join(root, "redis", "db_0", "hashes", "k.json"))
	fields := hashSubMap(t, got, "fields")
	envelope, ok := fields["blob"].(map[string]any)
	if !ok {
		t.Fatalf("expected base64 envelope for binary value, got %T(%v)", fields["blob"], fields["blob"])
	}
	if envelope["base64"] == "" {
		t.Fatalf("base64 envelope missing payload: %v", envelope)
	}
}

func TestRedisDB_HashRejectsMalformedMetaValueLength(t *testing.T) {
	t.Parallel()
	db, _ := newRedisDB(t)
	err := db.HandleHashMeta(hashMetaKey("k"), []byte{0x00})
	if !errors.Is(err, ErrRedisInvalidHashMeta) {
		t.Fatalf("err=%v want ErrRedisInvalidHashMeta", err)
	}
}

func TestRedisDB_HashRejectsTruncatedFieldKey(t *testing.T) {
	t.Parallel()
	db, _ := newRedisDB(t)
	// Field key with userKeyLen=10 but only 3 bytes of userKey:
	// must not silently route bytes into the wrong hash.
	bad := []byte(RedisHashFieldPrefix)
	bad = append(bad, 0x00, 0x00, 0x00, 0x0a) // ukLen = 10
	bad = append(bad, []byte("abc")...)
	err := db.HandleHashField(bad, []byte("v"))
	if !errors.Is(err, ErrRedisInvalidHashKey) {
		t.Fatalf("err=%v want ErrRedisInvalidHashKey", err)
	}
}

func TestRedisDB_HashSHAFallbackKeymapped(t *testing.T) {
	t.Parallel()
	db, root := newRedisDB(t)
	// 300-byte key forces EncodeSegment's SHA-fallback. The encoder
	// must record a KEYMAP entry so restore can recover the
	// original key bytes from a non-reversible filename.
	long := string(bytesRepeat('%', 300))
	if err := db.HandleHashMeta(hashMetaKey(long), encodeHashMetaValue(1)); err != nil {
		t.Fatal(err)
	}
	if err := db.HandleHashField(hashFieldKey(long, "f"), []byte("v")); err != nil {
		t.Fatal(err)
	}
	if err := db.Finalize(); err != nil {
		t.Fatal(err)
	}
	keymapPath := filepath.Join(root, "redis", "db_0", "KEYMAP.jsonl")
	if _, err := os.Stat(keymapPath); err != nil {
		t.Fatalf("KEYMAP.jsonl missing for SHA-fallback hash key: %v", err)
	}
}

// bytesRepeat is a small helper that mirrors bytes.Repeat without
// importing bytes everywhere — keeps test imports tight.
func bytesRepeat(b byte, n int) []byte {
	out := make([]byte, n)
	for i := range out {
		out[i] = b
	}
	return out
}
