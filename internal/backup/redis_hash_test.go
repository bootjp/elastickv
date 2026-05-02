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

// hashFieldArray fetches the `fields` JSON array from a decoded hash
// JSON file and returns it as a slice of {name, value} maps. The
// dump-format projection emits fields as an ARRAY of records (not a
// JSON object) so binary-safe Redis field NAMES round-trip without
// collision — see the comment on hashFieldRecord in redis_hash.go.
func hashFieldArray(t *testing.T, m map[string]any) []map[string]any {
	t.Helper()
	v, ok := m["fields"]
	if !ok {
		t.Fatalf("fields missing in %+v", m)
	}
	raw, ok := v.([]any)
	if !ok {
		t.Fatalf("fields = %T(%v), want []any", v, v)
	}
	out := make([]map[string]any, 0, len(raw))
	for i, r := range raw {
		rm, ok := r.(map[string]any)
		if !ok {
			t.Fatalf("fields[%d] = %T(%v), want map[string]any", i, r, r)
		}
		out = append(out, rm)
	}
	return out
}

// hashFieldByName looks up a single field in the array shape and
// returns its `value` element (UTF-8 string or base64 envelope).
func hashFieldByName(t *testing.T, m map[string]any, name string) any {
	t.Helper()
	for _, f := range hashFieldArray(t, m) {
		if f["name"] == name {
			return f["value"]
		}
	}
	t.Fatalf("field %q not found in %+v", name, m["fields"])
	return nil
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
	if v := hashFieldByName(t, got, "name"); v != "alice" {
		t.Fatalf("name = %v want alice", v)
	}
	if v := hashFieldByName(t, got, "age"); v != "30" {
		t.Fatalf("age = %v want 30", v)
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
	if fields := hashFieldArray(t, got); len(fields) != 0 {
		t.Fatalf("empty hash should emit empty fields array, got %v", fields)
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
	v := hashFieldByName(t, got, "blob")
	envelope, ok := v.(map[string]any)
	if !ok {
		t.Fatalf("expected base64 envelope for binary value, got %T(%v)", v, v)
	}
	if envelope["base64"] == "" {
		t.Fatalf("base64 envelope missing payload: %v", envelope)
	}
}

// TestRedisDB_HashBinaryFieldNameRoundTripsViaArray covers the Codex
// P1 round-12 scenario: a UTF-8-literal field name `%FF` and a
// 1-byte binary field name `0xFF` would collide if `fields` were a
// JSON object, because EncodeSegment percent-encodes 0xFF to `%FF`.
// With the array-of-records shape both names round-trip
// independently — the binary name uses the typed base64 envelope
// while the UTF-8 literal stays as a plain JSON string.
func TestRedisDB_HashBinaryFieldNameRoundTripsViaArray(t *testing.T) {
	t.Parallel()
	db, root := newRedisDB(t)
	if err := db.HandleHashMeta(hashMetaKey("k"), encodeHashMetaValue(2)); err != nil {
		t.Fatal(err)
	}
	if err := db.HandleHashField(hashFieldKey("k", "%FF"), []byte("literal-percent-ff")); err != nil {
		t.Fatal(err)
	}
	if err := db.HandleHashField(append(hashFieldKey("k", ""), 0xff), []byte("binary-byte")); err != nil {
		t.Fatal(err)
	}
	if err := db.Finalize(); err != nil {
		t.Fatal(err)
	}
	got := readHashJSON(t, filepath.Join(root, "redis", "db_0", "hashes", "k.json"))
	fields := hashFieldArray(t, got)
	if len(fields) != 2 {
		t.Fatalf("len(fields) = %d, want 2 (binary and literal must be distinct)", len(fields))
	}
	literalSeen, binarySeen := classifyHashFieldShapes(t, fields)
	if !literalSeen {
		t.Fatalf("literal %%FF field missing from %+v", fields)
	}
	if !binarySeen {
		t.Fatalf("binary 0xFF field missing from %+v", fields)
	}
}

// classifyHashFieldShapes folds the binary-vs-literal field-shape
// assertions out of the parent test so cyclomatic complexity stays
// under the project linter's ceiling. Returns (sawUTF8Literal,
// sawBase64Envelope).
func classifyHashFieldShapes(t *testing.T, fields []map[string]any) (bool, bool) {
	t.Helper()
	var literalSeen, binarySeen bool
	for _, f := range fields {
		switch n := f["name"].(type) {
		case string:
			if n == "%FF" {
				literalSeen = true
				if f["value"] != "literal-percent-ff" {
					t.Fatalf("literal value = %v", f["value"])
				}
			}
		case map[string]any:
			if n["base64"] == base64URLEncode(0xff) {
				binarySeen = true
				if f["value"] != "binary-byte" {
					t.Fatalf("binary value = %v", f["value"])
				}
			}
		default:
			t.Fatalf("unexpected name type %T(%v)", f["name"], f["name"])
		}
	}
	return literalSeen, binarySeen
}

func base64URLEncode(b byte) string {
	// base64.RawURLEncoding of a single byte; matches what
	// marshalRedisBinaryValue emits for non-UTF-8 content.
	const alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_"
	hi := alphabet[b>>2]
	lo := alphabet[(b&0x3)<<4]
	return string([]byte{hi, lo})
}

// TestRedisDB_HashAcceptsEmptyFieldName is the regression for Codex
// P1 round 13 (PR #725): Redis hash field names are binary-safe and
// `HSET k "" v` is a valid command, so the live store emits a key
// shaped exactly `!hs|fld|<len><userKey>` with no trailing field
// bytes. The previous explicit zero-length rejection in
// HandleHashField caused backup decoding to error out on real data.
// Now an empty field name is accepted and emitted as one of the
// hash record's `fields` entries with an empty-string `name`.
func TestRedisDB_HashAcceptsEmptyFieldName(t *testing.T) {
	t.Parallel()
	db, root := newRedisDB(t)
	if err := db.HandleHashMeta(hashMetaKey("k"), encodeHashMetaValue(2)); err != nil {
		t.Fatal(err)
	}
	if err := db.HandleHashField(hashFieldKey("k", ""), []byte("empty-name-value")); err != nil {
		t.Fatalf("empty field name must be accepted: %v", err)
	}
	if err := db.HandleHashField(hashFieldKey("k", "named"), []byte("named-value")); err != nil {
		t.Fatal(err)
	}
	if err := db.Finalize(); err != nil {
		t.Fatal(err)
	}
	got := readHashJSON(t, filepath.Join(root, "redis", "db_0", "hashes", "k.json"))
	fields := hashFieldArray(t, got)
	if len(fields) != 2 {
		t.Fatalf("len(fields) = %d, want 2", len(fields))
	}
	// The empty-string name sorts before "named"; the array form
	// preserves both as distinct records.
	if fields[0]["name"] != "" {
		t.Fatalf("fields[0].name = %v, want empty string", fields[0]["name"])
	}
	if fields[0]["value"] != "empty-name-value" {
		t.Fatalf("empty-name field value = %v", fields[0]["value"])
	}
	if fields[1]["name"] != "named" {
		t.Fatalf("fields[1].name = %v", fields[1]["name"])
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
