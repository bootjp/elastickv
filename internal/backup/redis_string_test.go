package backup

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/cockroachdb/errors"
)

// fixedExpireMs is a 2026-04-29 00:00:00Z epoch-ms used in fixtures so the
// asserted values do not drift with wall time.
const fixedExpireMs uint64 = 1788_998_400_000

func newRedisDB(t *testing.T) (*RedisDB, string) {
	t.Helper()
	root := t.TempDir()
	return NewRedisDB(root), root
}

func encodeNewStringValue(t *testing.T, value []byte, expireAtMs uint64) []byte {
	t.Helper()
	flags := byte(0)
	header := []byte{redisStrMagic, redisStrVersion, flags}
	body := value
	if expireAtMs > 0 {
		flags = redisStrHasTTL
		header[2] = flags
		var ttl [redisUint64Bytes]byte
		binary.BigEndian.PutUint64(ttl[:], expireAtMs)
		header = append(header, ttl[:]...)
	}
	return append(header, body...)
}

func encodeTTLValue(expireAtMs uint64) []byte {
	var b [redisUint64Bytes]byte
	binary.BigEndian.PutUint64(b[:], expireAtMs)
	return b[:]
}

func readBlob(t *testing.T, path string) []byte {
	t.Helper()
	b, err := os.ReadFile(path) //nolint:gosec // test path
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return b
}

type ttlRecord struct {
	Key        string `json:"key"`
	ExpireAtMs uint64 `json:"expire_at_ms"`
}

func readTTLJSONL(t *testing.T, path string) []ttlRecord {
	t.Helper()
	f, err := os.Open(path) //nolint:gosec // test path
	if err != nil {
		t.Fatalf("open %s: %v", path, err)
	}
	defer f.Close()
	var out []ttlRecord
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		var r ttlRecord
		if err := json.Unmarshal(sc.Bytes(), &r); err != nil {
			t.Fatalf("unmarshal %q: %v", sc.Text(), err)
		}
		out = append(out, r)
	}
	if err := sc.Err(); err != nil {
		t.Fatalf("scan: %v", err)
	}
	return out
}

func TestRedisDB_HandleString_NewFormatNoTTL(t *testing.T) {
	t.Parallel()
	db, root := newRedisDB(t)
	val := encodeNewStringValue(t, []byte("hello"), 0)
	if err := db.HandleString([]byte("greeting"), val); err != nil {
		t.Fatalf("HandleString: %v", err)
	}
	if err := db.Finalize(); err != nil {
		t.Fatalf("Finalize: %v", err)
	}
	body := readBlob(t, filepath.Join(root, "redis", "db_0", "strings", "greeting.bin"))
	if string(body) != "hello" {
		t.Fatalf("blob = %q want %q", body, "hello")
	}
	// No TTL → strings_ttl.jsonl must not exist (omit empty).
	if _, err := os.Stat(filepath.Join(root, "redis", "db_0", "strings_ttl.jsonl")); !os.IsNotExist(err) {
		t.Fatalf("expected no strings_ttl.jsonl, stat err=%v", err)
	}
}

func TestRedisDB_HandleString_NewFormatWithInlineTTL(t *testing.T) {
	t.Parallel()
	db, root := newRedisDB(t)
	val := encodeNewStringValue(t, []byte("expiring"), fixedExpireMs)
	if err := db.HandleString([]byte("session:abc"), val); err != nil {
		t.Fatalf("HandleString: %v", err)
	}
	if err := db.Finalize(); err != nil {
		t.Fatalf("Finalize: %v", err)
	}
	body := readBlob(t, filepath.Join(root, "redis", "db_0", "strings", "session%3Aabc.bin"))
	if string(body) != "expiring" {
		t.Fatalf("blob = %q want %q", body, "expiring")
	}
	recs := readTTLJSONL(t, filepath.Join(root, "redis", "db_0", "strings_ttl.jsonl"))
	if len(recs) != 1 {
		t.Fatalf("ttl records = %d, want 1", len(recs))
	}
	if recs[0].Key != "session%3Aabc" {
		t.Fatalf("ttl key = %q", recs[0].Key)
	}
	if recs[0].ExpireAtMs != fixedExpireMs {
		t.Fatalf("ttl ms = %d want %d", recs[0].ExpireAtMs, fixedExpireMs)
	}
}

func TestRedisDB_HandleString_LegacyFormatTreatedAsRawValue(t *testing.T) {
	t.Parallel()
	db, root := newRedisDB(t)
	// Legacy (no magic prefix): bytes are the user value verbatim.
	if err := db.HandleString([]byte("legacy"), []byte("\x00\xff\x01raw")); err != nil {
		t.Fatalf("HandleString: %v", err)
	}
	if err := db.Finalize(); err != nil {
		t.Fatalf("Finalize: %v", err)
	}
	body := readBlob(t, filepath.Join(root, "redis", "db_0", "strings", "legacy.bin"))
	if string(body) != "\x00\xff\x01raw" {
		t.Fatalf("blob bytes = %x", body)
	}
}

func TestRedisDB_HandleHLL_WritesRawSketch(t *testing.T) {
	t.Parallel()
	db, root := newRedisDB(t)
	sketch := []byte{0xde, 0xad, 0xbe, 0xef}
	if err := db.HandleHLL([]byte("uniques"), sketch); err != nil {
		t.Fatalf("HandleHLL: %v", err)
	}
	if err := db.Finalize(); err != nil {
		t.Fatalf("Finalize: %v", err)
	}
	body := readBlob(t, filepath.Join(root, "redis", "db_0", "hll", "uniques.bin"))
	if string(body) != string(sketch) {
		t.Fatalf("hll blob = %x want %x", body, sketch)
	}
}

func assertTTLSidecar(t *testing.T, path string, wantKey string, wantMs uint64) {
	t.Helper()
	recs := readTTLJSONL(t, path)
	if len(recs) != 1 {
		t.Fatalf("%s: %d records, want 1", path, len(recs))
	}
	if recs[0].Key != wantKey {
		t.Fatalf("%s: key %q want %q", path, recs[0].Key, wantKey)
	}
	if recs[0].ExpireAtMs != wantMs {
		t.Fatalf("%s: ms %d want %d", path, recs[0].ExpireAtMs, wantMs)
	}
}

func TestRedisDB_HandleTTL_RoutesByPriorTypeObservation(t *testing.T) {
	t.Parallel()
	db, root := newRedisDB(t)
	mustNoErr := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	// HLL key first, then string key, then TTL records (lex order in
	// snapshot: hll < str < ttl).
	mustNoErr(db.HandleHLL([]byte("hll-key"), []byte{0x01}))
	mustNoErr(db.HandleString([]byte("legacy-str"), []byte("legacy-raw")))
	mustNoErr(db.HandleTTL([]byte("hll-key"), encodeTTLValue(fixedExpireMs)))
	mustNoErr(db.HandleTTL([]byte("legacy-str"), encodeTTLValue(fixedExpireMs+1)))
	mustNoErr(db.Finalize())

	assertTTLSidecar(t, filepath.Join(root, "redis", "db_0", "hll_ttl.jsonl"), "hll-key", fixedExpireMs)
	assertTTLSidecar(t, filepath.Join(root, "redis", "db_0", "strings_ttl.jsonl"), "legacy-str", fixedExpireMs+1)
}

func TestRedisDB_HandleTTL_OrphanWarnsOnFinalize(t *testing.T) {
	t.Parallel()
	db, _ := newRedisDB(t)
	var events []string
	db.WithWarnSink(func(event string, fields ...any) {
		events = append(events, event)
	})
	// TTL for a key never claimed by HandleString or HandleHLL — likely
	// belongs to a wide-column type (hash/list/set/zset/stream) whose
	// encoder has not landed yet. Must not crash.
	if err := db.HandleTTL([]byte("orphan"), encodeTTLValue(fixedExpireMs)); err != nil {
		t.Fatal(err)
	}
	if err := db.Finalize(); err != nil {
		t.Fatal(err)
	}
	if len(events) != 1 || events[0] != "redis_orphan_ttl" {
		t.Fatalf("events = %v want [redis_orphan_ttl]", events)
	}
}

func TestRedisDB_RejectsTruncatedNewFormat(t *testing.T) {
	t.Parallel()
	cases := [][]byte{
		{redisStrMagic, redisStrVersion},                                   // header truncated (missing flags)
		{redisStrMagic, redisStrVersion, redisStrHasTTL, 0x00, 0x00, 0x00}, // ttl section truncated
	}
	for _, raw := range cases {
		db, _ := newRedisDB(t)
		err := db.HandleString([]byte("k"), raw)
		if !errors.Is(err, ErrRedisInvalidStringValue) {
			t.Fatalf("err=%v want ErrRedisInvalidStringValue (input %x)", err, raw)
		}
	}
}

func TestRedisDB_HandleTTL_RejectsBadLength(t *testing.T) {
	t.Parallel()
	db, _ := newRedisDB(t)
	err := db.HandleTTL([]byte("k"), []byte{0x01, 0x02})
	if !errors.Is(err, ErrRedisInvalidTTLValue) {
		t.Fatalf("err=%v want ErrRedisInvalidTTLValue", err)
	}
}

func TestRedisDB_FilenamesGoThroughEncodeSegment(t *testing.T) {
	t.Parallel()
	db, root := newRedisDB(t)
	// User key with reserved bytes. Filename encoding must match the
	// EncodeSegment contract (verified by filename_test.go).
	if err := db.HandleString([]byte("a/b:c"), encodeNewStringValue(t, []byte("v"), 0)); err != nil {
		t.Fatal(err)
	}
	if err := db.Finalize(); err != nil {
		t.Fatal(err)
	}
	want := filepath.Join(root, "redis", "db_0", "strings", "a%2Fb%3Ac.bin")
	if _, err := os.Stat(want); err != nil {
		t.Fatalf("expected file %s, stat err=%v", want, err)
	}
}

func TestRedisDB_AtomicWriteRefusesSymlinkOverwrite(t *testing.T) {
	t.Parallel()
	db, root := newRedisDB(t)
	dir := filepath.Join(root, "redis", "db_0", "strings")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	target := filepath.Join(dir, "victim.bin")
	bait := filepath.Join(root, "bait")
	if err := os.WriteFile(bait, []byte("stay-out"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(bait, target); err != nil {
		t.Fatal(err)
	}
	err := db.HandleString([]byte("victim"), encodeNewStringValue(t, []byte("attack"), 0))
	if err == nil || !strings.Contains(err.Error(), "refusing to overwrite symlink") {
		t.Fatalf("expected symlink-refusal error, got %v", err)
	}
	// Bait file must be untouched.
	if got, _ := os.ReadFile(bait); string(got) != "stay-out" { //nolint:gosec // test path
		t.Fatalf("bait file written through symlink: %q", got)
	}
}

func TestRedisDB_HasInlineTTL(t *testing.T) {
	t.Parallel()
	if !HasInlineTTL(encodeNewStringValue(t, []byte("v"), fixedExpireMs)) {
		t.Fatalf("HasInlineTTL = false on inline-TTL value")
	}
	if HasInlineTTL(encodeNewStringValue(t, []byte("v"), 0)) {
		t.Fatalf("HasInlineTTL = true on no-TTL value")
	}
	if HasInlineTTL([]byte("legacy-raw")) {
		t.Fatalf("HasInlineTTL = true on legacy value")
	}
}
