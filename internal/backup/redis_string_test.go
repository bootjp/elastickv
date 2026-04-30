package backup

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"

	"github.com/cockroachdb/errors"
)

// fixedExpireMs is a 2026-04-29 00:00:00Z epoch-ms used in fixtures so the
// asserted values do not drift with wall time.
const fixedExpireMs uint64 = 1788_998_400_000

func newRedisDB(t *testing.T) (*RedisDB, string) {
	t.Helper()
	root := t.TempDir()
	return NewRedisDB(root, 0), root
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

// TestRedisDB_NewFormatStringTTLNotDuplicatedByScanIndex is the regression
// for Codex P1 round 5: the live Redis encoder emits both `!redis|str|<k>`
// (with TTL embedded inline in the magic-prefix header) and the scan-index
// `!redis|ttl|<k>` for every expiring string. The backup decoder must
// recognise that HandleString already wrote the strings_ttl.jsonl record
// and drop the redundant !redis|ttl| record. Otherwise the same expiring
// string is duplicated in the sidecar, breaking the one-record-per-key
// contract.
func TestRedisDB_NewFormatStringTTLNotDuplicatedByScanIndex(t *testing.T) {
	t.Parallel()
	db, root := newRedisDB(t)
	mustNoErr := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	// Snapshot lex order: !redis|str|<k> comes before !redis|ttl|<k>
	// (because 's' < 't'). Mirror that sequence here.
	mustNoErr(db.HandleString([]byte("expiring"), encodeNewStringValue(t, []byte("v"), fixedExpireMs)))
	mustNoErr(db.HandleTTL([]byte("expiring"), encodeTTLValue(fixedExpireMs)))
	mustNoErr(db.Finalize())
	assertTTLSidecar(t, filepath.Join(root, "redis", "db_0", "strings_ttl.jsonl"), "expiring", fixedExpireMs)
}

// TestRedisDB_OpenJSONLRefusesHardLinkClobber is the regression for
// Codex P2 round 9: O_NOFOLLOW only blocks symlinks; an adversary
// who can write the output directory could pre-create
// strings_ttl.jsonl as a hard link to a file outside the dump tree
// (e.g. /etc/passwd) and the open's O_TRUNC would clobber that
// inode. openSidecarFile now opens WITHOUT O_TRUNC, fstat()s the
// descriptor, refuses if Nlink > 1, and only calls Truncate(0) on
// the verified-single-link case.
func TestRedisDB_OpenJSONLRefusesHardLinkClobber(t *testing.T) {
	t.Parallel()
	db, root := newRedisDB(t)
	dir := filepath.Join(root, "redis", "db_0")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	bait := filepath.Join(root, "bait-hardlink")
	if err := os.WriteFile(bait, []byte("stay-out"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Link(bait, filepath.Join(dir, redisStringsTTLFile)); err != nil {
		t.Fatal(err)
	}
	err := db.HandleString([]byte("k"), encodeNewStringValue(t, []byte("v"), fixedExpireMs))
	if err == nil || !strings.Contains(err.Error(), "refusing to overwrite hard-linked file") {
		t.Fatalf("expected hard-link refusal error from openSidecarFile, got %v", err)
	}
	if got, _ := os.ReadFile(bait); string(got) != "stay-out" { //nolint:gosec // test path
		t.Fatalf("bait file written through hard link: %q", got)
	}
}

// TestRedisDB_OpenJSONLRefusesSymlinkOverwrite is the regression for Codex
// P2 round 5 + P1 round 6: openJSONL must atomically refuse to follow
// symlinks. The earlier Lstat-then-Create variant left a TOCTOU window
// where a process that could write the output directory could swap the
// path to a symlink between the two syscalls; the round-6 fix uses
// O_NOFOLLOW on the open itself so the kernel returns ELOOP atomically.
func TestRedisDB_OpenJSONLRefusesSymlinkOverwrite(t *testing.T) {
	t.Parallel()
	db, root := newRedisDB(t)
	dir := filepath.Join(root, "redis", "db_0")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	bait := filepath.Join(root, "bait-jsonl")
	if err := os.WriteFile(bait, []byte("stay-out"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(bait, filepath.Join(dir, redisStringsTTLFile)); err != nil {
		t.Fatal(err)
	}
	// HandleString with TTL triggers the first openJSONL on
	// strings_ttl.jsonl, which must refuse the symlink rather than
	// truncate the bait target.
	err := db.HandleString([]byte("k"), encodeNewStringValue(t, []byte("v"), fixedExpireMs))
	if err == nil || !strings.Contains(err.Error(), "refusing to overwrite symlink") {
		t.Fatalf("expected symlink-refusal error from openJSONL, got %v", err)
	}
	if got, _ := os.ReadFile(bait); string(got) != "stay-out" { //nolint:gosec // test path
		t.Fatalf("bait file written through symlink: %q", got)
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

func TestRedisDB_PerDBIndexRoutesIntoOwnDirectory(t *testing.T) {
	t.Parallel()
	// Two encoders with the same outRoot but different db indices
	// must not collide. The previous hardcoded "db_0" path would
	// have routed both to the same blob file.
	root := t.TempDir()
	db0 := NewRedisDB(root, 0)
	db3 := NewRedisDB(root, 3)
	if err := db0.HandleString([]byte("k"), encodeNewStringValue(t, []byte("v0"), 0)); err != nil {
		t.Fatal(err)
	}
	if err := db3.HandleString([]byte("k"), encodeNewStringValue(t, []byte("v3"), 0)); err != nil {
		t.Fatal(err)
	}
	if err := db0.Finalize(); err != nil {
		t.Fatal(err)
	}
	if err := db3.Finalize(); err != nil {
		t.Fatal(err)
	}
	if got := readBlob(t, filepath.Join(root, "redis", "db_0", "strings", "k.bin")); string(got) != "v0" {
		t.Fatalf("db_0 blob = %q want %q", got, "v0")
	}
	if got := readBlob(t, filepath.Join(root, "redis", "db_3", "strings", "k.bin")); string(got) != "v3" {
		t.Fatalf("db_3 blob = %q want %q", got, "v3")
	}
}

// TestRedisDB_SHAFallbackKeymapped is the regression for Codex P1
// round 7: when a Redis user key is long enough that EncodeSegment
// takes its SHA-fallback path, the encoder must record a KEYMAP.jsonl
// entry for it. Otherwise the encoded `*.bin` filename and the JSONL
// TTL row's `key` are non-reversible and the original Redis user key
// bytes are irrecoverable from a backup.
func TestRedisDB_SHAFallbackKeymapped(t *testing.T) {
	t.Parallel()
	db, root := newRedisDB(t)
	// Drive a key whose length forces the fallback. EncodeSegment's
	// length cap is 240 bytes; pad past it with characters that
	// would percent-encode to 3× their length so we cannot
	// accidentally fit even with all-unreserved bytes.
	longKey := bytes.Repeat([]byte{'%'}, 300)
	encoded := EncodeSegment(longKey)
	if !IsShaFallback(encoded) {
		t.Fatalf("test premise broken: encoded %q is not a SHA fallback", encoded)
	}
	if err := db.HandleString(longKey, encodeNewStringValue(t, []byte("v"), fixedExpireMs)); err != nil {
		t.Fatalf("HandleString: %v", err)
	}
	if err := db.Finalize(); err != nil {
		t.Fatalf("Finalize: %v", err)
	}
	keymapPath := filepath.Join(root, "redis", "db_0", "KEYMAP.jsonl")
	f, err := os.Open(keymapPath) //nolint:gosec // test-controlled path
	if err != nil {
		t.Fatalf("KEYMAP.jsonl missing for SHA-fallback key: %v", err)
	}
	defer func() { _ = f.Close() }()
	got, err := LoadKeymap(f)
	if err != nil {
		t.Fatalf("LoadKeymap: %v", err)
	}
	rec, ok := got[encoded]
	if !ok {
		t.Fatalf("no keymap record for encoded %q; have %v", encoded, got)
	}
	if rec.Kind != KindSHAFallback {
		t.Fatalf("kind = %q, want %q", rec.Kind, KindSHAFallback)
	}
	orig, err := rec.Original()
	if err != nil {
		t.Fatalf("Original: %v", err)
	}
	if !bytes.Equal(orig, longKey) {
		t.Fatalf("Original mismatch: len got=%d want=%d", len(orig), len(longKey))
	}
}

// TestRedisDB_NoKeymapWhenAllReversible asserts the converse: a dump
// with only short keys produces no KEYMAP.jsonl. The empty-file
// removal in closeKeymap matches the s3 encoder's policy.
func TestRedisDB_NoKeymapWhenAllReversible(t *testing.T) {
	t.Parallel()
	db, root := newRedisDB(t)
	if err := db.HandleString([]byte("short"), encodeNewStringValue(t, []byte("v"), 0)); err != nil {
		t.Fatalf("HandleString: %v", err)
	}
	if err := db.Finalize(); err != nil {
		t.Fatalf("Finalize: %v", err)
	}
	keymapPath := filepath.Join(root, "redis", "db_0", "KEYMAP.jsonl")
	if _, err := os.Stat(keymapPath); !os.IsNotExist(err) {
		t.Fatalf("KEYMAP.jsonl present without any fallback keys: stat err=%v", err)
	}
}

func TestRedisDB_OrphanTTLCountedNotBuffered(t *testing.T) {
	t.Parallel()
	// Codex P2 round 6: orphan TTL records (those with no prior
	// HandleString/HandleHLL claim) must be counted only — the
	// per-key payload would allocate proportional to user-key size
	// and is unused before the wide-column encoders land. Drive a
	// sample of orphan records and assert the count, not a buffer.
	db, _ := newRedisDB(t)
	const n = 10_000
	for i := 0; i < n; i++ {
		key := []byte("orphan-" + intToDecimal(i))
		ms := uint64(i) + 1 //nolint:gosec // i bounded to n, never negative
		if err := db.HandleTTL(key, encodeTTLValue(ms)); err != nil {
			t.Fatalf("HandleTTL[%d]: %v", i, err)
		}
	}
	if db.orphanTTLCount != n {
		t.Fatalf("orphanTTLCount = %d, want %d", db.orphanTTLCount, n)
	}
}

func TestRedisDB_DirsCreatedCachesMkdirAll(t *testing.T) {
	t.Parallel()
	// Two HandleString calls in a row should populate dirsCreated
	// once for the strings/ subdir and skip MkdirAll on the second
	// call. Black-box: assert the dirsCreated map contains the
	// strings/ entry exactly once after two writes.
	db, _ := newRedisDB(t)
	if err := db.HandleString([]byte("a"), encodeNewStringValue(t, []byte("v"), 0)); err != nil {
		t.Fatal(err)
	}
	if err := db.HandleString([]byte("b"), encodeNewStringValue(t, []byte("v"), 0)); err != nil {
		t.Fatal(err)
	}
	wantSubstr := filepath.Join("redis", "db_0", "strings")
	count := 0
	for k := range db.dirsCreated {
		if strings.Contains(k, wantSubstr) {
			count++
		}
	}
	if count != 1 {
		t.Fatalf("strings/ dir entries = %d want 1; map=%v", count, db.dirsCreated)
	}
}

func TestRedisDB_IsBlobAtomicWriteOutOfSpace(t *testing.T) {
	t.Parallel()
	if !IsBlobAtomicWriteOutOfSpace(syscall.ENOSPC) {
		t.Fatalf("ENOSPC must be reported as out-of-space")
	}
	if !IsBlobAtomicWriteOutOfSpace(cockroachdbErrorsWrap(syscall.ENOSPC)) {
		t.Fatalf("wrapped ENOSPC must round-trip via errors.Is")
	}
	if IsBlobAtomicWriteOutOfSpace(io.ErrShortWrite) {
		t.Fatalf("ErrShortWrite must NOT be reported as out-of-space")
	}
	// Conversely IsBlobAtomicWriteRetriable must NOT report ENOSPC.
	if IsBlobAtomicWriteRetriable(syscall.ENOSPC) {
		t.Fatalf("ENOSPC must NOT be retriable")
	}
}

func cockroachdbErrorsWrap(err error) error {
	return errors.Join(errors.New("wrapped"), err)
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
