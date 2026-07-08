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

func encodeNewHLLValue(t *testing.T, value []byte, expireAtMs uint64) []byte {
	t.Helper()
	flags := byte(0)
	header := []byte{redisHLLMagic, redisHLLVersion, flags}
	body := value
	if expireAtMs > 0 {
		flags = redisHLLHasTTL
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

func TestRedisDB_HandleHLL_NewFormatStripsEnvelopeAndDeduplicatesTTL(t *testing.T) {
	t.Parallel()
	db, root := newRedisDB(t)
	sketch := []byte{0xde, 0xad, 0xbe, 0xef}
	if err := db.HandleHLL([]byte("uniques"), encodeNewHLLValue(t, sketch, fixedExpireMs)); err != nil {
		t.Fatalf("HandleHLL: %v", err)
	}
	if err := db.HandleTTL([]byte("uniques"), encodeTTLValue(fixedExpireMs)); err != nil {
		t.Fatalf("HandleTTL: %v", err)
	}
	if err := db.Finalize(); err != nil {
		t.Fatalf("Finalize: %v", err)
	}
	body := readBlob(t, filepath.Join(root, "redis", "db_0", "hll", "uniques.bin"))
	if !bytes.Equal(body, sketch) {
		t.Fatalf("hll blob = %x want %x", body, sketch)
	}
	recs := readTTLJSONL(t, filepath.Join(root, "redis", "db_0", "hll_ttl.jsonl"))
	if len(recs) != 1 {
		t.Fatalf("hll_ttl records = %d, want 1", len(recs))
	}
	if recs[0].Key != "uniques" || recs[0].ExpireAtMs != fixedExpireMs {
		t.Fatalf("hll_ttl[0] = %#v, want key uniques ms %d", recs[0], fixedExpireMs)
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

// TestRedisDB_EnsureDirRevalidatesAfterCachedSuccess is the regression
// for Codex P1 round 13 follow-up (PR #713 review #11): ensureDir's
// dirsCreated cache used to skip assertNoSymlinkAncestors on cache
// hits, so a directory that was safe on the first write could be
// swapped to a symlink between writes and subsequent HandleString
// calls would silently traverse it, redirecting blobs outside
// outRoot. The cache now short-circuits only MkdirAll; the ancestor
// check runs on every call.
func TestRedisDB_EnsureDirRevalidatesAfterCachedSuccess(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	bait := filepath.Join(root, "bait-tree")
	if err := os.MkdirAll(bait, 0o755); err != nil {
		t.Fatal(err)
	}
	db := NewRedisDB(root, 0)
	// First write seeds dirsCreated for <root>/redis/db_0/strings.
	if err := db.HandleString([]byte("k1"), encodeNewStringValue(t, []byte("v1"), 0)); err != nil {
		t.Fatalf("first HandleString: %v", err)
	}
	// Adversary swaps the cached real dir for a symlink to outside
	// outRoot. Without re-validation, the next write would follow
	// it and land in <bait>/.
	stringsDir := filepath.Join(root, "redis", "db_0", "strings")
	if err := os.RemoveAll(stringsDir); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(bait, stringsDir); err != nil {
		t.Fatal(err)
	}
	err := db.HandleString([]byte("k2"), encodeNewStringValue(t, []byte("v2"), 0))
	if err == nil || !strings.Contains(err.Error(), "refusing to traverse symlinked ancestor") {
		t.Fatalf("expected post-cache symlink refusal, got %v", err)
	}
	if _, statErr := os.Stat(filepath.Join(bait, "k2.bin")); !os.IsNotExist(statErr) {
		t.Fatalf("blob written through swapped-in symlink: stat err=%v", statErr)
	}
}

// TestRedisDB_RefusesSymlinkedAncestorOnTTLSidecar is the regression
// for Codex P1 round 13 (PR #713): writeBlob already routed through
// ensureDir/assertNoSymlinkAncestors, but TTL sidecars (appendTTL ->
// openJSONL) bypassed that guard because openJSONL only protected
// the final path element via openSidecarFile. A symlinked ancestor
// like `<outRoot>/redis/db_0 -> /tmp/outside` would then redirect
// strings_ttl.jsonl writes outside the dump root. appendTTL now
// calls ensureDir on the parent directory before opening the
// sidecar.
func TestRedisDB_RefusesSymlinkedAncestorOnTTLSidecar(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	bait := filepath.Join(root, "bait-tree")
	if err := os.MkdirAll(bait, 0o755); err != nil {
		t.Fatal(err)
	}
	parent := filepath.Join(root, "redis")
	if err := os.MkdirAll(parent, 0o755); err != nil {
		t.Fatal(err)
	}
	// Symlink `<outRoot>/redis/db_0` to a directory outside the
	// dump root. Without the ancestor guard, the first appendTTL
	// would happily write strings_ttl.jsonl into <bait>/.
	if err := os.Symlink(bait, filepath.Join(parent, "db_0")); err != nil {
		t.Fatal(err)
	}
	db := NewRedisDB(root, 0)
	// HandleString with a TTL-bearing value drives appendTTL.
	err := db.HandleString([]byte("k"), encodeNewStringValue(t, []byte("v"), fixedExpireMs))
	if err == nil || !strings.Contains(err.Error(), "refusing to traverse symlinked ancestor") {
		t.Fatalf("expected symlinked-ancestor refusal, got %v", err)
	}
	if _, statErr := os.Stat(filepath.Join(bait, "strings_ttl.jsonl")); !os.IsNotExist(statErr) {
		t.Fatalf("TTL sidecar written through ancestor symlink: stat err=%v", statErr)
	}
}

// TestRedisDB_RefusesSymlinkedAncestor is the regression for Codex P1
// round 9: O_NOFOLLOW only blocks the final-component symlink. A
// pre-existing directory symlink anywhere up the path (e.g.
// `<outRoot>/redis/db_0/strings -> /tmp/outside`) lets MkdirAll
// silently honor it and steers writeFileAtomic / openSidecarFile
// outside outRoot. ensureDir now Lstat-walks each ancestor under
// outRoot and refuses if any is a symlink.
func TestRedisDB_RefusesSymlinkedAncestor(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	// Pre-place the symlink trap before constructing the encoder so
	// the directory tree contains a poisoned ancestor at
	// <root>/redis/db_0/strings.
	bait := filepath.Join(root, "bait-tree")
	if err := os.MkdirAll(bait, 0o755); err != nil {
		t.Fatal(err)
	}
	parent := filepath.Join(root, "redis", "db_0")
	if err := os.MkdirAll(parent, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(bait, filepath.Join(parent, "strings")); err != nil {
		t.Fatal(err)
	}
	db := NewRedisDB(root, 0)
	err := db.HandleString([]byte("k"), encodeNewStringValue(t, []byte("v"), 0))
	if err == nil || !strings.Contains(err.Error(), "refusing to traverse symlinked ancestor") {
		t.Fatalf("expected symlinked-ancestor refusal, got %v", err)
	}
	// The symlink target must be untouched: no `k.bin` written.
	if _, statErr := os.Stat(filepath.Join(bait, "k.bin")); !os.IsNotExist(statErr) {
		t.Fatalf("blob written through ancestor symlink: stat err=%v", statErr)
	}
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

// warnFieldsContain returns true when fields includes the given
// even-indexed key. Helper for warn-sink assertions; avoids
// repeated `for i:=0; i<...; i+=2` in tests.
func warnFieldsContain(fields []any, key string) bool {
	for i := 0; i+1 < len(fields); i += 2 {
		if k, ok := fields[i].(string); ok && k == key {
			return true
		}
	}
	return false
}

// TestRedisDB_FinalizeWarnsOnOverflowOnlyEvenWhenOrphanCountZero
// pins the codex P2 fix (PR #790 round 13): Finalize must emit the
// orphan-ttl warning when pendingTTLOverflow > 0, independently of
// orphanTTLCount. Previously the warn sink only fired when
// orphanTTLCount > 0, so a caller that swallowed
// ErrPendingTTLBufferFull would lose the visibility signal for
// dropped expirations.
func TestRedisDB_FinalizeWarnsOnOverflowOnlyEvenWhenOrphanCountZero(t *testing.T) {
	t.Parallel()
	db, _ := newRedisDB(t)
	var events []string
	var lastFields []any
	db.WithWarnSink(func(event string, fields ...any) {
		events = append(events, event)
		lastFields = fields
	})
	// Small byte-cap to force overflow on the second key.
	const byteCap = 16 // single key "k0" costs 2+8=10; second key won't fit at 10+10=20>16.
	db.WithPendingTTLByteCap(byteCap)
	if err := db.HandleTTL([]byte("k0"), encodeTTLValue(1)); err != nil {
		t.Fatalf("first HandleTTL: %v", err)
	}
	// Second key fails closed — overflow++, orphan stays at 0
	// (entry never enters pendingTTL).
	if err := db.HandleTTL([]byte("k1"), encodeTTLValue(2)); !errors.Is(err, ErrPendingTTLBufferFull) {
		t.Fatalf("expected ErrPendingTTLBufferFull, got %v", err)
	}
	// Drain k0 via a wide-column state-init so orphanTTLCount ends
	// at 0; only pendingTTLOverflow remains > 0.
	if err := db.HandleSetMember(setMemberKey("k0", []byte("m")), nil); err != nil {
		t.Fatal(err)
	}
	if err := db.Finalize(); err != nil {
		t.Fatal(err)
	}
	assertPendingTTLState(t, "post-finalize", capturePendingTTLState(db),
		pendingTTLState{bytes: 0, mapLen: 0, overflow: 1})
	if db.orphanTTLCount != 0 {
		t.Fatalf("orphanTTLCount = %d, want 0 (drained)", db.orphanTTLCount)
	}
	if len(events) != 1 || events[0] != "redis_orphan_ttl" {
		t.Fatalf("events = %v want [redis_orphan_ttl] (overflow alone must trigger warn)", events)
	}
	if !warnFieldsContain(lastFields, "pending_ttl_buffer_overflow") {
		t.Fatalf("warn fields missing pending_ttl_buffer_overflow: %v", lastFields)
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
	// Codex P1 (PR #790): orphan TTL records are now BUFFERED in
	// pendingTTL during intake — the wide-column state-init
	// functions need to drain them when a typed record finally
	// registers a user key. The buffer holds (string-userKey,
	// uint64-expireAt) pairs; the per-record allocation cost is the
	// same as kindByKey's, which we already pay for every typed
	// record. The original Codex P2 round 6 concern (don't buffer
	// arbitrarily-large payload bytes) is preserved — we still
	// don't keep the value bytes.
	//
	// At Finalize, entries still in pendingTTL are counted as truly
	// unmatched orphans. This test now asserts:
	//   - During intake: orphanTTLCount stays at 0, pendingTTL grows.
	//   - After Finalize: orphanTTLCount == n (no typed record ever
	//     drained the entries).
	db, _ := newRedisDB(t)
	const n = 10_000
	for i := 0; i < n; i++ {
		key := []byte("orphan-" + intToDecimal(i))
		ms := uint64(i) + 1 //nolint:gosec // i bounded to n, never negative
		if err := db.HandleTTL(key, encodeTTLValue(ms)); err != nil {
			t.Fatalf("HandleTTL[%d]: %v", i, err)
		}
	}
	if db.orphanTTLCount != 0 {
		t.Fatalf("orphanTTLCount = %d at intake, want 0 (buffered)", db.orphanTTLCount)
	}
	if len(db.pendingTTL) != n {
		t.Fatalf("pendingTTL len = %d, want %d", len(db.pendingTTL), n)
	}
	if err := db.Finalize(); err != nil {
		t.Fatalf("Finalize: %v", err)
	}
	if db.orphanTTLCount != n {
		t.Fatalf("orphanTTLCount = %d after Finalize, want %d", db.orphanTTLCount, n)
	}
}

// TestRedisDB_PendingTTLFailsClosedAtByteCap pins the codex P1 fix
// on PR #790 round 6: the byte budget (not entry count) bounds
// pendingTTL memory. When an incoming entry's byte cost would
// exceed pendingTTLBytesCap, HandleTTL fails closed with
// ErrPendingTTLBufferFull rather than silently counting it as an
// orphan. Without this, an adversarial snapshot with a small
// number of 1 MiB unmatched TTL keys could still OOM the decoder
// — defeating the entry-count cap from r4/r5.
func TestRedisDB_PendingTTLFailsClosedAtByteCap(t *testing.T) {
	t.Parallel()
	// Each "orphan-N" key (N=0..7) is 8 bytes. With
	// pendingTTLEntryOverheadBytes=8, each entry costs 16 bytes.
	// 8 entries = 128 bytes; 9th would push to 144. Cap = 128 →
	// 8 fit, 9th fails closed.
	const byteCap = 128
	const entriesPerByteCap = 8
	db, _ := newRedisDB(t)
	db.WithPendingTTLByteCap(byteCap)
	for i := 0; i < entriesPerByteCap; i++ {
		key := []byte("orphan-" + intToDecimal(i))
		ms := uint64(i) + 1 //nolint:gosec // i bounded above
		if err := db.HandleTTL(key, encodeTTLValue(ms)); err != nil {
			t.Fatalf("HandleTTL[%d]: %v (should succeed within byte_cap)", i, err)
		}
	}
	if got := len(db.pendingTTL); got != entriesPerByteCap {
		t.Fatalf("pendingTTL len = %d, want %d", got, entriesPerByteCap)
	}
	if got := db.pendingTTLBytes; got != byteCap {
		t.Fatalf("pendingTTLBytes = %d, want %d", got, byteCap)
	}
	// One more entry should fail closed (would push to 144 > 128).
	err := db.HandleTTL([]byte("orphan-X"), encodeTTLValue(999))
	if !errors.Is(err, ErrPendingTTLBufferFull) {
		t.Fatalf("err = %v, want ErrPendingTTLBufferFull at byte_cap", err)
	}
	if got := db.pendingTTLOverflow; got != 1 {
		t.Fatalf("pendingTTLOverflow = %d, want 1", got)
	}
	if got := db.orphanTTLCount; got != 0 {
		t.Fatalf("orphanTTLCount = %d at intake, want 0 (failed closed)", got)
	}
}

// TestRedisDB_PendingTTLByteCapBoundedByLargeKey pins the
// large-key defense codex flagged on PR #790 round 6: a single
// 1 MiB key would have fit under any reasonable entry-count cap
// but exhausts a sensible byte budget. We use a synthetic small
// budget (64 bytes) and a 100-byte key to exercise the same logic
// without allocating a real 1 MiB blob.
func TestRedisDB_PendingTTLByteCapBoundedByLargeKey(t *testing.T) {
	t.Parallel()
	const byteCap = 64
	db, _ := newRedisDB(t)
	db.WithPendingTTLByteCap(byteCap)
	largeKey := make([]byte, 100) // 100 + 8 = 108 > 64
	for i := range largeKey {
		largeKey[i] = byte(i % 256)
	}
	err := db.HandleTTL(largeKey, encodeTTLValue(1))
	if !errors.Is(err, ErrPendingTTLBufferFull) {
		t.Fatalf("err = %v, want ErrPendingTTLBufferFull on oversize key", err)
	}
	if got := len(db.pendingTTL); got != 0 {
		t.Fatalf("pendingTTL must be empty after failed insert, got %d", got)
	}
}

// TestRedisDB_PendingTTLByteBudgetReclaimedOnClaim pins that the
// byte counter is decremented when an entry is drained via
// claimPendingTTL. A later wide-column state-init that drains the
// buffer must free byte budget so subsequent unknown-kind TTLs can
// be buffered again.
func TestRedisDB_PendingTTLByteBudgetReclaimedOnClaim(t *testing.T) {
	t.Parallel()
	db, _ := newRedisDB(t)
	const byteCap = 32 // ("k0"=2 + 8) * 3 = 30 entries fit; 4th would overflow at 40
	db.WithPendingTTLByteCap(byteCap)
	for i := 0; i < 3; i++ {
		if err := db.HandleTTL([]byte("k"+intToDecimal(i)), encodeTTLValue(1)); err != nil {
			t.Fatalf("HandleTTL[%d]: %v", i, err)
		}
	}
	if got := db.pendingTTLBytes; got != 30 {
		t.Fatalf("pendingTTLBytes = %d, want 30", got)
	}
	// Drain k0 via a wide-column state-init.
	if err := db.HandleSetMember(setMemberKey("k0", []byte("m")), nil); err != nil {
		t.Fatal(err)
	}
	if got := db.pendingTTLBytes; got != 20 {
		t.Fatalf("pendingTTLBytes after drain = %d, want 20 (reclaimed 10 bytes)", got)
	}
	// New buffer space available — another small entry fits.
	if err := db.HandleTTL([]byte("k3"), encodeTTLValue(1)); err != nil {
		t.Fatalf("HandleTTL after drain failed: %v", err)
	}
}

// pendingTTLState snapshots the in-flight buffer counters at one
// observation point.
type pendingTTLState struct {
	bytes    int
	mapLen   int
	overflow int
}

func capturePendingTTLState(db *RedisDB) pendingTTLState {
	return pendingTTLState{
		bytes:    db.pendingTTLBytes,
		mapLen:   len(db.pendingTTL),
		overflow: db.pendingTTLOverflow,
	}
}

func assertPendingTTLState(t *testing.T, label string, got, want pendingTTLState) {
	t.Helper()
	if got != want {
		t.Fatalf("%s: got %+v, want %+v", label, got, want)
	}
}

// TestRedisDB_PendingTTLDuplicateKeyDoesNotDoubleCountBytes pins
// the codex P2 fix (PR #790 round 7): when parkUnknownTTL receives
// the same userKey more than once before the key is claimed, the
// map entry overwrites in place (latest-wins) and the byte counter
// stays unchanged. A duplicate at byte-budget boundary must not
// spuriously fire ErrPendingTTLBufferFull.
func TestRedisDB_PendingTTLDuplicateKeyDoesNotDoubleCountBytes(t *testing.T) {
	t.Parallel()
	db, _ := newRedisDB(t)
	const byteCap = 20 // single key "k0" costs 2+8 = 10 bytes; cap fits exactly 2 entries.
	db.WithPendingTTLByteCap(byteCap)

	if err := db.HandleTTL([]byte("k0"), encodeTTLValue(1)); err != nil {
		t.Fatalf("first HandleTTL: %v", err)
	}
	assertPendingTTLState(t, "after first", capturePendingTTLState(db),
		pendingTTLState{bytes: 10, mapLen: 1, overflow: 0})

	// Replay the same userKey with a later expiry — must overwrite,
	// not inflate the byte counter.
	if err := db.HandleTTL([]byte("k0"), encodeTTLValue(2)); err != nil {
		t.Fatalf("duplicate HandleTTL: %v (must succeed; map size unchanged)", err)
	}
	assertPendingTTLState(t, "after duplicate", capturePendingTTLState(db),
		pendingTTLState{bytes: 10, mapLen: 1, overflow: 0})
	if got := db.pendingTTL["k0"]; got != 2 {
		t.Fatalf("pendingTTL[k0] = %d, want latest-wins value 2", got)
	}

	// Fill the cap with a second key.
	if err := db.HandleTTL([]byte("k1"), encodeTTLValue(1)); err != nil {
		t.Fatalf("second-key HandleTTL: %v", err)
	}
	assertPendingTTLState(t, "after fill", capturePendingTTLState(db),
		pendingTTLState{bytes: 20, mapLen: 2, overflow: 0})

	// At exact cap; duplicate of an existing key must still succeed.
	if err := db.HandleTTL([]byte("k0"), encodeTTLValue(3)); err != nil {
		t.Fatalf("duplicate at-cap HandleTTL: %v (must succeed; was P2 regression)", err)
	}
	assertPendingTTLState(t, "after duplicate at-cap", capturePendingTTLState(db),
		pendingTTLState{bytes: 20, mapLen: 2, overflow: 0})
}

// TestRedisDB_WithPendingTTLByteCapZeroOpts pins the explicit
// counter-only mode: byte_cap==0 disables buffering entirely and
// every unknown-kind TTL becomes an immediate orphan WITHOUT
// firing ErrPendingTTLBufferFull.
func TestRedisDB_WithPendingTTLByteCapZeroOpts(t *testing.T) {
	t.Parallel()
	db, _ := newRedisDB(t)
	db.WithPendingTTLByteCap(0)
	const n = 5
	for i := 0; i < n; i++ {
		if err := db.HandleTTL([]byte("k"+intToDecimal(i)), encodeTTLValue(1)); err != nil {
			t.Fatalf("HandleTTL[%d]: %v (counter-only mode must not fail)", i, err)
		}
	}
	if got := len(db.pendingTTL); got != 0 {
		t.Fatalf("pendingTTL len = %d, want 0 (cap disabled)", got)
	}
	if got := db.orphanTTLCount; got != n {
		t.Fatalf("orphanTTLCount = %d, want %d", got, n)
	}
}

// TestRedisDB_WithPendingTTLByteCapNegativeCoercedToZero pins
// input sanitisation.
func TestRedisDB_WithPendingTTLByteCapNegativeCoercedToZero(t *testing.T) {
	t.Parallel()
	db, _ := newRedisDB(t)
	db.WithPendingTTLByteCap(-100)
	if db.pendingTTLBytesCap != 0 {
		t.Fatalf("pendingTTLBytesCap = %d after WithPendingTTLByteCap(-100), want 0", db.pendingTTLBytesCap)
	}
}

// TestRedisDB_DefaultPendingTTLByteCapSurvives1GiBLoad pins the
// codex P1 fix (PR #790 round 8): the default cap must comfortably
// accommodate "millions of expiring wide-column keys" without
// hitting ErrPendingTTLBufferFull. Specifically the old 64 MiB
// default would refuse a 10M-expiring-wide-column-key snapshot
// with average 50-byte keys (cost = 10M * 58 = 580 MB); the new
// 1 GiB default tolerates that workload.
func TestRedisDB_DefaultPendingTTLByteCapSurvives1GiBLoad(t *testing.T) {
	t.Parallel()
	// Sentinel constant invariant: the default must be exactly 1 GiB.
	// Below 64 MiB would resurrect the round-6 regression; above
	// ~4 GiB risks raising the OOM ceiling beyond typical dump-host
	// budget headroom.
	const wantDefault = 1 << 30
	if defaultPendingTTLBytesCap != wantDefault {
		t.Fatalf("defaultPendingTTLBytesCap = %d, want %d (1 GiB)", defaultPendingTTLBytesCap, wantDefault)
	}
	db, _ := newRedisDB(t)
	if db.pendingTTLBytesCap != wantDefault {
		t.Fatalf("NewRedisDB pendingTTLBytesCap = %d, want %d", db.pendingTTLBytesCap, wantDefault)
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
