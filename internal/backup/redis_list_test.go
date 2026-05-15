package backup

import (
	"encoding/binary"
	"encoding/json"
	"math"
	"os"
	"path/filepath"
	"testing"

	"github.com/cockroachdb/errors"
)

// listMetaValue builds the 24-byte (Head, Tail, Len) blob the live
// store/list_helpers.go emits at !lst|meta|<userKey>. Tail = Head+Len
// is the live store's invariant; we honour it so the decoder sees the
// same shape it would see in a real snapshot.
func listMetaValue(head, length int64) []byte {
	buf := make([]byte, 24)
	binary.BigEndian.PutUint64(buf[0:8], uint64(head))         //nolint:gosec
	binary.BigEndian.PutUint64(buf[8:16], uint64(head+length)) //nolint:gosec
	binary.BigEndian.PutUint64(buf[16:24], uint64(length))     //nolint:gosec
	return buf
}

// listMetaKey is the test-side mirror of store.ListMetaKey:
// !lst|meta|<userKey> (no length prefix).
func listMetaKey(userKey string) []byte {
	return append([]byte(ListMetaPrefix), userKey...)
}

// listItemKey is the test-side mirror of store.ListItemKey:
// !lst|itm|<userKey><sortable_seq(8)>. The sortable encoding flips the
// sign bit so a forward byte-ordered scan yields ascending int64.
func listItemKey(userKey string, seq int64) []byte {
	out := append([]byte(ListItemPrefix), userKey...)
	var s [8]byte
	binary.BigEndian.PutUint64(s[:], uint64(seq^math.MinInt64)) //nolint:gosec // sortable-int64 sign-flip
	return append(out, s[:]...)
}

// listMetaDeltaKey mirrors store.ListMetaDeltaKey:
// !lst|meta|d|<userKeyLen(4)><userKey><commitTS(8)><seqInTxn(4)>.
// The shape is irrelevant to the encoder (it skips deltas), but we
// build a well-formed key here so the dispatcher integration test
// would exercise the same byte sequence the live store emits.
func listMetaDeltaKey(userKey string, commitTS uint64, seqInTxn uint32) []byte {
	out := []byte(ListMetaDeltaPrefix)
	var l [4]byte
	binary.BigEndian.PutUint32(l[:], uint32(len(userKey))) //nolint:gosec
	out = append(out, l[:]...)
	out = append(out, userKey...)
	var ts [8]byte
	binary.BigEndian.PutUint64(ts[:], commitTS)
	out = append(out, ts[:]...)
	var seq [4]byte
	binary.BigEndian.PutUint32(seq[:], seqInTxn)
	return append(out, seq[:]...)
}

// listClaimKey mirrors store.ListClaimKey:
// !lst|claim|<userKeyLen(4)><userKey><sortable_seq(8)>.
func listClaimKey(userKey string, seq int64) []byte {
	out := []byte(ListClaimPrefix)
	var l [4]byte
	binary.BigEndian.PutUint32(l[:], uint32(len(userKey))) //nolint:gosec
	out = append(out, l[:]...)
	out = append(out, userKey...)
	var s [8]byte
	binary.BigEndian.PutUint64(s[:], uint64(seq^math.MinInt64)) //nolint:gosec // sortable-int64 sign-flip
	return append(out, s[:]...)
}

func readListJSON(t *testing.T, path string) map[string]any {
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

func listItemsArray(t *testing.T, m map[string]any) []any {
	t.Helper()
	v, ok := m["items"]
	if !ok {
		t.Fatalf("items missing in %+v", m)
	}
	raw, ok := v.([]any)
	if !ok {
		t.Fatalf("items = %T(%v), want []any", v, v)
	}
	return raw
}

// listFloat fetches a numeric JSON field with a type-asserting helper
// that fails loudly with the field name (mirrors hashFloat).
func listFloat(t *testing.T, m map[string]any, key string) float64 {
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

// TestRedisDB_ListRoundTripPreservesLPUSHOrder pins the design's
// "Order is left-to-right" contract: LPUSH pushes negative seqs that
// sort first in the ascending-seq output, so the leftmost element is
// the one with the smallest seq. A round-trip with seqs {-2, -1, 0,
// 1} must surface as [b, c, d, e] in items order — regardless of the
// order the dispatcher hands us the !lst|itm| records.
func TestRedisDB_ListRoundTripPreservesLPUSHOrder(t *testing.T) {
	t.Parallel()
	db, root := newRedisDB(t)
	if err := db.HandleListMeta(listMetaKey("q"), listMetaValue(-2, 4)); err != nil {
		t.Fatalf("HandleListMeta: %v", err)
	}
	// Deliberately submit items out of order to exercise the
	// ascending-seq sort at flush time.
	submitListItems(t, db, "q", map[int64]string{1: "e", -1: "c", -2: "b", 0: "d"})
	if err := db.Finalize(); err != nil {
		t.Fatalf("Finalize: %v", err)
	}
	got := readListJSON(t, filepath.Join(root, "redis", "db_0", "lists", "q.json"))
	if listFloat(t, got, "format_version") != 1 {
		t.Fatalf("format_version = %v", got["format_version"])
	}
	if got["expire_at_ms"] != nil {
		t.Fatalf("expire_at_ms must be nil without TTL, got %v", got["expire_at_ms"])
	}
	assertListItems(t, got, []any{"b", "c", "d", "e"})
}

// submitListItems calls HandleListItem for each (seq, value) pair and
// fails the test on first error. Folds the loop boilerplate out of
// the round-trip test so cyclomatic complexity stays under the linter
// ceiling.
func submitListItems(t *testing.T, db *RedisDB, userKey string, items map[int64]string) {
	t.Helper()
	for seq, v := range items {
		if err := db.HandleListItem(listItemKey(userKey, seq), []byte(v)); err != nil {
			t.Fatalf("HandleListItem(seq=%d): %v", seq, err)
		}
	}
}

// assertListItems checks the decoded items array against the expected
// ordered slice, failing with the full surrounding context. Folded
// out of the round-trip test for the same cyclop reason.
func assertListItems(t *testing.T, got map[string]any, want []any) {
	t.Helper()
	items := listItemsArray(t, got)
	if len(items) != len(want) {
		t.Fatalf("len(items) = %d, want %d (got %v)", len(items), len(want), items)
	}
	for i := range want {
		if items[i] != want[i] {
			t.Fatalf("items[%d] = %v, want %v (full order: %v)", i, items[i], want[i], items)
		}
	}
}

// TestRedisDB_ListEmptyListStillEmitsFile mirrors the hash encoder's
// emit-empty rule: LLEN==0 is observable to clients (TYPE returns
// "list", LLEN returns 0), so the dump must preserve existence.
func TestRedisDB_ListEmptyListStillEmitsFile(t *testing.T) {
	t.Parallel()
	db, root := newRedisDB(t)
	if err := db.HandleListMeta(listMetaKey("empty"), listMetaValue(0, 0)); err != nil {
		t.Fatal(err)
	}
	if err := db.Finalize(); err != nil {
		t.Fatal(err)
	}
	got := readListJSON(t, filepath.Join(root, "redis", "db_0", "lists", "empty.json"))
	if items := listItemsArray(t, got); len(items) != 0 {
		t.Fatalf("empty list should emit empty items array, got %v", items)
	}
}

// TestRedisDB_ListTTLInlinedFromScanIndex pins that !redis|ttl| records
// for a list user key fold into the list's JSON `expire_at_ms` rather
// than landing in a separate sidecar (the strings/HLL pattern). A
// list without this field would silently restore as permanent.
func TestRedisDB_ListTTLInlinedFromScanIndex(t *testing.T) {
	t.Parallel()
	db, root := newRedisDB(t)
	if err := db.HandleListMeta(listMetaKey("k"), listMetaValue(0, 1)); err != nil {
		t.Fatal(err)
	}
	if err := db.HandleListItem(listItemKey("k", 0), []byte("v")); err != nil {
		t.Fatal(err)
	}
	if err := db.HandleTTL([]byte("k"), encodeTTLValue(fixedExpireMs)); err != nil {
		t.Fatal(err)
	}
	if err := db.Finalize(); err != nil {
		t.Fatal(err)
	}
	got := readListJSON(t, filepath.Join(root, "redis", "db_0", "lists", "k.json"))
	if listFloat(t, got, "expire_at_ms") != float64(fixedExpireMs) {
		t.Fatalf("expire_at_ms = %v want %d", got["expire_at_ms"], fixedExpireMs)
	}
	if _, err := os.Stat(filepath.Join(root, "redis", "db_0", "lists_ttl.jsonl")); !os.IsNotExist(err) {
		t.Fatalf("unexpected list TTL sidecar: stat err=%v", err)
	}
}

// TestRedisDB_ListLengthMismatchWarns pins the warn-on-mismatch
// contract — same shape as the hash encoder's
// redis_hash_length_mismatch.
func TestRedisDB_ListLengthMismatchWarns(t *testing.T) {
	t.Parallel()
	db, _ := newRedisDB(t)
	var events []string
	db.WithWarnSink(func(event string, _ ...any) { events = append(events, event) })
	if err := db.HandleListMeta(listMetaKey("l"), listMetaValue(0, 5)); err != nil {
		t.Fatal(err)
	}
	if err := db.HandleListItem(listItemKey("l", 0), []byte("only")); err != nil {
		t.Fatal(err)
	}
	if err := db.Finalize(); err != nil {
		t.Fatal(err)
	}
	want := "redis_list_length_mismatch"
	found := false
	for _, e := range events {
		if e == want {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected %q warning, got %v", want, events)
	}
}

// TestRedisDB_ListBinaryItemUsesBase64Envelope confirms non-UTF-8 item
// bytes round-trip via the typed `{"base64":"..."}` envelope rather
// than corrupting on the JSON string boundary.
func TestRedisDB_ListBinaryItemUsesBase64Envelope(t *testing.T) {
	t.Parallel()
	db, root := newRedisDB(t)
	if err := db.HandleListMeta(listMetaKey("blob"), listMetaValue(0, 1)); err != nil {
		t.Fatal(err)
	}
	if err := db.HandleListItem(listItemKey("blob", 0), []byte{0x80, 0xff, 0x01}); err != nil {
		t.Fatal(err)
	}
	if err := db.Finalize(); err != nil {
		t.Fatal(err)
	}
	got := readListJSON(t, filepath.Join(root, "redis", "db_0", "lists", "blob.json"))
	items := listItemsArray(t, got)
	if len(items) != 1 {
		t.Fatalf("len(items) = %d, want 1", len(items))
	}
	envelope, ok := items[0].(map[string]any)
	if !ok {
		t.Fatalf("expected base64 envelope, got %T(%v)", items[0], items[0])
	}
	if envelope["base64"] == "" {
		t.Fatalf("base64 envelope missing payload: %v", envelope)
	}
}

// TestRedisDB_ListHandleListMetaSkipsDeltaKey pins that the
// !lst|meta|d|... family is silently skipped by HandleListMeta. Without
// this, parsing the delta's userKeyLen prefix as the start of a
// userKey would corrupt the lists map. Mirrors the hash delta-key
// guard (Codex P1 round 14 PR #725).
func TestRedisDB_ListHandleListMetaSkipsDeltaKey(t *testing.T) {
	t.Parallel()
	db, _ := newRedisDB(t)
	// Delta record carries the binary 16-byte ListMetaDelta payload,
	// not the 24-byte meta. The encoder must skip the record without
	// consulting its value at all.
	deltaValue := make([]byte, 16)
	if err := db.HandleListMeta(listMetaDeltaKey("k", 7, 0), deltaValue); err != nil {
		t.Fatalf("delta key must be silently skipped, got %v", err)
	}
	if err := db.Finalize(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(db.outRoot, "redis", "db_0", "lists")); !os.IsNotExist(err) {
		t.Fatalf("delta-only run should not create lists/, stat err=%v", err)
	}
}

// TestRedisDB_ListHandleListClaimSkips pins that POP claim records do
// not perturb encoder state. The live read path doesn't consult
// claims (POPs also Del the item key in the same OCC commit), so
// the encoder mirrors that policy.
func TestRedisDB_ListHandleListClaimSkips(t *testing.T) {
	t.Parallel()
	db, root := newRedisDB(t)
	if err := db.HandleListMeta(listMetaKey("q"), listMetaValue(0, 2)); err != nil {
		t.Fatal(err)
	}
	if err := db.HandleListItem(listItemKey("q", 0), []byte("a")); err != nil {
		t.Fatal(err)
	}
	if err := db.HandleListItem(listItemKey("q", 1), []byte("b")); err != nil {
		t.Fatal(err)
	}
	// Stray claim for an already-POP'd seq (item key would already be
	// deleted in the live store; we're just confirming the encoder
	// doesn't blow up or drop unrelated items).
	if err := db.HandleListClaim(listClaimKey("q", -1), nil); err != nil {
		t.Fatalf("claim must be silently skipped, got %v", err)
	}
	if err := db.Finalize(); err != nil {
		t.Fatal(err)
	}
	got := readListJSON(t, filepath.Join(root, "redis", "db_0", "lists", "q.json"))
	items := listItemsArray(t, got)
	if len(items) != 2 || items[0] != "a" || items[1] != "b" {
		t.Fatalf("items = %v, want [a b]", items)
	}
}

// TestRedisDB_ListRejectsMalformedMetaValueLength pins that an
// !lst|meta| value of the wrong length surfaces as an error, not
// silent zero-length state. Mirrors the hash meta length check.
func TestRedisDB_ListRejectsMalformedMetaValueLength(t *testing.T) {
	t.Parallel()
	db, _ := newRedisDB(t)
	// 8 bytes — too short for the 24-byte (Head, Tail, Len) blob.
	err := db.HandleListMeta(listMetaKey("k"), make([]byte, 8))
	if err == nil {
		t.Fatalf("expected error for short meta value")
	}
	if !errors.Is(err, ErrRedisInvalidListMeta) {
		t.Fatalf("expected ErrRedisInvalidListMeta, got %v", err)
	}
}

// TestRedisDB_ListRejectsTruncatedItemKey pins that an !lst|itm| key
// missing the 8-byte trailing seq surfaces as a parse error rather
// than silently degenerating into a userKey-only state.
func TestRedisDB_ListRejectsTruncatedItemKey(t *testing.T) {
	t.Parallel()
	db, _ := newRedisDB(t)
	// Item key with no trailing seq bytes — userKey == "" too short.
	err := db.HandleListItem([]byte(ListItemPrefix), []byte("v"))
	if err == nil {
		t.Fatalf("expected error for truncated item key")
	}
	if !errors.Is(err, ErrRedisInvalidListKey) {
		t.Fatalf("expected ErrRedisInvalidListKey, got %v", err)
	}
}
