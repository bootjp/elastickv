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

// encodeSetMetaValue builds the 8-byte BE member-count value used by
// the live store/set_helpers.go (mirror of store.MarshalSetMeta).
func encodeSetMetaValue(memberCount int64) []byte {
	v := make([]byte, 8)
	binary.BigEndian.PutUint64(v, uint64(memberCount)) //nolint:gosec
	return v
}

// setMetaKey is the test-side mirror of store.SetMetaKey:
// !st|meta|<len(4)><userKey>.
func setMetaKey(userKey string) []byte {
	out := []byte(RedisSetMetaPrefix)
	var l [4]byte
	binary.BigEndian.PutUint32(l[:], uint32(len(userKey))) //nolint:gosec
	out = append(out, l[:]...)
	return append(out, userKey...)
}

// setMemberKey mirrors store.SetMemberKey:
// !st|mem|<len(4)><userKey><member>. Member is binary-safe.
func setMemberKey(userKey string, member []byte) []byte {
	out := []byte(RedisSetMemberPrefix)
	var l [4]byte
	binary.BigEndian.PutUint32(l[:], uint32(len(userKey))) //nolint:gosec
	out = append(out, l[:]...)
	out = append(out, userKey...)
	return append(out, member...)
}

// setMetaDeltaKey mirrors store.SetMetaDeltaKey:
// !st|meta|d|<len(4)><userKey><commitTS(8)><seqInTxn(4)>.
func setMetaDeltaKey(userKey string, commitTS uint64, seqInTxn uint32) []byte {
	out := []byte(RedisSetMetaDeltaPrefix)
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

func readSetJSON(t *testing.T, path string) map[string]any {
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

func setMembersArray(t *testing.T, m map[string]any) []any {
	t.Helper()
	v, ok := m["members"]
	if !ok {
		t.Fatalf("members missing in %+v", m)
	}
	raw, ok := v.([]any)
	if !ok {
		t.Fatalf("members = %T(%v), want []any", v, v)
	}
	return raw
}

func setFloat(t *testing.T, m map[string]any, key string) float64 {
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

// TestRedisDB_SetRoundTripBasic confirms a multi-member set
// round-trips through the encoder in sorted byte order and emits
// the correct JSON shape.
func TestRedisDB_SetRoundTripBasic(t *testing.T) {
	t.Parallel()
	db, root := newRedisDB(t)
	if err := db.HandleSetMeta(setMetaKey("colors"), encodeSetMetaValue(3)); err != nil {
		t.Fatalf("HandleSetMeta: %v", err)
	}
	// Submit out of byte order to exercise the sort at flush time.
	for _, m := range []string{"red", "green", "blue"} {
		if err := db.HandleSetMember(setMemberKey("colors", []byte(m)), nil); err != nil {
			t.Fatalf("HandleSetMember(%s): %v", m, err)
		}
	}
	if err := db.Finalize(); err != nil {
		t.Fatalf("Finalize: %v", err)
	}
	got := readSetJSON(t, filepath.Join(root, "redis", "db_0", "sets", "colors.json"))
	if setFloat(t, got, "format_version") != 1 {
		t.Fatalf("format_version = %v", got["format_version"])
	}
	if got["expire_at_ms"] != nil {
		t.Fatalf("expire_at_ms must be nil without TTL, got %v", got["expire_at_ms"])
	}
	members := setMembersArray(t, got)
	// Sorted byte order: blue < green < red.
	want := []any{"blue", "green", "red"}
	if len(members) != len(want) {
		t.Fatalf("len(members) = %d, want %d (got %v)", len(members), len(want), members)
	}
	for i := range want {
		if members[i] != want[i] {
			t.Fatalf("members[%d] = %v, want %v (full: %v)", i, members[i], want[i], members)
		}
	}
}

// TestRedisDB_SetEmptySetStillEmitsFile mirrors the hash/list
// emit-empty rule: SCARD==0 is observable to clients, so the dump
// must preserve existence.
func TestRedisDB_SetEmptySetStillEmitsFile(t *testing.T) {
	t.Parallel()
	db, root := newRedisDB(t)
	if err := db.HandleSetMeta(setMetaKey("empty"), encodeSetMetaValue(0)); err != nil {
		t.Fatal(err)
	}
	if err := db.Finalize(); err != nil {
		t.Fatal(err)
	}
	got := readSetJSON(t, filepath.Join(root, "redis", "db_0", "sets", "empty.json"))
	if members := setMembersArray(t, got); len(members) != 0 {
		t.Fatalf("empty set should emit empty members array, got %v", members)
	}
}

// TestRedisDB_SetTTLInlinedFromScanIndex pins that !redis|ttl|
// records for a set user key fold into the set's JSON expire_at_ms,
// not a separate sidecar (the strings/HLL pattern).
func TestRedisDB_SetTTLInlinedFromScanIndex(t *testing.T) {
	t.Parallel()
	db, root := newRedisDB(t)
	if err := db.HandleSetMeta(setMetaKey("k"), encodeSetMetaValue(1)); err != nil {
		t.Fatal(err)
	}
	if err := db.HandleSetMember(setMemberKey("k", []byte("m")), nil); err != nil {
		t.Fatal(err)
	}
	if err := db.HandleTTL([]byte("k"), encodeTTLValue(fixedExpireMs)); err != nil {
		t.Fatal(err)
	}
	if err := db.Finalize(); err != nil {
		t.Fatal(err)
	}
	got := readSetJSON(t, filepath.Join(root, "redis", "db_0", "sets", "k.json"))
	if setFloat(t, got, "expire_at_ms") != float64(fixedExpireMs) {
		t.Fatalf("expire_at_ms = %v want %d", got["expire_at_ms"], fixedExpireMs)
	}
	if _, err := os.Stat(filepath.Join(root, "redis", "db_0", "sets_ttl.jsonl")); !os.IsNotExist(err) {
		t.Fatalf("unexpected set TTL sidecar: stat err=%v", err)
	}
}

// TestRedisDB_SetLengthMismatchWarns pins the warn-on-mismatch
// contract — same shape as the hash/list encoders.
func TestRedisDB_SetLengthMismatchWarns(t *testing.T) {
	t.Parallel()
	db, _ := newRedisDB(t)
	var events []string
	db.WithWarnSink(func(event string, _ ...any) { events = append(events, event) })
	if err := db.HandleSetMeta(setMetaKey("s"), encodeSetMetaValue(5)); err != nil {
		t.Fatal(err)
	}
	if err := db.HandleSetMember(setMemberKey("s", []byte("only")), nil); err != nil {
		t.Fatal(err)
	}
	if err := db.Finalize(); err != nil {
		t.Fatal(err)
	}
	want := "redis_set_length_mismatch"
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

// TestRedisDB_SetBinaryMemberUsesBase64Envelope confirms non-UTF-8
// member bytes round-trip via the typed `{"base64":"..."}` envelope.
func TestRedisDB_SetBinaryMemberUsesBase64Envelope(t *testing.T) {
	t.Parallel()
	db, root := newRedisDB(t)
	if err := db.HandleSetMeta(setMetaKey("blob"), encodeSetMetaValue(1)); err != nil {
		t.Fatal(err)
	}
	if err := db.HandleSetMember(setMemberKey("blob", []byte{0x80, 0xff, 0x01}), nil); err != nil {
		t.Fatal(err)
	}
	if err := db.Finalize(); err != nil {
		t.Fatal(err)
	}
	got := readSetJSON(t, filepath.Join(root, "redis", "db_0", "sets", "blob.json"))
	members := setMembersArray(t, got)
	if len(members) != 1 {
		t.Fatalf("len(members) = %d, want 1", len(members))
	}
	envelope, ok := members[0].(map[string]any)
	if !ok {
		t.Fatalf("expected base64 envelope, got %T(%v)", members[0], members[0])
	}
	if envelope["base64"] == "" {
		t.Fatalf("base64 envelope missing payload: %v", envelope)
	}
}

// TestRedisDB_SetHandleSetMetaSkipsDeltaKey pins that the
// !st|meta|d|... family is silently skipped by HandleSetMeta.
// Mirrors the hash/list delta-key guards.
func TestRedisDB_SetHandleSetMetaSkipsDeltaKey(t *testing.T) {
	t.Parallel()
	db, _ := newRedisDB(t)
	// Delta value is 8-byte BE LenDelta; encoder must skip without
	// consulting the value at all.
	deltaValue := make([]byte, 8)
	if err := db.HandleSetMeta(setMetaDeltaKey("k", 7, 0), deltaValue); err != nil {
		t.Fatalf("delta key must be silently skipped, got %v", err)
	}
	if err := db.Finalize(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(db.outRoot, "redis", "db_0", "sets")); !os.IsNotExist(err) {
		t.Fatalf("delta-only run should not create sets/, stat err=%v", err)
	}
}

// TestRedisDB_SetRejectsMalformedMetaValueLength pins that an
// !st|meta| value of the wrong length surfaces as an error.
func TestRedisDB_SetRejectsMalformedMetaValueLength(t *testing.T) {
	t.Parallel()
	db, _ := newRedisDB(t)
	err := db.HandleSetMeta(setMetaKey("k"), []byte{0x00})
	if !errors.Is(err, ErrRedisInvalidSetMeta) {
		t.Fatalf("err=%v want ErrRedisInvalidSetMeta", err)
	}
}

// TestRedisDB_SetRejectsOverflowingMetaValue pins the high-bit
// overflow guard — same shape as hash + list encoders.
func TestRedisDB_SetRejectsOverflowingMetaValue(t *testing.T) {
	t.Parallel()
	db, _ := newRedisDB(t)
	overflow := make([]byte, 8)
	binary.BigEndian.PutUint64(overflow, 1<<63)
	err := db.HandleSetMeta(setMetaKey("k"), overflow)
	if !errors.Is(err, ErrRedisInvalidSetMeta) {
		t.Fatalf("err=%v want ErrRedisInvalidSetMeta", err)
	}
}

// TestRedisDB_SetMembersWithoutMetaStillEmitsFile pins that the
// items-without-meta contract from PR #755 round 2 holds for sets
// too: members may arrive before (or without) meta, and the encoder
// must still emit the JSON without firing the length-mismatch
// warning.
func TestRedisDB_SetMembersWithoutMetaStillEmitsFile(t *testing.T) {
	t.Parallel()
	db, root := newRedisDB(t)
	var events []string
	db.WithWarnSink(func(event string, _ ...any) { events = append(events, event) })
	if err := db.HandleSetMember(setMemberKey("s", []byte("a")), nil); err != nil {
		t.Fatal(err)
	}
	if err := db.Finalize(); err != nil {
		t.Fatal(err)
	}
	got := readSetJSON(t, filepath.Join(root, "redis", "db_0", "sets", "s.json"))
	members := setMembersArray(t, got)
	if len(members) != 1 || members[0] != "a" {
		t.Fatalf("members = %v, want [a]", members)
	}
	for _, e := range events {
		if e == "redis_set_length_mismatch" {
			t.Fatalf("members-without-meta must not fire length-mismatch warning, got events %v", events)
		}
	}
}

// TestRedisDB_SetDuplicateMembersCollapse pins the idempotency
// contract: a snapshot iterator that emits the same !st|mem| key
// twice (rare but legal for some scan replays) must NOT produce a
// duplicate entry in the dump. Redis sets are mathematical sets, so
// {a, a, b} dumps as {a, b}.
func TestRedisDB_SetDuplicateMembersCollapse(t *testing.T) {
	t.Parallel()
	db, root := newRedisDB(t)
	if err := db.HandleSetMeta(setMetaKey("s"), encodeSetMetaValue(2)); err != nil {
		t.Fatal(err)
	}
	if err := db.HandleSetMember(setMemberKey("s", []byte("a")), nil); err != nil {
		t.Fatal(err)
	}
	if err := db.HandleSetMember(setMemberKey("s", []byte("a")), nil); err != nil {
		t.Fatal(err)
	}
	if err := db.HandleSetMember(setMemberKey("s", []byte("b")), nil); err != nil {
		t.Fatal(err)
	}
	if err := db.Finalize(); err != nil {
		t.Fatal(err)
	}
	got := readSetJSON(t, filepath.Join(root, "redis", "db_0", "sets", "s.json"))
	members := setMembersArray(t, got)
	if len(members) != 2 {
		t.Fatalf("len(members) = %d, want 2 (duplicates must collapse)", len(members))
	}
}

// TestRedisDB_SetParseSetMetaKeyRejectsDelta is the parser-level
// guard companion to the dispatcher skip — pins that a future
// refactor bypassing HandleSetMeta's prefix check still surfaces a
// parse failure rather than silent state corruption.
func TestRedisDB_SetParseSetMetaKeyRejectsDelta(t *testing.T) {
	t.Parallel()
	if _, ok := parseSetMetaKey(setMetaDeltaKey("k", 1, 0)); ok {
		t.Fatalf("parseSetMetaKey must reject delta-prefixed keys")
	}
}

// TestRedisDB_SetMembersBytesPreservedThroughInt64Path is a sanity
// check that the overflow guard works at the math.MaxInt64
// boundary — declaredLen=math.MaxInt64 must be accepted, only > that
// rejected.
func TestRedisDB_SetMembersBytesPreservedThroughInt64Path(t *testing.T) {
	t.Parallel()
	db, _ := newRedisDB(t)
	boundary := make([]byte, 8)
	binary.BigEndian.PutUint64(boundary, math.MaxInt64) // exactly the int64 max — must NOT reject
	if err := db.HandleSetMeta(setMetaKey("k"), boundary); err != nil {
		t.Fatalf("math.MaxInt64 boundary must be accepted, got %v", err)
	}
}
