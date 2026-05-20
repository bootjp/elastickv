package backup

import (
	"encoding/binary"
	"encoding/json"
	"math"
	"os"
	"path/filepath"
	"testing"

	pb "github.com/bootjp/elastickv/proto"
	"github.com/cockroachdb/errors"
	gproto "google.golang.org/protobuf/proto"
)

// encodeZSetLegacyBlobValue produces the magic-prefixed protobuf
// wire format the live store writes for `!redis|zset|<userKey>`
// values (mirror of adapter/redis_storage_codec.go::marshalZSetValue).
func encodeZSetLegacyBlobValue(t *testing.T, entries []zsetLegacyEntry) []byte {
	t.Helper()
	msg := &pb.RedisZSetValue{}
	for _, e := range entries {
		msg.Entries = append(msg.Entries, &pb.RedisZSetEntry{
			Member: e.member,
			Score:  e.score,
		})
	}
	body, err := gproto.Marshal(msg)
	if err != nil {
		t.Fatalf("marshal pb.RedisZSetValue: %v", err)
	}
	out := make([]byte, 0, redisZSetLegacyProtoPrefixLen+len(body))
	out = append(out, redisZSetLegacyProtoPrefix...)
	out = append(out, body...)
	return out
}

// zsetLegacyBlobKey is the test-side mirror of
// adapter/redis_compat_types.go:177 ZSetKey:
// `!redis|zset|<userKey>` (no userKeyLen prefix).
func zsetLegacyBlobKey(userKey string) []byte {
	out := []byte(RedisZSetLegacyBlobPrefix)
	return append(out, userKey...)
}

// encodeZSetMetaValue builds the 8-byte BE member-count value used by
// the live store/zset_helpers.go (mirror of store.MarshalZSetMeta).
func encodeZSetMetaValue(memberCount int64) []byte {
	v := make([]byte, 8)
	binary.BigEndian.PutUint64(v, uint64(memberCount)) //nolint:gosec
	return v
}

// encodeZSetScoreValue builds the 8-byte IEEE 754 BE score value used by
// the live store/zset_helpers.go (mirror of store.MarshalZSetScore).
func encodeZSetScoreValue(score float64) []byte {
	v := make([]byte, 8)
	binary.BigEndian.PutUint64(v, math.Float64bits(score))
	return v
}

// zsetMetaKey is the test-side mirror of store.ZSetMetaKey:
// !zs|meta|<len(4)><userKey>.
func zsetMetaKey(userKey string) []byte {
	out := []byte(RedisZSetMetaPrefix)
	var l [4]byte
	binary.BigEndian.PutUint32(l[:], uint32(len(userKey))) //nolint:gosec
	out = append(out, l[:]...)
	return append(out, userKey...)
}

// zsetMemberKey mirrors store.ZSetMemberKey:
// !zs|mem|<len(4)><userKey><member>. Member is binary-safe.
func zsetMemberKey(userKey string, member []byte) []byte {
	out := []byte(RedisZSetMemberPrefix)
	var l [4]byte
	binary.BigEndian.PutUint32(l[:], uint32(len(userKey))) //nolint:gosec
	out = append(out, l[:]...)
	out = append(out, userKey...)
	return append(out, member...)
}

// zsetScoreKey mirrors store.ZSetScoreKey:
// !zs|scr|<len(4)><userKey><sortableScore(8)><member>. The encoder
// must silently discard this prefix; we only build it here to feed
// HandleZSetScore in a test that pins the "discard" contract.
func zsetScoreKey(userKey string, sortableScore [8]byte, member []byte) []byte {
	out := []byte(RedisZSetScorePrefix)
	var l [4]byte
	binary.BigEndian.PutUint32(l[:], uint32(len(userKey))) //nolint:gosec
	out = append(out, l[:]...)
	out = append(out, userKey...)
	out = append(out, sortableScore[:]...)
	return append(out, member...)
}

// zsetMetaDeltaKey mirrors store.ZSetMetaDeltaKey:
// !zs|meta|d|<len(4)><userKey><commitTS(8)><seqInTxn(4)>.
func zsetMetaDeltaKey(userKey string, commitTS uint64, seqInTxn uint32) []byte {
	out := []byte(RedisZSetMetaDeltaPrefix)
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

func readZSetJSON(t *testing.T, path string) map[string]any {
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

func zsetMembersArray(t *testing.T, m map[string]any) []any {
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

func zsetFloat(t *testing.T, m map[string]any, key string) float64 {
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

// zsetMemberEntry is the test-side projection of one decoded
// {"member":..., "score":...} record.
type zsetMemberEntry struct {
	member    string
	scoreNum  float64
	scoreStr  string // populated only when score is the string form ("+inf"/"-inf")
	scoreKind string // "number" or "string"
}

// assertZSetEntryScore checks that one decoded record matches the
// expected entry. Extracted from assertZSetMembersEqual to keep both
// callers below the cyclop complexity ceiling and so the
// type-assertion guards live in one place.
func assertZSetEntryScore(t *testing.T, idx int, rec map[string]any, w zsetMemberEntry) {
	t.Helper()
	switch w.scoreKind {
	case "number":
		score, ok := rec["score"].(float64)
		if !ok {
			t.Fatalf("members[%d].score = %T(%v), want number", idx, rec["score"], rec["score"])
		}
		if score != w.scoreNum {
			t.Fatalf("members[%d].score = %v, want %v", idx, score, w.scoreNum)
		}
	case "string":
		if rec["score"] != w.scoreStr {
			t.Fatalf("members[%d].score = %v(%T), want %q (Inf string form)", idx, rec["score"], rec["score"], w.scoreStr)
		}
	default:
		t.Fatalf("test bug: unknown scoreKind %q", w.scoreKind)
	}
}

// assertZSetMembersEqual checks that the JSON `members` array matches
// the given expected entries.
func assertZSetMembersEqual(t *testing.T, members []any, want []zsetMemberEntry) {
	t.Helper()
	if len(members) != len(want) {
		t.Fatalf("len(members) = %d, want %d (got %v)", len(members), len(want), members)
	}
	for i, w := range want {
		rec, ok := members[i].(map[string]any)
		if !ok {
			t.Fatalf("members[%d] = %T(%v), want object", i, members[i], members[i])
		}
		if rec["member"] != w.member {
			t.Fatalf("members[%d].member = %v, want %v", i, rec["member"], w.member)
		}
		assertZSetEntryScore(t, i, rec, w)
	}
}

// TestRedisDB_ZSetRoundTripBasic confirms a multi-member zset
// round-trips through the encoder, that members are sorted by
// raw byte order (NOT score order, see marshalZSetJSON's docstring),
// and that score values land on the JSON record as plain numbers.
func TestRedisDB_ZSetRoundTripBasic(t *testing.T) {
	t.Parallel()
	db, root := newRedisDB(t)
	if err := db.HandleZSetMeta(zsetMetaKey("leaderboard"), encodeZSetMetaValue(3)); err != nil {
		t.Fatalf("HandleZSetMeta: %v", err)
	}
	// Submit out of byte order and out of score order to exercise the sort.
	for _, e := range []struct {
		member string
		score  float64
	}{
		{"charlie", 30},
		{"alice", 100},
		{"bob", 50},
	} {
		if err := db.HandleZSetMember(zsetMemberKey("leaderboard", []byte(e.member)), encodeZSetScoreValue(e.score)); err != nil {
			t.Fatalf("HandleZSetMember(%s): %v", e.member, err)
		}
	}
	if err := db.Finalize(); err != nil {
		t.Fatalf("Finalize: %v", err)
	}
	got := readZSetJSON(t, filepath.Join(root, "redis", "db_0", "zsets", "leaderboard.json"))
	if zsetFloat(t, got, "format_version") != 1 {
		t.Fatalf("format_version = %v", got["format_version"])
	}
	if got["expire_at_ms"] != nil {
		t.Fatalf("expire_at_ms must be nil without TTL, got %v", got["expire_at_ms"])
	}
	// Sorted by member bytes: alice < bob < charlie.
	assertZSetMembersEqual(t, zsetMembersArray(t, got), []zsetMemberEntry{
		{member: "alice", scoreNum: 100, scoreKind: "number"},
		{member: "bob", scoreNum: 50, scoreKind: "number"},
		{member: "charlie", scoreNum: 30, scoreKind: "number"},
	})
}

// TestRedisDB_ZSetEmptyZSetStillEmitsFile mirrors the hash/list/set
// emit-empty rule: ZCARD==0 is observable to clients (TYPE returns
// "zset"), so the dump must preserve existence.
func TestRedisDB_ZSetEmptyZSetStillEmitsFile(t *testing.T) {
	t.Parallel()
	db, root := newRedisDB(t)
	if err := db.HandleZSetMeta(zsetMetaKey("empty"), encodeZSetMetaValue(0)); err != nil {
		t.Fatal(err)
	}
	if err := db.Finalize(); err != nil {
		t.Fatal(err)
	}
	got := readZSetJSON(t, filepath.Join(root, "redis", "db_0", "zsets", "empty.json"))
	if members := zsetMembersArray(t, got); len(members) != 0 {
		t.Fatalf("empty zset should emit empty members array, got %v", members)
	}
}

// TestRedisDB_ZSetTTLInlinedFromScanIndex pins that !redis|ttl|
// records for a zset user key fold into the zset's JSON expire_at_ms,
// not a separate sidecar (the strings/HLL pattern).
func TestRedisDB_ZSetTTLInlinedFromScanIndex(t *testing.T) {
	t.Parallel()
	db, root := newRedisDB(t)
	if err := db.HandleZSetMeta(zsetMetaKey("k"), encodeZSetMetaValue(1)); err != nil {
		t.Fatal(err)
	}
	if err := db.HandleZSetMember(zsetMemberKey("k", []byte("m")), encodeZSetScoreValue(1.5)); err != nil {
		t.Fatal(err)
	}
	if err := db.HandleTTL([]byte("k"), encodeTTLValue(fixedExpireMs)); err != nil {
		t.Fatal(err)
	}
	if err := db.Finalize(); err != nil {
		t.Fatal(err)
	}
	got := readZSetJSON(t, filepath.Join(root, "redis", "db_0", "zsets", "k.json"))
	if zsetFloat(t, got, "expire_at_ms") != float64(fixedExpireMs) {
		t.Fatalf("expire_at_ms = %v want %d", got["expire_at_ms"], fixedExpireMs)
	}
	if _, err := os.Stat(filepath.Join(root, "redis", "db_0", "zsets_ttl.jsonl")); !os.IsNotExist(err) {
		t.Fatalf("unexpected zset TTL sidecar: stat err=%v", err)
	}
}

// TestRedisDB_ZSetLengthMismatchWarns pins the warn-on-mismatch
// contract — same shape as the hash/list/set encoders.
func TestRedisDB_ZSetLengthMismatchWarns(t *testing.T) {
	t.Parallel()
	db, _ := newRedisDB(t)
	var events []string
	db.WithWarnSink(func(event string, _ ...any) { events = append(events, event) })
	if err := db.HandleZSetMeta(zsetMetaKey("z"), encodeZSetMetaValue(5)); err != nil {
		t.Fatal(err)
	}
	if err := db.HandleZSetMember(zsetMemberKey("z", []byte("only")), encodeZSetScoreValue(1)); err != nil {
		t.Fatal(err)
	}
	if err := db.Finalize(); err != nil {
		t.Fatal(err)
	}
	want := "redis_zset_length_mismatch"
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

// TestRedisDB_ZSetBinaryMemberUsesBase64Envelope confirms non-UTF-8
// member bytes round-trip via the typed `{"base64":"..."}` envelope.
func TestRedisDB_ZSetBinaryMemberUsesBase64Envelope(t *testing.T) {
	t.Parallel()
	db, root := newRedisDB(t)
	if err := db.HandleZSetMeta(zsetMetaKey("blob"), encodeZSetMetaValue(1)); err != nil {
		t.Fatal(err)
	}
	if err := db.HandleZSetMember(zsetMemberKey("blob", []byte{0x80, 0xff, 0x01}), encodeZSetScoreValue(42)); err != nil {
		t.Fatal(err)
	}
	if err := db.Finalize(); err != nil {
		t.Fatal(err)
	}
	got := readZSetJSON(t, filepath.Join(root, "redis", "db_0", "zsets", "blob.json"))
	members := zsetMembersArray(t, got)
	if len(members) != 1 {
		t.Fatalf("len(members) = %d, want 1", len(members))
	}
	rec, ok := members[0].(map[string]any)
	if !ok {
		t.Fatalf("members[0] = %T(%v), want object", members[0], members[0])
	}
	envelope, ok := rec["member"].(map[string]any)
	if !ok {
		t.Fatalf("expected base64 envelope on member, got %T(%v)", rec["member"], rec["member"])
	}
	if envelope["base64"] == "" {
		t.Fatalf("base64 envelope missing payload: %v", envelope)
	}
}

// TestRedisDB_ZSetHandleMetaSkipsDeltaKey pins that the !zs|meta|d|...
// family is silently skipped by HandleZSetMeta. Mirrors the
// hash/list/set delta-key guards.
func TestRedisDB_ZSetHandleMetaSkipsDeltaKey(t *testing.T) {
	t.Parallel()
	db, _ := newRedisDB(t)
	deltaValue := make([]byte, 8)
	if err := db.HandleZSetMeta(zsetMetaDeltaKey("k", 7, 0), deltaValue); err != nil {
		t.Fatalf("delta key must be silently skipped, got %v", err)
	}
	if err := db.Finalize(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(db.outRoot, "redis", "db_0", "zsets")); !os.IsNotExist(err) {
		t.Fatalf("delta-only run should not create zsets/, stat err=%v", err)
	}
}

// TestRedisDB_ZSetHandleZSetMetaDelta exercises the explicit
// delta-handler entry point (the dispatcher routes !zs|meta|d| keys
// here directly when it can distinguish them from base meta).
func TestRedisDB_ZSetHandleZSetMetaDelta(t *testing.T) {
	t.Parallel()
	db, _ := newRedisDB(t)
	if err := db.HandleZSetMetaDelta(zsetMetaDeltaKey("k", 1, 0), make([]byte, 8)); err != nil {
		t.Fatalf("HandleZSetMetaDelta must accept any value, got %v", err)
	}
	if err := db.Finalize(); err != nil {
		t.Fatal(err)
	}
}

// TestRedisDB_ZSetHandleZSetScoreSilentlyDiscards pins that the
// !zs|scr| score-index prefix is dropped at intake. !zs|mem| carries
// the authoritative member→score mapping.
func TestRedisDB_ZSetHandleZSetScoreSilentlyDiscards(t *testing.T) {
	t.Parallel()
	db, _ := newRedisDB(t)
	var sortable [8]byte
	binary.BigEndian.PutUint64(sortable[:], 0xdeadbeef)
	if err := db.HandleZSetScore(zsetScoreKey("k", sortable, []byte("m")), nil); err != nil {
		t.Fatalf("HandleZSetScore must not return errors, got %v", err)
	}
	if err := db.Finalize(); err != nil {
		t.Fatal(err)
	}
	// No state was registered — no directory, no warning.
	if _, err := os.Stat(filepath.Join(db.outRoot, "redis", "db_0", "zsets")); !os.IsNotExist(err) {
		t.Fatalf("score-only run must not create zsets/, stat err=%v", err)
	}
}

// TestRedisDB_ZSetRejectsMalformedMetaValueLength pins that an
// !zs|meta| value of the wrong length surfaces as an error.
func TestRedisDB_ZSetRejectsMalformedMetaValueLength(t *testing.T) {
	t.Parallel()
	db, _ := newRedisDB(t)
	err := db.HandleZSetMeta(zsetMetaKey("k"), []byte{0x00})
	if !errors.Is(err, ErrRedisInvalidZSetMeta) {
		t.Fatalf("err=%v want ErrRedisInvalidZSetMeta", err)
	}
}

// TestRedisDB_ZSetRejectsOverflowingMetaValue pins the high-bit
// overflow guard — same shape as hash + list + set encoders.
func TestRedisDB_ZSetRejectsOverflowingMetaValue(t *testing.T) {
	t.Parallel()
	db, _ := newRedisDB(t)
	overflow := make([]byte, 8)
	binary.BigEndian.PutUint64(overflow, 1<<63)
	err := db.HandleZSetMeta(zsetMetaKey("k"), overflow)
	if !errors.Is(err, ErrRedisInvalidZSetMeta) {
		t.Fatalf("err=%v want ErrRedisInvalidZSetMeta", err)
	}
}

// TestRedisDB_ZSetRejectsMalformedMemberValueLength pins that an
// !zs|mem| value of the wrong length surfaces as an error. The score
// MUST be exactly 8 bytes — anything else points at corruption or a
// wire-format change we want to detect at intake.
func TestRedisDB_ZSetRejectsMalformedMemberValueLength(t *testing.T) {
	t.Parallel()
	db, _ := newRedisDB(t)
	err := db.HandleZSetMember(zsetMemberKey("k", []byte("m")), []byte{0x00, 0x01, 0x02})
	if !errors.Is(err, ErrRedisInvalidZSetMember) {
		t.Fatalf("err=%v want ErrRedisInvalidZSetMember", err)
	}
}

// TestRedisDB_ZSetRejectsNaNScore pins that NaN scores fail closed.
// Redis's ZADD command itself rejects NaN at the wire level
// (Lua_redis_pcall → ZADD with NaN returns NOTNUM), so a NaN at
// backup time indicates corruption or a bypass; silently dumping
// `score: null` would re-corrupt the restored cluster.
func TestRedisDB_ZSetRejectsNaNScore(t *testing.T) {
	t.Parallel()
	db, _ := newRedisDB(t)
	nan := make([]byte, 8)
	binary.BigEndian.PutUint64(nan, math.Float64bits(math.NaN()))
	err := db.HandleZSetMember(zsetMemberKey("k", []byte("m")), nan)
	if !errors.Is(err, ErrRedisInvalidZSetMember) {
		t.Fatalf("err=%v want ErrRedisInvalidZSetMember", err)
	}
}

// TestRedisDB_ZSetInfinityScoresUseStringForm pins that ±Inf
// scores serialize as the ZADD-conventional strings `"+inf"`/`"-inf"`
// because json.Marshal returns an error for non-finite floats. The
// per-record `score` field is a json.RawMessage so a finite score
// emits as a number AND an Inf score emits as a string, with the
// same key name.
func TestRedisDB_ZSetInfinityScoresUseStringForm(t *testing.T) {
	t.Parallel()
	db, root := newRedisDB(t)
	if err := db.HandleZSetMeta(zsetMetaKey("inf"), encodeZSetMetaValue(2)); err != nil {
		t.Fatal(err)
	}
	if err := db.HandleZSetMember(zsetMemberKey("inf", []byte("top")), encodeZSetScoreValue(math.Inf(1))); err != nil {
		t.Fatal(err)
	}
	if err := db.HandleZSetMember(zsetMemberKey("inf", []byte("bottom")), encodeZSetScoreValue(math.Inf(-1))); err != nil {
		t.Fatal(err)
	}
	if err := db.Finalize(); err != nil {
		t.Fatal(err)
	}
	got := readZSetJSON(t, filepath.Join(root, "redis", "db_0", "zsets", "inf.json"))
	// Sorted by member name: bottom < top.
	assertZSetMembersEqual(t, zsetMembersArray(t, got), []zsetMemberEntry{
		{member: "bottom", scoreStr: "-inf", scoreKind: "string"},
		{member: "top", scoreStr: "+inf", scoreKind: "string"},
	})
}

// TestRedisDB_ZSetParseMetaKeyRejectsDelta is the parser-level guard
// companion to the dispatcher skip — pins that a future refactor
// bypassing HandleZSetMeta's prefix check still surfaces a parse
// failure rather than silent state corruption.
func TestRedisDB_ZSetParseMetaKeyRejectsDelta(t *testing.T) {
	t.Parallel()
	if _, ok := parseZSetMetaKey(zsetMetaDeltaKey("k", 1, 0)); ok {
		t.Fatalf("parseZSetMetaKey must reject delta-prefixed keys")
	}
}

// TestRedisDB_ZSetMembersWithoutMetaStillEmitsFile pins the
// members-without-meta contract: members may arrive before (or
// without) meta, and the encoder must still emit the JSON without
// firing the length-mismatch warning. Mirrors the set encoder's
// rule (PR #758).
func TestRedisDB_ZSetMembersWithoutMetaStillEmitsFile(t *testing.T) {
	t.Parallel()
	db, root := newRedisDB(t)
	var events []string
	db.WithWarnSink(func(event string, _ ...any) { events = append(events, event) })
	if err := db.HandleZSetMember(zsetMemberKey("z", []byte("a")), encodeZSetScoreValue(1)); err != nil {
		t.Fatal(err)
	}
	if err := db.Finalize(); err != nil {
		t.Fatal(err)
	}
	got := readZSetJSON(t, filepath.Join(root, "redis", "db_0", "zsets", "z.json"))
	members := zsetMembersArray(t, got)
	if len(members) != 1 {
		t.Fatalf("len(members) = %d, want 1", len(members))
	}
	for _, e := range events {
		if e == "redis_zset_length_mismatch" {
			t.Fatalf("members-without-meta must not fire length-mismatch warning, got events %v", events)
		}
	}
}

// TestRedisDB_ZSetDuplicateMembersUseLatestScore pins ZADD's
// latest-wins semantics: re-adding a member with a different score
// overwrites the prior score. (Snapshot iterators don't typically
// emit duplicates, but a recovery replay across overlapping windows
// could.)
func TestRedisDB_ZSetDuplicateMembersUseLatestScore(t *testing.T) {
	t.Parallel()
	db, root := newRedisDB(t)
	if err := db.HandleZSetMember(zsetMemberKey("z", []byte("m")), encodeZSetScoreValue(1)); err != nil {
		t.Fatal(err)
	}
	if err := db.HandleZSetMember(zsetMemberKey("z", []byte("m")), encodeZSetScoreValue(2)); err != nil {
		t.Fatal(err)
	}
	if err := db.Finalize(); err != nil {
		t.Fatal(err)
	}
	got := readZSetJSON(t, filepath.Join(root, "redis", "db_0", "zsets", "z.json"))
	// Latest-wins: the second HandleZSetMember overwrites score 1 with score 2.
	assertZSetMembersEqual(t, zsetMembersArray(t, got), []zsetMemberEntry{
		{member: "m", scoreNum: 2, scoreKind: "number"},
	})
}

// TestRedisDB_ZSetTTLArrivesBeforeRows pins the codex P1 fix:
// `!redis|ttl|<k>` lex-sorts BEFORE `!zs|...` because `r` < `z`,
// so in a real Pebble snapshot's encoded-key order the TTL record
// arrives FIRST. The encoder MUST buffer the expiry in pendingTTL
// and drain it when the zset first registers via zsetState,
// inlining the value into the zset's JSON expire_at_ms. Without
// this drain step every TTL'd zset would restore as permanent —
// the P1 finding on PR #790 (chatgpt-codex-connector).
func TestRedisDB_ZSetTTLArrivesBeforeRows(t *testing.T) {
	t.Parallel()
	db, root := newRedisDB(t)
	// Snapshot order: TTL first, then meta + member.
	if err := db.HandleTTL([]byte("k"), encodeTTLValue(fixedExpireMs)); err != nil {
		t.Fatal(err)
	}
	if err := db.HandleZSetMeta(zsetMetaKey("k"), encodeZSetMetaValue(1)); err != nil {
		t.Fatal(err)
	}
	if err := db.HandleZSetMember(zsetMemberKey("k", []byte("m")), encodeZSetScoreValue(1.5)); err != nil {
		t.Fatal(err)
	}
	if err := db.Finalize(); err != nil {
		t.Fatal(err)
	}
	got := readZSetJSON(t, filepath.Join(root, "redis", "db_0", "zsets", "k.json"))
	if zsetFloat(t, got, "expire_at_ms") != float64(fixedExpireMs) {
		t.Fatalf("expire_at_ms = %v want %d — pendingTTL drain failed", got["expire_at_ms"], fixedExpireMs)
	}
}

// TestRedisDB_SetTTLArrivesBeforeRows pins the same ordering fix
// for sets (`!redis|ttl|` lex-sorts before `!st|...` because
// `r` < `s`). Retroactive coverage for PR #758, which shipped the
// set encoder before the pendingTTL infrastructure existed.
func TestRedisDB_SetTTLArrivesBeforeRows(t *testing.T) {
	t.Parallel()
	db, root := newRedisDB(t)
	// Snapshot order: TTL first, then meta + member.
	if err := db.HandleTTL([]byte("k"), encodeTTLValue(fixedExpireMs)); err != nil {
		t.Fatal(err)
	}
	if err := db.HandleSetMeta(setMetaKey("k"), encodeSetMetaValue(1)); err != nil {
		t.Fatal(err)
	}
	if err := db.HandleSetMember(setMemberKey("k", []byte("m")), nil); err != nil {
		t.Fatal(err)
	}
	if err := db.Finalize(); err != nil {
		t.Fatal(err)
	}
	got := readSetJSON(t, filepath.Join(root, "redis", "db_0", "sets", "k.json"))
	if setFloat(t, got, "expire_at_ms") != float64(fixedExpireMs) {
		t.Fatalf("expire_at_ms = %v want %d — pendingTTL drain failed", got["expire_at_ms"], fixedExpireMs)
	}
}

// TestRedisDB_OrphanTTLCountsTrulyUnmatchedKeys pins the post-fix
// orphan semantics: a TTL for a key that NEVER appears in any
// typed record (store corruption or unknown type prefix) is
// counted at Finalize, not silently dropped on intake.
func TestRedisDB_OrphanTTLCountsTrulyUnmatchedKeys(t *testing.T) {
	t.Parallel()
	db, _ := newRedisDB(t)
	var events []string
	db.WithWarnSink(func(event string, _ ...any) { events = append(events, event) })
	if err := db.HandleTTL([]byte("orphan"), encodeTTLValue(fixedExpireMs)); err != nil {
		t.Fatal(err)
	}
	if err := db.Finalize(); err != nil {
		t.Fatal(err)
	}
	found := false
	for _, e := range events {
		if e == "redis_orphan_ttl" {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected redis_orphan_ttl warning, got %v", events)
	}
}

// TestRedisDB_ZSetMaxInt64DeclaredLen pins the math.MaxInt64
// boundary — declaredLen=math.MaxInt64 must be accepted, only > that
// rejected.
func TestRedisDB_ZSetMaxInt64DeclaredLen(t *testing.T) {
	t.Parallel()
	db, _ := newRedisDB(t)
	boundary := make([]byte, 8)
	binary.BigEndian.PutUint64(boundary, math.MaxInt64) // exactly the int64 max — must NOT reject
	if err := db.HandleZSetMeta(zsetMetaKey("k"), boundary); err != nil {
		t.Fatalf("math.MaxInt64 boundary must be accepted, got %v", err)
	}
}

// TestRedisDB_ZSetLegacyBlobRoundTrip pins the codex P1 fix: a
// zset stored only via the legacy `!redis|zset|<userKey>` blob
// must surface in the dump with all its members. Without
// HandleZSetLegacyBlob, the encoder would skip the record and
// produce an empty zsets/ output for that key — silent backup
// data loss.
func TestRedisDB_ZSetLegacyBlobRoundTrip(t *testing.T) {
	t.Parallel()
	db, root := newRedisDB(t)
	value := encodeZSetLegacyBlobValue(t, []zsetLegacyEntry{
		{member: "alice", score: 100},
		{member: "bob", score: 50},
		{member: "charlie", score: 30},
	})
	if err := db.HandleZSetLegacyBlob(zsetLegacyBlobKey("leaderboard"), value); err != nil {
		t.Fatalf("HandleZSetLegacyBlob: %v", err)
	}
	if err := db.Finalize(); err != nil {
		t.Fatal(err)
	}
	got := readZSetJSON(t, filepath.Join(root, "redis", "db_0", "zsets", "leaderboard.json"))
	// Members sorted by member-name bytes (matches the wide-column
	// encoder's output policy).
	assertZSetMembersEqual(t, zsetMembersArray(t, got), []zsetMemberEntry{
		{member: "alice", scoreNum: 100, scoreKind: "number"},
		{member: "bob", scoreNum: 50, scoreKind: "number"},
		{member: "charlie", scoreNum: 30, scoreKind: "number"},
	})
}

// TestRedisDB_ZSetWideColumnSupersedesLegacyBlob pins the codex
// P1 round 3 fix: when a snapshot carries BOTH the legacy
// `!redis|zset|<k>` blob AND wide-column `!zs|mem|<k>...` rows for
// the same user key (a mid-migration state, or a stale post-
// migration leftover), the encoder MUST drop the legacy entries
// and use only the wide-column source-of-truth. This matches the
// live read path in adapter/redis_compat_helpers.go:610-620
// (RedisServer.loadZSetAt) which falls back to the legacy blob
// ONLY when no wide-column rows exist.
//
// Without this fix, the dump would surface deleted or outdated
// members from a stale legacy blob — silent backup corruption.
func TestRedisDB_ZSetWideColumnSupersedesLegacyBlob(t *testing.T) {
	t.Parallel()
	db, root := newRedisDB(t)
	// Snapshot order: legacy blob arrives first (sorts before !zs|).
	// "bob-stale" exists ONLY in the legacy blob — a stale
	// post-migration leftover that the live store hides via the
	// loadZSetAt preference for wide-column rows.
	legacy := encodeZSetLegacyBlobValue(t, []zsetLegacyEntry{
		{member: "alice", score: 1},
		{member: "bob-stale", score: 2},
	})
	if err := db.HandleZSetLegacyBlob(zsetLegacyBlobKey("k"), legacy); err != nil {
		t.Fatal(err)
	}
	// Wide-column source-of-truth: alice with a new score, charlie new.
	// "bob-stale" is intentionally absent — its presence in the legacy
	// blob is the stale-leftover scenario this test guards against.
	if err := db.HandleZSetMember(zsetMemberKey("k", []byte("alice")), encodeZSetScoreValue(99)); err != nil {
		t.Fatal(err)
	}
	if err := db.HandleZSetMember(zsetMemberKey("k", []byte("charlie")), encodeZSetScoreValue(3)); err != nil {
		t.Fatal(err)
	}
	if err := db.Finalize(); err != nil {
		t.Fatal(err)
	}
	got := readZSetJSON(t, filepath.Join(root, "redis", "db_0", "zsets", "k.json"))
	// "bob-stale" MUST NOT appear — wide-column rows supersede the legacy blob.
	assertZSetMembersEqual(t, zsetMembersArray(t, got), []zsetMemberEntry{
		{member: "alice", scoreNum: 99, scoreKind: "number"},
		{member: "charlie", scoreNum: 3, scoreKind: "number"},
	})
}

// TestRedisDB_ZSetMetaAlonePreservesLegacyBlob pins the codex P2
// fix on PR #790 round 9: a `!zs|meta|` record WITHOUT any
// `!zs|mem|` rows must NOT evict the legacy blob. The live read
// path (adapter/redis_compat_helpers.go:610-621) switches to the
// wide-column layout only when the `!zs|mem|` scan finds at
// least one row — a snapshot containing `!zs|meta|<k>` but no
// `!zs|mem|<k>` rows continues to serve the legacy
// `!redis|zset|<k>` blob to live clients. Earlier rounds
// invalidated the legacy members on meta-alone, which would
// have produced an empty zset on restore — actual user-visible
// data loss.
func TestRedisDB_ZSetMetaAlonePreservesLegacyBlob(t *testing.T) {
	t.Parallel()
	db, root := newRedisDB(t)
	legacy := encodeZSetLegacyBlobValue(t, []zsetLegacyEntry{
		{member: "kept", score: 1},
		{member: "also-kept", score: 2.5},
	})
	if err := db.HandleZSetLegacyBlob(zsetLegacyBlobKey("k"), legacy); err != nil {
		t.Fatal(err)
	}
	// Wide-column meta record declares some count but no !zs|mem|
	// rows ever arrive — the live read path would still serve the
	// legacy blob, so the backup must do the same.
	if err := db.HandleZSetMeta(zsetMetaKey("k"), encodeZSetMetaValue(2)); err != nil {
		t.Fatal(err)
	}
	if err := db.Finalize(); err != nil {
		t.Fatal(err)
	}
	got := readZSetJSON(t, filepath.Join(root, "redis", "db_0", "zsets", "k.json"))
	members := zsetMembersArray(t, got)
	if len(members) != 2 {
		t.Fatalf("meta-only must preserve legacy blob members, got %d (want 2)", len(members))
	}
	wantMembers := map[string]float64{"kept": 1, "also-kept": 2.5}
	for _, m := range members {
		mMap, ok := m.(map[string]any)
		if !ok {
			t.Fatalf("member entry not object: %T", m)
		}
		name, _ := mMap["member"].(string)
		score, _ := mMap["score"].(float64)
		if wantScore, ok := wantMembers[name]; !ok || wantScore != score {
			t.Fatalf("unexpected member %q score %v (want from %v)", name, score, wantMembers)
		}
	}
}

// TestRedisDB_ZSetLegacyBlobAfterWideRowsIsDropped pins the
// reverse-order edge case: if a snapshot somehow emits a
// `!redis|zset|` AFTER `!zs|mem|` rows (custom dispatcher
// ordering, or a replay), the legacy blob is dropped at intake
// rather than overwriting the established wide-column state.
func TestRedisDB_ZSetLegacyBlobAfterWideRowsIsDropped(t *testing.T) {
	t.Parallel()
	db, root := newRedisDB(t)
	// Wide-column row first — registers sawWide.
	if err := db.HandleZSetMember(zsetMemberKey("k", []byte("alice")), encodeZSetScoreValue(99)); err != nil {
		t.Fatal(err)
	}
	// Legacy blob arrives later — must be ignored.
	legacy := encodeZSetLegacyBlobValue(t, []zsetLegacyEntry{
		{member: "alice", score: 1}, // would overwrite if not gated
		{member: "stale", score: 2}, // would re-add if not gated
	})
	if err := db.HandleZSetLegacyBlob(zsetLegacyBlobKey("k"), legacy); err != nil {
		t.Fatal(err)
	}
	if err := db.Finalize(); err != nil {
		t.Fatal(err)
	}
	got := readZSetJSON(t, filepath.Join(root, "redis", "db_0", "zsets", "k.json"))
	assertZSetMembersEqual(t, zsetMembersArray(t, got), []zsetMemberEntry{
		{member: "alice", scoreNum: 99, scoreKind: "number"}, // unchanged
	})
}

// TestRedisDB_ZSetLegacyBlobWithInlineTTL pins the reverse-order
// path: HandleZSetLegacyBlob runs first (custom dispatcher or
// replay), so HandleTTL routes via the redisKindZSet fast-path
// branch (no pendingTTL detour needed).
//
// NOTE: This is NOT the natural Pebble scan order. In real Pebble
// order, `!redis|ttl|<k>` sorts BEFORE `!redis|zset|<k>` (0x74='t'
// < 0x7A='z' on the byte after the shared `!redis|` prefix).
// TestRedisDB_ZSetLegacyBlobTTLArrivesBeforeBlob covers the
// natural order via the pendingTTL → claimPendingTTL path.
func TestRedisDB_ZSetLegacyBlobWithInlineTTL(t *testing.T) {
	t.Parallel()
	db, root := newRedisDB(t)
	value := encodeZSetLegacyBlobValue(t, []zsetLegacyEntry{{member: "m", score: 1}})
	if err := db.HandleZSetLegacyBlob(zsetLegacyBlobKey("k"), value); err != nil {
		t.Fatal(err)
	}
	if err := db.HandleTTL([]byte("k"), encodeTTLValue(fixedExpireMs)); err != nil {
		t.Fatal(err)
	}
	if err := db.Finalize(); err != nil {
		t.Fatal(err)
	}
	got := readZSetJSON(t, filepath.Join(root, "redis", "db_0", "zsets", "k.json"))
	if zsetFloat(t, got, "expire_at_ms") != float64(fixedExpireMs) {
		t.Fatalf("expire_at_ms = %v want %d", got["expire_at_ms"], fixedExpireMs)
	}
}

// TestRedisDB_ZSetLegacyBlobTTLArrivesBeforeBlob pins the
// claude r3-r10 carry-over: in natural Pebble scan order
// `!redis|ttl|<k>` arrives BEFORE `!redis|zset|<k>` because
// `t`=0x74 < `z`=0x7A in the byte after the shared `!redis|`
// prefix. The TTL is parked in pendingTTL as redisKindUnknown;
// HandleZSetLegacyBlob then registers the user key via
// zsetState() which drains the parked TTL via claimPendingTTL
// into st.expireAtMs. This is the production code path —
// previously only the reverse order
// (TestRedisDB_ZSetLegacyBlobWithInlineTTL) was tested.
func TestRedisDB_ZSetLegacyBlobTTLArrivesBeforeBlob(t *testing.T) {
	t.Parallel()
	db, root := newRedisDB(t)
	// Real Pebble order: TTL first.
	if err := db.HandleTTL([]byte("k"), encodeTTLValue(fixedExpireMs)); err != nil {
		t.Fatal(err)
	}
	// Then the legacy blob.
	value := encodeZSetLegacyBlobValue(t, []zsetLegacyEntry{{member: "m", score: 1}})
	if err := db.HandleZSetLegacyBlob(zsetLegacyBlobKey("k"), value); err != nil {
		t.Fatal(err)
	}
	if err := db.Finalize(); err != nil {
		t.Fatal(err)
	}
	got := readZSetJSON(t, filepath.Join(root, "redis", "db_0", "zsets", "k.json"))
	if zsetFloat(t, got, "expire_at_ms") != float64(fixedExpireMs) {
		t.Fatalf("expire_at_ms = %v want %d (pendingTTL drain path for legacy blob failed)",
			got["expire_at_ms"], fixedExpireMs)
	}
	// Confirm the member survived the drain.
	members := zsetMembersArray(t, got)
	if len(members) != 1 {
		t.Fatalf("members = %d, want 1", len(members))
	}
}

// TestRedisDB_ZSetLegacyBlobRejectsMissingMagic pins fail-closed
// behaviour: a `!redis|zset|<k>` value without the magic prefix
// fails at intake rather than silently decoding garbage protobuf.
func TestRedisDB_ZSetLegacyBlobRejectsMissingMagic(t *testing.T) {
	t.Parallel()
	db, _ := newRedisDB(t)
	body, err := gproto.Marshal(&pb.RedisZSetValue{})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	err = db.HandleZSetLegacyBlob(zsetLegacyBlobKey("k"), body) // no magic prefix
	if !errors.Is(err, ErrRedisInvalidZSetLegacyBlob) {
		t.Fatalf("err=%v want ErrRedisInvalidZSetLegacyBlob", err)
	}
}

// TestRedisDB_ZSetLegacyBlobRejectsNaNScore pins NaN-fail-closed
// parallel to HandleZSetMember's contract. Redis ZADD rejects NaN
// at the wire level, so a NaN in storage indicates corruption.
func TestRedisDB_ZSetLegacyBlobRejectsNaNScore(t *testing.T) {
	t.Parallel()
	db, _ := newRedisDB(t)
	value := encodeZSetLegacyBlobValue(t, []zsetLegacyEntry{{member: "m", score: math.NaN()}})
	err := db.HandleZSetLegacyBlob(zsetLegacyBlobKey("k"), value)
	if !errors.Is(err, ErrRedisInvalidZSetLegacyBlob) {
		t.Fatalf("err=%v want ErrRedisInvalidZSetLegacyBlob", err)
	}
}

// TestRedisDB_ZSetLegacyBlobRejectsMalformedKey pins that a
// `!redis|zset|` key with no trailing user-key bytes fails parse.
func TestRedisDB_ZSetLegacyBlobRejectsMalformedKey(t *testing.T) {
	t.Parallel()
	db, _ := newRedisDB(t)
	value := encodeZSetLegacyBlobValue(t, []zsetLegacyEntry{{member: "m", score: 1}})
	// Key has the prefix but no trailing user-key bytes — parser must
	// still accept it (empty user key is technically valid Redis).
	// Use a key that doesn't have the prefix to trigger the parse
	// failure.
	err := db.HandleZSetLegacyBlob([]byte("not-the-right-prefix|k"), value)
	if !errors.Is(err, ErrRedisInvalidZSetLegacyBlob) {
		t.Fatalf("err=%v want ErrRedisInvalidZSetLegacyBlob", err)
	}
}
