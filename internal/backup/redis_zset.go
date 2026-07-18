package backup

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"math"
	"path/filepath"
	"sort"

	pb "github.com/bootjp/elastickv/proto"
	cockroachdberr "github.com/cockroachdb/errors"
	gproto "google.golang.org/protobuf/proto"
)

// Redis zset encoder. Translates raw !zs|... snapshot records into the
// per-zset `zsets/<key>.json` shape defined by Phase 0
// (docs/design/2026_04_29_proposed_snapshot_logical_decoder.md).
//
// Wire format mirrors store/zset_helpers.go:
//   - !zs|meta|<userKeyLen(4)><userKey>                              → 8-byte BE Len
//   - !zs|mem|<userKeyLen(4)><userKey><member>                       → 8-byte IEEE 754 score
//   - !zs|scr|<userKeyLen(4)><userKey><sortableScore(8)><member>     → empty (score index)
//   - !zs|meta|d|<userKeyLen(4)><userKey><...>                       → 8-byte LenDelta
//
// Routing rules:
//   - `!zs|mem|` is the source of truth: it carries both the member name
//     (in the trailing key bytes) and its IEEE 754 score (in the value).
//   - `!zs|scr|` is a secondary index used at scan time on the live side;
//     for backup purposes it is redundant and is silently skipped, the
//     same way `!st|scr|` (no such prefix exists) and `!hs|meta|d|`
//     deltas are skipped by the hash/set encoders.
//   - `!zs|meta|d|` deltas are silently skipped; `!zs|mem|` already
//     reflects the post-delta state at backup time.
const (
	RedisZSetMetaPrefix      = "!zs|meta|"
	RedisZSetMemberPrefix    = "!zs|mem|"
	RedisZSetScorePrefix     = "!zs|scr|"
	RedisZSetMetaDeltaPrefix = "!zs|meta|d|"

	// RedisZSetLegacyBlobPrefix is the consolidated single-key
	// layout the live store still writes for non-empty persisted
	// zsets (`adapter/redis_compat_types.go:82` redisZSetPrefix,
	// produced by `adapter/redis_compat_commands.go:3495-3508` and
	// read by `adapter/redis_compat_helpers.go:610-631` as the
	// fallback when no wide-column members exist). A backup that
	// skipped this prefix would silently drop legacy-only zsets;
	// HandleZSetLegacyBlob decodes the blob and registers the same
	// per-member state HandleZSetMeta + HandleZSetMember would.
	// Codex P1 finding on PR #790 (round 2).
	RedisZSetLegacyBlobPrefix = "!redis|zset|"

	// redisZSetScoreSize is the size of the IEEE 754 big-endian score
	// stored in !zs|mem| values. Same constant as zsetMetaSizeBytes in
	// store/zset_helpers.go; duplicated here to keep the backup
	// package free of internal/storage imports.
	redisZSetScoreSize = 8

	// redisZSetLegacyProtoPrefixLen is the on-disk magic prefix size
	// for `!redis|zset|` values
	// (`adapter/redis_storage_codec.go:15` storedRedisZSetProtoPrefix).
	redisZSetLegacyProtoPrefixLen = 4
)

// redisZSetLegacyProtoPrefix mirrors
// adapter/redis_storage_codec.go:15 storedRedisZSetProtoPrefix. A
// rename on the live side without an accompanying backup update
// surfaces as ErrRedisInvalidZSetLegacyBlob on decode of any real
// cluster dump.
var redisZSetLegacyProtoPrefix = []byte{0x00, 'R', 'Z', 0x01}

// ErrRedisInvalidZSetMeta is returned when an !zs|meta| value is not
// the expected 8-byte big-endian member count.
var ErrRedisInvalidZSetMeta = cockroachdberr.New("backup: invalid !zs|meta| value")

// ErrRedisInvalidZSetLegacyBlob is returned when a `!redis|zset|`
// value's magic prefix is missing, its protobuf body fails to
// unmarshal, or its decoded scores include NaN. Fail-closed for
// the same reason as ErrRedisInvalidZSetMember: silently accepting
// a corrupt blob would lose the entire zset's contents at restore.
var ErrRedisInvalidZSetLegacyBlob = cockroachdberr.New("backup: invalid !redis|zset| value")

// ErrRedisInvalidZSetMember is returned when an !zs|mem| value is not
// the expected 8-byte IEEE 754 score, or contains a NaN score (Redis's
// ZADD command rejects NaN, so a NaN at backup time indicates store
// corruption and a silent fall-through would re-corrupt the restored
// cluster).
var ErrRedisInvalidZSetMember = cockroachdberr.New("backup: invalid !zs|mem| value")

// ErrRedisInvalidZSetKey is returned when an !zs| key cannot be parsed
// for its userKeyLen+userKey (or member) segments.
var ErrRedisInvalidZSetKey = cockroachdberr.New("backup: malformed !zs| key")

// redisZSetState buffers one userKey's zset during a snapshot scan.
// Members are stored as a map keyed by their byte string so duplicate
// HandleZSetMember calls collapse to the latest-wins score (matching
// Redis's ZADD semantics: re-adding a member overwrites the prior
// score).
//
// sawWide tracks whether ANY wide-column row (`!zs|meta|` or
// `!zs|mem|`) has been observed for the user key. This mirrors the
// live read path's source-of-truth selection in
// `adapter/redis_compat_helpers.go:610-620`
// (`RedisServer.loadZSetAt`): when wide-column rows exist they are
// authoritative, and any `!redis|zset|<k>` legacy blob is ignored
// (treated as a stale post-migration leftover). Without this flag,
// the encoder would merge stale legacy members on top of the
// wide-column source-of-truth, surfacing deleted or outdated
// entries in the restored zset. Codex P1 round 3 (PR #790).
type redisZSetState struct {
	metaSeen       bool
	declaredLen    int64
	members        map[string]float64
	sawWide        bool
	expireAtMs     uint64
	hasTTL         bool
	inlineTTLOwned bool
}

// HandleZSetMeta processes one !zs|meta|<len><userKey> record. The
// value is the 8-byte BE member count, optionally followed by an inline
// expireAtMs. We park the declared length so flushZSets can warn on a
// mismatch with the observed member count and register the user key so a
// later !redis|ttl|<userKey> record routes back to this zset state.
//
// !zs|meta|d|... delta keys share the !zs|meta| string prefix, so a
// snapshot dispatcher that routes by "starts with RedisZSetMetaPrefix"
// lands delta records here too. We mirror the hash/set encoder policy
// (PRs #725, #758) and silently skip the delta family because
// !zs|mem| records are the source of truth for restored zset
// contents.
func (r *RedisDB) HandleZSetMeta(key, value []byte) error {
	if bytes.HasPrefix(key, []byte(RedisZSetMetaDeltaPrefix)) {
		return nil
	}
	userKey, ok := parseZSetMetaKey(key)
	if !ok {
		return cockroachdberr.Wrapf(ErrRedisInvalidZSetKey, "meta key: %q", key)
	}
	// Bounds-check the uint64 declared count before narrowing to
	// int64; without this a corrupted store with the high bit set
	// would wrap to a negative declaredLen and fire spurious
	// redis_zset_length_mismatch warnings on every flush. Mirrors
	// the hash/list/set encoders' symmetric guard.
	declaredLen, expireAtMs, hasTTL, inlineTTL, err := decodeRedisCountMeta(value, ErrRedisInvalidZSetMeta)
	if err != nil {
		return err
	}
	st := r.zsetState(userKey)
	st.declaredLen = declaredLen
	st.metaSeen = true
	if inlineTTL {
		st.expireAtMs = expireAtMs
		st.hasTTL = hasTTL
		st.inlineTTLOwned = true
	}
	// Codex P2 (PR #790 r9): do NOT call markZSetWide here. The
	// live read path (adapter/redis_compat_helpers.go:610-621)
	// switches to the wide-column layout only when the !zs|mem|
	// scan finds at least one row. A snapshot containing a
	// !zs|meta| row WITHOUT any !zs|mem| rows continues to serve
	// the legacy `!redis|zset|<k>` blob to live clients. If we
	// invalidated the legacy members on meta-alone, the backup
	// would emit an empty/stale zset and lose user-visible data
	// on restore. Only HandleZSetMember triggers markZSetWide.
	return nil
}

// HandleZSetMember processes one !zs|mem|<len><userKey><member>
// record. The value is the 8-byte IEEE 754 big-endian score. The
// member bytes live in the key (binary-safe).
//
// NaN scores are rejected. Redis's ZADD command itself rejects NaN
// at the wire level, so a NaN at backup time indicates either a
// corrupted store or a write path that bypassed ZADD validation —
// either way, emitting it as `score: null` or letting json.Marshal
// fail mid-flush would corrupt the dump silently. Fail closed here
// so the problem surfaces at the source record.
func (r *RedisDB) HandleZSetMember(key, value []byte) error {
	userKey, member, ok := parseZSetMemberKey(key)
	if !ok {
		return cockroachdberr.Wrapf(ErrRedisInvalidZSetKey, "member key: %q", key)
	}
	if len(value) != redisZSetScoreSize {
		return cockroachdberr.Wrapf(ErrRedisInvalidZSetMember,
			"length %d != %d", len(value), redisZSetScoreSize)
	}
	score := math.Float64frombits(binary.BigEndian.Uint64(value))
	if math.IsNaN(score) {
		return cockroachdberr.Wrapf(ErrRedisInvalidZSetMember,
			"NaN score for member of user key %q (length %d)", userKey, len(userKey))
	}
	st := r.zsetState(userKey)
	// First wide-column row evicts any provisional legacy-blob
	// members on this user key. From here on, the wide-column
	// rows are the authoritative source of truth — matches the
	// live read path's preference in
	// adapter/redis_compat_helpers.go:610-620.
	r.markZSetWide(st)
	st.members[string(member)] = score
	return nil
}

// markZSetWide flips the sawWide flag and, if this is the first
// wide-column observation for the state, clears any provisional
// legacy-blob members. The live read path treats `!redis|zset|`
// as a fallback that is ONLY consulted when no wide-column rows
// exist (`adapter/redis_compat_helpers.go::RedisServer.loadZSetAt`
// line 610-620), so a snapshot containing both layouts for the
// same user key MUST drop the legacy entries to avoid surfacing
// stale post-migration leftovers. Codex P1 round 3 (PR #790).
func (r *RedisDB) markZSetWide(st *redisZSetState) {
	if st.sawWide {
		return
	}
	st.sawWide = true
	// Clear any legacy-blob members deposited before the first
	// wide-column row arrived. Re-using the same map keeps the
	// nil-vs-empty contract for empty-but-meta-seen zsets (an
	// explicit `make` would change observable Finalize behavior
	// for those edge cases).
	for k := range st.members {
		delete(st.members, k)
	}
}

// HandleZSetScore accepts and discards one !zs|scr|... record. The
// score index is a live-side secondary index for ZRANGEBYSCORE; the
// authoritative member→score mapping lives in !zs|mem|. Snapshot
// dispatchers should still route this prefix here (rather than to
// the orphan-record warning sink) so a future audit that greps for
// "every !zs| prefix has a handler" finds one.
func (r *RedisDB) HandleZSetScore(_, _ []byte) error { return nil }

// HandleZSetMetaDelta accepts and discards one !zs|meta|d|... record.
// See HandleZSetMeta's docstring for the rationale; !zs|mem| is the
// source of truth at backup time.
func (r *RedisDB) HandleZSetMetaDelta(_, _ []byte) error { return nil }

// HandleZSetLegacyBlob processes one `!redis|zset|<userKey>` record.
// This is the consolidated single-key layout the live store still
// writes for non-empty persisted zsets (see RedisZSetLegacyBlobPrefix
// docstring). The encoded value is a magic-prefixed
// `pb.RedisZSetValue` carrying every (member, score) pair.
//
// Decoded entries land in the same per-key state HandleZSetMember
// would have produced, so the per-zset JSON output is identical
// regardless of which layout the live store used. NaN scores fail
// closed at intake, matching HandleZSetMember's contract.
//
// Scan-order context (per Pebble lexicographic order):
//
//   - `!redis|zset|<k>` (0x72='r') sorts BEFORE `!zs|...` (0x7A='z'),
//     so a mid-migration store with both formats hits this handler
//     first; later wide-column records merge into the same state.
//     Duplicate HandleZSetMember calls follow the latest-wins policy.
//
//   - `!redis|zset|<k>` (0x72='r' < 0x74='t' for the byte after the
//     shared `!redis|` prefix would be the wrong direction; the
//     actual comparison is byte 'z'=0x7A > 't'=0x74) — so
//     `!redis|ttl|<k>` sorts BEFORE `!redis|zset|<k>`. The real
//     production order for a TTL'd legacy zset is therefore:
//
//     !redis|ttl|k   → HandleTTL (redisKindUnknown) → pendingTTL["k"] = ms
//     !redis|zset|k → HandleZSetLegacyBlob → zsetState() → claimPendingTTL()
//     drains pendingTTL into st.expireAtMs
//
//     The HandleTTL redisKindZSet fast-path branch only fires for
//     the reverse order (custom dispatcher or replay), where a
//     wide-column row registered the key before the TTL arrived.
func (r *RedisDB) HandleZSetLegacyBlob(key, value []byte) error {
	userKey, ok := parseZSetLegacyBlobKey(key)
	if !ok {
		return cockroachdberr.Wrapf(ErrRedisInvalidZSetLegacyBlob, "key: %q", key)
	}
	entries, err := decodeZSetLegacyBlobValue(value)
	if err != nil {
		return err
	}
	st := r.zsetState(userKey)
	if st.sawWide {
		// Wide-column rows already arrived for this user key (rare
		// in practice because !redis|zset| sorts before !zs|...,
		// but possible with a custom dispatcher ordering or a mid-
		// migration replay). The live read path ignores the legacy
		// blob in that case, so we do too — applying these entries
		// would surface stale post-migration leftovers in the dump.
		return nil
	}
	for _, e := range entries {
		st.members[e.member] = e.score
	}
	return nil
}

// zsetLegacyEntry is the per-(member, score) projection extracted
// from a `!redis|zset|` blob's protobuf body.
type zsetLegacyEntry struct {
	member string
	score  float64
}

// parseZSetLegacyBlobKey strips `!redis|zset|` and returns the
// user-key bytes. Unlike the wide-column meta key there is no
// userKeyLen prefix — the live store appends the user key directly
// (`adapter/redis_compat_types.go:177` ZSetKey).
func parseZSetLegacyBlobKey(key []byte) ([]byte, bool) {
	rest := bytes.TrimPrefix(key, []byte(RedisZSetLegacyBlobPrefix))
	if len(rest) == len(key) {
		return nil, false
	}
	return rest, true
}

// decodeZSetLegacyBlobValue strips the magic prefix and unmarshals
// the protobuf body into a slice of (member, score) entries.
// Rejects NaN scores (same fail-closed contract as
// HandleZSetMember).
func decodeZSetLegacyBlobValue(value []byte) ([]zsetLegacyEntry, error) {
	if len(value) < redisZSetLegacyProtoPrefixLen ||
		!bytes.Equal(value[:redisZSetLegacyProtoPrefixLen], redisZSetLegacyProtoPrefix) {
		return nil, cockroachdberr.Wrapf(ErrRedisInvalidZSetLegacyBlob,
			"missing or corrupt magic prefix (len=%d)", len(value))
	}
	msg := &pb.RedisZSetValue{}
	if err := gproto.Unmarshal(value[redisZSetLegacyProtoPrefixLen:], msg); err != nil {
		return nil, cockroachdberr.Wrapf(ErrRedisInvalidZSetLegacyBlob,
			"unmarshal: %v", err)
	}
	out := make([]zsetLegacyEntry, 0, len(msg.GetEntries()))
	for _, e := range msg.GetEntries() {
		score := e.GetScore()
		if math.IsNaN(score) {
			return nil, cockroachdberr.Wrapf(ErrRedisInvalidZSetLegacyBlob,
				"NaN score for member %q", e.GetMember())
		}
		out = append(out, zsetLegacyEntry{member: e.GetMember(), score: score})
	}
	return out, nil
}

// zsetState lazily creates per-key state. Mirrors the hash/list/set
// kindByKey-registration pattern so HandleZSetMeta, HandleZSetMember,
// and the HandleTTL back-edge all agree on the kind.
//
// On first registration we drain any pendingTTL for the user key.
// `!redis|ttl|<k>` lex-sorts BEFORE `!zs|...` (because `r` < `z`),
// so in real snapshot order the TTL arrives FIRST; HandleTTL parks
// it in pendingTTL, and this function inlines it into the zset's
// `expire_at_ms`. Without this drain step, every TTL'd zset would
// restore as permanent. Codex P1 finding on PR #790.
func (r *RedisDB) zsetState(userKey []byte) *redisZSetState {
	uk := string(userKey)
	if st, ok := r.zsets[uk]; ok {
		return st
	}
	st := &redisZSetState{members: make(map[string]float64)}
	if expireAtMs, ok := r.claimPendingTTL(userKey); ok {
		st.expireAtMs = expireAtMs
		st.hasTTL = true
	}
	r.zsets[uk] = st
	r.kindByKey[uk] = redisKindZSet
	return st
}

// parseZSetMetaKey strips !zs|meta| and the 4-byte BE userKeyLen
// prefix. Returns (userKey, true) on success. Delta keys
// (!zs|meta|d|...) share the meta string prefix and would otherwise
// be parsed as base-meta with a garbage userKeyLen — refuse them at
// the boundary so a misrouted delta surfaces a parse error rather
// than silent state corruption. Mirrors parseHashMetaKey/parseSetMetaKey.
func parseZSetMetaKey(key []byte) ([]byte, bool) {
	if bytes.HasPrefix(key, []byte(RedisZSetMetaDeltaPrefix)) {
		return nil, false
	}
	rest := bytes.TrimPrefix(key, []byte(RedisZSetMetaPrefix))
	if len(rest) == len(key) {
		return nil, false
	}
	return parseUserKeyLenPrefix(rest)
}

// parseZSetMemberKey strips !zs|mem| and the 4-byte BE userKeyLen
// prefix, then returns (userKey, member, true). The member bytes
// are everything after the userKey segment — binary-safe per
// Redis's ZADD contract.
func parseZSetMemberKey(key []byte) ([]byte, []byte, bool) {
	rest := bytes.TrimPrefix(key, []byte(RedisZSetMemberPrefix))
	if len(rest) == len(key) {
		return nil, nil, false
	}
	userKey, ok := parseUserKeyLenPrefix(rest)
	if !ok {
		return nil, nil, false
	}
	member := rest[wideColumnUserKeyLenSize+len(userKey):]
	return userKey, member, true
}

// flushZSets writes one JSON file per accumulated zset to
// zsets/<encoded>.json. Empty zsets (Len=0, no members) still emit a
// file when meta was seen, mirroring the hash/list/set encoders:
// their existence is observable to clients (TYPE returns "zset",
// ZCARD returns 0). Mismatched declared-vs-observed length surfaces
// an `redis_zset_length_mismatch` warning.
func (r *RedisDB) flushZSets() error {
	return flushWideColumnDir(r, r.zsets, "zsets", func(dir, uk string, st *redisZSetState) error {
		if r.warn != nil && st.metaSeen && int64(len(st.members)) != st.declaredLen {
			r.warn("redis_zset_length_mismatch",
				"user_key_len", len(uk),
				"declared_len", st.declaredLen,
				"observed_members", len(st.members),
				"hint", "meta record's Len does not match the count of !zs|mem| keys for this user key")
		}
		return r.writeZSetJSON(dir, []byte(uk), st)
	})
}

func (r *RedisDB) writeZSetJSON(dir string, userKey []byte, st *redisZSetState) error {
	encoded := EncodeSegment(userKey)
	if err := r.recordIfFallback(encoded, userKey); err != nil {
		return err
	}
	path := filepath.Join(dir, encoded+".json")
	body, err := marshalZSetJSON(st)
	if err != nil {
		return err
	}
	if err := writeFileAtomic(path, body); err != nil {
		return cockroachdberr.WithStack(err)
	}
	return nil
}

// zsetMemberRecord is the dump-format projection of one zset entry.
// `member` is binary-safe (via marshalRedisBinaryValue) and `score`
// is a JSON RawMessage so finite scores serialize as JSON numbers
// (matching the design example `"score": 100`) while ±Inf, which
// json.Marshal rejects, serialize as the conventional ZADD strings
// `"+inf"` / `"-inf"`. Phase 0b's reverse encoder reads both shapes.
type zsetMemberRecord struct {
	Member json.RawMessage `json:"member"`
	Score  json.RawMessage `json:"score"`
}

// marshalRedisZSetScore renders one IEEE 754 score as the dump's
// score field. ±Inf is emitted as a JSON string ("+inf"/"-inf")
// because json.Marshal returns an error for non-finite floats; this
// matches the conventional ZADD score syntax (Redis accepts "+inf",
// "-inf", "+Inf", "-Inf", and bare "inf"). NaN is unreachable here
// — HandleZSetMember rejects NaN scores at intake — so we do not
// emit a NaN representation.
func marshalRedisZSetScore(score float64) (json.RawMessage, error) {
	if math.IsInf(score, 1) {
		return json.RawMessage(`"+inf"`), nil
	}
	if math.IsInf(score, -1) {
		return json.RawMessage(`"-inf"`), nil
	}
	out, err := json.Marshal(score)
	if err != nil {
		return nil, cockroachdberr.WithStack(err)
	}
	return out, nil
}

// marshalZSetJSON renders one zset state as the design's
// `{format_version, members, expire_at_ms}` JSON shape. Members are
// emitted as an array (not a JSON object) sorted by raw byte order
// of the member name so identical snapshots produce identical dump
// output across runs — same rationale as the hash/set encoders'
// arrays (binary-safe member names that would collide under JSON
// object keying when percent-encoded). Each member name goes through
// marshalRedisBinaryValue so non-UTF-8 bytes round-trip via the
// `{"base64":"..."}` envelope.
//
// Member sort key is the member-name bytes only; if a Phase 0b
// consumer relies on score-sorted output, it should re-sort on
// read. We pick name order here so a `diff -r` of two dumps with
// the same logical contents but mutated scores still produces a
// stable line-by-line diff.
//
// The wrapper shape (format_version + <records> + expire_at_ms) is
// intentionally parallel to marshalHashJSON's because the design
// specifies parallel wrappers per Redis type. Collapsing them into a
// shared helper would require either a map[string]any (kills JSON
// field-order determinism) or a generic struct (can't carry distinct
// JSON tags per type), so we keep the structural parallel explicit.
// Reviewers comparing the two functions should diff
// (hashFieldRecord, "fields") against (zsetMemberRecord, "members") —
// the only intentional divergence.
func marshalZSetJSON(st *redisZSetState) ([]byte, error) { //nolint:dupl // see comment above
	names := make([]string, 0, len(st.members))
	for m := range st.members {
		names = append(names, m)
	}
	sort.Strings(names)
	out := make([]zsetMemberRecord, 0, len(names))
	for _, name := range names {
		nameJSON, err := marshalRedisBinaryValue([]byte(name))
		if err != nil {
			return nil, err
		}
		scoreJSON, err := marshalRedisZSetScore(st.members[name])
		if err != nil {
			return nil, err
		}
		out = append(out, zsetMemberRecord{Member: nameJSON, Score: scoreJSON})
	}
	type record struct {
		FormatVersion uint32             `json:"format_version"`
		Members       []zsetMemberRecord `json:"members"`
		ExpireAtMs    *uint64            `json:"expire_at_ms"`
	}
	rec := record{FormatVersion: 1, Members: out}
	if st.hasTTL {
		ms := st.expireAtMs
		rec.ExpireAtMs = &ms
	}
	body, err := json.MarshalIndent(rec, "", "  ")
	if err != nil {
		return nil, cockroachdberr.WithStack(err)
	}
	return body, nil
}
