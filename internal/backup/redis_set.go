package backup

import (
	"bytes"
	"encoding/json"
	"path/filepath"
	"sort"

	pb "github.com/bootjp/elastickv/proto"
	cockroachdberr "github.com/cockroachdb/errors"
	gproto "google.golang.org/protobuf/proto"
)

// Redis set encoder. Translates raw !st|... snapshot records into the
// per-set `sets/<key>.json` shape defined by Phase 0
// (docs/design/2026_04_29_proposed_snapshot_logical_decoder.md).
//
// Wire format mirrors store/set_helpers.go:
//   - !st|meta|<userKeyLen(4)><userKey>          → 8-byte BE Len
//   - !st|mem|<userKeyLen(4)><userKey><member>   → empty value; the
//     member bytes live in the key (binary-safe, per Redis's SADD
//     contract).
//   - !st|meta|d|<userKeyLen(4)><userKey>...     → 8-byte LenDelta;
//     skipped silently. Same policy as hash deltas: !st|mem| keys
//     are the source of truth at backup time, and delta arithmetic
//     does not need to be replayed.
const (
	RedisSetMetaPrefix      = "!st|meta|"
	RedisSetMemberPrefix    = "!st|mem|"
	RedisSetMetaDeltaPrefix = "!st|meta|d|"

	// RedisSetLegacyBlobPrefix is the consolidated single-key layout the live
	// store still reads for sets that predate the wide-column migration:
	// adapter/redis_set_cmds.go's buildSetLegacyMigrationElems falls back to
	// !redis|set|<userKey> until the next write migrates the key. A dump that
	// skipped it would silently omit those sets.
	RedisSetLegacyBlobPrefix = "!redis|set|"
)

// redisSetLegacyProtoPrefix is the 4-byte magic the live store writes ahead of
// the pb.RedisSetValue body (adapter/redis_storage_codec.go's
// storedRedisSetProtoPrefix). Duplicated rather than imported because the
// backup package must not depend on the adapter; a rename on the live side
// surfaces as ErrRedisInvalidSetLegacyBlob on any real dump.
var redisSetLegacyProtoPrefix = []byte{0x00, 'R', 'S', 0x01}

// ErrRedisInvalidSetLegacyBlob is returned when a !redis|set| value is not a
// magic-prefixed pb.RedisSetValue.
var ErrRedisInvalidSetLegacyBlob = cockroachdberr.New("backup: invalid !redis|set| value")

// ErrRedisInvalidSetMeta is returned when an !st|meta| value is not
// the expected big-endian member count, optionally followed by inline TTL.
var ErrRedisInvalidSetMeta = cockroachdberr.New("backup: invalid !st|meta| value")

// ErrRedisInvalidSetKey is returned when an !st| key cannot be parsed
// for its userKeyLen+userKey (or member) segments.
var ErrRedisInvalidSetKey = cockroachdberr.New("backup: malformed !st| key")

// redisSetState buffers one userKey's set during a snapshot scan.
// Members are stored as a map keyed by their byte string so duplicate
// HandleSetMember calls collapse idempotently (a snapshot iterator
// that re-emits a member is harmless — Redis sets are mathematical
// sets, not multisets).
//
// sawWide records whether any wide-column row (!st|meta| or !st|mem|) has been
// seen for the user key. The live read path treats those as authoritative and
// consults the !redis|set| blob only when none exist, so a store holding both
// layouts for one key must drop the legacy members rather than union stale
// ones onto the source of truth.
type redisSetState struct {
	metaSeen       bool
	declaredLen    int64
	members        map[string]struct{}
	sawWide        bool
	expireAtMs     uint64
	hasTTL         bool
	inlineTTLOwned bool
}

// markSetWide flips sawWide and, on the first wide-column row for this key,
// drops any members a legacy blob deposited first. The map is reused rather
// than reallocated so the nil-vs-empty contract for an empty-but-meta-seen set
// stays as it was.
func (r *RedisDB) markSetWide(st *redisSetState) {
	if st.sawWide {
		return
	}
	st.sawWide = true
	for k := range st.members {
		delete(st.members, k)
	}
}

// HandleSetLegacyBlob processes one !redis|set|<userKey> record: the
// consolidated layout the live store still reads for unmigrated sets. The
// value is a magic-prefixed pb.RedisSetValue carrying every member.
//
// Scan-order note: "!redis|set|" sorts before "!st|", so in a Pebble-ordered
// dump this handler normally runs first and any later wide-column row clears
// what it deposited through markSetWide. The reverse order (custom dispatcher
// or replay) drops the blob here instead.
func (r *RedisDB) HandleSetLegacyBlob(key, value []byte) error {
	userKey, ok := parseRedisLegacyBlobKey(key, RedisSetLegacyBlobPrefix)
	if !ok {
		return cockroachdberr.Wrapf(ErrRedisInvalidSetLegacyBlob, "key: %q", key)
	}
	members, err := decodeSetLegacyBlobValue(value)
	if err != nil {
		return err
	}
	st := r.setState(userKey)
	if st.sawWide {
		return nil
	}
	for _, member := range members {
		st.members[member] = struct{}{}
	}
	return nil
}

func decodeSetLegacyBlobValue(value []byte) ([]string, error) {
	if !bytes.HasPrefix(value, redisSetLegacyProtoPrefix) {
		return nil, cockroachdberr.Wrapf(ErrRedisInvalidSetLegacyBlob,
			"missing or corrupt magic prefix (len=%d)", len(value))
	}
	msg := &pb.RedisSetValue{}
	if err := gproto.Unmarshal(value[len(redisSetLegacyProtoPrefix):], msg); err != nil {
		return nil, cockroachdberr.Wrapf(ErrRedisInvalidSetLegacyBlob, "unmarshal: %v", err)
	}
	return msg.GetMembers(), nil
}

// parseRedisLegacyBlobKey strips a consolidated-layout prefix and returns the
// user-key bytes. Unlike the wide-column keys there is no userKeyLen header --
// the live store appends the user key directly -- so an empty remainder is the
// only malformed shape.
func parseRedisLegacyBlobKey(key []byte, prefix string) ([]byte, bool) {
	if !bytes.HasPrefix(key, []byte(prefix)) {
		return nil, false
	}
	rest := key[len(prefix):]
	if len(rest) == 0 {
		return nil, false
	}
	return rest, true
}

// HandleSetMeta processes one !st|meta|<len><userKey> record. The
// value is the 8-byte BE member count, optionally followed by an inline
// expireAtMs. We park the declared length so flushSets can warn on a
// mismatch with the observed member count and register the user key so a
// later !redis|ttl|<userKey> record routes back to this set state.
//
// !st|meta|d|... delta keys share the !st|meta| string prefix, so a
// snapshot dispatcher that routes by "starts with RedisSetMetaPrefix"
// lands delta records here too. The hash encoder solved the analogous
// problem (Codex P1 round 14 PR #725) by silently skipping the delta
// family; we mirror that policy because !st|mem| records are the
// source of truth for the restored set contents.
func (r *RedisDB) HandleSetMeta(key, value []byte) error {
	if bytes.HasPrefix(key, []byte(RedisSetMetaDeltaPrefix)) {
		return nil
	}
	userKey, ok := parseSetMetaKey(key)
	if !ok {
		return cockroachdberr.Wrapf(ErrRedisInvalidSetKey, "meta key: %q", key)
	}
	// Bounds-check the uint64 declared count before narrowing to
	// int64; without this a corrupted store with the high bit set
	// would wrap to a negative declaredLen and fire spurious
	// redis_set_length_mismatch warnings on every flush. Mirrors
	// the hash + list encoders' symmetric guard.
	declaredLen, expireAtMs, hasTTL, inlineTTL, err := decodeRedisCountMeta(value, ErrRedisInvalidSetMeta)
	if err != nil {
		return err
	}
	st := r.setState(userKey)
	r.markSetWide(st)
	st.declaredLen = declaredLen
	st.metaSeen = true
	if inlineTTL {
		st.expireAtMs = expireAtMs
		st.hasTTL = hasTTL
		st.inlineTTLOwned = true
	}
	return nil
}

// HandleSetMember processes one !st|mem|<len><userKey><member>
// record. The value is empty by design (Redis sets store the member
// bytes in the key, not the value), so HandleSetMember discards the
// value argument; the member bytes are extracted from the key's
// trailing segment.
func (r *RedisDB) HandleSetMember(key, _ []byte) error {
	userKey, member, ok := parseSetMemberKey(key)
	if !ok {
		return cockroachdberr.Wrapf(ErrRedisInvalidSetKey, "member key: %q", key)
	}
	st := r.setState(userKey)
	r.markSetWide(st)
	st.members[string(member)] = struct{}{}
	return nil
}

// HandleSetMetaDelta accepts and discards one !st|meta|d|... record.
// See HandleSetMeta's docstring for the rationale; !st|mem| is the
// source of truth at backup time.
func (r *RedisDB) HandleSetMetaDelta(_, _ []byte) error { return nil }

// setState lazily creates per-key state. Mirrors the hash/list
// kindByKey-registration pattern so HandleSetMeta, HandleSetMember,
// and the HandleTTL back-edge all agree on the kind.
//
// On first registration we drain any pendingTTL for the user key.
// `!redis|ttl|<k>` lex-sorts BEFORE `!st|...` (because `r` < `s`),
// so in real snapshot order the TTL arrives FIRST; HandleTTL parks
// it in pendingTTL, and this function inlines it into the set's
// `expire_at_ms`. Without this drain step, every TTL'd set would
// restore as permanent — a latent bug in PR #758 surfaced by codex
// on PR #790. Phase 0a tests added in the same PR pin the ordering.
func (r *RedisDB) setState(userKey []byte) *redisSetState {
	uk := string(userKey)
	if st, ok := r.sets[uk]; ok {
		return st
	}
	st := &redisSetState{members: make(map[string]struct{})}
	if expireAtMs, ok := r.claimPendingTTL(userKey); ok {
		st.expireAtMs = expireAtMs
		st.hasTTL = true
	}
	r.sets[uk] = st
	r.kindByKey[uk] = redisKindSet
	return st
}

// parseSetMetaKey strips !st|meta| and the 4-byte BE userKeyLen
// prefix. Returns (userKey, true) on success. Delta keys
// (!st|meta|d|...) share the meta string prefix and would otherwise
// be parsed as base-meta with a garbage userKeyLen — refuse them
// at the boundary so a misrouted delta surfaces a parse error
// rather than silent state corruption. Mirrors parseHashMetaKey's
// delta guard.
func parseSetMetaKey(key []byte) ([]byte, bool) {
	if bytes.HasPrefix(key, []byte(RedisSetMetaDeltaPrefix)) {
		return nil, false
	}
	rest := bytes.TrimPrefix(key, []byte(RedisSetMetaPrefix))
	if len(rest) == len(key) {
		return nil, false
	}
	return parseUserKeyLenPrefix(rest)
}

// parseSetMemberKey strips !st|mem| and the 4-byte BE userKeyLen
// prefix, then returns (userKey, member, true). The member bytes
// are everything after the userKey segment — binary-safe per
// Redis's SADD contract.
func parseSetMemberKey(key []byte) ([]byte, []byte, bool) {
	rest := bytes.TrimPrefix(key, []byte(RedisSetMemberPrefix))
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

// flushSets writes one JSON file per accumulated set to
// sets/<encoded>.json. Empty sets (Len=0, no members) still emit a
// file when meta was seen, mirroring the hash/list encoders: their
// existence is observable to clients (TYPE returns "set", SCARD
// returns 0). Mismatched declared-vs-observed length surfaces an
// `redis_set_length_mismatch` warning.
func (r *RedisDB) flushSets() error {
	return flushWideColumnDir(r, r.sets, "sets", func(dir, uk string, st *redisSetState) error {
		if r.warn != nil && st.metaSeen && int64(len(st.members)) != st.declaredLen {
			r.warn("redis_set_length_mismatch",
				"user_key_len", len(uk),
				"declared_len", st.declaredLen,
				"observed_members", len(st.members),
				"hint", "meta record's Len does not match the count of !st|mem| keys for this user key")
		}
		return r.writeSetJSON(dir, []byte(uk), st)
	})
}

func (r *RedisDB) writeSetJSON(dir string, userKey []byte, st *redisSetState) error {
	encoded := EncodeSegment(userKey)
	if err := r.recordIfFallback(encoded, userKey); err != nil {
		return err
	}
	path := filepath.Join(dir, encoded+".json")
	body, err := marshalSetJSON(st)
	if err != nil {
		return err
	}
	if err := writeFileAtomic(path, body); err != nil {
		return cockroachdberr.WithStack(err)
	}
	return nil
}

// marshalSetJSON renders one set state as the design's
// `{format_version, members, expire_at_ms}` JSON shape. Members are
// emitted as an array (not a JSON object) and sorted by raw byte
// order so identical snapshots produce identical dump output across
// runs — same rationale as the hash encoder's fields array
// (binary-safe member names that would collide under JSON object
// keying when percent-encoded). Each value goes through
// marshalRedisBinaryValue so non-UTF-8 members round-trip via the
// `{"base64":"..."}` envelope.
func marshalSetJSON(st *redisSetState) ([]byte, error) {
	members := make([]string, 0, len(st.members))
	for m := range st.members {
		members = append(members, m)
	}
	sort.Strings(members)
	out := make([]json.RawMessage, 0, len(members))
	for _, m := range members {
		v, err := marshalRedisBinaryValue([]byte(m))
		if err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	type record struct {
		FormatVersion uint32            `json:"format_version"`
		Members       []json.RawMessage `json:"members"`
		ExpireAtMs    *uint64           `json:"expire_at_ms"`
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
