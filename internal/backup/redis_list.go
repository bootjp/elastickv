package backup

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"math"
	"path/filepath"
	"sort"

	cockroachdberr "github.com/cockroachdb/errors"
)

// Redis list encoder. Translates raw !lst|... snapshot records into the
// per-list `lists/<key>.json` shape defined by Phase 0
// (docs/design/2026_04_29_implemented_snapshot_logical_decoder.md).
//
// Three on-disk key families share the `!lst|` namespace; only two
// carry restorable state:
//
//   - !lst|meta|<userKey>             -> 24-byte (Head, Tail, Len) blob
//   - !lst|itm|<userKey><seq(8)>      -> raw item bytes (Redis lists
//     are binary-safe)
//   - !lst|claim|...                  -> POP tombstone for OCC
//     uniqueness. The live read
//     path (rangeList →
//     fetchListRange in redis.go)
//     does NOT consult claims —
//     POPs also Del the underlying
//     item key in the same OCC
//     commit, so a snapshot taken
//     after a POP commit has no
//     item record for the popped
//     seq. The encoder therefore
//     skips claim keys entirely.
//   - !lst|meta|d|...                 -> meta delta. The hash encoder
//     skips its analogous deltas
//     and treats !hs|fld| as the
//     source of truth; the list
//     encoder mirrors that policy
//     — !lst|itm| keys are the
//     source of truth and the
//     delta arithmetic is not
//     replayed at backup time.
const (
	ListMetaPrefix      = "!lst|meta|"
	ListItemPrefix      = "!lst|itm|"
	ListMetaDeltaPrefix = "!lst|meta|d|"
	ListClaimPrefix     = "!lst|claim|"

	// listMetaBinarySize mirrors store/list_helpers.go (24 bytes:
	// Head(8) + Tail(8) + Len(8)). Re-declared here rather than
	// imported because the backup package is intentionally adapter-
	// and store-independent.
	listMetaBinarySize = 24

	// listSeqBytes is the fixed width of the trailing sortable-int64
	// sequence number in an !lst|itm| key.
	listSeqBytes = 8
)

// ErrRedisInvalidListMeta is returned when an !lst|meta| value is not
// the expected 24-byte (Head, Tail, Len) layout.
var ErrRedisInvalidListMeta = cockroachdberr.New("backup: invalid !lst|meta| value")

// ErrRedisInvalidListKey is returned when an !lst| key cannot be parsed
// for its userKey + (optional) seq segments.
var ErrRedisInvalidListKey = cockroachdberr.New("backup: malformed !lst| key")

// redisListState buffers one userKey's list during a snapshot scan.
// items is keyed by signed-int64 sequence so the seq-ordering at
// flush time matches the live store's left-to-right order regardless
// of the order in which !lst|itm| records arrive at the dispatcher.
type redisListState struct {
	metaSeen    bool
	declaredLen int64
	items       map[int64][]byte
	expireAtMs  uint64
	hasTTL      bool
}

// HandleListMeta processes one !lst|meta|<userKey> record. The value is
// the 24-byte (Head, Tail, Len) layout. We park the declared length so
// flushLists can warn on a mismatch with the observed item count and
// register the user key so a later !redis|ttl|<userKey> record routes
// back to this list state.
//
// !lst|meta|d|<userKey>... delta keys share the !lst|meta| string
// prefix, so a snapshot dispatcher that routes by "starts with
// ListMetaPrefix" lands delta records here too. The hash encoder
// solved the analogous problem (Codex P1 round 14 PR #725) by silently
// skipping the delta family; we mirror that policy because !lst|itm|
// records are the source of truth for the restored list contents and
// the delta arithmetic does not need to be replayed at backup time.
func (r *RedisDB) HandleListMeta(key, value []byte) error {
	if bytes.HasPrefix(key, []byte(ListMetaDeltaPrefix)) {
		return nil
	}
	userKey, ok := parseListMetaKey(key)
	if !ok {
		return cockroachdberr.Wrapf(ErrRedisInvalidListKey, "meta key: %q", key)
	}
	if len(value) != listMetaBinarySize {
		return cockroachdberr.Wrapf(ErrRedisInvalidListMeta,
			"length %d != %d", len(value), listMetaBinarySize)
	}
	// Length is the only field needed at backup time. Head/Tail are
	// recomputable from the observed seqs and the live store's
	// invariant (Tail = Head + Len), so we deliberately do not
	// persist them.
	rawLen := binary.BigEndian.Uint64(value[16:24])
	if rawLen > math.MaxInt64 {
		return cockroachdberr.Wrapf(ErrRedisInvalidListMeta,
			"declared len %d overflows int64", rawLen)
	}
	st := r.listState(userKey)
	st.declaredLen = int64(rawLen) //nolint:gosec // bounds-checked above
	st.metaSeen = true
	return nil
}

// HandleListItem processes one !lst|itm|<userKey><sortable_seq(8)>
// record. The value is the raw item bytes (binary-safe). The seq is
// the trailing 8-byte sortable-int64 — sortable encoding flips the
// sign bit so a forward byte-ordered scan yields ascending int64,
// which matches the live store's left-to-right read order.
func (r *RedisDB) HandleListItem(key, value []byte) error {
	userKey, seq, ok := parseListItemKey(key)
	if !ok {
		return cockroachdberr.Wrapf(ErrRedisInvalidListKey, "item key: %q", key)
	}
	st := r.listState(userKey)
	st.items[seq] = bytes.Clone(value)
	return nil
}

// HandleListClaim accepts and discards one !lst|claim|... record. See
// the file-level comment: the live read path does not consult claims;
// POP'd item keys are deleted in the same OCC commit. Restored lists
// therefore reflect the post-POP state without any claim replay.
func (r *RedisDB) HandleListClaim(_, _ []byte) error { return nil }

// HandleListMetaDelta accepts and discards one !lst|meta|d|... record.
// See HandleListMeta's docstring for the rationale; !lst|itm| is the
// source of truth at backup time.
func (r *RedisDB) HandleListMetaDelta(_, _ []byte) error { return nil }

// listState lazily creates per-key state. Mirrors the hashState
// kindByKey-registration pattern (PR #725 #1/#3) so HandleListMeta,
// HandleListItem, and the HandleTTL back-edge all agree on the kind.
func (r *RedisDB) listState(userKey []byte) *redisListState {
	uk := string(userKey)
	if st, ok := r.lists[uk]; ok {
		return st
	}
	st := &redisListState{items: make(map[int64][]byte)}
	r.lists[uk] = st
	r.kindByKey[uk] = redisKindList
	return st
}

// parseListMetaKey strips !lst|meta| from a meta key and returns
// (userKey, true). The list meta key shape is `prefix + userKey` with
// no length prefix (mirror of store.ListMetaKey), so the trimmed
// remainder is the userKey verbatim. Delta keys (!lst|meta|d|...)
// share the meta string prefix and must be rejected here so a
// misrouted delta surfaces a parse failure rather than silent state
// corruption — analogous to parseHashMetaKey's delta guard.
func parseListMetaKey(key []byte) ([]byte, bool) {
	if bytes.HasPrefix(key, []byte(ListMetaDeltaPrefix)) {
		return nil, false
	}
	rest := bytes.TrimPrefix(key, []byte(ListMetaPrefix))
	if len(rest) == len(key) {
		return nil, false
	}
	return rest, true
}

// parseListItemKey strips !lst|itm| and extracts (userKey, seq). The
// list item key shape (mirror of store.ListItemKey) is
// `prefix + userKey + sortableInt64(seq)`, with no userKey length
// prefix; the trailing 8 bytes are always the seq, and everything
// in between is the userKey. The seq is decoded by undoing the
// sign-flip encoding (seq^MinInt64) the live store applies for
// byte-order sortability.
func parseListItemKey(key []byte) ([]byte, int64, bool) {
	rest := bytes.TrimPrefix(key, []byte(ListItemPrefix))
	if len(rest) == len(key) {
		return nil, 0, false
	}
	if len(rest) < listSeqBytes {
		return nil, 0, false
	}
	userKey := rest[:len(rest)-listSeqBytes]
	rawSeq := binary.BigEndian.Uint64(rest[len(rest)-listSeqBytes:])
	seq := int64(rawSeq) ^ math.MinInt64 //nolint:gosec // sortable-int64 sign-flip; mirrors store.encodeSortableInt64
	return userKey, seq, true
}

// flushLists writes one JSON file per accumulated list to
// lists/<encoded>.json. Empty lists (Len=0, no items) still emit a
// file when meta was seen, mirroring the hash encoder: their existence
// is observable to clients (LLEN, TYPE) and a restorer that drops the
// file would silently turn an empty-but-extant list into a
// non-existent key. Mismatched declared-vs-observed length surfaces
// an `redis_list_length_mismatch` warning, again matching the hash
// encoder's contract.
func (r *RedisDB) flushLists() error {
	return flushWideColumnDir(r, r.lists, "lists", func(dir, uk string, st *redisListState) error {
		if r.warn != nil && st.metaSeen && int64(len(st.items)) != st.declaredLen {
			r.warn("redis_list_length_mismatch",
				"user_key_len", len(uk),
				"declared_len", st.declaredLen,
				"observed_items", len(st.items),
				"hint", "meta record's Len does not match the count of !lst|itm| keys for this user key")
		}
		return r.writeListJSON(dir, []byte(uk), st)
	})
}

func (r *RedisDB) writeListJSON(dir string, userKey []byte, st *redisListState) error {
	encoded := EncodeSegment(userKey)
	if err := r.recordIfFallback(encoded, userKey); err != nil {
		return err
	}
	path := filepath.Join(dir, encoded+".json")
	body, err := marshalListJSON(st)
	if err != nil {
		return err
	}
	if err := writeFileAtomic(path, body); err != nil {
		return cockroachdberr.WithStack(err)
	}
	return nil
}

// marshalListJSON renders one list state as the design's
// `{format_version, items, expire_at_ms}` JSON shape. Items are
// emitted in ascending seq order — which equals the live read path's
// LPUSH-leftmost-to-RPUSH-rightmost contract — and each value is
// projected through marshalRedisBinaryValue so non-UTF-8 items round-
// trip via the `{"base64":"..."}` envelope rather than corrupting on
// the JSON string boundary.
func marshalListJSON(st *redisListState) ([]byte, error) {
	seqs := make([]int64, 0, len(st.items))
	for s := range st.items {
		seqs = append(seqs, s)
	}
	sort.Slice(seqs, func(i, j int) bool { return seqs[i] < seqs[j] })
	items := make([]json.RawMessage, 0, len(seqs))
	for _, s := range seqs {
		v, err := marshalRedisBinaryValue(st.items[s])
		if err != nil {
			return nil, err
		}
		items = append(items, v)
	}
	type out struct {
		FormatVersion uint32            `json:"format_version"`
		Items         []json.RawMessage `json:"items"`
		ExpireAtMs    *uint64           `json:"expire_at_ms"`
	}
	rec := out{FormatVersion: 1, Items: items}
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
