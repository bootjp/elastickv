package adapter

import (
	"bytes"
	"context"
	"encoding/binary"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
)

// redisStrEncoding constants for the new magic+version prefixed string format.
// Format: [0xFF 0x01][flags(1)][expireAtMs(8, present iff has_ttl)][user value]
const (
	redisStrMagic      = byte(0xFF)
	redisStrVersion    = byte(0x01)
	redisStrHasTTL     = byte(0x01)
	redisStrBaseHeader = 3 // magic(1) + version(1) + flags(1)
)

// encodeRedisStr encodes a Redis string value with optional TTL.
// New format: [0xFF 0x01][flags][expireAtMs(8, optional)][user value]
func encodeRedisStr(value []byte, expireAt *time.Time) []byte {
	flags := byte(0)
	headerLen := redisStrBaseHeader
	if expireAt != nil {
		flags = redisStrHasTTL
		headerLen += redisUint64Bytes
	}
	buf := make([]byte, headerLen+len(value))
	buf[0] = redisStrMagic
	buf[1] = redisStrVersion
	buf[2] = flags
	if expireAt != nil {
		ms := max(expireAt.UnixMilli(), 0)
		binary.BigEndian.PutUint64(buf[redisStrBaseHeader:redisStrBaseHeader+redisUint64Bytes], uint64(ms)) // #nosec G115
		copy(buf[redisStrBaseHeader+redisUint64Bytes:], value)
	} else {
		copy(buf[redisStrBaseHeader:], value)
	}
	return buf
}

// decodeRedisStr decodes a Redis string value.
// For new format (magic prefix 0xFF 0x01): returns user value and optional expireAt.
// For legacy format (no magic): returns raw bytes as user value with nil expireAt.
func decodeRedisStr(raw []byte) (value []byte, expireAt *time.Time, err error) {
	if isNewRedisStrFormat(raw) {
		if len(raw) < redisStrBaseHeader {
			return nil, nil, errors.New("invalid encoded string: too short")
		}
		flags := raw[2]
		rest := raw[redisStrBaseHeader:]
		if flags&redisStrHasTTL != 0 {
			if len(rest) < redisUint64Bytes {
				return nil, nil, errors.New("invalid encoded string: missing TTL bytes")
			}
			ms := min(binary.BigEndian.Uint64(rest[:redisUint64Bytes]), math.MaxInt64)
			t := time.UnixMilli(int64(ms)) // #nosec G115
			return rest[redisUint64Bytes:], &t, nil
		}
		return rest, nil, nil
	}
	// Legacy format: raw bytes with no TTL header.
	return raw, nil, nil
}

// isNewRedisStrFormat reports whether raw uses the new magic+version prefix.
func isNewRedisStrFormat(raw []byte) bool {
	return len(raw) >= 2 && raw[0] == redisStrMagic && raw[1] == redisStrVersion
}

const (
	redisStrPrefix    = "!redis|str|"
	redisHashPrefix   = "!redis|hash|"
	redisSetPrefix    = "!redis|set|"
	redisZSetPrefix   = "!redis|zset|"
	redisHLLPrefix    = "!redis|hll|"
	redisStreamPrefix = "!redis|stream|"
	redisTTLPrefix    = "!redis|ttl|"
)

var redisInternalPrefixes = []string{
	redisStrPrefix,
	redisHashPrefix,
	redisSetPrefix,
	redisHLLPrefix,
	redisZSetPrefix,
	redisStreamPrefix,
}

const (
	redisUint64Bytes = 8
)

type redisValueType string

const (
	redisTypeNone   redisValueType = "none"
	redisTypeString redisValueType = "string"
	redisTypeList   redisValueType = "list"
	redisTypeHash   redisValueType = "hash"
	redisTypeSet    redisValueType = "set"
	redisTypeZSet   redisValueType = "zset"
	redisTypeStream redisValueType = "stream"
)

// isNonStringCollectionType reports whether typ is a collection type (list,
// hash, set, zset, stream) — i.e. not none and not string. Used to decide
// whether TTL must be stored in a separate !redis|ttl| key rather than
// embedded in the value.
func isNonStringCollectionType(typ redisValueType) bool {
	return typ != redisTypeNone && typ != redisTypeString
}

type redisHashValue map[string]string

type redisSetValue struct {
	Members []string `json:"members,omitempty"`
}

type redisZSetEntry struct {
	Member string  `json:"member"`
	Score  float64 `json:"score"`
}

type redisZSetValue struct {
	Entries []redisZSetEntry `json:"entries,omitempty"`
}

type redisStreamEntry struct {
	ID     string   `json:"id"`
	Fields []string `json:"fields,omitempty"`

	parsedID      redisStreamID
	parsedIDValid bool
}

type redisStreamValue struct {
	Entries []redisStreamEntry `json:"entries,omitempty"`
}

type redisStreamID struct {
	ms  uint64
	seq uint64
}

func redisStrKey(userKey []byte) []byte {
	return append([]byte(redisStrPrefix), userKey...)
}

func redisHashKey(userKey []byte) []byte {
	return append([]byte(redisHashPrefix), userKey...)
}

func redisSetKey(userKey []byte) []byte {
	return append([]byte(redisSetPrefix), userKey...)
}

func redisHLLKey(userKey []byte) []byte {
	return append([]byte(redisHLLPrefix), userKey...)
}

func redisZSetKey(userKey []byte) []byte {
	return append([]byte(redisZSetPrefix), userKey...)
}

func redisStreamKey(userKey []byte) []byte {
	return append([]byte(redisStreamPrefix), userKey...)
}

func redisTTLKey(userKey []byte) []byte {
	return append([]byte(redisTTLPrefix), userKey...)
}

func redisExactSetStorageKey(kind string, userKey []byte) []byte {
	switch kind {
	case "set":
		return redisSetKey(userKey)
	case "hll":
		return redisHLLKey(userKey)
	default:
		return nil
	}
}

func isRedisTTLKey(key []byte) bool {
	return bytes.HasPrefix(key, []byte(redisTTLPrefix))
}

// knownInternalPrefixes lists all key namespace prefixes used by internal
// subsystems. Keys matching any of these prefixes are never legacy Redis
// string keys and must be preserved by migration operations.
var knownInternalPrefixes = [][]byte{
	[]byte("!redis|"),
	[]byte("!lst|"),
	[]byte("!txn|"),
	[]byte("!ddb|"),
	[]byte("!s3|"),
	[]byte("!dist|"),
	[]byte("!zs|"),
	[]byte("!hs|"),
	[]byte("!st|"),
}

func isKnownInternalKey(key []byte) bool {
	if len(key) == 0 || key[0] != '!' {
		return false
	}
	for _, prefix := range knownInternalPrefixes {
		if bytes.HasPrefix(key, prefix) {
			return true
		}
	}
	return false
}

func extractRedisInternalUserKey(key []byte) []byte {
	switch {
	case bytes.HasPrefix(key, []byte(redisStrPrefix)):
		return bytes.TrimPrefix(key, []byte(redisStrPrefix))
	case bytes.HasPrefix(key, []byte(redisHashPrefix)):
		return bytes.TrimPrefix(key, []byte(redisHashPrefix))
	case bytes.HasPrefix(key, []byte(redisSetPrefix)):
		return bytes.TrimPrefix(key, []byte(redisSetPrefix))
	case bytes.HasPrefix(key, []byte(redisZSetPrefix)):
		return bytes.TrimPrefix(key, []byte(redisZSetPrefix))
	case bytes.HasPrefix(key, []byte(redisHLLPrefix)):
		return bytes.TrimPrefix(key, []byte(redisHLLPrefix))
	case bytes.HasPrefix(key, []byte(redisStreamPrefix)):
		return bytes.TrimPrefix(key, []byte(redisStreamPrefix))
	case bytes.HasPrefix(key, []byte(redisTTLPrefix)):
		return nil
	default:
		return nil
	}
}

func newRedisStreamEntry(id string, fields []string) redisStreamEntry {
	entry := redisStreamEntry{ID: id, Fields: fields}
	entry.cacheParsedID()
	return entry
}

func (e *redisStreamEntry) cacheParsedID() {
	e.parsedID, e.parsedIDValid = tryParseRedisStreamID(e.ID)
}

func (e redisStreamEntry) compareID(raw string, parsed redisStreamID, parsedValid bool) int {
	if !e.parsedIDValid {
		return compareRedisStreamID(e.ID, raw)
	}
	return compareParsedRedisStreamID(e.ID, e.parsedID, true, raw, parsed, parsedValid)
}

func encodeRedisTTL(expireAt time.Time) []byte {
	ms := max(expireAt.UnixMilli(), 0)
	buf := make([]byte, redisUint64Bytes)
	binary.BigEndian.PutUint64(buf, uint64(ms)) // #nosec G115 -- ms is clamped to non-negative int64 range.
	return buf
}

func decodeRedisTTL(raw []byte) (time.Time, error) {
	if len(raw) != redisUint64Bytes {
		return time.Time{}, errors.WithStack(errors.Newf("invalid ttl length %d", len(raw)))
	}
	ms := min(binary.BigEndian.Uint64(raw), math.MaxInt64)
	return time.UnixMilli(int64(ms)), nil // #nosec G115 -- ms <= MaxInt64 is guaranteed by the check above.
}

func (r *RedisServer) ttlAt(ctx context.Context, userKey []byte, readTS uint64) (*time.Time, error) {
	// For string keys with new encoding: TTL is embedded in the string value.
	// Trust only the embedded TTL; do not fall back to !redis|ttl| for new-format strings.
	raw, err := r.store.GetAt(ctx, redisStrKey(userKey), readTS)
	if err == nil {
		if isNewRedisStrFormat(raw) {
			_, expireAt, decErr := decodeRedisStr(raw)
			return expireAt, decErr
		}
		// Legacy string format: fall through to !redis|ttl| key.
	} else if !errors.Is(err, store.ErrKeyNotFound) {
		return nil, errors.WithStack(err)
	}

	return r.legacyIndexTTLAt(ctx, userKey, readTS)
}

// legacyIndexTTLAt reads the TTL from the !redis|ttl| secondary index only.
// Use this on non-string paths where the embedded-TTL probe would always miss.
func (r *RedisServer) legacyIndexTTLAt(ctx context.Context, userKey []byte, readTS uint64) (*time.Time, error) {
	raw, err := r.store.GetAt(ctx, redisTTLKey(userKey), readTS)
	if err != nil {
		if errors.Is(err, store.ErrKeyNotFound) {
			return nil, nil
		}
		return nil, errors.WithStack(err)
	}
	ttl, err := decodeRedisTTL(raw)
	if err != nil {
		return nil, err
	}
	return &ttl, nil
}

// hasExpired checks TTL expiry. When nonStringOnly is true, the embedded-TTL
// probe is skipped and only the !redis|ttl| index is consulted, avoiding a
// wasted GetAt on !redis|str|<key> for non-string types.
func (r *RedisServer) hasExpired(ctx context.Context, userKey []byte, readTS uint64, nonStringOnly bool) (bool, error) {
	var (
		ttl *time.Time
		err error
	)
	if nonStringOnly {
		ttl, err = r.legacyIndexTTLAt(ctx, userKey, readTS)
	} else {
		ttl, err = r.ttlAt(ctx, userKey, readTS)
	}
	if err != nil {
		return false, err
	}
	if ttl == nil {
		return false, nil
	}
	return !ttl.After(time.Now()), nil
}

func ttlMilliseconds(ttl *time.Time) int64 {
	if ttl == nil {
		return -1
	}
	remaining := time.Until(*ttl).Milliseconds()
	if remaining < 0 {
		return -2
	}
	return remaining
}

func sortZSetEntries(entries []redisZSetEntry) {
	sort.SliceStable(entries, func(i, j int) bool {
		if entries[i].Score != entries[j].Score {
			return entries[i].Score < entries[j].Score
		}
		return entries[i].Member < entries[j].Member
	})
}

func zsetEntriesToMap(entries []redisZSetEntry) map[string]float64 {
	out := make(map[string]float64, len(entries))
	for _, entry := range entries {
		out[entry.Member] = entry.Score
	}
	return out
}

func zsetMapToEntries(in map[string]float64) []redisZSetEntry {
	out := make([]redisZSetEntry, 0, len(in))
	for member, score := range in {
		out = append(out, redisZSetEntry{Member: member, Score: score})
	}
	sortZSetEntries(out)
	return out
}

func parseRedisStreamID(raw string) (redisStreamID, error) {
	sep := strings.IndexByte(raw, '-')
	if sep <= 0 || sep >= len(raw)-1 || strings.IndexByte(raw[sep+1:], '-') >= 0 {
		return redisStreamID{}, errors.WithStack(errors.Newf("invalid stream id %q", raw))
	}
	ms, err := strconv.ParseUint(raw[:sep], 10, 64)
	if err != nil {
		return redisStreamID{}, errors.WithStack(err)
	}
	seq, err := strconv.ParseUint(raw[sep+1:], 10, 64)
	if err != nil {
		return redisStreamID{}, errors.WithStack(err)
	}
	return redisStreamID{ms: ms, seq: seq}, nil
}

func compareRedisStreamID(left, right string) int {
	lid, lok := tryParseRedisStreamID(left)
	rid, rok := tryParseRedisStreamID(right)
	return compareParsedRedisStreamID(left, lid, lok, right, rid, rok)
}

func tryParseRedisStreamID(raw string) (redisStreamID, bool) {
	id, err := parseRedisStreamID(raw)
	return id, err == nil
}

func compareParsedRedisStreamID(leftRaw string, left redisStreamID, leftValid bool, rightRaw string, right redisStreamID, rightValid bool) int {
	switch {
	case !leftValid && !rightValid:
		return strings.Compare(leftRaw, rightRaw)
	case !leftValid:
		return -1
	case !rightValid:
		return 1
	case left.ms < right.ms:
		return -1
	case left.ms > right.ms:
		return 1
	case left.seq < right.seq:
		return -1
	case left.seq > right.seq:
		return 1
	default:
		return 0
	}
}

func formatRedisFloat(v float64) string {
	return strconv.FormatFloat(v, 'f', -1, 64)
}
